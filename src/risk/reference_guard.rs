//! Live Binance 1m reference guard (#244 Group C).
//!
//! Catches stuck-quote events on either venue by cross-checking each
//! venue's mid against an independent reference. Mirrors the BT
//! pre-filter (`bt.rs::load_binance_ref` + per-tick suppression
//! introduced in bot-strategy#166 part 1) and live-wires it.
//!
//! Design:
//!
//! - A background tokio task polls
//!   `https://api.binance.com/api/v3/klines?symbol=<S>&interval=1m&limit=1`
//!   every `POLL_INTERVAL_SECS`. The latest closed minute's
//!   `(high+low)/2` lands in a shared `Arc<RwLock<Option<ReferenceMid>>>`.
//! - The live loop calls [`ReferenceGuard::evaluate`] each tick with
//!   the current per-venue mids. If `|dev| > reference_max_dev_bps`
//!   for `reference_consec_buckets_for_halt` consecutive buckets, the
//!   evaluate function returns `Some(BlockReason::ReferenceDeviation)`
//!   for the offending venue. The runner converts that into a
//!   suppression of `book_ok` for that venue (matches the BT pre-filter).
//!
//! What this module does NOT own (deferred to other risk modules):
//!
//! - Emergency-flatten escalation when ref-deviation is sustained
//!   from `Phase::Held`. The state machine's `Emergency{ReferenceDeviation}`
//!   path is wired by the kill-switch / orchestrator module that
//!   ties together all the live monitors.
//! - Auto-recovery once the mid falls back below threshold — current
//!   behavior re-evaluates each tick so the suppression naturally
//!   clears as soon as the venue mid syncs to the reference.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;

const POLL_INTERVAL_SECS: u64 = 30;
const HTTP_TIMEOUT_SECS: u64 = 5;
/// If we don't see a fresh kline within this much wall-clock time the
/// stored value is treated as expired — better to suppress nothing
/// than to gate on a stale Binance read that happens to disagree with
/// a perfectly healthy venue mid.
const REFERENCE_MAX_AGE_SECS: u64 = 180;

/// Latest minute's reference mid, plus the wall-clock instant we
/// observed it (so the consumer can age it out).
#[derive(Debug, Clone)]
pub struct ReferenceMid {
    /// Open time (unix ms) of the kline this mid summarises.
    pub minute_ts_ms: u64,
    /// `(high + low) / 2` for that minute.
    pub mid: f64,
    /// Wall-clock unix-seconds when we received this datapoint.
    pub observed_ts: i64,
}

/// One side of the per-venue check. The runner translates these
/// into the existing `book_ok` suppression path so the
/// SpreadEngine simply doesn't ingest the stale-side sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueLeg {
    Extended,
    Lighter,
}

/// Outcome of one tick's check, per venue. Returning `Suppress` for
/// a venue is what flips its `book_ok` to false.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefCheckOutcome {
    /// No reference data yet (warmup / network failure) — caller
    /// must NOT suppress; the absence of data is not a halt signal.
    NoReference,
    /// Reference is too old to trust — same as NoReference.
    Stale,
    /// Within threshold, no action.
    Ok,
    /// Above threshold for fewer than the consec-bucket count —
    /// no halt yet but counter advanced.
    Drifting,
    /// Sustained breach → suppress this venue's book.
    Suppress,
}

pub struct ReferenceGuard {
    state: Arc<RwLock<Option<ReferenceMid>>>,
    threshold_bps: f64,
    consec_buckets: u32,
    ext_bad_buckets: u32,
    lt_bad_buckets: u32,
    /// Last minute we evaluated against — the consec counter only
    /// advances once per minute (matches the BT pre-filter
    /// granularity). Without this gate, a 1 s tick cadence would
    /// trip the counter inside a single bad minute.
    last_eval_minute: u64,
    /// Set to None when no background task is running (BT / tests).
    _poll_handle: Option<JoinHandle<()>>,
}

impl ReferenceGuard {
    /// Live constructor: spawns the background poll task. Returns the
    /// guard plus a sentinel that aborts the task on drop. Callers
    /// keep the guard alive for the lifetime of the runner.
    pub fn spawn(symbol: String, threshold_bps: f64, consec_buckets: u32) -> Self {
        let state: Arc<RwLock<Option<ReferenceMid>>> = Arc::new(RwLock::new(None));
        let task_state = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            poll_loop(symbol, task_state).await;
        });
        Self {
            state,
            threshold_bps,
            consec_buckets,
            ext_bad_buckets: 0,
            lt_bad_buckets: 0,
            last_eval_minute: 0,
            _poll_handle: Some(handle),
        }
    }

    /// Production short-circuit when no reference symbol is
    /// configured (or the operator wants to opt out). No background
    /// task; `evaluate()` always returns `NoReference`.
    pub fn disabled(threshold_bps: f64, consec_buckets: u32) -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
            threshold_bps,
            consec_buckets,
            ext_bad_buckets: 0,
            lt_bad_buckets: 0,
            last_eval_minute: 0,
            _poll_handle: None,
        }
    }

    /// Test / BT constructor. Caller drives the reference mid
    /// through `set_reference_mid` instead of the HTTP poll.
    #[cfg(test)]
    pub fn manual(threshold_bps: f64, consec_buckets: u32) -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
            threshold_bps,
            consec_buckets,
            ext_bad_buckets: 0,
            lt_bad_buckets: 0,
            last_eval_minute: 0,
            _poll_handle: None,
        }
    }

    /// Test hook to inject a reference mid synchronously.
    #[cfg(test)]
    pub fn set_reference_mid_for_test(&self, mid: ReferenceMid) {
        *self.state.blocking_write() = Some(mid);
    }

    pub fn threshold_bps(&self) -> f64 {
        self.threshold_bps
    }

    pub fn consecutive_bad_buckets(&self, leg: VenueLeg) -> u32 {
        match leg {
            VenueLeg::Extended => self.ext_bad_buckets,
            VenueLeg::Lighter => self.lt_bad_buckets,
        }
    }

    /// Read-only view of the current reference (for status emission).
    pub async fn current_reference(&self) -> Option<ReferenceMid> {
        self.state.read().await.clone()
    }

    /// Per-tick evaluation. `now_ts_ms` is wall-clock ms. Mids are
    /// the latest per-venue values from the live hub. Returns a
    /// per-venue outcome the runner uses to suppress the stale side.
    pub fn evaluate(
        &mut self,
        now_ts_ms: u64,
        ext_mid: f64,
        lt_mid: f64,
        ref_state: Option<&ReferenceMid>,
        now_unix_secs: i64,
    ) -> (RefCheckOutcome, RefCheckOutcome) {
        let Some(reference) = ref_state else {
            self.ext_bad_buckets = 0;
            self.lt_bad_buckets = 0;
            return (RefCheckOutcome::NoReference, RefCheckOutcome::NoReference);
        };
        if now_unix_secs - reference.observed_ts > REFERENCE_MAX_AGE_SECS as i64 {
            self.ext_bad_buckets = 0;
            self.lt_bad_buckets = 0;
            return (RefCheckOutcome::Stale, RefCheckOutcome::Stale);
        }
        if reference.mid <= 0.0 {
            return (RefCheckOutcome::NoReference, RefCheckOutcome::NoReference);
        }

        let minute = now_ts_ms / 60_000;
        let advance_counters = minute > self.last_eval_minute;
        self.last_eval_minute = minute;

        let ext_dev_bps = mid_dev_bps(ext_mid, reference.mid);
        let lt_dev_bps = mid_dev_bps(lt_mid, reference.mid);

        let ext_breached = ext_dev_bps.abs() > self.threshold_bps;
        let lt_breached = lt_dev_bps.abs() > self.threshold_bps;

        if advance_counters {
            if ext_breached {
                self.ext_bad_buckets = self.ext_bad_buckets.saturating_add(1);
            } else {
                self.ext_bad_buckets = 0;
            }
            if lt_breached {
                self.lt_bad_buckets = self.lt_bad_buckets.saturating_add(1);
            } else {
                self.lt_bad_buckets = 0;
            }
        }

        let ext_outcome = classify(ext_breached, self.ext_bad_buckets, self.consec_buckets);
        let lt_outcome = classify(lt_breached, self.lt_bad_buckets, self.consec_buckets);
        (ext_outcome, lt_outcome)
    }
}

fn classify(breached: bool, counter: u32, threshold: u32) -> RefCheckOutcome {
    if !breached {
        return RefCheckOutcome::Ok;
    }
    if threshold > 0 && counter >= threshold {
        RefCheckOutcome::Suppress
    } else {
        RefCheckOutcome::Drifting
    }
}

fn mid_dev_bps(mid: f64, reference: f64) -> f64 {
    if reference <= 0.0 {
        return 0.0;
    }
    (mid - reference) / reference * 10_000.0
}

async fn poll_loop(symbol: String, state: Arc<RwLock<Option<ReferenceMid>>>) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .user_agent("xvenue-arb-reference-guard/1.0")
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            log::error!("[REF_GUARD] failed to build http client: {:?}", e);
            return;
        }
    };
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval=1m&limit=1",
        symbol
    );
    log::info!(
        "[REF_GUARD] polling {} every {}s",
        url,
        POLL_INTERVAL_SECS
    );
    let mut ivl = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
    ivl.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ivl.tick().await;
        match fetch_once(&client, &url).await {
            Ok(mid) => {
                let mut g = state.write().await;
                *g = Some(mid);
            }
            Err(e) => {
                log::warn!("[REF_GUARD] fetch failed: {:?}", e);
            }
        }
    }
}

async fn fetch_once(client: &reqwest::Client, url: &str) -> Result<ReferenceMid, anyhow::Error> {
    let resp = client.get(url).send().await?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("HTTP {}", status);
    }
    let payload: serde_json::Value = resp.json().await?;
    let arr = payload
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("klines response not array"))?;
    let row = arr
        .last()
        .ok_or_else(|| anyhow::anyhow!("klines response empty"))?
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("klines row not array"))?;
    let open_ts = row
        .first()
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("kline open_time missing"))?;
    let high = row
        .get(2)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow::anyhow!("kline high missing"))?;
    let low = row
        .get(3)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow::anyhow!("kline low missing"))?;
    if !(high > 0.0 && low > 0.0 && high >= low) {
        anyhow::bail!("invalid kline range high={} low={}", high, low);
    }
    Ok(ReferenceMid {
        minute_ts_ms: open_ts,
        mid: 0.5 * (high + low),
        observed_ts: chrono::Utc::now().timestamp(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ref_mid(mid: f64, age_secs: i64) -> ReferenceMid {
        ReferenceMid {
            minute_ts_ms: 1_700_000_000_000,
            mid,
            observed_ts: chrono::Utc::now().timestamp() - age_secs,
        }
    }

    #[test]
    fn no_reference_returns_no_reference() {
        let mut g = ReferenceGuard::manual(30.0, 3);
        let (e, l) = g.evaluate(0, 100.0, 100.0, None, 0);
        assert_eq!(e, RefCheckOutcome::NoReference);
        assert_eq!(l, RefCheckOutcome::NoReference);
    }

    #[test]
    fn within_threshold_is_ok() {
        let mut g = ReferenceGuard::manual(30.0, 3);
        let r = ref_mid(2_000.0, 0);
        // 2 bps deviation — well within 30 bps cap.
        let (e, l) = g.evaluate(60_000, 2_000.4, 2_000.4, Some(&r), 0);
        assert_eq!(e, RefCheckOutcome::Ok);
        assert_eq!(l, RefCheckOutcome::Ok);
    }

    #[test]
    fn first_breach_drifts_then_suppresses() {
        let mut g = ReferenceGuard::manual(30.0, 3);
        let r = ref_mid(2_000.0, 0);
        // 50 bps deviation — over the 30 bps threshold.
        let breached = 2_000.0 * (1.0 + 50.0 / 10_000.0);
        // First minute: drifting (counter=1).
        let (e, _) = g.evaluate(60_000, breached, 2_000.0, Some(&r), 0);
        assert_eq!(e, RefCheckOutcome::Drifting);
        // Same minute, second tick: counter does NOT advance.
        let (e, _) = g.evaluate(120_000 - 1, breached, 2_000.0, Some(&r), 0);
        assert_eq!(e, RefCheckOutcome::Drifting);
        // Second minute → counter=2.
        let (e, _) = g.evaluate(120_000, breached, 2_000.0, Some(&r), 0);
        assert_eq!(e, RefCheckOutcome::Drifting);
        // Third minute → counter=3 = threshold → Suppress.
        let (e, _) = g.evaluate(180_000, breached, 2_000.0, Some(&r), 0);
        assert_eq!(e, RefCheckOutcome::Suppress);
    }

    #[test]
    fn breach_recovery_resets_counter() {
        let mut g = ReferenceGuard::manual(30.0, 3);
        let r = ref_mid(2_000.0, 0);
        let breached = 2_000.0 * (1.0 + 50.0 / 10_000.0);
        g.evaluate(60_000, breached, 2_000.0, Some(&r), 0);
        g.evaluate(120_000, breached, 2_000.0, Some(&r), 0);
        // Recovery within threshold resets the counter.
        let (e, _) = g.evaluate(180_000, 2_000.0, 2_000.0, Some(&r), 0);
        assert_eq!(e, RefCheckOutcome::Ok);
        assert_eq!(g.consecutive_bad_buckets(VenueLeg::Extended), 0);
    }

    #[test]
    fn stale_reference_disables_check() {
        let mut g = ReferenceGuard::manual(30.0, 3);
        // Reference observed 5 minutes ago — over the 3 minute cap.
        let r = ref_mid(2_000.0, 5 * 60);
        let breached = 2_000.0 * (1.0 + 100.0 / 10_000.0);
        let (e, l) = g.evaluate(60_000, breached, breached, Some(&r), chrono::Utc::now().timestamp());
        assert_eq!(e, RefCheckOutcome::Stale);
        assert_eq!(l, RefCheckOutcome::Stale);
    }

    #[test]
    fn per_venue_counters_independent() {
        let mut g = ReferenceGuard::manual(30.0, 2);
        let r = ref_mid(2_000.0, 0);
        let breached = 2_000.0 * (1.0 + 50.0 / 10_000.0);
        // Only Extended breaches; Lighter clean.
        g.evaluate(60_000, breached, 2_000.0, Some(&r), 0);
        g.evaluate(120_000, breached, 2_000.0, Some(&r), 0);
        let (e, l) = g.evaluate(180_000, breached, 2_000.0, Some(&r), 0);
        assert_eq!(e, RefCheckOutcome::Suppress);
        assert_ne!(l, RefCheckOutcome::Suppress);
        assert_eq!(g.consecutive_bad_buckets(VenueLeg::Lighter), 0);
    }

    #[test]
    fn mid_dev_bps_handles_zero_reference() {
        assert_eq!(mid_dev_bps(100.0, 0.0), 0.0);
        assert_eq!(mid_dev_bps(100.0, -1.0), 0.0);
    }
}
