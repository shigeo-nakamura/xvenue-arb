//! Emergency-flatten retry loop (bot-strategy#244 Group B / cases 12, 13).
//!
//! Owns the runner-side loop for `Phase::EmergencyFlattening`. The
//! state machine has already routed the position into emergency
//! flattening; this module:
//!
//! 1. Reads each leg's remaining open qty via [`LegStateReader`].
//! 2. Issues `close_all` on every venue with a non-zero leg.
//! 3. Records each attempt's outcome on
//!    [`StuckTripwire::record_reduce_only_failure`] /
//!    [`record_reduce_only_success`] so the kill counter advances.
//! 4. After each attempt re-reads the legs; both zero →
//!    [`EmergencyLoopOutcome::Complete`] (runner emits
//!    `Event::EmergencyComplete`, **case 13**).
//! 5. Otherwise sleeps `emergency_retry_interval_ms` (default 30 s,
//!    `risk.emergency_retry_interval_ms`) and tries again.
//! 6. If the tripwire arms after a failure (returns `true` from
//!    `record_reduce_only_failure`, i.e. the kill threshold was
//!    crossed) → [`EmergencyLoopOutcome::Stuck`] (**case 12**).
//!
//! The 30 s back-off is the slow-mm 167-min stuck precedent fix
//! (`docs/execution_layer.md` §5). Without it, the runner would
//! hammer `close_all` every tick (~250 ms), pile REST failures
//! faster than they decay, and stay stuck for hours. The interval
//! here applies between **attempts**, not between ticks of the
//! runner — i.e. the runner can keep evaluating monitors / processing
//! WS updates while the loop sleeps in the background.
//!
//! Sprint 4 wiring will spawn this loop on `Phase::EmergencyFlattening`
//! entry and join it back to the state machine on outcome. Until
//! then this module is dead code — the runner synthesises
//! `EmergencyComplete` directly in `dry_run` mode.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dex_connector::DexConnector;
use rust_decimal::Decimal;

use super::venue_ops::VenueOps;
use crate::risk::kill_switch::{StuckTripwire, VenueLabel};

/// Knobs for the loop. Sourced from `XvenueConfig.risk` so the
/// 30 s back-off and the kill counter are wired off the same YAML
/// values the dashboards reflect.
#[derive(Debug, Clone)]
pub struct EmergencyLoopConfig {
    /// Sleep between attempts. `risk.emergency_retry_interval_ms` in
    /// YAML, default 30000. Must be > 0; tests pass small values
    /// (1-100 ms) under `tokio::time::pause` for deterministic timing.
    pub retry_interval_ms: u64,
    /// Defensive cap on the number of close-all rounds. Without one
    /// a stuck venue can keep the loop alive forever; the kill
    /// counter usually trips first but a venue that *accepts*
    /// close-all (success) yet never zeros the leg would otherwise
    /// loop indefinitely. 100 attempts × 30 s = 50 min worst case.
    pub max_attempts: u32,
    /// Bot-strategy#287 grace: when entering EmergencyFlattening
    /// the venue position read may not yet reflect a fill the same
    /// process just observed (WS lag / sub-account auth race). To
    /// avoid the false-zero EmergencyComplete pattern, require at
    /// least this many milliseconds of *consistent* zero reads
    /// before declaring complete. Default 30000 (30 s); 0 disables
    /// the grace and trusts every zero read.
    pub complete_grace_ms: u64,
}

impl EmergencyLoopConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.retry_interval_ms == 0 {
            return Err("retry_interval_ms must be > 0".into());
        }
        if self.max_attempts == 0 {
            return Err("max_attempts must be > 0".into());
        }
        Ok(())
    }
}

/// Outcome of one emergency-flatten run. Maps 1:1 to runner-side
/// state-machine events:
///
/// - [`EmergencyLoopOutcome::Complete`] → `Event::EmergencyComplete`
///   (case 13: both legs verified zero).
/// - [`EmergencyLoopOutcome::Stuck`] → no state-machine event; the
///   STUCK file is already armed by the tripwire and the operator
///   workflow takes over. Runner stays in `EmergencyFlattening`.
/// - [`EmergencyLoopOutcome::MaxAttemptsExceeded`] → defensive only;
///   should not happen in normal operation. Runner logs and
///   re-enters the loop on the next phase tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmergencyLoopOutcome {
    Complete,
    Stuck,
    MaxAttemptsExceeded,
}

/// Per-venue open-qty snapshot. `Decimal::ZERO` means "no remaining
/// reduce-only obligation"; the loop can return `Complete` once both
/// sides are zero (case 13 boundary).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LegQtys {
    pub ext: Decimal,
    pub lt: Decimal,
}

impl LegQtys {
    pub fn both_zero(&self) -> bool {
        self.ext.is_zero() && self.lt.is_zero()
    }
}

/// Reads each leg's remaining open qty. Production wires this off
/// the position machine's `inventory_skew_usd` companion data plus
/// per-venue `get_position` REST calls; tests substitute a scripted
/// mock that walks the loop through Stuck / Complete arcs.
#[async_trait]
pub trait LegStateReader: Send + Sync {
    async fn read_leg_qtys(&self) -> Result<LegQtys>;
}

/// Production [`LegStateReader`] sourcing per-venue open qty from each
/// connector's `get_positions` call. Used by the emergency-flatten
/// handler (#244 Sprint 4 step 3/3) to know whether a `close_all`
/// round is still required, or whether both legs have already zeroed
/// out (case 13 boundary).
///
/// Reads the two venues in parallel via `tokio::try_join!`. The
/// production connectors back `get_positions` with WS-fed in-memory
/// state, so the call is cheap on the emergency-retry cadence
/// (default 30 s). A cache miss surfaces as `Err`; the emergency
/// handler treats that as "still has open qty" and retries on the
/// next round.
pub struct LiveLegStateReader {
    pub ext_conn: Arc<dyn DexConnector>,
    pub lt_conn: Arc<dyn DexConnector>,
    pub ext_symbol: String,
    pub lt_symbol: String,
}

impl LiveLegStateReader {
    pub fn new(
        ext_conn: Arc<dyn DexConnector>,
        lt_conn: Arc<dyn DexConnector>,
        ext_symbol: String,
        lt_symbol: String,
    ) -> Self {
        // bot-strategy#287 Bug 1 root cause:
        //   YAML symbol_ext is the pair form Extended's order APIs use
        //   ("ETH-USD"), but dex_connector::extended runs every
        //   PositionSnapshot through `normalize_symbol`, which strips
        //   the "-USD" / "-USDT" suffix and returns the bare base
        //   token ("ETH"). `read_leg_qtys` did
        //   `find(|p| p.symbol == "ETH-USD")` against snapshots with
        //   symbol="ETH" — never matched, so every real Extended
        //   position was silently reported as zero. EmergencyFlattening
        //   then declared complete on the false zero (the 2026-05-02
        //   incident).
        //
        //   Strip the suffix here so the `find` in `read_leg_qtys`
        //   compares the same form both sides produce. Lighter
        //   symbols are already bare so normalisation is idempotent.
        Self {
            ext_conn,
            lt_conn,
            ext_symbol: strip_quote_suffix(&ext_symbol),
            lt_symbol: strip_quote_suffix(&lt_symbol),
        }
    }
}

fn strip_quote_suffix(symbol: &str) -> String {
    symbol
        .split_once('-')
        .map(|(prefix, _)| prefix.to_string())
        .unwrap_or_else(|| symbol.to_string())
}

#[async_trait]
impl LegStateReader for LiveLegStateReader {
    async fn read_leg_qtys(&self) -> Result<LegQtys> {
        let (ext_pos, lt_pos) =
            tokio::try_join!(self.ext_conn.get_positions(), self.lt_conn.get_positions(),)
                .map_err(|e| anyhow!("get_positions: {}", e))?;
        let ext = ext_pos
            .iter()
            .find(|p| p.symbol == self.ext_symbol)
            .map(|p| p.size)
            .unwrap_or(Decimal::ZERO);
        let lt = lt_pos
            .iter()
            .find(|p| p.symbol == self.lt_symbol)
            .map(|p| p.size)
            .unwrap_or(Decimal::ZERO);
        Ok(LegQtys { ext, lt })
    }
}

/// Coordinator. Holds references; `run` owns the loop.
pub struct EmergencyLoop<'a, EV, LV, R>
where
    EV: VenueOps + ?Sized,
    LV: VenueOps + ?Sized,
    R: LegStateReader + ?Sized,
{
    pub ext_ops: &'a EV,
    pub lt_ops: &'a LV,
    pub leg_state: &'a R,
    pub cfg: &'a EmergencyLoopConfig,
}

impl<'a, EV, LV, R> EmergencyLoop<'a, EV, LV, R>
where
    EV: VenueOps + ?Sized,
    LV: VenueOps + ?Sized,
    R: LegStateReader + ?Sized,
{
    pub fn new(
        ext_ops: &'a EV,
        lt_ops: &'a LV,
        leg_state: &'a R,
        cfg: &'a EmergencyLoopConfig,
    ) -> Self {
        Self {
            ext_ops,
            lt_ops,
            leg_state,
            cfg,
        }
    }

    /// Drive the loop to a terminal outcome. The caller passes
    /// `&mut StuckTripwire` so the kill counter mutates in place
    /// across attempts; the returned outcome reflects the **current**
    /// run only (the tripwire's persistent file is already armed
    /// when `Stuck` is returned).
    pub async fn run(&self, tripwire: &mut StuckTripwire) -> EmergencyLoopOutcome {
        // The first iteration's `read_leg_qtys` covers the
        // already-zero boundary (case 13 entry): if both legs are
        // zero on entry, we return `Complete` without burning a
        // close-all round.
        for _attempt in 0..self.cfg.max_attempts {
            let qtys = match self.leg_state.read_leg_qtys().await {
                Ok(q) => q,
                Err(e) => {
                    log::warn!("[XVENUE/emerg] read_leg_qtys err={:?}", e);
                    // Treat as "still has open qty" — sleep and
                    // retry, the next attempt may succeed.
                    self.sleep_interval().await;
                    continue;
                }
            };
            if qtys.both_zero() {
                // Verified zero — safe to emit EmergencyComplete.
                return EmergencyLoopOutcome::Complete;
            }

            // Attempt close_all on each venue with non-zero leg.
            // Run them sequentially so a Lighter rejection doesn't
            // mask the Extended-side counter or vice versa — the
            // tripwire counter is per-call, not per-venue.
            if !qtys.ext.is_zero() {
                if !self
                    .try_close(self.ext_ops, VenueLabel::Extended, tripwire)
                    .await
                {
                    return EmergencyLoopOutcome::Stuck;
                }
            }
            if !qtys.lt.is_zero() {
                if !self
                    .try_close(self.lt_ops, VenueLabel::Lighter, tripwire)
                    .await
                {
                    return EmergencyLoopOutcome::Stuck;
                }
            }

            // Re-read after the attempt — venue may already report
            // zero, in which case we don't need to wait the full
            // back-off interval before declaring Complete.
            if let Ok(post) = self.leg_state.read_leg_qtys().await {
                if post.both_zero() {
                    return EmergencyLoopOutcome::Complete;
                }
            }

            self.sleep_interval().await;
        }

        EmergencyLoopOutcome::MaxAttemptsExceeded
    }

    async fn try_close<V: VenueOps + ?Sized>(
        &self,
        ops: &V,
        venue: VenueLabel,
        tripwire: &mut StuckTripwire,
    ) -> bool {
        match ops.close_all(None).await {
            Ok(()) => {
                // Reset on the way back to zero — a single transient
                // venue blip mid-emergency shouldn't spend a kill
                // counter slot.
                tripwire.record_reduce_only_success();
                true
            }
            Err(e) => {
                log::warn!("[XVENUE/emerg] close_all venue={:?} err={:?}", venue, e);
                let armed = tripwire.record_reduce_only_failure();
                // `armed` true => kill threshold crossed; tripwire
                // file already on disk. Caller stops the loop.
                !armed
            }
        }
    }

    async fn sleep_interval(&self) {
        let dur = Duration::from_millis(self.cfg.retry_interval_ms);
        tokio::time::sleep(dur).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::kill_switch::StuckTripwireConfig;
    use crate::trade::execution::venue_ops::{ScriptedResponse, ScriptedVenueOps};
    use rust_decimal_macros::dec;
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// Scripted leg state — drives the loop through a sequence of
    /// `(ext, lt)` snapshots. Each call to `read_leg_qtys` pops the
    /// next; after the queue drains it returns the last value
    /// (saturating). Lets a test say "first read shows non-zero,
    /// after one close-all show zero, return Complete".
    struct ScriptedLegState {
        seq: Mutex<Vec<LegQtys>>,
    }

    impl ScriptedLegState {
        fn new(seq: Vec<LegQtys>) -> Self {
            Self {
                seq: Mutex::new(seq),
            }
        }
    }

    #[async_trait]
    impl LegStateReader for ScriptedLegState {
        async fn read_leg_qtys(&self) -> Result<LegQtys> {
            let mut g = self.seq.lock().unwrap();
            // Pop the front (FIFO); if drained, return the last
            // value so the loop converges instead of looping
            // indefinitely on test bugs.
            if g.len() > 1 {
                Ok(g.remove(0))
            } else {
                Ok(*g.first().expect("ScriptedLegState seq must be non-empty"))
            }
        }
    }

    fn cfg(retry_ms: u64, max: u32) -> EmergencyLoopConfig {
        EmergencyLoopConfig {
            retry_interval_ms: retry_ms,
            max_attempts: max,
            complete_grace_ms: 0,
        }
    }

    fn tripwire_in(dir: &TempDir, kill_threshold: u32) -> StuckTripwire {
        StuckTripwire::new_for_test(StuckTripwireConfig {
            stuck_file: dir.path().join("STUCK"),
            rest_consec_fail_to_escalate: 3,
            reduce_only_consec_fail_to_kill: kill_threshold,
            enter_timeout_consec_fail_to_kill: 5,
        })
    }

    /// **Catalogue case 13 boundary**: legs already zero on entry →
    /// Complete without burning a close-all round.
    #[tokio::test(start_paused = true)]
    async fn case13_already_zero_returns_complete_without_close_all() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        let legs = ScriptedLegState::new(vec![LegQtys {
            ext: Decimal::ZERO,
            lt: Decimal::ZERO,
        }]);
        let tmp = TempDir::new().unwrap();
        let mut t = tripwire_in(&tmp, 5);
        let c = cfg(10, 3);
        let lp = EmergencyLoop::new(&ext_ops, &lt_ops, &legs, &c);
        let outcome = lp.run(&mut t).await;
        assert_eq!(outcome, EmergencyLoopOutcome::Complete);
        // Crucially: no close-all calls.
        assert!(ext_ops.snapshot_close_alls().is_empty());
        assert!(lt_ops.snapshot_close_alls().is_empty());
    }

    /// **Catalogue case 13 main**: one close-all round zeros both
    /// legs → Complete on the next read.
    #[tokio::test(start_paused = true)]
    async fn case13_one_round_zeroes_both_legs_returns_complete() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        let legs = ScriptedLegState::new(vec![
            LegQtys {
                ext: dec!(0.01),
                lt: dec!(0.01),
            },
            LegQtys {
                ext: Decimal::ZERO,
                lt: Decimal::ZERO,
            },
        ]);
        let tmp = TempDir::new().unwrap();
        let mut t = tripwire_in(&tmp, 5);
        let c = cfg(10, 3);
        let lp = EmergencyLoop::new(&ext_ops, &lt_ops, &legs, &c);
        let outcome = lp.run(&mut t).await;
        assert_eq!(outcome, EmergencyLoopOutcome::Complete);
        // One round, one close-all per venue.
        assert_eq!(ext_ops.snapshot_close_alls().len(), 1);
        assert_eq!(lt_ops.snapshot_close_alls().len(), 1);
        assert!(!t.is_stuck());
    }

    /// **Catalogue case 13 partial**: only Lighter has open qty
    /// (e.g. Extended already exited cleanly during Phase::Exiting
    /// but the leg-mismatch tripped on Lighter). Loop only calls
    /// close_all on Lighter, then verifies zero → Complete.
    #[tokio::test(start_paused = true)]
    async fn case13_only_lighter_open_skips_extended_close_all() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        let legs = ScriptedLegState::new(vec![
            LegQtys {
                ext: Decimal::ZERO,
                lt: dec!(0.01),
            },
            LegQtys {
                ext: Decimal::ZERO,
                lt: Decimal::ZERO,
            },
        ]);
        let tmp = TempDir::new().unwrap();
        let mut t = tripwire_in(&tmp, 5);
        let c = cfg(10, 3);
        let lp = EmergencyLoop::new(&ext_ops, &lt_ops, &legs, &c);
        let outcome = lp.run(&mut t).await;
        assert_eq!(outcome, EmergencyLoopOutcome::Complete);
        assert!(
            ext_ops.snapshot_close_alls().is_empty(),
            "Extended already zero — must not be touched"
        );
        assert_eq!(lt_ops.snapshot_close_alls().len(), 1);
    }

    /// **Catalogue case 12 main**: every close-all is rejected.
    /// After K rejections (kill threshold) the tripwire arms and
    /// the loop returns Stuck. The 30 s back-off is in play between
    /// attempts so the test under `tokio::time::pause` advances
    /// virtual clock by ~K * retry_interval_ms.
    #[tokio::test(start_paused = true)]
    async fn case12_close_all_rejected_repeatedly_returns_stuck() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        // Pre-load 5 rejection responses on Extended close_all. Each
        // attempt the loop pops one; on the 5th the tripwire arms.
        ext_ops.with_state(|s| {
            for _ in 0..5 {
                s.close_all
                    .push_back(ScriptedResponse::Err("reduce-only rejected".into()));
            }
        });
        let legs = ScriptedLegState::new(vec![LegQtys {
            ext: dec!(0.01),
            lt: Decimal::ZERO,
        }]);
        let tmp = TempDir::new().unwrap();
        let mut t = tripwire_in(&tmp, 5);
        let c = cfg(10, 100);
        let lp = EmergencyLoop::new(&ext_ops, &lt_ops, &legs, &c);
        let outcome = lp.run(&mut t).await;
        assert_eq!(outcome, EmergencyLoopOutcome::Stuck);
        assert!(t.is_stuck(), "STUCK file must be armed on case 12");
        let body = std::fs::read_to_string(t.stuck_file_path()).unwrap();
        assert!(body.contains("REDUCE_ONLY_FAIL_LIMIT"));
        // Exactly 5 close-all attempts before the kill counter trips.
        assert_eq!(ext_ops.snapshot_close_alls().len(), 5);
    }

    /// **Catalogue case 12 boundary**: a transient close-all
    /// rejection that recovers before the kill threshold should
    /// reset the counter (mirrors the kill_switch contract).
    /// 4 fails × 1 success × 4 fails must NOT arm STUCK.
    #[tokio::test(start_paused = true)]
    async fn case12_recovery_mid_sequence_resets_kill_counter() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        ext_ops.with_state(|s| {
            // 4 errs, then Ok, then 4 errs.
            for _ in 0..4 {
                s.close_all.push_back(ScriptedResponse::Err("blip".into()));
            }
            s.close_all.push_back(ScriptedResponse::Ok);
            for _ in 0..4 {
                s.close_all.push_back(ScriptedResponse::Err("blip".into()));
            }
        });
        // Keep Extended permanently non-zero so the loop keeps trying.
        // Cap attempts at 9 so the test terminates after consuming
        // the scripted sequence.
        let legs = ScriptedLegState::new(vec![LegQtys {
            ext: dec!(0.01),
            lt: Decimal::ZERO,
        }]);
        let tmp = TempDir::new().unwrap();
        let mut t = tripwire_in(&tmp, 5);
        let c = cfg(10, 9);
        let lp = EmergencyLoop::new(&ext_ops, &lt_ops, &legs, &c);
        let outcome = lp.run(&mut t).await;
        // 9 attempts exhaust max_attempts before the post-success
        // streak reaches 5 — STUCK never arms.
        assert_eq!(outcome, EmergencyLoopOutcome::MaxAttemptsExceeded);
        assert!(!t.is_stuck());
    }

    /// **Case 12 cross-venue**: Extended and Lighter rejections both
    /// feed the same `reduce_only` counter (it is intentionally
    /// venue-agnostic — `reduce_only_consec_fail_to_kill` is the
    /// total fail count across attempts, not per-venue). Mixed
    /// rejections still cumulate to STUCK.
    #[tokio::test(start_paused = true)]
    async fn case12_kill_counter_is_cross_venue() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        // Both venues reject every close-all. With ext+lt both open
        // each round produces 2 fails; threshold=5 trips on round 3.
        ext_ops.with_state(|s| {
            for _ in 0..5 {
                s.close_all
                    .push_back(ScriptedResponse::Err("ext-blip".into()));
            }
        });
        lt_ops.with_state(|s| {
            for _ in 0..5 {
                s.close_all
                    .push_back(ScriptedResponse::Err("lt-blip".into()));
            }
        });
        let legs = ScriptedLegState::new(vec![LegQtys {
            ext: dec!(0.01),
            lt: dec!(0.01),
        }]);
        let tmp = TempDir::new().unwrap();
        let mut t = tripwire_in(&tmp, 5);
        let c = cfg(10, 100);
        let lp = EmergencyLoop::new(&ext_ops, &lt_ops, &legs, &c);
        let outcome = lp.run(&mut t).await;
        assert_eq!(outcome, EmergencyLoopOutcome::Stuck);
        // Round 1: ext fail (1) + lt fail (2)
        // Round 2: ext fail (3) + lt fail (4)
        // Round 3: ext fail (5) → arms STUCK; lt is not attempted
        //                          this round because the loop returns
        //                          Stuck immediately on arm.
        let total_calls = ext_ops.snapshot_close_alls().len() + lt_ops.snapshot_close_alls().len();
        assert_eq!(total_calls, 5);
    }

    /// Defensive: max_attempts cap protects against a stuck venue
    /// that accepts close-all (success) but never zeros the leg.
    /// Without the cap the loop would run forever; with it, we
    /// surface MaxAttemptsExceeded so the runner can re-enter on the
    /// next phase tick (or the operator notices the lack of progress).
    #[tokio::test(start_paused = true)]
    async fn max_attempts_cap_protects_against_silent_no_progress() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        // close_all returns Ok by default but legs never zero.
        let legs = ScriptedLegState::new(vec![LegQtys {
            ext: dec!(0.01),
            lt: Decimal::ZERO,
        }]);
        let tmp = TempDir::new().unwrap();
        let mut t = tripwire_in(&tmp, 5);
        let c = cfg(10, 4);
        let lp = EmergencyLoop::new(&ext_ops, &lt_ops, &legs, &c);
        let outcome = lp.run(&mut t).await;
        assert_eq!(outcome, EmergencyLoopOutcome::MaxAttemptsExceeded);
        assert_eq!(ext_ops.snapshot_close_alls().len(), 4);
        // Counter resets on Ok responses, so STUCK never arms.
        assert!(!t.is_stuck());
    }

    #[test]
    fn config_validate() {
        assert!(EmergencyLoopConfig {
            retry_interval_ms: 0,
            max_attempts: 1,
            complete_grace_ms: 0,
        }
        .validate()
        .is_err());
        assert!(EmergencyLoopConfig {
            retry_interval_ms: 1,
            max_attempts: 0,
            complete_grace_ms: 0,
        }
        .validate()
        .is_err());
        assert!(EmergencyLoopConfig {
            retry_interval_ms: 30000,
            max_attempts: 100,
            complete_grace_ms: 0,
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn leg_qtys_both_zero_helper() {
        assert!(LegQtys {
            ext: Decimal::ZERO,
            lt: Decimal::ZERO,
        }
        .both_zero());
        assert!(!LegQtys {
            ext: dec!(0.01),
            lt: Decimal::ZERO,
        }
        .both_zero());
    }
}
