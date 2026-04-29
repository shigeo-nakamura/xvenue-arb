//! Live runner — Phase 2 paper-trading scope (bot-strategy#166).
//!
//! What this module DOES:
//! - Tick loop reading current mid from both venues via [`VenueHub`].
//! - Drives [`SpreadEngine`] / [`SignalEngine`] / [`PositionMachine`]
//!   exactly as the BT runner does, so live-vs-BT decisions are
//!   apples-to-apples.
//! - Emits structured log lines on every decision and a periodic status
//!   summary suitable for the dashboard.
//! - Honours `dry_run` — when true, [`Decision::Enter`] / [`Decision::Exit`]
//!   are logged but no orders flow downstream.
//!
//! What this module does NOT do (Phase 3 follow-ups, see
//! `docs/execution_layer.md`):
//! - Order placement, partial-fill aggregation, taker fallback.
//! - Emergency-flatten loop with 30s back-off.
//! - WS staleness monitor, skew monitor, reference guard live wiring.
//! - STUCK file IPC, REST consec-fail counters.
//!
//! The decision *logic* (signal + state transitions) is identical to BT;
//! the execution side stays no-op until Phase 3 binds it to real venues.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use super::config::XvenueConfig;
use super::signal::{Decision, ExitReason, SignalEngine, SpreadDirection};
use super::spread::SpreadEngine;
use super::state::{Event, PositionMachine};
use super::status::{equity_decimal_to_f64, StatusReporter};
use crate::risk::manager::{BlockReason, RiskManager};
use crate::risk::reference_guard::{RefCheckOutcome, ReferenceGuard};

/// Which venue an operation targets. Avoids leaking the underlying
/// DexConnector type into the runner core so the mock in
/// [`tests`] can substitute without re-implementing the full trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Extended,
    Lighter,
}

#[derive(Debug, Clone)]
pub struct MidSnapshot {
    pub ts_ms: u64,
    pub mid: Decimal,
    /// `false` when the top-of-book has zero size on one side. Spread
    /// engine drops these (see bt.rs zero-size filter rationale,
    /// bot-strategy#166 part 1).
    pub book_ok: bool,
}

/// Thin abstraction over the two venues. Production wires this to
/// [`Arc<dyn DexConnector>`] each side; tests substitute a deterministic
/// mock without re-implementing all of DexConnector.
#[async_trait]
pub trait VenueHub: Send + Sync {
    /// Read the current top-of-book mid for the venue, plus a
    /// `book_ok` flag (true iff both sides have positive size).
    async fn read_mid(&self, venue: Venue) -> Result<MidSnapshot>;

    /// Read the venue's equity in USD. The status emitter sums both
    /// venues to drive the dashboard's `pnl_total` / `pnl_today` line
    /// (`pnl_source: "equity"`, matching pairtrade). Best-effort —
    /// `Ok(None)` when the venue does not surface equity (e.g. the
    /// replay BT hub) and lets the caller fall back to zero without
    /// raising. Failures escalate to `Err` so the status loop can
    /// log + skip without panicking.
    async fn read_equity_usd(&self, venue: Venue) -> Result<Option<Decimal>>;
}

/// Per-loop diagnostic counters. Gets log-printed at
/// `status_log_interval_ms` cadence and returned at shutdown.
#[derive(Debug, Default, Clone)]
pub struct LivePaperSummary {
    pub ticks: u64,
    pub samples_committed: u64,
    pub decisions_hold: u64,
    pub decisions_enter_long: u64,
    pub decisions_enter_short: u64,
    pub decisions_exit: u64,
    pub last_dev_bps: Option<f64>,
    pub last_decision_ts_ms: Option<u64>,
    /// Count of `Decision::Enter` outcomes that were suppressed by the
    /// external KILL_SWITCH file (bot-strategy#244 D-1). Visible in
    /// `[STATUS]` log line and the shutdown summary so the operator
    /// can confirm the file actually held entries off.
    pub entries_blocked_by_kill_switch: u64,
    /// Count of entries blocked by the risk gates (#244 D-2..D-7).
    /// Bucketed by gate kind so the operator can tell daily-DD vs
    /// session-DD vs circuit-breaker apart from the [STATUS] line.
    pub entries_blocked_by_daily_dd: u64,
    pub entries_blocked_by_session_dd: u64,
    pub entries_blocked_by_circuit_breaker: u64,
    /// Number of ticks where the reference guard suppressed the
    /// Extended book (`book_ok` flipped to false). Bucketed per
    /// venue so a sustained one-sided stuck quote is visible
    /// without leaving the bot's own logs.
    pub ext_book_suppressed_by_ref_guard: u64,
    pub lt_book_suppressed_by_ref_guard: u64,
}

/// Control surface for the run loop.
pub struct LiveLoopConfig {
    /// Cadence of the read-mid + decide loop. Defaults to
    /// `cfg.spread_bucket_ms` so each tick can produce at most one
    /// committed sample, matching the BT semantics.
    pub tick_interval_ms: u64,
    /// How often to emit a `[STATUS]` log line. Defaults to 60s.
    pub status_log_interval_ms: u64,
}

impl LiveLoopConfig {
    pub fn from_xvenue(cfg: &XvenueConfig) -> Self {
        Self {
            tick_interval_ms: cfg.spread_bucket_ms,
            status_log_interval_ms: 60_000,
        }
    }
}

/// Run the paper-trading loop until `shutdown` resolves. The caller wires
/// `shutdown` to SIGTERM / SIGINT; on resolution we exit cleanly with
/// the latest counters.
pub async fn run_paper_loop<H: VenueHub + ?Sized>(
    cfg: XvenueConfig,
    loop_cfg: LiveLoopConfig,
    hub: Arc<H>,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<LivePaperSummary> {
    let mut spread = SpreadEngine::new(cfg.spread_config());
    let mut signal = SignalEngine::new(cfg.signal_config());
    let mut machine = PositionMachine::new();
    let mut summary = LivePaperSummary::default();
    let mut reporter = StatusReporter::from_env(&cfg);
    let mut risk_manager = RiskManager::new(
        cfg.risk_config(),
        cfg.agent_name.clone(),
    );
    let mut reference_guard = ReferenceGuard::spawn(
        cfg.binance_reference_symbol.clone(),
        cfg.reference_max_dev_bps,
        cfg.reference_consec_buckets_for_halt,
    );
    if let Some(r) = reporter.as_ref() {
        log::info!(
            "[STATUS] writing snapshots to {} (cadence ≥ 60s)",
            r.path().display()
        );
    }

    log::info!(
        "[XVENUE] live paper loop start agent={} ext={} lt={} dry_run={} bucket_ms={} \
         abs_thr={} persist_s={} max_hold_s={}",
        cfg.agent_name,
        cfg.symbol_ext,
        cfg.symbol_lt,
        cfg.dry_run,
        cfg.spread_bucket_ms,
        cfg.abs_threshold_bps,
        cfg.persistence_sec,
        cfg.max_hold_sec,
    );

    let mut tick_ivl = tokio::time::interval(Duration::from_millis(loop_cfg.tick_interval_ms));
    tick_ivl.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut status_ivl =
        tokio::time::interval(Duration::from_millis(loop_cfg.status_log_interval_ms));
    status_ivl.tick().await; // discard the immediate-fire tick
    let mut open_qty: Option<Decimal> = None;

    // Drop an initial snapshot so the dashboard sees the DRY_RUN pill /
    // agent identity on boot instead of waiting for the first
    // status_log_interval_ms (60 s default). Equity is best-effort —
    // the first read may be Err while the WS is still warming, in
    // which case PnL stays at zero until the next tick.
    if let Some(r) = reporter.as_mut() {
        refresh_equity(&*hub, r, &mut risk_manager).await;
        r.mark_dirty();
        publish_risk(&risk_manager, r);
        if let Err(e) = r.write_snapshot_if_due(&machine, wall_clock_ms()) {
            log::warn!("[STATUS] initial snapshot write failed: {:?}", e);
        }
    }

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown => {
                log::info!("[XVENUE] shutdown signal received");
                break;
            }

            _ = tick_ivl.tick() => {
                summary.ticks += 1;
                // Per-tick risk housekeeping: rolls UTC session,
                // consumes a RISK_ACK, ages out cooldowns. Side-
                // effect-free idempotent — safe even when the inner
                // tick errors out.
                risk_manager.tick(now_unix_secs());
                if let Err(e) = run_one_tick(
                    &cfg,
                    &*hub,
                    &mut spread,
                    &mut signal,
                    &mut machine,
                    &mut summary,
                    &mut open_qty,
                    reporter.as_mut(),
                    &mut risk_manager,
                    &mut reference_guard,
                ).await {
                    // Read-mid / decision errors are logged but don't
                    // terminate the loop. Phase 3 will add a consec-fail
                    // counter that escalates to STUCK file.
                    log::warn!("[XVENUE] tick error: {:?}", e);
                }
            }

            _ = status_ivl.tick() => {
                log::info!(
                    "[STATUS] ticks={} samples={} hold={} enter_l={} enter_s={} exit={} \
                     ks_blocked={} dd_blocked={} sd_blocked={} cb_blocked={} \
                     ref_supp_ext={} ref_supp_lt={} dev_bps={:?}",
                    summary.ticks,
                    summary.samples_committed,
                    summary.decisions_hold,
                    summary.decisions_enter_long,
                    summary.decisions_enter_short,
                    summary.decisions_exit,
                    summary.entries_blocked_by_kill_switch,
                    summary.entries_blocked_by_daily_dd,
                    summary.entries_blocked_by_session_dd,
                    summary.entries_blocked_by_circuit_breaker,
                    summary.ext_book_suppressed_by_ref_guard,
                    summary.lt_book_suppressed_by_ref_guard,
                    summary.last_dev_bps,
                );
                if let Some(r) = reporter.as_mut() {
                    refresh_equity(&*hub, r, &mut risk_manager).await;
                    publish_risk(&risk_manager, r);
                    let now_ts_ms = wall_clock_ms();
                    if let Err(e) = r.write_snapshot_if_due(&machine, now_ts_ms) {
                        log::warn!("[STATUS] snapshot write failed: {:?}", e);
                    }
                }
            }
        }
    }

    risk_manager.flush();
    Ok(summary)
}

fn now_unix_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

fn wall_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `true` when the external KILL_SWITCH file exists. File-presence is
/// the source of truth — no caching, no edge-trigger persistence —
/// so removing the file resumes entries on the next tick (#244 D-1).
fn kill_switch_active(path: &str) -> bool {
    !path.is_empty() && std::path::Path::new(path).exists()
}

/// Pulls equity from both venues and threads the sum into the reporter
/// so the dashboard's `pnl_total` / `pnl_today` line tracks the live
/// account. Also hands the equity sample to the risk manager so the
/// session-DD rolling peak (#244 D-4) tracks the same number the
/// dashboard renders. Best-effort: per-venue failures are logged and
/// treated as zero so a hung venue doesn't stall the snapshot.
async fn refresh_equity<H: VenueHub + ?Sized>(
    hub: &H,
    reporter: &mut StatusReporter,
    risk_manager: &mut RiskManager,
) {
    let mut total = Decimal::ZERO;
    let mut any_ok = false;
    for v in [Venue::Extended, Venue::Lighter] {
        match hub.read_equity_usd(v).await {
            Ok(Some(eq)) => {
                total += eq;
                any_ok = true;
            }
            Ok(None) => {}
            Err(e) => {
                log::warn!("[STATUS] read_equity_usd({:?}) failed: {:?}", v, e);
            }
        }
    }
    if any_ok {
        let eq_f64 = equity_decimal_to_f64(total);
        reporter.update_equity(eq_f64);
        risk_manager.record_equity_sample(eq_f64, now_unix_secs());
    }
}

fn publish_risk(risk_manager: &RiskManager, reporter: &mut StatusReporter) {
    reporter.set_daily_risk(risk_manager.daily_snapshot());
    reporter.set_session_risk(risk_manager.session_snapshot());
    reporter.set_circuit_breaker(Some(
        risk_manager.circuit_breaker_snapshot(now_unix_secs()),
    ));
    reporter.set_risk_history(risk_manager.risk_history());
}

async fn run_one_tick<H: VenueHub + ?Sized>(
    cfg: &XvenueConfig,
    hub: &H,
    spread: &mut SpreadEngine,
    signal: &mut SignalEngine,
    machine: &mut PositionMachine,
    summary: &mut LivePaperSummary,
    open_qty: &mut Option<Decimal>,
    mut reporter: Option<&mut StatusReporter>,
    risk_manager: &mut RiskManager,
    reference_guard: &mut ReferenceGuard,
) -> Result<()> {
    let mut ext_snap = hub.read_mid(Venue::Extended).await.context("read_mid Extended")?;
    let mut lt_snap = hub.read_mid(Venue::Lighter).await.context("read_mid Lighter")?;

    // Reference guard cross-check (#244 C). Reads the latest Binance
    // 1m mid and suppresses each venue's book_ok when its mid drifts
    // past `reference_max_dev_bps` for `reference_consec_buckets_for_halt`
    // consecutive minutes. Mirrors the BT pre-filter, so live and BT
    // see the same suppression behavior on stuck quotes.
    let ref_state = reference_guard.current_reference().await;
    if let (Some(ext_mid_f), Some(lt_mid_f)) = (
        rust_decimal::prelude::ToPrimitive::to_f64(&ext_snap.mid),
        rust_decimal::prelude::ToPrimitive::to_f64(&lt_snap.mid),
    ) {
        let now_ts_secs = now_unix_secs();
        let (ext_oc, lt_oc) = reference_guard.evaluate(
            ext_snap.ts_ms.min(lt_snap.ts_ms),
            ext_mid_f,
            lt_mid_f,
            ref_state.as_ref(),
            now_ts_secs,
        );
        if ext_oc == RefCheckOutcome::Suppress && ext_snap.book_ok {
            log::warn!(
                "[XVENUE] reference_guard suppressing Extended book: \
                 venue_mid={:.4} ref_mid={:?}",
                ext_mid_f,
                ref_state.as_ref().map(|r| r.mid)
            );
            ext_snap.book_ok = false;
            summary.ext_book_suppressed_by_ref_guard += 1;
        }
        if lt_oc == RefCheckOutcome::Suppress && lt_snap.book_ok {
            log::warn!(
                "[XVENUE] reference_guard suppressing Lighter book: \
                 venue_mid={:.4} ref_mid={:?}",
                lt_mid_f,
                ref_state.as_ref().map(|r| r.mid)
            );
            lt_snap.book_ok = false;
            summary.lt_book_suppressed_by_ref_guard += 1;
        }
    }

    if let Some(r) = reporter.as_deref_mut() {
        r.record_book_ok(
            if ext_snap.book_ok { Some(ext_snap.ts_ms) } else { None },
            if lt_snap.book_ok { Some(lt_snap.ts_ms) } else { None },
        );
    }

    let prev_committed = spread.samples_committed();
    if ext_snap.book_ok {
        spread.update_extended(ext_snap.ts_ms, ext_snap.mid);
    }
    if lt_snap.book_ok {
        spread.update_lighter(lt_snap.ts_ms, lt_snap.mid);
    }

    // Use the slower-of-the-two timestamp as the decision clock, same
    // policy as the BT runner.
    let now_ts_ms = ext_snap.ts_ms.min(lt_snap.ts_ms);
    let committed = spread.samples_committed() > prev_committed;
    summary.samples_committed = spread.samples_committed();

    let position = machine.summary();
    let evaluate = committed || position.is_some();
    if !evaluate {
        return Ok(());
    }

    let dev = spread.current_dev_bps();
    summary.last_dev_bps = dev;
    if let (Some(r), Some(d)) = (reporter.as_deref_mut(), dev) {
        if committed {
            r.push_spread_point(now_ts_ms, d);
        }
        r.record_samples_committed(spread.samples_committed());
    }
    let is_warm = spread.is_warm(cfg.min_warmup_samples);

    let mut decision = signal.decide(now_ts_ms, dev, is_warm, position);

    // External KILL_SWITCH gate (bot-strategy#244 D-1). Pairtrade-
    // symmetric: when /opt/debot/KILL_SWITCH exists, refuse new
    // entries; held positions still exit normally. We gate at the
    // live-loop level rather than the SignalEngine so the strategy
    // logic stays free of file-IO concerns.
    if matches!(decision, Decision::Enter(_)) && kill_switch_active(&cfg.kill_switch_file) {
        log::warn!(
            "[XVENUE] KILL_SWITCH active ({}); blocking new entry. \
             Existing positions exit normally; remove the file to resume.",
            cfg.kill_switch_file
        );
        summary.entries_blocked_by_kill_switch += 1;
        decision = Decision::Hold;
    }

    // Risk gates (#244 D-2..D-7). KILL_SWITCH ran first so the
    // operator-pause path doesn't burn a risk-gate counter; risk
    // gates fire only if the bot is otherwise willing to enter.
    if matches!(decision, Decision::Enter(_)) {
        if let Some(reason) = risk_manager.block_reason(now_unix_secs()) {
            log::warn!(
                "[XVENUE] risk gate {:?} blocking new entry. dev_bps={:?}",
                reason,
                dev
            );
            match reason {
                BlockReason::DailyDdHalted => {
                    summary.entries_blocked_by_daily_dd += 1;
                }
                BlockReason::SessionDdHalted => {
                    summary.entries_blocked_by_session_dd += 1;
                }
                BlockReason::CircuitBreakerCooldown => {
                    summary.entries_blocked_by_circuit_breaker += 1;
                }
            }
            decision = Decision::Hold;
        }
    }

    match decision {
        Decision::Hold => {
            summary.decisions_hold += 1;
        }
        Decision::Enter(dir) => {
            // Phase 2 paper: log the decision, walk the state machine
            // through synthetic Filled events so the engine stays
            // exercised end-to-end. Phase 3 replaces this with real
            // order placement.
            let qty = paper_qty(cfg.min_notional_usd, ext_snap.mid)?;
            let notional = Decimal::from_f64_retain(cfg.min_notional_usd)
                .unwrap_or(Decimal::ZERO);
            machine.apply(
                now_ts_ms,
                Event::EntrySignal {
                    direction: dir,
                    notional_usd: notional,
                },
            )?;
            machine.apply(now_ts_ms, Event::ExtendedFilled { qty })?;
            machine.apply(now_ts_ms, Event::LighterFilled { qty })?;
            *open_qty = Some(qty);
            if let Some(r) = reporter.as_deref_mut() {
                r.record_fill(true, true, now_ts_ms);
            }
            summary.last_decision_ts_ms = Some(now_ts_ms);
            match dir {
                SpreadDirection::Long => summary.decisions_enter_long += 1,
                SpreadDirection::Short => summary.decisions_enter_short += 1,
            }
            log::info!(
                "[XVENUE] {} ENTER dir={:?} dev_bps={:?} ext_mid={} lt_mid={} qty={} dry_run={}",
                if cfg.dry_run { "PAPER" } else { "LIVE" },
                dir,
                dev,
                ext_snap.mid,
                lt_snap.mid,
                qty,
                cfg.dry_run,
            );
        }
        Decision::Exit(reason) => {
            let qty = open_qty.take().unwrap_or(Decimal::ZERO);
            machine.apply(now_ts_ms, Event::ExitSignal { reason })?;
            if qty > Decimal::ZERO {
                machine.apply(now_ts_ms, Event::ExtendedExitFilled { qty })?;
                machine.apply(now_ts_ms, Event::LighterExitFilled { qty })?;
                if let Some(r) = reporter.as_deref_mut() {
                    r.record_fill(true, true, now_ts_ms);
                }
                // Paper PnL is 0 in DRY_RUN — Group B will replace
                // this with realized USD once orders flow. The
                // record_close call exercises the risk path so the
                // counters / persistence stay live during paper.
                risk_manager.record_close(0.0, now_unix_secs());
            }
            summary.last_decision_ts_ms = Some(now_ts_ms);
            summary.decisions_exit += 1;
            log::info!(
                "[XVENUE] {} EXIT reason={:?} dev_bps={:?} ext_mid={} lt_mid={} dry_run={}",
                if cfg.dry_run { "PAPER" } else { "LIVE" },
                reason,
                dev,
                ext_snap.mid,
                lt_snap.mid,
                cfg.dry_run,
            );
            // ExitReason isn't used for the open_qty reset — that
            // happens above via .take(). Keeping the reason in the log
            // for downstream analysis (and to make ExitReason live).
            let _ = ExitReason::MeanCross;
        }
    }
    Ok(())
}

fn paper_qty(notional_usd: f64, mid: Decimal) -> Result<Decimal> {
    if mid <= Decimal::ZERO {
        anyhow::bail!("non-positive mid");
    }
    let n = Decimal::from_f64_retain(notional_usd)
        .ok_or_else(|| anyhow::anyhow!("notional_usd not representable"))?;
    Ok((n / mid).round_dp(8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::manager::{RiskConfig, RiskManager};
    use crate::xvenue::test_helpers::{mid, stale_mid, ScriptedHub};
    use tokio::time::timeout;

    /// Test fixture: build a paused (manual) ReferenceGuard so unit
    /// tests don't spawn a real polling task / hit the network. The
    /// guard returns NoReference until a test injects a mid.
    fn test_reference_guard() -> ReferenceGuard {
        ReferenceGuard::manual(100.0, 3)
    }

    /// Builds a RiskManager pointing at a fresh temp dir so each test
    /// owns its own risk_state.json / RISK_ACK paths and parallel
    /// execution doesn't fight over /var/lib.
    fn test_risk_manager() -> (tempfile::TempDir, RiskManager) {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = RiskConfig {
            max_daily_loss_bps: 300,
            daily_reset_utc_hour: 0,
            max_session_loss_bps: 500,
            session_dd_lookback_secs: 86_400,
            session_dd_sample_secs: 60,
            cb_tier1_threshold: 5,
            cb_tier2_threshold: 8,
            cb_tier1_cooldown_secs: 1_800,
            cb_tier2_cooldown_secs: 21_600,
            risk_state_path: dir.path().join("risk_state.json"),
            risk_ack_path: dir.path().join("RISK_ACK"),
        };
        (dir, RiskManager::new(cfg, "test".to_string()))
    }

    fn min_cfg() -> XvenueConfig {
        let yaml = r#"
agent_name: test
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 100
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
spread_bucket_ms: 1000
rolling_window_sec: 30
min_warmup_samples: 3
abs_threshold_bps: 5.0
persistence_sec: 1
max_hold_sec: 60
"#;
        let c: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        c
    }

    #[tokio::test]
    async fn loop_exits_on_shutdown() {
        let hub = Arc::new(ScriptedHub::new(vec![mid(1000, 2000.0)], vec![mid(1000, 2000.0)]));
        let cfg = min_cfg();
        let loop_cfg = LiveLoopConfig {
            tick_interval_ms: 5,
            status_log_interval_ms: 10_000,
        };
        let (tx, rx) = oneshot::channel();
        // Send shutdown immediately
        let _ = tx.send(());
        let summary = timeout(Duration::from_secs(1), run_paper_loop(cfg, loop_cfg, hub, rx))
            .await
            .expect("loop did not terminate")
            .unwrap();
        // Even with immediate shutdown, the biased select! picks the
        // shutdown branch first → no ticks executed.
        assert_eq!(summary.ticks, 0);
    }

    #[tokio::test]
    async fn one_tick_runs_and_increments_counters() {
        let hub = Arc::new(ScriptedHub::new(
            vec![mid(1000, 2000.0), mid(2000, 2001.0)],
            vec![mid(1000, 2000.0), mid(2000, 2000.5)],
        ));
        let cfg = min_cfg();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;
        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        run_one_tick(
            &cfg,
            &*hub,
            &mut spread,
            &mut signal,
            &mut machine,
            &mut summary,
            &mut open_qty,
            None,
            &mut rm,
            &mut rg,
        )
        .await
        .unwrap();
        assert_eq!(summary.samples_committed, 1);
    }

    #[tokio::test]
    async fn book_not_ok_suppresses_commit() {
        let hub = Arc::new(ScriptedHub::new(
            vec![stale_mid(1000, 2000.0)],
            vec![mid(1000, 2000.0)],
        ));
        let cfg = min_cfg();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;
        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        run_one_tick(
            &cfg,
            &*hub,
            &mut spread,
            &mut signal,
            &mut machine,
            &mut summary,
            &mut open_qty,
            None,
            &mut rm,
            &mut rg,
        )
        .await
        .unwrap();
        // Lighter committed (book_ok=true) but Extended didn't, so no
        // aligned pair → samples_committed stays 0.
        assert_eq!(summary.samples_committed, 0);
    }

    #[tokio::test]
    async fn dev_breach_fires_decision_enter() {
        // Warm up with 30 buckets at zero spread to fill the 30s
        // rolling window with zeros, then several breached buckets at
        // +30 bps. After two breached buckets the rolling mean is only
        // ~2 bps (28 zeros + 2 thirties / 30) so dev ≈ 28 bps, well
        // over abs_threshold=5. Persistence=1s → Enter fires once a
        // breached state has lasted past one bucket.
        let lt_mid = 2000.0;
        let warm_ext = lt_mid;
        let breach_ext = lt_mid * (1.0 + 30.0 / 10_000.0); // +30 bps
        let mut ext = Vec::new();
        let mut lt = Vec::new();
        let warm_n = 30u64;
        let breach_n = 8u64;
        // Offset timestamps past the funding-lockout post window
        // (default 120s). Keep all ticks inside [120s, 2940s) so neither
        // pre nor post lockout fires.
        let t0 = 200_000u64;
        for i in 0..warm_n {
            let ts = t0 + i * 1000;
            ext.push(mid(ts, warm_ext));
            lt.push(mid(ts, lt_mid));
        }
        for i in warm_n..(warm_n + breach_n) {
            let ts = t0 + i * 1000;
            ext.push(mid(ts, breach_ext));
            lt.push(mid(ts, lt_mid));
        }
        let hub = Arc::new(ScriptedHub::new(ext, lt));
        let cfg = min_cfg();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;

        // Drive ticks through the entire scripted sequence.
        let total_ticks = warm_n + breach_n;
        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        for _ in 0..total_ticks {
            run_one_tick(
                &cfg,
                &*hub,
                &mut spread,
                &mut signal,
                &mut machine,
                &mut summary,
                &mut open_qty,
                None,
                &mut rm,
                &mut rg,
            )
            .await
            .unwrap();
        }

        // At least one short entry should have fired (ext > lt in bps
        // means the *Extended* leg is rich → SHORT extended + LONG lt).
        assert!(
            summary.decisions_enter_short >= 1,
            "expected enter_short ≥ 1, summary={:?}",
            summary
        );
        // open_qty should be set — the position is still held since
        // prices haven't reverted.
        assert!(open_qty.is_some());
    }

    #[tokio::test]
    async fn kill_switch_file_blocks_entries_and_clears_on_removal() {
        // Same scripted stream as `dev_breach_fires_decision_enter` —
        // a clean entry signal we expect to land. While the kill
        // switch file is present, the entry must be suppressed and
        // the counter must increment. After removal, re-driving the
        // breach should let the entry through.
        let lt_mid = 2000.0;
        let breach_ext = lt_mid * (1.0 + 30.0 / 10_000.0);
        let mut ext = Vec::new();
        let mut lt = Vec::new();
        let warm_n = 30u64;
        let breach_n = 8u64;
        let t0 = 200_000u64;
        for i in 0..warm_n {
            ext.push(mid(t0 + i * 1000, lt_mid));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        for i in warm_n..(warm_n + breach_n) {
            ext.push(mid(t0 + i * 1000, breach_ext));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        let hub = Arc::new(ScriptedHub::new(ext, lt));

        let tmp = tempfile::TempDir::new().unwrap();
        let ks_path = tmp.path().join("KILL_SWITCH");
        std::fs::write(&ks_path, b"").unwrap();

        // Inject the temp path into the config.
        let mut cfg = min_cfg();
        cfg.kill_switch_file = ks_path.to_string_lossy().into_owned();

        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;

        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        for _ in 0..(warm_n + breach_n) {
            run_one_tick(
                &cfg,
                &*hub,
                &mut spread,
                &mut signal,
                &mut machine,
                &mut summary,
                &mut open_qty,
                None,
                &mut rm,
                &mut rg,
            )
            .await
            .unwrap();
        }

        // No entry should have landed; counter increments per blocked
        // signal so it should be ≥ 1.
        assert_eq!(summary.decisions_enter_long, 0);
        assert_eq!(summary.decisions_enter_short, 0);
        assert!(open_qty.is_none());
        assert!(
            summary.entries_blocked_by_kill_switch >= 1,
            "expected ks_blocked ≥ 1, got {}",
            summary.entries_blocked_by_kill_switch
        );
    }

    #[tokio::test]
    async fn entry_fires_once_kill_switch_path_is_empty_or_missing() {
        // Symmetric check: pointing kill_switch_file at a path that
        // doesn't exist (or the empty string) must not block entries.
        let lt_mid = 2000.0;
        let breach_ext = lt_mid * (1.0 + 30.0 / 10_000.0);
        let mut ext = Vec::new();
        let mut lt = Vec::new();
        let warm_n = 30u64;
        let breach_n = 8u64;
        let t0 = 200_000u64;
        for i in 0..warm_n {
            ext.push(mid(t0 + i * 1000, lt_mid));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        for i in warm_n..(warm_n + breach_n) {
            ext.push(mid(t0 + i * 1000, breach_ext));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        let hub = Arc::new(ScriptedHub::new(ext, lt));

        let mut cfg = min_cfg();
        // Path that does not exist — the gate should treat this as
        // "no kill switch" and let entries through.
        cfg.kill_switch_file = "/tmp/xvenue-arb-test-nonexistent-ks".to_string();

        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;

        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        for _ in 0..(warm_n + breach_n) {
            run_one_tick(
                &cfg,
                &*hub,
                &mut spread,
                &mut signal,
                &mut machine,
                &mut summary,
                &mut open_qty,
                None,
                &mut rm,
                &mut rg,
            )
            .await
            .unwrap();
        }
        assert_eq!(summary.entries_blocked_by_kill_switch, 0);
        assert!(summary.decisions_enter_short >= 1);
    }

    #[tokio::test]
    async fn risk_session_dd_blocks_entry_in_live_loop() {
        // Same breach scenario as `dev_breach_fires_decision_enter`,
        // but pre-arm a session-DD halt by feeding the manager two
        // equity samples that produce a 6% drop. The live loop must
        // see the halt and convert Enter → Hold, incrementing the
        // session-dd counter.
        let lt_mid = 2000.0;
        let breach_ext = lt_mid * (1.0 + 30.0 / 10_000.0);
        let mut ext = Vec::new();
        let mut lt = Vec::new();
        let warm_n = 30u64;
        let breach_n = 8u64;
        let t0 = 200_000u64;
        for i in 0..warm_n {
            ext.push(mid(t0 + i * 1000, lt_mid));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        for i in warm_n..(warm_n + breach_n) {
            ext.push(mid(t0 + i * 1000, breach_ext));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        let hub = Arc::new(ScriptedHub::new(ext, lt));
        let cfg = min_cfg();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;

        // Pre-arm the manager: equity drops 6% > 500 bps threshold.
        let (_rm_dir, mut rm) = test_risk_manager();
        rm.record_equity_sample(1_000.0, 0);
        rm.record_equity_sample(940.0, 60);
        // Sanity — the manager itself should report the halt.
        assert_eq!(
            rm.block_reason(60),
            Some(BlockReason::SessionDdHalted),
            "test pre-arm: session-DD halt did not activate"
        );

        let mut rg = test_reference_guard();
        for _ in 0..(warm_n + breach_n) {
            run_one_tick(
                &cfg,
                &*hub,
                &mut spread,
                &mut signal,
                &mut machine,
                &mut summary,
                &mut open_qty,
                None,
                &mut rm,
                &mut rg,
            )
            .await
            .unwrap();
        }

        assert_eq!(summary.decisions_enter_long, 0);
        assert_eq!(summary.decisions_enter_short, 0);
        assert!(open_qty.is_none());
        assert!(
            summary.entries_blocked_by_session_dd >= 1,
            "expected sd_blocked ≥ 1, got {}",
            summary.entries_blocked_by_session_dd
        );
        assert_eq!(summary.entries_blocked_by_kill_switch, 0);
        assert_eq!(summary.entries_blocked_by_daily_dd, 0);
        assert_eq!(summary.entries_blocked_by_circuit_breaker, 0);
    }

    #[tokio::test]
    async fn tick_loop_advances_under_short_interval() {
        // Build a long-enough scripted stream so the loop has data to read.
        let mut ext = Vec::new();
        let mut lt = Vec::new();
        for i in 0..10 {
            ext.push(mid(i * 1000, 2000.0 + (i as f64) * 0.1));
            lt.push(mid(i * 1000, 2000.0));
        }
        let hub = Arc::new(ScriptedHub::new(ext, lt));
        let cfg = min_cfg();
        let loop_cfg = LiveLoopConfig {
            tick_interval_ms: 5,
            status_log_interval_ms: 10_000,
        };
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(run_paper_loop(cfg, loop_cfg, hub, rx));
        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = tx.send(());
        let summary = handle.await.unwrap().unwrap();
        // At 5ms interval we should have run several ticks before shutdown.
        assert!(
            summary.ticks >= 3,
            "expected at least 3 ticks, got {}",
            summary.ticks
        );
    }
}
