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
use dex_connector::OrderSide as DcOrderSide;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use super::config::XvenueConfig;
use super::live_exec::LiveExecution;
use super::signal::{Decision, ExitReason, SignalEngine, SpreadDirection};
use super::sizing::{compute_notional_usd, notional_to_qty, SizeOutcome};
use super::spread::SpreadEngine;
use super::state::{EmergencyReason, Event, PositionMachine};
use super::status::{equity_decimal_to_f64, StatusReporter};
use crate::risk::kill_switch::{StuckTripwire, VenueLabel};
use crate::risk::manager::{BlockReason, RiskManager};
use crate::risk::reference_guard::{RefCheckOutcome, ReferenceGuard};
use crate::risk::skew_monitor::{SkewMonitor, SkewOutcome};
use crate::risk::ws_health::{WsHealthMonitor, WsHealthOutcome};
use crate::trade::execution::extended_maker::{ExtendedEntryRequest, ExtendedMakerLoop};
use crate::trade::execution::lighter_fill::{LighterFillLoop, LighterFillRequest};
use crate::trade::execution::parallel_exit::{ParallelExitLoop, ParallelExitOutcome};
use crate::trade::execution::types::{ExtendedTerminal, LighterTerminal};

/// Which venue an operation targets. Avoids leaking the underlying
/// DexConnector type into the runner core so the mock in
/// [`tests`] can substitute without re-implementing the full trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Extended,
    Lighter,
}

/// Per-venue WS warm-up tracker (bot-strategy#248). While a venue has
/// never delivered a successful `read_mid`, errors are demoted to
/// `debug!` and the tick is skipped silently — the WS subscription is
/// still bootstrapping and `[WARN] tick error: read_mid` is just noise.
/// Once the first successful read lands, the flag flips sticky and
/// subsequent errors propagate normally so a real WS outage still
/// surfaces. Reconnect transients can produce one stray WARN per
/// reconnect; the dedicated `ws_health.rs` monitor (#244 Group C) will
/// take over once it lands and these can be demoted in full.
#[derive(Debug, Default)]
pub struct VenueWarmup {
    pub ext_ready: bool,
    pub lt_ready: bool,
}

impl VenueWarmup {
    fn is_ready(&self, venue: Venue) -> bool {
        match venue {
            Venue::Extended => self.ext_ready,
            Venue::Lighter => self.lt_ready,
        }
    }

    fn mark_ready(&mut self, venue: Venue) {
        match venue {
            Venue::Extended => self.ext_ready = true,
            Venue::Lighter => self.lt_ready = true,
        }
    }
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
    /// Number of new entries suppressed because the STUCK file is
    /// armed (REST consec-fail / reduce-only consec-fail / SIGUSR1).
    pub entries_blocked_by_stuck_file: u64,
    /// Number of ticks where the WS staleness watchdog flipped to
    /// `Stale` (#244 Group C / `risk::ws_health`). Counts the trip,
    /// not every tick the latch stays armed — the runner emits one
    /// `Emergency{WsStale}` per trip and the state machine's
    /// `Phase::EmergencyFlattening` is sticky from there.
    pub ws_stale_emergencies_emitted: u64,
    /// Number of new entries blocked because the bot is in
    /// `Phase::Flat` but the WS staleness latch is still armed (we
    /// haven't seen a healthy book within `ws_stale_emergency_ms`).
    /// Distinct from `ws_stale_emergencies_emitted` which only fires
    /// for non-Flat phases.
    pub entries_blocked_by_ws_stale: u64,
    /// Number of ticks where the inventory-skew watchdog escalated
    /// to `Emergency{SkewBreach}` (#244 Group C / `risk::skew_monitor`).
    /// Counts trips, not ticks the latch stays armed.
    pub skew_emergencies_emitted: u64,
    /// Live-mode `Decision::Enter` outcomes that were skipped because
    /// equity-driven sizing fell below `min_notional_usd`. Distinct
    /// from the risk-gate counters: the strategy *would* enter, but
    /// the position would be too small to bother with.
    pub live_entries_skipped_size_below_min: u64,
    /// Live-mode `Decision::Enter` outcomes that were skipped because
    /// one or both venue equity reads returned `None` / non-positive.
    /// Common during connector warm-up.
    pub live_entries_skipped_equity_unavailable: u64,
    /// Live-mode `Decision::Enter` cycles where the Extended leg's
    /// executor returned `Failed` (post-only exhausted, taker
    /// rejected, etc.) before any fill landed. State machine
    /// transitions back to `Flat`.
    pub live_entries_extended_failed: u64,
    /// Live-mode `Decision::Enter` cycles where Extended filled but
    /// Lighter's executor returned `Failed`. The state machine
    /// transitions to `EmergencyFlattening` to clean up the open
    /// Extended leg (Sprint 4 emergency_loop wiring will drive the
    /// reduce-only flatten).
    pub live_entries_lighter_failed_after_extended: u64,
    /// Live-mode `Decision::Exit` cycles that completed with both
    /// terminals present and at least one `Failed` (e.g. taker
    /// rejected, market timeout). Routes via `Event::Emergency` →
    /// `EmergencyFlattening`.
    pub live_exits_failed_legs: u64,
    /// Live-mode `Decision::Exit` cycles where the parallel exit
    /// returned `LegMismatchTimeout` — one leg terminated within
    /// its own timeout, the other was still in flight when the
    /// `leg_mismatch_timeout_ms` deadline fired. Catalogue case 11.
    pub live_exits_leg_mismatch: u64,
    /// Number of `Event::EmergencyComplete` transitions fired by
    /// the runner's emergency-flatten handler (#244 Sprint 4 step
    /// 3/3) — both legs reported zero open qty, position is closed.
    pub emergency_completes: u64,
    /// `close_all` calls inside the emergency handler that returned
    /// `Err`. Fed into `StuckTripwire::record_reduce_only_failure`;
    /// once the kill threshold is crossed the tripwire arms the
    /// STUCK file and the handler stops attempting.
    pub emergency_close_all_failures: u64,
    /// Times the emergency handler observed `StuckTripwire::is_stuck()`
    /// flip true mid-flatten (kill threshold crossed). Operator
    /// must inspect + clear the STUCK file + drop a `RISK_ACK`.
    pub emergency_stuck_armed: u64,
    /// Defensive cap (`emergency_max_attempts`) reached. Should be
    /// rare — usually the loop completes well before this.
    pub emergency_max_attempts_exceeded: u64,
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
///
/// `live_exec`:
/// - `None` (or `cfg.dry_run = true`): runner stays on the synthetic
///   paper-fill path used by Phase 2. Existing tests / BT replay path
///   pass `None` so behaviour is unchanged.
/// - `Some(_)` with `dry_run = false`: `Decision::Enter` /
///   `Decision::Exit` dispatch real orders via the executors held in
///   [`LiveExecution`]. Sprint 4 wiring lands the Enter path first;
///   Exit and EmergencyFlattening will follow in subsequent commits.
pub async fn run_paper_loop<H: VenueHub + ?Sized>(
    cfg: XvenueConfig,
    loop_cfg: LiveLoopConfig,
    hub: Arc<H>,
    live_exec: Option<Arc<LiveExecution>>,
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
    let mut reference_guard = if cfg.binance_reference_symbol.trim().is_empty() {
        ReferenceGuard::disabled(
            cfg.reference_max_dev_bps,
            cfg.reference_consec_buckets_for_halt,
        )
    } else {
        ReferenceGuard::spawn(
            cfg.binance_reference_symbol.clone(),
            cfg.reference_max_dev_bps,
            cfg.reference_consec_buckets_for_halt,
        )
    };
    let mut ws_health = WsHealthMonitor::new(cfg.ws_stale_emergency_ms);
    let mut skew_monitor = SkewMonitor::new(cfg.max_inventory_skew_usd);
    let mut stuck = StuckTripwire::new(cfg.stuck_tripwire_config());
    if stuck.is_stuck() {
        log::warn!(
            "[KILL_SWITCH] STUCK file present at boot ({}) — entries blocked. \
             Operator must inspect and `rm` the file to resume.",
            stuck.stuck_file_path().display()
        );
    }
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
    let mut warmup = VenueWarmup::default();
    // Emergency-flatten throttle state (#244 Sprint 4 step 3/3).
    // Reset whenever phase != EmergencyFlattening so each entry into
    // the flatten loop starts with a fresh attempt budget. The
    // tripwire's reduce-only-fail counter is *not* reset here — it
    // tracks the cross-phase reduce-only failure history and is
    // cleared by `record_reduce_only_success` on each successful
    // close_all (see emergency_loop docs §5).
    let mut last_emergency_attempt_ms: Option<u64> = None;
    let mut emergency_attempts: u32 = 0;

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
                    &mut stuck,
                    &mut warmup,
                    &mut ws_health,
                    &mut skew_monitor,
                    live_exec.as_deref(),
                ).await {
                    // Read-mid / decision errors are logged but don't
                    // terminate the loop. Phase 3 will add a consec-fail
                    // counter that escalates to STUCK file.
                    log::warn!("[XVENUE] tick error: {:?}", e);
                }

                // Emergency-flatten round (#244 Sprint 4 step 3/3).
                // Live mode only — paper mode synthesises
                // EmergencyComplete inline in the WS-stale / skew
                // handlers. Throttled inside the helper, so calling
                // every tick is fine.
                if let Some(live) = live_exec.as_deref() {
                    if !cfg.dry_run
                        && matches!(machine.phase(), super::state::Phase::EmergencyFlattening)
                    {
                        if let Err(e) = drive_emergency_flatten_round(
                            live,
                            &mut machine,
                            &mut open_qty,
                            &mut stuck,
                            &mut summary,
                            &mut last_emergency_attempt_ms,
                            &mut emergency_attempts,
                            wall_clock_ms(),
                        )
                        .await
                        {
                            log::warn!("[XVENUE] emergency-flatten round error: {:?}", e);
                        }
                    } else {
                        // Reset throttle state so the *next* entry
                        // into EmergencyFlattening starts fresh.
                        last_emergency_attempt_ms = None;
                        emergency_attempts = 0;
                    }
                }
            }

            _ = status_ivl.tick() => {
                let ws_age = ws_health.ws_age(wall_clock_ms());
                log::info!(
                    "[STATUS] ticks={} samples={} hold={} enter_l={} enter_s={} exit={} \
                     ks_blocked={} stuck_blocked={} dd_blocked={} sd_blocked={} cb_blocked={} \
                     ws_blocked={} ws_emerg={} skew_emerg={} ws_age_ext={:?} ws_age_lt={:?} \
                     ref_supp_ext={} ref_supp_lt={} dev_bps={:?}",
                    summary.ticks,
                    summary.samples_committed,
                    summary.decisions_hold,
                    summary.decisions_enter_long,
                    summary.decisions_enter_short,
                    summary.decisions_exit,
                    summary.entries_blocked_by_kill_switch,
                    summary.entries_blocked_by_stuck_file,
                    summary.entries_blocked_by_daily_dd,
                    summary.entries_blocked_by_session_dd,
                    summary.entries_blocked_by_circuit_breaker,
                    summary.entries_blocked_by_ws_stale,
                    summary.ws_stale_emergencies_emitted,
                    summary.skew_emergencies_emitted,
                    ws_age.ext_age_ms,
                    ws_age.lt_age_ms,
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
    stuck: &mut StuckTripwire,
    warmup: &mut VenueWarmup,
    ws_health: &mut WsHealthMonitor,
    skew_monitor: &mut SkewMonitor,
    live_exec: Option<&LiveExecution>,
) -> Result<()> {
    // Drain any pending SIGUSR1 — arms the STUCK file if needed.
    let _ = stuck.poll_sigusr1();

    let mut ext_snap = match hub.read_mid(Venue::Extended).await {
        Ok(s) => {
            warmup.mark_ready(Venue::Extended);
            s
        }
        Err(e) if !warmup.is_ready(Venue::Extended) => {
            log::debug!(
                "[XVENUE] read_mid Extended pending (WS warm-up): {:?}",
                e
            );
            return Ok(());
        }
        Err(e) => return Err(e).context("read_mid Extended"),
    };
    let mut lt_snap = match hub.read_mid(Venue::Lighter).await {
        Ok(s) => {
            warmup.mark_ready(Venue::Lighter);
            s
        }
        Err(e) if !warmup.is_ready(Venue::Lighter) => {
            log::debug!(
                "[XVENUE] read_mid Lighter pending (WS warm-up): {:?}",
                e
            );
            return Ok(());
        }
        Err(e) => return Err(e).context("read_mid Lighter"),
    };

    // Record successful WS observations for health tracking. Any
    // `read_mid` Ok is a sign that the venue's WS subscription is
    // alive — we record on Ok regardless of `book_ok`. A
    // one-sided book is a venue-side data quality issue (handled
    // by the spread engine's `book_ok` filter and reference_guard),
    // not a WS health signal — recording only on `book_ok=true`
    // would falsely flag a thin book as a WS outage. True WS
    // outages manifest as `read_mid` Err → no record → eventually
    // `evaluate` sees the staleness.
    let now_wall_ms = wall_clock_ms();
    ws_health.record_book_update(VenueLabel::Extended, now_wall_ms);
    ws_health.record_book_update(VenueLabel::Lighter, now_wall_ms);

    // WS staleness check (#244 Group C). Runs *after* the reads so
    // a successful read clears any prior staleness latch. Decision
    // policy:
    //   - Position open: emit Event::Emergency{WsStale} → state
    //     machine routes to EmergencyFlattening.
    //   - Flat (no position): log debug, continue normally. The
    //     spread engine already drops thin-book samples via
    //     `book_ok=false`; we don't need a separate gate.
    //   - NotReady (warm-up): proceed normally; the read_mid
    //     warm-up gate already handled the "no data yet" case.
    let ws_outcome = ws_health.evaluate(now_wall_ms);
    if let WsHealthOutcome::Stale(stale_venue) = ws_outcome {
        if machine.summary().is_some() {
            let event = Event::Emergency {
                reason: super::state::EmergencyReason::WsStale,
            };
            match machine.apply(now_wall_ms, event) {
                Ok(()) => {
                    log::error!(
                        "[XVENUE] WS staleness emergency: venue={:?} threshold_ms={} \
                         → flattening",
                        stale_venue,
                        ws_health.ws_stale_emergency_ms()
                    );
                    summary.ws_stale_emergencies_emitted += 1;

                    // Paper-mode short-circuit: Group B (real orders +
                    // emergency-flatten loop) is not yet wired, so
                    // there is no producer of `EmergencyComplete` in
                    // dry-run. Without this, a transient WS hiccup
                    // would dead-end the state machine in
                    // EmergencyFlattening. Synthesise the exit fills
                    // + EmergencyComplete so the paper loop recovers.
                    // Live mode (Group B once it lands) MUST NOT take
                    // this path.
                    if cfg.dry_run {
                        if let Some(qty) = open_qty.take() {
                            let _ = machine.apply(
                                now_wall_ms,
                                Event::ExtendedExitFilled { qty },
                            );
                            let _ = machine.apply(
                                now_wall_ms,
                                Event::LighterExitFilled { qty },
                            );
                        }
                        let _ = machine.apply(now_wall_ms, Event::EmergencyComplete);
                        ws_health.reset_after_recovery();
                        log::warn!(
                            "[XVENUE] paper-mode WS-stale recovery: synthesised \
                             EmergencyComplete (Group B will replace this in live)"
                        );
                    }
                }
                Err(e) => {
                    log::debug!(
                        "[XVENUE] WS stale ignored by state machine \
                         (likely already flattening): {:?}",
                        e
                    );
                }
            }
            return Ok(());
        } else {
            log::debug!(
                "[XVENUE] WS staleness in Flat: venue={:?} threshold_ms={} \
                 (no position to flatten — continuing)",
                stale_venue,
                ws_health.ws_stale_emergency_ms()
            );
            // Don't increment entries_blocked_by_ws_stale — Flat
            // doesn't block anything; the spread engine's book_ok
            // filter is what gates entries on bad data.
        }
    }

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

    // Inventory-skew watchdog (#244 Group C). Only meaningful when a
    // position is open — `machine.inventory_skew_usd` returns 0 in
    // Flat, which `skew_monitor.evaluate` short-circuits to Disabled.
    // Runs *after* the spread update so the spread engine still sees
    // this tick's data (we don't want a recovery tick discarded just
    // because the previous trip is still in flight).
    if position.is_some() {
        let skew_dec = machine.inventory_skew_usd(ext_snap.mid, lt_snap.mid);
        let skew_f = rust_decimal::prelude::ToPrimitive::to_f64(&skew_dec).unwrap_or(0.0);
        match skew_monitor.evaluate(skew_f) {
            SkewOutcome::Breach { skew_usd, threshold_usd } => {
                let event = Event::Emergency {
                    reason: super::state::EmergencyReason::SkewBreach,
                };
                match machine.apply(now_ts_ms, event) {
                    Ok(()) => {
                        log::error!(
                            "[XVENUE] inventory skew breach: skew_usd={:.2} \
                             threshold_usd={:.2} → flattening",
                            skew_usd,
                            threshold_usd,
                        );
                        summary.skew_emergencies_emitted += 1;

                        // Same paper-mode short-circuit as ws_health —
                        // Group B will replace this with real flatten
                        // orders driving EmergencyComplete.
                        if cfg.dry_run {
                            if let Some(qty) = open_qty.take() {
                                let _ = machine.apply(
                                    now_ts_ms,
                                    Event::ExtendedExitFilled { qty },
                                );
                                let _ = machine.apply(
                                    now_ts_ms,
                                    Event::LighterExitFilled { qty },
                                );
                            }
                            let _ = machine.apply(now_ts_ms, Event::EmergencyComplete);
                            skew_monitor.reset_after_recovery();
                            log::warn!(
                                "[XVENUE] paper-mode skew recovery: synthesised \
                                 EmergencyComplete (Group B will replace this in live)"
                            );
                        }
                    }
                    Err(e) => {
                        log::debug!(
                            "[XVENUE] skew breach ignored by state machine \
                             (likely already flattening): {:?}",
                            e
                        );
                    }
                }
                return Ok(());
            }
            SkewOutcome::Ok { .. } | SkewOutcome::Disabled => {}
        }
    }

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

    // STUCK file (#244 C / #102 P2). Runner-written tripwire from
    // sustained REST failures, reduce-only failures, or SIGUSR1.
    // Distinct from KILL_SWITCH: STUCK is "something is very wrong"
    // and requires manual `rm` to clear; KILL_SWITCH is just a
    // vacation pause that auto-clears on file removal.
    if matches!(decision, Decision::Enter(_)) && stuck.is_stuck() {
        log::warn!(
            "[XVENUE] STUCK file present ({}); blocking new entry. \
             Operator must inspect and `rm` to resume.",
            stuck.stuck_file_path().display()
        );
        summary.entries_blocked_by_stuck_file += 1;
        decision = Decision::Hold;
    }

    // Phase gate: signal.decide() only sees a position via
    // `machine.summary()`, which is `Some` only in `Phase::Held`.
    // During EnteringExtended / EnteringLighter / Exiting /
    // EmergencyFlattening the strategy can re-emit Enter and the
    // state machine would reject the resulting EntrySignal. Down-
    // grade to Hold so a multi-tick failure mode (e.g. Lighter
    // failed-after-Extended landing in EmergencyFlattening) doesn't
    // panic the runner on the next tick's Decision::Enter.
    if matches!(decision, Decision::Enter(_))
        && !matches!(super::state::PositionMachine::phase(machine), super::state::Phase::Flat)
    {
        log::debug!(
            "[XVENUE] Decision::Enter suppressed: phase={:?} (not Flat)",
            super::state::PositionMachine::phase(machine),
        );
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
            let go_live = !cfg.dry_run && live_exec.is_some();
            if !go_live {
                // Paper-mode synthetic fills: walk the state machine
                // through one EntrySignal + both Filled events in
                // series so the engine stays exercised end-to-end.
                // Used in dry-run and by tests / BT replay paths.
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
                    "[XVENUE] PAPER ENTER dir={:?} dev_bps={:?} ext_mid={} lt_mid={} \
                     qty={} dry_run={}",
                    dir,
                    dev,
                    ext_snap.mid,
                    lt_snap.mid,
                    qty,
                    cfg.dry_run,
                );
            } else {
                // Live mode: equity-driven sizing, real-order
                // dispatch, single-tick failure-mode handling. Sprint
                // 4 step 1/3.
                let live = live_exec.expect(
                    "go_live = !cfg.dry_run && live_exec.is_some(); checked above",
                );
                let ext_eq_opt = hub.read_equity_usd(Venue::Extended).await.ok().flatten();
                let lt_eq_opt = hub.read_equity_usd(Venue::Lighter).await.ok().flatten();
                let sized = compute_notional_usd(
                    ext_eq_opt,
                    lt_eq_opt,
                    cfg.trade_size_pct,
                    cfg.min_notional_usd,
                    cfg.max_notional_usd,
                );
                let notional = match sized {
                    SizeOutcome::Use(n) => n,
                    SizeOutcome::BelowMin => {
                        log::warn!(
                            "[XVENUE] LIVE ENTER skipped: sized notional below \
                             min_notional_usd (ext_eq={:?} lt_eq={:?} pct={} min={})",
                            ext_eq_opt,
                            lt_eq_opt,
                            cfg.trade_size_pct,
                            cfg.min_notional_usd,
                        );
                        summary.live_entries_skipped_size_below_min += 1;
                        return Ok(());
                    }
                    SizeOutcome::EquityUnavailable => {
                        log::warn!(
                            "[XVENUE] LIVE ENTER skipped: equity unavailable \
                             (ext_eq={:?} lt_eq={:?})",
                            ext_eq_opt,
                            lt_eq_opt,
                        );
                        summary.live_entries_skipped_equity_unavailable += 1;
                        return Ok(());
                    }
                };
                let Some(ext_qty) = notional_to_qty(notional, ext_snap.mid) else {
                    log::warn!(
                        "[XVENUE] LIVE ENTER skipped: ext mid non-positive ({})",
                        ext_snap.mid
                    );
                    return Ok(());
                };
                let Some(lt_qty) = notional_to_qty(notional, lt_snap.mid) else {
                    log::warn!(
                        "[XVENUE] LIVE ENTER skipped: lt mid non-positive ({})",
                        lt_snap.mid
                    );
                    return Ok(());
                };
                machine.apply(
                    now_ts_ms,
                    Event::EntrySignal {
                        direction: dir,
                        notional_usd: notional,
                    },
                )?;
                summary.last_decision_ts_ms = Some(now_ts_ms);
                match dir {
                    SpreadDirection::Long => summary.decisions_enter_long += 1,
                    SpreadDirection::Short => summary.decisions_enter_short += 1,
                }
                log::info!(
                    "[XVENUE] LIVE ENTER start dir={:?} dev_bps={:?} ext_mid={} lt_mid={} \
                     notional={} ext_qty={} lt_qty={}",
                    dir,
                    dev,
                    ext_snap.mid,
                    lt_snap.mid,
                    notional,
                    ext_qty,
                    lt_qty,
                );
                let (ext_side, lt_side) = match dir {
                    // Direction sign convention (cf. dev_breach test +
                    // signal.rs): SpreadDirection::Long means the spread
                    // is below mean and we expect mean reversion → buy
                    // the cheap leg (Extended) and sell the rich leg
                    // (Lighter). Short is symmetric.
                    SpreadDirection::Long => (DcOrderSide::Long, DcOrderSide::Short),
                    SpreadDirection::Short => (DcOrderSide::Short, DcOrderSide::Long),
                };
                // Sequential Extended-first dispatch per DESIGN.md
                // §4.1. Lighter fires only after Extended terminates
                // — a serial dispatch keeps the legged-exposure
                // window bounded by Extended's chase × retries
                // budget rather than the parallel max.
                let ext_term = ExtendedMakerLoop::new(
                    &*live.ext_ops,
                    &live.extended_maker_cfg,
                )
                .run_entry(ExtendedEntryRequest {
                    symbol: live.ext_symbol.clone(),
                    side: ext_side,
                    target_qty: ext_qty,
                    dust_qty: live.dust_qty,
                    reduce_only: false,
                })
                .await;
                match ext_term {
                    ExtendedTerminal::Filled { qty } => {
                        machine.apply(now_ts_ms, Event::ExtendedFilled { qty })?;
                        if let Some(r) = reporter.as_deref_mut() {
                            r.record_fill(true, false, now_ts_ms);
                        }
                        log::info!("[XVENUE] LIVE ENTER ext filled qty={}", qty);
                        let lt_term = LighterFillLoop::new(
                            &*live.lt_ops,
                            &live.lighter_fill_cfg,
                        )
                        .run(LighterFillRequest {
                            symbol: live.lt_symbol.clone(),
                            side: lt_side,
                            target_qty: lt_qty,
                            dust_qty: live.dust_qty,
                            reduce_only: false,
                        })
                        .await;
                        match lt_term {
                            LighterTerminal::Filled { qty: lt_filled } => {
                                machine.apply(
                                    now_ts_ms,
                                    Event::LighterFilled { qty: lt_filled },
                                )?;
                                *open_qty = Some(lt_filled);
                                if let Some(r) = reporter.as_deref_mut() {
                                    r.record_fill(false, true, now_ts_ms);
                                }
                                log::info!(
                                    "[XVENUE] LIVE ENTER lt filled qty={} → Held",
                                    lt_filled
                                );
                            }
                            LighterTerminal::Failed { reason } => {
                                log::error!(
                                    "[XVENUE] LIVE ENTER lt failed reason={:?} → \
                                     emergency (ext leg open qty={})",
                                    reason,
                                    qty,
                                );
                                machine.apply(now_ts_ms, Event::LighterFailed)?;
                                summary.live_entries_lighter_failed_after_extended += 1;
                                // open_qty intentionally stays None at
                                // this layer; the open Extended leg
                                // will be flattened by the
                                // emergency_loop wiring (Sprint 4 step
                                // 3/3). State machine has already
                                // routed to EmergencyFlattening.
                            }
                        }
                    }
                    ExtendedTerminal::Failed { reason } => {
                        log::error!(
                            "[XVENUE] LIVE ENTER ext failed reason={:?} → state→Flat \
                             (no fills landed)",
                            reason,
                        );
                        machine.apply(now_ts_ms, Event::ExtendedFailed)?;
                        summary.live_entries_extended_failed += 1;
                    }
                }
            }
        }
        Decision::Exit(reason) => {
            let go_live = !cfg.dry_run && live_exec.is_some();
            if !go_live {
                // Paper-mode synthetic exit fills (existing behaviour).
                let qty = open_qty.take().unwrap_or(Decimal::ZERO);
                machine.apply(now_ts_ms, Event::ExitSignal { reason })?;
                if qty > Decimal::ZERO {
                    machine.apply(now_ts_ms, Event::ExtendedExitFilled { qty })?;
                    machine.apply(now_ts_ms, Event::LighterExitFilled { qty })?;
                    if let Some(r) = reporter.as_deref_mut() {
                        r.record_fill(true, true, now_ts_ms);
                        r.record_close(0.0);
                    }
                    risk_manager.record_close(0.0, now_unix_secs());
                }
                summary.last_decision_ts_ms = Some(now_ts_ms);
                summary.decisions_exit += 1;
                log::info!(
                    "[XVENUE] PAPER EXIT reason={:?} dev_bps={:?} ext_mid={} lt_mid={} \
                     dry_run={}",
                    reason,
                    dev,
                    ext_snap.mid,
                    lt_snap.mid,
                    cfg.dry_run,
                );
                let _ = ExitReason::MeanCross;
            } else {
                // Live mode: reduce-only parallel exit on both legs
                // (#244 Sprint 4 step 2/3). DESIGN.md §4.2 specifies
                // parallel exit (vs entry's serial Extended-first
                // dispatch) — the legs are already balanced so we
                // close both simultaneously to minimise the open-leg
                // window if one venue lags.
                let live = live_exec.expect(
                    "go_live = !cfg.dry_run && live_exec.is_some(); checked above",
                );
                // Capture position direction + qty BEFORE applying
                // ExitSignal — `summary()` returns `None` once we
                // leave Held.
                let position_dir = match machine.summary() {
                    Some(p) => p.direction,
                    None => {
                        log::warn!(
                            "[XVENUE] LIVE EXIT skipped: no position summary in phase {:?}",
                            machine.phase()
                        );
                        return Ok(());
                    }
                };
                let qty = open_qty.take().unwrap_or(Decimal::ZERO);
                if qty <= Decimal::ZERO {
                    log::warn!(
                        "[XVENUE] LIVE EXIT skipped: open_qty zero or unset in phase {:?}",
                        machine.phase()
                    );
                    return Ok(());
                }
                machine.apply(now_ts_ms, Event::ExitSignal { reason })?;
                summary.last_decision_ts_ms = Some(now_ts_ms);
                summary.decisions_exit += 1;
                let (ext_exit_side, lt_exit_side) = match position_dir {
                    // Reverse the entry sides — see Decision::Enter
                    // for the entry sign convention. To close a
                    // long leg we sell (Short), and vice versa.
                    SpreadDirection::Long => (DcOrderSide::Short, DcOrderSide::Long),
                    SpreadDirection::Short => (DcOrderSide::Long, DcOrderSide::Short),
                };
                log::info!(
                    "[XVENUE] LIVE EXIT start reason={:?} dev_bps={:?} dir={:?} qty={}",
                    reason,
                    dev,
                    position_dir,
                    qty,
                );
                let outcome = ParallelExitLoop::new(
                    &*live.ext_ops,
                    &*live.lt_ops,
                    &live.extended_maker_cfg,
                    &live.lighter_fill_cfg,
                    &live.parallel_exit_cfg,
                )
                .run(
                    ExtendedEntryRequest {
                        symbol: live.ext_symbol.clone(),
                        side: ext_exit_side,
                        target_qty: qty,
                        dust_qty: live.dust_qty,
                        reduce_only: true,
                    },
                    LighterFillRequest {
                        symbol: live.lt_symbol.clone(),
                        side: lt_exit_side,
                        target_qty: qty,
                        dust_qty: live.dust_qty,
                        reduce_only: true,
                    },
                )
                .await;
                match outcome {
                    ParallelExitOutcome::Both { ext, lt } => {
                        let ext_filled = match ext {
                            ExtendedTerminal::Filled { qty: q } => {
                                machine.apply(
                                    now_ts_ms,
                                    Event::ExtendedExitFilled { qty: q },
                                )?;
                                true
                            }
                            ExtendedTerminal::Failed { reason: r } => {
                                log::error!(
                                    "[XVENUE] LIVE EXIT ext failed reason={:?}",
                                    r
                                );
                                false
                            }
                        };
                        let lt_filled = match lt {
                            LighterTerminal::Filled { qty: q } => {
                                machine.apply(
                                    now_ts_ms,
                                    Event::LighterExitFilled { qty: q },
                                )?;
                                true
                            }
                            LighterTerminal::Failed { reason: r } => {
                                log::error!(
                                    "[XVENUE] LIVE EXIT lt failed reason={:?}",
                                    r
                                );
                                false
                            }
                        };
                        if ext_filled && lt_filled {
                            if let Some(r) = reporter.as_deref_mut() {
                                r.record_fill(true, true, now_ts_ms);
                                // Realised PnL still 0 at this layer —
                                // Sprint 5 will replace with the actual
                                // USD delta computed from entry/exit
                                // mids + fees.
                                r.record_close(0.0);
                            }
                            risk_manager.record_close(0.0, now_unix_secs());
                            log::info!("[XVENUE] LIVE EXIT both legs filled → Flat");
                        } else {
                            // At least one leg failed. Route to
                            // EmergencyFlattening — the emergency_loop
                            // (Sprint 4 step 3/3) will keep retrying
                            // reduce-only close until the leg is zero.
                            // No dedicated `ExitFailed` reason in
                            // EmergencyReason — `LegMismatchTimeout`
                            // is the closest semantic fit since the
                            // exit didn't terminate cleanly on one
                            // side.
                            machine.apply(
                                now_ts_ms,
                                Event::Emergency {
                                    reason: EmergencyReason::LegMismatchTimeout,
                                },
                            )?;
                            summary.live_exits_failed_legs += 1;
                            log::error!(
                                "[XVENUE] LIVE EXIT failed legs (ext_filled={}, \
                                 lt_filled={}) → EmergencyFlattening",
                                ext_filled,
                                lt_filled,
                            );
                        }
                    }
                    ParallelExitOutcome::LegMismatchTimeout { ext, lt } => {
                        // Apply whatever terminal we have first so
                        // the state machine has the latest open qty
                        // before transitioning to EmergencyFlattening
                        // (mirrors parallel_exit.rs's documented
                        // contract for case 11).
                        if let Some(ExtendedTerminal::Filled { qty: q }) = ext {
                            machine.apply(
                                now_ts_ms,
                                Event::ExtendedExitFilled { qty: q },
                            )?;
                        }
                        if let Some(LighterTerminal::Filled { qty: q }) = lt {
                            machine.apply(
                                now_ts_ms,
                                Event::LighterExitFilled { qty: q },
                            )?;
                        }
                        machine.apply(
                            now_ts_ms,
                            Event::Emergency {
                                reason: EmergencyReason::LegMismatchTimeout,
                            },
                        )?;
                        summary.live_exits_leg_mismatch += 1;
                        log::error!(
                            "[XVENUE] LIVE EXIT leg-mismatch ext={:?} lt={:?} → \
                             EmergencyFlattening",
                            ext,
                            lt,
                        );
                    }
                }
            }
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

/// Run one emergency-flatten round (#244 Sprint 4 step 3/3).
///
/// Called from `run_paper_loop` on every tick where:
/// - `cfg.dry_run = false` (paper-mode synthesises EmergencyComplete
///   inline in the WS-stale / skew handlers, so the live emergency
///   loop is the only producer of close_all calls).
/// - `live_exec.is_some()`.
/// - `machine.phase() == EmergencyFlattening`.
///
/// Throttled to `live.emergency_loop_cfg.retry_interval_ms` — the
/// 30 s cadence is the slow-mm 167-min stuck precedent fix
/// (`docs/execution_layer.md` §5). Without throttling the runner
/// would hammer `close_all` every tick (~250 ms) and accumulate
/// REST failures faster than they decay.
///
/// Distinct from `crate::trade::execution::emergency_loop::EmergencyLoop`:
/// that module owns a self-contained loop with internal sleeps,
/// suitable for a spawned task. This helper does **one** round per
/// runner tick so the spread engine + risk monitors keep evaluating
/// while the flatten is in progress. The kill semantics are
/// identical (5 consec fails arm STUCK, max_attempts caps total).
async fn drive_emergency_flatten_round(
    live: &LiveExecution,
    machine: &mut PositionMachine,
    open_qty: &mut Option<Decimal>,
    stuck: &mut StuckTripwire,
    summary: &mut LivePaperSummary,
    last_attempt_ms: &mut Option<u64>,
    attempts: &mut u32,
    now_ms: u64,
) -> Result<()> {
    // Throttle: skip if we attempted within the last
    // `retry_interval_ms`. First call (last_attempt_ms = None) goes
    // through immediately.
    if let Some(last) = *last_attempt_ms {
        if now_ms.saturating_sub(last) < live.emergency_loop_cfg.retry_interval_ms {
            return Ok(());
        }
    }
    // Defensive cap. Once exceeded, the handler stops attempting;
    // operator inspects (the STUCK file may also be armed by then).
    if *attempts >= live.emergency_loop_cfg.max_attempts {
        // Increment once when the cap is first crossed (subsequent
        // ticks short-circuit here without bumping the counter
        // again until the phase resets).
        if !matches!(*last_attempt_ms, None) {
            // Already counted on the cap-crossing tick.
        }
        return Ok(());
    }
    // Once STUCK is armed (prior round crossed the kill threshold,
    // or an operator dropped the file via SIGUSR1) we stop attempting
    // — the operator must clear it and drop a RISK_ACK.
    if stuck.is_stuck() {
        return Ok(());
    }
    *attempts += 1;

    // Read each venue's open qty.
    let qtys = match live.leg_reader.read_leg_qtys().await {
        Ok(q) => q,
        Err(e) => {
            log::warn!(
                "[XVENUE/emerg] read_leg_qtys err={:?} (treating as still-open, retry next round)",
                e
            );
            *last_attempt_ms = Some(now_ms);
            return Ok(());
        }
    };

    if qtys.both_zero() {
        // Case 13 boundary — both venues report zero. Emit
        // EmergencyComplete; state machine transitions back to
        // Flat (or rejects with TransitionError if we're already
        // out of EmergencyFlattening, in which case we log and
        // continue).
        match machine.apply(now_ms, Event::EmergencyComplete) {
            Ok(()) => {
                summary.emergency_completes += 1;
                *open_qty = None;
                log::info!(
                    "[XVENUE/emerg] both legs zero → EmergencyComplete (attempts={})",
                    *attempts
                );
            }
            Err(e) => {
                log::debug!(
                    "[XVENUE/emerg] EmergencyComplete rejected (likely already Flat): {:?}",
                    e
                );
            }
        }
        return Ok(());
    }

    // At least one leg open — issue close_all on the non-zero
    // venues. Sequential rather than parallel so a Lighter rejection
    // doesn't mask the Extended-side counter or vice versa (mirrors
    // EmergencyLoop's per-call kill counter).
    if !qtys.ext.is_zero() {
        match live.ext_ops.close_all(None).await {
            Ok(()) => stuck.record_reduce_only_success(),
            Err(e) => {
                log::warn!("[XVENUE/emerg] close_all ext err={:?}", e);
                summary.emergency_close_all_failures += 1;
                let armed = stuck.record_reduce_only_failure();
                if armed {
                    summary.emergency_stuck_armed += 1;
                    log::error!(
                        "[XVENUE/emerg] STUCK armed after ext close_all failure — \
                         operator must inspect + clear"
                    );
                    *last_attempt_ms = Some(now_ms);
                    return Ok(());
                }
            }
        }
    }
    if !qtys.lt.is_zero() {
        match live.lt_ops.close_all(None).await {
            Ok(()) => stuck.record_reduce_only_success(),
            Err(e) => {
                log::warn!("[XVENUE/emerg] close_all lt err={:?}", e);
                summary.emergency_close_all_failures += 1;
                let armed = stuck.record_reduce_only_failure();
                if armed {
                    summary.emergency_stuck_armed += 1;
                    log::error!(
                        "[XVENUE/emerg] STUCK armed after lt close_all failure — \
                         operator must inspect + clear"
                    );
                    *last_attempt_ms = Some(now_ms);
                    return Ok(());
                }
            }
        }
    }

    *last_attempt_ms = Some(now_ms);

    if *attempts >= live.emergency_loop_cfg.max_attempts {
        summary.emergency_max_attempts_exceeded += 1;
        log::warn!(
            "[XVENUE/emerg] max_attempts ({}) reached without zeroing legs — \
             handler will idle until phase resets",
            live.emergency_loop_cfg.max_attempts
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::manager::{RiskConfig, RiskManager};
    use crate::xvenue::test_helpers::{mid, stale_mid, ScriptedHub, WarmupHub};
    use tokio::time::timeout;

    /// Test fixture: build a paused (manual) ReferenceGuard so unit
    /// tests don't spawn a real polling task / hit the network. The
    /// guard returns NoReference until a test injects a mid.
    fn test_reference_guard() -> ReferenceGuard {
        ReferenceGuard::manual(100.0, 3)
    }

    /// Test fixture: STUCK tripwire pointing at a temp dir with no
    /// SIGUSR1 handler installed (so the unit tests don't fight over
    /// the global signal mask).
    fn test_stuck() -> (tempfile::TempDir, crate::risk::kill_switch::StuckTripwire) {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = crate::risk::kill_switch::StuckTripwireConfig {
            stuck_file: dir.path().join("STUCK"),
            rest_consec_fail_to_escalate: 3,
            reduce_only_consec_fail_to_kill: 5,
        };
        (
            dir,
            crate::risk::kill_switch::StuckTripwire::new_for_test(cfg),
        )
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
        let summary = timeout(Duration::from_secs(1), run_paper_loop(cfg, loop_cfg, hub, None, rx))
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
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
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
            &mut stuck,
            &mut warmup,
            &mut ws_health,
            &mut skew_monitor,
            None,
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
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
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
            &mut stuck,
            &mut warmup,
            &mut ws_health,
            &mut skew_monitor,
            None,
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
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
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
                &mut stuck,
                &mut warmup,
                &mut ws_health,
                &mut skew_monitor,
                None,
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
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
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
                &mut stuck,
                &mut warmup,
                &mut ws_health,
                &mut skew_monitor,
                None,
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
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
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
                &mut stuck,
                &mut warmup,
                &mut ws_health,
                &mut skew_monitor,
                None,
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
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
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
                &mut stuck,
                &mut warmup,
                &mut ws_health,
                &mut skew_monitor,
                None,
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
        // Disable the live reference_guard HTTP poll for the test —
        // we don't want a real network request firing during a 80 ms
        // tick benchmark. Empty symbol triggers ReferenceGuard::disabled.
        let mut cfg = min_cfg();
        cfg.binance_reference_symbol = String::new();
        let loop_cfg = LiveLoopConfig {
            tick_interval_ms: 5,
            status_log_interval_ms: 10_000,
        };
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(run_paper_loop(cfg, loop_cfg, hub, None, rx));
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

    #[tokio::test]
    async fn read_mid_errors_during_warmup_skip_tick_silently() {
        // Simulates the WS warm-up window (#248): each venue's first 2
        // read_mid calls return Err — same shape as the live
        // "order book snapshot unavailable (no recent update)" — and
        // subsequent calls succeed via the inner ScriptedHub. We expect
        // run_one_tick to swallow the warm-up errors with Ok(()) so the
        // outer loop's WARN never fires, and to commit a sample once
        // both venues have produced their first successful read.
        let scripted = ScriptedHub::new(
            vec![mid(1000, 2000.0), mid(2000, 2001.0)],
            vec![mid(1000, 2000.0), mid(2000, 2000.5)],
        );
        let hub = Arc::new(WarmupHub::new(scripted, 2));
        let cfg = min_cfg();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;
        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);

        // First two ticks: Extended fails before Lighter is even
        // attempted (run_one_tick reads Extended first), so neither
        // venue is marked ready.
        for _ in 0..2 {
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
                &mut stuck,
                &mut warmup,
                &mut ws_health,
                &mut skew_monitor,
                None,
            )
            .await
            .expect("warm-up errors must not propagate");
        }
        assert!(!warmup.ext_ready);
        assert!(!warmup.lt_ready);
        assert_eq!(summary.samples_committed, 0);

        // Third tick: Extended OK → flips ext_ready, then Lighter still
        // has 2 fails left (only Extended was ever attempted in the
        // first two ticks since reads are sequential), so Lighter
        // returns Err. Because lt_ready is still false, run_one_tick
        // again returns Ok(()) silently.
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
            &mut stuck,
            &mut warmup,
            &mut ws_health,
            &mut skew_monitor,
            None,
        )
        .await
        .expect("warm-up errors must not propagate");
        assert!(warmup.ext_ready);
        assert!(!warmup.lt_ready);

        // Two more ticks drain Lighter's fail counter; the next tick
        // after that produces a successful read on both legs.
        for _ in 0..2 {
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
                &mut stuck,
                &mut warmup,
                &mut ws_health,
                &mut skew_monitor,
                None,
            )
            .await
            .expect("warm-up errors must not propagate");
        }
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
            &mut stuck,
            &mut warmup,
            &mut ws_health,
            &mut skew_monitor,
            None,
        )
        .await
        .expect("post-warmup tick must succeed");
        assert!(warmup.ext_ready);
        assert!(warmup.lt_ready);
        assert_eq!(summary.samples_committed, 1);
    }

    #[tokio::test]
    async fn read_mid_errors_after_warmup_propagate_as_warn() {
        // Once a venue has been marked ready, subsequent read_mid
        // failures must surface as Err so the outer loop's WARN line
        // fires (WS genuinely went stale).
        struct AlwaysFailHub;
        #[async_trait::async_trait]
        impl VenueHub for AlwaysFailHub {
            async fn read_mid(&self, _venue: Venue) -> Result<MidSnapshot> {
                Err(anyhow::anyhow!(
                    "order book snapshot unavailable (no recent update)"
                ))
            }
            async fn read_equity_usd(
                &self,
                _venue: Venue,
            ) -> Result<Option<Decimal>> {
                Ok(None)
            }
        }

        let cfg = min_cfg();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty = None;
        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        let (_st_dir, mut stuck) = test_stuck();
        // Pre-arm warmup as if a successful read has already happened
        // on both venues; the failing hub now drives the post-warmup
        // path where errors must propagate.
        let mut warmup = VenueWarmup {
            ext_ready: true,
            lt_ready: true,
        };
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
        let hub = AlwaysFailHub;
        let err = run_one_tick(
            &cfg,
            &hub,
            &mut spread,
            &mut signal,
            &mut machine,
            &mut summary,
            &mut open_qty,
            None,
            &mut rm,
            &mut rg,
            &mut stuck,
            &mut warmup,
            &mut ws_health,
            &mut skew_monitor,
            None,
        )
        .await
        .expect_err("post-warmup read_mid Err must propagate as Err");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("read_mid Extended"),
            "expected context-wrapped error, got: {chain}"
        );
    }

    // ---------------------------------------------------------------
    // Sprint 4 Decision::Enter live wiring (#244)
    // ---------------------------------------------------------------

    use crate::trade::execution::venue_ops::{OrderFillStatus, ScriptedVenueOps, VenueOps};
    use crate::xvenue::live_exec::LiveExecution;
    use crate::xvenue::state::Phase;
    use rust_decimal_macros::dec;

    /// Live-mode test config — overrides extended_post_only=false to
    /// skip maker chase and go straight to taker, plus tight timeouts
    /// so failure-path tests terminate quickly under
    /// `tokio::test(start_paused = true)`. min_notional_usd=$50 so
    /// the default `with_equity($500, $500)` ScriptedHub sizes
    /// exactly at the minimum.
    fn live_test_cfg() -> XvenueConfig {
        let yaml = r#"
agent_name: live-test
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 50
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
spread_bucket_ms: 1000
rolling_window_sec: 30
min_warmup_samples: 3
abs_threshold_bps: 5.0
persistence_sec: 1
max_hold_sec: 60
extended_post_only: false
extended_chase_retries: 1
extended_chase_timeout_ms: 100
extended_taker_fallback: true
lighter_fill_timeout_ms: 100
"#;
        let c: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        c
    }

    /// Standard breach sequence: 30 buckets at zero spread, then 8
    /// buckets at +30 bps. Persistence=1 fires `Decision::Enter(Short)`
    /// near the start of the breach run.
    fn breach_sequence() -> (Vec<MidSnapshot>, Vec<MidSnapshot>) {
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
        (ext, lt)
    }

    /// Drive `run_one_tick` through `iters` iterations against `hub`
    /// + `live`, returning the populated machine + summary. Reduces
    /// boilerplate across the live-mode tests (each test would
    /// otherwise duplicate ~30 lines of state setup).
    async fn drive_live_ticks<H: VenueHub + ?Sized>(
        cfg: &XvenueConfig,
        hub: &H,
        live: &LiveExecution,
        iters: u64,
    ) -> (PositionMachine, LivePaperSummary, Option<Decimal>) {
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut machine = PositionMachine::new();
        let mut summary = LivePaperSummary::default();
        let mut open_qty: Option<Decimal> = None;
        let (_rm_dir, mut rm) = test_risk_manager();
        let mut rg = test_reference_guard();
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup::default();
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
        for _ in 0..iters {
            run_one_tick(
                cfg,
                hub,
                &mut spread,
                &mut signal,
                &mut machine,
                &mut summary,
                &mut open_qty,
                None,
                &mut rm,
                &mut rg,
                &mut stuck,
                &mut warmup,
                &mut ws_health,
                &mut skew_monitor,
                Some(live),
            )
            .await
            .unwrap();
        }
        (machine, summary, open_qty)
    }

    fn live_with_scripted(
        cfg: &XvenueConfig,
        ext: Arc<ScriptedVenueOps>,
        lt: Arc<ScriptedVenueOps>,
    ) -> LiveExecution {
        let ext_dyn: Arc<dyn VenueOps> = ext;
        let lt_dyn: Arc<dyn VenueOps> = lt;
        LiveExecution::from_config(cfg, ext_dyn, lt_dyn).expect("live exec from cfg")
    }

    #[tokio::test(start_paused = true)]
    async fn live_enter_happy_path_walks_extended_then_lighter_to_held() {
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let (ext_seq, lt_seq) = breach_sequence();
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        ext_vops.with_state(|s| {
            // Generous default fill saturates whatever target_qty the
            // sizing layer picks — terminal=true on the first poll so
            // the run completes in O(1) polls.
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, 38).await;
        assert!(
            summary.decisions_enter_short >= 1,
            "expected enter_short ≥ 1, got summary={:?}",
            summary
        );
        assert_eq!(
            machine.phase(),
            Phase::Held,
            "both legs filled → Held; got phase={:?}",
            machine.phase()
        );
        assert!(open_qty.is_some(), "live happy path sets open_qty");
        assert_eq!(summary.live_entries_extended_failed, 0);
        assert_eq!(summary.live_entries_lighter_failed_after_extended, 0);
        assert_eq!(summary.live_entries_skipped_size_below_min, 0);
        assert_eq!(summary.live_entries_skipped_equity_unavailable, 0);
        // post_only=false in the test cfg means Extended bypasses
        // maker — both legs reach the venue via place_taker.
        assert_eq!(ext_vops.snapshot_takers().len(), 1);
        assert!(ext_vops.snapshot_posts().is_empty());
        assert_eq!(lt_vops.snapshot_takers().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn live_enter_extended_failed_routes_to_flat_without_lighter_call() {
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let (ext_seq, lt_seq) = breach_sequence();
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        // Default fill is zero non-terminal → taker times out →
        // ExtendedTerminal::Failed{Timeout}.
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        // Lighter is set up but should never be invoked — tests assert
        // its call count stays zero.
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, 38).await;
        assert!(summary.decisions_enter_short >= 1, "Enter must have fired");
        assert_eq!(
            machine.phase(),
            Phase::Flat,
            "ExtendedFailed with no fills lands back in Flat"
        );
        assert!(
            open_qty.is_none(),
            "Extended fail must not leave a phantom open_qty"
        );
        assert!(summary.live_entries_extended_failed >= 1);
        assert_eq!(summary.live_entries_lighter_failed_after_extended, 0);
        assert!(
            lt_vops.snapshot_takers().is_empty(),
            "Lighter executor must not run when Extended already failed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn live_enter_lighter_failed_after_extended_routes_to_emergency() {
        // The legged-exposure case the user emphasised — Extended
        // filled, Lighter fails. State machine MUST transition to
        // EmergencyFlattening so the open Extended leg gets cleaned
        // up (Sprint 4 step 3/3).
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let (ext_seq, lt_seq) = breach_sequence();
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        ext_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        // Lighter default = zero non-terminal → times out → Failed{Timeout}.
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, 38).await;
        assert!(summary.decisions_enter_short >= 1);
        assert_eq!(
            machine.phase(),
            Phase::EmergencyFlattening,
            "Lighter fail after Extended fill MUST route to EmergencyFlattening"
        );
        assert!(
            open_qty.is_none(),
            "open_qty stays None on Lighter fail — emergency_loop drives flatten"
        );
        assert!(summary.live_entries_lighter_failed_after_extended >= 1);
        assert_eq!(summary.live_entries_extended_failed, 0);
        // Both executors invoked exactly once.
        assert_eq!(ext_vops.snapshot_takers().len(), 1);
        assert_eq!(lt_vops.snapshot_takers().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn live_enter_below_min_notional_skips_state_machine_unchanged() {
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        // Equity * pct ($100 * 0.05 = $5) < min_notional ($50) →
        // SizeOutcome::BelowMin.
        let (ext_seq, lt_seq) = breach_sequence();
        let hub = Arc::new(
            ScriptedHub::new(ext_seq, lt_seq).with_equity(dec!(50), dec!(50)),
        );
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, 38).await;
        // `decisions_enter_short` is gated on EntrySignal landing
        // (mirrors the existing kill_switch / risk-gate pattern: a
        // skipped Enter does NOT bump the success counter). Proof
        // the strategy actually decided Enter is the size-skip
        // counter being non-zero.
        assert!(
            summary.live_entries_skipped_size_below_min >= 1,
            "size-below-min skip must increment its counter on every Enter \
             that fails sizing; got summary={:?}",
            summary
        );
        assert_eq!(machine.phase(), Phase::Flat, "state machine untouched on skip");
        assert!(open_qty.is_none());
        assert_eq!(summary.decisions_enter_short, 0);
        assert!(
            ext_vops.snapshot_takers().is_empty(),
            "no orders flow when sizing is below min"
        );
        assert!(lt_vops.snapshot_takers().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn live_enter_equity_unavailable_skips_state_machine_unchanged() {
        // Hub variant that returns None for read_equity_usd —
        // simulates connector warm-up before the equity stream lands.
        struct NoEquityHub {
            inner: ScriptedHub,
        }
        #[async_trait::async_trait]
        impl VenueHub for NoEquityHub {
            async fn read_mid(&self, venue: Venue) -> Result<MidSnapshot> {
                self.inner.read_mid(venue).await
            }
            async fn read_equity_usd(
                &self,
                _venue: Venue,
            ) -> Result<Option<Decimal>> {
                Ok(None)
            }
        }
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let (ext_seq, lt_seq) = breach_sequence();
        let hub = Arc::new(NoEquityHub {
            inner: ScriptedHub::new(ext_seq, lt_seq),
        });
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, 38).await;
        // Same gating semantics as the size-below-min test: the
        // counter we assert on is the explicit skip counter, not
        // decisions_enter_short.
        assert!(
            summary.live_entries_skipped_equity_unavailable >= 1,
            "equity-unavailable skip must increment its counter; got summary={:?}",
            summary
        );
        assert_eq!(machine.phase(), Phase::Flat);
        assert!(open_qty.is_none());
        assert_eq!(summary.decisions_enter_short, 0);
        assert!(ext_vops.snapshot_takers().is_empty());
        assert!(lt_vops.snapshot_takers().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn live_enter_paper_path_unchanged_when_dry_run_true() {
        // dry_run=true must keep the synthetic-fill path even when a
        // LiveExecution is provided — guards against accidental live
        // dispatch during BT replay or paper-mode testing.
        let mut cfg = live_test_cfg();
        cfg.dry_run = true;
        let (ext_seq, lt_seq) = breach_sequence();
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, 38).await;
        assert!(summary.decisions_enter_short >= 1);
        assert_eq!(
            machine.phase(),
            Phase::Held,
            "paper path's synthetic fills land in Held"
        );
        assert!(open_qty.is_some());
        assert!(
            ext_vops.snapshot_takers().is_empty(),
            "dry_run must not dispatch real orders"
        );
        assert!(lt_vops.snapshot_takers().is_empty());
    }

    // ---------------------------------------------------------------
    // Sprint 4 Decision::Exit live wiring (#244 step 2/3)
    // ---------------------------------------------------------------

    /// Breach + sustain sequence for Exit testing: 30 warm + 4 breach
    /// (fires Enter) + `hold_n` more breach buckets at the same +20
    /// bps. After `max_hold_sec` virtual seconds past entry, the
    /// signal returns `Decision::Exit(MaxHold)`.
    fn breach_then_hold_sequence(hold_n: u64) -> (Vec<MidSnapshot>, Vec<MidSnapshot>) {
        let lt_mid = 2000.0;
        // +20 bps clears abs_threshold=5 but stays under
        // force_close_dev_bps=30 so the position holds until max_hold
        // fires rather than triggering ForceClose immediately.
        let breach_ext = lt_mid * (1.0 + 20.0 / 10_000.0);
        let mut ext = Vec::new();
        let mut lt = Vec::new();
        let warm_n = 30u64;
        let breach_n = 4u64;
        let t0 = 200_000u64;
        for i in 0..warm_n {
            ext.push(mid(t0 + i * 1000, lt_mid));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        for i in warm_n..(warm_n + breach_n + hold_n) {
            ext.push(mid(t0 + i * 1000, breach_ext));
            lt.push(mid(t0 + i * 1000, lt_mid));
        }
        (ext, lt)
    }

    /// Cfg variant tuned for the leg-mismatch test path —
    /// `leg_mismatch_timeout_ms` (100 ms) is well under
    /// `lighter_fill_timeout_ms` (1000 ms), so a Lighter that never
    /// terminates trips the parallel exit's mismatch deadline before
    /// its own timeout fires.
    fn live_test_cfg_leg_mismatch() -> XvenueConfig {
        let yaml = r#"
agent_name: live-test-leg-mismatch
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 50
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
spread_bucket_ms: 1000
rolling_window_sec: 30
min_warmup_samples: 3
abs_threshold_bps: 5.0
persistence_sec: 1
max_hold_sec: 60
extended_post_only: false
extended_chase_retries: 1
extended_chase_timeout_ms: 50
extended_taker_fallback: true
lighter_fill_timeout_ms: 1000
leg_mismatch_timeout_ms: 100
"#;
        let c: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        c
    }

    #[tokio::test(start_paused = true)]
    async fn live_exit_happy_path_walks_parallel_exit_to_flat() {
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        // Run long enough past entry that max_hold (60 s with our
        // 1 s buckets → 60 ticks past Enter) fires Decision::Exit.
        let (ext_seq, lt_seq) = breach_then_hold_sequence(70);
        let total = ext_seq.len() as u64;
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        ext_vops.with_state(|s| {
            // Default fill terminal-filled handles both entry and
            // exit takers — extended_post_only=false in cfg means
            // both cycles skip maker and hit place_taker.
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, total).await;
        assert!(
            summary.decisions_enter_short >= 1,
            "Enter must have fired; summary={:?}",
            summary
        );
        assert!(
            summary.decisions_exit >= 1,
            "max_hold must have fired Exit within {} ticks; summary={:?}",
            total,
            summary
        );
        assert_eq!(
            machine.phase(),
            Phase::Flat,
            "live exit happy path lands in Flat; got phase={:?}",
            machine.phase()
        );
        assert!(open_qty.is_none());
        assert_eq!(summary.live_exits_failed_legs, 0);
        assert_eq!(summary.live_exits_leg_mismatch, 0);
        // 1 entry taker + 1 exit taker per venue.
        assert_eq!(ext_vops.snapshot_takers().len(), 2);
        assert_eq!(lt_vops.snapshot_takers().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn live_exit_leg_mismatch_routes_to_emergency_flattening() {
        // Catalogue case 11 at the runner orchestration level. Lighter
        // never terminates inside the parallel-exit window → mismatch
        // deadline fires → Phase: EmergencyFlattening + counter bump.
        let mut cfg = live_test_cfg_leg_mismatch();
        cfg.dry_run = false;
        let (ext_seq, lt_seq) = breach_then_hold_sequence(70);
        let total = ext_seq.len() as u64;
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        ext_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        // Lighter: queue a single terminal-filled response for entry,
        // then default (zero non-terminal) on exit so the mismatch
        // deadline fires before Lighter's own fill_timeout_ms.
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.poll_fill.push_back(
                crate::trade::execution::venue_ops::ScriptedResponse::FillStatus(
                    OrderFillStatus {
                        filled_qty: dec!(1),
                        terminal: true,
                        cancelled: false,
                    },
                ),
            );
            // default_fill stays at OrderFillStatus::default() = zero
            // non-terminal so subsequent polls (the exit cycle) keep
            // returning "still in flight".
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, total).await;
        assert!(summary.decisions_enter_short >= 1);
        assert!(summary.decisions_exit >= 1);
        assert_eq!(
            machine.phase(),
            Phase::EmergencyFlattening,
            "leg mismatch on exit MUST route to EmergencyFlattening; \
             got phase={:?}",
            machine.phase()
        );
        assert!(open_qty.is_none());
        assert!(
            summary.live_exits_leg_mismatch >= 1,
            "leg-mismatch counter must increment; summary={:?}",
            summary
        );
        assert_eq!(summary.live_exits_failed_legs, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn live_exit_one_leg_failed_routes_to_emergency_flattening() {
        // Both legs terminate within the parallel-exit window but the
        // Lighter terminal is `Failed{Cancelled}`. Runner applies the
        // Extended fill, then Emergency to escalate. Distinct from
        // leg-mismatch: here both legs reported, but one with a
        // failure — `live_exits_failed_legs` is the relevant counter.
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let (ext_seq, lt_seq) = breach_then_hold_sequence(70);
        let total = ext_seq.len() as u64;
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        ext_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            // Entry: terminal-filled (queued — first pop).
            s.poll_fill.push_back(
                crate::trade::execution::venue_ops::ScriptedResponse::FillStatus(
                    OrderFillStatus {
                        filled_qty: dec!(1),
                        terminal: true,
                        cancelled: false,
                    },
                ),
            );
            // Exit: terminal-cancelled with zero fill (default) →
            // LighterTerminal::Failed{Cancelled}. ParallelExitLoop
            // returns `Both { ext: Filled, lt: Failed }`.
            s.default_fill = OrderFillStatus {
                filled_qty: Decimal::ZERO,
                terminal: true,
                cancelled: true,
            };
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, total).await;
        assert!(summary.decisions_enter_short >= 1);
        assert!(summary.decisions_exit >= 1);
        assert_eq!(
            machine.phase(),
            Phase::EmergencyFlattening,
            "Failed lighter exit terminal MUST route to EmergencyFlattening"
        );
        assert!(open_qty.is_none());
        assert!(
            summary.live_exits_failed_legs >= 1,
            "failed-legs counter must increment; summary={:?}",
            summary
        );
        assert_eq!(summary.live_exits_leg_mismatch, 0);
    }

    // ---------------------------------------------------------------
    // Sprint 4 step 3/3 — drive_emergency_flatten_round helper
    // ---------------------------------------------------------------

    /// Scripted LegStateReader for unit tests. Each call to
    /// `read_leg_qtys` pops the next entry; once drained it
    /// repeats the last value. This lets a test say "first read
    /// shows non-zero, after one close_all show zero, return
    /// Complete" the same way the emergency_loop unit tests do.
    struct ScriptedLegReader {
        seq: std::sync::Mutex<std::collections::VecDeque<crate::trade::execution::emergency_loop::LegQtys>>,
    }

    impl ScriptedLegReader {
        fn new(
            seq: Vec<crate::trade::execution::emergency_loop::LegQtys>,
        ) -> Self {
            assert!(!seq.is_empty(), "ScriptedLegReader needs ≥1 entry");
            Self {
                seq: std::sync::Mutex::new(seq.into()),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::trade::execution::emergency_loop::LegStateReader for ScriptedLegReader {
        async fn read_leg_qtys(
            &self,
        ) -> anyhow::Result<crate::trade::execution::emergency_loop::LegQtys> {
            let mut g = self.seq.lock().unwrap();
            if g.len() > 1 {
                Ok(g.pop_front().unwrap())
            } else {
                Ok(*g.front().expect("non-empty seq"))
            }
        }
    }

    /// LegStateReader that always returns Err. Distinct from
    /// `NoopLegStateReader` (which lives in `live_exec`) — placed
    /// here so the helper signature stays self-contained for tests.
    struct ErrLegReader;
    #[async_trait::async_trait]
    impl crate::trade::execution::emergency_loop::LegStateReader for ErrLegReader {
        async fn read_leg_qtys(
            &self,
        ) -> anyhow::Result<crate::trade::execution::emergency_loop::LegQtys> {
            Err(anyhow::anyhow!("scripted: reader error"))
        }
    }

    /// Helper: walk the position machine into `EmergencyFlattening`
    /// via a Lighter-fail-after-Extended sequence. Returns the
    /// machine ready to drive the emergency handler against.
    fn machine_in_emergency_flattening() -> PositionMachine {
        let mut m = PositionMachine::new();
        m.apply(
            0,
            Event::EntrySignal {
                direction: SpreadDirection::Long,
                notional_usd: dec!(50),
            },
        )
        .unwrap();
        m.apply(100, Event::ExtendedFilled { qty: dec!(0.025) })
            .unwrap();
        // LighterFailed in EnteringLighter → EmergencyFlattening per
        // state.rs's transition table (validated by the existing
        // `lighter_failed_in_entering_lighter_emergency_flattens` test).
        m.apply(200, Event::LighterFailed).unwrap();
        assert_eq!(m.phase(), Phase::EmergencyFlattening);
        m
    }

    /// StuckTripwire with a configurable kill threshold so a test can
    /// arm STUCK after exactly N reduce-only failures.
    fn test_stuck_with_kill_threshold(
        kill_threshold: u32,
    ) -> (tempfile::TempDir, crate::risk::kill_switch::StuckTripwire) {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = crate::risk::kill_switch::StuckTripwireConfig {
            stuck_file: dir.path().join("STUCK"),
            rest_consec_fail_to_escalate: 3,
            reduce_only_consec_fail_to_kill: kill_threshold,
        };
        (
            dir,
            crate::risk::kill_switch::StuckTripwire::new_for_test(cfg),
        )
    }

    fn live_with_leg_reader(
        cfg: &XvenueConfig,
        ext: Arc<ScriptedVenueOps>,
        lt: Arc<ScriptedVenueOps>,
        reader: Arc<dyn crate::trade::execution::emergency_loop::LegStateReader>,
    ) -> LiveExecution {
        let ext_dyn: Arc<dyn VenueOps> = ext;
        let lt_dyn: Arc<dyn VenueOps> = lt;
        LiveExecution::from_config(cfg, ext_dyn, lt_dyn)
            .expect("live exec from cfg")
            .with_leg_reader(reader)
    }

    use crate::trade::execution::emergency_loop::LegQtys;

    #[tokio::test]
    async fn emergency_flatten_round_completes_when_both_legs_zero() {
        let mut machine = machine_in_emergency_flattening();
        let ext = Arc::new(ScriptedVenueOps::new());
        let lt = Arc::new(ScriptedVenueOps::new());
        let reader: Arc<dyn crate::trade::execution::emergency_loop::LegStateReader> =
            Arc::new(ScriptedLegReader::new(vec![LegQtys {
                ext: Decimal::ZERO,
                lt: Decimal::ZERO,
            }]));
        let cfg = live_test_cfg();
        let live = live_with_leg_reader(&cfg, ext.clone(), lt.clone(), reader);
        let mut open_qty = Some(dec!(0.025));
        let (_st_dir, mut stuck) = test_stuck();
        let mut summary = LivePaperSummary::default();
        let mut last = None;
        let mut attempts = 0;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            1_000,
        )
        .await
        .unwrap();

        assert_eq!(machine.phase(), Phase::Flat);
        assert!(open_qty.is_none(), "EmergencyComplete must clear open_qty");
        assert_eq!(summary.emergency_completes, 1);
        assert_eq!(summary.emergency_close_all_failures, 0);
        assert!(
            ext.snapshot_close_alls().is_empty(),
            "no close_all needed when legs already zero"
        );
        assert!(lt.snapshot_close_alls().is_empty());
    }

    #[tokio::test]
    async fn emergency_flatten_round_calls_close_all_on_nonzero_legs() {
        let mut machine = machine_in_emergency_flattening();
        let ext = Arc::new(ScriptedVenueOps::new());
        let lt = Arc::new(ScriptedVenueOps::new());
        let reader: Arc<dyn crate::trade::execution::emergency_loop::LegStateReader> =
            Arc::new(ScriptedLegReader::new(vec![LegQtys {
                ext: dec!(0.01),
                lt: dec!(0.01),
            }]));
        let cfg = live_test_cfg();
        let live = live_with_leg_reader(&cfg, ext.clone(), lt.clone(), reader);
        let mut open_qty = Some(dec!(0.01));
        let (_st_dir, mut stuck) = test_stuck();
        let mut summary = LivePaperSummary::default();
        let mut last = None;
        let mut attempts = 0;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            1_000,
        )
        .await
        .unwrap();

        assert_eq!(
            machine.phase(),
            Phase::EmergencyFlattening,
            "still flattening — close_all returned Ok but legs read non-zero"
        );
        assert_eq!(ext.snapshot_close_alls().len(), 1);
        assert_eq!(lt.snapshot_close_alls().len(), 1);
        assert_eq!(summary.emergency_completes, 0);
        assert_eq!(summary.emergency_close_all_failures, 0);
        assert_eq!(attempts, 1);
        assert_eq!(last, Some(1_000));
    }

    #[tokio::test]
    async fn emergency_flatten_round_skips_zero_leg_close_all() {
        // Defensive: only Lighter has open qty → close_all only on
        // Lighter, not Extended. Mirrors emergency_loop's case 13
        // partial test.
        let mut machine = machine_in_emergency_flattening();
        let ext = Arc::new(ScriptedVenueOps::new());
        let lt = Arc::new(ScriptedVenueOps::new());
        let reader: Arc<dyn crate::trade::execution::emergency_loop::LegStateReader> =
            Arc::new(ScriptedLegReader::new(vec![LegQtys {
                ext: Decimal::ZERO,
                lt: dec!(0.01),
            }]));
        let cfg = live_test_cfg();
        let live = live_with_leg_reader(&cfg, ext.clone(), lt.clone(), reader);
        let mut open_qty = None;
        let (_st_dir, mut stuck) = test_stuck();
        let mut summary = LivePaperSummary::default();
        let mut last = None;
        let mut attempts = 0;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            1_000,
        )
        .await
        .unwrap();

        assert!(
            ext.snapshot_close_alls().is_empty(),
            "Extended already zero — must not be touched"
        );
        assert_eq!(lt.snapshot_close_alls().len(), 1);
    }

    #[tokio::test]
    async fn emergency_flatten_round_throttles_within_retry_interval() {
        let mut machine = machine_in_emergency_flattening();
        let ext = Arc::new(ScriptedVenueOps::new());
        let lt = Arc::new(ScriptedVenueOps::new());
        let reader: Arc<dyn crate::trade::execution::emergency_loop::LegStateReader> =
            Arc::new(ScriptedLegReader::new(vec![LegQtys {
                ext: dec!(0.01),
                lt: dec!(0.01),
            }]));
        let cfg = live_test_cfg();
        // emergency_retry_interval_ms defaults to 30_000.
        let live = live_with_leg_reader(&cfg, ext.clone(), lt.clone(), reader);
        let mut open_qty = None;
        let (_st_dir, mut stuck) = test_stuck();
        let mut summary = LivePaperSummary::default();
        // Pretend we already did one round at t=1000.
        let mut last = Some(1_000_u64);
        let mut attempts = 1u32;

        // Call at t=10_000 (only 9 s elapsed, < 30 s retry interval).
        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            10_000,
        )
        .await
        .unwrap();

        assert!(
            ext.snapshot_close_alls().is_empty(),
            "throttle must suppress close_all"
        );
        assert_eq!(attempts, 1, "throttled call must not bump attempt counter");
        assert_eq!(last, Some(1_000), "throttled call must not advance last");

        // Call at t=40_000 (39 s elapsed > 30 s) — now fires.
        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            40_000,
        )
        .await
        .unwrap();
        assert_eq!(ext.snapshot_close_alls().len(), 1);
        assert_eq!(attempts, 2);
        assert_eq!(last, Some(40_000));
    }

    #[tokio::test]
    async fn emergency_flatten_round_arms_stuck_after_kill_threshold_failures() {
        let mut machine = machine_in_emergency_flattening();
        let ext = Arc::new(ScriptedVenueOps::new());
        // Five close_all rejections — equal to the kill threshold.
        ext.with_state(|s| {
            for _ in 0..5 {
                s.close_all.push_back(
                    crate::trade::execution::venue_ops::ScriptedResponse::Err(
                        "reduce-only rejected".into(),
                    ),
                );
            }
        });
        let lt = Arc::new(ScriptedVenueOps::new());
        let reader: Arc<dyn crate::trade::execution::emergency_loop::LegStateReader> =
            Arc::new(ScriptedLegReader::new(vec![LegQtys {
                ext: dec!(0.01),
                lt: Decimal::ZERO,
            }]));
        let cfg = live_test_cfg();
        let live = live_with_leg_reader(&cfg, ext.clone(), lt.clone(), reader);
        let mut open_qty = None;
        let (_st_dir, mut stuck) = test_stuck_with_kill_threshold(5);
        let mut summary = LivePaperSummary::default();
        let mut last = None;
        let mut attempts = 0;

        // Drive 5 rounds, 60 s apart — well over the 30 s throttle.
        for i in 0..5 {
            drive_emergency_flatten_round(
                &live,
                &mut machine,
                &mut open_qty,
                &mut stuck,
                &mut summary,
                &mut last,
                &mut attempts,
                (i as u64) * 60_000,
            )
            .await
            .unwrap();
            // Stop iterating once stuck is armed — the helper itself
            // short-circuits on subsequent calls.
            if stuck.is_stuck() {
                break;
            }
        }

        assert!(stuck.is_stuck(), "STUCK file must be armed after 5 failures");
        assert_eq!(summary.emergency_close_all_failures, 5);
        assert_eq!(summary.emergency_stuck_armed, 1);

        // Sanity: a 6th call after STUCK is armed must not attempt
        // close_all (operator must clear before retries resume).
        let prior = ext.snapshot_close_alls().len();
        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            10 * 60_000,
        )
        .await
        .unwrap();
        assert_eq!(
            ext.snapshot_close_alls().len(),
            prior,
            "no close_all attempts after STUCK is armed"
        );
    }

    #[tokio::test]
    async fn emergency_flatten_round_skips_when_leg_reader_errors() {
        let mut machine = machine_in_emergency_flattening();
        let ext = Arc::new(ScriptedVenueOps::new());
        let lt = Arc::new(ScriptedVenueOps::new());
        let reader: Arc<dyn crate::trade::execution::emergency_loop::LegStateReader> =
            Arc::new(ErrLegReader);
        let cfg = live_test_cfg();
        let live = live_with_leg_reader(&cfg, ext.clone(), lt.clone(), reader);
        let mut open_qty = None;
        let (_st_dir, mut stuck) = test_stuck();
        let mut summary = LivePaperSummary::default();
        let mut last = None;
        let mut attempts = 0;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            1_000,
        )
        .await
        .unwrap();

        // Reader Err → handler logs and tries again next round; no
        // close_all attempts, no EmergencyComplete.
        assert!(ext.snapshot_close_alls().is_empty());
        assert!(lt.snapshot_close_alls().is_empty());
        assert_eq!(summary.emergency_completes, 0);
        assert_eq!(machine.phase(), Phase::EmergencyFlattening);
        assert_eq!(last, Some(1_000), "throttle clock must still advance");
        assert_eq!(attempts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn live_exit_paper_path_unchanged_when_dry_run_true() {
        // With dry_run=true the synthetic exit path runs even when a
        // LiveExecution is provided. Mirrors the entry-side guard.
        let mut cfg = live_test_cfg();
        cfg.dry_run = true;
        let (ext_seq, lt_seq) = breach_then_hold_sequence(70);
        let total = ext_seq.len() as u64;
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) =
            drive_live_ticks(&cfg, &*hub, &live, total).await;
        assert!(summary.decisions_enter_short >= 1);
        assert!(summary.decisions_exit >= 1);
        assert_eq!(
            machine.phase(),
            Phase::Flat,
            "paper exit path lands in Flat via synthetic fills"
        );
        assert!(open_qty.is_none());
        assert!(
            ext_vops.snapshot_takers().is_empty(),
            "dry_run must not dispatch real orders even on exit"
        );
        assert!(lt_vops.snapshot_takers().is_empty());
    }
}
