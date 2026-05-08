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
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use super::config::XvenueConfig;
use super::live_exec::LiveExecution;
use super::signal::{
    effective_dev_bps, Decision, ExitReason, PositionSummary, SignalEngine, SpreadDirection,
};
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
use crate::trade::execution::lighter_maker::{LighterMakerLoop, LighterMakerRequest};
use crate::trade::execution::parallel_exit::{ParallelExitLoop, ParallelExitOutcome};
use crate::trade::execution::types::{ExecutionFailure, ExtendedTerminal, LighterTerminal};

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

#[derive(Debug, Clone, Default)]
pub struct MidSnapshot {
    pub ts_ms: u64,
    pub mid: Decimal,
    /// `false` when the top-of-book has zero size on one side. Spread
    /// engine drops these (see bt.rs zero-size filter rationale,
    /// bot-strategy#166 part 1).
    pub book_ok: bool,
    /// Top-of-book bid/ask + sizes. Added per bot-strategy#309 to enable
    /// touch-to-touch signal (`cap_long = lt_bid - ext_ask`, etc.) and
    /// book-depth filtering for the maker-on-Lighter redesign. Test
    /// constructors that pre-date this leave them as `Decimal::ZERO`
    /// via `Default` — the [STATUS] emitter handles zero gracefully.
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_size: Decimal,
    pub ask_size: Decimal,
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
    /// Touch-to-touch capturable spread + book depth, snapshotted per
    /// tick. Added per bot-strategy#309 to enable maker-on-Lighter
    /// redesign verification during DRY_RUN soak. None until the
    /// upstream connector returns book depth (production live, not
    /// scripted-hub tests). The [STATUS] emitter prints `None` for any
    /// missing value; analysis tools can grep for `cap_long=` and
    /// related fields once they appear.
    pub last_cap_long_bps: Option<f64>,
    pub last_cap_short_bps: Option<f64>,
    pub last_ext_inside_bps: Option<f64>,
    pub last_lt_inside_bps: Option<f64>,
    pub last_lt_bid_size: Option<f64>,
    pub last_lt_ask_size: Option<f64>,
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
    /// bot-strategy#317: count of `Decision::Enter` outcomes blocked
    /// because Extended is in maintenance (or a declared maintenance
    /// window starts within 1 hour). Mirrors pairtrade's
    /// `is_upcoming_maintenance(1)` gate. Without this, every entry
    /// in maintenance burns one `Maintenance mode` REST round-trip on
    /// Extended; the gate replaces that with one cheap connector-side
    /// flag check per tick.
    pub entries_blocked_by_maintenance: u64,
    /// bot-strategy#309 step 4: count of `Decision::Enter` outcomes
    /// dropped because the Lighter side we'd post on already has more
    /// than `lt_book_max_eth` size at touch (we'd be too deep in queue
    /// for the maker fill premise to hold). 0 when the filter is
    /// disabled (`lt_book_max_eth: None`).
    pub entries_blocked_by_book_depth: u64,
    /// bot-strategy#309 step 5: would-be maker fill telemetry, only
    /// populated in paper-mode (dry_run + no live executor). Each
    /// `Decision::Enter` increments `attempts`, the depth-conditional
    /// fill model contributes its sampled outcome to `fills`, and the
    /// raw probability accumulates in `p_sum` so the [STATUS] line can
    /// report `p_sum / attempts` as the running mean. Phase 0 exit
    /// gate: `fills / attempts ≥ 0.5`.
    pub would_be_maker_attempts: u64,
    pub would_be_maker_fills: u64,
    pub would_be_maker_p_sum: f64,
    /// bot-strategy#330: would-be exit-side maker fill telemetry.
    /// Mirror of the entry-side counters above — populated only in
    /// paper-mode, on every `Decision::Exit` that consumes a paper
    /// position. Each exit increments `attempts`; the depth-conditional
    /// model run against the *opposite* side of the Lighter book (a
    /// Long position closes by buying back at bid; a Short closes at
    /// ask) contributes the sampled outcome to `fills` and the raw
    /// probability to `p_sum`. The exit-gate from #330 acceptance
    /// criteria reads off `wb_exit_fill_rate`.
    pub would_be_maker_exit_attempts: u64,
    pub would_be_maker_exit_fills: u64,
    pub would_be_maker_exit_p_sum: f64,
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
    /// Number of times the runner forced a flatten because the
    /// session-DD halt latched while a position was open (#268
    /// S5-3). Idempotency-guarded by the position machine's phase
    /// check, so this counts trips, not ticks the latch stays armed.
    pub live_session_dd_forced_flattens: u64,
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
    /// USD PnL of the most recent live exit cycle (`Decision::Exit`
    /// + `ParallelExitOutcome::Both { Filled, Filled }`). Computed
    /// via [`compute_realised_pnl`] from entry/exit mids and
    /// per-venue fee rates. `None` until the first live round-trip
    /// completes; failed-exit / leg-mismatch cycles record `None`
    /// rather than overwriting with zero.
    pub last_realised_pnl_usd: Option<f64>,
    /// Per-venue cumulative count of post-warmup `read_mid` errors
    /// (bot-strategy#303). Each increment corresponds to one
    /// `[XVENUE] tick error: read_mid {Venue}` WARN line. Pre-warmup
    /// errors are debug-demoted and do not count. Surfaced in
    /// `[STATUS]` so recurrence is visible without grepping logs.
    pub read_mid_err_ext: u64,
    pub read_mid_err_lt: u64,
}

/// Snapshot of an open live position needed to compute realised PnL
/// at exit time (#268 S5-1). Populated in `Decision::Enter`'s live
/// happy path once both legs have terminal-filled; consumed in the
/// `Decision::Exit` `Both { Filled, Filled }` branch to feed
/// [`compute_realised_pnl`]. Failure paths (Lighter-fail-after-ext,
/// Both with at least one Failed, LegMismatchTimeout) consume the
/// ctx via `take()` without computing PnL — partial-fill PnL is
/// out of scope for Phase 3 (see #268 S5-1 'Out of scope').
#[derive(Debug, Clone)]
pub struct LiveEntryCtx {
    pub direction: SpreadDirection,
    /// Per-venue mid at the time the entry leg landed. Mid-based
    /// approximation — actual fill prices would be slightly worse
    /// (post-only at touch / taker crosses spread) but the
    /// difference is absorbed into the fee rate (default 5 bps
    /// covers typical maker/taker mix). Replace with real fill
    /// prices in a Sprint 6 once the executor surfaces them.
    pub ext_entry_mid: Decimal,
    pub lt_entry_mid: Decimal,
    pub ext_entry_qty: Decimal,
    pub lt_entry_qty: Decimal,
}

/// Realised USD PnL for one live round-trip (#268 S5-1).
///
/// Gross: spread-direction-aware delta times the smaller of the two
/// exit fill qtys (the truly delta-neutral portion of the round
/// trip). For SpreadDirection::Long the position profits when the
/// spread widens (`exit_spread > entry_spread`); Short is symmetric.
///
/// Fees: per-leg, per-side. Each leg's notional is `mid * qty`;
/// the fee rate (`*_fee_bps`) applies to that notional. Entry +
/// exit fees on both venues sum into the total.
///
/// Mid-based pricing is an approximation — actual fill prices on
/// Lighter taker can be a tick worse than mid, and Extended
/// post-only fills happen at the touch which is already at mid
/// (zero spread cost). The fee-rate default (5 bps) is set
/// conservatively to absorb this approximation.
pub fn compute_realised_pnl(
    direction: SpreadDirection,
    ext_entry_mid: Decimal,
    lt_entry_mid: Decimal,
    ext_exit_mid: Decimal,
    lt_exit_mid: Decimal,
    ext_entry_qty: Decimal,
    lt_entry_qty: Decimal,
    ext_exit_qty: Decimal,
    lt_exit_qty: Decimal,
    ext_fee_bps: f64,
    lt_fee_bps: f64,
) -> Decimal {
    let realised_qty = ext_exit_qty.min(lt_exit_qty);
    if realised_qty <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let entry_spread = ext_entry_mid - lt_entry_mid;
    let exit_spread = ext_exit_mid - lt_exit_mid;
    let gross = match direction {
        SpreadDirection::Long => (exit_spread - entry_spread) * realised_qty,
        SpreadDirection::Short => (entry_spread - exit_spread) * realised_qty,
    };
    let bps_div = Decimal::new(10_000, 0);
    let ext_rate = Decimal::from_f64_retain(ext_fee_bps).unwrap_or(Decimal::ZERO) / bps_div;
    let lt_rate = Decimal::from_f64_retain(lt_fee_bps).unwrap_or(Decimal::ZERO) / bps_div;
    let ext_fees = (ext_entry_mid * ext_entry_qty + ext_exit_mid * ext_exit_qty) * ext_rate;
    let lt_fees = (lt_entry_mid * lt_entry_qty + lt_exit_mid * lt_exit_qty) * lt_rate;
    gross - ext_fees - lt_fees
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
    let mut risk_manager = RiskManager::new(cfg.risk_config(), cfg.agent_name.clone());
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
    // bot-strategy#287: timestamp of the FIRST 'both legs zero'
    // read since entering EmergencyFlattening. Used by the
    // post-fill grace window so a stale-zero read seconds after a
    // confirmed fill doesn't trip a false EmergencyComplete.
    let mut first_emergency_zero_ms: Option<u64> = None;
    // Entry-time mids + qtys captured at Decision::Enter happy
    // landing for the realised-PnL helper at Decision::Exit (#268
    // S5-1). Cleared whenever the position machine returns to Flat
    // — covers normal exit (already taken inside Decision::Exit),
    // EmergencyComplete (drive_emergency_flatten_round), and any
    // operator-driven Reset. Stays None in paper mode.
    let mut live_entry_ctx: Option<LiveEntryCtx> = None;

    // Drop an initial snapshot so the dashboard sees the DRY_RUN pill /
    // agent identity on boot instead of waiting for the first
    // status_log_interval_ms (60 s default). Equity is best-effort —
    // the first read may be Err while the WS is still warming, in
    // which case PnL stays at zero until the next tick.
    if let Some(r) = reporter.as_mut() {
        refresh_equity(&*hub, r, &mut risk_manager).await;
        r.mark_dirty();
        publish_risk(&risk_manager, r);
        publish_kill_switch(&cfg, r);
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
                    &mut live_entry_ctx,
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
                handle_emergency_flatten_tick(
                    &cfg,
                    live_exec.as_deref(),
                    &mut machine,
                    &mut open_qty,
                    &mut stuck,
                    &mut summary,
                    &mut last_emergency_attempt_ms,
                    &mut emergency_attempts,
                    &mut first_emergency_zero_ms,
                    reporter.as_mut(),
                    &mut risk_manager,
                )
                .await;

                // Drop a stale live_entry_ctx whenever the position
                // machine has returned to Flat without a corresponding
                // Decision::Exit Both{Filled,Filled} consume — covers
                // EmergencyComplete (post-flatten cleanup), operator
                // Reset, and any future failure path that lands in
                // Flat. Idempotent if ctx is already None.
                if matches!(machine.phase(), super::state::Phase::Flat) {
                    live_entry_ctx = None;
                }
            }

            _ = status_ivl.tick() => {
                report_status_tick(
                    &cfg,
                    &*hub,
                    &summary,
                    &ws_health,
                    &machine,
                    &mut risk_manager,
                    reporter.as_mut(),
                ).await;
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
    reporter.set_circuit_breaker(Some(risk_manager.circuit_breaker_snapshot(now_unix_secs())));
    reporter.set_risk_history(risk_manager.risk_history());
}

/// Refresh `kill_switch_active` on the reporter so the dashboard's
/// `kill_switch_active` field stays current without an SSM probe (#343).
/// Called once per tick from each `write_snapshot_if_due` call site so
/// the reporter sees the same state the live `kill_switch_active()`
/// gate sees inside `gate_decision`.
fn publish_kill_switch(cfg: &XvenueConfig, reporter: &mut StatusReporter) {
    reporter.set_kill_switch(kill_switch_active(&cfg.kill_switch_file));
}

/// Evaluate the WS staleness latch and, when a position is open and
/// the venue stream went stale, route the position machine into
/// `EmergencyFlattening`. Returns `true` when the caller should bail
/// out of the rest of the tick (`return Ok(())`); `false` means
/// continue. Three outcomes map to:
///   - `WsHealthOutcome::Stale` + position open  → emergency apply,
///     return `true`.
///   - `WsHealthOutcome::Stale` + flat           → debug-log, return
///     `false` (the spread engine's `book_ok` filter is the entry
///     gate; flat has nothing to flatten).
///   - non-Stale                                 → return `false`.
///
/// Behaviour-preserving: identical log lines, identical paper-mode
/// EmergencyComplete synthesis (live mode must not take that path),
/// identical summary counter increment.
fn handle_ws_stale_emergency(
    cfg: &XvenueConfig,
    ws_health: &mut WsHealthMonitor,
    machine: &mut PositionMachine,
    open_qty: &mut Option<Decimal>,
    summary: &mut LivePaperSummary,
    now_wall_ms: u64,
) -> bool {
    let WsHealthOutcome::Stale(stale_venue) = ws_health.evaluate(now_wall_ms) else {
        return false;
    };
    if machine.summary().is_none() {
        log::debug!(
            "[XVENUE] WS staleness in Flat: venue={:?} threshold_ms={} \
             (no position to flatten — continuing)",
            stale_venue,
            ws_health.ws_stale_emergency_ms()
        );
        // Don't increment entries_blocked_by_ws_stale — Flat doesn't
        // block anything; the spread engine's book_ok filter is what
        // gates entries on bad data.
        return false;
    }

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
            // emergency-flatten loop) is not yet wired, so there is no
            // producer of `EmergencyComplete` in dry-run. Without
            // this, a transient WS hiccup would dead-end the state
            // machine in EmergencyFlattening. Synthesise the exit
            // fills + EmergencyComplete so the paper loop recovers.
            // Live mode (Group B once it lands) MUST NOT take this
            // path.
            if cfg.dry_run {
                if let Some(qty) = open_qty.take() {
                    let _ = machine.apply(now_wall_ms, Event::ExtendedExitFilled { qty });
                    let _ = machine.apply(now_wall_ms, Event::LighterExitFilled { qty });
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
    true
}

/// Apply the four short-circuit gates that can downgrade a
/// `Decision::Enter` to `Decision::Hold` before it reaches the state
/// machine: KILL_SWITCH (#244 D-1), STUCK file (#244 C / #102 P2),
/// phase gate (signal can re-emit Enter while the machine is in
/// EnteringX / Exiting / EmergencyFlattening; the resulting
/// EntrySignal would be rejected and panic), and risk gates (#244
/// D-2..D-7: daily DD / session DD / circuit breaker).
///
/// Order is meaningful and preserved from the original inline form:
/// KILL_SWITCH first so an operator pause doesn't burn a risk-gate
/// counter, then STUCK, then phase, then risk. Each gate increments
/// its own `summary.entries_blocked_by_*` counter on a block.
///
/// Behaviour-preserving: log lines, counter increments, and the
/// short-circuit chain are byte-identical to the prior 4-block form.
fn apply_entry_gates(
    cfg: &XvenueConfig,
    decision: &mut Decision,
    risk_manager: &RiskManager,
    stuck: &StuckTripwire,
    machine: &PositionMachine,
    summary: &mut LivePaperSummary,
    dev: Option<f64>,
) {
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
        *decision = Decision::Hold;
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
        *decision = Decision::Hold;
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
        && !matches!(machine.phase(), super::state::Phase::Flat)
    {
        log::debug!(
            "[XVENUE] Decision::Enter suppressed: phase={:?} (not Flat)",
            machine.phase(),
        );
        *decision = Decision::Hold;
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
            *decision = Decision::Hold;
        }
    }
}

/// bot-strategy#309 step 4: queue-depth filter for the maker-on-Lighter
/// redesign. Returns `true` when the entry should be blocked because
/// the Lighter side we would post on has more than `max_eth` size at
/// touch. Returns `false` when the filter is disabled (`max_eth =
/// None`), when the relevant size is zero/missing (no book → can't
/// evaluate, fall through), or when the size is within budget.
///
/// Long entry posts on Lighter ASK (we're the seller) so checks
/// `lt_ask_size`. Short entry posts on Lighter BID so checks
/// `lt_bid_size`. Mirrors the BT redesign cell that demonstrated
/// net +$47.64 over 5.28d at $50 notional with `lt_book_max=2 ETH`.
fn book_depth_blocks_entry(
    dir: SpreadDirection,
    lt_snap: &MidSnapshot,
    max_eth: Option<f64>,
) -> bool {
    let max = match max_eth {
        Some(m) => m,
        None => return false,
    };
    let size = match dir {
        SpreadDirection::Long => lt_snap.ask_size,
        SpreadDirection::Short => lt_snap.bid_size,
    };
    let size_f = match size.to_f64() {
        Some(s) if s.is_finite() => s,
        _ => return false,
    };
    // size==0 indicates an empty side; fall through rather than block,
    // mirroring the existing book_ok semantics where a zero-size side
    // is handled upstream by the spread engine, not here.
    if size_f <= 0.0 {
        return false;
    }
    size_f > max
}

/// bot-strategy#309 step 5: would-be Lighter maker fill outcome for
/// DRY_RUN soak telemetry. Models a single Bernoulli draw:
///   `p = clamp_to_unit(1 - our_size / depth_at_touch)`
/// with the queue side picked from the entry direction (Long → ask,
/// Short → bid). The model is intentionally simple — soak's job is to
/// feed the post-hoc analyst raw `(direction, size, depth)` tuples in
/// the log; this in-process draw is just a single-glance fill rate so
/// the operator can confirm the Phase 0 ≥ 50% gate without having to
/// re-derive it from logs every time.
///
/// Returns `None` when sizing or book is unavailable (caller should
/// skip the telemetry but still count the would-be attempt).
///
/// `seed` is mixed into the RNG for reproducibility — production
/// callers pass `now_ts_ms` so re-running analysis on a logged tuple
/// yields the same draw.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WouldBeMakerOutcome {
    pub depth_eth: f64,
    pub our_size_eth: f64,
    pub fill_p: f64,
    pub sampled_fill: bool,
}

fn would_be_maker_fill_outcome(
    dir: SpreadDirection,
    our_size: Decimal,
    lt_snap: &MidSnapshot,
    seed: u64,
) -> Option<WouldBeMakerOutcome> {
    let our_size_eth = our_size.to_f64().filter(|s| s.is_finite() && *s > 0.0)?;
    let depth = match dir {
        SpreadDirection::Long => lt_snap.ask_size,
        SpreadDirection::Short => lt_snap.bid_size,
    };
    let depth_eth = depth.to_f64().filter(|d| d.is_finite() && *d > 0.0)?;
    // Linear-decay-by-depth model: p = max(0, 1 - our_size / depth).
    // Bounded to [0, 1] so noisy book reads don't propagate as a >1
    // probability into the draw.
    let raw = 1.0 - (our_size_eth / depth_eth);
    let fill_p = raw.clamp(0.0, 1.0);
    // Deterministic sample from a per-decision seed. Using StdRng so
    // tests + post-hoc analysis can replay an exact log line and get
    // the same sampled_fill outcome the live bot recorded.
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let draw: f64 = rng.gen();
    let sampled_fill = draw < fill_p;
    Some(WouldBeMakerOutcome {
        depth_eth,
        our_size_eth,
        fill_p,
        sampled_fill,
    })
}

/// S5-3 forced flatten on session DD (#268). Runs *before* signal.decide
/// so a HALTed position routes to `EmergencyFlattening` rather than
/// letting the strategy decide a natural exit (which could chase for
/// seconds while the position bleeds). Idempotent via the state
/// machine's phase gate: once applied, machine.phase() leaves Held →
/// subsequent ticks see the Emergency apply rejected and fall through
/// without flipping `position`.
///
/// Live mode only — paper synthesises EmergencyComplete in the
/// existing dry_run short-circuits in `handle_ws_stale_emergency` /
/// `handle_skew_breach_emergency`. `position` is refreshed in place on
/// successful apply so signal.decide further down sees the post-
/// Emergency state instead of the stale `Some(_)` (which would race
/// signal.decide into a Decision::Exit and then get rejected by the
/// state machine).
#[allow(clippy::too_many_arguments)]
fn force_flatten_on_session_dd_halt(
    cfg: &XvenueConfig,
    risk_manager: &RiskManager,
    machine: &mut PositionMachine,
    summary: &mut LivePaperSummary,
    live_exec: Option<&LiveExecution>,
    position: &mut Option<PositionSummary>,
    now_ts_ms: u64,
) {
    if position.is_none() || cfg.dry_run || live_exec.is_none() {
        return;
    }
    if !matches!(
        risk_manager.block_reason(now_unix_secs()),
        Some(BlockReason::SessionDdHalted)
    ) {
        return;
    }
    log::error!(
        "[XVENUE] session_dd halt while position held → forced flatten \
         (machine→EmergencyFlattening; emergency_loop drives close_all)"
    );
    let event = Event::Emergency {
        reason: super::state::EmergencyReason::SessionDdHalted,
    };
    match machine.apply(now_ts_ms, event) {
        Ok(()) => {
            summary.live_session_dd_forced_flattens += 1;
            // Refresh the cached `position` snapshot — signal.decide()
            // further down would otherwise see the pre-Emergency
            // Some(_) and could fire Decision::Exit, which the state
            // machine then rejects with `ExitSignal in
            // EmergencyFlattening`.
            *position = machine.summary();
        }
        Err(e) => {
            // Already in EmergencyFlattening (idempotent re-fire) or
            // other transient — debug-log and continue.
            log::debug!("[XVENUE] forced-flatten Emergency rejected: {:?}", e);
        }
    }
}

/// Reference guard cross-check (#244 C). Reads the latest Binance 1m
/// mid and suppresses each venue's `book_ok` when its mid drifts past
/// `reference_max_dev_bps` for `reference_consec_buckets_for_halt`
/// consecutive minutes. Mirrors the BT pre-filter so live and BT see
/// the same suppression behaviour on stuck quotes. The
/// `ToPrimitive::to_f64` step can fail in theory for absurdly-large
/// Decimals — when it does, the guard is silently skipped (matching
/// the prior `if let (Some, Some) = ...` shape).
async fn apply_reference_guard(
    reference_guard: &mut ReferenceGuard,
    ext_snap: &mut MidSnapshot,
    lt_snap: &mut MidSnapshot,
    summary: &mut LivePaperSummary,
) {
    let ref_state = reference_guard.current_reference().await;
    let (Some(ext_mid_f), Some(lt_mid_f)) = (
        rust_decimal::prelude::ToPrimitive::to_f64(&ext_snap.mid),
        rust_decimal::prelude::ToPrimitive::to_f64(&lt_snap.mid),
    ) else {
        return;
    };
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

/// Inventory-skew watchdog mirror of `handle_ws_stale_emergency`.
/// Computes the position's current skew_usd, asks the skew monitor
/// whether it has breached, and on breach routes the position machine
/// into `EmergencyFlattening`. Returns `true` when the caller should
/// bail out of the rest of the tick. Caller is expected to gate on
/// `position.is_some()` first — this helper trusts that gate so the
/// skew_monitor never sees the synthetic 0-skew of a flat machine.
///
/// Behaviour-preserving: same log lines, same paper-mode synth, same
/// summary counter increment as the prior inline form.
#[allow(clippy::too_many_arguments)]
fn handle_skew_breach_emergency(
    cfg: &XvenueConfig,
    skew_monitor: &mut SkewMonitor,
    machine: &mut PositionMachine,
    open_qty: &mut Option<Decimal>,
    summary: &mut LivePaperSummary,
    ext_mid: Decimal,
    lt_mid: Decimal,
    now_ts_ms: u64,
) -> bool {
    let skew_dec = machine.inventory_skew_usd(ext_mid, lt_mid);
    let skew_f = rust_decimal::prelude::ToPrimitive::to_f64(&skew_dec).unwrap_or(0.0);
    let SkewOutcome::Breach {
        skew_usd,
        threshold_usd,
    } = skew_monitor.evaluate(skew_f)
    else {
        return false;
    };

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

            // Same paper-mode short-circuit as ws_health — Group B
            // will replace this with real flatten orders driving
            // EmergencyComplete.
            if cfg.dry_run {
                if let Some(qty) = open_qty.take() {
                    let _ = machine.apply(now_ts_ms, Event::ExtendedExitFilled { qty });
                    let _ = machine.apply(now_ts_ms, Event::LighterExitFilled { qty });
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
    true
}

/// Read the current mid from both venues, gated by the per-venue
/// warm-up latch. Returns `Ok(None)` when either venue is still
/// warming up — the caller should treat this as "skip the rest of
/// this tick" exactly like the previous inline `return Ok(())`. A
/// hard error propagates as `Err`. On success the warm-up latch is
/// flipped on for the corresponding venue.
async fn read_both_mids<H: VenueHub + ?Sized>(
    hub: &H,
    warmup: &mut VenueWarmup,
    summary: &mut LivePaperSummary,
) -> Result<Option<(MidSnapshot, MidSnapshot)>> {
    let ext_snap = match hub.read_mid(Venue::Extended).await {
        Ok(s) => {
            warmup.mark_ready(Venue::Extended);
            s
        }
        Err(e) if !warmup.is_ready(Venue::Extended) => {
            log::debug!("[XVENUE] read_mid Extended pending (WS warm-up): {:?}", e);
            return Ok(None);
        }
        Err(e) => {
            summary.read_mid_err_ext += 1;
            return Err(e).context("read_mid Extended");
        }
    };
    let lt_snap = match hub.read_mid(Venue::Lighter).await {
        Ok(s) => {
            warmup.mark_ready(Venue::Lighter);
            s
        }
        Err(e) if !warmup.is_ready(Venue::Lighter) => {
            log::debug!("[XVENUE] read_mid Lighter pending (WS warm-up): {:?}", e);
            return Ok(None);
        }
        Err(e) => {
            summary.read_mid_err_lt += 1;
            return Err(e).context("read_mid Lighter");
        }
    };
    Ok(Some((ext_snap, lt_snap)))
}

/// Live-mode emergency-flatten round driver invoked from the tick arm
/// of `run_paper_loop`. Behaviour-preserving wrapper around
/// `drive_emergency_flatten_round` that also (a) resets the throttle
/// state when the position machine is *not* in EmergencyFlattening, and
/// (b) fires the bot-strategy#288 Action B/C `record_close` placeholder
/// when an EmergencyComplete just landed. Skips entirely when
/// `live_exec` is None (paper-mode loops have no orders to flatten).
#[allow(clippy::too_many_arguments)]
async fn handle_emergency_flatten_tick(
    cfg: &XvenueConfig,
    live_exec: Option<&LiveExecution>,
    machine: &mut PositionMachine,
    open_qty: &mut Option<Decimal>,
    stuck: &mut StuckTripwire,
    summary: &mut LivePaperSummary,
    last_emergency_attempt_ms: &mut Option<u64>,
    emergency_attempts: &mut u32,
    first_emergency_zero_ms: &mut Option<u64>,
    reporter: Option<&mut StatusReporter>,
    risk_manager: &mut RiskManager,
) {
    let Some(live) = live_exec else {
        return;
    };
    if cfg.dry_run || !matches!(machine.phase(), super::state::Phase::EmergencyFlattening) {
        // Reset throttle state so the *next* entry into
        // EmergencyFlattening starts fresh.
        *last_emergency_attempt_ms = None;
        *emergency_attempts = 0;
        *first_emergency_zero_ms = None;
        return;
    }

    let prev_emergency_completes = summary.emergency_completes;
    if let Err(e) = drive_emergency_flatten_round(
        live,
        machine,
        open_qty,
        stuck,
        summary,
        last_emergency_attempt_ms,
        emergency_attempts,
        first_emergency_zero_ms,
        wall_clock_ms(),
    )
    .await
    {
        log::warn!("[XVENUE] emergency-flatten round error: {:?}", e);
    }
    // bot-strategy#288 Action B/C: when EmergencyComplete just fired we
    // owe the round-trip a record_close so daily_risk.daily_pnl and
    // trade_stats reflect it. Sprint 5's happy-path record_close lives
    // in the Decision::Exit Both{Filled,Filled} consume which never
    // runs on the emergency-recovered route. Use 0.0 as placeholder PnL
    // — the real cost shows up in the equity-based pnl_today; an exact
    // figure here would need entry-mid + exit-fill prices from the
    // venue side.
    if summary.emergency_completes > prev_emergency_completes {
        if let Some(r) = reporter {
            r.record_close(0.0);
        }
        risk_manager.record_close(0.0, now_unix_secs());
        log::info!(
            "[XVENUE/emerg] record_close fired with placeholder pnl=0.0 (real cost via equity-based pnl_today)"
        );
    }
}

/// Periodic [STATUS] log + dashboard snapshot write fired by the
/// `status_ivl` arm in `run_paper_loop`. Pulled out so the loop body
/// reads as a state machine rather than a paragraph of formatting.
/// Behaviour-preserving: identical log line, identical equity refresh /
/// risk publish / snapshot-write order.
async fn report_status_tick<H: VenueHub + ?Sized>(
    cfg: &XvenueConfig,
    hub: &H,
    summary: &LivePaperSummary,
    ws_health: &WsHealthMonitor,
    machine: &PositionMachine,
    risk_manager: &mut RiskManager,
    reporter: Option<&mut StatusReporter>,
) {
    let ws_age = ws_health.ws_age(wall_clock_ms());
    let wb_fill_rate = if summary.would_be_maker_attempts > 0 {
        summary.would_be_maker_fills as f64 / summary.would_be_maker_attempts as f64
    } else {
        0.0
    };
    let wb_p_avg = if summary.would_be_maker_attempts > 0 {
        summary.would_be_maker_p_sum / summary.would_be_maker_attempts as f64
    } else {
        0.0
    };
    let wb_exit_fill_rate = if summary.would_be_maker_exit_attempts > 0 {
        summary.would_be_maker_exit_fills as f64 / summary.would_be_maker_exit_attempts as f64
    } else {
        0.0
    };
    let wb_exit_p_avg = if summary.would_be_maker_exit_attempts > 0 {
        summary.would_be_maker_exit_p_sum / summary.would_be_maker_exit_attempts as f64
    } else {
        0.0
    };
    log::info!(
        "[STATUS] ticks={} samples={} hold={} enter_l={} enter_s={} exit={} \
         ks_blocked={} stuck_blocked={} dd_blocked={} sd_blocked={} cb_blocked={} \
         ws_blocked={} depth_blocked={} maint_blocked={} ws_emerg={} skew_emerg={} \
         ws_age_ext={:?} ws_age_lt={:?} \
         ref_supp_ext={} ref_supp_lt={} read_mid_err_ext={} read_mid_err_lt={} \
         dev_bps={:?} cap_long={:?} cap_short={:?} \
         ext_inside={:?} lt_inside={:?} lt_bid_sz={:?} lt_ask_sz={:?} \
         wb_attempts={} wb_fills={} wb_fill_rate={:.4} wb_p_avg={:.4} \
         wb_exit_attempts={} wb_exit_fills={} wb_exit_fill_rate={:.4} wb_exit_p_avg={:.4}",
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
        summary.entries_blocked_by_book_depth,
        summary.entries_blocked_by_maintenance,
        summary.ws_stale_emergencies_emitted,
        summary.skew_emergencies_emitted,
        ws_age.ext_age_ms,
        ws_age.lt_age_ms,
        summary.ext_book_suppressed_by_ref_guard,
        summary.lt_book_suppressed_by_ref_guard,
        summary.read_mid_err_ext,
        summary.read_mid_err_lt,
        summary.last_dev_bps,
        summary.last_cap_long_bps,
        summary.last_cap_short_bps,
        summary.last_ext_inside_bps,
        summary.last_lt_inside_bps,
        summary.last_lt_bid_size,
        summary.last_lt_ask_size,
        summary.would_be_maker_attempts,
        summary.would_be_maker_fills,
        wb_fill_rate,
        wb_p_avg,
        summary.would_be_maker_exit_attempts,
        summary.would_be_maker_exit_fills,
        wb_exit_fill_rate,
        wb_exit_p_avg,
    );
    if let Some(r) = reporter {
        refresh_equity(hub, r, risk_manager).await;
        publish_risk(risk_manager, r);
        publish_kill_switch(cfg, r);
        let now_ts_ms = wall_clock_ms();
        if let Err(e) = r.write_snapshot_if_due(machine, now_ts_ms) {
            log::warn!("[STATUS] snapshot write failed: {:?}", e);
        }
    }
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
    live_entry_ctx: &mut Option<LiveEntryCtx>,
) -> Result<()> {
    // Drain any pending SIGUSR1 — arms the STUCK file if needed.
    let _ = stuck.poll_sigusr1();

    let Some((mut ext_snap, mut lt_snap)) = read_both_mids(hub, warmup, summary).await? else {
        return Ok(());
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

    // WS staleness check (#244 Group C). Runs *after* the reads so a
    // successful read clears any prior staleness latch.
    if handle_ws_stale_emergency(cfg, ws_health, machine, open_qty, summary, now_wall_ms) {
        return Ok(());
    }

    // Reference guard cross-check (#244 C).
    apply_reference_guard(reference_guard, &mut ext_snap, &mut lt_snap, summary).await;

    if let Some(r) = reporter.as_deref_mut() {
        r.record_book_ok(
            if ext_snap.book_ok {
                Some(ext_snap.ts_ms)
            } else {
                None
            },
            if lt_snap.book_ok {
                Some(lt_snap.ts_ms)
            } else {
                None
            },
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
    if position.is_some()
        && handle_skew_breach_emergency(
            cfg,
            skew_monitor,
            machine,
            open_qty,
            summary,
            ext_snap.mid,
            lt_snap.mid,
            now_ts_ms,
        )
    {
        return Ok(());
    }

    let mut position = position;
    force_flatten_on_session_dd_halt(
        cfg,
        risk_manager,
        machine,
        summary,
        live_exec,
        &mut position,
        now_ts_ms,
    );

    let evaluate = committed || position.is_some();
    if !evaluate {
        return Ok(());
    }

    let dev = spread.current_dev_bps();
    summary.last_dev_bps = dev;

    // Touch-to-touch + book-depth metrics for #309 redesign DRY_RUN
    // soak. Cheap (a handful of f64 ops on each evaluated tick) and
    // surfaced in the periodic [STATUS] log line. Only computes when
    // both bid/ask are populated (skips scripted-hub tests where the
    // default-zero connector returns 0/0).
    if ext_snap.bid > Decimal::ZERO
        && ext_snap.ask > Decimal::ZERO
        && lt_snap.bid > Decimal::ZERO
        && lt_snap.ask > Decimal::ZERO
    {
        let ext_bid = ext_snap.bid.to_f64().unwrap_or(0.0);
        let ext_ask = ext_snap.ask.to_f64().unwrap_or(0.0);
        let ext_mid = ext_snap.mid.to_f64().unwrap_or(0.0);
        let lt_bid = lt_snap.bid.to_f64().unwrap_or(0.0);
        let lt_ask = lt_snap.ask.to_f64().unwrap_or(0.0);
        let lt_mid = lt_snap.mid.to_f64().unwrap_or(0.0);
        let mid_avg = (ext_mid + lt_mid) / 2.0;
        if mid_avg > 0.0 {
            summary.last_cap_long_bps = Some((lt_bid - ext_ask) / mid_avg * 10_000.0);
            summary.last_cap_short_bps = Some((ext_bid - lt_ask) / mid_avg * 10_000.0);
        }
        if ext_mid > 0.0 {
            summary.last_ext_inside_bps = Some((ext_ask - ext_bid) / ext_mid * 10_000.0);
        }
        if lt_mid > 0.0 {
            summary.last_lt_inside_bps = Some((lt_ask - lt_bid) / lt_mid * 10_000.0);
        }
        summary.last_lt_bid_size = Some(lt_snap.bid_size.to_f64().unwrap_or(0.0));
        summary.last_lt_ask_size = Some(lt_snap.ask_size.to_f64().unwrap_or(0.0));
    }

    if let (Some(r), Some(d)) = (reporter.as_deref_mut(), dev) {
        if committed {
            r.push_spread_point(now_ts_ms, d);
        }
        r.record_samples_committed(spread.samples_committed());
    }
    let is_warm = spread.is_warm(cfg.min_warmup_samples);

    // bot-strategy#309 step 3: route the configured signal source into
    // SignalEngine. Mid-to-mid (default) preserves the legacy v2 path;
    // touch-to-touch maps the directional inside-spread caps onto the
    // signed scale decide() expects.
    let signal_dev = effective_dev_bps(
        signal.config().signal_mode,
        dev,
        summary.last_cap_long_bps,
        summary.last_cap_short_bps,
    );

    let mut decision = signal.decide(now_ts_ms, signal_dev, is_warm, position);

    apply_entry_gates(
        cfg,
        &mut decision,
        risk_manager,
        stuck,
        machine,
        summary,
        dev,
    );

    // bot-strategy#317 / #321: Extended maintenance gate. Mirrors pairtrade's
    // `is_upcoming_maintenance(1)` check at pairtrade/src/pairtrade/mod.rs.
    // Evaluated once per tick (not just on Enter) so the flag also drives
    // `status.maintenance` (gated by error-watch workflow, see
    // bot-strategy#168/#199) and `error_counter::set_counting_suppressed`
    // — without the latter, the `Maintenance mode` REST rejections, WS
    // reconnect bursts, and stale-book WARNs that fire while the venue is
    // rejecting requests inflate `error_summary` and trip the auto-error
    // workflow even when the bot is correctly blocked. Lighter has no
    // maintenance protocol so the default-`false` trait impl on its
    // VenueOps means we only consult Extended.
    let maintenance_block_entries = match live_exec {
        Some(live) => live.ext_ops.is_upcoming_maintenance(1).await,
        None => false,
    };
    if let Some(r) = reporter.as_deref_mut() {
        r.set_maintenance(if maintenance_block_entries {
            Some("upcoming_or_active".to_string())
        } else {
            None
        });
    }
    crate::error_counter::set_counting_suppressed(maintenance_block_entries);
    if matches!(decision, Decision::Enter(_)) && maintenance_block_entries {
        log::warn!(
            "[XVENUE] Extended maintenance upcoming/active; blocking new entry. \
             dev_bps={:?}",
            dev
        );
        summary.entries_blocked_by_maintenance += 1;
        decision = Decision::Hold;
    }

    // bot-strategy#309 step 4: queue-depth filter. Runs after the
    // standard entry gates so a kill_switch / risk-halt block doesn't
    // get re-counted as a depth block. Disabled by default; flips on
    // for the maker-on-Lighter redesign once `lt_book_max_eth` is set
    // in the YAML.
    if let Decision::Enter(dir) = decision {
        if book_depth_blocks_entry(dir, &lt_snap, cfg.lt_book_max_eth) {
            log::debug!(
                "[XVENUE] book-depth filter blocked entry: dir={:?} \
                 lt_bid_sz={:?} lt_ask_sz={:?} max={:?}",
                dir,
                summary.last_lt_bid_size,
                summary.last_lt_ask_size,
                cfg.lt_book_max_eth,
            );
            summary.entries_blocked_by_book_depth += 1;
            decision = Decision::Hold;
        }
    }

    match decision {
        Decision::Hold => {
            summary.decisions_hold += 1;
        }
        Decision::Enter(dir) => {
            handle_decision_enter(
                cfg,
                hub,
                live_exec,
                machine,
                summary,
                open_qty,
                live_entry_ctx,
                reporter.as_deref_mut(),
                stuck,
                &ext_snap,
                &lt_snap,
                dir,
                now_ts_ms,
                dev,
            )
            .await?;
        }
        Decision::Exit(reason) => {
            handle_decision_exit(
                cfg,
                live_exec,
                machine,
                summary,
                open_qty,
                live_entry_ctx,
                reporter.as_deref_mut(),
                risk_manager,
                &ext_snap,
                &lt_snap,
                reason,
                now_ts_ms,
                dev,
            )
            .await?;
        }
    }
    Ok(())
}

/// Handle `Decision::Enter(dir)` — the post-gate entry dispatch.
/// Branches on dry-run vs live:
///   - Paper mode: synthesise EntrySignal + both *Filled events so the
///     state machine stays exercised end-to-end (used by tests and BT
///     replay). No real orders, no live_entry_ctx capture (paper PnL
///     stays at 0 — the equity-based pnl_today path is what
///     dashboard renders).
///   - Live mode: equity-driven sizing → Extended-first serial entry
///     dispatch (per DESIGN.md §4.1) → on Extended Fill, fire Lighter.
///     On either leg's Failed terminal, route the state machine to
///     Flat (Extended failed before any fills) or EmergencyFlattening
///     (Lighter failed after Extended landed).
///
/// Behaviour-preserving wholesale extraction of the previous inline
/// `Decision::Enter` arm. All log lines, summary counter increments,
/// state-machine apply ordering, and stuck-tripwire `record_enter_*`
/// calls are byte-identical to the prior form.
#[allow(clippy::too_many_arguments)]
async fn handle_decision_enter<H: VenueHub + ?Sized>(
    cfg: &XvenueConfig,
    hub: &H,
    live_exec: Option<&LiveExecution>,
    machine: &mut PositionMachine,
    summary: &mut LivePaperSummary,
    open_qty: &mut Option<Decimal>,
    live_entry_ctx: &mut Option<LiveEntryCtx>,
    mut reporter: Option<&mut StatusReporter>,
    stuck: &mut StuckTripwire,
    ext_snap: &MidSnapshot,
    lt_snap: &MidSnapshot,
    dir: SpreadDirection,
    now_ts_ms: u64,
    dev: Option<f64>,
) -> Result<()> {
    let go_live = !cfg.dry_run && live_exec.is_some();
    if !go_live {
        // Paper-mode synthetic fills: walk the state machine through
        // one EntrySignal + both Filled events in series so the engine
        // stays exercised end-to-end. Used in dry-run and by tests /
        // BT replay paths.
        let qty = paper_qty(cfg.min_notional_usd, ext_snap.mid)?;
        let notional = Decimal::from_f64_retain(cfg.min_notional_usd).unwrap_or(Decimal::ZERO);
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

        // bot-strategy#309 step 5: would-be maker fill telemetry. Each
        // paper-mode entry counts as a would-be attempt; the helper
        // returns None when the book read is unusable (e.g. scripted-
        // hub tests with default-zero sizes), in which case we still
        // record the attempt but skip the fill / probability update.
        // The depth + our_size fields go into the log line so post-hoc
        // analysis can recompute the fill rate under a different model.
        summary.would_be_maker_attempts += 1;
        if let Some(out) = would_be_maker_fill_outcome(dir, qty, lt_snap, now_ts_ms) {
            summary.would_be_maker_p_sum += out.fill_p;
            if out.sampled_fill {
                summary.would_be_maker_fills += 1;
            }
            log::info!(
                "[XVENUE] WOULD-BE MAKER dir={:?} our_size_eth={:.6} \
                 depth_eth={:.6} fill_p={:.4} sampled_fill={}",
                dir,
                out.our_size_eth,
                out.depth_eth,
                out.fill_p,
                out.sampled_fill,
            );
        }

        return Ok(());
    }

    // Live mode: equity-driven sizing, real-order dispatch, single-tick
    // failure-mode handling. Sprint 4 step 1/3.
    let live = live_exec.expect("go_live = !cfg.dry_run && live_exec.is_some(); checked above");
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
         notional={} ext_qty={} lt_qty={} \
         ext_bid={} ext_ask={} lt_bid={} lt_ask={} \
         lt_bid_size={} lt_ask_size={}",
        dir,
        dev,
        ext_snap.mid,
        lt_snap.mid,
        notional,
        ext_qty,
        lt_qty,
        ext_snap.bid,
        ext_snap.ask,
        lt_snap.bid,
        lt_snap.ask,
        lt_snap.bid_size,
        lt_snap.ask_size,
    );
    let (ext_side, lt_side) = match dir {
        // Direction sign convention (cf. dev_breach test + signal.rs):
        // SpreadDirection::Long means the spread is below mean and we
        // expect mean reversion → buy the cheap leg (Extended) and
        // sell the rich leg (Lighter). Short is symmetric.
        SpreadDirection::Long => (DcOrderSide::Long, DcOrderSide::Short),
        SpreadDirection::Short => (DcOrderSide::Short, DcOrderSide::Long),
    };
    // Sequential Extended-first dispatch per DESIGN.md §4.1. Lighter
    // fires only after Extended terminates — a serial dispatch keeps
    // the legged-exposure window bounded by Extended's chase × retries
    // budget rather than the parallel max.
    let ext_term = ExtendedMakerLoop::new(&*live.ext_ops, &live.extended_maker_cfg)
        .run_entry(ExtendedEntryRequest {
            symbol: live.ext_symbol.clone(),
            side: ext_side,
            target_qty: ext_qty,
            dust_qty: live.dust_qty,
            venue_min_qty: live.ext_min_qty,
            reduce_only: false,
        })
        .await;
    match ext_term {
        ExtendedTerminal::Filled { qty } => {
            // bot-strategy#244 / #282 silent-reject gate: any
            // successful fill resets the consec-Timeout counter so a
            // healthy run doesn't leave a stale stuck arm pending.
            stuck.record_enter_timeout_success();
            machine.apply(now_ts_ms, Event::ExtendedFilled { qty })?;
            if let Some(r) = reporter.as_deref_mut() {
                r.record_fill(true, false, now_ts_ms);
            }
            log::info!("[XVENUE] LIVE ENTER ext filled qty={}", qty);
            // bot-strategy#309 step 6: route the Lighter entry leg
            // through the post-only chase loop when the YAML flips
            // `lighter_post_only: true`. Default keeps the legacy
            // taker behavior. dex-connector verification gate is the
            // `lighter-spike` binary at $50 notional (#317). The exit
            // path mirrors this gate inside `ParallelExitLoop::run`
            // (#330), so flipping `lighter_post_only` toggles both
            // legs in lockstep.
            let lt_term = if live.lighter_maker_cfg.post_only {
                LighterMakerLoop::new(&*live.lt_ops, &live.lighter_maker_cfg)
                    .run(LighterMakerRequest {
                        symbol: live.lt_symbol.clone(),
                        side: lt_side,
                        target_qty: lt_qty,
                        dust_qty: live.dust_qty,
                        venue_min_qty: live.lt_min_qty,
                        reduce_only: false,
                    })
                    .await
            } else {
                LighterFillLoop::new(&*live.lt_ops, &live.lighter_fill_cfg)
                    .run(LighterFillRequest {
                        symbol: live.lt_symbol.clone(),
                        side: lt_side,
                        target_qty: lt_qty,
                        dust_qty: live.dust_qty,
                        reduce_only: false,
                    })
                    .await
            };
            match lt_term {
                LighterTerminal::Filled { qty: lt_filled } => {
                    machine.apply(now_ts_ms, Event::LighterFilled { qty: lt_filled })?;
                    *open_qty = Some(lt_filled);
                    // Capture entry context for the realised-PnL
                    // helper at exit time (#268 S5-1). Only set when
                    // BOTH legs filled; failure paths leave the ctx
                    // None so partial-fill PnL is out of scope.
                    *live_entry_ctx = Some(LiveEntryCtx {
                        direction: dir,
                        ext_entry_mid: ext_snap.mid,
                        lt_entry_mid: lt_snap.mid,
                        ext_entry_qty: qty,
                        lt_entry_qty: lt_filled,
                    });
                    if let Some(r) = reporter.as_deref_mut() {
                        r.record_fill(false, true, now_ts_ms);
                    }
                    log::info!("[XVENUE] LIVE ENTER lt filled qty={} → Held", lt_filled);
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
                    // open_qty intentionally stays None at this layer;
                    // the open Extended leg will be flattened by the
                    // emergency_loop wiring (Sprint 4 step 3/3). State
                    // machine has already routed to
                    // EmergencyFlattening.
                }
            }
        }
        ExtendedTerminal::Failed { reason } => {
            log::error!(
                "[XVENUE] LIVE ENTER ext failed reason={:?} → state→Flat \
                 (no fills landed)",
                reason,
            );
            // bot-strategy#244 / #282: count consecutive Timeout
            // failures so the bot self-arms STUCK after N in a row.
            // Only Timeout is the silent-reject signature; other
            // reasons (VenueRejected, TakerRejected, PostOnlyExhausted)
            // have their own meanings and the operator should
            // investigate them separately.
            if matches!(reason, ExecutionFailure::Timeout) {
                stuck.record_enter_timeout_failure();
            }
            machine.apply(now_ts_ms, Event::ExtendedFailed)?;
            summary.live_entries_extended_failed += 1;
        }
    }
    Ok(())
}

/// Handle `Decision::Exit(reason)` — the post-gate exit dispatch.
/// Branches on dry-run vs live:
///   - Paper mode: synthesise ExitSignal + both *ExitFilled events
///     against the cached open_qty (matching the prior inline form;
///     paper PnL is recorded as 0.0 so the equity-based pnl_today
///     remains the live-mode renderer).
///   - Live mode: read per-venue qty + direction from the position
///     machine (#268 S5-2 source-of-truth move) → reduce-only parallel
///     exit on both legs (#244 Sprint 4 step 2/3) → on Both{Filled,
///     Filled} compute realised PnL via compute_realised_pnl, on any
///     Failed leg drop the entry ctx and route to EmergencyFlattening.
///   - LegMismatchTimeout: apply whatever terminal qtys landed before
///     routing to EmergencyFlattening (mirrors parallel_exit case 11).
///
/// Behaviour-preserving wholesale extraction of the previous inline
/// `Decision::Exit` arm. All log lines, summary counters, state-
/// machine apply ordering, record_close calls (with the 0.0
/// placeholder on paper-mode and on Both/Some+None Failed paths), and
/// the live_entry_ctx.take() points are byte-identical.
#[allow(clippy::too_many_arguments)]
async fn handle_decision_exit(
    cfg: &XvenueConfig,
    live_exec: Option<&LiveExecution>,
    machine: &mut PositionMachine,
    summary: &mut LivePaperSummary,
    open_qty: &mut Option<Decimal>,
    live_entry_ctx: &mut Option<LiveEntryCtx>,
    mut reporter: Option<&mut StatusReporter>,
    risk_manager: &mut RiskManager,
    ext_snap: &MidSnapshot,
    lt_snap: &MidSnapshot,
    reason: ExitReason,
    now_ts_ms: u64,
    dev: Option<f64>,
) -> Result<()> {
    let go_live = !cfg.dry_run && live_exec.is_some();
    if !go_live {
        // Paper-mode synthetic exit fills (existing behaviour).
        // Capture the position direction *before* applying ExitSignal —
        // the state machine keeps `position` populated through Exiting,
        // but we want a deterministic source for the would-be-maker
        // telemetry so the outcome is well-defined even if a future
        // refactor clears it earlier.
        let position_dir = machine.position().map(|p| p.direction);
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

        // bot-strategy#330: would-be exit-side maker telemetry. Each
        // paper exit counts as a would-be attempt; the helper is run
        // against the *opposite* book side because a Long position
        // closes by buying back the Lighter leg (depth = bid_size,
        // matching `would_be_maker_fill_outcome`'s Short branch) and
        // a Short closes by selling back (depth = ask_size, Long
        // branch). The seed mixes `now_ts_ms ^ 1` so a tick that
        // re-uses the same wall clock for an entry+exit pair (rare,
        // but possible in tight BT replay) draws independently.
        if qty > Decimal::ZERO {
            if let Some(dir) = position_dir {
                summary.would_be_maker_exit_attempts += 1;
                let exit_dir = match dir {
                    SpreadDirection::Long => SpreadDirection::Short,
                    SpreadDirection::Short => SpreadDirection::Long,
                };
                if let Some(out) =
                    would_be_maker_fill_outcome(exit_dir, qty, lt_snap, now_ts_ms ^ 1)
                {
                    summary.would_be_maker_exit_p_sum += out.fill_p;
                    if out.sampled_fill {
                        summary.would_be_maker_exit_fills += 1;
                    }
                    log::info!(
                        "[XVENUE] WOULD-BE MAKER EXIT pos_dir={:?} our_size_eth={:.6} \
                         depth_eth={:.6} fill_p={:.4} sampled_fill={}",
                        dir,
                        out.our_size_eth,
                        out.depth_eth,
                        out.fill_p,
                        out.sampled_fill,
                    );
                }
            }
        }

        let _ = ExitReason::MeanCross;
        return Ok(());
    }

    // Live mode: reduce-only parallel exit on both legs (#244 Sprint 4
    // step 2/3). DESIGN.md §4.2 specifies parallel exit (vs entry's
    // serial Extended-first dispatch) — the legs are already balanced
    // so we close both simultaneously to minimise the open-leg window
    // if one venue lags.
    let live = live_exec.expect("go_live = !cfg.dry_run && live_exec.is_some(); checked above");
    // Read direction + per-venue open qty from the state machine's
    // `Position` (#268 S5-2). The machine is the source of truth: each
    // venue's qty is updated independently as `*Filled` events land
    // during entry, so an asymmetric entry (e.g. Extended partial-fill
    // + Lighter full-fill) surfaces here as ext_qty != lt_qty. The
    // runner-level `open_qty` cache is no longer consulted on exit —
    // but kept because the paper-mode WS-stale / skew short-circuits
    // still use it.
    let (position_dir, ext_qty, lt_qty) = match machine.position() {
        Some(p) => (p.direction, p.extended_open_qty, p.lighter_open_qty),
        None => {
            log::warn!(
                "[XVENUE] LIVE EXIT skipped: no position in phase {:?}",
                machine.phase()
            );
            return Ok(());
        }
    };
    if ext_qty <= Decimal::ZERO && lt_qty <= Decimal::ZERO {
        // No legs to close — log + skip. Don't touch open_qty (leave
        // its existing value alone for paper-mode short-circuits).
        log::warn!(
            "[XVENUE] LIVE EXIT skipped: both legs already zero in phase {:?}",
            machine.phase()
        );
        return Ok(());
    }
    // Clear the runner-level cache so a subsequent paper-mode short-
    // circuit doesn't synthesise stale ExitFilled events. Live mode no
    // longer reads it post-S5-2.
    let _ = open_qty.take();
    machine.apply(now_ts_ms, Event::ExitSignal { reason })?;
    summary.last_decision_ts_ms = Some(now_ts_ms);
    summary.decisions_exit += 1;
    let (ext_exit_side, lt_exit_side) = match position_dir {
        // Reverse the entry sides — see Decision::Enter for the entry
        // sign convention. To close a long leg we sell (Short), and
        // vice versa.
        SpreadDirection::Long => (DcOrderSide::Short, DcOrderSide::Long),
        SpreadDirection::Short => (DcOrderSide::Long, DcOrderSide::Short),
    };
    log::info!(
        "[XVENUE] LIVE EXIT start reason={:?} dev_bps={:?} dir={:?} \
         ext_qty={} lt_qty={} \
         ext_mid={} lt_mid={} ext_bid={} ext_ask={} lt_bid={} lt_ask={} \
         lt_bid_size={} lt_ask_size={}",
        reason,
        dev,
        position_dir,
        ext_qty,
        lt_qty,
        ext_snap.mid,
        lt_snap.mid,
        ext_snap.bid,
        ext_snap.ask,
        lt_snap.bid,
        lt_snap.ask,
        lt_snap.bid_size,
        lt_snap.ask_size,
    );
    // bot-strategy#330: route both exit legs through the same maker
    // selector the entry path uses. `lt_maker_cfg.post_only=true` flips
    // the Lighter leg from `LighterFillLoop` (taker) to
    // `LighterMakerLoop` (post-only chase + taker fallback) inside
    // `ParallelExitLoop::run`; the legacy fill cfg is still passed so
    // the gate flips cleanly without re-plumbing the runner.
    let outcome = ParallelExitLoop::new(
        &*live.ext_ops,
        &*live.lt_ops,
        &live.extended_maker_cfg,
        &live.lighter_fill_cfg,
        &live.lighter_maker_cfg,
        &live.parallel_exit_cfg,
    )
    .with_lt_min_qty(live.lt_min_qty)
    .run(
        ExtendedEntryRequest {
            symbol: live.ext_symbol.clone(),
            side: ext_exit_side,
            target_qty: ext_qty,
            dust_qty: live.dust_qty,
            venue_min_qty: live.ext_min_qty,
            reduce_only: true,
        },
        LighterFillRequest {
            symbol: live.lt_symbol.clone(),
            side: lt_exit_side,
            target_qty: lt_qty,
            dust_qty: live.dust_qty,
            reduce_only: true,
        },
    )
    .await;
    match outcome {
        ParallelExitOutcome::Both { ext, lt } => {
            let ext_exit_qty = match ext {
                ExtendedTerminal::Filled { qty: q } => {
                    machine.apply(now_ts_ms, Event::ExtendedExitFilled { qty: q })?;
                    Some(q)
                }
                ExtendedTerminal::Failed { reason: r } => {
                    log::error!("[XVENUE] LIVE EXIT ext failed reason={:?}", r);
                    None
                }
            };
            let lt_exit_qty = match lt {
                LighterTerminal::Filled { qty: q } => {
                    machine.apply(now_ts_ms, Event::LighterExitFilled { qty: q })?;
                    Some(q)
                }
                LighterTerminal::Failed { reason: r } => {
                    log::error!("[XVENUE] LIVE EXIT lt failed reason={:?}", r);
                    None
                }
            };
            match (ext_exit_qty, lt_exit_qty) {
                (Some(ext_eq), Some(lt_eq)) => {
                    // Happy path — compute realised PnL (#268 S5-1).
                    // If the ctx is missing (shouldn't happen but
                    // defensive), fall back to 0.0 + log warn.
                    let pnl = match live_entry_ctx.take() {
                        Some(ctx) => compute_realised_pnl(
                            ctx.direction,
                            ctx.ext_entry_mid,
                            ctx.lt_entry_mid,
                            ext_snap.mid,
                            lt_snap.mid,
                            ctx.ext_entry_qty,
                            ctx.lt_entry_qty,
                            ext_eq,
                            lt_eq,
                            cfg.extended_fee_bps,
                            cfg.lighter_fee_bps,
                        ),
                        None => {
                            log::warn!(
                                "[XVENUE] LIVE EXIT realised PnL: \
                                 entry ctx missing, recording 0.0"
                            );
                            Decimal::ZERO
                        }
                    };
                    let pnl_f64 = rust_decimal::prelude::ToPrimitive::to_f64(&pnl).unwrap_or(0.0);
                    summary.last_realised_pnl_usd = Some(pnl_f64);
                    if let Some(r) = reporter.as_deref_mut() {
                        r.record_fill(true, true, now_ts_ms);
                        r.record_close(pnl_f64);
                    }
                    risk_manager.record_close(pnl_f64, now_unix_secs());
                    log::info!(
                        "[XVENUE] LIVE EXIT both legs filled → Flat \
                         pnl_usd={:.4}",
                        pnl_f64
                    );
                }
                (ext_filled, lt_filled) => {
                    // Failure: at least one leg returned Failed. Drop
                    // the entry ctx (no PnL computation for partial
                    // trades — out of scope per #268 S5-1) and route
                    // to EmergencyFlattening. No dedicated
                    // `ExitFailed` reason in EmergencyReason —
                    // `LegMismatchTimeout` is the closest semantic
                    // fit.
                    let _ = live_entry_ctx.take();
                    machine.apply(
                        now_ts_ms,
                        Event::Emergency {
                            reason: EmergencyReason::LegMismatchTimeout,
                        },
                    )?;
                    summary.live_exits_failed_legs += 1;
                    log::error!(
                        "[XVENUE] LIVE EXIT failed legs \
                         (ext_filled={}, lt_filled={}) → EmergencyFlattening",
                        ext_filled.is_some(),
                        lt_filled.is_some(),
                    );
                }
            }
        }
        ParallelExitOutcome::LegMismatchTimeout { ext, lt } => {
            // Apply whatever terminal we have first so the state
            // machine has the latest open qty before transitioning to
            // EmergencyFlattening (mirrors parallel_exit.rs's
            // documented contract for case 11). PnL is not computed —
            // see #268 S5-1 'Out of scope' for partial trade PnL
            // accounting.
            if let Some(ExtendedTerminal::Filled { qty: q }) = ext {
                machine.apply(now_ts_ms, Event::ExtendedExitFilled { qty: q })?;
            }
            if let Some(LighterTerminal::Filled { qty: q }) = lt {
                machine.apply(now_ts_ms, Event::LighterExitFilled { qty: q })?;
            }
            let _ = live_entry_ctx.take();
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
    first_zero_observed_ms: &mut Option<u64>,
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
        // bot-strategy#287 grace: a zero read taken right after a
        // fill the same process observed may not yet reflect venue
        // truth (WS lag / sub-account race). Require complete_grace_ms
        // of *consistent* zero before emitting EmergencyComplete.
        // Set to 0 to disable the grace and trust every zero read.
        let grace_ms = live.emergency_loop_cfg.complete_grace_ms;
        if grace_ms > 0 {
            match *first_zero_observed_ms {
                None => {
                    *first_zero_observed_ms = Some(now_ms);
                    *last_attempt_ms = Some(now_ms);
                    log::info!(
                        "[XVENUE/emerg] both legs zero — entering grace ({} ms) before declaring complete (attempts={})",
                        grace_ms,
                        *attempts
                    );
                    return Ok(());
                }
                Some(first_zero_ms) => {
                    let elapsed = now_ms.saturating_sub(first_zero_ms);
                    if elapsed < grace_ms {
                        // Still inside the grace window — keep
                        // polling, don't declare complete yet. The
                        // throttle's retry_interval_ms ensures we
                        // re-read at a sensible cadence (default
                        // 30 s). If the read flips to non-zero on
                        // a later tick the grace timer resets and
                        // we attempt close_all on the discovered leg.
                        log::info!(
                            "[XVENUE/emerg] still both zero ({} ms / {} ms grace) — holding for grace (attempts={})",
                            elapsed,
                            grace_ms,
                            *attempts
                        );
                        *last_attempt_ms = Some(now_ms);
                        return Ok(());
                    }
                    // grace elapsed and read still says zero — trust it.
                }
            }
        }

        // Verified zero (after grace, or grace disabled). Emit
        // EmergencyComplete; state machine transitions back to
        // Flat (or rejects with TransitionError if we're already
        // out of EmergencyFlattening, in which case we log and
        // continue).
        match machine.apply(now_ms, Event::EmergencyComplete) {
            Ok(()) => {
                summary.emergency_completes += 1;
                *open_qty = None;
                *first_zero_observed_ms = None;
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

    // Non-zero read — reset the grace timer; any future zero
    // observation must restart the grace window.
    *first_zero_observed_ms = None;

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

    fn lt_snap_with_sizes(bid_size: Decimal, ask_size: Decimal) -> MidSnapshot {
        MidSnapshot {
            ts_ms: 0,
            mid: Decimal::from(2000),
            book_ok: true,
            bid: Decimal::from(1999),
            ask: Decimal::from(2001),
            bid_size,
            ask_size,
        }
    }

    fn d(n: i64, scale: u32) -> Decimal {
        Decimal::new(n, scale)
    }

    #[test]
    fn book_depth_filter_disabled_when_max_is_none() {
        let snap = lt_snap_with_sizes(Decimal::from(100), Decimal::from(100));
        assert!(!book_depth_blocks_entry(SpreadDirection::Long, &snap, None));
        assert!(!book_depth_blocks_entry(
            SpreadDirection::Short,
            &snap,
            None
        ));
    }

    #[test]
    fn book_depth_filter_blocks_long_when_ask_too_deep() {
        // Long posts on Lighter ASK (we're the seller). ask_size=5,
        // max=2 ETH → blocked.
        let snap = lt_snap_with_sizes(d(5, 1), Decimal::from(5));
        assert!(book_depth_blocks_entry(
            SpreadDirection::Long,
            &snap,
            Some(2.0)
        ));
        // Short would post on the (thin) BID side, so not blocked.
        assert!(!book_depth_blocks_entry(
            SpreadDirection::Short,
            &snap,
            Some(2.0)
        ));
    }

    #[test]
    fn book_depth_filter_blocks_short_when_bid_too_deep() {
        let snap = lt_snap_with_sizes(Decimal::from(5), d(5, 1));
        assert!(book_depth_blocks_entry(
            SpreadDirection::Short,
            &snap,
            Some(2.0)
        ));
        assert!(!book_depth_blocks_entry(
            SpreadDirection::Long,
            &snap,
            Some(2.0)
        ));
    }

    #[test]
    fn book_depth_filter_admits_at_or_below_threshold() {
        let snap = lt_snap_with_sizes(Decimal::from(2), Decimal::from(2));
        // Equal to threshold → admit (strict > comparator)
        assert!(!book_depth_blocks_entry(
            SpreadDirection::Long,
            &snap,
            Some(2.0)
        ));
        assert!(!book_depth_blocks_entry(
            SpreadDirection::Short,
            &snap,
            Some(2.0)
        ));
    }

    #[test]
    fn would_be_outcome_zero_p_when_we_swamp_the_book() {
        // Our 5 ETH order vs 1 ETH ask depth → fill_p clamped to 0.
        let snap = lt_snap_with_sizes(Decimal::ZERO, Decimal::from(1));
        let out = would_be_maker_fill_outcome(SpreadDirection::Long, Decimal::from(5), &snap, 42)
            .expect("snapshot has positive ask depth");
        assert_eq!(out.fill_p, 0.0);
        assert!(!out.sampled_fill, "p=0 must never sample to true");
    }

    #[test]
    fn would_be_outcome_full_p_when_book_is_huge() {
        // 0.01 ETH order vs 100 ETH bid depth → fill_p ≈ 0.9999.
        // With a deterministic seed the draw must be < p so we get a
        // sampled fill.
        let snap = lt_snap_with_sizes(Decimal::from(100), Decimal::ZERO);
        let out = would_be_maker_fill_outcome(
            SpreadDirection::Short,
            d(1, 2), // 0.01
            &snap,
            7,
        )
        .expect("snapshot has positive bid depth");
        assert!(out.fill_p > 0.999, "expected near-1 p; got {}", out.fill_p);
        assert!(out.sampled_fill);
    }

    #[test]
    fn would_be_outcome_picks_correct_side_per_direction() {
        // Long looks at ask_size, Short looks at bid_size.
        let snap = lt_snap_with_sizes(Decimal::from(10), d(1, 2));
        let long_out =
            would_be_maker_fill_outcome(SpreadDirection::Long, Decimal::from(1), &snap, 1).unwrap();
        // ask_size = 0.01, our_size = 1 → p = 0
        assert_eq!(long_out.fill_p, 0.0);
        assert_eq!(long_out.depth_eth, 0.01);

        let short_out =
            would_be_maker_fill_outcome(SpreadDirection::Short, Decimal::from(1), &snap, 1)
                .unwrap();
        // bid_size = 10, our_size = 1 → p = 0.9
        assert!((short_out.fill_p - 0.9).abs() < 1e-9);
        assert_eq!(short_out.depth_eth, 10.0);
    }

    #[test]
    fn would_be_outcome_none_when_book_empty() {
        let snap = lt_snap_with_sizes(Decimal::ZERO, Decimal::ZERO);
        assert!(
            would_be_maker_fill_outcome(SpreadDirection::Long, Decimal::from(1), &snap, 0)
                .is_none()
        );
        assert!(
            would_be_maker_fill_outcome(SpreadDirection::Short, Decimal::from(1), &snap, 0)
                .is_none()
        );
    }

    #[test]
    fn would_be_outcome_deterministic_for_same_seed() {
        // Reproducibility: same (dir, size, depth, seed) must produce
        // the same draw — so post-hoc analysis on a logged tuple
        // returns the bot's recorded outcome exactly.
        let snap = lt_snap_with_sizes(Decimal::ZERO, Decimal::from(2));
        let a = would_be_maker_fill_outcome(SpreadDirection::Long, Decimal::from(1), &snap, 12345)
            .unwrap();
        let b = would_be_maker_fill_outcome(SpreadDirection::Long, Decimal::from(1), &snap, 12345)
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn book_depth_filter_falls_through_on_zero_size() {
        // Empty side: spread engine handles via book_ok upstream;
        // here we don't double-gate.
        let snap = lt_snap_with_sizes(Decimal::ZERO, Decimal::ZERO);
        assert!(!book_depth_blocks_entry(
            SpreadDirection::Long,
            &snap,
            Some(2.0)
        ));
        assert!(!book_depth_blocks_entry(
            SpreadDirection::Short,
            &snap,
            Some(2.0)
        ));
    }

    // bot-strategy#330 — paper-mode exit-side maker telemetry. Mirror
    // of the entry-side `would_be_*` coverage above. Tests below drive
    // `handle_decision_exit` directly with a hand-prepared Held machine
    // to keep the surface tight (a full `run_one_tick` round-trip
    // depends on spread engine warm-up + signal threshold and is
    // covered separately in the loop-level tests further down).
    fn build_held_machine_for_exit(dir: SpreadDirection, qty: Decimal) -> PositionMachine {
        let mut m = PositionMachine::new();
        m.apply(
            0,
            Event::EntrySignal {
                direction: dir,
                notional_usd: Decimal::from(100),
            },
        )
        .unwrap();
        m.apply(0, Event::ExtendedFilled { qty }).unwrap();
        m.apply(0, Event::LighterFilled { qty }).unwrap();
        m
    }

    #[tokio::test]
    async fn paper_exit_records_attempt_and_uses_bid_depth_for_long_close() {
        // Long position closes by buying back the Lighter leg, so the
        // depth that matters is the *bid* size. We supply ask=ZERO,
        // bid=large to force fill_p > 0; if the helper accidentally
        // looked at ask_size instead, fill_p would be 0 and `attempts`
        // would still tick but `p_sum` would stay at 0.0.
        let qty = Decimal::new(5, 2); // 0.05 ETH
        let mut machine = build_held_machine_for_exit(SpreadDirection::Long, qty);
        let mut summary = LivePaperSummary::default();
        let mut open_qty = Some(qty);
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;
        let cfg = min_cfg();
        let (_d, mut rm) = test_risk_manager();
        let ext_snap = mid(0, 2000.0);
        // bid=10 ETH (deep) vs our 0.05 → fill_p ≈ 0.995
        let lt_snap = lt_snap_with_sizes(Decimal::from(10), Decimal::ZERO);

        handle_decision_exit(
            &cfg,
            None,
            &mut machine,
            &mut summary,
            &mut open_qty,
            &mut live_entry_ctx,
            None,
            &mut rm,
            &ext_snap,
            &lt_snap,
            ExitReason::MeanCross,
            1_000,
            Some(0.5),
        )
        .await
        .unwrap();

        assert_eq!(summary.would_be_maker_exit_attempts, 1);
        assert!(
            summary.would_be_maker_exit_p_sum > 0.99,
            "Long close should consume bid depth (p≈0.995), got p_sum={}",
            summary.would_be_maker_exit_p_sum
        );
    }

    #[tokio::test]
    async fn paper_exit_records_attempt_and_uses_ask_depth_for_short_close() {
        // Short position closes by selling back, so depth = ask. ask=ZERO
        // would force fill_p=0; we set ask=large to confirm the helper
        // looked at the ask side.
        let qty = Decimal::new(5, 2);
        let mut machine = build_held_machine_for_exit(SpreadDirection::Short, qty);
        let mut summary = LivePaperSummary::default();
        let mut open_qty = Some(qty);
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;
        let cfg = min_cfg();
        let (_d, mut rm) = test_risk_manager();
        let ext_snap = mid(0, 2000.0);
        let lt_snap = lt_snap_with_sizes(Decimal::ZERO, Decimal::from(10));

        handle_decision_exit(
            &cfg,
            None,
            &mut machine,
            &mut summary,
            &mut open_qty,
            &mut live_entry_ctx,
            None,
            &mut rm,
            &ext_snap,
            &lt_snap,
            ExitReason::MeanCross,
            1_000,
            None,
        )
        .await
        .unwrap();

        assert_eq!(summary.would_be_maker_exit_attempts, 1);
        assert!(
            summary.would_be_maker_exit_p_sum > 0.99,
            "Short close should consume ask depth (p≈0.995), got p_sum={}",
            summary.would_be_maker_exit_p_sum
        );
    }

    #[tokio::test]
    async fn paper_exit_zero_open_qty_records_no_attempt() {
        // open_qty=None (or ZERO) is the degenerate case: ExitSignal
        // still routes through the state machine for symmetry, but
        // there's nothing to "fill" so we should not pollute the
        // would-be telemetry with attempts that have no associated
        // notional.
        let qty = Decimal::new(5, 2);
        let mut machine = build_held_machine_for_exit(SpreadDirection::Long, qty);
        let mut summary = LivePaperSummary::default();
        let mut open_qty: Option<Decimal> = None;
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;
        let cfg = min_cfg();
        let (_d, mut rm) = test_risk_manager();
        let ext_snap = mid(0, 2000.0);
        let lt_snap = lt_snap_with_sizes(Decimal::from(10), Decimal::from(10));

        handle_decision_exit(
            &cfg,
            None,
            &mut machine,
            &mut summary,
            &mut open_qty,
            &mut live_entry_ctx,
            None,
            &mut rm,
            &ext_snap,
            &lt_snap,
            ExitReason::MeanCross,
            1_000,
            None,
        )
        .await
        .unwrap();

        assert_eq!(summary.would_be_maker_exit_attempts, 0);
        assert_eq!(summary.would_be_maker_exit_fills, 0);
        assert_eq!(summary.would_be_maker_exit_p_sum, 0.0);
    }

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
            enter_timeout_consec_fail_to_kill: 5,
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
        let hub = Arc::new(ScriptedHub::new(
            vec![mid(1000, 2000.0)],
            vec![mid(1000, 2000.0)],
        ));
        let cfg = min_cfg();
        let loop_cfg = LiveLoopConfig {
            tick_interval_ms: 5,
            status_log_interval_ms: 10_000,
        };
        let (tx, rx) = oneshot::channel();
        // Send shutdown immediately
        let _ = tx.send(());
        let summary = timeout(
            Duration::from_secs(1),
            run_paper_loop(cfg, loop_cfg, hub, None, rx),
        )
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
            &mut None,
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
            &mut None,
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
                &mut None,
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
                &mut None,
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
                &mut None,
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
                &mut None,
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
                &mut None,
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
            &mut None,
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
                &mut None,
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
            &mut None,
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
            async fn read_equity_usd(&self, _venue: Venue) -> Result<Option<Decimal>> {
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
            &mut None,
        )
        .await
        .expect_err("post-warmup read_mid Err must propagate as Err");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("read_mid Extended"),
            "expected context-wrapped error, got: {chain}"
        );
        // bot-strategy#303: Extended fails first, short-circuiting
        // before the Lighter read — only the Extended counter advances.
        assert_eq!(summary.read_mid_err_ext, 1);
        assert_eq!(summary.read_mid_err_lt, 0);
    }

    #[tokio::test]
    async fn read_mid_err_lt_counter_increments_when_only_lighter_fails() {
        // bot-strategy#303: when Extended succeeds but Lighter fails
        // post-warmup, only the Lighter counter should advance.
        struct LighterFailHub {
            ext_mid: Decimal,
        }
        #[async_trait::async_trait]
        impl VenueHub for LighterFailHub {
            async fn read_mid(&self, venue: Venue) -> Result<MidSnapshot> {
                match venue {
                    Venue::Extended => Ok(MidSnapshot {
                        ts_ms: 0,
                        mid: self.ext_mid,
                        book_ok: true,
                        ..Default::default()
                    }),
                    Venue::Lighter => Err(anyhow::anyhow!(
                        "order book snapshot unavailable (no recent update)"
                    )),
                }
            }
            async fn read_equity_usd(&self, _venue: Venue) -> Result<Option<Decimal>> {
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
        let mut warmup = VenueWarmup {
            ext_ready: true,
            lt_ready: true,
        };
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
        let hub = LighterFailHub {
            ext_mid: Decimal::from(2000),
        };
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
            &mut None,
        )
        .await
        .expect_err("post-warmup Lighter read_mid Err must propagate");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("read_mid Lighter"),
            "expected context-wrapped Lighter error, got: {chain}"
        );
        assert_eq!(summary.read_mid_err_ext, 0);
        assert_eq!(summary.read_mid_err_lt, 1);
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
emergency_complete_grace_ms: 0  # tests assert immediate-complete on zero (#287 grace defaults to 30s in production)
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
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;
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
                &mut live_entry_ctx,
            )
            .await
            .unwrap();
            // Mirror the run_paper_loop guard: drop stale ctx when
            // the position machine is back to Flat. Tests that drive
            // multi-cycle scenarios depend on this resetting.
            if matches!(machine.phase(), Phase::Flat) {
                live_entry_ctx = None;
            }
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, 38).await;
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, 38).await;
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, 38).await;
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
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq).with_equity(dec!(50), dec!(50)));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, 38).await;
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
        assert_eq!(
            machine.phase(),
            Phase::Flat,
            "state machine untouched on skip"
        );
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
            async fn read_equity_usd(&self, _venue: Venue) -> Result<Option<Decimal>> {
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, 38).await;
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, 38).await;
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
emergency_complete_grace_ms: 0  # tests assert immediate-complete on zero (#287)
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, total).await;
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
                crate::trade::execution::venue_ops::ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(1),
                    terminal: true,
                    cancelled: false,
                }),
            );
            // default_fill stays at OrderFillStatus::default() = zero
            // non-terminal so subsequent polls (the exit cycle) keep
            // returning "still in flight".
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, total).await;
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
                crate::trade::execution::venue_ops::ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(1),
                    terminal: true,
                    cancelled: false,
                }),
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, total).await;
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
        seq: std::sync::Mutex<
            std::collections::VecDeque<crate::trade::execution::emergency_loop::LegQtys>,
        >,
    }

    impl ScriptedLegReader {
        fn new(seq: Vec<crate::trade::execution::emergency_loop::LegQtys>) -> Self {
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
            enter_timeout_consec_fail_to_kill: 5,
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
        let mut first_zero: Option<u64> = None;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            &mut first_zero,
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
        let mut first_zero: Option<u64> = None;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            &mut first_zero,
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
        let mut first_zero: Option<u64> = None;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            &mut first_zero,
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
        let mut first_zero: Option<u64> = None;

        // Call at t=10_000 (only 9 s elapsed, < 30 s retry interval).
        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            &mut first_zero,
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
            &mut first_zero,
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
                s.close_all
                    .push_back(crate::trade::execution::venue_ops::ScriptedResponse::Err(
                        "reduce-only rejected".into(),
                    ));
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
        let mut first_zero: Option<u64> = None;

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
                &mut first_zero,
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

        assert!(
            stuck.is_stuck(),
            "STUCK file must be armed after 5 failures"
        );
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
            &mut first_zero,
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
        let mut first_zero: Option<u64> = None;

        drive_emergency_flatten_round(
            &live,
            &mut machine,
            &mut open_qty,
            &mut stuck,
            &mut summary,
            &mut last,
            &mut attempts,
            &mut first_zero,
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
        let (machine, summary, open_qty) = drive_live_ticks(&cfg, &*hub, &live, total).await;
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

    // ---------------------------------------------------------------
    // Sprint 5 step 1 — compute_realised_pnl helper (#268 S5-1)
    // ---------------------------------------------------------------

    #[test]
    fn pnl_long_no_fees_profits_when_spread_widens() {
        // Long: bought ext at 100, sold lt at 100 (spread 0).
        // Exit at ext=110, lt=105 (spread +5). Profit per unit = 5.
        let pnl = compute_realised_pnl(
            SpreadDirection::Long,
            dec!(100),
            dec!(100),
            dec!(110),
            dec!(105),
            dec!(1),
            dec!(1),
            dec!(1),
            dec!(1),
            0.0,
            0.0,
        );
        assert_eq!(pnl, dec!(5));
    }

    #[test]
    fn pnl_short_no_fees_profits_when_spread_compresses() {
        // Short: sold ext at 110, bought lt at 100 (spread +10).
        // Exit at ext=105, lt=100 (spread +5). Profit per unit = 5.
        let pnl = compute_realised_pnl(
            SpreadDirection::Short,
            dec!(110),
            dec!(100),
            dec!(105),
            dec!(100),
            dec!(1),
            dec!(1),
            dec!(1),
            dec!(1),
            0.0,
            0.0,
        );
        assert_eq!(pnl, dec!(5));
    }

    #[test]
    fn pnl_long_no_fees_loses_when_spread_compresses() {
        // Long bet but spread moved the wrong way.
        let pnl = compute_realised_pnl(
            SpreadDirection::Long,
            dec!(100),
            dec!(100),
            dec!(105),
            dec!(110),
            dec!(1),
            dec!(1),
            dec!(1),
            dec!(1),
            0.0,
            0.0,
        );
        assert_eq!(pnl, dec!(-5));
    }

    #[test]
    fn pnl_fees_subtract_per_leg_per_side() {
        // Spread unchanged → gross 0. Fees: 5 bps on Extended,
        // 2 bps on Lighter. Each leg: notional = mid * qty per side
        // (entry + exit).
        // ext fee: (100 * 1 + 100 * 1) * 5 / 10000 = 0.1
        // lt fee:  (100 * 1 + 100 * 1) * 2 / 10000 = 0.04
        // pnl = 0 - 0.1 - 0.04 = -0.14
        let pnl = compute_realised_pnl(
            SpreadDirection::Long,
            dec!(100),
            dec!(100),
            dec!(100),
            dec!(100),
            dec!(1),
            dec!(1),
            dec!(1),
            dec!(1),
            5.0,
            2.0,
        );
        // Round to 4 dp to absorb f64-derived rate noise.
        let rounded = pnl.round_dp(4);
        assert_eq!(rounded, dec!(-0.14));
    }

    #[test]
    fn pnl_zero_exit_qty_returns_zero() {
        let pnl = compute_realised_pnl(
            SpreadDirection::Long,
            dec!(100),
            dec!(100),
            dec!(110),
            dec!(105),
            dec!(1),
            dec!(1),
            Decimal::ZERO,
            dec!(1),
            5.0,
            0.0,
        );
        assert_eq!(pnl, Decimal::ZERO);
    }

    #[test]
    fn pnl_uses_min_exit_qty_for_gross() {
        // Asymmetric exit fills: ext=0.5, lt=1.0. The realised
        // delta-neutral qty is min = 0.5. Gross = 5 * 0.5 = 2.5.
        // Fees apply to each leg's ACTUAL exit qty (not the min).
        let pnl = compute_realised_pnl(
            SpreadDirection::Long,
            dec!(100),
            dec!(100),
            dec!(110),
            dec!(105),
            dec!(1),
            dec!(1),
            dec!(0.5),
            dec!(1),
            0.0,
            0.0,
        );
        assert_eq!(pnl, dec!(2.5));
    }

    #[test]
    fn pnl_short_with_fees_realistic_scenario() {
        // Realistic Phase 3 case: short ETH spread, +30 bps spread
        // entry, +20 bps spread exit (10 bps compression), $50
        // notional. The expected economics are a wash at 5 bps
        // Extended fee: 10 bps gross × $50 = $0.05 ≈ Extended fees
        // (5 bps × $50 × 2 sides = $0.05), so net ≈ 0.
        //
        // This is the threshold the strategy needs to clear: at
        // ≥10 bps captured the per-trade economics are positive
        // before slippage. Sub-10 bps captures will lose money.
        let pnl = compute_realised_pnl(
            SpreadDirection::Short,
            dec!(2006),
            dec!(2000),
            dec!(2004),
            dec!(2000),
            dec!(0.025),
            dec!(0.025),
            dec!(0.025),
            dec!(0.025),
            5.0,
            0.0,
        );
        let pnl_f64 = rust_decimal::prelude::ToPrimitive::to_f64(&pnl).unwrap();
        // Expect roughly zero — bounded by typical f64-derived
        // rounding noise (< $0.005 on a $50 notional).
        assert!(
            pnl_f64.abs() < 0.005,
            "expected ~$0 (10 bps gross ≈ 5 bps × 2 sides fees); got {}",
            pnl_f64
        );
    }

    #[tokio::test(start_paused = true)]
    async fn live_exit_happy_path_records_realised_pnl() {
        // End-to-end: live happy path lands a non-zero PnL on
        // summary.last_realised_pnl_usd. With sustained breach mids
        // (entry and exit at the same +20 bps spread) the gross is
        // zero, so the recorded PnL equals -fees ≈ -$0.20 with
        // default 5 bps Extended + 0 bps Lighter and qty=1.
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
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, _) = drive_live_ticks(&cfg, &*hub, &live, total).await;
        assert_eq!(machine.phase(), Phase::Flat);
        assert!(
            summary.last_realised_pnl_usd.is_some(),
            "live exit happy path must record realised PnL"
        );
        let pnl = summary.last_realised_pnl_usd.unwrap();
        // Sustained breach → entry_spread == exit_spread → gross = 0.
        // Fees consume the value: ext 5 bps × ~4004 notional × 1 qty
        // = ~$2.0. Lighter free. Final PnL ≈ -$2.0.
        assert!(
            pnl < 0.0,
            "fees should consume sustained-spread PnL → negative; got {}",
            pnl
        );
        assert!(
            pnl > -3.0,
            "loss should be bounded by total fees (~$2 expected); got {}",
            pnl
        );
    }

    #[tokio::test(start_paused = true)]
    async fn live_exit_per_venue_qty_diverges_when_entry_was_asymmetric() {
        // S5-2: when entry produces different per-venue fills (e.g.
        // Extended partial-fill 0.5 + Lighter full-fill 1.0), the
        // exit MUST close each venue's actual open qty, not a single
        // cached value. Pre-S5-2 both legs would have used the same
        // `open_qty` cache, over- or under-targeting one leg.
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let (ext_seq, lt_seq) = breach_then_hold_sequence(70);
        let total = ext_seq.len() as u64;
        let hub = Arc::new(ScriptedHub::new(ext_seq, lt_seq));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        ext_vops.with_state(|s| {
            // Asymmetric fills: Extended 0.5, Lighter 1.0. Both
            // terminal so the chase loops short-circuit on first
            // poll — keeps the test deterministic without juggling
            // queues.
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(0.5),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_qty: dec!(1.0),
                terminal: true,
                cancelled: false,
            };
        });
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (machine, summary, _open_qty) = drive_live_ticks(&cfg, &*hub, &live, total).await;
        assert!(summary.decisions_enter_short >= 1);
        assert!(summary.decisions_exit >= 1);
        assert_eq!(
            machine.phase(),
            Phase::Flat,
            "exit completes even on asymmetric fills"
        );
        // Two takers per venue: one entry, one exit.
        let ext_takers = ext_vops.snapshot_takers();
        let lt_takers = lt_vops.snapshot_takers();
        assert_eq!(ext_takers.len(), 2, "ext takers: entry + exit");
        assert_eq!(lt_takers.len(), 2, "lt takers: entry + exit");
        // Exit (index 1) must use per-venue qty matching what each
        // venue actually has open. The position machine recorded
        // ExtendedFilled{0.5} + LighterFilled{1.0} during entry,
        // so the exit's target_qty diverges per leg.
        assert_eq!(
            ext_takers[1].2,
            dec!(0.5),
            "ext exit qty must match position.extended_open_qty (0.5)"
        );
        assert_eq!(
            lt_takers[1].2,
            dec!(1.0),
            "lt exit qty must match position.lighter_open_qty (1.0)"
        );
        // Both exit takers reduce_only=true.
        assert!(ext_takers[1].3, "ext exit must be reduce_only");
        assert!(lt_takers[1].3, "lt exit must be reduce_only");
    }

    // ---------------------------------------------------------------
    // Sprint 5 step 3 — forced flatten on session DD halt (#268 S5-3)
    // ---------------------------------------------------------------

    /// Helper: drive a position machine into Held with given qtys
    /// so the S5-3 check has something to flatten. Mirrors the
    /// happy-path entry sequence the runner emits in live mode.
    fn machine_in_held(qty: Decimal) -> PositionMachine {
        let mut m = PositionMachine::new();
        m.apply(
            0,
            Event::EntrySignal {
                direction: SpreadDirection::Long,
                notional_usd: dec!(50),
            },
        )
        .unwrap();
        m.apply(100, Event::ExtendedFilled { qty }).unwrap();
        m.apply(200, Event::LighterFilled { qty }).unwrap();
        assert_eq!(m.phase(), Phase::Held);
        m
    }

    /// RiskManager pre-armed with a session-DD halt — feed two
    /// equity samples a 6% apart so the rolling-peak check trips
    /// the configured 500 bps threshold.
    fn pre_armed_session_dd_manager() -> (tempfile::TempDir, RiskManager) {
        let (dir, mut rm) = test_risk_manager();
        rm.record_equity_sample(1_000.0, 0);
        rm.record_equity_sample(940.0, 60);
        assert_eq!(
            rm.block_reason(60),
            Some(BlockReason::SessionDdHalted),
            "test pre-arm: session DD halt did not activate"
        );
        (dir, rm)
    }

    #[tokio::test]
    async fn live_session_dd_halt_forces_flatten_when_held() {
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let mut machine = machine_in_held(dec!(0.025));
        // Single tick — the breach data doesn't matter because we
        // expect the S5-3 check to fire Emergency before signal
        // evaluation. Use a one-element ScriptedHub with a quiet
        // mid pair so read_mid succeeds.
        let hub = Arc::new(ScriptedHub::new(
            vec![mid(1_000, 2_000.0)],
            vec![mid(1_000, 2_000.0)],
        ));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (_rm_dir, mut rm) = pre_armed_session_dd_manager();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut summary = LivePaperSummary::default();
        let mut open_qty = Some(dec!(0.025));
        let mut rg = test_reference_guard();
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup {
            ext_ready: true,
            lt_ready: true,
        };
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;

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
            Some(&live),
            &mut live_entry_ctx,
        )
        .await
        .unwrap();

        assert_eq!(
            machine.phase(),
            Phase::EmergencyFlattening,
            "session_dd halt while held MUST force flatten"
        );
        assert_eq!(summary.live_session_dd_forced_flattens, 1);
        assert_eq!(
            machine.position().and_then(|p| p.last_emergency_reason),
            Some(EmergencyReason::SessionDdHalted),
            "position must record SessionDdHalted as the emergency reason"
        );
    }

    #[tokio::test]
    async fn live_session_dd_halt_does_not_re_emergency_after_first_fire() {
        // Drive 3 ticks with the halt persistently active. The S5-3
        // counter should bump exactly once (first tick when phase
        // was still Held); subsequent ticks see phase ==
        // EmergencyFlattening and skip via the position-is-some
        // gate. Idempotency by phase, not by an explicit flag.
        let mut cfg = live_test_cfg();
        cfg.dry_run = false;
        let mut machine = machine_in_held(dec!(0.025));
        let hub = Arc::new(ScriptedHub::new(
            vec![
                mid(1_000, 2_000.0),
                mid(2_000, 2_000.0),
                mid(3_000, 2_000.0),
            ],
            vec![
                mid(1_000, 2_000.0),
                mid(2_000, 2_000.0),
                mid(3_000, 2_000.0),
            ],
        ));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (_rm_dir, mut rm) = pre_armed_session_dd_manager();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut summary = LivePaperSummary::default();
        let mut open_qty = Some(dec!(0.025));
        let mut rg = test_reference_guard();
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup {
            ext_ready: true,
            lt_ready: true,
        };
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;

        for _ in 0..3 {
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
                Some(&live),
                &mut live_entry_ctx,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            summary.live_session_dd_forced_flattens, 1,
            "forced-flatten counter must increment exactly once per halt entry"
        );
        assert_eq!(machine.phase(), Phase::EmergencyFlattening);
    }

    #[tokio::test]
    async fn live_session_dd_halt_dry_run_does_not_force_flatten() {
        // Paper mode — the S5-3 check must skip even with the halt
        // armed and a position in Held. The existing dry_run
        // synthesise paths handle paper-mode emergency flow.
        let mut cfg = live_test_cfg();
        cfg.dry_run = true;
        let mut machine = machine_in_held(dec!(0.025));
        let hub = Arc::new(ScriptedHub::new(
            vec![mid(1_000, 2_000.0)],
            vec![mid(1_000, 2_000.0)],
        ));
        let ext_vops = Arc::new(ScriptedVenueOps::new());
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        let live = live_with_scripted(&cfg, ext_vops.clone(), lt_vops.clone());
        let (_rm_dir, mut rm) = pre_armed_session_dd_manager();
        let mut spread = SpreadEngine::new(cfg.spread_config());
        let mut signal = SignalEngine::new(cfg.signal_config());
        let mut summary = LivePaperSummary::default();
        let mut open_qty = Some(dec!(0.025));
        let mut rg = test_reference_guard();
        let (_st_dir, mut stuck) = test_stuck();
        let mut warmup = VenueWarmup {
            ext_ready: true,
            lt_ready: true,
        };
        let mut ws_health = WsHealthMonitor::new(0);
        let mut skew_monitor = SkewMonitor::new(0.0);
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;

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
            Some(&live),
            &mut live_entry_ctx,
        )
        .await
        .unwrap();

        // Paper mode never enters EmergencyFlattening via the S5-3
        // path. The phase outcome here depends on whether signal
        // happens to fire a synthetic Exit, which is incidental;
        // what matters is the counter stays zero.
        assert_eq!(
            summary.live_session_dd_forced_flattens, 0,
            "dry_run must not increment the forced-flatten counter"
        );
        assert_ne!(
            machine.phase(),
            Phase::EmergencyFlattening,
            "S5-3 must not push paper mode into EmergencyFlattening"
        );
    }
}
