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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use dex_connector::OrderSide as DcOrderSide;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use super::config::XvenueConfig;
use super::live_exec::LiveExecution;
use super::live_pnl::compute_realised_pnl;
use super::signal::{SignalEngine, SpreadDirection};
use super::spread::SpreadEngine;
use super::state::{Event, PositionMachine};
use super::status::StatusReporter;
use crate::risk::kill_switch::StuckTripwire;
use crate::risk::manager::RiskManager;
use crate::risk::reference_guard::ReferenceGuard;
use crate::risk::skew_monitor::SkewMonitor;
use crate::risk::ws_health::WsHealthMonitor;
use crate::trade::execution::types::avg_price_from_value_qty;
use crate::trade::execution::venue_ops::FillRecord;

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
    pub(super) fn is_ready(&self, venue: Venue) -> bool {
        match venue {
            Venue::Extended => self.ext_ready,
            Venue::Lighter => self.lt_ready,
        }
    }

    pub(super) fn mark_ready(&mut self, venue: Venue) {
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
    /// bot-strategy#429: count of `Decision::Enter` outcomes dropped
    /// because the defensive entry filter observed an unhealthy Lighter
    /// regime over the recent rolling window (inside-spread spike
    /// and/or top-of-book depth collapse). 0 when both filter
    /// thresholds are disabled in YAML.
    pub entries_blocked_by_entry_filter: u64,
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
    /// bot-strategy#431 Phase 0(c): would-be Extended-side maker fill
    /// telemetry. Paper-mode counterparts to `would_be_maker_*` above
    /// but evaluated against the Extended book (ext_snap.bid_size /
    /// ask_size) so the Phase 0 24h soak can compare Extended's
    /// touch-fill behavior against Lighter's. Same seed-mixed
    /// linear-decay-by-depth model — Phase 1 implementation may
    /// supersede with a venue-specific fit if Extended's aggressor
    /// population invalidates the linear approximation.
    pub would_be_ext_maker_attempts: u64,
    pub would_be_ext_maker_fills: u64,
    pub would_be_ext_maker_p_sum: f64,
    pub would_be_ext_maker_exit_attempts: u64,
    pub would_be_ext_maker_exit_fills: u64,
    pub would_be_ext_maker_exit_p_sum: f64,
    /// bot-strategy#330 follow-up: per-RT touch-to-touch projected PnL
    /// for paper mode. The mid-to-mid `dev_bps` in `[XVENUE] PAPER ENTER`
    /// / `PAPER EXIT` overstates capturable edge because it ignores
    /// Extended's half-spread cross and Lighter's maker-rebate /
    /// taker-cost asymmetry. These counters track the calibrated
    /// projection (computed in `paper_pnl_projection` at exit time):
    /// `paper_net_attempts` increments per completed RT with a captured
    /// entry ctx, `paper_gross_bps_sum` is the cumulative touch-level
    /// gross, `paper_net_bps_sum` is gross minus fees and minus/plus
    /// Lighter half-spread depending on `sampled_fill` at entry/exit.
    /// Surfaced in `[STATUS]` as `paper_n / paper_gross_bps_avg /
    /// paper_net_bps_avg` so the LIVE re-probe gate can compare against
    /// the Phase 0 BT M5 cell (≥+5 bps/RT net) using the same calibration
    /// the live executor will see.
    pub paper_net_attempts: u64,
    pub paper_gross_bps_sum: f64,
    pub paper_net_bps_sum: f64,
    /// Captured at `Decision::Enter` (paper branch) and consumed at the
    /// matching `Decision::Exit`. Internal bookkeeping for the projected
    /// PnL line. Cleared when the position machine returns to Flat
    /// outside of a normal exit (mirrors `live_entry_ctx` semantics in
    /// the run loop).
    pub paper_entry_ctx: Option<PaperEntryCtx>,
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
    /// Equity samples skipped because one venue returned a value but
    /// the other did not (bot-strategy#360). Recording such a sample
    /// halves the rolling-peak equity and trips a spurious session_dd
    /// halt during single-venue maintenance. Boot-time "all venues
    /// unavailable" stays silent and does not increment this counter.
    pub equity_samples_skipped_partial: u64,
    /// Flipped true the first time `read_total_equity_for_sample`
    /// observes a positive equity from both venues. Pre-init this
    /// gates `update_equity(0)` so `equity_day_start` does not lock
    /// to 0 during the dex-connector's WS-cache warm-up window,
    /// inflating `pnl_today` by the full equity once the real
    /// balance lands. See bot-strategy#382 (the pairtrade companion
    /// fix at pairtrade@1063983). Post-init zero readings ARE
    /// accepted — a genuinely rekt bot must still surface on
    /// dashboards.
    pub equity_initialized: bool,
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
    /// Per-venue mid at the time the entry leg landed. Used as the
    /// fallback when the executor did not surface an avg fill price
    /// for the entry round (e.g. dry-run paper synthesis, reduce-only
    /// "Position is missing" short-circuit). bot-strategy#435.
    pub ext_entry_mid: Decimal,
    pub lt_entry_mid: Decimal,
    /// Volume-weighted average fill price for the entry leg, derived
    /// from the venue's per-partial-fill metadata via
    /// `*Terminal::Filled.avg_fill_price`. `None` means at least one
    /// contributing round did not surface a fill value — downstream
    /// `compute_realised_pnl` falls back to `*_entry_mid` for the
    /// affected leg. bot-strategy#435.
    pub ext_entry_avg_fill_price: Option<Decimal>,
    pub lt_entry_avg_fill_price: Option<Decimal>,
    pub ext_entry_qty: Decimal,
    pub lt_entry_qty: Decimal,
}

/// bot-strategy#330 follow-up: captured at the paper-mode entry to fund
/// a calibrated projected-PnL line at the matching paper exit. The
/// mid-to-mid `dev_bps` already in `[XVENUE] PAPER ENTER` is the signal
/// value; this struct adds the touch-level prices + the sampled maker
/// outcome so the exit-time projection mirrors what a live executor
/// would actually capture (Ext crosses the half-spread as taker; Lt
/// either earns or pays the half-spread depending on whether the
/// post-only filled).
#[derive(Debug, Clone)]
pub struct PaperEntryCtx {
    pub direction: SpreadDirection,
    pub ext_entry_mid: Decimal,
    pub ext_entry_bid: Decimal,
    pub ext_entry_ask: Decimal,
    pub lt_entry_mid: Decimal,
    pub lt_entry_bid: Decimal,
    pub lt_entry_ask: Decimal,
    pub qty: Decimal,
    /// `sampled_fill` from `would_be_maker_fill_outcome` at entry. None
    /// when the helper returned None (book read unusable, scripted-hub
    /// tests with default-zero sizes). We treat None as taker for the
    /// projection (conservative).
    pub maker_entry: Option<bool>,
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
    // bot-strategy#429: rolling-window quote history for the defensive
    // entry filter. Always live; the filter itself is opt-in via the
    // two `entry_filter_lt_*` thresholds, so when neither is set this
    // costs ~one push per tick and does not gate anything.
    let mut quote_history =
        super::entry_filter::RecentQuoteHistory::new(cfg.entry_filter_window_sec);
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
    // bot-strategy#434: per-venue snapshots of {trade_id} captured on
    // the first tick after the phase transitions into
    // EmergencyFlattening. At EmergencyComplete the handler lists
    // fills again, diffs against these baselines, and aggregates the
    // new entries (filtered to the reduce-only side per the round
    // trip's direction) into a volume-weighted average exit price
    // that feeds `compute_realised_pnl`. Reset to None whenever the
    // handler's reset branch runs (phase no longer EmergencyFlattening).
    let mut emergency_ext_pre_fills: Option<HashSet<String>> = None;
    let mut emergency_lt_pre_fills: Option<HashSet<String>> = None;
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
        super::live_status::refresh_equity(&*hub, r, &mut risk_manager, &mut summary).await;
        r.mark_dirty();
        super::live_status::publish_risk(&risk_manager, r);
        super::live_status::publish_kill_switch(&cfg, r);
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
                if let Err(e) = super::live_tick::run_one_tick(
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
                    &mut quote_history,
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
                    &*hub,
                    live_exec.as_deref(),
                    &mut machine,
                    &mut open_qty,
                    &mut stuck,
                    &mut summary,
                    &mut last_emergency_attempt_ms,
                    &mut emergency_attempts,
                    &mut first_emergency_zero_ms,
                    &mut emergency_ext_pre_fills,
                    &mut emergency_lt_pre_fills,
                    &mut live_entry_ctx,
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
                    // bot-strategy#330 follow-up: mirror the live ctx
                    // sweep on the paper-mode projection ctx so a
                    // EmergencyComplete / Reset path doesn't carry
                    // stale entry state into the next round-trip.
                    summary.paper_entry_ctx = None;
                }
            }

            _ = status_ivl.tick() => {
                super::live_status::report_status_tick(
                    &cfg,
                    &*hub,
                    &mut summary,
                    &ws_health,
                    &machine,
                    &mut risk_manager,
                    &stuck,
                    reporter.as_mut(),
                ).await;
            }
        }
    }

    risk_manager.flush();
    Ok(summary)
}

pub(super) fn now_unix_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

pub(super) fn wall_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `true` when the external KILL_SWITCH file exists. File-presence is
/// the source of truth — no caching, no edge-trigger persistence —
/// so removing the file resumes entries on the next tick (#244 D-1).
pub(super) fn kill_switch_active(path: &str) -> bool {
    !path.is_empty() && std::path::Path::new(path).exists()
}

/// Live-mode emergency-flatten round driver invoked from the tick arm
/// of `run_paper_loop`. Wrapper around `drive_emergency_flatten_round`
/// that also (a) resets the throttle state when the position machine
/// is *not* in EmergencyFlattening, and (b) on the tick the round
/// emits `EmergencyComplete` computes a real realised-PnL figure from
/// the venue fill diff (bot-strategy#434, replaces the pre-#434 0.0
/// placeholder). Skips entirely when `live_exec` is None (paper-mode
/// loops have no orders to flatten).
///
/// The pre/post-emergency fill snapshots are captured per-venue and
/// diffed by `trade_id`: `close_all_positions` does not surface the
/// IOC order ids it issues, so the trade_id diff is the cheapest way
/// to attribute fills that landed only because of the recovery. The
/// diff is filtered to the reduce-only side per the round trip's
/// direction so any unrelated fills on the venue (vanishingly rare on
/// xvenue-arb's single-symbol bot but defensible) do not pollute the
/// figure.
#[allow(clippy::too_many_arguments)]
async fn handle_emergency_flatten_tick<H: VenueHub + ?Sized>(
    cfg: &XvenueConfig,
    hub: &H,
    live_exec: Option<&LiveExecution>,
    machine: &mut PositionMachine,
    open_qty: &mut Option<Decimal>,
    stuck: &mut StuckTripwire,
    summary: &mut LivePaperSummary,
    last_emergency_attempt_ms: &mut Option<u64>,
    emergency_attempts: &mut u32,
    first_emergency_zero_ms: &mut Option<u64>,
    emergency_ext_pre_fills: &mut Option<HashSet<String>>,
    emergency_lt_pre_fills: &mut Option<HashSet<String>>,
    live_entry_ctx: &mut Option<LiveEntryCtx>,
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
        // bot-strategy#434: drop the pre-emergency snapshot too — the
        // next entry's pre-snapshot must reflect the post-EmergencyComplete
        // fill state, not the previous emergency's.
        *emergency_ext_pre_fills = None;
        *emergency_lt_pre_fills = None;
        return;
    }

    // bot-strategy#434: capture the pre-emergency fill baseline on the
    // first tick the handler runs inside `EmergencyFlattening`. The
    // close_all does not happen until `drive_emergency_flatten_round`
    // below, so this snapshot is guaranteed to predate any emergency-
    // produced fills (the entry-side fills that put us here are
    // already present and will be filtered out by the post-EmergencyComplete
    // diff).
    if emergency_ext_pre_fills.is_none() {
        match live.ext_ops.list_filled_orders(&live.ext_symbol).await {
            Ok(fills) => {
                *emergency_ext_pre_fills =
                    Some(fills.into_iter().map(|f| f.trade_id).collect());
            }
            Err(e) => {
                log::warn!(
                    "[XVENUE/emerg] pre-snapshot ext list_filled_orders err={:?} \
                     — emergency PnL will fall back to 0.0 placeholder for this cycle",
                    e
                );
                // Mark as Some(empty) so we don't keep retrying every
                // tick — if the post-EmergencyComplete diff sees no
                // baseline it would over-count.
                *emergency_ext_pre_fills = Some(HashSet::new());
            }
        }
    }
    if emergency_lt_pre_fills.is_none() {
        match live.lt_ops.list_filled_orders(&live.lt_symbol).await {
            Ok(fills) => {
                *emergency_lt_pre_fills = Some(fills.into_iter().map(|f| f.trade_id).collect());
            }
            Err(e) => {
                log::warn!(
                    "[XVENUE/emerg] pre-snapshot lt list_filled_orders err={:?} \
                     — emergency PnL will fall back to 0.0 placeholder for this cycle",
                    e
                );
                *emergency_lt_pre_fills = Some(HashSet::new());
            }
        }
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
    if summary.emergency_completes > prev_emergency_completes {
        // bot-strategy#434: compute realised PnL from the venue fill
        // diff instead of the pre-#434 0.0 placeholder. The take()
        // consumes both the entry ctx and the pre-snapshots so a
        // subsequent EmergencyComplete (different round-trip) starts
        // fresh.
        let pre_ext = emergency_ext_pre_fills.take().unwrap_or_default();
        let pre_lt = emergency_lt_pre_fills.take().unwrap_or_default();
        let entry_ctx = live_entry_ctx.take();
        let pnl_usd = compute_emergency_realised_pnl(
            cfg, live, hub, entry_ctx, pre_ext, pre_lt,
        )
        .await;
        if let Some(r) = reporter {
            r.record_close(pnl_usd);
        }
        risk_manager.record_close(pnl_usd, now_unix_secs());
        summary.last_realised_pnl_usd = Some(pnl_usd);
        log::info!(
            "[XVENUE/emerg] record_close fired with realised pnl_usd={:.4} (bot-strategy#434)",
            pnl_usd
        );
    }
}

/// bot-strategy#434 helper — derive the realised PnL of one emergency
/// recovery from:
///
/// * `entry_ctx` — captured at the original `Decision::Enter` when both
///   legs filled. Provides direction + per-venue entry qty + entry
///   avg fill price (real, from the executor terminal). `None` when the
///   round trip went into emergency without ever producing a balanced
///   position (Lighter failed after Extended on entry), in which case
///   the helper falls back to 0.0 and logs a warning — the
///   asymmetric-entry case is rare enough on xvenue-arb's current state
///   that the additional plumbing is deferred.
/// * `pre_ext_trade_ids` / `pre_lt_trade_ids` — baseline `{trade_id}`
///   snapshots captured on the first emergency tick. `list_filled_orders`
///   returns the full fill history the venue's WS cache holds; anything
///   NOT in the baseline must have landed during the recovery.
///
/// Exit prices fall back to current mid (read from `hub.read_mid`) per
/// leg when the venue layer did not surface a `filled_value` on the
/// reduce-only fills. The mid fallback under-reports IOC slippage on
/// `close_all` but is materially better than the pre-#434 0.0
/// placeholder; the cleanest case (Extended-side fills with
/// `filled_value` populated) yields a fill-accurate figure.
async fn compute_emergency_realised_pnl<H: VenueHub + ?Sized>(
    cfg: &XvenueConfig,
    live: &LiveExecution,
    hub: &H,
    entry_ctx: Option<LiveEntryCtx>,
    pre_ext_trade_ids: HashSet<String>,
    pre_lt_trade_ids: HashSet<String>,
) -> f64 {
    let Some(ctx) = entry_ctx else {
        log::warn!(
            "[XVENUE/emerg] entry ctx unavailable — recording placeholder pnl=0.0 \
             (likely asymmetric-entry emergency where no balanced position landed)"
        );
        return 0.0;
    };

    let ext_post = match live.ext_ops.list_filled_orders(&live.ext_symbol).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!(
                "[XVENUE/emerg] post-snapshot ext list_filled_orders err={:?} \
                 — recording placeholder pnl=0.0",
                e
            );
            return 0.0;
        }
    };
    let lt_post = match live.lt_ops.list_filled_orders(&live.lt_symbol).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!(
                "[XVENUE/emerg] post-snapshot lt list_filled_orders err={:?} \
                 — recording placeholder pnl=0.0",
                e
            );
            return 0.0;
        }
    };

    // The reduce-only side per venue is the opposite of the entry side.
    let (ext_close_side, lt_close_side) = match ctx.direction {
        SpreadDirection::Long => (DcOrderSide::Short, DcOrderSide::Long),
        SpreadDirection::Short => (DcOrderSide::Long, DcOrderSide::Short),
    };

    let (ext_exit_qty, ext_exit_value) =
        aggregate_emergency_fills(&ext_post, &pre_ext_trade_ids, ext_close_side);
    let (lt_exit_qty, lt_exit_value) =
        aggregate_emergency_fills(&lt_post, &pre_lt_trade_ids, lt_close_side);

    let ext_exit_avg = avg_price_from_value_qty(ext_exit_value, ext_exit_qty);
    let lt_exit_avg = avg_price_from_value_qty(lt_exit_value, lt_exit_qty);

    // Fallback exit mids — read fresh so the avg_price_from_value_qty
    // miss (no filled_value) doesn't fall back to the entry mid. If
    // either read fails we fall back to the entry mid (best available).
    let ext_exit_mid = hub
        .read_mid(Venue::Extended)
        .await
        .ok()
        .map(|s| s.mid)
        .filter(|m| *m > Decimal::ZERO)
        .unwrap_or(ctx.ext_entry_mid);
    let lt_exit_mid = hub
        .read_mid(Venue::Lighter)
        .await
        .ok()
        .map(|s| s.mid)
        .filter(|m| *m > Decimal::ZERO)
        .unwrap_or(ctx.lt_entry_mid);

    let pnl = compute_realised_pnl(
        ctx.direction,
        ctx.ext_entry_mid,
        ctx.lt_entry_mid,
        ext_exit_mid,
        lt_exit_mid,
        ctx.ext_entry_avg_fill_price,
        ctx.lt_entry_avg_fill_price,
        ext_exit_avg,
        lt_exit_avg,
        ctx.ext_entry_qty,
        ctx.lt_entry_qty,
        ext_exit_qty,
        lt_exit_qty,
        cfg.extended_fee_bps,
        cfg.lighter_fee_bps,
    );
    log::info!(
        "[XVENUE/emerg] realised pnl breakdown: dir={:?} \
         ext_entry_qty={} lt_entry_qty={} ext_exit_qty={} lt_exit_qty={} \
         ext_entry_avg={:?} lt_entry_avg={:?} ext_exit_avg={:?} lt_exit_avg={:?} \
         ext_exit_mid={} lt_exit_mid={} pnl_usd={}",
        ctx.direction,
        ctx.ext_entry_qty,
        ctx.lt_entry_qty,
        ext_exit_qty,
        lt_exit_qty,
        ctx.ext_entry_avg_fill_price,
        ctx.lt_entry_avg_fill_price,
        ext_exit_avg,
        lt_exit_avg,
        ext_exit_mid,
        lt_exit_mid,
        pnl,
    );
    pnl.to_f64().unwrap_or(0.0)
}

/// Sum `(filled_size, filled_value)` across the post-snapshot fills
/// whose `trade_id` is not in the pre-snapshot baseline AND whose side
/// matches the reduce-only direction for the leg. Returns
/// `(qty=0, value=None)` when no matching fills exist; the caller's
/// `avg_price_from_value_qty` handles that case (returns None →
/// `compute_realised_pnl` falls back to mid for the leg).
fn aggregate_emergency_fills(
    post: &[FillRecord],
    pre_trade_ids: &HashSet<String>,
    close_side: DcOrderSide,
) -> (Decimal, Option<Decimal>) {
    let mut qty = Decimal::ZERO;
    let mut value_sum = Decimal::ZERO;
    let mut any_value = false;
    for f in post {
        if pre_trade_ids.contains(&f.trade_id) {
            continue;
        }
        if f.side != close_side {
            continue;
        }
        qty += f.filled_size;
        if let Some(v) = f.filled_value {
            value_sum += v;
            any_value = true;
        }
    }
    let value = if any_value { Some(value_sum) } else { None };
    (qty, value)
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
    use super::super::emergency_handlers::book_depth_blocks_entry;
    use super::super::entry_dispatch::handle_decision_enter;
    use super::super::entry_filter::RecentQuoteHistory;
    use super::super::exit_dispatch::handle_decision_exit;
    use super::super::live_pnl::{
        compute_realised_pnl, paper_pnl_projection, would_be_maker_fill_outcome,
    };
    use super::super::live_status::read_total_equity_for_sample;
    use super::super::live_tick::run_one_tick;
    use super::super::status::equity_decimal_to_f64;
    use super::*;
    use crate::risk::manager::{BlockReason, RiskConfig, RiskManager};
    use crate::xvenue::signal::ExitReason;
    use crate::xvenue::state::EmergencyReason;
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
        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
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
        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
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
            let mut quote_history = RecentQuoteHistory::new(0);
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
                &mut quote_history,
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
            let mut quote_history = RecentQuoteHistory::new(0);
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
                &mut quote_history,
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
            let mut quote_history = RecentQuoteHistory::new(0);
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
                &mut quote_history,
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
            let mut quote_history = RecentQuoteHistory::new(0);
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
                &mut quote_history,
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
            let mut quote_history = RecentQuoteHistory::new(0);
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
                &mut quote_history,
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
        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
        )
        .await
        .expect("warm-up errors must not propagate");
        assert!(warmup.ext_ready);
        assert!(!warmup.lt_ready);

        // Two more ticks drain Lighter's fail counter; the next tick
        // after that produces a successful read on both legs.
        for _ in 0..2 {
            let mut quote_history = RecentQuoteHistory::new(0);
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
                &mut quote_history,
            )
            .await
            .expect("warm-up errors must not propagate");
        }
        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
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
        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
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
        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
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
        let mut quote_history = RecentQuoteHistory::new(cfg.entry_filter_window_sec);
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
                &mut quote_history,
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
                filled_value: None,
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_value: None,
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
                filled_value: None,
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
                filled_value: None,
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

    /// Helper hub that lets tests dictate per-venue equity outcomes
    /// (Some / None / Err) so the bot-strategy#360 partial-skip path
    /// can be exercised without standing up a StatusReporter.
    struct EquityScriptHub {
        ext: std::result::Result<Option<Decimal>, &'static str>,
        lt: std::result::Result<Option<Decimal>, &'static str>,
    }

    #[async_trait::async_trait]
    impl VenueHub for EquityScriptHub {
        async fn read_mid(&self, _venue: Venue) -> Result<MidSnapshot> {
            unreachable!("read_total_equity_for_sample never reads mid")
        }
        async fn read_equity_usd(&self, venue: Venue) -> Result<Option<Decimal>> {
            let cell = match venue {
                Venue::Extended => &self.ext,
                Venue::Lighter => &self.lt,
            };
            match cell {
                Ok(opt) => Ok(*opt),
                Err(msg) => Err(anyhow::anyhow!(*msg)),
            }
        }
    }

    /// bot-strategy#360: when one venue is unreachable (here: Lighter
    /// returns Err, mirroring a maintenance window where REST fails)
    /// the partial sample must be skipped — recording only the
    /// surviving venue would halve the rolling peak and trip a
    /// spurious session_dd halt. Counter must bump for visibility.
    #[tokio::test]
    async fn refresh_equity_skips_partial_when_lighter_unavailable() {
        let hub = EquityScriptHub {
            ext: Ok(Some(dec!(497.20))),
            lt: Err("lighter rest 503 (maintenance)"),
        };
        let mut summary = LivePaperSummary::default();
        let result = read_total_equity_for_sample(&hub, &mut summary).await;
        assert_eq!(
            result, None,
            "partial sample (Extended only) must not be recorded"
        );
        assert_eq!(
            summary.equity_samples_skipped_partial, 1,
            "partial-skip must increment its counter for visibility"
        );
    }

    /// Symmetric: Extended unreachable, Lighter OK → still skipped.
    #[tokio::test]
    async fn refresh_equity_skips_partial_when_extended_unavailable() {
        let hub = EquityScriptHub {
            ext: Ok(None),
            lt: Ok(Some(dec!(499.29))),
        };
        let mut summary = LivePaperSummary::default();
        let result = read_total_equity_for_sample(&hub, &mut summary).await;
        assert_eq!(result, None);
        assert_eq!(summary.equity_samples_skipped_partial, 1);
    }

    /// Boot-time path: both venues unavailable while WS is warming up.
    /// Stay silent — no counter bump, no log spam.
    #[tokio::test]
    async fn refresh_equity_silent_when_all_venues_unavailable() {
        let hub = EquityScriptHub {
            ext: Ok(None),
            lt: Err("lighter ws not ready"),
        };
        let mut summary = LivePaperSummary::default();
        let result = read_total_equity_for_sample(&hub, &mut summary).await;
        assert_eq!(result, None);
        assert_eq!(
            summary.equity_samples_skipped_partial, 0,
            "all-failed boot case must NOT increment partial counter"
        );
    }

    /// Happy path: both venues report → record the sum.
    #[tokio::test]
    async fn refresh_equity_records_sum_when_all_venues_ok() {
        let hub = EquityScriptHub {
            ext: Ok(Some(dec!(497.20))),
            lt: Ok(Some(dec!(499.29))),
        };
        let mut summary = LivePaperSummary::default();
        let result = read_total_equity_for_sample(&hub, &mut summary).await;
        assert_eq!(result, Some(dec!(996.49)));
        assert_eq!(summary.equity_samples_skipped_partial, 0);
        assert!(
            summary.equity_initialized,
            "first positive equity sum must arm the init flag"
        );
    }

    /// bot-strategy#382 (pairtrade companion): dex-connector's WS-derived
    /// balance cache can return Ok(equity=0) before the first account
    /// dump lands. When both venues fall into that state simultaneously
    /// `read_total_equity_for_sample` would sum them to `Some(0)` and
    /// propagate to `reporter.update_equity(0)`, locking
    /// `equity_day_start` to 0 for the rest of the UTC day and
    /// inflating `pnl_today` by the full real equity once it arrives.
    /// The pre-init gate skips this case; post-init zero (legitimate
    /// rekt signal) is accepted unchanged.
    #[tokio::test]
    async fn refresh_equity_drops_zero_sum_before_init() {
        let mut summary = LivePaperSummary::default();
        assert!(!summary.equity_initialized, "default must be uninitialized");

        // Phase 1: both venues' WS caches empty — Ok(Some(0)) from each.
        let hub_zero = EquityScriptHub {
            ext: Ok(Some(Decimal::ZERO)),
            lt: Ok(Some(Decimal::ZERO)),
        };
        let result = read_total_equity_for_sample(&hub_zero, &mut summary).await;
        assert_eq!(
            result, None,
            "pre-init: zero sum must not propagate to update_equity"
        );
        assert!(
            !summary.equity_initialized,
            "zero sum must not arm the init flag"
        );
        assert_eq!(
            summary.equity_samples_skipped_partial, 0,
            "zero-sum skip is a different category — must not bump partial counter"
        );

        // Phase 2: WS dumps land — both venues report positive.
        let hub_real = EquityScriptHub {
            ext: Ok(Some(dec!(497.20))),
            lt: Ok(Some(dec!(499.29))),
        };
        let result = read_total_equity_for_sample(&hub_real, &mut summary).await;
        assert_eq!(result, Some(dec!(996.49)));
        assert!(summary.equity_initialized, "first positive must arm");

        // Phase 3: post-init, a 0 reading IS accepted — a rekt bot's
        // dashboard must reflect the loss rather than silently pin to
        // the last positive value.
        let hub_rekt = EquityScriptHub {
            ext: Ok(Some(Decimal::ZERO)),
            lt: Ok(Some(Decimal::ZERO)),
        };
        let result = read_total_equity_for_sample(&hub_rekt, &mut summary).await;
        assert_eq!(
            result,
            Some(Decimal::ZERO),
            "post-init zero must surface to dashboards"
        );
    }

    /// Reproduces the 2026-05-10 13:05 UTC incident at the risk-
    /// manager level: the spurious session_dd halt was driven by a
    /// $996 → $497 equity sample landing in the rolling peak. With
    /// the #360 fix in place, the partial sample is suppressed so
    /// `record_equity_sample` is never called and the halt does not
    /// arm.
    #[tokio::test]
    async fn refresh_equity_prevents_spurious_session_dd_halt() {
        let hub = EquityScriptHub {
            ext: Ok(Some(dec!(497.20))),
            lt: Err("lighter maintenance"),
        };
        let mut summary = LivePaperSummary::default();
        let (_dir, mut rm) = test_risk_manager();
        // Seed a healthy peak so any $497 sample would trip the
        // 500 bps session DD threshold.
        rm.record_equity_sample(996.49, 0);
        let baseline_samples = rm
            .session_snapshot()
            .expect("snapshot Some after baseline seed")
            .sample_count;

        if let Some(total) = read_total_equity_for_sample(&hub, &mut summary).await {
            rm.record_equity_sample(equity_decimal_to_f64(total), 60);
        }

        let snap = rm
            .session_snapshot()
            .expect("snapshot Some after seeding a baseline sample");
        assert_eq!(
            snap.sample_count, baseline_samples,
            "partial sample must not be recorded into the rolling peak"
        );
        assert!(
            !snap.session_halted,
            "session_dd must NOT arm on a single-venue maintenance event"
        );
        assert_eq!(summary.equity_samples_skipped_partial, 1);
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
                filled_value: None,
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_value: None,
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
                filled_value: None,
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
                    filled_value: None,
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
                filled_value: None,
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
                    filled_value: None,
                    filled_qty: dec!(1),
                    terminal: true,
                    cancelled: false,
                }),
            );
            // Exit: terminal-cancelled with zero fill (default) →
            // LighterTerminal::Failed{Cancelled}. ParallelExitLoop
            // returns `Both { ext: Filled, lt: Failed }`.
            s.default_fill = OrderFillStatus {
                filled_value: None,
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
    // bot-strategy#434 — emergency-flatten realised PnL helpers
    // ---------------------------------------------------------------

    fn fr(trade_id: &str, side: DcOrderSide, size: Decimal, value: Option<Decimal>) -> FillRecord {
        FillRecord {
            order_id: format!("o-{}", trade_id),
            trade_id: trade_id.into(),
            side,
            filled_size: size,
            filled_value: value,
            filled_ts_ms: None,
        }
    }

    #[test]
    fn aggregate_emergency_fills_keeps_only_new_trade_ids_on_close_side() {
        // Long position: ext close side is Short, lt close side is Long.
        // Pre-snapshot contains the entry trade ids; only post-snapshot
        // entries NOT in that set (and on the matching close side) count.
        let post = vec![
            // Entry (pre-snapshot ext): Long, ignored by trade-id diff.
            fr("ext-entry", DcOrderSide::Long, dec!(0.02), Some(dec!(60))),
            // Two emergency exit fills on Short — these are the targets.
            fr(
                "ext-emerg-1",
                DcOrderSide::Short,
                dec!(0.012),
                Some(dec!(36.06)),
            ),
            fr(
                "ext-emerg-2",
                DcOrderSide::Short,
                dec!(0.008),
                Some(dec!(23.92)),
            ),
            // Unrelated subsequent Long fill (e.g. next round-trip): wrong side, ignored.
            fr(
                "ext-other-long",
                DcOrderSide::Long,
                dec!(0.02),
                Some(dec!(60)),
            ),
        ];
        let pre: HashSet<String> = ["ext-entry".to_string()].into_iter().collect();
        let (qty, value) = aggregate_emergency_fills(&post, &pre, DcOrderSide::Short);
        assert_eq!(qty, dec!(0.020));
        assert_eq!(value, Some(dec!(59.98)));
    }

    #[test]
    fn aggregate_emergency_fills_returns_none_value_when_no_partial_has_filled_value() {
        // Lighter case: filled_value is None on every partial, so the
        // aggregate must surface None so the caller falls back to the
        // mid price for the leg (Lighter's WS fill stream surfaces qty
        // but not notional today).
        let post = vec![
            fr("lt-entry", DcOrderSide::Short, dec!(0.02), None),
            fr("lt-emerg-1", DcOrderSide::Long, dec!(0.014), None),
            fr("lt-emerg-2", DcOrderSide::Long, dec!(0.006), None),
        ];
        let pre: HashSet<String> = ["lt-entry".to_string()].into_iter().collect();
        let (qty, value) = aggregate_emergency_fills(&post, &pre, DcOrderSide::Long);
        assert_eq!(qty, dec!(0.020));
        assert_eq!(value, None);
    }

    #[test]
    fn aggregate_emergency_fills_short_position_picks_long_close_on_ext() {
        // Short position: ext close side is Long. Pre-snapshot has the
        // Short entry — must be ignored.
        let post = vec![
            fr(
                "ext-short-entry",
                DcOrderSide::Short,
                dec!(0.015),
                Some(dec!(45.0)),
            ),
            fr(
                "ext-emerg",
                DcOrderSide::Long,
                dec!(0.015),
                Some(dec!(45.15)),
            ),
        ];
        let pre: HashSet<String> = ["ext-short-entry".to_string()].into_iter().collect();
        let (qty, value) = aggregate_emergency_fills(&post, &pre, DcOrderSide::Long);
        assert_eq!(qty, dec!(0.015));
        assert_eq!(value, Some(dec!(45.15)));
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
            None,
            None,
            None,
            None,
            dec!(1),
            dec!(1),
            dec!(1),
            dec!(1),
            0.0,
            0.0,
        );
        assert_eq!(pnl, dec!(5));
    }

    /// bot-strategy#435: when avg_fill_price is provided for every
    /// leg + side, the function uses it instead of mids. Reproduces
    /// the 2026-05-19 05:14 UTC live cycle:
    ///
    ///   - Short direction, qty 0.023
    ///   - Mid view at ENTER: ext=2131.05, lt=2128.77
    ///   - Mid view at EXIT:  ext=2129.45, lt=2129.82
    ///   - Actual fills (Extended trade_pnl exports + Lighter CSV):
    ///       ext_entry SELL 2128.20, ext_exit BUY 2129.50
    ///       lt_entry  BUY  2129.61, lt_exit  SELL 2130.08
    ///   - Extended fees 5 bps both sides
    ///
    /// Expected: ground-truth realised across both venues comes to
    /// roughly -\$0.043 (Ext -0.054 trade -0.024 fees, Lt +0.011),
    /// vs the mid-based +\$0.0121 the pre-#435 function reported.
    /// We assert the fill-based path matches the venue export
    /// within rounding.
    #[test]
    fn pnl_uses_fill_prices_when_provided_short_cycle() {
        let pnl = compute_realised_pnl(
            SpreadDirection::Short,
            dec!(2131.05),       // ext_entry_mid (ignored — fill provided)
            dec!(2128.77),       // lt_entry_mid (ignored)
            dec!(2129.45),       // ext_exit_mid (ignored)
            dec!(2129.82),       // lt_exit_mid (ignored)
            Some(dec!(2128.20)), // ext entry fill
            Some(dec!(2129.61)), // lt entry fill
            Some(dec!(2129.50)), // ext exit fill
            Some(dec!(2130.08)), // lt exit fill
            dec!(0.023),         // ext_entry_qty
            dec!(0.023),         // lt_entry_qty (use 0.023 to keep min identical)
            dec!(0.023),         // ext_exit_qty
            dec!(0.023),         // lt_exit_qty
            5.0,                 // ext fee bps
            0.0,                 // lt fee bps
        );
        // Manual derivation:
        //   entry_spread = 2128.20 - 2129.61 = -1.41
        //   exit_spread  = 2129.50 - 2130.08 = -0.58
        //   gross (Short) = (entry - exit) * qty = (-1.41 - -0.58) * 0.023 = -0.01909
        //   ext_fees = (2128.20 + 2129.50) * 0.023 * 0.0005 = 0.0489...
        //   net = -0.01909 - 0.0489 = -0.0680 USDC
        // (Lighter has 0 fees by config.)
        let expected = dec!(-0.06799385);
        let diff = (pnl - expected).abs();
        assert!(
            diff < dec!(0.0001),
            "fill-based PnL {} did not match expected {} (diff {})",
            pnl,
            expected,
            diff
        );
    }

    /// Mixed: entry has fill prices but exit doesn't (e.g. exit
    /// went through `Position is missing` reduce-only short-circuit
    /// which returns synthetic success without a real fill). The
    /// function falls back to the exit *mid* only for the missing
    /// side; the entry path still uses fills. This is the back-compat
    /// behaviour required so the existing mid-based dry-run paper
    /// path and the emergency-recovery 0.0 placeholder don't break
    /// when only some sides surface fill data. bot-strategy#435.
    #[test]
    fn pnl_falls_back_to_mid_per_leg_when_fill_unavailable() {
        let pnl = compute_realised_pnl(
            SpreadDirection::Long,
            dec!(100),       // ext_entry_mid (fallback, not used)
            dec!(100),       // lt_entry_mid (fallback, not used)
            dec!(110),       // ext_exit_mid (USED — exit fill is None)
            dec!(105),       // lt_exit_mid (USED — exit fill is None)
            Some(dec!(101)), // ext entry fill — replaces mid
            Some(dec!(102)), // lt entry fill — replaces mid
            None,            // ext exit — fall back to mid 110
            None,            // lt exit  — fall back to mid 105
            dec!(1),
            dec!(1),
            dec!(1),
            dec!(1),
            0.0,
            0.0,
        );
        // entry_spread = 101 - 102 = -1
        // exit_spread  = 110 - 105 = +5
        // gross (Long) = (5 - (-1)) * 1 = 6
        assert_eq!(pnl, dec!(6));
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
            None,
            None,
            None,
            None,
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
            None,
            None,
            None,
            None,
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
            None,
            None,
            None,
            None,
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
            None,
            None,
            None,
            None,
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
            None,
            None,
            None,
            None,
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
            None,
            None,
            None,
            None,
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
                filled_value: None,
                filled_qty: dec!(1),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_value: None,
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
                filled_value: None,
                filled_qty: dec!(0.5),
                terminal: true,
                cancelled: false,
            };
        });
        let lt_vops = Arc::new(ScriptedVenueOps::new());
        lt_vops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_value: None,
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

        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
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
            let mut quote_history = RecentQuoteHistory::new(0);
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
                &mut quote_history,
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

        let mut quote_history = RecentQuoteHistory::new(0);
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
            &mut quote_history,
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

    // -- bot-strategy#330 follow-up: paper_pnl_projection tests --

    fn paper_ctx(dir: SpreadDirection, maker_entry: Option<bool>) -> PaperEntryCtx {
        // ETH-like prices. Ext is tight (0.5 tick inside = 5 bps),
        // Lt is wider (1 tick inside = 10 bps) — mirrors the live
        // venue asymmetry the redesign relies on.
        PaperEntryCtx {
            direction: dir,
            ext_entry_mid: Decimal::from(2000),
            ext_entry_bid: Decimal::new(19995, 1),
            ext_entry_ask: Decimal::new(20005, 1),
            lt_entry_mid: Decimal::from(2000),
            lt_entry_bid: Decimal::from(1999),
            lt_entry_ask: Decimal::from(2001),
            qty: Decimal::new(1, 2),
            maker_entry,
        }
    }

    fn full_snap(mid_v: f64, bid_v: f64, ask_v: f64) -> MidSnapshot {
        MidSnapshot {
            ts_ms: 0,
            mid: Decimal::from_f64_retain(mid_v).unwrap(),
            book_ok: true,
            bid: Decimal::from_f64_retain(bid_v).unwrap(),
            ask: Decimal::from_f64_retain(ask_v).unwrap(),
            bid_size: Decimal::from(10),
            ask_size: Decimal::from(10),
        }
    }

    #[test]
    fn paper_pnl_long_both_maker_captures_lt_inside_minus_ext_cross() {
        // Flat mids — no spread reversion alpha. Capture should equal
        // (lt_inside - ext_inside) bps before fees.
        // ext_inside = 1.0 / 2000 = 5 bps; lt_inside = 2.0 / 2000 = 10 bps
        // → gross = 5 bps. Fees 2 × 5 = 10 → net = -5 bps.
        let ctx = paper_ctx(SpreadDirection::Long, Some(true));
        let ext_exit = full_snap(2000.0, 1999.5, 2000.5);
        let lt_exit = full_snap(2000.0, 1999.0, 2001.0);
        let (gross, net) = paper_pnl_projection(&ctx, &ext_exit, &lt_exit, Some(true), 5.0, 0.0)
            .expect("projection succeeds with full prices");
        assert!(
            (gross - 5.0).abs() < 1e-6,
            "gross should equal lt_inside - ext_inside = 5 bps, got {}",
            gross
        );
        assert!(
            (net - (-5.0)).abs() < 1e-6,
            "net = gross - 2*ext_fee = 5 - 10 = -5, got {}",
            net
        );
    }

    #[test]
    fn paper_pnl_long_all_taker_pays_both_insides() {
        // Same flat mids; all-taker path loses both half-spreads.
        // gross = -(ext_inside + lt_inside) = -15 bps.
        let ctx = paper_ctx(SpreadDirection::Long, Some(false));
        let ext_exit = full_snap(2000.0, 1999.5, 2000.5);
        let lt_exit = full_snap(2000.0, 1999.0, 2001.0);
        let (gross, net) =
            paper_pnl_projection(&ctx, &ext_exit, &lt_exit, Some(false), 5.0, 0.0).unwrap();
        assert!(
            (gross - (-15.0)).abs() < 1e-6,
            "gross all-taker = -(ext_inside + lt_inside) = -15, got {}",
            gross
        );
        assert!((net - (-25.0)).abs() < 1e-6, "net = -15 - 10 = -25");
    }

    #[test]
    fn paper_pnl_long_entry_only_maker_half_rebate() {
        // Entry captures Lt inside half, exit pays Lt inside half.
        // Net Lt effect = 0; only ext_inside cost remains.
        // gross = -ext_inside = -5 bps.
        let ctx = paper_ctx(SpreadDirection::Long, Some(true));
        let ext_exit = full_snap(2000.0, 1999.5, 2000.5);
        let lt_exit = full_snap(2000.0, 1999.0, 2001.0);
        let (gross, _net) =
            paper_pnl_projection(&ctx, &ext_exit, &lt_exit, Some(false), 5.0, 0.0).unwrap();
        assert!(
            (gross - (-5.0)).abs() < 1e-6,
            "asymmetric maker_in=true / maker_out=false gross = -ext_inside = -5, got {}",
            gross
        );
    }

    #[test]
    fn paper_pnl_short_mirrors_long() {
        // Short direction with both makers and flat mids reproduces
        // the Long both-maker number (5 bps) under symmetric prices.
        let ctx = paper_ctx(SpreadDirection::Short, Some(true));
        let ext_exit = full_snap(2000.0, 1999.5, 2000.5);
        let lt_exit = full_snap(2000.0, 1999.0, 2001.0);
        let (gross, _net) =
            paper_pnl_projection(&ctx, &ext_exit, &lt_exit, Some(true), 5.0, 0.0).unwrap();
        assert!(
            (gross - 5.0).abs() < 1e-6,
            "Short both-maker gross should mirror Long = 5 bps, got {}",
            gross
        );
    }

    #[test]
    fn paper_pnl_long_with_spread_reversion_adds_alpha() {
        // Entry dev_bps ≈ -10 (ext 2000 cheap vs lt 2002). Exit at
        // converged mids. Long captures the 10-bps mid reversion on
        // top of the structural maker rebate.
        let ctx = PaperEntryCtx {
            direction: SpreadDirection::Long,
            ext_entry_mid: Decimal::from(2000),
            ext_entry_bid: Decimal::new(19995, 1),
            ext_entry_ask: Decimal::new(20005, 1),
            lt_entry_mid: Decimal::from(2002),
            lt_entry_bid: Decimal::from(2001),
            lt_entry_ask: Decimal::from(2003),
            qty: Decimal::new(1, 2),
            maker_entry: Some(true),
        };
        // Exit at converged mid = 2001
        let ext_exit = full_snap(2001.0, 2000.5, 2001.5);
        let lt_exit = full_snap(2001.0, 2000.0, 2002.0);
        let (gross, _net) =
            paper_pnl_projection(&ctx, &ext_exit, &lt_exit, Some(true), 5.0, 0.0).unwrap();
        // Ext leg: buy 2000.5, sell 2000.5 → 0
        // Lt leg: sell at maker entry ask = 2003, buy at maker exit bid = 2000 → +3
        // gross = 3 / 2000 * 10000 = 15 bps
        assert!(
            (gross - 15.0).abs() < 1e-6,
            "Long with 10 bps mid reversion + both makers should net 15 bps gross, got {}",
            gross
        );
    }

    #[test]
    fn paper_pnl_none_outcome_treated_as_taker() {
        // maker_entry / maker_exit = None defaults to taker. Should
        // match the all-taker number on flat mids.
        let ctx = paper_ctx(SpreadDirection::Long, None);
        let ext_exit = full_snap(2000.0, 1999.5, 2000.5);
        let lt_exit = full_snap(2000.0, 1999.0, 2001.0);
        let (gross, _net) =
            paper_pnl_projection(&ctx, &ext_exit, &lt_exit, None, 5.0, 0.0).unwrap();
        assert!(
            (gross - (-15.0)).abs() < 1e-6,
            "None outcome should match all-taker = -15 bps, got {}",
            gross
        );
    }

    #[test]
    fn paper_pnl_returns_none_on_missing_touch() {
        // Default MidSnapshot has bid=0/ask=0 (scripted-hub tests).
        // Projection bails out so we don't silently log garbage bps.
        let ctx = paper_ctx(SpreadDirection::Long, Some(true));
        let blank = mid(0, 2000.0); // bid=0, ask=0 via Default
        let out = paper_pnl_projection(&ctx, &blank, &blank, Some(true), 5.0, 0.0);
        assert!(out.is_none(), "missing bid/ask must return None");
    }

    #[tokio::test]
    async fn paper_exit_with_pre_populated_ctx_consumes_and_records() {
        // Drive the exit branch directly: pre-populate the entry ctx
        // (as handle_decision_enter would) and verify that
        // handle_decision_exit consumes it + bumps the projection
        // counters. Sidesteps the hub plumbing of handle_decision_enter.
        let qty = Decimal::new(5, 2); // 0.05 ETH
        let mut machine = build_held_machine_for_exit(SpreadDirection::Long, qty);
        let mut summary = LivePaperSummary::default();
        summary.paper_entry_ctx = Some(paper_ctx(SpreadDirection::Long, Some(true)));
        let mut open_qty = Some(qty);
        let mut live_entry_ctx: Option<LiveEntryCtx> = None;
        let cfg = min_cfg();
        let (_d, mut rm) = test_risk_manager();
        let ext_exit = full_snap(2000.0, 1999.5, 2000.5);
        let lt_exit = MidSnapshot {
            ts_ms: 0,
            mid: Decimal::from(2000),
            book_ok: true,
            bid: Decimal::from(1999),
            ask: Decimal::from(2001),
            bid_size: Decimal::from(10),
            ask_size: Decimal::from(10),
        };

        handle_decision_exit(
            &cfg,
            None,
            &mut machine,
            &mut summary,
            &mut open_qty,
            &mut live_entry_ctx,
            None,
            &mut rm,
            &ext_exit,
            &lt_exit,
            ExitReason::MeanCross,
            1_500,
            Some(0.5),
        )
        .await
        .unwrap();

        assert!(
            summary.paper_entry_ctx.is_none(),
            "paper exit should consume entry ctx"
        );
        assert_eq!(
            summary.paper_net_attempts, 1,
            "one completed RT should bump the projection counter"
        );
    }
}
