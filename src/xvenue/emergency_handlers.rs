//! Pre-decide gates and emergency routing extracted from `live.rs`
//! per bot-strategy#386 (seam 3 of the 2026-05-13 audit).
//!
//! All six functions run from `run_one_tick` *before* `signal.decide`,
//! gating or pre-empting the strategy's view of the world:
//!
//! - [`handle_ws_stale_emergency`] / [`handle_skew_breach_emergency`]
//!   route a held position into `EmergencyFlattening` when the WS health
//!   monitor / inventory skew monitor breaches (#244 Group C).
//! - [`force_flatten_on_session_dd_halt`] does the same for a session-DD
//!   risk halt (#268 S5-3).
//! - [`apply_reference_guard`] suppresses each venue's `book_ok` when its
//!   mid drifts past the Binance reference (#244 C).
//! - [`apply_entry_gates`] downgrades `Decision::Enter` to `Decision::Hold`
//!   under any of KILL_SWITCH / STUCK / wrong-phase / risk-halt gates
//!   (#244 D-1 / D-2..D-7, #102 P2).
//! - [`book_depth_blocks_entry`] applies the maker-on-Lighter queue-depth
//!   filter (#309 step 4).
//!
//! All bodies are byte-identical to their original `live.rs` form;
//! the move is purely cohesion-driven. `kill_switch_active` and
//! `now_unix_secs` stay in `live.rs` (the former is also called from
//! `live_status::publish_kill_switch`, the latter from many other live
//! paths) — both are imported here via `super::live::*`.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::config::XvenueConfig;
use super::live::{kill_switch_active, now_unix_secs, LivePaperSummary, MidSnapshot};
use super::live_exec::LiveExecution;
use super::signal::{Decision, PositionSummary, SpreadDirection};
use super::state::{EmergencyReason, Event, PositionMachine};
use crate::prom;
use crate::risk::kill_switch::StuckTripwire;
use crate::risk::manager::{BlockReason, RiskManager};
use crate::risk::reference_guard::{RefCheckOutcome, ReferenceGuard};
use crate::risk::skew_monitor::{SkewMonitor, SkewOutcome};
use crate::risk::ws_health::{WsHealthMonitor, WsHealthOutcome};

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
pub(super) fn handle_ws_stale_emergency(
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
        reason: EmergencyReason::WsStale,
    };
    match machine.apply(now_wall_ms, event) {
        Ok(()) => {
            prom::record_close(
                &cfg.agent_name,
                EmergencyReason::WsStale.as_close_reason_str(),
            );
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
pub(super) fn apply_entry_gates(
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
pub(super) fn book_depth_blocks_entry(
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
pub(super) fn force_flatten_on_session_dd_halt(
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
        reason: EmergencyReason::SessionDdHalted,
    };
    match machine.apply(now_ts_ms, event) {
        Ok(()) => {
            prom::record_close(
                &cfg.agent_name,
                EmergencyReason::SessionDdHalted.as_close_reason_str(),
            );
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
pub(super) async fn apply_reference_guard(
    reference_guard: &mut ReferenceGuard,
    ext_snap: &mut MidSnapshot,
    lt_snap: &mut MidSnapshot,
    summary: &mut LivePaperSummary,
) {
    let ref_state = reference_guard.current_reference().await;
    let (Some(ext_mid_f), Some(lt_mid_f)) = (ext_snap.mid.to_f64(), lt_snap.mid.to_f64()) else {
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
pub(super) fn handle_skew_breach_emergency(
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
    let skew_f = skew_dec.to_f64().unwrap_or(0.0);
    let SkewOutcome::Breach {
        skew_usd,
        threshold_usd,
    } = skew_monitor.evaluate(skew_f)
    else {
        return false;
    };

    let event = Event::Emergency {
        reason: EmergencyReason::SkewBreach,
    };
    match machine.apply(now_ts_ms, event) {
        Ok(()) => {
            prom::record_close(
                &cfg.agent_name,
                EmergencyReason::SkewBreach.as_close_reason_str(),
            );
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
