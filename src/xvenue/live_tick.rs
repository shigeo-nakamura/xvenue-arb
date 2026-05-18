//! Per-tick decision orchestration — extracted from `live.rs` per
//! bot-strategy#387 (seam 4 of the 2026-05-13 audit). With the prior
//! seams (#383 error_counter, #384 live_pnl, #385 live_status, #386
//! emergency_handlers) landed, [`run_one_tick`] reads as pure routing:
//! read mids → record WS health → pre-decide gates → spread update →
//! signal decide → entry-gate cascade → dispatch.
//!
//! Every helper called here lives in a sibling module; the move is
//! cohesion-only, no behavioural change. `run_paper_loop` and the
//! tests inside `live::tests` call into here via `super::live_tick`.

use anyhow::{Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::config::XvenueConfig;
use super::emergency_handlers::{
    apply_entry_gates, apply_reference_guard, book_depth_blocks_entry,
    force_flatten_on_session_dd_halt, handle_skew_breach_emergency, handle_ws_stale_emergency,
};
use super::entry_dispatch::handle_decision_enter;
use super::entry_filter::{
    evaluate_entry_filter, EntryFilterOutcome, QuoteSample, RecentQuoteHistory,
};
use super::exit_dispatch::handle_decision_exit;
use super::live::{
    wall_clock_ms, LiveEntryCtx, LivePaperSummary, MidSnapshot, Venue, VenueHub, VenueWarmup,
};
use super::live_exec::LiveExecution;
use super::signal::{effective_dev_bps, Decision, SignalEngine};
use super::spread::SpreadEngine;
use super::state::PositionMachine;
use super::status::StatusReporter;
use crate::risk::kill_switch::{StuckTripwire, VenueLabel};
use crate::risk::manager::RiskManager;
use crate::risk::reference_guard::ReferenceGuard;
use crate::risk::skew_monitor::SkewMonitor;
use crate::risk::ws_health::WsHealthMonitor;

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

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_one_tick<H: VenueHub + ?Sized>(
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
    quote_history: &mut RecentQuoteHistory,
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

        // bot-strategy#429: feed the rolling-window history for the
        // defensive entry filter. Only push when both venues have
        // populated books — same gate as the snapshot fields above —
        // so scripted-hub tests don't pollute the buffer with
        // synthetic zeros.
        quote_history.push(QuoteSample {
            ts_ms: now_ts_ms,
            lt_inside_bps: summary.last_lt_inside_bps.unwrap_or(0.0),
            lt_bid_size: summary.last_lt_bid_size.unwrap_or(0.0),
            lt_ask_size: summary.last_lt_ask_size.unwrap_or(0.0),
        });
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

    // bot-strategy#429: defensive entry filter. Runs after the
    // book-depth gate so a "too thick" block isn't re-counted as a
    // "regime unstable" block. Both fields opt-in via YAML; when
    // both are `None` evaluate_entry_filter returns Allow and the
    // call is a no-op.
    if let Decision::Enter(dir) = decision {
        match evaluate_entry_filter(
            quote_history,
            cfg.entry_filter_lt_inside_max_bps,
            cfg.entry_filter_lt_min_depth_eth,
        ) {
            EntryFilterOutcome::Allow => {}
            EntryFilterOutcome::BlockInsideSpike {
                observed_bps,
                threshold_bps,
            } => {
                log::warn!(
                    "[XVENUE] entry filter blocked: inside-spike dir={:?} \
                     observed_max_bps={:.2} threshold_bps={:.2} \
                     window_samples={} window_sec={}",
                    dir,
                    observed_bps,
                    threshold_bps,
                    quote_history.len(),
                    cfg.entry_filter_window_sec,
                );
                summary.entries_blocked_by_entry_filter += 1;
                decision = Decision::Hold;
            }
            EntryFilterOutcome::BlockMinDepth {
                observed_eth,
                floor_eth,
            } => {
                log::warn!(
                    "[XVENUE] entry filter blocked: depth-floor dir={:?} \
                     observed_min_eth={:.4} floor_eth={:.4} \
                     window_samples={} window_sec={}",
                    dir,
                    observed_eth,
                    floor_eth,
                    quote_history.len(),
                    cfg.entry_filter_window_sec,
                );
                summary.entries_blocked_by_entry_filter += 1;
                decision = Decision::Hold;
            }
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
