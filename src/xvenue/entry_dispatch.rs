//! `Decision::Enter` dispatch — paper-mode synthesis and live-mode
//! Extended-first serial entry (DESIGN.md §4.1).
//!
//! Extracted wholesale from `live.rs` for bot-strategy#381; behaviour is
//! byte-identical to the prior inline form. All paper-mode telemetry
//! (`PAPER ENTER`, `WOULD-BE MAKER`, `paper_entry_ctx` capture) and live-
//! mode state-machine apply ordering, stuck-tripwire updates, and
//! summary counter increments are preserved.

use anyhow::Result;
use dex_connector::OrderSide as DcOrderSide;
use rust_decimal::Decimal;

use super::config::XvenueConfig;
use super::live::{LiveEntryCtx, LivePaperSummary, MidSnapshot, PaperEntryCtx, Venue, VenueHub};
use super::live_exec::LiveExecution;
use super::live_pnl::{paper_qty, would_be_maker_fill_outcome};
use super::signal::SpreadDirection;
use super::sizing::{compute_notional_usd, notional_to_qty, SizeOutcome};
use super::state::{Event, PositionMachine};
use super::status::StatusReporter;
use crate::risk::kill_switch::StuckTripwire;
use crate::trade::execution::extended_maker::{ExtendedEntryRequest, ExtendedMakerLoop};
use crate::trade::execution::lighter_fill::{LighterFillLoop, LighterFillRequest};
use crate::trade::execution::lighter_maker::{LighterMakerLoop, LighterMakerRequest};
use crate::trade::execution::types::{ExecutionFailure, ExtendedTerminal, LighterTerminal};

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
/// `Decision::Enter` arm. All log lines, summary counters, state-
/// machine apply ordering, record_fill calls, and the live_entry_ctx
/// capture point are byte-identical.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_decision_enter<H: VenueHub + ?Sized>(
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
        let maker_entry_outcome = would_be_maker_fill_outcome(dir, qty, lt_snap, now_ts_ms);
        if let Some(out) = maker_entry_outcome {
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

        // bot-strategy#330 follow-up: capture the touch-level entry
        // state so the matching exit can emit a calibrated projected
        // PnL line. The mid-to-mid `dev_bps` already in the PAPER ENTER
        // log overstates capturable edge — see paper_pnl_projection
        // doc-comment.
        summary.paper_entry_ctx = Some(PaperEntryCtx {
            direction: dir,
            ext_entry_mid: ext_snap.mid,
            ext_entry_bid: ext_snap.bid,
            ext_entry_ask: ext_snap.ask,
            lt_entry_mid: lt_snap.mid,
            lt_entry_bid: lt_snap.bid,
            lt_entry_ask: lt_snap.ask,
            qty,
            maker_entry: maker_entry_outcome.map(|o| o.sampled_fill),
        });

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
        ExtendedTerminal::Filled {
            qty,
            avg_fill_price: ext_entry_avg_fill_price,
        } => {
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
                LighterTerminal::Filled {
                    qty: lt_filled,
                    avg_fill_price: lt_entry_avg_fill_price,
                } => {
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
                        ext_entry_avg_fill_price,
                        lt_entry_avg_fill_price,
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
