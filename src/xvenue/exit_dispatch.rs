//! `Decision::Exit` dispatch — paper-mode synthesis and live-mode
//! parallel reduce-only exit (DESIGN.md §4.2, bot-strategy#244 / #268
//! S5 / #330).
//!
//! Extracted wholesale from `live.rs` for bot-strategy#381; behaviour is
//! byte-identical to the prior inline form. All paper-mode telemetry
//! (`PAPER EXIT`, `WOULD-BE MAKER EXIT`, `PAPER NET`), realised-PnL
//! computation, state-machine apply ordering, record_close calls (with
//! the 0.0 placeholder on paper-mode and on partial/failed paths), and
//! the live_entry_ctx.take() points are preserved.

use anyhow::Result;
use dex_connector::OrderSide as DcOrderSide;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::config::XvenueConfig;
use super::live::{now_unix_secs, LiveEntryCtx, LivePaperSummary, MidSnapshot};
use super::live_exec::LiveExecution;
use super::live_pnl::{
    compute_realised_pnl, paper_pnl_projection, would_be_maker_fill_outcome, WouldBeMakerOutcome,
};
use super::signal::{ExitReason, SpreadDirection};
use super::state::{EmergencyReason, Event, PositionMachine};
use super::status::StatusReporter;
use crate::risk::manager::RiskManager;
use crate::trade::execution::extended_maker::ExtendedEntryRequest;
use crate::trade::execution::lighter_fill::LighterFillRequest;
use crate::trade::execution::parallel_exit::{ParallelExitLoop, ParallelExitOutcome};
use crate::trade::execution::types::{ExtendedTerminal, LighterTerminal};
use crate::trade::execution::venue_ops::VenueOps;

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
pub(super) async fn handle_decision_exit(
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
        let mut maker_exit_outcome: Option<WouldBeMakerOutcome> = None;
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
                    maker_exit_outcome = Some(out);
                }
            }
        }

        // bot-strategy#330 follow-up: projected per-RT PnL at touch
        // level, gated on having a matching entry ctx (taken so a stray
        // exit without entry — e.g. operator Reset edge case — doesn't
        // pollute the cumulative counters). Computed even when the
        // would-be maker outcome above was None: paper_pnl_projection
        // treats `maker_*: None` as taker (conservative) so the floor
        // case still produces a sensible figure.
        if let Some(ctx) = summary.paper_entry_ctx.take() {
            if let Some((gross_bps, net_bps)) = paper_pnl_projection(
                &ctx,
                ext_snap,
                lt_snap,
                maker_exit_outcome.map(|o| o.sampled_fill),
                cfg.extended_fee_bps,
                cfg.lighter_fee_bps,
            ) {
                summary.paper_net_attempts += 1;
                summary.paper_gross_bps_sum += gross_bps;
                summary.paper_net_bps_sum += net_bps;
                log::info!(
                    "[XVENUE] PAPER NET dir={:?} maker_in={:?} maker_out={:?} \
                     gross_bps={:.2} net_bps={:.2} ext_fee_bps={} lt_fee_bps={}",
                    ctx.direction,
                    ctx.maker_entry,
                    maker_exit_outcome.map(|o| o.sampled_fill),
                    gross_bps,
                    net_bps,
                    cfg.extended_fee_bps,
                    cfg.lighter_fee_bps,
                );
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
                    // bot-strategy#418 — flush sub-min residuals BEFORE
                    // logging "→ Flat" so the state machine actually
                    // reaches Flat. The post_only chase loop honours
                    // `dust_qty.max(min_qty)` as the "treat as fully
                    // filled" floor (#299 / #331) and returns a
                    // `Filled { qty }` smaller than the requested qty
                    // when the venue settles the order in two trade
                    // events and the trailing fill lands after the
                    // loop's terminal poll. Without this flush
                    // `lighter_open_qty` (or `extended_open_qty`) is
                    // left at e.g. 0.0007 ETH < `lighter_min_qty=0.001`,
                    // `maybe_complete_flat` keeps the phase at
                    // `Exiting`, and the bot rejects every subsequent
                    // `EntrySignal` until restart. The same min_qty
                    // floor that the chase loop already trusts is the
                    // natural threshold here.
                    flush_sub_min_exit_residuals(
                        machine,
                        live.ext_min_qty,
                        live.lt_min_qty,
                        now_ts_ms,
                    )?;
                    // bot-strategy#418 re-open (2026-05-17): the sub-min
                    // flush only handles residuals strictly below min_qty.
                    // A trailing trade ≥ min_qty (observed 0.0043 ETH on
                    // a post_only order's second WS trade event arriving
                    // after the chase loop returned its terminal qty)
                    // leaves the state machine at e.g.
                    // `lighter_open_qty=0.0222` while the venue is
                    // actually flat. Reconcile against the venue's own
                    // position read so the machine reflects reality
                    // before maybe_complete_flat runs.
                    reconcile_open_qty_with_exchange(
                        machine,
                        &*live.ext_ops,
                        &*live.lt_ops,
                        &live.ext_symbol,
                        &live.lt_symbol,
                        live.dust_qty.max(live.ext_min_qty),
                        live.dust_qty.max(live.lt_min_qty),
                        now_ts_ms,
                    )
                    .await?;

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
                    let pnl_f64 = ToPrimitive::to_f64(&pnl).unwrap_or(0.0);
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

/// bot-strategy#418 — close the residual gap left when the post_only
/// chase loop's terminal qty under-reports the venue's final fill.
///
/// The chase loop calls a round done at `remaining ≤ dust_qty.max(min_qty)`
/// and returns the qty it observed by that poll. When the venue settles the
/// order in two trade events and the second trade lands a few ms after the
/// terminal poll, the state machine ends up with
/// `open_qty = requested - first_trade` (e.g. 0.0228 - 0.0221 = 0.0007),
/// which is below the venue's min order size. `place_post_only` would
/// reject any follow-up exit of that residual with `code:21706`, so the
/// chase loop's "treat as fully filled" policy is correct — but the state
/// machine never sees the policy. This helper bridges that gap by applying
/// an additional `*ExitFilled { qty: residual }` event for any leg whose
/// post-apply `open_qty` is strictly between zero and the leg's min_qty,
/// letting `maybe_complete_flat` transition the phase to `Flat`.
///
/// `min_qty = 0` (the config default per #299 / #331) disables the flush
/// for that leg, preserving the legacy back-compat semantics in which the
/// dust gate alone governs sub-min residuals.
fn flush_sub_min_exit_residuals(
    machine: &mut PositionMachine,
    ext_min_qty: Decimal,
    lt_min_qty: Decimal,
    now_ts_ms: u64,
) -> Result<()> {
    let (ext_residual, lt_residual) = match machine.position() {
        Some(p) => (p.extended_open_qty, p.lighter_open_qty),
        None => return Ok(()),
    };
    if ext_residual > Decimal::ZERO && ext_residual < ext_min_qty {
        log::info!(
            "[XVENUE] LIVE EXIT residual flush ext={} (< min_qty={}) — \
             treating as fully closed (bot-strategy#418)",
            ext_residual,
            ext_min_qty,
        );
        machine.apply(now_ts_ms, Event::ExtendedExitFilled { qty: ext_residual })?;
    }
    if lt_residual > Decimal::ZERO && lt_residual < lt_min_qty {
        log::info!(
            "[XVENUE] LIVE EXIT residual flush lt={} (< min_qty={}) — \
             treating as fully closed (bot-strategy#418)",
            lt_residual,
            lt_min_qty,
        );
        machine.apply(now_ts_ms, Event::LighterExitFilled { qty: lt_residual })?;
    }
    Ok(())
}

/// bot-strategy#418 re-open (2026-05-17): catch the residual case that
/// [`flush_sub_min_exit_residuals`] cannot — a trailing trade *larger*
/// than `min_qty` that lands after the post_only chase loop's terminal
/// poll. The chase loop returns `Filled { qty: first_trade }` and the
/// state machine subtracts only that, but the venue settled the rest
/// in a follow-up trade event that the chase loop's terminal already
/// missed. The state thinks `open_qty=residual` while the exchange
/// position is actually flat (or below dust_floor); without this
/// reconciliation the machine stays in `Exiting` forever and the bot
/// silently parks.
///
/// We ask each venue what *its* position currently is (cheap call — the
/// connectors back this with their WS position cache) and treat a value
/// below `dust_floor` as "the venue says we're flat, so trust that".
/// We then apply a synthetic `*ExitFilled { qty: state_open }` so
/// `maybe_complete_flat` can drain the position.
///
/// A `get_positions` failure surfaces as a `warn!` rather than `Err` —
/// the exit path has already filled both legs, blocking the happy path
/// because of a transient API hiccup would convert a recoverable stuck
/// state into a forced restart.
///
/// `dust_floor` is the same `dust_qty.max(min_qty)` floor the chase
/// loop's "treat as fully filled" gate uses (#299 / #331). A venue
/// position at or above the floor is treated as a genuine partial —
/// we leave the state residual untouched and emit a warn so the
/// operator can investigate. The Both-happy-path completes; the next
/// exit cycle / Emergency loop will pick the residual up.
async fn reconcile_open_qty_with_exchange(
    machine: &mut PositionMachine,
    ext_ops: &dyn VenueOps,
    lt_ops: &dyn VenueOps,
    ext_symbol: &str,
    lt_symbol: &str,
    ext_dust_floor: Decimal,
    lt_dust_floor: Decimal,
    now_ts_ms: u64,
) -> Result<()> {
    let (ext_open, lt_open) = match machine.position() {
        Some(p) => (p.extended_open_qty, p.lighter_open_qty),
        None => return Ok(()),
    };
    if ext_open > Decimal::ZERO {
        match ext_ops.current_position_size(ext_symbol).await {
            Ok(exchange_qty) if exchange_qty < ext_dust_floor => {
                log::info!(
                    "[XVENUE] LIVE EXIT exchange reconcile ext: state={} \
                     exchange={} (< dust_floor={}) — flushing state residual \
                     (bot-strategy#418 re-open)",
                    ext_open,
                    exchange_qty,
                    ext_dust_floor,
                );
                machine.apply(now_ts_ms, Event::ExtendedExitFilled { qty: ext_open })?;
            }
            Ok(exchange_qty) => log::warn!(
                "[XVENUE] LIVE EXIT exchange reconcile ext: state={} \
                 exchange={} (≥ dust_floor={}) — leaving state residual; \
                 next cycle or Emergency loop will pick it up \
                 (bot-strategy#418 re-open)",
                ext_open,
                exchange_qty,
                ext_dust_floor,
            ),
            Err(e) => log::warn!(
                "[XVENUE] LIVE EXIT exchange reconcile ext: get_positions \
                 failed ({}); skipping reconcile (bot-strategy#418 re-open)",
                e
            ),
        }
    }
    if lt_open > Decimal::ZERO {
        match lt_ops.current_position_size(lt_symbol).await {
            Ok(exchange_qty) if exchange_qty < lt_dust_floor => {
                log::info!(
                    "[XVENUE] LIVE EXIT exchange reconcile lt: state={} \
                     exchange={} (< dust_floor={}) — flushing state residual \
                     (bot-strategy#418 re-open)",
                    lt_open,
                    exchange_qty,
                    lt_dust_floor,
                );
                machine.apply(now_ts_ms, Event::LighterExitFilled { qty: lt_open })?;
            }
            Ok(exchange_qty) => log::warn!(
                "[XVENUE] LIVE EXIT exchange reconcile lt: state={} \
                 exchange={} (≥ dust_floor={}) — leaving state residual; \
                 next cycle or Emergency loop will pick it up \
                 (bot-strategy#418 re-open)",
                lt_open,
                exchange_qty,
                lt_dust_floor,
            ),
            Err(e) => log::warn!(
                "[XVENUE] LIVE EXIT exchange reconcile lt: get_positions \
                 failed ({}); skipping reconcile (bot-strategy#418 re-open)",
                e
            ),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xvenue::signal::ExitReason;
    use crate::xvenue::state::Phase;
    use rust_decimal_macros::dec;

    fn held_machine(direction: SpreadDirection, qty: Decimal) -> PositionMachine {
        let mut m = PositionMachine::new();
        m.apply(
            0,
            Event::EntrySignal {
                direction,
                notional_usd: dec!(100),
            },
        )
        .unwrap();
        m.apply(0, Event::ExtendedFilled { qty }).unwrap();
        m.apply(0, Event::LighterFilled { qty }).unwrap();
        m
    }

    fn enter_exiting(m: &mut PositionMachine) {
        m.apply(
            1_000,
            Event::ExitSignal {
                reason: ExitReason::MeanCross,
            },
        )
        .unwrap();
    }

    #[test]
    fn flush_zeroes_lighter_residual_below_min_and_transitions_to_flat() {
        // Reproduces the 2026-05-16 stuck-Exiting incident: lt leg
        // closes 0.0221 of 0.0228 leaving a 0.0007 residual that is
        // below `lighter_min_qty=0.001` while the ext leg closes
        // cleanly.
        let mut m = held_machine(SpreadDirection::Short, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0228) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0221) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Exiting, "pre-flush phase");
        let p = m.position().unwrap();
        assert_eq!(p.extended_open_qty, Decimal::ZERO);
        assert_eq!(p.lighter_open_qty, dec!(0.0007));

        flush_sub_min_exit_residuals(&mut m, dec!(0.01), dec!(0.001), 1_300).unwrap();

        assert_eq!(
            m.phase(),
            Phase::Flat,
            "flush should drain lt residual → Flat"
        );
        assert!(m.position().is_none());
    }

    #[test]
    fn flush_zeroes_extended_residual_below_min_and_transitions_to_flat() {
        // Symmetric coverage for the Extended leg residual.
        let mut m = held_machine(SpreadDirection::Long, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0227) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0228) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Exiting, "pre-flush phase");
        let p = m.position().unwrap();
        assert_eq!(p.extended_open_qty, dec!(0.0001));
        assert_eq!(p.lighter_open_qty, Decimal::ZERO);

        // ext_min_qty=0.01 (ETH on Extended per #299) — 0.0001 < 0.01.
        flush_sub_min_exit_residuals(&mut m, dec!(0.01), dec!(0.001), 1_300).unwrap();

        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }

    #[test]
    fn flush_is_noop_when_both_legs_already_zero() {
        let mut m = held_machine(SpreadDirection::Short, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0228) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0228) })
            .unwrap();
        // Auto-flat already fired — machine.position is None.
        assert_eq!(m.phase(), Phase::Flat);

        flush_sub_min_exit_residuals(&mut m, dec!(0.01), dec!(0.001), 1_300).unwrap();

        // Idempotent on a flat machine — phase stays Flat, no panic.
        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }

    #[test]
    fn flush_does_not_touch_residual_at_or_above_min_qty() {
        // 0.0015 residual with lt_min_qty=0.001 → strictly NOT < min,
        // so the chase loop would have re-submitted. Leave the residual
        // intact so the next exit cycle / Emergency loop handles it.
        let mut m = held_machine(SpreadDirection::Short, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0228) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0213) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Exiting);
        let p_before = m.position().unwrap().lighter_open_qty;
        assert_eq!(p_before, dec!(0.0015));

        flush_sub_min_exit_residuals(&mut m, dec!(0.01), dec!(0.001), 1_300).unwrap();

        assert_eq!(
            m.phase(),
            Phase::Exiting,
            "residual ≥ min must stay Exiting"
        );
        assert_eq!(m.position().unwrap().lighter_open_qty, dec!(0.0015));
    }

    #[test]
    fn flush_is_disabled_when_min_qty_is_zero_back_compat() {
        // Default config has *_min_qty=0 (#299/#331 opt-in). With the
        // floor disabled, a sub-dust residual must NOT be auto-flushed
        // — the legacy dust gate inside the chase loop is the only
        // arbiter. This guards against an accidental policy change for
        // bots that haven't opted in.
        let mut m = held_machine(SpreadDirection::Short, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0228) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0221) })
            .unwrap();
        let residual = m.position().unwrap().lighter_open_qty;
        assert_eq!(residual, dec!(0.0007));

        flush_sub_min_exit_residuals(&mut m, Decimal::ZERO, Decimal::ZERO, 1_300).unwrap();

        assert_eq!(m.phase(), Phase::Exiting);
        assert_eq!(m.position().unwrap().lighter_open_qty, dec!(0.0007));
    }

    // ------------------------------------------------------------------
    // bot-strategy#418 re-open (2026-05-17) — exchange reconcile tests.
    // ------------------------------------------------------------------

    use crate::trade::execution::venue_ops::ScriptedVenueOps;

    fn scripted_with_position(symbol: &str, qty: Decimal) -> ScriptedVenueOps {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.current_positions.insert(symbol.to_string(), qty);
        });
        ops
    }

    #[tokio::test]
    async fn reconcile_flushes_lighter_residual_when_exchange_is_flat() {
        // Reproduces the 2026-05-17 stuck-Exiting incident: a trailing
        // 0.0043 ETH trade lands after the post_only chase loop's
        // terminal returns Filled{qty:0.0006}, so state.lighter_open_qty
        // ends at 0.0222 while the venue is actually flat. min_qty
        // (=0.001) flush does NOT trigger because 0.0222 ≥ min_qty.
        let mut m = held_machine(SpreadDirection::Short, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0228) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0006) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Exiting);
        assert_eq!(m.position().unwrap().lighter_open_qty, dec!(0.0222));

        // sub-min flush correctly does NOT clear this residual (≥ min).
        flush_sub_min_exit_residuals(&mut m, dec!(0.01), dec!(0.001), 1_300).unwrap();
        assert_eq!(m.position().unwrap().lighter_open_qty, dec!(0.0222));

        // Venue says position=0 → reconcile should flush the residual.
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = scripted_with_position("ETH", Decimal::ZERO);
        reconcile_open_qty_with_exchange(
            &mut m,
            &ext_ops,
            &lt_ops,
            "ETH-USD",
            "ETH",
            dec!(0.01),
            dec!(0.001),
            1_400,
        )
        .await
        .unwrap();

        assert_eq!(
            m.phase(),
            Phase::Flat,
            "reconcile should flush lt residual → Flat"
        );
        assert!(m.position().is_none());
    }

    #[tokio::test]
    async fn reconcile_flushes_extended_residual_when_exchange_is_flat() {
        // Symmetric coverage for the Extended leg residual.
        let mut m = held_machine(SpreadDirection::Long, dec!(0.022));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.005) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.022) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Exiting);
        assert_eq!(m.position().unwrap().extended_open_qty, dec!(0.017));

        flush_sub_min_exit_residuals(&mut m, dec!(0.01), dec!(0.001), 1_300).unwrap();
        assert_eq!(m.position().unwrap().extended_open_qty, dec!(0.017));

        let ext_ops = scripted_with_position("ETH-USD", Decimal::ZERO);
        let lt_ops = ScriptedVenueOps::new();
        reconcile_open_qty_with_exchange(
            &mut m,
            &ext_ops,
            &lt_ops,
            "ETH-USD",
            "ETH",
            dec!(0.01),
            dec!(0.001),
            1_400,
        )
        .await
        .unwrap();

        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }

    #[tokio::test]
    async fn reconcile_leaves_residual_when_exchange_still_has_position() {
        // Genuine partial: venue position 0.015 ETH ≥ dust_floor (0.001).
        // Don't fabricate a fill — the next cycle / Emergency loop will
        // pick the residual up. Phase stays Exiting.
        let mut m = held_machine(SpreadDirection::Short, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0228) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0078) })
            .unwrap();
        assert_eq!(m.position().unwrap().lighter_open_qty, dec!(0.0150));

        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = scripted_with_position("ETH", dec!(0.0150));
        reconcile_open_qty_with_exchange(
            &mut m,
            &ext_ops,
            &lt_ops,
            "ETH-USD",
            "ETH",
            dec!(0.01),
            dec!(0.001),
            1_400,
        )
        .await
        .unwrap();

        assert_eq!(
            m.phase(),
            Phase::Exiting,
            "genuine partial must keep Exiting for follow-up"
        );
        assert_eq!(m.position().unwrap().lighter_open_qty, dec!(0.0150));
    }

    #[tokio::test]
    async fn reconcile_is_noop_when_state_already_flat() {
        // Clean exit (both legs fully filled in the chase loop) →
        // maybe_complete_flat already fired, no residual, no venue call
        // needed. The function must early-return on no-position.
        let mut m = held_machine(SpreadDirection::Short, dec!(0.0228));
        enter_exiting(&mut m);
        m.apply(1_100, Event::ExtendedExitFilled { qty: dec!(0.0228) })
            .unwrap();
        m.apply(1_200, Event::LighterExitFilled { qty: dec!(0.0228) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Flat);

        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        reconcile_open_qty_with_exchange(
            &mut m,
            &ext_ops,
            &lt_ops,
            "ETH-USD",
            "ETH",
            dec!(0.01),
            dec!(0.001),
            1_400,
        )
        .await
        .unwrap();

        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }
}
