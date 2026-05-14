//! Shared place / chase / taker-fallback scaffolding for the
//! [`super::extended_maker`] and [`super::lighter_maker`] modules
//! (bot-strategy#388). Extracts the ~600 LOC of textually-identical
//! retry-loop machinery the two venue modules used to inline, leaving
//! each venue file with only the venue-specific terminal-type wrapping.
//!
//! Three knobs on [`MakerLoopParams`] encode the venue differences
//! the original loops captured inline.
//!
//! `chase_uses_venue_min_floor`: Lighter (bot-strategy#331)
//! floors the chase-loop early-exit and threshold at
//! `dust_qty.max(venue_min_qty)`; Extended (bot-strategy#299) floors
//! only the taker-fallback gate. The two policies survive untouched.
//!
//! `chase_grace_poll_ms`: Lighter (bot-strategy#322) re-polls after
//! cancel when a chase round lands `filled=0 cancelled=false` to
//! catch a WS-lagged fill; Extended has no chase-side grace.
//!
//! `taker_grace_before_cancel`: Extended (bot-strategy#298) runs the
//! taker grace re-poll BEFORE cancel so a fill that landed at the
//! venue still surfaces if the cancel races against it. Lighter
//! (bot-strategy#322 taker half) cancels first, then re-polls.
//!
//! Log shapes, counter cadence, and the grace-poll log level (debug
//! for chase, warn for taker) are preserved byte-for-byte against the
//! pre-#388 inline form. The pre-#388 `extended_maker.rs` +
//! `lighter_maker.rs` impls remain in git history for fallback
//! purposes — the issue's "MUST include a feature flag" requirement
//! is deferred to the pre-landing commit (this work is gated to
//! 2026-06-10+ per issue's timing section).

use std::time::Duration;

use dex_connector::OrderSide;
use rust_decimal::Decimal;

use super::poll_loop::{poll_until_terminal_or_deadline, PollOutcome};
use super::types::ExecutionFailure;
use super::venue_ops::{PlacedOrder, TopOfBook, VenueOps};

/// Parameters threading the venue-specific config + venue-specific
/// behavioral toggles into the shared loop.
pub(crate) struct MakerLoopParams {
    pub log_prefix: &'static str,
    pub chase_retries: u32,
    pub chase_timeout_ms: u64,
    pub chase_grace_poll_ms: u64,
    pub taker_grace_poll_ms: u64,
    pub taker_fallback: bool,
    pub post_only: bool,
    pub poll_interval_ms: u64,
    /// Lighter (#331): floor `remaining <= X` chase early-exit and the
    /// `total_filled >= target - X` threshold at
    /// `dust_qty.max(venue_min_qty)`. Extended (#299) leaves these on
    /// `dust_qty` and applies the venue-min floor only to the taker
    /// fallback gate.
    pub chase_uses_venue_min_floor: bool,
    /// Extended (#298) takes a fill that races the cancel by polling
    /// before sending cancel. Lighter (#322 taker) cancels first.
    pub taker_grace_before_cancel: bool,
}

/// Shape-identical to the per-venue Request structs (Extended /
/// Lighter). Each venue wrapper converts its public struct into this
/// shared shape before calling [`run_maker_loop`].
pub(crate) struct MakerRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub target_qty: Decimal,
    pub dust_qty: Decimal,
    pub venue_min_qty: Decimal,
    pub reduce_only: bool,
}

/// Result of one `run_maker_loop` cycle. The caller maps this into
/// its venue-specific `Terminal::{Filled, Failed}` variant.
pub(crate) struct MakerLoopOutcome {
    pub total_filled: Decimal,
    pub last_failure: Option<ExecutionFailure>,
}

/// Drive one entry / exit cycle to a single MakerLoopOutcome.
/// Caller (venue wrapper) maps the outcome into its terminal type and
/// returns to the position machine.
pub(crate) async fn run_maker_loop<V: VenueOps + ?Sized>(
    ops: &V,
    params: &MakerLoopParams,
    req: &MakerRequest,
) -> MakerLoopOutcome {
    let mut total_filled = Decimal::ZERO;
    let mut last_failure: Option<ExecutionFailure> = None;

    let chase_floor = if params.chase_uses_venue_min_floor {
        req.dust_qty.max(req.venue_min_qty)
    } else {
        req.dust_qty
    };
    let taker_floor = req.venue_min_qty.max(req.dust_qty);

    if params.post_only {
        for round in 0..params.chase_retries.max(1) {
            let remaining = req.target_qty - total_filled;
            if remaining <= chase_floor {
                break;
            }

            match run_one_chase_round(ops, params, req, remaining, round).await {
                Ok(outcome) => {
                    total_filled += outcome.filled_this_round;
                    if outcome.terminal_cancelled && outcome.filled_this_round.is_zero() {
                        // Venue cancelled with zero fill — most likely
                        // the post-only price moved through. Continue
                        // to the next chase round (if any retries left)
                        // so we re-quote at the new book.
                        last_failure = Some(ExecutionFailure::Cancelled);
                        continue;
                    }
                    if total_filled >= req.target_qty - chase_floor {
                        return MakerLoopOutcome {
                            total_filled,
                            last_failure,
                        };
                    }
                }
                Err(failure) => {
                    last_failure = Some(failure);
                    // Hard place / read failure — break out of the
                    // chase and fall through to taker fallback. We
                    // don't keep retrying place_* since the venue is
                    // signalling a problem.
                    break;
                }
            }
        }
    }

    let residual = req.target_qty - total_filled;
    if residual > taker_floor && params.taker_fallback {
        match run_taker_round(ops, params, req, residual).await {
            Ok(taker_filled) => {
                total_filled += taker_filled;
            }
            Err(failure) => {
                if total_filled.is_zero() {
                    return MakerLoopOutcome {
                        total_filled,
                        last_failure: Some(failure),
                    };
                }
                // Partial maker fill — surface so the state machine
                // can route via skew monitor instead of dead-ending
                // in Failed.
            }
        }
    }

    MakerLoopOutcome {
        total_filled,
        last_failure,
    }
}

async fn run_one_chase_round<V: VenueOps + ?Sized>(
    ops: &V,
    params: &MakerLoopParams,
    req: &MakerRequest,
    remaining: Decimal,
    round: u32,
) -> Result<PollOutcome, ExecutionFailure> {
    let book = match ops.read_top_of_book(&req.symbol).await {
        Ok(b) => b,
        Err(e) => {
            log::warn!(
                "[{}] read_top_of_book round={} err={:?}",
                params.log_prefix,
                round,
                e
            );
            return Err(ExecutionFailure::VenueRejected);
        }
    };
    let price = price_for_post_only(req.side, &book);
    if price <= Decimal::ZERO {
        return Err(ExecutionFailure::VenueRejected);
    }

    let placed: PlacedOrder = match ops
        .place_post_only(&req.symbol, req.side, remaining, price, req.reduce_only)
        .await
    {
        Ok(o) => o,
        Err(e) => {
            log::warn!(
                "[{}] place_post_only round={} err={:?}",
                params.log_prefix,
                round,
                e
            );
            return Err(ExecutionFailure::VenueRejected);
        }
    };
    log::info!(
        "[{}] post_only placed round={} side={:?} qty={} price={} order_id={}",
        params.log_prefix,
        round,
        req.side,
        remaining,
        price,
        placed.order_id
    );

    let mut outcome = poll_until_terminal_or_deadline(
        ops,
        &req.symbol,
        &placed.order_id,
        params.chase_timeout_ms,
        params.poll_interval_ms,
        params.log_prefix,
    )
    .await;
    log::info!(
        "[{}] post_only round={} done filled={} cancelled={} order_id={}",
        params.log_prefix,
        round,
        outcome.filled_this_round,
        outcome.terminal_cancelled,
        placed.order_id
    );

    // Cancel residual regardless of outcome — idempotent on the mock,
    // harmless on a venue that has already terminated.
    let _ = ops.cancel(&req.symbol, &placed.order_id).await;

    // Lighter (#322): WS-lag grace re-poll AFTER cancel.
    // Extended skips this (chase_grace_poll_ms = 0).
    if outcome.filled_this_round.is_zero()
        && !outcome.terminal_cancelled
        && params.chase_grace_poll_ms > 0
    {
        tokio::time::sleep(Duration::from_millis(params.chase_grace_poll_ms)).await;
        match ops.poll_fill_status(&req.symbol, &placed.order_id).await {
            Ok(s) => {
                if s.filled_qty > Decimal::ZERO {
                    log::info!(
                        "[{}] post_only round={} grace-recovered \
                         filled={} terminal={} cancelled={} order_id={}",
                        params.log_prefix,
                        round,
                        s.filled_qty,
                        s.terminal,
                        s.cancelled,
                        placed.order_id
                    );
                    outcome = PollOutcome {
                        filled_this_round: s.filled_qty,
                        terminal_cancelled: s.cancelled,
                    };
                } else {
                    log::debug!(
                        "[{}] post_only round={} grace-poll no-late-fill \
                         terminal={} cancelled={} order_id={}",
                        params.log_prefix,
                        round,
                        s.terminal,
                        s.cancelled,
                        placed.order_id
                    );
                }
            }
            Err(e) => {
                log::warn!(
                    "[{}] post_only round={} grace-poll err={:?} order_id={}",
                    params.log_prefix,
                    round,
                    e,
                    placed.order_id
                );
            }
        }
    }

    Ok(outcome)
}

async fn run_taker_round<V: VenueOps + ?Sized>(
    ops: &V,
    params: &MakerLoopParams,
    req: &MakerRequest,
    residual: Decimal,
) -> Result<Decimal, ExecutionFailure> {
    let placed = match ops
        .place_taker(&req.symbol, req.side, residual, req.reduce_only)
        .await
    {
        Ok(o) => o,
        Err(e) => {
            log::warn!("[{}] place_taker err={:?}", params.log_prefix, e);
            return Err(ExecutionFailure::TakerRejected);
        }
    };
    log::info!(
        "[{}] taker placed side={:?} qty={} reduce_only={} order_id={}",
        params.log_prefix,
        req.side,
        residual,
        req.reduce_only,
        placed.order_id
    );
    let mut outcome = poll_until_terminal_or_deadline(
        ops,
        &req.symbol,
        &placed.order_id,
        params.chase_timeout_ms,
        params.poll_interval_ms,
        params.log_prefix,
    )
    .await;
    log::info!(
        "[{}] taker done filled={} cancelled={} order_id={}",
        params.log_prefix,
        outcome.filled_this_round,
        outcome.terminal_cancelled,
        placed.order_id
    );

    if params.taker_grace_before_cancel {
        // Extended (#298): re-poll BEFORE cancel so a fill that
        // landed at the venue still gets recorded if the cancel
        // races against it.
        outcome =
            maybe_taker_grace_repoll(ops, params, &req.symbol, &placed.order_id, outcome).await;
        let _ = ops.cancel(&req.symbol, &placed.order_id).await;
    } else {
        // Lighter (#322 taker): cancel first, then re-poll.
        let _ = ops.cancel(&req.symbol, &placed.order_id).await;
        outcome =
            maybe_taker_grace_repoll(ops, params, &req.symbol, &placed.order_id, outcome).await;
    }

    if outcome.filled_this_round > Decimal::ZERO {
        Ok(outcome.filled_this_round)
    } else if outcome.terminal_cancelled {
        Err(ExecutionFailure::TakerRejected)
    } else {
        Err(ExecutionFailure::Timeout)
    }
}

async fn maybe_taker_grace_repoll<V: VenueOps + ?Sized>(
    ops: &V,
    params: &MakerLoopParams,
    symbol: &str,
    order_id: &str,
    current: PollOutcome,
) -> PollOutcome {
    if !current.filled_this_round.is_zero()
        || current.terminal_cancelled
        || params.taker_grace_poll_ms == 0
    {
        return current;
    }
    tokio::time::sleep(Duration::from_millis(params.taker_grace_poll_ms)).await;
    match ops.poll_fill_status(symbol, order_id).await {
        Ok(s) => {
            if s.filled_qty > Decimal::ZERO {
                log::info!(
                    "[{}] taker grace-recovered filled={} terminal={} cancelled={} order_id={}",
                    params.log_prefix,
                    s.filled_qty,
                    s.terminal,
                    s.cancelled,
                    order_id
                );
                PollOutcome {
                    filled_this_round: s.filled_qty,
                    terminal_cancelled: s.cancelled,
                }
            } else {
                log::warn!(
                    "[{}] taker grace-poll no-late-fill terminal={} cancelled={} order_id={}",
                    params.log_prefix,
                    s.terminal,
                    s.cancelled,
                    order_id
                );
                current
            }
        }
        Err(e) => {
            log::warn!(
                "[{}] taker grace-poll err={:?} order_id={}",
                params.log_prefix,
                e,
                order_id
            );
            current
        }
    }
}

fn price_for_post_only(side: OrderSide, book: &TopOfBook) -> Decimal {
    match side {
        // Buy post-only at the best bid (rest passively).
        OrderSide::Long => book.best_bid,
        // Sell post-only at the best ask.
        OrderSide::Short => book.best_ask,
    }
}
