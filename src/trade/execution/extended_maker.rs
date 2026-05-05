//! Extended post-only place / chase / taker fallback
//! (bot-strategy#244 Group B).
//!
//! Drives one Extended entry or exit cycle to a single
//! [`ExtendedTerminal`] event. Handles:
//!
//! - Post-only place at the right top-of-book price.
//! - Chase: re-quote when the book moves through the resting order
//!   without filling. `chase_retries` cap; each round bounded by
//!   `chase_timeout_ms`.
//! - Taker fallback when chase exhausts and the residual qty is
//!   above dust.
//! - Partial-fill aggregation across rounds (catalogue case 6) so
//!   a 60 % maker fill + 40 % taker fallback emits one
//!   `ExtendedTerminal::Filled { qty: full }` rather than a stream
//!   of partial events the state machine can't make sense of.
//!
//! What this module does NOT own:
//!
//! - Time. The loop uses [`tokio::time::sleep`] / [`Instant`] —
//!   tests run under `tokio::time::pause` so deadline behavior is
//!   deterministic without sleeping for real wall-clock seconds.
//! - The reduce-only emergency-flatten retry loop (lives in
//!   `xvenue::live` next to the position machine; see
//!   `docs/execution_layer.md` §5).
//! - Lighter execution. That's `lighter_fill.rs`.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use dex_connector::OrderSide;
use rust_decimal::Decimal;

use super::poll_loop::{poll_until_terminal_or_deadline, Executor, PollOutcome};
use super::types::{ExecutionFailure, ExtendedMakerConfig, ExtendedTerminal};
use super::venue_ops::{PlacedOrder, VenueOps};

/// Inputs to one execution cycle.
#[derive(Debug, Clone)]
pub struct ExtendedEntryRequest {
    pub symbol: String,
    pub side: OrderSide,
    /// Total qty we want filled. Aggregator sums fills across
    /// chase rounds + taker fallback against this.
    pub target_qty: Decimal,
    /// Min qty to keep chasing for. Below this, the residual is
    /// treated as dust and the executor either returns the partial
    /// fill or escalates to taker (per `cfg.taker_fallback`).
    pub dust_qty: Decimal,
    /// bot-strategy#299: venue-side minimum order size. The taker
    /// fallback gate uses `residual > venue_min_qty.max(dust_qty)`,
    /// so a residual below this is treated as fully filled rather
    /// than handed to `place_taker` only to be rejected with
    /// "Order size N below min M" by the connector. 0 disables the
    /// guard (back-compat for tests / non-Extended venues).
    pub venue_min_qty: Decimal,
    /// Whether this is a reduce-only order (used by exit /
    /// emergency-flatten paths). Surfaces to `place_taker` so the
    /// venue rejects accidental position grows.
    pub reduce_only: bool,
}

pub struct ExtendedMakerLoop<'a, V: VenueOps + ?Sized> {
    pub ops: &'a V,
    pub cfg: &'a ExtendedMakerConfig,
    /// Cached at construction time from `cfg.common.poll_interval_ms`
    /// so `with_poll_interval` can override it for tests without
    /// mutating the borrowed `cfg`. Tight enough that a 500 ms
    /// timeout still catches a fill within ~50 ms of the venue
    /// reporting it.
    poll_interval_ms: u64,
}

impl<'a, V: VenueOps + ?Sized> ExtendedMakerLoop<'a, V> {
    pub fn new(ops: &'a V, cfg: &'a ExtendedMakerConfig) -> Self {
        Self {
            ops,
            cfg,
            poll_interval_ms: cfg.common.poll_interval_ms,
        }
    }

    /// Test hook — lets unit tests pin the poll cadence so a paused
    /// tokio clock can advance deterministically without depending
    /// on the default constant.
    pub fn with_poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms.max(1);
        self
    }

    /// Run one entry cycle. The returned terminal is what the
    /// runner translates into `Event::ExtendedFilled` /
    /// `Event::ExtendedFailed`.
    pub async fn run_entry(&self, req: ExtendedEntryRequest) -> ExtendedTerminal {
        if req.target_qty <= Decimal::ZERO {
            return ExtendedTerminal::Failed {
                reason: ExecutionFailure::VenueRejected,
            };
        }

        let mut total_filled = Decimal::ZERO;
        let mut last_failure: Option<ExecutionFailure> = None;

        // Maker chase loop. `cfg.post_only = false` short-circuits
        // straight to taker (operator escape valve for venue-degraded
        // scenarios where every post-only is rejected).
        if self.cfg.post_only {
            for round in 0..self.cfg.chase_retries.max(1) {
                let remaining = req.target_qty - total_filled;
                if remaining <= req.dust_qty {
                    break;
                }

                match self.run_one_chase_round(&req, remaining, round).await {
                    Ok(round_filled) => {
                        total_filled += round_filled.filled_this_round;
                        if round_filled.terminal_cancelled
                            && round_filled.filled_this_round.is_zero()
                        {
                            // Venue cancelled with zero fill — most
                            // likely the post-only price moved
                            // through. Continue to the next chase
                            // round (if any retries left) so we
                            // re-quote at the new book.
                            last_failure = Some(ExecutionFailure::Cancelled);
                            continue;
                        }
                        if total_filled >= req.target_qty - req.dust_qty {
                            return ExtendedTerminal::Filled { qty: total_filled };
                        }
                    }
                    Err(failure) => {
                        last_failure = Some(failure);
                        // Hard place / read failure — break out of
                        // the chase and fall through to taker
                        // fallback. We don't keep retrying place_*
                        // since the venue is signalling a problem.
                        break;
                    }
                }
            }
        }

        // If chase didn't fill it all, decide whether to fall
        // through to taker for the residual. bot-strategy#299: also
        // gate against the venue minimum order size — a residual
        // smaller than `venue_min_qty` cannot be placed and would
        // emit a noisy `place_taker err=Order size 0 below min` WARN
        // for what is effectively a clean post_only fill.
        let residual = req.target_qty - total_filled;
        let taker_min_threshold = req.venue_min_qty.max(req.dust_qty);
        if residual > taker_min_threshold && self.cfg.taker_fallback {
            match self.run_taker_round(&req, residual).await {
                Ok(taker_filled) => {
                    total_filled += taker_filled;
                }
                Err(failure) => {
                    if total_filled.is_zero() {
                        return ExtendedTerminal::Failed { reason: failure };
                    }
                    // Got a partial maker fill — surface the partial
                    // so the state machine can route via skew
                    // monitor instead of dead-ending in Failed.
                    return ExtendedTerminal::Filled { qty: total_filled };
                }
            }
        }

        if total_filled > Decimal::ZERO {
            ExtendedTerminal::Filled { qty: total_filled }
        } else {
            ExtendedTerminal::Failed {
                reason: last_failure.unwrap_or(ExecutionFailure::PostOnlyExhausted),
            }
        }
    }

    /// Inner: one place + poll-until-filled-or-timeout + cancel
    /// cycle. Returns the qty filled this round and a `cancelled`
    /// flag the outer chase uses to decide whether to re-quote.
    async fn run_one_chase_round(
        &self,
        req: &ExtendedEntryRequest,
        remaining: Decimal,
        round: u32,
    ) -> Result<PollOutcome, ExecutionFailure> {
        let book = match self.ops.read_top_of_book(&req.symbol).await {
            Ok(b) => b,
            Err(e) => {
                log::warn!(
                    "[XVENUE/extmaker] read_top_of_book round={} err={:?}",
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

        let placed: PlacedOrder = match self
            .ops
            .place_post_only(&req.symbol, req.side, remaining, price, req.reduce_only)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                log::warn!(
                    "[XVENUE/extmaker] place_post_only round={} err={:?}",
                    round,
                    e
                );
                return Err(ExecutionFailure::VenueRejected);
            }
        };
        log::info!(
            "[XVENUE/extmaker] post_only placed round={} side={:?} qty={} price={} order_id={}",
            round,
            req.side,
            remaining,
            price,
            placed.order_id
        );

        let outcome = poll_until_terminal_or_deadline(
            self.ops,
            &req.symbol,
            &placed.order_id,
            self.cfg.chase_timeout_ms,
            self.poll_interval_ms,
            "XVENUE/extmaker",
        )
        .await;
        log::info!(
            "[XVENUE/extmaker] post_only round={} done filled={} cancelled={} order_id={}",
            round,
            outcome.filled_this_round,
            outcome.terminal_cancelled,
            placed.order_id
        );

        // Cancel residual regardless of outcome — Idempotent on the
        // mock, harmless on a venue that has already terminated the
        // order.
        let _ = self.ops.cancel(&req.symbol, &placed.order_id).await;

        Ok(outcome)
    }

    async fn run_taker_round(
        &self,
        req: &ExtendedEntryRequest,
        residual: Decimal,
    ) -> Result<Decimal, ExecutionFailure> {
        let placed = match self
            .ops
            .place_taker(&req.symbol, req.side, residual, req.reduce_only)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                log::warn!("[XVENUE/extmaker] place_taker err={:?}", e);
                return Err(ExecutionFailure::TakerRejected);
            }
        };
        log::info!(
            "[XVENUE/extmaker] taker placed side={:?} qty={} reduce_only={} order_id={}",
            req.side,
            residual,
            req.reduce_only,
            placed.order_id
        );
        let mut outcome = poll_until_terminal_or_deadline(
            self.ops,
            &req.symbol,
            &placed.order_id,
            self.cfg.chase_timeout_ms,
            self.poll_interval_ms,
            "XVENUE/extmaker",
        )
        .await;
        log::info!(
            "[XVENUE/extmaker] taker done filled={} cancelled={} order_id={}",
            outcome.filled_this_round,
            outcome.terminal_cancelled,
            placed.order_id
        );

        // bot-strategy#298: WS-lag grace re-poll. The 2026-05-03
        // EXIT-side timeouts (cases C/D in #298) all showed the
        // taker landing on `filled=0 cancelled=false` — an IOC that
        // neither filled nor terminal-cancelled within the chase
        // deadline. That's the WS feed lagging the venue's actual
        // result; one extra poll after a short grace lets a late
        // fill surface as Filled rather than fall through to
        // EmergencyFlattening on what was a clean fill. Runs before
        // cancel so a fill that landed at the venue still gets
        // recorded if the cancel races against it.
        if outcome.filled_this_round.is_zero()
            && !outcome.terminal_cancelled
            && self.cfg.taker_grace_poll_ms > 0
        {
            tokio::time::sleep(Duration::from_millis(self.cfg.taker_grace_poll_ms)).await;
            match self
                .ops
                .poll_fill_status(&req.symbol, &placed.order_id)
                .await
            {
                Ok(s) => {
                    if s.filled_qty > Decimal::ZERO {
                        log::info!(
                            "[XVENUE/extmaker] taker grace-recovered \
                             filled={} terminal={} cancelled={} order_id={}",
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
                        log::warn!(
                            "[XVENUE/extmaker] taker grace-poll no-late-fill \
                             terminal={} cancelled={} order_id={}",
                            s.terminal,
                            s.cancelled,
                            placed.order_id
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[XVENUE/extmaker] taker grace-poll err={:?} order_id={}",
                        e,
                        placed.order_id
                    );
                }
            }
        }

        let _ = self.ops.cancel(&req.symbol, &placed.order_id).await;
        if outcome.filled_this_round > Decimal::ZERO {
            Ok(outcome.filled_this_round)
        } else if outcome.terminal_cancelled {
            Err(ExecutionFailure::TakerRejected)
        } else {
            Err(ExecutionFailure::Timeout)
        }
    }
}

#[async_trait]
impl<'a, V: VenueOps + ?Sized + Sync> Executor for ExtendedMakerLoop<'a, V> {
    type Request = ExtendedEntryRequest;
    type Terminal = ExtendedTerminal;

    async fn run(&self, req: Self::Request) -> Self::Terminal {
        self.run_entry(req).await
    }
}

fn price_for_post_only(side: OrderSide, book: &super::venue_ops::TopOfBook) -> Decimal {
    match side {
        // Buy post-only at the best bid (rest passively).
        OrderSide::Long => book.best_bid,
        // Sell post-only at the best ask.
        OrderSide::Short => book.best_ask,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::types::{CommonExecutorConfig, ExtendedMakerConfig};
    use crate::trade::execution::venue_ops::{
        OrderFillStatus, PlacedOrder, ScriptedResponse, ScriptedVenueOps, TopOfBook,
    };
    use rust_decimal_macros::dec;

    fn cfg_with_taker_fallback() -> ExtendedMakerConfig {
        ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 3,
            chase_timeout_ms: 500,
            taker_fallback: true,
            post_only: true,
            taker_grace_poll_ms: 0,
        }
    }

    fn cfg_no_fallback() -> ExtendedMakerConfig {
        ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 2,
            chase_timeout_ms: 500,
            taker_fallback: false,
            post_only: true,
            taker_grace_poll_ms: 0,
        }
    }

    fn req_long(qty: Decimal) -> ExtendedEntryRequest {
        ExtendedEntryRequest {
            symbol: "BTC-USD".to_string(),
            side: OrderSide::Long,
            target_qty: qty,
            dust_qty: dec!(0.0001),
            venue_min_qty: Decimal::ZERO,
            reduce_only: false,
        }
    }

    /// Catalogue case: post-only fills fully on the first round.
    #[tokio::test(start_paused = true)]
    async fn post_only_fills_in_one_round() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            // Single poll returns terminal-filled with the full qty.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = cfg_with_taker_fallback();
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.1) });
        let posts = ops.snapshot_posts();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].3, dec!(78000)); // post-only at best bid
        assert!(ops.snapshot_takers().is_empty());
    }

    /// Catalogue case 1: chase exhausted, fallback disabled →
    /// `PostOnlyExhausted`.
    #[tokio::test(start_paused = true)]
    async fn chase_exhausted_no_fallback_returns_post_only_exhausted() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            // Default fill status = no fill, not terminal — every
            // chase round will exhaust its timeout.
        });
        let cfg = cfg_no_fallback();
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(50);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        assert_eq!(
            res,
            ExtendedTerminal::Failed {
                reason: ExecutionFailure::PostOnlyExhausted
            }
        );
        // 2 chase rounds × 1 post per round.
        assert_eq!(ops.snapshot_posts().len(), 2);
        assert!(ops.snapshot_takers().is_empty());
        // Cancel called once per round.
        assert_eq!(ops.snapshot_cancels().len(), 2);
    }

    /// Catalogue case 2: chase + taker fallback succeeds.
    /// FIFO queue: pre-fill with non-terminal zero polls so the
    /// maker times out, then a terminal-filled for the taker call.
    #[tokio::test(start_paused = true)]
    async fn chase_then_taker_fallback_succeeds() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            // Maker round 1 polls (chase_timeout_ms=100,
            // poll_interval=20 → ~5 polls before deadline). Push
            // enough non-terminal-zero responses so they fall
            // through to default and the maker times out.
            for _ in 0..6 {
                s.poll_fill
                    .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                        filled_qty: Decimal::ZERO,
                        terminal: false,
                        cancelled: false,
                    }));
            }
            s.place_taker
                .push_back(ScriptedResponse::PlacedOrder(PlacedOrder {
                    order_id: "taker-1".into(),
                }));
            // Taker poll returns terminal-filled — popped after the
            // maker's 6 non-terminal polls.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1, // single chase round, then fallback
            chase_timeout_ms: 100,
            taker_fallback: true,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(20);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.1) });
        assert_eq!(ops.snapshot_posts().len(), 1);
        assert_eq!(ops.snapshot_takers().len(), 1);
    }

    /// Catalogue case 6: partial fill on maker, taker mops up the
    /// residual, aggregator emits one Filled with full qty.
    #[tokio::test(start_paused = true)]
    async fn partial_maker_then_taker_emits_aggregated_filled() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            // FIFO order: first push = first popped.
            // Maker round 1 polls — partial 0.04, then terminal at 0.04.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.04),
                    terminal: false,
                    cancelled: false,
                }));
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.04),
                    terminal: true,
                    cancelled: false,
                }));
            // Taker fallback poll: fills the 0.06 residual.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.06),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 200,
            taker_fallback: true,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(20);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        // Maker 0.04 + taker 0.06 = 0.10.
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.1) });
    }

    /// Post-only short uses best_ask, not best_bid.
    #[tokio::test(start_paused = true)]
    async fn post_only_short_uses_best_ask() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = cfg_with_taker_fallback();
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let mut req = req_long(dec!(0.1));
        req.side = OrderSide::Short;
        let _ = lp.run_entry(req).await;
        let posts = ops.snapshot_posts();
        assert_eq!(posts[0].3, dec!(78001)); // best_ask
    }

    /// `post_only=false` skips maker entirely → goes straight to
    /// taker. Operator-emergency mode.
    #[tokio::test(start_paused = true)]
    async fn post_only_false_goes_straight_to_taker() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 500,
            taker_fallback: true,
            post_only: false,
            taker_grace_poll_ms: 0,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.1) });
        assert!(ops.snapshot_posts().is_empty());
        assert_eq!(ops.snapshot_takers().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_target_qty_returns_failed() {
        let ops = ScriptedVenueOps::new();
        let cfg = cfg_with_taker_fallback();
        let lp = ExtendedMakerLoop::new(&ops, &cfg);
        let res = lp.run_entry(req_long(Decimal::ZERO)).await;
        assert_eq!(
            res,
            ExtendedTerminal::Failed {
                reason: ExecutionFailure::VenueRejected
            }
        );
    }

    /// Place error escalates to taker fallback (since fallback is on).
    #[tokio::test(start_paused = true)]
    async fn place_error_falls_through_to_taker() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.place_post_only
                .push_back(ScriptedResponse::Err("auth fail".into()));
            // Taker succeeds.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = cfg_with_taker_fallback();
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.1) });
    }

    /// Place error + no fallback → VenueRejected.
    #[tokio::test(start_paused = true)]
    async fn place_error_without_fallback_returns_venue_rejected() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.place_post_only
                .push_back(ScriptedResponse::Err("auth fail".into()));
        });
        let cfg = cfg_no_fallback();
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        assert_eq!(
            res,
            ExtendedTerminal::Failed {
                reason: ExecutionFailure::VenueRejected
            }
        );
    }

    /// Cancelled by venue (post-only price moved through the book) →
    /// retry chase round with new book.
    #[tokio::test(start_paused = true)]
    async fn venue_cancel_advances_to_next_chase_round() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            // FIFO: round 1 first → terminal-cancelled with zero fill.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.0),
                    terminal: true,
                    cancelled: true,
                }));
            // Round 2 next → terminal-filled.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 2,
            chase_timeout_ms: 100,
            taker_fallback: false,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(20);
        let res = lp.run_entry(req_long(dec!(0.1))).await;
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.1) });
        assert_eq!(ops.snapshot_posts().len(), 2);
    }

    /// Reduce-only flag propagates to taker call.
    #[tokio::test(start_paused = true)]
    async fn reduce_only_flag_reaches_taker() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 100,
            taker_fallback: true,
            post_only: false,
            taker_grace_poll_ms: 0,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let req = ExtendedEntryRequest {
            reduce_only: true,
            ..req_long(dec!(0.1))
        };
        let _ = lp.run_entry(req).await;
        let takers = ops.snapshot_takers();
        assert_eq!(takers.len(), 1);
        assert!(
            takers[0].3,
            "reduce_only flag must propagate to place_taker"
        );
    }

    /// bot-strategy#299: when post_only fills `target_qty - small`
    /// and the residual is below `venue_min_qty`, the taker fallback
    /// must NOT be invoked — otherwise the connector rejects with
    /// "Order size N below min M" and emits a noisy WARN. The aggregated
    /// fill should still be reported as `Filled { qty: post_only_total }`.
    #[tokio::test(start_paused = true)]
    async fn taker_fallback_skipped_when_residual_below_venue_min() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(2300),
                best_ask: dec!(2301),
            };
            // Single chase round fills 0.021 of the 0.0216 target.
            // residual = 0.0006 < venue_min_qty (0.01) → no taker.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.021),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 100,
            taker_fallback: true,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(20);
        let req = ExtendedEntryRequest {
            symbol: "ETH-USD".to_string(),
            side: OrderSide::Long,
            target_qty: dec!(0.0216),
            dust_qty: dec!(0.00001),
            venue_min_qty: dec!(0.01),
            reduce_only: false,
        };
        let res = lp.run_entry(req).await;
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.021) });
        assert!(
            ops.snapshot_takers().is_empty(),
            "taker must not be called when residual < venue_min_qty"
        );
    }

    /// bot-strategy#299: with `venue_min_qty=0` (the default for
    /// non-Extended venues / tests), the existing dust-only behavior
    /// is preserved — a residual above dust still escalates to taker.
    #[tokio::test(start_paused = true)]
    async fn taker_fallback_invoked_when_venue_min_qty_disabled() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(2300),
                best_ask: dec!(2301),
            };
            // post_only round 0: 0.021 of 0.0216 (zero-fill cancelled
            // would loop, so report terminal-filled with the partial).
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.021),
                    terminal: true,
                    cancelled: false,
                }));
            // Taker poll fills the 0.0006 residual.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.0006),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 100,
            taker_fallback: true,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(20);
        // venue_min_qty=0 keeps the old behavior — residual > dust → taker.
        let req = req_long(dec!(0.0216));
        let res = lp.run_entry(req).await;
        assert_eq!(res, ExtendedTerminal::Filled { qty: dec!(0.0216) });
        assert_eq!(
            ops.snapshot_takers().len(),
            1,
            "taker must be invoked when venue_min_qty is disabled"
        );
    }

    /// bot-strategy#298: when the taker round reports
    /// `filled=0 cancelled=false` (WS lag), and `taker_grace_poll_ms`
    /// is configured, the executor re-polls once after the grace
    /// window and accepts a fill that landed late.
    #[tokio::test(start_paused = true)]
    async fn taker_grace_poll_recovers_late_fill() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(2300),
                best_ask: dec!(2301),
            };
            // Maker stage skipped (post_only=false); straight to taker.
            // First poll-loop reports filled=0 cancelled=false until
            // the deadline (FillStatus with terminal=false stays open).
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: Decimal::ZERO,
                    terminal: false,
                    cancelled: false,
                }));
            // The grace re-poll sees the late fill.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.021),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 50,
            taker_fallback: true,
            post_only: false,
            taker_grace_poll_ms: 200,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run_entry(req_long(dec!(0.021))).await;
        assert_eq!(
            res,
            ExtendedTerminal::Filled { qty: dec!(0.021) },
            "grace-poll should recover the late fill instead of returning Failed{{Timeout}}"
        );
    }

    /// bot-strategy#298: when grace-poll is enabled but no fill ever
    /// lands, the round still falls through to Timeout (no false
    /// recovery).
    #[tokio::test(start_paused = true)]
    async fn taker_grace_poll_no_late_fill_still_times_out() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(2300),
                best_ask: dec!(2301),
            };
            // Initial taker poll: never terminal.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: Decimal::ZERO,
                    terminal: false,
                    cancelled: false,
                }));
            // Grace re-poll: still no fill.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: Decimal::ZERO,
                    terminal: false,
                    cancelled: false,
                }));
        });
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 50,
            taker_fallback: true,
            post_only: false,
            taker_grace_poll_ms: 200,
        };
        let lp = ExtendedMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run_entry(req_long(dec!(0.021))).await;
        assert_eq!(
            res,
            ExtendedTerminal::Failed {
                reason: ExecutionFailure::Timeout
            }
        );
    }
}
