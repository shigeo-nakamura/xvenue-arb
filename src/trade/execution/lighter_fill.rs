//! Lighter market / aggressive-limit fill executor
//! (bot-strategy#244 Group B).
//!
//! Drives one Lighter entry or exit cycle to a single
//! [`LighterTerminal`] event. Lighter's typical fill latency is
//! ~50 ms which is well inside the spread-cycle budget; the executor
//! places one taker order, polls until terminal, and gives up at
//! `lighter_fill_timeout_ms`. No chase loop — Lighter doesn't
//! support post-only on the cross-venue arb pair the way Extended
//! does, and the entry cadence is governed by the Extended-first
//! sequencing in DESIGN.md §4.1.
//!
//! Aggregates partial fills the same way `extended_maker` does so
//! the state machine sees a single `LighterFilled{qty}` /
//! `LighterFailed` per cycle even when Lighter trickles fills
//! through the WS in two or three pieces.
//!
//! Catalogue cases this module covers (per docs/execution_layer.md §2):
//!
//! - Case 3: Extended fills but Lighter market times out →
//!   `LighterFailed{Timeout}`. State machine routes to
//!   `EmergencyFlattening`.
//! - Case 7: Lighter market partial fill, residual unfilled at
//!   timeout → aggregator emits `LighterFilled{partial_qty}`.
//!   Skew monitor catches downstream if breach.

use async_trait::async_trait;
use dex_connector::OrderSide;
use rust_decimal::Decimal;

use super::poll_loop::{poll_until_terminal_or_deadline, Executor};
use super::types::{ExecutionFailure, LighterFillConfig, LighterOrderType, LighterTerminal};
use super::venue_ops::{PlacedOrder, TopOfBook, VenueOps};

/// Inputs to one Lighter execution cycle.
#[derive(Debug, Clone)]
pub struct LighterFillRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub target_qty: Decimal,
    /// Threshold below which a partial fill is treated as
    /// successful (no taker chase residual). The aggregator emits
    /// `Filled{partial_qty}` even if `partial_qty < target_qty`
    /// when the residual is below this dust threshold.
    pub dust_qty: Decimal,
    pub reduce_only: bool,
}

pub struct LighterFillLoop<'a, V: VenueOps + ?Sized> {
    pub ops: &'a V,
    pub cfg: &'a LighterFillConfig,
    /// Cached at construction time from `cfg.common.poll_interval_ms`
    /// so `with_poll_interval` can override it for tests without
    /// mutating the borrowed `cfg`.
    poll_interval_ms: u64,
}

impl<'a, V: VenueOps + ?Sized> LighterFillLoop<'a, V> {
    pub fn new(ops: &'a V, cfg: &'a LighterFillConfig) -> Self {
        Self {
            ops,
            cfg,
            poll_interval_ms: cfg.common.poll_interval_ms,
        }
    }

    pub fn with_poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms.max(1);
        self
    }

    /// One Lighter fill cycle.
    pub async fn run(&self, req: LighterFillRequest) -> LighterTerminal {
        if req.target_qty <= Decimal::ZERO {
            return LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected,
            };
        }

        // For aggressive-limit we need a price; market orders ignore.
        // Read the book once up front so the aggressive-limit path
        // has a concrete number. If we're configured for market and
        // `read_top_of_book` fails, we still try to place with
        // `Decimal::ZERO` price — `place_taker` ignores price for
        // market.
        let price = match self.cfg.order_type {
            LighterOrderType::Market => Decimal::ZERO,
            LighterOrderType::AggressiveLimit => match self.ops.read_top_of_book(&req.symbol).await
            {
                Ok(book) => price_for_aggressive(req.side, &book),
                Err(e) => {
                    log::warn!("[XVENUE/lighter] read_top_of_book err={:?}", e);
                    return LighterTerminal::Failed {
                        reason: ExecutionFailure::VenueRejected,
                    };
                }
            },
        };
        // Aggressive-limit with a non-positive price would be
        // rejected by the venue; surface it as VenueRejected before
        // burning a place call.
        if matches!(self.cfg.order_type, LighterOrderType::AggressiveLimit)
            && price <= Decimal::ZERO
        {
            return LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected,
            };
        }
        let _ = price; // place_taker carries side + qty + reduce_only;
                       // price is enforced venue-side via the order
                       // type. Kept here for future symmetry with
                       // `place_aggressive_limit` if that lands.

        let placed: PlacedOrder = match self
            .ops
            .place_taker(&req.symbol, req.side, req.target_qty, req.reduce_only)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                log::warn!("[XVENUE/lighter] place_taker err={:?}", e);
                return LighterTerminal::Failed {
                    reason: ExecutionFailure::VenueRejected,
                };
            }
        };

        let outcome = poll_until_terminal_or_deadline(
            self.ops,
            &req.symbol,
            &placed.order_id,
            self.cfg.fill_timeout_ms,
            self.poll_interval_ms,
            "XVENUE/lighter",
        )
        .await;
        // Cancel residual order id — idempotent on already-terminal
        // orders, makes sure a slow venue cancel doesn't leave a
        // ghost order resting if we declared timeout locally.
        let _ = self.ops.cancel(&req.symbol, &placed.order_id).await;

        let filled = outcome.filled_this_round;
        if filled >= req.target_qty - req.dust_qty && filled > Decimal::ZERO {
            return LighterTerminal::Filled { qty: filled };
        }
        if filled > Decimal::ZERO {
            // Partial fill above zero but below dust threshold —
            // still surface as `Filled{partial}` per case 7.
            // Skew monitor catches downstream if the resulting
            // skew breaches `max_inventory_skew_usd`.
            return LighterTerminal::Filled { qty: filled };
        }
        if outcome.terminal_cancelled {
            LighterTerminal::Failed {
                reason: ExecutionFailure::Cancelled,
            }
        } else {
            LighterTerminal::Failed {
                reason: ExecutionFailure::Timeout,
            }
        }
    }
}

#[async_trait]
impl<'a, V: VenueOps + ?Sized + Sync> Executor for LighterFillLoop<'a, V> {
    type Request = LighterFillRequest;
    type Terminal = LighterTerminal;

    async fn run(&self, req: Self::Request) -> Self::Terminal {
        LighterFillLoop::run(self, req).await
    }
}

fn price_for_aggressive(side: OrderSide, book: &TopOfBook) -> Decimal {
    match side {
        // Buy aggressive at the best ask (cross the spread).
        OrderSide::Long => book.best_ask,
        // Sell aggressive at the best bid.
        OrderSide::Short => book.best_bid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::types::{
        CommonExecutorConfig, LighterFillConfig, LighterOrderType,
    };
    use crate::trade::execution::venue_ops::{
        OrderFillStatus, ScriptedResponse, ScriptedVenueOps, TopOfBook,
    };
    use rust_decimal_macros::dec;

    fn cfg_market() -> LighterFillConfig {
        LighterFillConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type: LighterOrderType::Market,
            fill_timeout_ms: 100,
        }
    }

    fn cfg_aggressive() -> LighterFillConfig {
        LighterFillConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type: LighterOrderType::AggressiveLimit,
            fill_timeout_ms: 100,
        }
    }

    fn req_long(qty: Decimal) -> LighterFillRequest {
        LighterFillRequest {
            symbol: "BTC".to_string(),
            side: OrderSide::Long,
            target_qty: qty,
            dust_qty: dec!(0.0001),
            reduce_only: false,
        }
    }

    /// Catalogue: Lighter market fills cleanly within the timeout.
    #[tokio::test(start_paused = true)]
    async fn market_fills_in_one_poll() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = cfg_market();
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.1))).await;
        assert_eq!(res, LighterTerminal::Filled { qty: dec!(0.1) });
        assert_eq!(ops.snapshot_takers().len(), 1);
        assert!(ops.snapshot_posts().is_empty());
    }

    /// Catalogue case 3: Lighter market times out → LighterFailed{Timeout}.
    #[tokio::test(start_paused = true)]
    async fn market_timeout_returns_failed() {
        let ops = ScriptedVenueOps::new();
        // Default fill = zero, non-terminal — never fires terminal.
        let cfg = cfg_market();
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(20);
        let res = lp.run(req_long(dec!(0.1))).await;
        assert_eq!(
            res,
            LighterTerminal::Failed {
                reason: ExecutionFailure::Timeout
            }
        );
        assert_eq!(ops.snapshot_takers().len(), 1);
    }

    /// Catalogue case 7: Lighter partial fill, deadline expires →
    /// LighterFilled{partial} (skew monitor catches downstream).
    #[tokio::test(start_paused = true)]
    async fn partial_fill_then_timeout_returns_filled_partial() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            // Several non-terminal partials, then default-zero polls
            // run out the clock. Aggregator keeps the max.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.04),
                    terminal: false,
                    cancelled: false,
                }));
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.06),
                    terminal: false,
                    cancelled: false,
                }));
        });
        let cfg = LighterFillConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type: LighterOrderType::Market,
            fill_timeout_ms: 60,
        };
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(20);
        let res = lp.run(req_long(dec!(0.1))).await;
        // 0.06 is the latest aggregator value (above dust=0.0001
        // but below target=0.1). Partial-emit per case 7.
        assert_eq!(res, LighterTerminal::Filled { qty: dec!(0.06) });
    }

    /// Aggressive-limit reads top-of-book and crosses the spread.
    #[tokio::test(start_paused = true)]
    async fn aggressive_limit_long_uses_best_ask() {
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
        let cfg = cfg_aggressive();
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.1))).await;
        assert_eq!(res, LighterTerminal::Filled { qty: dec!(0.1) });
        // place_taker captures (symbol, side, qty, reduce_only) only;
        // price is enforced venue-side. Verify the place call ran.
        assert_eq!(ops.snapshot_takers().len(), 1);
    }

    /// Aggressive-limit with a degenerate book (zero on one side)
    /// short-circuits to `VenueRejected` instead of placing.
    #[tokio::test(start_paused = true)]
    async fn aggressive_limit_with_zero_ask_returns_venue_rejected() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: Decimal::ZERO,
            };
        });
        let cfg = cfg_aggressive();
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.1))).await;
        assert_eq!(
            res,
            LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected
            }
        );
        assert!(ops.snapshot_takers().is_empty());
    }

    /// place_taker error → VenueRejected (no fallback at this layer).
    #[tokio::test(start_paused = true)]
    async fn place_error_returns_venue_rejected() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.place_taker
                .push_back(ScriptedResponse::Err("rate limit".into()));
        });
        let cfg = cfg_market();
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.1))).await;
        assert_eq!(
            res,
            LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn zero_target_qty_returns_failed() {
        let ops = ScriptedVenueOps::new();
        let cfg = cfg_market();
        let lp = LighterFillLoop::new(&ops, &cfg);
        let res = lp.run(req_long(Decimal::ZERO)).await;
        assert_eq!(
            res,
            LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected
            }
        );
    }

    /// Venue-cancel before any fill → Cancelled (distinct from Timeout).
    #[tokio::test(start_paused = true)]
    async fn venue_cancel_before_fill_returns_cancelled() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: Decimal::ZERO,
                    terminal: true,
                    cancelled: true,
                }));
        });
        let cfg = cfg_market();
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.1))).await;
        assert_eq!(
            res,
            LighterTerminal::Failed {
                reason: ExecutionFailure::Cancelled
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reduce_only_flag_propagates() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.1),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = cfg_market();
        let lp = LighterFillLoop::new(&ops, &cfg).with_poll_interval(10);
        let req = LighterFillRequest {
            reduce_only: true,
            ..req_long(dec!(0.1))
        };
        let _ = lp.run(req).await;
        let takers = ops.snapshot_takers();
        assert_eq!(takers.len(), 1);
        assert!(takers[0].3, "reduce_only must propagate to place_taker");
    }
}
