//! Lighter post-only place / chase / taker fallback
//! (bot-strategy#309 step 6: maker-on-Lighter execution redesign).
//!
//! Mirrors [`super::extended_maker::ExtendedMakerLoop`] for Lighter.
//! Drives one Lighter entry or exit cycle to a single
//! [`LighterTerminal`] event. Handles:
//!
//! - Post-only place at the right top-of-book price.
//! - Chase: re-quote when the book moves through the resting order
//!   without filling. `chase_retries` cap; each round bounded by
//!   `chase_timeout_ms`.
//! - Taker fallback when chase exhausts and the residual qty is above
//!   dust.
//! - Partial-fill aggregation across rounds so the state machine sees
//!   one `LighterFilled { qty: full }` per cycle.
//!
//! ## Why this exists
//!
//! Phase 0 of #309 confirmed that Lighter inside-spread is volatile and
//! captureable (mean ~13 bps over 5.28d ETH dump). Routing the Lighter
//! leg as a post_only maker — instead of the legacy market taker — lets
//! the bot earn that spread instead of paying it. Use this loop only
//! once the dex-connector verification gate passes (issue body's
//! "verify Lighter post_only + cancel-order paths work as expected at
//! \$50 notional"); the runner switch is gated on `lighter_post_only`
//! in the YAML.
//!
//! ## What this module does NOT own
//!
//! - The `lighter_post_only` runner switch — see `xvenue::live`.
//! - Cross-venue `leg_mismatch_timeout_ms` arithmetic — see
//!   `LighterMakerConfig::worst_case_budget_ms` + the validator in
//!   `xvenue::config` that rejects YAMLs where the chase budget breaks
//!   the #288 invariant.
//! - The grace-poll WS-lag recovery (Extended's #298 fix) — Lighter's
//!   fill latency is ~50 ms with no observed history of late-fill
//!   races, so the simpler chase loop applies.

use anyhow::Result;
use async_trait::async_trait;
use dex_connector::OrderSide;
use rust_decimal::Decimal;

use super::poll_loop::{poll_until_terminal_or_deadline, Executor, PollOutcome};
use super::types::{ExecutionFailure, LighterMakerConfig, LighterTerminal};
use super::venue_ops::{PlacedOrder, VenueOps};

/// Inputs to one Lighter post-only entry / exit cycle.
#[derive(Debug, Clone)]
pub struct LighterMakerRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub target_qty: Decimal,
    /// Below this residual, the loop treats the cycle as fully filled
    /// rather than chasing further or invoking taker fallback.
    pub dust_qty: Decimal,
    /// Reduce-only is required on exit / emergency-flatten paths so a
    /// race between place_post_only rounds can't accidentally flip the
    /// position to the opposite direction (mirrors the Extended-side
    /// rationale recorded on `VenueOps::place_post_only`).
    pub reduce_only: bool,
}

pub struct LighterMakerLoop<'a, V: VenueOps + ?Sized> {
    pub ops: &'a V,
    pub cfg: &'a LighterMakerConfig,
    poll_interval_ms: u64,
}

impl<'a, V: VenueOps + ?Sized> LighterMakerLoop<'a, V> {
    pub fn new(ops: &'a V, cfg: &'a LighterMakerConfig) -> Self {
        Self {
            ops,
            cfg,
            poll_interval_ms: cfg.common.poll_interval_ms,
        }
    }

    /// Test hook — pin the poll cadence so `tokio::time::pause` tests
    /// can advance deterministically without depending on the
    /// production default.
    pub fn with_poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms.max(1);
        self
    }

    /// Run one entry / exit cycle. The returned terminal is what the
    /// runner translates into `Event::LighterFilled` /
    /// `Event::LighterFailed`.
    pub async fn run(&self, req: LighterMakerRequest) -> LighterTerminal {
        if req.target_qty <= Decimal::ZERO {
            return LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected,
            };
        }

        let mut total_filled = Decimal::ZERO;
        let mut last_failure: Option<ExecutionFailure> = None;

        // Maker chase loop. `cfg.post_only = false` short-circuits
        // straight to taker — operator escape valve mirroring Extended.
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
                            // Venue cancelled with zero fill — likely
                            // the post-only price moved through. Re-
                            // quote at the new book on the next round.
                            last_failure = Some(ExecutionFailure::Cancelled);
                            continue;
                        }
                        if total_filled >= req.target_qty - req.dust_qty {
                            return LighterTerminal::Filled { qty: total_filled };
                        }
                    }
                    Err(failure) => {
                        last_failure = Some(failure);
                        // Hard place / read failure — break out of the
                        // chase and decide whether taker fallback
                        // applies. Don't keep retrying place_* against
                        // a venue that just signalled an error.
                        break;
                    }
                }
            }
        }

        let residual = req.target_qty - total_filled;
        if residual > req.dust_qty && self.cfg.taker_fallback {
            match self.run_taker_round(&req, residual).await {
                Ok(taker_filled) => {
                    total_filled += taker_filled;
                }
                Err(failure) => {
                    if total_filled.is_zero() {
                        return LighterTerminal::Failed { reason: failure };
                    }
                    // Got a partial maker fill — surface so the state
                    // machine can route via skew monitor instead of
                    // dead-ending in Failed.
                    return LighterTerminal::Filled { qty: total_filled };
                }
            }
        }

        if total_filled > Decimal::ZERO {
            LighterTerminal::Filled { qty: total_filled }
        } else {
            LighterTerminal::Failed {
                reason: last_failure.unwrap_or(ExecutionFailure::PostOnlyExhausted),
            }
        }
    }

    async fn run_one_chase_round(
        &self,
        req: &LighterMakerRequest,
        remaining: Decimal,
        round: u32,
    ) -> Result<PollOutcome, ExecutionFailure> {
        let book = match self.ops.read_top_of_book(&req.symbol).await {
            Ok(b) => b,
            Err(e) => {
                log::warn!(
                    "[XVENUE/lightmaker] read_top_of_book round={} err={:?}",
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
                    "[XVENUE/lightmaker] place_post_only round={} err={:?}",
                    round,
                    e
                );
                return Err(ExecutionFailure::VenueRejected);
            }
        };
        log::info!(
            "[XVENUE/lightmaker] post_only placed round={} side={:?} qty={} price={} order_id={}",
            round, req.side, remaining, price, placed.order_id
        );

        let outcome = poll_until_terminal_or_deadline(
            self.ops,
            &req.symbol,
            &placed.order_id,
            self.cfg.chase_timeout_ms,
            self.poll_interval_ms,
            "XVENUE/lightmaker",
        )
        .await;
        log::info!(
            "[XVENUE/lightmaker] post_only round={} done filled={} cancelled={} order_id={}",
            round, outcome.filled_this_round, outcome.terminal_cancelled, placed.order_id
        );

        // Cancel residual regardless of outcome — idempotent on the
        // mock, harmless on a venue that has already terminated.
        let _ = self.ops.cancel(&req.symbol, &placed.order_id).await;

        Ok(outcome)
    }

    async fn run_taker_round(
        &self,
        req: &LighterMakerRequest,
        residual: Decimal,
    ) -> Result<Decimal, ExecutionFailure> {
        let placed = match self
            .ops
            .place_taker(&req.symbol, req.side, residual, req.reduce_only)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                log::warn!("[XVENUE/lightmaker] place_taker err={:?}", e);
                return Err(ExecutionFailure::TakerRejected);
            }
        };
        log::info!(
            "[XVENUE/lightmaker] taker placed side={:?} qty={} reduce_only={} order_id={}",
            req.side, residual, req.reduce_only, placed.order_id
        );
        let outcome = poll_until_terminal_or_deadline(
            self.ops,
            &req.symbol,
            &placed.order_id,
            self.cfg.chase_timeout_ms,
            self.poll_interval_ms,
            "XVENUE/lightmaker",
        )
        .await;
        log::info!(
            "[XVENUE/lightmaker] taker done filled={} cancelled={} order_id={}",
            outcome.filled_this_round, outcome.terminal_cancelled, placed.order_id
        );

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
impl<'a, V: VenueOps + ?Sized + Sync> Executor for LighterMakerLoop<'a, V> {
    type Request = LighterMakerRequest;
    type Terminal = LighterTerminal;

    async fn run(&self, req: Self::Request) -> Self::Terminal {
        LighterMakerLoop::run(self, req).await
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
    use crate::trade::execution::types::{CommonExecutorConfig, LighterMakerConfig};
    use crate::trade::execution::venue_ops::{
        OrderFillStatus, ScriptedResponse, ScriptedVenueOps, TopOfBook,
    };
    use rust_decimal_macros::dec;

    fn cfg_with_taker_fallback() -> LighterMakerConfig {
        LighterMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 25 },
            chase_ticks: 1,
            chase_retries: 3,
            chase_timeout_ms: 250,
            taker_fallback: true,
            post_only: true,
        }
    }

    fn cfg_no_fallback() -> LighterMakerConfig {
        LighterMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 25 },
            chase_ticks: 1,
            chase_retries: 2,
            chase_timeout_ms: 250,
            taker_fallback: false,
            post_only: true,
        }
    }

    fn req_long(qty: Decimal) -> LighterMakerRequest {
        LighterMakerRequest {
            symbol: "ETH".to_string(),
            side: OrderSide::Long,
            target_qty: qty,
            dust_qty: dec!(0.0001),
            reduce_only: false,
        }
    }

    fn primed_book(bid: Decimal, ask: Decimal) -> ScriptedVenueOps {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: bid,
                best_ask: ask,
            };
        });
        ops
    }

    /// Chase round one fills cleanly — no taker round needed.
    #[tokio::test(start_paused = true)]
    async fn post_only_fills_in_one_round() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            s.poll_fill.push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                filled_qty: dec!(0.5),
                terminal: true,
                cancelled: false,
            }));
        });
        let cfg = cfg_with_taker_fallback();
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.5))).await;
        assert_eq!(res, LighterTerminal::Filled { qty: dec!(0.5) });
        let posts = ops.snapshot_posts();
        assert_eq!(posts.len(), 1, "exactly one post_only place");
        assert!(ops.snapshot_takers().is_empty(), "no taker fallback");
    }

    /// Partial post-only fill, taker fallback fills the residual.
    #[tokio::test(start_paused = true)]
    async fn post_only_partial_then_taker_fills_residual() {
        let ops = primed_book(dec!(2000), dec!(2001));
        // Chase round consumes a terminal partial; loop exits because
        // chase_retries=1; residual > dust → taker round consumes the
        // second push.
        ops.with_state(|s| {
            s.poll_fill.push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                filled_qty: dec!(0.4),
                terminal: true,
                cancelled: false,
            }));
            s.poll_fill.push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                filled_qty: dec!(0.1),
                terminal: true,
                cancelled: false,
            }));
        });
        let cfg = LighterMakerConfig {
            chase_retries: 1,
            chase_timeout_ms: 50,
            ..cfg_with_taker_fallback()
        };
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.5))).await;
        assert_eq!(res, LighterTerminal::Filled { qty: dec!(0.5) });
        assert_eq!(ops.snapshot_posts().len(), 1, "one post_only round");
        assert_eq!(ops.snapshot_takers().len(), 1, "taker fallback fired");
    }

    /// Post-only chase exhausted, taker_fallback=false → Failed{PostOnlyExhausted}.
    #[tokio::test(start_paused = true)]
    async fn post_only_exhausts_no_fallback_returns_failed() {
        let ops = primed_book(dec!(2000), dec!(2001));
        let cfg = cfg_no_fallback();
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.5))).await;
        assert!(matches!(res, LighterTerminal::Failed { .. }));
        assert!(ops.snapshot_takers().is_empty());
    }

    /// post_only=false short-circuits to taker without ever placing a
    /// post-only order.
    #[tokio::test(start_paused = true)]
    async fn post_only_disabled_goes_straight_to_taker() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            s.poll_fill.push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                filled_qty: dec!(0.5),
                terminal: true,
                cancelled: false,
            }));
        });
        let cfg = LighterMakerConfig {
            post_only: false,
            ..cfg_with_taker_fallback()
        };
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.5))).await;
        assert_eq!(res, LighterTerminal::Filled { qty: dec!(0.5) });
        assert!(ops.snapshot_posts().is_empty(), "no post_only placed");
        assert_eq!(ops.snapshot_takers().len(), 1);
    }

    /// target_qty = 0 → VenueRejected before any place call (defensive).
    #[tokio::test(start_paused = true)]
    async fn zero_qty_rejects_without_placing() {
        let ops = primed_book(dec!(2000), dec!(2001));
        let cfg = cfg_with_taker_fallback();
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(Decimal::ZERO)).await;
        assert_eq!(
            res,
            LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected
            }
        );
        assert!(ops.snapshot_posts().is_empty());
        assert!(ops.snapshot_takers().is_empty());
    }

    /// Buy post-only must rest at best_bid; sell at best_ask.
    #[tokio::test(start_paused = true)]
    async fn post_only_price_picks_correct_side() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            s.poll_fill.push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                filled_qty: dec!(0.5),
                terminal: true,
                cancelled: false,
            }));
        });
        let cfg = cfg_with_taker_fallback();
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let _ = lp.run(req_long(dec!(0.5))).await;
        let posts = ops.snapshot_posts();
        let (_, _, _, price, _) = &posts[0];
        // Long → buy post-only at best_bid (2000)
        assert_eq!(*price, dec!(2000));
    }
}
