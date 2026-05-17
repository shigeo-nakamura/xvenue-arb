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

use async_trait::async_trait;
use dex_connector::OrderSide;
use rust_decimal::Decimal;

use super::maker_loop::{run_maker_loop, MakerLoopParams, MakerRequest};
use super::poll_loop::Executor;
use super::types::{ExecutionFailure, LighterMakerConfig, LighterTerminal};
use super::venue_ops::VenueOps;

/// Inputs to one Lighter post-only entry / exit cycle.
#[derive(Debug, Clone)]
pub struct LighterMakerRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub target_qty: Decimal,
    /// Below this residual, the loop treats the cycle as fully filled
    /// rather than chasing further or invoking taker fallback.
    pub dust_qty: Decimal,
    /// bot-strategy#331 (Lighter mirror of #299): Lighter-side venue
    /// minimum order size. The "remaining ≤ floor" gate (chase entry +
    /// taker fallback) uses `dust_qty.max(venue_min_qty)`, so a
    /// residual below this is treated as fully filled instead of
    /// being passed to `place_post_only` only to be rejected by
    /// Lighter with `code:21706 invalid order base or quote amount`.
    /// 0 disables the guard (dust-only behavior, back-compat for
    /// tests / non-Lighter venues).
    pub venue_min_qty: Decimal,
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
    ///
    /// Behaviour is unchanged from the pre-#388 inline form: the
    /// shared `super::maker_loop::run_maker_loop` drives the chase
    /// and taker fallback with Lighter-specific knobs.
    /// `chase_uses_venue_min_floor = true` (per #331),
    /// `taker_grace_before_cancel = false` (Lighter cancels before
    /// re-polling), and `chase_grace_poll_ms` is wired from cfg for
    /// the #322 fix.
    pub async fn run(&self, req: LighterMakerRequest) -> LighterTerminal {
        if req.target_qty <= Decimal::ZERO {
            return LighterTerminal::Failed {
                reason: ExecutionFailure::VenueRejected,
            };
        }
        let params = MakerLoopParams {
            log_prefix: "XVENUE/lightmaker",
            chase_retries: self.cfg.chase_retries,
            chase_timeout_ms: self.cfg.chase_timeout_ms,
            chase_grace_poll_ms: self.cfg.chase_grace_poll_ms,
            taker_grace_poll_ms: self.cfg.taker_grace_poll_ms,
            taker_fallback: self.cfg.taker_fallback,
            post_only: self.cfg.post_only,
            poll_interval_ms: self.poll_interval_ms,
            chase_uses_venue_min_floor: true,
            taker_grace_before_cancel: false,
            // bot-strategy#424: opt-in via YAML `lighter_exit_improve_tick`.
            // 0 (default) → None → legacy join-touch behaviour. >0 → Some(t)
            // → reduce_only requests improve the touch by `t` units.
            exit_improve_tick: if self.cfg.exit_improve_tick > Decimal::ZERO {
                Some(self.cfg.exit_improve_tick)
            } else {
                None
            },
        };
        let shared_req = MakerRequest {
            symbol: req.symbol,
            side: req.side,
            target_qty: req.target_qty,
            dust_qty: req.dust_qty,
            venue_min_qty: req.venue_min_qty,
            reduce_only: req.reduce_only,
        };
        let outcome = run_maker_loop(self.ops, &params, &shared_req).await;
        if outcome.total_filled > Decimal::ZERO {
            LighterTerminal::Filled {
                qty: outcome.total_filled,
            }
        } else {
            LighterTerminal::Failed {
                reason: outcome
                    .last_failure
                    .unwrap_or(ExecutionFailure::PostOnlyExhausted),
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::types::{CommonExecutorConfig, LighterMakerConfig};
    use crate::trade::execution::venue_ops::{
        OrderFillStatus, ScriptedResponse, ScriptedVenueOps, ScriptedVenueOpsState, TopOfBook,
    };
    use rust_decimal_macros::dec;

    fn cfg_with_taker_fallback() -> LighterMakerConfig {
        LighterMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            chase_ticks: 1,
            chase_retries: 3,
            chase_timeout_ms: 250,
            taker_fallback: true,
            post_only: true,
            chase_grace_poll_ms: 0,
            taker_grace_poll_ms: 0,
            exit_improve_tick: Decimal::ZERO,
        }
    }

    fn cfg_no_fallback() -> LighterMakerConfig {
        LighterMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            chase_ticks: 1,
            chase_retries: 2,
            chase_timeout_ms: 250,
            taker_fallback: false,
            post_only: true,
            chase_grace_poll_ms: 0,
            taker_grace_poll_ms: 0,
            exit_improve_tick: Decimal::ZERO,
        }
    }

    fn req_long(qty: Decimal) -> LighterMakerRequest {
        LighterMakerRequest {
            symbol: "ETH".to_string(),
            side: OrderSide::Long,
            target_qty: qty,
            dust_qty: dec!(0.0001),
            venue_min_qty: Decimal::ZERO,
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
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
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
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.4),
                    terminal: true,
                    cancelled: false,
                }));
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
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
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
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
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
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

    /// bot-strategy#424 Option B: when `exit_improve_tick > 0` and the
    /// request is `reduce_only=true`, the post_only price improves the
    /// touch by `tick` instead of joining it.
    #[tokio::test(start_paused = true)]
    async fn exit_post_only_improves_touch_by_tick() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.5),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let mut cfg = cfg_with_taker_fallback();
        cfg.exit_improve_tick = dec!(0.01);
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        // reduce_only=true (exit-side) + Short → SELL improved 1 tick
        // BELOW best_ask: 2001.00 - 0.01 = 2000.99.
        let req = LighterMakerRequest {
            symbol: "ETH".to_string(),
            side: OrderSide::Short,
            target_qty: dec!(0.5),
            dust_qty: Decimal::ZERO,
            venue_min_qty: Decimal::ZERO,
            reduce_only: true,
        };
        let _ = lp.run(req).await;
        let posts = ops.snapshot_posts();
        let (_, _, _, price, _) = &posts[0];
        assert_eq!(*price, dec!(2000.99));
    }

    /// Symmetric coverage: exit-side Long (e.g. closing a short Lighter
    /// position) improves the bid 1 tick UP.
    #[tokio::test(start_paused = true)]
    async fn exit_post_only_improves_bid_for_long() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.5),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let mut cfg = cfg_with_taker_fallback();
        cfg.exit_improve_tick = dec!(0.01);
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let req = LighterMakerRequest {
            symbol: "ETH".to_string(),
            side: OrderSide::Long,
            target_qty: dec!(0.5),
            dust_qty: Decimal::ZERO,
            venue_min_qty: Decimal::ZERO,
            reduce_only: true,
        };
        let _ = lp.run(req).await;
        let posts = ops.snapshot_posts();
        let (_, _, _, price, _) = &posts[0];
        // Long exit → BUY improved 1 tick ABOVE best_bid: 2000 + 0.01 = 2000.01
        assert_eq!(*price, dec!(2000.01));
    }

    /// Back-compat: entry-side post_only ignores `exit_improve_tick`
    /// even when set (only `reduce_only=true` triggers the improve).
    #[tokio::test(start_paused = true)]
    async fn entry_post_only_ignores_improve_tick() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.5),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let mut cfg = cfg_with_taker_fallback();
        cfg.exit_improve_tick = dec!(0.01);
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        // reduce_only=false (entry) → join touch regardless of tick.
        let _ = lp.run(req_long(dec!(0.5))).await;
        let posts = ops.snapshot_posts();
        let (_, _, _, price, _) = &posts[0];
        assert_eq!(*price, dec!(2000));
    }

    /// Back-compat: `exit_improve_tick=0` keeps legacy join-touch even
    /// for reduce_only=true requests. This is the default for bots
    /// that haven't opted in via YAML.
    #[tokio::test(start_paused = true)]
    async fn exit_with_zero_tick_falls_back_to_join_touch() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.5),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = cfg_with_taker_fallback(); // exit_improve_tick stays 0
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let req = LighterMakerRequest {
            symbol: "ETH".to_string(),
            side: OrderSide::Short,
            target_qty: dec!(0.5),
            dust_qty: Decimal::ZERO,
            venue_min_qty: Decimal::ZERO,
            reduce_only: true,
        };
        let _ = lp.run(req).await;
        let posts = ops.snapshot_posts();
        let (_, _, _, price, _) = &posts[0];
        // Falls back to join-touch (best_ask = 2001).
        assert_eq!(*price, dec!(2001));
    }

    /// Push N non-terminal poll responses. The grace-poll tests need
    /// the queue to feed exactly the round's poll-loop iterations so
    /// the late-fill (pushed last) sits at the position the grace
    /// re-poll pops.
    fn push_non_terminal_polls(s: &mut ScriptedVenueOpsState, n: usize) {
        for _ in 0..n {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: Decimal::ZERO,
                    terminal: false,
                    cancelled: false,
                }));
        }
    }

    /// bot-strategy#322: chase round timed out at filled=0 cancelled=false,
    /// then a late WS fill surfaces during the grace window. Loop must
    /// pick up the late fill instead of placing another order on top.
    /// Without this fix, the chase round's `cancelled=false filled=0`
    /// outcome fell through to either the next chase round (stacking)
    /// or to taker — both observed live as orders that stacked on
    /// Lighter and then needed emergency-flatten unwinding.
    ///
    /// Test parameters:
    /// - chase_timeout_ms=20, poll_interval=10 → 3 polls per chase round
    /// - chase_retries=1, taker_fallback=false → grace is the ONLY path
    ///   to a successful fill
    /// - Queue layout: 3 non-terminals (consumed by chase polls) +
    ///   1 late-fill (consumed by chase grace re-poll)
    #[tokio::test(start_paused = true)]
    async fn chase_grace_poll_recovers_late_fill() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            push_non_terminal_polls(s, 3);
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.5),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = LighterMakerConfig {
            chase_retries: 1,
            chase_timeout_ms: 20,
            chase_grace_poll_ms: 200,
            taker_fallback: false,
            ..cfg_with_taker_fallback()
        };
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.5))).await;
        assert_eq!(
            res,
            LighterTerminal::Filled { qty: dec!(0.5) },
            "grace re-poll must surface the late fill (taker_fallback=false \
             so any other path returns Failed)"
        );
        // Critical invariant: only ONE post_only place call. Without
        // grace, the loop would have placed a fresh order on top of the
        // late-filling order — exactly the live bug from #322.
        assert_eq!(ops.snapshot_posts().len(), 1, "no order stacking");
        assert!(ops.snapshot_takers().is_empty(), "taker_fallback=false");
    }

    /// bot-strategy#322: taker round timed out filled=0 cancelled=false,
    /// late fill during grace must be counted. Mirrors Extended #298.
    /// Queue: 3 (chase polls) + 3 (taker polls) + 1 (taker grace re-poll).
    #[tokio::test(start_paused = true)]
    async fn taker_grace_poll_recovers_late_fill() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            push_non_terminal_polls(s, 6);
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.5),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = LighterMakerConfig {
            chase_retries: 1,
            chase_timeout_ms: 20,
            chase_grace_poll_ms: 0, // skip chase grace so chase exhausts cleanly
            taker_grace_poll_ms: 200,
            ..cfg_with_taker_fallback()
        };
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.5))).await;
        assert_eq!(
            res,
            LighterTerminal::Filled { qty: dec!(0.5) },
            "taker grace re-poll must surface the late fill"
        );
        assert_eq!(ops.snapshot_takers().len(), 1, "exactly one taker round");
    }

    /// Grace poll runs but no late fill ever arrives — must keep
    /// behaving as the no-grace path (Failed/Timeout). Defends against
    /// a bug where the grace branch inadvertently masks a true exhaust.
    #[tokio::test(start_paused = true)]
    async fn chase_grace_poll_no_late_fill_still_exhausts() {
        let ops = primed_book(dec!(2000), dec!(2001));
        ops.with_state(|s| {
            // All polls (round + grace) get non-terminal — fill never
            // lands. With queue empty after 3 polls + 1 grace, the
            // mock falls back to default_fill which is also non-
            // terminal, so any extra polls behave the same.
            push_non_terminal_polls(s, 4);
        });
        let cfg = LighterMakerConfig {
            chase_retries: 1,
            chase_timeout_ms: 20,
            chase_grace_poll_ms: 100,
            taker_fallback: false,
            ..cfg_no_fallback()
        };
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let res = lp.run(req_long(dec!(0.5))).await;
        assert!(matches!(res, LighterTerminal::Failed { .. }));
        assert!(ops.snapshot_takers().is_empty());
    }

    /// bot-strategy#331: chase round 0 partial-fills the bulk of
    /// `target_qty`, leaving a sub-min residual. With
    /// `venue_min_qty=0` (default / pre-fix behavior) the loop went
    /// on to round 1 and the connector returned 21706 because Lighter
    /// rejects post_only on `base_amount=1`. With `venue_min_qty`
    /// raised to a value above the residual, the loop must:
    ///   - exit the chase after round 0 (no second post_only)
    ///   - skip the taker fallback (residual ≤ floor)
    ///   - return Filled{ qty=round0_filled }
    ///
    /// Test parameters:
    /// - target_qty=0.02099165, round 0 fills 0.0209 → residual 0.00009165
    /// - venue_min_qty=0.001, dust_qty=0.00001 → floor=0.001 > residual
    /// - chase_retries=4 (production setting per #328) — without the
    ///   guard, all 4 rounds would each post the residual; the test
    ///   asserts only ONE post_only call so a regression that drops
    ///   the floor surfaces as a count mismatch
    #[tokio::test(start_paused = true)]
    async fn chase_skips_round_when_residual_below_venue_min() {
        let ops = primed_book(dec!(2370), dec!(2371));
        ops.with_state(|s| {
            // Round 0: terminal partial fill — 0.0209 of target
            // 0.02099165 (mirrors live ETH chase round behavior with
            // size_decimals=4 lot truncation).
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.0209),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let cfg = LighterMakerConfig {
            chase_retries: 4,
            chase_timeout_ms: 50,
            chase_grace_poll_ms: 0,
            ..cfg_with_taker_fallback()
        };
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let req = LighterMakerRequest {
            symbol: "ETH".to_string(),
            side: OrderSide::Short,
            target_qty: dec!(0.02099165),
            dust_qty: dec!(0.00001),
            venue_min_qty: dec!(0.001),
            reduce_only: false,
        };
        let res = lp.run(req).await;
        assert_eq!(
            res,
            LighterTerminal::Filled { qty: dec!(0.0209) },
            "sub-min residual must be treated as fully filled"
        );
        assert_eq!(
            ops.snapshot_posts().len(),
            1,
            "exactly one post_only round — sub-min residual must NOT \
             trigger a second place_post_only that Lighter would reject \
             with code:21706 (#329 / #331)"
        );
        assert!(
            ops.snapshot_takers().is_empty(),
            "sub-min residual must NOT trigger taker fallback either — \
             the taker would also be rejected on a sub-lot size"
        );
    }

    /// bot-strategy#331: back-compat — `venue_min_qty=0` keeps the
    /// pre-fix dust-only gating behavior. Same setup as the previous
    /// test except `venue_min_qty=0`; the loop now sees
    /// `remaining=0.00009165 > dust_qty=0.00001` and continues to the
    /// next round / taker fallback. We only assert the count and the
    /// fact that the post_only chase was NOT short-circuited; the
    /// connector-level 21706 isn't modeled here so the second round
    /// just sees a fresh non-terminal poll and either fills, exhausts,
    /// or falls through to taker — all branches are covered by the
    /// existing tests above.
    #[tokio::test(start_paused = true)]
    async fn chase_does_not_skip_when_venue_min_qty_zero() {
        let ops = primed_book(dec!(2370), dec!(2371));
        ops.with_state(|s| {
            // Round 0: same partial fill as the guarded test.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: dec!(0.0209),
                    terminal: true,
                    cancelled: false,
                }));
            // Round 1: terminal cancelled → loop exits chase.
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_qty: Decimal::ZERO,
                    terminal: true,
                    cancelled: true,
                }));
        });
        let cfg = LighterMakerConfig {
            chase_retries: 4,
            chase_timeout_ms: 50,
            chase_grace_poll_ms: 0,
            taker_fallback: false,
            ..cfg_with_taker_fallback()
        };
        let lp = LighterMakerLoop::new(&ops, &cfg).with_poll_interval(10);
        let req = LighterMakerRequest {
            symbol: "ETH".to_string(),
            side: OrderSide::Short,
            target_qty: dec!(0.02099165),
            dust_qty: dec!(0.00001),
            venue_min_qty: Decimal::ZERO, // disabled — pre-fix behavior
            reduce_only: false,
        };
        let _ = lp.run(req).await;
        assert!(
            ops.snapshot_posts().len() >= 2,
            "with venue_min_qty=0 the chase must NOT short-circuit on \
             a sub-min residual (back-compat)"
        );
    }
}
