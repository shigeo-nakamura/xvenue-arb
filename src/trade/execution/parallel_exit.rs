//! Parallel exit coordinator (bot-strategy#244 Group B / case 11).
//!
//! Owns the simultaneous reduce-only exit of both legs on
//! `Held → Exiting`. Per DESIGN.md §4.2 exit is parallel (vs entry
//! which is serial). Drives one
//! [`ExtendedMakerLoop::run_entry`](super::extended_maker::ExtendedMakerLoop)
//! and one Lighter executor concurrently and aggregates them into a
//! single [`ParallelExitOutcome`]. The Lighter executor is selected at
//! `run` time based on `LighterMakerConfig::post_only` (#330):
//!
//! - `lighter_post_only=true`  →
//!   [`LighterMakerLoop`](super::lighter_maker::LighterMakerLoop)
//!   (post-only chase + taker fallback, mirrors entry path).
//! - `lighter_post_only=false` →
//!   [`LighterFillLoop`](super::lighter_fill::LighterFillLoop)
//!   (legacy market / aggressive-limit taker).
//!
//! Both loops terminate to a single [`LighterTerminal`] so the
//! aggregation surface is identical regardless of which path runs.
//!
//! Case 11 contract (per `docs/execution_layer.md` §2): if **one leg
//! terminates and the other doesn't within `leg_mismatch_timeout_ms`**,
//! the coordinator returns [`ParallelExitOutcome::LegMismatchTimeout`].
//! The runner translates that to `Event::Emergency { reason:
//! LegMismatchTimeout }` so the state machine re-enters
//! `EmergencyFlattening` carrying whichever leg is still open. The
//! mismatch deadline only starts counting **after the first leg
//! terminates** — both-legs-still-chasing simply waits for the
//! per-executor timeouts (chase × retries on Extended,
//! `fill_timeout_ms` on Lighter taker / chase × retries on Lighter
//! maker) to expire on their own.

use std::time::Duration;

use rust_decimal::Decimal;
use tokio::pin;
use tokio::time::timeout;

use super::extended_maker::{ExtendedEntryRequest, ExtendedMakerLoop};
use super::lighter_fill::{LighterFillLoop, LighterFillRequest};
use super::lighter_maker::{LighterMakerLoop, LighterMakerRequest};
use super::types::{
    ExtendedMakerConfig, ExtendedTerminal, LighterFillConfig, LighterMakerConfig, LighterTerminal,
};
use super::venue_ops::VenueOps;

/// Knobs for the parallel exit. Sourced from `XvenueConfig.risk` so
/// the same YAML threshold drives the runner deadline and the
/// matching `Event::Emergency` reason.
#[derive(Debug, Clone)]
pub struct ParallelExitConfig {
    /// Outer deadline (from when the **first** leg terminates) for
    /// the second leg to also terminate. `risk.leg_mismatch_timeout_ms`
    /// in YAML, default 3000. Below 1000 ms is pathological — Lighter's
    /// own `fill_timeout_ms` is 1000 and Extended's chase is 500 × N
    /// retries; tighter than that and the mismatch deadline starts
    /// firing on healthy fills. Validation lives at the YAML-load
    /// layer; the loop itself just trusts the value.
    pub leg_mismatch_timeout_ms: u64,
}

impl ParallelExitConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.leg_mismatch_timeout_ms == 0 {
            return Err("leg_mismatch_timeout_ms must be > 0".into());
        }
        Ok(())
    }
}

/// Outcome of one parallel exit cycle. Maps 1:1 to state-machine
/// events the runner emits (per `state.rs`):
///
/// - [`ParallelExitOutcome::Both`] → emit
///   `ExtendedExitFilled{qty}` + `LighterExitFilled{qty}` (or the
///   `*Failed` equivalents for failure terminals — the state machine
///   handles those uniformly via existing emergency routing on
///   single-leg failure during Exiting).
/// - [`ParallelExitOutcome::LegMismatchTimeout`] → emit
///   `Event::Emergency { reason: LegMismatchTimeout }`. Whichever
///   leg's terminal is `Some` should also be applied first as
///   `*ExitFilled` so the position machine has the current open qty
///   when it transitions to `EmergencyFlattening`.
#[derive(Debug, Clone, PartialEq)]
pub enum ParallelExitOutcome {
    Both {
        ext: ExtendedTerminal,
        lt: LighterTerminal,
    },
    /// One leg is still pending after the deadline. `Some` leg has
    /// already terminated (apply its fill before entering
    /// `EmergencyFlattening`); `None` leg is still in flight on the
    /// venue.
    LegMismatchTimeout {
        ext: Option<ExtendedTerminal>,
        lt: Option<LighterTerminal>,
    },
}

/// Coordinator. Holds the per-venue executor configs + `VenueOps`
/// references; `run` does the actual concurrent dispatch. `V` is
/// generic so the production wires a single `LiveVenueOps` and tests
/// can wire two `ScriptedVenueOps` (one per venue) for independent
/// scripting.
///
/// The Lighter leg can route through either the legacy
/// [`LighterFillLoop`] (taker) or the [`LighterMakerLoop`] (post-only
/// chase + taker fallback) — the runtime selector is
/// `lt_maker_cfg.post_only`. Both configs are always carried so the
/// gate flips without re-plumbing the loop. `lt_min_qty` is the Lighter
/// venue minimum order size and is only consulted on the maker path
/// (mirrors `LighterMakerRequest::venue_min_qty`); the legacy fill path
/// ignores it.
pub struct ParallelExitLoop<'a, EV, LV>
where
    EV: VenueOps + ?Sized,
    LV: VenueOps + ?Sized,
{
    pub ext_ops: &'a EV,
    pub lt_ops: &'a LV,
    pub ext_cfg: &'a ExtendedMakerConfig,
    pub lt_fill_cfg: &'a LighterFillConfig,
    pub lt_maker_cfg: &'a LighterMakerConfig,
    pub cfg: &'a ParallelExitConfig,
    /// Lighter venue minimum order size, propagated to
    /// `LighterMakerRequest::venue_min_qty` when the maker path runs.
    /// `Decimal::ZERO` = dust-only behavior (back-compat for tests).
    lt_min_qty: Decimal,
    /// Test hook — pinned poll cadence for both inner executors so a
    /// paused tokio clock can step deterministically. None = use the
    /// per-executor defaults.
    poll_interval_ms: Option<u64>,
}

impl<'a, EV, LV> ParallelExitLoop<'a, EV, LV>
where
    EV: VenueOps + ?Sized,
    LV: VenueOps + ?Sized,
{
    pub fn new(
        ext_ops: &'a EV,
        lt_ops: &'a LV,
        ext_cfg: &'a ExtendedMakerConfig,
        lt_fill_cfg: &'a LighterFillConfig,
        lt_maker_cfg: &'a LighterMakerConfig,
        cfg: &'a ParallelExitConfig,
    ) -> Self {
        Self {
            ext_ops,
            lt_ops,
            ext_cfg,
            lt_fill_cfg,
            lt_maker_cfg,
            cfg,
            lt_min_qty: Decimal::ZERO,
            poll_interval_ms: None,
        }
    }

    pub fn with_poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = Some(ms.max(1));
        self
    }

    /// Set the Lighter venue minimum order size used by the maker path
    /// (`LighterMakerRequest::venue_min_qty`). Only consulted when
    /// `lt_maker_cfg.post_only=true`.
    pub fn with_lt_min_qty(mut self, qty: Decimal) -> Self {
        self.lt_min_qty = qty;
        self
    }

    /// Run one parallel exit. Both legs are dispatched concurrently;
    /// the first to terminate starts a `leg_mismatch_timeout_ms`
    /// deadline on the second. The Lighter leg dispatches through
    /// `LighterMakerLoop` when `lt_maker_cfg.post_only=true` (#330,
    /// mirrors entry path) and through `LighterFillLoop` otherwise.
    /// `lt_req.target_qty` / `side` / `dust_qty` / `reduce_only` are
    /// preserved across both paths; `venue_min_qty` is sourced from
    /// `self.lt_min_qty` (set via `with_lt_min_qty`) and only consulted
    /// on the maker path.
    pub async fn run(
        &self,
        ext_req: ExtendedEntryRequest,
        lt_req: LighterFillRequest,
    ) -> ParallelExitOutcome {
        // Build the Extended loop with pinned poll cadence (test path)
        // or its default (production path).
        let ext_loop = {
            let mut l = ExtendedMakerLoop::new(self.ext_ops, self.ext_cfg);
            if let Some(ms) = self.poll_interval_ms {
                l = l.with_poll_interval(ms);
            }
            l
        };
        let ext_fut = ext_loop.run_entry(ext_req);
        pin!(ext_fut);

        // Construct the Lighter future with the loop selected by
        // `lt_maker_cfg.post_only`. Both arms return `LighterTerminal`,
        // so we erase to a boxed pinned future for `tokio::select!`.
        // The future borrows `self`'s references, hence the `'a` bound.
        let lt_fut: std::pin::Pin<
            Box<dyn std::future::Future<Output = LighterTerminal> + Send + 'a>,
        > = if self.lt_maker_cfg.post_only {
            let mut l = LighterMakerLoop::new(self.lt_ops, self.lt_maker_cfg);
            if let Some(ms) = self.poll_interval_ms {
                l = l.with_poll_interval(ms);
            }
            let maker_req = LighterMakerRequest {
                symbol: lt_req.symbol,
                side: lt_req.side,
                target_qty: lt_req.target_qty,
                dust_qty: lt_req.dust_qty,
                venue_min_qty: self.lt_min_qty,
                reduce_only: lt_req.reduce_only,
            };
            Box::pin(async move { l.run(maker_req).await })
        } else {
            let mut l = LighterFillLoop::new(self.lt_ops, self.lt_fill_cfg);
            if let Some(ms) = self.poll_interval_ms {
                l = l.with_poll_interval(ms);
            }
            Box::pin(async move { l.run(lt_req).await })
        };
        pin!(lt_fut);

        let mut ext_term: Option<ExtendedTerminal> = None;
        let mut lt_term: Option<LighterTerminal> = None;

        // Phase 1: wait for the first terminal. `select!` consumes
        // the branch that completes; the other future stays pinned
        // for phase 2. `biased` keeps the test-side ordering stable
        // when both arms are technically ready in the same tick.
        tokio::select! {
            biased;
            e = &mut ext_fut => {
                ext_term = Some(e);
            }
            l = &mut lt_fut => {
                lt_term = Some(l);
            }
        }

        // Phase 2: race the second leg against the mismatch deadline.
        let deadline = Duration::from_millis(self.cfg.leg_mismatch_timeout_ms);
        match (ext_term.is_some(), lt_term.is_some()) {
            (true, false) => match timeout(deadline, &mut lt_fut).await {
                Ok(l) => lt_term = Some(l),
                Err(_) => {
                    // Lighter still in flight — leg mismatch.
                    return ParallelExitOutcome::LegMismatchTimeout {
                        ext: ext_term,
                        lt: None,
                    };
                }
            },
            (false, true) => match timeout(deadline, &mut ext_fut).await {
                Ok(e) => ext_term = Some(e),
                Err(_) => {
                    return ParallelExitOutcome::LegMismatchTimeout {
                        ext: None,
                        lt: lt_term,
                    };
                }
            },
            // Phase 1 returns when at least one branch fires, so
            // exactly one Some is the invariant. Defensive arm; not
            // expected in normal flow.
            _ => {}
        }

        ParallelExitOutcome::Both {
            ext: ext_term.expect("phase 2 fills the missing terminal or returns early"),
            lt: lt_term.expect("phase 2 fills the missing terminal or returns early"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::types::{
        CommonExecutorConfig, ExecutionFailure, ExtendedMakerConfig, LighterFillConfig,
        LighterMakerConfig, LighterOrderType,
    };
    use crate::trade::execution::venue_ops::{
        OrderFillStatus, ScriptedResponse, ScriptedVenueOps, TopOfBook,
    };
    use dex_connector::OrderSide;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn ext_cfg() -> ExtendedMakerConfig {
        ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 100,
            taker_fallback: true,
            post_only: true,
            taker_grace_poll_ms: 0,
        }
    }

    fn lt_cfg() -> LighterFillConfig {
        LighterFillConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type: LighterOrderType::Market,
            fill_timeout_ms: 100,
        }
    }

    /// Default maker cfg with `post_only=false` so existing tests
    /// continue to exercise the legacy `LighterFillLoop` path. Tests
    /// that want the maker path opt in by flipping `post_only=true`.
    fn lt_maker_cfg_off() -> LighterMakerConfig {
        LighterMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 100,
            taker_fallback: true,
            post_only: false,
            chase_grace_poll_ms: 0,
            taker_grace_poll_ms: 0,
            exit_improve_tick: Decimal::ZERO,
        }
    }

    fn parallel_cfg(leg_mismatch_ms: u64) -> ParallelExitConfig {
        ParallelExitConfig {
            leg_mismatch_timeout_ms: leg_mismatch_ms,
        }
    }

    fn ext_req(qty: Decimal) -> ExtendedEntryRequest {
        ExtendedEntryRequest {
            symbol: "BTC-USD".into(),
            side: OrderSide::Short,
            target_qty: qty,
            dust_qty: dec!(0.0001),
            venue_min_qty: Decimal::ZERO,
            // Reduce-only on exit per DESIGN.md §4.2.
            reduce_only: true,
        }
    }

    fn lt_req(qty: Decimal) -> LighterFillRequest {
        LighterFillRequest {
            symbol: "BTC".into(),
            side: OrderSide::Long,
            target_qty: qty,
            dust_qty: dec!(0.0001),
            reduce_only: true,
        }
    }

    /// Happy-path baseline: both legs fill cleanly within the inner
    /// timeouts. Mismatch deadline is never armed.
    #[tokio::test(start_paused = true)]
    async fn both_legs_fill_returns_both() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        ext_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        lt_ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let ec = ext_cfg();
        let lc = lt_cfg();
        let lmc = lt_maker_cfg_off();
        let pc = parallel_cfg(3000);
        let lp =
            ParallelExitLoop::new(&ext_ops, &lt_ops, &ec, &lc, &lmc, &pc).with_poll_interval(10);
        let outcome = lp.run(ext_req(dec!(0.01)), lt_req(dec!(0.01))).await;
        match outcome {
            ParallelExitOutcome::Both { ext, lt } => {
                assert_eq!(
                    ext,
                    ExtendedTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    }
                );
                assert_eq!(
                    lt,
                    LighterTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    }
                );
            }
            other => panic!("expected Both, got {:?}", other),
        }
    }

    /// **Catalogue case 11**: Lighter fills, Extended is still
    /// chasing past the mismatch deadline → `LegMismatchTimeout` with
    /// `ext: None` and the Lighter terminal preserved.
    #[tokio::test(start_paused = true)]
    async fn case11_lighter_fills_extended_pending_returns_leg_mismatch() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        ext_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            // Default fill = zero non-terminal; Extended will keep
            // polling until its own chase loop expires. With the
            // configured `chase_retries=1`, `chase_timeout_ms=2000`,
            // taker_fallback=false, total Extended runtime ~ 2000 ms.
            // Mismatch deadline at 200 ms (well before 2000) → timer
            // fires while Extended is still chasing.
        });
        lt_ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let ec = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 2000,
            taker_fallback: false,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lc = LighterFillConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type: LighterOrderType::Market,
            fill_timeout_ms: 100,
        };
        let lmc = lt_maker_cfg_off();
        let pc = parallel_cfg(200);
        let lp =
            ParallelExitLoop::new(&ext_ops, &lt_ops, &ec, &lc, &lmc, &pc).with_poll_interval(20);
        let outcome = lp.run(ext_req(dec!(0.01)), lt_req(dec!(0.01))).await;
        match outcome {
            ParallelExitOutcome::LegMismatchTimeout { ext, lt } => {
                assert!(ext.is_none(), "Extended must still be in flight");
                assert_eq!(
                    lt,
                    Some(LighterTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    })
                );
            }
            other => panic!("expected LegMismatchTimeout, got {:?}", other),
        }
    }

    /// **Catalogue case 11 mirror**: Extended fills first, Lighter
    /// is the laggard — symmetric outcome with `lt: None`.
    #[tokio::test(start_paused = true)]
    async fn case11_extended_fills_lighter_pending_returns_leg_mismatch() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        ext_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        // Lighter never gets a terminal — default zero non-terminal
        // polls until its `fill_timeout_ms` expires (2000 ms here).
        let ec = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 100,
            taker_fallback: false,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lc = LighterFillConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type: LighterOrderType::Market,
            fill_timeout_ms: 2000,
        };
        let lmc = lt_maker_cfg_off();
        let pc = parallel_cfg(200);
        let lp =
            ParallelExitLoop::new(&ext_ops, &lt_ops, &ec, &lc, &lmc, &pc).with_poll_interval(20);
        let outcome = lp.run(ext_req(dec!(0.01)), lt_req(dec!(0.01))).await;
        match outcome {
            ParallelExitOutcome::LegMismatchTimeout { ext, lt } => {
                assert_eq!(
                    ext,
                    Some(ExtendedTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    })
                );
                assert!(lt.is_none(), "Lighter must still be in flight");
            }
            other => panic!("expected LegMismatchTimeout, got {:?}", other),
        }
    }

    /// Both legs slow but both arrive within the mismatch deadline →
    /// `Both`. Asserts the deadline is measured **from first
    /// terminal**, not from `run` start: Lighter takes ~80 ms,
    /// Extended takes ~150 ms, mismatch deadline is 100 ms → with a
    /// "from run start" interpretation Extended would miss; with the
    /// correct "from first terminal" interpretation Extended finishes
    /// 70 ms after Lighter, well inside the 100 ms window.
    #[tokio::test(start_paused = true)]
    async fn deadline_starts_from_first_terminal_not_from_run_start() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        // Extended takes a few polls to fill so its terminal lands
        // after Lighter's first poll terminal.
        ext_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            // 7 non-terminal polls then terminal (~140 ms at 20 ms cadence).
            for _ in 0..7 {
                s.poll_fill
                    .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                        filled_value: None,
                        filled_qty: Decimal::ZERO,
                        terminal: false,
                        cancelled: false,
                    }));
            }
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        lt_ops.with_state(|s| {
            // Lighter terminal on the first poll (~10 ms).
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let ec = ExtendedMakerConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: 1,
            chase_retries: 1,
            chase_timeout_ms: 1000,
            taker_fallback: false,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        let lc = LighterFillConfig {
            common: CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type: LighterOrderType::Market,
            fill_timeout_ms: 1000,
        };
        let lmc = lt_maker_cfg_off();
        // 200 ms mismatch — Extended finishes ~140 ms after run start,
        // ~140 ms after Lighter's first-poll terminal at ~0 ms. So
        // it's right at the edge; well inside the 200 ms window.
        let pc = parallel_cfg(200);
        let lp =
            ParallelExitLoop::new(&ext_ops, &lt_ops, &ec, &lc, &lmc, &pc).with_poll_interval(20);
        let outcome = lp.run(ext_req(dec!(0.01)), lt_req(dec!(0.01))).await;
        match outcome {
            ParallelExitOutcome::Both { ext, lt } => {
                assert_eq!(
                    ext,
                    ExtendedTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    }
                );
                assert_eq!(
                    lt,
                    LighterTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    }
                );
            }
            other => panic!("expected Both, got {:?}", other),
        }
    }

    /// Both legs reduce_only flag propagates to the venue calls.
    /// Belt-and-suspenders against a runner regression that would
    /// silently grow a position via the exit path.
    #[tokio::test(start_paused = true)]
    async fn both_legs_carry_reduce_only_flag_to_venue() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        ext_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        lt_ops.with_state(|s| {
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        // Configure Extended to skip maker entirely so it goes
        // straight to taker — that's the path that records the
        // reduce_only flag at the venue layer.
        let ec = ExtendedMakerConfig {
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
        let lc = lt_cfg();
        let lmc = lt_maker_cfg_off();
        let pc = parallel_cfg(3000);
        let lp =
            ParallelExitLoop::new(&ext_ops, &lt_ops, &ec, &lc, &lmc, &pc).with_poll_interval(10);
        let _ = lp.run(ext_req(dec!(0.01)), lt_req(dec!(0.01))).await;
        let ext_takers = ext_ops.snapshot_takers();
        let lt_takers = lt_ops.snapshot_takers();
        assert_eq!(ext_takers.len(), 1);
        assert!(ext_takers[0].3, "ext reduce_only must propagate");
        assert_eq!(lt_takers.len(), 1);
        assert!(lt_takers[0].3, "lt reduce_only must propagate");
    }

    /// One leg returns `Failed` (e.g. Lighter venue rejected the
    /// taker), Extended fills cleanly. Both terminals are present so
    /// the outcome is `Both`. The runner separately decides whether
    /// to escalate based on the failure terminals — this loop's job
    /// is just aggregation.
    #[tokio::test(start_paused = true)]
    async fn lighter_failed_extended_filled_returns_both_with_failed_terminal() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        ext_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        lt_ops.with_state(|s| {
            s.place_taker
                .push_back(ScriptedResponse::Err("rate limit".into()));
        });
        let ec = ext_cfg();
        let lc = lt_cfg();
        let lmc = lt_maker_cfg_off();
        let pc = parallel_cfg(3000);
        let lp =
            ParallelExitLoop::new(&ext_ops, &lt_ops, &ec, &lc, &lmc, &pc).with_poll_interval(10);
        let outcome = lp.run(ext_req(dec!(0.01)), lt_req(dec!(0.01))).await;
        match outcome {
            ParallelExitOutcome::Both { ext, lt } => {
                assert_eq!(
                    ext,
                    ExtendedTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    }
                );
                assert!(matches!(
                    lt,
                    LighterTerminal::Failed {
                        reason: ExecutionFailure::VenueRejected
                    }
                ));
            }
            other => panic!("expected Both with mixed terminals, got {:?}", other),
        }
    }

    /// **#330**: with `lt_maker_cfg.post_only=true`, the Lighter exit
    /// leg routes through `LighterMakerLoop` — a `place_post_only` call
    /// must show up at the venue layer (and zero `place_taker` calls in
    /// the happy path where the post-only fills cleanly). The
    /// `LighterFillLoop` legacy path would have placed a taker order
    /// instead, so this test pins the gate behavior end-to-end through
    /// `ParallelExitLoop::run`.
    #[tokio::test(start_paused = true)]
    async fn lighter_post_only_routes_exit_through_maker_loop() {
        let ext_ops = ScriptedVenueOps::new();
        let lt_ops = ScriptedVenueOps::new();
        ext_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(78000),
                best_ask: dec!(78001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.01),
                    terminal: true,
                    cancelled: false,
                }));
        });
        // Lighter book is required for the maker chase to compute a
        // resting price; one terminal fill on the first chase round
        // closes the round cleanly with no taker fallback.
        lt_ops.with_state(|s| {
            s.default_book = TopOfBook {
                best_bid: dec!(2000),
                best_ask: dec!(2001),
            };
            s.poll_fill
                .push_back(ScriptedResponse::FillStatus(OrderFillStatus {
                    filled_value: None,
                    filled_qty: dec!(0.5),
                    terminal: true,
                    cancelled: false,
                }));
        });
        let ec = ext_cfg();
        let lc = lt_cfg();
        let lmc = LighterMakerConfig {
            post_only: true,
            ..lt_maker_cfg_off()
        };
        let pc = parallel_cfg(3000);
        let lp =
            ParallelExitLoop::new(&ext_ops, &lt_ops, &ec, &lc, &lmc, &pc).with_poll_interval(10);
        let lt_request = LighterFillRequest {
            symbol: "ETH".into(),
            side: OrderSide::Long,
            target_qty: dec!(0.5),
            dust_qty: dec!(0.0001),
            reduce_only: true,
        };
        let outcome = lp.run(ext_req(dec!(0.01)), lt_request).await;
        match outcome {
            ParallelExitOutcome::Both { ext, lt } => {
                assert_eq!(
                    ext,
                    ExtendedTerminal::Filled {
                        qty: dec!(0.01),
                        avg_fill_price: None
                    }
                );
                assert_eq!(
                    lt,
                    LighterTerminal::Filled {
                        qty: dec!(0.5),
                        avg_fill_price: None
                    }
                );
            }
            other => panic!("expected Both, got {:?}", other),
        }
        // Lighter went through the maker path → exactly one post_only
        // place, no taker fallback, reduce_only=true propagated.
        let posts = lt_ops.snapshot_posts();
        assert_eq!(posts.len(), 1, "expected one post_only place on Lighter");
        let (_, _, _, _, reduce_only) = &posts[0];
        assert!(
            *reduce_only,
            "exit-side post_only must carry reduce_only=true"
        );
        assert!(
            lt_ops.snapshot_takers().is_empty(),
            "happy-path maker fill: no taker fallback"
        );
    }

    #[test]
    fn config_rejects_zero_timeout() {
        let pc = ParallelExitConfig {
            leg_mismatch_timeout_ms: 0,
        };
        assert!(pc.validate().is_err());
    }

    #[test]
    fn config_accepts_realistic_timeout() {
        let pc = ParallelExitConfig {
            leg_mismatch_timeout_ms: 3000,
        };
        assert!(pc.validate().is_ok());
    }
}
