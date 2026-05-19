//! Venue-side primitives shared by `extended_maker` and
//! `lighter_fill` (bot-strategy#244 Group B).
//!
//! Wraps the subset of `dex_connector::DexConnector` the executor
//! actually needs into an `async_trait` we can substitute in tests.
//! Production wires this to the live `DexConnectorBox`; unit tests
//! drive a `ScriptedVenueOps` to deterministically replay book +
//! fill sequences.
//!
//! Why an extra layer over `DexConnector`:
//!
//! - The executor only cares about a tiny slice (place / cancel /
//!   poll fills / read top-of-book). Mocking the full `DexConnector`
//!   trait with all 30+ methods would be noise per-test.
//! - `DexConnector` returns rich response types (`CreateOrderResponse`,
//!   `FilledOrdersResponse`) that include venue-specific fields the
//!   chase loop never reads. Stripping them down keeps the failure
//!   matrix in `docs/execution_layer.md` §2 readable.
//! - The `Cancelled` failure mode (catalogue case 1 / partial cases)
//!   currently has no clean rep in `DexConnector`'s error type — we
//!   surface it via the trait-level enum so tests can simulate it
//!   without faking up `DexError` variants.

use std::collections::VecDeque;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use dex_connector::OrderSide;
use rust_decimal::Decimal;

/// What `place_post_only` / `place_taker` returns. The executor only
/// needs the order id; everything else (`exchange_order_id`, fees,
/// etc.) is venue-specific noise the chase loop ignores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedOrder {
    pub order_id: String,
}

/// Top-of-book snapshot for the chase loop's reprice path.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopOfBook {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
}

/// Filled aggregator output for one specific `order_id`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderFillStatus {
    /// Total qty filled against this order so far. Sum of all
    /// partial fills the venue has reported.
    pub filled_qty: Decimal,
    /// Total notional value filled against this order so far —
    /// `sum(fill_price_i * fill_qty_i)` across the partial fills.
    /// Always non-negative. `None` when the underlying venue layer
    /// does not (yet) populate it; the consumer should fall back to
    /// the mid-based PnL approximation in that case. Populated for
    /// Extended via `FilledOrder.filled_value` (dex-connector) and
    /// for Lighter the same way (bot-strategy#435). Used by
    /// `compute_realised_pnl` to replace `*_entry_mid` /
    /// `*_exit_mid` with the volume-weighted average fill price.
    pub filled_value: Option<Decimal>,
    /// True when the venue has reported the order as terminal —
    /// either fully filled or cancelled. The executor uses this to
    /// stop polling without waiting for the full timeout.
    pub terminal: bool,
    /// True when the venue cancelled the order on its own (e.g.
    /// post-only price moved through the book). Distinct from
    /// `terminal && filled_qty == 0` because the runner emits
    /// `Cancelled` failure rather than `Timeout` in that case.
    pub cancelled: bool,
}

#[async_trait]
pub trait VenueOps: Send + Sync {
    /// Reads top-of-book once. Used by the maker to compute the
    /// next post-only price each chase round. The executor does not
    /// subscribe to a feed — single point read keeps the trait
    /// thin and lets the caller throttle.
    async fn read_top_of_book(&self, symbol: &str) -> Result<TopOfBook>;

    /// Places a post-only limit order. Returns the venue order id
    /// once the venue has acknowledged the placement. Errors are
    /// surfaced as anyhow so the caller can decide whether to
    /// retry, escalate, or fall through to taker.
    ///
    /// `reduce_only=true` MUST be set for exit-side post-only chases:
    /// without it, multiple rounds racing against each other can
    /// over-fill and flip the held position to the opposite direction
    /// (bot-strategy#289 — observed 2026-05-03 round-trip 2).
    /// `reduce_only=false` for entry-side; the entry path opens
    /// fresh exposure where reduce-only would just block fills.
    async fn place_post_only(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
    ) -> Result<PlacedOrder>;

    /// Places a taker (market or aggressive limit) order. Used by
    /// both the Extended taker fallback and the Lighter fill path.
    async fn place_taker(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        reduce_only: bool,
    ) -> Result<PlacedOrder>;

    /// Cancels one specific order by id. Idempotent — the caller
    /// may call cancel after the order has already terminated and
    /// the trait must not surface that as an error.
    async fn cancel(&self, symbol: &str, order_id: &str) -> Result<()>;

    /// Polls the venue for the latest fill aggregate on one order.
    /// Cheap call — the trait expects an in-memory cache fed by
    /// the venue's WS fill stream, not a fresh REST hit per poll.
    async fn poll_fill_status(&self, symbol: &str, order_id: &str) -> Result<OrderFillStatus>;

    /// Reduce-only "close everything" call. Used by the
    /// emergency-flatten loop in `Phase::EmergencyFlattening`. The
    /// `symbol` argument is `None` to reduce-only on every position
    /// the account currently holds — Extended and Lighter both
    /// support this shape via `close_all_positions(None)`.
    async fn close_all(&self, symbol: Option<&str>) -> Result<()>;

    /// True when the venue is in maintenance OR a declared maintenance
    /// window is within `hours_ahead` hours. Mirrors the
    /// `DexConnector::is_upcoming_maintenance` contract — the runner's
    /// pre-decision gate consults this to block new entries before the
    /// venue starts rejecting orders. Defaults to `false` so connectors
    /// without maintenance protocol semantics (e.g. Lighter) don't
    /// have to opt into the check; only Extended currently surfaces
    /// real maintenance state (bot-strategy#196 + #317).
    async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
        false
    }

    /// Absolute size of the current open position for `symbol` (returns
    /// `0` when the venue holds no position). Exit dispatch consults
    /// this to reconcile the state machine's `*_open_qty` against the
    /// actual venue position when the post_only chase loop's terminal
    /// under-reports a trailing trade that arrived after the loop
    /// returned (bot-strategy#418 re-open 2026-05-17). Default returns
    /// `0` so impls that don't matter for this reconciliation (e.g.
    /// scripted mocks for unrelated tests) stay opt-out.
    async fn current_position_size(&self, _symbol: &str) -> Result<Decimal> {
        Ok(Decimal::ZERO)
    }
}

// ---------------------------------------------------------------------
// Scripted mock for unit tests.
// ---------------------------------------------------------------------

/// One scripted response for a single venue op. Tests build a queue
/// of these and the mock pops them in order. Default behavior on an
/// empty queue is to return `Ok` with sensible defaults so the bulk
/// of a test scenario can omit the boring setup calls.
#[derive(Debug, Clone)]
pub enum ScriptedResponse {
    Ok,
    /// Specific fill status to return on the next `poll_fill_status`.
    /// Tests use this to walk the chase loop through partial fills,
    /// terminal-filled, terminal-cancelled, etc.
    FillStatus(OrderFillStatus),
    /// Top-of-book to return on the next `read_top_of_book`.
    Book(TopOfBook),
    /// Order id to return on the next `place_*` call.
    PlacedOrder(PlacedOrder),
    /// Inject a venue error for the next call. Anyhow surface keeps
    /// the test compact (no need to construct `DexError` variants).
    Err(String),
}

/// Per-method script queues. Exposed as an `Arc<Mutex<…>>` because
/// the trait method takes `&self` and tests share the mock across
/// chase iterations. FIFO order (`VecDeque::pop_front`) so a test's
/// `push_back` reads naturally as "next response, then next, …".
#[derive(Default, Debug)]
pub struct ScriptedVenueOpsState {
    pub book: VecDeque<ScriptedResponse>,
    pub place_post_only: VecDeque<ScriptedResponse>,
    pub place_taker: VecDeque<ScriptedResponse>,
    pub cancel: VecDeque<ScriptedResponse>,
    pub poll_fill: VecDeque<ScriptedResponse>,
    pub close_all: VecDeque<ScriptedResponse>,

    /// Per-symbol mock position size returned by
    /// [`VenueOps::current_position_size`]. Defaults to empty (= 0 for
    /// any symbol) so tests that don't care can ignore it.
    pub current_positions: std::collections::HashMap<String, Decimal>,

    /// Default fill status returned when the `poll_fill` queue is
    /// empty. Tests that don't care about polling ordering can set
    /// this once and let the mock keep returning it.
    pub default_fill: OrderFillStatus,
    /// Default top-of-book for the same reason.
    pub default_book: TopOfBook,
    /// Counter for auto-generated order ids when `place_*` queues
    /// are empty.
    pub next_order_id: u64,

    /// Captured place_post_only calls (symbol, side, qty, price).
    /// Exposed for assertions.
    pub posts: Vec<(String, OrderSide, Decimal, Decimal, bool)>,
    /// Captured place_taker calls (symbol, side, qty, reduce_only).
    pub takers: Vec<(String, OrderSide, Decimal, bool)>,
    /// Captured cancel calls (symbol, order_id).
    pub cancels: Vec<(String, String)>,
    /// Captured close_all calls.
    pub close_alls: Vec<Option<String>>,
}

#[derive(Debug)]
pub struct ScriptedVenueOps {
    inner: Mutex<ScriptedVenueOpsState>,
}

impl Default for ScriptedVenueOps {
    fn default() -> Self {
        Self {
            inner: Mutex::new(ScriptedVenueOpsState::default()),
        }
    }
}

impl ScriptedVenueOps {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mutate the state under the mutex. Tests prefer this over
    /// taking a write guard themselves so the lock scope stays tiny.
    pub fn with_state<F: FnOnce(&mut ScriptedVenueOpsState) -> R, R>(&self, f: F) -> R {
        let mut g = self.inner.lock().unwrap();
        f(&mut g)
    }

    pub fn snapshot_posts(&self) -> Vec<(String, OrderSide, Decimal, Decimal, bool)> {
        self.inner.lock().unwrap().posts.clone()
    }

    pub fn snapshot_takers(&self) -> Vec<(String, OrderSide, Decimal, bool)> {
        self.inner.lock().unwrap().takers.clone()
    }

    pub fn snapshot_cancels(&self) -> Vec<(String, String)> {
        self.inner.lock().unwrap().cancels.clone()
    }

    pub fn snapshot_close_alls(&self) -> Vec<Option<String>> {
        self.inner.lock().unwrap().close_alls.clone()
    }

    fn next_id(&self) -> String {
        let mut g = self.inner.lock().unwrap();
        g.next_order_id = g.next_order_id.saturating_add(1);
        format!("mock-order-{}", g.next_order_id)
    }
}

#[async_trait]
impl VenueOps for ScriptedVenueOps {
    async fn read_top_of_book(&self, _symbol: &str) -> Result<TopOfBook> {
        let mut g = self.inner.lock().unwrap();
        if let Some(resp) = g.book.pop_front() {
            match resp {
                ScriptedResponse::Book(b) => return Ok(b),
                ScriptedResponse::Err(msg) => return Err(anyhow::anyhow!(msg)),
                _ => return Err(anyhow::anyhow!("unexpected scripted response for book")),
            }
        }
        Ok(g.default_book)
    }

    async fn place_post_only(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
    ) -> Result<PlacedOrder> {
        let resp_opt = {
            let mut g = self.inner.lock().unwrap();
            g.posts
                .push((symbol.to_string(), side, qty, price, reduce_only));
            g.place_post_only.pop_front()
        };
        match resp_opt {
            Some(ScriptedResponse::PlacedOrder(o)) => Ok(o),
            Some(ScriptedResponse::Err(msg)) => Err(anyhow::anyhow!(msg)),
            Some(ScriptedResponse::Ok) | None => Ok(PlacedOrder {
                order_id: self.next_id(),
            }),
            Some(other) => Err(anyhow::anyhow!(
                "unexpected scripted response for place_post_only: {:?}",
                other
            )),
        }
    }

    async fn place_taker(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        reduce_only: bool,
    ) -> Result<PlacedOrder> {
        let resp_opt = {
            let mut g = self.inner.lock().unwrap();
            g.takers.push((symbol.to_string(), side, qty, reduce_only));
            g.place_taker.pop_front()
        };
        match resp_opt {
            Some(ScriptedResponse::PlacedOrder(o)) => Ok(o),
            Some(ScriptedResponse::Err(msg)) => Err(anyhow::anyhow!(msg)),
            Some(ScriptedResponse::Ok) | None => Ok(PlacedOrder {
                order_id: self.next_id(),
            }),
            Some(other) => Err(anyhow::anyhow!(
                "unexpected scripted response for place_taker: {:?}",
                other
            )),
        }
    }

    async fn cancel(&self, symbol: &str, order_id: &str) -> Result<()> {
        let resp_opt = {
            let mut g = self.inner.lock().unwrap();
            g.cancels.push((symbol.to_string(), order_id.to_string()));
            g.cancel.pop_front()
        };
        match resp_opt {
            Some(ScriptedResponse::Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Ok(()),
        }
    }

    async fn poll_fill_status(&self, _symbol: &str, _order_id: &str) -> Result<OrderFillStatus> {
        let mut g = self.inner.lock().unwrap();
        if let Some(resp) = g.poll_fill.pop_front() {
            match resp {
                ScriptedResponse::FillStatus(fs) => return Ok(fs),
                ScriptedResponse::Err(msg) => return Err(anyhow::anyhow!(msg)),
                _ => {
                    return Err(anyhow::anyhow!(
                        "unexpected scripted response for poll_fill_status"
                    ))
                }
            }
        }
        Ok(g.default_fill)
    }

    async fn close_all(&self, symbol: Option<&str>) -> Result<()> {
        let resp_opt = {
            let mut g = self.inner.lock().unwrap();
            g.close_alls.push(symbol.map(str::to_string));
            g.close_all.pop_front()
        };
        match resp_opt {
            Some(ScriptedResponse::Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Ok(()),
        }
    }

    async fn current_position_size(&self, symbol: &str) -> Result<Decimal> {
        let g = self.inner.lock().unwrap();
        Ok(g.current_positions
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO))
    }
}

impl Default for OrderFillStatus {
    fn default() -> Self {
        Self {
            filled_qty: Decimal::ZERO,
            filled_value: None,
            terminal: false,
            cancelled: false,
        }
    }
}

impl Default for TopOfBook {
    fn default() -> Self {
        Self {
            best_bid: Decimal::ZERO,
            best_ask: Decimal::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn place_post_only_records_call_and_returns_default_id() {
        let ops = ScriptedVenueOps::new();
        let placed = ops
            .place_post_only("BTC-USD", OrderSide::Long, dec!(0.1), dec!(78000), false)
            .await
            .unwrap();
        assert!(placed.order_id.starts_with("mock-order-"));
        let posts = ops.snapshot_posts();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].0, "BTC-USD");
        assert_eq!(posts[0].1, OrderSide::Long);
        assert_eq!(posts[0].2, dec!(0.1));
        assert_eq!(posts[0].3, dec!(78000));
    }

    #[tokio::test]
    async fn poll_fill_status_returns_default_when_queue_empty() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.default_fill = OrderFillStatus {
                filled_value: None,
                filled_qty: dec!(0.05),
                terminal: false,
                cancelled: false,
            };
        });
        let fs = ops.poll_fill_status("BTC-USD", "x").await.unwrap();
        assert_eq!(fs.filled_qty, dec!(0.05));
        assert!(!fs.terminal);
    }

    #[tokio::test]
    async fn scripted_err_propagates() {
        let ops = ScriptedVenueOps::new();
        ops.with_state(|s| {
            s.place_post_only
                .push_back(ScriptedResponse::Err("boom".into()));
        });
        let err = ops
            .place_post_only("BTC-USD", OrderSide::Long, dec!(0.1), dec!(78000), false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn cancel_is_idempotent_and_records_calls() {
        let ops = ScriptedVenueOps::new();
        ops.cancel("BTC-USD", "id-1").await.unwrap();
        ops.cancel("BTC-USD", "id-1").await.unwrap();
        let snap = ops.snapshot_cancels();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0], ("BTC-USD".to_string(), "id-1".to_string()));
    }

    #[tokio::test]
    async fn close_all_records_symbol_arg() {
        let ops = ScriptedVenueOps::new();
        ops.close_all(Some("BTC-USD")).await.unwrap();
        ops.close_all(None).await.unwrap();
        let snap = ops.snapshot_close_alls();
        assert_eq!(snap, vec![Some("BTC-USD".to_string()), None]);
    }
}
