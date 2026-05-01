//! Production [`VenueOps`] adapter wrapping an `Arc<dyn DexConnector>`
//! (bot-strategy#244 Group B plumbing).
//!
//! Translates the small [`VenueOps`] surface the executors care about
//! (`extended_maker`, `lighter_fill`, `emergency_loop`, `parallel_exit`)
//! into the broader [`DexConnector`] trait. Sprint 4 will plug
//! instances of this adapter into `xvenue::live`'s `Decision::Enter` /
//! `Decision::Exit` paths; this commit lands the adapter only.
//!
//! ## API mapping
//!
//! | VenueOps method     | DexConnector call(s)                                       |
//! |---------------------|------------------------------------------------------------|
//! | `read_top_of_book`  | `get_order_book(symbol, 1)`                                |
//! | `place_post_only`   | `create_order(price=Some, spread=Some(-2), reduce_only=false)` |
//! | `place_taker`       | `create_order(price=None, spread=None, reduce_only=…)`     |
//! | `cancel`            | `cancel_order(symbol, order_id)`                           |
//! | `poll_fill_status`  | `get_filled_orders` + `get_canceled_orders` + `get_open_orders`, filter by `order_id`, aggregate |
//! | `close_all`         | `close_all_positions(symbol.map(String::from))`            |
//!
//! The `Some(-2)` marker on `create_order`'s `spread` parameter is
//! Extended's post-only flag (extended_connector.rs:3098). Other
//! venues ignore the marker and place a regular limit; the chase
//! loop's terminal-cancelled and partial-fill handling still copes
//! with whatever fill behaviour results.
//!
//! `poll_fill_status` does three connector reads. In production the
//! per-venue connectors back these with an in-memory WS-fed cache
//! (no REST round-trip per call), so the fan-out via `try_join!` is
//! cheap enough for the chase loop's ~50 ms poll cadence. When a
//! cache lookup fails the entire poll surfaces `Err`; the executor's
//! existing `poll_fill_status` error path keeps polling until its
//! per-round deadline.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dex_connector::{DexConnector, OrderSide};
use rust_decimal::Decimal;

use super::venue_ops::{OrderFillStatus, PlacedOrder, TopOfBook, VenueOps};

/// Sentinel value Extended interprets as the post-only flag on
/// `create_order`'s `spread` parameter
/// (extended_connector.rs:3098 — `let post_only = matches!(spread, Some(-2));`).
const EXTENDED_POST_ONLY_SPREAD_MARKER: i64 = -2;

/// Adapter holding a shared connector handle. One instance per venue;
/// the runner clones the inner `Arc` to pass independent handles to
/// each executor (`extended_maker` / `lighter_fill` /
/// `emergency_loop`).
pub struct LiveVenueOps {
    pub conn: Arc<dyn DexConnector>,
}

impl LiveVenueOps {
    pub fn new(conn: Arc<dyn DexConnector>) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl VenueOps for LiveVenueOps {
    async fn read_top_of_book(&self, symbol: &str) -> Result<TopOfBook> {
        let snap = self
            .conn
            .get_order_book(symbol, 1)
            .await
            .map_err(|e| anyhow!("get_order_book {}: {}", symbol, e))?;
        let best_bid = snap
            .bids
            .first()
            .map(|l| l.price)
            .unwrap_or(Decimal::ZERO);
        let best_ask = snap
            .asks
            .first()
            .map(|l| l.price)
            .unwrap_or(Decimal::ZERO);
        Ok(TopOfBook { best_bid, best_ask })
    }

    async fn place_post_only(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
    ) -> Result<PlacedOrder> {
        let resp = self
            .conn
            .create_order(
                symbol,
                qty,
                side,
                Some(price),
                Some(EXTENDED_POST_ONLY_SPREAD_MARKER),
                false,
                None,
            )
            .await
            .map_err(|e| anyhow!("create_order post-only {}: {}", symbol, e))?;
        Ok(PlacedOrder {
            order_id: resp.order_id,
        })
    }

    async fn place_taker(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        reduce_only: bool,
    ) -> Result<PlacedOrder> {
        let resp = self
            .conn
            .create_order(symbol, qty, side, None, None, reduce_only, None)
            .await
            .map_err(|e| anyhow!("create_order taker {}: {}", symbol, e))?;
        Ok(PlacedOrder {
            order_id: resp.order_id,
        })
    }

    async fn cancel(&self, symbol: &str, order_id: &str) -> Result<()> {
        self.conn
            .cancel_order(symbol, order_id)
            .await
            .map_err(|e| anyhow!("cancel_order {} {}: {}", symbol, order_id, e))
    }

    async fn poll_fill_status(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<OrderFillStatus> {
        let (filled, canceled, open) = tokio::try_join!(
            self.conn.get_filled_orders(symbol),
            self.conn.get_canceled_orders(symbol),
            self.conn.get_open_orders(symbol),
        )
        .map_err(|e| anyhow!("poll_fill_status {} {}: {}", symbol, order_id, e))?;

        let filled_qty: Decimal = filled
            .orders
            .iter()
            .filter(|o| o.order_id == order_id && !o.is_rejected)
            .filter_map(|o| o.filled_size)
            .sum();

        let cancelled = canceled.orders.iter().any(|o| o.order_id == order_id);
        let still_open = open.orders.iter().any(|o| o.order_id == order_id);

        // Terminal when the venue has either cancelled the order or
        // closed it after some fill (no longer in the open list, has
        // a non-zero fill aggregate). A still-open order with a
        // partial fill is non-terminal so the chase loop keeps
        // polling. A just-placed order that hasn't appeared in any
        // list yet (WS lag) also stays non-terminal.
        let terminal = cancelled || (!still_open && filled_qty > Decimal::ZERO);

        Ok(OrderFillStatus {
            filled_qty,
            terminal,
            cancelled,
        })
    }

    async fn close_all(&self, symbol: Option<&str>) -> Result<()> {
        self.conn
            .close_all_positions(symbol.map(str::to_owned))
            .await
            .map_err(|e| anyhow!("close_all_positions: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dex_connector::{
        BalanceResponse, CanceledOrder, CanceledOrdersResponse, CombinedBalanceResponse,
        CreateOrderResponse, DexError, FilledOrder, FilledOrdersResponse, LastTradesResponse,
        OpenOrder, OpenOrdersResponse, OrderBookLevel, OrderBookSnapshot, PositionSnapshot,
        TickerResponse, TpSl, TriggerOrderStyle,
    };
    use rust_decimal_macros::dec;
    use std::sync::Mutex;

    /// Captures one `create_order` call's positional args. Tests
    /// assert against this to verify the post-only / taker mapping.
    #[derive(Debug, Clone, PartialEq)]
    struct CreateOrderCall {
        symbol: String,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        spread: Option<i64>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    }

    /// Per-method scripted outputs. Tests pre-load the relevant
    /// queues; the stub returns the queued value (or a sane default
    /// for "not the focus of this test" cases).
    #[derive(Default)]
    struct StubState {
        order_book: Option<OrderBookSnapshot>,
        order_book_err: Option<String>,
        next_create_order_id: u64,
        create_order_err: Option<String>,
        cancel_order_err: Option<String>,
        close_all_err: Option<String>,
        filled_orders: Vec<FilledOrder>,
        canceled_orders: Vec<CanceledOrder>,
        open_orders: Vec<OpenOrder>,

        create_order_calls: Vec<CreateOrderCall>,
        cancel_order_calls: Vec<(String, String)>,
        close_all_calls: Vec<Option<String>>,
        get_order_book_calls: Vec<(String, usize)>,
    }

    /// Stub `DexConnector` for adapter-mapping tests. Implements the
    /// full trait — methods the adapter never touches return the
    /// `Default` response so a stray call won't silently corrupt a
    /// test (a real bug would surface as an unexpected fill, etc.).
    struct StubConnector {
        state: Mutex<StubState>,
    }

    impl StubConnector {
        fn new() -> Self {
            Self {
                state: Mutex::new(StubState::default()),
            }
        }

        fn arc(self) -> Arc<dyn DexConnector> {
            Arc::new(self)
        }
    }

    #[async_trait]
    impl DexConnector for StubConnector {
        async fn start(&self) -> Result<(), DexError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), DexError> {
            Ok(())
        }
        async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
            Ok(())
        }
        async fn set_leverage(&self, _: &str, _: u32) -> Result<(), DexError> {
            Ok(())
        }
        async fn get_ticker(
            &self,
            _: &str,
            _: Option<Decimal>,
        ) -> Result<TickerResponse, DexError> {
            Ok(TickerResponse::default())
        }
        async fn get_filled_orders(&self, _: &str) -> Result<FilledOrdersResponse, DexError> {
            let g = self.state.lock().unwrap();
            Ok(FilledOrdersResponse {
                orders: g.filled_orders.clone(),
            })
        }
        async fn get_canceled_orders(&self, _: &str) -> Result<CanceledOrdersResponse, DexError> {
            let g = self.state.lock().unwrap();
            Ok(CanceledOrdersResponse {
                orders: g.canceled_orders.clone(),
            })
        }
        async fn get_open_orders(&self, _: &str) -> Result<OpenOrdersResponse, DexError> {
            let g = self.state.lock().unwrap();
            Ok(OpenOrdersResponse {
                orders: g.open_orders.clone(),
            })
        }
        async fn get_balance(&self, _: Option<&str>) -> Result<BalanceResponse, DexError> {
            Ok(BalanceResponse::default())
        }
        async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
            Ok(CombinedBalanceResponse::default())
        }
        async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
            Ok(Vec::new())
        }
        async fn get_last_trades(&self, _: &str) -> Result<LastTradesResponse, DexError> {
            Ok(LastTradesResponse::default())
        }
        async fn get_order_book(
            &self,
            symbol: &str,
            depth: usize,
        ) -> Result<OrderBookSnapshot, DexError> {
            let mut g = self.state.lock().unwrap();
            g.get_order_book_calls.push((symbol.to_string(), depth));
            if let Some(msg) = g.order_book_err.take() {
                return Err(DexError::Other(msg));
            }
            Ok(g.order_book.clone().unwrap_or_default())
        }
        async fn clear_filled_order(&self, _: &str, _: &str) -> Result<(), DexError> {
            Ok(())
        }
        async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }
        async fn clear_canceled_order(&self, _: &str, _: &str) -> Result<(), DexError> {
            Ok(())
        }
        async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }
        async fn create_order(
            &self,
            symbol: &str,
            size: Decimal,
            side: OrderSide,
            price: Option<Decimal>,
            spread: Option<i64>,
            reduce_only: bool,
            expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            let mut g = self.state.lock().unwrap();
            g.create_order_calls.push(CreateOrderCall {
                symbol: symbol.to_string(),
                size,
                side,
                price,
                spread,
                reduce_only,
                expiry_secs,
            });
            if let Some(msg) = g.create_order_err.take() {
                return Err(DexError::Other(msg));
            }
            g.next_create_order_id += 1;
            Ok(CreateOrderResponse {
                order_id: format!("stub-order-{}", g.next_create_order_id),
                ..Default::default()
            })
        }
        async fn create_advanced_trigger_order(
            &self,
            _: &str,
            _: Decimal,
            _: OrderSide,
            _: Decimal,
            _: Option<Decimal>,
            _: TriggerOrderStyle,
            _: Option<u32>,
            _: TpSl,
            _: bool,
            _: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            Ok(CreateOrderResponse::default())
        }
        async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
            let mut g = self.state.lock().unwrap();
            g.cancel_order_calls
                .push((symbol.to_string(), order_id.to_string()));
            if let Some(msg) = g.cancel_order_err.take() {
                return Err(DexError::Other(msg));
            }
            Ok(())
        }
        async fn cancel_all_orders(&self, _: Option<String>) -> Result<(), DexError> {
            Ok(())
        }
        async fn cancel_orders(
            &self,
            _: Option<String>,
            _: Vec<String>,
        ) -> Result<(), DexError> {
            Ok(())
        }
        async fn close_all_positions(
            &self,
            symbol: Option<String>,
        ) -> Result<(), DexError> {
            let mut g = self.state.lock().unwrap();
            g.close_all_calls.push(symbol);
            if let Some(msg) = g.close_all_err.take() {
                return Err(DexError::Other(msg));
            }
            Ok(())
        }
        async fn clear_last_trades(&self, _: &str) -> Result<(), DexError> {
            Ok(())
        }
        async fn is_upcoming_maintenance(&self, _: i64) -> bool {
            false
        }
        async fn sign_evm_65b(&self, _: &str) -> Result<String, DexError> {
            Ok(String::new())
        }
        async fn sign_evm_65b_with_eip191(&self, _: &str) -> Result<String, DexError> {
            Ok(String::new())
        }
    }

    fn book(best_bid: Decimal, best_ask: Decimal) -> OrderBookSnapshot {
        OrderBookSnapshot {
            bids: vec![OrderBookLevel {
                price: best_bid,
                size: dec!(1),
            }],
            asks: vec![OrderBookLevel {
                price: best_ask,
                size: dec!(1),
            }],
        }
    }

    #[tokio::test]
    async fn read_top_of_book_extracts_top_levels() {
        let stub = StubConnector::new();
        stub.state.lock().unwrap().order_book = Some(book(dec!(78000), dec!(78001)));
        let ops = LiveVenueOps::new(stub.arc());
        let tob = ops.read_top_of_book("BTC-USD").await.unwrap();
        assert_eq!(tob.best_bid, dec!(78000));
        assert_eq!(tob.best_ask, dec!(78001));
    }

    #[tokio::test]
    async fn read_top_of_book_returns_zero_on_empty_levels() {
        let stub = StubConnector::new();
        stub.state.lock().unwrap().order_book = Some(OrderBookSnapshot::default());
        let ops = LiveVenueOps::new(stub.arc());
        let tob = ops.read_top_of_book("BTC-USD").await.unwrap();
        assert_eq!(tob.best_bid, Decimal::ZERO);
        assert_eq!(tob.best_ask, Decimal::ZERO);
    }

    #[tokio::test]
    async fn read_top_of_book_propagates_connector_error() {
        let stub = StubConnector::new();
        stub.state.lock().unwrap().order_book_err = Some("ws stale".into());
        let ops = LiveVenueOps::new(stub.arc());
        let err = ops.read_top_of_book("BTC-USD").await.unwrap_err();
        assert!(err.to_string().contains("get_order_book"));
        assert!(err.to_string().contains("ws stale"));
    }

    #[tokio::test]
    async fn read_top_of_book_requests_depth_one() {
        // Defensive: the chase loop only needs the touch, so depth=1
        // keeps each call cheap. Higher depths would slow the chase
        // poll cadence on connectors that fall through to REST.
        let stub_arc: Arc<StubConnector> = Arc::new(StubConnector::new());
        stub_arc.state.lock().unwrap().order_book = Some(book(dec!(1), dec!(2)));
        let ops = LiveVenueOps::new(stub_arc.clone());
        let _ = ops.read_top_of_book("BTC-USD").await.unwrap();
        let calls = stub_arc.state.lock().unwrap().get_order_book_calls.clone();
        assert_eq!(calls, vec![("BTC-USD".to_string(), 1usize)]);
    }

    #[tokio::test]
    async fn place_post_only_uses_extended_marker_and_clears_reduce_only() {
        let stub_arc: Arc<StubConnector> = Arc::new(StubConnector::new());
        let ops = LiveVenueOps::new(stub_arc.clone());
        let placed = ops
            .place_post_only("BTC-USD", OrderSide::Long, dec!(0.5), dec!(78000))
            .await
            .unwrap();
        assert_eq!(placed.order_id, "stub-order-1");
        let calls = stub_arc.state.lock().unwrap().create_order_calls.clone();
        assert_eq!(calls.len(), 1);
        let c = &calls[0];
        assert_eq!(c.symbol, "BTC-USD");
        assert_eq!(c.side, OrderSide::Long);
        assert_eq!(c.size, dec!(0.5));
        assert_eq!(c.price, Some(dec!(78000)));
        assert_eq!(c.spread, Some(EXTENDED_POST_ONLY_SPREAD_MARKER));
        assert!(!c.reduce_only, "post-only entries are never reduce-only");
        assert_eq!(c.expiry_secs, None);
    }

    #[tokio::test]
    async fn place_taker_passes_reduce_only_and_omits_price_spread() {
        let stub_arc: Arc<StubConnector> = Arc::new(StubConnector::new());
        let ops = LiveVenueOps::new(stub_arc.clone());
        let _ = ops
            .place_taker("ETH", OrderSide::Short, dec!(0.1), true)
            .await
            .unwrap();
        let calls = stub_arc.state.lock().unwrap().create_order_calls.clone();
        assert_eq!(calls.len(), 1);
        let c = &calls[0];
        assert_eq!(c.symbol, "ETH");
        assert_eq!(c.side, OrderSide::Short);
        assert_eq!(c.size, dec!(0.1));
        assert_eq!(c.price, None);
        assert_eq!(c.spread, None);
        assert!(c.reduce_only);
    }

    #[tokio::test]
    async fn place_post_only_propagates_create_error() {
        let stub = StubConnector::new();
        stub.state.lock().unwrap().create_order_err = Some("auth fail".into());
        let ops = LiveVenueOps::new(stub.arc());
        let err = ops
            .place_post_only("BTC-USD", OrderSide::Long, dec!(0.5), dec!(78000))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("create_order"));
        assert!(err.to_string().contains("auth fail"));
    }

    #[tokio::test]
    async fn cancel_forwards_symbol_and_order_id() {
        let stub_arc: Arc<StubConnector> = Arc::new(StubConnector::new());
        let ops = LiveVenueOps::new(stub_arc.clone());
        ops.cancel("BTC-USD", "order-42").await.unwrap();
        let calls = stub_arc.state.lock().unwrap().cancel_order_calls.clone();
        assert_eq!(
            calls,
            vec![("BTC-USD".to_string(), "order-42".to_string())]
        );
    }

    #[tokio::test]
    async fn cancel_propagates_connector_error() {
        let stub = StubConnector::new();
        stub.state.lock().unwrap().cancel_order_err = Some("not found".into());
        let ops = LiveVenueOps::new(stub.arc());
        let err = ops.cancel("BTC-USD", "x").await.unwrap_err();
        assert!(err.to_string().contains("cancel_order"));
    }

    #[tokio::test]
    async fn close_all_passes_symbol_to_owned() {
        let stub_arc: Arc<StubConnector> = Arc::new(StubConnector::new());
        let ops = LiveVenueOps::new(stub_arc.clone());
        ops.close_all(Some("BTC-USD")).await.unwrap();
        ops.close_all(None).await.unwrap();
        let calls = stub_arc.state.lock().unwrap().close_all_calls.clone();
        assert_eq!(calls, vec![Some("BTC-USD".to_string()), None]);
    }

    #[tokio::test]
    async fn poll_fill_status_aggregates_matching_filled_size() {
        let stub = StubConnector::new();
        {
            let mut g = stub.state.lock().unwrap();
            g.filled_orders = vec![
                FilledOrder {
                    order_id: "target".into(),
                    is_rejected: false,
                    trade_id: "t1".into(),
                    filled_size: Some(dec!(0.3)),
                    ..Default::default()
                },
                FilledOrder {
                    order_id: "target".into(),
                    is_rejected: false,
                    trade_id: "t2".into(),
                    filled_size: Some(dec!(0.2)),
                    ..Default::default()
                },
                // Different order_id — must be ignored.
                FilledOrder {
                    order_id: "other".into(),
                    is_rejected: false,
                    trade_id: "t3".into(),
                    filled_size: Some(dec!(99)),
                    ..Default::default()
                },
                // Rejected fill — must be ignored even on matching id.
                FilledOrder {
                    order_id: "target".into(),
                    is_rejected: true,
                    trade_id: "t4".into(),
                    filled_size: Some(dec!(99)),
                    ..Default::default()
                },
            ];
            // Order is no longer open (so terminal=true).
            g.open_orders = vec![];
        }
        let ops = LiveVenueOps::new(stub.arc());
        let s = ops.poll_fill_status("BTC-USD", "target").await.unwrap();
        assert_eq!(s.filled_qty, dec!(0.5));
        assert!(s.terminal);
        assert!(!s.cancelled);
    }

    #[tokio::test]
    async fn poll_fill_status_non_terminal_while_still_open() {
        let stub = StubConnector::new();
        {
            let mut g = stub.state.lock().unwrap();
            g.filled_orders = vec![FilledOrder {
                order_id: "target".into(),
                is_rejected: false,
                trade_id: "t1".into(),
                filled_size: Some(dec!(0.04)),
                ..Default::default()
            }];
            g.open_orders = vec![OpenOrder {
                order_id: "target".into(),
                symbol: "BTC-USD".into(),
                side: OrderSide::Long,
                size: dec!(0.1),
                price: dec!(78000),
                status: "open".into(),
            }];
        }
        let ops = LiveVenueOps::new(stub.arc());
        let s = ops.poll_fill_status("BTC-USD", "target").await.unwrap();
        assert_eq!(s.filled_qty, dec!(0.04));
        assert!(!s.terminal, "still in open_orders → keep polling");
        assert!(!s.cancelled);
    }

    #[tokio::test]
    async fn poll_fill_status_cancelled_marks_both_flags() {
        let stub = StubConnector::new();
        stub.state.lock().unwrap().canceled_orders = vec![CanceledOrder {
            order_id: "target".into(),
            canceled_timestamp: 0,
        }];
        let ops = LiveVenueOps::new(stub.arc());
        let s = ops.poll_fill_status("BTC-USD", "target").await.unwrap();
        assert_eq!(s.filled_qty, Decimal::ZERO);
        assert!(s.terminal);
        assert!(s.cancelled);
    }

    #[tokio::test]
    async fn poll_fill_status_zero_fill_no_lists_stays_non_terminal() {
        // A just-placed order whose acks haven't propagated to any of
        // the three lists yet must NOT terminal-out — otherwise the
        // chase loop would treat WS lag as a venue cancel.
        let stub = StubConnector::new();
        let ops = LiveVenueOps::new(stub.arc());
        let s = ops.poll_fill_status("BTC-USD", "target").await.unwrap();
        assert_eq!(s.filled_qty, Decimal::ZERO);
        assert!(!s.terminal);
        assert!(!s.cancelled);
    }
}
