//! Adapter from `Arc<dyn DexConnector>` × 2 to the runner's [`VenueHub`].
//!
//! The runner core only needs `read_mid(venue)`; this module wraps the
//! full DexConnector trait down to that surface so the live binary can
//! drop in real Lighter / Extended connectors without leaking dex-
//! connector types into `xvenue::live`.
//!
//! `LiveVenueHub::read_mid` issues `get_order_book(symbol, 1)` against the
//! relevant venue, computes mid from `(top_bid + top_ask) / 2`, and
//! flags `book_ok = false` when either side has zero size — the exact
//! same gate the BT runner uses (`bt.rs::read_snapshot`). This is the
//! reason the BT-vs-Python parity fix from bot-strategy#166 part 4
//! works out of the box for live too.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dex_connector::DexConnector;
use rust_decimal::Decimal;

use crate::xvenue::live::{MidSnapshot, Venue, VenueHub};

/// Per-venue REST timeout for `get_balance` calls inside the status
/// loop. Equity reads are advisory — losing them is preferable to
/// stalling the snapshot cadence behind a hung venue.
const EQUITY_READ_TIMEOUT_MS: u64 = 1_500;

pub struct LiveVenueHub {
    pub extended: Arc<dyn DexConnector>,
    pub lighter: Arc<dyn DexConnector>,
    pub symbol_extended: String,
    pub symbol_lighter: String,
}

#[async_trait]
impl VenueHub for LiveVenueHub {
    async fn read_mid(&self, venue: Venue) -> Result<MidSnapshot> {
        let (conn, sym) = match venue {
            Venue::Extended => (&self.extended, self.symbol_extended.as_str()),
            Venue::Lighter => (&self.lighter, self.symbol_lighter.as_str()),
        };

        let ob = conn
            .get_order_book(sym, 1)
            .await
            .map_err(|e| anyhow!("get_order_book({}, 1): {:?}", sym, e))?;

        let bid = ob.bids.first();
        let ask = ob.asks.first();
        let book_ok = bid.map(|b| b.size > Decimal::ZERO).unwrap_or(false)
            && ask.map(|a| a.size > Decimal::ZERO).unwrap_or(false);

        let mid = match (bid, ask) {
            (Some(b), Some(a)) => (b.price + a.price) / Decimal::from(2),
            (Some(b), None) => b.price,
            (None, Some(a)) => a.price,
            (None, None) => Decimal::ZERO,
        };
        let bid_price = bid.map(|b| b.price).unwrap_or(Decimal::ZERO);
        let ask_price = ask.map(|a| a.price).unwrap_or(Decimal::ZERO);
        let bid_size = bid.map(|b| b.size).unwrap_or(Decimal::ZERO);
        let ask_size = ask.map(|a| a.size).unwrap_or(Decimal::ZERO);

        // Live doesn't get a per-record timestamp (replay does); use
        // wall-clock at read time. dev_bps + persistence accumulate against
        // this clock; jitter from network latency is in the few-ms range
        // and well below the 15s persistence default.
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Ok(MidSnapshot {
            ts_ms,
            mid,
            book_ok,
            bid: bid_price,
            ask: ask_price,
            bid_size,
            ask_size,
        })
    }

    async fn read_equity_usd(&self, venue: Venue) -> Result<Option<Decimal>> {
        let conn = match venue {
            Venue::Extended => &self.extended,
            Venue::Lighter => &self.lighter,
        };

        // `get_balance(None)` returns whole-account equity (collateral
        // currency = USD on both venues). Passing the trading symbol
        // makes Extended's REST endpoint reject with "Unsupported
        // balance symbol …" — the symbol arg is for spot/multi-asset
        // accounts, not the perp collateral query we want here.
        let fut = conn.get_balance(None);
        let bal = tokio::time::timeout(
            std::time::Duration::from_millis(EQUITY_READ_TIMEOUT_MS),
            fut,
        )
        .await
        .map_err(|_| anyhow!("get_balance({:?}) timed out", venue))?
        .map_err(|e| anyhow!("get_balance({:?}): {:?}", venue, e))?;
        Ok(Some(bal.equity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use dex_connector::{
        BalanceResponse, CanceledOrdersResponse, CombinedBalanceResponse, CreateOrderResponse,
        DexConnector, DexError, FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse,
        OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSnapshot, TickerResponse, TpSl,
        TriggerOrderStyle,
    };
    use rust_decimal_macros::dec;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Minimal `DexConnector` stub that only stubs the two methods
    /// `LiveVenueHub` actually calls. Everything else `unimplemented!()`
    /// so a stray call surfaces as a test panic rather than silently
    /// passing.
    struct DualStub {
        book: Mutex<OrderBookSnapshot>,
        balance: Mutex<BalanceResponse>,
        get_order_book_err: Mutex<Option<String>>,
        get_balance_err: Mutex<Option<String>>,
        /// When >0, `get_balance` waits this many ms before returning.
        /// Used by the timeout test together with `tokio::time::pause`.
        balance_delay_ms: Mutex<u64>,
    }

    impl DualStub {
        fn new() -> Self {
            Self {
                book: Mutex::new(OrderBookSnapshot::default()),
                balance: Mutex::new(BalanceResponse {
                    equity: Decimal::ZERO,
                    balance: Decimal::ZERO,
                    position_entry_price: None,
                    position_sign: None,
                }),
                get_order_book_err: Mutex::new(None),
                get_balance_err: Mutex::new(None),
                balance_delay_ms: Mutex::new(0),
            }
        }

        fn arc(self) -> Arc<dyn DexConnector> {
            Arc::new(self)
        }
    }

    #[async_trait]
    impl DexConnector for DualStub {
        async fn get_order_book(
            &self,
            _symbol: &str,
            _depth: usize,
        ) -> Result<OrderBookSnapshot, DexError> {
            if let Some(msg) = self.get_order_book_err.lock().unwrap().clone() {
                return Err(DexError::Transient(msg));
            }
            Ok(self.book.lock().unwrap().clone())
        }

        async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
            let delay = *self.balance_delay_ms.lock().unwrap();
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if let Some(msg) = self.get_balance_err.lock().unwrap().clone() {
                return Err(DexError::Transient(msg));
            }
            Ok(self.balance.lock().unwrap().clone())
        }

        // ---- everything below is unused by LiveVenueHub. unimplemented!()
        //      so a stray call would panic the test rather than silently
        //      stub-out. ----
        async fn start(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn stop(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn set_leverage(&self, _: &str, _: u32) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn get_ticker(
            &self,
            _: &str,
            _: Option<Decimal>,
        ) -> Result<TickerResponse, DexError> {
            unimplemented!()
        }
        async fn get_filled_orders(&self, _: &str) -> Result<FilledOrdersResponse, DexError> {
            unimplemented!()
        }
        async fn get_canceled_orders(&self, _: &str) -> Result<CanceledOrdersResponse, DexError> {
            unimplemented!()
        }
        async fn get_open_orders(&self, _: &str) -> Result<OpenOrdersResponse, DexError> {
            unimplemented!()
        }
        async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
            unimplemented!()
        }
        async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
            unimplemented!()
        }
        async fn get_last_trades(&self, _: &str) -> Result<LastTradesResponse, DexError> {
            unimplemented!()
        }
        async fn clear_filled_order(&self, _: &str, _: &str) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_canceled_order(&self, _: &str, _: &str) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn create_order(
            &self,
            _: &str,
            _: Decimal,
            _: OrderSide,
            _: Option<Decimal>,
            _: Option<i64>,
            _: bool,
            _: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            unimplemented!()
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
            unimplemented!()
        }
        async fn cancel_order(&self, _: &str, _: &str) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn cancel_all_orders(&self, _: Option<String>) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn cancel_orders(&self, _: Option<String>, _: Vec<String>) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn close_all_positions(&self, _: Option<String>) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_last_trades(&self, _: &str) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn is_upcoming_maintenance(&self, _: i64) -> bool {
            unimplemented!()
        }
        async fn sign_evm_65b(&self, _: &str) -> Result<String, DexError> {
            unimplemented!()
        }
        async fn sign_evm_65b_with_eip191(&self, _: &str) -> Result<String, DexError> {
            unimplemented!()
        }
    }

    fn hub(ext: Arc<dyn DexConnector>, lt: Arc<dyn DexConnector>) -> LiveVenueHub {
        LiveVenueHub {
            extended: ext,
            lighter: lt,
            symbol_extended: "ETH-USD".into(),
            symbol_lighter: "ETH".into(),
        }
    }

    #[tokio::test]
    async fn read_mid_book_ok_returns_midpoint() {
        let ext = DualStub::new();
        let lt = DualStub::new();
        *ext.book.lock().unwrap() = OrderBookSnapshot {
            bids: vec![OrderBookLevel {
                price: dec!(2_000),
                size: dec!(0.5),
            }],
            asks: vec![OrderBookLevel {
                price: dec!(2_010),
                size: dec!(0.5),
            }],
        };
        let h = hub(ext.arc(), lt.arc());
        let snap = h.read_mid(Venue::Extended).await.unwrap();
        assert_eq!(snap.mid, dec!(2_005));
        assert!(snap.book_ok);
    }

    #[tokio::test]
    async fn read_mid_zero_size_bid_drops_book_ok() {
        // BT runner's parity gate: zero size on either side flips
        // book_ok=false even though mid is still well-defined.
        let ext = DualStub::new();
        let lt = DualStub::new();
        *ext.book.lock().unwrap() = OrderBookSnapshot {
            bids: vec![OrderBookLevel {
                price: dec!(2_000),
                size: Decimal::ZERO,
            }],
            asks: vec![OrderBookLevel {
                price: dec!(2_010),
                size: dec!(0.5),
            }],
        };
        let h = hub(ext.arc(), lt.arc());
        let snap = h.read_mid(Venue::Extended).await.unwrap();
        assert_eq!(snap.mid, dec!(2_005), "mid still computes from prices");
        assert!(!snap.book_ok, "zero-size bid must flip book_ok=false");
    }

    #[tokio::test]
    async fn read_mid_empty_book_returns_zero_and_not_ok() {
        let ext = DualStub::new();
        let lt = DualStub::new();
        // Default OrderBookSnapshot has empty bids + asks vectors.
        let h = hub(ext.arc(), lt.arc());
        let snap = h.read_mid(Venue::Lighter).await.unwrap();
        assert_eq!(snap.mid, Decimal::ZERO);
        assert!(!snap.book_ok);
    }

    #[tokio::test]
    async fn read_mid_propagates_connector_error() {
        let ext = DualStub::new();
        let lt = DualStub::new();
        *ext.get_order_book_err.lock().unwrap() = Some("ws stale".into());
        let h = hub(ext.arc(), lt.arc());
        let err = h.read_mid(Venue::Extended).await.unwrap_err();
        assert!(err.to_string().contains("get_order_book"));
        assert!(err.to_string().contains("ws stale"));
    }

    #[tokio::test]
    async fn read_equity_usd_returns_balance_equity() {
        let ext = DualStub::new();
        let lt = DualStub::new();
        ext.balance.lock().unwrap().equity = dec!(1_234.56);
        let h = hub(ext.arc(), lt.arc());
        let eq = h.read_equity_usd(Venue::Extended).await.unwrap();
        assert_eq!(eq, Some(dec!(1_234.56)));
    }

    #[tokio::test]
    async fn read_equity_usd_propagates_connector_error() {
        let ext = DualStub::new();
        let lt = DualStub::new();
        *ext.get_balance_err.lock().unwrap() = Some("auth expired".into());
        let h = hub(ext.arc(), lt.arc());
        let err = h.read_equity_usd(Venue::Extended).await.unwrap_err();
        assert!(err.to_string().contains("get_balance"));
        assert!(err.to_string().contains("auth expired"));
    }

    /// Timeout path at lines 90-96: when `get_balance` doesn't return
    /// within `EQUITY_READ_TIMEOUT_MS`, surface as `Err(timed out)`
    /// rather than blocking the status loop. Uses `tokio::time::pause`
    /// + `start_paused` so the auto-advance fires the timeout
    /// deterministically.
    #[tokio::test(start_paused = true)]
    async fn read_equity_usd_times_out_when_connector_hangs() {
        let ext = DualStub::new();
        let lt = DualStub::new();
        // 60 s sleep — well past the 1500 ms timeout, so the timeout
        // arm fires first.
        *ext.balance_delay_ms.lock().unwrap() = 60_000;
        let h = hub(ext.arc(), lt.arc());
        let err = h.read_equity_usd(Venue::Extended).await.unwrap_err();
        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains("Extended"));
    }
}
