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
