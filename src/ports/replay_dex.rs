use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dex_connector::{
    BalanceResponse, CanceledOrdersResponse, CombinedBalanceResponse, CreateOrderResponse,
    DexConnector, DexError, FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse,
    OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSnapshot, TickerResponse, TpSl,
    TriggerOrderStyle,
};
use rand;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

// Data structures that mirror the JSONL dump file
#[derive(Debug, Clone, Deserialize)]
struct DumpedSymbolSnapshot {
    price: Decimal,
    funding_rate: Decimal,
    #[serde(default)]
    bid_price: Option<Decimal>,
    #[serde(default)]
    ask_price: Option<Decimal>,
    bid_size: Decimal,
    ask_size: Decimal,
    /// Exchange-side tick second (live bot fills this from the DEX response).
    /// Missing in very old dumps; when absent we fall back to the record's
    /// top-level `timestamp` in `get_ticker`. The live bot uses this field
    /// (not `now()`) for bar-bucket assignment, so replaying BT without it
    /// shifts ticks across bucket boundaries and drifts the OLS history.
    /// See bot-strategy#27 comment 2026-04-16.
    #[serde(default)]
    exchange_ts: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct DumpedDataEntry {
    timestamp: i64,
    prices: HashMap<String, DumpedSymbolSnapshot>,
}

// Bincode-compatible representations using f64 (bincode doesn't support Decimal).
#[derive(Serialize, Deserialize)]
struct BincodeSymbolSnapshot {
    price: f64,
    funding_rate: f64,
    bid_price: f64,
    ask_price: f64,
    bid_size: f64,
    ask_size: f64,
    /// Per-symbol exchange tick second mirrored from the JSONL dump.
    /// bincode 1.x is a positional format, so this field has no
    /// `serde(default)` safety net — old `.bin` files without it will
    /// fail to parse. `bt_live_data.sh` always rebuilds `.bin` from
    /// JSONL before running, so this does not affect the live pipeline.
    /// `0` is the sentinel for "unknown" (we fall back to top-level ts).
    exchange_ts: i64,
}

#[derive(Serialize, Deserialize)]
struct BincodeDataEntry {
    timestamp: i64,
    prices: HashMap<String, BincodeSymbolSnapshot>,
}

impl From<&DumpedDataEntry> for BincodeDataEntry {
    fn from(e: &DumpedDataEntry) -> Self {
        Self {
            timestamp: e.timestamp,
            prices: e
                .prices
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        BincodeSymbolSnapshot {
                            price: v.price.to_f64().unwrap_or(0.0),
                            funding_rate: v.funding_rate.to_f64().unwrap_or(0.0),
                            bid_price: v.bid_price.and_then(|p| p.to_f64()).unwrap_or(0.0),
                            ask_price: v.ask_price.and_then(|p| p.to_f64()).unwrap_or(0.0),
                            bid_size: v.bid_size.to_f64().unwrap_or(0.0),
                            ask_size: v.ask_size.to_f64().unwrap_or(0.0),
                            exchange_ts: v.exchange_ts.unwrap_or(0),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<BincodeDataEntry> for DumpedDataEntry {
    fn from(e: BincodeDataEntry) -> Self {
        Self {
            timestamp: e.timestamp,
            prices: e
                .prices
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        DumpedSymbolSnapshot {
                            price: Decimal::from_f64(v.price).unwrap_or_default(),
                            funding_rate: Decimal::from_f64(v.funding_rate).unwrap_or_default(),
                            bid_price: if v.bid_price == 0.0 {
                                None
                            } else {
                                Decimal::from_f64(v.bid_price)
                            },
                            ask_price: if v.ask_price == 0.0 {
                                None
                            } else {
                                Decimal::from_f64(v.ask_price)
                            },
                            bid_size: Decimal::from_f64(v.bid_size).unwrap_or_default(),
                            ask_size: Decimal::from_f64(v.ask_size).unwrap_or_default(),
                            exchange_ts: if v.exchange_ts == 0 {
                                None
                            } else {
                                Some(v.exchange_ts)
                            },
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct ReplayConnector {
    data: std::sync::Arc<Vec<DumpedDataEntry>>,
    cursor: AtomicUsize,
}

impl ReplayConnector {
    pub fn new(path: &str) -> Result<Self, DexError> {
        let data = if path.ends_with(".bin") {
            Self::load_bincode(path)?
        } else {
            Self::load_jsonl(path)?
        };

        if data.is_empty() {
            return Err(DexError::Other(
                anyhow!("Data dump file is empty or invalid").to_string(),
            ));
        }

        Ok(Self {
            data: std::sync::Arc::new(data),
            cursor: AtomicUsize::new(0),
        })
    }

    /// Build a fresh connector that shares the parsed dump (Arc) with
    /// `self` but starts at cursor 0 with independent atomic state. Used
    /// by the BT grid runner so we parse the dump once and replay it
    /// many times in parallel without re-loading. See
    /// bot-strategy#166 Phase 1.
    pub fn clone_with_fresh_cursor(&self) -> Self {
        Self {
            data: std::sync::Arc::clone(&self.data),
            cursor: AtomicUsize::new(0),
        }
    }

    fn load_jsonl(path: &str) -> Result<Vec<DumpedDataEntry>, DexError> {
        let file = File::open(path)
            .map_err(|e| DexError::Other(format!("failed to open replay file: {}", e)))?;
        let reader = BufReader::new(file);
        let mut data = Vec::new();

        for line in reader.lines() {
            let line =
                line.map_err(|e| DexError::Other(format!("failed to read replay line: {}", e)))?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: DumpedDataEntry = serde_json::from_str(&line).map_err(|e| {
                DexError::Other(format!("failed to parse replay entry '{}': {}", line, e))
            })?;
            data.push(entry);
        }
        Ok(data)
    }

    fn load_bincode(path: &str) -> Result<Vec<DumpedDataEntry>, DexError> {
        let bytes = std::fs::read(path)
            .map_err(|e| DexError::Other(format!("failed to read bincode file: {}", e)))?;
        let bincode_data: Vec<BincodeDataEntry> = bincode::deserialize(&bytes)
            .map_err(|e| DexError::Other(format!("failed to deserialize bincode: {}", e)))?;
        Ok(bincode_data
            .into_iter()
            .map(DumpedDataEntry::from)
            .collect())
    }

    /// Convert a JSONL file to bincode format. Used by the convert-data tool.
    pub fn convert_jsonl_to_bincode(input: &str, output: &str) -> Result<(), DexError> {
        Self::convert_jsonl_to_bincode_with_interval(input, output, 0)
    }

    /// Convert JSONL to bincode with optional downsampling.
    /// `interval_secs`: minimum seconds between samples (0 = keep all).
    pub fn convert_jsonl_to_bincode_with_interval(
        input: &str,
        output: &str,
        interval_secs: u64,
    ) -> Result<(), DexError> {
        let data = Self::load_jsonl(input)?;
        let original_len = data.len();
        let filtered: Vec<&_> = if interval_secs > 0 {
            let interval_ms = (interval_secs * 1000) as i64;
            let mut last_ts: i64 = 0;
            data.iter()
                .filter(|e| {
                    if e.timestamp - last_ts >= interval_ms {
                        last_ts = e.timestamp;
                        true
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            data.iter().collect()
        };
        eprintln!(
            "Records: {} -> {} (interval={}s)",
            original_len,
            filtered.len(),
            interval_secs
        );
        let bincode_data: Vec<BincodeDataEntry> = filtered
            .iter()
            .map(|e| BincodeDataEntry::from(*e))
            .collect();
        let bytes = bincode::serialize(&bincode_data)
            .map_err(|e| DexError::Other(format!("failed to serialize bincode: {}", e)))?;
        std::fs::write(output, bytes)
            .map_err(|e| DexError::Other(format!("failed to write bincode file: {}", e)))?;
        Ok(())
    }

    /// Reset cursor to beginning for batch mode reuse.
    pub fn reset(&self) {
        self.cursor.store(0, AtomicOrdering::SeqCst);
    }

    /// Number of data entries.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    // Advances the simulation by one step. Returns false if the end is reached.
    pub fn tick(&self) -> bool {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        if current_cursor < self.data.len() - 1 {
            self.cursor.fetch_add(1, AtomicOrdering::SeqCst);
            true
        } else {
            false
        }
    }

    pub fn current_timestamp_secs(&self) -> Option<i64> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        self.data.get(current_cursor).map(|e| e.timestamp / 1000) // stored as ms
    }

    pub fn current_timestamp_ms(&self) -> Option<i64> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        self.data.get(current_cursor).map(|e| e.timestamp)
    }

    /// Timestamp_ms of the cursor+1 record, or `None` when at the last
    /// record. Used by [`DualReplay`] for event-time merge ordering.
    pub fn peek_next_timestamp_ms(&self) -> Option<i64> {
        let next = self.cursor.load(AtomicOrdering::SeqCst).checked_add(1)?;
        self.data.get(next).map(|e| e.timestamp)
    }

    pub fn at_end(&self) -> bool {
        self.cursor.load(AtomicOrdering::SeqCst) >= self.data.len().saturating_sub(1)
    }

    #[cfg(test)]
    fn from_entries(data: Vec<DumpedDataEntry>) -> Self {
        Self {
            data: std::sync::Arc::new(data),
            cursor: AtomicUsize::new(0),
        }
    }
}

/// Identifies which venue advanced on a given [`DualReplay::advance`] step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Extended,
    Lighter,
}

/// Coordinates two [`ReplayConnector`] instances (one per venue) over a
/// shared event-time clock. Each `advance()` call ticks whichever venue's
/// next record carries the older timestamp, producing a deterministic merge
/// of two independently-recorded JSONL dumps with possibly different
/// cadences. Both connectors are exposed as `Arc<ReplayConnector>` so the
/// strategy can call them through the existing `DexConnector` trait.
///
/// Bot-strategy#166 Phase 1 BT prep.
pub struct DualReplay {
    extended: std::sync::Arc<ReplayConnector>,
    lighter: std::sync::Arc<ReplayConnector>,
}

impl DualReplay {
    /// Load both venue dumps. `extended_path` and `lighter_path` accept the
    /// same `.jsonl` / `.bin` extensions as [`ReplayConnector::new`].
    pub fn new(extended_path: &str, lighter_path: &str) -> Result<Self, DexError> {
        Ok(Self {
            extended: std::sync::Arc::new(ReplayConnector::new(extended_path)?),
            lighter: std::sync::Arc::new(ReplayConnector::new(lighter_path)?),
        })
    }

    pub fn extended(&self) -> std::sync::Arc<ReplayConnector> {
        std::sync::Arc::clone(&self.extended)
    }

    pub fn lighter(&self) -> std::sync::Arc<ReplayConnector> {
        std::sync::Arc::clone(&self.lighter)
    }

    /// Advance the venue whose next record has the older timestamp. When
    /// only one venue still has data, that venue advances. Returns the
    /// venue that advanced, or `None` when both are exhausted.
    ///
    /// Tie-break: when both `peek_next_timestamp_ms` are equal, Extended
    /// advances first. The strategy must read both venues every call (the
    /// other side did not move, but still has a fresh-from-its-perspective
    /// snapshot at its current cursor) — order of reads within a tick is
    /// the strategy's responsibility.
    pub fn advance(&self) -> Option<Venue> {
        let ext_next = self.extended.peek_next_timestamp_ms();
        let lt_next = self.lighter.peek_next_timestamp_ms();
        match (ext_next, lt_next) {
            (Some(e), Some(l)) => {
                if e <= l {
                    self.extended.tick();
                    Some(Venue::Extended)
                } else {
                    self.lighter.tick();
                    Some(Venue::Lighter)
                }
            }
            (Some(_), None) => {
                self.extended.tick();
                Some(Venue::Extended)
            }
            (None, Some(_)) => {
                self.lighter.tick();
                Some(Venue::Lighter)
            }
            (None, None) => None,
        }
    }

    /// `min(ext_current_ts, lt_current_ts)` in ms. Treat as the BT clock —
    /// both venues have committed at least one snapshot at or before this
    /// instant, so any [`crate::xvenue::spread::SpreadEngine`] sample
    /// corresponding to this bucket is back-fillable. `None` until at
    /// least one venue has loaded its first record (always loaded after
    /// `new()`, so in practice always `Some`).
    pub fn aligned_timestamp_ms(&self) -> Option<i64> {
        let e = self.extended.current_timestamp_ms();
        let l = self.lighter.current_timestamp_ms();
        match (e, l) {
            (Some(e), Some(l)) => Some(e.min(l)),
            (Some(e), None) => Some(e),
            (None, Some(l)) => Some(l),
            (None, None) => None,
        }
    }

    /// Advance until both venues have committed a record on or after
    /// `target_ts_ms`, then stop. Useful for warm-up (`SpreadEngine`'s
    /// rolling window needs `min_warmup_samples` paired observations).
    /// Returns `false` if both venues exhausted before reaching the
    /// target.
    pub fn advance_until_ms(&self, target_ts_ms: i64) -> bool {
        loop {
            let e = self.extended.current_timestamp_ms().unwrap_or(i64::MIN);
            let l = self.lighter.current_timestamp_ms().unwrap_or(i64::MIN);
            if e >= target_ts_ms && l >= target_ts_ms {
                return true;
            }
            if self.advance().is_none() {
                return false;
            }
        }
    }

    pub fn reset(&self) {
        self.extended.reset();
        self.lighter.reset();
    }

    pub fn at_end(&self) -> bool {
        self.extended.at_end() && self.lighter.at_end()
    }

    /// Build a fresh `DualReplay` whose connectors share the parsed
    /// dumps (Arc) with `self` but reset their cursors to 0. The grid
    /// runner uses this to spawn many independent replays from a
    /// single load. Bot-strategy#166 Phase 1.
    pub fn clone_with_fresh_cursors(&self) -> Self {
        Self {
            extended: std::sync::Arc::new(self.extended.clone_with_fresh_cursor()),
            lighter: std::sync::Arc::new(self.lighter.clone_with_fresh_cursor()),
        }
    }

    #[cfg(test)]
    fn from_connectors(extended: ReplayConnector, lighter: ReplayConnector) -> Self {
        Self {
            extended: std::sync::Arc::new(extended),
            lighter: std::sync::Arc::new(lighter),
        }
    }
}

#[async_trait]
impl DexConnector for ReplayConnector {
    async fn start(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn restart(&self, _within_hours: i32) -> Result<(), DexError> {
        Ok(())
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        let current_snapshot = self
            .data
            .get(current_cursor)
            .ok_or_else(|| DexError::Other("Cursor out of bounds".to_string()))?;

        let symbol_data = current_snapshot.prices.get(symbol).ok_or_else(|| {
            DexError::Other(format!(
                "Symbol '{}' not found in this data entry at cursor {}",
                symbol, current_cursor
            ))
        })?;

        let price = test_price.unwrap_or(symbol_data.price);

        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price,
            min_tick: None,
            min_order: None,
            size_decimals: None,
            volume: None,
            num_trades: None,
            open_interest: None,
            funding_rate: Some(symbol_data.funding_rate),
            oracle_price: Some(symbol_data.price),
            // Prefer the per-symbol `exchange_ts` (the DEX-side tick second
            // the live bot uses for bar bucket assignment). The top-level
            // `timestamp` is the bot's wall-clock write time and typically
            // runs ~1s ahead of `exchange_ts`, which shifts the final tick
            // of a bucket into the next bucket and drifts close prices
            // across the whole history. Fallback is for ancient dumps
            // missing the field. Originally this returned the cursor
            // index — a separate layer of the same bug. See
            // bot-strategy#27 comment 2026-04-16.
            exchange_ts: Some(
                symbol_data
                    .exchange_ts
                    .unwrap_or(current_snapshot.timestamp / 1000) as u64,
            ),
        })
    }

    async fn get_filled_orders(&self, _symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        Ok(FilledOrdersResponse { orders: vec![] })
    }

    async fn get_canceled_orders(&self, _symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        Ok(CanceledOrdersResponse { orders: vec![] })
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        Ok(OpenOrdersResponse { orders: vec![] })
    }

    async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        Ok(BalanceResponse {
            equity: Decimal::new(10_000, 0),
            balance: Decimal::new(10_000, 0),
            position_entry_price: None,
            position_sign: None,
        })
    }

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        Ok(CombinedBalanceResponse::default())
    }

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        Ok(Vec::new())
    }

    async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
        Ok(LastTradesResponse { trades: vec![] })
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        _depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        let current_snapshot = self
            .data
            .get(current_cursor)
            .ok_or_else(|| DexError::Other("Cursor out of bounds".to_string()))?;

        let symbol_data = current_snapshot.prices.get(symbol).ok_or_else(|| {
            DexError::Other(format!(
                "Symbol '{}' not found in this data entry at cursor {}",
                symbol, current_cursor
            ))
        })?;

        Ok(OrderBookSnapshot {
            bids: vec![OrderBookLevel {
                price: symbol_data.bid_price.unwrap_or(symbol_data.price),
                size: symbol_data.bid_size,
            }],
            asks: vec![OrderBookLevel {
                price: symbol_data.ask_price.unwrap_or(symbol_data.price),
                size: symbol_data.ask_size,
            }],
        })
    }

    async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_canceled_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
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
        _spread: Option<i64>,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        let snapshot = self
            .data
            .get(current_cursor)
            .ok_or_else(|| DexError::Other("Cursor out of bounds".to_string()))?;
        let symbol_data = snapshot
            .prices
            .get(symbol)
            .ok_or_else(|| DexError::Other(format!("Symbol '{}' not found", symbol)))?;

        // Fill at the appropriate side of the book (taker model):
        // buys fill at ask price, sells fill at bid price.
        let fill_price = match side {
            OrderSide::Long => symbol_data.ask_price.unwrap_or(symbol_data.price),
            OrderSide::Short => symbol_data.bid_price.unwrap_or(symbol_data.price),
        };

        log::info!(
            "[BACKTEST_FILL] symbol={}, side={:?}, size={}, price={} (limit={:?})",
            symbol,
            side,
            size,
            fill_price,
            price,
        );

        Ok(CreateOrderResponse {
            order_id: rand::random::<u64>().to_string(),
            exchange_order_id: None,
            ordered_price: fill_price,
            ordered_size: size,
            client_order_id: None,
        })
    }

    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        limit_px: Option<Decimal>,
        _order_style: TriggerOrderStyle,
        _slippage_bps: Option<u32>,
        _tpsl: TpSl,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        self.create_order(
            symbol,
            size,
            side,
            limit_px.or(Some(trigger_px)),
            None,
            false,
            None,
        )
        .await
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Ok(())
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn is_upcoming_maintenance(&self, _within_hours: i64) -> bool {
        false
    }

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError> {
        Ok(format!("signed:{}", message))
    }

    async fn sign_evm_65b_with_eip191(&self, message: &str) -> Result<String, DexError> {
        Ok(format!("signed_eip191:{}", message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_entry(timestamp_ms: i64, price: f64, symbol_exchange_ts: Option<i64>) -> DumpedDataEntry {
        let mut prices = HashMap::new();
        prices.insert(
            "BTC".to_string(),
            DumpedSymbolSnapshot {
                price: Decimal::from_f64(price).unwrap(),
                funding_rate: Decimal::ZERO,
                bid_price: None,
                ask_price: None,
                bid_size: Decimal::ZERO,
                ask_size: Decimal::ZERO,
                exchange_ts: symbol_exchange_ts,
            },
        );
        DumpedDataEntry {
            timestamp: timestamp_ms,
            prices,
        }
    }

    /// Regression test for bot-strategy#27 (2026-04-16): replay was returning
    /// the cursor index as `exchange_ts`, which the downstream `BarBuilder`
    /// then used as a wall-clock timestamp for bucket alignment. At the ~5s
    /// dump cadence that stretched every "1-minute" bar to ~5 minutes of
    /// real time and smoothed away the 2026-04-15 std collapse. `exchange_ts`
    /// must be the dump's real UNIX seconds for BT bar bucketing to match
    /// live.
    #[tokio::test]
    async fn ticker_exchange_ts_is_real_seconds_not_cursor_index() {
        // Two records 5s apart, both far from epoch so any "cursor index"
        // would be trivially distinguishable (cursor=0 vs timestamp≈1.78e9).
        let r = ReplayConnector::from_entries(vec![
            mk_entry(1_776_229_320_000, 71_000.0, None), // 2026-04-15 05:02:00 UTC
            mk_entry(1_776_229_325_000, 71_010.0, None),
        ]);

        let t0 = r.get_ticker("BTC", None).await.unwrap();
        assert_eq!(t0.exchange_ts, Some(1_776_229_320));
        assert_ne!(t0.exchange_ts, Some(0)); // not cursor index

        assert!(r.tick());
        let t1 = r.get_ticker("BTC", None).await.unwrap();
        assert_eq!(t1.exchange_ts, Some(1_776_229_325));
        // 5-second real delta, not 1-step cursor delta
        assert_eq!(
            t1.exchange_ts.unwrap() - t0.exchange_ts.unwrap(),
            5,
            "exchange_ts must advance by real elapsed seconds"
        );
    }

    /// Regression test for bot-strategy#27 (2026-04-16, follow-up): when the
    /// dump record carries a per-symbol `exchange_ts` (the DEX-side tick
    /// second the live bot itself uses for bucket assignment), the replay
    /// must surface that value — not the record's top-level `timestamp`,
    /// which is the bot's wall-clock write time and typically runs ~1s
    /// ahead. At bucket boundaries that 1s offset flips the final tick into
    /// the next bucket and drifts `close_a` / the OLS history.
    #[tokio::test]
    async fn ticker_prefers_per_symbol_exchange_ts_over_top_level_timestamp() {
        // Exactly the boundary case observed in 4/15 06:02 UTC live dump:
        // top-level write ts = xxx920119ms (would assign to next bucket);
        // per-symbol exchange_ts = xxx919 (correctly the last tick of the
        // closing bucket).
        let r = ReplayConnector::from_entries(vec![mk_entry(
            1_776_232_920_119,
            73_998.15,
            Some(1_776_232_919),
        )]);
        let t = r.get_ticker("BTC", None).await.unwrap();
        assert_eq!(
            t.exchange_ts,
            Some(1_776_232_919),
            "must use per-symbol exchange_ts, not top-level timestamp/1000",
        );
    }

    // ---- DualReplay tests (bot-strategy#166 Phase 1) ------------------

    fn mk_replay(timestamps_ms: &[i64]) -> ReplayConnector {
        let entries = timestamps_ms
            .iter()
            .map(|ts| mk_entry(*ts, 70_000.0, Some(ts / 1000)))
            .collect();
        ReplayConnector::from_entries(entries)
    }

    #[test]
    fn dual_replay_advances_older_venue_first() {
        // Extended ticks every 1s, Lighter every 5s — common case where one
        // venue has a faster cadence. Merge order should walk Extended four
        // times for every Lighter step.
        let ext = mk_replay(&[1_000, 2_000, 3_000, 4_000, 5_000]);
        let lt = mk_replay(&[1_000, 5_000]);
        let dual = DualReplay::from_connectors(ext, lt);

        // Initial state: both at index 0 (ts=1000). aligned_ts = min = 1000.
        assert_eq!(dual.aligned_timestamp_ms(), Some(1_000));

        // Tie-break favors Extended on equal next timestamps. The first
        // peek_next: ext_next=2000, lt_next=5000 -> Extended wins.
        assert_eq!(dual.advance(), Some(Venue::Extended));
        assert_eq!(dual.extended().current_timestamp_ms(), Some(2_000));
        assert_eq!(dual.lighter().current_timestamp_ms(), Some(1_000));

        // ext_next=3000, lt_next=5000 -> Extended.
        assert_eq!(dual.advance(), Some(Venue::Extended));
        // ext_next=4000, lt_next=5000 -> Extended.
        assert_eq!(dual.advance(), Some(Venue::Extended));
        // ext_next=5000, lt_next=5000 -> tie, Extended wins.
        assert_eq!(dual.advance(), Some(Venue::Extended));
        assert_eq!(dual.extended().current_timestamp_ms(), Some(5_000));

        // Now Extended is at end (cursor at last). lt is still at index 0.
        // ext peek_next = None, lt peek_next = 5000 -> Lighter advances.
        assert_eq!(dual.advance(), Some(Venue::Lighter));
        assert_eq!(dual.lighter().current_timestamp_ms(), Some(5_000));

        // Both exhausted.
        assert_eq!(dual.advance(), None);
        assert!(dual.at_end());
    }

    #[test]
    fn dual_replay_advance_until_ms() {
        let ext = mk_replay(&[1_000, 2_000, 3_000, 4_000]);
        let lt = mk_replay(&[1_500, 2_500, 3_500]);
        let dual = DualReplay::from_connectors(ext, lt);

        // Advance until both venues have committed >= 3000ms.
        assert!(dual.advance_until_ms(3_000));
        assert!(dual.extended().current_timestamp_ms().unwrap() >= 3_000);
        assert!(dual.lighter().current_timestamp_ms().unwrap() >= 3_000);
    }

    #[test]
    fn dual_replay_advance_until_ms_returns_false_on_exhaustion() {
        let ext = mk_replay(&[1_000, 2_000]);
        let lt = mk_replay(&[1_000, 2_000]);
        let dual = DualReplay::from_connectors(ext, lt);

        // Target is past the last record on both sides.
        assert!(!dual.advance_until_ms(10_000));
        assert!(dual.at_end());
    }

    #[test]
    fn dual_replay_aligned_timestamp_is_min() {
        // aligned_timestamp = min(ext_current, lt_current). The strategy can
        // only safely emit a SpreadEngine sample at or before this instant
        // because the slower-cadence venue hasn't reported newer data yet.
        let ext = mk_replay(&[1_000, 2_000, 3_000]);
        let lt = mk_replay(&[1_000, 5_000]);
        let dual = DualReplay::from_connectors(ext, lt);

        assert_eq!(dual.aligned_timestamp_ms(), Some(1_000));
        // Walk Extended forward; aligned should follow Lighter (the slower
        // venue) until Lighter ticks.
        dual.advance(); // ext->2000
        assert_eq!(dual.aligned_timestamp_ms(), Some(1_000));
        dual.advance(); // ext->3000 (last)
        assert_eq!(dual.aligned_timestamp_ms(), Some(1_000));
        dual.advance(); // lt->5000
        assert_eq!(dual.aligned_timestamp_ms(), Some(3_000)); // ext capped at 3000
    }

    #[test]
    fn dual_replay_independent_cursors_share_no_state() {
        let ext = mk_replay(&[1_000, 2_000, 3_000]);
        let lt = mk_replay(&[1_000, 2_000, 3_000]);
        let dual = DualReplay::from_connectors(ext, lt);

        // After advance(), Extended advances, Lighter does not.
        assert_eq!(dual.advance(), Some(Venue::Extended));
        assert_eq!(dual.extended().current_timestamp_ms(), Some(2_000));
        assert_eq!(dual.lighter().current_timestamp_ms(), Some(1_000));
    }

    #[test]
    fn dual_replay_reset_resets_both() {
        let ext = mk_replay(&[1_000, 2_000, 3_000]);
        let lt = mk_replay(&[1_000, 2_000]);
        let dual = DualReplay::from_connectors(ext, lt);
        dual.advance();
        dual.advance();
        dual.advance();
        assert!(dual.extended().current_timestamp_ms().unwrap() > 1_000);
        dual.reset();
        assert_eq!(dual.extended().current_timestamp_ms(), Some(1_000));
        assert_eq!(dual.lighter().current_timestamp_ms(), Some(1_000));
    }

    #[test]
    fn replay_peek_next_returns_none_at_last_record() {
        let r = mk_replay(&[1_000, 2_000]);
        assert_eq!(r.peek_next_timestamp_ms(), Some(2_000));
        r.tick();
        assert_eq!(r.peek_next_timestamp_ms(), None);
        assert!(r.at_end());
    }
}
