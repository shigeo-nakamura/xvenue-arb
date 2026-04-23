//! OHLC bar aggregation extracted from the monolithic pairtrade module.
//!
//! `BarBuilder` accumulates ticks into wall-clock-aligned buckets and emits
//! a deterministic close price per bucket so that multiple bots observing the
//! same WS feed converge on identical bars. See pairtrade#4.

use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub(super) struct BarBuilder {
    window_secs: u64,
    start_ts: Option<i64>,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    /// Exchange timestamp of the tick currently used as `close`. Used to keep
    /// the bar close monotonic with respect to the exchange clock so that two
    /// bots observing the same WS feed converge on the same close price for
    /// the same bucket. Updates from older ts are ignored even if they arrive
    /// later in wall-clock time, and the open price is locked to the
    /// earliest tick of the bucket. See pairtrade#4.
    close_ts: Option<i64>,
    open_ts: Option<i64>,
}

impl BarBuilder {
    pub(super) fn new(window_secs: u64) -> Self {
        Self {
            window_secs,
            start_ts: None,
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            close_ts: None,
            open_ts: None,
        }
    }

    /// Align a timestamp down to the wall-clock bucket boundary.
    ///
    /// Buckets are anchored to the Unix epoch (`floor(ts / window) * window`),
    /// so all bots observing the same stream produce identical bucket IDs
    /// regardless of their own startup phase. This is required for multi-bot
    /// A/B fairness: without this, each process anchors its first bar to its
    /// own first tick, causing beta/mean/std/z to diverge across bots even
    /// though they share the same price feed. See pairtrade#4.
    fn bucket_start(&self, ts: i64) -> i64 {
        let w = self.window_secs as i64;
        if w <= 0 {
            return ts;
        }
        ts - ts.rem_euclid(w)
    }

    pub(super) fn push(&mut self, ts: i64, price: Decimal) -> Option<(Decimal, i64)> {
        let current_bucket = self.bucket_start(ts);
        match self.start_ts {
            None => {
                self.start_ts = Some(current_bucket);
                self.open = price;
                self.high = price;
                self.low = price;
                self.close = price;
                self.close_ts = Some(ts);
                self.open_ts = Some(ts);
                None
            }
            Some(start) => {
                if current_bucket > start {
                    let prev_close = self.close;
                    let bar_close_ts = start.saturating_add(self.window_secs as i64);
                    self.start_ts = Some(current_bucket);
                    self.open = price;
                    self.high = price;
                    self.low = price;
                    self.close = price;
                    self.close_ts = Some(ts);
                    self.open_ts = Some(ts);
                    Some((prev_close, bar_close_ts))
                } else {
                    // Within the same bucket: pick the tick with the largest
                    // exchange ts as the canonical close (deterministic across
                    // processes); fall back to last-write-wins if ts info is
                    // missing. The open price is locked to the earliest ts.
                    if price > self.high {
                        self.high = price;
                    }
                    if price < self.low || self.low.is_zero() {
                        self.low = price;
                    }
                    match self.close_ts {
                        Some(prev_close_ts) if ts < prev_close_ts => {
                            // older tick — leave close unchanged
                        }
                        _ => {
                            self.close = price;
                            self.close_ts = Some(ts);
                        }
                    }
                    match self.open_ts {
                        Some(prev_open_ts) if ts >= prev_open_ts => {
                            // newer tick — open already locked to earlier ts
                        }
                        _ => {
                            self.open = price;
                            self.open_ts = Some(ts);
                        }
                    }
                    None
                }
            }
        }
    }
}
