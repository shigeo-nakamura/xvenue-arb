//! Test fixtures shared between unit tests and (eventually) integration
//! tests. Compiled only under `#[cfg(test)]` so it never ends up in the
//! release binary.
//!
//! `ScriptedHub` is the deterministic [`VenueHub`] mock: feed it a
//! sequence of mid snapshots per venue, and the runner ticks through
//! them. Suitable for testing decision flow, book_ok suppression,
//! shutdown handling, and (in Phase 3) order-placement path once the
//! hub trait is extended with `place_order` / `get_position`.

#![cfg(test)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::live::{MidSnapshot, Venue, VenueHub};

/// Deterministic VenueHub: serves a scripted sequence of mid pairs per
/// venue. Each `read_mid` call pops the next ts/mid for the requested
/// venue. When a venue's stack runs down to its last entry, that entry
/// is held (the loop can keep ticking past the script's end).
pub struct ScriptedHub {
    ext: Mutex<Vec<MidSnapshot>>,
    lt: Mutex<Vec<MidSnapshot>>,
    /// Equity returned by `read_equity_usd` — same value per call so
    /// status-emitter tests assert PnL behavior against a known total
    /// without juggling per-call sequences.
    equity_ext: Decimal,
    equity_lt: Decimal,
}

impl ScriptedHub {
    /// Build with explicit per-venue sequences. Both inputs are consumed
    /// in order — passing `vec![ms(1000, 2000.0), ms(2000, 2001.0)]`
    /// returns the 1000-tagged snapshot first, then the 2000-tagged one.
    pub fn new(ext: Vec<MidSnapshot>, lt: Vec<MidSnapshot>) -> Self {
        let mut ext = ext;
        ext.reverse();
        let mut lt = lt;
        lt.reverse();
        Self {
            ext: Mutex::new(ext),
            lt: Mutex::new(lt),
            equity_ext: dec!(500),
            equity_lt: dec!(500),
        }
    }

    pub fn with_equity(mut self, ext: Decimal, lt: Decimal) -> Self {
        self.equity_ext = ext;
        self.equity_lt = lt;
        self
    }

    /// Convenience: pop the next entry, or repeat the last one once the
    /// sequence is exhausted. Lets long-running loops see a stable price
    /// instead of an empty book after the script ends.
    fn pop_or_last(stack: &Mutex<Vec<MidSnapshot>>) -> MidSnapshot {
        let mut s = stack.lock().unwrap();
        if s.len() > 1 {
            s.pop().unwrap()
        } else {
            s.last().cloned().unwrap_or(MidSnapshot {
                ts_ms: 0,
                mid: dec!(1),
                book_ok: true,
            })
        }
    }
}

#[async_trait]
impl VenueHub for ScriptedHub {
    async fn read_mid(&self, venue: Venue) -> Result<MidSnapshot> {
        Ok(match venue {
            Venue::Extended => Self::pop_or_last(&self.ext),
            Venue::Lighter => Self::pop_or_last(&self.lt),
        })
    }

    async fn read_equity_usd(&self, venue: Venue) -> Result<Option<Decimal>> {
        Ok(Some(match venue {
            Venue::Extended => self.equity_ext,
            Venue::Lighter => self.equity_lt,
        }))
    }
}

/// VenueHub mock that fails the first `fail_n_per_venue` calls per
/// venue with `Err`, then delegates to a wrapped [`ScriptedHub`].
/// Used to exercise the WS warm-up suppression path (bot-strategy#248).
pub struct WarmupHub {
    inner: ScriptedHub,
    ext_remaining: AtomicUsize,
    lt_remaining: AtomicUsize,
}

impl WarmupHub {
    pub fn new(inner: ScriptedHub, fail_n_per_venue: usize) -> Self {
        Self {
            inner,
            ext_remaining: AtomicUsize::new(fail_n_per_venue),
            lt_remaining: AtomicUsize::new(fail_n_per_venue),
        }
    }

    fn consume_or_pass(counter: &AtomicUsize) -> bool {
        // Returns true when this call should fail (warm-up window),
        // false once the counter has been drained and we should
        // delegate to the inner hub.
        loop {
            let cur = counter.load(Ordering::Relaxed);
            if cur == 0 {
                return false;
            }
            if counter
                .compare_exchange(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }
}

#[async_trait]
impl VenueHub for WarmupHub {
    async fn read_mid(&self, venue: Venue) -> Result<MidSnapshot> {
        let counter = match venue {
            Venue::Extended => &self.ext_remaining,
            Venue::Lighter => &self.lt_remaining,
        };
        if Self::consume_or_pass(counter) {
            return Err(anyhow!(
                "get_order_book({:?}, 1): Other(\"order book snapshot unavailable (no recent update)\")",
                venue
            ));
        }
        self.inner.read_mid(venue).await
    }

    async fn read_equity_usd(&self, venue: Venue) -> Result<Option<Decimal>> {
        self.inner.read_equity_usd(venue).await
    }
}

/// Helper for building MidSnapshot values inline in tests.
pub fn mid(ts_ms: u64, value: f64) -> MidSnapshot {
    MidSnapshot {
        ts_ms,
        mid: Decimal::from_f64_retain(value).expect("non-finite mid in test"),
        book_ok: true,
    }
}

/// Like [`mid`] but with `book_ok = false` (one-sided book).
pub fn stale_mid(ts_ms: u64, value: f64) -> MidSnapshot {
    let mut m = mid(ts_ms, value);
    m.book_ok = false;
    m
}
