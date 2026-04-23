//! Market data snapshot type and small per-snapshot helpers.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SymbolSnapshot {
    pub(super) price: Decimal,
    pub(super) funding_rate: Decimal,
    pub(super) bid_price: Option<Decimal>,
    pub(super) ask_price: Option<Decimal>,
    pub(super) bid_size: Decimal,
    pub(super) ask_size: Decimal,
    pub(super) min_order: Option<Decimal>,
    pub(super) min_tick: Option<Decimal>,
    pub(super) size_decimals: Option<u32>,
    /// Exchange-side timestamp (Unix seconds) for the most recent price update
    /// from the connector. When `Some`, all bots observing the same feed see
    /// identical values for the same update — used to align bar buckets across
    /// processes (pairtrade#4).
    #[serde(default)]
    pub(super) exchange_ts: Option<i64>,
}

pub(super) fn net_funding_for_direction(
    z: f64,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
) -> f64 {
    if z > 0.0 {
        // plan to short base (p1) and long quote (p2)
        (p2.funding_rate - p1.funding_rate).to_f64().unwrap_or(0.0) / 24.0
    } else {
        // plan to long base (p1) and short quote (p2)
        (p1.funding_rate - p2.funding_rate).to_f64().unwrap_or(0.0) / 24.0
    }
}

pub(super) fn liquidity_score(p1: &SymbolSnapshot, p2: &SymbolSnapshot) -> f64 {
    let s1 = p1.bid_size.min(p1.ask_size).to_f64().unwrap_or(0.0);
    let s2 = p2.bid_size.min(p2.ask_size).to_f64().unwrap_or(0.0);
    (s1 + s2).max(0.0)
}
