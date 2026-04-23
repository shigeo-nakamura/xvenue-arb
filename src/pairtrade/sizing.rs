//! Position-sizing helpers extracted from the monolithic pairtrade module.

use anyhow::{anyhow, Result};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

use super::config::PairTradeConfig;
use super::market::SymbolSnapshot;

pub(super) fn hedged_sizes(
    cfg: &PairTradeConfig,
    equity: f64,
    beta: f64,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
) -> Result<(Decimal, Decimal)> {
    // `equity` is expected to be pre-floored by the caller with the
    // per-instance equity_usd_fallback so each variant sizes against
    // its own sub-account capacity. See StrategyInstance.equity_usd_fallback.
    let total_risk = equity * cfg.risk_pct_per_trade * cfg.max_leverage;
    let leg_notional = (total_risk / 2.0).max(10.0);
    let notional = Decimal::from_f64(leg_notional).ok_or_else(|| anyhow!("invalid notional"))?;

    let qty_a = if p1.price == Decimal::ZERO {
        Decimal::ZERO
    } else {
        let mut qty = notional / p1.price;
        if let Some(decimals) = p1.size_decimals {
            qty = qty.round_dp(decimals);
        }
        if let Some(min_ord) = p1.min_order {
            if qty > Decimal::ZERO && qty < min_ord {
                qty = min_ord;
            }
        }
        qty
    };
    // Compute qty_b from the actual notional of leg A (after min_order adjustment)
    // so that the hedge ratio matches beta: notional_b = notional_a * beta
    let actual_notional_a = qty_a * p1.price;
    let qty_b = if p2.price == Decimal::ZERO {
        Decimal::ZERO
    } else {
        let beta_dec = Decimal::from_f64(beta.abs()).unwrap_or(Decimal::ONE);
        let mut qty = (actual_notional_a * beta_dec) / p2.price;
        if let Some(decimals) = p2.size_decimals {
            qty = qty.round_dp(decimals);
        }
        if let Some(min_ord) = p2.min_order {
            if qty > Decimal::ZERO && qty < min_ord {
                qty = min_ord;
            }
        }
        qty
    };
    Ok((qty_a, qty_b))
}
