//! Order pricing/quantization helpers extracted from the monolithic
//! pairtrade module.

use std::collections::HashMap;

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

use super::market::SymbolSnapshot;
use super::util::{quantize_size_by_step, quantize_size_by_step_ceiling};

pub(super) fn apply_slippage(
    slippage_bps: i32,
    price: Option<Decimal>,
    side: dex_connector::OrderSide,
) -> Option<Decimal> {
    let p = price?;
    if slippage_bps == 0 {
        return Some(p);
    }
    let factor =
        Decimal::from_f64((slippage_bps.abs() as f64) / 10_000.0).unwrap_or(Decimal::ZERO);
    let passive = slippage_bps < 0;
    match side {
        dex_connector::OrderSide::Long => {
            if passive {
                Some(p * (Decimal::ONE - factor))
            } else {
                Some(p * (Decimal::ONE + factor))
            }
        }
        dex_connector::OrderSide::Short => {
            if passive {
                Some(p * (Decimal::ONE + factor))
            } else {
                Some(p * (Decimal::ONE - factor))
            }
        }
    }
}

pub(super) fn quantize_order_size(
    symbol: &str,
    size: Decimal,
    prices: &HashMap<String, SymbolSnapshot>,
) -> Decimal {
    if size <= Decimal::ZERO {
        return size;
    }
    if let Some(snapshot) = prices.get(symbol) {
        let min_order = snapshot.min_order.clone();
        let step = min_order
            .clone()
            .or_else(|| snapshot.size_decimals.map(|d| Decimal::new(1, d.min(28))));
        if let Some(step) = step {
            let quantized = quantize_size_by_step(size, step, min_order);
            if quantized > Decimal::ZERO {
                return quantized;
            }
        }
    }
    size
}

pub(super) fn quantize_order_size_exit(
    symbol: &str,
    size: Decimal,
    prices: &HashMap<String, SymbolSnapshot>,
) -> Decimal {
    if size <= Decimal::ZERO {
        return size;
    }
    if let Some(snapshot) = prices.get(symbol) {
        let min_order = snapshot.min_order.clone();
        let step = min_order
            .clone()
            .or_else(|| snapshot.size_decimals.map(|d| Decimal::new(1, d.min(28))));
        if let Some(step) = step {
            let quantized = quantize_size_by_step_ceiling(size, step, min_order);
            if quantized > Decimal::ZERO {
                return quantized;
            }
        }
    }
    size
}

pub(super) fn quantize_order_size_close(
    symbol: &str,
    size: Decimal,
    prices: &HashMap<String, SymbolSnapshot>,
) -> Decimal {
    if size <= Decimal::ZERO {
        return size;
    }
    if let Some(snapshot) = prices.get(symbol) {
        let step = snapshot
            .size_decimals
            .map(|d| Decimal::new(1, d.min(28)))
            .or_else(|| snapshot.min_order.clone());
        if let Some(step) = step {
            let quantized = quantize_size_by_step_ceiling(size, step, None);
            if quantized > Decimal::ZERO {
                return quantized;
            }
        }
    }
    size
}
