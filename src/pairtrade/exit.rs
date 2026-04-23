//! Exit-decision helpers extracted from the monolithic pairtrade module.

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

use super::config::{PairParams, PairTradeConfig};
use super::state::{PairState, Position, PositionDirection};
use super::market::SymbolSnapshot;

pub(super) fn exit_reason(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    z: f64,
    std: f64,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
    equity_base: f64,
    now_ts: i64,
) -> Option<&'static str> {
    let pos = state.position.as_ref()?;
    if z.abs() >= pp.stop_loss_z {
        return Some("stop_loss_z");
    }
    if now_ts.saturating_sub(pos.entered_ts) >= pp.force_close_secs as i64 {
        return Some("force_close");
    }
    if pp.exit_z > 0.0 && z.abs() <= pp.exit_z {
        return Some("exit_z");
    }
    let pnl = compute_pnl(pos, p1.price, p2.price);
    if let Some(pnl) = pnl {
        let risk_budget = equity_base * cfg.risk_pct_per_trade;
        if let Some(target) = Decimal::from_f64(risk_budget) {
            if target > Decimal::ZERO {
                if pp.max_loss_r_mult > 0.0 {
                    let loss_mult = Decimal::from_f64(pp.max_loss_r_mult).unwrap_or(Decimal::ONE);
                    let max_loss = -target * loss_mult;
                    if pnl <= max_loss {
                        return Some("max_loss_r");
                    }
                }
                if pnl >= target {
                    return Some("risk_budget");
                }
            }
        }
    }
    if std > 1e-9 {
        if let Some(pnl) = pnl {
            if pnl > Decimal::ZERO {
                let half_life_hours = state.half_life_hours;
                if half_life_hours.is_finite() && half_life_hours > 0.0 {
                    let elapsed_secs = now_ts.saturating_sub(pos.entered_ts).max(0) as f64;
                    let remaining_secs = (pp.force_close_secs as f64) - elapsed_secs;
                    if remaining_secs > 0.0 {
                        let half_life_secs = half_life_hours * 3600.0;
                        let k = (2.0_f64).ln() / half_life_secs;
                        let decay = (-k * remaining_secs).exp();
                        let expected_improvement = z.abs() * (1.0 - decay);
                        let total_cost_bps = cfg.fee_bps * 2.0 + cfg.slippage_cost_bps() * 2.0;
                        let cost_ratio = total_cost_bps / 10_000.0;
                        let cost_in_sigma = cost_ratio / std;
                        if expected_improvement <= cost_in_sigma {
                            return Some("expected_value");
                        }
                    }
                }
            }
        }
    }
    None
}

pub(super) fn compute_pnl(
    pos: &Position,
    exit_price_a: Decimal,
    exit_price_b: Decimal,
) -> Option<Decimal> {
    let entry_price_a = pos.entry_price_a?;
    let entry_price_b = pos.entry_price_b?;
    let entry_size_a = pos.entry_size_a?;
    let entry_size_b = pos.entry_size_b?;
    let (pnl_a, pnl_b) = match pos.direction {
        PositionDirection::LongSpread => (
            (exit_price_a - entry_price_a) * entry_size_a,
            (entry_price_b - exit_price_b) * entry_size_b,
        ),
        PositionDirection::ShortSpread => (
            (entry_price_a - exit_price_a) * entry_size_a,
            (exit_price_b - entry_price_b) * entry_size_b,
        ),
    };
    Some(pnl_a + pnl_b)
}
