//! Position sizing for the cross-venue arb (DESIGN.md §4.4).
//!
//! `notional_usd = (extended_equity + lighter_equity) * trade_size_pct`
//! clamped to `[min_notional_usd, max_notional_usd]`. Both legs use
//! the same notional for delta-neutrality; Lighter's finer tick lets
//! it match the Extended leg exactly so we don't have a separate
//! per-venue notional.
//!
//! The `max_notional_usd` clamp is the safety cap from
//! bot-strategy#244 D-6 — equity-driven sizing must NEVER produce a
//! leg above this regardless of how much equity grows. Mirrors
//! pairtrade's `max_notional_usd_per_leg` enforcement.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Outcome of sizing one entry pair. Drops below `min_notional_usd`
/// downgrade to `BelowMin` rather than rounding up — taking a tiny
/// position is worse than skipping the cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeOutcome {
    /// Use this notional for both legs.
    Use(Decimal),
    /// Equity * pct fell below `min_notional_usd`. Caller must skip
    /// the entry (no order flows on either leg).
    BelowMin,
    /// One or both equities were missing/non-positive — refuse to
    /// size. Caller should treat as "wait for next tick".
    EquityUnavailable,
}

/// Compute the per-leg notional. `total_equity_usd = extended + lighter`.
///
/// `trade_size_pct` is the fraction of equity to deploy per cycle,
/// e.g. 0.05 = 5%. The min/max bounds come straight from the YAML
/// (`XvenueConfig::min_notional_usd` / `max_notional_usd`).
pub fn compute_notional_usd(
    extended_equity: Option<Decimal>,
    lighter_equity: Option<Decimal>,
    trade_size_pct: f64,
    min_notional_usd: f64,
    max_notional_usd: f64,
) -> SizeOutcome {
    let (Some(ext), Some(lt)) = (extended_equity, lighter_equity) else {
        return SizeOutcome::EquityUnavailable;
    };
    if ext <= Decimal::ZERO || lt <= Decimal::ZERO {
        return SizeOutcome::EquityUnavailable;
    }
    let total = ext + lt;
    let Some(pct) = Decimal::from_f64_retain(trade_size_pct) else {
        return SizeOutcome::EquityUnavailable;
    };
    // Round to 8 dp so float-derived `pct` doesn't leak its
    // representation noise (e.g. 0.05 → 0.05000000…0277) into the
    // notional. 8 dp is well below penny precision and matches the
    // qty rounding in `notional_to_qty`.
    let raw = (total * pct).round_dp(8);
    let min_d = Decimal::from_f64_retain(min_notional_usd).unwrap_or(Decimal::ZERO);
    let max_d = Decimal::from_f64_retain(max_notional_usd).unwrap_or_else(|| dec!(1_000_000));
    if raw < min_d {
        return SizeOutcome::BelowMin;
    }
    let clamped = if raw > max_d { max_d } else { raw };
    SizeOutcome::Use(clamped)
}

/// Convert a notional in USD to a per-venue qty given the venue mid.
/// Both legs share the same notional in delta-neutral mode, so the
/// caller invokes this twice (once per venue mid). Mid <= 0 returns
/// `None` so the caller can skip the cycle without a panic.
pub fn notional_to_qty(notional_usd: Decimal, mid: Decimal) -> Option<Decimal> {
    if mid <= Decimal::ZERO {
        return None;
    }
    Some((notional_usd / mid).round_dp(8))
}

/// Convenience: f64 view of the SizeOutcome notional (or 0 / NaN-safe).
/// Used by status-emitter / log paths that already work in f64.
pub fn outcome_to_f64(o: SizeOutcome) -> Option<f64> {
    match o {
        SizeOutcome::Use(v) => v.to_f64(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_returns_pct_of_equity() {
        let r = compute_notional_usd(Some(dec!(500)), Some(dec!(500)), 0.05, 20.0, 1000.0);
        assert_eq!(r, SizeOutcome::Use(dec!(50)));
    }

    #[test]
    fn caps_at_max_notional_when_equity_grows() {
        // 5% of $40k = $2000 raw, but max is $1000 — must clamp.
        let r = compute_notional_usd(Some(dec!(20_000)), Some(dec!(20_000)), 0.05, 20.0, 1_000.0);
        assert_eq!(r, SizeOutcome::Use(dec!(1_000)));
    }

    #[test]
    fn small_equity_falls_below_min_notional() {
        // 5% of $200 = $10 raw, but min is $20 — must skip.
        let r = compute_notional_usd(Some(dec!(100)), Some(dec!(100)), 0.05, 20.0, 1000.0);
        assert_eq!(r, SizeOutcome::BelowMin);
    }

    #[test]
    fn missing_equity_blocks_sizing() {
        let r1 = compute_notional_usd(None, Some(dec!(500)), 0.05, 20.0, 1000.0);
        let r2 = compute_notional_usd(Some(dec!(500)), None, 0.05, 20.0, 1000.0);
        let r3 = compute_notional_usd(None, None, 0.05, 20.0, 1000.0);
        assert_eq!(r1, SizeOutcome::EquityUnavailable);
        assert_eq!(r2, SizeOutcome::EquityUnavailable);
        assert_eq!(r3, SizeOutcome::EquityUnavailable);
    }

    #[test]
    fn zero_or_negative_equity_blocks_sizing() {
        let r = compute_notional_usd(Some(Decimal::ZERO), Some(dec!(500)), 0.05, 20.0, 1000.0);
        assert_eq!(r, SizeOutcome::EquityUnavailable);
        let r = compute_notional_usd(Some(dec!(-1)), Some(dec!(500)), 0.05, 20.0, 1000.0);
        assert_eq!(r, SizeOutcome::EquityUnavailable);
    }

    #[test]
    fn cap_holds_for_unrealistically_large_equity() {
        // The whole point of the D-6 cap: even if equity is at the
        // moon, leg notional must not exceed max.
        let r = compute_notional_usd(
            Some(dec!(10_000_000)),
            Some(dec!(10_000_000)),
            0.05,
            20.0,
            1_000.0,
        );
        assert_eq!(r, SizeOutcome::Use(dec!(1_000)));
    }

    #[test]
    fn notional_to_qty_basic_round_trip() {
        let qty = notional_to_qty(dec!(100), dec!(2_000)).unwrap();
        assert_eq!(qty, dec!(0.05));
    }

    #[test]
    fn notional_to_qty_rejects_non_positive_mid() {
        assert!(notional_to_qty(dec!(100), Decimal::ZERO).is_none());
        assert!(notional_to_qty(dec!(100), dec!(-1)).is_none());
    }

    #[test]
    fn outcome_to_f64_handles_each_variant() {
        assert_eq!(outcome_to_f64(SizeOutcome::Use(dec!(123.45))), Some(123.45));
        assert_eq!(outcome_to_f64(SizeOutcome::BelowMin), None);
        assert_eq!(outcome_to_f64(SizeOutcome::EquityUnavailable), None);
    }
}
