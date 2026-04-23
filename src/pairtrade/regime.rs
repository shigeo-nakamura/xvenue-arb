//! Regime filter: block entries during high-volatility or strong-trend
//! periods of a reference asset (typically BTC).

use std::collections::VecDeque;

use super::stats::PriceSample;

#[derive(Debug, Clone, Copy)]
pub(super) struct RegimeState {
    pub(super) realized_vol: f64,
    pub(super) trend_strength: f64,
}

/// Compute regime indicators from a symbol's price history.
///
/// `realized_vol` – standard deviation of per-bar log returns over the
/// last `vol_window` bars.
///
/// `trend_strength` – |slope / std| of log prices over the last
/// `trend_window` bars (same normalisation as `spread_slope_sigma`).
///
/// Returns `None` when there is not enough data.
pub(super) fn compute_regime(
    history: &VecDeque<PriceSample>,
    vol_window: usize,
    trend_window: usize,
) -> Option<RegimeState> {
    let need = vol_window.max(trend_window) + 1;
    if history.len() < need {
        return None;
    }

    // --- realized vol (std of log returns) ---
    let vol = {
        let start = history.len() - vol_window - 1;
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        for i in 1..=vol_window {
            let r = history[start + i].log_price - history[start + i - 1].log_price;
            sum += r;
            sum_sq += r * r;
        }
        let n = vol_window as f64;
        let mean = sum / n;
        let var = (sum_sq / n) - mean * mean;
        var.max(0.0).sqrt()
    };

    // --- trend strength (|slope / std| of log prices) ---
    let trend = {
        let start = history.len() - trend_window;
        let n = trend_window as f64;
        let mean_i = (n - 1.0) / 2.0;
        let mut mean_p = 0.0;
        for j in 0..trend_window {
            mean_p += history[start + j].log_price;
        }
        mean_p /= n;
        let mut cov = 0.0;
        let mut var_i = 0.0;
        let mut var_p = 0.0;
        for j in 0..trend_window {
            let di = j as f64 - mean_i;
            let dp = history[start + j].log_price - mean_p;
            cov += di * dp;
            var_i += di * di;
            var_p += dp * dp;
        }
        let std_p = (var_p / n).max(0.0).sqrt();
        let slope = if var_i.abs() < 1e-15 { 0.0 } else { cov / var_i };
        if std_p < 1e-9 {
            0.0
        } else {
            (slope / std_p).abs()
        }
    };

    Some(RegimeState {
        realized_vol: vol,
        trend_strength: trend,
    })
}

/// Returns `true` when the current regime allows entry.
/// A threshold of 0.0 disables that dimension.
pub(super) fn regime_allows_entry(
    regime: Option<RegimeState>,
    vol_max: f64,
    trend_max: f64,
) -> bool {
    if vol_max <= 0.0 && trend_max <= 0.0 {
        return true; // filter disabled
    }
    let Some(r) = regime else {
        return true; // no data → allow
    };
    if vol_max > 0.0 && r.realized_vol > vol_max {
        return false;
    }
    if trend_max > 0.0 && r.trend_strength > trend_max {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_history(log_prices: &[f64]) -> VecDeque<PriceSample> {
        log_prices
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64 * 60,
            })
            .collect()
    }

    #[test]
    fn flat_market_low_vol_and_trend() {
        // 100 bars of constant price
        let h = make_history(&vec![10.0; 100]);
        let r = compute_regime(&h, 60, 60).unwrap();
        assert!(r.realized_vol < 1e-9);
        assert!(r.trend_strength < 1e-9);
        assert!(regime_allows_entry(Some(r), 0.001, 0.5));
    }

    #[test]
    fn trending_market_detected() {
        // Steady uptrend: 0.001 per bar over 100 bars
        let prices: Vec<f64> = (0..100).map(|i| 10.0 + 0.001 * i as f64).collect();
        let h = make_history(&prices);
        let r = compute_regime(&h, 60, 60).unwrap();
        // slope/std for a linear ramp ≈ 0.058 (sqrt(3)/n normalisation)
        assert!(
            r.trend_strength > 0.01,
            "trend_strength {} should be > 0.01 for a steady trend",
            r.trend_strength
        );
    }

    #[test]
    fn volatile_market_detected() {
        // Alternating up/down: high vol, zero trend
        let prices: Vec<f64> = (0..100)
            .map(|i| 10.0 + if i % 2 == 0 { 0.01 } else { -0.01 })
            .collect();
        let h = make_history(&prices);
        let r = compute_regime(&h, 60, 60).unwrap();
        assert!(
            r.realized_vol > 0.005,
            "realized_vol {} should be > 0.005",
            r.realized_vol
        );
    }

    #[test]
    fn disabled_when_thresholds_zero() {
        assert!(regime_allows_entry(None, 0.0, 0.0));
        let r = RegimeState {
            realized_vol: 999.0,
            trend_strength: 999.0,
        };
        assert!(regime_allows_entry(Some(r), 0.0, 0.0));
    }

    #[test]
    fn blocks_when_vol_exceeds_threshold() {
        let r = RegimeState {
            realized_vol: 0.005,
            trend_strength: 0.01,
        };
        assert!(!regime_allows_entry(Some(r), 0.003, 0.0));
        assert!(regime_allows_entry(Some(r), 0.01, 0.0));
    }

    #[test]
    fn blocks_when_trend_exceeds_threshold() {
        let r = RegimeState {
            realized_vol: 0.001,
            trend_strength: 0.8,
        };
        assert!(!regime_allows_entry(Some(r), 0.0, 0.5));
        assert!(regime_allows_entry(Some(r), 0.0, 1.0));
    }
}
