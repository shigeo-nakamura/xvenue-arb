//! Entry-decision helpers extracted from the monolithic pairtrade module.
//! Pure functions over config, params, and per-pair state.

use std::collections::VecDeque;

use super::config::{PairParams, PairTradeConfig};
use super::state::PairState;
use super::stats::spread_slope_sigma;
use super::util::tail_std;

fn median_of(values: &VecDeque<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut buf: Vec<f64> = values.iter().copied().collect();
    buf.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = buf.len() / 2;
    if buf.len() % 2 == 0 {
        Some((buf[mid - 1] + buf[mid]) / 2.0)
    } else {
        Some(buf[mid])
    }
}

/// Returns `true` when the std-collapse guard should block a new entry.
/// The z-score denominator has fallen far below its own recent median, so
/// the current |z| is no longer a meaningful mean-reversion signal
/// (bot-strategy#62).
pub(super) fn std_collapsed(
    std: f64,
    std_history: &VecDeque<f64>,
    window_bars: usize,
    min_ratio: f64,
) -> bool {
    if window_bars == 0 || min_ratio <= 0.0 {
        return false;
    }
    let min_samples = (window_bars / 2).max(2);
    if std_history.len() < min_samples {
        return false;
    }
    let Some(median) = median_of(std_history) else {
        return false;
    };
    if median <= 1e-9 {
        return false;
    }
    std / median < min_ratio
}

/// Lower bound for any dynamic entry-z scaling factor (vol or funding).
/// Prevents the threshold from collapsing on noisy single-bar inputs.
const ENTRY_Z_SCALE_MIN: f64 = 0.5;
/// Upper bound for any dynamic entry-z scaling factor.
const ENTRY_Z_SCALE_MAX: f64 = 2.0;
/// Discount applied to the entry-z threshold when net funding is positive,
/// nudging the strategy to take small carry-favorable trades it would
/// otherwise skip. The continuous `funding_entry_z_scale` filter (PairParams)
/// is layered on top.
const FUNDING_CARRY_ENTRY_DISCOUNT: f64 = 0.9;

pub(super) fn entry_z_for_pair(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    vol_median: f64,
) -> f64 {
    let entry_vol_len =
        ((pp.entry_vol_lookback_hours * 3600) / cfg.trading_period_secs).max(1) as usize;
    let vol_pair = tail_std(&state.spread_history, entry_vol_len).unwrap_or(1.0);
    let alpha = (vol_pair / vol_median).clamp(ENTRY_Z_SCALE_MIN, ENTRY_Z_SCALE_MAX);
    let z = pp.entry_z_base * alpha;
    z.clamp(pp.entry_z_min, pp.entry_z_max)
}

pub(super) fn should_enter(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    z: f64,
    std: f64,
    net_funding: f64,
    now_ts: i64,
) -> bool {
    if let Some(last_exit_ts) = state.last_exit_ts {
        if now_ts.saturating_sub(last_exit_ts) < pp.cooldown_secs as i64 {
            return false;
        }
    }

    // --- Phase 2 filter: spread momentum block ---
    // Block entry when spread is moving fast (likely trending, not mean-reverting).
    // Disabled when entry_velocity_block_sigma_per_min == 0.0.
    if pp.entry_velocity_block_sigma_per_min > 0.0
        && state.last_velocity_sigma_per_min.abs() >= pp.entry_velocity_block_sigma_per_min
    {
        return false;
    }

    // --- Std collapse guard (bot-strategy#62) ---
    // z = (latest - mean) / std; when std collapses relative to its own recent
    // history the z-score stops being a meaningful mean-reversion signal.
    // In observe_only mode the guard logs but lets the entry through — lets
    // operators measure trigger frequency on live data without disturbing
    // the #41 A/B/C test window.
    if std_collapsed(
        std,
        &state.std_history,
        pp.std_collapse_window_bars,
        pp.std_collapse_min_ratio,
    ) {
        let median = median_of(&state.std_history).unwrap_or(0.0);
        let ratio = if median > 1e-9 { std / median } else { 0.0 };
        if pp.std_collapse_observe_only {
            log::warn!(
                "[STD_COLLAPSE_OBSERVE] z={:.2} std={:.6} median={:.6} ratio={:.4} threshold={:.4} (observe-only, entry allowed)",
                z,
                std,
                median,
                ratio,
                pp.std_collapse_min_ratio,
            );
        } else {
            log::warn!(
                "[STD_COLLAPSE_BLOCK] z={:.2} std={:.6} median={:.6} ratio={:.4} threshold={:.4}",
                z,
                std,
                median,
                ratio,
                pp.std_collapse_min_ratio,
            );
            return false;
        }
    }

    let mut entry_threshold = if net_funding > 0.0 {
        // prefer positive carry by easing the required entry slightly
        state.z_entry * FUNDING_CARRY_ENTRY_DISCOUNT
    } else {
        state.z_entry
    };

    // --- Phase 2 filter: funding rate continuous scaling ---
    // Scale entry_z based on funding magnitude (beyond the simple discount
    // above). funding_entry_z_scale > 0: entry_z *= 1.0 - scale * net_funding
    //   positive funding → lower threshold (easier entry)
    //   negative funding → higher threshold (harder entry)
    // Disabled when funding_entry_z_scale == 0.0.
    if pp.funding_entry_z_scale > 0.0 {
        let adjustment = 1.0 - pp.funding_entry_z_scale * net_funding;
        entry_threshold *= adjustment.clamp(ENTRY_Z_SCALE_MIN, ENTRY_Z_SCALE_MAX);
    }

    // --- Phase 2 filter: beta gap dynamic adjustment ---
    // Raise entry threshold when beta_s and beta_l diverge (hedge unreliable).
    // entry_z *= 1.0 + scale * beta_gap
    // Disabled when beta_gap_entry_z_scale == 0.0.
    if pp.beta_gap_entry_z_scale > 0.0 {
        entry_threshold *= 1.0 + pp.beta_gap_entry_z_scale * state.beta_gap;
    }

    // Avoid entering when the current z already triggers stop-loss exit.
    if z.abs() >= pp.stop_loss_z {
        return false;
    }
    // Spread trend filter: block entry if spread is trending
    if let Some(slope_sigma) = spread_slope_sigma(&state.spread_history, cfg.metrics_window) {
        if slope_sigma > pp.spread_trend_max_slope_sigma {
            return false;
        }
    }
    // Beta stability filter: block entry if beta_s and beta_l diverge
    if state.beta_gap > pp.beta_divergence_max {
        return false;
    }
    // Beta minimum filter: block entry if beta is too low (hedge leg too small)
    if pp.beta_min > 0.0 && state.beta < pp.beta_min {
        return false;
    }
    // Account for estimated cost (fees + slippage) in sigma units
    let total_cost_bps = cfg.fee_bps * 2.0 + cfg.slippage_cost_bps() * 2.0; // two legs
    let cost_ratio = total_cost_bps / 10_000.0;
    let cost_in_sigma = if std <= 1e-9 { 0.0 } else { cost_ratio / std };
    if z.abs() < entry_threshold {
        return false;
    }

    // Multi-timeframe z-score confluence filter.
    // All configured windows must show z in the same direction and above mtf_z_min.
    // Disabled when mtf_windows is empty or mtf_z_min == 0.0.
    if !pp.mtf_windows.is_empty() && pp.mtf_z_min > 0.0 {
        let primary_sign = z.signum();
        for &w in &pp.mtf_windows {
            if let Some(z_w) = state.z_score_for_window(w) {
                if z_w.signum() != primary_sign || z_w.abs() < pp.mtf_z_min {
                    return false;
                }
            }
            // Insufficient data for this window → skip (permissive)
        }
    }

    z.abs() >= entry_threshold + cost_in_sigma && net_funding >= cfg.net_funding_min_per_hour
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_history(values: &[f64]) -> VecDeque<f64> {
        values.iter().copied().collect()
    }

    #[test]
    fn std_collapsed_disabled_when_window_zero() {
        let h = make_history(&[1.0, 1.0, 1.0, 1.0]);
        assert!(!std_collapsed(0.001, &h, 0, 0.2));
    }

    #[test]
    fn std_collapsed_disabled_when_ratio_zero() {
        let h = make_history(&[1.0, 1.0, 1.0, 1.0]);
        assert!(!std_collapsed(0.001, &h, 30, 0.0));
    }

    #[test]
    fn std_collapsed_permissive_before_warmup() {
        // window=30 → min_samples=15; three samples is well under that
        let h = make_history(&[1.0, 1.0, 1.0]);
        assert!(!std_collapsed(0.001, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_blocks_when_current_far_below_median() {
        // Replicates bot-strategy#62: median ≈ 1.0, current = 0.0016 → ratio 0.0016
        let samples: Vec<f64> = vec![1.0; 30];
        let h = make_history(&samples);
        assert!(std_collapsed(0.0016, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_allows_when_current_near_median() {
        let samples: Vec<f64> = vec![1.0; 30];
        let h = make_history(&samples);
        assert!(!std_collapsed(0.9, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_boundary_inclusive_allows_equal_ratio() {
        // std / median == min_ratio → not blocked (strict less-than)
        let samples: Vec<f64> = vec![1.0; 30];
        let h = make_history(&samples);
        assert!(!std_collapsed(0.2, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_handles_zero_median() {
        let samples: Vec<f64> = vec![0.0; 30];
        let h = make_history(&samples);
        assert!(!std_collapsed(0.001, &h, 30, 0.2));
    }

    #[test]
    fn median_of_odd_and_even() {
        let odd = make_history(&[3.0, 1.0, 2.0]);
        assert_eq!(median_of(&odd), Some(2.0));
        let even = make_history(&[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(median_of(&even), Some(2.5));
        assert_eq!(median_of(&VecDeque::<f64>::new()), None);
    }
}
