//! Per-pair eligibility evaluation extracted from the monolithic pairtrade module.

use std::collections::{HashMap, VecDeque};

use super::config::{PairSpec, PairTradeConfig, WarmStartMode};
use super::stats::{regression_beta, tail_samples, PriceSample};
use super::util::half_life_and_p;

/// Weight on the short-window beta when blending into `beta_eff`. The
/// `BETA_EFF_LONG_WEIGHT` companion weights the long-window beta. Kept as
/// two separate consts (not derived as `1 - SHORT`) to avoid IEEE-754 drift
/// from rebalancing the literal `0.3` into `1.0 - 0.7`.
const BETA_EFF_SHORT_WEIGHT: f64 = 0.7;
const BETA_EFF_LONG_WEIGHT: f64 = 0.3;
/// Weights on the inverse-p-value and half-life terms in the eligibility
/// ranking score. Kept as two literals for the same reason as the beta
/// weights above.
const SCORE_PVALUE_WEIGHT: f64 = 0.6;
const SCORE_HALF_LIFE_WEIGHT: f64 = 0.4;
/// Eligibility threshold on `beta_gap` (relative beta divergence).
const ELIGIBILITY_BETA_GAP_MAX: f64 = 0.2;

#[derive(Debug)]
pub(super) struct PairEvaluation {
    pub(super) beta_short: f64,
    pub(super) beta_long: f64,
    pub(super) beta_eff: f64,
    pub(super) half_life_hours: f64,
    pub(super) adf_p_value: f64,
    pub(super) eligible: bool,
    pub(super) score: f64,
    pub(super) beta_gap: f64,
}

pub(super) fn evaluate_pair(
    cfg: &PairTradeConfig,
    history: &HashMap<String, VecDeque<PriceSample>>,
    pair: &PairSpec,
) -> Option<PairEvaluation> {
    let key = format!("{}/{}", pair.base, pair.quote);
    let pp = cfg.params_for(&key);
    let hist_a = history.get(&pair.base)?;
    let hist_b = history.get(&pair.quote)?;
    let available = hist_a.len().min(hist_b.len());
    let desired_long =
        ((pp.lookback_hours_long * 3600) / cfg.trading_period_secs).max(1) as usize;
    let desired_short =
        ((pp.lookback_hours_short * 3600) / cfg.trading_period_secs).max(1) as usize;
    let (long_len, short_len) = match cfg.warm_start_mode {
        WarmStartMode::Strict => {
            if available < desired_long {
                return None;
            }
            (desired_long, desired_short)
        }
        WarmStartMode::Relaxed => {
            let min_bars = pp.warm_start_min_bars.max(1);
            if available < min_bars {
                return None;
            }
            let long_len = desired_long.min(available);
            let short_len = desired_short.min(long_len);
            (long_len, short_len)
        }
    };

    let tail_a = tail_samples(hist_a, long_len);
    let tail_b = tail_samples(hist_b, long_len);
    let beta_long = regression_beta(&tail_b, &tail_a);
    let beta_short = regression_beta(
        &tail_b[tail_b.len() - short_len..],
        &tail_a[tail_a.len() - short_len..],
    );
    let beta_eff = BETA_EFF_SHORT_WEIGHT * beta_short + BETA_EFF_LONG_WEIGHT * beta_long;

    // Build spread series for diagnostics using long window
    let spreads: Vec<f64> = tail_a
        .iter()
        .zip(tail_b.iter())
        .map(|(sa, sb)| sa.log_price - beta_eff * sb.log_price)
        .collect();
    let (half_life_samples, adf_p_value) = half_life_and_p(&spreads);
    let half_life_hours = half_life_samples * (cfg.trading_period_secs as f64) / 3600.0;
    let beta_gap = ((beta_short - beta_long) / beta_eff.max(1e-6)).abs();
    let half_ok = half_life_hours <= pp.half_life_max_hours;
    let adf_ok = adf_p_value <= pp.adf_p_threshold;
    let beta_ok = beta_gap <= ELIGIBILITY_BETA_GAP_MAX;
    let score = half_ok as u8 + adf_ok as u8 + beta_ok as u8;
    let eligible = score >= 2;
    // softer ranking: weight lower p and faster half-life
    let continuous_score = (1.0 - adf_p_value.min(1.0)) * SCORE_PVALUE_WEIGHT
        + (1.0 / (1.0 + half_life_hours)) * SCORE_HALF_LIFE_WEIGHT;

    Some(PairEvaluation {
        beta_short,
        beta_long,
        beta_eff,
        half_life_hours,
        adf_p_value,
        eligible,
        score: continuous_score,
        beta_gap,
    })
}
