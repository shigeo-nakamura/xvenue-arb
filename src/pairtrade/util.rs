//! Pure numeric and quantization helpers extracted from the monolithic
//! pairtrade module. No dependencies on engine state.

use std::collections::VecDeque;

use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

pub(super) fn mean_std(window: &VecDeque<f64>) -> Option<(f64, f64)> {
    if window.is_empty() {
        return None;
    }
    let mean = window.iter().copied().sum::<f64>() / window.len() as f64;
    let var = window
        .iter()
        .map(|v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / window.len().max(1) as f64;
    Some((mean, var.sqrt()))
}

pub(super) fn half_life_and_p(spreads: &[f64]) -> (f64, f64) {
    // ADF-style AR(1) on levels: dY_t = phi * Y_{t-1} + eps
    if spreads.len() < 5 {
        return (f64::INFINITY, 1.0);
    }
    let mut x: Vec<f64> = Vec::with_capacity(spreads.len() - 1);
    let mut dy: Vec<f64> = Vec::with_capacity(spreads.len() - 1);
    for win in spreads.windows(2) {
        let prev = win[0];
        let curr = win[1];
        x.push(prev);
        dy.push(curr - prev);
    }
    let n = x.len();
    let mean_x = x.iter().sum::<f64>() / n as f64;
    let mean_dy = dy.iter().sum::<f64>() / n as f64;
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..n {
        let dx = x[i] - mean_x;
        let ddy = dy[i] - mean_dy;
        num += dx * ddy;
        den += dx * dx;
    }
    if den.abs() < 1e-12 {
        return (f64::INFINITY, 1.0);
    }
    let phi = (num / den).clamp(-0.999, 0.999);

    // residual variance and standard error of phi
    let mut rss = 0.0;
    for i in 0..n {
        let fit = phi * (x[i] - mean_x) + mean_dy;
        let err = dy[i] - fit;
        rss += err * err;
    }
    let sigma2 = rss / (n.saturating_sub(2)).max(1) as f64;
    let se_phi = (sigma2 / den).sqrt();
    let t_stat = if se_phi < 1e-12 { 0.0 } else { phi / se_phi };
    let p_value: f64 = df_p_value(t_stat, n);

    let ar_coef = 1.0 + phi;
    let half_life = if ar_coef <= 0.0 || ar_coef >= 1.0 {
        f64::INFINITY
    } else {
        -((2.0_f64).ln()) / ar_coef.ln()
    };

    (half_life, p_value.clamp(0.0, 1.0))
}

pub(super) fn df_p_value(t_stat: f64, n: usize) -> f64 {
    // Interpolated Dickey-Fuller critical values (with constant), approximate
    const CRITS: &[(usize, f64, f64, f64)] = &[
        (25, -3.75, -3.00, -2.63),
        (50, -3.58, -2.93, -2.60),
        (100, -3.51, -2.89, -2.58),
        (250, -3.46, -2.88, -2.57),
        (500, -3.44, -2.87, -2.57),
    ];
    let (c1, c5, c10) = interpolate_crits(n, CRITS);
    if t_stat < c1 {
        0.005
    } else if t_stat < c5 {
        0.025
    } else if t_stat < c10 {
        0.075
    } else {
        0.5
    }
}

pub(super) fn interpolate_crits(n: usize, table: &[(usize, f64, f64, f64)]) -> (f64, f64, f64) {
    if n <= table[0].0 {
        return (table[0].1, table[0].2, table[0].3);
    }
    for w in table.windows(2) {
        let (n1, c1_1, c5_1, c10_1) = w[0];
        let (n2, c1_2, c5_2, c10_2) = w[1];
        if n >= n1 && n <= n2 {
            let t = (n - n1) as f64 / (n2 - n1) as f64;
            let lerp = |a: f64, b: f64| a + t * (b - a);
            return (lerp(c1_1, c1_2), lerp(c5_1, c5_2), lerp(c10_1, c10_2));
        }
    }
    let last = table.last().unwrap();
    (last.1, last.2, last.3)
}

pub(super) fn tail_std(window: &VecDeque<f64>, len: usize) -> Option<f64> {
    if window.is_empty() || len == 0 {
        return None;
    }
    let start = window.len().saturating_sub(len);
    let mut sum = 0.0;
    let mut sum_sq = 0.0;
    let mut count = 0;
    for v in window.iter().skip(start) {
        sum += *v;
        sum_sq += v * v;
        count += 1;
    }
    if count == 0 {
        return None;
    }
    let mean = sum / count as f64;
    let var = (sum_sq / count as f64) - mean * mean;
    Some(var.max(0.0).sqrt())
}

/// Helper to round a price into `step` multiples according to the required direction.
pub(super) fn round_price_by_tick(
    price: Decimal,
    step: Decimal,
    side: dex_connector::OrderSide,
) -> Decimal {
    if step <= Decimal::ZERO {
        return price;
    }
    let rounding = match side {
        dex_connector::OrderSide::Long => RoundingStrategy::ToNegativeInfinity,
        dex_connector::OrderSide::Short => RoundingStrategy::ToPositiveInfinity,
    };
    let mut multiples = (price / step).round_dp_with_strategy(0, rounding);
    if multiples < Decimal::ONE {
        multiples = Decimal::ONE;
    }
    let rounded = multiples * step;
    let step_scale = step.scale();
    rounded.round_dp_with_strategy(step_scale, RoundingStrategy::ToZero)
}

pub(super) fn quantize_size_by_step(
    size: Decimal,
    step: Decimal,
    min_order: Option<Decimal>,
) -> Decimal {
    if step <= Decimal::ZERO {
        return size;
    }
    let mut multiples = (size / step).trunc();
    if let Some(mo) = min_order {
        if mo > Decimal::ZERO {
            let min_multiplier = (mo / step).ceil();
            if min_multiplier > multiples {
                multiples = min_multiplier;
            }
        }
    }
    let multiplier = if multiples >= Decimal::ONE {
        multiples
    } else {
        Decimal::ONE
    };
    multiplier * step
}

pub(super) fn quantize_size_by_step_ceiling(
    size: Decimal,
    step: Decimal,
    min_order: Option<Decimal>,
) -> Decimal {
    if step <= Decimal::ZERO {
        return size;
    }
    let mut multiples =
        (size / step).round_dp_with_strategy(0, RoundingStrategy::ToPositiveInfinity);
    if let Some(mo) = min_order {
        if mo > Decimal::ZERO {
            let min_multiplier = (mo / step).ceil();
            if min_multiplier > multiples {
                multiples = min_multiplier;
            }
        }
    }
    let multiplier = if multiples >= Decimal::ONE {
        multiples
    } else {
        Decimal::ONE
    };
    let rounded = multiplier * step;
    rounded.round_dp_with_strategy(step.scale(), RoundingStrategy::ToZero)
}
