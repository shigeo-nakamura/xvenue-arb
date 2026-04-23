//! Pure statistical helpers extracted from the monolithic pairtrade module.
//! No dependencies on engine state.

use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub(super) struct PriceSample {
    pub(super) log_price: f64,
    pub(super) ts: i64,
}

pub(super) fn tail_samples(history: &VecDeque<PriceSample>, len: usize) -> Vec<PriceSample> {
    let take = len.min(history.len());
    let mut v: Vec<PriceSample> = history.iter().rev().take(take).cloned().collect();
    v.reverse();
    v
}

pub(super) fn regression_beta(x: &[PriceSample], y: &[PriceSample]) -> f64 {
    let n = x.len().min(y.len());
    if n < 2 {
        return 1.0;
    }
    let (mut sum_x, mut sum_y) = (0.0, 0.0);
    for i in 0..n {
        sum_x += x[i].log_price;
        sum_y += y[i].log_price;
    }
    let mean_x = sum_x / n as f64;
    let mean_y = sum_y / n as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    for i in 0..n {
        let dx = x[i].log_price - mean_x;
        let dy = y[i].log_price - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
    }
    if var_x.abs() < 1e-9 {
        1.0
    } else {
        (cov / var_x).clamp(0.1, 10.0)
    }
}

pub(super) fn spread_slope_sigma(history: &VecDeque<f64>, window: usize) -> Option<f64> {
    let len = history.len().min(window);
    if len < 3 {
        return None;
    }
    let start = history.len() - len;
    let n = len as f64;
    let mean_i = (n - 1.0) / 2.0;
    let (mut mean_x, mut cov, mut var_i) = (0.0, 0.0, 0.0);
    for j in 0..len {
        mean_x += history[start + j];
    }
    mean_x /= n;
    for j in 0..len {
        let di = j as f64 - mean_i;
        let dx = history[start + j] - mean_x;
        cov += di * dx;
        var_i += di * di;
    }
    if var_i.abs() < 1e-15 {
        return None;
    }
    let slope = cov / var_i;
    let mut sum_sq = 0.0;
    for j in 0..len {
        let dx = history[start + j] - mean_x;
        sum_sq += dx * dx;
    }
    let std = (sum_sq / n).max(0.0).sqrt();
    if std < 1e-9 {
        return None;
    }
    Some((slope / std).abs())
}
