//! Cross-venue spread engine.
//!
//! `spread_bps = (P_ext_mid - P_lt_mid) / P_lt_mid * 10_000`.
//!
//! The signal layer reads `dev = spread - μ_roll` rather than a raw
//! z-score: the cross-venue spread carries a structural per-symbol
//! funding-bias premium that doesn't revert to zero, so de-meaning
//! against the rolling window is the more honest baseline.

use std::collections::VecDeque;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

/// Tunables for the spread engine. Defaults match the per-symbol
/// production config; override per-venue at construction.
#[derive(Debug, Clone)]
pub struct SpreadConfig {
    /// Bucket cadence in milliseconds. We snap each venue's mid to the
    /// most recent bucket; values within the same bucket from the same
    /// venue overwrite — last-in-bucket wins, matching how a live bot
    /// would read its own book just before a place call.
    pub bucket_ms: u64,
    /// Rolling-mean window in seconds. Tracks the slow basis drift
    /// without over-fitting noise; 30 min is a reasonable default.
    pub rolling_window_sec: u64,
}

impl Default for SpreadConfig {
    fn default() -> Self {
        Self {
            bucket_ms: 1_000,
            rolling_window_sec: 1_800,
        }
    }
}

/// Compute the cross-venue spread in basis points.
///
/// Returns `None` if either mid is non-positive (degenerate book).
pub fn spread_bps(extended_mid: Decimal, lighter_mid: Decimal) -> Option<f64> {
    if lighter_mid.is_zero() || lighter_mid.is_sign_negative() {
        return None;
    }
    if extended_mid.is_sign_negative() {
        return None;
    }
    let l = lighter_mid.to_f64()?;
    let e = extended_mid.to_f64()?;
    if l <= 0.0 {
        return None;
    }
    Some((e - l) / l * 10_000.0)
}

/// Bounded ring buffer that tracks the running mean of the last
/// `capacity` samples. We keep a parallel running sum so each push is
/// O(1) — important because the live loop tags every bucket.
pub struct RollingMean {
    buf: VecDeque<f64>,
    capacity: usize,
    sum: f64,
}

impl RollingMean {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
            sum: 0.0,
        }
    }

    pub fn push(&mut self, x: f64) {
        if !x.is_finite() {
            // Skip non-finite samples so a single feed glitch doesn't
            // poison the rolling sum forever.
            return;
        }
        if self.buf.len() == self.capacity {
            if let Some(old) = self.buf.pop_front() {
                self.sum -= old;
            }
        }
        self.buf.push_back(x);
        self.sum += x;
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_warm(&self, min_samples: usize) -> bool {
        self.buf.len() >= min_samples
    }

    /// Returns `None` until at least one sample has been seen.
    pub fn mean(&self) -> Option<f64> {
        if self.buf.is_empty() {
            None
        } else {
            Some(self.sum / self.buf.len() as f64)
        }
    }
}

/// Spread + rolling-mean engine.
///
/// One instance per (extended, lighter) symbol pair. Feed it bucket
/// timestamps + per-venue mids (with `update_extended` /
/// `update_lighter`); call `current_dev_bps` once per tick to ask "is
/// the current spread far from the structural mean, and by how much".
pub struct SpreadEngine {
    cfg: SpreadConfig,
    rolling: RollingMean,
    last_bucket: Option<u64>,
    /// Latest mid per venue, both indexed by bucket. We commit a
    /// `spread_bps` sample to `rolling` only when both venues have
    /// reported in the same bucket — i.e. an aligned snapshot.
    latest_ext: Option<(u64, Decimal)>,
    latest_lt: Option<(u64, Decimal)>,
    last_committed_spread: Option<f64>,
    samples_committed: u64,
}

impl SpreadEngine {
    pub fn new(cfg: SpreadConfig) -> Self {
        let cap = (cfg.rolling_window_sec * 1_000 / cfg.bucket_ms.max(1)).max(1) as usize;
        Self {
            cfg,
            rolling: RollingMean::new(cap),
            last_bucket: None,
            latest_ext: None,
            latest_lt: None,
            last_committed_spread: None,
            samples_committed: 0,
        }
    }

    fn bucket_of(&self, ts_ms: u64) -> u64 {
        let b = self.cfg.bucket_ms.max(1);
        (ts_ms / b) * b
    }

    pub fn update_extended(&mut self, ts_ms: u64, mid: Decimal) {
        let bucket = self.bucket_of(ts_ms);
        self.latest_ext = Some((bucket, mid));
        self.maybe_commit(bucket);
    }

    pub fn update_lighter(&mut self, ts_ms: u64, mid: Decimal) {
        let bucket = self.bucket_of(ts_ms);
        self.latest_lt = Some((bucket, mid));
        self.maybe_commit(bucket);
    }

    fn maybe_commit(&mut self, bucket: u64) {
        // Only commit when both venues have reported into the same
        // bucket. Cross-bucket samples skew the spread by per-venue
        // latency rather than capturing a real dislocation.
        let (eb, em) = match self.latest_ext {
            Some(p) => p,
            None => return,
        };
        let (lb, lm) = match self.latest_lt {
            Some(p) => p,
            None => return,
        };
        if eb != lb || eb != bucket {
            return;
        }
        // Skip if we already committed for this bucket
        if self.last_bucket == Some(bucket) {
            return;
        }
        if let Some(s) = spread_bps(em, lm) {
            self.rolling.push(s);
            self.last_committed_spread = Some(s);
            self.last_bucket = Some(bucket);
            self.samples_committed += 1;
        }
    }

    /// Most recently committed spread sample. `None` before the first
    /// aligned bucket.
    pub fn last_spread_bps(&self) -> Option<f64> {
        self.last_committed_spread
    }

    /// Rolling mean of the spread (μ_roll). `None` before any sample.
    pub fn rolling_mean(&self) -> Option<f64> {
        self.rolling.mean()
    }

    /// `dev = spread - μ_roll`. The signal layer compares this to the
    /// configured absolute threshold. `None` if either component is
    /// missing — caller treats as "no signal".
    pub fn current_dev_bps(&self) -> Option<f64> {
        let s = self.last_committed_spread?;
        let m = self.rolling_mean()?;
        Some(s - m)
    }

    /// Number of aligned-bucket samples committed so far. Used by the
    /// signal layer to decide if the rolling mean is warm enough to
    /// trade against (we do not enter on the first few samples).
    pub fn samples_committed(&self) -> u64 {
        self.samples_committed
    }

    /// True once the rolling buffer has at least `min_samples` entries.
    /// Use to gate trading until the mean is meaningful.
    pub fn is_warm(&self, min_samples: usize) -> bool {
        self.rolling.is_warm(min_samples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn spread_basic_above() {
        let s = spread_bps(dec!(78010), dec!(78000)).unwrap();
        assert!((s - 1.282).abs() < 0.01, "got {s}");
    }

    #[test]
    fn spread_negative_when_extended_below() {
        let s = spread_bps(dec!(77990), dec!(78000)).unwrap();
        assert!(s < 0.0);
    }

    #[test]
    fn spread_rejects_zero_lighter() {
        assert!(spread_bps(dec!(78000), dec!(0)).is_none());
    }

    #[test]
    fn rolling_mean_is_running() {
        let mut r = RollingMean::new(3);
        assert_eq!(r.mean(), None);
        r.push(1.0);
        r.push(2.0);
        r.push(3.0);
        assert!((r.mean().unwrap() - 2.0).abs() < 1e-9);
        // Fourth sample evicts the first
        r.push(4.0);
        assert!((r.mean().unwrap() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn rolling_mean_skips_non_finite() {
        let mut r = RollingMean::new(3);
        r.push(1.0);
        r.push(f64::NAN);
        r.push(3.0);
        assert!((r.mean().unwrap() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn engine_only_commits_aligned_buckets() {
        let cfg = SpreadConfig {
            bucket_ms: 1_000,
            rolling_window_sec: 60,
        };
        let mut eng = SpreadEngine::new(cfg);
        // Different buckets: no commit
        eng.update_extended(1_000, dec!(78010));
        eng.update_lighter(2_000, dec!(78000));
        assert_eq!(eng.samples_committed(), 0);
        // Same bucket: commit
        eng.update_extended(2_500, dec!(78010));
        assert_eq!(eng.samples_committed(), 1);
        assert!(eng.last_spread_bps().unwrap() > 0.0);
    }

    #[test]
    fn engine_one_sample_per_bucket() {
        let cfg = SpreadConfig {
            bucket_ms: 1_000,
            rolling_window_sec: 60,
        };
        let mut eng = SpreadEngine::new(cfg);
        eng.update_extended(1_000, dec!(78010));
        eng.update_lighter(1_500, dec!(78000));
        assert_eq!(eng.samples_committed(), 1);
        // Late update inside the same bucket: no extra commit
        eng.update_extended(1_900, dec!(78020));
        assert_eq!(eng.samples_committed(), 1);
    }

    #[test]
    fn engine_dev_centers_on_rolling_mean() {
        let cfg = SpreadConfig {
            bucket_ms: 1_000,
            rolling_window_sec: 60,
        };
        let mut eng = SpreadEngine::new(cfg);
        // Stream with a steady ~+0.26 bps spread — biases μ_roll to
        // that value.
        for i in 0..20 {
            let t = (i as u64) * 1_000;
            eng.update_extended(t, dec!(78002));
            eng.update_lighter(t, dec!(78000));
        }
        let mean = eng.rolling_mean().unwrap();
        assert!((mean - 0.2564).abs() < 0.01, "mean={mean}");
        // Now jump the spread to ~+10 bps; dev should be ~+10 - 0.26 ≈ +9.74
        // (the new sample also bumps the rolling mean a touch, so the
        // expected dev is slightly under 10 bps).
        eng.update_extended(20_000, dec!(78078));
        eng.update_lighter(20_000, dec!(78000));
        let dev = eng.current_dev_bps().unwrap();
        assert!(dev > 8.0, "dev={dev}");
        assert!(dev < 10.0, "dev={dev} should be less than the raw spread");
    }
}
