//! Defensive entry filter — bot-strategy#429.
//!
//! Tracks recent Lighter quote regime metrics (inside-spread, top-of-book
//! depth) over a rolling window and blocks new `Decision::Enter` outcomes
//! when the regime looks unstable. Motivation: in the 2026-05-17/18 24h
//! LIVE window, 5.5% of entries ended in `ForceClose` averaging -29 bps,
//! consuming the +2.0 bps mean MeanCross edge. Each ForceClose cycle saw
//! a Lighter inside-spread spike or a sparse-side blowout in the
//! 30-second window preceding entry.
//!
//! The filter is opt-in: each threshold field defaults to `None` (off)
//! so existing live behaviour is preserved until the operator flips the
//! gate via YAML.

use std::collections::VecDeque;

/// One rolling-window sample. `ts_ms` is the slower-venue timestamp the
/// signal engine consumes (matches `now_ts_ms` in `run_one_tick`).
#[derive(Debug, Clone, Copy)]
pub(super) struct QuoteSample {
    pub ts_ms: u64,
    pub lt_inside_bps: f64,
    pub lt_bid_size: f64,
    pub lt_ask_size: f64,
}

/// Ring buffer of recent quote samples, evicted by elapsed time so the
/// window survives a change in `spread_bucket_ms` without re-tuning.
#[derive(Debug)]
pub(super) struct RecentQuoteHistory {
    buf: VecDeque<QuoteSample>,
    window_ms: u64,
}

impl RecentQuoteHistory {
    pub fn new(window_sec: u64) -> Self {
        Self {
            buf: VecDeque::new(),
            window_ms: window_sec.saturating_mul(1_000),
        }
    }

    pub fn push(&mut self, sample: QuoteSample) {
        self.buf.push_back(sample);
        let cutoff = sample.ts_ms.saturating_sub(self.window_ms);
        while let Some(front) = self.buf.front() {
            if front.ts_ms < cutoff {
                self.buf.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    fn fold_max<F: Fn(&QuoteSample) -> f64>(&self, f: F) -> Option<f64> {
        self.buf.iter().map(f).fold(None, |acc, x| match acc {
            None => Some(x),
            Some(m) => Some(m.max(x)),
        })
    }

    fn fold_min<F: Fn(&QuoteSample) -> f64>(&self, f: F) -> Option<f64> {
        self.buf.iter().map(f).fold(None, |acc, x| match acc {
            None => Some(x),
            Some(m) => Some(m.min(x)),
        })
    }

    pub fn max_lt_inside_bps(&self) -> Option<f64> {
        self.fold_max(|s| s.lt_inside_bps)
    }

    pub fn min_lt_bid_size(&self) -> Option<f64> {
        self.fold_min(|s| s.lt_bid_size)
    }

    pub fn min_lt_ask_size(&self) -> Option<f64> {
        self.fold_min(|s| s.lt_ask_size)
    }
}

/// Outcome from `evaluate_entry_filter`. Each variant carries enough
/// detail for a single-line log without re-walking the history.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum EntryFilterOutcome {
    Allow,
    BlockInsideSpike {
        observed_bps: f64,
        threshold_bps: f64,
    },
    BlockMinDepth {
        observed_eth: f64,
        floor_eth: f64,
    },
}

/// Apply the defensive entry filter against the recent history. Both
/// thresholds are opt-in via `Option`; when both fields are `None` the
/// filter is a no-op. When history is empty (very early bot start), the
/// filter allows the entry — the regime isn't being claimed safe, just
/// not yet judged.
pub(super) fn evaluate_entry_filter(
    history: &RecentQuoteHistory,
    inside_max_bps: Option<f64>,
    min_depth_eth: Option<f64>,
) -> EntryFilterOutcome {
    if history.len() == 0 {
        return EntryFilterOutcome::Allow;
    }
    if let Some(thr) = inside_max_bps {
        if let Some(observed) = history.max_lt_inside_bps() {
            if observed > thr {
                return EntryFilterOutcome::BlockInsideSpike {
                    observed_bps: observed,
                    threshold_bps: thr,
                };
            }
        }
    }
    if let Some(floor) = min_depth_eth {
        let bid_min = history.min_lt_bid_size().unwrap_or(f64::INFINITY);
        let ask_min = history.min_lt_ask_size().unwrap_or(f64::INFINITY);
        let observed = bid_min.min(ask_min);
        if observed < floor {
            return EntryFilterOutcome::BlockMinDepth {
                observed_eth: observed,
                floor_eth: floor,
            };
        }
    }
    EntryFilterOutcome::Allow
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(ts: u64, inside: f64, bid_sz: f64, ask_sz: f64) -> QuoteSample {
        QuoteSample {
            ts_ms: ts,
            lt_inside_bps: inside,
            lt_bid_size: bid_sz,
            lt_ask_size: ask_sz,
        }
    }

    #[test]
    fn ring_evicts_samples_outside_window() {
        let mut h = RecentQuoteHistory::new(60);
        h.push(s(0, 1.0, 5.0, 5.0));
        h.push(s(30_000, 2.0, 5.0, 5.0));
        h.push(s(60_000, 3.0, 5.0, 5.0));
        h.push(s(70_000, 4.0, 5.0, 5.0));
        // cutoff = 70_000 - 60_000 = 10_000; sample at ts=0 is evicted
        assert_eq!(h.len(), 3);
        assert_eq!(h.max_lt_inside_bps(), Some(4.0));
    }

    #[test]
    fn ring_keeps_boundary_sample() {
        let mut h = RecentQuoteHistory::new(60);
        h.push(s(0, 1.0, 5.0, 5.0));
        h.push(s(60_000, 2.0, 5.0, 5.0));
        // cutoff = 60_000 - 60_000 = 0; sample at ts=0 is kept (ts >= cutoff)
        assert_eq!(h.len(), 2);
    }

    #[test]
    fn allows_when_history_empty() {
        let h = RecentQuoteHistory::new(60);
        assert_eq!(
            evaluate_entry_filter(&h, Some(5.0), Some(0.5)),
            EntryFilterOutcome::Allow
        );
    }

    #[test]
    fn allows_when_filter_disabled() {
        let mut h = RecentQuoteHistory::new(60);
        // hostile regime — still passes when both thresholds are None
        h.push(s(0, 100.0, 0.001, 0.001));
        assert_eq!(
            evaluate_entry_filter(&h, None, None),
            EntryFilterOutcome::Allow
        );
    }

    #[test]
    fn blocks_on_inside_spike() {
        let mut h = RecentQuoteHistory::new(60);
        h.push(s(0, 1.0, 5.0, 5.0));
        h.push(s(5_000, 7.0, 5.0, 5.0));
        h.push(s(10_000, 1.5, 5.0, 5.0));
        match evaluate_entry_filter(&h, Some(5.0), None) {
            EntryFilterOutcome::BlockInsideSpike {
                observed_bps,
                threshold_bps,
            } => {
                assert!((observed_bps - 7.0).abs() < 1e-9);
                assert!((threshold_bps - 5.0).abs() < 1e-9);
            }
            other => panic!("expected BlockInsideSpike, got {:?}", other),
        }
    }

    #[test]
    fn blocks_on_thin_bid_side() {
        let mut h = RecentQuoteHistory::new(60);
        h.push(s(0, 1.0, 5.0, 5.0));
        h.push(s(5_000, 1.0, 0.05, 5.0));
        h.push(s(10_000, 1.0, 5.0, 5.0));
        match evaluate_entry_filter(&h, None, Some(0.5)) {
            EntryFilterOutcome::BlockMinDepth {
                observed_eth,
                floor_eth,
            } => {
                assert!((observed_eth - 0.05).abs() < 1e-9);
                assert!((floor_eth - 0.5).abs() < 1e-9);
            }
            other => panic!("expected BlockMinDepth, got {:?}", other),
        }
    }

    #[test]
    fn blocks_on_thin_ask_side() {
        let mut h = RecentQuoteHistory::new(60);
        h.push(s(0, 1.0, 5.0, 5.0));
        h.push(s(5_000, 1.0, 5.0, 0.1));
        match evaluate_entry_filter(&h, None, Some(0.5)) {
            EntryFilterOutcome::BlockMinDepth {
                observed_eth,
                floor_eth,
            } => {
                assert!((observed_eth - 0.1).abs() < 1e-9);
                assert!((floor_eth - 0.5).abs() < 1e-9);
            }
            other => panic!("expected BlockMinDepth, got {:?}", other),
        }
    }

    #[test]
    fn allows_in_healthy_regime() {
        let mut h = RecentQuoteHistory::new(60);
        for i in 0..6 {
            h.push(s(i * 5_000, 1.2, 3.0, 4.0));
        }
        assert_eq!(
            evaluate_entry_filter(&h, Some(5.0), Some(0.5)),
            EntryFilterOutcome::Allow
        );
    }

    #[test]
    fn inside_spike_checked_before_depth() {
        // When both thresholds fail, inside-spike takes priority for the
        // log line; verify the deterministic order so operators can
        // grep one reason per cycle without ambiguity.
        let mut h = RecentQuoteHistory::new(60);
        h.push(s(0, 9.0, 0.01, 0.01));
        match evaluate_entry_filter(&h, Some(5.0), Some(0.5)) {
            EntryFilterOutcome::BlockInsideSpike { .. } => {}
            other => panic!("expected BlockInsideSpike first, got {:?}", other),
        }
    }
}
