//! Grid search runner over `BtConfig` axes.
//!
//! Bot-strategy#166 Phase 1. Loads both venue dumps once, then walks
//! every cell of `(abs_threshold_bps × persistence_sec × max_hold_sec ×
//! rolling_window_sec)` in parallel via `rayon::par_iter`. Each cell
//! gets its own `DualReplay` with fresh cursors but shares the
//! underlying dump bytes (see [`DualReplay::clone_with_fresh_cursors`]).

use anyhow::Result;
use rayon::prelude::*;
use serde::Serialize;

use super::bt::{run_bt, BtConfig};
use crate::ports::replay_dex::DualReplay;

#[derive(Debug, Clone)]
pub struct GridSpec {
    pub abs_threshold_bps: Vec<f64>,
    pub persistence_sec: Vec<u64>,
    pub max_hold_sec: Vec<u64>,
    pub rolling_window_sec: Vec<u64>,
}

impl GridSpec {
    /// Cartesian-product expansion size.
    pub fn cell_count(&self) -> usize {
        self.abs_threshold_bps.len()
            * self.persistence_sec.len()
            * self.max_hold_sec.len()
            * self.rolling_window_sec.len()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GridResult {
    pub abs_threshold_bps: f64,
    pub persistence_sec: u64,
    pub max_hold_sec: u64,
    pub rolling_window_sec: u64,
    pub trades: usize,
    pub total_net_pnl_usd: f64,
    pub win_rate: f64,
    pub mean_net_bps: f64,
    pub samples_committed: u64,
}

/// Run the grid in parallel. `replay` is consumed by repeated cloning of
/// its cursors via `clone_with_fresh_cursors`, so the dump is parsed only
/// once. Returns one `GridResult` per cell, in unspecified order — sort
/// the result yourself.
pub fn run_grid(replay: &DualReplay, base_cfg: &BtConfig, spec: &GridSpec) -> Vec<GridResult> {
    let cells: Vec<(f64, u64, u64, u64)> = spec
        .abs_threshold_bps
        .iter()
        .flat_map(|abs| {
            spec.persistence_sec.iter().flat_map(move |persist| {
                spec.max_hold_sec.iter().flat_map(move |max_hold| {
                    spec.rolling_window_sec
                        .iter()
                        .map(move |rolling| (*abs, *persist, *max_hold, *rolling))
                })
            })
        })
        .collect();

    cells
        .par_iter()
        .map(|(abs, persist, max_hold, rolling)| -> Result<GridResult> {
            let r = replay.clone_with_fresh_cursors();
            let mut cfg = base_cfg.clone();
            cfg.signal.abs_threshold_bps = *abs;
            cfg.signal.persistence_sec = *persist;
            cfg.signal.max_hold_sec = *max_hold;
            cfg.spread.rolling_window_sec = *rolling;
            // Recompute rolling buffer cap inside SpreadEngine::new (called
            // from run_bt) — no extra work needed here.
            let summary = run_bt(&r, cfg)?;
            let total = summary.total_net_pnl_usd();
            Ok(GridResult {
                abs_threshold_bps: *abs,
                persistence_sec: *persist,
                max_hold_sec: *max_hold,
                rolling_window_sec: *rolling,
                trades: summary.trades.len(),
                total_net_pnl_usd: rust_decimal::prelude::ToPrimitive::to_f64(&total)
                    .unwrap_or(0.0),
                win_rate: summary.win_rate(),
                mean_net_bps: summary.mean_net_bps(),
                samples_committed: summary.samples_committed,
            })
        })
        .filter_map(|r| match r {
            Ok(g) => Some(g),
            Err(e) => {
                log::warn!("grid cell failed: {:?}", e);
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::replay_dex::DualReplay;

    fn dump_line(timestamp_ms: i64, symbol: &str, mid: f64) -> String {
        format!(
            r#"{{"timestamp":{ts},"prices":{{"{sym}":{{"price":"{p}","funding_rate":"0","bid_price":"{p}","ask_price":"{p}","bid_size":"1","ask_size":"1","exchange_ts":{ets}}}}}}}"#,
            ts = timestamp_ms,
            sym = symbol,
            p = mid,
            ets = timestamp_ms / 1000
        )
    }

    fn write_dump(dir: &std::path::Path, name: &str, lines: &[String]) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, lines.join("\n")).unwrap();
        path
    }

    fn build_replay() -> (tempfile::TempDir, DualReplay) {
        let dir = tempfile::tempdir().unwrap();
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        for i in 0..200i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            let lt_mid = 78_000.0_f64;
            let ext_mid = if (60..130).contains(&i) {
                lt_mid * 1.002
            } else {
                lt_mid
            };
            ext_lines.push(dump_line(ts_ms, "BTC", ext_mid));
            lt_lines.push(dump_line(ts_ms, "BTC", lt_mid));
        }
        let ext_path = write_dump(dir.path(), "ext.jsonl", &ext_lines);
        let lt_path = write_dump(dir.path(), "lt.jsonl", &lt_lines);
        let r = DualReplay::new(ext_path.to_str().unwrap(), lt_path.to_str().unwrap()).unwrap();
        (dir, r)
    }

    #[test]
    fn grid_runs_all_cells_with_shared_data() {
        let (_dir, replay) = build_replay();
        let spec = GridSpec {
            abs_threshold_bps: vec![3.0, 5.0, 8.0],
            persistence_sec: vec![5],
            max_hold_sec: vec![600],
            rolling_window_sec: vec![60],
        };
        let mut base = BtConfig::default();
        base.signal.min_warmup_samples = 30;
        base.extended_taker_fee_bps = 0.0;
        base.lighter_taker_fee_bps = 0.0;
        let results = run_grid(&replay, &base, &spec);
        assert_eq!(results.len(), 3);
        // Higher threshold should produce strictly fewer or equal trades
        let mut by_thresh: Vec<_> = results
            .iter()
            .map(|r| (r.abs_threshold_bps, r.trades))
            .collect();
        by_thresh.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        for w in by_thresh.windows(2) {
            assert!(
                w[0].1 >= w[1].1,
                "trades should be monotone non-increasing in threshold: {:?}",
                by_thresh
            );
        }
    }

    #[test]
    fn grid_does_not_share_cursor_across_cells() {
        // If parallel cells leaked cursor state, repeated runs of the
        // same cell would give different trade counts. Use a 2x2 grid
        // where each abs_threshold appears once, then re-run and check
        // results stay deterministic.
        let (_dir, replay) = build_replay();
        let spec = GridSpec {
            abs_threshold_bps: vec![5.0, 8.0],
            persistence_sec: vec![5, 10],
            max_hold_sec: vec![600],
            rolling_window_sec: vec![60],
        };
        let mut base = BtConfig::default();
        base.signal.min_warmup_samples = 30;
        base.extended_taker_fee_bps = 0.0;
        base.lighter_taker_fee_bps = 0.0;
        let r1 = run_grid(&replay, &base, &spec);
        let r2 = run_grid(&replay, &base, &spec);
        assert_eq!(r1.len(), 4);
        assert_eq!(r2.len(), 4);
        // Sort by axes for stable comparison
        let key = |r: &GridResult| {
            (
                (r.abs_threshold_bps * 100.0) as i64,
                r.persistence_sec,
                r.max_hold_sec,
                r.rolling_window_sec,
            )
        };
        let mut s1 = r1.clone();
        let mut s2 = r2.clone();
        s1.sort_by_key(key);
        s2.sort_by_key(key);
        for (a, b) in s1.iter().zip(s2.iter()) {
            assert_eq!(a.trades, b.trades);
            assert!((a.total_net_pnl_usd - b.total_net_pnl_usd).abs() < 1e-9);
        }
    }

    #[test]
    fn grid_spec_cell_count() {
        let s = GridSpec {
            abs_threshold_bps: vec![1.0, 2.0, 3.0],
            persistence_sec: vec![5, 10],
            max_hold_sec: vec![600, 1200, 1800, 3600],
            rolling_window_sec: vec![60],
        };
        assert_eq!(s.cell_count(), 3 * 2 * 4 * 1);
    }
}
