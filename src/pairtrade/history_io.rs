//! On-disk history persistence helpers extracted from the monolithic
//! pairtrade module.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::config::PairTradeConfig;
use super::stats::PriceSample;

/// On-disk snapshot schema used by the live bot. Version 2 adds
/// `spread_histories` — the per-pair `state.spread_history` that the
/// engine accumulates at runtime — so that at restart we can restore
/// the real spread series instead of rebuilding a synthetic one via
/// `warm_start_states_from_history` (which applies a single OLS beta
/// to the full log_price window and produces an artificially
/// low-variance spread_history, the mechanism behind the 2026-04-15
/// 06:02 UTC "std collapse" incident — bot-strategy#62).
///
/// Version 1 (no `_v` field) was a bare `HashMap<String,
/// Vec<(f64, i64)>>`. The loader parses v2 first and falls back to
/// v1 on failure, so pre-existing history files keep working.
#[derive(Serialize, Deserialize, Default)]
struct SnapshotV2 {
    #[serde(rename = "_v")]
    version: u32,
    prices: HashMap<String, Vec<(f64, i64)>>,
    /// Pair key (e.g. "BTC/ETH") → the live engine's
    /// `state.spread_history` as a plain `Vec<f64>`. Missing in older
    /// files; defaulted to empty by `#[serde(default)]`.
    #[serde(default)]
    spread_histories: HashMap<String, Vec<f64>>,
}

pub(super) fn persist_history_to_disk(
    cfg: &PairTradeConfig,
    history: &HashMap<String, VecDeque<PriceSample>>,
    spread_histories: &HashMap<String, VecDeque<f64>>,
    history_path: &std::path::Path,
) {
    if cfg.disable_history_persist {
        return;
    }
    // Backtest replay re-drives this per tick, producing hundreds of
    // thousands of disk writes per run. That serialises a grid of
    // concurrent backtest processes on ext4 and leaves them wedged in
    // `Dl` state. The persisted file is only consumed by peer live bots
    // for A/B/C alignment, which is irrelevant under replay.
    if cfg.backtest_mode {
        return;
    }
    let prices: HashMap<String, Vec<(f64, i64)>> = history
        .iter()
        .map(|(sym, deque)| {
            let v: Vec<(f64, i64)> = deque.iter().map(|p| (p.log_price, p.ts)).collect();
            (sym.clone(), v)
        })
        .collect();
    let spread_histories: HashMap<String, Vec<f64>> = spread_histories
        .iter()
        .map(|(k, deque)| (k.clone(), deque.iter().copied().collect()))
        .collect();
    let snapshot = SnapshotV2 {
        version: 2,
        prices,
        spread_histories,
    };
    if let Ok(json) = serde_json::to_string(&snapshot) {
        // Atomic write: tmpfile in the same directory + rename. Multiple
        // bots may be writing this shared file concurrently (pairtrade#4);
        // rename guarantees readers never observe a torn JSON document.
        let path = history_path;
        let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let file_name = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "pairtrade_history.json".to_string());
        let tmp = dir.join(format!(".{}.tmp.{}", file_name, std::process::id()));
        if let Err(e) = fs::write(&tmp, json) {
            log::debug!("persist history tmp write failed: {:?}", e);
            return;
        }
        if let Err(e) = fs::rename(&tmp, path) {
            log::debug!("persist history rename failed: {:?}", e);
            let _ = fs::remove_file(&tmp);
        }
    }

    archive_snapshot_hourly(cfg, history_path);
}

fn archive_snapshot_hourly(cfg: &PairTradeConfig, history_path: &Path) {
    let Some(archive_dir) = &cfg.history_archive_dir else {
        return;
    };
    let archive_dir = Path::new(archive_dir);
    if let Err(e) = fs::create_dir_all(archive_dir) {
        log::debug!("archive dir create failed: {:?}", e);
        return;
    }
    let stem = history_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let hour_tag = Utc::now().format("%Y%m%dT%H00Z");
    let archive_path = archive_dir.join(format!("{}.{}.json", stem, hour_tag));
    if archive_path.exists() {
        return;
    }
    if let Err(e) = fs::copy(history_path, &archive_path) {
        log::debug!("archive snapshot copy failed: {:?}", e);
        return;
    }
    log::info!(
        "[HISTORY_ARCHIVE] saved {}",
        archive_path.file_name().unwrap_or_default().to_string_lossy()
    );
    cleanup_old_archives(archive_dir, cfg.history_archive_retention_days);
}

fn cleanup_old_archives(dir: &Path, retention_days: u32) {
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(retention_days as u64 * 86400))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        let Ok(modified) = meta.modified() else { continue };
        if modified < cutoff {
            let _ = fs::remove_file(entry.path());
            log::info!(
                "[HISTORY_ARCHIVE] removed expired {}",
                entry.file_name().to_string_lossy()
            );
        }
    }
}

/// Parse the persisted history file, accepting both v2 (explicit
/// `SnapshotV2` struct) and legacy v1 (bare per-symbol map). Returns
/// (prices, spread_histories) where `spread_histories` is empty for v1.
fn parse_snapshot_file(
    path: &std::path::Path,
) -> Option<(
    HashMap<String, Vec<(f64, i64)>>,
    HashMap<String, Vec<f64>>,
)> {
    let content = fs::read_to_string(path).ok()?;
    // Try v2 first (has explicit schema with `_v` and `prices`).
    if let Ok(v2) = serde_json::from_str::<SnapshotV2>(&content) {
        if v2.version >= 2 {
            return Some((v2.prices, v2.spread_histories));
        }
    }
    // Fall back to v1 (bare `HashMap<String, Vec<(f64, i64)>>`).
    let prices: HashMap<String, Vec<(f64, i64)>> = serde_json::from_str(&content).ok()?;
    Some((prices, HashMap::new()))
}

/// Load a history snapshot for backtest warm-start. Unlike
/// `load_history_from_disk`, this skips the stale-guard check (the
/// snapshot is always older than the replay cursor) and instead accepts
/// all samples within `max_history_len` bars of the *newest* sample in
/// each symbol, regardless of `now_ts`. Also populates
/// `spread_histories_out` when the snapshot is v2.
pub(super) fn load_history_snapshot_for_bt(
    history: &mut HashMap<String, VecDeque<PriceSample>>,
    spread_histories_out: &mut HashMap<String, VecDeque<f64>>,
    snapshot_path: &std::path::Path,
    max_history_len: usize,
) {
    let Some((prices, spreads)) = parse_snapshot_file(snapshot_path) else {
        log::warn!(
            "[BT_WARM_START] failed to read or parse snapshot {}",
            snapshot_path.display()
        );
        return;
    };
    for (sym, entries) in prices {
        if entries.is_empty() {
            continue;
        }
        let newest_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
        let max_age = (max_history_len as i64) * 60; // assume 60s bars
        let mut deque = VecDeque::new();
        for (log_price, ts) in entries {
            if newest_ts.saturating_sub(ts) <= max_age {
                deque.push_back(PriceSample { log_price, ts });
            }
        }
        if !deque.is_empty() {
            log::info!(
                "[BT_WARM_START] loaded {} bars for {} from snapshot",
                deque.len(),
                sym
            );
            history.insert(sym, deque);
        }
    }
    for (pair_key, series) in spreads {
        if series.is_empty() {
            continue;
        }
        let len = series.len();
        let deque: VecDeque<f64> = series.into_iter().collect();
        log::info!(
            "[BT_WARM_START] loaded {} persisted spread_history bars for {}",
            len,
            pair_key
        );
        spread_histories_out.insert(pair_key, deque);
    }
}

pub(super) fn load_history_from_disk(
    cfg: &PairTradeConfig,
    history: &mut HashMap<String, VecDeque<PriceSample>>,
    spread_histories_out: &mut HashMap<String, VecDeque<f64>>,
    history_path: &std::path::Path,
    now_ts: i64,
    max_history_len: usize,
) {
    if cfg.disable_history_persist {
        return;
    }
    // Skip persisted-history loading entirely under backtest replay: the
    // file's timestamps reflect the wall clock at dump time and would
    // always look stale relative to the replayed cursor, producing
    // millions of WARN lines without contributing anything useful (the
    // replay data already supplies a clean, gap-free history).
    if cfg.backtest_mode {
        return;
    }
    let Some((prices, spreads)) = parse_snapshot_file(history_path) else {
        return;
    };
    let max_age_secs =
        (max_history_len as i64).saturating_mul(cfg.trading_period_secs as i64);
    // Stale-history guard (pairtrade#4): if the newest sample for a symbol
    // is older than a few bars, the persisted file is from a stopped bot
    // and replaying it would freeze a stale rolling window. Drop it and
    // let the live feed warm up from scratch.
    let stale_threshold_secs = (cfg.trading_period_secs as i64).saturating_mul(5).max(60);
    let mut any_stale = false;
    for (sym, entries) in prices {
        let newest_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
        if now_ts.saturating_sub(newest_ts) > stale_threshold_secs {
            log::debug!(
                "discarding stale persisted history for {}: newest sample {}s old",
                sym,
                now_ts.saturating_sub(newest_ts)
            );
            any_stale = true;
            continue;
        }
        let mut deque = VecDeque::new();
        for (log_price, ts) in entries {
            if now_ts.saturating_sub(ts) > max_age_secs {
                continue;
            }
            deque.push_back(PriceSample { log_price, ts });
        }
        if !deque.is_empty() {
            history.insert(sym, deque);
        }
    }
    // If any symbol was discarded as stale, the persisted spread_history
    // is also stale — discard it rather than pairing it with a
    // freshly-built log_price window. This triggers the cold-start
    // synthesis path in `warm_start_states_from_history`, which is still
    // the fallback for genuinely stale files.
    if !any_stale {
        for (pair_key, series) in spreads {
            if series.is_empty() {
                continue;
            }
            let deque: VecDeque<f64> = series.into_iter().collect();
            spread_histories_out.insert(pair_key, deque);
        }
    }
}
