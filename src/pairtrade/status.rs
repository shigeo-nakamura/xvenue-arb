//! Status snapshot/equity reporting and shutdown-status types extracted
//! from the monolithic pairtrade module. The reporter writes a JSON status
//! file consumed by the dashboard, plus an equity history JSONL.

use std::collections::HashMap;
use std::cmp::Ordering;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Utc};
use dex_connector::PositionSnapshot;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::config::PairTradeConfig;
use super::pnl_log::sanitize_pnl_tag;
use crate::error_counter::{self, ErrorSummary};

use std::env;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct EquityBaseline {
    pub(super) date: String,
    pub(super) equity: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct EquityHistoryPoint {
    pub(super) ts: i64,
    pub(super) equity: f64,
}

#[derive(Debug)]
pub(super) struct StatusReporter {
    pub(super) path: PathBuf,
    pub(super) id: Option<String>,
    pub(super) agent: Option<String>,
    pub(super) dex: String,
    pub(super) dry_run: bool,
    pub(super) backtest_mode: bool,
    pub(super) interval_secs: u64,
    pub(super) snapshot_every: Duration,
    pub(super) pnl_total: f64,
    pub(super) pnl_today: f64,
    pub(super) pnl_today_date: NaiveDate,
    pub(super) equity_day_start: f64,
    pub(super) equity_day_start_set: bool,
    pub(super) equity_baseline_path: PathBuf,
    pub(super) equity_history_path: PathBuf,
    pub(super) last_equity_history_ts: Option<i64>,
    pub(super) last_snapshot: Option<Instant>,
    pub(super) trade_stats: Option<PairTradeStats>,
    pub(super) maintenance: Option<String>,
    pub(super) shutdown: Option<ShutdownStatus>,
}

#[derive(Debug, Serialize)]
pub(super) struct StatusPosition {
    pub(super) symbol: String,
    pub(super) side: String,
    pub(super) size: String,
    pub(super) entry_price: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct StatusSnapshot {
    pub(super) ts: i64,
    pub(super) updated_at: String,
    pub(super) id: Option<String>,
    pub(super) agent: Option<String>,
    pub(super) dex: String,
    pub(super) dry_run: bool,
    pub(super) backtest_mode: bool,
    pub(super) interval_secs: u64,
    pub(super) positions_ready: bool,
    pub(super) position_count: usize,
    pub(super) has_position: bool,
    pub(super) positions: Vec<StatusPosition>,
    pub(super) pnl_total: f64,
    pub(super) pnl_today: f64,
    pub(super) pnl_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) trade_stats: Option<PairTradeStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) maintenance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) shutdown: Option<ShutdownStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) error_summary: Option<ErrorSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct PairTradeStats {
    pub(super) trades: u64,
    pub(super) wins: u64,
    pub(super) win_rate: f64,
    pub(super) max_dd: f64,
    pub(super) pnl: f64,
}

/// Graceful shutdown status surfaced in the status snapshot so the
/// dashboard can show when the bot is winding down and when each open
/// leg will be auto-flushed by `force_close_secs`. See pairtrade#6.
#[derive(Debug, Clone, Serialize)]
pub(super) struct ShutdownStatus {
    pub(super) pending: bool,
    /// Unix timestamp (s) at which the grace window expires and any
    /// remaining positions will be force-closed unconditionally.
    pub(super) grace_deadline_ts: i64,
    /// Earliest force_close ETA across all open positions (Unix ts, s).
    /// None when there are no open positions at shutdown start.
    pub(super) force_close_eta_ts: Option<i64>,
    pub(super) positions: Vec<ShutdownPosition>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ShutdownPosition {
    pub(super) key: String,
    pub(super) entered_ts: i64,
    pub(super) force_close_eta_ts: i64,
}

impl StatusReporter {
    /// Per-instance variant of `from_env` for the multi-strategy
    /// single-process architecture (shigeo-nakamura/bot-strategy#25).
    ///
    /// When `multi_instance == false`, returns exactly what `from_env`
    /// returns so single-bot deployments keep writing to the same
    /// `<dir>/<DEBOT_STATUS_ID>/status.json` path and the dashboard
    /// keeps finding it.
    ///
    /// When `multi_instance == true`, the on-disk directory is
    /// suffixed with `-{instance_id}` so each strategy variant has its
    /// own status.json that the dashboard can subscribe to via a
    /// separate `status_path` entry.
    pub(super) fn from_env_for_instance(
        cfg: &PairTradeConfig,
        instance_id: &str,
        multi_instance: bool,
    ) -> Option<Self> {
        let reporter = Self::from_env(cfg)?;
        if !multi_instance {
            return Some(reporter);
        }
        let suffix = sanitize_pnl_tag(instance_id);
        if suffix.is_empty() {
            return Some(reporter);
        }
        let mut reporter = reporter;
        // Rewrite the on-disk parent directory to include the instance
        // suffix. The original layout is `<dir>/<id>/status.json`; the
        // new layout is `<dir>/<id>-<instance>/status.json`. When `id`
        // is None we degrade to `<dir>/<instance>/status.json`.
        if let Some(parent) = reporter.path.parent() {
            let last = parent
                .file_name()
                .map(|os| os.to_string_lossy().into_owned())
                .unwrap_or_default();
            let new_last = if last.is_empty() {
                suffix.clone()
            } else {
                format!("{last}-{suffix}")
            };
            let grand = parent.parent().map(PathBuf::from).unwrap_or_default();
            let new_parent = grand.join(new_last);
            let file_name = reporter
                .path
                .file_name()
                .map(|os| os.to_string_lossy().into_owned())
                .unwrap_or_else(|| "status.json".to_string());
            reporter.path = new_parent.join(file_name);
        }
        // Keep auxiliary files (`equity.json`, `equity_history.jsonl`)
        // co-located with the rewritten status.json.
        reporter.equity_baseline_path = reporter.path.with_extension("equity.json");
        reporter.equity_history_path = reporter.path.with_extension("equity_history.jsonl");
        reporter.id = Some(match reporter.id.take() {
            Some(prev) if !prev.is_empty() => format!("{prev}-{suffix}"),
            _ => suffix,
        });
        Some(reporter)
    }

    pub(super) fn from_env(cfg: &PairTradeConfig) -> Option<Self> {
        let enabled = env::var("DEBOT_STATUS_ENABLED")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no")
            })
            .unwrap_or(true);
        if !enabled {
            return None;
        }

        let id = env::var("DEBOT_STATUS_ID")
            .ok()
            .map(|v| sanitize_pnl_tag(&v))
            .filter(|v| !v.is_empty());

        let path = env::var("DEBOT_STATUS_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                env::var("DEBOT_STATUS_DIR")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .map(PathBuf::from)
                    .map(|dir| match &id {
                        Some(id) => dir.join(id).join("status.json"),
                        None => dir.join("status.json"),
                    })
            })
            .or_else(|| {
                env::var("HOME")
                    .ok()
                    .map(|home| PathBuf::from(home).join("debot_status"))
                    .map(|base| match &id {
                        Some(id) => base.join(id).join("status.json"),
                        None => base.join("status.json"),
                    })
            })
            .unwrap_or_else(|| PathBuf::from("status.json"));

        let equity_baseline_path = path.with_extension("equity.json");
        let equity_history_path = path.with_extension("equity_history.jsonl");
        let interval_secs = cfg.interval_secs.max(1);
        let snapshot_every = {
            let target_secs = 60_u64;
            let n = ((target_secs + interval_secs - 1) / interval_secs).max(1);
            Duration::from_secs(interval_secs.saturating_mul(n).max(1))
        };

        let mut reporter = Self {
            path,
            id,
            agent: cfg.agent_name.clone(),
            dex: cfg.dex_name.clone(),
            dry_run: cfg.dry_run,
            backtest_mode: cfg.backtest_mode,
            interval_secs: cfg.interval_secs,
            snapshot_every,
            pnl_total: 0.0,
            pnl_today: 0.0,
            pnl_today_date: Utc::now().date_naive(),
            equity_day_start: 0.0,
            equity_day_start_set: false,
            equity_baseline_path,
            equity_history_path,
            last_equity_history_ts: None,
            last_snapshot: None,
            trade_stats: Some(PairTradeStats {
                trades: 0,
                wins: 0,
                win_rate: 0.0,
                max_dd: 0.0,
                pnl: 0.0,
            }),
            maintenance: None,
            shutdown: None,
        };
        reporter.load_equity_baseline();
        if let Err(err) = reporter.ensure_status_file() {
            log::warn!(
                "[STATUS] failed to create status file {}: {:?}",
                reporter.path.display(),
                err
            );
        }
        Some(reporter)
    }

    pub(super) fn ensure_status_file(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }

    pub(super) fn load_equity_baseline(&mut self) {
        let Ok(payload) = fs::read_to_string(&self.equity_baseline_path) else {
            return;
        };
        let Ok(baseline) = serde_json::from_str::<EquityBaseline>(&payload) else {
            return;
        };
        let Ok(date) = NaiveDate::parse_from_str(&baseline.date, "%Y-%m-%d") else {
            return;
        };
        self.equity_day_start = baseline.equity;
        self.pnl_today_date = date;
        self.equity_day_start_set = true;
    }

    pub(super) fn persist_equity_baseline(&self) {
        let baseline = EquityBaseline {
            date: self.pnl_today_date.format("%Y-%m-%d").to_string(),
            equity: self.equity_day_start,
        };
        let payload = match serde_json::to_string(&baseline) {
            Ok(v) => v,
            Err(err) => {
                log::warn!("[STATUS] failed to encode equity baseline: {:?}", err);
                return;
            }
        };
        if let Some(parent) = self.equity_baseline_path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                log::warn!("[STATUS] failed to create equity baseline dir: {:?}", err);
                return;
            }
        }
        let tmp_path = self.equity_baseline_path.with_extension("equity.json.tmp");
        if let Err(err) = fs::write(&tmp_path, payload) {
            log::warn!("[STATUS] failed to write equity baseline: {:?}", err);
            return;
        }
        if let Err(err) = fs::rename(&tmp_path, &self.equity_baseline_path) {
            log::warn!("[STATUS] failed to finalize equity baseline: {:?}", err);
        }
    }

    pub(super) fn append_equity_history(&mut self, equity: f64) {
        let ts = Utc::now().timestamp_millis();
        if self.last_equity_history_ts == Some(ts) {
            return;
        }
        self.last_equity_history_ts = Some(ts);
        let point = EquityHistoryPoint { ts, equity };
        let line = match serde_json::to_string(&point) {
            Ok(v) => v,
            Err(err) => {
                log::warn!("[STATUS] failed to encode equity history: {:?}", err);
                return;
            }
        };
        if let Some(parent) = self.equity_history_path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                log::warn!("[STATUS] failed to create equity history dir: {:?}", err);
                return;
            }
        }
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.equity_history_path)
        {
            Ok(f) => f,
            Err(err) => {
                log::warn!("[STATUS] failed to open equity history: {:?}", err);
                return;
            }
        };
        if writeln!(file, "{line}").is_err() {
            log::warn!("[STATUS] failed to write equity history");
        }
    }

    pub(super) fn update_equity(&mut self, equity: f64) {
        let today = Utc::now().date_naive();
        self.pnl_total = equity;
        if !self.equity_day_start_set || self.pnl_today_date != today {
            self.pnl_today_date = today;
            self.equity_day_start = equity;
            self.equity_day_start_set = true;
            self.persist_equity_baseline();
        }
        if self.equity_day_start_set {
            self.pnl_today = equity - self.equity_day_start;
        }
        self.append_equity_history(equity);
    }

    pub(super) fn set_maintenance(&mut self, status: Option<String>) {
        self.maintenance = status;
    }

    pub(super) fn set_shutdown_status(&mut self, status: Option<ShutdownStatus>) {
        self.shutdown = status;
    }

    pub(super) fn write_snapshot(
        &mut self,
        open_positions: &HashMap<String, PositionSnapshot>,
        positions_ready: bool,
    ) -> std::io::Result<()> {
        self.reset_daily_if_needed();
        let positions: Vec<StatusPosition> = open_positions
            .values()
            .filter(|pos| pos.sign != 0 && pos.size > Decimal::ZERO)
            .map(|pos| StatusPosition {
                symbol: pos.symbol.clone(),
                side: match pos.sign.cmp(&0) {
                    Ordering::Greater => "LONG".to_string(),
                    Ordering::Less => "SHORT".to_string(),
                    Ordering::Equal => "FLAT".to_string(),
                },
                size: pos.size.to_string(),
                entry_price: pos.entry_price.map(|v| v.to_string()),
            })
            .collect();
        let snapshot = StatusSnapshot {
            ts: Utc::now().timestamp(),
            updated_at: Utc::now().to_rfc3339(),
            id: self.id.clone(),
            agent: self.agent.clone(),
            dex: self.dex.clone(),
            dry_run: self.dry_run,
            backtest_mode: self.backtest_mode,
            interval_secs: self.interval_secs,
            positions_ready,
            position_count: positions.len(),
            has_position: !positions.is_empty(),
            positions,
            pnl_total: self.pnl_total,
            pnl_today: self.pnl_today,
            pnl_source: "equity".to_string(),
            trade_stats: self.trade_stats.clone(),
            maintenance: self.maintenance.clone(),
            shutdown: self.shutdown.clone(),
            error_summary: error_counter::global().map(|h| h.snapshot()),
        };
        let payload = serde_json::to_string(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self.path.with_extension("json.tmp");
        fs::write(&tmp_path, payload)?;
        fs::rename(tmp_path, &self.path)?;
        Ok(())
    }

    pub(super) fn write_snapshot_if_due(
        &mut self,
        open_positions: &HashMap<String, PositionSnapshot>,
        positions_ready: bool,
    ) -> std::io::Result<bool> {
        let due = self
            .last_snapshot
            .map(|t| t.elapsed() >= self.snapshot_every)
            .unwrap_or(true);
        if !due {
            return Ok(false);
        }
        self.write_snapshot(open_positions, positions_ready)?;
        self.last_snapshot = Some(Instant::now());
        Ok(true)
    }

    pub(super) fn reset_daily_if_needed(&mut self) {
        if !self.equity_day_start_set {
            return;
        }
        let today = Utc::now().date_naive();
        if today != self.pnl_today_date {
            self.pnl_today_date = today;
            self.equity_day_start = self.pnl_total;
            self.persist_equity_baseline();
        }
        self.pnl_today = self.pnl_total - self.equity_day_start;
    }
}
