//! `status.json` emitter for the live runner. bot-strategy#244 Group A.
//!
//! Emits a pairtrade-shaped status snapshot the dashboard already
//! understands (`debot-dashboard/main.go::StatusData`) plus xvenue-arb
//! extensions per `docs/DESIGN.md` §7:
//!
//! - per-venue WS health (`lt_ws_age_ms`, `ext_ws_age_ms`)
//! - per-leg `last_fill_ts`
//! - bounded `recent_taker_fills` ring
//! - `spread_series` snapshot for the chart
//!
//! Risk gates (`daily_risk` / `session_risk` / `circuit_breaker` /
//! `risk_history`) are surfaced as plumbed-but-empty for now; Group D
//! fills them in. Doing so keeps the dashboard happy with one schema
//! across both fleets and avoids a follow-up status.rs churn when the
//! risk modules land.
//!
//! On-disk path layout matches pairtrade so the dashboard's existing
//! `status_path` config knob points at the same template:
//!     `<DEBOT_STATUS_DIR>/<DEBOT_STATUS_ID>/status.json`
//!
//! Writes are atomic (`tmpfile + rename`).

use std::collections::VecDeque;
use std::env;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::error_counter::{self, ErrorSummary};
use crate::risk::manager::{
    CircuitBreakerSnapshot, DailyRiskSnapshot, RiskHistoryEvent, SessionRiskSnapshot,
};

use super::config::XvenueConfig;
use super::signal::SpreadDirection;
use super::state::{Phase, PositionMachine};

/// Bounded `spread_series` capacity. The dashboard chart shows a few
/// minutes of dev_bps; 300 samples at 1s bucket = 5 min, plenty for
/// the strip and small enough to keep the JSON payload sub-50 KB.
const SPREAD_SERIES_CAP: usize = 300;

/// Bounded `recent_taker_fills` capacity. xvenue-arb's trade rate is
/// O(10k/yr); 50 entries holds roughly the last day of taker fallbacks
/// without bloating the payload.
const RECENT_TAKER_FILLS_CAP: usize = 50;

/// Default snapshot cadence target. Picked at 60 s to match pairtrade
/// — the dashboard polls each target on a 20 s loop, so any cadence
/// ≤ 60 s guarantees two fresh snapshots per poll cycle.
const DEFAULT_SNAPSHOT_INTERVAL_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize)]
pub struct StatusPosition {
    pub symbol: String,
    pub side: String,
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_price: Option<String>,
}

/// Per-venue WS / fill view for the dashboard. None values render as
/// "no data yet" so the panel stays informative through warmup.
#[derive(Debug, Clone, Serialize)]
pub struct VenueState {
    pub venue: &'static str,
    /// ms since the last `book_ok=true` read on this venue. None until
    /// we've seen at least one healthy book.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ws_age_ms: Option<u64>,
    /// Unix ts (s) of the last fill on this leg. Phase 2 paper fills
    /// populate this so the field exercises end-to-end before Group B
    /// binds real orders.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fill_ts: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TakerFillRecord {
    pub ts: i64,
    pub venue: &'static str,
    pub qty: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpreadPoint {
    pub ts_ms: u64,
    pub dev_bps: f64,
}

/// Round-trip trade counter mirrored from pairtrade's `PairTradeStats`
/// and the dashboard's `TradeStats` (debot-dashboard/main.go). Emitted
/// only after the first close so a fresh boot doesn't surface "0
/// trades" before any signal has fired. Paper round-trips count too —
/// during DRY_RUN this surfaces the strategy's exit cadence on the
/// dashboard. Group B will start passing realized USD into
/// `record_close` once real fills land.
#[derive(Debug, Clone, Serialize)]
pub struct TradeStats {
    pub trades: u64,
    pub wins: u64,
    pub win_rate: f64,
    pub max_dd: f64,
    pub pnl: f64,
}

/// Inline shape for the dashboard's StatusData. Only the fields Group A
/// fills are present; risk gates / shutdown surface as `None` /
/// `serde_skip_if_none` so they don't clutter the payload but stay
/// schema-compatible with pairtrade for parity rendering.
#[derive(Debug, Serialize)]
pub struct StatusSnapshot {
    pub ts: i64,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub dex: String,
    pub dry_run: bool,
    pub backtest_mode: bool,
    pub interval_secs: u64,
    pub positions_ready: bool,
    pub position_count: usize,
    pub has_position: bool,
    pub positions: Vec<StatusPosition>,
    pub pnl_total: f64,
    pub pnl_today: f64,
    pub pnl_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_stats: Option<TradeStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_summary: Option<ErrorSummary>,

    // ---- xvenue-arb extensions (DESIGN.md §7) ----
    pub venues: Vec<VenueState>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub recent_taker_fills: Vec<TakerFillRecord>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub spread_series: Vec<SpreadPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_dev_bps: Option<f64>,
    pub samples_committed: u64,

    // ---- Risk gates (#244 D-2..D-7). All optional — emitted only
    // when the manager has populated them so a fresh boot does not
    // surface noisy zeros to the dashboard. ----
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_risk: Option<DailyRiskSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_risk: Option<SessionRiskSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerSnapshot>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub risk_history: Vec<RiskHistoryEvent>,
}

/// Persisted equity baseline. One file per status_path, same shape as
/// pairtrade. Lets the bot reload `equity_day_start` after a restart so
/// `pnl_today` doesn't reset to zero each boot.
#[derive(Debug, Serialize, Deserialize)]
struct EquityBaseline {
    date: String,
    equity: f64,
}

pub struct StatusReporter {
    path: PathBuf,
    equity_baseline_path: PathBuf,

    id: Option<String>,
    agent: Option<String>,
    dex: String,
    dry_run: bool,
    backtest_mode: bool,
    interval_secs: u64,
    snapshot_every: Duration,

    // PnL bookkeeping (equity-based, matches pairtrade).
    pnl_total: f64,
    pnl_today: f64,
    pnl_today_date: NaiveDate,
    equity_day_start: f64,
    equity_day_start_set: bool,

    // Per-venue health tracked from the live loop.
    last_ext_book_ok_ms: Option<u64>,
    last_lt_book_ok_ms: Option<u64>,
    last_ext_fill_ts: Option<i64>,
    last_lt_fill_ts: Option<i64>,

    recent_taker_fills: VecDeque<TakerFillRecord>,
    spread_series: VecDeque<SpreadPoint>,
    last_dev_bps: Option<f64>,
    samples_committed: u64,

    // Round-trip trade counters surfaced as `trade_stats`. Cleared on
    // boot — the dashboard treats an absent `trade_stats` field as
    // "no data yet" so a fresh restart doesn't show 0/0/0%/$0 until the
    // first close happens. Paper closes increment too; Group B replaces
    // the 0.0 PnL passed by `record_close` with realized USD.
    trades_count: u64,
    wins_count: u64,
    total_pnl: f64,
    peak_pnl: f64,
    max_dd: f64,

    last_snapshot: Option<Instant>,

    // Risk-manager-supplied snapshots. Set via `set_risk_*` from the
    // live loop so this module stays free of risk-domain concerns.
    daily_risk: Option<DailyRiskSnapshot>,
    session_risk: Option<SessionRiskSnapshot>,
    circuit_breaker: Option<CircuitBreakerSnapshot>,
    risk_history: Vec<RiskHistoryEvent>,
}

impl StatusReporter {
    /// Builds a reporter from `XvenueConfig` + `DEBOT_STATUS_*` env. Returns
    /// `None` when status emission is explicitly disabled
    /// (`DEBOT_STATUS_ENABLED=0`) so the runner can opt-out cleanly.
    pub fn from_env(cfg: &XvenueConfig) -> Option<Self> {
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
            .map(|v| sanitize_id(&v))
            .filter(|v| !v.is_empty());

        let path = resolve_path(&id);
        let equity_baseline_path = path.with_extension("equity.json");

        let interval_secs = DEFAULT_SNAPSHOT_INTERVAL_SECS;
        let snapshot_every = Duration::from_secs(interval_secs.max(1));

        // dex tag mirrors what pairtrade emits — the dashboard groups
        // cards by the field, so picking a consistent label here keeps
        // xvenue-arb in its own row instead of merging into pairtrade.
        let dex = "lighter+extended".to_string();

        let mut reporter = Self {
            path,
            equity_baseline_path,
            id,
            agent: Some(cfg.agent_name.clone()),
            dex,
            dry_run: cfg.dry_run,
            backtest_mode: false,
            interval_secs,
            snapshot_every,
            pnl_total: 0.0,
            pnl_today: 0.0,
            pnl_today_date: Utc::now().date_naive(),
            equity_day_start: 0.0,
            equity_day_start_set: false,
            last_ext_book_ok_ms: None,
            last_lt_book_ok_ms: None,
            last_ext_fill_ts: None,
            last_lt_fill_ts: None,
            recent_taker_fills: VecDeque::with_capacity(RECENT_TAKER_FILLS_CAP),
            spread_series: VecDeque::with_capacity(SPREAD_SERIES_CAP),
            last_dev_bps: None,
            samples_committed: 0,
            trades_count: 0,
            wins_count: 0,
            total_pnl: 0.0,
            peak_pnl: 0.0,
            max_dd: 0.0,
            last_snapshot: None,
            daily_risk: None,
            session_risk: None,
            circuit_breaker: None,
            risk_history: Vec::new(),
        };
        reporter.load_equity_baseline();
        if let Err(err) = reporter.ensure_status_file() {
            log::warn!(
                "[STATUS] failed to ensure status file {}: {:?}",
                reporter.path.display(),
                err
            );
        }
        Some(reporter)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Forces the next `write_snapshot_if_due` call to emit, regardless
    /// of cadence. Used at boot so the dashboard sees the DRY_RUN pill
    /// without waiting for the first 60 s tick.
    pub fn mark_dirty(&mut self) {
        self.last_snapshot = None;
    }

    /// Records a healthy `book_ok=true` read for a venue. Drives
    /// `ws_age_ms` in the snapshot.
    pub fn record_book_ok(&mut self, ext_ts_ms: Option<u64>, lt_ts_ms: Option<u64>) {
        if let Some(ts) = ext_ts_ms {
            self.last_ext_book_ok_ms = Some(ts);
        }
        if let Some(ts) = lt_ts_ms {
            self.last_lt_book_ok_ms = Some(ts);
        }
    }

    /// Records a leg fill (synthetic in dry-run, real once Group B
    /// binds orders). Drives `last_fill_ts` per venue.
    pub fn record_fill(&mut self, ext: bool, lt: bool, now_ts_ms: u64) {
        let ts = (now_ts_ms / 1000) as i64;
        if ext {
            self.last_ext_fill_ts = Some(ts);
        }
        if lt {
            self.last_lt_fill_ts = Some(ts);
        }
    }

    /// Records a round-trip close. `pnl` is realized USD for that
    /// round-trip; in DRY_RUN the runner passes 0.0 today so the
    /// counter still ticks while leaving paper PnL flat. Group B
    /// passes realized USD once orders flow.
    pub fn record_close(&mut self, pnl: f64) {
        self.trades_count += 1;
        self.total_pnl += pnl;
        if pnl > 0.0 {
            self.wins_count += 1;
        }
        if self.total_pnl > self.peak_pnl {
            self.peak_pnl = self.total_pnl;
        }
        let dd = self.peak_pnl - self.total_pnl;
        if dd > self.max_dd {
            self.max_dd = dd;
        }
    }

    /// Pushes one (ts, dev_bps) sample into the series. Caller decides
    /// when a sample is interesting — we just bound the buffer.
    pub fn push_spread_point(&mut self, ts_ms: u64, dev_bps: f64) {
        if self.spread_series.len() >= SPREAD_SERIES_CAP {
            self.spread_series.pop_front();
        }
        self.spread_series.push_back(SpreadPoint { ts_ms, dev_bps });
        self.last_dev_bps = Some(dev_bps);
    }

    pub fn record_samples_committed(&mut self, n: u64) {
        self.samples_committed = n;
    }

    pub fn set_daily_risk(&mut self, v: Option<DailyRiskSnapshot>) {
        self.daily_risk = v;
    }

    pub fn set_session_risk(&mut self, v: Option<SessionRiskSnapshot>) {
        self.session_risk = v;
    }

    pub fn set_circuit_breaker(&mut self, v: Option<CircuitBreakerSnapshot>) {
        self.circuit_breaker = v;
    }

    pub fn set_risk_history(&mut self, v: Vec<RiskHistoryEvent>) {
        self.risk_history = v;
    }

    /// Updates equity → PnL bookkeeping. Same contract as pairtrade:
    /// `pnl_total` is the running equity, `pnl_today` is delta vs
    /// `equity_day_start` (rolled at UTC midnight).
    pub fn update_equity(&mut self, equity: f64) {
        self.pnl_total = equity;
        let today = Utc::now().date_naive();
        if !self.equity_day_start_set || self.pnl_today_date != today {
            self.pnl_today_date = today;
            self.equity_day_start = equity;
            self.equity_day_start_set = true;
            self.persist_equity_baseline();
        }
        if self.equity_day_start_set {
            self.pnl_today = equity - self.equity_day_start;
        }
    }

    /// Writes the snapshot if the cadence interval has elapsed.
    /// Returns `Ok(true)` if a write happened.
    pub fn write_snapshot_if_due(
        &mut self,
        machine: &PositionMachine,
        now_ts_ms: u64,
    ) -> std::io::Result<bool> {
        let due = self
            .last_snapshot
            .map(|t| t.elapsed() >= self.snapshot_every)
            .unwrap_or(true);
        if !due {
            return Ok(false);
        }
        self.write_snapshot(machine, now_ts_ms)?;
        self.last_snapshot = Some(Instant::now());
        Ok(true)
    }

    /// Forces a write regardless of cadence. Tests use this; runtime
    /// path goes through `write_snapshot_if_due`.
    pub fn write_snapshot(
        &mut self,
        machine: &PositionMachine,
        now_ts_ms: u64,
    ) -> std::io::Result<()> {
        let snapshot = self.build_snapshot(machine, now_ts_ms);
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

    fn build_snapshot(&self, machine: &PositionMachine, now_ts_ms: u64) -> StatusSnapshot {
        let positions = render_positions(machine);
        let position_count = positions.len();
        let has_position = position_count > 0;

        let venues = vec![
            VenueState {
                venue: "extended",
                ws_age_ms: self
                    .last_ext_book_ok_ms
                    .map(|t| now_ts_ms.saturating_sub(t)),
                last_fill_ts: self.last_ext_fill_ts,
            },
            VenueState {
                venue: "lighter",
                ws_age_ms: self
                    .last_lt_book_ok_ms
                    .map(|t| now_ts_ms.saturating_sub(t)),
                last_fill_ts: self.last_lt_fill_ts,
            },
        ];

        StatusSnapshot {
            ts: Utc::now().timestamp(),
            updated_at: Utc::now().to_rfc3339(),
            id: self.id.clone(),
            agent: self.agent.clone(),
            dex: self.dex.clone(),
            dry_run: self.dry_run,
            backtest_mode: self.backtest_mode,
            interval_secs: self.interval_secs,
            // The bot has positions tracked once both connectors have
            // reported at least one healthy book. The state machine
            // owns the actual position; positions_ready=true once
            // we've seen any book at all on each venue.
            positions_ready: self.last_ext_book_ok_ms.is_some()
                && self.last_lt_book_ok_ms.is_some(),
            position_count,
            has_position,
            positions,
            pnl_total: self.pnl_total,
            pnl_today: self.pnl_today,
            pnl_source: "equity".to_string(),
            trade_stats: (self.trades_count > 0).then(|| TradeStats {
                trades: self.trades_count,
                wins: self.wins_count,
                win_rate: self.wins_count as f64 / self.trades_count as f64 * 100.0,
                max_dd: self.max_dd,
                pnl: self.total_pnl,
            }),
            error_summary: error_counter::global().map(|h| h.snapshot()),
            venues,
            recent_taker_fills: self.recent_taker_fills.iter().cloned().collect(),
            spread_series: self.spread_series.iter().cloned().collect(),
            current_dev_bps: self.last_dev_bps,
            samples_committed: self.samples_committed,
            daily_risk: self.daily_risk.clone(),
            session_risk: self.session_risk.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            risk_history: self.risk_history.clone(),
        }
    }

    fn ensure_status_file(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }

    fn load_equity_baseline(&mut self) {
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

    fn persist_equity_baseline(&self) {
        let baseline = EquityBaseline {
            date: self.pnl_today_date.format("%Y-%m-%d").to_string(),
            equity: self.equity_day_start,
        };
        let Ok(payload) = serde_json::to_string(&baseline) else {
            return;
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
}

fn render_positions(machine: &PositionMachine) -> Vec<StatusPosition> {
    let Some(p) = machine.position() else {
        return Vec::new();
    };
    // EmergencyFlattening with both legs zero is reported as flat for
    // dashboard purposes — the operator card already surfaces the
    // emergency reason via `error_summary` / risk panels.
    if p.extended_open_qty.is_zero() && p.lighter_open_qty.is_zero() {
        return Vec::new();
    }

    // The strategy direction (`Long` ⇒ Lighter LONG / Extended SHORT)
    // is reported per leg so the dashboard's "side" column matches
    // each venue's signed position rather than a single strategy
    // direction the operator would have to mentally flip.
    let phase = machine.phase();
    let label = phase_label(phase);
    let (ext_side, lt_side) = match p.direction {
        SpreadDirection::Long => ("SHORT", "LONG"),
        SpreadDirection::Short => ("LONG", "SHORT"),
    };
    let mut out = Vec::new();
    if p.extended_open_qty > Decimal::ZERO {
        out.push(StatusPosition {
            symbol: format!("EXT:{}", label),
            side: ext_side.to_string(),
            size: p.extended_open_qty.to_string(),
            entry_price: None,
        });
    }
    if p.lighter_open_qty > Decimal::ZERO {
        out.push(StatusPosition {
            symbol: format!("LT:{}", label),
            side: lt_side.to_string(),
            size: p.lighter_open_qty.to_string(),
            entry_price: None,
        });
    }
    out
}

fn phase_label(phase: Phase) -> &'static str {
    match phase {
        Phase::Flat => "FLAT",
        Phase::EnteringExtended => "ENTERING_EXT",
        Phase::EnteringLighter => "ENTERING_LT",
        Phase::Held => "HELD",
        Phase::Exiting => "EXITING",
        Phase::EmergencyFlattening => "EMERGENCY_FLATTEN",
    }
}

fn resolve_path(id: &Option<String>) -> PathBuf {
    if let Ok(p) = env::var("DEBOT_STATUS_PATH") {
        if !p.trim().is_empty() {
            return PathBuf::from(p);
        }
    }
    let dir = env::var("DEBOT_STATUS_DIR")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| env::var("HOME").ok().map(|h| PathBuf::from(h).join("debot_status")))
        .unwrap_or_else(|| PathBuf::from("."));
    match id {
        Some(id) => dir.join(id).join("status.json"),
        None => dir.join("status.json"),
    }
}

fn sanitize_id(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter_map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                Some(c)
            } else if c == ' ' {
                Some('-')
            } else {
                None
            }
        })
        .collect()
}

/// `Decimal::to_f64`-with-fallback. Used by the live runner to convert
/// a summed equity (Decimal) into the `f64` field the dashboard struct
/// expects without panicking on the impossible NaN case.
pub fn equity_decimal_to_f64(equity: Decimal) -> f64 {
    equity.to_f64().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xvenue::signal::SpreadDirection;
    use crate::xvenue::state::Event;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use tempfile::TempDir;

    /// Cargo runs tests in parallel; every test in this module mutates
    /// `DEBOT_STATUS_*` env vars, which are process-global. Hold this
    /// mutex across the env-write + reporter-build to keep them
    /// deterministic. Poisoning is recovered transparently — a panic
    /// in one test should not cascade.
    fn env_guard() -> MutexGuard<'static, ()> {
        static M: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = M.get_or_init(|| Mutex::new(()));
        match lock.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn min_cfg() -> XvenueConfig {
        let yaml = r#"
agent_name: test
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 100
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
"#;
        let c: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        c
    }

    fn reporter_in(dir: &TempDir, cfg: &XvenueConfig) -> StatusReporter {
        // Caller must hold env_guard() across this + the test body.
        let path = dir.path().join("status.json");
        std::env::set_var("DEBOT_STATUS_ENABLED", "1");
        std::env::set_var("DEBOT_STATUS_PATH", &path);
        std::env::remove_var("DEBOT_STATUS_DIR");
        std::env::remove_var("DEBOT_STATUS_ID");
        StatusReporter::from_env(cfg).expect("reporter built")
    }

    #[test]
    fn from_env_disabled_returns_none() {
        let _g = env_guard();
        std::env::set_var("DEBOT_STATUS_ENABLED", "0");
        let r = StatusReporter::from_env(&min_cfg());
        std::env::remove_var("DEBOT_STATUS_ENABLED");
        assert!(r.is_none());
    }

    #[test]
    fn snapshot_includes_dry_run_and_baseline_fields() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        r.update_equity(1_000.0);
        r.record_book_ok(Some(1_000), Some(1_000));
        let machine = PositionMachine::new();
        r.write_snapshot(&machine, 1_500).unwrap();
        let raw = std::fs::read_to_string(r.path()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v.get("dry_run").and_then(|x| x.as_bool()), Some(false));
        assert_eq!(v.get("dex").and_then(|x| x.as_str()), Some("lighter+extended"));
        assert_eq!(
            v.get("agent").and_then(|x| x.as_str()),
            Some("test")
        );
        assert_eq!(v.get("pnl_total").and_then(|x| x.as_f64()), Some(1_000.0));
        assert_eq!(v.get("pnl_today").and_then(|x| x.as_f64()), Some(0.0));
        assert_eq!(
            v.get("pnl_source").and_then(|x| x.as_str()),
            Some("equity")
        );
        assert_eq!(v.get("position_count").and_then(|x| x.as_u64()), Some(0));
        assert_eq!(v.get("has_position").and_then(|x| x.as_bool()), Some(false));
        assert!(v.get("ts").is_some());
        assert!(v.get("updated_at").is_some());
        assert!(v.get("venues").is_some());
    }

    #[test]
    fn trade_stats_absent_until_first_close_then_aggregates_pnl() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        let machine = PositionMachine::new();

        // Pre-close: trade_stats omitted so the dashboard renders "no
        // data yet" rather than 0/0/0%/$0 on a fresh boot.
        r.write_snapshot(&machine, 0).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(r.path()).unwrap()).unwrap();
        assert!(v.get("trade_stats").is_none());

        // 3 wins + 1 loss interleaved so the running-peak max_dd is
        // exercised: after the -1.5 close total dips to -0.5 from a
        // peak of +1.0 → max_dd=1.5; subsequent wins lift past that
        // peak but max_dd stays at the recorded high-water mark.
        r.record_close(1.0);
        r.record_close(-1.5);
        r.record_close(2.0);
        r.record_close(0.5);

        r.write_snapshot(&machine, 1_000).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(r.path()).unwrap()).unwrap();
        let ts = v.get("trade_stats").expect("trade_stats present");
        assert_eq!(ts.get("trades").and_then(|x| x.as_u64()), Some(4));
        assert_eq!(ts.get("wins").and_then(|x| x.as_u64()), Some(3));
        assert_eq!(ts.get("win_rate").and_then(|x| x.as_f64()), Some(75.0));
        assert_eq!(ts.get("pnl").and_then(|x| x.as_f64()), Some(2.0));
        assert_eq!(ts.get("max_dd").and_then(|x| x.as_f64()), Some(1.5));
    }

    #[test]
    fn dry_run_flag_propagates_from_config() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let yaml = r#"
agent_name: test
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 100
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
dry_run: true
"#;
        let cfg: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        let mut r = reporter_in(&tmp, &cfg);
        let machine = PositionMachine::new();
        r.write_snapshot(&machine, 0).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(r.path()).unwrap()).unwrap();
        assert_eq!(v.get("dry_run").and_then(|x| x.as_bool()), Some(true));
    }

    #[test]
    fn ws_age_ms_reflects_book_ok_ts() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        r.record_book_ok(Some(1_000), Some(2_000));
        let machine = PositionMachine::new();
        r.write_snapshot(&machine, 5_000).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(r.path()).unwrap()).unwrap();
        let venues = v.get("venues").and_then(|v| v.as_array()).unwrap();
        let by_venue: HashMap<&str, &serde_json::Value> = venues
            .iter()
            .filter_map(|x| {
                x.get("venue")
                    .and_then(|s| s.as_str())
                    .map(|n| (n, x))
            })
            .collect();
        assert_eq!(
            by_venue["extended"].get("ws_age_ms").and_then(|x| x.as_u64()),
            Some(4_000)
        );
        assert_eq!(
            by_venue["lighter"].get("ws_age_ms").and_then(|x| x.as_u64()),
            Some(3_000)
        );
    }

    #[test]
    fn position_render_shows_both_legs_when_held() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        let mut machine = PositionMachine::new();
        machine
            .apply(
                1_000,
                Event::EntrySignal {
                    direction: SpreadDirection::Short,
                    notional_usd: dec!(100),
                },
            )
            .unwrap();
        machine
            .apply(1_500, Event::ExtendedFilled { qty: dec!(0.05) })
            .unwrap();
        machine
            .apply(1_800, Event::LighterFilled { qty: dec!(0.05) })
            .unwrap();
        r.write_snapshot(&machine, 2_000).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(r.path()).unwrap()).unwrap();
        let positions = v.get("positions").and_then(|v| v.as_array()).unwrap();
        assert_eq!(positions.len(), 2);
        assert_eq!(v.get("position_count").and_then(|x| x.as_u64()), Some(2));
        assert_eq!(v.get("has_position").and_then(|x| x.as_bool()), Some(true));
        // Short strategy ⇒ Extended LONG, Lighter SHORT.
        let sides: Vec<&str> = positions
            .iter()
            .filter_map(|p| p.get("side").and_then(|s| s.as_str()))
            .collect();
        assert!(sides.contains(&"LONG"));
        assert!(sides.contains(&"SHORT"));
    }

    #[test]
    fn spread_series_buffer_is_bounded() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        for i in 0..(SPREAD_SERIES_CAP + 50) {
            r.push_spread_point(i as u64 * 1_000, i as f64);
        }
        let machine = PositionMachine::new();
        r.write_snapshot(&machine, 0).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(r.path()).unwrap()).unwrap();
        let pts = v.get("spread_series").and_then(|x| x.as_array()).unwrap();
        assert_eq!(pts.len(), SPREAD_SERIES_CAP);
        // Oldest 50 dropped — first ts_ms should be 50 * 1000.
        assert_eq!(
            pts[0].get("ts_ms").and_then(|x| x.as_u64()),
            Some(50_000)
        );
    }

    #[test]
    fn equity_baseline_persists_across_reload() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        {
            let mut r = reporter_in(&tmp, &cfg);
            r.update_equity(1_000.0);
            assert_eq!(r.pnl_today, 0.0);
        }
        // New reporter, same path → baseline reloads.
        let mut r2 = reporter_in(&tmp, &cfg);
        r2.update_equity(1_050.0);
        assert!((r2.pnl_today - 50.0).abs() < 1e-9, "pnl_today={}", r2.pnl_today);
    }

    #[test]
    fn write_snapshot_if_due_respects_cadence() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        let machine = PositionMachine::new();
        // First call writes (last_snapshot is None).
        assert!(r.write_snapshot_if_due(&machine, 0).unwrap());
        // Immediate retry skips the write.
        assert!(!r.write_snapshot_if_due(&machine, 0).unwrap());
        // mark_dirty forces the next call.
        r.mark_dirty();
        assert!(r.write_snapshot_if_due(&machine, 0).unwrap());
    }

    #[test]
    fn fill_ts_per_leg_propagates() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        r.record_fill(true, false, 60_000); // ext only, t=60s
        r.record_fill(false, true, 90_000); // lt only, t=90s
        let machine = PositionMachine::new();
        r.write_snapshot(&machine, 0).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(r.path()).unwrap()).unwrap();
        let venues = v.get("venues").and_then(|v| v.as_array()).unwrap();
        let by_venue: HashMap<&str, &serde_json::Value> = venues
            .iter()
            .filter_map(|x| x.get("venue").and_then(|s| s.as_str()).map(|n| (n, x)))
            .collect();
        assert_eq!(
            by_venue["extended"].get("last_fill_ts").and_then(|x| x.as_i64()),
            Some(60)
        );
        assert_eq!(
            by_venue["lighter"].get("last_fill_ts").and_then(|x| x.as_i64()),
            Some(90)
        );
    }

    /// Inline mirror of debot-dashboard's StatusData. Lives in this
    /// test so a schema drift trips the test suite instead of waiting
    /// for the dashboard's parse to fail in production. See
    /// debot-dashboard/main.go::StatusData.
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct DashboardStatusData {
        ts: i64,
        updated_at: String,
        id: Option<String>,
        agent: Option<String>,
        dex: String,
        dry_run: bool,
        backtest_mode: bool,
        interval_secs: u64,
        positions_ready: bool,
        position_count: u64,
        has_position: bool,
        positions: Vec<DashboardPosition>,
        pnl_total: f64,
        pnl_today: f64,
        pnl_source: String,
    }

    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct DashboardPosition {
        symbol: String,
        side: String,
        size: String,
        entry_price: Option<String>,
    }

    #[test]
    fn snapshot_parses_into_dashboard_struct() {
        let _g = env_guard();
        let tmp = TempDir::new().unwrap();
        let cfg = min_cfg();
        let mut r = reporter_in(&tmp, &cfg);
        r.update_equity(500.0);
        let machine = PositionMachine::new();
        r.write_snapshot(&machine, 0).unwrap();
        let raw = std::fs::read_to_string(r.path()).unwrap();
        let parsed: DashboardStatusData =
            serde_json::from_str(&raw).expect("dashboard struct should parse cleanly");
        assert_eq!(parsed.dex, "lighter+extended");
        assert_eq!(parsed.pnl_source, "equity");
    }
}
