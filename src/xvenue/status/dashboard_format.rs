//! Wire-format structs for the dashboard's `status.json` payload.
//!
//! Mirrors `debot-dashboard/main.go::StatusData` (and the pairtrade-
//! shared subset). Pulled out of `status.rs` so the on-disk format
//! sits next to its serde-derive metadata, separate from the
//! `StatusReporter` lifecycle / file-I/O code.
//!
//! All structs are `#[derive(Serialize)]`-only — the dashboard reads
//! one direction (bot → dashboard). The test module in
//! `super::tests` defines its own `Deserialize`-side mirror for
//! round-trip coverage.

use serde::Serialize;

use crate::error_counter::ErrorSummary;
use crate::risk::manager::{
    CircuitBreakerSnapshot, DailyRiskSnapshot, RiskHistoryEvent, SessionRiskSnapshot,
};

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
    /// Free-form tag identifying that the venue has been detected as
    /// in/upcoming maintenance (e.g. `"upcoming_or_active"`). The
    /// error-watch workflow gates on `maintenance != null` to suppress
    /// false-positive issue creation while the bot is correctly blocked.
    /// Mirrors pairtrade `status.rs::StatusSnapshot.maintenance`. See
    /// bot-strategy#321.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<String>,

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
