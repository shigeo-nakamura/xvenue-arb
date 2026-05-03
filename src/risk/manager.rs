//! Risk gates for xvenue-arb (#244 D-2..D-7).
//!
//! Single-instance counterpart to pairtrade's #185 risk module:
//!
//! - **D-2** persisted `RiskState` (`risk_state.json`) so a restart
//!   inside an active halt does not silently re-arm the bot.
//! - **D-3** daily DD limit — block new entries when realized PnL
//!   today < -`max_daily_loss_bps` of `session_start_equity`. Auto-
//!   clears at the next UTC reset.
//! - **D-4** session DD / rolling peak — sticky halt when
//!   `(peak - current) / peak > max_dd_pct`. Cleared only by the
//!   operator dropping `RISK_ACK`.
//! - **D-5** `/opt/debot/RISK_ACK` consume-and-delete. Pairtrade-
//!   symmetric path so one operator workflow drives both fleets.
//! - **D-7** `circuit_breaker` field surfaced for dashboard parity
//!   (low-priority for xvenue-arb's >=90% win profile but still
//!   useful as a secondary defence).
//!
//! Out of scope: KILL_SWITCH file (D-1, already in `live.rs`).
//! reference_guard / STUCK / WS health / skew live in their own
//! modules.

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::persistence::{load_state, persist_state};

const RISK_HISTORY_BUFFER_CAP: usize = 200;
const PERSIST_MIN_INTERVAL_SECS: u64 = 5;

/// Equity sample point, used by the rolling-peak session-DD check.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct EquitySample {
    pub ts: i64,
    pub equity: f64,
}

/// Persisted risk state. All fields are independent of the YAML
/// config — config drives **thresholds**, this struct holds
/// **observation** state. Restarting reloads this verbatim so
/// halts and counters survive.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RiskState {
    /// Consecutive-loss counter (D-7 mirror). Resets on a winning
    /// trade.
    #[serde(default)]
    pub consecutive_losses: u32,
    /// Wall-clock unix-seconds when the cooldown ends. None when no
    /// active cooldown.
    #[serde(default)]
    pub cb_until_ts: Option<i64>,
    /// Equity at the start of the current UTC session — denominator
    /// for `max_daily_loss_bps`.
    #[serde(default)]
    pub session_start_equity: f64,
    #[serde(default)]
    pub session_start_ts: i64,
    /// Realized PnL since the last UTC reset (closed cycles only).
    #[serde(default)]
    pub realized_pnl_today: f64,
    /// Sparse equity samples driving the session-DD rolling peak.
    /// Pruned to `lookback_secs` on every update so the file size is
    /// bounded by `lookback / sample_interval`.
    #[serde(default)]
    pub equity_samples: Vec<EquitySample>,
    /// Sticky session-DD halt. True == bot must not enter; cleared
    /// only via the RISK_ACK file. Survives restart.
    #[serde(default)]
    pub session_halted: bool,
    #[serde(default)]
    pub session_halt_reason: Option<String>,
    #[serde(default)]
    pub session_halt_ts: Option<i64>,
}

/// Configuration knobs for the risk gates. Built from `XvenueConfig`
/// plus a couple of operator-side env vars (paths). Kept separate
/// from the persisted state so config changes don't trigger writes.
#[derive(Debug, Clone)]
pub struct RiskConfig {
    /// `-300` = blocks at -3% of session_start_equity. 0 disables.
    pub max_daily_loss_bps: u32,
    pub daily_reset_utc_hour: u8,
    /// Session DD threshold in bps of peak equity. 0 disables.
    pub max_session_loss_bps: u32,
    pub session_dd_lookback_secs: u64,
    pub session_dd_sample_secs: u64,
    /// Tier-1 cooldown when `consecutive_losses >= tier1_threshold`.
    pub cb_tier1_threshold: u32,
    pub cb_tier1_cooldown_secs: i64,
    /// Tier-2 (longer) cooldown at the higher threshold.
    pub cb_tier2_threshold: u32,
    pub cb_tier2_cooldown_secs: i64,
    /// On-disk path for `RiskState` persistence.
    pub risk_state_path: PathBuf,
    /// Path the operator drops to clear a session-DD halt.
    pub risk_ack_path: PathBuf,
}

/// Reasons `can_enter()` returns false. Surfaced in `[RISK]` log
/// lines and the auto-issue framework's error_summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReason {
    DailyDdHalted,
    SessionDdHalted,
    CircuitBreakerCooldown,
}

impl BlockReason {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockReason::DailyDdHalted => "daily_dd",
            BlockReason::SessionDdHalted => "session_dd",
            BlockReason::CircuitBreakerCooldown => "circuit_breaker",
        }
    }
}

/// One entry in the bounded ring buffer of halt transitions.
/// Mirrors pairtrade's RiskHistoryEvent so the dashboard's halt
/// strip renders both fleets the same way.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RiskHistoryEvent {
    pub ts: i64,
    pub instance_id: String,
    /// "daily_dd" | "session_dd" | "circuit_breaker" | "kill_switch"
    pub kind: String,
    /// "activated" | "cleared" | "ack"
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

/// Snapshot structs published in `status.json`. Mirror the dashboard
/// `StatusData` field types verbatim (debot-dashboard/main.go).

#[derive(Serialize, Debug, Clone)]
pub struct DailyRiskSnapshot {
    pub daily_pnl: f64,
    pub daily_pnl_bps: f64,
    pub session_start_equity: f64,
    pub session_start_ts: i64,
    pub max_daily_loss_bps: u32,
    pub effective_max_daily_loss_bps: f64,
    pub risk_halted: bool,
}

#[derive(Serialize, Debug, Clone)]
pub struct SessionRiskSnapshot {
    pub current_equity: f64,
    pub peak_equity: f64,
    pub dd_bps: f64,
    pub max_session_loss_bps: u32,
    pub effective_max_session_loss_bps: f64,
    pub lookback_secs: u64,
    pub sample_count: usize,
    pub session_halted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub halt_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub halt_ts: Option<i64>,
}

#[derive(Serialize, Debug, Clone)]
pub struct CircuitBreakerSnapshot {
    pub consecutive_losses: u32,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_remaining_secs: Option<i64>,
    pub tier1_threshold: u32,
    pub tier2_threshold: u32,
}

/// Owns the live `RiskState`, the bounded `risk_history` ring, and
/// timer-driven persistence. Live loop calls `tick()` every iteration
/// and `block_reason()` before emitting a `Decision::Enter`.
pub struct RiskManager {
    config: RiskConfig,
    state: RiskState,
    instance_id: String,
    risk_history: VecDeque<RiskHistoryEvent>,
    last_persist: Option<Instant>,
    last_equity: Option<f64>,
}

impl RiskManager {
    pub fn new(config: RiskConfig, instance_id: String) -> Self {
        let state = load_state(&config.risk_state_path);
        let risk_history = VecDeque::with_capacity(RISK_HISTORY_BUFFER_CAP);
        Self {
            config,
            state,
            instance_id,
            risk_history,
            last_persist: None,
            last_equity: None,
        }
    }

    /// Per-tick housekeeping: rolls the UTC session if a day ticked
    /// over, consumes a RISK_ACK file if dropped, ages out cooldowns.
    /// Idempotent — safe to call every tick.
    pub fn tick(&mut self, now_ts: i64) {
        self.maybe_roll_daily(now_ts);
        self.maybe_consume_risk_ack(now_ts);
        self.maybe_clear_cooldown(now_ts);
    }

    /// `Some(reason)` when an entry should be blocked, `None` to
    /// proceed. Side-effect-free; the live loop reads this then
    /// passes the result to its summary counters.
    pub fn block_reason(&self, now_ts: i64) -> Option<BlockReason> {
        if self.state.session_halted {
            return Some(BlockReason::SessionDdHalted);
        }
        if self.config.max_daily_loss_bps > 0 {
            let halted = self.daily_pnl_bps()
                .map(|bps| bps <= -(self.config.max_daily_loss_bps as f64))
                .unwrap_or(false);
            if halted {
                return Some(BlockReason::DailyDdHalted);
            }
        }
        if let Some(until) = self.state.cb_until_ts {
            if until > now_ts {
                return Some(BlockReason::CircuitBreakerCooldown);
            }
        }
        None
    }

    /// Equity sample point. Updates the rolling peak, runs the
    /// session-DD check, and prunes old samples. Persists if the
    /// in-memory state changed.
    pub fn record_equity_sample(&mut self, equity: f64, now_ts: i64) {
        if !equity.is_finite() || equity <= 0.0 {
            return;
        }
        self.last_equity = Some(equity);

        // Bootstrap session_start_equity on first sample of a fresh
        // session (rolled but never observed).
        if self.state.session_start_equity == 0.0 {
            self.state.session_start_equity = equity;
            self.state.session_start_ts = now_ts;
        }

        // Pre-prune by lookback so the sample sees a current peak.
        let cutoff = now_ts - self.config.session_dd_lookback_secs as i64;
        self.state.equity_samples.retain(|s| s.ts >= cutoff);

        // Sample at most every `session_dd_sample_secs` to keep the
        // file bounded. First-ever sample always lands so the
        // rolling peak has at least one observation to compare
        // against — otherwise the very first equity reading after
        // boot would be both the peak and the current value (no
        // drawdown ever).
        let due = match self.state.equity_samples.last() {
            None => true,
            Some(last) => now_ts - last.ts >= self.config.session_dd_sample_secs as i64,
        };
        if due {
            self.state.equity_samples.push(EquitySample {
                ts: now_ts,
                equity,
            });
        }

        // Rolling peak from the retained samples (plus the new one).
        let peak = self
            .state
            .equity_samples
            .iter()
            .map(|s| s.equity)
            .fold(equity, f64::max);

        // Session DD check.
        if !self.state.session_halted && self.config.max_session_loss_bps > 0 && peak > 0.0 {
            let dd_bps = ((peak - equity) / peak) * 10_000.0;
            if dd_bps >= self.config.max_session_loss_bps as f64 {
                self.state.session_halted = true;
                let reason = format!(
                    "peak={:.2} current={:.2} dd_bps={:.1} thresh_bps={}",
                    peak, equity, dd_bps, self.config.max_session_loss_bps
                );
                self.state.session_halt_reason = Some(reason.clone());
                self.state.session_halt_ts = Some(now_ts);
                log::error!(
                    "[RISK] session DD halt activated: {} (manual ack required: drop {})",
                    reason,
                    self.config.risk_ack_path.display()
                );
                self.push_history(RiskHistoryEvent {
                    ts: now_ts,
                    instance_id: self.instance_id.clone(),
                    kind: "session_dd".into(),
                    event_type: "activated".into(),
                    reason: Some(reason),
                    detail: Some(serde_json::json!({
                        "peak": peak,
                        "current": equity,
                        "dd_bps": dd_bps,
                        "thresh_bps": self.config.max_session_loss_bps
                    })),
                });
            }
        }

        self.maybe_persist(now_ts);
    }

    /// Hook the live loop calls when a position closes. PnL is the
    /// realized USD result of the cycle (positive = gain). Drives the
    /// daily-DD denominator and the consecutive-loss counter.
    pub fn record_close(&mut self, realized_pnl: f64, now_ts: i64) {
        if !realized_pnl.is_finite() {
            return;
        }
        self.state.realized_pnl_today += realized_pnl;
        if realized_pnl < 0.0 {
            self.state.consecutive_losses = self.state.consecutive_losses.saturating_add(1);
            self.maybe_arm_cooldown(now_ts);
        } else if realized_pnl > 0.0 {
            // Winning trade clears the streak — pairtrade-symmetric.
            self.state.consecutive_losses = 0;
            // Auto-clear an active cooldown on a win is *not* the
            // pairtrade convention (it lets cooldown expire on the
            // clock); we mirror that here.
        }
        // Daily DD activation is checked lazily via `block_reason`
        // — no state mutation needed beyond the running sum.
        if self.config.max_daily_loss_bps > 0 {
            if let Some(bps) = self.daily_pnl_bps() {
                if bps <= -(self.config.max_daily_loss_bps as f64) {
                    log::error!(
                        "[RISK] daily DD halt activated: realized_pnl_today={:.2} \
                         session_start_equity={:.2} bps={:.1} thresh={}",
                        self.state.realized_pnl_today,
                        self.state.session_start_equity,
                        bps,
                        self.config.max_daily_loss_bps
                    );
                    self.push_history(RiskHistoryEvent {
                        ts: now_ts,
                        instance_id: self.instance_id.clone(),
                        kind: "daily_dd".into(),
                        event_type: "activated".into(),
                        reason: Some(format!("bps={:.1} thresh={}", bps, self.config.max_daily_loss_bps)),
                        detail: Some(serde_json::json!({
                            "realized_pnl_today": self.state.realized_pnl_today,
                            "session_start_equity": self.state.session_start_equity,
                            "dd_bps": bps,
                            "thresh_bps": self.config.max_daily_loss_bps
                        })),
                    });
                }
            }
        }
        self.maybe_persist(now_ts);
    }

    pub fn daily_snapshot(&self) -> Option<DailyRiskSnapshot> {
        if self.state.session_start_equity <= 0.0 {
            return None;
        }
        let bps = self.daily_pnl_bps().unwrap_or(0.0);
        let halted = self.config.max_daily_loss_bps > 0
            && bps <= -(self.config.max_daily_loss_bps as f64);
        Some(DailyRiskSnapshot {
            daily_pnl: self.state.realized_pnl_today,
            daily_pnl_bps: bps,
            session_start_equity: self.state.session_start_equity,
            session_start_ts: self.state.session_start_ts,
            max_daily_loss_bps: self.config.max_daily_loss_bps,
            // Effective threshold equals raw — xvenue-arb is
            // delta-neutral so the leverage-scale amendment from
            // pairtrade does not apply (no `× max_leverage`).
            effective_max_daily_loss_bps: self.config.max_daily_loss_bps as f64,
            risk_halted: halted,
        })
    }

    pub fn session_snapshot(&self) -> Option<SessionRiskSnapshot> {
        let current = self.last_equity?;
        let peak = self
            .state
            .equity_samples
            .iter()
            .map(|s| s.equity)
            .fold(current, f64::max);
        let dd_bps = if peak > 0.0 {
            ((peak - current) / peak) * 10_000.0
        } else {
            0.0
        };
        Some(SessionRiskSnapshot {
            current_equity: current,
            peak_equity: peak,
            dd_bps,
            max_session_loss_bps: self.config.max_session_loss_bps,
            effective_max_session_loss_bps: self.config.max_session_loss_bps as f64,
            lookback_secs: self.config.session_dd_lookback_secs,
            sample_count: self.state.equity_samples.len(),
            session_halted: self.state.session_halted,
            halt_reason: self.state.session_halt_reason.clone(),
            halt_ts: self.state.session_halt_ts,
        })
    }

    pub fn circuit_breaker_snapshot(&self, now_ts: i64) -> CircuitBreakerSnapshot {
        let active = self
            .state
            .cb_until_ts
            .map(|until| until > now_ts)
            .unwrap_or(false);
        let cooldown_remaining_secs = self.state.cb_until_ts.map(|until| (until - now_ts).max(0));
        CircuitBreakerSnapshot {
            consecutive_losses: self.state.consecutive_losses,
            active,
            until_ts: self.state.cb_until_ts,
            cooldown_remaining_secs,
            tier1_threshold: self.config.cb_tier1_threshold,
            tier2_threshold: self.config.cb_tier2_threshold,
        }
    }

    pub fn risk_history(&self) -> Vec<RiskHistoryEvent> {
        self.risk_history.iter().cloned().collect()
    }

    pub fn state_for_test(&self) -> &RiskState {
        &self.state
    }

    fn daily_pnl_bps(&self) -> Option<f64> {
        if self.state.session_start_equity <= 0.0 {
            return None;
        }
        Some(self.state.realized_pnl_today / self.state.session_start_equity * 10_000.0)
    }

    fn maybe_roll_daily(&mut self, now_ts: i64) {
        if self.state.session_start_ts == 0 {
            // First-ever boot: defer until the first equity sample
            // bootstraps `session_start_equity`. Recording the ts
            // here would freeze a 0 baseline forever.
            return;
        }
        let prev_day = utc_session_day(self.state.session_start_ts, self.config.daily_reset_utc_hour);
        let cur_day = utc_session_day(now_ts, self.config.daily_reset_utc_hour);
        if cur_day > prev_day {
            log::info!(
                "[RISK] daily reset: realized_pnl_today={:.2} → 0 (carried equity={:.2})",
                self.state.realized_pnl_today,
                self.last_equity.unwrap_or(0.0)
            );
            self.state.realized_pnl_today = 0.0;
            self.state.session_start_ts = now_ts;
            if let Some(eq) = self.last_equity {
                self.state.session_start_equity = eq;
            }
            // Daily DD auto-clears on the rollover.
            self.push_history(RiskHistoryEvent {
                ts: now_ts,
                instance_id: self.instance_id.clone(),
                kind: "daily_dd".into(),
                event_type: "cleared".into(),
                reason: Some("daily_reset".into()),
                detail: None,
            });
            self.maybe_persist(now_ts);
        }
    }

    fn maybe_consume_risk_ack(&mut self, now_ts: i64) {
        let path = &self.config.risk_ack_path;
        if path.as_os_str().is_empty() {
            return;
        }
        let raw = match fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                log::warn!("[RISK_ACK] read {}: {:?}", path.display(), e);
                return;
            }
        };
        // Accept either a well-formed JSON ack or an empty / plain
        // file (which we treat as an unsigned ack from the
        // dashboard's flatten button — same convenience as pairtrade).
        //
        // Heuristic for "intended JSON": content starts with `{`
        // after stripping leading whitespace. If the operator meant
        // JSON but typo'd it (e.g. unclosed brace), we refuse rather
        // than silently fall back to the synthetic `"ack_by": "file"`
        // stamp — better to surface the malformed input and let the
        // operator fix it than to log a misleading audit line.
        // (#244 D-5 / #268 S5-4.)
        let trimmed = raw.trim_start();
        let parsed: serde_json::Value = if trimmed.starts_with('{') {
            match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!(
                        "[RISK_ACK] {} appears to be JSON but fails to parse ({}); \
                         refusing to clear halt — fix the file or replace with plain text",
                        path.display(),
                        e
                    );
                    return;
                }
            }
        } else {
            serde_json::json!({"ack_by": "file"})
        };
        let ack_by = parsed
            .get("ack_by")
            .and_then(|v| v.as_str())
            .unwrap_or("operator")
            .to_string();
        if !self.state.session_halted {
            // No active halt to clear; remove the file anyway so a
            // stale ack doesn't accidentally clear a future halt.
            log::info!("[RISK_ACK] dropped while no halt is active — removing file");
            let _ = fs::remove_file(path);
            return;
        }
        log::info!(
            "[RISK_ACK] clearing session_dd halt by={} reason={:?}",
            ack_by,
            self.state.session_halt_reason
        );
        let prev_reason = self.state.session_halt_reason.take();
        self.state.session_halted = false;
        self.state.session_halt_ts = None;
        // Drop the rolling-peak window so the next sample bootstraps
        // a fresh peak — otherwise the bot trips again on the same
        // pre-ack peak.
        self.state.equity_samples.clear();
        if let Err(e) = fs::remove_file(path) {
            log::warn!("[RISK_ACK] remove {}: {:?}", path.display(), e);
        }
        self.push_history(RiskHistoryEvent {
            ts: now_ts,
            instance_id: self.instance_id.clone(),
            kind: "session_dd".into(),
            event_type: "ack".into(),
            reason: prev_reason,
            detail: Some(serde_json::json!({"ack_by": ack_by})),
        });
        self.maybe_persist(now_ts);
    }

    fn maybe_clear_cooldown(&mut self, now_ts: i64) {
        if let Some(until) = self.state.cb_until_ts {
            if until <= now_ts {
                self.state.cb_until_ts = None;
                self.push_history(RiskHistoryEvent {
                    ts: now_ts,
                    instance_id: self.instance_id.clone(),
                    kind: "circuit_breaker".into(),
                    event_type: "cleared".into(),
                    reason: None,
                    detail: None,
                });
                self.maybe_persist(now_ts);
            }
        }
    }

    fn maybe_arm_cooldown(&mut self, now_ts: i64) {
        let losses = self.state.consecutive_losses;
        let secs = if self.config.cb_tier2_threshold > 0
            && losses >= self.config.cb_tier2_threshold
        {
            self.config.cb_tier2_cooldown_secs
        } else if self.config.cb_tier1_threshold > 0
            && losses >= self.config.cb_tier1_threshold
        {
            self.config.cb_tier1_cooldown_secs
        } else {
            return;
        };
        let until = now_ts + secs;
        // Take the more conservative (later) of any active cooldown
        // and the new one.
        let final_until = self.state.cb_until_ts.map(|prev| prev.max(until)).unwrap_or(until);
        self.state.cb_until_ts = Some(final_until);
        log::warn!(
            "[RISK] circuit_breaker armed: losses={} cooldown_secs={} until_ts={}",
            losses,
            secs,
            final_until
        );
        self.push_history(RiskHistoryEvent {
            ts: now_ts,
            instance_id: self.instance_id.clone(),
            kind: "circuit_breaker".into(),
            event_type: "activated".into(),
            reason: Some(format!("losses={}", losses)),
            detail: Some(serde_json::json!({
                "cooldown_secs": secs,
                "until_ts": final_until
            })),
        });
    }

    fn push_history(&mut self, ev: RiskHistoryEvent) {
        if self.risk_history.len() >= RISK_HISTORY_BUFFER_CAP {
            self.risk_history.pop_front();
        }
        // Best-effort jsonl audit log alongside the state file.
        if let Some(parent) = self.config.risk_state_path.parent() {
            let path = parent.join("risk_history.jsonl");
            if let Ok(line) = serde_json::to_string(&ev) {
                if let Err(e) = (|| -> std::io::Result<()> {
                    fs::create_dir_all(parent)?;
                    let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
                    writeln!(f, "{}", line)
                })() {
                    log::warn!("[RISK] history append {}: {:?}", path.display(), e);
                }
            }
        }
        self.risk_history.push_back(ev);
    }

    fn maybe_persist(&mut self, _now_ts: i64) {
        let due = self
            .last_persist
            .map(|t| t.elapsed().as_secs() >= PERSIST_MIN_INTERVAL_SECS)
            .unwrap_or(true);
        if !due {
            return;
        }
        persist_state(&self.config.risk_state_path, &self.state);
        self.last_persist = Some(Instant::now());
    }

    /// Forces a write regardless of the rate limit. Used at shutdown
    /// (live loop calls it before exit) so a graceful SIGTERM doesn't
    /// drop the last few seconds of risk state.
    pub fn flush(&mut self) {
        persist_state(&self.config.risk_state_path, &self.state);
        self.last_persist = Some(Instant::now());
    }
}

/// UTC day index, optionally shifted by the operator's daily reset
/// hour. Used to detect day-boundary crossings without dragging
/// chrono::DateTime through every comparison.
fn utc_session_day(ts: i64, daily_reset_utc_hour: u8) -> i64 {
    let shifted = ts - (daily_reset_utc_hour as i64) * 3_600;
    shifted.div_euclid(86_400)
}

/// Wall-clock timestamp helper; used by the live loop and any caller
/// that doesn't already have a `chrono::Utc::now()` handy.
pub fn now_unix_secs() -> i64 {
    Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn cfg_in(dir: &TempDir) -> RiskConfig {
        RiskConfig {
            max_daily_loss_bps: 300,
            daily_reset_utc_hour: 0,
            max_session_loss_bps: 500,
            session_dd_lookback_secs: 86_400,
            session_dd_sample_secs: 60,
            cb_tier1_threshold: 5,
            cb_tier2_threshold: 8,
            cb_tier1_cooldown_secs: 1_800,
            cb_tier2_cooldown_secs: 21_600,
            risk_state_path: dir.path().join("risk_state.json"),
            risk_ack_path: dir.path().join("RISK_ACK"),
        }
    }

    fn mgr(dir: &TempDir) -> RiskManager {
        RiskManager::new(cfg_in(dir), "test".to_string())
    }

    #[test]
    fn fresh_manager_does_not_block() {
        let tmp = TempDir::new().unwrap();
        let m = mgr(&tmp);
        assert_eq!(m.block_reason(0), None);
    }

    #[test]
    fn daily_dd_activates_after_realized_loss() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        // Bootstrap session: feed a $1000 equity sample at t=100.
        m.record_equity_sample(1_000.0, 100);
        // Realized loss of -$31 = -310 bps of $1000 → above 300 bps.
        m.record_close(-31.0, 200);
        assert_eq!(m.block_reason(200), Some(BlockReason::DailyDdHalted));
    }

    #[test]
    fn daily_dd_clears_on_utc_rollover() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 100);
        m.record_close(-31.0, 200);
        assert_eq!(m.block_reason(200), Some(BlockReason::DailyDdHalted));
        // Tick into the next day.
        let next_day = 200 + 86_400;
        m.tick(next_day);
        assert_eq!(m.block_reason(next_day), None);
        assert_eq!(m.state_for_test().realized_pnl_today, 0.0);
    }

    #[test]
    fn session_dd_activates_on_peak_drawdown() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        // Peak at t=0, then a 6% drop by t=120.
        m.record_equity_sample(1_000.0, 0);
        m.record_equity_sample(1_000.0, 60);
        m.record_equity_sample(940.0, 120);
        // 6% = 600 bps > 500 bps threshold.
        assert_eq!(m.block_reason(120), Some(BlockReason::SessionDdHalted));
    }

    #[test]
    fn session_dd_persists_across_restart() {
        let tmp = TempDir::new().unwrap();
        {
            let mut m = mgr(&tmp);
            m.record_equity_sample(1_000.0, 0);
            m.record_equity_sample(940.0, 60);
            m.flush();
            assert_eq!(m.block_reason(60), Some(BlockReason::SessionDdHalted));
        }
        // Reload from disk — sticky halt must survive.
        let m = mgr(&tmp);
        assert!(m.state_for_test().session_halted);
        assert_eq!(m.block_reason(60), Some(BlockReason::SessionDdHalted));
    }

    #[test]
    fn risk_ack_clears_session_halt() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        m.record_equity_sample(940.0, 60);
        assert!(m.state_for_test().session_halted);
        // Drop a well-formed ack.
        let ack_payload = serde_json::json!({"ack_by": "alice", "ts": 1_000}).to_string();
        std::fs::write(&m.config.risk_ack_path, ack_payload).unwrap();
        m.tick(120);
        assert!(!m.state_for_test().session_halted);
        // File consumed.
        assert!(!m.config.risk_ack_path.exists());
        assert_eq!(m.block_reason(120), None);
    }

    #[test]
    fn risk_ack_plain_text_still_clears_halt() {
        // Backwards-compat: a plain-text RISK_ACK (e.g. an empty
        // `touch` from the dashboard's flatten button) clears the
        // halt with `ack_by: "file"`. The malformed-JSON refusal
        // (#268 S5-4) only kicks in when the file content looks
        // like JSON but parses badly.
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        m.record_equity_sample(940.0, 60);
        assert!(m.state_for_test().session_halted);
        std::fs::write(&m.config.risk_ack_path, "operator pressed flatten\n").unwrap();
        m.tick(120);
        assert!(!m.state_for_test().session_halted);
        assert!(!m.config.risk_ack_path.exists());
    }

    #[test]
    fn risk_ack_empty_file_clears_halt() {
        // `sudo touch /opt/debot/RISK_ACK` is a documented operator
        // shortcut (`runbook_xvenue_arb_risk.md` §3) — must keep
        // working.
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        m.record_equity_sample(940.0, 60);
        std::fs::write(&m.config.risk_ack_path, "").unwrap();
        m.tick(120);
        assert!(!m.state_for_test().session_halted);
        assert!(!m.config.risk_ack_path.exists());
    }

    #[test]
    fn risk_ack_malformed_json_refused_and_halt_persists() {
        // S5-4: file looks like JSON (starts with `{`) but parses
        // badly → bot refuses to clear halt, leaves file on disk
        // for the operator to fix. Better to surface the typo
        // than silently fall through to a synthetic ack stamp.
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        m.record_equity_sample(940.0, 60);
        assert!(m.state_for_test().session_halted);
        // Unclosed object — the kind of typo a hand-edited ack
        // could plausibly produce.
        std::fs::write(&m.config.risk_ack_path, r#"{"ack_by":"alice"#).unwrap();
        m.tick(120);
        assert!(
            m.state_for_test().session_halted,
            "malformed JSON must NOT clear the halt"
        );
        assert!(
            m.config.risk_ack_path.exists(),
            "malformed file must stay on disk for operator inspection"
        );
        // Sanity: replacing it with a well-formed payload clears.
        let ack_payload = serde_json::json!({"ack_by": "alice"}).to_string();
        std::fs::write(&m.config.risk_ack_path, ack_payload).unwrap();
        m.tick(180);
        assert!(!m.state_for_test().session_halted);
        assert!(!m.config.risk_ack_path.exists());
    }

    #[test]
    fn risk_ack_whitespace_prefixed_json_still_validated() {
        // Edge case: the `{` heuristic must survive leading
        // whitespace (an editor's auto-format could produce
        // `  {"ack_by":...` with a leading newline). Both the
        // happy path (valid) and the refused path (malformed)
        // must respect the same trimming rule.
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        m.record_equity_sample(940.0, 60);
        // Valid JSON with leading whitespace — accepted.
        std::fs::write(&m.config.risk_ack_path, "  \n{\"ack_by\":\"bob\"}\n").unwrap();
        m.tick(120);
        assert!(!m.state_for_test().session_halted);
    }

    #[test]
    fn circuit_breaker_arms_at_tier1_then_decays() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        // 5 small losses to hit tier1 threshold without breaching daily DD.
        for i in 0..5 {
            m.record_close(-1.0, 100 + i);
        }
        let snap = m.circuit_breaker_snapshot(110);
        assert_eq!(snap.consecutive_losses, 5);
        assert!(snap.active);
        // Far-future tick crosses the cooldown deadline.
        m.tick(110 + 1_801);
        let snap = m.circuit_breaker_snapshot(110 + 1_801);
        assert!(!snap.active);
    }

    #[test]
    fn winning_trade_resets_consecutive_losses() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        for i in 0..3 {
            m.record_close(-0.5, 100 + i);
        }
        assert_eq!(m.state_for_test().consecutive_losses, 3);
        m.record_close(0.1, 200);
        assert_eq!(m.state_for_test().consecutive_losses, 0);
    }

    #[test]
    fn equity_samples_pruned_to_lookback() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        // Lookback default is 86400; sample interval is 60.
        // Feed > 1 day of samples and confirm only the last day kept.
        for i in 0..2_000 {
            // At 60s cadence, 2000 samples = 120k seconds ≈ 33h.
            m.record_equity_sample(1_000.0, i * 60);
        }
        let last_ts = (1_999_i64) * 60;
        assert!(m
            .state_for_test()
            .equity_samples
            .iter()
            .all(|s| s.ts >= last_ts - 86_400));
    }

    #[test]
    fn snapshots_match_dashboard_field_set() {
        let tmp = TempDir::new().unwrap();
        let mut m = mgr(&tmp);
        m.record_equity_sample(1_000.0, 0);
        let daily = m.daily_snapshot().unwrap();
        assert_eq!(daily.session_start_equity, 1_000.0);
        assert_eq!(daily.max_daily_loss_bps, 300);
        let session = m.session_snapshot().unwrap();
        assert!(session.peak_equity > 0.0);
        let cb = m.circuit_breaker_snapshot(0);
        assert_eq!(cb.tier1_threshold, 5);
    }

    #[test]
    fn utc_session_day_buckets_correctly() {
        // Two timestamps 24 hours apart must produce day indices that
        // differ by exactly one regardless of the start offset.
        let t0: i64 = 1_700_000_000;
        assert_eq!(utc_session_day(t0 + 86_400, 0) - utc_session_day(t0, 0), 1);
        // Reset-hour offset shifts where the day boundary lands but
        // does not change the 1-day delta.
        assert_eq!(utc_session_day(t0 + 86_400, 12) - utc_session_day(t0, 12), 1);
        // A 12-hour shift can land an instant in the previous bucket
        // when reset_hour is set to bring midnight forward.
        let mid_day: i64 = 1_700_000_000 + 6 * 3_600;
        assert!(utc_session_day(mid_day, 12) <= utc_session_day(mid_day, 0));
    }
}
