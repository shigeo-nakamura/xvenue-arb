//! Pure-logic risk-gate primitives extracted from `manager.rs`.
//!
//! Everything here is a free function over `&RiskState` / `&RiskConfig`
//! / scalar inputs — no I/O, no `&mut self`. `RiskManager` becomes a
//! thin façade that owns the state and forwards to these.

use super::manager::{EquitySample, RiskConfig, RiskState};

/// Reasons `RiskManager::block_reason` returns Some. Surfaced in
/// `[RISK]` log lines and the auto-issue framework's error_summary.
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

/// Realized-PnL drawdown today, expressed in bps of
/// `session_start_equity`. `None` when the session denominator is
/// unset (first-ever boot, before the first equity sample).
pub fn daily_pnl_bps(state: &RiskState) -> Option<f64> {
    if state.session_start_equity <= 0.0 {
        return None;
    }
    Some(state.realized_pnl_today / state.session_start_equity * 10_000.0)
}

/// Combined gate: returns the first reason the live loop should block
/// a `Decision::Enter`. Order is sticky-first (`session_halted`) →
/// daily-DD → cooldown, matching the previous inline logic.
pub fn compute_block_reason(
    state: &RiskState,
    config: &RiskConfig,
    now_ts: i64,
) -> Option<BlockReason> {
    if state.session_halted {
        return Some(BlockReason::SessionDdHalted);
    }
    if config.max_daily_loss_bps > 0 {
        let halted = daily_pnl_bps(state)
            .map(|bps| bps <= -(config.max_daily_loss_bps as f64))
            .unwrap_or(false);
        if halted {
            return Some(BlockReason::DailyDdHalted);
        }
    }
    if let Some(until) = state.cb_until_ts {
        if until > now_ts {
            return Some(BlockReason::CircuitBreakerCooldown);
        }
    }
    None
}

/// Rolling peak from a sparse equity-sample buffer plus a seed reading.
/// The seed is folded in so a fresh boot (empty samples) returns the
/// seed itself, and so the *current* equity always participates in the
/// peak even if it hasn't been promoted to a sample yet (sample cadence
/// is throttled to `session_dd_sample_secs`).
pub fn rolling_peak(samples: &[EquitySample], seed: f64) -> f64 {
    samples.iter().map(|s| s.equity).fold(seed, f64::max)
}

/// UTC day index, optionally shifted by the operator's daily reset
/// hour. Used to detect day-boundary crossings without dragging
/// chrono::DateTime through every comparison.
pub fn utc_session_day(ts: i64, daily_reset_utc_hour: u8) -> i64 {
    let shifted = ts - (daily_reset_utc_hour as i64) * 3_600;
    shifted.div_euclid(86_400)
}
