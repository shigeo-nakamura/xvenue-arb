//! Default values and magic constants for the pairtrade engine. Extracted
//! from the monolithic pairtrade module as part of bot-strategy#26.

pub(super) const DEFAULT_INTERVAL_SECS: u64 = 20;
pub(super) const DEFAULT_TRADING_PERIOD_SECS: u64 = 60;
pub(super) const DEFAULT_METRICS_WINDOW: usize = 240;
pub(super) const DEFAULT_ENTRY_Z_BASE: f64 = 2.0;
pub(super) const DEFAULT_ENTRY_Z_MIN: f64 = 1.8;
pub(super) const DEFAULT_ENTRY_Z_MAX: f64 = 2.3;
pub(super) const DEFAULT_EXIT_Z: f64 = 0.5;
pub(super) const DEFAULT_STOP_LOSS_Z: f64 = 3.3;
pub(super) const DEFAULT_FORCE_CLOSE_SECS: u64 = 3600;
pub(super) const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 3660; // DEFAULT_FORCE_CLOSE_SECS + 60s buffer
pub(super) const DEFAULT_COOLDOWN_SECS: u64 = 30;
pub(super) const MAX_EXIT_RETRIES: u32 = 3;
pub(super) const DEFAULT_NET_FUNDING_MIN_PER_HOUR: f64 = -0.005;
pub(super) const DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN: f64 = 0.1;
pub(super) const DEFAULT_NOTIONAL_PER_LEG: f64 = 100.0;
pub(super) const DEFAULT_RISK_PCT_PER_TRADE: f64 = 0.01;
pub(super) const DEFAULT_MAX_LOSS_R_MULT: f64 = 1.0;
pub(super) const DEFAULT_EQUITY_USD: f64 = 10_000.0;
pub(super) const DEFAULT_LOOKBACK_HOURS_SHORT: u64 = 4;
pub(super) const DEFAULT_LOOKBACK_HOURS_LONG: u64 = 24;
pub(super) const DEFAULT_HALF_LIFE_MAX_HOURS: f64 = 1.5;
pub(super) const DEFAULT_ADF_P_THRESHOLD: f64 = 0.05;
pub(super) const PAIR_SELECTION_INTERVAL_SECS: u64 = 3600;
pub(super) const DEFAULT_ENTRY_VOL_LOOKBACK_HOURS: u64 = 24;
pub(super) const DEFAULT_SLIPPAGE_BPS: i32 = 0;
pub(super) const DEFAULT_FEE_BPS: f64 = 0.0;
pub(super) const DEFAULT_MAX_LEVERAGE: f64 = 5.0;
pub(super) const DEFAULT_REEVAL_JUMP_Z_MULT: f64 = 1.5;
pub(super) const DEFAULT_VOL_SPIKE_MULT: f64 = 2.5;
pub(super) const DEFAULT_MAX_ACTIVE_PAIRS: usize = 3;
pub(super) const DEFAULT_WARM_START_MODE: &str = "strict";
pub(super) const DEFAULT_ORDER_TIMEOUT_SECS: u64 = 120;
pub(super) const DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES: u32 = 3;
pub(super) const DEFAULT_FORCE_CLOSE_ON_STARTUP: bool = true;
pub(super) const DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS: u32 = 3;
pub(super) const DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS: u64 = 3;
pub(super) const POST_ONLY_ENTRY_ATTEMPTS: usize = 3;
pub(super) const POST_ONLY_EXIT_ATTEMPTS: usize = 3;
pub(super) const POST_ONLY_RETRY_DELAY_MS: u64 = 200;
pub(super) const POST_ONLY_RETRY_MAX_ELAPSED_MS: u64 = 1500;
pub(super) const DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA: f64 = 0.5;
pub(super) const DEFAULT_BETA_DIVERGENCE_MAX: f64 = 0.15;
pub(super) const DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES: u32 = 3;
pub(super) const DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 1800;
pub(super) const DEFAULT_CB_TIER1_LOSSES: u32 = 0;
pub(super) const DEFAULT_CB_TIER1_COOLDOWN_SECS: u64 = 0;
pub(super) const DEFAULT_CB_TIER2_LOSSES: u32 = 0;
pub(super) const DEFAULT_CB_TIER2_COOLDOWN_SECS: u64 = 0;
pub(super) const DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS: u64 = 0;

// Multi-timeframe z-score confluence (disabled by default)
pub(super) const DEFAULT_MTF_Z_MIN: f64 = 0.0;

// Kalman filter beta estimation (disabled by default)
pub(super) const DEFAULT_USE_KALMAN_BETA: bool = false;
pub(super) const DEFAULT_KALMAN_Q: f64 = 1e-5;
pub(super) const DEFAULT_KALMAN_R: f64 = 1e-3;
pub(super) const DEFAULT_KALMAN_INITIAL_P: f64 = 1.0;
pub(super) const DEFAULT_KALMAN_MIN_UPDATES: u64 = 60;

// Std collapse guard (disabled by default: window=0 or ratio=0.0 → filter inactive).
// See bot-strategy#62: on 2026-04-15 the BTC/ETH spread std collapsed from
// 1.018 → 0.0016 within minutes, producing meaningless z-scores that all three
// bots interpreted as deep mean-reversion signals and lost on. Guard blocks
// entry when the current full-window std is a small fraction of the rolling
// median of recent stds, i.e. the z denominator is no longer trustworthy.
pub(super) const DEFAULT_STD_COLLAPSE_WINDOW_BARS: usize = 0;
pub(super) const DEFAULT_STD_COLLAPSE_MIN_RATIO: f64 = 0.0;
/// Observe-only mode: when true, the guard only logs that it *would* block
/// the entry, but lets the trade through. Lets operators measure trigger
/// frequency against live data before enabling the block. See bot-strategy#62.
pub(super) const DEFAULT_STD_COLLAPSE_OBSERVE_ONLY: bool = false;

// Regime filter (disabled by default: thresholds 0.0 → filter inactive)
pub(super) const DEFAULT_REGIME_VOL_WINDOW: usize = 60;
pub(super) const DEFAULT_REGIME_VOL_MAX: f64 = 0.0;
pub(super) const DEFAULT_REGIME_TREND_WINDOW: usize = 60;
pub(super) const DEFAULT_REGIME_TREND_MAX: f64 = 0.0;
pub(super) const DEFAULT_REGIME_REFERENCE_SYMBOL: &str = "BTC";
