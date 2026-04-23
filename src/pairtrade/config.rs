//! Pairtrade configuration: YAML schema types, resolved shapes and the
//! env/YAML → resolved-config builder.

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use super::defaults::*;

/// Resolved per-pair parameters (global defaults merged with any pair-specific overrides).
#[derive(Debug, Clone, Default)]
pub struct PairParams {
    pub entry_z_base: f64,
    pub entry_z_min: f64,
    pub entry_z_max: f64,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub force_close_secs: u64,
    pub cooldown_secs: u64,
    pub max_loss_r_mult: f64,
    pub half_life_max_hours: f64,
    pub adf_p_threshold: f64,
    pub spread_velocity_max_sigma_per_min: f64,
    pub spread_trend_max_slope_sigma: f64,
    pub beta_divergence_max: f64,
    pub beta_min: f64,
    pub hedge_ratio_max_deviation: f64,
    pub lookback_hours_short: u64,
    pub lookback_hours_long: u64,
    pub entry_vol_lookback_hours: u64,
    pub warm_start_min_bars: usize,
    pub reeval_jump_z_mult: f64,
    pub vol_spike_mult: f64,
    pub circuit_breaker_tier1_losses: u32,
    pub circuit_breaker_tier1_cooldown_secs: u64,
    pub circuit_breaker_tier2_losses: u32,
    pub circuit_breaker_tier2_cooldown_secs: u64,
    pub entry_post_only_timeout_secs: u64,
    // Phase 2 filters (0.0 = disabled)
    pub entry_velocity_block_sigma_per_min: f64,
    pub funding_entry_z_scale: f64,
    pub beta_gap_entry_z_scale: f64,
    // Multi-timeframe z-score confluence (empty = disabled)
    pub mtf_windows: Vec<usize>,
    pub mtf_z_min: f64,
    // Std collapse guard (both 0 = disabled). See bot-strategy#62.
    pub std_collapse_window_bars: usize,
    pub std_collapse_min_ratio: f64,
    pub std_collapse_observe_only: bool,
}

#[derive(Debug, Clone)]
pub struct PairSpec {
    pub base: String,
    pub quote: String,
}

pub(super) fn env_has_universe_override() -> bool {
    env::var("UNIVERSE_PAIRS")
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || env::var("UNIVERSE_SYMBOLS")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

pub(super) fn parse_pairs_vec(pairs: &[String]) -> Result<Vec<PairSpec>> {
    let joined = pairs.join(",");
    parse_pairs_list(&joined)
}

pub(super) fn parse_symbols_vec(symbols: &[String]) -> Result<Vec<PairSpec>> {
    let syms: Vec<String> = symbols
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if syms.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    let mut pairs = Vec::new();
    for i in 0..syms.len() {
        for j in (i + 1)..syms.len() {
            let a = syms[i].clone();
            let b = syms[j].clone();
            let (base, quote) = if a < b { (a, b) } else { (b, a) };
            pairs.push(PairSpec { base, quote });
        }
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    Ok(pairs)
}

pub(super) fn resolve_universe_from_yaml(yaml: &PairTradeYaml) -> Result<Vec<PairSpec>> {
    if env_has_universe_override() {
        return parse_universe_pairs();
    }
    if let Some(pairs) = yaml.universe_pairs.clone() {
        let pairs = pairs.into_vec();
        if pairs.is_empty() {
            return Err(anyhow!("universe_pairs produced no valid pairs"));
        }
        return parse_pairs_vec(&pairs);
    }
    if let Some(symbols) = yaml.universe_symbols.clone() {
        let symbols = symbols.into_vec();
        if symbols.is_empty() {
            return Err(anyhow!("universe_symbols produced no valid pairs"));
        }
        return parse_symbols_vec(&symbols);
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

pub(super) fn parse_universe_pairs() -> Result<Vec<PairSpec>> {
    if let Ok(raw_pairs) = env::var("UNIVERSE_PAIRS") {
        if !raw_pairs.trim().is_empty() {
            return parse_pairs_list(&raw_pairs);
        }
    }
    if let Ok(symbols_raw) = env::var("UNIVERSE_SYMBOLS") {
        if !symbols_raw.trim().is_empty() {
            let syms: Vec<String> = symbols_raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let mut pairs = Vec::new();
            if syms.len() == 1 {
                // Single-symbol mode: create a self-pair for data-dump collection
                pairs.push(PairSpec {
                    base: syms[0].clone(),
                    quote: syms[0].clone(),
                });
            } else {
                for i in 0..syms.len() {
                    for j in (i + 1)..syms.len() {
                        let a = syms[i].clone();
                        let b = syms[j].clone();
                        let (base, quote) = if a < b { (a, b) } else { (b, a) };
                        pairs.push(PairSpec { base, quote });
                    }
                }
            }
            if pairs.is_empty() {
                return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
            }
            return Ok(pairs);
        }
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

pub(super) fn parse_pairs_list(raw: &str) -> Result<Vec<PairSpec>> {
    let mut pairs = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut split = trimmed.split('/');
        let base = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        let quote = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        pairs.push(PairSpec {
            base: base.to_string(),
            quote: quote.to_string(),
        });
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_PAIRS produced no valid pairs"));
    }
    Ok(pairs)
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub(super) enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

impl StringOrVec {
    pub(super) fn into_vec(self) -> Vec<String> {
        match self {
            StringOrVec::String(value) => value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect(),
            StringOrVec::Vec(values) => values
                .into_iter()
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) struct PairTradeYaml {
    pub(super) dex_name: Option<String>,
    pub(super) rest_endpoint: Option<String>,
    pub(super) web_socket_endpoint: Option<String>,
    pub(super) dry_run: Option<bool>,
    pub(super) agent_name: Option<String>,
    pub(super) interval_secs: Option<u64>,
    pub(super) trading_period_secs: Option<u64>,
    pub(super) metrics_window_length: Option<usize>,
    pub(super) entry_z_score_base: Option<f64>,
    pub(super) entry_z_score_min: Option<f64>,
    pub(super) entry_z_score_max: Option<f64>,
    pub(super) exit_z_score: Option<f64>,
    pub(super) stop_loss_z_score: Option<f64>,
    pub(super) force_close_time_secs: Option<u64>,
    pub(super) cooldown_secs: Option<u64>,
    pub(super) net_funding_min_per_hour: Option<f64>,
    pub(super) spread_velocity_max_sigma_per_min: Option<f64>,
    pub(super) notional_per_leg_usd: Option<f64>,
    pub(super) risk_pct_per_trade: Option<f64>,
    pub(super) max_loss_r_mult: Option<f64>,
    pub(super) equity_usd_fallback: Option<f64>,
    pub(super) universe_pairs: Option<StringOrVec>,
    pub(super) universe_symbols: Option<StringOrVec>,
    pub(super) pair_selection_lookback_hours_short: Option<u64>,
    pub(super) pair_selection_lookback_hours_long: Option<u64>,
    pub(super) half_life_max_hours: Option<f64>,
    pub(super) adf_p_threshold: Option<f64>,
    pub(super) entry_vol_lookback_hours: Option<u64>,
    pub(super) slippage_bps: Option<i32>,
    pub(super) fee_bps: Option<f64>,
    pub(super) max_leverage: Option<f64>,
    pub(super) reeval_jump_z_mult: Option<f64>,
    pub(super) vol_spike_mult: Option<f64>,
    pub(super) max_active_pairs: Option<usize>,
    pub(super) warm_start_mode: Option<String>,
    pub(super) warm_start_min_bars: Option<usize>,
    pub(super) order_timeout_secs: Option<u64>,
    pub(super) entry_partial_fill_max_retries: Option<u32>,
    pub(super) startup_force_close_attempts: Option<u32>,
    pub(super) startup_force_close_wait_secs: Option<u64>,
    pub(super) force_close_on_startup: Option<bool>,
    pub(super) enable_data_dump: Option<bool>,
    pub(super) data_dump_file: Option<String>,
    pub(super) observe_only: Option<bool>,
    pub(super) disable_history_persist: Option<bool>,
    pub(super) history_file: Option<String>,
    pub(super) history_archive_dir: Option<String>,
    pub(super) history_archive_retention_days: Option<u32>,
    pub(super) backtest_mode: Option<bool>,
    pub(super) backtest_file: Option<String>,
    pub(super) spread_trend_max_slope_sigma: Option<f64>,
    pub(super) beta_divergence_max: Option<f64>,
    pub(super) beta_min: Option<f64>,
    pub(super) hedge_ratio_max_deviation: Option<f64>,
    pub(super) circuit_breaker_consecutive_losses: Option<u32>,
    pub(super) circuit_breaker_cooldown_secs: Option<u64>,
    pub(super) circuit_breaker_tier1_losses: Option<u32>,
    pub(super) circuit_breaker_tier1_cooldown_secs: Option<u64>,
    pub(super) circuit_breaker_tier2_losses: Option<u32>,
    pub(super) circuit_breaker_tier2_cooldown_secs: Option<u64>,
    pub(super) entry_post_only_timeout_secs: Option<u64>,
    // Phase 2 filters (default off: 0.0 disables)
    pub(super) entry_velocity_block_sigma_per_min: Option<f64>,
    pub(super) funding_entry_z_scale: Option<f64>,
    pub(super) beta_gap_entry_z_scale: Option<f64>,
    pub(super) mtf_windows: Option<Vec<usize>>,
    pub(super) mtf_z_min: Option<f64>,
    pub(super) std_collapse_window_bars: Option<usize>,
    pub(super) std_collapse_min_ratio: Option<f64>,
    pub(super) std_collapse_observe_only: Option<bool>,
    pub(super) pair_overrides: Option<HashMap<String, PairOverrideYaml>>,
    /// Graceful shutdown: max seconds to wait for natural exit on SIGTERM before
    /// force-closing both legs. 0 = immediate force close (legacy behavior).
    pub(super) shutdown_grace_secs: Option<u64>,
    /// Optional list of strategy variants for the single-process A/B/C
    /// architecture (shigeo-nakamura/bot-strategy#25). When absent, the
    /// loader synthesizes a single strategy from the top-level scalars
    /// (legacy single-bot YAML format) so existing configs keep working.
    pub(super) strategies: Option<Vec<StrategyYaml>>,
    // Kalman filter beta estimation
    pub(super) use_kalman_beta: Option<bool>,
    pub(super) kalman_q: Option<f64>,
    pub(super) kalman_r: Option<f64>,
    pub(super) kalman_initial_p: Option<f64>,
    pub(super) kalman_min_updates: Option<u64>,
    // Regime filter
    pub(super) regime_vol_window: Option<usize>,
    pub(super) regime_vol_max: Option<f64>,
    pub(super) regime_trend_window: Option<usize>,
    pub(super) regime_trend_max: Option<f64>,
    pub(super) regime_reference_symbol: Option<String>,
}

/// Per-strategy override block in the new multi-strategy YAML format.
/// Every field is optional; unset fields fall back to the corresponding
/// top-level scalar (or its compile-time default).
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(super) struct StrategyYaml {
    pub(super) id: Option<String>,
    pub(super) agent_name: Option<String>,
    pub(super) exit_z_score: Option<f64>,
    pub(super) stop_loss_z_score: Option<f64>,
    pub(super) max_loss_r_mult: Option<f64>,
    pub(super) risk_pct_per_trade: Option<f64>,
    pub(super) equity_usd_fallback: Option<f64>,
    pub(super) enable_data_dump: Option<bool>,
    pub(super) data_dump_file: Option<String>,
    // Per-strategy PairParams overrides (None = inherit from top-level)
    pub(super) force_close_time_secs: Option<u64>,
    pub(super) mtf_windows: Option<Vec<usize>>,
    pub(super) mtf_z_min: Option<f64>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(super) struct PairOverrideYaml {
    pub(super) entry_z_score_base: Option<f64>,
    pub(super) entry_z_score_min: Option<f64>,
    pub(super) entry_z_score_max: Option<f64>,
    pub(super) exit_z_score: Option<f64>,
    pub(super) stop_loss_z_score: Option<f64>,
    pub(super) force_close_time_secs: Option<u64>,
    pub(super) cooldown_secs: Option<u64>,
    pub(super) max_loss_r_mult: Option<f64>,
    pub(super) half_life_max_hours: Option<f64>,
    pub(super) adf_p_threshold: Option<f64>,
    pub(super) spread_velocity_max_sigma_per_min: Option<f64>,
    pub(super) spread_trend_max_slope_sigma: Option<f64>,
    pub(super) beta_divergence_max: Option<f64>,
    pub(super) beta_min: Option<f64>,
    pub(super) hedge_ratio_max_deviation: Option<f64>,
    pub(super) pair_selection_lookback_hours_short: Option<u64>,
    pub(super) pair_selection_lookback_hours_long: Option<u64>,
    pub(super) entry_vol_lookback_hours: Option<u64>,
    pub(super) warm_start_min_bars: Option<usize>,
    pub(super) reeval_jump_z_mult: Option<f64>,
    pub(super) vol_spike_mult: Option<f64>,
    pub(super) circuit_breaker_tier1_losses: Option<u32>,
    pub(super) circuit_breaker_tier1_cooldown_secs: Option<u64>,
    pub(super) circuit_breaker_tier2_losses: Option<u32>,
    pub(super) circuit_breaker_tier2_cooldown_secs: Option<u64>,
    pub(super) entry_post_only_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PairTradeConfig {
    pub dex_name: String,
    pub rest_endpoint: String,
    pub web_socket_endpoint: String,
    pub dry_run: bool,
    pub agent_name: Option<String>,
    pub interval_secs: u64,
    pub trading_period_secs: u64,
    pub metrics_window: usize,
    pub net_funding_min_per_hour: f64,
    pub notional_per_leg: f64,
    pub risk_pct_per_trade: f64,
    pub equity_usd: f64,
    pub universe: Vec<PairSpec>,
    pub slippage_bps: i32,
    pub fee_bps: f64,
    pub max_leverage: f64,
    pub max_active_pairs: usize,
    pub warm_start_mode: WarmStartMode,
    pub order_timeout_secs: u64,
    pub entry_partial_fill_max_retries: u32,
    pub startup_force_close_attempts: u32,
    pub startup_force_close_wait_secs: u64,
    pub force_close_on_startup: bool,
    // For data dump feature
    pub enable_data_dump: bool,
    pub data_dump_file: Option<String>,
    // Safety guard to avoid real orders while observing market data
    pub observe_only: bool,
    pub disable_history_persist: bool,
    pub history_file: String,
    pub history_archive_dir: Option<String>,
    pub history_archive_retention_days: u32,
    // For backtest feature
    pub backtest_mode: bool,
    pub backtest_file: Option<String>,
    /// Path to a history snapshot file for BT warm-start. When set,
    /// the replay loads price history from this file before the first
    /// tick, giving the BT an identical starting state to a live bot.
    pub bt_warm_start_snapshot: Option<String>,
    /// Path to a file listing live eval firing timestamps (one UNIX
    /// second per line). In BT mode, when set, the pair re-evaluation
    /// gate is overridden to fire ONLY at these exact timestamps —
    /// replaying the exact wall-clock phase at which the live bot ran
    /// `evaluate_pair` so that `state.beta` (and therefore every
    /// subsequent spread = log_a − β·log_b written to
    /// `spread_history`) follows the live trajectory. Without this
    /// override, BT and live eval gates desync within a few hours due
    /// to 1s-level phase drift and the `last_eval_ts`-based interval
    /// gate, which compounds into a spread_history divergence large
    /// enough to suppress sub-minute std collapses in replay.
    /// See bot-strategy#27 comment 2026-04-16.
    pub bt_eval_timestamps: Option<std::collections::HashSet<i64>>,
    /// Path to a file listing UNIX seconds at which the live bot was
    /// restarted (from `systemd` / `journalctl -u ... | grep Started`).
    /// In BT mode, when `now_ts` equals one of these, the engine fires
    /// `warm_start_states_from_history` once — re-computing `state.beta`
    /// via a fresh OLS over the current 240-bar `history` and re-seeding
    /// `spread_history` with 240 single-beta spreads. That is exactly
    /// what the live bot does at every service restart, and the
    /// low-variance seeded spread_history is the mechanism behind the
    /// 2026-04-15 06:02 UTC "std collapse" incident (bot-strategy#62 is
    /// now known to be a restart artifact, not a market regime break).
    /// Firing is one-shot per timestamp: each matched ts is removed
    /// from the set after firing.
    pub bt_restart_timestamps: Option<std::collections::HashSet<i64>>,
    /// Simulated fill delay for BT exit orders (seconds). In live mode,
    /// exit orders take 1-5s to fill on the exchange; during that window
    /// the position is still held and the bot cannot enter a new trade.
    /// In dry_run BT mode exits are instant, which lets BT enter slightly
    /// earlier than live and cascades into entry-count mismatches.
    /// When > 0, the dry_run exit path defers position clearing by this
    /// many replay-seconds, keeping the position "held" during the delay.
    /// Env: BT_FILL_DELAY_SECS (default 0 = legacy instant-fill).
    pub bt_fill_delay_secs: i64,
    pub circuit_breaker_consecutive_losses: u32,
    pub circuit_breaker_cooldown_secs: u64,
    /// All per-pair tunables — z-score thresholds, hedge gates, lookback
    /// windows, circuit-breaker tiers, Phase 2 filters — live here. Engine
    /// reads them via `params_for(key)` so per-pair YAML overrides win.
    pub pair_params: HashMap<String, PairParams>,
    pub default_pair_params: PairParams,
    /// Graceful shutdown: max seconds to wait for natural pair exit on SIGTERM
    /// before force-closing both legs. 0 = immediate force close (legacy).
    pub shutdown_grace_secs: u64,
    /// Resolved strategy variants. Always non-empty: legacy single-bot YAML
    /// produces a single entry derived from top-level scalars; new
    /// multi-strategy YAML produces N entries (shigeo-nakamura/bot-strategy#25).
    pub strategies: Vec<StrategyConfig>,
    // Kalman filter beta estimation (log-only, disabled by default)
    pub use_kalman_beta: bool,
    pub kalman_q: f64,
    pub kalman_r: f64,
    pub kalman_initial_p: f64,
    pub kalman_min_updates: u64,
    // Regime filter (disabled by default: thresholds 0.0 → filter inactive)
    pub regime_vol_window: usize,
    pub regime_vol_max: f64,
    pub regime_trend_window: usize,
    pub regime_trend_max: f64,
    pub regime_reference_symbol: String,
}

/// Resolved per-strategy config for one A/B/C variant. Fields here override
/// the top-level scalar of the same name when an instance runs.
#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub id: String,
    pub agent_name: Option<String>,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub max_loss_r_mult: f64,
    pub risk_pct_per_trade: f64,
    pub equity_usd: f64,
    pub enable_data_dump: bool,
    pub data_dump_file: Option<String>,
    // Per-strategy PairParams overrides (None = inherit from top-level)
    pub force_close_time_secs: Option<u64>,
    pub mtf_windows: Option<Vec<usize>>,
    pub mtf_z_min: Option<f64>,
}

impl PairTradeConfig {
    pub fn params_for(&self, pair_key: &str) -> &PairParams {
        self.pair_params
            .get(pair_key)
            .unwrap_or(&self.default_pair_params)
    }

    /// Largest `force_close_secs` across the resolved default, per-pair
    /// overrides, and per-strategy overrides. The graceful-shutdown grace
    /// window must exceed this, or a position can be prematurely flushed by
    /// shutdown before its own `force_close` would have closed it.
    fn max_force_close_secs(&self) -> u64 {
        let mut m = self.default_pair_params.force_close_secs;
        for p in self.pair_params.values() {
            m = m.max(p.force_close_secs);
        }
        for s in &self.strategies {
            if let Some(fc) = s.force_close_time_secs {
                m = m.max(fc);
            }
        }
        m
    }

    /// Assert that `shutdown_grace_secs` covers the longest per-strategy /
    /// per-pair `force_close_secs` plus a small buffer. Catches config drift
    /// like bot-strategy#50, where a strategy's `force_close_time_secs` was
    /// extended without raising the global shutdown grace.
    fn validate(&self) -> Result<()> {
        const BUFFER_SECS: u64 = 60;
        // 0 = legacy immediate force-close on SIGTERM; no grace window to
        // validate.
        if self.shutdown_grace_secs == 0 {
            return Ok(());
        }
        let max_fc = self.max_force_close_secs();
        let required = max_fc.saturating_add(BUFFER_SECS);
        if self.shutdown_grace_secs < required {
            return Err(anyhow!(
                "shutdown_grace_secs ({}) is shorter than max force_close_time_secs ({}) + {}s buffer = {}. \
                 Graceful shutdown would force-close a position before its own force_close window expires (see bot-strategy#50).",
                self.shutdown_grace_secs,
                max_fc,
                BUFFER_SECS,
                required,
            ));
        }
        Ok(())
    }

    fn build_pair_params_map(
        &self,
        overrides: &Option<HashMap<String, PairOverrideYaml>>,
    ) -> HashMap<String, PairParams> {
        apply_pair_overrides(&self.default_pair_params, overrides)
    }

    pub fn from_env_or_yaml() -> Result<Self> {
        let config_path = env::var("PAIRTRADE_CONFIG_PATH")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                env::var("DEBOT_CONFIG")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            });
        if let Some(path) = config_path {
            return Self::from_yaml_path(path);
        }
        Self::from_env()
    }

    pub fn from_yaml_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let file = File::open(path_ref)
            .with_context(|| format!("failed to open PairTrade config {}", path_ref.display()))?;
        let yaml: PairTradeYaml = serde_yaml::from_reader(file)
            .with_context(|| format!("failed to parse PairTrade config {}", path_ref.display()))?;

        let history_file_from_yaml = yaml.history_file.is_some();
        let warm_start_min_from_yaml = yaml.warm_start_min_bars.is_some();

        let universe = resolve_universe_from_yaml(&yaml)?;
        let metrics_window = yaml.metrics_window_length.unwrap_or(DEFAULT_METRICS_WINDOW);
        let warm_start_mode = yaml
            .warm_start_mode
            .as_deref()
            .unwrap_or(DEFAULT_WARM_START_MODE)
            .parse()
            .unwrap_or(WarmStartMode::Strict);
        let history_file = yaml
            .history_file
            .clone()
            .unwrap_or_else(|| default_history_file(&universe, yaml.agent_name.as_deref()));

        // Build the resolved per-pair defaults before consuming `yaml` into the
        // PairTradeConfig literal below.
        let mut default_pair_params = default_pair_params_from_yaml(&yaml);
        if default_pair_params.warm_start_min_bars == 0 {
            default_pair_params.warm_start_min_bars = metrics_window;
        }

        let mut cfg = PairTradeConfig {
            dex_name: yaml.dex_name.unwrap_or_else(|| "hyperliquid".to_string()),
            rest_endpoint: yaml
                .rest_endpoint
                .unwrap_or_else(|| "https://api.hyperliquid.xyz".to_string()),
            web_socket_endpoint: yaml
                .web_socket_endpoint
                .unwrap_or_else(|| "wss://api.hyperliquid.xyz/ws".to_string()),
            dry_run: yaml.dry_run.unwrap_or(true),
            agent_name: yaml.agent_name,
            interval_secs: yaml.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS),
            trading_period_secs: yaml
                .trading_period_secs
                .unwrap_or(DEFAULT_TRADING_PERIOD_SECS),
            metrics_window,
            net_funding_min_per_hour: yaml
                .net_funding_min_per_hour
                .unwrap_or(DEFAULT_NET_FUNDING_MIN_PER_HOUR),
            notional_per_leg: yaml
                .notional_per_leg_usd
                .unwrap_or(DEFAULT_NOTIONAL_PER_LEG),
            risk_pct_per_trade: yaml
                .risk_pct_per_trade
                .unwrap_or(DEFAULT_RISK_PCT_PER_TRADE),
            equity_usd: yaml.equity_usd_fallback.unwrap_or(DEFAULT_EQUITY_USD),
            universe,
            slippage_bps: yaml.slippage_bps.unwrap_or(DEFAULT_SLIPPAGE_BPS),
            fee_bps: yaml.fee_bps.unwrap_or(DEFAULT_FEE_BPS),
            max_leverage: yaml.max_leverage.unwrap_or(DEFAULT_MAX_LEVERAGE),
            max_active_pairs: yaml.max_active_pairs.unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS),
            warm_start_mode,
            order_timeout_secs: yaml
                .order_timeout_secs
                .unwrap_or(DEFAULT_ORDER_TIMEOUT_SECS),
            entry_partial_fill_max_retries: yaml
                .entry_partial_fill_max_retries
                .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES),
            startup_force_close_attempts: yaml
                .startup_force_close_attempts
                .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS),
            startup_force_close_wait_secs: yaml
                .startup_force_close_wait_secs
                .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS),
            force_close_on_startup: yaml
                .force_close_on_startup
                .unwrap_or(DEFAULT_FORCE_CLOSE_ON_STARTUP),
            enable_data_dump: yaml.enable_data_dump.unwrap_or(false),
            data_dump_file: yaml.data_dump_file,
            observe_only: yaml.observe_only.unwrap_or(false),
            disable_history_persist: yaml.disable_history_persist.unwrap_or(false),
            history_file,
            history_archive_dir: yaml.history_archive_dir,
            history_archive_retention_days: yaml.history_archive_retention_days.unwrap_or(360),
            backtest_mode: yaml.backtest_mode.unwrap_or(false),
            backtest_file: yaml.backtest_file,
            bt_warm_start_snapshot: None, // env-only, not in YAML
            bt_eval_timestamps: None,     // env-only, not in YAML
            bt_restart_timestamps: None,  // env-only, not in YAML
            bt_fill_delay_secs: 0,         // env-only, not in YAML
            circuit_breaker_consecutive_losses: yaml
                .circuit_breaker_consecutive_losses
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES),
            circuit_breaker_cooldown_secs: yaml
                .circuit_breaker_cooldown_secs
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS),
            shutdown_grace_secs: yaml
                .shutdown_grace_secs
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECS),
            pair_params: HashMap::new(),
            default_pair_params,
            strategies: Vec::new(),
            use_kalman_beta: yaml.use_kalman_beta.unwrap_or(DEFAULT_USE_KALMAN_BETA),
            kalman_q: yaml.kalman_q.unwrap_or(DEFAULT_KALMAN_Q),
            kalman_r: yaml.kalman_r.unwrap_or(DEFAULT_KALMAN_R),
            kalman_initial_p: yaml.kalman_initial_p.unwrap_or(DEFAULT_KALMAN_INITIAL_P),
            kalman_min_updates: yaml.kalman_min_updates.unwrap_or(DEFAULT_KALMAN_MIN_UPDATES),
            regime_vol_window: yaml.regime_vol_window.unwrap_or(DEFAULT_REGIME_VOL_WINDOW),
            regime_vol_max: yaml.regime_vol_max.unwrap_or(DEFAULT_REGIME_VOL_MAX),
            regime_trend_window: yaml.regime_trend_window.unwrap_or(DEFAULT_REGIME_TREND_WINDOW),
            regime_trend_max: yaml.regime_trend_max.unwrap_or(DEFAULT_REGIME_TREND_MAX),
            regime_reference_symbol: yaml
                .regime_reference_symbol
                .clone()
                .unwrap_or_else(|| DEFAULT_REGIME_REFERENCE_SYMBOL.to_string()),
        };

        cfg.pair_params = cfg.build_pair_params_map(&yaml.pair_overrides);
        cfg.apply_env_overrides(history_file_from_yaml, warm_start_min_from_yaml)?;
        cfg.strategies = resolve_strategies(&cfg, yaml.strategies.as_deref());
        // apply_env_overrides mutates cfg.default_pair_params in place; re-merge
        // pair-specific overrides on top so YAML pair_overrides still win.
        let pair_params_rebuilt = cfg.build_pair_params_map(&yaml.pair_overrides);
        if !pair_params_rebuilt.is_empty() {
            cfg.pair_params = pair_params_rebuilt;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_env() -> Result<Self> {
        let dex_name = env::var("DEX_NAME").unwrap_or_else(|_| "hyperliquid".to_string());
        let rest_endpoint =
            env::var("REST_ENDPOINT").unwrap_or_else(|_| "https://api.hyperliquid.xyz".to_string());
        let web_socket_endpoint = env::var("WEB_SOCKET_ENDPOINT")
            .unwrap_or_else(|_| "wss://api.hyperliquid.xyz/ws".to_string());
        let dry_run = env::var("DRY_RUN")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == "true";
        let agent_name = env::var("AGENT_NAME").ok();
        let interval_secs = env::var("INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_INTERVAL_SECS);
        let trading_period_secs = env::var("TRADING_PERIOD_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_TRADING_PERIOD_SECS);
        let metrics_window = env::var("METRICS_WINDOW_LENGTH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_METRICS_WINDOW);
        let net_funding_min_per_hour = env::var("NET_FUNDING_MIN_PER_HOUR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_NET_FUNDING_MIN_PER_HOUR);
        let notional_per_leg = env::var("NOTIONAL_PER_LEG_USD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_NOTIONAL_PER_LEG);
        let risk_pct_per_trade = env::var("RISK_PCT_PER_TRADE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_RISK_PCT_PER_TRADE);
        let equity_usd = env::var("EQUITY_USD_FALLBACK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_EQUITY_USD);
        let universe = parse_universe_pairs()?;
        let slippage_bps = env::var("SLIPPAGE_BPS")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(DEFAULT_SLIPPAGE_BPS);
        let fee_bps = env::var("FEE_BPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_FEE_BPS);
        let max_leverage = env::var("MAX_LEVERAGE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_LEVERAGE);
        let max_active_pairs = env::var("MAX_ACTIVE_PAIRS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS);
        let warm_start_mode = env::var("WARM_START_MODE")
            .ok()
            .unwrap_or_else(|| DEFAULT_WARM_START_MODE.to_string())
            .parse()
            .unwrap_or(WarmStartMode::Strict);
        let order_timeout_secs = env::var("ORDER_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ORDER_TIMEOUT_SECS);
        let entry_partial_fill_max_retries = env::var("ENTRY_PARTIAL_FILL_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES);
        let startup_force_close_attempts = env::var("STARTUP_FORCE_CLOSE_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS);
        let startup_force_close_wait_secs = env::var("STARTUP_FORCE_CLOSE_WAIT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS);
        let force_close_on_startup = env::var("FORCE_CLOSE_ON_STARTUP")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes"
            })
            .unwrap_or(DEFAULT_FORCE_CLOSE_ON_STARTUP);

        let enable_data_dump = env::var("ENABLE_DATA_DUMP")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";
        let data_dump_file = env::var("DATA_DUMP_FILE").ok();
        if enable_data_dump && data_dump_file.is_none() {
            return Err(anyhow!(
                "DATA_DUMP_FILE must be set if ENABLE_DATA_DUMP is true"
            ));
        }
        let observe_only = env::var("OBSERVE_ONLY")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true"
            || enable_data_dump;
        let disable_history_persist = env::var("DISABLE_HISTORY_PERSIST")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes"
            })
            .unwrap_or(false);
        let history_file = env::var("PAIRTRADE_HISTORY_FILE")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| default_history_file(&universe, agent_name.as_deref()));

        let backtest_mode = env::var("BACKTEST_MODE")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";
        let backtest_file = env::var("BACKTEST_FILE").ok();
        if backtest_mode && backtest_file.is_none() {
            return Err(anyhow!(
                "BACKTEST_FILE must be set if BACKTEST_MODE is true"
            ));
        }

        let mut cfg = Self {
            dex_name,
            rest_endpoint,
            web_socket_endpoint,
            dry_run,
            agent_name,
            interval_secs,
            trading_period_secs,
            metrics_window,
            net_funding_min_per_hour,
            notional_per_leg,
            risk_pct_per_trade,
            equity_usd,
            universe,
            slippage_bps,
            fee_bps,
            max_leverage,
            max_active_pairs,
            warm_start_mode,
            order_timeout_secs,
            entry_partial_fill_max_retries,
            startup_force_close_attempts,
            startup_force_close_wait_secs,
            force_close_on_startup,
            enable_data_dump,
            data_dump_file,
            observe_only,
            disable_history_persist,
            history_file,
            history_archive_dir: env::var("HISTORY_ARCHIVE_DIR").ok().filter(|v| !v.trim().is_empty()),
            history_archive_retention_days: env::var("HISTORY_ARCHIVE_RETENTION_DAYS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(360),
            backtest_mode,
            backtest_file,
            bt_warm_start_snapshot: env::var("BT_WARM_START_SNAPSHOT").ok().filter(|v| !v.trim().is_empty()),
            bt_eval_timestamps: load_bt_eval_timestamps(),
            bt_restart_timestamps: load_bt_restart_timestamps(),
            bt_fill_delay_secs: env::var("BT_FILL_DELAY_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            circuit_breaker_consecutive_losses: env::var("CIRCUIT_BREAKER_CONSECUTIVE_LOSSES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES),
            circuit_breaker_cooldown_secs: env::var("CIRCUIT_BREAKER_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS),
            shutdown_grace_secs: env::var("SHUTDOWN_GRACE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECS),
            pair_params: HashMap::new(),
            // Placeholder rebuilt immediately below.
            default_pair_params: PairParams::default(),
            strategies: Vec::new(),
            use_kalman_beta: env::var("USE_KALMAN_BETA")
                .ok()
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(DEFAULT_USE_KALMAN_BETA),
            kalman_q: env_parse("KALMAN_Q", DEFAULT_KALMAN_Q),
            kalman_r: env_parse("KALMAN_R", DEFAULT_KALMAN_R),
            kalman_initial_p: env_parse("KALMAN_INITIAL_P", DEFAULT_KALMAN_INITIAL_P),
            kalman_min_updates: env_parse("KALMAN_MIN_UPDATES", DEFAULT_KALMAN_MIN_UPDATES),
            regime_vol_window: env_parse("REGIME_VOL_WINDOW", DEFAULT_REGIME_VOL_WINDOW),
            regime_vol_max: env_parse("REGIME_VOL_MAX", DEFAULT_REGIME_VOL_MAX),
            regime_trend_window: env_parse("REGIME_TREND_WINDOW", DEFAULT_REGIME_TREND_WINDOW),
            regime_trend_max: env_parse("REGIME_TREND_MAX", DEFAULT_REGIME_TREND_MAX),
            regime_reference_symbol: env::var("REGIME_REFERENCE_SYMBOL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_REGIME_REFERENCE_SYMBOL.to_string()),
        };
        cfg.default_pair_params = default_pair_params_from_env();
        if cfg.default_pair_params.warm_start_min_bars == 0 {
            cfg.default_pair_params.warm_start_min_bars = cfg.metrics_window;
        }
        cfg.strategies = resolve_strategies(&cfg, None);
        cfg.validate()?;
        Ok(cfg)
    }

    fn apply_env_overrides(
        &mut self,
        history_file_from_yaml: bool,
        warm_start_min_from_yaml: bool,
    ) -> Result<()> {
        if let Ok(value) = env::var("DEX_NAME") {
            if !value.trim().is_empty() {
                self.dex_name = value;
            }
        }
        if let Ok(value) = env::var("REST_ENDPOINT") {
            if !value.trim().is_empty() {
                self.rest_endpoint = value;
            }
        }
        if let Ok(value) = env::var("WEB_SOCKET_ENDPOINT") {
            if !value.trim().is_empty() {
                self.web_socket_endpoint = value;
            }
        }
        if let Ok(value) = env::var("DRY_RUN") {
            self.dry_run = value.to_lowercase() == "true";
        }
        if let Ok(value) = env::var("AGENT_NAME") {
            if !value.trim().is_empty() {
                self.agent_name = Some(value);
            }
        }

        let prev_metrics_window = self.metrics_window;
        env_override("INTERVAL_SECS", &mut self.interval_secs);
        env_override("TRADING_PERIOD_SECS", &mut self.trading_period_secs);
        env_override("METRICS_WINDOW_LENGTH", &mut self.metrics_window);
        env_override("ENTRY_Z_SCORE_BASE", &mut self.default_pair_params.entry_z_base);
        env_override("ENTRY_Z_SCORE_MIN", &mut self.default_pair_params.entry_z_min);
        env_override("ENTRY_Z_SCORE_MAX", &mut self.default_pair_params.entry_z_max);
        env_override("EXIT_Z_SCORE", &mut self.default_pair_params.exit_z);
        env_override("STOP_LOSS_Z_SCORE", &mut self.default_pair_params.stop_loss_z);
        env_override("FORCE_CLOSE_TIME_SECS", &mut self.default_pair_params.force_close_secs);
        env_override("COOLDOWN_SECS", &mut self.default_pair_params.cooldown_secs);
        env_override("NET_FUNDING_MIN_PER_HOUR", &mut self.net_funding_min_per_hour);
        env_override("SPREAD_VELOCITY_MAX_SIGMA_PER_MIN", &mut self.default_pair_params.spread_velocity_max_sigma_per_min);
        env_override("NOTIONAL_PER_LEG_USD", &mut self.notional_per_leg);
        env_override("RISK_PCT_PER_TRADE", &mut self.risk_pct_per_trade);
        env_override("MAX_LOSS_R_MULT", &mut self.default_pair_params.max_loss_r_mult);
        env_override("EQUITY_USD_FALLBACK", &mut self.equity_usd);
        env_override("PAIR_SELECTION_LOOKBACK_HOURS_SHORT", &mut self.default_pair_params.lookback_hours_short);
        env_override("PAIR_SELECTION_LOOKBACK_HOURS_LONG", &mut self.default_pair_params.lookback_hours_long);
        env_override("HALF_LIFE_MAX_HOURS", &mut self.default_pair_params.half_life_max_hours);
        env_override("ADF_P_THRESHOLD", &mut self.default_pair_params.adf_p_threshold);
        env_override("ENTRY_VOL_LOOKBACK_HOURS", &mut self.default_pair_params.entry_vol_lookback_hours);
        if let Ok(value) = env::var("SLIPPAGE_BPS") {
            if let Ok(parsed) = value.parse::<i32>() {
                self.slippage_bps = parsed;
            }
        }
        env_override("FEE_BPS", &mut self.fee_bps);
        env_override("MAX_LEVERAGE", &mut self.max_leverage);
        env_override("REEVAL_JUMP_Z_MULT", &mut self.default_pair_params.reeval_jump_z_mult);
        env_override("VOL_SPIKE_MULT", &mut self.default_pair_params.vol_spike_mult);
        env_override("MAX_ACTIVE_PAIRS", &mut self.max_active_pairs);
        env_override("WARM_START_MODE", &mut self.warm_start_mode);
        let mut warm_start_min_overridden = false;
        if let Ok(value) = env::var("WARM_START_MIN_BARS") {
            if let Ok(parsed) = value.parse() {
                self.default_pair_params.warm_start_min_bars = parsed;
                warm_start_min_overridden = true;
            }
        }
        if !warm_start_min_overridden
            && !warm_start_min_from_yaml
            && self.default_pair_params.warm_start_min_bars == prev_metrics_window
            && self.metrics_window != prev_metrics_window
        {
            self.default_pair_params.warm_start_min_bars = self.metrics_window;
        }
        env_override("ORDER_TIMEOUT_SECS", &mut self.order_timeout_secs);
        env_override("ENTRY_PARTIAL_FILL_MAX_RETRIES", &mut self.entry_partial_fill_max_retries);
        if let Ok(value) = env::var("STARTUP_FORCE_CLOSE_ATTEMPTS") {
            if let Ok(parsed) = value.parse::<u32>() {
                if parsed > 0 {
                    self.startup_force_close_attempts = parsed;
                }
            }
        }
        if let Ok(value) = env::var("STARTUP_FORCE_CLOSE_WAIT_SECS") {
            if let Ok(parsed) = value.parse::<u64>() {
                self.startup_force_close_wait_secs = parsed;
            }
        }
        if let Ok(value) = env::var("FORCE_CLOSE_ON_STARTUP") {
            let lower = value.trim().to_ascii_lowercase();
            self.force_close_on_startup = lower == "1" || lower == "true" || lower == "yes";
        }

        let env_pairs = env::var("UNIVERSE_PAIRS")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let env_symbols = env::var("UNIVERSE_SYMBOLS")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let universe_overridden = env_pairs.is_some() || env_symbols.is_some();
        if universe_overridden {
            self.universe = parse_universe_pairs()?;
        }

        if let Ok(value) = env::var("ENABLE_DATA_DUMP") {
            self.enable_data_dump = value.to_lowercase() == "true";
        }
        if let Ok(value) = env::var("DATA_DUMP_FILE") {
            if !value.trim().is_empty() {
                self.data_dump_file = Some(value);
            }
        }
        if self.enable_data_dump && self.data_dump_file.is_none() {
            return Err(anyhow!(
                "DATA_DUMP_FILE must be set if ENABLE_DATA_DUMP is true"
            ));
        }

        if let Ok(value) = env::var("OBSERVE_ONLY") {
            self.observe_only = value.to_lowercase() == "true";
        }
        // Note: enable_data_dump no longer forces observe_only. Data dump
        // is just JSONL writes to disk and is independent of trading.
        // The previous forced linkage prevented running a bot that both
        // collects data and trades live (e.g. debot-pair-btceth running
        // as the data collector while the A leg of an A/B test).

        if let Ok(value) = env::var("DISABLE_HISTORY_PERSIST") {
            let lower = value.trim().to_ascii_lowercase();
            self.disable_history_persist = lower == "1" || lower == "true" || lower == "yes";
        }
        if let Ok(value) = env::var("PAIRTRADE_HISTORY_FILE") {
            if !value.trim().is_empty() {
                self.history_file = value.trim().to_string();
            }
        } else if universe_overridden && !history_file_from_yaml {
            self.history_file = default_history_file(&self.universe, self.agent_name.as_deref());
        }

        if let Ok(value) = env::var("BACKTEST_MODE") {
            self.backtest_mode = value.to_lowercase() == "true";
        }
        if let Ok(value) = env::var("BACKTEST_FILE") {
            if !value.trim().is_empty() {
                self.backtest_file = Some(value);
            }
        }
        if self.backtest_mode && self.backtest_file.is_none() {
            return Err(anyhow!(
                "BACKTEST_FILE must be set if BACKTEST_MODE is true"
            ));
        }
        if let Ok(value) = env::var("BT_WARM_START_SNAPSHOT") {
            if !value.trim().is_empty() {
                self.bt_warm_start_snapshot = Some(value);
            }
        }
        // BT eval-timestamp replay — see struct field doc.
        if env::var("BT_EVAL_TIMESTAMPS_FILE").is_ok() {
            self.bt_eval_timestamps = load_bt_eval_timestamps();
        }
        // BT restart-timestamp replay — see struct field doc.
        if env::var("BT_RESTART_TIMESTAMPS_FILE").is_ok() {
            self.bt_restart_timestamps = load_bt_restart_timestamps();
        }
        env_override("BT_FILL_DELAY_SECS", &mut self.bt_fill_delay_secs);

        env_override("SPREAD_TREND_MAX_SLOPE_SIGMA", &mut self.default_pair_params.spread_trend_max_slope_sigma);
        env_override("BETA_DIVERGENCE_MAX", &mut self.default_pair_params.beta_divergence_max);
        env_override("CIRCUIT_BREAKER_CONSECUTIVE_LOSSES", &mut self.circuit_breaker_consecutive_losses);
        env_override("CIRCUIT_BREAKER_COOLDOWN_SECS", &mut self.circuit_breaker_cooldown_secs);
        env_override("CIRCUIT_BREAKER_TIER1_LOSSES", &mut self.default_pair_params.circuit_breaker_tier1_losses);
        env_override("CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS", &mut self.default_pair_params.circuit_breaker_tier1_cooldown_secs);
        env_override("CIRCUIT_BREAKER_TIER2_LOSSES", &mut self.default_pair_params.circuit_breaker_tier2_losses);
        env_override("CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS", &mut self.default_pair_params.circuit_breaker_tier2_cooldown_secs);
        env_override("ENTRY_POST_ONLY_TIMEOUT_SECS", &mut self.default_pair_params.entry_post_only_timeout_secs);
        env_override("ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN", &mut self.default_pair_params.entry_velocity_block_sigma_per_min);
        env_override("FUNDING_ENTRY_Z_SCALE", &mut self.default_pair_params.funding_entry_z_scale);
        env_override("BETA_GAP_ENTRY_Z_SCALE", &mut self.default_pair_params.beta_gap_entry_z_scale);
        if let Ok(value) = env::var("MTF_WINDOWS") {
            self.default_pair_params.mtf_windows = value
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
        }
        env_override("MTF_Z_MIN", &mut self.default_pair_params.mtf_z_min);
        env_override(
            "STD_COLLAPSE_WINDOW_BARS",
            &mut self.default_pair_params.std_collapse_window_bars,
        );
        env_override(
            "STD_COLLAPSE_MIN_RATIO",
            &mut self.default_pair_params.std_collapse_min_ratio,
        );
        if let Ok(value) = env::var("STD_COLLAPSE_OBSERVE_ONLY") {
            let lower = value.trim().to_ascii_lowercase();
            self.default_pair_params.std_collapse_observe_only =
                matches!(lower.as_str(), "1" | "true" | "yes");
        }

        // Kalman filter
        if let Ok(value) = env::var("USE_KALMAN_BETA") {
            self.use_kalman_beta = value.to_lowercase() == "true";
        }
        env_override("KALMAN_Q", &mut self.kalman_q);
        env_override("KALMAN_R", &mut self.kalman_r);
        env_override("KALMAN_INITIAL_P", &mut self.kalman_initial_p);
        env_override("KALMAN_MIN_UPDATES", &mut self.kalman_min_updates);

        // Regime filter
        env_override("REGIME_VOL_WINDOW", &mut self.regime_vol_window);
        env_override("REGIME_VOL_MAX", &mut self.regime_vol_max);
        env_override("REGIME_TREND_WINDOW", &mut self.regime_trend_window);
        env_override("REGIME_TREND_MAX", &mut self.regime_trend_max);
        if let Ok(value) = env::var("REGIME_REFERENCE_SYMBOL") {
            if !value.trim().is_empty() {
                self.regime_reference_symbol = value;
            }
        }

        Ok(())
    }

    pub(super) fn slippage_cost_bps(&self) -> f64 {
        self.slippage_bps.max(0) as f64
    }

    pub(super) fn circuit_breaker_cooldown_for(&self, losses: u32) -> Option<Duration> {
        let dpp = &self.default_pair_params;
        // Graduated tiers (check tier2 first as higher threshold)
        if dpp.circuit_breaker_tier2_losses > 0 && losses >= dpp.circuit_breaker_tier2_losses {
            return Some(Duration::from_secs(dpp.circuit_breaker_tier2_cooldown_secs));
        }
        if dpp.circuit_breaker_tier1_losses > 0 && losses >= dpp.circuit_breaker_tier1_losses {
            return Some(Duration::from_secs(dpp.circuit_breaker_tier1_cooldown_secs));
        }
        // Legacy fallback
        if self.circuit_breaker_consecutive_losses > 0
            && losses >= self.circuit_breaker_consecutive_losses
        {
            return Some(Duration::from_secs(self.circuit_breaker_cooldown_secs));
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmStartMode {
    Strict,
    Relaxed,
}

impl std::str::FromStr for WarmStartMode {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "strict" => Ok(WarmStartMode::Strict),
            "relaxed" => Ok(WarmStartMode::Relaxed),
            _ => Err(()),
        }
    }
}

fn default_history_file(universe: &[PairSpec], _agent_name: Option<&str>) -> String {
    let mut symbols: Vec<String> = universe
        .iter()
        .flat_map(|p| [p.base.clone(), p.quote.clone()])
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    // A/B/C bots watching the same pair on the same host intentionally share
    // one history file so their rolling regression windows stay identical
    // (pairtrade#4). The shared file is written atomically via tmpfile+rename
    // in persist_history_to_disk to avoid torn reads under concurrent writers.
    if symbols.is_empty() {
        return "pairtrade_history.json".to_string();
    }
    symbols.sort();
    let parts: Vec<String> = symbols
        .into_iter()
        .map(|sym| sanitize_symbol_for_filename(&sym))
        .filter(|sym| !sym.is_empty())
        .collect();
    if parts.is_empty() {
        return "pairtrade_history.json".to_string();
    }
    format!("pairtrade_history_{}.json", parts.join("_"))
}

fn sanitize_symbol_for_filename(symbol: &str) -> String {
    let mut out = String::with_capacity(symbol.len());
    for ch in symbol.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

fn env_parse<T: std::str::FromStr>(key: &str, fallback: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

/// If `key` is set in the environment AND parses, overwrite `target`. Used by
/// `apply_env_overrides` to collapse the dozens of `if let Ok(value)…` blocks.
fn env_override<T: std::str::FromStr>(key: &str, target: &mut T) {
    if let Ok(value) = env::var(key) {
        if let Ok(parsed) = value.parse() {
            *target = parsed;
        }
    }
}

/// Load the BT eval-timestamps file (one UNIX second per line) referenced by
/// the `BT_EVAL_TIMESTAMPS_FILE` env var. Ignored silently when the env var
/// is unset, the path is unreadable, or no numeric lines are found — live
/// mode and vanilla BT (without the override) must stay unchanged.
fn load_bt_eval_timestamps() -> Option<std::collections::HashSet<i64>> {
    load_ts_set("BT_EVAL_TIMESTAMPS_FILE", "[BT_EVAL_TIMESTAMPS]")
}

/// Load BT restart timestamps (one UNIX second per line). See
/// `PairTradeConfig::bt_restart_timestamps` for semantics.
fn load_bt_restart_timestamps() -> Option<std::collections::HashSet<i64>> {
    load_ts_set("BT_RESTART_TIMESTAMPS_FILE", "[BT_RESTART_TIMESTAMPS]")
}

fn load_ts_set(env_key: &str, tag: &str) -> Option<std::collections::HashSet<i64>> {
    use std::collections::HashSet;
    let path = env::var(env_key).ok()?;
    let path = path.trim();
    if path.is_empty() {
        return None;
    }
    let contents = std::fs::read_to_string(path)
        .map_err(|e| log::warn!("{} failed to read {}: {}", tag, path, e))
        .ok()?;
    let mut set: HashSet<i64> = HashSet::new();
    for line in contents.lines() {
        if let Ok(ts) = line.trim().parse::<i64>() {
            set.insert(ts);
        }
    }
    if set.is_empty() {
        log::warn!("{} {} contained no parseable timestamps", tag, path);
        return None;
    }
    log::info!("{} loaded {} timestamps from {}", tag, set.len(), path);
    Some(set)
}

/// Resolve global per-pair defaults from environment variables, falling back
/// to compile-time `DEFAULT_*` constants for any missing entries.
pub(super) fn default_pair_params_from_env() -> PairParams {
    PairParams {
        entry_z_base: env_parse("ENTRY_Z_SCORE_BASE", DEFAULT_ENTRY_Z_BASE),
        entry_z_min: env_parse("ENTRY_Z_SCORE_MIN", DEFAULT_ENTRY_Z_MIN),
        entry_z_max: env_parse("ENTRY_Z_SCORE_MAX", DEFAULT_ENTRY_Z_MAX),
        exit_z: env_parse("EXIT_Z_SCORE", DEFAULT_EXIT_Z),
        stop_loss_z: env_parse("STOP_LOSS_Z_SCORE", DEFAULT_STOP_LOSS_Z),
        force_close_secs: env_parse("FORCE_CLOSE_TIME_SECS", DEFAULT_FORCE_CLOSE_SECS),
        cooldown_secs: env_parse("COOLDOWN_SECS", DEFAULT_COOLDOWN_SECS),
        max_loss_r_mult: env_parse("MAX_LOSS_R_MULT", DEFAULT_MAX_LOSS_R_MULT),
        half_life_max_hours: env_parse("HALF_LIFE_MAX_HOURS", DEFAULT_HALF_LIFE_MAX_HOURS),
        adf_p_threshold: env_parse("ADF_P_THRESHOLD", DEFAULT_ADF_P_THRESHOLD),
        spread_velocity_max_sigma_per_min: env_parse(
            "SPREAD_VELOCITY_MAX_SIGMA_PER_MIN",
            DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN,
        ),
        spread_trend_max_slope_sigma: env_parse(
            "SPREAD_TREND_MAX_SLOPE_SIGMA",
            DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA,
        ),
        beta_divergence_max: env_parse("BETA_DIVERGENCE_MAX", DEFAULT_BETA_DIVERGENCE_MAX),
        beta_min: env_parse("BETA_MIN", 0.0),
        hedge_ratio_max_deviation: env_parse("HEDGE_RATIO_MAX_DEVIATION", 1.0),
        lookback_hours_short: env_parse(
            "PAIR_SELECTION_LOOKBACK_HOURS_SHORT",
            DEFAULT_LOOKBACK_HOURS_SHORT,
        ),
        lookback_hours_long: env_parse(
            "PAIR_SELECTION_LOOKBACK_HOURS_LONG",
            DEFAULT_LOOKBACK_HOURS_LONG,
        ),
        entry_vol_lookback_hours: env_parse(
            "ENTRY_VOL_LOOKBACK_HOURS",
            DEFAULT_ENTRY_VOL_LOOKBACK_HOURS,
        ),
        // Caller is responsible for filling warm_start_min_bars from
        // metrics_window when omitted.
        warm_start_min_bars: env_parse::<usize>("WARM_START_MIN_BARS", 0),
        reeval_jump_z_mult: env_parse("REEVAL_JUMP_Z_MULT", DEFAULT_REEVAL_JUMP_Z_MULT),
        vol_spike_mult: env_parse("VOL_SPIKE_MULT", DEFAULT_VOL_SPIKE_MULT),
        circuit_breaker_tier1_losses: env_parse("CIRCUIT_BREAKER_TIER1_LOSSES", DEFAULT_CB_TIER1_LOSSES),
        circuit_breaker_tier1_cooldown_secs: env_parse(
            "CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS",
            DEFAULT_CB_TIER1_COOLDOWN_SECS,
        ),
        circuit_breaker_tier2_losses: env_parse("CIRCUIT_BREAKER_TIER2_LOSSES", DEFAULT_CB_TIER2_LOSSES),
        circuit_breaker_tier2_cooldown_secs: env_parse(
            "CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS",
            DEFAULT_CB_TIER2_COOLDOWN_SECS,
        ),
        entry_post_only_timeout_secs: env_parse(
            "ENTRY_POST_ONLY_TIMEOUT_SECS",
            DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS,
        ),
        entry_velocity_block_sigma_per_min: env_parse("ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN", 0.0),
        funding_entry_z_scale: env_parse("FUNDING_ENTRY_Z_SCALE", 0.0),
        beta_gap_entry_z_scale: env_parse("BETA_GAP_ENTRY_Z_SCALE", 0.0),
        mtf_windows: env::var("MTF_WINDOWS")
            .ok()
            .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
            .unwrap_or_default(),
        mtf_z_min: env_parse("MTF_Z_MIN", DEFAULT_MTF_Z_MIN),
        std_collapse_window_bars: env_parse(
            "STD_COLLAPSE_WINDOW_BARS",
            DEFAULT_STD_COLLAPSE_WINDOW_BARS,
        ),
        std_collapse_min_ratio: env_parse(
            "STD_COLLAPSE_MIN_RATIO",
            DEFAULT_STD_COLLAPSE_MIN_RATIO,
        ),
        std_collapse_observe_only: env::var("STD_COLLAPSE_OBSERVE_ONLY")
            .ok()
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_STD_COLLAPSE_OBSERVE_ONLY),
    }
}

/// Build the resolved `strategies: Vec<StrategyConfig>` for a `PairTradeConfig`.
///
/// If the YAML supplied a `strategies:` list, every entry becomes one
/// `StrategyConfig` and any unset field falls back to the resolved
/// top-level value already on `cfg`. If `yaml_strategies` is `None`
/// (legacy single-bot YAML, or env-only `from_env`), this returns a
/// single `StrategyConfig` derived entirely from the top-level scalars,
/// preserving today's behavior.
///
/// commit 2 of shigeo-nakamura/bot-strategy#25: parsing only — the
/// engine still runs `instances.len() == 1` and does not yet branch on
/// per-strategy values.
pub(super) fn resolve_strategies(
    cfg: &PairTradeConfig,
    yaml_strategies: Option<&[StrategyYaml]>,
) -> Vec<StrategyConfig> {
    let default_id = cfg
        .agent_name
        .clone()
        .unwrap_or_else(|| "default".to_string());
    match yaml_strategies {
        Some(list) if !list.is_empty() => list
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                let id = s
                    .id
                    .clone()
                    .or_else(|| s.agent_name.clone())
                    .unwrap_or_else(|| format!("strategy-{}", idx));
                StrategyConfig {
                    id,
                    agent_name: s.agent_name.clone().or_else(|| cfg.agent_name.clone()),
                    exit_z: s.exit_z_score.unwrap_or(cfg.default_pair_params.exit_z),
                    stop_loss_z: s
                        .stop_loss_z_score
                        .unwrap_or(cfg.default_pair_params.stop_loss_z),
                    max_loss_r_mult: s
                        .max_loss_r_mult
                        .unwrap_or(cfg.default_pair_params.max_loss_r_mult),
                    risk_pct_per_trade: s
                        .risk_pct_per_trade
                        .unwrap_or(cfg.risk_pct_per_trade),
                    equity_usd: s.equity_usd_fallback.unwrap_or(cfg.equity_usd),
                    enable_data_dump: s.enable_data_dump.unwrap_or(cfg.enable_data_dump),
                    data_dump_file: s
                        .data_dump_file
                        .clone()
                        .or_else(|| cfg.data_dump_file.clone()),
                    force_close_time_secs: s.force_close_time_secs,
                    mtf_windows: s.mtf_windows.clone(),
                    mtf_z_min: s.mtf_z_min,
                }
            })
            .collect(),
        _ => vec![StrategyConfig {
            id: default_id,
            agent_name: cfg.agent_name.clone(),
            exit_z: cfg.default_pair_params.exit_z,
            stop_loss_z: cfg.default_pair_params.stop_loss_z,
            max_loss_r_mult: cfg.default_pair_params.max_loss_r_mult,
            risk_pct_per_trade: cfg.risk_pct_per_trade,
            equity_usd: cfg.equity_usd,
            enable_data_dump: cfg.enable_data_dump,
            data_dump_file: cfg.data_dump_file.clone(),
            force_close_time_secs: None,
            mtf_windows: None,
            mtf_z_min: None,
        }],
    }
}

/// Resolve global per-pair defaults directly from a YAML document, falling
/// back to compile-time `DEFAULT_*` constants for any missing fields.
pub(super) fn default_pair_params_from_yaml(yaml: &PairTradeYaml) -> PairParams {
    PairParams {
        entry_z_base: yaml.entry_z_score_base.unwrap_or(DEFAULT_ENTRY_Z_BASE),
        entry_z_min: yaml.entry_z_score_min.unwrap_or(DEFAULT_ENTRY_Z_MIN),
        entry_z_max: yaml.entry_z_score_max.unwrap_or(DEFAULT_ENTRY_Z_MAX),
        exit_z: yaml.exit_z_score.unwrap_or(DEFAULT_EXIT_Z),
        stop_loss_z: yaml.stop_loss_z_score.unwrap_or(DEFAULT_STOP_LOSS_Z),
        force_close_secs: yaml
            .force_close_time_secs
            .unwrap_or(DEFAULT_FORCE_CLOSE_SECS),
        cooldown_secs: yaml.cooldown_secs.unwrap_or(DEFAULT_COOLDOWN_SECS),
        max_loss_r_mult: yaml.max_loss_r_mult.unwrap_or(DEFAULT_MAX_LOSS_R_MULT),
        half_life_max_hours: yaml
            .half_life_max_hours
            .unwrap_or(DEFAULT_HALF_LIFE_MAX_HOURS),
        adf_p_threshold: yaml.adf_p_threshold.unwrap_or(DEFAULT_ADF_P_THRESHOLD),
        spread_velocity_max_sigma_per_min: yaml
            .spread_velocity_max_sigma_per_min
            .unwrap_or(DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN),
        spread_trend_max_slope_sigma: yaml
            .spread_trend_max_slope_sigma
            .unwrap_or(DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA),
        beta_divergence_max: yaml
            .beta_divergence_max
            .unwrap_or(DEFAULT_BETA_DIVERGENCE_MAX),
        beta_min: yaml.beta_min.unwrap_or(0.0),
        hedge_ratio_max_deviation: yaml.hedge_ratio_max_deviation.unwrap_or(1.0),
        lookback_hours_short: yaml
            .pair_selection_lookback_hours_short
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_SHORT),
        lookback_hours_long: yaml
            .pair_selection_lookback_hours_long
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_LONG),
        entry_vol_lookback_hours: yaml
            .entry_vol_lookback_hours
            .unwrap_or(DEFAULT_ENTRY_VOL_LOOKBACK_HOURS),
        // Caller is responsible for clamping warm_start_min_bars to
        // metrics_window when omitted (it has a cross-field default).
        warm_start_min_bars: yaml.warm_start_min_bars.unwrap_or(0),
        reeval_jump_z_mult: yaml
            .reeval_jump_z_mult
            .unwrap_or(DEFAULT_REEVAL_JUMP_Z_MULT),
        vol_spike_mult: yaml.vol_spike_mult.unwrap_or(DEFAULT_VOL_SPIKE_MULT),
        circuit_breaker_tier1_losses: yaml
            .circuit_breaker_tier1_losses
            .unwrap_or(DEFAULT_CB_TIER1_LOSSES),
        circuit_breaker_tier1_cooldown_secs: yaml
            .circuit_breaker_tier1_cooldown_secs
            .unwrap_or(DEFAULT_CB_TIER1_COOLDOWN_SECS),
        circuit_breaker_tier2_losses: yaml
            .circuit_breaker_tier2_losses
            .unwrap_or(DEFAULT_CB_TIER2_LOSSES),
        circuit_breaker_tier2_cooldown_secs: yaml
            .circuit_breaker_tier2_cooldown_secs
            .unwrap_or(DEFAULT_CB_TIER2_COOLDOWN_SECS),
        entry_post_only_timeout_secs: yaml
            .entry_post_only_timeout_secs
            .unwrap_or(DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS),
        entry_velocity_block_sigma_per_min: yaml
            .entry_velocity_block_sigma_per_min
            .unwrap_or(0.0),
        funding_entry_z_scale: yaml.funding_entry_z_scale.unwrap_or(0.0),
        beta_gap_entry_z_scale: yaml.beta_gap_entry_z_scale.unwrap_or(0.0),
        mtf_windows: yaml.mtf_windows.clone().unwrap_or_default(),
        mtf_z_min: yaml.mtf_z_min.unwrap_or(DEFAULT_MTF_Z_MIN),
        std_collapse_window_bars: yaml
            .std_collapse_window_bars
            .unwrap_or(DEFAULT_STD_COLLAPSE_WINDOW_BARS),
        std_collapse_min_ratio: yaml
            .std_collapse_min_ratio
            .unwrap_or(DEFAULT_STD_COLLAPSE_MIN_RATIO),
        std_collapse_observe_only: yaml
            .std_collapse_observe_only
            .unwrap_or(DEFAULT_STD_COLLAPSE_OBSERVE_ONLY),
    }
}

/// Build the resolved per-pair params map from the resolved global defaults
/// plus any per-pair YAML overrides. Free function so it does not depend on
/// `PairTradeConfig`'s currently-duplicated per-pair fields.
fn apply_pair_overrides(
    default: &PairParams,
    overrides: &Option<HashMap<String, PairOverrideYaml>>,
) -> HashMap<String, PairParams> {
    let mut map = HashMap::new();
    let Some(overrides) = overrides else {
        return map;
    };
    for (pair_key, ovr) in overrides {
        let pp = PairParams {
            entry_z_base: ovr.entry_z_score_base.unwrap_or(default.entry_z_base),
            entry_z_min: ovr.entry_z_score_min.unwrap_or(default.entry_z_min),
            entry_z_max: ovr.entry_z_score_max.unwrap_or(default.entry_z_max),
            exit_z: ovr.exit_z_score.unwrap_or(default.exit_z),
            stop_loss_z: ovr.stop_loss_z_score.unwrap_or(default.stop_loss_z),
            force_close_secs: ovr.force_close_time_secs.unwrap_or(default.force_close_secs),
            cooldown_secs: ovr.cooldown_secs.unwrap_or(default.cooldown_secs),
            max_loss_r_mult: ovr.max_loss_r_mult.unwrap_or(default.max_loss_r_mult),
            half_life_max_hours: ovr
                .half_life_max_hours
                .unwrap_or(default.half_life_max_hours),
            adf_p_threshold: ovr.adf_p_threshold.unwrap_or(default.adf_p_threshold),
            spread_velocity_max_sigma_per_min: ovr
                .spread_velocity_max_sigma_per_min
                .unwrap_or(default.spread_velocity_max_sigma_per_min),
            spread_trend_max_slope_sigma: ovr
                .spread_trend_max_slope_sigma
                .unwrap_or(default.spread_trend_max_slope_sigma),
            beta_divergence_max: ovr
                .beta_divergence_max
                .unwrap_or(default.beta_divergence_max),
            beta_min: ovr.beta_min.unwrap_or(default.beta_min),
            hedge_ratio_max_deviation: ovr
                .hedge_ratio_max_deviation
                .unwrap_or(default.hedge_ratio_max_deviation),
            lookback_hours_short: ovr
                .pair_selection_lookback_hours_short
                .unwrap_or(default.lookback_hours_short),
            lookback_hours_long: ovr
                .pair_selection_lookback_hours_long
                .unwrap_or(default.lookback_hours_long),
            entry_vol_lookback_hours: ovr
                .entry_vol_lookback_hours
                .unwrap_or(default.entry_vol_lookback_hours),
            warm_start_min_bars: ovr
                .warm_start_min_bars
                .unwrap_or(default.warm_start_min_bars),
            reeval_jump_z_mult: ovr
                .reeval_jump_z_mult
                .unwrap_or(default.reeval_jump_z_mult),
            vol_spike_mult: ovr.vol_spike_mult.unwrap_or(default.vol_spike_mult),
            circuit_breaker_tier1_losses: ovr
                .circuit_breaker_tier1_losses
                .unwrap_or(default.circuit_breaker_tier1_losses),
            circuit_breaker_tier1_cooldown_secs: ovr
                .circuit_breaker_tier1_cooldown_secs
                .unwrap_or(default.circuit_breaker_tier1_cooldown_secs),
            circuit_breaker_tier2_losses: ovr
                .circuit_breaker_tier2_losses
                .unwrap_or(default.circuit_breaker_tier2_losses),
            circuit_breaker_tier2_cooldown_secs: ovr
                .circuit_breaker_tier2_cooldown_secs
                .unwrap_or(default.circuit_breaker_tier2_cooldown_secs),
            entry_post_only_timeout_secs: ovr
                .entry_post_only_timeout_secs
                .unwrap_or(default.entry_post_only_timeout_secs),
            entry_velocity_block_sigma_per_min: default.entry_velocity_block_sigma_per_min,
            funding_entry_z_scale: default.funding_entry_z_scale,
            beta_gap_entry_z_scale: default.beta_gap_entry_z_scale,
            mtf_windows: default.mtf_windows.clone(),
            mtf_z_min: default.mtf_z_min,
            std_collapse_window_bars: default.std_collapse_window_bars,
            std_collapse_min_ratio: default.std_collapse_min_ratio,
            std_collapse_observe_only: default.std_collapse_observe_only,
        };
        map.insert(pair_key.clone(), pp);
    }
    map
}
