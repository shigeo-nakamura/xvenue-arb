//! xvenue-arb live runner config (YAML).
//!
//! Flat field layout to match pairtrade's house style. The runner reads
//! this from `$XVENUE_CONFIG_PATH` (or the default per-symbol path) at
//! startup, then builds the typed engine sub-configs via
//! [`XvenueConfig::spread_config`] / [`XvenueConfig::signal_config`].
//!
//! Field semantics: `docs/execution_layer.md` (timeout policy + IPC),
//! `docs/DESIGN.md` §6 (overall schema), and the per-field comments
//! below.
//!
//! Credentials (Lighter / Extended API keys, KMS data key) are NOT
//! here — those stay in env vars for `src/config.rs` to load.

use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use super::signal::SignalConfig;
use super::spread::SpreadConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct XvenueConfig {
    // ---- Identity / ops ----
    pub agent_name: String,
    #[serde(default)]
    pub dry_run: bool,

    // ---- Symbol pair (one symbol per process) ----
    pub symbol_ext: String,
    pub symbol_lt: String,

    // ---- Spread engine ----
    #[serde(default = "default_spread_bucket_ms")]
    pub spread_bucket_ms: u64,
    #[serde(default = "default_rolling_window_sec")]
    pub rolling_window_sec: u64,
    /// Drop spread samples whose `|spread|` exceeds this. `None`
    /// disables the filter. Default 100 bps mirrors the v2 sim.
    #[serde(default = "default_max_abs_spread_bps")]
    pub max_abs_spread_bps: Option<f64>,
    #[serde(default = "default_min_warmup_samples")]
    pub min_warmup_samples: usize,

    // ---- Signal engine ----
    #[serde(default = "default_abs_threshold_bps")]
    pub abs_threshold_bps: f64,
    #[serde(default = "default_persistence_sec")]
    pub persistence_sec: u64,
    #[serde(default = "default_true")]
    pub exit_at_mean_cross: bool,
    #[serde(default = "default_max_hold_sec")]
    pub max_hold_sec: u64,
    #[serde(default = "default_force_close_dev_bps")]
    pub force_close_dev_bps: f64,
    #[serde(default = "default_true")]
    pub entry_check_threshold_at_fire: bool,
    /// Funding settle cadence (seconds). 0 disables the lockout.
    #[serde(default = "default_funding_cycle_sec")]
    pub funding_cycle_sec: u64,
    /// Block new entries within this many seconds *before* settle.
    #[serde(default = "default_funding_lockout_pre_sec")]
    pub funding_lockout_pre_sec: u64,
    /// Block new entries within this many seconds *after* settle.
    #[serde(default = "default_funding_lockout_post_sec")]
    pub funding_lockout_post_sec: u64,

    // ---- Sizing ----
    /// % of (ext_equity + lt_equity). 0.05 = 5%.
    pub trade_size_pct: f64,
    pub min_notional_usd: f64,
    pub max_notional_usd: f64,

    // ---- Execution: Extended ----
    #[serde(default = "default_true")]
    pub extended_post_only: bool,
    #[serde(default = "default_extended_chase_ticks")]
    pub extended_chase_ticks: u64,
    #[serde(default = "default_extended_chase_retries")]
    pub extended_chase_retries: u32,
    #[serde(default = "default_extended_chase_timeout_ms")]
    pub extended_chase_timeout_ms: u64,
    #[serde(default = "default_true")]
    pub extended_taker_fallback: bool,

    // ---- Execution: Lighter ----
    /// "market" or "limit".
    #[serde(default = "default_lighter_order_type")]
    pub lighter_order_type: String,
    #[serde(default = "default_lighter_fill_timeout_ms")]
    pub lighter_fill_timeout_ms: u64,

    // ---- Risk ----
    #[serde(default = "default_ws_stale_emergency_ms")]
    pub ws_stale_emergency_ms: u64,
    #[serde(default = "default_max_inventory_skew_usd")]
    pub max_inventory_skew_usd: f64,
    #[serde(default = "default_leg_mismatch_timeout_ms")]
    pub leg_mismatch_timeout_ms: u64,
    /// Pairtrade-symmetric **external KILL_SWITCH file**, dropped by
    /// the operator to block new entries (existing positions exit
    /// normally). bot-strategy#244 D-1. See `docs/execution_layer.md`
    /// §4 for the reconciliation between this and `stuck_file`.
    #[serde(default = "default_kill_switch_file")]
    pub kill_switch_file: String,
    /// Runner-written **STUCK file**, used by the runner to flag
    /// unrecoverable emergency-flatten state (#102 P2 precedent).
    /// Operator must inspect + clear via `RISK_ACK` (D-5). Old YAMLs
    /// that still write to `kill_switch_file: /var/run/xvenue-arb/STUCK`
    /// keep working through the `kill_switch_file` alias below.
    #[serde(default = "default_stuck_file", alias = "kill_switch_file_legacy")]
    pub stuck_file: String,
    /// 30s cadence in EmergencyFlattening — slow-mm 167-min stuck fix.
    /// See docs/execution_layer.md §5.
    #[serde(default = "default_emergency_retry_interval_ms")]
    pub emergency_retry_interval_ms: u64,
    #[serde(default = "default_rest_consec_fail_to_escalate")]
    pub rest_consec_fail_to_escalate: u32,
    #[serde(default = "default_reduce_only_consec_fail_to_kill")]
    pub reduce_only_consec_fail_to_kill: u32,

    // ---- Reference guard (Binance 1m cross-check) ----
    /// Binance pair, e.g. "BTCUSDT" or "ETHUSDT".
    pub binance_reference_symbol: String,
    /// Per-symbol threshold. BTC: 30 bps. ETH: 100 bps (per #166 part 9
    /// finding — 30 bps over-filters legitimate ETH moves).
    pub reference_max_dev_bps: f64,
    #[serde(default = "default_reference_consec_buckets_for_halt")]
    pub reference_consec_buckets_for_halt: u32,
}

impl XvenueConfig {
    pub fn from_yaml_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let file = File::open(path_ref)
            .with_context(|| format!("failed to open xvenue-arb config {}", path_ref.display()))?;
        let cfg: XvenueConfig = serde_yaml::from_reader(file)
            .with_context(|| format!("failed to parse xvenue-arb config {}", path_ref.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Sanity checks that catch config drift before the runner starts.
    pub fn validate(&self) -> Result<()> {
        if self.trade_size_pct <= 0.0 || self.trade_size_pct > 1.0 {
            anyhow::bail!(
                "trade_size_pct must be in (0, 1]; got {}",
                self.trade_size_pct
            );
        }
        if self.min_notional_usd >= self.max_notional_usd {
            anyhow::bail!(
                "min_notional_usd ({}) must be < max_notional_usd ({})",
                self.min_notional_usd,
                self.max_notional_usd
            );
        }
        // Holding past a funding settle violates the design invariant
        // (DESIGN §4 / docs/execution_layer.md §2 case 10).
        if self.funding_cycle_sec > 0 && self.max_hold_sec > self.funding_lockout_pre_sec {
            anyhow::bail!(
                "max_hold_sec ({}) must be ≤ funding_lockout_pre_sec ({}); \
                 a held position could otherwise cross a funding settle",
                self.max_hold_sec,
                self.funding_lockout_pre_sec
            );
        }
        if self.lighter_order_type != "market" && self.lighter_order_type != "limit" {
            anyhow::bail!(
                "lighter_order_type must be \"market\" or \"limit\"; got {}",
                self.lighter_order_type
            );
        }
        Ok(())
    }

    pub fn spread_config(&self) -> SpreadConfig {
        SpreadConfig {
            bucket_ms: self.spread_bucket_ms,
            rolling_window_sec: self.rolling_window_sec,
            max_abs_spread_bps: self.max_abs_spread_bps,
        }
    }

    pub fn signal_config(&self) -> SignalConfig {
        SignalConfig {
            abs_threshold_bps: self.abs_threshold_bps,
            persistence_sec: self.persistence_sec,
            exit_at_mean_cross: self.exit_at_mean_cross,
            max_hold_sec: self.max_hold_sec,
            force_close_dev_bps: self.force_close_dev_bps,
            min_warmup_samples: self.min_warmup_samples,
            entry_check_threshold_at_fire: self.entry_check_threshold_at_fire,
            funding_cycle_sec: self.funding_cycle_sec,
            funding_lockout_pre_sec: self.funding_lockout_pre_sec,
            funding_lockout_post_sec: self.funding_lockout_post_sec,
        }
    }
}

// ---- Default fns (serde requires fns, not literals) ----

fn default_true() -> bool {
    true
}
fn default_spread_bucket_ms() -> u64 {
    1_000
}
fn default_rolling_window_sec() -> u64 {
    1_800
}
fn default_max_abs_spread_bps() -> Option<f64> {
    Some(100.0)
}
fn default_min_warmup_samples() -> usize {
    90
}
fn default_abs_threshold_bps() -> f64 {
    5.0
}
fn default_persistence_sec() -> u64 {
    15
}
fn default_max_hold_sec() -> u64 {
    600
}
fn default_force_close_dev_bps() -> f64 {
    30.0
}
fn default_funding_cycle_sec() -> u64 {
    3600
}
fn default_funding_lockout_pre_sec() -> u64 {
    660
}
fn default_funding_lockout_post_sec() -> u64 {
    120
}
fn default_extended_chase_ticks() -> u64 {
    1
}
fn default_extended_chase_retries() -> u32 {
    3
}
fn default_extended_chase_timeout_ms() -> u64 {
    500
}
fn default_lighter_order_type() -> String {
    "market".to_string()
}
fn default_lighter_fill_timeout_ms() -> u64 {
    1_000
}
fn default_ws_stale_emergency_ms() -> u64 {
    5_000
}
fn default_max_inventory_skew_usd() -> f64 {
    50.0
}
fn default_leg_mismatch_timeout_ms() -> u64 {
    3_000
}
fn default_kill_switch_file() -> String {
    // Pairtrade-symmetric path so one operator workflow drives both
    // fleets (bot-strategy#244 D-1).
    "/opt/debot/KILL_SWITCH".to_string()
}
fn default_stuck_file() -> String {
    "/var/run/xvenue-arb/STUCK".to_string()
}
fn default_emergency_retry_interval_ms() -> u64 {
    30_000
}
fn default_rest_consec_fail_to_escalate() -> u32 {
    3
}
fn default_reduce_only_consec_fail_to_kill() -> u32 {
    5
}
fn default_reference_consec_buckets_for_halt() -> u32 {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_yaml() -> &'static str {
        r#"
agent_name: debot-xvenue-arb-eth-test
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
"#
    }

    fn parse(s: &str) -> XvenueConfig {
        let cfg: XvenueConfig = serde_yaml::from_str(s).unwrap();
        cfg.validate().unwrap();
        cfg
    }

    #[test]
    fn defaults_apply_when_only_required_fields_set() {
        let cfg = parse(minimal_yaml());
        assert_eq!(cfg.agent_name, "debot-xvenue-arb-eth-test");
        assert_eq!(cfg.abs_threshold_bps, 5.0);
        assert_eq!(cfg.persistence_sec, 15);
        assert_eq!(cfg.max_hold_sec, 600);
        assert_eq!(cfg.emergency_retry_interval_ms, 30_000);
        assert_eq!(cfg.kill_switch_file, "/opt/debot/KILL_SWITCH");
        assert_eq!(cfg.stuck_file, "/var/run/xvenue-arb/STUCK");
        assert_eq!(cfg.lighter_order_type, "market");
        assert!(!cfg.dry_run);
    }

    #[test]
    fn explicit_overrides_take_effect() {
        let yaml = r#"
agent_name: debot-xvenue-arb-eth
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
abs_threshold_bps: 7.5
persistence_sec: 30
"#;
        let cfg = parse(yaml);
        let s = cfg.signal_config();
        assert_eq!(s.abs_threshold_bps, 7.5);
        assert_eq!(s.persistence_sec, 30);
    }

    #[test]
    fn rejects_max_hold_exceeding_funding_lockout_pre() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
max_hold_sec: 1000
funding_lockout_pre_sec: 660
"#;
        let cfg: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("max_hold_sec"));
    }

    #[test]
    fn rejects_invalid_trade_size_pct() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 1.5
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
"#;
        let cfg: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("trade_size_pct"));
    }

    #[test]
    fn rejects_unknown_lighter_order_type() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
lighter_order_type: post-only
"#;
        let cfg: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("lighter_order_type"));
    }

    #[test]
    fn spread_config_round_trip() {
        let cfg = parse(minimal_yaml());
        let s = cfg.spread_config();
        assert_eq!(s.bucket_ms, 1_000);
        assert_eq!(s.rolling_window_sec, 1_800);
        assert_eq!(s.max_abs_spread_bps, Some(100.0));
    }

    #[test]
    fn shipped_yaml_examples_parse_and_validate() {
        // Parse the two YAML files we ship under configs/. Catches drift
        // between the schema and the example configs (and vice versa).
        for p in &[
            "configs/xvenue-arb/debot-xvenue-arb-eth.yaml",
            "configs/xvenue-arb/debot-xvenue-arb-btc.yaml",
        ] {
            let cfg = XvenueConfig::from_yaml_path(p).unwrap_or_else(|e| {
                panic!("failed to load shipped config {}: {:?}", p, e)
            });
            // Sanity: signal/spread builders round-trip without panic.
            let _ = cfg.signal_config();
            let _ = cfg.spread_config();
        }
    }

    #[test]
    fn min_must_be_less_than_max_notional() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 1000
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
"#;
        let cfg: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("notional_usd"));
    }
}
