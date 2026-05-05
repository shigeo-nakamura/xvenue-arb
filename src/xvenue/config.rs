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

use super::signal::{SignalConfig, SignalMode};
use super::spread::SpreadConfig;
use crate::risk::kill_switch::StuckTripwireConfig;
use crate::risk::manager::RiskConfig;
use crate::trade::execution::emergency_loop::EmergencyLoopConfig;
use crate::trade::execution::parallel_exit::ParallelExitConfig;
use crate::trade::execution::types::{
    parse_lighter_order_type, ExtendedMakerConfig, LighterFillConfig,
};

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
    /// "mid_to_mid" (default, legacy v2 behavior) or "touch_to_touch"
    /// (bot-strategy#309 maker-on-Lighter redesign — feeds the
    /// directional inside-spread caps `cap_long_bps` / `cap_short_bps`
    /// instead of the rolling-mean dev). Touch-to-touch values are
    /// smaller in scale (max ~12 bps observed in 5.28d ETH dump vs
    /// ~30 bps for mid-to-mid), so operators flipping the mode also
    /// need to drop `abs_threshold_bps` (recommended starting point: 1.0).
    #[serde(default = "default_signal_mode")]
    pub signal_mode: String,
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
    /// bot-strategy#299: per-asset Extended venue minimum order
    /// size. ETH = 0.01, BTC = 0.0001 (consult the live `MarketModel`
    /// the connector caches). 0 (default) disables the guard so
    /// dust_qty is the only floor. The taker fallback gate uses
    /// `residual > extended_min_qty.max(dust_qty)` — a residual below
    /// this is treated as fully filled rather than handed to the
    /// connector only to be rejected with "Order size N below min M".
    #[serde(default = "default_extended_min_qty")]
    pub extended_min_qty: f64,
    /// bot-strategy#298: WS-lag grace re-poll for late taker fills.
    /// Default 0 keeps the previous behavior (Timeout immediately on
    /// `filled=0 cancelled=false`). Production sets ~1000 ms so a
    /// fill that landed at the venue but propagated through the
    /// connector cache slightly past `chase_timeout_ms` is recovered
    /// instead of falling through to EmergencyFlattening. Largely
    /// obviated by #302 (true IOC), but kept for safety until verified.
    #[serde(default = "default_extended_taker_grace_poll_ms")]
    pub extended_taker_grace_poll_ms: u64,
    /// bot-strategy#302: slippage budget for the Extended IOC taker
    /// path (`create_order_taker_ioc`). The connector places at touch
    /// ± 1 tick ± `slippage_bps` so the order crosses on the first
    /// opposing level even when the book moves a few ticks between
    /// read and submit. Default 50 bps mirrors `close_all_positions`'s
    /// `CLOSE_ALL_POSITIONS_SLIPPAGE_BPS` default — wide enough to fill
    /// reliably at $50 notional, narrow enough to bound slippage cost.
    #[serde(default = "default_extended_taker_slippage_bps")]
    pub extended_taker_slippage_bps: u32,

    // ---- Execution: Lighter ----
    /// "market" or "limit".
    #[serde(default = "default_lighter_order_type")]
    pub lighter_order_type: String,
    #[serde(default = "default_lighter_fill_timeout_ms")]
    pub lighter_fill_timeout_ms: u64,

    // ---- Realised PnL fees (#268 S5-1) ----
    /// Per-side fee rate the realised-PnL helper subtracts on each
    /// venue leg (entry + exit). Default 5 bps for Extended is the
    /// conservative taker rate; the actual maker rate is ~2.5 bps
    /// but the executor doesn't surface fill type to the runner, so
    /// we use the worse of the two. Tune downward once the chase
    /// loop reliably hits maker most of the time.
    #[serde(default = "default_extended_fee_bps")]
    pub extended_fee_bps: f64,
    /// Lighter standard-tier fee. 0 bps in production today; left as
    /// a YAML field so a future Lighter-Premium switch (or a
    /// promotional rate change) doesn't require a code patch.
    #[serde(default = "default_lighter_fee_bps")]
    pub lighter_fee_bps: f64,

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
    /// Defensive cap on emergency-flatten attempts. Stops a venue
    /// that accepts `close_all` (Ok) but never zeros the leg from
    /// looping forever. 100 × 30 s = 50 min worst case before the
    /// loop yields `MaxAttemptsExceeded` and the runner re-enters
    /// EmergencyFlattening on the next phase tick.
    #[serde(default = "default_emergency_max_attempts")]
    pub emergency_max_attempts: u32,
    /// Grace window before EmergencyFlattening trusts a `both legs zero`
    /// read. Defends against the false-zero pattern (#287) where a
    /// fill the same process just observed isn't yet reflected by
    /// `get_positions()` (WS lag / sub-account race). Default 30000
    /// ms; 0 disables.
    #[serde(default = "default_emergency_complete_grace_ms")]
    pub emergency_complete_grace_ms: u64,
    #[serde(default = "default_rest_consec_fail_to_escalate")]
    pub rest_consec_fail_to_escalate: u32,
    #[serde(default = "default_reduce_only_consec_fail_to_kill")]
    pub reduce_only_consec_fail_to_kill: u32,
    /// Arm STUCK after this many consecutive `LIVE ENTER ext failed
    /// reason=Timeout` results. 0 disables. Catches the silent-reject
    /// pattern (#244 / #282) where neither REST nor reduce-only counter
    /// fires.
    #[serde(default = "default_enter_timeout_consec_fail_to_kill")]
    pub enter_timeout_consec_fail_to_kill: u32,

    // ---- Risk gates (#244 D-2..D-7) ----
    /// 0 disables. Daily DD blocks new entries when realized PnL
    /// today crosses below `-max_daily_loss_bps` of session start
    /// equity (auto-clears at the next UTC reset).
    #[serde(default = "default_max_daily_loss_bps")]
    pub max_daily_loss_bps: u32,
    #[serde(default = "default_daily_reset_utc_hour")]
    pub daily_reset_utc_hour: u8,
    /// Sticky session-DD halt; cleared only via `risk_ack_path`.
    #[serde(default = "default_max_session_loss_bps")]
    pub max_session_loss_bps: u32,
    #[serde(default = "default_session_dd_lookback_secs")]
    pub session_dd_lookback_secs: u64,
    #[serde(default = "default_session_dd_sample_secs")]
    pub session_dd_sample_secs: u64,
    /// Consecutive-loss cooldowns. Lower priority for xvenue-arb (>=
    /// 90% win profile) but emitted for dashboard parity.
    #[serde(default = "default_cb_tier1_threshold")]
    pub cb_tier1_threshold: u32,
    #[serde(default = "default_cb_tier1_cooldown_secs")]
    pub cb_tier1_cooldown_secs: i64,
    #[serde(default = "default_cb_tier2_threshold")]
    pub cb_tier2_threshold: u32,
    #[serde(default = "default_cb_tier2_cooldown_secs")]
    pub cb_tier2_cooldown_secs: i64,
    /// `risk_state.json` location — pairtrade uses `/opt/debot/`,
    /// xvenue-arb defaults to `/var/lib/xvenue-arb/` so the two
    /// fleets don't fight over the file.
    #[serde(default = "default_risk_state_path")]
    pub risk_state_path: String,
    /// Pairtrade-symmetric `RISK_ACK` path (#244 D-5).
    #[serde(default = "default_risk_ack_path")]
    pub risk_ack_path: String,

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
        if parse_signal_mode(&self.signal_mode).is_none() {
            anyhow::bail!(
                "signal_mode must be \"mid_to_mid\" or \"touch_to_touch\"; got {}",
                self.signal_mode
            );
        }
        Ok(())
    }

    pub fn signal_mode_enum(&self) -> SignalMode {
        // validate() guarantees this Some — fall back to the legacy
        // mode rather than panic if someone bypasses validate().
        parse_signal_mode(&self.signal_mode).unwrap_or(SignalMode::MidToMid)
    }

    pub fn spread_config(&self) -> SpreadConfig {
        SpreadConfig {
            bucket_ms: self.spread_bucket_ms,
            rolling_window_sec: self.rolling_window_sec,
            max_abs_spread_bps: self.max_abs_spread_bps,
        }
    }

    pub fn stuck_tripwire_config(&self) -> StuckTripwireConfig {
        StuckTripwireConfig {
            stuck_file: self.stuck_file.clone().into(),
            rest_consec_fail_to_escalate: self.rest_consec_fail_to_escalate,
            reduce_only_consec_fail_to_kill: self.reduce_only_consec_fail_to_kill,
            enter_timeout_consec_fail_to_kill: self.enter_timeout_consec_fail_to_kill,
        }
    }

    pub fn risk_config(&self) -> RiskConfig {
        RiskConfig {
            max_daily_loss_bps: self.max_daily_loss_bps,
            daily_reset_utc_hour: self.daily_reset_utc_hour,
            max_session_loss_bps: self.max_session_loss_bps,
            session_dd_lookback_secs: self.session_dd_lookback_secs,
            session_dd_sample_secs: self.session_dd_sample_secs,
            cb_tier1_threshold: self.cb_tier1_threshold,
            cb_tier1_cooldown_secs: self.cb_tier1_cooldown_secs,
            cb_tier2_threshold: self.cb_tier2_threshold,
            cb_tier2_cooldown_secs: self.cb_tier2_cooldown_secs,
            risk_state_path: self.risk_state_path.clone().into(),
            risk_ack_path: self.risk_ack_path.clone().into(),
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
            signal_mode: self.signal_mode_enum(),
            entry_check_threshold_at_fire: self.entry_check_threshold_at_fire,
            funding_cycle_sec: self.funding_cycle_sec,
            funding_lockout_pre_sec: self.funding_lockout_pre_sec,
            funding_lockout_post_sec: self.funding_lockout_post_sec,
        }
    }

    /// Knobs for [`crate::trade::execution::extended_maker::ExtendedMakerLoop`].
    /// Sprint 4 wiring will pass this into the chase loop on each
    /// `Decision::Enter` / `Decision::Exit` for the Extended leg.
    pub fn extended_maker_config(&self) -> ExtendedMakerConfig {
        ExtendedMakerConfig {
            common: crate::trade::execution::types::CommonExecutorConfig {
                poll_interval_ms: 50,
            },
            chase_ticks: self.extended_chase_ticks,
            chase_retries: self.extended_chase_retries,
            chase_timeout_ms: self.extended_chase_timeout_ms,
            taker_fallback: self.extended_taker_fallback,
            post_only: self.extended_post_only,
            taker_grace_poll_ms: self.extended_taker_grace_poll_ms,
        }
    }

    /// bot-strategy#299: Extended venue min order size, surfaced as
    /// `Decimal` for `LiveExecution.ext_min_qty`. f64 → Decimal goes
    /// through the same retain path the rest of sizing uses.
    pub fn ext_min_qty(&self) -> rust_decimal::Decimal {
        rust_decimal::Decimal::from_f64_retain(self.extended_min_qty)
            .unwrap_or(rust_decimal::Decimal::ZERO)
    }

    /// Knobs for [`crate::trade::execution::lighter_fill::LighterFillLoop`].
    /// `lighter_order_type` is validated as `"market"` or `"limit"`
    /// at YAML load (see [`Self::validate`]); the helper still returns
    /// `Result` so a bad value caught only at use-site surfaces an
    /// error instead of panicking.
    pub fn lighter_fill_config(&self) -> Result<LighterFillConfig> {
        let order_type = parse_lighter_order_type(&self.lighter_order_type)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(LighterFillConfig {
            common: crate::trade::execution::types::CommonExecutorConfig {
                poll_interval_ms: 25,
            },
            order_type,
            fill_timeout_ms: self.lighter_fill_timeout_ms,
        })
    }

    /// Knobs for [`crate::trade::execution::parallel_exit::ParallelExitLoop`].
    pub fn parallel_exit_config(&self) -> ParallelExitConfig {
        ParallelExitConfig {
            leg_mismatch_timeout_ms: self.leg_mismatch_timeout_ms,
        }
    }

    /// Knobs for [`crate::trade::execution::emergency_loop::EmergencyLoop`].
    /// `max_attempts` is YAML-driven (`emergency_max_attempts`,
    /// default 100) so an operator can shorten the loop in a venue
    /// that's known to ack-without-progress.
    pub fn emergency_loop_config(&self) -> EmergencyLoopConfig {
        EmergencyLoopConfig {
            retry_interval_ms: self.emergency_retry_interval_ms,
            max_attempts: self.emergency_max_attempts,
            complete_grace_ms: self.emergency_complete_grace_ms,
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
fn default_extended_min_qty() -> f64 {
    0.0
}
fn default_extended_taker_grace_poll_ms() -> u64 {
    0
}
fn default_extended_taker_slippage_bps() -> u32 {
    // bot-strategy#302: matches the production setting in YAML
    // (50 bps). 0 here would mean "exact-touch IOC", which would
    // regress fill rate to today's broken state if YAML were ever
    // missing the field.
    50
}
fn default_lighter_order_type() -> String {
    "market".to_string()
}
fn default_signal_mode() -> String {
    "mid_to_mid".to_string()
}

fn parse_signal_mode(s: &str) -> Option<SignalMode> {
    match s {
        "mid_to_mid" => Some(SignalMode::MidToMid),
        "touch_to_touch" => Some(SignalMode::TouchToTouch),
        _ => None,
    }
}
fn default_lighter_fill_timeout_ms() -> u64 {
    1_000
}
fn default_extended_fee_bps() -> f64 {
    5.0
}
fn default_lighter_fee_bps() -> f64 {
    0.0
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
fn default_emergency_max_attempts() -> u32 {
    100
}
fn default_emergency_complete_grace_ms() -> u64 {
    30_000
}
fn default_rest_consec_fail_to_escalate() -> u32 {
    3
}
fn default_reduce_only_consec_fail_to_kill() -> u32 {
    5
}
fn default_enter_timeout_consec_fail_to_kill() -> u32 {
    5
}
fn default_reference_consec_buckets_for_halt() -> u32 {
    3
}
fn default_max_daily_loss_bps() -> u32 {
    300
}
fn default_daily_reset_utc_hour() -> u8 {
    0
}
fn default_max_session_loss_bps() -> u32 {
    500
}
fn default_session_dd_lookback_secs() -> u64 {
    86_400
}
fn default_session_dd_sample_secs() -> u64 {
    60
}
fn default_cb_tier1_threshold() -> u32 {
    5
}
fn default_cb_tier1_cooldown_secs() -> i64 {
    1_800
}
fn default_cb_tier2_threshold() -> u32 {
    8
}
fn default_cb_tier2_cooldown_secs() -> i64 {
    21_600
}
fn default_risk_state_path() -> String {
    "/var/lib/xvenue-arb/risk_state.json".to_string()
}
fn default_risk_ack_path() -> String {
    "/opt/debot/RISK_ACK".to_string()
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
        assert_eq!(cfg.emergency_max_attempts, 100);
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
    fn signal_mode_default_is_mid_to_mid() {
        let cfg = parse(minimal_yaml());
        assert_eq!(cfg.signal_mode, "mid_to_mid");
        assert_eq!(cfg.signal_mode_enum(), SignalMode::MidToMid);
        assert_eq!(cfg.signal_config().signal_mode, SignalMode::MidToMid);
    }

    #[test]
    fn signal_mode_touch_to_touch_round_trips() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
signal_mode: touch_to_touch
abs_threshold_bps: 1.0
"#;
        let cfg = parse(yaml);
        assert_eq!(cfg.signal_mode_enum(), SignalMode::TouchToTouch);
        assert_eq!(cfg.signal_config().signal_mode, SignalMode::TouchToTouch);
        assert_eq!(cfg.signal_config().abs_threshold_bps, 1.0);
    }

    #[test]
    fn rejects_unknown_signal_mode() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
signal_mode: bogus
"#;
        let cfg: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("signal_mode"));
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

    #[test]
    fn extended_maker_config_round_trips_yaml_fields() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
extended_post_only: false
extended_chase_ticks: 2
extended_chase_retries: 5
extended_chase_timeout_ms: 750
extended_taker_fallback: false
"#;
        let cfg = parse(yaml);
        let m = cfg.extended_maker_config();
        assert_eq!(m.chase_ticks, 2);
        assert_eq!(m.chase_retries, 5);
        assert_eq!(m.chase_timeout_ms, 750);
        assert!(!m.taker_fallback);
        assert!(!m.post_only);
        // Validation lives on ExtendedMakerConfig itself; the helper
        // does not pre-validate (callers can choose to surface a
        // YAML-vs-runtime error separately).
        assert!(m.validate().is_ok());
    }

    #[test]
    fn extended_maker_config_defaults_match_yaml_defaults() {
        let cfg = parse(minimal_yaml());
        let m = cfg.extended_maker_config();
        assert_eq!(m.chase_ticks, 1);
        assert_eq!(m.chase_retries, 3);
        assert_eq!(m.chase_timeout_ms, 500);
        assert!(m.taker_fallback);
        assert!(m.post_only);
    }

    #[test]
    fn lighter_fill_config_parses_market_and_limit() {
        let cfg = parse(minimal_yaml());
        let l = cfg.lighter_fill_config().unwrap();
        assert!(matches!(
            l.order_type,
            crate::trade::execution::types::LighterOrderType::Market
        ));
        assert_eq!(l.fill_timeout_ms, 1_000);

        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
lighter_order_type: limit
lighter_fill_timeout_ms: 2500
"#;
        let cfg = parse(yaml);
        let l = cfg.lighter_fill_config().unwrap();
        assert!(matches!(
            l.order_type,
            crate::trade::execution::types::LighterOrderType::AggressiveLimit
        ));
        assert_eq!(l.fill_timeout_ms, 2500);
    }

    #[test]
    fn parallel_exit_config_uses_yaml_field() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
leg_mismatch_timeout_ms: 5000
"#;
        let cfg = parse(yaml);
        let p = cfg.parallel_exit_config();
        assert_eq!(p.leg_mismatch_timeout_ms, 5_000);
        assert!(p.validate().is_ok());
    }

    #[test]
    fn emergency_loop_config_round_trip_with_max_attempts() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
emergency_retry_interval_ms: 5000
emergency_max_attempts: 25
"#;
        let cfg = parse(yaml);
        let e = cfg.emergency_loop_config();
        assert_eq!(e.retry_interval_ms, 5_000);
        assert_eq!(e.max_attempts, 25);
        assert!(e.validate().is_ok());
    }

    #[test]
    fn emergency_loop_config_defaults_to_100_attempts() {
        let cfg = parse(minimal_yaml());
        let e = cfg.emergency_loop_config();
        assert_eq!(e.retry_interval_ms, 30_000);
        assert_eq!(e.max_attempts, 100);
    }
}
