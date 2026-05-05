//! Execution-context bundle for `xvenue::live`'s real-order path
//! (bot-strategy#244 Sprint 4).
//!
//! Holds per-venue [`VenueOps`] adapters plus the typed configs each
//! executor needs. `run_paper_loop` takes
//! `Option<Arc<LiveExecution>>`:
//!
//! - `None` (or `cfg.dry_run = true`): runner stays on the
//!   synthetic-fill paper path used by Phase 2.
//! - `Some(_)` with `dry_run = false`: `Decision::Enter` /
//!   `Decision::Exit` drive real orders via
//!   [`ExtendedMakerLoop`](crate::trade::execution::extended_maker::ExtendedMakerLoop) /
//!   [`LighterFillLoop`](crate::trade::execution::lighter_fill::LighterFillLoop) /
//!   [`ParallelExitLoop`](crate::trade::execution::parallel_exit::ParallelExitLoop) /
//!   [`EmergencyLoop`](crate::trade::execution::emergency_loop::EmergencyLoop).
//!
//! Construction lives in `main.rs` (production: wraps the live
//! `DexConnectorBox` per venue via [`LiveVenueOps`](crate::trade::execution::live_venue_ops::LiveVenueOps)).
//! Tests build it from `Arc<ScriptedVenueOps>` to drive the new flow
//! deterministically without spinning up real connectors.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::config::XvenueConfig;
use crate::trade::execution::emergency_loop::{EmergencyLoopConfig, LegQtys, LegStateReader};
use crate::trade::execution::parallel_exit::ParallelExitConfig;
use crate::trade::execution::types::{ExtendedMakerConfig, LighterFillConfig, LighterMakerConfig};
use crate::trade::execution::venue_ops::VenueOps;

/// Default dust threshold below which a chase-loop residual is
/// treated as filled. For ETH/BTC a 0.00001-coin floor sits well
/// above the qty rounding precision (`notional_to_qty` rounds to 8
/// dp) and well below any meaningful dollar exposure
/// (~$0.78 of BTC at $78 k). Per-asset tuning is a Sprint 5 concern.
fn default_dust_qty() -> Decimal {
    dec!(0.00001)
}

pub struct LiveExecution {
    pub ext_ops: Arc<dyn VenueOps>,
    pub lt_ops: Arc<dyn VenueOps>,
    pub extended_maker_cfg: ExtendedMakerConfig,
    pub lighter_fill_cfg: LighterFillConfig,
    /// bot-strategy#309 step 6: knobs for the post-only chase loop on
    /// the Lighter leg. Active only when `post_only = true` (i.e.
    /// `lighter_post_only` flipped on in the YAML); otherwise the
    /// runner stays on the legacy `LighterFillLoop` path.
    pub lighter_maker_cfg: LighterMakerConfig,
    pub parallel_exit_cfg: ParallelExitConfig,
    pub emergency_loop_cfg: EmergencyLoopConfig,
    /// Symbol for the Extended leg (e.g. "ETH-USD").
    pub ext_symbol: String,
    /// Symbol for the Lighter leg (e.g. "ETH").
    pub lt_symbol: String,
    /// Dust threshold passed into each executor cycle. The chase
    /// loop short-circuits on residuals below this.
    pub dust_qty: Decimal,
    /// bot-strategy#299: Extended-side venue minimum order size.
    /// Surfaces to `ExtendedEntryRequest.venue_min_qty` so the taker
    /// fallback gate skips residuals the venue would silently reject
    /// with `Order size N below min M`. ETH on Extended is 0.01;
    /// BTC is smaller. 0 disables the guard (dust-only behavior).
    pub ext_min_qty: Decimal,
    /// bot-strategy#331 (Lighter mirror of #299): Lighter-side venue
    /// minimum order size. Surfaces to
    /// `LighterMakerRequest.venue_min_qty` so the chase loop skips
    /// sub-min residuals (after a partial fill) instead of feeding
    /// `place_post_only` an amount Lighter rejects with
    /// `code:21706 invalid order base or quote amount`. 0 disables.
    pub lt_min_qty: Decimal,
    /// Reads each venue's open qty for the emergency-flatten round
    /// (#244 Sprint 4 step 3/3). Defaults to a [`NoopLegStateReader`]
    /// that surfaces an Err so the runner skips emergency rounds
    /// without a real reader configured — production builds attach
    /// `LiveLegStateReader` via [`Self::with_leg_reader`].
    pub leg_reader: Arc<dyn LegStateReader>,
}

impl LiveExecution {
    pub fn from_config(
        cfg: &XvenueConfig,
        ext_ops: Arc<dyn VenueOps>,
        lt_ops: Arc<dyn VenueOps>,
    ) -> Result<Self> {
        let exec = Self {
            ext_ops,
            lt_ops,
            extended_maker_cfg: cfg.extended_maker_config(),
            lighter_fill_cfg: cfg.lighter_fill_config()?,
            lighter_maker_cfg: cfg.lighter_maker_config(),
            parallel_exit_cfg: cfg.parallel_exit_config(),
            emergency_loop_cfg: cfg.emergency_loop_config(),
            ext_symbol: cfg.symbol_ext.clone(),
            lt_symbol: cfg.symbol_lt.clone(),
            dust_qty: default_dust_qty(),
            ext_min_qty: cfg.ext_min_qty(),
            lt_min_qty: cfg.lt_min_qty(),
            leg_reader: Arc::new(NoopLegStateReader),
        };
        exec.validate()?;
        Ok(exec)
    }

    /// Builder: replace the default [`NoopLegStateReader`] with a real
    /// implementation. Production wires `LiveLegStateReader`; tests
    /// build a scripted reader to drive the emergency-flatten round.
    pub fn with_leg_reader(mut self, reader: Arc<dyn LegStateReader>) -> Self {
        self.leg_reader = reader;
        self
    }
}

/// Default [`LegStateReader`] used when production hasn't wired
/// [`crate::trade::execution::live_venue_ops::LiveLegStateReader`]
/// yet. Returns Err so the emergency-flatten round logs a warning
/// and skips, rather than silently flipping to `EmergencyComplete`
/// on a fake "both legs zero" reading.
pub struct NoopLegStateReader;

#[async_trait]
impl LegStateReader for NoopLegStateReader {
    async fn read_leg_qtys(&self) -> Result<LegQtys> {
        Err(anyhow::anyhow!(
            "NoopLegStateReader: no production leg reader wired \
             — use LiveExecution::with_leg_reader to install one"
        ))
    }
}

impl LiveExecution {
    pub fn validate(&self) -> Result<()> {
        self.extended_maker_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!(e))?;
        self.lighter_fill_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!(e))?;
        self.lighter_maker_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!(e))?;
        self.parallel_exit_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!(e))?;
        self.emergency_loop_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!(e))?;
        if self.dust_qty <= Decimal::ZERO {
            anyhow::bail!("dust_qty must be > 0; got {}", self.dust_qty);
        }
        if self.ext_min_qty < Decimal::ZERO {
            anyhow::bail!("ext_min_qty must be >= 0; got {}", self.ext_min_qty);
        }
        if self.lt_min_qty < Decimal::ZERO {
            anyhow::bail!("lt_min_qty must be >= 0; got {}", self.lt_min_qty);
        }
        if self.ext_symbol.trim().is_empty() {
            anyhow::bail!("ext_symbol must be non-empty");
        }
        if self.lt_symbol.trim().is_empty() {
            anyhow::bail!("lt_symbol must be non-empty");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::venue_ops::ScriptedVenueOps;

    fn cfg() -> XvenueConfig {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
"#;
        let c: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        c
    }

    #[test]
    fn from_config_round_trips_yaml_into_typed_configs_and_symbols() {
        let c = cfg();
        let ext: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let lt: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let exec = LiveExecution::from_config(&c, ext, lt).unwrap();
        assert_eq!(exec.ext_symbol, "ETH-USD");
        assert_eq!(exec.lt_symbol, "ETH");
        assert_eq!(exec.dust_qty, dec!(0.00001));
        assert_eq!(exec.extended_maker_cfg.chase_retries, 3);
        assert_eq!(exec.lighter_fill_cfg.fill_timeout_ms, 1_000);
        assert_eq!(exec.parallel_exit_cfg.leg_mismatch_timeout_ms, 3_000);
        assert_eq!(exec.emergency_loop_cfg.retry_interval_ms, 30_000);
        assert_eq!(exec.emergency_loop_cfg.max_attempts, 100);
        // bot-strategy#309 step 6: lighter_maker_cfg is built from
        // the same YAML; defaults keep post_only OFF so the legacy
        // taker path stays in force until the operator flips the YAML.
        assert!(!exec.lighter_maker_cfg.post_only);
        assert_eq!(exec.lighter_maker_cfg.chase_retries, 3);
    }

    #[test]
    fn lighter_post_only_yaml_propagates_into_live_execution() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
lighter_post_only: true
lighter_chase_retries: 4
lighter_chase_timeout_ms: 200
extended_chase_timeout_ms: 500
leg_mismatch_timeout_ms: 5000
"#;
        let c: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        let ext: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let lt: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let exec = LiveExecution::from_config(&c, ext, lt).unwrap();
        assert!(exec.lighter_maker_cfg.post_only);
        assert_eq!(exec.lighter_maker_cfg.chase_retries, 4);
        assert_eq!(exec.lighter_maker_cfg.chase_timeout_ms, 200);
        // Legacy LighterFillLoop config is still populated — exit
        // path keeps using it even when entry flips to maker.
        assert_eq!(exec.lighter_fill_cfg.fill_timeout_ms, 1_000);
    }

    /// bot-strategy#331: `lighter_min_qty` YAML knob propagates into
    /// `LiveExecution.lt_min_qty` so the chase loop's sub-lot residual
    /// guard activates. Disabled by default (=0) for back-compat.
    #[test]
    fn lighter_min_qty_yaml_propagates_into_live_execution() {
        let yaml = r#"
agent_name: x
symbol_ext: ETH-USD
symbol_lt: ETH
trade_size_pct: 0.05
min_notional_usd: 20
max_notional_usd: 1000
binance_reference_symbol: ETHUSDT
reference_max_dev_bps: 100
lighter_min_qty: 0.001
"#;
        let c: XvenueConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        let ext: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let lt: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let exec = LiveExecution::from_config(&c, ext, lt).unwrap();
        // f64 0.001 is not exactly representable, so compare against
        // the same `from_f64_retain` path the helper uses rather than
        // a `dec!(0.001)` literal. Rounding to 6 dp strips the f64
        // noise and matches what live sizing code does.
        assert_eq!(exec.lt_min_qty.round_dp(6), dec!(0.001));
    }

    #[test]
    fn lt_min_qty_defaults_to_zero_when_yaml_omits_it() {
        let exec = LiveExecution::from_config(
            &cfg(),
            Arc::new(ScriptedVenueOps::new()),
            Arc::new(ScriptedVenueOps::new()),
        )
        .unwrap();
        assert_eq!(exec.lt_min_qty, Decimal::ZERO);
    }

    #[test]
    fn validate_rejects_negative_lt_min_qty() {
        let exec = LiveExecution {
            ext_ops: Arc::new(ScriptedVenueOps::new()),
            lt_ops: Arc::new(ScriptedVenueOps::new()),
            extended_maker_cfg: cfg().extended_maker_config(),
            lighter_fill_cfg: cfg().lighter_fill_config().unwrap(),
            lighter_maker_cfg: cfg().lighter_maker_config(),
            parallel_exit_cfg: cfg().parallel_exit_config(),
            emergency_loop_cfg: cfg().emergency_loop_config(),
            ext_symbol: "ETH-USD".into(),
            lt_symbol: "ETH".into(),
            dust_qty: dec!(0.00001),
            ext_min_qty: Decimal::ZERO,
            lt_min_qty: dec!(-0.001),
            leg_reader: Arc::new(NoopLegStateReader),
        };
        let err = exec.validate().unwrap_err();
        assert!(err.to_string().contains("lt_min_qty"));
    }

    #[test]
    fn validate_rejects_zero_dust_qty() {
        let c = cfg();
        let ext: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let lt: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let mut exec = LiveExecution::from_config(&c, ext, lt).unwrap();
        exec.dust_qty = Decimal::ZERO;
        let err = exec.validate().unwrap_err();
        assert!(err.to_string().contains("dust_qty"));
    }

    #[test]
    fn validate_rejects_empty_symbols() {
        let c = cfg();
        let ext: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let lt: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let mut exec = LiveExecution::from_config(&c, ext.clone(), lt.clone()).unwrap();
        exec.ext_symbol = "  ".to_string();
        assert!(exec
            .validate()
            .unwrap_err()
            .to_string()
            .contains("ext_symbol"));

        let mut exec = LiveExecution::from_config(&c, ext, lt).unwrap();
        exec.lt_symbol = "".to_string();
        assert!(exec
            .validate()
            .unwrap_err()
            .to_string()
            .contains("lt_symbol"));
    }

    #[test]
    fn from_config_propagates_lighter_order_type_parse_error() {
        // Build a config that passes XvenueConfig::validate (only
        // accepts "market" / "limit"), then poke the field to a
        // value parse_lighter_order_type can still digest cleanly,
        // and assert the helper threads the value through. Negative
        // case: bypass YAML validation by mutating after construction.
        let mut c = cfg();
        c.lighter_order_type = "post_only".into();
        let ext: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let lt: Arc<dyn VenueOps> = Arc::new(ScriptedVenueOps::new());
        let err = LiveExecution::from_config(&c, ext, lt)
            .err()
            .expect("from_config must reject post_only as lighter_order_type");
        assert!(err.to_string().contains("lighter_order_type"));
    }
}
