//! xvenue-arb entrypoint — Phase 2 paper trading runner.
//!
//! Loads `XvenueConfig` from `$XVENUE_CONFIG_PATH`, brings up Lighter +
//! Extended connectors, and drives the paper-trading loop in
//! `xvenue::live`. Order placement is deferred to Phase 3 (the loop logs
//! `Decision::Enter` / `Exit` but does not call `create_order`).
//!
//! See `docs/execution_layer.md` for the timeout and IPC layout that
//! Phase 3 will fill in.

use std::sync::Arc;

use chrono::{DateTime, FixedOffset, Utc};
use debot::error_counter::{self, ErrorCountingLogger};
use debot::ports::live_dual::LiveVenueHub;
use debot::trade::execution::emergency_loop::{LegStateReader, LiveLegStateReader};
use debot::trade::execution::live_venue_ops::LiveVenueOps;
use debot::trade::execution::venue_ops::VenueOps;
use debot::xvenue::config::XvenueConfig;
use debot::xvenue::live::{run_paper_loop, LiveLoopConfig};
use debot::xvenue::live_exec::LiveExecution;
use dex_connector::DexConnector;
use env_logger::Builder;
use log::LevelFilter;
use std::env;
use std::io::Write;
use std::str::FromStr;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;

#[cfg(feature = "lighter-sdk")]
use debot::config::get_lighter_config_from_env;
#[cfg(feature = "lighter-sdk")]
use dex_connector::{create_lighter_connector, LighterConnector, LighterConnectorConfig};

#[cfg(feature = "extended-sdk")]
use debot::config::get_extended_config_from_env;
#[cfg(feature = "extended-sdk")]
use dex_connector::create_extended_connector;

fn init_logger() {
    let offset_seconds = env::var("TIMEZONE_OFFSET")
        .unwrap_or_else(|_| "32400".to_string()) // JST default (Tokyo deploy)
        .parse::<i32>()
        .expect("Invalid TIMEZONE_OFFSET");
    let offset = FixedOffset::east_opt(offset_seconds).expect("Invalid offset");
    let filter = LevelFilter::from_str(
        &env::var("RUST_LOG")
            .unwrap_or_else(|_| "info,tokio_tungstenite=info,tungstenite=info".to_string()),
    )
    .unwrap_or(LevelFilter::Info);
    let inner = Builder::from_default_env()
        .format(move |buf, record| {
            let utc_now: DateTime<Utc> = Utc::now();
            let local_now = utc_now.with_timezone(&offset);
            writeln!(
                buf,
                "{} [{}] - {}",
                local_now.format("%Y-%m-%dT%H:%M:%S%z"),
                record.level(),
                record.args()
            )
        })
        .filter(None, filter)
        .build();
    let max_level = inner.filter();
    let (logger, handle) = ErrorCountingLogger::wrap(Box::new(inner));
    error_counter::install_global(handle);
    if log::set_boxed_logger(Box::new(logger)).is_ok() {
        log::set_max_level(max_level);
    }
}

/// Build a Lighter `Arc<dyn DexConnector>` using the env-driven config in
/// `src/config.rs`. Mirrors pairtrade's `dex_connector_box.rs`
/// init flow (account-index discovery → final connector). Returns the
/// dry-run-friendly `LighterConnector::new` when `dry_run` is true so we
/// don't talk to the real exchange during paper trading.
#[cfg(feature = "lighter-sdk")]
async fn build_lighter(symbol: &str, dry_run: bool) -> anyhow::Result<Arc<dyn DexConnector>> {
    let lighter_config = get_lighter_config_from_env(None)
        .await
        .map_err(|e| anyhow::anyhow!("get_lighter_config_from_env: {:?}", e))?;

    // Account index discovery if not set (matches pairtrade behavior).
    let mut account_index = lighter_config.account_index;
    if account_index == 0 {
        let wallet = lighter_config
            .wallet_address
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("LIGHTER_WALLET_ADDRESS required for discovery"))?;
        log::info!(
            "[BOOT] discovering Lighter account index for api_key_index={}...",
            lighter_config.api_key_index
        );
        let tmp = LighterConnector::new(LighterConnectorConfig {
            api_key_public: lighter_config.api_key.clone(),
            api_key_index: lighter_config.api_key_index,
            api_private_key_hex: lighter_config.private_key.clone(),
            evm_wallet_private_key: lighter_config.evm_wallet_private_key.clone(),
            account_index: 0,
            base_url: lighter_config.base_url.clone(),
            websocket_url: lighter_config.websocket_url.clone(),
            tracked_symbols: vec![],
            ob_stale_secs: None,
        })?;
        account_index = tmp.discover_account_index(wallet).await?;
        log::info!("[BOOT] discovered Lighter account_index={}", account_index);
    }

    let cfg = LighterConnectorConfig {
        api_key_public: lighter_config.api_key,
        api_key_index: lighter_config.api_key_index,
        api_private_key_hex: lighter_config.private_key,
        evm_wallet_private_key: lighter_config.evm_wallet_private_key,
        account_index,
        base_url: lighter_config.base_url,
        websocket_url: lighter_config.websocket_url,
        tracked_symbols: vec![symbol.to_string()],
        // Sole freshness gate for `read_mid` → `get_order_book` (no REST
        // fallback exists for that path; cf. pairtrade's `get_ticker`
        // which has a 30s gate + REST fallback). Set explicitly so the
        // dependency on `DEFAULT_ORDERBOOK_STALE_SECS` is locally
        // visible. See bot-strategy#303.
        ob_stale_secs: Some(15),
    };

    let connector: Arc<dyn DexConnector> = if dry_run {
        Arc::new(LighterConnector::new(cfg)?)
    } else {
        Arc::from(create_lighter_connector(cfg)?)
    };
    Ok(connector)
}

#[cfg(feature = "extended-sdk")]
async fn build_extended(symbol: &str) -> anyhow::Result<Arc<dyn DexConnector>> {
    let ec = get_extended_config_from_env()
        .await
        .map_err(|e| anyhow::anyhow!("get_extended_config_from_env: {:?}", e))?;
    let connector = create_extended_connector(
        ec.api_key,
        ec.public_key,
        ec.private_key,
        ec.vault,
        ec.base_url,
        ec.websocket_url,
        vec![symbol.to_string()],
    )
    .await?;
    Ok(Arc::from(connector))
}

async fn run() -> anyhow::Result<()> {
    let cfg_path = env::var("XVENUE_CONFIG_PATH")
        .map_err(|_| anyhow::anyhow!("XVENUE_CONFIG_PATH must be set"))?;
    let cfg = XvenueConfig::from_yaml_path(&cfg_path)?;
    log::info!(
        "[BOOT] loaded config from {} agent={} ext={} lt={} dry_run={}",
        cfg_path,
        cfg.agent_name,
        cfg.symbol_ext,
        cfg.symbol_lt,
        cfg.dry_run
    );

    #[cfg(not(all(feature = "lighter-sdk", feature = "extended-sdk")))]
    {
        anyhow::bail!(
            "xvenue-arb requires both lighter-sdk and extended-sdk features. \
             Build with --features lighter-sdk,extended-sdk (default)."
        );
    }

    #[cfg(all(feature = "lighter-sdk", feature = "extended-sdk"))]
    {
        let lighter = build_lighter(&cfg.symbol_lt, cfg.dry_run).await?;
        let extended = build_extended(&cfg.symbol_ext).await?;

        log::info!("[BOOT] starting Lighter connector");
        lighter
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("lighter.start: {:?}", e))?;
        log::info!("[BOOT] starting Extended connector");
        extended
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("extended.start: {:?}", e))?;

        let hub = Arc::new(LiveVenueHub {
            extended: extended.clone(),
            lighter: lighter.clone(),
            symbol_extended: cfg.symbol_ext.clone(),
            symbol_lighter: cfg.symbol_lt.clone(),
        });

        // Build LiveExecution by wrapping each connector in
        // LiveVenueOps (#244 Sprint 4 plumbing). When `cfg.dry_run`
        // is true the runner ignores it and stays on the synthetic-
        // fill paper path; only `dry_run = false` actually exercises
        // the executors.
        //
        // bot-strategy#302: Extended uses the IOC taker path
        // (`create_order_taker_ioc`) with the slippage budget from
        // YAML so the venue actually receives a true IOC instead of a
        // 1 h GTT LIMIT. Lighter keeps the legacy `create_order` path
        // — its market-order semantics already work.
        let ext_ops: Arc<dyn VenueOps> = Arc::new(LiveVenueOps::with_taker_ioc_slippage(
            extended.clone(),
            cfg.extended_taker_slippage_bps,
        ));
        let lt_ops: Arc<dyn VenueOps> = Arc::new(LiveVenueOps::new(lighter.clone()));
        let leg_reader: Arc<dyn LegStateReader> = Arc::new(LiveLegStateReader::new(
            extended.clone(),
            lighter.clone(),
            cfg.symbol_ext.clone(),
            cfg.symbol_lt.clone(),
        ));
        let live_exec = Arc::new(
            LiveExecution::from_config(&cfg, ext_ops, lt_ops)?.with_leg_reader(leg_reader),
        );

        // Wire SIGTERM + SIGINT to a oneshot so the loop exits cleanly.
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
            let mut intr = signal(SignalKind::interrupt()).expect("install SIGINT handler");
            tokio::select! {
                _ = term.recv() => log::info!("[SIGNAL] SIGTERM"),
                _ = intr.recv() => log::info!("[SIGNAL] SIGINT"),
            }
            let _ = shutdown_tx.send(());
        });

        let loop_cfg = LiveLoopConfig::from_xvenue(&cfg);
        let summary = run_paper_loop(cfg, loop_cfg, hub, Some(live_exec), shutdown_rx).await?;
        log::info!(
            "[EXIT] ticks={} samples={} hold={} enter_l={} enter_s={} exit={} \
             ks_blocked={} stuck_blocked={} dd_blocked={} sd_blocked={} cb_blocked={}",
            summary.ticks,
            summary.samples_committed,
            summary.decisions_hold,
            summary.decisions_enter_long,
            summary.decisions_enter_short,
            summary.decisions_exit,
            summary.entries_blocked_by_kill_switch,
            summary.entries_blocked_by_stuck_file,
            summary.entries_blocked_by_daily_dd,
            summary.entries_blocked_by_session_dd,
            summary.entries_blocked_by_circuit_breaker,
        );

        // Best-effort connector shutdown; any errors are logged but don't
        // propagate so a hung WS doesn't block exit.
        if let Err(e) = lighter.stop().await {
            log::warn!("[EXIT] lighter.stop: {:?}", e);
        }
        if let Err(e) = extended.stop().await {
            log::warn!("[EXIT] extended.stop: {:?}", e);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logger();
    let dex_connector_git = option_env!("DEX_CONNECTOR_GIT_HASH").unwrap_or("unknown");
    log::info!("dex-connector git: {}", dex_connector_git);
    log::info!("xvenue-arb starting (bot-strategy#166 Phase 2 paper trading)");
    if let Err(e) = run().await {
        log::error!("[FATAL] {:?}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ));
    }
    Ok(())
}
