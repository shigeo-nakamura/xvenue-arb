//! Backtest plumbing extracted from the monolithic pairtrade module. Right
//! now this only owns the connector construction fork (replay vs live), but
//! is the natural home for any future backtest-only helpers (data loading,
//! replay-clock policy, …).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use dex_connector::DexConnector;

use super::config::PairTradeConfig;
use crate::ports::replay_dex::ReplayConnector;
use crate::trade::execution::dex_connector_box::DexConnectorBox;

/// Build one connector per resolved strategy variant on `cfg`.
///
/// In backtest mode every strategy shares the same `ReplayConnector` so the
/// replay clock stays consistent across instances. In live mode each strategy
/// gets its own `DexConnectorBox::create()` call with its `id` passed as
/// `instance_id`, which lets the Lighter env loader pick up sub-account
/// credentials suffixed with that id (see `lighter_env()` in
/// `crate::config`). The single-strategy path gets exactly one connector
/// with `instance_id = None`, preserving today's behavior byte-for-byte.
///
/// Returns: `(primary, instance_connectors, replay)` where `primary` is the
/// connector for the first strategy (also stored on `PairTradeEngine` for
/// back-compat with legacy call sites), `instance_connectors` is the
/// per-strategy list, and `replay` is `Some` only in backtest mode.
///
/// commit 3 of shigeo-nakamura/bot-strategy#25.
pub(super) async fn create_connector(
    cfg: &PairTradeConfig,
) -> Result<(
    Arc<dyn DexConnector + Send + Sync>,
    Vec<Arc<dyn DexConnector + Send + Sync>>,
    Option<Arc<ReplayConnector>>,
)> {
    if cfg.backtest_mode {
        let replay = Arc::new(ReplayConnector::new(
            cfg.backtest_file.as_ref().unwrap().as_str(),
        )?);
        let primary: Arc<dyn DexConnector + Send + Sync> = replay.clone();
        let n = cfg.strategies.len().max(1);
        let instance_connectors = std::iter::repeat(primary.clone()).take(n).collect();
        return Ok((primary, instance_connectors, Some(replay)));
    }

    let tokens: Vec<String> = cfg
        .universe
        .iter()
        .flat_map(|p| [p.base.clone(), p.quote.clone()])
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // The single-instance path passes instance_id=None so the Lighter env
    // loader uses the unsuffixed env var names — preserving existing
    // single-bot deployments byte-for-byte. Multi-strategy YAML drives the
    // suffixed path.
    let mut instance_connectors: Vec<Arc<dyn DexConnector + Send + Sync>> = Vec::new();
    if cfg.strategies.len() <= 1 {
        let conn = DexConnectorBox::create(
            &cfg.dex_name,
            &cfg.rest_endpoint,
            &cfg.web_socket_endpoint,
            cfg.dry_run,
            cfg.agent_name.clone(),
            &tokens,
            None,
        )
        .await
        .context("failed to initialize connector")?;
        conn.start()
            .await
            .context("failed to start connector")?;
        instance_connectors.push(Arc::new(conn));
    } else {
        // Lighter enforces a short-window rate limit across both /account
        // and /apikeys. Each instance's connector.start() triggers a Go-SDK
        // CheckClient() call that hits /apikeys?account_index=X&api_key_index=Y
        // from inside the FFI, so the shared Rust rate-tracker can't see it.
        // All three production sub-accounts share one wallet, and /apikeys
        // is throttled per-wallet, so the three validation hits must stay
        // spread outside Lighter's per-wallet short-window (~60s, observed
        // via a 109s Retry-After on 429 in 2026-04-22 startup). 30s here
        // puts the three calls at t=0, t=30, t=60 — outside the window.
        // The previous 10s setting still tripped 429 because each
        // LighterConnector::start() then ran an independent random
        // LIGHTER_STARTUP_JITTER_SECS jitter (default 30s) that could
        // collapse the parent's spacing back together; see bot-strategy#127
        // / #143 / #163. The wrapper now exports
        // LIGHTER_STARTUP_JITTER_SECS=0 so this constant is the only thing
        // controlling the cadence.
        const INIT_ACCOUNT_SPACING: Duration = Duration::from_secs(30);
        let mut last_iter_start: Option<Instant> = None;
        for strategy in &cfg.strategies {
            if let Some(t) = last_iter_start {
                let elapsed = t.elapsed();
                if elapsed < INIT_ACCOUNT_SPACING {
                    tokio::time::sleep(INIT_ACCOUNT_SPACING - elapsed).await;
                }
            }
            last_iter_start = Some(Instant::now());
            let conn = DexConnectorBox::create(
                &cfg.dex_name,
                &cfg.rest_endpoint,
                &cfg.web_socket_endpoint,
                cfg.dry_run,
                strategy.agent_name.clone().or_else(|| cfg.agent_name.clone()),
                &tokens,
                Some(strategy.id.as_str()),
            )
            .await
            .with_context(|| format!("failed to initialize connector for {}", strategy.id))?;
            conn.start()
                .await
                .with_context(|| format!("failed to start connector for {}", strategy.id))?;
            instance_connectors.push(Arc::new(conn));
        }
    }

    let primary = instance_connectors[0].clone();
    Ok((primary, instance_connectors, None))
}
