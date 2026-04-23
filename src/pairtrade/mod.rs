use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use dex_connector::{DexConnector, DexError, PositionSnapshot};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

use crate::email_client::EmailClient;
use crate::ports::replay_dex::ReplayConnector;

mod backtest;
mod bar;
mod config;
mod data_dump;
mod defaults;
mod entry;
mod exit;
mod history_io;
mod kalman;
mod market;
mod order_pricing;
mod pair_eval;
mod pnl_log;
mod regime;
mod sizing;
mod state;
mod stats;
mod status;
mod util;
use bar::BarBuilder;
use entry::{entry_z_for_pair, should_enter};
use exit::{compute_pnl, exit_reason};
use market::{liquidity_score, net_funding_for_direction, SymbolSnapshot};
use pair_eval::PairEvaluation;
use pnl_log::{PnlLogRecord, PnlLogger};
use stats::{regression_beta, spread_slope_sigma, tail_samples, PriceSample};
pub use config::{PairTradeConfig, WarmStartMode};
use config::PairParams;
use config::PairSpec;
use defaults::*;
use state::{
    BtDeferredExit, PairState, PartialOrderPlacementError, PendingLeg, PendingOrders,
    PendingStatus, Position, PositionDirection,
};
use status::{
    PairTradeStats, ShutdownPosition, ShutdownStatus, StatusReporter,
};
use util::{round_price_by_tick, tail_std};

/// Max age of the per-instance equity cache before `refresh_equity_if_needed`
/// fetches a fresh value from the exchange. Now a low-frequency dashboard tick:
/// exit/loss-cut uses locally-computed PnL from WS prices, so `equity_cache`
/// only scales the slowly-drifting R-budget and feeds the status reporter.
/// Entry sizing forces a fresh fetch inline (see `fetch_equity_rest` call in
/// the entry branch of `step()`), independent of this cache. See
/// bot-strategy#156.
const EQUITY_REFRESH_CACHE_SECS: u64 = 1800;


struct StrategyInstance {
    #[allow(dead_code)]
    id: String,
    /// Per-strategy connector. For single-instance deployments this is the
    /// same `Arc` as `PairTradeEngine.connector`. For multi-strategy
    /// deployments each instance owns its own connector pointing at its
    /// sub-account credentials.
    #[allow(dead_code)]
    connector: Arc<dyn DexConnector + Send + Sync>,
    /// Per-instance live equity from the instance's connector.
    equity_cache: f64,
    last_equity_fetch: Option<Instant>,
    /// Per-strategy equity floor from the YAML `equity_usd_fallback`.
    /// Used as the min-floor for sizing and exit decisions instead of
    /// the engine-wide `cfg.equity_usd` so each variant sizes against
    /// its own sub-account capacity (A=1000, B=500, C=500).
    equity_usd_fallback: f64,
    states: HashMap<String, PairState>,
    pnl_logger: Option<PnlLogger>,
    status_reporter: Option<StatusReporter>,
    consecutive_losses: u32,
    circuit_breaker_until: Option<Instant>,
    /// Replay-aware companion to `circuit_breaker_until`. Compared against
    /// the per-step `now_ts` so backtest replays can honour the same
    /// cool-down logic as live.
    circuit_breaker_until_ts: Option<i64>,
    total_trades: u64,
    total_wins: u64,
    total_pnl: f64,
    peak_pnl: f64,
    max_dd: f64,
    /// Per-instance pair parameter overrides. Built at `new_inner` time by
    /// overlaying the strategy's `exit_z` / `stop_loss_z` / `max_loss_r_mult`
    /// on top of the engine-wide defaults. Look up via
    /// `PairTradeEngine::pair_params_for(inst_idx, key)`.
    pair_params: HashMap<String, PairParams>,
    default_pair_params: PairParams,
}

pub struct PairTradeEngine {
    cfg: PairTradeConfig,
    connector: Arc<dyn DexConnector + Send + Sync>,
    instances: Vec<StrategyInstance>,
    history: HashMap<String, VecDeque<PriceSample>>,
    bar_builders: HashMap<String, BarBuilder>,
    last_metrics_log: Option<Instant>,
    last_ob_warn: HashMap<String, Instant>,
    last_ticker_warn: HashMap<String, Instant>,
    last_position_warn: HashMap<String, Instant>,
    min_order_warned: HashSet<String>,
    min_tick_warned: HashSet<String>,
    positions_ready: bool,
    open_positions: HashMap<String, PositionSnapshot>,
    /// Last time ANY /account REST call was fired across all instances.
    /// Used to pace calls ≥ MIN_ACCOUNT_SPACING apart without a blocking
    /// per-instance sleep; the previous `inst_idx * 5s` stagger blocked
    /// step() for the full span even when no recent call had been made.
    /// See bot-strategy#122.
    last_account_rest_call: Option<Instant>,
    history_path: PathBuf,
    data_dump_writer: Option<data_dump::RotatingDumpWriter>,
    replay_connector: Option<Arc<ReplayConnector>>,
    /// Graceful shutdown flag. When true:
    ///   - new entries are blocked
    ///   - existing exit logic (exit_z / stop_loss_z / force_close_secs) runs normally
    ///   - live loop exits as soon as open_positions is empty, or after shutdown_grace_secs
    shutdown_pending: bool,
}

struct PlannedAction {
    pair: PairSpec,
    key: String,
    action: TradeAction,
    net_funding_per_hour: f64,
    abs_z: f64,
    liquidity_score: f64,
    p1: SymbolSnapshot,
    p2: SymbolSnapshot,
}

enum TradeAction {
    Open {
        direction: PositionDirection,
        z: f64,
        beta: f64,
    },
    Close {
        direction: PositionDirection,
        z: f64,
        beta: f64,
        force: bool,
    },
    None,
}

impl PairTradeEngine {
    /// Create a new engine with a pre-loaded ReplayConnector (for batch mode).
    pub async fn new_with_replay(
        cfg: PairTradeConfig,
        replay: Arc<ReplayConnector>,
    ) -> Result<Self> {
        replay.reset();
        let primary: Arc<dyn DexConnector + Send + Sync> = replay.clone();
        let n = cfg.strategies.len().max(1);
        let instance_connectors = std::iter::repeat(primary.clone()).take(n).collect();
        Self::new_inner(cfg, primary, instance_connectors, Some(replay)).await
    }

    pub async fn new(cfg: PairTradeConfig) -> Result<Self> {
        let (connector, instance_connectors, replay_connector) =
            backtest::create_connector(&cfg).await?;
        Self::new_inner(cfg, connector, instance_connectors, replay_connector).await
    }

    async fn new_inner(
        cfg: PairTradeConfig,
        connector: Arc<dyn DexConnector + Send + Sync>,
        instance_connectors: Vec<Arc<dyn DexConnector + Send + Sync>>,
        replay_connector: Option<Arc<ReplayConnector>>,
    ) -> Result<Self> {
        let mut history = HashMap::new();
        let mut bar_builders = HashMap::new();
        for pair in &cfg.universe {
            history.insert(pair.base.clone(), VecDeque::new());
            history.insert(pair.quote.clone(), VecDeque::new());
            bar_builders.insert(pair.base.clone(), BarBuilder::new(cfg.trading_period_secs));
            bar_builders.insert(pair.quote.clone(), BarBuilder::new(cfg.trading_period_secs));
        }

        let history_path = PathBuf::from(cfg.history_file.as_str());

        let min_order_warned = HashSet::new();
        let min_tick_warned = HashSet::new();
        let data_dump_writer = if cfg.enable_data_dump {
            let file_path = cfg.data_dump_file.as_ref().unwrap(); // is_none checked in from_env
            Some(data_dump::RotatingDumpWriter::new(file_path)?)
        } else {
            None
        };

        let backtest_mode = cfg.backtest_mode;
        let _ = backtest_mode;
        let multi_instance = cfg.strategies.len() > 1;

        // Build one StrategyInstance per entry in cfg.strategies. For legacy
        // single-strategy YAML this is exactly one instance whose parameters
        // match today's behavior (golden-test stable). For multi-strategy
        // YAML this produces N instances that share the engine's history /
        // bar_builders but each hold their own pair_params overlay,
        // connector, PnL log, and status reporter.
        let mut built_instances: Vec<StrategyInstance> = Vec::new();
        let strategies = cfg.strategies.clone();
        for (i, strategy) in strategies.iter().enumerate() {
            // Overlay per-strategy exit_z / stop_loss_z / max_loss_r_mult on
            // top of the engine's default_pair_params and per-pair overrides
            // so each variant evaluates z-exits at its own thresholds.
            let mut inst_default = cfg.default_pair_params.clone();
            inst_default.exit_z = strategy.exit_z;
            inst_default.stop_loss_z = strategy.stop_loss_z;
            inst_default.max_loss_r_mult = strategy.max_loss_r_mult;
            if let Some(fc) = strategy.force_close_time_secs {
                inst_default.force_close_secs = fc;
            }
            if let Some(ref w) = strategy.mtf_windows {
                inst_default.mtf_windows = w.clone();
            }
            if let Some(z) = strategy.mtf_z_min {
                inst_default.mtf_z_min = z;
            }

            let mut inst_pair_params: HashMap<String, PairParams> = HashMap::new();
            for (k, v) in cfg.pair_params.iter() {
                let mut pp = v.clone();
                pp.exit_z = strategy.exit_z;
                pp.stop_loss_z = strategy.stop_loss_z;
                pp.max_loss_r_mult = strategy.max_loss_r_mult;
                if let Some(fc) = strategy.force_close_time_secs {
                    pp.force_close_secs = fc;
                }
                if let Some(ref w) = strategy.mtf_windows {
                    pp.mtf_windows = w.clone();
                }
                if let Some(z) = strategy.mtf_z_min {
                    pp.mtf_z_min = z;
                }
                inst_pair_params.insert(k.clone(), pp);
            }

            let mut states = HashMap::new();
            for pair in &cfg.universe {
                let pair_key = format!("{}/{}", pair.base, pair.quote);
                let pp = inst_pair_params
                    .get(&pair_key)
                    .unwrap_or(&inst_default);
                let mut ps = PairState::new(cfg.metrics_window, pp.entry_z_base);
                if cfg.use_kalman_beta {
                    ps.kalman = Some(kalman::KalmanBeta::new(
                        1.0,
                        cfg.kalman_initial_p,
                        cfg.kalman_q,
                        cfg.kalman_r,
                    ));
                }
                states.insert(pair_key, ps);
            }

            let instance_connector = instance_connectors
                .get(i)
                .cloned()
                .unwrap_or_else(|| connector.clone());
            let pnl_logger =
                PnlLogger::from_env_for_instance(&cfg, &strategy.id, multi_instance);
            let mut status_reporter =
                StatusReporter::from_env_for_instance(&cfg, &strategy.id, multi_instance);
            if let Some(reporter) = status_reporter.as_mut() {
                reporter.trade_stats = Some(PairTradeStats {
                    trades: 0,
                    wins: 0,
                    win_rate: 0.0,
                    max_dd: 0.0,
                    pnl: 0.0,
                });
            }

            // Stagger the per-instance equity-refresh cycle so N instances
            // don't all hit `/account` inside the same 5-min expiry boundary.
            // Each instance is phase-shifted by i * (CACHE_SECS / N) so over
            // a 5-min window the N calls are spread evenly (~100s apart for
            // N=3) instead of back-to-back. Avoids Lighter's short-window
            // 429 on the burst head (bot-strategy#142).
            let instance_count = strategies.len();
            let last_equity_fetch = if i == 0 || instance_count <= 1 {
                None
            } else {
                let offset_secs =
                    (EQUITY_REFRESH_CACHE_SECS * i as u64) / instance_count as u64;
                let phase = EQUITY_REFRESH_CACHE_SECS.saturating_sub(offset_secs);
                Some(Instant::now() - Duration::from_secs(phase))
            };

            built_instances.push(StrategyInstance {
                id: strategy.id.clone(),
                connector: instance_connector,
                equity_cache: strategy.equity_usd,
                last_equity_fetch,
                equity_usd_fallback: strategy.equity_usd,
                states,
                pnl_logger,
                status_reporter,
                consecutive_losses: 0,
                circuit_breaker_until: None,
                circuit_breaker_until_ts: None,
                total_trades: 0,
                total_wins: 0,
                total_pnl: 0.0,
                peak_pnl: 0.0,
                max_dd: 0.0,
                pair_params: inst_pair_params,
                default_pair_params: inst_default,
            });
        }

        Ok(Self {
            cfg,
            connector,
            replay_connector,
            instances: built_instances,
            history,
            bar_builders,
            last_metrics_log: None,
            last_ob_warn: HashMap::new(),
            last_ticker_warn: HashMap::new(),
            last_position_warn: HashMap::new(),
            min_order_warned,
            min_tick_warned,
            positions_ready: backtest_mode,
            open_positions: HashMap::new(),
            last_account_rest_call: None,
            history_path,
            data_dump_writer,
            shutdown_pending: false,
        })
    }

    /// Whether every strategy instance is currently flat. For single-instance
    /// deployments this is exactly today's `self.open_positions.is_empty()`
    /// check (golden-test stable). For multi-instance deployments this also
    /// requires every per-pair `state.position` across every instance to be
    /// `None`, so SIGTERM waits for all A/B/C variants to flatten before
    /// exiting. commit 5 of shigeo-nakamura/bot-strategy#25.
    fn all_instances_flat(&self) -> bool {
        if self.instances.len() <= 1 {
            return self.open_positions.is_empty();
        }
        self.open_positions.is_empty()
            && self.instances.iter().all(|inst| {
                inst.states.values().all(|s| s.position.is_none())
            })
    }

    /// Total open-position count surfaced in shutdown log lines. Mirrors
    /// `all_instances_flat`'s split: single-instance returns today's count,
    /// multi-instance sums per-pair `state.position` presence across all
    /// instances so the log reflects everything graceful shutdown is
    /// waiting on.
    fn total_open_positions(&self) -> usize {
        if self.instances.len() <= 1 {
            return self.open_positions.len();
        }
        let from_states: usize = self
            .instances
            .iter()
            .map(|inst| inst.states.values().filter(|s| s.position.is_some()).count())
            .sum();
        from_states.max(self.open_positions.len())
    }

    /// Return the per-instance `PairParams` for a pair key, falling back to
    /// the instance's `default_pair_params` when the pair has no override.
    /// Use this inside any per-instance phase in place of
    /// `self.cfg.params_for(key)` so each variant sees its own
    /// `exit_z` / `stop_loss_z` / `max_loss_r_mult`.
    fn pair_params_for(&self, inst_idx: usize, key: &str) -> &PairParams {
        let inst = &self.instances[inst_idx];
        inst.pair_params.get(key).unwrap_or(&inst.default_pair_params)
    }

    fn write_pnl_record(&mut self, inst_idx: usize, record: PnlLogRecord) {
        // Update trade stats
        self.instances[inst_idx].total_trades += 1;
        self.instances[inst_idx].total_pnl += record.pnl;
        if record.pnl > 0.0 {
            self.instances[inst_idx].total_wins += 1;
        }
        if self.instances[inst_idx].total_pnl > self.instances[inst_idx].peak_pnl {
            self.instances[inst_idx].peak_pnl = self.instances[inst_idx].total_pnl;
        }
        let dd = self.instances[inst_idx].peak_pnl - self.instances[inst_idx].total_pnl;
        if dd > self.instances[inst_idx].max_dd {
            self.instances[inst_idx].max_dd = dd;
        }

        // Update status reporter
        let inst = &mut self.instances[inst_idx];
        if let Some(reporter) = &mut inst.status_reporter {
            let wr = if inst.total_trades > 0 {
                inst.total_wins as f64 / inst.total_trades as f64 * 100.0
            } else { 0.0 };
            reporter.trade_stats = Some(PairTradeStats {
                trades: inst.total_trades,
                wins: inst.total_wins,
                win_rate: wr,
                max_dd: inst.max_dd,
                pnl: inst.total_pnl,
            });
        }

        if let Some(logger) = &mut self.instances[inst_idx].pnl_logger {
            if let Err(err) = logger.log(record) {
                log::warn!("[PNL] failed to write pnl log: {:?}", err);
            }
        }
    }

    fn is_inconsistent_state(err: &anyhow::Error) -> bool {
        let msg = err.to_string();
        msg.contains("Inconsistent state")
    }

    async fn log_inconsistent_state_debug(&mut self, err: &anyhow::Error) {
        if !Self::is_inconsistent_state(err) {
            return;
        }

        // Log internal state for active pairs
        for inst in self.instances.iter() {
            for (key, state) in inst.states.iter() {
                let is_active = state.position.is_some()
                    || state.pending_entry.is_some()
                    || state.pending_exit.is_some()
                    || state.bt_deferred_exit.is_some()
                    || state.position_guard;
                if !is_active {
                    continue;
                }
                log::error!(
                    "[DEBUG][STATE] key={} position={:?} pending_entry={:?} pending_exit={:?} guard={} positions_ready={}",
                    key,
                    state.position,
                    state.pending_entry.as_ref().map(|p| p.legs.len()),
                    state.pending_exit.as_ref().map(|p| p.legs.len()),
                    state.position_guard,
                    self.positions_ready
                );
            }
        }

        // Log what the exchange reports for positions
        match self.connector.get_positions().await {
            Ok(pos) => {
                let filtered: Vec<_> = pos
                    .into_iter()
                    .filter(|p| p.sign != 0 && p.size > Decimal::ZERO)
                    .collect();
                log::error!("[DEBUG][EXCHANGE_POSITIONS] {:?}", filtered);
            }
            Err(get_err) => {
                log::error!(
                    "[DEBUG][EXCHANGE_POSITIONS] failed to fetch positions: {:?}",
                    get_err
                );
            }
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        log::info!("[CONFIG] DEX_NAME is: {}", self.cfg.dex_name);
        log::info!(
            "[CONFIG] FEE_BPS={} SLIPPAGE_BPS={} post_only_supported={} post_only_enabled={}",
            self.cfg.fee_bps,
            self.cfg.slippage_bps,
            self.post_only_supported(),
            self.should_post_only()
        );
        self.load_history_from_disk();
        // BT warm-start: load a live history snapshot so the replay starts
        // with an identical spread_history / beta to the live bot, instead
        // of building from scratch over the first 4 hours of data.
        if self.cfg.backtest_mode {
            if let Some(ref path) = self.cfg.bt_warm_start_snapshot {
                let max_len = self.max_history_len();
                let mut loaded_spreads: HashMap<String, VecDeque<f64>> = HashMap::new();
                history_io::load_history_snapshot_for_bt(
                    &mut self.history,
                    &mut loaded_spreads,
                    std::path::Path::new(path),
                    max_len,
                );
                for inst in &mut self.instances {
                    for (pair_key, spreads) in &loaded_spreads {
                        if let Some(state) = inst.states.get_mut(pair_key) {
                            state.last_spread = spreads.back().copied();
                            state.spread_history = spreads.clone();
                        }
                    }
                }
            }
        }
        self.warm_start_states_from_history();

        if self.replay_connector.is_some() {
            // --- Backtest Mode ---
            log::info!("[BACKTEST] Running in backtest mode.");
            loop {
                if let Err(e) = self.step().await {
                    // In backtest, we might want to stop on error. For now, just log it.
                    log::error!("[BACKTEST] Step failed: {:?}", e);
                }
                // Advance the replay connector to the next data point
                let has_more = {
                    let replay = self
                        .replay_connector
                        .as_ref()
                        .expect("replay connector should exist in backtest mode");
                    replay.tick()
                };
                if !has_more {
                    log::info!("[BACKTEST] End of data file reached. Backtest finished.");
                    break;
                }
            }
        } else {
            // --- Live Mode ---
            log::info!("[LIVE] Running in live mode.");
            // Allow the per-instance WS streams to warm up and populate their
            // position snapshots BEFORE force_close_on_startup probes them;
            // otherwise the first get_positions attempts fail with "positions
            // not ready from websocket" and the retry loop used to call
            // close_all_positions blindly (which in turn REST-hit /account and
            // occasionally 429'd during the multi-instance startup burst).
            // bot-strategy#143.
            sleep(Duration::from_secs(5)).await;
            if self.cfg.force_close_on_startup {
                self.force_close_on_startup().await?;
            }
            // Wall-clock aligned ticker: fires at floor(now/interval)*interval + interval boundaries
            // so every bot process observing the same stream ticks at identical wall-clock seconds.
            // This is required on top of the BarBuilder bucket alignment (pairtrade#4): without
            // aligning the tick phase itself, two bots would sample the last tick of a 60s bucket
            // at different wall-clock seconds and therefore see slightly different close prices,
            // which cascades into divergent beta/mean/std/z.
            let interval_secs = self.cfg.interval_secs.max(1);
            fn next_wall_clock_boundary(interval_secs: u64) -> tokio::time::Instant {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_unix_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let interval_ms = interval_secs.saturating_mul(1000);
                let next_boundary_ms = ((now_unix_ms / interval_ms) + 1) * interval_ms;
                let wait_ms = next_boundary_ms.saturating_sub(now_unix_ms);
                tokio::time::Instant::now() + Duration::from_millis(wait_ms)
            }
            let mut next_tick = next_wall_clock_boundary(interval_secs);
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            )
            .expect("failed to register SIGTERM handler");
            let mut sigint = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::interrupt(),
            )
            .expect("failed to register SIGINT handler");

            let grace = Duration::from_secs(self.cfg.shutdown_grace_secs);
            let mut shutdown_deadline: Option<Instant> = None;
            let mut force_shutdown = false;
            loop {
                // Graceful shutdown: exit as soon as positions are flat, or after grace expires.
                if self.shutdown_pending {
                    if self.all_instances_flat() {
                        log::info!("[PAIR] Shutdown: all positions flat, exiting");
                        break;
                    }
                    if let Some(dl) = shutdown_deadline {
                        if Instant::now() >= dl {
                            log::warn!(
                                "[PAIR] Shutdown grace ({}s) expired with {} open positions, force-closing",
                                self.cfg.shutdown_grace_secs,
                                self.total_open_positions()
                            );
                            force_shutdown = true;
                            break;
                        }
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep_until(next_tick) => {
                        next_tick = next_wall_clock_boundary(interval_secs);
                        // Monitor step() execution time. If it exceeds interval_secs,
                        // the next wall-clock boundary will be skipped, causing tick
                        // phase to drift across A/B/C bots and breaking bar alignment
                        // (pairtrade#4). WARN so we can spot it in production logs.
                        let step_start = Instant::now();
                        if let Err(e) = self.step().await {
                            self.log_inconsistent_state_debug(&e).await;
                            log::error!("pairtrade step failed: {:?}", e);
                        }
                        let step_elapsed = step_start.elapsed();
                        let interval = Duration::from_secs(interval_secs);
                        // Warn only on critical overrun (>=1.5x interval), where a
                        // wall-clock tick is genuinely skipped and A/B/C bars drift.
                        // Mild overruns (just past the boundary) are logged at info
                        // so they stay visible without inflating warn_count.
                        let critical = interval + interval / 2;
                        if step_elapsed >= critical {
                            log::warn!(
                                "[STEP_OVERRUN] step() took {:.2}s >= {:.2}s (1.5x interval_secs={}); \
                                 wall-clock tick skipped",
                                step_elapsed.as_secs_f64(),
                                critical.as_secs_f64(),
                                interval_secs
                            );
                        } else if step_elapsed >= interval {
                            log::info!(
                                "[STEP_OVERRUN] step() took {:.2}s >= interval_secs={} (mild)",
                                step_elapsed.as_secs_f64(),
                                interval_secs
                            );
                        }
                    }
                    _ = sigterm.recv() => {
                        if !self.shutdown_pending {
                            let flat = self.all_instances_flat();
                            if flat || self.cfg.shutdown_grace_secs == 0 {
                                log::info!(
                                    "[PAIR] SIGTERM received, shutting down (flat={}, grace={}s)",
                                    flat,
                                    self.cfg.shutdown_grace_secs
                                );
                                force_shutdown = !flat;
                                break;
                            }
                            log::info!(
                                "[PAIR] SIGTERM received, entering graceful shutdown: \
                                 waiting for natural exit of {} open positions (grace={}s). \
                                 Send SIGTERM/SIGINT again to force.",
                                self.total_open_positions(),
                                self.cfg.shutdown_grace_secs
                            );
                            // Surface per-position force_close ETA so operators can
                            // see when each leg will be auto-flushed if it doesn't
                            // exit naturally. Iterates every instance so multi-strategy
                            // shutdown reports the union of A/B/C positions, not
                            // just the first instance. See pairtrade#6, extended in
                            // commit 5 of shigeo-nakamura/bot-strategy#25.
                            let now_ts = chrono::Utc::now().timestamp();
                            let grace_deadline_ts =
                                now_ts + self.cfg.shutdown_grace_secs as i64;
                            let per_instance_positions: Vec<Vec<ShutdownPosition>> = self
                                .instances
                                .iter()
                                .map(|inst| {
                                    let mut out = Vec::new();
                                    for (key, state) in &inst.states {
                                        if let Some(pos) = &state.position {
                                            let pp = inst
                                                .pair_params
                                                .get(key)
                                                .unwrap_or(&inst.default_pair_params);
                                            let elapsed =
                                                now_ts.saturating_sub(pos.entered_ts).max(0);
                                            let remaining = (pp.force_close_secs as i64)
                                                .saturating_sub(elapsed);
                                            let eta_ts =
                                                pos.entered_ts + pp.force_close_secs as i64;
                                            log::info!(
                                                "[PAIR] shutdown: [{}] {} held={}s \
                                                 force_close_secs={} force_close_in={}s",
                                                inst.id,
                                                key,
                                                elapsed,
                                                pp.force_close_secs,
                                                remaining.max(0),
                                            );
                                            out.push(ShutdownPosition {
                                                key: key.clone(),
                                                entered_ts: pos.entered_ts,
                                                force_close_eta_ts: eta_ts,
                                            });
                                        }
                                    }
                                    out
                                })
                                .collect();
                            for (inst, shutdown_positions) in
                                self.instances.iter_mut().zip(per_instance_positions.into_iter())
                            {
                                let earliest_eta = shutdown_positions
                                    .iter()
                                    .map(|p| p.force_close_eta_ts)
                                    .min();
                                if let Some(reporter) = &mut inst.status_reporter {
                                    reporter.set_shutdown_status(Some(ShutdownStatus {
                                        pending: true,
                                        grace_deadline_ts,
                                        force_close_eta_ts: earliest_eta,
                                        positions: shutdown_positions,
                                    }));
                                }
                            }
                            self.shutdown_pending = true;
                            shutdown_deadline = Some(Instant::now() + grace);
                        } else {
                            log::info!("[PAIR] Second SIGTERM received, force-closing immediately");
                            force_shutdown = true;
                            break;
                        }
                    }
                    _ = sigint.recv() => {
                        if self.shutdown_pending {
                            log::info!("[PAIR] SIGINT received during graceful shutdown, force-closing");
                            force_shutdown = true;
                            break;
                        } else {
                            log::info!("[PAIR] SIGINT received, shutting down...");
                            force_shutdown = !self.all_instances_flat();
                            break;
                        }
                    }
                }
            }

            if force_shutdown {
                log::info!("[PAIR] Force-closing all open positions on shutdown");
                if let Err(e) = self.connector.close_all_positions(None).await {
                    log::error!("[PAIR] close_all_positions on shutdown failed: {:?}", e);
                }
            }
        }
        for inst in self.instances.iter_mut() {
            if let Some(reporter) = &mut inst.status_reporter {
                if let Err(err) = reporter.write_snapshot(&self.open_positions, self.positions_ready) {
                    log::warn!("[STATUS] failed to write status: {:?}", err);
                }
            }
        }
        Ok(())
    }

    async fn reissue_partial_legs(
        &mut self,
        pending: &PendingOrders,
        filled_qtys: &HashMap<String, Decimal>,
        price_map: &HashMap<String, SymbolSnapshot>,
        reduce_only: bool,
        use_market: bool,
        retry_count: u32,
    ) -> Result<Option<PendingOrders>> {
        let mut new_legs = Vec::new();
        let stage = if reduce_only { "exit" } else { "entry" };
        for leg in &pending.legs {
            let filled = filled_qtys
                .get(&leg.order_id)
                .cloned()
                .unwrap_or(Decimal::ZERO)
                .max(leg.filled)
                .min(leg.target);
            let remaining = (leg.target - filled).max(Decimal::ZERO);
            if remaining <= Decimal::ZERO {
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            if !use_market {
                let has_price = price_map
                    .get(&leg.symbol)
                    .map(|s| s.price > Decimal::ZERO)
                    .unwrap_or(false);
                if !has_price {
                    log::warn!(
                        "[ORDER] Cannot reissue {} leg {}: missing price",
                        stage,
                        leg.symbol
                    );
                    let mut kept = leg.clone();
                    kept.filled = filled;
                    new_legs.push(kept);
                    continue;
                }
            }
            let quantized_size = if reduce_only {
                self.quantize_order_size_close(&leg.symbol, remaining, price_map)
            } else {
                self.quantize_order_size(&leg.symbol, remaining, price_map)
            };
            if quantized_size <= Decimal::ZERO {
                log::warn!(
                    "[ORDER] {} leg {} remaining {} below tick; skipping",
                    stage,
                    leg.symbol,
                    remaining
                );
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            let limit = if use_market {
                None
            } else {
                self.limit_price_for(&leg.symbol, leg.side, price_map)
            };
            if !use_market && limit.is_none() {
                log::warn!(
                    "[ORDER] Cannot reissue {} leg {}: missing reference price",
                    stage,
                    leg.symbol
                );
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            let spread = self.order_spread_param(limit, false);
            match self
                .connector
                .create_order(
                    &leg.symbol,
                    quantized_size,
                    leg.side,
                    limit,
                    spread,
                    reduce_only,
                    None,
                )
                .await
            {
                Ok(resp) => {
                    log::info!(
                        "[ORDER] Reissued {} leg {} size={}",
                        stage,
                        leg.symbol,
                        quantized_size
                    );
                    if filled > Decimal::ZERO {
                        new_legs.push(PendingLeg {
                            symbol: leg.symbol.clone(),
                            order_id: leg.order_id.clone(),
                            exchange_order_id: leg.exchange_order_id.clone(),
                            target: filled,
                            filled,
                            side: leg.side,
                            limit_price: None,
                        });
                    }
                    new_legs.push(PendingLeg {
                        symbol: leg.symbol.clone(),
                        order_id: resp.order_id,
                        exchange_order_id: resp.exchange_order_id,
                        target: quantized_size,
                        filled: Decimal::ZERO,
                        side: leg.side,
                        limit_price: None,
                    });
                }
                Err(e) => {
                    let symbol = leg.symbol.clone();
                    if reduce_only && Self::is_reduce_only_position_missing_error(&e) {
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} leg {} already closed; skipping reissue",
                                stage,
                                symbol
                            );
                            let mut kept = leg.clone();
                            kept.filled = leg.target;
                            new_legs.push(kept);
                        } else {
                            log::error!(
                                "[ORDER] Failed to reissue {} leg {}: {:?}",
                                stage,
                                symbol,
                                e
                            );
                            let mut kept = leg.clone();
                            kept.filled = filled;
                            new_legs.push(kept);
                        }
                    } else {
                        log::error!(
                            "[ORDER] Failed to reissue {} leg {}: {:?}",
                            stage,
                            symbol,
                            e
                        );
                        let mut kept = leg.clone();
                        kept.filled = filled;
                        new_legs.push(kept);
                    }
                }
            }
        }
        if new_legs.is_empty() {
            return Ok(None);
        }
        Ok(Some(PendingOrders {
            legs: new_legs,
            direction: pending.direction,
            placed_at: Instant::now(),
            hedge_retry_count: retry_count,
            post_only_hybrid: false,
        }))
    }

    async fn reissue_entry_as_taker(
        &mut self,
        key: &str,
        pending: &PendingOrders,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<Option<PendingOrders>> {
        let mut new_legs = Vec::new();
        for leg in &pending.legs {
            let size = self.quantize_order_size(&leg.symbol, leg.target, price_map);
            if size <= Decimal::ZERO {
                log::warn!(
                    "[ORDER] {} taker reissue leg {} below min size; skipping",
                    key,
                    leg.symbol
                );
                continue;
            }
            match self
                .connector
                .create_order(
                    &leg.symbol,
                    size,
                    leg.side,
                    None, // no limit price = market/taker
                    None,
                    false,
                    None,
                )
                .await
            {
                Ok(resp) => {
                    log::info!(
                        "[ORDER] {} taker reissue leg {} size={}",
                        key,
                        leg.symbol,
                        size
                    );
                    new_legs.push(PendingLeg {
                        symbol: leg.symbol.clone(),
                        order_id: resp.order_id,
                        exchange_order_id: resp.exchange_order_id,
                        target: size,
                        filled: Decimal::ZERO,
                        side: leg.side,
                        limit_price: None,
                    });
                }
                Err(e) => {
                    log::error!(
                        "[ORDER] {} taker reissue failed for {}: {:?}",
                        key,
                        leg.symbol,
                        e
                    );
                }
            }
        }
        if new_legs.is_empty() {
            return Ok(None);
        }
        Ok(Some(PendingOrders {
            legs: new_legs,
            direction: pending.direction,
            placed_at: Instant::now(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
        }))
    }

    fn format_positions_summary(positions: &[PositionSnapshot]) -> String {
        let mut parts = Vec::with_capacity(positions.len());
        for position in positions {
            let side = match position.sign.cmp(&0) {
                Ordering::Greater => "LONG",
                Ordering::Less => "SHORT",
                Ordering::Equal => "FLAT",
            };
            let entry = position
                .entry_price
                .map(|price| price.to_string())
                .unwrap_or_else(|| "n/a".to_string());
            parts.push(format!(
                "{} {} size={} entry={}",
                position.symbol, side, position.size, entry
            ));
        }
        parts.join(", ")
    }

    async fn force_close_on_startup(&self) -> Result<()> {
        if self.cfg.dry_run || self.cfg.observe_only {
            log::info!(
                "[Startup] DRY RUN/OBSERVE ONLY: Would cancel all orders and close all positions"
            );
            return Ok(());
        }
        let attempts = self.cfg.startup_force_close_attempts.max(1);
        let wait_secs = self.cfg.startup_force_close_wait_secs;
        log::info!(
            "[Startup] Force closing any existing orders/positions (attempts={}, wait_secs={})",
            attempts,
            wait_secs
        );
        if let Err(err) = self.connector.cancel_all_orders(None).await {
            log::warn!("[Startup] cancel_all_orders failed: {:?}", err);
        }
        for attempt in 1..=attempts {
            let positions_result = self.connector.get_positions().await;
            match positions_result {
                Ok(positions) if positions.is_empty() => {
                    if attempt == 1 {
                        log::info!("[Startup] No open positions detected");
                    } else {
                        log::info!("[Startup] All positions closed");
                    }
                    return Ok(());
                }
                Ok(positions) => {
                    log::info!(
                        "[Startup] close attempt {}/{}: {}",
                        attempt,
                        attempts,
                        Self::format_positions_summary(&positions)
                    );
                    if let Err(err) = self.connector.close_all_positions(None).await {
                        log::error!("[Startup] close_all_positions failed: {:?}", err);
                    }
                }
                Err(err) => {
                    // Don't call close_all_positions when we can't confirm positions
                    // state from the WS cache — its internal /account REST call would
                    // burst the startup rate-limit window alongside the other
                    // instances' connects and 429, producing a spurious RateLimit
                    // email. Just wait for the WS to populate on the next attempt.
                    // See bot-strategy#143.
                    log::warn!(
                        "[Startup] get_positions failed on attempt {}/{}: {:?}",
                        attempt,
                        attempts,
                        err
                    );
                }
            }

            if attempt < attempts && wait_secs > 0 {
                sleep(Duration::from_secs(wait_secs)).await;
            }
        }

        if wait_secs > 0 {
            sleep(Duration::from_secs(wait_secs)).await;
        }
        match self.connector.get_positions().await {
            Ok(positions) if positions.is_empty() => {
                log::info!("[Startup] All positions closed");
            }
            Ok(positions) => {
                let summary = Self::format_positions_summary(&positions);
                log::error!(
                    "[Startup] positions still open after {} attempts: {}",
                    attempts,
                    summary
                );
                let subject = match self.cfg.agent_name.as_deref() {
                    Some(name) => format!("[{}] Startup close failed", name),
                    None => format!(
                        "[Startup] Failed to close positions (dex={})",
                        self.cfg.dex_name
                    ),
                };
                let body = format!(
                    "Startup force close failed after {} attempts.\nOpen positions: {}",
                    attempts, summary
                );
                EmailClient::new().send(&subject, &body);
            }
            Err(err) => {
                log::error!(
                    "[Startup] get_positions failed after {} attempts: {:?}",
                    attempts,
                    err
                );
            }
        }
        Ok(())
    }

    async fn force_close_all_positions(&self, key: &str, reason: &str) {
        if self.cfg.dry_run || self.cfg.observe_only {
            log::warn!(
                "[EXIT] {} force close skipped (mode) reason={}",
                key,
                reason
            );
            return;
        }
        log::error!(
            "[EXIT] {} exceeded exit retries; invoking close_all_positions reason={}",
            key,
            reason
        );
        if let Err(err) = self.connector.close_all_positions(None).await {
            log::error!("[EXIT] close_all_positions failed: {:?}", err);
        }
    }

    pub async fn step(&mut self) -> Result<()> {
        // One process, one shared WS subscription is the goal of #25. Until
        // the connector layer truly merges WS, instances[0]'s connector is
        // the canonical source for the shared price fetch. The per-instance
        // phase below will re-point self.connector at each instance's
        // connector for order placement / balance / sync calls.
        if !self.instances.is_empty() {
            self.connector = self.instances[0].connector.clone();
        }
        let Some((price_map, updated)) = self.step_shared().await? else {
            return Ok(());
        };
        for inst_idx in 0..self.instances.len() {
            self.connector = self.instances[inst_idx].connector.clone();
            self.step_for_instance(inst_idx, &price_map, &updated).await?;
        }
        Ok(())
    }

    /// Shared phase: run once per outer step. Fetches the canonical price
    /// tick, advances the ReplayConnector clock exactly once, updates the
    /// engine-wide history + bar builders, and returns the `(price_map,
    /// updated)` pair for the per-instance phase. Returns `Ok(None)` when a
    /// host-shared cooldown is active and every instance should skip.
    async fn step_shared(
        &mut self,
    ) -> Result<Option<(HashMap<String, SymbolSnapshot>, HashSet<String>)>> {
        // Lighter WAF cooldown is host-shared. Any REST call we make here
        // would be rejected anyway and would refresh the rolling window,
        // turning a 60s cooldown into a permanent block. Skip silently.
        // dex-connector logs once on engagement; the email goes out via
        // report_rate_limit. See bot-strategy#35.
        #[cfg(feature = "lighter-sdk")]
        if dex_connector::lighter_waf_cooldown::cooldown_remaining().is_some() {
            return Ok(None);
        }

        let price_map = self.fetch_latest_prices().await?;

        if let Some(writer) = &mut self.data_dump_writer {
            let dump_entry = DataDumpEntry {
                timestamp: Utc::now().timestamp_millis(),
                prices: &price_map,
            };
            if let Ok(json_string) = serde_json::to_string(&dump_entry) {
                if writer.write_line(&json_string).is_err() {
                    log::error!("[DataDump] Failed to write to dump file");
                }
            }
        }

        // Bar build + history update is engine-wide: all instances read
        // from the same `self.history`, so we must do it exactly once per
        // outer tick before any per-instance decision logic runs.
        let max_history_len = self.max_history_len();
        let now_ts = self.current_now_ts();
        self.load_history_from_disk();

        // BT restart simulation (bot-strategy#27 comment 2026-04-16): when
        // the replay crosses a timestamp listed in
        // `BT_RESTART_TIMESTAMPS_FILE`, re-run `warm_start_states_from_history`
        // to mirror what the live bot does at each systemd restart —
        // re-compute `state.beta` from OLS and re-seed `spread_history`
        // with 240 single-beta spreads. That seeded low-variance history
        // is the mechanism behind the 2026-04-15 06:02 UTC "std collapse"
        // (bot-strategy#62 — now known to be a restart artifact, not a
        // regime break). We fire on crossing, not exact match, because
        // the live dump has a gap (WS down) around the restart second, so
        // the exact `restart_ts` often has no replay record. Each matched
        // ts is removed from the set, so each restart fires at most once.
        let restart_passed = self
            .cfg
            .bt_restart_timestamps
            .as_mut()
            .map(|set| {
                let passed: Vec<i64> = set.iter().filter(|&&t| t <= now_ts).copied().collect();
                for t in &passed {
                    set.remove(t);
                }
                !passed.is_empty()
            })
            .unwrap_or(false);
        if restart_passed {
            log::warn!(
                "[BT_RESTART] simulating live service restart (now_ts={})",
                now_ts
            );
            self.warm_start_states_from_history();
        }
        let mut updated = HashSet::new();
        for (symbol, snapshot) in price_map.iter() {
            if let Some(builder) = self.bar_builders.get_mut(symbol) {
                let tick_ts = snapshot.exchange_ts.unwrap_or(now_ts);
                if let Some((close_price, close_ts)) = builder.push(tick_ts, snapshot.price) {
                    let entry = self
                        .history
                        .entry(symbol.clone())
                        .or_insert_with(VecDeque::new);
                    let log_price = close_price
                        .to_f64()
                        .ok_or_else(|| anyhow!("invalid price for {}", symbol))?
                        .ln();
                    if entry.back().map(|s| s.ts) != Some(close_ts) {
                        if entry.len() >= max_history_len {
                            entry.pop_front();
                        }
                        entry.push_back(PriceSample {
                            log_price,
                            ts: close_ts,
                        });
                    }
                    updated.insert(symbol.clone());
                }
            } else {
                log::debug!("no bar builder for {}", symbol);
            }
        }
        self.persist_history_to_disk();

        Ok(Some((price_map, updated)))
    }

    async fn step_for_instance(
        &mut self,
        inst_idx: usize,
        price_map: &HashMap<String, SymbolSnapshot>,
        updated: &HashSet<String>,
    ) -> Result<()> {
        // Skip new entries if maintenance is upcoming within 1 hour
        let maintenance_block_entries = self.connector.is_upcoming_maintenance(1).await;
        if maintenance_block_entries {
            log::info!("Upcoming maintenance detected; blocking new entries this cycle");
        }
        if let Some(reporter) = &mut self.instances[inst_idx].status_reporter {
            reporter.set_maintenance(if maintenance_block_entries {
                Some("blocking_entries".to_string())
            } else {
                None
            });
        }

        self.refresh_equity_if_needed(inst_idx).await?;
        self.sync_positions_from_exchange(inst_idx, price_map).await?;

        let vol_median = self.compute_vol_median(inst_idx);

        // Regime filter: compute once per step cycle (not per pair)
        let regime_state = if self.cfg.regime_vol_max > 0.0 || self.cfg.regime_trend_max > 0.0 {
            self.history
                .get(&self.cfg.regime_reference_symbol)
                .and_then(|h| {
                    regime::compute_regime(h, self.cfg.regime_vol_window, self.cfg.regime_trend_window)
                })
        } else {
            None
        };
        let regime_ok = regime::regime_allows_entry(
            regime_state,
            self.cfg.regime_vol_max,
            self.cfg.regime_trend_max,
        );
        if let Some(rs) = regime_state {
            if !regime_ok {
                log::info!(
                    "[REGIME] entry blocked: vol={:.6} (max={:.6}) trend={:.4} (max={:.4}) ref={}",
                    rs.realized_vol,
                    self.cfg.regime_vol_max,
                    rs.trend_strength,
                    self.cfg.regime_trend_max,
                    self.cfg.regime_reference_symbol,
                );
            }
        }

        let positions_clear = self.open_positions.is_empty();
        let has_pending_orders = self
            .instances[inst_idx]
            .states
            .values()
            .any(|state| state.pending_entry.is_some() || state.pending_exit.is_some());
        if !positions_clear && !has_pending_orders && self.should_log_position_warn("entry_block") {
            log::info!(
                "[POSITION] open positions detected ({} symbols) with no pending orders; blocking new entries",
                self.open_positions.len()
            );
            self.last_position_warn
                .insert("entry_block".to_string(), Instant::now());
        }
        let mut planned: Vec<PlannedAction> = Vec::new();
        let now_ts = self.current_now_ts();

        let universe = self.cfg.universe.clone();
        for pair in &universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let (p1, p2) = match (price_map.get(&pair.base), price_map.get(&pair.quote)) {
                (Some(a), Some(b)) => (a, b),
                _ => continue,
            };
            if !(updated.contains(&pair.base) && updated.contains(&pair.quote)) {
                continue;
            }

            // Resolve BT deferred exits whose fill delay has elapsed
            // (bot-strategy#69). Must run before reconcile so the position
            // is cleared before entry evaluation on the same tick.
            if self.cfg.bt_fill_delay_secs > 0 {
                if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                    if let Some(ref deferred) = state.bt_deferred_exit {
                        if now_ts >= deferred.resolve_at_ts {
                            log::debug!(
                                "[BT_FILL_DELAY] {} resolved (delay={}s, now_ts={})",
                                key, self.cfg.bt_fill_delay_secs, now_ts
                            );
                            state.position = None;
                            state.last_exit_at = Some(Instant::now());
                            state.last_exit_ts = Some(now_ts);
                            state.bt_deferred_exit = None;
                        }
                    }
                }
            }

            // First, reconcile any pending entry/exit orders for this pair
            self.reconcile_pending_orders(inst_idx, &key, price_map).await?;

            let mut action = TradeAction::None;
            let log_a = self
                .latest_log_price(&pair.base)
                .ok_or_else(|| anyhow!("no bar for {}", pair.base))?;
            let log_b = self
                .latest_log_price(&pair.quote)
                .ok_or_else(|| anyhow!("no bar for {}", pair.quote))?;

            let (
                prev_eligible,
                z_snapshot,
                last_eval_ts,
                z_entry_copy,
                spread_len,
                position_state,
                velocity,
                beta_eff,
                beta_short,
                beta_long,
            ) = {
                let state = self
                    .instances[inst_idx]
                    .states
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("missing state for {}", key))?;
                let prev_eligible = state.eligible;
                // Kalman filter update: feed log-return diffs (dx, dy) per bar
                if let Some(ref mut kf) = state.kalman {
                    if state.last_spread.is_some() {
                        if let (Some(hist_b), Some(hist_a)) = (
                            self.history.get(&pair.quote),
                            self.history.get(&pair.base),
                        ) {
                            if hist_b.len() >= 2 && hist_a.len() >= 2 {
                                let dx = log_b - hist_b[hist_b.len() - 2].log_price;
                                let dy = log_a - hist_a[hist_a.len() - 2].log_price;
                                kf.update(dx, dy);
                            }
                        }
                    }
                }
                let spread = log_a - state.beta * log_b;
                state.push_spread(spread, self.cfg.metrics_window, &self.cfg);
                (
                    prev_eligible,
                    state.z_score_details(),
                    state.last_evaluated_ts,
                    state.z_entry,
                    state.spread_history.len(),
                    state.position.clone(),
                    state.last_velocity_sigma_per_min,
                    state.beta,
                    state.beta_short,
                    state.beta_long,
                )
            };

            // [ZCHECK] Per-step alignment audit log. Designed for side-by-side
            // comparison across A/B/C bots running the same pair: if buckets are
            // properly aligned, identical bucket_ts rows should show identical
            // close/beta/mean/std/z values across processes. See pairtrade#4.
            let base_bar = self.history.get(&pair.base).and_then(|h| h.back()).cloned();
            let quote_bar = self.history.get(&pair.quote).and_then(|h| h.back()).cloned();
            if let (Some(ba), Some(bq)) = (base_bar, quote_bar) {
                if let Some((z, std, mean, latest)) = z_snapshot {
                    log::info!(
                        "[ZCHECK] {} bucket_ts={} close_a={:.6} close_b={:.6} \
                         beta_eff={:.4} beta_s={:.4} beta_l={:.4} mean={:.6} std={:.6} \
                         spread={:.6} z={:.4} hist={}",
                        key,
                        ba.ts,
                        ba.log_price,
                        bq.log_price,
                        beta_eff,
                        beta_short,
                        beta_long,
                        mean,
                        std,
                        latest,
                        z,
                        spread_len,
                    );
                }
            }

            // Kalman beta diagnostic log (only when enabled, so golden test is not affected)
            if self.cfg.use_kalman_beta {
                let state = &self.instances[inst_idx].states[&key];
                if let Some(ref kf) = state.kalman {
                    log::info!(
                        "[KALMAN] {} kalman_beta={:.4} ols_beta={:.4} diff={:.4} p={:.6} warm={}",
                        key,
                        kf.beta,
                        beta_eff,
                        kf.beta - beta_eff,
                        kf.p,
                        kf.is_warm(self.cfg.kalman_min_updates),
                    );
                }
            }

            let pp = self.pair_params_for(inst_idx, &key).clone();
            let pp = &pp;
            let force_close_due = position_state
                .as_ref()
                .map(|pos| now_ts.saturating_sub(pos.entered_ts) >= pp.force_close_secs as i64)
                .unwrap_or(false);
            if force_close_due {
                if let Some(pos) = &position_state {
                    log::info!("[EXIT_CHECK] {} reason=force_close", key);
                    action = TradeAction::Close {
                        direction: pos.direction,
                        z: 0.0,
                        beta: beta_eff,
                        force: true,
                    };
                }
            }

            if self.instances[inst_idx].states[&key].pending_entry.is_some()
                || self.instances[inst_idx].states[&key].pending_exit.is_some()
                || self.instances[inst_idx].states[&key].bt_deferred_exit.is_some()
            {
                if !matches!(action, TradeAction::None) {
                    log::debug!("[ORDER] {} has pending orders; skipping new actions", key);
                }
                continue;
            }
            if self.instances[inst_idx].states[&key].position_guard {
                if matches!(action, TradeAction::None) {
                    if self.should_log_position_warn(&key) {
                        log::warn!(
                            "[POSITION] {} in unhedged/mismatch state; skipping new actions",
                            key
                        );
                        self.last_position_warn.insert(key.clone(), Instant::now());
                    }
                    continue;
                }
            }

            let needs_eval_interval = last_eval_ts
                .map(|t| now_ts.saturating_sub(t) >= PAIR_SELECTION_INTERVAL_SECS as i64)
                .unwrap_or(true);
            let needs_eval_jump = z_snapshot
                .map(|(z, _, _, _)| z.abs() >= z_entry_copy * pp.reeval_jump_z_mult)
                .unwrap_or(false);
            let needs_eval_velocity =
                velocity.abs() >= pp.spread_velocity_max_sigma_per_min * pp.reeval_jump_z_mult;
            let vol_spike = z_snapshot
                .and_then(|(_, std, _, _)| {
                    tail_std(&self.instances[inst_idx].states[&key].spread_history, self.cfg.metrics_window).map(
                        |base_std| {
                            if base_std <= 1e-9 {
                                0.0
                            } else {
                                std / base_std
                            }
                        },
                    )
                })
                .map(|ratio| ratio >= pp.vol_spike_mult)
                .unwrap_or(false);

            // TODO(#25 follow-up): evaluate_pair reads from engine.history
            // (shared) but is called per-instance and gated by
            // state.last_evaluated_ts (per-instance). In live mode the
            // per-instance now_ts values differ by a few seconds, so an
            // eval interval boundary can fall between instances — one
            // triggers eval while the others skip. This causes ~0.3% beta
            // drift between instances, which propagates into spread_history
            // / mean / std / z. The drift is negligible for A/B test
            // validity (variant params dominate by 50x) and self-corrects
            // within the 240-bar window, but to guarantee byte-identical z
            // long-term, move evaluate_pair + spread_history + beta to the
            // shared phase (step_shared) so all instances consume the same
            // evaluation result per tick.
            // BT replay override: when `BT_EVAL_TIMESTAMPS_FILE` is loaded
            // (bot-strategy#27 comment 2026-04-16), fire eval ONLY at the
            // exact wall-clock seconds where the live bot ran evaluate_pair.
            // This reproduces live's state.beta trajectory, which in turn
            // makes every past spread_history entry match live even when
            // the interval / z-jump / velocity gates would desync on the
            // compounded drift.
            let bt_eval_force = self
                .cfg
                .bt_eval_timestamps
                .as_ref()
                .map(|set| set.contains(&now_ts));
            let should_eval = match bt_eval_force {
                Some(force) => force,
                None => needs_eval_interval || needs_eval_jump || needs_eval_velocity || vol_spike,
            };
            let eval = if should_eval
            {
                let res = self.evaluate_pair(pair);
                if let Some(ref e) = res {
                    log::info!(
                        "[EVAL] {} beta_s={:.3} beta_l={:.3} beta={:.3} hl={:.2}h p={:.3} eligible={} score={:.3}",
                        key,
                        e.beta_short,
                        e.beta_long,
                        e.beta_eff,
                        e.half_life_hours,
                        e.adf_p_value,
                        e.eligible,
                        e.score
                    );
                } else {
                    let (avail_a, avail_b) = (
                        self.history.get(&pair.base).map(|h| h.len()).unwrap_or(0),
                        self.history.get(&pair.quote).map(|h| h.len()).unwrap_or(0),
                    );
                    log::debug!(
                        "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                        key,
                        pair.base,
                        avail_a,
                        pp.lookback_hours_long
                            .max(pp.lookback_hours_short)
                            * 3600
                            / self.cfg.trading_period_secs,
                        (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                        self.cfg.warm_start_mode
                    );
                    log::debug!(
                        "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                        key,
                        pair.quote,
                        avail_b,
                        pp.lookback_hours_long
                            .max(pp.lookback_hours_short)
                            * 3600
                            / self.cfg.trading_period_secs,
                        (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                        self.cfg.warm_start_mode
                    );
                }
                res
            } else {
                None
            };

            let mut log_positions_not_ready = false;
            let circuit_breaker_until_ts_snapshot = self.instances[inst_idx].circuit_breaker_until_ts;
            let consecutive_losses_snapshot = self.instances[inst_idx].consecutive_losses;
            let equity_cache_snapshot = self.instances[inst_idx].equity_cache;
            let equity_fallback_snapshot = self.instances[inst_idx].equity_usd_fallback;
            {
                let state = self
                    .instances[inst_idx]
                    .states
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("missing state for {}", key))?;
                if let Some(ref eval) = eval {
                    if self.cfg.use_kalman_beta {
                        if let Some(ref kf) = state.kalman {
                            if kf.is_warm(self.cfg.kalman_min_updates) {
                                state.beta = kf.beta;
                            } else {
                                state.beta = eval.beta_eff;
                            }
                        } else {
                            state.beta = eval.beta_eff;
                        }
                    } else {
                        state.beta = eval.beta_eff;
                    }
                    state.beta_short = eval.beta_short;
                    state.beta_long = eval.beta_long;
                    state.half_life_hours = eval.half_life_hours;
                    state.adf_p_value = eval.adf_p_value;
                    state.eligible = eval.eligible;
                    state.p_value_weighted_score = eval.score;
                    state.beta_gap = eval.beta_gap;
                    state.last_evaluated = Some(Instant::now());
                    state.last_evaluated_ts = Some(now_ts);
                }
                if prev_eligible != state.eligible {
                    log::info!(
                        "[ELIGIBILITY] {} -> {} (p={:.3} hl={:.2}h beta_gap={:.3})",
                        key,
                        state.eligible,
                        state.adf_p_value,
                        state.half_life_hours,
                        (state.beta_short - state.beta_long).abs()
                    );
                }

                let z_entry = entry_z_for_pair(&self.cfg, pp, state, vol_median);
                state.z_entry = z_entry;

                let min_points = (self.cfg.metrics_window / 2).max(10);
                if matches!(action, TradeAction::None) {
                    if state.eligible && spread_len >= min_points {
                        if let Some((z, std, mean, latest_spread)) = z_snapshot {
                            let net_funding = net_funding_for_direction(z, p1, p2);
                            if let Some(pos) = &state.position {
                                let equity_base = equity_cache_snapshot.max(equity_fallback_snapshot);
                                if let Some(reason) =
                                    exit_reason(&self.cfg, pp, state, z, std, p1, p2, equity_base, now_ts)
                                {
                                    log::info!(
                                    "[EXIT_CHECK] {} reason={} z={:.2} exit_z={:.2} stop_z={:.2} vel={:.3} max_vel={:.3}",
                                    key,
                                    reason,
                                    z,
                                    pp.exit_z,
                                    pp.stop_loss_z,
                                    state.last_velocity_sigma_per_min,
                                    pp.spread_velocity_max_sigma_per_min
                                );
                                    action = TradeAction::Close {
                                        direction: pos.direction,
                                        z,
                                        beta: state.beta,
                                        force: false,
                                    };
                                }
                            } else if !self.positions_ready {
                                log_positions_not_ready = true;
                            } else if circuit_breaker_until_ts_snapshot
                                .map_or(false, |until| now_ts < until)
                            {
                                // entry blocked by circuit breaker; logged via ZCHECK
                            } else if last_eval_ts.is_none() {
                                // Block entry until first evaluate_pair() completes,
                                // because beta is still at its initial value (1.0).
                            } else if !regime_ok {
                                // entry blocked by regime filter
                            } else if should_enter(&self.cfg, pp, state, z, std, net_funding, now_ts) {
                                let direction = if z > 0.0 {
                                    PositionDirection::ShortSpread
                                } else {
                                    PositionDirection::LongSpread
                                };
                                action = TradeAction::Open {
                                    direction,
                                    z,
                                    beta: state.beta,
                                };
                            }
                            let slope_sig =
                                spread_slope_sigma(&state.spread_history, self.cfg.metrics_window);
                            log::debug!(
                            "[ZCHECK] {} z={:.2} entry={:.2} std={:.4} mean={:.4} spread={:.4} hist={} beta_s={:.3} beta_l={:.3} funding={:.5} eligible={} beta_gap={:.3} slope_sigma={:.3} consec_loss={}",
                            key,
                            z,
                            state.z_entry,
                            std,
                            mean,
                            latest_spread,
                            spread_len,
                            beta_short,
                            beta_long,
                            net_funding,
                            state.eligible,
                            state.beta_gap,
                            slope_sig.unwrap_or(0.0),
                            consecutive_losses_snapshot
                        );
                        }
                    } else if state.eligible && spread_len < min_points {
                        log::debug!(
                            "[ZCHECK] {} skipped (spread history too short: {} < {})",
                            key,
                            spread_len,
                            min_points
                        );
                    } else if position_state.is_some() && !state.eligible {
                        // If pair falls out of eligibility, flatten
                        if let Some(pos) = &state.position {
                            log::info!("[EXIT_CHECK] {} reason=ineligible", key);
                            action = TradeAction::Close {
                                direction: pos.direction,
                                z: 0.0,
                                beta: state.beta,
                                force: false,
                            };
                        }
                    }
                }
            }
            if !positions_clear && matches!(action, TradeAction::Open { .. }) {
                log::debug!("[ENTRY] blocked due to open positions; key={}", key);
                action = TradeAction::None;
            }
            if maintenance_block_entries && matches!(action, TradeAction::Open { .. }) {
                action = TradeAction::None;
            }
            if self.shutdown_pending && matches!(action, TradeAction::Open { .. }) {
                log::debug!("[ENTRY] blocked by graceful shutdown; key={}", key);
                action = TradeAction::None;
            }

            if log_positions_not_ready && self.should_log_position_warn(&self.cfg.dex_name) {
                log::warn!("[POSITION] positions not synced yet; skipping entry");
                self.last_position_warn
                    .insert(self.cfg.dex_name.clone(), Instant::now());
            }

            if !matches!(action, TradeAction::None) {
                let net_funding = net_funding_for_direction(
                    match &action {
                        TradeAction::Open { z, .. } => *z,
                        TradeAction::Close { z, .. } => *z,
                        TradeAction::None => 0.0,
                    },
                    p1,
                    p2,
                );
                let abs_z = match &action {
                    TradeAction::Open { z, .. } | TradeAction::Close { z, .. } => z.abs(),
                    TradeAction::None => 0.0,
                };
                planned.push(PlannedAction {
                    pair: pair.clone(),
                    key: key.clone(),
                    action,
                    net_funding_per_hour: net_funding,
                    abs_z,
                    liquidity_score: liquidity_score(p1, p2),
                    p1: p1.clone(),
                    p2: p2.clone(),
                });
            }
        }

        self.maybe_log_metrics(inst_idx);
        // Process exits first
        for plan in planned.iter() {
            if let TradeAction::Close {
                direction,
                z,
                beta,
                force,
            } = plan.action
            {
                let qtys = self
                    .exit_sizes_for_pair(inst_idx, &plan.key, &plan.pair, beta, &plan.p1, &plan.p2)
                    .context("exit_sizes_for_pair")?;
                if qtys.0 <= Decimal::ZERO && qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} no open position sizes available; clearing state",
                        plan.key
                    );
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.position = None;
                        state.pending_exit = None;
                        state.position_guard = false;
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                    continue;
                }
                if qtys.0 <= Decimal::ZERO || qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} missing leg size (base={}, quote={}); closing available legs only",
                        plan.key,
                        qtys.0,
                        qtys.1
                    );
                }
                if self.cfg.dry_run {
                    let price_a = price_map
                        .get(&plan.pair.base)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let price_b = price_map
                        .get(&plan.pair.quote)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let pnl = self
                        .instances[inst_idx]
                        .states
                        .get(&plan.key)
                        .and_then(|s| s.position.as_ref())
                        .and_then(|pos| compute_pnl(pos, price_a, price_b));
                    if let Some(pnl) = pnl {
                        if let Some(pnl_value) = pnl.to_f64() {
                            let pos_ref = self.instances[inst_idx].states.get(&plan.key)
                                .and_then(|s| s.position.as_ref());
                            let hold_secs = pos_ref
                                .map(|p| now_ts.saturating_sub(p.entered_ts).max(0) as f64);
                            let entry_a = pos_ref
                                .and_then(|p| p.entry_price_a)
                                .and_then(|v| v.to_f64());
                            let entry_b = pos_ref
                                .and_then(|p| p.entry_price_b)
                                .and_then(|v| v.to_f64());
                            let record = PnlLogRecord::new(
                                &plan.pair.base,
                                &plan.pair.quote,
                                direction,
                                pnl_value,
                                now_ts,
                                "exit_dry_run",
                            ).with_trade_details(
                                entry_a, entry_b,
                                price_a.to_f64(), price_b.to_f64(),
                                Some(beta), Some(z),
                                self.instances[inst_idx].states.get(&plan.key)
                                    .and_then(|s| s.last_spread.map(|_| z)),
                                hold_secs,
                            );
                            self.write_pnl_record(inst_idx, record);
                            if pnl_value < 0.0 {
                                self.instances[inst_idx].consecutive_losses += 1;
                                if let Some(cooldown) = self
                                    .cfg
                                    .circuit_breaker_cooldown_for(self.instances[inst_idx].consecutive_losses)
                                {
                                    self.instances[inst_idx].circuit_breaker_until = Some(Instant::now() + cooldown);
                                    self.instances[inst_idx].circuit_breaker_until_ts =
                                        Some(now_ts + cooldown.as_secs() as i64);
                                    log::warn!(
                                        "[CIRCUIT_BREAKER] activated after {} consecutive losses, cooldown {}s",
                                        self.instances[inst_idx].consecutive_losses, cooldown.as_secs()
                                    );
                                }
                            } else if pnl_value > 0.0 {
                                if self.instances[inst_idx].consecutive_losses > 0 {
                                    log::info!("[CIRCUIT_BREAKER] reset after win (was {} consecutive losses)", self.instances[inst_idx].consecutive_losses);
                                }
                                self.instances[inst_idx].consecutive_losses = 0;
                                self.instances[inst_idx].circuit_breaker_until = None;
                                self.instances[inst_idx].circuit_breaker_until_ts = None;
                            }
                        }
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} pnl={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            pnl,
                            now_ts
                        );
                    } else {
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            now_ts
                        );
                    }
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        if self.cfg.backtest_mode && self.cfg.bt_fill_delay_secs > 0 {
                            // Defer position clearing to simulate exchange
                            // fill latency (bot-strategy#69).
                            state.bt_deferred_exit = Some(BtDeferredExit {
                                resolve_at_ts: now_ts + self.cfg.bt_fill_delay_secs,
                            });
                        } else {
                            state.position = None;
                            state.last_exit_at = Some(Instant::now());
                            state.last_exit_ts = Some(now_ts);
                        }
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[EXIT] observe-only mode; skipping close orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    let legs = match self
                        .close_pair_orders(&plan.pair, direction, qtys, price_map, force)
                        .await
                    {
                        Ok(legs) => legs,
                        Err(err) => {
                            self.register_partial_leg_failure(inst_idx, &plan.key, direction, &err, true);
                            return Err(err);
                        }
                    };
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.pending_exit = Some(PendingOrders {
                            legs,
                            direction,
                            placed_at: Instant::now(),
                            hedge_retry_count: 0,
                            post_only_hybrid: false,
                        });
                    }
                }
            }
        }

        let mut active_symbols: HashSet<String> = self
            .cfg
            .universe
            .iter()
            .filter_map(|pair| {
                let key = format!("{}/{}", pair.base, pair.quote);
                let state = self.instances[inst_idx].states.get(&key)?;
                let is_active = state.position.is_some()
                    || state.pending_entry.is_some()
                    || state.pending_exit.is_some()
                    || state.bt_deferred_exit.is_some()
                    || state.position_guard;
                if is_active {
                    let mut symbols = HashSet::new();
                    symbols.insert(pair.base.clone());
                    symbols.insert(pair.quote.clone());
                    Some(symbols)
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        for symbol in self.open_positions.keys() {
            if self.history.contains_key(symbol) {
                active_symbols.insert(symbol.clone());
            }
        }

        // Among entry candidates, shortlist by model score then pick best by funding->score->liquidity->|z|
        let mut entry_candidates: Vec<&PlannedAction> = planned
            .iter()
            .filter(|p| matches!(p.action, TradeAction::Open { .. }))
            .filter(|p| {
                if active_symbols.is_empty() {
                    return true;
                }
                let overlaps =
                    active_symbols.contains(&p.pair.base) || active_symbols.contains(&p.pair.quote);
                if overlaps {
                    log::debug!(
                        "[OVERLAP] skipping {}/{} due to active symbol overlap",
                        p.pair.base,
                        p.pair.quote
                    );
                }
                !overlaps
            })
            .collect();
        entry_candidates.sort_by(|a, b| {
            self.state_score(inst_idx, &b.key)
                .partial_cmp(&self.state_score(inst_idx, &a.key))
                .unwrap_or(Ordering::Equal)
        });
        let shortlisted: Vec<&PlannedAction> = entry_candidates
            .into_iter()
            .take(self.cfg.max_active_pairs.max(1))
            .collect();
        let best_entry = shortlisted.into_iter().max_by(|a, b| {
            a.net_funding_per_hour
                .partial_cmp(&b.net_funding_per_hour)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    self.state_score(inst_idx, &a.key)
                        .partial_cmp(&self.state_score(inst_idx, &b.key))
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| {
                    a.liquidity_score
                        .partial_cmp(&b.liquidity_score)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| a.abs_z.partial_cmp(&b.abs_z).unwrap_or(Ordering::Equal))
        });
        if let Some(plan) = best_entry {
            if let TradeAction::Open { direction, z, beta } = plan.action {
                // Force-fresh equity immediately before sizing: entries happen
                // rarely enough that this REST call is cheap, and it keeps
                // notional sized against the current balance rather than the
                // 30-min cache used for dashboard / R-budget. See
                // bot-strategy#156.
                self.fetch_equity_rest(inst_idx).await;
                let qtys = self
                    .hedged_sizes(inst_idx, &plan.pair, beta, &plan.p1, &plan.p2)
                    .context("hedged_sizes")?;
                let price_a = price_map
                    .get(&plan.pair.base)
                    .map(|s| s.price)
                    .unwrap_or_default();
                let price_b = price_map
                    .get(&plan.pair.quote)
                    .map(|s| s.price)
                    .unwrap_or_default();
                if self.cfg.dry_run {
                    log::info!(
                            "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            plan.net_funding_per_hour,
                            now_ts
                        );
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.position = Some(Position {
                            direction,
                            entered_at: Instant::now(),
                            entered_ts: now_ts,
                            entry_price_a: Some(price_a),
                            entry_price_b: Some(price_b),
                            entry_size_a: Some(qtys.0),
                            entry_size_b: Some(qtys.1),
                            entry_z: Some(z),
                        });
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[ENTRY] observe-only mode; skipping entry orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    log::info!(
                        "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                        plan.pair.base,
                        plan.pair.quote,
                        direction,
                        qtys.0,
                        price_a,
                        qtys.1,
                        price_b,
                        z,
                        beta,
                        plan.net_funding_per_hour,
                        now_ts
                    );
                    let legs = match self
                        .place_pair_orders(inst_idx, &plan.pair, direction, qtys, price_map)
                        .await
                    {
                        Ok(legs) => legs,
                        Err(err) => {
                            self.register_partial_leg_failure(inst_idx, &plan.key, direction, &err, false);
                            return Err(err);
                        }
                    };
                    let entry_pp = self.pair_params_for(inst_idx, &plan.key).clone();
                    let entry_pp = &entry_pp;
                    let hybrid =
                        entry_pp.entry_post_only_timeout_secs > 0 && self.post_only_supported();
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.pending_entry = Some(PendingOrders {
                            legs,
                            direction,
                            placed_at: Instant::now(),
                            hedge_retry_count: 0,
                            post_only_hybrid: hybrid,
                        });
                    }
                }
            }
        }

        if let Some(reporter) = &mut self.instances[inst_idx].status_reporter {
            if let Err(err) =
                reporter.write_snapshot_if_due(&self.open_positions, self.positions_ready)
            {
                log::warn!("[STATUS] failed to write status: {:?}", err);
            }
        }
        Ok(())
    }

    fn latest_log_price(&self, symbol: &str) -> Option<f64> {
        self.history
            .get(symbol)
            .and_then(|h| h.back())
            .map(|p| p.log_price)
    }

    async fn refresh_equity_if_needed(&mut self, inst_idx: usize) -> Result<()> {
        const CACHE_SECS: u64 = EQUITY_REFRESH_CACHE_SECS;
        if self.instances[inst_idx]
            .last_equity_fetch
            .map(|t| t.elapsed() < Duration::from_secs(CACHE_SECS))
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.fetch_equity_rest(inst_idx).await;
        Ok(())
    }

    async fn fetch_equity_rest(&mut self, inst_idx: usize) {
        // Minimum spacing between /account REST calls across all instances.
        // Lighter enforces a per-IP short-window rate-limit on /account the
        // sidecar can't see; empirically ~1 call per 5s survives. Shared
        // across instances so step() only waits when a recent call exists,
        // not unconditionally on every inst_idx > 0. See bot-strategy#122.
        const MIN_ACCOUNT_SPACING: Duration = Duration::from_millis(5_500);
        if let Some(last) = self.last_account_rest_call {
            let elapsed = last.elapsed();
            if elapsed < MIN_ACCOUNT_SPACING {
                tokio::time::sleep(MIN_ACCOUNT_SPACING - elapsed).await;
            }
        }
        self.last_account_rest_call = Some(Instant::now());
        match self.connector.get_balance(None).await {
            Ok(resp) => {
                if let Some(eq) = resp.equity.to_f64() {
                    let inst = &mut self.instances[inst_idx];
                    inst.equity_cache = eq.max(0.0);
                    inst.last_equity_fetch = Some(Instant::now());
                    if let Some(reporter) = &mut inst.status_reporter {
                        reporter.update_equity(inst.equity_cache);
                    }
                }
            }
            Err(err) => {
                log::warn!("equity refresh failed for {}: {:?}", self.instances[inst_idx].id, err);
                self.instances[inst_idx].last_equity_fetch = Some(Instant::now());
            }
        }
    }

    async fn sync_positions_from_exchange(
        &mut self,
        inst_idx: usize,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Result<()> {
        if self.replay_connector.is_some() {
            return Ok(());
        }
        let now_ts = self.current_now_ts();
        let positions = match self.connector.get_positions().await {
            Ok(v) => v,
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("positions not ready from websocket") {
                    let stale_clear_secs = self.cfg.order_timeout_secs.max(1).saturating_mul(6);
                    self.clear_stale_pending(inst_idx, Duration::from_secs(stale_clear_secs), "ws_not_ready");
                    // Startup transient: the Lighter WS hasn't pushed the
                    // initial position snapshot yet. Resolves within seconds
                    // of the first WS push. Log at INFO so it does not
                    // inflate error_summary and trigger the error-watch
                    // workflow (bot-strategy#49) on every restart. Other
                    // get_positions failures keep WARN below.
                    if self.should_log_position_warn(&self.cfg.dex_name) {
                        log::info!(
                            "[POSITION] waiting for initial WS positions on {}",
                            self.cfg.dex_name
                        );
                        self.last_position_warn
                            .insert(self.cfg.dex_name.clone(), Instant::now());
                    }
                    self.positions_ready = false;
                    return Ok(());
                }
                if self.should_log_position_warn(&self.cfg.dex_name) {
                    log::warn!(
                        "[POSITION] get_positions not available for {}: {:?}",
                        self.cfg.dex_name,
                        err
                    );
                    self.last_position_warn
                        .insert(self.cfg.dex_name.clone(), Instant::now());
                }
                return Ok(());
            }
        };
        self.positions_ready = true;

        let mut snapshots: HashMap<String, PositionSnapshot> = HashMap::new();
        for snapshot in positions {
            if snapshot.sign == 0 || snapshot.size <= Decimal::ZERO {
                continue;
            }
            if self.is_dust_position(&snapshot, prices) {
                continue;
            }
            snapshots.insert(snapshot.symbol.clone(), snapshot);
        }
        self.open_positions = snapshots.clone();

        let mut unhedged_attempted: HashSet<String> = HashSet::new();
        let mut unhedged_closures: Vec<(String, String, i32, Decimal)> = Vec::new();
        for pair in &self.cfg.universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let log_warn = self.should_log_position_warn(&key);

            let Some(state) = self.instances[inst_idx].states.get_mut(&key) else {
                continue;
            };

            let base = snapshots.get(&pair.base);
            let quote = snapshots.get(&pair.quote);

            if state.pending_entry.is_some() || state.pending_exit.is_some() {
                // Keep pending orders; reconciliation handles timeouts/hedging.
                continue;
            }

            match (base, quote) {
                (None, None) => {
                    if state.position.is_some() || state.position_guard {
                        log::info!("[POSITION] {} cleared by exchange snapshot", key);
                    }
                    state.position = None;
                    state.position_guard = false;
                }
                (Some(b), Some(q)) => {
                    if b.sign * q.sign >= 0 {
                        if log_warn {
                            log::warn!(
                                "[POSITION] {} has mismatched legs (signs {} / {})",
                                key,
                                b.sign,
                                q.sign
                            );
                        }
                        if log_warn {
                            self.last_position_warn.insert(key.clone(), Instant::now());
                        }
                        state.position = None;
                        state.position_guard = true;
                        continue;
                    }

                    let direction = if b.sign > 0 {
                        PositionDirection::LongSpread
                    } else {
                        PositionDirection::ShortSpread
                    };
                    let (entered_at, entered_ts) = state
                        .position
                        .as_ref()
                        .map(|p| (p.entered_at, p.entered_ts))
                        .unwrap_or((Instant::now(), now_ts));
                    let prev_entry_z = state.position.as_ref().and_then(|p| p.entry_z);
                    state.position = Some(Position {
                        direction,
                        entered_at,
                        entered_ts,
                        entry_price_a: b.entry_price,
                        entry_price_b: q.entry_price,
                        entry_size_a: Some(b.size),
                        entry_size_b: Some(q.size),
                        entry_z: prev_entry_z,
                    });
                    state.position_guard = false;
                }
                _ => {
                    let active_for_warn = state.position.is_some()
                        || state.pending_entry.is_some()
                        || state.pending_exit.is_some();
                    if state.pending_entry.is_none() && state.pending_exit.is_none() {
                        if let Some((symbol, snapshot)) = base
                            .map(|b| (pair.base.clone(), b))
                            .or_else(|| quote.map(|q| (pair.quote.clone(), q)))
                        {
                            if unhedged_attempted.insert(symbol.clone()) {
                                unhedged_closures.push((
                                    key.clone(),
                                    symbol.clone(),
                                    snapshot.sign,
                                    snapshot.size,
                                ));
                            }
                        }
                    }
                    if log_warn && active_for_warn {
                        log::warn!(
                            "[POSITION] {} has unhedged leg (base={}, quote={})",
                            key,
                            base.is_some(),
                            quote.is_some()
                        );
                        self.last_position_warn.insert(key.clone(), Instant::now());
                        state.position_guard = true;
                    } else {
                        state.position_guard = false;
                    }
                    if !active_for_warn {
                        state.position = None;
                    }
                }
            }
        }

        for (key, symbol, sign, size) in unhedged_closures {
            self.try_close_unhedged_leg(inst_idx, &key, &symbol, sign, size, prices)
                .await;
        }

        Ok(())
    }

    async fn try_close_unhedged_leg(
        &mut self,
        inst_idx: usize,
        key: &str,
        symbol: &str,
        sign: i32,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) {
        let now_ts = self.current_now_ts();
        if self.cfg.dry_run || self.cfg.observe_only {
            log::warn!(
                "[UNHEDGED] {} close skipped (mode) symbol={} size={}",
                key,
                symbol,
                size
            );
            return;
        }

        const UNHEDGED_CLOSE_COOLDOWN_SECS: u64 = 30;
        let last_exit = self.instances[inst_idx].states.get(key).and_then(|state| state.last_exit_at);
        if let Some(last_exit) = last_exit {
            if last_exit.elapsed() < Duration::from_secs(UNHEDGED_CLOSE_COOLDOWN_SECS) {
                return;
            }
        }

        let side = if sign >= 0 {
            dex_connector::OrderSide::Short
        } else {
            dex_connector::OrderSide::Long
        };
        let qty = self.quantize_order_size_close(symbol, size, prices);
        if qty <= Decimal::ZERO {
            log::warn!(
                "[UNHEDGED] {} close skipped (qty=0) symbol={} size={}",
                key,
                symbol,
                size
            );
            return;
        }

        log::warn!(
            "[UNHEDGED] {} closing lone leg symbol={} sign={} size={} qty={} side={:?}",
            key,
            symbol,
            sign,
            size,
            qty,
            side
        );

        let res = self
            .connector
            .create_order(symbol, qty, side, None, None, true, None)
            .await;

        match res {
            Ok(res) => {
                log::info!(
                    "[UNHEDGED] {} close submitted symbol={} order_id={}",
                    key,
                    symbol,
                    res.order_id
                );
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                }
            }
            Err(err) => {
                if Self::is_reduce_only_position_missing_error(&err)
                    && self.confirm_reduce_only_position_missing(symbol).await
                {
                    log::info!(
                        "[UNHEDGED] {} close skipped; position already closed symbol={}",
                        key,
                        symbol
                    );
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                } else {
                    log::error!(
                        "[UNHEDGED] {} close failed symbol={} err={:?}",
                        key,
                        symbol,
                        err
                    );
                }
            }
        }
    }

    fn clear_stale_pending(&mut self, inst_idx: usize, max_age: Duration, reason: &str) {
        let now_ts = self.current_now_ts();
        for (key, state) in self.instances[inst_idx].states.iter_mut() {
            let entry_age = state.pending_entry.as_ref().map(|p| p.placed_at.elapsed());
            let exit_age = state.pending_exit.as_ref().map(|p| p.placed_at.elapsed());
            let age = match (entry_age, exit_age) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            if let Some(age) = age {
                if age >= max_age {
                    log::warn!(
                        "[POSITION] {} pending cleared (reason={}, age={}s)",
                        key,
                        reason,
                        age.as_secs()
                    );
                    state.pending_entry = None;
                    state.pending_exit = None;
                    state.position = None;
                    state.position_guard = false;
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                }
            }
        }
    }

    fn compute_vol_median(&self, inst_idx: usize) -> f64 {
        let tail_len = self.entry_vol_window();
        let mut vols: Vec<f64> = self
            .instances[inst_idx]
            .states
            .values()
            .filter_map(|s| tail_std(&s.spread_history, tail_len))
            .collect();
        if vols.is_empty() {
            return 1.0;
        }
        vols.sort_by(|a, b| a.partial_cmp(b).unwrap());
        vols[vols.len() / 2].max(1e-9)
    }

    fn maybe_log_metrics(&mut self, inst_idx: usize) {
        const LOG_INTERVAL: u64 = 300;
        if self
            .last_metrics_log
            .map(|t| t.elapsed() < Duration::from_secs(LOG_INTERVAL))
            .unwrap_or(false)
        {
            return;
        }
        let mut lines = Vec::new();
        for (k, s) in &self.instances[inst_idx].states {
            let z = s.z_score().map(|(z, _)| z).unwrap_or(0.0);
            lines.push(format!(
                "{} elig={} z={:.2} beta={:.2} hl={:.2}h p={:.3}",
                k, s.eligible, z, s.beta, s.half_life_hours, s.adf_p_value
            ));
        }
        lines.sort();
        if !lines.is_empty() {
            log::info!("[METRICS] {}", lines.join(" | "));
        }
        self.last_metrics_log = Some(Instant::now());
    }

    fn state_score(&self, inst_idx: usize, key: &str) -> f64 {
        self.instances[inst_idx].states
            .get(key)
            .map(|s| s.p_value_weighted_score)
            .unwrap_or(0.0)
    }

    fn should_log_ob_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ob_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn should_log_ticker_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ticker_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn should_log_position_warn(&self, key: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_position_warn
            .get(key)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn is_dust_position(
        &self,
        snapshot: &PositionSnapshot,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> bool {
        let Some(symbol_snapshot) = prices.get(&snapshot.symbol) else {
            return false;
        };
        let Some(min_order) = symbol_snapshot.min_order else {
            return false;
        };
        snapshot.size < min_order
    }

    fn is_ticker_auth_error(msg: &str) -> bool {
        let lower = msg.to_ascii_lowercase();
        lower.contains("403")
            || lower.contains("forbidden")
            || lower.contains("failed to deserialize response")
            || lower.contains("expected value at line 1 column 1")
    }

    fn is_reduce_only_position_missing_error(err: &DexError) -> bool {
        let msg = match err {
            DexError::ServerResponse(message) | DexError::Other(message) => message,
            _ => return false,
        };
        let lower = msg.to_ascii_lowercase();
        lower.contains("position is missing for reduce-only order")
            || lower.contains("position is missing for reduce only order")
    }

    async fn confirm_reduce_only_position_missing(&mut self, symbol: &str) -> bool {
        let cached_has_position = self
            .open_positions
            .get(symbol)
            .map(|p| p.sign != 0 && p.size > Decimal::ZERO)
            .unwrap_or(false);
        if !cached_has_position && self.positions_ready {
            return true;
        }

        match self.connector.get_positions().await {
            Ok(positions) => {
                let has_position = positions
                    .iter()
                    .any(|p| p.symbol == symbol && p.sign != 0 && p.size > Decimal::ZERO);
                if !has_position {
                    self.open_positions.remove(symbol);
                    return true;
                }
            }
            Err(err) => {
                log::warn!(
                    "[ORDER] reduce-only missing check failed for {}: {:?}",
                    symbol,
                    err
                );
            }
        }
        false
    }

    fn persist_history_to_disk(&self) {
        // Persist the engine's shared log-price history plus the first
        // instance's per-pair `spread_history`. We pick instance 0 as
        // the representative: A/B/C instances drift ≤0.3% per the
        // existing TODO near evaluate_pair, which on reload converges
        // back to whatever was persisted. Persisting per-instance would
        // require an instance ID in the schema — over-engineered for
        // the single-bot-per-process setup this field currently supports.
        let spread_histories: HashMap<String, VecDeque<f64>> = self
            .instances
            .first()
            .map(|inst| {
                inst.states
                    .iter()
                    .map(|(k, s)| (k.clone(), s.spread_history.clone()))
                    .collect()
            })
            .unwrap_or_default();
        history_io::persist_history_to_disk(
            &self.cfg,
            &self.history,
            &spread_histories,
            &self.history_path,
        );
    }

    fn load_history_from_disk(&mut self) {
        let now = self.current_now_ts();
        let max_len = self.max_history_len();
        let mut loaded_spreads: HashMap<String, VecDeque<f64>> = HashMap::new();
        history_io::load_history_from_disk(
            &self.cfg,
            &mut self.history,
            &mut loaded_spreads,
            &self.history_path,
            now,
            max_len,
        );
        if loaded_spreads.is_empty() {
            return;
        }
        // Apply the persisted spread_history only when the instance's own
        // spread_history is still empty — i.e. on the initial post-restart
        // load, before any ticks have pushed a live spread. Subsequent
        // per-tick loads must NOT clobber the instance's accumulating
        // series, otherwise every step would silently revert the
        // previous step's push (in single-bot mode) or import another
        // bot's beta trajectory (in multi-bot mode, which is not the
        // intended sharing axis — peer bots coordinate on log_prices,
        // not on state.beta-dependent derived series).
        for inst in &mut self.instances {
            for (pair_key, spreads) in &loaded_spreads {
                if let Some(state) = inst.states.get_mut(pair_key) {
                    if state.spread_history.is_empty() {
                        state.last_spread = spreads.back().copied();
                        state.spread_history = spreads.clone();
                    }
                }
            }
        }
    }

    /// Rebuild each pair's beta and spread_history from the shared on-disk
    /// price history so A/B/C bots have identical regression windows the
    /// instant they start, instead of waiting metrics_window live bars to
    /// converge (pairtrade#4). Computes beta directly from whatever bars
    /// are available — does not go through evaluate_pair() because that
    /// path enforces full lookback_hours_long under Strict warm-start and
    /// would skip the seed when the loaded history is shorter than the
    /// configured long window.
    fn warm_start_states_from_history(&mut self) {
        if self.cfg.disable_history_persist {
            return;
        }
        for inst_idx in 0..self.instances.len() {
            for pair in self.cfg.universe.clone() {
                let key = format!("{}/{}", pair.base, pair.quote);
                let (Some(hist_a), Some(hist_b)) =
                    (self.history.get(&pair.base), self.history.get(&pair.quote))
                else { continue };
                let take = self.cfg.metrics_window.min(hist_a.len()).min(hist_b.len());
                if take < 2 { continue }
                let tail_a = tail_samples(hist_a, take);
                let tail_b = tail_samples(hist_b, take);
                let beta = regression_beta(&tail_b, &tail_a);
                let Some(state) = self.instances[inst_idx].states.get_mut(&key) else { continue };
                state.beta = beta;
                state.beta_short = beta;
                state.beta_long = beta;
                // If `load_history_from_disk` / `load_history_snapshot_for_bt`
                // has already restored the real persisted `spread_history`
                // (v2 snapshot), keep it as-is. Synthesizing a
                // single-OLS-beta series here would overwrite a 240-bar
                // real series with one whose variance is artificially
                // compressed — the mechanism behind the 2026-04-15 06:02
                // UTC "std collapse" restart incident (bot-strategy#62).
                // Only synthesize when the instance has no live spreads
                // (fresh start with no persisted snapshot, or a v1
                // snapshot from a pre-fix bot).
                if state.spread_history.is_empty() {
                    let spreads: VecDeque<f64> = tail_a
                        .iter()
                        .zip(tail_b.iter())
                        .map(|(sa, sb)| sa.log_price - beta * sb.log_price)
                        .collect();
                    state.last_spread = spreads.back().copied();
                    state.spread_history = spreads;
                    log::info!(
                        "[WARM_START] {} synthesized spread_history len={} beta={:.4} (no persisted v2 series)",
                        key, state.spread_history.len(), state.beta
                    );
                } else {
                    log::info!(
                        "[WARM_START] {} kept persisted spread_history len={} beta={:.4} (no synthesis)",
                        key, state.spread_history.len(), state.beta
                    );
                }
            }
        }
    }

    fn entry_vol_window(&self) -> usize {
        ((self.cfg.default_pair_params.entry_vol_lookback_hours * 3600)
            / self.cfg.trading_period_secs)
            .max(1) as usize
    }

    /// Virtual clock used by all duration-based decisions. In live mode this
    /// is the wall-clock UTC second; in backtest mode it tracks the replay
    /// connector's logical timestamp so cooldown / force_close /
    /// circuit_breaker / re-eval intervals fire correctly under replay.
    fn current_now_ts(&self) -> i64 {
        if self.cfg.backtest_mode {
            self.replay_connector
                .as_ref()
                .and_then(|r| r.current_timestamp_secs())
                .unwrap_or_else(|| chrono::Utc::now().timestamp())
        } else {
            chrono::Utc::now().timestamp()
        }
    }

    fn max_history_len(&self) -> usize {
        let mut max_needed = 0usize;
        // Consider all per-pair params and the default
        let all_params =
            std::iter::once(&self.cfg.default_pair_params).chain(self.cfg.pair_params.values());
        for pp in all_params {
            let max_hrs = pp.lookback_hours_long.max(pp.lookback_hours_short);
            let needed = (max_hrs * 3600 / self.cfg.trading_period_secs) as usize;
            let vol_needed = ((pp.entry_vol_lookback_hours * 3600) / self.cfg.trading_period_secs)
                .max(1) as usize;
            max_needed = max_needed.max(needed).max(vol_needed);
        }
        max_needed.max(self.cfg.metrics_window)
    }

    async fn reconcile_pending_orders(
        &mut self,
        inst_idx: usize,
        key: &str,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<()> {
        let timeout = Duration::from_secs(self.cfg.order_timeout_secs.max(1));
        let now_ts = self.current_now_ts();
        let (pending_entry, pending_exit) = {
            let state = self
                .instances[inst_idx]
                .states
                .get_mut(key)
                .ok_or_else(|| anyhow!("missing state for {}", key))?;
            (state.pending_entry.take(), state.pending_exit.take())
        };

        if let Some(mut pending) = pending_entry {
            let status = self.pending_status(&pending).await?;
            self.update_pending_fills(&mut pending, &status.fills);
            let filled_qtys = self.filled_by_leg(&pending, &status.fills);
            if self.all_filled(&pending, &status.fills) {
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    let (mut ep_a, mut ep_b, mut es_a, mut es_b) = (None, None, None, None);
                    if let Some((base, quote)) = key.split_once('/') {
                        for leg in &pending.legs {
                            if leg.symbol == base {
                                ep_a = price_map.get(base).map(|s| s.price);
                                es_a = Some(leg.target);
                            } else if leg.symbol == quote {
                                ep_b = price_map.get(quote).map(|s| s.price);
                                es_b = Some(leg.target);
                            }
                        }
                    }
                    let z_at_entry = state.z_score().map(|(z, _)| z);
                    state.position = Some(Position {
                        direction: pending.direction,
                        entered_at: Instant::now(),
                        entered_ts: now_ts,
                        entry_price_a: ep_a,
                        entry_price_b: ep_b,
                        entry_size_a: es_a,
                        entry_size_b: es_b,
                        entry_z: z_at_entry,
                    });
                    state.pending_entry = None;
                }
                log::info!("[ORDER] {} entry orders filled", key);
            } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                let max_retries = self.cfg.entry_partial_fill_max_retries;
                let use_market = max_retries > 0 && next_retry > max_retries;
                if use_market {
                    log::info!(
                        "[ORDER] {} entry leg partially filled, retries exceeded ({} > {}); reissuing remaining legs as MARKET",
                        key,
                        next_retry,
                        max_retries
                    );
                } else if max_retries > 0 {
                    log::info!(
                        "[ORDER] {} entry leg partially filled, reissuing remaining legs (retry {}/{})",
                        key,
                        next_retry,
                        max_retries
                    );
                } else {
                    log::warn!(
                        "[ORDER] {} entry leg partially filled, reissuing remaining legs",
                        key
                    );
                }
                self.cancel_pending_orders(&pending).await?;
                if let Some(new_pending) = self
                    .reissue_partial_legs(
                        &pending,
                        &filled_qtys,
                        price_map,
                        false,
                        use_market,
                        next_retry,
                    )
                    .await?
                {
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_entry = Some(new_pending);
                    }
                } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_entry = None;
                }
                return Ok(());
            } else if pending.post_only_hybrid {
                let recon_pp = self.pair_params_for(inst_idx, key).clone();
                let recon_pp = &recon_pp;
                if recon_pp.entry_post_only_timeout_secs > 0
                    && pending.placed_at.elapsed()
                        >= Duration::from_secs(recon_pp.entry_post_only_timeout_secs)
                {
                    // Phase 0 instrumentation (bot-strategy#165): capture per-leg
                    // fill status, posted limit vs current book, and z-movement
                    // from entry to timeout so we can tell why the post-only
                    // legs didn't fill before falling back to taker.
                    let (z_entry, z_now) = {
                        let state_ref = self.instances[inst_idx].states.get(key);
                        let ze = state_ref.map(|s| s.z_entry).unwrap_or(0.0);
                        let zn = state_ref
                            .and_then(|s| s.z_score().map(|(z, _)| z))
                            .unwrap_or(0.0);
                        (ze, zn)
                    };
                    let leg_details: Vec<String> = pending
                        .legs
                        .iter()
                        .map(|leg| {
                            let filled = status
                                .fills
                                .get(&leg.order_id)
                                .cloned()
                                .unwrap_or(Decimal::ZERO);
                            let open = status.open_ids.contains(&leg.order_id);
                            let snap = price_map.get(&leg.symbol);
                            let bid = snap.and_then(|s| s.bid_price);
                            let ask = snap.and_then(|s| s.ask_price);
                            let tick = snap.and_then(|s| s.min_tick);
                            format!(
                                "[{}|{:?}|tgt={}|filled={}|open={}|limit={}|bid={}|ask={}|tick={}]",
                                leg.symbol,
                                leg.side,
                                leg.target,
                                filled,
                                open,
                                leg.limit_price
                                    .map(|d| d.to_string())
                                    .unwrap_or_else(|| "none".into()),
                                bid.map(|d| d.to_string())
                                    .unwrap_or_else(|| "?".into()),
                                ask.map(|d| d.to_string())
                                    .unwrap_or_else(|| "?".into()),
                                tick.map(|d| d.to_string())
                                    .unwrap_or_else(|| "?".into()),
                            )
                        })
                        .collect();
                    log::info!(
                        "[ORDER_FALLBACK_DETAIL] {} elapsed={}s dir={:?} z_entry={:.2} z_now={:.2} legs={}",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        pending.direction,
                        z_entry,
                        z_now,
                        leg_details.join(" ")
                    );

                    // Post-only entry timed out; cancel and reissue as taker
                    log::info!(
                        "[ORDER] {} post-only entry timeout ({}s), falling back to taker",
                        key,
                        recon_pp.entry_post_only_timeout_secs
                    );
                    self.cancel_pending_orders(&pending).await?;
                    let new_pending = self
                        .reissue_entry_as_taker(key, &pending, price_map)
                        .await?;
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_entry = new_pending;
                    }
                }
            } else if pending.placed_at.elapsed() >= timeout {
                // Partial fill or stuck orders; cancel and flatten any filled leg
                if status.open_remaining > 0 {
                    log::warn!(
                        "[ORDER] {} entry orders stale ({}s), cancelling {} legs",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        status.open_remaining
                    );
                    for leg in &pending.legs {
                        let filled = filled_qtys
                            .get(&leg.order_id)
                            .cloned()
                            .unwrap_or(Decimal::ZERO);
                        let is_open = status.open_ids.contains(&leg.order_id);
                        log::debug!(
                            "[ORDER] {} entry leg status symbol={} order_id={} target={} filled={} open={}",
                            key,
                            leg.symbol,
                            leg.order_id,
                            leg.target,
                            filled,
                            is_open
                        );
                    }
                    self.cancel_pending_orders(&pending).await?;
                }
                let filled_qtys = self.filled_by_leg(&pending, &status.fills);
                let mut flattened_any = false;
                let mut hedge_failed = false;
                let mut retry_count = pending.hedge_retry_count;
                let max_retries = 3u32;
                for leg in &pending.legs {
                    let filled = filled_qtys
                        .get(&leg.order_id)
                        .cloned()
                        .unwrap_or(Decimal::ZERO);
                    if filled > Decimal::ZERO {
                        if price_map.contains_key(&leg.symbol) {
                            let hedge_side = match leg.side {
                                dex_connector::OrderSide::Long => dex_connector::OrderSide::Short,
                                dex_connector::OrderSide::Short => dex_connector::OrderSide::Long,
                            };
                            let use_market = retry_count + 1 >= max_retries;
                            let limit = if use_market {
                                None
                            } else {
                                self.limit_price_for(&leg.symbol, hedge_side, price_map)
                            };
                            if !use_market && limit.is_none() {
                                log::warn!(
                                    "[ORDER] Missing reference price for hedge {} leg {}",
                                    leg.symbol,
                                    leg.order_id
                                );
                                hedge_failed = true;
                                continue;
                            }
                            let spread = self.order_spread_param(limit, false);
                            if let Err(e) = self
                                .connector
                                .create_order(
                                    &leg.symbol,
                                    filled,
                                    hedge_side,
                                    limit,
                                    spread,
                                    true,
                                    None,
                                )
                                .await
                            {
                                log::error!(
                                    "[ORDER] Failed to hedge partial entry {} ({}): {:?}",
                                    leg.symbol,
                                    leg.order_id,
                                    e
                                );
                                hedge_failed = true;
                            } else {
                                flattened_any = true;
                                let mode = if use_market { "MARKET" } else { "LIMIT" };
                                log::warn!(
                                    "[ORDER] Hedged partial entry on {} size={} mode={} retries={}",
                                    leg.symbol,
                                    filled,
                                    mode,
                                    retry_count
                                );
                            }
                        } else {
                            log::warn!(
                                "[ORDER] Missing price map entry for hedge {} leg {}",
                                leg.symbol,
                                leg.order_id
                            );
                            hedge_failed = true;
                        }
                    }
                }
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    if hedge_failed {
                        retry_count = retry_count.saturating_add(1);
                        pending.hedge_retry_count = retry_count;
                        log::warn!(
                            "[ORDER] Hedge retry scheduled for {} (retry {} of {})",
                            key,
                            retry_count,
                            max_retries
                        );
                        pending.placed_at = Instant::now();
                        state.pending_entry = Some(pending);
                    } else {
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                        state.pending_entry = None;
                        if flattened_any {
                            state.position = None;
                        }
                    }
                }
            } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                state.pending_entry = Some(pending);
            }
        }

        if let Some(pending) = pending_exit {
            let status = self.pending_status(&pending).await?;
            let mut pending = pending;
            self.update_pending_fills(&mut pending, &status.fills);
            let filled_qtys = self.filled_by_leg(&pending, &status.fills);
            let mut pnl_record: Option<(PnlLogRecord, f64)> = None;
            if status.open_remaining == 0 && self.all_filled(&pending, &status.fills) {
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    if let Some(pos) = state.position.as_ref() {
                        if let Some((base, quote)) = key.split_once('/') {
                            if let (Some(p1), Some(p2)) =
                                (price_map.get(base), price_map.get(quote))
                            {
                                if let Some(pnl) =
                                    compute_pnl(pos, p1.price, p2.price).and_then(|p| p.to_f64())
                                {
                                    let hold_secs = Some(
                                        now_ts.saturating_sub(pos.entered_ts).max(0) as f64,
                                    );
                                    let entry_a = pos.entry_price_a.and_then(|v| v.to_f64());
                                    let entry_b = pos.entry_price_b.and_then(|v| v.to_f64());
                                    let z_exit = state.z_score().map(|(z, _)| z);
                                    let beta_val = Some(state.beta);
                                    pnl_record = Some((
                                        PnlLogRecord::new(
                                            base,
                                            quote,
                                            pos.direction,
                                            pnl,
                                            now_ts,
                                            "exit_fill",
                                        ).with_trade_details(
                                            entry_a, entry_b,
                                            p1.price.to_f64(), p2.price.to_f64(),
                                            beta_val,
                                            pos.entry_z,
                                            z_exit,
                                            hold_secs,
                                        ),
                                        pnl,
                                    ));
                                }
                            }
                        }
                    }
                    state.position = None;
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                    state.pending_exit = None;
                }
                log::info!("[ORDER] {} exit orders filled", key);
                if let Some((record, pnl_value)) = pnl_record {
                    self.write_pnl_record(inst_idx, record);
                    if pnl_value < 0.0 {
                        self.instances[inst_idx].consecutive_losses += 1;
                        if let Some(cooldown) = self
                            .cfg
                            .circuit_breaker_cooldown_for(self.instances[inst_idx].consecutive_losses)
                        {
                            self.instances[inst_idx].circuit_breaker_until = Some(Instant::now() + cooldown);
                            self.instances[inst_idx].circuit_breaker_until_ts =
                                Some(now_ts + cooldown.as_secs() as i64);
                            log::warn!(
                                "[CIRCUIT_BREAKER] activated after {} consecutive losses, cooldown {}s",
                                self.instances[inst_idx].consecutive_losses, cooldown.as_secs()
                            );
                        }
                    } else if pnl_value > 0.0 {
                        if self.instances[inst_idx].consecutive_losses > 0 {
                            log::info!(
                                "[CIRCUIT_BREAKER] reset after win (was {} consecutive losses)",
                                self.instances[inst_idx].consecutive_losses
                            );
                        }
                        self.instances[inst_idx].consecutive_losses = 0;
                        self.instances[inst_idx].circuit_breaker_until = None;
                        self.instances[inst_idx].circuit_breaker_until_ts = None;
                    }
                }
            } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                if next_retry > MAX_EXIT_RETRIES {
                    self.force_close_all_positions(key, "partial_fill").await;
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_exit = None;
                    }
                    return Ok(());
                }
                log::info!(
                    "[ORDER] {} exit leg partially filled, reissuing remaining legs",
                    key
                );
                self.cancel_pending_orders(&pending).await?;
                if let Some(new_pending) = self
                    .reissue_partial_legs(&pending, &filled_qtys, price_map, true, true, next_retry)
                    .await?
                {
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_exit = Some(new_pending);
                    }
                } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_exit = None;
                }
                return Ok(());
            } else if pending.placed_at.elapsed() >= timeout || status.open_remaining == 0 {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                if next_retry > MAX_EXIT_RETRIES {
                    self.force_close_all_positions(key, "timeout").await;
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_exit = None;
                    }
                    return Ok(());
                }
                if status.open_remaining > 0 {
                    log::warn!(
                        "[ORDER] {} exit orders stale ({}s), cancelling {} legs",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        status.open_remaining
                    );
                    for leg in &pending.legs {
                        let filled = filled_qtys
                            .get(&leg.order_id)
                            .cloned()
                            .unwrap_or(Decimal::ZERO);
                        let is_open = status.open_ids.contains(&leg.order_id);
                        log::debug!(
                            "[ORDER] {} exit leg status symbol={} order_id={} target={} filled={} open={}",
                            key,
                            leg.symbol,
                            leg.order_id,
                            leg.target,
                            filled,
                            is_open
                        );
                    }
                    self.cancel_pending_orders(&pending).await?;
                }
                // Re-attempt closing missing legs based on filled qty
                // reusing filled_qtys defined earlier
                let mut new_legs = Vec::new();
                for leg in &pending.legs {
                    let filled = filled_qtys
                        .get(&leg.order_id)
                        .cloned()
                        .unwrap_or(Decimal::ZERO);
                    let remaining_qty = (leg.target - filled).max(Decimal::ZERO);
                    if remaining_qty > Decimal::ZERO {
                        let quantized =
                            self.quantize_order_size_exit(&leg.symbol, remaining_qty, price_map);
                        if quantized <= Decimal::ZERO {
                            continue;
                        }
                        let limit = None;
                        match self
                            .connector
                            .create_order(&leg.symbol, quantized, leg.side, limit, None, true, None)
                            .await
                        {
                            Ok(resp) => {
                                new_legs.push(PendingLeg {
                                    symbol: leg.symbol.clone(),
                                    order_id: resp.order_id,
                                    exchange_order_id: resp.exchange_order_id,
                                    target: quantized,
                                    filled: Decimal::ZERO,
                                    side: leg.side,
                                    limit_price: None,
                                });
                                log::warn!(
                                    "[ORDER] Retrying exit leg {} size={} mode=MARKET",
                                    leg.symbol,
                                    quantized
                                );
                            }
                            Err(e) => log::error!(
                                "[ORDER] Failed to retry exit leg {}: {:?}",
                                leg.symbol,
                                e
                            ),
                        }
                    }
                }
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    if new_legs.is_empty() {
                        state.pending_exit = None;
                        // Keep position state unchanged; will retry next loop
                    } else {
                        state.pending_exit = Some(PendingOrders {
                            legs: new_legs,
                            direction: pending.direction,
                            placed_at: Instant::now(),
                            hedge_retry_count: next_retry,
                            post_only_hybrid: false,
                        });
                    }
                }
            } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                state.pending_exit = Some(pending);
            }
        }

        Ok(())
    }

    async fn cancel_pending_orders(&self, pending: &PendingOrders) -> Result<()> {
        let mut by_symbol: HashMap<String, Vec<String>> = HashMap::new();
        for leg in &pending.legs {
            by_symbol
                .entry(leg.symbol.clone())
                .or_default()
                .push(leg.order_id.clone());
        }
        for (symbol, order_ids) in by_symbol {
            if let Err(e) = self
                .connector
                .cancel_orders(Some(symbol.clone()), order_ids.clone())
                .await
            {
                log::error!(
                    "[ORDER] cancel failed for {} ({} ids): {:?}",
                    symbol,
                    order_ids.len(),
                    e
                );
            }
        }
        Ok(())
    }

    async fn pending_status(&self, pending: &PendingOrders) -> Result<PendingStatus> {
        let mut open_remaining = 0;
        let mut fills: HashMap<String, Decimal> = HashMap::new();
        let mut open_ids: HashSet<String> = HashSet::new();
        let mut per_symbol_open: HashMap<String, HashSet<String>> = HashMap::new();
        let mut per_symbol_fill: HashMap<String, HashSet<String>> = HashMap::new();
        for leg in &pending.legs {
            per_symbol_open
                .entry(leg.symbol.clone())
                .or_default()
                .insert(leg.order_id.clone());
            let fill_ids = per_symbol_fill.entry(leg.symbol.clone()).or_default();
            fill_ids.insert(leg.order_id.clone());
            if let Some(exchange_id) = &leg.exchange_order_id {
                fill_ids.insert(exchange_id.clone());
            }
        }
        for (symbol, open_ids_filter) in per_symbol_open.iter() {
            let fill_ids_filter = per_symbol_fill.get(symbol).cloned().unwrap_or_default();
            let open = self
                .connector
                .get_open_orders(symbol)
                .await
                .with_context(|| format!("open orders {}", symbol))?;
            let mut open_count = 0;
            for order in open
                .orders
                .iter()
                .filter(|o| open_ids_filter.contains(&o.order_id))
            {
                open_ids.insert(order.order_id.clone());
                open_count += 1;
            }
            open_remaining += open_count;

            let filled = self
                .connector
                .get_filled_orders(symbol)
                .await
                .with_context(|| format!("filled orders {}", symbol))?;
            for order in filled.orders {
                if fill_ids_filter.contains(&order.order_id) {
                    let sz = order.filled_size.unwrap_or(Decimal::ZERO);
                    *fills.entry(order.order_id.clone()).or_default() += sz;
                    log::debug!(
                        "[ORDER][FILLED] symbol={} order_id={} side={:?} size={} value={:?} fee={:?} trade_id={}",
                        symbol,
                        order.order_id,
                        order.filled_side,
                        sz,
                        order.filled_value,
                        order.filled_fee,
                        order.trade_id
                    );
                }
            }
            log::debug!(
                "[ORDER][PENDING_STATUS] symbol={} open_orders={} tracked_orders={} filled_entries={}",
                symbol,
                open_count,
                open_ids_filter.len(),
                fills.len()
            );
        }
        Ok(PendingStatus {
            open_remaining,
            fills,
            open_ids,
        })
    }

    fn leg_fill_from_map(&self, leg: &PendingLeg, fills: &HashMap<String, Decimal>) -> Decimal {
        fills
            .get(&leg.order_id)
            .cloned()
            .or_else(|| {
                leg.exchange_order_id
                    .as_ref()
                    .and_then(|id| fills.get(id).cloned())
            })
            .unwrap_or(Decimal::ZERO)
    }

    fn update_pending_fills(&self, pending: &mut PendingOrders, fills: &HashMap<String, Decimal>) {
        for leg in &mut pending.legs {
            let filled = self.leg_fill_from_map(leg, fills);
            if filled > leg.filled {
                leg.filled = filled.min(leg.target);
            }
        }
    }

    fn filled_for_leg(&self, leg: &PendingLeg, fills: &HashMap<String, Decimal>) -> Decimal {
        let filled = self.leg_fill_from_map(leg, fills);
        filled.max(leg.filled).min(leg.target)
    }

    fn filled_by_leg(
        &self,
        pending: &PendingOrders,
        fills: &HashMap<String, Decimal>,
    ) -> HashMap<String, Decimal> {
        let mut map = HashMap::new();
        for leg in &pending.legs {
            let filled = self.filled_for_leg(leg, fills);
            map.insert(leg.order_id.clone(), filled);
        }
        map
    }

    fn all_filled(&self, pending: &PendingOrders, fills: &HashMap<String, Decimal>) -> bool {
        pending
            .legs
            .iter()
            .all(|leg| self.filled_for_leg(leg, fills) >= leg.target)
    }

    fn evaluate_pair(&self, pair: &PairSpec) -> Option<PairEvaluation> {
        pair_eval::evaluate_pair(&self.cfg, &self.history, pair)
    }

    fn exit_sizes_for_pair(
        &self,
        inst_idx: usize,
        key: &str,
        pair: &PairSpec,
        beta: f64,
        p1: &SymbolSnapshot,
        p2: &SymbolSnapshot,
    ) -> Result<(Decimal, Decimal)> {
        let base_snapshot = self.open_positions.get(&pair.base);
        let quote_snapshot = self.open_positions.get(&pair.quote);
        if base_snapshot.is_some() || quote_snapshot.is_some() {
            let qty_a = base_snapshot.map(|p| p.size).unwrap_or(Decimal::ZERO);
            let qty_b = quote_snapshot.map(|p| p.size).unwrap_or(Decimal::ZERO);
            return Ok((qty_a, qty_b));
        }

        let mut qty_a = Decimal::ZERO;
        let mut qty_b = Decimal::ZERO;
        if let Some(state) = self.instances[inst_idx].states.get(key).and_then(|s| s.position.as_ref()) {
            qty_a = state.entry_size_a.unwrap_or(Decimal::ZERO);
            qty_b = state.entry_size_b.unwrap_or(Decimal::ZERO);
        }

        if qty_a <= Decimal::ZERO && qty_b <= Decimal::ZERO {
            log::warn!(
                "[EXIT] {} missing position sizes from exchange/state; falling back to hedge sizing",
                key
            );
            return self.hedged_sizes(inst_idx, pair, beta, p1, p2);
        }

        Ok((qty_a, qty_b))
    }

    fn hedged_sizes(
        &self,
        inst_idx: usize,
        _pair: &PairSpec,
        beta: f64,
        p1: &SymbolSnapshot,
        p2: &SymbolSnapshot,
    ) -> Result<(Decimal, Decimal)> {
        let inst = &self.instances[inst_idx];
        let equity = inst.equity_cache.max(inst.equity_usd_fallback);
        sizing::hedged_sizes(&self.cfg, equity, beta, p1, p2)
    }

    fn post_only_supported(&self) -> bool {
        let dex = self.cfg.dex_name.to_ascii_lowercase();
        dex.contains("lighter") || dex.contains("extended")
    }

    fn should_post_only(&self) -> bool {
        self.cfg.fee_bps > 0.0 && self.post_only_supported()
    }

    fn order_reference_price_from_snapshot(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Decimal {
        let use_book = self.cfg.slippage_bps < 0 || self.should_post_only();
        if use_book {
            let side_price = match side {
                dex_connector::OrderSide::Long => snapshot.ask_price,
                dex_connector::OrderSide::Short => snapshot.bid_price,
            };
            if side_price.is_none() {
                log::debug!(
                    "[ORDER] {} missing top-of-book price; using ticker price",
                    symbol
                );
            }
            return side_price.unwrap_or(snapshot.price);
        }
        snapshot.price
    }

    fn order_reference_price(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        Some(self.order_reference_price_from_snapshot(symbol, side, snapshot))
    }

    fn limit_price_for(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        let reference = self.order_reference_price_from_snapshot(symbol, side, snapshot);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot(symbol, adjusted, side, snapshot))
    }

    fn limit_price_for_snapshot(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Option<Decimal> {
        let reference = self.order_reference_price_from_snapshot(symbol, side, snapshot);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot(symbol, adjusted, side, snapshot))
    }

    async fn refreshed_limit_price(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        match self.refresh_symbol_snapshot(symbol).await {
            Ok(snapshot) => self.limit_price_for_snapshot(symbol, side, &snapshot),
            Err(err) => {
                log::debug!(
                    "[ORDER] Failed to refresh price snapshot for {}: {:?}",
                    symbol,
                    err
                );
                self.limit_price_for(symbol, side, prices)
            }
        }
    }

    async fn refresh_symbol_snapshot(&mut self, symbol: &str) -> Result<SymbolSnapshot> {
        let ticker = self
            .connector
            .get_ticker(symbol, None)
            .await
            .with_context(|| format!("ticker {}", symbol))?;
        let (bid_price, ask_price, bid_size, ask_size) =
            match self.connector.get_order_book(symbol, 1).await {
                Ok(ob) => (
                    ob.bids.first().map(|l| l.price),
                    ob.asks.first().map(|l| l.price),
                    ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                ),
                Err(err) => {
                    log::debug!(
                        "[ORDER] orderbook {} unavailable during retry: {:?}",
                        symbol,
                        err
                    );
                    (None, None, Decimal::ZERO, Decimal::ZERO)
                }
            };
        Ok(SymbolSnapshot {
            price: ticker.price,
            funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            min_order: ticker.min_order,
            min_tick: ticker.min_tick,
            size_decimals: ticker.size_decimals,
            exchange_ts: ticker.exchange_ts.map(|v| v as i64),
        })
    }

    fn order_spread_param(&self, limit: Option<Decimal>, allow_post_only: bool) -> Option<i64> {
        if allow_post_only && limit.is_some() && self.should_post_only() {
            Some(-2)
        } else {
            None
        }
    }

    fn apply_slippage(
        &self,
        price: Option<Decimal>,
        side: dex_connector::OrderSide,
    ) -> Option<Decimal> {
        order_pricing::apply_slippage(self.cfg.slippage_bps, price, side)
    }

    fn quantize_order_size(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size(symbol, size, prices)
    }

    fn quantize_order_size_exit(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size_exit(symbol, size, prices)
    }

    fn quantize_order_size_close(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size_close(symbol, size, prices)
    }

    fn quantize_order_price_with_snapshot(
        &mut self,
        symbol: &str,
        price: Decimal,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Decimal {
        let mut effective_tick_size = snapshot.min_tick;

        // Extended occasionally returns markets without `min_tick` populated
        // in the snapshot (dex-connector fills this from the markets cache,
        // which may lag a reconnect). Fall back to tick=1 so we don't spam
        // the "No min tick" warning every cycle.
        if effective_tick_size.is_none() && self.cfg.dex_name.contains("extended") {
            effective_tick_size = Some(Decimal::ONE);
        }

        let Some(tick_size) = effective_tick_size else {
            if !self.min_tick_warned.contains(symbol) {
                log::warn!(
                    "[ORDER] No min tick for {}; price rounding disabled",
                    symbol
                );

                self.min_tick_warned.insert(symbol.to_string());
            }

            return price;
        };

        if tick_size <= Decimal::ZERO {
            return price;
        }

        round_price_by_tick(price, tick_size, side)
    }

    async fn create_order_with_post_only_retry(
        &mut self,
        symbol: &str,
        size: Decimal,
        side: dex_connector::OrderSide,
        reduce_only: bool,
        prices: &HashMap<String, SymbolSnapshot>,
        allow_post_only: bool,
        max_post_only_attempts: usize,
        fallback_to_taker: bool,
    ) -> Result<dex_connector::CreateOrderResponse, DexError> {
        let use_post_only = allow_post_only && self.should_post_only();
        let max_attempts = max_post_only_attempts.max(1);
        let max_elapsed = Duration::from_millis(POST_ONLY_RETRY_MAX_ELAPSED_MS);
        let start = Instant::now();
        let mut attempt = 0usize;

        let last_err = loop {
            attempt += 1;
            let limit = if use_post_only {
                self.refreshed_limit_price(symbol, side, prices).await
            } else {
                self.limit_price_for(symbol, side, prices)
            };
            if use_post_only && limit.is_none() {
                return Err(DexError::Other(format!(
                    "[ORDER] Missing reference price for post-only {}",
                    symbol
                )));
            }
            let spread = self.order_spread_param(limit, use_post_only);
            match self
                .connector
                .create_order(symbol, size, side, limit, spread, reduce_only, None)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    if !use_post_only {
                        return Err(err);
                    }
                    if attempt >= max_attempts || start.elapsed() >= max_elapsed {
                        break err;
                    }
                }
            }

            log::info!(
                "[ORDER] {} post-only attempt {} failed; retrying",
                symbol,
                attempt
            );
            sleep(Duration::from_millis(POST_ONLY_RETRY_DELAY_MS)).await;
        };

        if use_post_only && fallback_to_taker {
            log::warn!(
                "[ORDER] {} post-only attempts exhausted; falling back to taker",
                symbol
            );
            return self
                .connector
                .create_order(symbol, size, side, None, None, reduce_only, None)
                .await;
        }

        Err(last_err)
    }

    async fn place_pair_orders(
        &mut self,
        inst_idx: usize,
        pair: &PairSpec,
        direction: PositionDirection,
        qtys: (Decimal, Decimal),
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Result<Vec<PendingLeg>> {
        let (side_a, side_b) = match direction {
            PositionDirection::LongSpread => (
                dex_connector::OrderSide::Long,
                dex_connector::OrderSide::Short,
            ),
            PositionDirection::ShortSpread => (
                dex_connector::OrderSide::Short,
                dex_connector::OrderSide::Long,
            ),
        };
        let ref_price_a = self.order_reference_price(&pair.base, side_a, prices);
        let ref_price_b = self.order_reference_price(&pair.quote, side_b, prices);
        let qty_a = self.quantize_order_size_exit(&pair.base, qtys.0, prices);
        let qty_b = self.quantize_order_size_exit(&pair.quote, qtys.1, prices);
        if qty_a != qtys.0 {
            log::debug!(
                "[ORDER_ADJUST][ENTRY] {} settled qty_a {} -> {}",
                pair.base,
                qtys.0,
                qty_a
            );
        }
        if qty_b != qtys.1 {
            log::debug!(
                "[ORDER_ADJUST][ENTRY] {} settled qty_b {} -> {}",
                pair.quote,
                qtys.1,
                qty_b
            );
        }
        // Check hedge ratio deviation after size rounding
        let pair_key_for_dev = format!("{}/{}", pair.base, pair.quote);
        let pp_for_dev = self.pair_params_for(inst_idx, &pair_key_for_dev).clone();
        let pp_for_dev = &pp_for_dev;
        if pp_for_dev.hedge_ratio_max_deviation < 1.0 {
            let dev_a = if qtys.0.is_zero() {
                0.0
            } else {
                ((qty_a / qtys.0) - Decimal::ONE).abs().to_f64().unwrap_or(0.0)
            };
            let dev_b = if qtys.1.is_zero() {
                0.0
            } else {
                ((qty_b / qtys.1) - Decimal::ONE).abs().to_f64().unwrap_or(0.0)
            };
            let max_dev = dev_a.max(dev_b);
            if max_dev > pp_for_dev.hedge_ratio_max_deviation {
                log::info!(
                    "[ORDER_ADJUST][ENTRY] {}/{} BLOCKED: size rounding deviation {:.1}% exceeds limit {:.1}%",
                    pair.base, pair.quote, max_dev * 100.0, pp_for_dev.hedge_ratio_max_deviation * 100.0
                );
                return Ok(Vec::new());
            }
        }
        let limit_a = self.limit_price_for(&pair.base, side_a, prices);
        let limit_b = self.limit_price_for(&pair.quote, side_b, prices);
        let pair_key_for_hybrid = format!("{}/{}", pair.base, pair.quote);
        let pp_for_hybrid = self.pair_params_for(inst_idx, &pair_key_for_hybrid).clone();
        let pp_for_hybrid = &pp_for_hybrid;
        let hybrid_active =
            pp_for_hybrid.entry_post_only_timeout_secs > 0 && self.post_only_supported();
        let post_only = self.should_post_only();
        let entry_attempts = if hybrid_active {
            1
        } else {
            POST_ONLY_ENTRY_ATTEMPTS
        };
        log::debug!(
            "[ORDER_PARAMS][ENTRY] pair={}/{} side_a={:?} qty_a={} ref_price_a={} limit_a={:?} side_b={:?} qty_b={} ref_price_b={} limit_b={:?} post_only={} hybrid={}",
            pair.base,
            pair.quote,
            side_a,
            qty_a,
            ref_price_a.unwrap_or(Decimal::ZERO),
            limit_a,
            side_b,
            qty_b,
            ref_price_b.unwrap_or(Decimal::ZERO),
            limit_b,
            post_only,
            hybrid_active
        );
        let mut legs: Vec<PendingLeg> = Vec::new();
        let res_a = self
            .create_order_with_post_only_retry(
                &pair.base,
                qty_a,
                side_a,
                false,
                prices,
                true,
                entry_attempts,
                false,
            )
            .await
            .context("place leg A")?;
        let target_a = if res_a.ordered_size > Decimal::ZERO {
            if res_a.ordered_size != qtys.0 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.base,
                    qtys.0,
                    res_a.ordered_size
                );
            }
            res_a.ordered_size
        } else {
            qtys.0
        };
        legs.push(PendingLeg {
            symbol: pair.base.clone(),
            order_id: res_a.order_id.clone(),
            exchange_order_id: res_a.exchange_order_id.clone(),
            target: target_a,
            filled: Decimal::ZERO,
            side: side_a,
            limit_price: limit_a,
        });

        let res_b = match self
            .create_order_with_post_only_retry(
                &pair.quote,
                qty_b,
                side_b,
                false,
                prices,
                true,
                entry_attempts,
                false,
            )
            .await
        {
            Ok(res) => res,
            Err(e) => {
                self.recover_from_leg_b_failure(pair, &res_a, side_a, &e).await;
                return Err(PartialOrderPlacementError::new(legs.clone(), e).into());
            }
        };
        let target_b = if res_b.ordered_size > Decimal::ZERO {
            if res_b.ordered_size != qtys.1 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.quote,
                    qtys.1,
                    res_b.ordered_size
                );
            }
            res_b.ordered_size
        } else {
            qtys.1
        };
        legs.push(PendingLeg {
            symbol: pair.quote.clone(),
            order_id: res_b.order_id.clone(),
            exchange_order_id: res_b.exchange_order_id.clone(),
            target: target_b,
            filled: Decimal::ZERO,
            side: side_b,
            limit_price: limit_b,
        });
        Ok(legs)
    }

    /// Recovery path when leg B placement fails after leg A succeeded:
    /// cancel leg A, wait briefly, check whether the exchange filled it
    /// anyway, and if so submit a market reduce-only order in the opposite
    /// direction to neutralize the unhedged exposure. All errors here are
    /// logged but not propagated — the caller still surfaces the original
    /// leg-B failure.
    async fn recover_from_leg_b_failure(
        &self,
        pair: &PairSpec,
        res_a: &dex_connector::CreateOrderResponse,
        side_a: dex_connector::OrderSide,
        leg_b_err: &DexError,
    ) {
        log::error!(
            "[ORDER] Failed to place leg B for {}/{} (leg A={}): {:?}",
            pair.base,
            pair.quote,
            res_a.order_id,
            leg_b_err
        );

        // Attempt to cancel leg A, but proceed even if it fails.
        if let Err(cancel_err) = self
            .connector
            .cancel_order(&pair.base, &res_a.order_id)
            .await
        {
            log::warn!(
                "[SAFETY] Failed to cancel leg A {} after leg B failed: {:?}",
                res_a.order_id,
                cancel_err
            );
        } else {
            log::info!(
                "[SAFETY] Canceled leg A {} after leg B failed.",
                res_a.order_id
            );
        }

        // Give the exchange time to settle any concurrent fill.
        sleep(Duration::from_secs(5)).await;

        let filled_orders = match self.connector.get_filled_orders(&pair.base).await {
            Ok(orders) => orders,
            Err(e) => {
                log::error!(
                    "[SAFETY] Could not check for filled orders for {}: {:?}",
                    pair.base,
                    e
                );
                return;
            }
        };

        let matches_order = |order_id: &str| {
            order_id == res_a.order_id
                || res_a
                    .exchange_order_id
                    .as_ref()
                    .map_or(false, |id| order_id == id)
        };
        let Some(filled_order) = filled_orders
            .orders
            .iter()
            .find(|o| matches_order(&o.order_id))
        else {
            return;
        };
        let filled_size = filled_order.filled_size.unwrap_or(Decimal::ZERO);
        if filled_size <= Decimal::ZERO {
            return;
        }

        log::warn!(
            "[SAFETY] Leg A {} was filled for {}. Hedging immediately.",
            res_a.order_id,
            pair.base
        );
        let hedge_side = match side_a {
            dex_connector::OrderSide::Long => dex_connector::OrderSide::Short,
            dex_connector::OrderSide::Short => dex_connector::OrderSide::Long,
        };
        if let Err(hedge_err) = self
            .connector
            .create_order(&pair.base, filled_size, hedge_side, None, None, true, None)
            .await
        {
            log::error!(
                "[SAFETY] FAILED TO HEDGE partial fill for {}: {:?}",
                pair.base,
                hedge_err
            );
        } else {
            log::info!(
                "[SAFETY] Successfully submitted hedge order for partial fill on {}",
                pair.base
            );
        }
    }

    async fn close_pair_orders(
        &mut self,
        pair: &PairSpec,
        direction: PositionDirection,
        qtys: (Decimal, Decimal),
        prices: &HashMap<String, SymbolSnapshot>,
        use_market: bool,
    ) -> Result<Vec<PendingLeg>> {
        let (side_a, side_b) = match direction {
            PositionDirection::LongSpread => (
                dex_connector::OrderSide::Short,
                dex_connector::OrderSide::Long,
            ),
            PositionDirection::ShortSpread => (
                dex_connector::OrderSide::Long,
                dex_connector::OrderSide::Short,
            ),
        };
        let ref_price_a = self.order_reference_price(&pair.base, side_a, prices);
        let ref_price_b = self.order_reference_price(&pair.quote, side_b, prices);
        let qty_a = self.quantize_order_size_close(&pair.base, qtys.0, prices);
        let qty_b = self.quantize_order_size_close(&pair.quote, qtys.1, prices);
        if qty_a != qtys.0 {
            log::debug!(
                "[ORDER_ADJUST][EXIT] {} settled qty_a {} -> {}",
                pair.base,
                qtys.0,
                qty_a
            );
        }
        if qty_b != qtys.1 {
            log::debug!(
                "[ORDER_ADJUST][EXIT] {} settled qty_b {} -> {}",
                pair.quote,
                qtys.1,
                qty_b
            );
        }
        let limit_a = if use_market {
            None
        } else {
            self.limit_price_for(&pair.base, side_a, prices)
        };
        let limit_b = if use_market {
            None
        } else {
            self.limit_price_for(&pair.quote, side_b, prices)
        };
        let post_only = !use_market && self.should_post_only();
        log::debug!(
            "[ORDER_PARAMS][EXIT] pair={}/{} side_a={:?} qty_a={} ref_price_a={} limit_a={:?} side_b={:?} qty_b={} ref_price_b={} limit_b={:?} post_only={}",
            pair.base,
            pair.quote,
            side_a,
            qty_a,
            ref_price_a.unwrap_or(Decimal::ZERO),
            limit_a,
            side_b,
            qty_b,
            ref_price_b.unwrap_or(Decimal::ZERO),
            limit_b,
            post_only
        );
        let mut legs: Vec<PendingLeg> = Vec::new();
        let mut res_a = None;
        if qty_a > Decimal::ZERO {
            let res = if use_market {
                self.connector
                    .create_order(&pair.base, qty_a, side_a, None, None, true, None)
                    .await
            } else {
                self.create_order_with_post_only_retry(
                    &pair.base,
                    qty_a,
                    side_a,
                    true,
                    prices,
                    true,
                    POST_ONLY_EXIT_ATTEMPTS,
                    true,
                )
                .await
            };
            match res {
                Ok(res) => {
                    if res.ordered_size > Decimal::ZERO && res.ordered_size != qty_a {
                        log::debug!(
                            "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                            pair.base,
                            qty_a,
                            res.ordered_size
                        );
                    }
                    legs.push(PendingLeg {
                        symbol: pair.base.clone(),
                        order_id: res.order_id.clone(),
                        exchange_order_id: res.exchange_order_id.clone(),
                        target: qty_a,
                        filled: Decimal::ZERO,
                        side: side_a,
                        limit_price: None,
                    });
                    res_a = Some(res);
                }
                Err(err) => {
                    if Self::is_reduce_only_position_missing_error(&err) {
                        let symbol = pair.base.clone();
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} reduce-only close skipped; position already closed",
                                symbol
                            );
                        } else {
                            return Err(err).context("close leg A");
                        }
                    } else {
                        return Err(err).context("close leg A");
                    }
                }
            }
        }

        if qty_b > Decimal::ZERO {
            let res_b = if use_market {
                self.connector
                    .create_order(&pair.quote, qty_b, side_b, None, None, true, None)
                    .await
            } else {
                self.create_order_with_post_only_retry(
                    &pair.quote,
                    qty_b,
                    side_b,
                    true,
                    prices,
                    true,
                    POST_ONLY_EXIT_ATTEMPTS,
                    true,
                )
                .await
            };
            let res_b = match res_b {
                Ok(res) => Some(res),
                Err(e) => {
                    let mut skip = false;
                    if Self::is_reduce_only_position_missing_error(&e) {
                        let symbol = pair.quote.clone();
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} reduce-only close skipped; position already closed",
                                symbol
                            );
                            skip = true;
                        }
                    }
                    if skip {
                        None
                    } else {
                        if let Some(ref res_a) = res_a {
                            self.recover_from_leg_b_failure(pair, res_a, side_a, &e).await;
                        } else {
                            log::error!(
                                "[ORDER] Failed to close leg B for {}/{}: {:?}",
                                pair.base,
                                pair.quote,
                                e
                            );
                        }

                        return Err(PartialOrderPlacementError::new(legs.clone(), e).into());
                    }
                }
            };
            if let Some(res_b) = res_b {
                if res_b.ordered_size > Decimal::ZERO && res_b.ordered_size != qty_b {
                    log::debug!(
                        "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                        pair.quote,
                        qty_b,
                        res_b.ordered_size
                    );
                }
                legs.push(PendingLeg {
                    symbol: pair.quote.clone(),
                    order_id: res_b.order_id.clone(),
                    exchange_order_id: res_b.exchange_order_id.clone(),
                    target: qty_b,
                    filled: Decimal::ZERO,
                    side: side_b,
                    limit_price: None,
                });
            }
        }

        if legs.is_empty() {
            log::warn!(
                "[ORDER] No exit legs placed for {}/{} (qty_a={}, qty_b={})",
                pair.base,
                pair.quote,
                qty_a,
                qty_b
            );
        }
        Ok(legs)
    }

    fn register_partial_leg_failure(
        &mut self,
        inst_idx: usize,
        key: &str,
        direction: PositionDirection,
        err: &anyhow::Error,
        is_exit: bool,
    ) {
        if let Some(partial) = err.downcast_ref::<PartialOrderPlacementError>() {
            if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                let pending = PendingOrders {
                    legs: partial.legs().to_vec(),
                    direction,
                    placed_at: Instant::now(),
                    hedge_retry_count: 0,
                    post_only_hybrid: false,
                };
                if is_exit {
                    state.pending_exit = Some(pending);
                } else {
                    state.pending_entry = Some(pending);
                }
            }
        }
    }

    async fn fetch_latest_prices(&mut self) -> Result<HashMap<String, SymbolSnapshot>> {
        let symbols: Vec<String> = self
            .cfg
            .universe
            .iter()
            .flat_map(|p| [p.base.clone(), p.quote.clone()])
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let connector = self.connector.clone();
        let mut join_set = tokio::task::JoinSet::new();
        for sym in symbols.iter().cloned() {
            let conn = connector.clone();
            join_set.spawn(async move {
                let (ticker_res, ob_res) = tokio::join!(
                    conn.get_ticker(&sym, None),
                    conn.get_order_book(&sym, 1),
                );
                (sym, ticker_res, ob_res)
            });
        }
        let mut results = Vec::new();
        while let Some(res) = join_set.join_next().await {
            results.push(res.expect("fetch task panicked"));
        }

        let mut map = HashMap::new();
        for (symbol, ticker_res, ob_res) in results {
            let ticker = match ticker_res {
                Ok(ticker) => ticker,
                Err(e) => {
                    let msg = e.to_string();
                    if Self::is_ticker_auth_error(&msg) {
                        if self.should_log_ticker_warn(&symbol) {
                            log::warn!("ticker {} unavailable: {}", symbol, msg);
                            self.last_ticker_warn.insert(symbol.clone(), Instant::now());
                        } else {
                            log::debug!("ticker {} unavailable: {}", symbol, msg);
                        }
                        continue;
                    }
                    return Err(e).with_context(|| format!("ticker {}", symbol));
                }
            };
            let (top_bid_price, top_ask_price, top_bid_size, top_ask_size) = match ob_res {
                Ok(ob) => (
                    ob.bids.first().map(|l| l.price),
                    ob.asks.first().map(|l| l.price),
                    ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                ),
                Err(e) => {
                    let msg = format!("{:?}", e);
                    let is_stale = msg.contains("order book snapshot unavailable");
                    if is_stale {
                        log::debug!("orderbook {} unavailable: {}", symbol, msg);
                    } else if self.should_log_ob_warn(&symbol) {
                        log::warn!("orderbook {} unavailable: {}", symbol, msg);
                        self.last_ob_warn.insert(symbol.clone(), Instant::now());
                    } else {
                        log::debug!("orderbook {} unavailable: {}", symbol, msg);
                    }
                    (None, None, Decimal::ZERO, Decimal::ZERO)
                }
            };
            if ticker.min_order.is_none() && !self.min_order_warned.contains(&symbol) {
                let size_decimals_desc = ticker
                    .size_decimals
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_order (size_decimals={}); using fallback step",
                    symbol,
                    size_decimals_desc
                );
                self.min_order_warned.insert(symbol.clone());
            }
            if ticker.min_tick.is_none() && !self.min_tick_warned.contains(&symbol) {
                let min_tick_desc = ticker
                    .min_tick
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_tick (ticker reports {}); price will be rounded with fallback",
                    symbol,
                    min_tick_desc
                );
                self.min_tick_warned.insert(symbol.clone());
            }
            map.insert(
                symbol.clone(),
                SymbolSnapshot {
                    price: ticker.price,
                    funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
                    bid_price: top_bid_price,
                    ask_price: top_ask_price,
                    bid_size: top_bid_size,
                    ask_size: top_ask_size,
                    min_order: ticker.min_order,
                    min_tick: ticker.min_tick,
                    size_decimals: ticker.size_decimals,
                    exchange_ts: ticker.exchange_ts.map(|v| v as i64),
                },
            );
            log::debug!(
                "[PRICE_SNAPSHOT] {} price={} bid={:?} ask={:?} bid_sz={} ask_sz={} min_order={:?} min_tick={:?}",
                symbol,
                ticker.price,
                top_bid_price,
                top_ask_price,
                top_bid_size,
                top_ask_size,
                ticker.min_order,
                ticker.min_tick
            );
        }
        Ok(map)
    }
}

#[cfg(test)]
impl PairTradeEngine {
    fn test_instance(connector: Arc<dyn DexConnector + Send + Sync>) -> Self {
        let cfg = PairTradeConfig {
            dex_name: "test".to_string(),
            rest_endpoint: "http://localhost".to_string(),
            web_socket_endpoint: "ws://localhost".to_string(),
            dry_run: true,
            agent_name: None,
            interval_secs: 1,
            trading_period_secs: 1,
            metrics_window: 1,
            net_funding_min_per_hour: 0.0,
            notional_per_leg: 1.0,
            risk_pct_per_trade: 0.01,
            equity_usd: DEFAULT_EQUITY_USD,
            universe: vec![PairSpec {
                base: "AAA".to_string(),
                quote: "BBB".to_string(),
            }],
            slippage_bps: 0,
            fee_bps: 0.0,
            max_leverage: 1.0,
            max_active_pairs: 1,
            warm_start_mode: WarmStartMode::Strict,
            order_timeout_secs: DEFAULT_ORDER_TIMEOUT_SECS,
            entry_partial_fill_max_retries: DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES,
            startup_force_close_attempts: DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS,
            startup_force_close_wait_secs: DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS,
            force_close_on_startup: false,
            enable_data_dump: false,
            data_dump_file: None,
            observe_only: false,
            disable_history_persist: true,
            history_file: "test-history.json".to_string(),
            history_archive_dir: None,
            history_archive_retention_days: 14,
            backtest_mode: false,
            backtest_file: None,
            bt_warm_start_snapshot: None,
            bt_eval_timestamps: None,
            bt_restart_timestamps: None,
            circuit_breaker_consecutive_losses: DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES,
            circuit_breaker_cooldown_secs: DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS,
            shutdown_grace_secs: 0,
            pair_params: HashMap::new(),
            default_pair_params: PairParams {
                entry_z_base: 2.0,
                entry_z_min: 1.8,
                entry_z_max: 2.3,
                exit_z: 0.5,
                stop_loss_z: 3.0,
                force_close_secs: 60,
                cooldown_secs: 1,
                max_loss_r_mult: 1.0,
                half_life_max_hours: 1.0,
                adf_p_threshold: 0.05,
                spread_velocity_max_sigma_per_min: 0.1,
                lookback_hours_short: 1,
                lookback_hours_long: 1,
                entry_vol_lookback_hours: 1,
                warm_start_min_bars: 1,
                hedge_ratio_max_deviation: 1.0,
                ..PairParams::default()
            },
            strategies: Vec::new(),
            use_kalman_beta: DEFAULT_USE_KALMAN_BETA,
            kalman_q: DEFAULT_KALMAN_Q,
            kalman_r: DEFAULT_KALMAN_R,
            kalman_initial_p: DEFAULT_KALMAN_INITIAL_P,
            kalman_min_updates: DEFAULT_KALMAN_MIN_UPDATES,
            regime_vol_window: DEFAULT_REGIME_VOL_WINDOW,
            regime_vol_max: DEFAULT_REGIME_VOL_MAX,
            regime_trend_window: DEFAULT_REGIME_TREND_WINDOW,
            regime_trend_max: DEFAULT_REGIME_TREND_MAX,
            regime_reference_symbol: DEFAULT_REGIME_REFERENCE_SYMBOL.to_string(),
            bt_fill_delay_secs: 0,
        };

        let history_path = PathBuf::from(cfg.history_file.as_str());

        Self {
            cfg,
            connector: connector.clone(),
            instances: vec![StrategyInstance {
                id: "default".to_string(),
                connector,
                equity_cache: DEFAULT_EQUITY_USD,
                last_equity_fetch: None,
                equity_usd_fallback: DEFAULT_EQUITY_USD,
                states: HashMap::new(),
                pnl_logger: None,
                status_reporter: None,
                consecutive_losses: 0,
                circuit_breaker_until: None,
                circuit_breaker_until_ts: None,
                total_trades: 0,
                total_wins: 0,
                total_pnl: 0.0,
                peak_pnl: 0.0,
                max_dd: 0.0,
                pair_params: HashMap::new(),
                default_pair_params: PairParams::default(),
            }],
            history: HashMap::new(),
            bar_builders: HashMap::new(),
            last_metrics_log: None,
            last_ob_warn: HashMap::new(),
            last_ticker_warn: HashMap::new(),
            last_position_warn: HashMap::new(),
            min_order_warned: HashSet::new(),
            min_tick_warned: HashSet::new(),
            positions_ready: false,
            open_positions: HashMap::new(),
            last_account_rest_call: None,
            history_path,
            data_dump_writer: None,
            replay_connector: None,
            shutdown_pending: false,
        }
    }
}


#[derive(Serialize)]
struct DataDumpEntry<'a> {
    timestamp: i64,
    prices: &'a HashMap<String, SymbolSnapshot>,
}




#[cfg(test)]
mod tests {
    use super::*;
    use super::util::{quantize_size_by_step, quantize_size_by_step_ceiling};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn round_price_by_tick_rounds_long_down() {
        let price = dec("100.123");
        let step = dec("0.01");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
        assert_eq!(quantized, dec("100.12"));
    }

    #[test]
    fn round_price_by_tick_rounds_short_up() {
        let price = dec("100.123");
        let step = dec("0.01");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Short);
        assert_eq!(quantized, dec("100.13"));
    }

    #[test]
    fn round_price_by_tick_enforces_minimum_step() {
        let price = dec("0.0001");
        let step = dec("0.005");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
        assert_eq!(quantized, step);
    }

    #[test]
    fn quantize_size_by_step_uses_size_decimals() {
        let size = dec("0.0023");
        let step = dec("0.001");
        let quantized = quantize_size_by_step(size, step, None);
        assert_eq!(quantized, dec("0.002"));
    }

    #[test]
    fn quantize_size_by_step_respects_min_order_floor() {
        let size = dec("0.0002");
        let step = dec("0.0001");
        let quantized = quantize_size_by_step(size, step, Some(dec("0.001")));
        assert_eq!(quantized, dec("0.001"));
    }

    #[test]
    fn quantize_size_by_step_ceiling_rounds_up() {
        let size = dec("0.0023");
        let step = dec("0.001");
        let quantized = quantize_size_by_step_ceiling(size, step, None);
        assert_eq!(quantized, dec("0.003"));
    }
}

#[cfg(test)]
mod pending_tests {
    use super::*;
    use async_trait::async_trait;
    use dex_connector::{
        BalanceResponse, CanceledOrdersResponse, CreateOrderResponse, DexConnector, DexError,
        FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse, OrderBookSnapshot, OrderSide,
        PositionSnapshot, TickerResponse, TpSl, TriggerOrderStyle,
    };
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[derive(Default)]
    struct DummyConnector {
        calls: Mutex<Vec<(String, Decimal, OrderSide, Option<Decimal>, bool)>>,
        next_id: AtomicUsize,
        balance_calls: AtomicUsize,
        balance_equity: Mutex<Option<Decimal>>,
    }

    #[async_trait]
    impl DexConnector for DummyConnector {
        async fn start(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn stop(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
            Ok(())
        }

        async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
            Ok(())
        }

        async fn get_ticker(
            &self,
            _symbol: &str,
            _test_price: Option<Decimal>,
        ) -> Result<TickerResponse, DexError> {
            Err(DexError::Other("not used".to_string()))
        }

        async fn get_filled_orders(&self, _symbol: &str) -> Result<FilledOrdersResponse, DexError> {
            Ok(FilledOrdersResponse::default())
        }

        async fn get_canceled_orders(
            &self,
            _symbol: &str,
        ) -> Result<CanceledOrdersResponse, DexError> {
            Ok(CanceledOrdersResponse::default())
        }

        async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
            Ok(OpenOrdersResponse::default())
        }

        async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
            self.balance_calls.fetch_add(1, Ordering::SeqCst);
            let equity = self.balance_equity.lock().unwrap().unwrap_or_default();
            Ok(BalanceResponse {
                equity,
                balance: equity,
                position_entry_price: None,
                position_sign: None,
            })
        }

        async fn get_combined_balance(
            &self,
        ) -> Result<dex_connector::CombinedBalanceResponse, DexError> {
            Ok(dex_connector::CombinedBalanceResponse::default())
        }

        async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
            Ok(vec![])
        }

        async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
            Ok(LastTradesResponse::default())
        }

        async fn get_order_book(
            &self,
            _symbol: &str,
            _depth: usize,
        ) -> Result<OrderBookSnapshot, DexError> {
            Ok(OrderBookSnapshot::default())
        }

        async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_canceled_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn create_order(
            &self,
            symbol: &str,
            size: Decimal,
            side: OrderSide,
            price: Option<Decimal>,
            _spread: Option<i64>,
            reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            let order_id = format!("test-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
            let ordered_price = price.unwrap_or_else(|| Decimal::ONE);
            self.calls
                .lock()
                .unwrap()
                .push((symbol.to_string(), size, side, price, reduce_only));
            Ok(CreateOrderResponse {
                order_id,
                exchange_order_id: None,
                ordered_price,
                ordered_size: size,
                client_order_id: None,
            })
        }

        async fn create_advanced_trigger_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _trigger_px: Decimal,
            _limit_px: Option<Decimal>,
            _order_style: TriggerOrderStyle,
            _slippage_bps: Option<u32>,
            _tpsl: TpSl,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            Err(DexError::Other("not used".to_string()))
        }

        async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
            Ok(())
        }

        async fn cancel_orders(
            &self,
            _symbol: Option<String>,
            _order_ids: Vec<String>,
        ) -> Result<(), DexError> {
            Ok(())
        }

        async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
            false
        }

        async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
            Ok("signed".to_string())
        }

        async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
            Ok("signed".to_string())
        }
    }

    #[tokio::test]
    async fn reissue_partial_entry_leg_reorders_remaining() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        let pending = PendingOrders {
            legs: vec![PendingLeg {
                symbol: "AAA".to_string(),
                order_id: "leg1".to_string(),
                exchange_order_id: None,
                target: dec("0.05"),
                filled: Decimal::ZERO,
                side: OrderSide::Long,
                limit_price: None,
            }],
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
        };
        let mut price_map = HashMap::new();
        price_map.insert(
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("100.0"),
                funding_rate: Decimal::ZERO,
                bid_price: None,
                ask_price: None,
                bid_size: Decimal::ZERO,
                ask_size: Decimal::ZERO,
                min_order: Some(dec("0.001")),
                min_tick: Some(dec("0.001")),
                size_decimals: Some(3),
                exchange_ts: None,
            },
        );
        let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

        let result = engine
            .reissue_partial_legs(&pending, &filled_qtys, &price_map, false, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.legs.len(), 2);
        assert!(result
            .legs
            .iter()
            .any(|leg| leg.target == dec("0.02") && leg.filled == dec("0.02")));
        assert!(result
            .legs
            .iter()
            .any(|leg| leg.target == dec("0.03") && leg.filled == Decimal::ZERO));
        let calls = connector.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "AAA");
        assert_eq!(calls[0].3, Some(dec("100.0")));
        assert!(!calls[0].4);
    }

    #[tokio::test]
    async fn reissue_partial_entry_missing_price_keeps_pending() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        let pending = PendingOrders {
            legs: vec![PendingLeg {
                symbol: "AAA".to_string(),
                order_id: "leg1".to_string(),
                exchange_order_id: None,
                target: dec("0.05"),
                filled: Decimal::ZERO,
                side: OrderSide::Long,
                limit_price: None,
            }],
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
        };
        let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

        let result = engine
            .reissue_partial_legs(&pending, &filled_qtys, &HashMap::new(), false, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.legs.len(), 1);
        assert_eq!(result.legs[0].target, dec("0.05"));
        assert_eq!(result.legs[0].filled, dec("0.02"));
    }

    #[tokio::test]
    async fn refresh_equity_if_needed_skips_when_cache_is_fresh() {
        let connector = Arc::new(DummyConnector::default());
        *connector.balance_equity.lock().unwrap() = Some(dec("1234.56"));
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        engine.instances[0].last_equity_fetch = Some(Instant::now());
        let initial_equity = engine.instances[0].equity_cache;

        engine.refresh_equity_if_needed(0).await.unwrap();

        assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.instances[0].equity_cache, initial_equity);
    }

    #[tokio::test]
    async fn refresh_equity_if_needed_fetches_when_cache_is_stale() {
        let connector = Arc::new(DummyConnector::default());
        *connector.balance_equity.lock().unwrap() = Some(dec("1234.56"));
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        engine.instances[0].last_equity_fetch = Some(
            Instant::now() - Duration::from_secs(EQUITY_REFRESH_CACHE_SECS + 1),
        );

        engine.refresh_equity_if_needed(0).await.unwrap();

        assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 1);
        assert!((engine.instances[0].equity_cache - 1234.56).abs() < 1e-6);
    }

    #[tokio::test]
    async fn fetch_equity_rest_bypasses_cache() {
        // Pre-entry path must hit REST regardless of cache age so the
        // about-to-be-placed order is sized against a current value.
        let connector = Arc::new(DummyConnector::default());
        *connector.balance_equity.lock().unwrap() = Some(dec("777.0"));
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        engine.instances[0].last_equity_fetch = Some(Instant::now());

        engine.fetch_equity_rest(0).await;

        assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 1);
        assert!((engine.instances[0].equity_cache - 777.0).abs() < 1e-6);
    }
}

#[cfg(test)]
mod shutdown_grace_tests {
    use super::*;

    fn config_path(name: &str) -> String {
        format!("{}/configs/pairtrade/{}", env!("CARGO_MANIFEST_DIR"), name)
    }

    #[test]
    fn default_when_yaml_omits_key() {
        // from_env() path with no env var set = default
        // Use a scoped env guard to avoid bleeding into other tests.
        let prev = std::env::var("SHUTDOWN_GRACE_SECS").ok();
        std::env::remove_var("SHUTDOWN_GRACE_SECS");
        // Also ensure required env vars have sensible fallbacks.
        std::env::set_var("DEX_NAME", "hyperliquid");
        std::env::set_var("UNIVERSE_PAIRS", "BTC/ETH");
        let cfg = PairTradeConfig::from_env().expect("from_env failed");
        assert_eq!(cfg.shutdown_grace_secs, DEFAULT_SHUTDOWN_GRACE_SECS);
        assert_eq!(cfg.shutdown_grace_secs, 3660);
        if let Some(v) = prev {
            std::env::set_var("SHUTDOWN_GRACE_SECS", v);
        }
    }

    #[test]
    fn live_btceth_configs_pin_grace_above_force_close() {
        // The -b / -c YAMLs were folded into the single multi-strategy
        // debot-pair-btceth.yaml in commit 7 of #25; only the consolidated
        // file is checked here. Expected grace values are pinned per-file:
        // btceth's strategy A has a 7200s force_close override, so the grace
        // must cover it (see bot-strategy#50).
        let expected: &[(&str, u64)] = &[("debot-pair-btceth.yaml", 7260)];
        for (name, expected_grace) in expected {
            let path = config_path(name);
            let cfg = PairTradeConfig::from_yaml_path(&path)
                .unwrap_or_else(|e| panic!("failed to load {path}: {e}"));
            assert_eq!(
                cfg.shutdown_grace_secs, *expected_grace,
                "{name}: expected shutdown_grace_secs={}, got {}",
                expected_grace, cfg.shutdown_grace_secs
            );
        }
    }

    /// Regression guard for bot-strategy#50: if any strategy raises
    /// `force_close_time_secs` above `shutdown_grace_secs - 60s`, config load
    /// must fail rather than silently shipping a config that would
    /// prematurely force-close positions on SIGTERM.
    #[test]
    fn validate_rejects_strategy_force_close_exceeding_grace() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("pairtrade_validate_regression.yaml");
        let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
force_close_time_secs: 3600
shutdown_grace_secs: 3660
strategies:
  - id: a
    force_close_time_secs: 7200
"#;
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();
        let err = PairTradeConfig::from_yaml_path(&path)
            .expect_err("validate() must reject grace=3660 when strategy A force_close=7200");
        let msg = format!("{err}");
        assert!(
            msg.contains("shutdown_grace_secs"),
            "error should mention shutdown_grace_secs, got: {msg}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
