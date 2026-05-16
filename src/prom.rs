//! In-process Prometheus exporter for xvenue-arb (bot-strategy#314 Group 5).
//!
//! Mirrors the layout of `pairtrade::prom` (bot-strategy#409): gauges
//! are defined eagerly and the HTTP `/metrics` server only binds when
//! `PROM_LISTEN` is present in the environment (e.g.
//! `PROM_LISTEN=127.0.0.1:9464`). Without it the gauges still receive
//! writes but no socket is opened, keeping the production rollout
//! opt-in per host.
//!
//! Naming differs from pairtrade in two ways:
//! - prefix is `xvenue_arb_` so the two strategies stay distinct in
//!   the shared Grafana Cloud Prometheus stack;
//! - there is no `variant` label — xvenue-arb runs a single strategy
//!   across two venues, so per-leg detail lives in a `venue` label
//!   instead.
//!
//! Counter-shaped values that already live as `u64` on
//! `LivePaperSummary` are exposed as `IntGaugeVec` rather than
//! `IntCounterVec`: the live loop owns the canonical counter, this
//! file mirrors the latest reading at the status tick. Use PromQL
//! `delta()` rather than `increase()` for windowed rates.

use anyhow::Result;
use once_cell::sync::Lazy;
use prometheus::{Encoder, GaugeVec, IntGaugeVec, Opts, Registry, TextEncoder};
use std::env;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const ENV_LISTEN: &str = "PROM_LISTEN";

/// Process-wide registry. All metrics are registered here at first
/// access.
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

fn register_gauge(name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(Opts::new(name, help), labels)
        .expect("prometheus GaugeVec construction never fails for static names");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("prometheus registry rejected duplicate metric");
    g
}

fn register_int_gauge(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), labels)
        .expect("prometheus IntGaugeVec construction never fails for static names");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("prometheus registry rejected duplicate metric");
    g
}

// === Signal / spread ===

pub static DEV_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_dev_bps",
        "Latest cross-venue spread deviation (bps). Sign convention matches \
         `LivePaperSummary.last_dev_bps`; positive = Extended rich vs Lighter.",
        &["agent"],
    )
});

pub static CAP_LONG_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_cap_long_bps",
        "Touch-to-touch capturable spread (bps) for a Long-Lighter / Short-Extended \
         entry, as last snapshotted by the tick loop. `NaN` while book depth is \
         unavailable.",
        &["agent"],
    )
});

pub static CAP_SHORT_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_cap_short_bps",
        "Touch-to-touch capturable spread (bps) for a Short-Lighter / Long-Extended \
         entry. Sign-symmetric with `xvenue_arb_cap_long_bps`.",
        &["agent"],
    )
});

pub static INSIDE_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_inside_bps",
        "Latest inside half-spread (bps) per venue. Used to diagnose one-sided \
         book widening behind missed entries.",
        &["agent", "venue"],
    )
});

pub static LT_TOUCH_SIZE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_lt_touch_size",
        "Latest Lighter top-of-book size at the touch the bot would maker into. \
         `bid` and `ask` labelled. Drives the `lt_book_max_eth` queue-depth filter.",
        &["agent", "side"],
    )
});

// === Position / activity ===

pub static HAS_POSITION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_has_position",
        "1 when the position machine is in `Held` (or any non-Flat) phase.",
        &["agent"],
    )
});

pub static POSITION_AGE_SECONDS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_position_age_seconds",
        "Seconds since the position machine entered its current phase. 0 when Flat.",
        &["agent"],
    )
});

pub static PHASE_STATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_phase_state",
        "Position machine phase as an int: 0=Flat, 1=EnteringExtended, \
         2=EnteringLighter, 3=Held, 4=Exiting, 5=EmergencyFlattening. \
         Lets dashboards highlight stuck transitions.",
        &["agent"],
    )
});

// === Decisions / blocks (mirrored from LivePaperSummary) ===

pub static DECISIONS_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_decisions_total",
        "Cumulative SignalEngine decision count by kind. Counter semantics: \
         use PromQL `delta()` for windowed rates (mirrored from u64 counters, \
         not a true Prometheus counter).",
        &["agent", "kind"],
    )
});

pub static ENTRIES_BLOCKED_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_entries_blocked_total",
        "Cumulative count of `Decision::Enter` outcomes suppressed by a gate. \
         `reason` label distinguishes kill_switch / stuck / daily_dd / \
         session_dd / circuit_breaker / ws_stale / book_depth / maintenance.",
        &["agent", "reason"],
    )
});

pub static REF_GUARD_SUPPRESSED_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_ref_guard_suppressed_total",
        "Cumulative count of ticks where the Binance reference guard suppressed \
         a venue's book (one-sided stuck-quote detector). `venue` ∈ \
         {extended, lighter}.",
        &["agent", "venue"],
    )
});

pub static READ_MID_ERR_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_read_mid_err_total",
        "Cumulative post-warmup `read_mid` errors per venue (bot-strategy#303). \
         Each increment is one `[XVENUE] tick error: read_mid {Venue}` WARN.",
        &["agent", "venue"],
    )
});

pub static EQUITY_SAMPLES_SKIPPED_PARTIAL_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_equity_samples_skipped_partial_total",
        "Cumulative equity samples skipped because one venue reported but the \
         other did not (bot-strategy#360). Indicates single-venue maintenance \
         windows that would otherwise trip a spurious session_dd halt.",
        &["agent"],
    )
});

// === Risk / kill state ===

pub static KILL_SWITCH_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_kill_switch_active",
        "1 when the external KILL_SWITCH file is present.",
        &["agent"],
    )
});

pub static STUCK_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_stuck_active",
        "1 when the StuckTripwire latch is armed (REST consec-fail / reduce-only \
         consec-fail / operator SIGUSR1).",
        &["agent"],
    )
});

pub static SESSION_DD_HALT_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_session_dd_halt_active",
        "1 when the rolling-peak session-DD threshold has tripped and not been \
         RISK_ACK'd.",
        &["agent"],
    )
});

pub static DAILY_DD_HALT_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_daily_dd_halt_active",
        "1 when the daily realized-PnL halt is active (resets at UTC rollover).",
        &["agent"],
    )
});

pub static CIRCUIT_BREAKER_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_circuit_breaker_active",
        "1 while the consecutive-loss cooldown is in effect.",
        &["agent"],
    )
});

// === Equity / drawdown ===

pub static EQUITY_CURRENT_USD: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_equity_current_usd",
        "Current total equity in USD (sum of both venue equities; only published \
         when *both* venues report — see bot-strategy#360).",
        &["agent"],
    )
});

pub static EQUITY_PEAK_USD: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_equity_peak_usd",
        "Rolling-peak equity over the session-DD lookback window.",
        &["agent"],
    )
});

pub static SESSION_DD_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_session_dd_bps",
        "Current session drawdown in bps of rolling peak. Threshold comparison \
         is against the bot's `max_session_loss_bps` config.",
        &["agent"],
    )
});

pub static DAILY_PNL_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_daily_pnl_bps",
        "Realized PnL today in bps of session_start_equity. Negative = loss; \
         compared against `max_daily_loss_bps` for the daily halt.",
        &["agent"],
    )
});

pub static EFFECTIVE_MAX_DAILY_LOSS_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_effective_max_daily_loss_bps",
        "Effective daily-loss threshold (bps), after any temporary RISK_ACK \
         relaxations. 0 disables the halt.",
        &["agent"],
    )
});

pub static EFFECTIVE_MAX_SESSION_LOSS_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_effective_max_session_loss_bps",
        "Effective session-loss threshold (bps). 0 disables the halt.",
        &["agent"],
    )
});

// === System health ===

pub static WS_AGE_MS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_ws_age_ms",
        "Milliseconds since the last healthy WS book observation per venue. \
         Tripped by `ws_stale_emergency_ms` into Emergency{WsStale}.",
        &["agent", "venue"],
    )
});

pub static SNAPSHOT_AGE_SECONDS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "xvenue_arb_snapshot_age_seconds",
        "Age of the `status.json` snapshot on disk (file mtime delta).",
        &["agent"],
    )
});

pub static PROCESS_START_TIMESTAMP_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_process_start_timestamp_seconds",
        "Unix timestamp of process boot. Subtract from `time()` for uptime.",
        &["agent"],
    )
});

pub static BOT_VERSION_INFO: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_bot_version_info",
        "Always 1; carries package version and dex-connector git hash labels. \
         Lets dashboards confirm the live binary is the expected build.",
        &["agent", "version", "dex_connector_sha"],
    )
});

pub static DRY_RUN_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "xvenue_arb_dry_run_active",
        "1 when the bot is running with `cfg.dry_run = true` (paper-fill path; \
         no live orders dispatched).",
        &["agent"],
    )
});

/// Spawn the metrics HTTP server if `PROM_LISTEN` is set in the
/// environment. Identical contract to `pairtrade::prom::maybe_start_exporter`.
pub fn maybe_start_exporter() {
    let addr_str = match env::var(ENV_LISTEN) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            log::info!(
                "[PROM] {} not set; metrics recorded but /metrics endpoint disabled",
                ENV_LISTEN
            );
            return;
        }
    };
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            log::warn!(
                "[PROM] failed to parse {}={}: {}; exporter disabled",
                ENV_LISTEN,
                addr_str,
                e
            );
            return;
        }
    };
    tokio::spawn(async move {
        if let Err(e) = serve(addr).await {
            log::warn!("[PROM] exporter exited: {:?}", e);
        }
    });
}

async fn serve(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    log::info!("[PROM] exporter listening on http://{}/metrics", addr);
    loop {
        let (mut sock, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                log::warn!("[PROM] accept error: {}", e);
                continue;
            }
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                sock.read(&mut buf),
            )
            .await;
            let body = match encode_metrics() {
                Ok(b) => b,
                Err(e) => {
                    log::warn!("[PROM] encode error for {}: {}", peer, e);
                    return;
                }
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                TextEncoder::new().format_type(),
                body.len()
            );
            if let Err(e) = sock.write_all(resp.as_bytes()).await {
                log::debug!("[PROM] write header to {} failed: {}", peer, e);
                return;
            }
            let _ = sock.write_all(&body).await;
        });
    }
}

fn encode_metrics() -> Result<Vec<u8>> {
    let encoder = TextEncoder::new();
    let mf = REGISTRY.gather();
    let mut buf = Vec::with_capacity(8 * 1024);
    encoder.encode(&mf, &mut buf)?;
    Ok(buf)
}

/// Stamp boot-time gauges. Idempotent. Called once from `main` before
/// the loop starts so the dashboard sees uptime + version + dry-run on
/// first scrape rather than after the first status tick.
pub fn record_process_info(agent: &str, process_started_at: i64, dry_run: bool) {
    PROCESS_START_TIMESTAMP_SECONDS
        .with_label_values(&[agent])
        .set(process_started_at);
    BOT_VERSION_INFO
        .with_label_values(&[
            agent,
            env!("CARGO_PKG_VERSION"),
            option_env!("DEX_CONNECTOR_GIT_HASH").unwrap_or("unknown"),
        ])
        .set(1);
    DRY_RUN_ACTIVE
        .with_label_values(&[agent])
        .set(if dry_run { 1 } else { 0 });
}
