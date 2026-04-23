//! xvenue-arb entrypoint — skeleton.
//!
//! This is a scaffold binary. Real strategy, connector init, and event
//! loop are deferred until Phase 0 data feasibility GO — see
//! `docs/DESIGN.md`. For now the process just initializes logging and
//! exits, so `cargo build` produces a working artifact that the deploy
//! pipeline can be wired up against ahead of time.

use chrono::{DateTime, FixedOffset, Utc};
use debot::error_counter::{self, ErrorCountingLogger};
use env_logger::Builder;
use log::LevelFilter;
use std::env;
use std::io::Write;
use std::str::FromStr;

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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logger();
    let dex_connector_git = option_env!("DEX_CONNECTOR_GIT_HASH").unwrap_or("unknown");
    log::info!("dex-connector git: {}", dex_connector_git);
    log::info!(
        "xvenue-arb skeleton starting — strategy not yet implemented (Phase 0 GO pending)"
    );
    log::info!("See bot-strategy#166 and docs/DESIGN.md for the plan.");
    Ok(())
}
