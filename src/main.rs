use chrono::{DateTime, FixedOffset, Utc};
use debot::error_counter::{self, ErrorCountingLogger};
use debot::pairtrade::{PairTradeConfig, PairTradeEngine};
use debot::ports::replay_dex::ReplayConnector;
use env_logger::Builder;
use log::LevelFilter;
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

fn init_logger() {
    let offset_seconds = env::var("TIMEZONE_OFFSET")
        .unwrap_or_else(|_| "3600".to_string())
        .parse::<i32>()
        .expect("Invalid TIMEZONE_OFFSET");
    let offset = FixedOffset::east_opt(offset_seconds).expect("Invalid offset");
    let filter = LevelFilter::from_str(&env::var("RUST_LOG").unwrap_or_else(|_| {
        "info,tokio_tungstenite=info,tungstenite=info".to_string()
    }))
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

async fn run_single() -> std::io::Result<()> {
    let dex_connector_git = option_env!("DEX_CONNECTOR_GIT_HASH").unwrap_or("unknown");
    log::info!("dex-connector git: {}", dex_connector_git);
    log::info!("Starting pair-trade loop...");
    let cfg = PairTradeConfig::from_env_or_yaml().expect("invalid pair trade config");
    let mut engine = init_engine_with_retry(cfg)
        .await
        .expect("failed to initialize pair trade engine");
    engine
        .run()
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

// Startup hardening for transient Lighter errors (bot-strategy#120). If
// Lighter is rate-limited when the bot comes up (e.g. after a WAF episode),
// the connector.start() and account-discovery paths surface the error to
// main.rs and used to panic immediately. Under systemd Restart=on-failure
// that became a tight crash-loop whose re-login attempts themselves kept
// the cooldown active. Now we retry the whole engine init with backoff
// inside the process for transient signatures; permanent errors (bad
// config, missing keys, unexpected shapes) still propagate straight out.
async fn init_engine_with_retry(
    cfg: PairTradeConfig,
) -> Result<PairTradeEngine, anyhow::Error> {
    const MAX_ATTEMPTS: u32 = 20;
    let mut attempt: u32 = 0;
    let mut backoff = std::time::Duration::from_secs(3);
    loop {
        attempt += 1;
        match PairTradeEngine::new(cfg.clone()).await {
            Ok(e) => {
                if attempt > 1 {
                    log::info!("[INIT_RETRY] engine initialized on attempt {}", attempt);
                }
                return Ok(e);
            }
            Err(e) => {
                let chain = format!("{:?}", e);
                // Match:
                //   - raw Lighter 429 JSON still present in a stringified error
                //   - dex-connector's `DexError::RateLimited` (Display:
                //     `Lighter WAF cooldown active until unix=... (rate-limited)`).
                //     `CheckClient` 429 now returns this variant after
                //     engaging the shared cooldown (bot-strategy#151), so
                //     the rate-limit shape is always the full 75s wait
                //     rather than a 3s retry storm.
                let transient_429 = chain.contains("Too Many Requests")
                    || chain.contains("\"code\":23000")
                    || chain.contains(" 429 ")
                    || chain.contains("rate-limited")
                    || chain.contains("WAF cooldown");
                let transient = transient_429
                    || (chain.contains("Could not find account for api_key_index=")
                        && chain.contains("Set LIGHTER_ACCOUNT_INDEX"));
                if !transient || attempt >= MAX_ATTEMPTS {
                    return Err(e);
                }
                // Lighter's per-IP /account short-window is ~60s. Retrying
                // inside that window just re-burns the budget; wait past it
                // on the 429 path before retrying. Other transient shapes
                // (account-index rediscovery) keep the fast backoff.
                // See bot-strategy#127.
                let sleep_for = if transient_429 {
                    backoff.max(std::time::Duration::from_secs(75))
                } else {
                    backoff
                };
                log::warn!(
                    "[INIT_RETRY] transient startup error (attempt {}/{}), sleeping {}s. Reason: {}",
                    attempt,
                    MAX_ATTEMPTS,
                    sleep_for.as_secs(),
                    chain.lines().next().unwrap_or(&chain),
                );
                tokio::time::sleep(sleep_for).await;
                backoff = (sleep_for * 2).min(std::time::Duration::from_secs(60));
            }
        }
    }
}

async fn run_batch(batch_file: &str) -> std::io::Result<()> {
    use std::io::{BufRead, BufReader};

    // Read all param sets from the batch file (JSONL format).
    let file = std::fs::File::open(batch_file).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("failed to open batch file {}: {}", batch_file, e),
        )
    })?;
    let reader = BufReader::new(file);
    let param_sets: Vec<HashMap<String, String>> = reader
        .lines()
        .filter_map(|line| {
            let line = line.ok()?;
            if line.trim().is_empty() {
                return None;
            }
            serde_json::from_str(&line).ok()
        })
        .collect();

    if param_sets.is_empty() {
        eprintln!("[BATCH] No param sets found in {}", batch_file);
        return Ok(());
    }

    // Load replay data once.
    let backtest_file = env::var("BACKTEST_FILE").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "BACKTEST_FILE must be set for batch mode",
        )
    })?;
    eprintln!(
        "[BATCH] Loading data from {} ({} param sets)...",
        backtest_file,
        param_sets.len()
    );
    let replay = Arc::new(ReplayConnector::new(&backtest_file).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("failed to load replay data: {}", e),
        )
    })?);
    eprintln!("[BATCH] Data loaded: {} entries.", replay.len());

    // Output dir for per-run log files.
    let log_dir = env::var("BATCH_LOG_DIR").unwrap_or_else(|_| "/tmp/batch_logs".to_string());
    std::fs::create_dir_all(&log_dir)?;

    // Save original env vars that will be overridden.
    let override_keys: Vec<String> = param_sets
        .iter()
        .flat_map(|ps| ps.keys().cloned())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let original_env: HashMap<String, Option<String>> = override_keys
        .iter()
        .map(|k| (k.clone(), env::var(k).ok()))
        .collect();

    for (idx, params) in param_sets.iter().enumerate() {
        // Set env vars for this param set.
        for (k, v) in params {
            env::set_var(k, v);
        }

        // Build config from env (picks up the overridden vars).
        let cfg = match PairTradeConfig::from_env_or_yaml() {
            Ok(c) => c,
            Err(e) => {
                let result = serde_json::json!({
                    "index": idx,
                    "log_file": serde_json::Value::Null,
                    "error": format!("{}", e),
                });
                println!("{}", result);
                // Restore env vars before continuing.
                for (k, orig) in &original_env {
                    match orig {
                        Some(v) => env::set_var(k, v),
                        None => env::remove_var(k),
                    }
                }
                continue;
            }
        };

        // Redirect log output to a per-run file.
        let log_file_path = format!("{}/batch_{}.log", log_dir, idx);

        // Create engine with shared replay data.
        let mut engine = match PairTradeEngine::new_with_replay(cfg, replay.clone()).await {
            Ok(e) => e,
            Err(e) => {
                let result = serde_json::json!({
                    "index": idx,
                    "log_file": log_file_path,
                    "error": format!("{}", e),
                });
                println!("{}", result);
                for (k, orig) in &original_env {
                    match orig {
                        Some(v) => env::set_var(k, v),
                        None => env::remove_var(k),
                    }
                }
                continue;
            }
        };

        // Run backtest, capturing log output to a file.
        {
            let log_file = std::fs::File::create(&log_file_path)?;
            let log_file_clone = log_file.try_clone()?;
            // Redirect stdout to the log file for this run.
            use std::os::unix::io::AsRawFd;
            let stdout_fd = std::io::stdout().as_raw_fd();
            let saved_stdout = unsafe { libc::dup(stdout_fd) };
            unsafe {
                libc::dup2(log_file.as_raw_fd(), stdout_fd);
            }
            // Also redirect stderr for log output.
            let stderr_fd = std::io::stderr().as_raw_fd();
            let saved_stderr = unsafe { libc::dup(stderr_fd) };
            unsafe {
                libc::dup2(log_file_clone.as_raw_fd(), stderr_fd);
            }

            let _result = engine.run().await;

            // Restore stdout/stderr.
            unsafe {
                libc::dup2(saved_stdout, stdout_fd);
                libc::close(saved_stdout);
                libc::dup2(saved_stderr, stderr_fd);
                libc::close(saved_stderr);
            }
        }

        // Output result as JSON to stdout.
        let result = serde_json::json!({
            "index": idx,
            "log_file": log_file_path,
        });
        println!("{}", result);

        // Restore env vars for next iteration.
        for (k, orig) in &original_env {
            match orig {
                Some(v) => env::set_var(k, v),
                None => env::remove_var(k),
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logger();

    if let Ok(batch_file) = env::var("BATCH_PARAMS_FILE") {
        run_batch(&batch_file).await
    } else {
        run_single().await
    }
}
