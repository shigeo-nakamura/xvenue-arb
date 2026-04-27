//! BT runner CLI. Bot-strategy#166 Phase 1.
//!
//! Usage:
//!   bt single --ext-dump <path> --lt-dump <path> --symbol <SYM>
//!     [--abs-threshold-bps 5] [--persistence-sec 15] [--max-hold-sec 600]
//!     [--rolling-window-sec 1800] [--notional 100]
//!     [--ext-fee-bps 2.5] [--lt-fee-bps 0.0]
//!     [--warmup-samples 60]
//!
//!   bt grid --ext-dump <path> --lt-dump <path> --symbol <SYM>
//!     --abs-threshold-bps "3.0,5.0,8.0,12.0"
//!     --persistence-sec "5,15,30,60"
//!     --max-hold-sec "300,600,1200"
//!     --rolling-window-sec "600,1800,3600"
//!     [--notional 100] [--ext-fee-bps 2.5] [--lt-fee-bps 0.0]
//!     [--warmup-samples 60] [--top-n 20]
//!
//! Glob support: --ext-dump and --lt-dump accept either a single file or
//! a comma-separated list. A future enhancement could concatenate dumps,
//! but for now the recommended path is to pre-merge JSONLs with `cat`
//! into one file per venue.

use std::process::ExitCode;

use anyhow::{anyhow, Result};
use debot::ports::replay_dex::DualReplay;
use debot::xvenue::bt::{run_bt, BtConfig};
use debot::xvenue::bt_grid::{run_grid, GridSpec};
use rust_decimal::Decimal;

fn main() -> ExitCode {
    if let Err(e) = run() {
        eprintln!("error: {:?}", e);
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        return Err(anyhow!("missing subcommand"));
    }
    match args[1].as_str() {
        "single" => run_single(&args[2..]),
        "grid" => run_grid_cmd(&args[2..]),
        "-h" | "--help" => {
            usage();
            Ok(())
        }
        other => {
            usage();
            Err(anyhow!("unknown subcommand: {}", other))
        }
    }
}

fn usage() {
    eprintln!(
        "Usage:
  bt single --ext-dump P --lt-dump P --symbol S [--abs-threshold-bps F]
            [--persistence-sec U] [--max-hold-sec U] [--rolling-window-sec U]
            [--notional D] [--ext-fee-bps F] [--lt-fee-bps F] [--warmup-samples U]

  bt grid   --ext-dump P --lt-dump P --symbol S
            --abs-threshold-bps F1,F2,...   --persistence-sec U1,U2,...
            --max-hold-sec U1,U2,...        --rolling-window-sec U1,U2,...
            [--notional D] [--ext-fee-bps F] [--lt-fee-bps F]
            [--warmup-samples U] [--top-n U]
"
    );
}

struct Args {
    map: std::collections::HashMap<String, String>,
}

impl Args {
    fn parse(argv: &[String]) -> Result<Self> {
        let mut map = std::collections::HashMap::new();
        let mut i = 0;
        while i < argv.len() {
            let k = &argv[i];
            if !k.starts_with("--") {
                return Err(anyhow!("expected flag, got: {}", k));
            }
            let v = argv
                .get(i + 1)
                .ok_or_else(|| anyhow!("missing value for {}", k))?;
            map.insert(k[2..].to_string(), v.clone());
            i += 2;
        }
        Ok(Self { map })
    }

    fn opt<T: std::str::FromStr>(&self, k: &str, default: T) -> Result<T>
    where
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        match self.map.get(k) {
            Some(v) => v.parse::<T>().map_err(|e| anyhow!("--{}: {:?}", k, e)),
            None => Ok(default),
        }
    }

    fn req_str(&self, k: &str) -> Result<&str> {
        self.map
            .get(k)
            .map(|s| s.as_str())
            .ok_or_else(|| anyhow!("missing required --{}", k))
    }

    fn req_vec_f64(&self, k: &str) -> Result<Vec<f64>> {
        let raw = self
            .map
            .get(k)
            .ok_or_else(|| anyhow!("missing required --{}", k))?;
        raw.split(',')
            .map(|s| s.trim().parse::<f64>().map_err(|e| anyhow!("--{}: {:?}", k, e)))
            .collect()
    }

    fn req_vec_u64(&self, k: &str) -> Result<Vec<u64>> {
        let raw = self
            .map
            .get(k)
            .ok_or_else(|| anyhow!("missing required --{}", k))?;
        raw.split(',')
            .map(|s| s.trim().parse::<u64>().map_err(|e| anyhow!("--{}: {:?}", k, e)))
            .collect()
    }
}

fn build_base(args: &Args, symbol: &str) -> Result<BtConfig> {
    let mut cfg = BtConfig::default();
    cfg.symbol_extended = symbol.to_string();
    cfg.symbol_lighter = symbol.to_string();
    cfg.trade_notional_usd = args
        .opt::<Decimal>("notional", Decimal::from(100))?;
    cfg.extended_taker_fee_bps = args.opt::<f64>("ext-fee-bps", 2.5)?;
    cfg.lighter_taker_fee_bps = args.opt::<f64>("lt-fee-bps", 0.0)?;
    cfg.extended_round_trip_slippage_bps =
        args.opt::<f64>("ext-slippage-bps", 0.0)?;
    cfg.lighter_round_trip_slippage_bps =
        args.opt::<f64>("lt-slippage-bps", 0.0)?;
    cfg.signal.min_warmup_samples = args.opt::<usize>("warmup-samples", 60)?;
    cfg.spread.bucket_ms = args.opt::<u64>("bucket-ms", 1_000)?;
    // Phase 0 v2 parity diagnostic. When set, Enter fires at the bar
    // where persistence elapses regardless of current dev — matching
    // the offline sim's "open at i+persistence_buckets without
    // dev[entry_idx] check". See bot-strategy#166 for context.
    if args.opt::<bool>("python-compat-entry", false)? {
        cfg.signal.entry_check_threshold_at_fire = false;
    }
    Ok(cfg)
}

fn run_single(argv: &[String]) -> Result<()> {
    let args = Args::parse(argv)?;
    let symbol = args.req_str("symbol")?;
    let mut cfg = build_base(&args, symbol)?;
    cfg.signal.abs_threshold_bps = args.opt::<f64>("abs-threshold-bps", 5.0)?;
    cfg.signal.persistence_sec = args.opt::<u64>("persistence-sec", 15)?;
    cfg.signal.max_hold_sec = args.opt::<u64>("max-hold-sec", 600)?;
    cfg.spread.rolling_window_sec = args.opt::<u64>("rolling-window-sec", 1800)?;
    if args.map.contains_key("out-buckets-csv") {
        cfg.record_buckets = true;
    }

    let ext_dump = args.req_str("ext-dump")?;
    let lt_dump = args.req_str("lt-dump")?;
    let replay = DualReplay::new(ext_dump, lt_dump)
        .map_err(|e| anyhow!("DualReplay::new: {:?}", e))?;

    let summary = run_bt(&replay, cfg.clone())?;

    if let Some(path) = args.map.get("out-buckets-csv") {
        let mut wtr = std::fs::File::create(path)?;
        use std::io::Write;
        writeln!(
            wtr,
            "bucket_ts_ms,ext_ts_ms,lt_ts_ms,ext_mid,lt_mid,spread_bps,rolling_mean_bps,dev_bps"
        )?;
        for b in &summary.buckets {
            writeln!(
                wtr,
                "{},{},{},{},{},{:.6},{:.6},{:.6}",
                b.bucket_ts_ms,
                b.ext_ts_ms,
                b.lt_ts_ms,
                b.ext_mid,
                b.lt_mid,
                b.spread_bps,
                b.rolling_mean_bps,
                b.dev_bps,
            )?;
        }
    }

    if let Some(path) = args.map.get("out-trades-csv") {
        let mut wtr = std::fs::File::create(path)?;
        use std::io::Write;
        writeln!(
            wtr,
            "entry_ts_ms,exit_ts_ms,direction,exit_reason,entry_dev,exit_dev,entry_ext_mid,entry_lt_mid,exit_ext_mid,exit_lt_mid,qty,gross_pnl_usd,fees_usd,net_pnl_usd,net_bps,hold_secs"
        )?;
        for t in &summary.trades {
            writeln!(
                wtr,
                "{},{},{:?},{:?},{:.4},{:.4},{},{},{},{},{},{},{},{},{:.4},{}",
                t.entry_ts_ms,
                t.exit_ts_ms,
                t.direction,
                t.exit_reason,
                t.entry_dev_bps,
                t.exit_dev_bps,
                t.entry_ext_mid,
                t.entry_lt_mid,
                t.exit_ext_mid,
                t.exit_lt_mid,
                t.qty,
                t.gross_pnl_usd,
                t.fees_usd,
                t.net_pnl_usd,
                t.net_bps,
                t.hold_secs,
            )?;
        }
    }

    println!("== Single BT ==");
    println!(
        "config: abs_threshold={} persistence={}s max_hold={}s rolling={}s warmup={} notional={} ext_fee={}bps lt_fee={}bps",
        cfg.signal.abs_threshold_bps,
        cfg.signal.persistence_sec,
        cfg.signal.max_hold_sec,
        cfg.spread.rolling_window_sec,
        cfg.signal.min_warmup_samples,
        cfg.trade_notional_usd,
        cfg.extended_taker_fee_bps,
        cfg.lighter_taker_fee_bps,
    );
    println!("ticks evaluated     : {}", summary.ticks);
    println!("samples committed   : {}", summary.samples_committed);
    println!("trades              : {}", summary.trades.len());
    println!("total net PnL (USD) : {:.4}", summary.total_net_pnl_usd());
    println!("win rate            : {:.1}%", summary.win_rate() * 100.0);
    println!("mean net bps        : {:.2}", summary.mean_net_bps());
    if !summary.trades.is_empty() {
        let max_win = summary
            .trades
            .iter()
            .map(|t| t.net_bps)
            .fold(f64::NEG_INFINITY, f64::max);
        let max_loss = summary
            .trades
            .iter()
            .map(|t| t.net_bps)
            .fold(f64::INFINITY, f64::min);
        let avg_hold = summary
            .trades
            .iter()
            .map(|t| t.hold_secs as f64)
            .sum::<f64>()
            / summary.trades.len() as f64;
        println!("max win bps         : {:.2}", max_win);
        println!("max loss bps        : {:.2}", max_loss);
        println!("avg hold secs       : {:.0}", avg_hold);
    }
    Ok(())
}

fn run_grid_cmd(argv: &[String]) -> Result<()> {
    let args = Args::parse(argv)?;
    let symbol = args.req_str("symbol")?;
    let base = build_base(&args, symbol)?;

    let spec = GridSpec {
        abs_threshold_bps: args.req_vec_f64("abs-threshold-bps")?,
        persistence_sec: args.req_vec_u64("persistence-sec")?,
        max_hold_sec: args.req_vec_u64("max-hold-sec")?,
        rolling_window_sec: args.req_vec_u64("rolling-window-sec")?,
    };

    let ext_dump = args.req_str("ext-dump")?;
    let lt_dump = args.req_str("lt-dump")?;
    let replay = DualReplay::new(ext_dump, lt_dump)
        .map_err(|e| anyhow!("DualReplay::new: {:?}", e))?;

    let n = spec.cell_count();
    eprintln!("grid: {} cells", n);
    let started = std::time::Instant::now();
    let mut results = run_grid(&replay, &base, &spec);
    eprintln!("grid done in {:.2}s", started.elapsed().as_secs_f64());

    // Sort by total_net_pnl_usd descending, then by mean_net_bps.
    results.sort_by(|a, b| {
        b.total_net_pnl_usd
            .partial_cmp(&a.total_net_pnl_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(
                b.mean_net_bps
                    .partial_cmp(&a.mean_net_bps)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
    });

    let top_n: usize = args.opt("top-n", 20usize)?;
    println!(
        "{:>8} {:>11} {:>10} {:>10} {:>8} {:>14} {:>9} {:>11}",
        "thresh", "persist_s", "maxhold_s", "rolling_s", "trades", "net_pnl_usd", "win%", "mean_bps"
    );
    for r in results.iter().take(top_n) {
        println!(
            "{:>8.2} {:>11} {:>10} {:>10} {:>8} {:>14.4} {:>9.1} {:>11.2}",
            r.abs_threshold_bps,
            r.persistence_sec,
            r.max_hold_sec,
            r.rolling_window_sec,
            r.trades,
            r.total_net_pnl_usd,
            r.win_rate * 100.0,
            r.mean_net_bps,
        );
    }
    if results.len() > top_n {
        eprintln!("... {} more cells; raise --top-n to see all", results.len() - top_n);
    }
    Ok(())
}
