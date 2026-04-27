//! Backtest runner — ties [`DualReplay`] data to [`SpreadEngine`] /
//! [`SignalEngine`] / [`PositionMachine`] in a single in-process loop.
//!
//! Bot-strategy#166 Phase 1. The fill model is intentionally simple at
//! this stage: every entry / exit fills instantly at the current venue
//! mid, and venue-specific fees are applied as bps deductions on the
//! traded notional. Refining to "Extended post-only with taker fallback
//! after timeout" is a follow-up — the simple model is sufficient to
//! grid-search `(abs_threshold_bps, persistence_sec, max_hold_sec,
//! rolling_window_sec)` and check whether the strategy clears the
//! 2.5 bps round-trip floor.
//!
//! What the runner is NOT (yet):
//! - No funding-cycle filter (entry/exit lockout near settle).
//! - No stale-quote guard against an external reference (Binance 1m).
//! - No partial-fill / timeout simulation; both legs are always filled.
//! - No emergency-flatten path; `EmergencyFlattening` is unreachable
//!   under the current fill model. The state machine is still routed
//!   through so the BT loop matches the live event flow when those
//!   cases get added.

use anyhow::{anyhow, Result};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use std::sync::Arc;

use super::signal::{Decision, ExitReason, SignalConfig, SignalEngine, SpreadDirection};
use super::spread::{SpreadConfig, SpreadEngine};
use super::state::{Event, PositionMachine};
use crate::ports::replay_dex::{DualReplay, ReplayConnector};

/// BT configuration. `symbol_extended` / `symbol_lighter` are the keys
/// inside each venue's JSONL dump (e.g. "BTC", "ETH"); they may differ
/// across dumps because the recorders write whatever the source DEX uses.
#[derive(Debug, Clone)]
pub struct BtConfig {
    pub spread: SpreadConfig,
    pub signal: SignalConfig,
    pub symbol_extended: String,
    pub symbol_lighter: String,
    /// Per-leg notional in USD. Both legs use the same target for
    /// delta-neutrality; per-venue qty is `notional / mid`.
    pub trade_notional_usd: Decimal,
    /// Round-trip taker fee on Extended in bps (2.5 in production).
    pub extended_taker_fee_bps: f64,
    /// Round-trip taker fee on Lighter in bps (0 in production).
    pub lighter_taker_fee_bps: f64,
    /// When true, BtSummary.buckets is populated with one record per
    /// aligned-bucket commit. Off by default — only the bt CLI's
    /// `--out-buckets-csv` flag turns it on. bot-strategy#166 parity.
    pub record_buckets: bool,
}

impl Default for BtConfig {
    fn default() -> Self {
        Self {
            spread: SpreadConfig::default(),
            signal: SignalConfig::default(),
            symbol_extended: "BTC".to_string(),
            symbol_lighter: "BTC".to_string(),
            trade_notional_usd: Decimal::from(100),
            extended_taker_fee_bps: 2.5,
            lighter_taker_fee_bps: 0.0,
            record_buckets: false,
        }
    }
}

/// One closed trade. Open positions at end-of-replay are not emitted.
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub direction: SpreadDirection,
    pub entry_ts_ms: u64,
    pub exit_ts_ms: u64,
    pub hold_secs: u64,
    pub exit_reason: ExitReason,
    pub entry_dev_bps: f64,
    pub exit_dev_bps: f64,
    pub entry_ext_mid: Decimal,
    pub entry_lt_mid: Decimal,
    pub exit_ext_mid: Decimal,
    pub exit_lt_mid: Decimal,
    pub qty: Decimal,
    pub gross_pnl_usd: Decimal,
    pub fees_usd: Decimal,
    pub net_pnl_usd: Decimal,
    /// `net_pnl / (2 * trade_notional_usd) * 10_000`. Two legs of
    /// matching notional, so divide by 2× to make this comparable to
    /// the inside-spread bps the signal layer thresholds against.
    pub net_bps: f64,
}

/// Per-bucket commit record for parity diagnostics. Rust BT exposes
/// these so we can diff bucket-by-bucket against the Python sim's
/// `--out-csv` output (bot-strategy#166).
#[derive(Debug, Clone)]
pub struct BucketRecord {
    pub bucket_ts_ms: u64,
    pub ext_ts_ms: u64,
    pub lt_ts_ms: u64,
    pub ext_mid: Decimal,
    pub lt_mid: Decimal,
    pub spread_bps: f64,
    pub rolling_mean_bps: f64,
    pub dev_bps: f64,
}

#[derive(Debug, Clone)]
pub struct BtSummary {
    pub trades: Vec<TradeRecord>,
    /// SpreadEngine sample count at end of replay.
    pub samples_committed: u64,
    /// Strategy ticks evaluated. >= samples_committed because the
    /// strategy decides on every advance, but only commits a spread
    /// sample on aligned-bucket pairs.
    pub ticks: u64,
    /// Per-commit bucket records. Empty unless `BtConfig.record_buckets`
    /// is true (avoid 25k×Vec allocation cost on grid runs).
    pub buckets: Vec<BucketRecord>,
}

impl BtSummary {
    pub fn total_net_pnl_usd(&self) -> Decimal {
        self.trades.iter().map(|t| t.net_pnl_usd).sum()
    }

    pub fn win_rate(&self) -> f64 {
        if self.trades.is_empty() {
            return 0.0;
        }
        let wins = self
            .trades
            .iter()
            .filter(|t| t.net_pnl_usd > Decimal::ZERO)
            .count();
        wins as f64 / self.trades.len() as f64
    }

    pub fn mean_net_bps(&self) -> f64 {
        if self.trades.is_empty() {
            return 0.0;
        }
        let s: f64 = self.trades.iter().map(|t| t.net_bps).sum();
        s / self.trades.len() as f64
    }
}

/// Track the open position's entry context for PnL settlement on exit.
struct OpenLeg {
    direction: SpreadDirection,
    entry_ts_ms: u64,
    entry_dev_bps: f64,
    entry_ext_mid: Decimal,
    entry_lt_mid: Decimal,
    qty: Decimal,
}

/// Run a backtest end-to-end. Drives the `DualReplay` to exhaustion;
/// any position still open at end-of-replay is dropped (not recorded).
///
/// Note: this is intentionally synchronous over an async-trait connector.
/// The replay connector is single-threaded and never awaits real I/O,
/// so we use `block_on` per call. For grid search we'd run many BTs in
/// parallel via `rayon::par_iter`, each on its own runtime.
pub fn run_bt(replay: &DualReplay, cfg: BtConfig) -> Result<BtSummary> {
    let rt = tokio::runtime::Builder::new_current_thread().build()?;
    rt.block_on(run_bt_async(replay, cfg))
}

async fn run_bt_async(replay: &DualReplay, cfg: BtConfig) -> Result<BtSummary> {
    let mut spread = SpreadEngine::new(cfg.spread.clone());
    let mut signal = SignalEngine::new(cfg.signal.clone());
    let mut machine = PositionMachine::new();
    let mut trades: Vec<TradeRecord> = Vec::new();
    let mut buckets: Vec<BucketRecord> = Vec::new();
    let mut open: Option<OpenLeg> = None;
    let mut ticks: u64 = 0;

    let ext = replay.extended();
    let lt = replay.lighter();

    loop {
        ticks += 1;

        let ts_ms = match replay.aligned_timestamp_ms() {
            Some(ts) => ts as u64,
            None => break,
        };

        // Each venue's snapshot is tagged with that venue's OWN cursor
        // timestamp, not `min(cursors)`. Tagging with the merged
        // timestamp lets the SpreadEngine commit cross-time samples (ext
        // mid from one bucket paired with lt mid from another) at any
        // bucket the merged ts touches — Python's inner-join sim sees
        // ~10% fewer aligned buckets than Rust did before this fix.
        // bot-strategy#166 byte-parity work.
        let ext_ts = ext.current_timestamp_ms().unwrap_or(ts_ms as i64) as u64;
        let lt_ts = lt.current_timestamp_ms().unwrap_or(ts_ms as i64) as u64;

        // Both connectors carry valid current-cursor snapshots; read
        // each. Symbols may differ per venue (Extended vs Lighter
        // dumps record under whatever each DEX uses).
        let (ext_mid, ext_book_ok) = read_snapshot(&ext, &cfg.symbol_extended).await?;
        let (lt_mid, lt_book_ok) = read_snapshot(&lt, &cfg.symbol_lighter).await?;

        // Skip the SpreadEngine update on stale one-sided books — a
        // venue that writes zero bid_size or ask_size has no tradeable
        // counterparty on that side and the dump's mid is artificially
        // shifted. Lighter's per-symbol BTC dump has ~49% zero-size
        // rows in the 2026-04-22..24 window; without this filter the
        // spread engine sees phantom dislocations and emits trades on
        // them. Phase 0 v2 simulator does the same drop. See
        // `phase0/spread_analysis.py::parse_jsonl_mid` and
        // bot-strategy#166 v2 refinement.
        let prev_committed = spread.samples_committed();
        if ext_book_ok {
            spread.update_extended(ext_ts, ext_mid);
        }
        if lt_book_ok {
            spread.update_lighter(lt_ts, lt_mid);
        }

        // Run the strategy ONLY when this advance produced a fresh
        // aligned-bucket sample, OR when a position is open and we're
        // checking for exit. Calling `decide` on every advance lets the
        // wall-clock-based persistence timer accumulate elapsed time
        // between commits, firing entries on stale dev — Phase 0 v2
        // iterates one decision per aligned bucket, so we mirror that
        // by gating on commit. Exits are still evaluated every tick so
        // max_hold and force_close can fire promptly. bot-strategy#166.
        let committed = spread.samples_committed() > prev_committed;
        if committed && cfg.record_buckets {
            // Capture the just-committed bucket for parity diagnostics.
            // `last_spread_bps` and `rolling_mean` are post-push, so
            // they reflect the state after the new sample is included.
            let bucket_ts = (ext_ts.min(lt_ts) / cfg.spread.bucket_ms)
                * cfg.spread.bucket_ms;
            let s = spread.last_spread_bps().unwrap_or(0.0);
            let m = spread.rolling_mean().unwrap_or(0.0);
            buckets.push(BucketRecord {
                bucket_ts_ms: bucket_ts,
                ext_ts_ms: ext_ts,
                lt_ts_ms: lt_ts,
                ext_mid,
                lt_mid,
                spread_bps: s,
                rolling_mean_bps: m,
                dev_bps: s - m,
            });
        }
        let position = machine.summary();
        let evaluate = committed || position.is_some();
        if !evaluate {
            if replay.advance().is_none() {
                break;
            }
            continue;
        }

        let dev = spread.current_dev_bps();
        let is_warm = spread.is_warm(cfg.signal.min_warmup_samples);

        match signal.decide(ts_ms, dev, is_warm, position) {
            Decision::Hold => {}
            Decision::Enter(dir) => {
                let dev_at_entry = dev.unwrap_or(0.0);
                let qty = entry_qty(cfg.trade_notional_usd, ext_mid)?;
                machine.apply(
                    ts_ms,
                    Event::EntrySignal {
                        direction: dir,
                        notional_usd: cfg.trade_notional_usd,
                    },
                )?;
                machine.apply(ts_ms, Event::ExtendedFilled { qty })?;
                machine.apply(ts_ms, Event::LighterFilled { qty })?;
                open = Some(OpenLeg {
                    direction: dir,
                    entry_ts_ms: ts_ms,
                    entry_dev_bps: dev_at_entry,
                    entry_ext_mid: ext_mid,
                    entry_lt_mid: lt_mid,
                    qty,
                });
            }
            Decision::Exit(reason) => {
                let leg = open
                    .take()
                    .ok_or_else(|| anyhow!("Decision::Exit with no open leg"))?;
                let qty = leg.qty;
                machine.apply(ts_ms, Event::ExitSignal { reason })?;
                machine.apply(ts_ms, Event::ExtendedExitFilled { qty })?;
                machine.apply(ts_ms, Event::LighterExitFilled { qty })?;

                let dev_at_exit = dev.unwrap_or(0.0);
                let record = settle_trade(
                    &cfg,
                    leg,
                    ts_ms,
                    dev_at_exit,
                    ext_mid,
                    lt_mid,
                    reason,
                );
                trades.push(record);
            }
        }

        if replay.advance().is_none() {
            break;
        }
    }

    Ok(BtSummary {
        trades,
        samples_committed: spread.samples_committed(),
        ticks,
        buckets,
    })
}

/// Read mid + book-validity flag from the connector's current snapshot.
/// `book_ok` is `true` only when both top-of-book sizes are positive —
/// see the call site in `run_bt_async` for the rationale.
///
/// **Mid is `(bid + ask) / 2`, NOT `t.price`.** Extended's dump writes a
/// stale `price` field (e.g. 75522 when the real BTC bid/ask is 76320/
/// 76321 in the same record); using `t.price` gives a constant ~100 bps
/// phantom spread that the Phase 0 v2 Python sim avoids by computing
/// mid from bid/ask. This was the dominant source of the Rust-vs-Python
/// BT divergence (bot-strategy#166).
async fn read_snapshot(
    c: &Arc<ReplayConnector>,
    symbol: &str,
) -> Result<(Decimal, bool)> {
    use dex_connector::DexConnector;
    let ob = c
        .get_order_book(symbol, 1)
        .await
        .map_err(|e| anyhow!("get_order_book({}): {:?}", symbol, e))?;
    let bid = ob.bids.first();
    let ask = ob.asks.first();
    let book_ok = bid.map(|b| b.size > Decimal::ZERO).unwrap_or(false)
        && ask.map(|a| a.size > Decimal::ZERO).unwrap_or(false);
    let mid = match (bid, ask) {
        (Some(b), Some(a)) if b.price > Decimal::ZERO && a.price > Decimal::ZERO => {
            (b.price + a.price) / Decimal::from(2)
        }
        _ => {
            // Degenerate / one-sided book: fall back to ticker price so
            // we don't blow up. The book_ok flag will still suppress
            // committing this sample upstream.
            let t = c
                .get_ticker(symbol, None)
                .await
                .map_err(|e| anyhow!("get_ticker({}): {:?}", symbol, e))?;
            t.price
        }
    };
    Ok((mid, book_ok))
}

fn entry_qty(notional_usd: Decimal, ext_mid: Decimal) -> Result<Decimal> {
    if ext_mid <= Decimal::ZERO {
        return Err(anyhow!("non-positive Extended mid"));
    }
    Ok(notional_usd / ext_mid)
}

fn settle_trade(
    cfg: &BtConfig,
    leg: OpenLeg,
    exit_ts_ms: u64,
    exit_dev_bps: f64,
    exit_ext_mid: Decimal,
    exit_lt_mid: Decimal,
    reason: ExitReason,
) -> TradeRecord {
    // Spread P&L: a Long-spread position opens with [+1 ext, -1 lt] qty
    // (in price-change terms). When the spread tightens (extended falls
    // relative to lighter) PnL is negative; when it widens, positive.
    //
    // PnL_long  = qty * ((exit_ext - entry_ext) - (exit_lt - entry_lt))
    // PnL_short = -PnL_long
    let ext_delta = exit_ext_mid - leg.entry_ext_mid;
    let lt_delta = exit_lt_mid - leg.entry_lt_mid;
    let signed_qty = match leg.direction {
        SpreadDirection::Long => leg.qty,
        SpreadDirection::Short => -leg.qty,
    };
    let gross = signed_qty * (ext_delta - lt_delta);

    // Fees: round-trip on each leg = 2 * fee_bps * notional / 10_000.
    // Simplification: notional is held constant at the configured value
    // (entry/exit price drift typically <1% on these holds).
    let two = Decimal::from(2);
    let bps_div = Decimal::from(10_000);
    let ext_fee = decimal_from_f64(cfg.extended_taker_fee_bps).unwrap_or(Decimal::ZERO)
        * cfg.trade_notional_usd
        * two
        / bps_div;
    let lt_fee = decimal_from_f64(cfg.lighter_taker_fee_bps).unwrap_or(Decimal::ZERO)
        * cfg.trade_notional_usd
        * two
        / bps_div;
    let fees = ext_fee + lt_fee;
    let net = gross - fees;

    let net_bps = (net.to_f64().unwrap_or(0.0)
        / (cfg.trade_notional_usd.to_f64().unwrap_or(1.0) * 2.0))
        * 10_000.0;

    let hold_secs = (exit_ts_ms.saturating_sub(leg.entry_ts_ms)) / 1_000;

    TradeRecord {
        direction: leg.direction,
        entry_ts_ms: leg.entry_ts_ms,
        exit_ts_ms,
        hold_secs,
        exit_reason: reason,
        entry_dev_bps: leg.entry_dev_bps,
        exit_dev_bps,
        entry_ext_mid: leg.entry_ext_mid,
        entry_lt_mid: leg.entry_lt_mid,
        exit_ext_mid: exit_ext_mid,
        exit_lt_mid: exit_lt_mid,
        qty: leg.qty,
        gross_pnl_usd: gross,
        fees_usd: fees,
        net_pnl_usd: net,
        net_bps,
    }
}

fn decimal_from_f64(v: f64) -> Option<Decimal> {
    Decimal::from_f64(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::replay_dex::DualReplay;
    use rust_decimal_macros::dec;

    /// Helper to construct a JSONL line for a venue dump.
    fn dump_line(timestamp_ms: i64, symbol: &str, mid: f64) -> String {
        dump_line_sized(timestamp_ms, symbol, mid, 1.0, 1.0)
    }

    /// Helper variant where `bid_size` and `ask_size` can be set
    /// independently (used to test the zero-size stale-quote filter).
    fn dump_line_sized(
        timestamp_ms: i64,
        symbol: &str,
        mid: f64,
        bid_size: f64,
        ask_size: f64,
    ) -> String {
        format!(
            r#"{{"timestamp":{ts},"prices":{{"{sym}":{{"price":"{p}","funding_rate":"0","bid_price":"{p}","ask_price":"{p}","bid_size":"{bs}","ask_size":"{as_}","exchange_ts":{ets}}}}}}}"#,
            ts = timestamp_ms,
            bs = bid_size,
            as_ = ask_size,
            sym = symbol,
            p = mid,
            ets = timestamp_ms / 1000
        )
    }

    fn write_dump(dir: &std::path::Path, name: &str, lines: &[String]) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, lines.join("\n")).unwrap();
        path
    }

    /// End-to-end mini replay that exercises one full enter/exit cycle:
    /// - Steady spread for warmup
    /// - Spread blows to +20 bps (above 5 bps threshold) and stays there
    ///   long enough to clear persistence
    /// - Spread reverts to 0; exit fires on mean cross
    /// - Exactly one TradeRecord with negative gross before fees
    #[test]
    fn bt_runs_one_full_cycle_with_mean_cross_exit() {
        let dir = tempfile::tempdir().unwrap();

        // 200s of paired ticks at 1s cadence. Both venues report at the
        // same wall-clock so SpreadEngine's bucket alignment commits a
        // sample every tick.
        //
        // Warmup: 60 ticks of zero spread -> rolling mean ≈ 0.
        // Pump:   t=60..130 extended above lighter by 20 bps.
        // Revert: t=130..200 spread back to 0.
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        for i in 0..200i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            let lt_mid = 78_000.0_f64;
            let ext_mid = if (60..130).contains(&i) {
                lt_mid * 1.002 // +20 bps
            } else {
                lt_mid
            };
            ext_lines.push(dump_line(ts_ms, "BTC", ext_mid));
            lt_lines.push(dump_line(ts_ms, "BTC", lt_mid));
        }
        let ext_path = write_dump(dir.path(), "ext.jsonl", &ext_lines);
        let lt_path = write_dump(dir.path(), "lt.jsonl", &lt_lines);

        let replay =
            DualReplay::new(ext_path.to_str().unwrap(), lt_path.to_str().unwrap()).unwrap();

        let mut cfg = BtConfig::default();
        // Warmup small enough to fit inside the 60-tick lead-in.
        cfg.signal.min_warmup_samples = 30;
        // Persistence small enough to fire inside the 70-tick pump.
        cfg.signal.persistence_sec = 5;
        cfg.signal.abs_threshold_bps = 5.0;
        // No taker fees on either side so the trade can clear gross.
        cfg.extended_taker_fee_bps = 0.0;
        cfg.lighter_taker_fee_bps = 0.0;
        cfg.trade_notional_usd = dec!(100);

        let summary = run_bt(&replay, cfg).unwrap();
        assert_eq!(summary.trades.len(), 1, "expected exactly one closed trade");
        let trade = &summary.trades[0];
        // We entered short-spread (ext above lt) and exited at mean cross.
        assert_eq!(trade.direction, SpreadDirection::Short);
        assert_eq!(trade.exit_reason, ExitReason::MeanCross);
        // Without fees and with the spread closing cleanly back to ~0,
        // the short-spread position should be roughly profitable.
        assert!(
            trade.net_pnl_usd > Decimal::ZERO,
            "net_pnl_usd should be positive without fees: {}",
            trade.net_pnl_usd
        );
        // SpreadEngine committed at least one sample per tick; we have
        // 200 ticks aligned, so >= ~190 samples (one per bucket).
        assert!(summary.samples_committed >= 100);
    }

    #[test]
    fn bt_records_zero_trades_when_spread_never_breaches() {
        let dir = tempfile::tempdir().unwrap();
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        for i in 0..120i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            let lt_mid = 78_000.0_f64;
            // Steady tiny spread well below the 5-bps threshold.
            let ext_mid = lt_mid * 1.0001; // +1 bp
            ext_lines.push(dump_line(ts_ms, "BTC", ext_mid));
            lt_lines.push(dump_line(ts_ms, "BTC", lt_mid));
        }
        let ext_path = write_dump(dir.path(), "ext.jsonl", &ext_lines);
        let lt_path = write_dump(dir.path(), "lt.jsonl", &lt_lines);
        let replay =
            DualReplay::new(ext_path.to_str().unwrap(), lt_path.to_str().unwrap()).unwrap();

        let mut cfg = BtConfig::default();
        cfg.signal.min_warmup_samples = 30;
        cfg.signal.persistence_sec = 5;
        cfg.signal.abs_threshold_bps = 5.0;

        let summary = run_bt(&replay, cfg).unwrap();
        assert!(summary.trades.is_empty());
    }

    #[test]
    fn bt_force_close_exit_on_blowout() {
        // Spread widens past force_close_dev_bps, then reverts to 0
        // quickly enough that the strategy doesn't re-enter and trigger
        // another ForceClose round-trip. Verifies the ForceClose exit
        // path fires at all and the resulting trade is recorded with
        // a wrong-way gross PnL.
        let dir = tempfile::tempdir().unwrap();
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        for i in 0..200i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            let lt_mid = 78_000.0_f64;
            // Warmup (zero spread) -> +10 bps breach -> +50 bps blowout
            // (single tick) -> revert to 0 immediately. The single-tick
            // blowout fires ForceClose without re-entry afterwards.
            let ext_mid = if i < 60 {
                lt_mid
            } else if i < 80 {
                lt_mid * 1.001 // +10 bps; entry persistence accumulates
            } else if i == 80 {
                lt_mid * 1.005 // +50 bps; ForceClose tick
            } else {
                lt_mid // back to 0
            };
            ext_lines.push(dump_line(ts_ms, "BTC", ext_mid));
            lt_lines.push(dump_line(ts_ms, "BTC", lt_mid));
        }
        let ext_path = write_dump(dir.path(), "ext.jsonl", &ext_lines);
        let lt_path = write_dump(dir.path(), "lt.jsonl", &lt_lines);
        let replay =
            DualReplay::new(ext_path.to_str().unwrap(), lt_path.to_str().unwrap()).unwrap();

        let mut cfg = BtConfig::default();
        cfg.signal.min_warmup_samples = 30;
        cfg.signal.persistence_sec = 5;
        cfg.signal.abs_threshold_bps = 5.0;
        cfg.signal.force_close_dev_bps = 30.0;
        cfg.extended_taker_fee_bps = 0.0;
        cfg.lighter_taker_fee_bps = 0.0;

        let summary = run_bt(&replay, cfg).unwrap();
        assert_eq!(summary.trades.len(), 1);
        assert_eq!(summary.trades[0].exit_reason, ExitReason::ForceClose);
        // Short-spread blown wider then reverted: position closes at a
        // worse level than entry mid, so gross is negative.
        assert!(summary.trades[0].gross_pnl_usd < Decimal::ZERO);
    }

    #[test]
    fn bt_skips_zero_size_buckets_no_phantom_trades() {
        // Lighter occasionally writes stale one-sided books with
        // bid_size or ask_size = 0; the displayed mid in those rows is
        // shifted by however much the missing side leaned. Without the
        // filter, BT would treat that shift as a real spread breach
        // and fire a trade. Construct a dump where every `lt` row is
        // zero-size and the displayed mid is artificially +30 bps
        // wide; the filter should produce zero trades despite the
        // signal threshold being only 5 bps.
        let dir = tempfile::tempdir().unwrap();
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        for i in 0..200i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            // Extended steady at 78000 with positive sizes
            ext_lines.push(dump_line_sized(ts_ms, "BTC", 78_000.0, 1.0, 1.0));
            // Lighter "fake mid" 78230 (~+29.5 bps lower-than-Ext) but
            // the book is one-sided (zero ask_size); the strategy must
            // not accumulate spread samples here.
            lt_lines.push(dump_line_sized(ts_ms, "BTC", 78_230.0, 1.0, 0.0));
        }
        let ext_path = write_dump(dir.path(), "ext.jsonl", &ext_lines);
        let lt_path = write_dump(dir.path(), "lt.jsonl", &lt_lines);
        let replay =
            DualReplay::new(ext_path.to_str().unwrap(), lt_path.to_str().unwrap()).unwrap();

        let mut cfg = BtConfig::default();
        cfg.signal.min_warmup_samples = 30;
        cfg.signal.persistence_sec = 5;
        cfg.signal.abs_threshold_bps = 5.0;

        let summary = run_bt(&replay, cfg).unwrap();
        // No aligned-bucket samples (Lighter side filtered out) → no
        // dev → no entries.
        assert_eq!(summary.samples_committed, 0);
        assert_eq!(summary.trades.len(), 0);
    }

    #[test]
    fn bt_summary_metrics_aggregate() {
        // Two trades scenario: pump + revert + pump + revert.
        let dir = tempfile::tempdir().unwrap();
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        for i in 0..300i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            let lt_mid = 78_000.0_f64;
            let ext_mid = match i {
                60..130 => lt_mid * 1.002, // first pump
                180..250 => lt_mid * 1.002, // second pump
                _ => lt_mid,
            };
            ext_lines.push(dump_line(ts_ms, "BTC", ext_mid));
            lt_lines.push(dump_line(ts_ms, "BTC", lt_mid));
        }
        let ext_path = write_dump(dir.path(), "ext.jsonl", &ext_lines);
        let lt_path = write_dump(dir.path(), "lt.jsonl", &lt_lines);
        let replay =
            DualReplay::new(ext_path.to_str().unwrap(), lt_path.to_str().unwrap()).unwrap();

        let mut cfg = BtConfig::default();
        cfg.signal.min_warmup_samples = 30;
        cfg.signal.persistence_sec = 5;
        cfg.signal.abs_threshold_bps = 5.0;
        cfg.extended_taker_fee_bps = 0.0;
        cfg.lighter_taker_fee_bps = 0.0;

        let summary = run_bt(&replay, cfg).unwrap();
        assert!(
            summary.trades.len() >= 1,
            "expected at least one trade, got {}",
            summary.trades.len()
        );
        // Aggregate helpers don't panic on populated summary.
        let _ = summary.total_net_pnl_usd();
        let _ = summary.win_rate();
        let _ = summary.mean_net_bps();
    }
}
