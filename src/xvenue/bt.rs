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
    /// Round-trip slippage on Extended in bps (entry + exit, both legs).
    /// Approximates the cost of crossing the bid-ask vs hitting mid.
    /// Extended's tight 1-tick book makes this small (~0.3 bps default).
    /// Set to 0 to keep mid-fill semantics. bot-strategy#166 Phase 1
    /// fill-model refinement.
    pub extended_round_trip_slippage_bps: f64,
    /// Round-trip slippage on Lighter in bps. Lighter's wider inside
    /// (~10 bps for ETH) means taker crosses ~10 bps round-trip; with
    /// post-only at 50% fill rate the average becomes ~0; with always
    /// post-only ~-10 (rebate). Default 5.0 mirrors taker-only baseline
    /// so live numbers don't surprise downward; tune per Lighter regime.
    pub lighter_round_trip_slippage_bps: f64,
    /// Path to a Binance 1m kline JSONL (one row per minute with
    /// `ts_ms` / `high` / `low`) used as a stale-quote reference. When
    /// set together with `binance_ref_max_dev_bps > 0`, any venue mid
    /// that drifts farther than the threshold from the corresponding
    /// minute's `(high + low) / 2` is suppressed for that tick (its
    /// `book_ok` becomes false → no spread commit). Mirrors the Phase 0
    /// v2 `--drop-ref-deviation-bps` pre-filter. None = disabled.
    pub binance_ref_path: Option<String>,
    pub binance_ref_max_dev_bps: f64,
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
            extended_round_trip_slippage_bps: 0.0,
            lighter_round_trip_slippage_bps: 0.0,
            binance_ref_path: None,
            binance_ref_max_dev_bps: 0.0,
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

/// Maximum full-spread (in bps of mid) the IOC fill model will treat as
/// tradable. Beyond this, the book is judged a phantom / stale-guard
/// read and the BT falls back to the flat slippage knob. bot-strategy#454.
const MAX_TRADABLE_FULL_SPREAD_BPS: i64 = 50;

/// Top-of-book snapshot at an evaluation point — used by the IOC fill
/// model to compute the actual fill price as a function of side, qty,
/// and the resting liquidity. bot-strategy#454 step 2a.
///
/// Only the touch level is captured because the replay dumps only carry
/// top-of-book (`bid_price` / `ask_price` / `bid_size` / `ask_size`),
/// not full L2. The fill model walks "into the book" via a simple
/// 1-bps-per-overflow-multiple heuristic when our qty exceeds the top
/// level — adequate for the bot's `$50` notional regime where the top
/// is usually deeper than our order, but acknowledged as conservative
/// when notional ramps approach top-of-book size.
#[derive(Debug, Clone, Copy)]
pub struct BookSnapshot {
    pub mid: Decimal,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub bid_size: Decimal,
    pub ask_size: Decimal,
}

impl BookSnapshot {
    /// True when both sides have a positive price AND a positive size
    /// AND the inside spread is plausible. The IOC fill model is
    /// undefined on degenerate books, and on phantom books with wild
    /// guard prices (Lighter's dump occasionally records bid=mid-X / ask=mid+X
    /// pairs hundreds of dollars wide when one side has no near-touch
    /// liquidity — e.g. `bid=1774 ask=2494 mid=2134` with 0.1 size on
    /// both sides). We treat anything wider than `MAX_TRADABLE_FULL_SPREAD_BPS`
    /// as a non-tradable book so the BT falls back to the flat slippage
    /// knob instead of charging a $720-wide phantom IOC at the touch.
    pub fn tradable(&self) -> bool {
        if self.bid_price <= Decimal::ZERO
            || self.ask_price <= Decimal::ZERO
            || self.bid_size <= Decimal::ZERO
            || self.ask_size <= Decimal::ZERO
            || self.mid <= Decimal::ZERO
        {
            return false;
        }
        // Spread sanity check. Lighter ETH normally runs <= ~5 bps full
        // spread (per memory bot-strategy#192 — wider than Extended's
        // ~0.5 bps but well within this cap). Anything past 50 bps is a
        // stale / one-sided guard book.
        let full_spread = self.ask_price - self.bid_price;
        if full_spread <= Decimal::ZERO {
            return false;
        }
        let full_spread_bps = full_spread / self.mid * Decimal::from(10_000);
        full_spread_bps <= Decimal::from(MAX_TRADABLE_FULL_SPREAD_BPS)
    }

    /// IOC taker fill price for `qty` on the venue side implied by
    /// `direction`. Walking model:
    ///
    /// - If `qty <= touch_size`: fill at the touch (best_ask for buy /
    ///   best_bid for sell). Slippage vs mid is the half-spread.
    /// - If `qty > touch_size`: fill at the touch plus a 1 bp penalty
    ///   for each whole multiple of `touch_size` we exceed. e.g.
    ///   `qty = 1.5 * touch_size` → +1 bp; `qty = 3 * touch_size` → +3
    ///   bps. Heuristic-only because the dump lacks L2; tune the
    ///   multiplier if calibration shows it's biased.
    ///
    /// Returns `None` if the book isn't tradable (caller falls back to
    /// the flat `*_round_trip_slippage_bps` knob).
    pub fn ioc_fill_price(&self, side: IocSide, qty: Decimal) -> Option<Decimal> {
        if !self.tradable() || qty <= Decimal::ZERO {
            return None;
        }
        let (touch_price, touch_size) = match side {
            IocSide::Buy => (self.ask_price, self.ask_size),
            IocSide::Sell => (self.bid_price, self.bid_size),
        };
        if touch_price <= Decimal::ZERO || touch_size <= Decimal::ZERO {
            return None;
        }
        if qty <= touch_size {
            return Some(touch_price);
        }
        // Overflow penalty: 1 bp per full overflow multiple of
        // touch_size. Adverse direction depending on side.
        let overflow_ratio = (qty / touch_size).to_f64().unwrap_or(1.0);
        let extra_multiples = (overflow_ratio - 1.0).max(0.0).floor();
        let penalty_bps = extra_multiples; // 1 bp per overflow multiple
        let penalty = Decimal::from_f64(penalty_bps / 10_000.0).unwrap_or(Decimal::ZERO);
        let signed_penalty = match side {
            IocSide::Buy => Decimal::ONE + penalty,
            IocSide::Sell => Decimal::ONE - penalty,
        };
        Some(touch_price * signed_penalty)
    }
}

/// Which side of the touch a marketable taker order lands on. Mirrors
/// `dex_connector::OrderSide` semantically but avoids dragging the
/// connector type into the BT model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IocSide {
    /// We're buying — cross the ask.
    Buy,
    /// We're selling — cross the bid.
    Sell,
}

/// Helper: which side does each leg land on at entry, given the round-trip
/// `direction`? Long spread (= long Extended, short Lighter) buys Ext and
/// sells Lt at entry; Short is symmetric.
pub fn entry_sides(direction: SpreadDirection) -> (IocSide, IocSide) {
    match direction {
        SpreadDirection::Long => (IocSide::Buy, IocSide::Sell),
        SpreadDirection::Short => (IocSide::Sell, IocSide::Buy),
    }
}

/// Helper: exit sides are simply reversed.
pub fn exit_sides(direction: SpreadDirection) -> (IocSide, IocSide) {
    let (ext, lt) = entry_sides(direction);
    (
        match ext {
            IocSide::Buy => IocSide::Sell,
            IocSide::Sell => IocSide::Buy,
        },
        match lt {
            IocSide::Buy => IocSide::Sell,
            IocSide::Sell => IocSide::Buy,
        },
    )
}

/// Track the open position's entry context for PnL settlement on exit.
struct OpenLeg {
    direction: SpreadDirection,
    entry_ts_ms: u64,
    entry_dev_bps: f64,
    entry_ext_mid: Decimal,
    entry_lt_mid: Decimal,
    /// bot-strategy#454 step 2a: per-leg book snapshots at entry, so
    /// settle_trade can compute the actual IOC fill price (vs the prior
    /// flat-slippage approximation).
    entry_ext_book: Option<BookSnapshot>,
    entry_lt_book: Option<BookSnapshot>,
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

    let ref_map = match (&cfg.binance_ref_path, cfg.binance_ref_max_dev_bps) {
        (Some(path), thr) if thr > 0.0 => Some(load_binance_ref(path)?),
        _ => None,
    };

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
        let (ext_book, mut ext_book_ok) = read_snapshot(&ext, &cfg.symbol_extended).await?;
        let (lt_book, mut lt_book_ok) = read_snapshot(&lt, &cfg.symbol_lighter).await?;
        let ext_mid = ext_book.mid;
        let lt_mid = lt_book.mid;

        // Binance 1m reference cross-check: when the loaded reference
        // covers the current minute, suppress any side whose mid drifts
        // farther than `binance_ref_max_dev_bps` from the reference mid.
        // Catches the 2026-04-21 22:33-22:55 UTC kind of stuck-quote
        // event (Lighter froze for 23 min while spot moved). The 100
        // bps spread filter already drops the most extreme cases; this
        // catches the smaller-but-still-stale ones a per-spread cap
        // misses. See bot-strategy#166 design and
        // `phase0/spread_analysis.py --drop-ref-deviation-bps`.
        if let Some(ref_map) = ref_map.as_ref() {
            let minute_ts = (ext_ts.min(lt_ts) / 60_000) * 60_000;
            if let Some(ref_mid) = ref_map.get(&minute_ts) {
                let cap = cfg.binance_ref_max_dev_bps;
                let ext_dev = mid_dev_bps(ext_mid, *ref_mid);
                let lt_dev = mid_dev_bps(lt_mid, *ref_mid);
                if ext_dev.abs() > cap {
                    ext_book_ok = false;
                }
                if lt_dev.abs() > cap {
                    lt_book_ok = false;
                }
            }
        }

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
            let bucket_ts = (ext_ts.min(lt_ts) / cfg.spread.bucket_ms) * cfg.spread.bucket_ms;
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
                    entry_ext_book: if ext_book_ok { Some(ext_book) } else { None },
                    entry_lt_book: if lt_book_ok { Some(lt_book) } else { None },
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
                let exit_ext_book = if ext_book_ok { Some(ext_book) } else { None };
                let exit_lt_book = if lt_book_ok { Some(lt_book) } else { None };
                let record = settle_trade(
                    &cfg,
                    leg,
                    ts_ms,
                    dev_at_exit,
                    ext_mid,
                    lt_mid,
                    exit_ext_book,
                    exit_lt_book,
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
) -> Result<(BookSnapshot, bool)> {
    use dex_connector::DexConnector;
    let ob = c
        .get_order_book(symbol, 1)
        .await
        .map_err(|e| anyhow!("get_order_book({}): {:?}", symbol, e))?;
    let bid = ob.bids.first();
    let ask = ob.asks.first();
    let bid_size = bid.map(|b| b.size).unwrap_or(Decimal::ZERO);
    let ask_size = ask.map(|a| a.size).unwrap_or(Decimal::ZERO);
    let bid_price = bid.map(|b| b.price).unwrap_or(Decimal::ZERO);
    let ask_price = ask.map(|a| a.price).unwrap_or(Decimal::ZERO);
    let book_ok = bid_size > Decimal::ZERO && ask_size > Decimal::ZERO;
    let mid = if bid_price > Decimal::ZERO && ask_price > Decimal::ZERO {
        (bid_price + ask_price) / Decimal::from(2)
    } else {
        // Degenerate / one-sided book: fall back to ticker price so we
        // don't blow up. `book_ok=false` will still suppress committing
        // this sample upstream.
        let t = c
            .get_ticker(symbol, None)
            .await
            .map_err(|e| anyhow!("get_ticker({}): {:?}", symbol, e))?;
        t.price
    };
    Ok((
        BookSnapshot {
            mid,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
        },
        book_ok,
    ))
}

fn entry_qty(notional_usd: Decimal, ext_mid: Decimal) -> Result<Decimal> {
    if ext_mid <= Decimal::ZERO {
        return Err(anyhow!("non-positive Extended mid"));
    }
    Ok(notional_usd / ext_mid)
}

/// bot-strategy#454 step 2a — per-leg slippage cost in USD.
///
/// When both entry and exit book snapshots are present, compute
/// `|fill - mid| * qty` for each side (entry + exit), summed. Falls
/// back to the legacy flat `fallback_round_trip_slippage_bps * notional`
/// formula when either snapshot is missing, so pre-#454 tests and
/// dry-run fixtures keep working.
#[allow(clippy::too_many_arguments)]
fn compute_leg_slippage_cost(
    entry_book: Option<BookSnapshot>,
    exit_book: Option<BookSnapshot>,
    entry_mid: Decimal,
    exit_mid: Decimal,
    qty: Decimal,
    entry_side: IocSide,
    exit_side: IocSide,
    fallback_round_trip_slippage_bps: f64,
    notional_usd: Decimal,
) -> Decimal {
    let bps_div = Decimal::from(10_000);
    let book_side_cost = |book: Option<BookSnapshot>, mid: Decimal, side: IocSide| -> Option<Decimal> {
        let book = book?;
        let fill = book.ioc_fill_price(side, qty)?;
        let adverse = (fill - mid).abs();
        Some(adverse * qty)
    };
    let entry_cost = book_side_cost(entry_book, entry_mid, entry_side);
    let exit_cost = book_side_cost(exit_book, exit_mid, exit_side);
    match (entry_cost, exit_cost) {
        (Some(e), Some(x)) => e + x,
        _ => {
            // Legacy flat slippage when book data is missing on either
            // side. Equivalent to the pre-#454 single-knob model.
            decimal_from_f64(fallback_round_trip_slippage_bps).unwrap_or(Decimal::ZERO)
                * notional_usd
                / bps_div
        }
    }
}

fn settle_trade(
    cfg: &BtConfig,
    leg: OpenLeg,
    exit_ts_ms: u64,
    exit_dev_bps: f64,
    exit_ext_mid: Decimal,
    exit_lt_mid: Decimal,
    exit_ext_book: Option<BookSnapshot>,
    exit_lt_book: Option<BookSnapshot>,
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

    // bot-strategy#454 step 2a — IOC slippage from book snapshots.
    //
    // When a venue book snapshot is available at entry AND exit we
    // model the actual fill price (touch + walk-into-book penalty per
    // `BookSnapshot::ioc_fill_price`). The per-leg slippage cost is the
    // sum of `|fill_price - mid| * qty` across both legs' entry+exit
    // sides — i.e. the *real* dollar cost of crossing the inside spread.
    //
    // When a snapshot is missing (degenerate book / pre-#454 fixtures),
    // we fall back to the legacy flat `*_round_trip_slippage_bps` knob
    // so existing tests stay green.
    let (ext_entry_side, lt_entry_side) = entry_sides(leg.direction);
    let (ext_exit_side, lt_exit_side) = exit_sides(leg.direction);
    let ext_slip = compute_leg_slippage_cost(
        leg.entry_ext_book,
        exit_ext_book,
        leg.entry_ext_mid,
        exit_ext_mid,
        leg.qty,
        ext_entry_side,
        ext_exit_side,
        cfg.extended_round_trip_slippage_bps,
        cfg.trade_notional_usd,
    );
    let lt_slip = compute_leg_slippage_cost(
        leg.entry_lt_book,
        exit_lt_book,
        leg.entry_lt_mid,
        exit_lt_mid,
        leg.qty,
        lt_entry_side,
        lt_exit_side,
        cfg.lighter_round_trip_slippage_bps,
        cfg.trade_notional_usd,
    );

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
    let fees = ext_fee + lt_fee + ext_slip + lt_slip;
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

/// `(venue_mid - ref_mid) / ref_mid * 10_000` in bps. Returns 0.0 if
/// `ref_mid` is non-positive (defensive — shouldn't happen with sane
/// kline data, but avoids a division-by-zero panic).
fn mid_dev_bps(venue_mid: Decimal, ref_mid: f64) -> f64 {
    if ref_mid <= 0.0 {
        return 0.0;
    }
    let m = venue_mid.to_f64().unwrap_or(0.0);
    (m - ref_mid) / ref_mid * 10_000.0
}

/// Load Binance 1m kline JSONL into a `minute_ts_ms → (high+low)/2` map.
/// Same shape as `phase0/fetch_reference.sh` writes. Lines that fail to
/// parse are skipped silently — the BT will only suppress samples for
/// minutes that did parse, which matches the Phase 0 sim's behavior.
fn load_binance_ref(path: &str) -> Result<std::collections::HashMap<u64, f64>> {
    use std::io::BufRead;
    let f = std::fs::File::open(path).map_err(|e| anyhow!("open binance ref {}: {}", path, e))?;
    let r = std::io::BufReader::new(f);
    let mut out = std::collections::HashMap::new();
    for line in r.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        let v: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let ts_ms = match v.get("ts_ms").and_then(|x| x.as_u64()) {
            Some(t) => t,
            None => continue,
        };
        let high = v
            .get("high")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let low = v
            .get("low")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        match (high, low) {
            (Some(h), Some(l)) if h > 0.0 && l > 0.0 && h >= l => {
                out.insert(ts_ms, 0.5 * (h + l));
            }
            _ => {}
        }
    }
    Ok(out)
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

    /// bot-strategy#454 step 2a: fixture variant that lets the caller
    /// set an explicit bid/ask spread so the IOC fill model has a non-
    /// zero half-spread to charge. Mid is `(bid + ask) / 2`.
    fn dump_line_spread(
        timestamp_ms: i64,
        symbol: &str,
        bid: f64,
        ask: f64,
        bid_size: f64,
        ask_size: f64,
    ) -> String {
        let mid = (bid + ask) / 2.0;
        format!(
            r#"{{"timestamp":{ts},"prices":{{"{sym}":{{"price":"{m}","funding_rate":"0","bid_price":"{b}","ask_price":"{a}","bid_size":"{bs}","ask_size":"{as_}","exchange_ts":{ets}}}}}}}"#,
            ts = timestamp_ms,
            b = bid,
            a = ask,
            bs = bid_size,
            as_ = ask_size,
            sym = symbol,
            m = mid,
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
                60..130 => lt_mid * 1.002,  // first pump
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

    #[test]
    fn binance_ref_filter_suppresses_stale_venue_quotes() {
        // Construct a 200-tick scenario where Lighter's mid drifts to
        // a stale value that's 50 bps off the Binance reference, while
        // Extended stays in sync. With a 30 bps cap, the filter should
        // suppress every Lighter update → no aligned-bucket commits at
        // all → zero trades, despite the spread crossing threshold.
        // bot-strategy#166 stale-quote guard.
        let dir = tempfile::tempdir().unwrap();
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        let mut ref_lines = Vec::new();
        let true_mid = 78_000.0_f64;
        for i in 0..200i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            ext_lines.push(dump_line(ts_ms, "BTC", true_mid));
            // Lighter writes a stale mid +50 bps off the truth — the
            // BT's spread will read +50 bps but the ref check should
            // suppress it.
            lt_lines.push(dump_line(ts_ms, "BTC", true_mid * 1.005));
        }
        // One Binance kline per minute covering all the ticks above.
        // (high, low) chosen so (h+l)/2 = true_mid exactly.
        for minute in (0..4i64) {
            let m_ts = 1_776_000_000_000 + minute * 60_000;
            ref_lines.push(format!(
                r#"{{"ts_ms":{},"high":"{}","low":"{}"}}"#,
                m_ts,
                true_mid + 1.0,
                true_mid - 1.0,
            ));
        }
        let ext_path = write_dump(dir.path(), "ext.jsonl", &ext_lines);
        let lt_path = write_dump(dir.path(), "lt.jsonl", &lt_lines);
        let ref_path = dir.path().join("ref.jsonl");
        std::fs::write(&ref_path, ref_lines.join("\n")).unwrap();

        let replay =
            DualReplay::new(ext_path.to_str().unwrap(), lt_path.to_str().unwrap()).unwrap();
        let mut cfg = BtConfig::default();
        cfg.signal.min_warmup_samples = 30;
        cfg.signal.persistence_sec = 5;
        cfg.signal.abs_threshold_bps = 5.0;
        cfg.binance_ref_path = Some(ref_path.to_str().unwrap().to_string());
        cfg.binance_ref_max_dev_bps = 30.0;
        let summary = run_bt(&replay, cfg).unwrap();
        // Every Lighter update gets suppressed → no paired commits.
        assert_eq!(summary.samples_committed, 0);
        assert!(summary.trades.is_empty());
    }

    /// bot-strategy#454 step 2a — legacy flat slippage still applies
    /// when no book snapshot is available. Build a fixture where one
    /// venue's book is degenerate (zero size on one side) and confirm
    /// the fallback path is exercised. We can't easily express this
    /// with the existing run_bt because the same flag also suppresses
    /// the spread commit, so we exercise `compute_leg_slippage_cost`
    /// directly instead.
    #[test]
    fn flat_slippage_applies_when_book_snapshot_missing() {
        let cost = compute_leg_slippage_cost(
            None,
            None,
            dec!(2000),
            dec!(2000),
            dec!(0.025),
            IocSide::Buy,
            IocSide::Sell,
            5.0, // bps
            dec!(50),
        );
        // 5 bps × $50 / 10_000 = $0.025
        assert_eq!(cost, dec!(0.025));
    }

    /// bot-strategy#454 step 2a — book-aware path: when both snapshots
    /// are present, slippage = |fill - mid| × qty across entry + exit.
    /// The fallback flat knob is ignored even when set.
    #[test]
    fn book_aware_slippage_uses_snapshots_over_flat_knob() {
        // Bid 1999.5 / Ask 2000.5 / mid 2000.0 → half-spread = 0.5 per
        // unit. Qty 0.025 → cost per side = 0.5 × 0.025 = 0.0125.
        // Entry + exit = 0.025 USD per leg. Flat knob ignored.
        let book = BookSnapshot {
            mid: dec!(2000.0),
            bid_price: dec!(1999.5),
            ask_price: dec!(2000.5),
            bid_size: dec!(10),
            ask_size: dec!(10),
        };
        let cost = compute_leg_slippage_cost(
            Some(book),
            Some(book),
            dec!(2000.0),
            dec!(2000.0),
            dec!(0.025),
            IocSide::Buy,
            IocSide::Sell,
            999.0, // intentionally absurd flat knob — must be ignored
            dec!(50),
        );
        assert_eq!(cost, dec!(0.025));
    }

    /// bot-strategy#454 step 2a — IOC overflow penalty: qty > touch size
    /// triggers a 1 bp/overflow-multiple penalty. Touch size = 0.01,
    /// qty = 0.025 → overflow ratio 2.5 → floor(1.5) = 1 multiple → 1 bp.
    #[test]
    fn ioc_fill_price_penalises_qty_exceeding_top_size() {
        let book = BookSnapshot {
            mid: dec!(2000),
            bid_price: dec!(1999),
            ask_price: dec!(2001),
            bid_size: dec!(0.01),
            ask_size: dec!(0.01),
        };
        // Buy 0.025 ETH while ask_size is 0.01: overflow ratio = 2.5,
        // extra multiples = floor(1.5) = 1 → +1 bp penalty above ask.
        let fill = book.ioc_fill_price(IocSide::Buy, dec!(0.025)).unwrap();
        // 2001 × (1 + 0.0001) = 2001.2001
        let expected = dec!(2001) * (Decimal::ONE + dec!(0.0001));
        assert_eq!(fill, expected);
    }

    /// bot-strategy#454 step 2a — qty ≤ touch fills at the touch.
    #[test]
    fn ioc_fill_price_at_touch_when_qty_under_top_size() {
        let book = BookSnapshot {
            mid: dec!(2000),
            bid_price: dec!(1999),
            ask_price: dec!(2001),
            bid_size: dec!(10),
            ask_size: dec!(10),
        };
        assert_eq!(
            book.ioc_fill_price(IocSide::Buy, dec!(0.025)).unwrap(),
            dec!(2001)
        );
        assert_eq!(
            book.ioc_fill_price(IocSide::Sell, dec!(0.025)).unwrap(),
            dec!(1999)
        );
    }

    /// bot-strategy#454 step 2a — degenerate book returns None.
    #[test]
    fn ioc_fill_price_none_on_degenerate_book() {
        let book = BookSnapshot {
            mid: dec!(2000),
            bid_price: dec!(0),
            ask_price: dec!(2001),
            bid_size: dec!(10),
            ask_size: dec!(10),
        };
        assert!(book.ioc_fill_price(IocSide::Buy, dec!(0.025)).is_none());
    }

    /// bot-strategy#454 step 2a — end-to-end: a one-cycle BT with an
    /// explicit bid/ask spread should charge book-aware slippage equal
    /// to the per-leg half-spread × qty across entry + exit, summed
    /// across the two venues. With Ext 1-tick spread and Lt 5-tick
    /// spread, slip is dominated by Lt.
    #[test]
    fn book_aware_slippage_charges_half_spread_in_run_bt() {
        let dir = tempfile::tempdir().unwrap();
        let mut ext_lines = Vec::new();
        let mut lt_lines = Vec::new();
        for i in 0..200i64 {
            let ts_ms = 1_776_000_000_000 + i * 1_000;
            let lt_mid = 78_000.0_f64;
            let ext_mid = if (60..130).contains(&i) {
                lt_mid * 1.002 // +20 bps breach
            } else {
                lt_mid
            };
            // Extended: 0.5-wide book (1 bp half-spread on each side)
            ext_lines.push(dump_line_spread(
                ts_ms,
                "BTC",
                ext_mid - 0.25,
                ext_mid + 0.25,
                1.0,
                1.0,
            ));
            // Lighter: 5-wide book (3 bps half-spread on each side)
            lt_lines.push(dump_line_spread(
                ts_ms,
                "BTC",
                lt_mid - 2.5,
                lt_mid + 2.5,
                1.0,
                1.0,
            ));
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
        cfg.trade_notional_usd = dec!(100);
        let summary = run_bt(&replay, cfg).unwrap();
        assert_eq!(summary.trades.len(), 1);
        // Per leg: qty = $100 / mid ≈ 0.00128, half-spread × 2 sides:
        //   Ext: 0.25 × 0.00128 × 2 ≈ $0.00064
        //   Lt:  2.5  × 0.00128 × 2 ≈ $0.0064
        // Total ≈ $0.007. With zero fees, fees_usd should equal
        // approximately this slippage.
        let trade = &summary.trades[0];
        let fees = trade.fees_usd.to_f64().unwrap();
        assert!(
            (0.005..=0.010).contains(&fees),
            "expected book-aware slip ~$0.007, got fees_usd={}",
            fees
        );
    }
}
