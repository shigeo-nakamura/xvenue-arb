//! Pure paper-mode PnL helpers extracted from `live.rs`
//! (bot-strategy#384).
//!
//! All functions here are byte-identical to their `live.rs` originals;
//! the move is purely about cohesion (`live.rs` was 5,014 lines pre-#381
//! and this seam was the easiest of the four remaining). Every item is
//! [`pub(super)`] so the entry/exit dispatch siblings and the tests
//! still under `live::tests` can keep importing them; nothing here
//! escapes the `xvenue` subtree.

use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::live::{MidSnapshot, PaperEntryCtx};
use super::signal::SpreadDirection;

/// bot-strategy#330 follow-up: per-RT projected PnL in basis points,
/// computed at touch level instead of mid-to-mid. Mirrors what a live
/// executor would capture given the current execution wiring (Ext
/// taker IOC + Lt post-only with taker fallback).
///
/// Returns `(gross_bps, net_bps)`:
/// * `gross_bps` — touch-to-touch capture before fees. Includes the
///   Ext half-spread cross both ways and the Lt half-spread maker
///   rebate / taker cost on each leg, determined by `maker_entry` /
///   `maker_exit`. None outcomes default to taker.
/// * `net_bps` — `gross_bps - 2*ext_fee_bps - 2*lt_fee_bps`. Per-fill
///   convention matches `compute_realised_pnl` (#268 S5-1); RT total
///   fee is twice the per-fill rate on each venue.
///
/// Sign convention: positive = profit. For `SpreadDirection::Long` we
/// buy Ext at entry / sell at exit, and sell Lt at entry / buy at exit;
/// Short is symmetric.
pub(super) fn paper_pnl_projection(
    ctx: &PaperEntryCtx,
    ext_exit: &MidSnapshot,
    lt_exit: &MidSnapshot,
    maker_exit: Option<bool>,
    ext_fee_bps: f64,
    lt_fee_bps: f64,
) -> Option<(f64, f64)> {
    let ext_entry_mid = ctx.ext_entry_mid.to_f64()?;
    let ext_entry_bid = ctx.ext_entry_bid.to_f64()?;
    let ext_entry_ask = ctx.ext_entry_ask.to_f64()?;
    let lt_entry_bid = ctx.lt_entry_bid.to_f64()?;
    let lt_entry_ask = ctx.lt_entry_ask.to_f64()?;
    let ext_exit_bid = ext_exit.bid.to_f64()?;
    let ext_exit_ask = ext_exit.ask.to_f64()?;
    let lt_exit_bid = lt_exit.bid.to_f64()?;
    let lt_exit_ask = lt_exit.ask.to_f64()?;
    if !(ext_entry_mid > 0.0
        && ext_entry_bid > 0.0
        && ext_entry_ask > 0.0
        && lt_entry_bid > 0.0
        && lt_entry_ask > 0.0
        && ext_exit_bid > 0.0
        && ext_exit_ask > 0.0
        && lt_exit_bid > 0.0
        && lt_exit_ask > 0.0)
    {
        return None;
    }
    // Per-leg fill prices. Ext is always taker (config flips
    // `extended_post_only: false`, see #302), so entry buys at ask /
    // sells at bid. Lt fill price depends on maker outcome: a maker
    // fill on the rest side captures the half-spread (sell at ask /
    // buy at bid for a position close); a taker fallback crosses the
    // touch (sell at bid / buy at ask).
    let (ext_buy_px, ext_sell_px, lt_sell_px, lt_buy_px) = match ctx.direction {
        SpreadDirection::Long => {
            // Entry: buy Ext at ask, sell Lt at maker_entry ? ask : bid.
            // Exit: sell Ext at bid (taker), buy Lt at maker_exit ? bid : ask.
            let lt_sell = if maker_exit_or_entry(ctx.maker_entry) {
                lt_entry_ask
            } else {
                lt_entry_bid
            };
            let lt_buy = if maker_exit_or_entry(maker_exit) {
                lt_exit_bid
            } else {
                lt_exit_ask
            };
            (ext_entry_ask, ext_exit_bid, lt_sell, lt_buy)
        }
        SpreadDirection::Short => {
            // Entry: sell Ext at bid, buy Lt at maker_entry ? bid : ask.
            // Exit: buy Ext at ask, sell Lt at maker_exit ? ask : bid.
            let lt_buy = if maker_exit_or_entry(ctx.maker_entry) {
                lt_entry_bid
            } else {
                lt_entry_ask
            };
            let lt_sell = if maker_exit_or_entry(maker_exit) {
                lt_exit_ask
            } else {
                lt_exit_bid
            };
            (ext_exit_ask, ext_entry_bid, lt_sell, lt_buy)
        }
    };
    // Per-share PnL across both legs. For Long the Ext leg profits on
    // (sell - buy) and the Lt leg on (sell - buy) of the short side;
    // Short is symmetric. Normalising to the Ext entry mid keeps the
    // bps figure stable across the small ext/lt mid difference.
    let pnl_per_unit = (ext_sell_px - ext_buy_px) + (lt_sell_px - lt_buy_px);
    let gross_bps = pnl_per_unit / ext_entry_mid * 10_000.0;
    let net_bps = gross_bps - 2.0 * ext_fee_bps - 2.0 * lt_fee_bps;
    Some((gross_bps, net_bps))
}

#[inline]
fn maker_exit_or_entry(flag: Option<bool>) -> bool {
    flag.unwrap_or(false)
}

/// Realised USD PnL for one live round-trip (#268 S5-1).
///
/// Gross: spread-direction-aware delta times the smaller of the two
/// exit fill qtys (the truly delta-neutral portion of the round
/// trip). For SpreadDirection::Long the position profits when the
/// spread widens (`exit_spread > entry_spread`); Short is symmetric.
///
/// Fees: per-leg, per-side. Each leg's notional is `mid * qty`;
/// the fee rate (`*_fee_bps`) applies to that notional. Entry +
/// exit fees on both venues sum into the total.
///
/// Pricing uses **actual volume-weighted average fill prices** when
/// the executor surfaced them via `*Terminal::Filled.avg_fill_price`,
/// and falls back to the mid price for the affected leg/side when
/// the executor did not (e.g. dry-run paper synthesis, reduce-only
/// Position-is-missing short-circuit). Pre-#435 this function used
/// mids unconditionally, which over-reported gross by ~10-12 bps/RT
/// on xvenue-arb at \$50 notional (the actual IOC slippage plus
/// post-only chase-up cost is invisible to a mid-based calculation).
/// See bot-strategy#435 for the diagnosis and ground-truth analysis.
///
/// The fee rate (`*_fee_bps`) is applied to the **fill-priced**
/// notional, so the fee charge matches what the venue's
/// realised_pnl export reports.
#[allow(clippy::too_many_arguments)]
pub(super) fn compute_realised_pnl(
    direction: SpreadDirection,
    ext_entry_mid: Decimal,
    lt_entry_mid: Decimal,
    ext_exit_mid: Decimal,
    lt_exit_mid: Decimal,
    ext_entry_avg_fill_price: Option<Decimal>,
    lt_entry_avg_fill_price: Option<Decimal>,
    ext_exit_avg_fill_price: Option<Decimal>,
    lt_exit_avg_fill_price: Option<Decimal>,
    ext_entry_qty: Decimal,
    lt_entry_qty: Decimal,
    ext_exit_qty: Decimal,
    lt_exit_qty: Decimal,
    ext_fee_bps: f64,
    lt_fee_bps: f64,
) -> Decimal {
    let realised_qty = ext_exit_qty.min(lt_exit_qty);
    if realised_qty <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let ext_entry_px = ext_entry_avg_fill_price.unwrap_or(ext_entry_mid);
    let lt_entry_px = lt_entry_avg_fill_price.unwrap_or(lt_entry_mid);
    let ext_exit_px = ext_exit_avg_fill_price.unwrap_or(ext_exit_mid);
    let lt_exit_px = lt_exit_avg_fill_price.unwrap_or(lt_exit_mid);
    let entry_spread = ext_entry_px - lt_entry_px;
    let exit_spread = ext_exit_px - lt_exit_px;
    let gross = match direction {
        SpreadDirection::Long => (exit_spread - entry_spread) * realised_qty,
        SpreadDirection::Short => (entry_spread - exit_spread) * realised_qty,
    };
    let bps_div = Decimal::new(10_000, 0);
    let ext_rate = Decimal::from_f64_retain(ext_fee_bps).unwrap_or(Decimal::ZERO) / bps_div;
    let lt_rate = Decimal::from_f64_retain(lt_fee_bps).unwrap_or(Decimal::ZERO) / bps_div;
    let ext_fees = (ext_entry_px * ext_entry_qty + ext_exit_px * ext_exit_qty) * ext_rate;
    let lt_fees = (lt_entry_px * lt_entry_qty + lt_exit_px * lt_exit_qty) * lt_rate;
    gross - ext_fees - lt_fees
}

/// bot-strategy#309 step 5: would-be Lighter maker fill outcome for
/// DRY_RUN soak telemetry. Models a single Bernoulli draw:
///   `p = clamp_to_unit(1 - our_size / depth_at_touch)`
/// with the queue side picked from the entry direction (Long → ask,
/// Short → bid). The model is intentionally simple — soak's job is to
/// feed the post-hoc analyst raw `(direction, size, depth)` tuples in
/// the log; this in-process draw is just a single-glance fill rate so
/// the operator can confirm the Phase 0 ≥ 50% gate without having to
/// re-derive it from logs every time.
///
/// Returns `None` when sizing or book is unavailable (caller should
/// skip the telemetry but still count the would-be attempt).
///
/// `seed` is mixed into the RNG for reproducibility — production
/// callers pass `now_ts_ms` so re-running analysis on a logged tuple
/// yields the same draw.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct WouldBeMakerOutcome {
    pub depth_eth: f64,
    pub our_size_eth: f64,
    pub fill_p: f64,
    pub sampled_fill: bool,
}

pub(super) fn would_be_maker_fill_outcome(
    dir: SpreadDirection,
    our_size: Decimal,
    lt_snap: &MidSnapshot,
    seed: u64,
) -> Option<WouldBeMakerOutcome> {
    let our_size_eth = our_size.to_f64().filter(|s| s.is_finite() && *s > 0.0)?;
    let depth = match dir {
        SpreadDirection::Long => lt_snap.ask_size,
        SpreadDirection::Short => lt_snap.bid_size,
    };
    let depth_eth = depth.to_f64().filter(|d| d.is_finite() && *d > 0.0)?;
    // Linear-decay-by-depth model: p = max(0, 1 - our_size / depth).
    // Bounded to [0, 1] so noisy book reads don't propagate as a >1
    // probability into the draw.
    let raw = 1.0 - (our_size_eth / depth_eth);
    let fill_p = raw.clamp(0.0, 1.0);
    // Deterministic sample from a per-decision seed. Using StdRng so
    // tests + post-hoc analysis can replay an exact log line and get
    // the same sampled_fill outcome the live bot recorded.
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let draw: f64 = rng.gen();
    let sampled_fill = draw < fill_p;
    Some(WouldBeMakerOutcome {
        depth_eth,
        our_size_eth,
        fill_p,
        sampled_fill,
    })
}

pub(super) fn paper_qty(notional_usd: f64, mid: Decimal) -> Result<Decimal> {
    if mid <= Decimal::ZERO {
        anyhow::bail!("non-positive mid");
    }
    let n = Decimal::from_f64_retain(notional_usd)
        .ok_or_else(|| anyhow::anyhow!("notional_usd not representable"))?;
    Ok((n / mid).round_dp(8))
}
