#!/usr/bin/env python3
"""Phase 0 cross-venue spread feasibility analysis (bot-strategy#166).

Reads Lighter and Extended pairtrade-style dumps for BTC, aligns them
on a common time bucket, and reports:

  - Spread distribution (mean, std, quantiles) in bps
  - Autocorrelation at 1/5/30/60/300/1800 sec lags
  - Ornstein-Uhlenbeck half-life estimate (OLS fit of Δs on s)
  - Entry-threshold trade count and expected gross PnL per trade
  - GO/NO-GO summary against the 2.5 bps round-trip cost lower bound

Both dump files share the pairtrade schema:
  {"timestamp": <epoch_ms>, "prices": {"BTC": {"bid_price": "...",
   "ask_price": "...", ...}, "ETH": {...}}}

Per §0 xvenue-arb only trades BTC in Phase 0-3, so ETH is ignored.

Usage:
  python3 scripts/phase0/spread_analysis.py \\
      --lighter-dir  /tmp/xvenue-phase0/lighter \\
      --extended-dir /tmp/xvenue-phase0/extended \\
      --bucket-sec   5 \\
      --entry-z      1.5 \\
      --exit-z       0.3 \\
      --roundtrip-bps 2.5

With Binance cross-check (bot-strategy#166 smoke test found Lighter-side
stale quotes; the reference anchors which venue is mis-quoting and lets
us drop those buckets cleanly):

  scripts/phase0/fetch_reference.sh 1776870000000 1776956400000
  python3 scripts/phase0/spread_analysis.py \\
      --lighter-dir  /tmp/xvenue-phase0/lighter \\
      --extended-dir /tmp/xvenue-phase0/extended \\
      --reference-jsonl /tmp/xvenue-phase0/reference/binance_btcusdt_1m.jsonl \\
      --drop-ref-deviation-bps 50

Requires: numpy, pandas (no scipy / statsmodels — we keep deps light).
"""

import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd


def parse_jsonl_mid(path: str, symbol: str = "BTC") -> pd.DataFrame:
    """Return a DataFrame with columns (ts_ms, bid, ask, mid) for `symbol`.

    Rows are dropped when:
      - the symbol is missing
      - bid or ask is non-positive, or ask < bid
      - **either side has zero size** — Lighter writes stale one-sided
        books into its dump (`bid_size:"0.0000"`); the displayed mid in
        that case has no tradeable counterparty on one side and skews
        the cross-venue spread by dozens of bps.
    """
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            prices = rec.get("prices") or {}
            p = prices.get(symbol)
            if not p:
                continue
            try:
                bid = float(p.get("bid_price", "0") or 0)
                ask = float(p.get("ask_price", "0") or 0)
                bid_sz = float(p.get("bid_size", "0") or 0)
                ask_sz = float(p.get("ask_size", "0") or 0)
            except (TypeError, ValueError):
                continue
            if bid <= 0 or ask <= 0 or ask < bid:
                continue
            if bid_sz <= 0 or ask_sz <= 0:
                continue
            ts_ms = rec.get("timestamp")
            if ts_ms is None:
                continue
            rows.append((int(ts_ms), bid, ask, 0.5 * (bid + ask)))
    return pd.DataFrame(rows, columns=["ts_ms", "bid", "ask", "mid"])


def load_venue(dir_: str, symbol: str = "BTC") -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(dir_, "*.jsonl")))
    if not files:
        raise SystemExit(f"No .jsonl files in {dir_}")
    frames = []
    for f in files:
        df = parse_jsonl_mid(f, symbol)
        frames.append(df)
        print(f"  loaded {len(df):>8d} rows from {os.path.basename(f)}", file=sys.stderr)
    df = pd.concat(frames, ignore_index=True).sort_values("ts_ms").drop_duplicates("ts_ms")
    return df


def load_reference(path: str) -> pd.DataFrame:
    """Load Binance 1m klines written by fetch_reference.sh.

    Returns a DataFrame indexed by bar open timestamp (ms) with column
    `mid_ref` = (high + low) / 2. Using mid-of-range instead of close
    gives us a value that straddles whatever intra-minute volatility
    happened, which is closer to a tradeable mid than the last-print
    close. The venue quotes are still fine-grained (tick) — we are only
    sanity-checking that they live within the minute's band.
    """
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            try:
                high = float(rec["high"])
                low = float(rec["low"])
            except (KeyError, TypeError, ValueError):
                continue
            if high <= 0 or low <= 0 or high < low:
                continue
            rows.append((int(rec["ts_ms"]), 0.5 * (high + low)))
    df = pd.DataFrame(rows, columns=["ts_ms", "mid_ref"]).sort_values("ts_ms")
    df = df.drop_duplicates("ts_ms").set_index("ts_ms")
    return df


def attach_reference(aligned: pd.DataFrame, ref: pd.DataFrame, bucket_sec: int) -> pd.DataFrame:
    """Annotate `aligned` (bucket-indexed DataFrame) with a `mid_ref`
    column by forward-filling the 1m reference onto each bucket.

    A bucket at ts gets the reference bar whose open is the greatest
    minute-open <= ts, i.e. the bar that the bucket lies inside. Buckets
    that fall before the first reference bar become NaN and are left
    as-is (so the caller can see coverage explicitly).
    """
    # Floor each bucket to the enclosing 1m bar; then ffill-fetch that
    # minute's reference mid. `reindex(method='ffill')` handles both the
    # enclosing-bar lookup and any gap in the kline series (rare for
    # Binance but safe against a missing minute).
    minute_keys = (aligned.index // 60_000) * 60_000
    ref_sorted = ref.sort_index()
    # reindex the reference directly at the minute keys we need; ffill
    # fills in any minute that fell into a gap in the upstream kline
    # data. Output length equals len(minute_keys), which matches aligned.
    ref_series = ref_sorted["mid_ref"].reindex(minute_keys, method="ffill").to_numpy()
    out = aligned.copy()
    out["mid_ref"] = ref_series
    return out


def align_buckets(lt: pd.DataFrame, ext: pd.DataFrame, bucket_sec: int) -> pd.DataFrame:
    """Bucket both series to `bucket_sec` seconds (taking the LAST mid in
    each bucket), then inner-join. Returns a DataFrame indexed by
    bucket ts with columns `mid_lt`, `mid_ext`."""
    bkt_ms = bucket_sec * 1000
    lt = lt.copy()
    ext = ext.copy()
    lt["bucket"] = (lt["ts_ms"] // bkt_ms) * bkt_ms
    ext["bucket"] = (ext["ts_ms"] // bkt_ms) * bkt_ms
    # Last-in-bucket mid — matches how a live bot would snap its own
    # orderbook just before placing an order.
    lt_b = lt.groupby("bucket", as_index=True)["mid"].last().rename("mid_lt")
    ext_b = ext.groupby("bucket", as_index=True)["mid"].last().rename("mid_ext")
    return pd.concat([lt_b, ext_b], axis=1, join="inner").sort_index()


def compute_spread_bps(df: pd.DataFrame) -> pd.Series:
    return ((df["mid_ext"] - df["mid_lt"]) / df["mid_lt"] * 10_000.0).astype(float)


def autocorrelation(x: np.ndarray, lag: int) -> float:
    if lag <= 0 or lag >= len(x):
        return float("nan")
    a = x[:-lag] - x[:-lag].mean()
    b = x[lag:] - x[lag:].mean()
    denom = np.sqrt((a * a).sum() * (b * b).sum())
    if denom == 0:
        return float("nan")
    return float((a * b).sum() / denom)


def ou_half_life(x: np.ndarray, dt_sec: int) -> float:
    """OLS estimate of OU half-life from Δs = -κ (s - μ) dt + σ dW.

    Regress Δx on (x - mean) centered around 0; slope = -κ * dt.
    half_life = ln(2) / κ. Returns np.inf if κ ≤ 0 (non-reverting).
    """
    if len(x) < 50:
        return float("nan")
    dx = np.diff(x)
    xc = x[:-1] - x[:-1].mean()
    # Slope = cov(xc, dx) / var(xc); matches np.polyfit deg=1 (minus intercept).
    var = (xc * xc).sum()
    if var == 0:
        return float("nan")
    slope = (xc * dx).sum() / var
    kappa_per_bucket = -slope
    if kappa_per_bucket <= 0:
        return float("inf")
    kappa_per_sec = kappa_per_bucket / dt_sec
    return float(np.log(2.0) / kappa_per_sec)


@dataclass
class ThresholdStats:
    entries: int
    expected_gross_bps: float
    total_gross_bps: float
    annualized_trades: float


@dataclass
class PersistenceStats:
    """Persistence-filtered trade simulation results.

    Discovered 2026-04-25 (issue #166 comment): the original z-score-based
    `threshold_trade_count` counts every threshold crossing, including
    bucket-scale oscillations around the mean. With 5s buckets and a
    spread that mean-reverts on a few-second timescale, this conflates
    noise with edge and inflates trade counts ~100×.

    The persistence-filtered model only opens a trade if the deviation
    has held for at least `persistence_sec` seconds, simulating the time
    needed in practice to (1) confirm the signal and (2) get both legs
    filled. It then holds until the spread reverts past the running mean
    or `max_hold_sec` is reached. Win rate, hold time, and PnL match what
    a real bot can capture far more closely than the threshold-crossing
    count.
    """
    trades: int
    avg_gross_bps: float
    median_hold_sec: float
    p90_hold_sec: float
    win_rate: float
    annualized_trades: float
    annualized_gross_bps: float


def persistence_filtered_trade_stats(
    spread_bps: np.ndarray,
    mean_offset: np.ndarray,
    abs_threshold_bps: float,
    persistence_buckets: int,
    max_hold_buckets: int,
    bucket_sec: int,
    strict_entry: bool = False,
    timestamps_ms: "np.ndarray | None" = None,
    trades_csv_path: "str | None" = None,
) -> PersistenceStats:
    """Simulate trades that require sustained breach before entry.

    Algorithm:
      1. At each bucket i, compute dev = spread_bps[i] - mean_offset[i].
      2. If |dev| >= abs_threshold_bps and the *next* persistence_buckets
         all have dev on the same side and >= threshold, open a trade at
         bucket (i + persistence_buckets) — i.e., enter only after the
         signal has confirmed itself.
      3. Hold until dev crosses zero (the running mean) or max_hold_buckets
         elapse. Capture = sign * (entry_dev - exit_dev), positive iff
         the spread reverted toward mean during the hold.

    `mean_offset` is typically a slow EMA / rolling mean of the spread,
    so trades center on the structural Lighter-Extended basis (which is
    ~+2 bps for BTC and ~+3 bps for ETH per 2026-04-22..24 dump) rather
    than zero. Without this, the strategy systematically over-fires on
    the natural-state side of the basis.
    """
    n = len(spread_bps)
    if n == 0:
        return PersistenceStats(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    grosses = []
    holds_buckets = []
    trade_rows = [] if trades_csv_path else None
    i = 0
    while i < n:
        dev = spread_bps[i] - mean_offset[i]
        if abs(dev) < abs_threshold_bps or np.isnan(dev):
            i += 1
            continue
        breach_side = 1 if dev > 0 else -1
        # Confirm: next persistence_buckets all on same side, all over threshold
        confirm_end = min(i + persistence_buckets, n)
        if confirm_end - i < persistence_buckets:
            break
        confirmed = True
        for j in range(i, confirm_end):
            d = spread_bps[j] - mean_offset[j]
            if np.isnan(d) or d * breach_side < abs_threshold_bps:
                confirmed = False
                break
        if not confirmed:
            i += 1
            continue
        # Open at the bar AFTER the confirmation window
        entry_idx = min(i + persistence_buckets, n - 1)
        entry_dev = spread_bps[entry_idx] - mean_offset[entry_idx]
        # Strict-entry mode (bot-strategy#166 parity diagnostic): only
        # open if dev at the entry bar is *still* past threshold on the
        # same side. Mirrors the live SignalEngine's "dev must still
        # confirm at fire tick" rule, and rejects entries booked at a
        # near-mean dev (which inflates v2's apparent win rate by
        # capturing the tail of the revert as an "arb"). Falling back
        # to skipping the candidate entirely matches what a live bot
        # would do — there's no order to place once dev has reverted.
        if strict_entry:
            if np.isnan(entry_dev) or entry_dev * breach_side < abs_threshold_bps:
                i += 1
                continue
        # Hold until revert past mean or max_hold reached
        exit_idx = entry_idx
        exit_dev = entry_dev
        for k in range(entry_idx + 1, min(entry_idx + max_hold_buckets, n)):
            dk = spread_bps[k] - mean_offset[k]
            if np.isnan(dk):
                continue
            if dk * breach_side <= 0:
                exit_idx = k
                exit_dev = dk
                break
            exit_idx = k
            exit_dev = dk
        gross = (entry_dev - exit_dev) * breach_side
        grosses.append(gross)
        holds_buckets.append(exit_idx - entry_idx)
        if trade_rows is not None:
            entry_ts = (
                int(timestamps_ms[entry_idx])
                if timestamps_ms is not None
                else entry_idx * bucket_sec * 1000
            )
            exit_ts = (
                int(timestamps_ms[exit_idx])
                if timestamps_ms is not None
                else exit_idx * bucket_sec * 1000
            )
            trade_rows.append(
                {
                    "entry_ts_ms": entry_ts,
                    "exit_ts_ms": exit_ts,
                    "entry_idx": entry_idx,
                    "exit_idx": exit_idx,
                    "direction": "Short" if breach_side == 1 else "Long",
                    "entry_spread": float(spread_bps[entry_idx]),
                    "exit_spread": float(spread_bps[exit_idx]),
                    "entry_dev": float(entry_dev),
                    "exit_dev": float(exit_dev),
                    "entry_mu": float(mean_offset[entry_idx]),
                    "exit_mu": float(mean_offset[exit_idx]),
                    "gross_bps": float(gross),
                    "hold_buckets": int(exit_idx - entry_idx),
                }
            )
        i = exit_idx + 1

    if trade_rows is not None:
        import csv as _csv

        with open(trades_csv_path, "w", newline="") as fh:
            if trade_rows:
                w = _csv.DictWriter(fh, fieldnames=list(trade_rows[0].keys()))
                w.writeheader()
                w.writerows(trade_rows)
            else:
                fh.write("entry_ts_ms,exit_ts_ms,entry_idx,exit_idx,direction,entry_spread,exit_spread,entry_dev,exit_dev,entry_mu,exit_mu,gross_bps,hold_buckets\n")

    if not grosses:
        return PersistenceStats(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    grosses_arr = np.asarray(grosses)
    holds_arr = np.asarray(holds_buckets)
    sample_secs = n * bucket_sec
    annualized = len(grosses) / max(1, sample_secs) * (365 * 86400)
    avg = float(grosses_arr.mean())
    return PersistenceStats(
        trades=len(grosses),
        avg_gross_bps=avg,
        median_hold_sec=float(np.median(holds_arr) * bucket_sec),
        p90_hold_sec=float(np.quantile(holds_arr, 0.9) * bucket_sec),
        win_rate=float((grosses_arr > 0).mean()),
        annualized_trades=annualized,
        annualized_gross_bps=avg * annualized,
    )


def threshold_trade_count(
    z: np.ndarray,
    spread_bps: np.ndarray,
    entry_z: float,
    exit_z: float,
    bucket_sec: int,
) -> ThresholdStats:
    """Count entries (|z| > entry_z crossings) and estimate gross PnL.

    Simplification: each entry closes at the next |z| < exit_z crossing
    (or at end of sample). Gross PnL per trade = sign(z_entry) *
    (spread_bps_entry - spread_bps_exit), which is positive iff the
    spread reverted toward the mean. No funding / fees applied here —
    those are subtracted by the caller against the round-trip floor.
    """
    entries = 0
    total_bps = 0.0
    in_trade = False
    entry_spread = 0.0
    entry_sign = 0
    for i, zi in enumerate(z):
        if not in_trade:
            if zi > entry_z:
                in_trade = True
                entry_sign = +1  # short ext / long lt
                entry_spread = spread_bps[i]
                entries += 1
            elif zi < -entry_z:
                in_trade = True
                entry_sign = -1  # long ext / short lt
                entry_spread = spread_bps[i]
                entries += 1
        else:
            if abs(zi) < exit_z:
                total_bps += entry_sign * (entry_spread - spread_bps[i])
                in_trade = False
    # Unclosed trade at end: assume exit at current z (neutral, won't skew much)
    if in_trade:
        total_bps += entry_sign * (entry_spread - spread_bps[-1])

    sample_days = max(1e-9, (len(z) * bucket_sec) / 86400.0)
    annualized = entries / sample_days * 365.0
    expected = total_bps / entries if entries > 0 else 0.0
    return ThresholdStats(
        entries=entries,
        expected_gross_bps=expected,
        total_gross_bps=total_bps,
        annualized_trades=annualized,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lighter-dir", required=True)
    ap.add_argument("--extended-dir", required=True)
    ap.add_argument("--bucket-sec", type=int, default=5)
    ap.add_argument("--roll-window-sec", type=int, default=1800,
                    help="Rolling window for μ/σ (used for z-score series).")
    ap.add_argument("--entry-z", type=float, default=1.5)
    ap.add_argument("--exit-z", type=float, default=0.3)
    ap.add_argument("--roundtrip-bps", type=float, default=2.5,
                    help="Round-trip cost floor (Ext maker 0 + Lt 0 + slippage).")
    ap.add_argument("--max-abs-bps", type=float, default=100.0,
                    help="Trim spreads with |s| > this for distribution stats (feed-stall guard).")
    ap.add_argument("--reference-jsonl", default=None,
                    help="Optional Binance 1m klines JSONL (see fetch_reference.sh). "
                         "When supplied, each venue's mid is cross-checked against the "
                         "reference and a data-quality section is printed.")
    ap.add_argument("--drop-ref-deviation-bps", type=float, default=None,
                    help="When a reference is loaded, drop buckets where either venue "
                         "deviates more than this many bps from the reference. "
                         "Typical values: 30-50 bps. If unset, reference is used for "
                         "diagnostics only and no rows are filtered.")
    ap.add_argument("--symbol", default="BTC", choices=("BTC", "ETH"),
                    help="Which symbol to analyse. Defaults to BTC; the 2026-04-25 "
                         "Phase 0 refinement (issue #166 comment) found ETH has "
                         "materially better arb economics on this venue pair.")
    ap.add_argument("--abs-threshold-bps", type=float, default=5.0,
                    help="Absolute |spread - rolling_mean| threshold (bps) for the "
                         "persistence-filtered trade simulator. Replaces the z-score "
                         "in the v2 GO criterion. Default 5 bps clears the 2.5 bps "
                         "round-trip cost floor with margin.")
    ap.add_argument("--persistence-sec", type=int, default=15,
                    help="Seconds the deviation must hold before a trade opens "
                         "in the persistence-filtered simulator. 15s is roughly "
                         "the time needed to confirm signal + place both legs.")
    ap.add_argument("--max-hold-sec", type=int, default=600,
                    help="Maximum seconds to hold an open arb trade before "
                         "force-closing in the persistence-filtered simulator. "
                         "10 min covers >99% of natural reverts in the 2026-04-22..24 "
                         "preview window.")
    ap.add_argument("--out-csv", default=None,
                    help="Optional: write aligned (ts, spread_bps, z) CSV for plotting.")
    ap.add_argument("--out-trades-csv", default=None,
                    help="Write the persistence-filtered trade list to this "
                         "CSV path. Used for Rust BT parity diff.")
    ap.add_argument("--strict-entry", action="store_true",
                    help="Reject candidates whose dev at entry_idx (i + "
                         "persistence_buckets) has already reverted below "
                         "abs_threshold_bps. Mirrors the live bot's "
                         "SignalEngine entry rule. Without this, v2 opens "
                         "trades at near-mean dev values that book the tail "
                         "of the revert as a gross-bps capture (likely "
                         "inflating Phase 0 v2 GO numbers — see "
                         "bot-strategy#166).")
    args = ap.parse_args()

    print(f"[load] Lighter  from {args.lighter_dir} (symbol={args.symbol})", file=sys.stderr)
    lt = load_venue(args.lighter_dir, args.symbol)
    print(f"[load] Extended from {args.extended_dir} (symbol={args.symbol})", file=sys.stderr)
    ext = load_venue(args.extended_dir, args.symbol)

    print(f"[align] bucket_sec={args.bucket_sec}", file=sys.stderr)
    aligned = align_buckets(lt, ext, args.bucket_sec)
    if aligned.empty:
        raise SystemExit("No overlapping buckets between venues.")

    ref_loaded = False
    if args.reference_jsonl:
        print(f"[ref] loading {args.reference_jsonl}", file=sys.stderr)
        ref_df = load_reference(args.reference_jsonl)
        if ref_df.empty:
            print("[ref] WARN: reference file produced zero rows — ignoring", file=sys.stderr)
        else:
            aligned = attach_reference(aligned, ref_df, args.bucket_sec)
            # Per-venue deviation from the external reference, in bps.
            aligned["dev_lt_bps"] = (
                (aligned["mid_lt"] - aligned["mid_ref"]) / aligned["mid_ref"] * 10_000.0
            )
            aligned["dev_ext_bps"] = (
                (aligned["mid_ext"] - aligned["mid_ref"]) / aligned["mid_ref"] * 10_000.0
            )
            ref_loaded = True

    spread = compute_spread_bps(aligned)
    aligned["spread_bps"] = spread

    # Optional reference-based pre-filter: if the caller gave us a
    # --drop-ref-deviation-bps threshold, drop any bucket where either
    # venue's mid deviates from the Binance 1m reference by more than
    # that many bps *before* any other stat is computed. This is the
    # strongest stale-quote guard we have: it removes the whole row
    # rather than just trimming the cross-venue spread, and keeps the
    # rolling μ/σ / OU half-life / threshold trade count honest.
    ref_prefilter_dropped = 0
    if ref_loaded and args.drop_ref_deviation_bps is not None:
        thr = args.drop_ref_deviation_bps
        dev_mask = (
            aligned["dev_lt_bps"].abs().fillna(np.inf).le(thr)
            & aligned["dev_ext_bps"].abs().fillna(np.inf).le(thr)
        )
        ref_prefilter_dropped = int((~dev_mask).sum())
        aligned = aligned[dev_mask]
        if aligned.empty:
            raise SystemExit(
                f"Reference pre-filter at {thr} bps dropped every bucket — "
                "widen the threshold or inspect the reference vs venue dumps."
            )
        spread = aligned["spread_bps"]

    # Robust outlier trim. Cross-venue spreads of hundreds of bps come
    # from feed stalls (one venue's quote frozen while the other moves
    # through a liquidity event), not from tradeable dislocations, and
    # dominate σ + bias the backtest if left in. We zero them out from
    # the working series so rolling stats, z-score, and threshold trades
    # all see the clean dataset.
    med = spread.median()
    mad = float((spread - med).abs().median()) or 1.0
    robust_sigma = 1.4826 * mad
    cutoff = max(args.max_abs_bps, 10 * robust_sigma)
    trim_mask = spread.abs() <= cutoff
    dropped = int((~trim_mask).sum())

    aligned_raw = aligned.copy()  # preserve for outlier diagnostics
    aligned = aligned[trim_mask]
    spread = aligned["spread_bps"]

    # Rolling z-score (centered on trailing window — lookahead-free)
    win = max(10, args.roll_window_sec // args.bucket_sec)
    mu = spread.rolling(win, min_periods=max(10, win // 4)).mean()
    sd = spread.rolling(win, min_periods=max(10, win // 4)).std()
    z = (spread - mu) / sd
    aligned["z"] = z

    # Drop warmup NaNs from the rolling window
    valid = aligned.dropna(subset=["z"])

    s = valid["spread_bps"].to_numpy()
    s_all = s  # alias — trimmed series is the canonical one from here
    zs = valid["z"].to_numpy()

    dur_days = (valid.index.max() - valid.index.min()) / 1000.0 / 86400.0
    print()
    print("=" * 68)
    print("Phase 0 cross-venue spread analysis (BTC)")
    print("=" * 68)
    print(f"aligned buckets     : {len(valid):>10,d}  ({dur_days:.2f} days of overlap)")
    print(f"bucket size         : {args.bucket_sec:>10d} s")
    print(f"rolling window      : {args.roll_window_sec:>10d} s  ({win} buckets)")
    print(f"outlier trim cutoff : {cutoff:>10.1f} bps  (dropped {dropped} / {len(spread)} rows)")
    print()
    print(f"spread mean (trimmed): {s.mean():+.3f} bps")
    print(f"spread std  (trimmed): {s.std():.3f} bps")
    print(f"robust σ  (MAD*1.48) : {robust_sigma:.3f} bps")
    raw_max = float(aligned_raw["spread_bps"].abs().max())
    print(f"spread |max| (raw)  : {raw_max:.3f} bps")
    print("spread quantiles (trimmed, bps):")
    for q in [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]:
        print(f"  p{int(q*100):02d}               : {np.quantile(s, q):+.3f}")
    print()
    # Show top-5 outliers so the user can see what the filter cut.
    # When a reference is loaded we also print per-venue deviation, which
    # tells us *which* side was stale — the prior version only showed the
    # cross-venue delta, leaving root-cause attribution to manual lookup.
    outlier_rows = aligned_raw[~trim_mask.reindex(aligned_raw.index).fillna(True)]
    if len(outlier_rows) > 0:
        print(f"top outliers (|s| > {cutoff:.0f} bps, max 5 shown):")
        top = outlier_rows.reindex(
            outlier_rows["spread_bps"].abs().sort_values(ascending=False).index
        ).head(5)
        for ts_ms, row in top.iterrows():
            base = (
                f"  ts={pd.Timestamp(int(ts_ms), unit='ms', tz='UTC')}  "
                f"spread={row['spread_bps']:+.1f} bps  "
                f"lt={row['mid_lt']:.2f}  ext={row['mid_ext']:.2f}"
            )
            if ref_loaded and "mid_ref" in row and pd.notna(row["mid_ref"]):
                dev_lt = row.get("dev_lt_bps", float("nan"))
                dev_ext = row.get("dev_ext_bps", float("nan"))
                culprit = "lt" if abs(dev_lt) > abs(dev_ext) else "ext"
                print(
                    f"{base}  "
                    f"ref={row['mid_ref']:.2f}  "
                    f"dev_lt={dev_lt:+.1f}bps  dev_ext={dev_ext:+.1f}bps  "
                    f"[stale={culprit}]"
                )
            else:
                print(base)
        print()

    if ref_loaded:
        # Data-quality snapshot: how often each venue strays from the
        # external reference, and by how much. A healthy venue lives
        # within a few bps of Binance spot for BTC; persistent excursions
        # are stale-quote evidence. Thresholds below are diagnostic, not
        # used for filtering unless --drop-ref-deviation-bps is set.
        dev_lt = aligned["dev_lt_bps"].dropna()
        dev_ext = aligned["dev_ext_bps"].dropna()
        # Count rows where reference itself is missing (buckets before
        # the first reference bar, or if the ref file didn't cover the
        # window) so we can warn about low coverage.
        ref_missing = int(aligned["mid_ref"].isna().sum())
        print("data quality vs Binance 1m reference:")
        print(f"  reference coverage : {len(aligned) - ref_missing:>8d} / {len(aligned)} buckets"
              f"  (missing {ref_missing})")
        for label, series in (("Lighter ", dev_lt), ("Extended", dev_ext)):
            if series.empty:
                print(f"  {label} deviation  : (no data)")
                continue
            abs_s = series.abs()
            print(f"  {label} deviation  : "
                  f"mean={series.mean():+.2f} bps  "
                  f"median={series.median():+.2f}  "
                  f"p95|dev|={abs_s.quantile(0.95):.2f}  "
                  f"p99|dev|={abs_s.quantile(0.99):.2f}  "
                  f"max|dev|={abs_s.max():.2f}")
            for thr in (10.0, 30.0, 100.0):
                pct = (abs_s > thr).mean() * 100.0
                print(f"    |dev| > {thr:>5.0f} bps    : {pct:>6.3f}%"
                      f"   ({int((abs_s > thr).sum())} buckets)")
        if args.drop_ref_deviation_bps is not None:
            print(f"  pre-filter @ {args.drop_ref_deviation_bps:.0f} bps: "
                  f"dropped {ref_prefilter_dropped} buckets before stats.")
        print()
    print("autocorrelation (trimmed, de-meaned spread):")
    for lag_sec in [1, 5, 30, 60, 300, 1800, 3600]:
        lag_b = max(1, lag_sec // args.bucket_sec)
        print(f"  lag {lag_sec:>5d}s         : {autocorrelation(s, lag_b):+.4f}")
    print()
    hl = ou_half_life(s, args.bucket_sec)
    print(f"OU half-life (approx): {hl:.1f} s")
    print()

    print(f"threshold stats v1 (z-score, entry_z={args.entry_z}, exit_z={args.exit_z}):")
    print("  (legacy methodology — counts every threshold crossing, including bucket noise.")
    print("   Kept for back-compat. The v2 metric below is the primary GO/NO-GO driver as of 2026-04-25.)")
    ts_stats = threshold_trade_count(zs, s, args.entry_z, args.exit_z, args.bucket_sec)
    print(f"  entries in sample  : {ts_stats.entries}")
    print(f"  annualized trades  : {ts_stats.annualized_trades:.1f} / year")
    print(f"  gross bps / trade  : {ts_stats.expected_gross_bps:+.3f}")
    print(f"  total gross bps    : {ts_stats.total_gross_bps:+.1f}")
    net_per_trade = ts_stats.expected_gross_bps - args.roundtrip_bps
    print(f"  net bps / trade    : {net_per_trade:+.3f}  (round-trip floor {args.roundtrip_bps} bps)")
    net_annual_bps = net_per_trade * ts_stats.annualized_trades
    print(f"  net bps / year     : {net_annual_bps:+.1f}")
    print()

    # v2 methodology (2026-04-25, issue #166 comment): persistence-filtered
    # absolute-threshold trade simulator. Centers on rolling mean μ rather
    # than zero, so the structural Lighter-Extended basis (~+2 bps for BTC,
    # +3 bps for ETH) doesn't bias the signal.
    persist_buckets = max(1, args.persistence_sec // args.bucket_sec)
    max_hold_buckets = max(1, args.max_hold_sec // args.bucket_sec)
    mu_arr = mu.reindex(valid.index).to_numpy()
    valid_ts = valid.index.to_numpy()  # bucket-start ts in ms
    p_stats = persistence_filtered_trade_stats(
        spread_bps=s,
        mean_offset=mu_arr,
        abs_threshold_bps=args.abs_threshold_bps,
        persistence_buckets=persist_buckets,
        max_hold_buckets=max_hold_buckets,
        bucket_sec=args.bucket_sec,
        strict_entry=args.strict_entry,
        timestamps_ms=valid_ts,
        trades_csv_path=args.out_trades_csv,
    )
    entry_label = "strict" if args.strict_entry else "v2-default"
    print(f"persistence-filtered stats v2 "
          f"(threshold={args.abs_threshold_bps} bps, "
          f"persist={args.persistence_sec}s, max_hold={args.max_hold_sec}s, "
          f"entry={entry_label}):")
    print(f"  trades in sample   : {p_stats.trades}")
    print(f"  annualized trades  : {p_stats.annualized_trades:.0f} / year")
    print(f"  win rate           : {p_stats.win_rate*100:.1f}%")
    print(f"  median hold        : {p_stats.median_hold_sec:.0f} s")
    print(f"  p90 hold           : {p_stats.p90_hold_sec:.0f} s")
    print(f"  gross bps / trade  : {p_stats.avg_gross_bps:+.2f}")
    p_net_per_trade = p_stats.avg_gross_bps - args.roundtrip_bps
    print(f"  net bps / trade    : {p_net_per_trade:+.2f}  (round-trip floor {args.roundtrip_bps} bps)")
    p_net_annual_bps = p_net_per_trade * p_stats.annualized_trades
    print(f"  net bps / year     : {p_net_annual_bps:+.0f}")
    print()

    # v2 GO criteria (PRIMARY as of 2026-04-25):
    #  - persistence-filtered trade count in a sane band (500..50k/year)
    #  - win rate > 90% (real arb opportunities are nearly always profitable
    #    when the persistence filter cuts noise; below 90% suggests we are
    #    still picking up trend-thru events rather than mean-reversion)
    #  - net bps/trade > 5 (clears 2.5 bps cost floor with 2× margin)
    #  - p90 hold < 10 min (longer holds are funding-sensitive and the
    #    backtester's "hold to mean revert" assumption breaks down)
    go_v2_freq = 500 <= p_stats.annualized_trades <= 50_000
    go_v2_win = p_stats.win_rate >= 0.90
    go_v2_net = p_net_per_trade >= 5.0
    go_v2_hold = p_stats.p90_hold_sec <= 600
    go_v2 = go_v2_freq and go_v2_win and go_v2_net and go_v2_hold

    # v1 (legacy) GO retained as secondary signal for back-compat
    go_bps = ts_stats.expected_gross_bps > args.roundtrip_bps
    go_freq = 100 <= ts_stats.annualized_trades <= 10_000
    min_hl_sec = max(30, 2 * args.bucket_sec)
    go_hl = np.isfinite(hl) and min_hl_sec <= hl <= 3600
    go_v1 = go_bps and go_freq and go_hl

    go = go_v2  # v2 is the primary verdict as of 2026-04-25

    print("=" * 68)
    verdict = "GO" if go else "NO-GO"
    print(f"Phase 0 verdict (v2): {verdict}   (v1 legacy: {'GO' if go_v1 else 'NO-GO'})")
    print("=" * 68)
    if not go:
        print("v2 reasons to reconsider:")
        if not go_v2_freq:
            if p_stats.annualized_trades < 500:
                print(f"  - annualized trades ({p_stats.annualized_trades:.0f}) < 500 — "
                      "too few real signals to pay fixed costs")
            else:
                print(f"  - annualized trades ({p_stats.annualized_trades:.0f}) > 50000 — "
                      "still trading noise even after persistence filter; "
                      "raise --abs-threshold-bps or --persistence-sec")
        if not go_v2_win:
            print(f"  - win rate {p_stats.win_rate*100:.1f}% < 90% — "
                  f"persistence-filtered signals aren't mean-reverting reliably; "
                  f"may be trending-through events (raise threshold or persist)")
        if not go_v2_net:
            print(f"  - net/trade ({p_net_per_trade:+.2f} bps) < 5 bps — "
                  f"insufficient margin over round-trip cost floor")
        if not go_v2_hold:
            print(f"  - p90 hold ({p_stats.p90_hold_sec:.0f} s) > 600 s — "
                  f"holds run into funding cycles, simple revert model breaks down")
    if dur_days < 7:
        print()
        print(f"NOTE: only {dur_days:.2f} days of overlap. A 7-day sample is required for")
        print("      the real GO decision (see bot-strategy#123 Phase 3 completion).")

    if args.out_csv:
        valid.to_csv(args.out_csv, index=True)
        print(f"\n[csv] wrote {args.out_csv}", file=sys.stderr)

    return 0 if go else 1


if __name__ == "__main__":
    sys.exit(main())
