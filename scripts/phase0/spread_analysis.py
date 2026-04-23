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
    ap.add_argument("--out-csv", default=None,
                    help="Optional: write aligned (ts, spread_bps, z) CSV for plotting.")
    args = ap.parse_args()

    print(f"[load] Lighter  from {args.lighter_dir}", file=sys.stderr)
    lt = load_venue(args.lighter_dir)
    print(f"[load] Extended from {args.extended_dir}", file=sys.stderr)
    ext = load_venue(args.extended_dir)

    print(f"[align] bucket_sec={args.bucket_sec}", file=sys.stderr)
    aligned = align_buckets(lt, ext, args.bucket_sec)
    if aligned.empty:
        raise SystemExit("No overlapping buckets between venues.")

    spread = compute_spread_bps(aligned)
    aligned["spread_bps"] = spread

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
    outlier_rows = aligned_raw[~trim_mask.reindex(aligned_raw.index).fillna(True)]
    if len(outlier_rows) > 0:
        print(f"top outliers (|s| > {cutoff:.0f} bps, max 5 shown):")
        top = outlier_rows.reindex(
            outlier_rows["spread_bps"].abs().sort_values(ascending=False).index
        ).head(5)
        for ts_ms, row in top.iterrows():
            print(f"  ts={pd.Timestamp(int(ts_ms), unit='ms', tz='UTC')}  "
                  f"spread={row['spread_bps']:+.1f} bps  "
                  f"lt={row['mid_lt']:.2f}  ext={row['mid_ext']:.2f}")
        print()
    print("autocorrelation (trimmed, de-meaned spread):")
    for lag_sec in [1, 5, 30, 60, 300, 1800, 3600]:
        lag_b = max(1, lag_sec // args.bucket_sec)
        print(f"  lag {lag_sec:>5d}s         : {autocorrelation(s, lag_b):+.4f}")
    print()
    hl = ou_half_life(s, args.bucket_sec)
    print(f"OU half-life (approx): {hl:.1f} s")
    print()

    print(f"threshold stats (entry_z={args.entry_z}, exit_z={args.exit_z}):")
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

    # GO criteria (all must hold):
    #  - gross bps/trade beats the round-trip floor
    #  - annualized trades in a sane band; > 100 ensures sample, < 10k
    #    ensures we are not just re-entering on noise
    #  - OU half-life longer than the bucket cadence (otherwise we are
    #    chasing white noise faster than we can observe it) and shorter
    #    than one hour (so holds and funding bars do not erase the edge)
    go_bps = ts_stats.expected_gross_bps > args.roundtrip_bps
    go_freq = 100 <= ts_stats.annualized_trades <= 10_000
    min_hl_sec = max(30, 2 * args.bucket_sec)
    go_hl = np.isfinite(hl) and min_hl_sec <= hl <= 3600
    go = go_bps and go_freq and go_hl

    print("=" * 68)
    verdict = "GO" if go else "NO-GO"
    print(f"Phase 0 verdict     : {verdict}")
    print("=" * 68)
    if not go:
        print("Reasons to reconsider:")
        if not go_bps:
            print(f"  - gross/trade ({ts_stats.expected_gross_bps:+.2f} bps) "
                  f"<= round-trip floor ({args.roundtrip_bps} bps)")
        if not go_freq:
            if ts_stats.annualized_trades < 100:
                print(f"  - annualized trades ({ts_stats.annualized_trades:.0f}) < 100 — too few to pay fixed costs")
            else:
                print(f"  - annualized trades ({ts_stats.annualized_trades:.0f}) > 10000 — "
                      "we are churning, likely trading noise")
        if not go_hl:
            if not np.isfinite(hl):
                print(f"  - OU half-life non-reverting — no mean reversion detected")
            elif hl < min_hl_sec:
                print(f"  - OU half-life ({hl:.1f} s) < {min_hl_sec:.0f} s — edge decays inside the bucket cadence")
            else:
                print(f"  - OU half-life ({hl:.1f} s) > 3600 s — edge decays too slowly for funding-sensitive holds")
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
