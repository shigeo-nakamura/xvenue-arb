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
