# Phase 0 — Cross-venue spread feasibility

Goal: decide whether the cross-venue spread between Lighter (Frankfurt)
and Extended (Tokyo) has a tradeable mean-reverting edge before building
any runtime code. As of 2026-04-25 (issue bot-strategy#166 comment) the
GO criterion uses persistence-filtered absolute-threshold stats (the v2
methodology); the legacy z-score / OU-half-life checks (v1) remain in
the script for cross-reference but do not gate the verdict.

GO (v2) iff:

- persistence-filtered annualised trades ∈ [500, 50,000] / year
- win rate ≥ 90% (real arb signals revert to mean reliably)
- net bps / trade ≥ 5 (clears 2.5 bps round-trip floor with margin)
- p90 hold time ≤ 600 s (longer holds run into funding cycles)

The 2026-04-23 v1 NO-GO verdict (OU half-life 5.2 s, 225k trades/year)
was a methodology artifact of counting bucket-scale noise as trades
without a persistence filter. The 2026-04-25 reanalysis on the same
dump shows v2 GO for both BTC (15k trades/yr, +9 bps net, 98% win) and
ETH (36k trades/yr, +25 bps net, 94% win). ETH is the leading symbol.

NO-GO → issue bot-strategy#166 is closed; no live infra is built.

## Prerequisites

- `ssh debot` and `ssh debot-tokyo` reachable.
- Tokyo Extended dump available for ≥ 7 consecutive days (bot-strategy#123
  Phase 3 collection completes ~2026-04-29 — running before then gives
  you a shorter but still usable sample).
- Python 3 with `numpy` and `pandas` (no scipy / statsmodels).

## Run

```bash
cd ~/bot/xvenue-arb

# 1. Pull dumps to /tmp/xvenue-phase0/{lighter,extended}/
scripts/phase0/fetch_data.sh

# 2. (recommended) Pull a Binance 1m reference for the same window
#    so stale-quote outliers can be attributed to the right venue.
scripts/phase0/fetch_reference.sh <epoch_ms_start> <epoch_ms_end>

# 3. Analyse — primary symbol ETH (per 2026-04-25 refinement).
python3 scripts/phase0/spread_analysis.py \
  --lighter-dir  /tmp/xvenue-phase0/lighter \
  --extended-dir /tmp/xvenue-phase0/extended \
  --reference-jsonl /tmp/xvenue-phase0/reference/binance_btcusdt_1m.jsonl \
  --drop-ref-deviation-bps 30 \
  --bucket-sec   5 \
  --roll-window-sec 1800 \
  --abs-threshold-bps 5 --persistence-sec 15 --max-hold-sec 600 \
  --roundtrip-bps 2.5 \
  --symbol ETH \
  --out-csv /tmp/xvenue-phase0/aligned_eth.csv

# Repeat with --symbol BTC for the secondary check.
```

Exit code is `0` on v2 GO and `1` on v2 NO-GO so the script fits into
an automated gate if we ever want one.

## Tuning levers

| Flag | Why change it |
|---|---|
| `--symbol` | `BTC` (default) or `ETH`. ETH has materially better arb economics on this venue pair — wider Lighter inside spread, larger cross-venue σ — so run with `--symbol ETH` first |
| `--abs-threshold-bps` | v2 entry threshold (absolute bps from rolling mean). Default 5 bps clears the 2.5 bps round-trip floor by 2× |
| `--persistence-sec` | v2 confirmation window. 15s is the time to confirm a signal + place both legs. Raise (e.g. 30s) if v2 trade count exceeds 50k/year (still trading noise) |
| `--max-hold-sec` | Force-close after this. 600s covers >99% of natural reverts in the preview window; longer holds run into funding cycles |
| `--bucket-sec` | Match the dump cadence. 5s tracks the live ~5s tick, 1s only helps if both venues dump faster |
| `--roll-window-sec` | The rolling window for the running mean used by the v2 simulator and z-score by v1. Default 1800s (30 min) tracks the funding-bias drift without over-fitting noise |
| `--reference-jsonl` + `--drop-ref-deviation-bps` | Strongly recommended. Drops buckets where Lighter or Extended deviates from Binance 1m mid by more than the threshold — protects against the stale-quote pattern that produced the +2182 bps phantom signal on 2026-04-21 |
| `--entry-z / --exit-z` | Legacy v1 only. Kept for back-compat reporting |
| `--roundtrip-bps` | Raise to model real execution (e.g. 5 bps if Extended ends up in taker path for both entry and exit). Default 2.5 bps assumes asymmetric leg execution: Lighter taker (0 fee) + Extended post-only-with-fallback |

## Output interpretation

The script prints two distinct stats blocks:

- **threshold stats v1 (z-score, legacy)**: counts every threshold
  crossing including bucket-scale oscillations. Trade counts here are
  inflated 100× and the v1 GO criterion (100..10k trades/year, 30s..1h
  half-life) routinely returns NO-GO even when real arb edge exists.
  Reported for back-compat only.
- **persistence-filtered stats v2 (PRIMARY as of 2026-04-25)**: requires
  the deviation to hold for `persistence-sec` before opening. Hold runs
  until the spread reverts past the rolling mean or `max-hold-sec` is
  hit. Win rate, hold time, and PnL are realistic.

Other things to look at:

- **spread mean ≠ 0**: systematic premium between venues (listing,
  funding baseline). The v2 simulator centers on the rolling mean so
  this is not an issue for the GO decision; v1 z-score also de-means.
- **top outliers**: the script prints the 5 largest pre-trim outliers
  and (if a Binance reference is loaded) attributes each to the venue
  whose deviation from the reference is larger. Persistent attribution
  to one venue indicates a feed-stall pattern; treat that venue's data
  with suspicion in Phase 1.
- **GO verdict**: proceed to Phase 1 (`ReplayConnector` dual-venue
  extension, proper BT).
- **NO-GO verdict**: close bot-strategy#166 referencing the numbers.

## Limitations of this analysis

- Assumes immediate fills at mid — ignores slippage beyond the
  `--roundtrip-bps` floor.
- Funding accrual is not modelled; for short holds it's a rounding
  error, for longer holds Phase 1 needs to add it.
- Dump cadence drift (#168 showed ~±30% jitter on pairtrade's own
  writer) means bucket alignment may drop ~5-10% of slots. Acceptable
  for a feasibility check.
