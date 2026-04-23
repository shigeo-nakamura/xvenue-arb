# Phase 0 — Cross-venue spread feasibility

Goal: decide whether the BTC mid-price spread between Lighter
(Frankfurt) and Extended (Tokyo) has a tradeable mean-reverting edge
before building any runtime code. GO iff the analysis shows

- annualised trade count ≥ ~100 at `entry_z=1.5 / exit_z=0.3`,
- expected gross bps per trade > round-trip cost floor (2.5 bps), and
- OU half-life < 1 hour (edge decays in a reasonable window).

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

# 2. Analyse. Defaults match DESIGN.md §6 / §9.
python3 scripts/phase0/spread_analysis.py \
  --lighter-dir  /tmp/xvenue-phase0/lighter \
  --extended-dir /tmp/xvenue-phase0/extended \
  --bucket-sec   5 \
  --roll-window-sec 1800 \
  --entry-z 1.5 --exit-z 0.3 \
  --roundtrip-bps 2.5 \
  --out-csv /tmp/xvenue-phase0/aligned.csv
```

Exit code is `0` on GO and `1` on NO-GO so the script fits into an
automated gate if we ever want one.

## Tuning levers

| Flag | Why change it |
|---|---|
| `--bucket-sec` | Match the dump cadence. 5s tracks the live ~5s tick, 1s only helps if both venues dump faster |
| `--roll-window-sec` | If the ACF suggests a different regime (e.g. minute-scale rather than 30-min) |
| `--entry-z / --exit-z` | Sensitivity check — DESIGN.md §0 fixes the starting pair but Phase 0 can suggest a different working point |
| `--roundtrip-bps` | Raise to model real execution (e.g. 5 bps if Extended ends up in taker path for both entry and exit) |

## Output interpretation

- **spread mean ≠ 0**: systematic premium between venues (listing,
  funding baseline). Not fatal — z-score already de-means.
- **ACF decays fast (< 0.1 by 1800 s)**: spread is noisy, limited
  auto-correlation for mid-horizon plays.
- **OU half-life 30 min-2 h**: typical for stat-arb with manageable
  holding cost.
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
