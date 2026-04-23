#!/bin/bash
# Fetch Binance BTCUSDT 1m klines for the analysis window so
# spread_analysis.py can cross-check each venue's mid against an
# independent reference.
#
# Rationale: smoke-test of 04-22/23 showed 5 top outliers all at Lighter
# $70k / Extended $78k. Without a third reference we cannot attribute
# the dislocation to a specific venue, only drop it.  See bot-strategy#166
# comment (2026-04-23 smoke test).
#
# Usage:
#   scripts/phase0/fetch_reference.sh START_MS END_MS [OUTPUT_DIR]
#     START_MS, END_MS : epoch milliseconds (inclusive open, exclusive close)
#     OUTPUT_DIR       : default /tmp/xvenue-phase0
#
# Example (covers 2026-04-22 00:00 UTC to 2026-04-23 00:00 UTC):
#   scripts/phase0/fetch_reference.sh 1776870000000 1776956400000
#
# Idempotent: output file is rewritten on each run. API is anonymous
# (no auth) and Binance's public klines has a weight budget of ~1200/min
# which 7 days * 11 requests stays well below.

set -euo pipefail

START_MS="${1:?missing START_MS}"
END_MS="${2:?missing END_MS}"
OUT_DIR="${3:-/tmp/xvenue-phase0}/reference"
mkdir -p "$OUT_DIR"

OUT_FILE="$OUT_DIR/binance_btcusdt_1m.jsonl"
: > "$OUT_FILE"

# Binance returns up to 1000 rows per request. 1m interval → 1000 min =
# ~16.7h per page. Page until we cover [START_MS, END_MS).
CURSOR="$START_MS"
PAGES=0
while [[ "$CURSOR" -lt "$END_MS" ]]; do
  PAGES=$((PAGES + 1))
  URL="https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000&startTime=${CURSOR}&endTime=${END_MS}"
  RESP=$(curl -sS --max-time 10 "$URL")

  # Emit one JSON-per-line in our own schema: {ts_ms, open, high, low, close}
  ROWS=$(echo "$RESP" | jq -c '.[] | {ts_ms: .[0], open: .[1], high: .[2], low: .[3], close: .[4]}')
  if [[ -z "$ROWS" ]]; then
    break
  fi
  echo "$ROWS" >> "$OUT_FILE"

  # Advance cursor to the next minute after the last kline we saw.
  LAST_MS=$(echo "$RESP" | jq '.[-1][0]')
  if [[ -z "$LAST_MS" || "$LAST_MS" == "null" ]]; then
    break
  fi
  NEXT=$((LAST_MS + 60000))
  if [[ "$NEXT" -le "$CURSOR" ]]; then
    # defensive: avoid infinite loop if upstream returns stale data
    break
  fi
  CURSOR="$NEXT"

  # Be polite even though the weight budget is generous.
  sleep 0.1
done

COUNT=$(wc -l < "$OUT_FILE")
echo "==> $COUNT 1m klines over $PAGES page(s) → $OUT_FILE"
