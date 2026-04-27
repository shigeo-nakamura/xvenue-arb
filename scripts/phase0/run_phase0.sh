#!/bin/bash
# One-shot Phase 0 v2 runner: fetch dumps + Binance refs + run the
# v2 spread analysis for ETH (primary) and BTC (secondary), in both
# v2-default and strict-entry modes. Prints a summary table and exits
# 0 only if all four runs return GO.
#
# Built for the 2026-04-29 7-day formal verdict on bot-strategy#166,
# but parameterised so it can run any window.
#
# Usage:
#   scripts/phase0/run_phase0.sh [START_DATE_UTC] [END_DATE_UTC] [WORK_DIR]
#     START_DATE_UTC : YYYY-MM-DD, default = today UTC - 7 days
#     END_DATE_UTC   : YYYY-MM-DD (exclusive), default = today UTC
#     WORK_DIR       : default = /tmp/xvenue-phase0-7d
#
# Examples:
#   # Default: last 7 days ending today UTC
#   scripts/phase0/run_phase0.sh
#
#   # Explicit window (e.g. the 7-day overlap for the formal verdict)
#   scripts/phase0/run_phase0.sh 2026-04-22 2026-04-29
#
# Requires:
#   - SSH aliases `debot` (Frankfurt Lighter) and `debot-tokyo` (Extended)
#   - $REPO/.venv with numpy + pandas (auto-created on first run)

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Default: last 7 days, ending today (exclusive).
TODAY_UTC="$(date -u +%Y-%m-%d)"
START_DATE="${1:-$(date -u -d "$TODAY_UTC - 7 days" +%Y-%m-%d)}"
END_DATE="${2:-$TODAY_UTC}"
WORK_DIR="${3:-/tmp/xvenue-phase0-7d}"

START_MS=$(( $(date -u -d "$START_DATE 00:00:00" +%s) * 1000 ))
END_MS=$((   $(date -u -d "$END_DATE 00:00:00" +%s) * 1000 ))
DAYS=$(( (END_MS - START_MS) / 86400000 ))

echo "==================================================================="
echo "Phase 0 v2 runner"
echo "  window      : $START_DATE 00:00 UTC → $END_DATE 00:00 UTC ($DAYS days)"
echo "  work dir    : $WORK_DIR"
echo "==================================================================="

mkdir -p "$WORK_DIR/lighter" "$WORK_DIR/extended" "$WORK_DIR/results"

# ----- venv -----
if [[ ! -x "$REPO/.venv/bin/python" ]]; then
  echo "==> creating venv with numpy + pandas"
  python3 -m venv "$REPO/.venv"
  "$REPO/.venv/bin/pip" install --quiet numpy pandas
fi
PY="$REPO/.venv/bin/python"

# ----- fetch dumps -----
echo "==> fetching Lighter dumps from debot ($START_DATE..$END_DATE)"
CUR_MS="$START_MS"
while [[ "$CUR_MS" -lt "$END_MS" ]]; do
  D=$(date -u -d "@$((CUR_MS / 1000))" +%Y%m%d)
  scp -q "debot:/opt/debot/market_data_btceth_${D}.jsonl" "$WORK_DIR/lighter/" || \
    echo "  (skip lighter $D — not available)"
  CUR_MS=$((CUR_MS + 86400000))
done

echo "==> fetching Extended dumps from debot-tokyo ($START_DATE..$END_DATE)"
CUR_MS="$START_MS"
while [[ "$CUR_MS" -lt "$END_MS" ]]; do
  D=$(date -u -d "@$((CUR_MS / 1000))" +%Y%m%d)
  scp -q "debot-tokyo:/opt/debot-extended/market_data_btceth_extended_${D}.jsonl" \
    "$WORK_DIR/extended/" || \
    echo "  (skip extended $D — not available)"
  CUR_MS=$((CUR_MS + 86400000))
done

# ----- fetch references -----
echo "==> fetching Binance BTCUSDT 1m reference"
"$REPO/scripts/phase0/fetch_reference.sh" "$START_MS" "$END_MS" BTCUSDT "$WORK_DIR" >/dev/null

echo "==> fetching Binance ETHUSDT 1m reference"
"$REPO/scripts/phase0/fetch_reference.sh" "$START_MS" "$END_MS" ETHUSDT "$WORK_DIR" >/dev/null

# ----- run analysis -----
# ETH primary uses 100 bps ref guard (per #166 part 9 finding: 30 bps over-filters
# ETH legitimate moves). BTC keeps 30 bps (its noise floor is much lower).
run_analysis() {
  local SYMBOL="$1"; local MODE="$2"; local REF_DEV="$3"; local REF_FILE="$4"
  local OUT="$WORK_DIR/results/${SYMBOL,,}_${MODE}.txt"
  local STRICT_FLAG=""
  [[ "$MODE" == "strict" ]] && STRICT_FLAG="--strict-entry"

  "$PY" "$REPO/scripts/phase0/spread_analysis.py" \
    --lighter-dir  "$WORK_DIR/lighter" \
    --extended-dir "$WORK_DIR/extended" \
    --reference-jsonl "$REF_FILE" \
    --drop-ref-deviation-bps "$REF_DEV" \
    --bucket-sec 5 --roll-window-sec 1800 \
    --abs-threshold-bps 5 --persistence-sec 15 --max-hold-sec 600 \
    --roundtrip-bps 2.5 \
    --symbol "$SYMBOL" $STRICT_FLAG \
    > "$OUT" 2>&1 || true

  local VERDICT
  VERDICT=$(grep -m1 "Phase 0 verdict (v2)" "$OUT" | sed -E 's/.*\(v2\): *([A-Z-]+).*/\1/' || echo "?")
  echo "$VERDICT"
}

declare -A VERDICTS
echo "==> running ETH v2-default (ref-guard 100 bps)"
VERDICTS[eth_default]=$(run_analysis ETH default 100 "$WORK_DIR/reference/binance_ethusdt_1m.jsonl")
echo "==> running ETH strict-entry (ref-guard 100 bps)"
VERDICTS[eth_strict]=$(run_analysis ETH strict 100 "$WORK_DIR/reference/binance_ethusdt_1m.jsonl")
echo "==> running BTC v2-default (ref-guard 30 bps)"
VERDICTS[btc_default]=$(run_analysis BTC default 30 "$WORK_DIR/reference/binance_btcusdt_1m.jsonl")
echo "==> running BTC strict-entry (ref-guard 30 bps)"
VERDICTS[btc_strict]=$(run_analysis BTC strict 30 "$WORK_DIR/reference/binance_btcusdt_1m.jsonl")

# ----- summary -----
extract_row() {
  # Pull the v2 block once then sed each field out. Fields are matched
  # by a label and then the first signed number after the colon, so
  # we don't depend on whitespace columning.
  local FILE="$1"
  local BLOCK
  BLOCK=$(awk '/persistence-filtered stats v2/{f=1} f; /^=/&&f{exit}' "$FILE")
  local ANN WIN NET GROSS P90
  ANN=$(  echo "$BLOCK" | sed -nE 's/^[[:space:]]*annualized trades[[:space:]]*:[[:space:]]*([0-9.]+).*/\1/p' | head -1)
  WIN=$(  echo "$BLOCK" | sed -nE 's/^[[:space:]]*win rate[[:space:]]*:[[:space:]]*([0-9.]+%).*/\1/p' | head -1)
  NET=$(  echo "$BLOCK" | sed -nE 's/^[[:space:]]*net bps \/ trade[[:space:]]*:[[:space:]]*([+-]?[0-9.]+).*/\1/p' | head -1)
  GROSS=$(echo "$BLOCK" | sed -nE 's/^[[:space:]]*gross bps \/ trade[[:space:]]*:[[:space:]]*([+-]?[0-9.]+).*/\1/p' | head -1)
  P90=$(  echo "$BLOCK" | sed -nE 's/^[[:space:]]*p90 hold[[:space:]]*:[[:space:]]*(.+)$/\1/p' | head -1)
  printf "%s\t%s\t%s\t%s\t%s" "$ANN" "$WIN" "$NET" "$GROSS" "$P90"
}

echo
echo "==================================================================="
echo "Phase 0 v2 summary — $START_DATE → $END_DATE ($DAYS days)"
echo "==================================================================="
printf "%-15s %-10s %-12s %-8s %-12s %-12s %-10s\n" \
  "run" "verdict" "trades/yr" "win%" "net bps/tr" "gross bps/tr" "p90 hold"
echo "-------------------------------------------------------------------"
for KEY in eth_default eth_strict btc_default btc_strict; do
  ROW=$(extract_row "$WORK_DIR/results/${KEY/_/_}.txt" 2>/dev/null || echo)
  IFS=$'\t' read -r ANN WIN NET GROSS P90 <<<"$ROW"
  printf "%-15s %-10s %-12s %-8s %-12s %-12s %-10s\n" \
    "$KEY" "${VERDICTS[$KEY]}" "${ANN:-?}" "${WIN:-?}" "${NET:-?}" "${GROSS:-?}" "${P90:-?}"
done
echo "==================================================================="

ALL_GO=1
for V in "${VERDICTS[@]}"; do
  [[ "$V" == "GO" ]] || ALL_GO=0
done

if [[ "$ALL_GO" == 1 ]]; then
  echo "==> ALL FOUR RUNS GO — formal Phase 0 v2 verdict: GO"
  exit 0
else
  echo "==> AT LEAST ONE RUN NO-GO — review individual reports under $WORK_DIR/results/"
  exit 1
fi
