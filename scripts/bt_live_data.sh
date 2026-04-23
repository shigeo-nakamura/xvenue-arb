#!/bin/bash
# Run BT on live data dump with automatic warm-up period exclusion.
# Usage: bt_live_data.sh [data_dir] [extra_env_vars...]
#
# Concatenates all dated dump files in chronological order,
# converts to bin, runs BT, and analyzes PnL excluding the
# warm-up period (first 4 hours).
#
# Recommended (minimum) invocation — ships log_prices + v2 spread_history
# from the live file, so no separate extraction is needed after the
# bot-strategy#62 warm_start fix is deployed:
#   BT_WARM_START_SNAPSHOT=/opt/debot/pairtrade_history_BTC_ETH.json \
#   scripts/bt_live_data.sh /opt/debot
#
# For BYTE-EXACT live reproduction (state.beta trajectory identical),
# also run `scripts/extract_bt_replay_events.sh` first and pass the
# produced files via BT_EVAL_TIMESTAMPS_FILE / BT_RESTART_TIMESTAMPS_FILE.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DATA_DIR="${1:-/opt/debot}"
shift 2>/dev/null || true

BINARY=./target/release/debot
CONFIG=$REPO_ROOT/configs/pairtrade/debot-pair-btceth.yaml
ANALYZER=$SCRIPT_DIR/log_analyzer.py
WORK_DIR=/tmp/bt_live_data
WARMUP_SECS=14400  # 4 hours (ignored when BT_WARM_START_SNAPSHOT is set)

mkdir -p "$WORK_DIR"

echo "=== Collecting live data dump files from $DATA_DIR ==="
FILES=$(ls -1 "$DATA_DIR"/market_data_btceth_*.jsonl 2>/dev/null | sort)
if [ -z "$FILES" ]; then
    echo "No data dump files found in $DATA_DIR"
    exit 1
fi
echo "$FILES"

cat $FILES > "$WORK_DIR/combined.jsonl"
LINES=$(wc -l < "$WORK_DIR/combined.jsonl")
echo "Combined: $LINES lines"

BOUNDS=$(python3 -c "
import json
from datetime import datetime, timezone, timedelta
with open('$WORK_DIR/combined.jsonl') as f:
    first = json.loads(f.readline())
    for line in f: pass
    last = json.loads(line)
ft = datetime.fromtimestamp(first['timestamp']/1000, tz=timezone.utc)
lt = datetime.fromtimestamp(last['timestamp']/1000, tz=timezone.utc)
warmup_end = ft + timedelta(seconds=$WARMUP_SECS)
print(f'{ft.isoformat()}|{lt.isoformat()}|{warmup_end.strftime(\"%Y-%m-%dT%H:%M:%S%z\")}|{(lt-ft).total_seconds()/86400:.1f}')
")
IFS='|' read -r FIRST LAST WARMUP_END SPAN_DAYS <<< "$BOUNDS"
echo "Span: $FIRST → $LAST ($SPAN_DAYS days)"

if [ -n "$BT_WARM_START_SNAPSHOT" ]; then
    echo "Warm-start snapshot: $BT_WARM_START_SNAPSHOT (no warm-up exclusion)"
    WARMUP_SECS=0
    WARMUP_END="$FIRST"
else
    echo "Warm-up ends at: $WARMUP_END (first ${WARMUP_SECS}s excluded from PnL)"
fi

echo ""
echo "=== Converting to bin ==="
# interval=0 preserves every dump tick. The ~5s cadence is already the
# live bot's fetch rate, so downsampling at interval=5s routinely drops
# the trailing sub-5s tick of a bucket (observed at dump cadence 3.5-6.5s)
# and flips the bar close to the previous tick, drifting close_a vs live.
# See bot-strategy#27 comment 2026-04-16.
cargo run --release --bin convert-data -- "$WORK_DIR/combined.jsonl" "$WORK_DIR/live.bin" 0 2>&1

echo ""
echo "=== Running backtest ==="
BACKTEST_MODE=true \
BACKTEST_FILE="$WORK_DIR/live.bin" \
DRY_RUN=true \
ENABLE_DATA_DUMP=false \
RUST_LOG="warn,debot::pairtrade=info" \
UNIVERSE_PAIRS="BTC/ETH" \
PAIRTRADE_CONFIG_PATH="$CONFIG" \
"$@" \
$BINARY > "$WORK_DIR/bt.log" 2>&1

echo "BT log: $WORK_DIR/bt.log ($(wc -l < "$WORK_DIR/bt.log") lines)"

echo ""
echo "=== Results (PnL from $WARMUP_END onward) ==="
python3 -c "
import sys, math
sys.path.insert(0, '$SCRIPT_DIR')
from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe
from datetime import datetime, timezone

warmup_end = datetime.fromisoformat('$WARMUP_END')
for fee_label, fee_val in [('0bp', 0.0), ('5bp', 5.0)]:
    pnl, tp, tr, hs = calculate_pnl('$WORK_DIR/bt.log', warmup_end, None, fee_val, 0.0)
    n = len(tp)
    if n == 0:
        print(f'PnL({fee_label}): no trades after warm-up')
        continue
    w = sum(1 for p in tp if p > 0)
    dd = compute_max_drawdown(tp)
    sh = compute_sharpe(tp)
    cm = float(pnl)/dd if dd > 0 else float('inf')
    print(f'PnL({fee_label}): \${float(pnl):.4f}  Trades: {n}  Win: {w/n*100:.1f}%  Sharpe: {sh:.3f}  MaxDD: \${dd:.2f}  Calmar: {cm:.3f}')
"
