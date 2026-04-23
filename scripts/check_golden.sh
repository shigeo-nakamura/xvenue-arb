#!/bin/bash
# Run backtest and diff against golden_baseline.txt.
# Used by bot-strategy#26 refactoring to verify behavior is unchanged after each phase.
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

LD_LIBRARY_PATH=/home/guest/bot/lighter-go:${LD_LIBRARY_PATH:-} cargo build --release --quiet

BACKTEST_MODE=true \
BACKTEST_FILE=$REPO_ROOT/market_data_btceth_365d.bin \
DRY_RUN=true \
ENABLE_DATA_DUMP=false \
RUST_LOG="warn,debot::pairtrade=info" \
UNIVERSE_PAIRS="BTC/ETH" \
PAIRTRADE_CONFIG_PATH=$REPO_ROOT/configs/pairtrade/debot-pair-btceth.yaml \
LD_LIBRARY_PATH=/home/guest/bot/lighter-go:${LD_LIBRARY_PATH:-} \
./target/release/debot > /tmp/check_golden.log 2>&1

sed 's/^[0-9T:+-]* //' /tmp/check_golden.log > /tmp/check_golden.txt

if diff -q golden_baseline.txt /tmp/check_golden.txt > /dev/null; then
    echo "OK: backtest output matches golden_baseline.txt"
    exit 0
else
    echo "FAIL: backtest output differs from golden_baseline.txt"
    diff golden_baseline.txt /tmp/check_golden.txt | head -40
    exit 1
fi
