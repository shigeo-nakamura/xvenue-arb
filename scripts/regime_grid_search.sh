#!/bin/bash
# Grid search for regime filter thresholds (bot-strategy#20).
# Uses Bot A live config as baseline, tests regime_vol_max x regime_trend_max combos.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

BINARY=./target/release/debot
BACKTEST_FILE=$REPO_ROOT/market_data_btceth_365d.bin
CONFIG=$REPO_ROOT/configs/pairtrade/debot-pair-btceth.yaml
LOG_DIR=/tmp/regime_grid
ANALYZER=$SCRIPT_DIR/log_analyzer.py

mkdir -p "$LOG_DIR"

# Regime vol_max grid (0.0 = disabled baseline)
VOL_MAX_VALUES="0.0 0.0005 0.001 0.002 0.003 0.005"
# Regime trend_max grid (0.0 = disabled baseline)
TREND_MAX_VALUES="0.0 0.3 0.5 0.8 1.0"
# Regime window sizes to test
VOL_WINDOW=60
TREND_WINDOW=60

echo "=== Regime Filter Grid Search ==="
echo "Data: $BACKTEST_FILE"
echo "Config: $CONFIG (Bot A baseline)"
echo ""
printf "%-12s %-12s %10s %8s %10s %10s\n" "vol_max" "trend_max" "PnL(\$)" "Trades" "Sharpe" "MaxDD"
echo "-------------------------------------------------------------------"

for vol_max in $VOL_MAX_VALUES; do
  for trend_max in $TREND_MAX_VALUES; do
    TAG="vol${vol_max}_trend${trend_max}"
    LOG_FILE="$LOG_DIR/${TAG}.log"

    BACKTEST_MODE=true \
    BACKTEST_FILE="$BACKTEST_FILE" \
    DRY_RUN=true \
    ENABLE_DATA_DUMP=false \
    RUST_LOG="warn,debot::pairtrade=info" \
    UNIVERSE_PAIRS="BTC/ETH" \
    PAIRTRADE_CONFIG_PATH="$CONFIG" \
    REGIME_VOL_WINDOW="$VOL_WINDOW" \
    REGIME_VOL_MAX="$vol_max" \
    REGIME_TREND_WINDOW="$TREND_WINDOW" \
    REGIME_TREND_MAX="$trend_max" \
    REGIME_REFERENCE_SYMBOL="BTC" \
    $BINARY > "$LOG_FILE" 2>&1 || true

    # Extract metrics using log_analyzer.py
    RESULT=$(python3 "$ANALYZER" "$LOG_FILE" 2>/dev/null || echo "-inf")
    PNL=$(echo "$RESULT" | head -1)

    # Count trades from log
    TRADES=$(grep -c '\[FILL_DETECTION\].*ENTRY' "$LOG_FILE" 2>/dev/null || echo "0")

    # Compute sharpe and maxdd via analyzer env vars
    METRICS=$(OPTIMIZER_SCORE_MODE=return FEE_BPS=5 python3 -c "
import sys, os, math
sys.path.insert(0, '$SCRIPT_DIR')
from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe
pnl, trade_pnls, trade_returns, hold_secs = calculate_pnl('$LOG_FILE', None, None, 5.0, 0.0)
n = len(trade_pnls)
dd = compute_max_drawdown(trade_pnls) if trade_pnls else 0.0
sh = compute_sharpe(trade_pnls) if trade_pnls else 0.0
print(f'{float(pnl):.4f} {n} {sh:.4f} {dd:.4f}')
" 2>/dev/null || echo "0.0000 0 0.0000 0.0000")

    read M_PNL M_TRADES M_SHARPE M_DD <<< "$METRICS"

    printf "%-12s %-12s %10s %8s %10s %10s\n" "$vol_max" "$trend_max" "$M_PNL" "$M_TRADES" "$M_SHARPE" "$M_DD"
  done
done

echo ""
echo "Done. Logs in $LOG_DIR/"
