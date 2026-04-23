#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." &> /dev/null && pwd)"
LOG_FILE="${REPO_ROOT}/optimizer.log"

export OPTIMIZER_CONFIG_PATH="${REPO_ROOT}/configs/pairtrade/debot00.yaml"
export DEBOT_ENV="${SCRIPT_DIR}/debot00.env"
export DATA_DUMP_FILE="${REPO_ROOT}/market_data_30d.jsonl"
export OPTIMIZER_MODE="optimize"
export OPTIMIZER_MAX_COMBOS="768"
export DEX_NAME="lighter"
export OPTIMIZER_LOG_PATH="${LOG_FILE}"

# BTC/ETH single pair
export UNIVERSE_PAIRS="BTC/ETH"
export OPTIMIZER_DATA_TAIL_DAYS="7"
export OPTIMIZER_MIN_DATA_DAYS="7"
export OPTIMIZER_GAP_FILL_MAX_SECS="3600"

# Sharpe-driven scoring: penalize drawdown, reward stability over frequency
export OPTIMIZER_RETURN_SCALE="100"
export OPTIMIZER_SHARPE_BONUS="50.0"
export OPTIMIZER_MAX_DRAWDOWN="50"
export OPTIMIZER_DRAWDOWN_PENALTY="2.0"
export OPTIMIZER_MIN_TRADES="5"
export OPTIMIZER_TRADE_FREQ_BONUS="0.0"
export OPTIMIZER_CVAR_PENALTY="2.0"

# Batch mode: run N param sets per process (data loaded once per batch).
# Set to 0 to disable batch mode and use per-run subprocess mode.
export OPTIMIZER_BATCH_SIZE="48"

# optimizer.py manages its own log file via OPTIMIZER_LOG_PATH;
# redirect nohup output to /dev/null to avoid deleted-inode conflict.
echo "Starting optimizer in background. Log: ${LOG_FILE}"
nohup python3 -u "${SCRIPT_DIR}/optimizer.py" > /dev/null 2>&1 &
echo "PID: $!"
