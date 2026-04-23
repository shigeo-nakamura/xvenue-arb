#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." &> /dev/null && pwd)"
cd "$SCRIPT_DIR"

echo "Starting debot PairTrade strategy..."

# Load common non-secret defaults if present.
COMMON_ENV_PATH="${SCRIPT_DIR}/debot.env"
if [ -f "$COMMON_ENV_PATH" ]; then
    if [ -n "$FISH_VERSION" ]; then
        source "$COMMON_ENV_PATH"
    else
        . "$COMMON_ENV_PATH"
    fi
fi

# If the caller (e.g. optimizer) passes env overrides, preserve them across sourcing debot_*.env.
# debot_*.env historically exported hard defaults (e.g. DRY_RUN=false) which would otherwise clobber
# the caller's values.
OVERRIDE_VARS=(
    BACKTEST_MODE
    BACKTEST_FILE
    DRY_RUN
    ENABLE_DATA_DUMP
    DATA_DUMP_FILE
    UNIVERSE_PAIRS
    UNIVERSE_SYMBOLS
    MAX_ACTIVE_PAIRS
    INTERVAL_SECS
    TRADING_PERIOD_SECS
    METRICS_WINDOW_LENGTH
    PAIR_SELECTION_LOOKBACK_HOURS_SHORT
    PAIR_SELECTION_LOOKBACK_HOURS_LONG
    ENTRY_Z_SCORE_BASE
    ENTRY_Z_SCORE_MIN
    ENTRY_Z_SCORE_MAX
    EXIT_Z_SCORE
    STOP_LOSS_Z_SCORE
    MAX_LOSS_R_MULT
    RISK_PCT_PER_TRADE
    FORCE_CLOSE_TIME_SECS
    COOLDOWN_SECS
    NET_FUNDING_MIN_PER_HOUR
    SPREAD_VELOCITY_MAX_SIGMA_PER_MIN
    ENTRY_VOL_LOOKBACK_HOURS
    REEVAL_JUMP_Z_MULT
    HALF_LIFE_MAX_HOURS
    ADF_P_THRESHOLD
    WARM_START_MODE
    WARM_START_MIN_BARS
    SLIPPAGE_BPS
    FEE_BPS
    MAX_LEVERAGE
    LIGHTER_GO_PATH
    PAIRTRADE_CONFIG_PATH
    ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN
    FUNDING_ENTRY_Z_SCALE
    BETA_GAP_ENTRY_Z_SCALE
)
declare -A SAVED_OVERRIDES
for var in "${OVERRIDE_VARS[@]}"; do
    if [ "${!var+x}" = "x" ]; then
        SAVED_OVERRIDES["$var"]="${!var}"
    fi
done

# Resolve PairTrade YAML config path.
if [ -z "${PAIRTRADE_CONFIG_PATH:-}" ]; then
    if [ -n "${DEBOT_CONFIG:-}" ]; then
        PAIRTRADE_CONFIG_PATH="$DEBOT_CONFIG"
    else
        if [ -n "${DEBOT_ENV:-}" ]; then
            CONFIG_BASE="$(basename "$DEBOT_ENV" .env)"
        else
            CONFIG_BASE="debot00"
        fi
        PAIRTRADE_CONFIG_PATH="${REPO_ROOT}/configs/pairtrade/${CONFIG_BASE}.yaml"
    fi
    export PAIRTRADE_CONFIG_PATH
fi

if [ ! -f "$PAIRTRADE_CONFIG_PATH" ]; then
    echo "Error: PairTrade config not found at $PAIRTRADE_CONFIG_PATH"
    exit 1
fi

# Load secrets env (gitignored). Default to matching config basename in scripts/.
CONFIG_BASE="$(basename "$PAIRTRADE_CONFIG_PATH" .yaml)"
SECRETS_ENV_PATH="${DEBOT_ENV:-$SCRIPT_DIR/${CONFIG_BASE}.env}"
if [ -f "$SECRETS_ENV_PATH" ]; then
    echo "Loading debot secrets env..."
    if [ -n "$FISH_VERSION" ]; then
        source "$SECRETS_ENV_PATH"
    else
        . "$SECRETS_ENV_PATH"
    fi
else
    if [ "${BACKTEST_MODE:-}" = "true" ]; then
        echo "Warning: debot env not found at $SECRETS_ENV_PATH (backtest mode)"
    else
        echo "Error: debot env not found at $SECRETS_ENV_PATH"
        exit 1
    fi
fi

for var in "${!SAVED_OVERRIDES[@]}"; do
    export "$var"="${SAVED_OVERRIDES[$var]}"
done

# Logging: set conservative defaults unless KEEP_RUST_LOG=1 is set by caller
if [ "${KEEP_RUST_LOG:-0}" != "1" ]; then
    export RUST_LOG="info,pairtrade=info,dex_connector=warn"
fi

# Ensure lighter-go shared library is available
LIGHTER_GO_PATH="${LIGHTER_GO_PATH:-$REPO_ROOT/../lighter-go}"
if [ ! -f "$LIGHTER_GO_PATH/libsigner.so" ]; then
    echo "Warning: lighter-go shared library not found at $LIGHTER_GO_PATH/libsigner.so"
    echo "Please build lighter-go first: cd $LIGHTER_GO_PATH && just build-linux-local"
else
    # Ensure runtime linker can find libsigner.so
    export LD_LIBRARY_PATH="$LIGHTER_GO_PATH:${LD_LIBRARY_PATH:-}"
fi

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    # Build debot with PairTrade strategy (release for backtest speed)
    echo "Building debot with PairTrade strategy..."
    cd "$REPO_ROOT"
    cargo build --release
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build debot_v5"
        exit 1
    fi
else
    echo "Skipping build because SKIP_BUILD=1"
    cd "$REPO_ROOT"
fi

echo "Starting PairTrade strategy execution..."
echo "Configuration:"
echo "  - Config: $PAIRTRADE_CONFIG_PATH"
echo "  - DEX: $DEX_NAME"
echo "  - Dry Run: $DRY_RUN"
echo "  - Universe: $UNIVERSE_PAIRS"
echo "  - Entry Z: $ENTRY_Z_SCORE_BASE (min $ENTRY_Z_SCORE_MIN, max $ENTRY_Z_SCORE_MAX)"
echo "  - Exit Z: $EXIT_Z_SCORE"
echo "  - Stop Loss Z: $STOP_LOSS_Z_SCORE"
echo "  - Funding min/hr: $NET_FUNDING_MIN_PER_HOUR"
echo "  - Slippage bps: ${SLIPPAGE_BPS:-0}"

# Run debot_v5 (use release if available)
if [ -x "$REPO_ROOT/target/release/debot" ]; then
    exec "$REPO_ROOT/target/release/debot"
else
    exec "$REPO_ROOT/target/debug/debot"
fi
