#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"

if [ -z "$1" ]; then
    echo "Error: No environment argument provided."
    exit 1
fi

# Load common non-secret defaults if present.
COMMON_ENV_PATH="${SCRIPT_DIR}/debot.env"
if [ -f "$COMMON_ENV_PATH" ]; then
    source "$COMMON_ENV_PATH"
fi

# Resolve PairTrade YAML config path.
if [ -z "${PAIRTRADE_CONFIG_PATH:-}" ]; then
    PAIRTRADE_CONFIG_PATH="${BASE_DIR}/configs/pairtrade/$1.yaml"
    export PAIRTRADE_CONFIG_PATH
fi
if [ ! -f "$PAIRTRADE_CONFIG_PATH" ]; then
    echo "Error: PairTrade config not found: $PAIRTRADE_CONFIG_PATH"
    exit 1
fi

ENV_DIR="${DEBOT_ENV_DIR:-${BASE_DIR}/scripts}"
ENV_FILE="${ENV_DIR}/$1.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: Env file not found: $ENV_FILE"
    exit 1
fi

source "$ENV_FILE"

# Ensure lighter-go shared library is discoverable even if the env file didn't set it.
if [ -z "${LIGHTER_GO_PATH:-}" ] && [ -f "${BASE_DIR}/lib/libsigner.so" ]; then
    export LIGHTER_GO_PATH="${BASE_DIR}/lib"
fi
if [ -n "${LIGHTER_GO_PATH:-}" ]; then
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

# for dashboard
export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID="$1"

exec "${BASE_DIR}/bin/debot"
