#!/bin/bash
# Tokyo deployment wrapper for xvenue-arb (bot-strategy#166).
#
# Single process, two DexConnectors (Lighter + Extended), both driven
# from debot-tokyo. The env file below carries BOTH venues' credentials
# for this bot's dedicated sub-accounts (see DESIGN.md §0 / §6) — the
# Extended sub-account is new (separated from debot-pair-btceth-extended)
# and Lighter credentials may also point to a new sub-account depending
# on how capital is provisioned.

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"

# `set -a` turns every subsequent variable assignment into an export, so
# plain `VAR=value` lines in sourced env files become visible to the
# child `debot` process. Matches the Extended/Tokyo pattern from
# debot-pair-btceth-extended.sh.
set -a

# KMS data key shared across all bots on this host.
if [ -f "$ENV_DIR/debot_secrets_common.env" ]; then
    # shellcheck disable=SC1090
    source "$ENV_DIR/debot_secrets_common.env"
fi
# Logging / telemetry defaults.
if [ -f "$ENV_DIR/debot.env" ]; then
    # shellcheck disable=SC1090
    source "$ENV_DIR/debot.env"
fi
# xvenue-arb per-bot env (Extended + Lighter credentials in one file).
# shellcheck disable=SC1090
source "$ENV_DIR/debot-xvenue-arb.env"

set +a

export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-xvenue-arb
export XVENUE_CONFIG_PATH=/opt/debot/configs/xvenue-arb/debot-xvenue-arb-eth.yaml

# Suppress the per-connector startup random jitter. xvenue-arb hits each
# venue exactly once at boot, so the jitter (meant for multi-variant
# pairtrade A/B/C) just delays the first quote for no benefit.
export LIGHTER_STARTUP_JITTER_SECS=0

# Perps-only bot — skip Lighter /api/v1/orderBooks spot fetch at
# market-cache init to reduce startup 429 / WAF cooldown risk (see
# bot-strategy#128 follow-up).
export LIGHTER_SKIP_SPOT_MARKETS=1

# Make libsigner.so discoverable. CI cross-compiles it for arm64 and
# ships it into /opt/debot/lib/; see docs/DESIGN.md §8.1.
if [ -f /opt/debot/lib/libsigner.so ]; then
    export LIGHTER_GO_PATH=/opt/debot/lib
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$DEBOT_STATUS_DIR"

exec /opt/debot/bin/debot
