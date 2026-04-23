#!/bin/bash
# Tokyo deployment wrapper for single-bot pairtrade on Extended Exchange
# (bot-strategy#123 Phase 3). No A/B/C split — Phase 3 runs one champion-A
# bot against Extended to gather Extended-specific price data, and the
# Phase 4 grid-search decides whether a multi-strategy config is warranted.
#
# Secrets loading mirrors the Lighter wrapper: `debot-pair-btceth-extended.env`
# provides EXTENDED_{API_KEY,PUBLIC_KEY,PRIVATE_KEY,VAULT,REST_ENDPOINT,
# WEB_SOCKET_ENDPOINT}, `debot_secrets_common.env` provides
# ENCRYPTED_DATA_KEY (KMS-wrapped data key used by debot_utils::
# decrypt_data_with_kms to unwrap EXTENDED_PRIVATE_KEY at runtime).

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"

# `set -a` turns every subsequent variable assignment into an export, which
# makes plain `VAR=value` lines in the sourced env files visible to the
# child `debot` process. Without it the bot panics with `EXTENDED_API_KEY
# must be set` because bash's `source` only populates shell variables, not
# the environment.
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
# Extended per-account env.
# shellcheck disable=SC1090
source "$ENV_DIR/debot-pair-btceth-extended.env"

set +a

export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-pair-btceth-ext
export PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-btceth-extended.yaml

mkdir -p "$DEBOT_STATUS_DIR"

# No libsigner: extended-sdk is pure Rust (no Go cgo dep).
exec /opt/debot/bin/debot
