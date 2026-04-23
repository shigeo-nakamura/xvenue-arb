#!/bin/bash
# Production wrapper for the consolidated single-process pairtrade
# A/B/C deployment (shigeo-nakamura/bot-strategy#25 commit 9 cutover).
#
# Loads the three legacy per-bot env files in isolated subshells so
# unset vars in B/C (e.g. LIGHTER_ACCOUNT_INDEX which they auto-discover
# via wallet) cannot leak A's value into B/C's slot, then re-exports
# every credential as LIGHTER_*_{A,B,C} for the suffixed env lookup
# added in commit 3. The legacy env files themselves stay untouched so
# rollback to a single-instance deployment is just one commit away.

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"

# Common KMS-encrypted shared key + RUST_LOG defaults
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"

vars_for_variant() {
    # Echoes "VAR_<id>=value" lines for the given env file's per-account
    # credentials, plus "VAR=value" lines (no suffix) for process-wide
    # settings that happen to live in the same per-account file. Runs in a
    # subshell so the sourced exports do not leak into the parent.
    (
        # shellcheck disable=SC1090
        source "$2" >/dev/null 2>&1
        for var in LIGHTER_PUBLIC_API_KEY LIGHTER_PRIVATE_API_KEY \
                   LIGHTER_API_KEY_INDEX LIGHTER_WALLET_ADDRESS \
                   LIGHTER_EVM_WALLET_PRIVATE_KEY LIGHTER_ACCOUNT_INDEX; do
            if [ -n "${!var:-}" ]; then
                printf '%s_%s=%s\n' "$var" "$1" "${!var}"
            fi
        done
        # Process-wide settings (no _A/_B/_C suffix). All variants define
        # the same value so order is irrelevant — last writer wins.
        for var in LIGHTER_MAINTENANCE_TTL_MINS; do
            if [ -n "${!var:-}" ]; then
                printf '%s=%s\n' "$var" "${!var}"
            fi
        done
    )
}

while IFS='=' read -r k v; do export "$k=$v"; done < <(vars_for_variant A "$ENV_DIR/debot-pair-btceth.env")
while IFS='=' read -r k v; do export "$k=$v"; done < <(vars_for_variant B "$ENV_DIR/debot-pair-btceth-b.env")
while IFS='=' read -r k v; do export "$k=$v"; done < <(vars_for_variant C "$ENV_DIR/debot-pair-btceth-c.env")

# Wipe the suffix-less LIGHTER_* so lighter_env() never falls through to
# them and accidentally cross-wires variants. Each instance must hit its
# own suffixed copy or fail loudly.
unset LIGHTER_PUBLIC_API_KEY LIGHTER_PRIVATE_API_KEY LIGHTER_ACCOUNT_INDEX
unset LIGHTER_API_KEY_INDEX LIGHTER_WALLET_ADDRESS LIGHTER_EVM_WALLET_PRIVATE_KEY

# Production paths. DEBOT_STATUS_ID=debot-pair-btceth makes the
# StatusReporter::from_env_for_instance suffix logic produce
# /home/ec2-user/debot_status/debot-pair-btceth-{a,b,c}/status.json.
# Same shape for the pnl logger.
export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-pair-btceth
export PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-btceth.yaml

# Perps-only bot — skip Lighter /api/v1/orderBooks spot fetch at market-cache
# init. That call right after the heavy orderBookDetails response was the
# single biggest trigger of the startup 429 / WAF cooldown. See
# bot-strategy#128 follow-up.
export LIGHTER_SKIP_SPOT_MARKETS=1

# Disable the per-connector startup random jitter. The consolidated A/B/C
# deployment uses INIT_ACCOUNT_SPACING in pairtrade's create_connector loop
# to deterministically space the three CheckClient() hits on /api/v1/apikeys
# (which throttles per-wallet, and all three sub-accounts share one wallet).
# The dex-connector default of 0..30s random sleep at start() then collapses
# that spacing — A=9s + B=5s after the 10s parent gap = both /apikeys hits
# inside the per-wallet short-window → 429 on every restart. See
# bot-strategy#163.
export LIGHTER_STARTUP_JITTER_SECS=0

# Make libsigner.so discoverable
if [ -f /opt/debot/lib/libsigner.so ]; then
    export LIGHTER_GO_PATH=/opt/debot/lib
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$DEBOT_STATUS_DIR"

exec /opt/debot/bin/debot
