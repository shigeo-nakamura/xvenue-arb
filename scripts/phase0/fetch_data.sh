#!/bin/bash
# Pull pairtrade dump files from both venues into a local working dir
# so phase0/spread_analysis.py can align the BTC quotes.
#
# Frankfurt (`debot` alias): Lighter side, dumps from pairtrade-btceth
#   /opt/debot/market_data_btceth_*.jsonl
# Tokyo (`debot-tokyo` alias): Extended side, dumps from pairtrade-btceth-extended
#   /opt/debot-extended/market_data_btceth_extended_*.jsonl
#   (path moved from /opt/debot/ to /opt/debot-extended/ with bot-strategy#218
#    where Tokyo Lighter took over /opt/debot/ on the same instance)
#
# Usage: scripts/phase0/fetch_data.sh [OUTPUT_DIR]
#        Default OUTPUT_DIR = /tmp/xvenue-phase0
#
# Re-run is safe: files already present locally are skipped by scp's
# default overwrite (we rely on mtime-based selection upstream, not
# here).

set -euo pipefail

OUT="${1:-/tmp/xvenue-phase0}"
mkdir -p "$OUT/lighter" "$OUT/extended"

echo "==> Frankfurt Lighter dumps → $OUT/lighter/"
scp 'debot:/opt/debot/market_data_btceth_*.jsonl' "$OUT/lighter/" 2>&1 \
  | tail -20

echo "==> Tokyo Extended dumps → $OUT/extended/"
scp 'debot-tokyo:/opt/debot-extended/market_data_btceth_extended_*.jsonl' "$OUT/extended/" 2>&1 \
  | tail -20

echo
echo "==> Local counts:"
echo "Lighter:"
ls -la "$OUT/lighter/" | tail -n +2 | head -20
echo "Extended:"
ls -la "$OUT/extended/" | tail -n +2 | head -20
