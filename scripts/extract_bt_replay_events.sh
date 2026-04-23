#!/bin/bash
# Extract live eval + restart timestamps from the debot journal for BT replay.
#
# With the snapshot-v2 `spread_history` persistence fix (bot-strategy#62 /
# #27) in place, the warm_start std-collapse artifact no longer occurs,
# so pure price-level BT reproduction only needs the data dump + v2
# snapshot — no event extraction. This helper exists for EXACT replay
# fidelity (state.beta trajectory byte-identical to live), which requires
# matching live's evaluate_pair firing phase.
#
# Usage:
#   scripts/extract_bt_replay_events.sh SERVICE SINCE UNTIL OUT_DIR
#
# Example:
#   scripts/extract_bt_replay_events.sh debot-pair-btceth \
#     '2026-04-12 13:00:00' '2026-04-16 18:00:00' /tmp/bt_events/
#
# Writes:
#   OUT_DIR/eval_ts.txt     — UNIX seconds of [EVAL] BTC/ETH firings
#   OUT_DIR/restart_ts.txt  — UNIX seconds of systemd `Started` events
#
# Pass to the BT binary via:
#   BT_EVAL_TIMESTAMPS_FILE=OUT_DIR/eval_ts.txt \
#   BT_RESTART_TIMESTAMPS_FILE=OUT_DIR/restart_ts.txt \
#   scripts/bt_live_data.sh ...
set -e

SERVICE="${1:?usage: $0 SERVICE SINCE UNTIL OUT_DIR}"
SINCE="${2:?usage: $0 SERVICE SINCE UNTIL OUT_DIR}"
UNTIL="${3:?usage: $0 SERVICE SINCE UNTIL OUT_DIR}"
OUT_DIR="${4:?usage: $0 SERVICE SINCE UNTIL OUT_DIR}"

mkdir -p "$OUT_DIR"

echo "=== Extracting [EVAL] BTC/ETH timestamps from $SERVICE ==="
ssh debot "sudo journalctl -u $SERVICE --since '$SINCE' --until '$UNTIL' --no-pager | grep -E '\\[EVAL\\] BTC/ETH'" \
    > "$OUT_DIR/eval_raw.txt" || true

# The inline per-line stamp (`2026-04-15T07:02:05+0100`) is authoritative;
# we parse it directly and convert to UTC seconds. +0100 == CET on this
# server year-round, so UTC = stamped - 1h.
python3 - "$OUT_DIR/eval_raw.txt" "$OUT_DIR/eval_ts.txt" <<'PYEOF'
import re, sys
from datetime import datetime, timezone, timedelta
raw, out = sys.argv[1], sys.argv[2]
cet = timezone(timedelta(hours=1))
seen = set()
with open(raw) as f:
    for line in f:
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\+0100', line)
        if not m: continue
        dt_cet = datetime(*(int(g) for g in m.groups()), tzinfo=cet)
        seen.add(int(dt_cet.timestamp()))
with open(out, 'w') as f:
    for t in sorted(seen):
        f.write(f"{t}\n")
print(f"wrote {len(seen)} eval timestamps to {out}")
PYEOF

echo
echo "=== Extracting systemd restart timestamps for $SERVICE ==="
ssh debot "sudo journalctl -u $SERVICE --since '$SINCE' --until '$UNTIL' --no-pager | grep -E 'systemd\\[1\\]: Started $SERVICE'" \
    > "$OUT_DIR/restart_raw.txt" || true

# journalctl's outer stamp (`Apr 15 06:00:14`) has no year. Use the year
# of the SINCE argument to disambiguate (callers rarely span new-year).
python3 - "$OUT_DIR/restart_raw.txt" "$OUT_DIR/restart_ts.txt" "$SINCE" <<'PYEOF'
import sys
from datetime import datetime, timezone
raw, out, since = sys.argv[1], sys.argv[2], sys.argv[3]
year = int(since.split('-')[0])
ts = []
with open(raw) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        try:
            dt = datetime.strptime(
                f"{year} {parts[0]} {parts[1]} {parts[2]}",
                "%Y %b %d %H:%M:%S",
            ).replace(tzinfo=timezone.utc)
            ts.append(int(dt.timestamp()))
        except Exception:
            pass
ts = sorted(set(ts))
with open(out, 'w') as f:
    for t in ts:
        f.write(f"{t}\n")
print(f"wrote {len(ts)} restart timestamps to {out}")
PYEOF

rm -f "$OUT_DIR/eval_raw.txt" "$OUT_DIR/restart_raw.txt"
echo
echo "Done. For a BT run:"
echo "  BT_EVAL_TIMESTAMPS_FILE=$OUT_DIR/eval_ts.txt \\"
echo "  BT_RESTART_TIMESTAMPS_FILE=$OUT_DIR/restart_ts.txt \\"
echo "  BT_WARM_START_SNAPSHOT=/opt/debot/pairtrade_history_BTC_ETH.json \\"
echo "  scripts/bt_live_data.sh /opt/debot"
