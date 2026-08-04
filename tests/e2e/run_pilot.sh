#!/usr/bin/env bash
# Runs the `sessionize` pilot as a real map-reduce on a live cluster.
#
#   tests/e2e/run_local_cluster.sh     # once
#   tests/e2e/run_pilot.sh
#
# The pilot is a production-shaped workload — wide mixed-type rows, non-UTF-8
# byte columns, two output tables per phase, a reduce over a realistic key, and
# deliberately corrupt input. Its purpose is to make the API fail under load, not
# to demonstrate that it works.
#
# Unlike run_e2e.sh, the two phases are run as separate operations: `map` writes
# events and rejects to two tables, then a `reduce` over the sorted events writes
# sessions and per-user summaries. That mirrors how a real pipeline is staged and
# keeps the rejects table inspectable between phases.
set -euo pipefail

PROXY="${YT_PROXY:-localhost:8000}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE="${YT_PILOT_BASE:-//tmp/ytsaurus_rs_pilot}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

BINDIR="$ROOT/target/x86_64-unknown-linux-musl/release-worker"
YSON='<format=binary>yson'
MEM='536870912'

yt() { command yt --proxy "$PROXY" "$@"; }
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()  { printf '   \033[32mok\033[0m %s\n' "$*"; }
die() { printf '   \033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

command -v yt >/dev/null || die "the 'yt' CLI is not installed (pip install ytsaurus-client ytsaurus-yson)"
yt list / >/dev/null 2>&1 || die "cannot reach the cluster at $PROXY"

say "Building the pilot worker"
"$ROOT/scripts/build-worker.sh" sessionize >/dev/null
file "$BINDIR/sessionize" | grep -q 'ELF 64-bit LSB.*x86-64' \
  || die "sessionize is not a Linux x86-64 binary"
ok "$(ls -lh "$BINDIR/sessionize" | awk '{print $5}') static binary"

say "Generating input"
yt remove "$BASE" --recursive --force
yt create map_node "$BASE" --recursive >/dev/null
python3 "$ROOT/tests/e2e/generate_pilot_input.py" "$WORK/events.yson"
yt write-table --format "$YSON" "$BASE/raw" < "$WORK/events.yson"
ok "$(yt get "$BASE/raw/@row_count") raw events in $BASE/raw"

say "Map: validate and quarantine"
yt map "./sessionize map" \
  --src "$BASE/raw" \
  --dst "$BASE/events" --dst "$BASE/rejects" \
  --format "$YSON" \
  --local-file "$BINDIR/sessionize" \
  --spec "{mapper={memory_limit=$MEM}}"

CLEAN=$(yt get "$BASE/events/@row_count")
BAD=$(yt get "$BASE/rejects/@row_count")
ok "$CLEAN events kept, $BAD rejected"
[ "$BAD" -gt 0 ] || die "expected the corrupt rows to be quarantined, got none"

say "Reject reasons"
yt read-table --format json "$BASE/rejects" \
  | python3 -c "
import json, sys, collections
reasons = collections.Counter(json.loads(l)['reason'] for l in sys.stdin if l.strip())
for reason, n in sorted(reasons.items()):
    print(f'   {n:>4}  {reason}')
"

say "Sort by the reduce key"
yt sort --src "$BASE/events" --dst "$BASE/events_sorted" --sort-by user_id --sort-by timestamp
ok "sorted by user_id, timestamp"

say "Reduce: sessionize"
# A `reduce` operation has one job type, so control attributes go under `job_io`.
yt reduce "./sessionize reduce" \
  --src "$BASE/events_sorted" \
  --dst "$BASE/sessions" --dst "$BASE/users" \
  --reduce-by user_id \
  --format "$YSON" \
  --local-file "$BINDIR/sessionize" \
  --spec "{reducer={memory_limit=$MEM};job_io={control_attributes={enable_key_switch=%true}}}"

SESSIONS=$(yt get "$BASE/sessions/@row_count")
USERS=$(yt get "$BASE/users/@row_count")
ok "$SESSIONS sessions across $USERS users"

say "Checking the result against the input"
yt read-table --format json "$BASE/users" > "$WORK/users.json"
yt read-table --format json "$BASE/sessions" > "$WORK/sessions.json"
python3 "$ROOT/tests/e2e/check_pilot_output.py" \
  "$WORK/events.yson" "$WORK/users.json" "$WORK/sessions.json" "$CLEAN"

say "Pilot passed"
echo "Left at $BASE; remove with: yt --proxy $PROXY remove $BASE --recursive"
