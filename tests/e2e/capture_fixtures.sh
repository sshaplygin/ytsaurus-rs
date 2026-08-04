#!/usr/bin/env bash
# Captures the offline test's golden fixtures from a **real** YTsaurus cluster.
#
#   tests/e2e/run_local_cluster.sh     # once
#   tests/e2e/capture_fixtures.sh
#
# Writes, into tests/e2e/fixtures/:
#
#   cat_input.bin             exactly the bytes a job receives on fd 0, with
#                             every control attribute enabled
#   cat_expected_table_0.bin  the rows of input table 0, as the cluster stores
#   cat_expected_table_1.bin  and returns them
#   cat_expected_single.bin   both, in stream order
#
# Why capture rather than construct: the control-record framing a job sees is
# the cluster's to define. Writing it by hand means testing our reading of the
# documentation against itself. The captured bytes differ from a naive reading —
# YTsaurus emits `<table_index=0;>#` with a trailing semicolon inside the
# attribute block, for instance.
#
# The capture works by running a map operation whose *output* format is text
# YSON while its input stays binary, so a shell one-liner can base64 the whole
# of stdin into a single row. That is the only way to get the raw stream back
# out of a job without writing a special binary for it.
set -euo pipefail

PROXY="${YT_PROXY:-localhost:8000}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURES="$ROOT/tests/e2e/fixtures"
BASE="${YT_E2E_BASE:-//tmp/ytsaurus_rs_capture}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

YSON='<format=binary>yson'

yt() { command yt --proxy "$PROXY" "$@"; }
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()  { printf '   \033[32mok\033[0m %s\n' "$*"; }
die() { printf '   \033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

command -v yt >/dev/null || die "the 'yt' CLI is not installed (pip install ytsaurus-client ytsaurus-yson)"
yt list / >/dev/null 2>&1 || die "cannot reach the cluster at $PROXY"

say "Regenerating table payloads"
python3 "$ROOT/tests/e2e/generate_fixtures.py"

say "Uploading to $BASE"
yt remove "$BASE" --recursive --force
yt create map_node "$BASE" --recursive >/dev/null
yt write-table --format "$YSON" "$BASE/in0" < "$FIXTURES/table_rows_0.bin"
yt write-table --format "$YSON" "$BASE/in1" < "$FIXTURES/table_rows_1.bin"
ok "$(yt get "$BASE/in0/@row_count") + $(yt get "$BASE/in1/@row_count") rows"

say "Capturing the job input stream"
# Input binary, output text: the job base64s all of stdin into one text row.
yt map 'printf "{capture=\""; base64 | tr -d "\n"; printf "\"}"' \
  --src "$BASE/in0" --src "$BASE/in1" --dst "$BASE/capture" \
  --input-format "$YSON" --output-format '<format=text>yson' \
  --spec '{mapper={memory_limit=536870912};job_io={control_attributes={enable_table_index=%true;enable_row_index=%true;enable_range_index=%true}}}' \
  >/dev/null 2>&1

[ "$(yt get "$BASE/capture/@row_count")" = "1" ] \
  || die "expected exactly one capture row; the input was split across jobs"

yt read-table --format json "$BASE/capture" > "$WORK/capture.json" 2>/dev/null
python3 - "$WORK/capture.json" "$FIXTURES/cat_input.bin" <<'PY'
import base64, json, sys
row = json.loads(open(sys.argv[1]).read().strip().split("\n")[0])
data = base64.b64decode(row["capture"])
open(sys.argv[2], "wb").write(data)
print(f"   captured {len(data)} bytes")
PY
ok "cat_input.bin"

say "Capturing the expected outputs"
yt read-table --format "$YSON" "$BASE/in0" > "$FIXTURES/cat_expected_table_0.bin" 2>/dev/null
yt read-table --format "$YSON" "$BASE/in1" > "$FIXTURES/cat_expected_table_1.bin" 2>/dev/null
cat "$FIXTURES/cat_expected_table_0.bin" "$FIXTURES/cat_expected_table_1.bin" \
  > "$FIXTURES/cat_expected_single.bin"

# The rows a job is handed must be byte-identical to the rows the table returns;
# if that ever stops holding, the fixtures are inconsistent and the offline test
# would be checking the wrong thing.
python3 - "$FIXTURES" <<'PY'
import pathlib, sys
f = pathlib.Path(sys.argv[1])
job_input = (f / "cat_input.bin").read_bytes()
for i in (0, 1):
    rows = (f / f"cat_expected_table_{i}.bin").read_bytes()
    if job_input.find(rows) < 0:
        sys.exit(f"table {i} rows are not a verbatim substring of the job input")
print("   consistency check passed: table rows appear verbatim in the job input")
PY
ok "cat_expected_table_0.bin, cat_expected_table_1.bin, cat_expected_single.bin"

say "Verifying the offline test against the fresh fixtures"
(cd "$ROOT" && cargo test -p ytsaurus-examples --test cat_e2e 2>&1 | tail -12)

yt remove "$BASE" --recursive --force
say "Done"
for f in "$FIXTURES"/*.bin; do
  printf '  %-28s %s bytes\n' "$(basename "$f")" "$(wc -c < "$f" | tr -d ' ')"
done
