#!/usr/bin/env bash
# End-to-end test against a real YTsaurus cluster.
#
#   tests/cluster-e2e/run_local_cluster.sh     # once
#   tests/cluster-e2e/run_e2e.sh               # this script
#
# Runs the `cat` worker as a real map operation and asserts the output table is
# identical to the input, then repeats with two input and two output tables to
# exercise table switching. Finishes with a wordcount map-reduce.
#
# Requires: the `yt` CLI (`pip install ytsaurus-client`) and a reachable cluster.
set -euo pipefail

PROXY="${YT_PROXY:-localhost:8000}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE="${YT_E2E_BASE:-//tmp/ytsaurus_rs_e2e}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

BINDIR="$ROOT/target/x86_64-unknown-linux-musl/release-worker"
YSON='<format=binary>yson'

yt() { command yt --proxy "$PROXY" "$@"; }

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()  { printf '   \033[32mok\033[0m %s\n' "$*"; }
die() { printf '   \033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

# ----------------------------------------------------------------- preflight

command -v yt >/dev/null || die "the 'yt' CLI is not installed (pip install ytsaurus-client)"
yt --version >/dev/null 2>&1 || die "cannot reach the cluster at $PROXY"

say "Building worker binaries"
"$ROOT/scripts/build-worker.sh" cat wordcount >/dev/null
for bin in cat wordcount; do
  [ -x "$BINDIR/$bin" ] || die "missing $BINDIR/$bin"
done
# The cluster runs Linux/x86_64; a binary built for the host would fail there
# with a confusing exec error, so check before uploading.
file "$BINDIR/cat" | grep -q 'ELF 64-bit LSB.*x86-64' \
  || die "cat is not a Linux x86-64 binary: $(file -b "$BINDIR/cat")"
ok "built and verified"

say "Preparing Cypress"
yt remove "$BASE" --recursive --force
yt create map_node "$BASE" --recursive >/dev/null
ok "$BASE"

# ------------------------------------------------------- single-table identity

say "Uploading fixtures"
python3 "$ROOT/tests/cluster-e2e/generate_fixtures.py" >/dev/null

# A table holds data rows only, never control records. `table_rows_N.bin` is
# exactly that: a list fragment of rows, which is what `write-table` wants.
FIXTURES="$ROOT/tests/cluster-e2e/fixtures"
cp "$FIXTURES/table_rows_0.bin" "$WORK/rows.yson"
cp "$FIXTURES/table_rows_1.bin" "$WORK/rows2.yson"

yt write-table --format "$YSON" "$BASE/input" < "$WORK/rows.yson"
ok "$(yt get "$BASE/input/@row_count") rows in $BASE/input"

say "Running cat as a map operation"
# `--spec` is parsed as YSON, not JSON: `=` for key/value and `;` between
# entries. A JSON spec fails with "Unexpected token ':'".
yt map "./cat" \
  --src "$BASE/input" --dst "$BASE/output" \
  --format "$YSON" \
  --local-file "$BINDIR/cat" \
  --spec '{mapper={memory_limit=536870912}}'
ok "operation finished"

say "Comparing input and output byte-for-byte"
# Both sides are read back through the same path, so an identity map must
# produce identical bytes. Comparing against the uploaded file instead would
# fail for reasons that have nothing to do with the job (the cluster re-encodes).
yt read-table --format "$YSON" "$BASE/input"  > "$WORK/before.bin"
yt read-table --format "$YSON" "$BASE/output" > "$WORK/after.bin"
cmp "$WORK/before.bin" "$WORK/after.bin" || die "cat changed its input"
ok "identical ($(wc -c < "$WORK/before.bin") bytes)"

# ------------------------------------------------------ two tables + switching

say "Two input tables, two output tables, with table switching"
yt write-table --format "$YSON" "$BASE/in0" < "$WORK/rows.yson"
yt write-table --format "$YSON" "$BASE/in1" < "$WORK/rows2.yson"

yt map "./cat --tables 2" \
  --src "$BASE/in0" --src "$BASE/in1" \
  --dst "$BASE/out0" --dst "$BASE/out1" \
  --format "$YSON" \
  --local-file "$BINDIR/cat" \
  --spec '{mapper={memory_limit=536870912;enable_input_table_index=%true}}'

for i in 0 1; do
  yt read-table --format "$YSON" "$BASE/in$i"  > "$WORK/in$i.bin"
  yt read-table --format "$YSON" "$BASE/out$i" > "$WORK/out$i.bin"
  cmp "$WORK/in$i.bin" "$WORK/out$i.bin" || die "table $i diverged"
  ok "table $i identical"
done

# ------------------------------------------------------------------ wordcount

say "Wordcount map-reduce"
yt remove "$BASE/lines" --force
python3 - "$WORK/lines.yson" <<'PY'
import struct, sys

def uvarint(v):
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80); v >>= 7
    out.append(v); return bytes(out)

def zz(v):
    return uvarint(((v << 1) ^ (v >> 63)) & 0xFFFFFFFFFFFFFFFF)

def s(b):
    return b"\x01" + zz(len(b)) + b

LINES = [b"the quick brown fox", b"jumps over the lazy dog",
         b"the fox and the dog", b"quick quick fox"]
out = bytearray()
for line in LINES:
    out += b"{" + s(b"text") + b"=" + s(line) + b"};"
open(sys.argv[1], "wb").write(bytes(out))
PY
yt write-table --format "$YSON" "$BASE/lines" < "$WORK/lines.yson"

yt map-reduce \
  --mapper "./wordcount map" --reducer "./wordcount reduce" \
  --reduce-by word \
  --src "$BASE/lines" --dst "$BASE/counts" \
  --format "$YSON" \
  --map-local-file "$BINDIR/wordcount" \
  --reduce-local-file "$BINDIR/wordcount" \
  --spec '{mapper={memory_limit=536870912};reducer={memory_limit=536870912};reduce_job_io={control_attributes={enable_key_switch=%true}}}'

yt read-table --format json "$BASE/counts" | python3 -c "
import json, sys
counts = {}
for line in sys.stdin:
    if not line.strip():
        continue
    row = json.loads(line)
    counts[row['word']] = row['count']

expected = {'the': 4, 'quick': 3, 'brown': 1, 'fox': 3, 'jumps': 1,
            'over': 1, 'lazy': 1, 'dog': 2, 'and': 1}
if counts != expected:
    print('   FAIL wordcount mismatch', file=sys.stderr)
    print(f'     got      {sorted(counts.items())}', file=sys.stderr)
    print(f'     expected {sorted(expected.items())}', file=sys.stderr)
    sys.exit(1)
print(f'   ok wordcount matches the reference ({len(counts)} words)')
"

say "All end-to-end checks passed"
echo "Cypress tree left at $BASE; remove it with: yt --proxy $PROXY remove $BASE --recursive"
