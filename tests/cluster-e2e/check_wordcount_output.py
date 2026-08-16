#!/usr/bin/env python3
r"""Compares the wordcount map-reduce output against a hand-written reference.

    yt read-table --format json //tmp/…/counts | python3 check_wordcount_output.py

Reads the `counts` table as JSON lines on stdin — one `{"word": …, "count": …}`
object per row — and compares the whole mapping to `EXPECTED`. The comparison is
equality, not containment, so a missing word, an extra word and a wrong count all
fail.

`EXPECTED` is a literal rather than something recomputed from the input, so that
a bug in the wordcount job cannot cancel out against the same bug here. It is the
other half of `generate_wordcount_input.py`: change `LINES` there and this table
has to change with it.

**Why this is a fix and not tidying.** In `run_e2e.sh` this was `python3 -c "…"`
with *double* quotes, which made bash — not Python — the first reader of the
source. Inside double quotes the shell still expands `$name`, `$(…)` and
backticks, and still collapses `\\` to `\`. Nothing in the block happened to
contain a `$` or a backtick, so there was no live bug; but a regex anchor
(`r"count$"`), a `"\\d"` escape, or any string that merely looked like a shell
variable was one edit away from being rewritten before Python ever saw it. Worse,
`run_e2e.sh` runs under `set -u`, so a `$` in front of an undefined name does not
mangle the program — it aborts the entire end-to-end run with "unbound variable"
from a line that reads like Python. Source in a `.py` file has exactly one
reader.

This check can fail, and was made to fail by hand:

    echo '{"word": "the", "count": 99}' | python3 check_wordcount_output.py

prints the mismatch and exits 1. Empty stdin — the shape a truncated or failed
`yt read-table` takes — also fails, because `{}` is not `EXPECTED`; silence here
is never mistaken for a pass.
"""

import json
import sys

# The counts implied by `LINES` in generate_wordcount_input.py.
EXPECTED = {
    "the": 4,
    "quick": 3,
    "brown": 1,
    "fox": 3,
    "jumps": 1,
    "over": 1,
    "lazy": 1,
    "dog": 2,
    "and": 1,
}


def main() -> int:
    counts: dict[str, int] = {}
    for line in sys.stdin:
        if not line.strip():
            continue
        row = json.loads(line)
        counts[row["word"]] = row["count"]

    if counts != EXPECTED:
        print("   FAIL wordcount mismatch", file=sys.stderr)
        print(f"     got      {sorted(counts.items())}", file=sys.stderr)
        print(f"     expected {sorted(EXPECTED.items())}", file=sys.stderr)
        return 1

    print(f"   ok wordcount matches the reference ({len(counts)} words)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
