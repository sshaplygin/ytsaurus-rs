#!/usr/bin/env python3
"""Tallies the pilot's quarantined rows by the reason the job recorded.

`sessionize map` sends every unusable row to a `rejects` table with a `reason`
column — one short, stable string per failure mode. This turns that table into a
histogram, so a pilot run reports *how* its input was bad and not merely how
much of it was. It is the one place in `run_pilot.sh` that reads a table for a
human rather than for an assertion.

It is a file rather than a `python3 -c "..."` block inside the shell script,
which is what it used to be. In **double** quotes the shell rewrites the program
before Python ever sees it, and the rewrites are silent:

    # `reason` is a stable string    <- backticks: the shell runs `reason`
    print("left at $BASE")           <- $BASE expands to the caller's path
    counts[row["reason"]] += 1       <- the `"` ends the shell's argument

A backslash is dropped as well, whenever it precedes a dollar, a backtick, a
double quote or another backslash, so a doubled backslash in a Python literal
arrives single. Only the third line above is noisy; the rest quietly produce a
*different, still-valid* program. This repository's comment style quotes
identifiers in backticks, so the first line is not a hypothetical — writing
house-style Python inside those quotes is enough to hand part of it to the
shell. Single quotes or a quoted heredoc would close the hole too, but a real
file also gets linted, formatted and type-checked like everything else here.

Reads stdin, takes no arguments:

    yt read-table --format json //tmp/ytsaurus_rs_pilot/rejects \\
        | python3 tests/cluster-e2e/summarize_rejects.py

It can fail, and exits 1 rather than printing an empty report, because nothing
else in the pilot would notice:

  * Empty stdin. `run_pilot.sh` has just asserted the rejects table is
    non-empty, so no rows arriving here means the read broke, not that the input
    was clean. Prove it: `: | python3 tests/cluster-e2e/summarize_rejects.py`.
  * A line that is not JSON, or a row without a string `reason` — the format
    flag or the rejects schema changed underneath it. Prove it:
    `echo '{"row_index": 0}' | python3 tests/cluster-e2e/summarize_rejects.py`.

Under the script's `set -euo pipefail` either exit aborts the pilot.
"""

import json
import sys
from collections import Counter
from collections.abc import Iterable
from typing import NoReturn

USAGE = "yt read-table --format json <rejects> | python3 summarize_rejects.py"


def fail(message: str) -> NoReturn:
    """Same shape as `die` in run_pilot.sh, so a failure here reads like the rest."""
    print(f"   \033[31mFAIL\033[0m {message}", file=sys.stderr)
    sys.exit(1)


def tally(lines: Iterable[str]) -> Counter[str]:
    """Counts the `reason` column over JSON-lines rows.

    Blank lines are skipped because `yt read-table` ends its output with one.
    Anything else unexpected is fatal: a rejects row with no reason is a schema
    change, and quietly counting the rows it could parse would hide it.
    """
    reasons: Counter[str] = Counter()
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            fail(f"stdin line {number} is not JSON ({error}); expected --format json")
        if not isinstance(row, dict) or not isinstance(row.get("reason"), str):
            fail(f"stdin line {number} has no string `reason` column: {line.strip()[:120]}")
        reasons[row["reason"]] += 1
    return reasons


def main() -> None:
    if len(sys.argv) > 1:
        fail(f"takes no arguments and reads stdin: {USAGE}")

    reasons = tally(sys.stdin)
    if not reasons:
        fail(f"no rows on stdin, so the rejects table read as empty: {USAGE}")

    for reason, count in sorted(reasons.items()):
        print(f"   {count:>4}  {reason}")


if __name__ == "__main__":
    main()
