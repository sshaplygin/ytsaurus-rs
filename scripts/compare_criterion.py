#!/usr/bin/env python3
"""Renders the Criterion main-vs-PR table that the benchmarks workflow posts.

`.github/workflows/benchmarks.yml` runs four suites, each of them twice — once
on the PR base and once on the PR head, on one VM with one toolchain — and
uploads the raw `cargo bench` logs. This turns those logs into the markdown
comment. It is deliberately advisory: GitHub-hosted runners are noisy, so the
±20% marker flags a delta worth a look rather than gating a merge.

It also does the artifact assembly, which used to be a shell loop in the
workflow, so that rendering is one command: point `--logs` at the
`download-artifact` directory and it concatenates
`criterion-main-vs-pr-<suite>/{base,pr}/benchmarks.txt` for every suite itself.
`--files` still accepts two already-assembled logs, which is what keeps the
thing runnable by hand against a pair of `cargo bench | tee` captures.

This is a port of the former `scripts/compare-criterion.mjs` and its output is
byte-for-byte identical. Two pieces of JavaScript arithmetic are load-bearing
and are reproduced rather than improved:

  * `Number.prototype.toFixed` strips the sign *before* it rounds, so a tie
    goes away from zero — `-6.25` prints as `-6.3` where Python's banker's
    `round` gives `-6.2`. Stripping the sign first is also why a benchmark
    0.03% faster is reported as `+0.0%`. See `to_fixed_1`.
  * `String.prototype.localeCompare` sorts by ICU collation, not by codepoint:
    `_` sorts before the digits, and case is only a tie-break, so
    `decode_dynamic` precedes `Serialize Binary`. See `collation_key`.

Neither is hypothetical, and both are reachable from timings Criterion prints
in four significant digits: 160.00 ns against 170.00 ns is a delta of exactly
+6.25, the tie the two rounding rules disagree on, and 1000.0 ns against
999.70 ns rounds to `-0`. Getting either wrong quietly changes a published
comment, so neither is a detail to tidy up.

How it fails, so that its silence means something:

    python3 scripts/compare_criterion.py --files /dev/null /dev/null base pr

exits 1 with "no Criterion time measurements found" rather than printing an
empty and reassuring table. A missing per-suite artifact, a benchmark reported
twice, and Criterion printing mixed units within one measurement all exit 1 as
well, none of them producing a report.

Run from anywhere:

    python3 scripts/compare_criterion.py --logs benchmark-logs <base-sha> <head-sha>
"""

from __future__ import annotations

import argparse
import re
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import NamedTuple

# Advisory only. A delta this large on a shared, noisy runner is worth reading;
# it is not evidence on its own, and nothing fails a build over it.
REGRESSION_THRESHOLD = 20

# The suites benchmarks.yml runs, in the order the shell loop concatenated
# them. Order does not reach the report, which is sorted, but a log assembled
# in a different order would diff against an older one for no reason.
SUITES = ("yson", "skiff", "job", "client")

# The micro sign here is U+00B5, not U+03BC GREEK SMALL LETTER MU; that is the
# codepoint Criterion's formatter emits, and matching on the other one would
# silently drop every sub-millisecond benchmark.
UNIT_TO_NANOSECONDS = {
    "ns": 1,
    "µs": 1_000,
    "us": 1_000,
    "ms": 1_000_000,
    "s": 1_000_000_000,
}

# `[0-9.]`, not `\d`: Python's `\d` also matches e.g. Devanagari digits, which
# `float` then rejects, while JavaScript's `\d` is ASCII-only.
#
# The `]` anchored at end of line is load-bearing. When a baseline exists,
# Criterion prints a *second* `time:   [...]` line holding percentage changes,
# and that one ends in `(p = 0.93 > 0.05)`. The anchor is the only thing
# stopping a relative delta from being read as an absolute measurement.
TIME = re.compile(
    r"^\s*time:\s+\[\s*([0-9.]+)\s*(ns|µs|us|ms|s)"
    r"\s+([0-9.]+)\s*(ns|µs|us|ms|s)"
    r"\s+([0-9.]+)\s*(ns|µs|us|ms|s)\s*\]$"
)


class Measurement(NamedTuple):
    """One benchmark's point estimate: as Criterion printed it, and in ns."""

    display: str
    nanoseconds: float


def read(path: Path) -> str:
    """Read a log the way Node's `readFileSync(path, "utf8")` did.

    `errors="replace"` is that lossy decode. A `cargo bench` log can carry
    arbitrary bytes — a panic payload, a truncated write — and losing the whole
    report to one of them would be a worse outcome than a replacement
    character in a benchmark name.
    """
    return path.read_text(encoding="utf-8", errors="replace")


def read_logs(directory: Path, side: str) -> str:
    """Concatenate one side of the downloaded per-suite artifacts.

    Raw concatenation, byte for byte, exactly as the `cat` loop this replaced
    behaved: a log not ending in a newline runs into the next one. A missing
    artifact is fatal — a report covering three suites while claiming to cover
    four is the failure mode worth avoiding here.
    """
    parts = []
    for suite in SUITES:
        path = directory / f"criterion-main-vs-pr-{suite}" / side / "benchmarks.txt"
        if not path.is_file():
            sys.exit(f"missing benchmark log: {path}")
        parts.append(read(path))
    return "".join(parts)


def parse_benchmarks(text: str, source: str) -> dict[str, Measurement]:
    """Every Criterion point estimate in `text`, keyed by benchmark id.

    Criterion prints an id on a line of its own only when it is longer than 23
    characters; a shorter id shares its line with the timing, and then no line
    starting with `time:` precedes it. Such a benchmark is invisible here, as
    it was to the .mjs. Widening that is a behaviour change, not a fix, and
    would rewrite every historical comparison.

    The `Benchmarking ` guard matters because the workflow captures with
    `2>&1`: Criterion's progress lines go to stderr and can interleave into the
    position where a benchmark id would otherwise sit.
    """
    lines = re.split(r"\r?\n", text)
    benchmarks: dict[str, Measurement] = {}

    for index in range(len(lines) - 1):
        name = lines[index].strip()
        time = TIME.match(lines[index + 1])

        if not time or not name or name.startswith("Benchmarking "):
            continue

        unit = time[4]
        if time[2] != unit or time[6] != unit:
            sys.exit(f"{source}: Criterion printed mixed units for {name}")

        if name in benchmarks:
            sys.exit(f"{source}: benchmark {name} was reported twice")

        benchmarks[name] = Measurement(
            display=f"{time[3]} {unit}",
            nanoseconds=float(time[3]) * UNIT_TO_NANOSECONDS[unit],
        )

    if not benchmarks:
        sys.exit(f"{source}: no Criterion time measurements found")

    return benchmarks


# The ICU root collation order of the printable ASCII characters, which is what
# `localeCompare` was sorting by. It is not codepoint order: `_` and `-` come
# before the digits, and case is a tie-break rather than a primary distinction,
# so `decode_dynamic` sorts before `Serialize Binary` where a codepoint sort
# puts `S` (0x53) first. Case is folded out of this table and applied as the
# second level below.
_PRIMARY_ORDER = " _-,;:!?.'\"()[]{}@*/\\&#%`^+<=>|~$0123456789abcdefghijklmnopqrstuvwxyz"
_PRIMARY = {character: index for index, character in enumerate(_PRIMARY_ORDER)}

CollationKey = tuple[tuple[tuple[int, int], ...], tuple[int, ...]]


def _primary_weight(character: str) -> tuple[int, int]:
    """Where a character sorts at the primary level, ignoring its case.

    Anything outside the table — that is, anything non-ASCII — sorts after
    every character in it, by codepoint. That is a defined fallback rather than
    ICU's answer; a Criterion id in this workspace is built from a Rust string
    literal in `crates/*/benches/`, and none of them reach it.
    """
    index = _PRIMARY.get(character.lower())
    return (0, index) if index is not None else (1, ord(character))


def collation_key(name: str) -> CollationKey:
    """Sort key reproducing `String.prototype.localeCompare` for ASCII ids.

    Two levels, as the Unicode collation algorithm orders them: the primary
    weight of every character first, then case as the tie-break. ASCII carries
    no secondary (accent) weight, so those two levels are the whole comparison,
    and no printable ASCII character is ignorable, so the two levels always
    have the same length once the primary level ties.
    """
    return (
        tuple(_primary_weight(character) for character in name),
        tuple(int(character.isupper()) for character in name),
    )


_ONE_DECIMAL = Decimal("0.1")


def to_fixed_1(value: float) -> str:
    """`Number.prototype.toFixed(1)`, which is not Python's `round`.

    ECMA-262 moves the sign into the output string and negates the value
    *before* choosing the integer to print, so a tie rounds away from zero:
    `(-6.25).toFixed(1)` is `-6.3`, while `round(-6.25, 1)` is `-6.2` because
    Python rounds a tie to even. Hence `Decimal` and `ROUND_HALF_UP` on the
    magnitude.

    Negating first also means `-0` prints as `0.0` with no sign. Combined with
    `-0 >= 0` holding, that is how a benchmark 0.03% faster is reported as
    `+0.0%`, and reproducing it is the difference between matching the old
    output and nearly matching it.
    """
    sign = "-" if value < 0 else ""
    return f"{sign}{Decimal(abs(value)).quantize(_ONE_DECIMAL, rounding=ROUND_HALF_UP)}"


def markdown(value: str) -> str:
    """Escape a benchmark id so it cannot break out of its table cell."""
    return value.replace("\\", "\\\\").replace("|", "\\|")


def revision(value: str) -> str:
    return value[:12]


def change(value: float) -> str:
    return f"{'+' if value >= 0 else ''}{to_fixed_1(value)}%"


def render(
    base: dict[str, Measurement],
    pull_request: dict[str, Measurement],
    base_revision: str,
    pr_revision: str,
) -> str:
    """The whole comment, as one string, without a trailing newline."""
    names = sorted(dict.fromkeys([*base, *pull_request]), key=collation_key)

    regressions = 0
    improvements = 0
    unavailable = 0
    rows = []

    for name in names:
        before = base.get(name)
        after = pull_request.get(name)

        if before is None or after is None:
            unavailable += 1
            rows.append(
                f"| {markdown(name)} "
                f"| {before.display if before is not None else '—'} "
                f"| {after.display if after is not None else '—'} "
                f"| — | not comparable |"
            )
            continue

        # Classify the same one-decimal value the report prints. Without this,
        # e.g. a displayed +20.0% may be a binary float infinitesimally below
        # the advisory threshold and get a contradictory "within runner noise"
        # label.
        relative_change = float(to_fixed_1((after.nanoseconds / before.nanoseconds - 1) * 100))
        # The `noqa`s below silence RUF001's "did you mean `i`/`-`?": these are
        # the emoji of the published comment, not typos for ASCII lookalikes.
        signal = "ℹ️ within runner noise"  # noqa: RUF001
        if relative_change >= REGRESSION_THRESHOLD:
            signal = "⚠️ regression"
            regressions += 1
        elif relative_change <= -REGRESSION_THRESHOLD:
            signal = "✅ improved"
            improvements += 1

        rows.append(
            f"| {markdown(name)} | {before.display} | {after.display} "
            f"| {change(relative_change)} | {signal} |"
        )

    summary = [
        f"📊 {len(names)} benchmark(s) compared",
        f"⚠️ {regressions} regression(s) at ≥{REGRESSION_THRESHOLD}%",
        f"✅ {improvements} improvement(s) at ≥{REGRESSION_THRESHOLD}%",
    ]
    if unavailable > 0:
        summary.append(f"➖ {unavailable} not comparable")  # noqa: RUF001

    return "\n".join(
        [
            "<!-- ytsaurus-rs:criterion-benchmark-comparison -->",
            "## 📊 Criterion benchmarks: main vs PR",
            "",
            f"🔍 Main `{revision(base_revision)}` → PR `{revision(pr_revision)}`",
            "",
            f"**{' · '.join(summary)}**",
            "",
            "ℹ️ Time is lower-is-better. "  # noqa: RUF001
            "Both revisions ran on the same GitHub-hosted VM; "
            "±20% is an advisory marker, not a merge gate.",
            "",
            "| Benchmark | main | PR | change | signal |",
            "| --- | ---: | ---: | ---: | --- |",
            *rows,
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Render the Criterion main-vs-PR benchmark comparison on stdout.",
    )
    parser.add_argument("base_revision", help="commit the `main` column was measured at")
    parser.add_argument("pr_revision", help="commit the `PR` column was measured at")

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--logs",
        type=Path,
        metavar="DIR",
        help="download-artifact directory holding criterion-main-vs-pr-<suite>/{base,pr}",
    )
    source.add_argument(
        "--files",
        nargs=2,
        type=Path,
        metavar=("BASE", "PR"),
        help="two already-assembled `cargo bench` logs",
    )
    args = parser.parse_args(argv)

    if args.logs is not None:
        base = parse_benchmarks(read_logs(args.logs, "base"), f"{args.logs} (base)")
        pull_request = parse_benchmarks(read_logs(args.logs, "pr"), f"{args.logs} (pr)")
    else:
        base_path, pr_path = args.files
        base = parse_benchmarks(read(base_path), str(base_path))
        pull_request = parse_benchmarks(read(pr_path), str(pr_path))

    print(render(base, pull_request, args.base_revision, args.pr_revision))
    return 0


if __name__ == "__main__":
    sys.exit(main())
