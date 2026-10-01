#!/usr/bin/env python3
"""Enforces the countable part of AGENTS.md's *Writing* rules.

Markdown files are checked for em-dashes, bold spans and banned phrases.
Comments in Rust files are checked for banned phrases, issue numbers, history
("used to") and the longest `///` block on one item.

A file with an entry in `prose_budget.json` is held to the counts recorded
there; a metric it does not record is held to the floor. A file without an
entry is held to the defaults, which for markdown scale with its length.

    python3 scripts/check_prose.py              # check
    python3 scripts/check_prose.py --tighten    # lower budgets to current counts
    python3 scripts/check_prose.py --self-test  # the check fails on bad input
    python3 scripts/check_prose.py --bootstrap  # record every file from scratch

`--tighten` never raises a budget and never adds a file. `--bootstrap` records
the current tree, so its output is reviewed like a hand edit.

The history pattern cannot tell "a name used to map" from "this used to be";
write "that maps" for the first.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BUDGET = REPO / "scripts" / "prose_budget.json"
SKIP = ("third_party/", "crates/ytsaurus-proto/src/generated/")

BANNED = re.compile(
    r"learned the hard way|the (?:refutation|contradiction) is the finding"
    r"|sideways, not forward|not settled and not lost|sibling pull request"
    r"|this branch cannot",
    re.I,
)
# `#40`, `(#38)`, `[#30]`, `#36:`; not `[#0:#10]`, `[0#9]` or `PKCS#7`.
ISSUE_REF = re.compile(r"(?<![\w#/&:])#\d+\b(?!:#)")
# "this used to be", "was used to"; not "is used to decode".
HISTORY = re.compile(
    r"(?<!\bis )(?<!\bare )(?<!\bbe )(?<!\bbeen )(?<!\bbeing )\bused to\b|\bwhat it did until\b",
    re.I,
)
BOLD = re.compile(r"\*\*[^*\n]+\*\*")

PER_KW = 5  # em-dashes or bold spans per 1000 words, for a file with no entry
FLOOR = {"em_dash": 3, "bold": 3, "longest_doc": 25}  # everything else: 0

Counts = dict[str, int]
Budget = dict[str, Counts]


def markdown_counts(text: str) -> Counts:
    return {
        "words": len(text.split()),
        "em_dash": text.count("—"),
        "bold": len(BOLD.findall(text)),
        "banned": len(BANNED.findall(text)),
    }


def rust_counts(text: str) -> Counts:
    comments: list[str] = []
    longest = run = 0
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("//"):
            comments.append(stripped)
        is_doc = stripped.startswith("///") and not stripped.startswith("////")
        run = run + 1 if is_doc else 0
        longest = max(longest, run)
    joined = "\n".join(comments)
    return {
        "banned": len(BANNED.findall(joined)),
        "issue_refs": len(ISSUE_REF.findall(joined)),
        "history": len(HISTORY.findall(joined)),
        "longest_doc": longest,
    }


def metrics(counts: Counts) -> list[str]:
    return [m for m in counts if m != "words"]


def allowed(counts: Counts, metric: str, entry: Counts | None) -> int:
    floor = FLOOR.get(metric, 0)
    if entry is not None:
        return max(floor, entry.get(metric, 0))
    if metric in ("em_dash", "bold"):
        return max(floor, counts["words"] * PER_KW // 1000)
    return floor


def violations(files: dict[str, Counts], budget: Budget) -> list[str]:
    found = []
    for path, counts in sorted(files.items()):
        for metric in metrics(counts):
            limit = allowed(counts, metric, budget.get(path))
            if counts[metric] > limit:
                found.append(f"{path}: {metric} is {counts[metric]}, budget {limit}")
    return found


def over_floor(counts: Counts, keep: Counts | None = None) -> Counts:
    """Metrics above the floor, each capped at `keep`'s value where given."""
    entry = {}
    for metric in metrics(counts):
        value = counts[metric]
        if keep is not None:
            value = min(value, max(FLOOR.get(metric, 0), keep.get(metric, 0)))
        if value > FLOOR.get(metric, 0):
            entry[metric] = value
    return entry


def tighten(files: dict[str, Counts], budget: Budget) -> Budget:
    new = {}
    for path in sorted(budget):
        # An empty entry still holds the file to the floor, not the defaults.
        if path in files:
            new[path] = over_floor(files[path], budget[path])
    return new


def bootstrap(files: dict[str, Counts]) -> Budget:
    return {path: e for path, counts in sorted(files.items()) if (e := over_floor(counts))}


def tracked_files() -> dict[str, Counts]:
    listed = subprocess.run(
        ["git", "ls-files", "*.md", "*.rs"], capture_output=True, text=True, cwd=REPO, check=True
    ).stdout.split()
    files = {}
    for path in listed:
        if path.startswith(SKIP):
            continue
        text = (REPO / path).read_text(encoding="utf-8")
        files[path] = markdown_counts(text) if path.endswith(".md") else rust_counts(text)
    return files


def self_test() -> list[str]:
    """Each case that does not behave, described; empty when all pass."""
    failed = []

    def expect(name: str, got: object, want: object) -> None:
        if got != want:
            failed.append(f"{name}: got {got!r}, want {want!r}")

    def flagged(files: dict[str, Counts], budget: Budget) -> set[str]:
        return {v.split(": ")[1].split(" ")[0] for v in violations(files, budget)}

    bad_md = {"a.md": markdown_counts("**a** **b** **c** **d** — — — —. Learned the hard way.")}
    expect("bad markdown", flagged(bad_md, {}), {"em_dash", "bold", "banned"})
    expect("clean markdown", flagged({"a.md": markdown_counts("One — dash.")}, {}), set())

    bad_rs = (
        "/// This used to be a String (#40), see [#30].\n"
        "// From #36: it was used to hold ids; examples used to build rows.\n"
        + "/// x\n" * 30
        + "fn f() {}\n"
    )
    expect("issue refs", rust_counts(bad_rs)["issue_refs"], 3)
    expect("history", rust_counts(bad_rs)["history"], 3)
    expect(
        "bad rust flags",
        flagged({"a.rs": rust_counts(bad_rs)}, {}),
        {"history", "issue_refs", "longest_doc"},
    )
    clean_rs = "/// Reads `//tmp/t[#0:#10]`, key `[0#9]`, PKCS#7; is used to decode.\nfn f() {}\n"
    expect("clean rust", flagged({"a.rs": rust_counts(clean_rs)}, {}), set())

    # --tighten never raises and never adds a file.
    expect("tighten skips new file", tighten(bad_md, {}), {})
    expect("tighten caps growth", tighten(bad_md, {"a.md": {"em_dash": 3}}), {"a.md": {}})
    cleaned = {"a.md": {"words": 4000, "em_dash": 0, "bold": 3, "banned": 0}}
    kept = tighten(cleaned, {"a.md": {"bold": 4}})
    cleaned["a.md"]["em_dash"] = 4
    expect("cleaned file stays at the floor", flagged(cleaned, kept), {"em_dash"})
    grown = {"a.md": {"words": 100, "em_dash": 9, "bold": 5, "banned": 0}}
    expect(
        "tighten lowers only",
        tighten(grown, {"a.md": {"em_dash": 7, "bold": 6}}),
        {"a.md": {"em_dash": 7, "bold": 5}},
    )
    # A recorded file is held to counts, not to its length.
    long_doc = {"a.md": {"words": 4000, "em_dash": 0, "bold": 19, "banned": 0}}
    budget = bootstrap(long_doc)
    long_doc["a.md"]["words"] = 1000
    expect("cutting words", flagged(long_doc, budget), set())
    return failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--tighten", action="store_true")
    mode.add_argument("--bootstrap", action="store_true")
    mode.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        failed = self_test()
        print("\n".join(failed) or "self-test passed")
        return 1 if failed else 0

    files = tracked_files()
    if args.bootstrap:
        BUDGET.write_text(json.dumps(bootstrap(files), indent=2) + "\n")
        return 0
    budget = json.loads(BUDGET.read_text()) if BUDGET.exists() else {}
    if args.tighten:
        BUDGET.write_text(json.dumps(tighten(files, budget), indent=2) + "\n")
        return 0
    found = violations(files, budget)
    for line in found:
        print(line)
    if found:
        print(f"\n{len(found)} over budget. See *Writing* in AGENTS.md.")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
