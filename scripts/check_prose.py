#!/usr/bin/env python3
"""Enforces the mechanical half of AGENTS.md's *Writing* rules.

Markdown files are checked for em-dashes, bold spans and banned phrases.
Comments in Rust files are checked for banned phrases, issue numbers, history
("used to") and the longest `///` block on one item.

A file within the default limits needs no entry. A file over them is held to
the counts recorded for it in `prose_budget.json`, which may only go down:

    python3 scripts/check_prose.py              # check
    python3 scripts/check_prose.py --tighten    # lower budgets to current counts
    python3 scripts/check_prose.py --self-test  # the check fails on bad input

`--tighten` never raises a budget. Raising one is an edit to the JSON file,
made by hand and reviewed.
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
# `#40`, `(#38)`; not the row selectors `[#0:#10]`.
ISSUE_REF = re.compile(r"(?<![\[:\w#/&])#\d+\b")
# "this used to be"; not "is used to decode".
HISTORY = re.compile(
    r"(?<!\bis )(?<!\bare )(?<!\bbe )(?<!\bwas )(?<!\bwere )(?<!\bbeen )(?<!\bbeing )"
    r"\bused to\b(?! (?:map|decode|encode|build|compute|identify|select|find|detect|mark|store))"
    r"|\bwhat it did until\b",
    re.I,
)
BOLD = re.compile(r"\*\*[^*\n]+\*\*")

DEFAULT_PER_KW = 5  # em-dashes or bold spans per 1000 words
DEFAULT_FLOOR = 3  # allowed in any file, however short
DEFAULT_DOC_BLOCK = 25  # lines of `///` on one item

Counts = dict[str, int]


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


def defaults(counts: Counts) -> Counts:
    if "words" in counts:
        per_file = max(DEFAULT_FLOOR, counts["words"] * DEFAULT_PER_KW // 1000)
        return {"em_dash": per_file, "bold": per_file, "banned": 0}
    return {"banned": 0, "issue_refs": 0, "history": 0, "longest_doc": DEFAULT_DOC_BLOCK}


def over(counts: Counts, recorded: Counts) -> dict[str, tuple[int, int]]:
    """Metrics above the larger of the default and the recorded budget."""
    result = {}
    for metric, limit in defaults(counts).items():
        allowed = max(limit, recorded.get(metric, 0))
        if counts[metric] > allowed:
            result[metric] = (counts[metric], allowed)
    return result


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


def check(budget: dict[str, Counts]) -> int:
    failures = 0
    for path, counts in sorted(tracked_files().items()):
        for metric, (value, allowed) in over(counts, budget.get(path, {})).items():
            print(f"{path}: {metric} is {value}, budget {allowed}")
            failures += 1
    if failures:
        print(f"\n{failures} over budget. See *Writing* in AGENTS.md.")
    return 1 if failures else 0


def tighten(budget: dict[str, Counts]) -> dict[str, Counts]:
    """Current counts where they exceed the defaults, never above the old budget."""
    new: dict[str, Counts] = {}
    for path, counts in sorted(tracked_files().items()):
        recorded = budget.get(path)
        entry = {}
        for metric, limit in defaults(counts).items():
            if counts[metric] <= limit:
                continue
            if recorded is not None:
                entry[metric] = min(counts[metric], recorded.get(metric, limit))
            else:
                entry[metric] = counts[metric]
        if entry:
            new[path] = entry
    return new


def self_test() -> int:
    bad_md = "One **a** **b** **c** **d** — — — —. Learned the hard way."
    assert over(markdown_counts(bad_md), {}).keys() == {"em_dash", "bold", "banned"}
    assert not over(markdown_counts("Plain text, one — dash."), {})

    bad_rs = "/// This used to be a String (#40).\n" + "/// line\n" * 30 + "fn f() {}\n"
    assert over(rust_counts(bad_rs), {}).keys() == {"history", "issue_refs", "longest_doc"}
    clean_rs = (
        "/// Reads `//tmp/t[#0:#10]`; the buffer is used to decode rows.\n"
        "/// A name used to map a field to a column.\nfn f() {}\n"
    )
    assert not over(rust_counts(clean_rs), {}), rust_counts(clean_rs)

    counts = markdown_counts(bad_md)
    assert not over(counts, {"em_dash": 4, "bold": 4, "banned": 1})
    print("self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tighten", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    budget = json.loads(BUDGET.read_text()) if BUDGET.exists() else {}
    if args.tighten:
        BUDGET.write_text(json.dumps(tighten(budget), indent=2) + "\n")
        return 0
    return check(budget)


if __name__ == "__main__":
    sys.exit(main())
