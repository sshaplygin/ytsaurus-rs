#!/usr/bin/env python3
"""Fails if README.md's quick start differs from the example it quotes.

The README's quick-start block is the ```rust fence on the line after
`<!-- quickstart.rs -->`. It must equal, line for line, the lines of
crates/ytsaurus-client/examples/quickstart.rs between `// README-START` and
`// README-END`, which `cargo clippy --all-targets` compiles.

    python3 scripts/check_readme_example.py              # check
    python3 scripts/check_readme_example.py --self-test  # the check fails on bad input
"""

from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "README.md"
EXAMPLE = REPO / "crates" / "ytsaurus-client" / "examples" / "quickstart.rs"

MARKER = "<!-- quickstart.rs -->"
FENCE = "```rust"
START = "// README-START"
END = "// README-END"


class Malformed(Exception):
    """A marker is missing or out of place."""


def readme_block(text: str) -> list[str]:
    lines = text.splitlines()
    found = lines.count(MARKER)
    if found != 1:
        raise Malformed(f"README.md: expected one {MARKER!r} line, found {found}")
    at = lines.index(MARKER)
    if at + 1 >= len(lines) or lines[at + 1] != FENCE:
        raise Malformed(f"README.md: {MARKER!r} must be followed by {FENCE!r}")
    body = lines[at + 2 :]
    if "```" not in body:
        raise Malformed("README.md: the quick-start fence is never closed")
    return body[: body.index("```")]


def example_region(text: str) -> list[str]:
    lines = text.splitlines()
    for marker in (START, END):
        found = lines.count(marker)
        if found != 1:
            raise Malformed(f"quickstart.rs: expected one {marker!r} line, found {found}")
    begin, end = lines.index(START), lines.index(END)
    if end < begin:
        raise Malformed(f"quickstart.rs: {END!r} comes before {START!r}")
    return lines[begin + 1 : end]


def differences(readme: str, example: str) -> list[str]:
    """The unified diff from example to README; empty when they match."""
    return list(
        difflib.unified_diff(
            example_region(example),
            readme_block(readme),
            fromfile="quickstart.rs (README-START..README-END)",
            tofile="README.md (quick start)",
            lineterm="",
        )
    )


def self_test() -> list[str]:
    """Each case that does not behave, described; empty when all pass."""
    failed = []
    example = f"//! doc\n\n{START}\nfn main() {{\n    run();\n}}\n{END}\n"
    readme = f"# t\n\n{MARKER}\n{FENCE}\nfn main() {{\n    run();\n}}\n```\n\nmore\n"

    def expect_diff(name: str, readme: str, example: str, want: bool) -> None:
        got = bool(differences(readme, example))
        if got != want:
            failed.append(f"{name}: differences found is {got}, want {want}")

    def expect_malformed(name: str, readme: str, example: str) -> None:
        try:
            differences(readme, example)
        except Malformed:
            return
        failed.append(f"{name}: not reported as malformed")

    expect_diff("identical", readme, example, False)
    expect_diff("changed line", readme.replace("run()", "walk()"), example, True)
    expect_diff("extra line", readme.replace("```\n\nmore", "// x\n```\n\nmore"), example, True)
    expect_diff("missing line", readme.replace("    run();\n", ""), example, True)
    expect_diff("indentation", readme.replace("    run", "  run"), example, True)
    expect_malformed("no README marker", readme.replace(MARKER, ""), example)
    expect_malformed("two README markers", readme + MARKER + "\n", example)
    expect_malformed("marker without fence", readme.replace(FENCE, "```"), example)
    expect_malformed("unclosed fence", readme.replace("```\n\nmore", "more"), example)
    expect_malformed("no START", readme, example.replace(START, ""))
    expect_malformed("no END", readme, example.replace(END, ""))
    swapped = example.replace(START, "@").replace(END, START).replace("@", END)
    expect_malformed("END before START", readme, swapped)
    return failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        failed = self_test()
        print("\n".join(failed) or "self-test passed")
        return 1 if failed else 0

    try:
        diff = differences(README.read_text(encoding="utf-8"), EXAMPLE.read_text(encoding="utf-8"))
    except Malformed as error:
        print(error)
        return 1
    if diff:
        print("\n".join(diff))
        print("\nREADME.md's quick start differs from the example. Copy the region across.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
