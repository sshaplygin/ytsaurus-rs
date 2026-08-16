#!/usr/bin/env python3
"""Fails if a published file pulls in data from outside its own crate.

`include_str!` and `include_bytes!` resolve at **compile time**, so a file
reaching into `tests/skiff-go-interop/` or `tests/cluster-e2e/fixtures/`
compiles in this repository and cannot compile from a `.crate` tarball — the
fixture is not in it, and no consumer can put it there. `cargo package` does not
catch this: it verifies by building the library, not the tests.

`ytsaurus-skiff` 0.3.0 and `ytsaurus-job` 0.3.0 shipped with exactly that
defect. It was looked for beforehand, with a line-based grep, which matched

    include_str!("../../../tests/…

and missed

    hex_fixture(include_str!(
        "../../../tests/…"
    ))

— the same macro with a newline in it. Hence a pattern written to span newlines,
and hence this running in CI rather than being remembered.

It is a regex, not a Rust parser, and so has known edges: it matches an
`include_str!` inside a comment, and it does not see a path built with
`concat!(env!("CARGO_MANIFEST_DIR"), …)`. The first is a false positive an
`exclude` silences; the second is a real gap, and nothing in this workspace
uses that form today.

The fix for a violation is an `exclude` entry in that crate's Cargo.toml, the
way `ytsaurus-skiff` and `ytsaurus-job` already carry one.

Run from anywhere:

    python3 scripts/check_package_includes.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent

# Multi-line by construction: `re.S` plus `\s*` across the paren and the string.
# That is the whole point — see the module docstring.
INCLUDE = re.compile(r'include_(?:str|bytes)!\s*\(\s*"([^"]+)"', re.S)


def run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, cwd=REPO)


def publishable_packages() -> list[dict[str, Any]]:
    """Every workspace member cargo would upload, so `publish = false` is skipped."""
    proc = run("cargo", "metadata", "--no-deps", "--format-version", "1")
    if proc.returncode != 0:
        sys.exit(f"cargo metadata failed:\n{proc.stderr}")
    return [pkg for pkg in json.loads(proc.stdout)["packages"] if pkg.get("publish") != []]


def shipped_files(name: str) -> list[str]:
    """What actually goes in the tarball, so an `exclude`d file is not reported.

    A failure here is **fatal**, not a warning. Skipping a package cargo could
    not list would report zero violations for it, which is the failure mode this
    repository has already been bitten by twice — a guard whose *absence* of
    output is indistinguishable from a pass.
    """
    proc = run("cargo", "package", "--list", "--allow-dirty", "-p", name)
    if proc.returncode != 0:
        sys.exit(f"cargo package --list failed for {name}:\n{proc.stderr}")
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def escapes(rust_file: Path, crate_root: Path) -> list[str]:
    """The include targets in `rust_file` that resolve outside `crate_root`."""
    try:
        source = rust_file.read_text()
    except (OSError, UnicodeDecodeError):
        return []

    out = []
    for match in INCLUDE.finditer(source):
        target = (rust_file.parent / match.group(1)).resolve()
        try:
            target.relative_to(crate_root)
        except ValueError:
            out.append(match.group(1))
    return out


def main() -> int:
    violations: list[tuple[str, str, str]] = []

    for pkg in publishable_packages():
        crate_root = Path(pkg["manifest_path"]).parent.resolve()
        for rel in sorted(shipped_files(pkg["name"])):
            if not rel.endswith(".rs"):
                continue
            rust_file = crate_root / rel
            if not rust_file.is_file():
                continue
            for target in escapes(rust_file, crate_root):
                violations.append((pkg["name"], rel, target))

    if violations:
        print("ERROR: published files include data from outside their own crate.")
        print("These compile here and cannot compile from a crates.io tarball.\n")
        for name, rel, target in violations:
            print(f"  {name}: {rel}")
            print(f"      -> {target}")
        print("\nAdd the file to that crate's `exclude` in Cargo.toml.")
        return 1

    print("OK: no published file reaches outside its own crate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
