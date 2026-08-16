#!/usr/bin/env bash
#
# No published file may `include_str!`/`include_bytes!` something outside its own
# crate.
#
# Those macros resolve at **compile time**, so a file that reaches into
# `tests/skiff-go-interop/` or `tests/cluster-e2e/fixtures/` compiles here and
# cannot compile from a `.crate` tarball — the fixture is not in it, and no
# consumer can put it there. `cargo package` does not catch this: it verifies by
# building the library, not the tests.
#
# `ytsaurus-skiff` 0.3.0 and `ytsaurus-job` 0.3.0 shipped with exactly that
# defect. It was looked for beforehand, with a line-based grep, which matched
#
#     include_str!("../../../tests/…")
#
# and missed
#
#     hex_fixture(include_str!(
#         "../../../tests/…"
#     ))
#
# — the same macro with a newline in it. Hence a real parse below rather than a
# pattern, and hence this running in CI rather than being remembered.
#
# The fix for a violation is an `exclude` entry in that crate's Cargo.toml, the
# way `ytsaurus-skiff` and `ytsaurus-job` already carry one.

set -euo pipefail

cd "$(dirname "$0")/.."

python3 - <<'PY'
import re, subprocess, sys, json
from pathlib import Path

# Multi-line by construction: `re.S` plus `\s*` across the paren and the string.
PAT = re.compile(r'include_(?:str|bytes)!\s*\(\s*"([^"]+)"', re.S)

meta = json.loads(subprocess.run(
    ["cargo", "metadata", "--no-deps", "--format-version", "1"],
    capture_output=True, text=True, check=True).stdout)

bad = []
for pkg in meta["packages"]:
    if pkg.get("publish") == []:            # publish = false
        continue
    root = Path(pkg["manifest_path"]).parent

    # Ask cargo what actually ships, so an `exclude`d file is not reported.
    listed = subprocess.run(
        ["cargo", "package", "--list", "--allow-dirty", "-p", pkg["name"]],
        capture_output=True, text=True)
    if listed.returncode != 0:
        print(f"warning: could not list package {pkg['name']}", file=sys.stderr)
        continue
    shipped = {line.strip() for line in listed.stdout.splitlines() if line.strip()}

    for rel in sorted(shipped):
        if not rel.endswith(".rs"):
            continue
        f = root / rel
        if not f.is_file():
            continue
        try:
            src = f.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        for m in PAT.finditer(src):
            target = (f.parent / m.group(1)).resolve()
            try:
                target.relative_to(root.resolve())
            except ValueError:
                bad.append((pkg["name"], rel, m.group(1)))

if bad:
    print("ERROR: published files include data from outside their own crate.")
    print("These compile here and cannot compile from a crates.io tarball.\n")
    for name, rel, target in bad:
        print(f"  {name}: {rel}")
        print(f"      -> {target}")
    print("\nAdd the file to that crate's `exclude` in Cargo.toml.")
    sys.exit(1)

print("OK: no published file reaches outside its own crate")
PY
