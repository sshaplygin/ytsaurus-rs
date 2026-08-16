#!/usr/bin/env python3
"""Fails unless every worker binary is a fully static executable.

A YTsaurus job is an arbitrary executable the cluster copies to a node and runs.
Nothing on that node is ours: no glibc of a version we chose, no shared objects
we shipped. A worker that turns out to be dynamically linked does not fail at
build time or at upload time — it fails on the cluster, as a job that exits
non-zero before any of its own code runs, and the operation reports that as a
user error.

So this asserts what `scripts/build-worker.sh` was supposed to have produced.

**It must be able to fail, and the interesting way it can stop failing is by
finding nothing at all.** A glob that matches no binaries would otherwise walk
its loop zero times and report success, which is exactly the shape of guard this
repository has already shipped twice — one that passes because it checked
nothing. Hence the explicit count and the empty-directory error below, and hence
a missing `file` or `ldd` being fatal rather than skipped: on a machine without
them every binary would be "verified" without being read.

Linux only, because `ldd` is. That is not a limitation worth removing — the
binaries are `x86_64-unknown-linux-musl` and CI is where they are asserted.

Run from anywhere, after ./scripts/build-worker.sh:

    python3 scripts/check_worker_binaries.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_DIR = REPO / "target" / "x86_64-unknown-linux-musl" / "release-worker"

# rustc emits static-pie for musl; both spellings mean "no libc.so".
STATIC_MARKERS = ("statically linked", "static-pie linked")

# Build leftovers that share the directory with the binaries.
NOT_A_BINARY = (".d", ".rlib")


def tool(name: str) -> str:
    """Absolute path to a required external tool, or exit.

    Absent tools are fatal on purpose: skipping them would turn this whole
    script into a guard that reports success without reading anything.
    """
    found = shutil.which(name)
    if found is None:
        sys.exit(f"{name} is required to verify worker binaries and was not found on PATH")
    return found


def is_static(path: Path, file_bin: str) -> tuple[bool, str]:
    """What `file` says, and whether it says the binary is static."""
    described = subprocess.run(
        [file_bin, str(path)], capture_output=True, text=True, check=True
    ).stdout.strip()
    return any(marker in described for marker in STATIC_MARKERS), described


def has_dynamic_deps(path: Path, ldd_bin: str) -> bool:
    """True if `ldd` lists a resolved shared object.

    `ldd` exits non-zero on a static binary — "not a dynamic executable" — so the
    exit code is not the signal and stderr is folded in. A resolved dependency is
    what an `=> /` line means.
    """
    proc = subprocess.run([ldd_bin, str(path)], capture_output=True, text=True)
    return any("=> /" in line for line in (proc.stdout + proc.stderr).splitlines())


def main() -> int:
    if not WORKER_DIR.is_dir():
        sys.exit(f"no worker output at {WORKER_DIR} — run ./scripts/build-worker.sh first")

    file_bin, ldd_bin = tool("file"), tool("ldd")

    checked = 0
    for path in sorted(WORKER_DIR.iterdir()):
        if not path.is_file() or path.suffix in NOT_A_BINARY:
            continue
        if not path.stat().st_mode & 0o111:
            continue

        print(f"--- {path}")
        static, described = is_static(path, file_bin)
        print(described)

        if not static:
            print(f"ERROR: {path.name} is not statically linked")
            return 1
        if has_dynamic_deps(path, ldd_bin):
            print(f"ERROR: {path.name} has dynamic dependencies")
            subprocess.run([ldd_bin, str(path)], check=False)
            return 1

        checked += 1

    if checked == 0:
        print(f"ERROR: no worker binaries were produced in {WORKER_DIR}")
        return 1

    print(f"OK: {checked} statically linked worker binaries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
