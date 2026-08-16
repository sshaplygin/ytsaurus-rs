#!/usr/bin/env python3
"""Fails if tracing, TLS or an async runtime reaches the static musl worker.

`tls` and `tracing` are both off in a worker build, and both would break or
bloat it: `rustls` reaches `ring`, which wants a C cross-compiler, and a worker
should carry only what it runs on. Neither is off by accident — `ytsaurus-job`
takes the client as a `default-features = false` dev-dependency, for the
`selfrun` example — so this asserts the invariant rather than trusting it.

`tokio` and `prost` are here for the same reason and a newer one: the client's
`rpc` feature reaches both, and that feature is off by default *and required to
stay off* precisely because this graph is what a musl worker links.
`ytsaurus-client`'s changelog promises this check exists, so it does.
`rustls-platform-verifier` is named for a third reason: the
`platform-verifier` feature is gated on `tls`, and this is what says so out
loud.

The graph is read **with** dev-dependencies, which is the whole point: cargo
compiles a package's dev-dependencies whenever it builds that package's
examples, and the workers are examples. That is also why criterion is pinned
below 0.8 — 0.8 reaches `alloca`, whose build script wants the C
cross-compiler this build is meant not to need.

Two things are deliberate about the shape of this check.

*One* `cargo tree` invocation that must succeed, rather than a `cargo tree -i`
call per crate whose *failure* was the passing signal. `-i` exits 101 both when
the crate is absent and when cargo itself could not run — a typo'd `-p`, a
manifest or lockfile error, an unreachable registry — so reading its exit code
turned every one of those into a silent pass. The message does not separate
them either: `-i` is resolved before `-p`, so a misspelled package prints the
same "did not match any packages" as an absent crate. One invocation that must
succeed puts all of that on the failing side and leaves a plain membership
test.

And the sentinel. A graph that lost the client is a graph this check is not
reading, and every absence below it would be vacuously true — the guard would
pass loudest exactly when it had stopped looking at anything.

No cross toolchain is needed: `cargo tree --target` resolves the dependency
graph without compiling, so this runs on macOS as well as on CI's Linux.

Run from anywhere:

    python3 scripts/check_worker_graph.py

To prove it can fail — and it must be provable, because a guard whose silence
is indistinguishable from a pass is what this repository has been bitten by
twice — add a crate that *is* in the worker graph (`serde`, say) to FORBIDDEN.
It exits 1, names the crate and prints the inverse tree that leads to it.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

PACKAGE = "ytsaurus-job"
TARGET = "x86_64-unknown-linux-musl"

# Without this in the graph, every absence below means nothing. See the docstring.
SENTINEL = "ytsaurus-client"

FORBIDDEN = ("tracing", "rustls", "ring", "rustls-platform-verifier", "tokio", "prost")


def cargo_tree(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("cargo", "tree", "-p", PACKAGE, "--target", TARGET, *args),
        capture_output=True,
        text=True,
        cwd=REPO,
    )


def worker_graph() -> set[str]:
    """Every crate name in the worker's dependency graph.

    A failure here is **fatal**. Returning an empty or partial graph would
    report zero violations, which is the one outcome this check exists to make
    impossible.
    """
    proc = cargo_tree("--prefix", "none", "--no-dedupe")
    if proc.returncode != 0:
        sys.exit(f"cargo tree failed for {PACKAGE}:\n{proc.stderr}")

    # `--prefix none` puts the crate name first on every non-blank line; the
    # rest is version, path and feature noise.
    return {line.split()[0] for line in proc.stdout.splitlines() if line.split()}


def print_inverse_tree(crate: str) -> None:
    """Show *how* the crate got in, so the fix is not a guessing game.

    Diagnostic only: its exit code is ignored on purpose, because the caller has
    already decided this is a failure and a broken `-i` must not turn that into
    a different, less informative error.
    """
    proc = cargo_tree("-i", crate)
    print(proc.stdout, end="")
    print(proc.stderr, end="", file=sys.stderr)


def main() -> int:
    graph = worker_graph()

    if SENTINEL not in graph:
        print(f"ERROR: the worker graph does not contain {SENTINEL}")
        for crate in sorted(graph):
            print(crate)
        return 1

    for crate in FORBIDDEN:
        if crate in graph:
            print(f"ERROR: {crate} reached the worker build")
            print_inverse_tree(crate)
            return 1
        print(f"OK: no {crate} in the worker graph")

    return 0


if __name__ == "__main__":
    sys.exit(main())
