#!/usr/bin/env bash
# Build worker binaries as fully static x86_64 Linux executables.
#
#   scripts/build-worker.sh            # all example workers
#   scripts/build-worker.sh cat        # just one
#
# On Linux the stock `cc` driver links the musl target fine (rustc ships the
# musl crt objects and libc.a itself, "self-contained" linking). On macOS there
# is no ELF-capable `cc`, so we point the linker at the `rust-lld` that comes
# with the toolchain — no Homebrew cross-toolchain and no Docker needed.
#
# Every worker is an example of `ytsaurus-job`, which is where the crate they
# demonstrate lives and where its end-to-end tests can run them. Cargo puts an
# example at `<profile>/examples/<name>`; this script **stages each one into
# `<profile>/<name>`**, so what it produces is one flat directory of workers.
# Every document, example and CI step that names
# `target/x86_64-unknown-linux-musl/release-worker/<name>` keeps working, and
# nothing outside this file has to know that they are examples.
#
# `selfrun` is the one that is also a launcher, so it needs `ytsaurus-client` —
# a **path-only, `default-features = false` dev-dependency** of `ytsaurus-job`,
# which is what keeps `rustls` (and so `ring`, and so a C cross-compiler) out of
# this build. `--features example-tls` puts TLS back for an https cluster, and
# is for the host build of the launcher, never for the worker.
set -euo pipefail

TARGET=x86_64-unknown-linux-musl
PROFILE=release-worker
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORKERS=(boom cat counted hello selfrun sessionize shards skiff_cat wordcount)

wanted=()
if [ "$#" -eq 0 ]; then
  wanted=("${WORKERS[@]}")
else
  for name in "$@"; do
    found=0
    for known in "${WORKERS[@]}"; do
      [ "$known" = "$name" ] && found=1 && break
    done
    if [ "$found" -eq 0 ]; then
      echo "error: no worker named '$name'" >&2
      echo "known: ${WORKERS[*]}" >&2
      exit 1
    fi
    wanted+=("$name")
  done
fi

if [ "$(uname -s)" = "Darwin" ]; then
  host="$(rustc -vV | awk '/^host: /{print $2}')"
  lld="$(rustc --print sysroot)/lib/rustlib/$host/bin/rust-lld"
  if [ ! -x "$lld" ]; then
    echo "error: rust-lld not found at $lld" >&2
    echo "hint: rustup component add llvm-tools, or install a musl cross-linker" >&2
    exit 1
  fi
  export RUSTFLAGS="${RUSTFLAGS:-} -Clinker=$lld"
fi

outdir="target/$TARGET/$PROFILE"

args=(build -p ytsaurus-job --profile "$PROFILE" --target "$TARGET")
for name in "${wanted[@]}"; do
  args+=(--example "$name")
done
cargo "${args[@]}"

# Staged rather than left where cargo put it, so the output directory has one
# shape. A copy and not a symlink: these get uploaded to a cluster, scp'd and
# docker-cp'd, and a dangling link is a worse failure than a duplicated file.
for name in "${wanted[@]}"; do
  cp -f "$outdir/examples/$name" "$outdir/$name"
done

echo
echo "Built into $outdir:"
for name in "${wanted[@]}"; do
  f="$outdir/$name"
  [ -f "$f" ] && [ -x "$f" ] || continue
  printf '  %-20s %s\n' "$name" "$(file -b "$f")"
done
