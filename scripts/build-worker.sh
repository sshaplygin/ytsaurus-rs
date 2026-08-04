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
set -euo pipefail

TARGET=x86_64-unknown-linux-musl
PROFILE=release-worker
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

args=(build -p ytsaurus-examples --profile "$PROFILE" --target "$TARGET")
for bin in "$@"; do
  args+=(--bin "$bin")
done

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

cargo "${args[@]}"

outdir="target/$TARGET/$PROFILE"
echo
echo "Built into $outdir:"
for f in "$outdir"/*; do
  [ -f "$f" ] && [ -x "$f" ] || continue
  case "$f" in *.d | *.rlib) continue ;; esac
  printf '  %-20s %s\n' "$(basename "$f")" "$(file -b "$f")"
done
