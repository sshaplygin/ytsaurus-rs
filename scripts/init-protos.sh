#!/usr/bin/env bash
#
# Checks out the `.proto` files `ytsaurus-proto` builds from.
#
# The submodule is the YTsaurus monorepo, and only its `yt/yt_proto/` subtree is
# wanted. A plain `git submodule update --init` would materialise the whole
# thing; this clones it shallow (`--depth 1`), without blobs
# (`--filter=blob:none`) and sparse, so the working tree holds the protos and
# little else — about 23 MB.
#
# Idempotent: safe to re-run, and re-runs restore the pinned commit if the
# submodule has been moved.
set -euo pipefail

repository_root=$(git rev-parse --show-toplevel)
cd "$repository_root"

submodule_path=third_party/ytsaurus
sparse_path=yt/yt_proto/yt

if [ ! -e "$submodule_path/.git" ]; then
    echo "Cloning the YTsaurus protos (shallow, sparse)..."
    git submodule update --init --depth 1 --filter=blob:none -- "$submodule_path"
fi

# `git submodule update` does not know about sparse checkout, so it is applied
# afterwards. Cone mode keeps the pattern a plain directory prefix.
git -C "$submodule_path" sparse-checkout set --cone "$sparse_path"

# Return the submodule to the commit this repository pins, undoing any local
# move. `git submodule update` alone would do it, but only once the sparse
# configuration above is in place.
git submodule update --init --depth 1 --filter=blob:none -- "$submodule_path"

pinned=$(git -C "$submodule_path" rev-parse HEAD)
count=$(find "$submodule_path/$sparse_path" -name '*.proto' | wc -l | tr -d ' ')

echo "YTsaurus protos ready: $count files at $pinned"
