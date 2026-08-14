#!/usr/bin/env bash
#
# Checks out the `.proto` files `ytsaurus-proto` builds from.
#
# The submodule is the whole YTsaurus monorepo, and only its `yt/yt_proto/`
# subtree is wanted, at exactly the commit this repository pins. So rather than
# `git submodule update --init`, which materialises far more than that, this
# fetches the pinned commit alone: `--depth 1` for one commit, `--no-tags`
# because the monorepo has hundreds and none is needed, `--filter=blob:none` so
# file contents arrive only for the sparse paths, and a sparse checkout of
# `yt/yt_proto/` alone.
#
# That takes a couple of seconds and leaves about 23 MB. Fetching the commit by
# its SHA rather than by branch also means the pin keeps working after upstream
# moves `stable/25.4` on, which a plain `--depth 1` fetch of the branch would
# not.
#
# Idempotent: re-runs are a no-op, and a submodule left on the wrong commit is
# returned to the pinned one.
set -euo pipefail

repository_root=$(git rev-parse --show-toplevel)
cd "$repository_root"

submodule_path=third_party/ytsaurus
sparse_path=yt/yt_proto/yt

# `git config --get` exits 1 when the key is absent, which under `set -e`
# would abort the script with no output at all.
url=$(git config --file .gitmodules --get submodule.ytsaurus.url || true)
if [ -z "$url" ]; then
    echo "error: .gitmodules has no url for submodule.ytsaurus; is it truncated?" >&2
    exit 1
fi

# The pin is the gitlink recorded in the commit, so this script has no SHA of
# its own to drift from it.
pinned=$(git rev-parse "HEAD:$submodule_path" 2>/dev/null || true)
if [ -z "$pinned" ]; then
    echo "error: $submodule_path is not recorded in HEAD; is this the right repository?" >&2
    exit 1
fi

if [ ! -e "$submodule_path/.git" ]; then
    mkdir -p "$submodule_path"
    git init --quiet "$submodule_path"
fi

# Set rather than added, so a submodule left without an origin — or pointed at
# the wrong one — is repaired instead of failing on the fetch below.
if [ -z "$(git -C "$submodule_path" remote get-url origin 2>/dev/null || true)" ]; then
    git -C "$submodule_path" remote add origin "$url"
else
    git -C "$submodule_path" remote set-url origin "$url"
fi

# This is a partial clone: file contents are fetched on demand, and which remote
# to fetch them from is recorded per remote. Removing and re-adding origin loses
# that, after which git cannot resolve any blob it does not already hold and
# prints `invalid object` for each one. Restated every run so the state is
# repaired rather than merely avoided.
git -C "$submodule_path" config remote.origin.promisor true
git -C "$submodule_path" config remote.origin.partialclonefilter blob:none

# Applied on every run, not only on the first. It costs nothing, and a submodule
# left with a narrower cone by some earlier command is otherwise
# indistinguishable from a correct one — the pinned commit is checked out, but
# half the files the build needs are absent.
git -C "$submodule_path" sparse-checkout init --cone
git -C "$submodule_path" sparse-checkout set "$sparse_path"

# Only the network round trip is skipped when the pin is already checked out.
if [ "$(git -C "$submodule_path" rev-parse HEAD 2>/dev/null || true)" != "$pinned" ]; then
    git -C "$submodule_path" fetch --depth 1 --filter=blob:none --no-tags origin "$pinned"
fi

git -C "$submodule_path" checkout --quiet --detach "$pinned"

# Restored every run, and only over the sparse subtree. The commit being right
# says nothing about the working tree: a file deleted by hand, an edit, or a run
# interrupted part-way leaves HEAD at the pin with files missing or wrong, and
# the build then fails somewhere far from the cause. Local edits are discarded
# without ceremony — this is a read-only copy of someone else's repository.
#
# Scoped to `$sparse_path` rather than `checkout --force`, which reaches for
# blobs outside the sparse cone that a blobless clone does not have and prints
# `invalid object` for each one while still succeeding.
git -C "$submodule_path" restore --source="$pinned" --worktree -- "$sparse_path"

# Belt and braces: the restore above fixes tracked files, so anything still
# missing means the sparse configuration is wrong rather than the checkout.
missing=$(git -C "$submodule_path" ls-files --deleted -- "$sparse_path" | head -5)
if [ -n "$missing" ]; then
    echo "error: files are missing from $submodule_path after checkout:" >&2
    echo "$missing" >&2
    exit 1
fi

# The file `ytsaurus-proto`'s build script reads first, checked by name so a
# sparse cone that excludes it fails here rather than inside protoc.
witness=$submodule_path/$sparse_path/client/api/rpc_proxy/proto/api_service.proto
if [ ! -f "$witness" ]; then
    echo "error: $witness is missing after checkout; the sparse configuration is wrong" >&2
    exit 1
fi

count=$(find "$submodule_path/$sparse_path" -name '*.proto' | wc -l | tr -d ' ')
echo "YTsaurus protos ready: $count files at $pinned"
