# ytsaurus-proto

Generated protobuf bindings for the YTsaurus RPC proxy.

**Pre-release and unpublished.** It exists so [`ytsaurus-rpc`](../ytsaurus-rpc)
has types to speak with; the compatibility contract for both is
[docs/rpc-compatibility.md](../../docs/rpc-compatibility.md).

## The files are not copied

`build.rs` runs `prost-build` over the upstream `.proto` files in the
`third_party/ytsaurus` submodule — not over a vendored copy of them. A submodule
records an exact upstream commit, which is the pinning that protocol work
requires, while keeping the definitions byte-identical to upstream's. There is
no copy that can silently drift, and nothing is fetched while building.

The pin is `stable/25.4` @ `c91fcbe2cd0b9bf8a2fbae078885b9d423f22b62`, chosen
over `main` because it is the branch the local cluster this was verified against
actually runs. It moves only when a human moves it.

## Setup

```sh
./scripts/init-protos.sh
```

Run once after cloning. It checks the submodule out **shallow and sparse** —
about 23 MB, holding `yt/yt_proto/` and little else. A bare
`git submodule update --init` would materialise the whole monorepo instead;
that is why the script exists.

`protoc` is not needed on `PATH`: a vendored binary is used unless `PROTOC` is
set, so `cargo test --workspace` works with nothing but the Rust toolchain —
the same standard `scripts/build-worker.sh` is held to.

## What is generated

The transitive import closure of the RPC-proxy API surface: 20 files, listed
explicitly in `build.rs` rather than globbed, so a file appearing upstream does
not silently enlarge what this crate compiles.

`prost` names each module after its protobuf package and writes cross-package
references as `super::`-relative paths, so the module nesting in `lib.rs` mirrors
the package names exactly — it has to, or the generated code does not resolve.
Short aliases sit on top:

| Alias | Package | Holds |
| --- | --- | --- |
| `misc` | `NYT.NProto` | `TGuid`, `TError` |
| `bus` | `NYT.NBus.NProto` | `THandshake` |
| `rpc` | `NYT.NRpc.NProto` | request and response headers |
| `ytree` | `NYT.NYTree.NProto` | the attribute dictionary errors carry |
| `api` | `NYT.NApi.NRpcProxy.NProto` | all 158 request types and their responses |

## Two things to know when using them

**`required` and `optional` are both visible.** These are proto2 files, and
`prost` renders a `required` field as a plain value and an `optional` one as an
`Option`. Which spelling a field has is not guessable from its name; read the
generated type.

**Extensions are not generated.** `prost` does not support proto2 extensions, so
`TRequestHeader`'s extension fields — `credentials_ext` at 110 and the rest —
are absent from the generated struct. `ytsaurus-rpc` appends the credentials
field by hand, which is wire-identical because an extension is an ordinary field
with a reserved number.

## Publishing

`cargo package` does not include submodules, so this crate cannot be published
as it stands: the generated bindings would have to be committed, or the protos
vendored at package time. That is gate H in the compatibility document.
