# Skiff compatibility contract

This is the implementation contract for `ytsaurus-skiff`. It defines what
“compatible with the Go SDK” means and makes every gap visible before a job is
allowed to depend on it.

## Reference baseline

- **Go module:** `go.ytsaurus.tech/yt/go` **v0.0.33** (published 2026-06-04).
- **Protocol:** the [official Skiff format documentation](https://ytsaurus.tech/docs/en/user-guide/storage/skiff).
- **Cluster:** the local Docker YTsaurus used by `tests/e2e/`.

The Go version is pinned in [`tests/skiff-go-interop/go.mod`](../tests/skiff-go-interop/go.mod).
Changing it requires an intentional change to this document, the Go vectors and
the bidirectional test results. It must never advance merely because a newer
module version is available.

**Go is the executable reference; C++ is the normative one.** `library/cpp/skiff`
is what a cluster links and what the job proxy writes with, and Go implements a
subset of it — no `int128`/`int256` codec, no `$sparse_columns`, no
`$other_columns`. So a green Go gate is necessary and not sufficient, and where
the two disagree C++ decides. [`tests/skiff-cpp-interop/`](../tests/skiff-cpp-interop/)
compares against C++ directly, through a wheel rather than an Arcadia build; its
limits, and what only a cluster can reach, are recorded there and in
[the full-support plan](./skiff-full-support-plan.md).

## Compatibility matrix

| Surface in Go v0.0.33 | Current Rust state | Ship gate |
| --- | --- | --- |
| `WireType`: all twenty values | **Schema model implemented** | Rust enum/YSON tests plus Go reference test |
| `Schema`, inline table schema and registry reference | **Implemented and structurally validated** | table roots must be named-field tuples; format parses/renders against Go-shaped values |
| Dynamic encoder/decoder for the primitives Go codes, variants, repeated variants and tuples | **Implemented** | Go v0.0.33 vector, Rust round trips, one-byte reads, malformed tag, truncation, blob-limit and row-limit tests. The C++ corpus finds these bytes identical to C++'s for scalars, optionals, list and struct shapes, sparse columns, other-columns, control columns and multiplexed tables. |
| `int128` and `int256` | **Implemented, Rust-only** | Go v0.0.33 has no codec for either: `decodeStruct`/`decodeSimpleTypeGeneric` answer "unexpected wire type" and the encoder matches, so the shared corpus cannot contain them. Byte order is asserted against Rust alone until a cluster fixture can settle it. |
| `uint128` and `uint256` | **Missing** | C++ `EWireType` has both and this crate has neither, so a `uuid` column — carried as `uint128` — cannot be described at all. Go has neither either, so no Go gate will ever surface this. Phase 1 of [the full-support plan](./skiff-full-support-plan.md). |
| Typed rows and schema inference | Planned | Go → Rust and Rust → Go byte vectors |
| `Format`, `InferFormat`, `MustInferFormat` | Format model only; inference planned | generated YSON compared structurally |
| Dynamic decoder, indexes and key switch | **Implemented** | one-byte reads, Go vector, row/range/key-switch extraction tests |
| Typed `Scan` / typed `Write` | Planned | descriptor and Go decoder interop |
| `SkiffJobReader` | **Implemented for dynamic rows** | shared Go control corpus, system-field prefix and reduce-control tests |
| `SkiffJobWriter` | **Implemented for dynamic rows** | one single-table Skiff stream per descriptor; input-only system fields rejected; real `skiff_cat` worker e2e |
| Shared worker/client format selection | **Implemented** | non-exhaustive `DataFormat` enum drives worker I/O, operation specs, and direct table I/O; YSON and Skiff remain explicit row representations |
| Map / map-reduce / reduce / vanilla Skiff operation formats | **Implemented** | rendered spec tests for map, mapper, reducer and vanilla task; a format whose table-schema count cannot describe the operation's tables is refused before the spec is sent; binary YSON remains the default |
| Skiff table client I/O | **Implemented; the single-table map path is cluster-verified** | mock-proxy request-shape/truncation tests, plus `skiff_launch` run against a managed multi-node cluster on 2026-08-09: a Skiff stream written, mapped and read back, both rows compared element by element including non-UTF-8 `string32`. That covers **one** table and **one** output descriptor; the multi-table shapes of required test 5 and the Go-against-the-cluster half are still required. |

No typed row codec or inference API is claimed compatible until its ship gate is
green. The implemented dynamic APIs remain pre-release until the real-cluster
and bidirectional Go gates below are green.

**What the 2026-08-09 cluster run did and did not settle.** It settles that the
dynamic Skiff *map* path works against a real multi-node installation end to end
— which until then had only been asserted against a mock proxy and a local
worker. It settles nothing about table indexes, row or range indexes, key
switches or multiple output descriptors, because a one-table, one-output map
never emits any of them; and the Go side ran only against checked-in vectors,
never against the cluster. Required test 5 below is therefore still open, and
the sentence to use about all this is "the dynamic Skiff map path is
cluster-verified", not "Skiff is cluster-verified".

## Required tests

1. **C++ corpus.** `cargo test -p ytsaurus-skiff --test cpp_interop` asserts this
   crate against the checked-in C++ bytes in both directions, and runs in CI with
   the rest of the workspace. Regenerating those bytes —
   `(cd tests/skiff-cpp-interop && python cpp_reference.py)`, which runs the
   compiled `library/cpp/skiff` from a wheel and fails if its writer and parser
   disagree — is a manual step taken when the pin in `requirements.txt` moves;
   CI does not install the bindings. This is the normative gate as far as it
   reaches: where it and the Go corpus disagree, this one is right.
2. **Go corpus.** `(cd tests/skiff-go-interop && go test ./...)` compiles the pinned SDK and
   verifies its reference vectors. It includes the small `Variant16` / `uint64`
   / `string32` framing vector matching the example shared by
   [@AzazKamaz](https://gist.github.com/AzazKamaz/711234fde6c17cfe04c83702bced19d9),
   plus shared scalar, optional-field and job-control corpora. The scalar
   corpus also exercises a schema-registry reference in both decoders.
3. **Rust unit/property/fuzz tests.** Every supported wire type gets boundary,
   malformed-length, malformed-tag and truncation-at-every-byte coverage. A
   deterministic 10,000-stream fuzz smoke test covers nested variants and
   blobs, under both limits. No input may panic or allocate past the configured
   row limit: `max_blob_bytes` bounds one payload and `max_row_bytes` bounds the
   decoded row, because a repeated variant costs far more in memory than on the
   wire and the first bound alone does not imply the second.
4. **Bidirectional differential tests.** The checked-in scalar corpus is
   independently encoded and decoded by both Go and Rust. Extend it to Go's
   optional, complex and registry forms; compare bytes when the format is
   canonical and decoded values otherwise.
5. **Cluster fixtures.** Capture raw Skiff streams from real jobs. Cover table
   indexes, row/range indexes, key switches and multiple output descriptors.
   **Open.** `skiff_launch` has now run on a real cluster (see the matrix row
   above) and covers none of these four: it is one input table and one output
   descriptor by construction. The piece that does not need Go on the cluster is
   a two-input, two-output Skiff shape — the same gap `cat --tables 2` fills for
   the YSON path — and that is the next thing to write here.
6. **Regression.** Existing binary-YSON unit, offline e2e and cluster e2e tests
   stay green. Skiff is additive and must not alter their byte-exact behavior.

## Why the attribution is here

[@AzazKamaz](https://gist.github.com/AzazKamaz/711234fde6c17cfe04c83702bced19d9)
provided the initial source-job framing example. Its `u16` table selector and
little-endian fixed/length-prefixed fields are a useful practical vector, but a
complete implementation must additionally carry and validate the protocol
schema, format attributes and job system fields.
