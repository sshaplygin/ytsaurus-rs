# Skiff compatibility contract

What "compatible with the Go SDK" means for `ytsaurus-skiff`, and every gap.

## Reference baseline

- Go module: `go.ytsaurus.tech/yt/go` v0.0.33 (published 2026-06-04).
- Protocol: the [official Skiff format documentation](https://ytsaurus.tech/docs/en/user-guide/storage/skiff).
- Cluster: the local Docker YTsaurus used by `tests/cluster-e2e/`.

The Go version is pinned in [`tests/skiff-go-interop/go.mod`](../tests/skiff-go-interop/go.mod).
Moving it means changing this document, the Go vectors and the bidirectional
test results together, never just because a newer module exists.

Go is the executable reference; C++ is the normative one. A cluster links
`library/cpp/skiff` and its job proxy writes with it; Go implements a subset: no `int128`/`int256` codec, no `$sparse_columns`, no
`$other_columns`. A green Go gate is necessary, not sufficient, and where the
two disagree C++ decides. [`tests/skiff-cpp-interop/`](../tests/skiff-cpp-interop/)
compares against C++ through a wheel; its limits are in its README and
[the full-support plan](./skiff-full-support-plan.md).

## Compatibility matrix

| Surface in Go v0.0.33 | Current Rust state | Ship gate |
| --- | --- | --- |
| `WireType`: all twenty values | Schema model implemented | Rust enum/YSON tests plus Go reference test |
| `Schema`, inline table schema and registry reference | Implemented and structurally validated | table roots must be named-field tuples; format parses/renders against Go-shaped values |
| Dynamic encoder/decoder for the primitives Go codes, variants, repeated variants and tuples | Implemented | Go v0.0.33 vector, Rust round trips, one-byte reads, malformed tag, truncation, blob-limit and row-limit tests. The C++ corpus finds these bytes identical to C++'s for scalars, optionals, list and struct shapes, sparse columns, other-columns, control columns and multiplexed tables. |
| `int128` and `int256` | Implemented, Rust-only | Go v0.0.33 has no codec for either (`decodeStruct`/`decodeSimpleTypeGeneric` answer "unexpected wire type"; the encoder matches), so the shared corpus cannot contain them, and the C++ bindings expose no 128- or 256-bit type. Byte order is asserted against Rust alone until a cluster fixture or a newer Go SDK settles it. |
| `uint128` and `uint256` | Missing | C++ `EWireType` has both, so a `uuid` column, carried as `uint128`, cannot be described. Go has neither, so no Go gate will surface this. Phase 1 of [the full-support plan](./skiff-full-support-plan.md). |
| Typed rows and schema inference | Planned | Go → Rust and Rust → Go byte vectors. This also blocks the format decision: [format-comparison.md](format-comparison.md) can compare typed YSON only against dynamic Skiff, and typed YSON against typed Skiff cannot be measured until this ships. |
| `Format`, `InferFormat`, `MustInferFormat` | Format model only; inference planned | generated YSON compared structurally |
| Dynamic decoder, indexes and key switch | Implemented | one-byte reads, Go vector, row/range/key-switch extraction tests |
| Typed `Scan` / typed `Write` | Planned | descriptor and Go decoder interop |
| `SkiffJobReader` | Implemented for dynamic rows | shared Go control corpus, system-field prefix and reduce-control tests |
| `SkiffJobWriter` | Implemented for dynamic rows | one single-table Skiff stream per descriptor; input-only system fields rejected; real `skiff_cat` worker e2e |
| Shared worker/client format selection | Implemented | non-exhaustive `DataFormat` enum drives worker I/O, operation specs, and direct table I/O; YSON and Skiff remain explicit row representations |
| Map / map-reduce / reduce / vanilla Skiff operation formats | Implemented | rendered spec tests for map, mapper, reducer and vanilla task; a format whose table-schema count cannot describe the operation's tables is refused before the spec is sent; binary YSON remains the default |
| Skiff table client I/O | Implemented; single-table map path cluster-verified, and run at nine columns and 412 554 rows on the local cluster | mock-proxy request-shape/truncation tests; `skiff_launch` on a managed multi-node cluster, `format_compare` on the local Docker cluster under arm64 emulation (below). Both are one input table and one output descriptor; required test 5's multi-table shapes and Go against the cluster are still required. |

No typed row codec or inference API is claimed compatible until its gate is
green; the dynamic APIs stay pre-release until the real-cluster and
bidirectional Go gates below are.

### Cluster runs

`skiff_launch`, on a managed multi-node cluster on 2026-08-09: a Skiff stream
written, mapped and read back, both rows compared element by element,
including non-UTF-8 `string32`. It verifies the dynamic Skiff map path end to
end, previously checked only against a mock proxy and a local worker. A
one-table, one-output map emits no indexes, key switches or extra descriptors,
and Go ran only against checked-in vectors, so the claim is "the dynamic Skiff
map path is cluster-verified", not "Skiff is cluster-verified".

`format_compare`'s `project` task, on 2026-08-13/14, on the single-node local
Docker cluster running x86-64 images under arm64 emulation: a Skiff map over
412 554 rows / 48 MiB in one job, nine mixed-type columns, a `Variant8`
optional column, `string32` columns that are deliberately not UTF-8, and a
hand-written positional schema. Its decoded output was diffed row for row
against a typed-serde YSON leg, a `YsonValue` leg and a YQL query at the start
of each of three nine-round runs, before any clock was read; all agreed. That diff was exact and order-sensitive, and the matrix row rests
on it. The harness now compares sorted canonical binary-YSON encodings as a
multiset, so a re-run confirms presence, absence and multiplicity, not order.

It also showed that YQL's own job I/O is Skiff (read from the operation
spec), with a schema that differs from the hand-written one only in carrying
`$row_index` as `variant8<nothing;int64>` and the derived boolean as optional;
and two costs in this crate: the decoder `Box::new()`s an absent variant's
payload on every row, and the reader makes ~14 `read_exact` calls a row
through `BufReader` where the YSON path parses in place. It adds nothing to
required test 5.

## Required tests

1. C++ corpus. `cargo test -p ytsaurus-skiff --test cpp_interop` asserts this
   crate against the checked-in C++ bytes in both directions, in CI.
   Regenerating them is manual, when the `requirements.txt` pin moves
   ([README](../tests/skiff-cpp-interop/README.md)). This is the normative
   gate as far as it reaches: where it and the Go corpus disagree, it is right.
2. Go corpus. `(cd tests/skiff-go-interop && go test ./...)` compiles the
   pinned SDK and verifies its reference vectors: the small `Variant16` /
   `uint64` / `string32` framing vector matching the example shared by
   [@AzazKamaz](https://gist.github.com/AzazKamaz/711234fde6c17cfe04c83702bced19d9),
   plus shared scalar, optional-field and job-control corpora. The scalar
   corpus also exercises a schema-registry reference in both decoders.
3. Rust unit/property/fuzz tests: boundary, malformed-length, malformed-tag
   and truncation-at-every-byte coverage for every supported wire type, and a
   deterministic 10,000-stream fuzz smoke test of nested variants and blobs
   under both limits. No input may panic or allocate past the configured row
   limit: `max_blob_bytes` bounds one payload and `max_row_bytes` the decoded
   row, since a repeated variant costs far more in memory than on the wire;
   the first bound alone does not imply the second.
4. Bidirectional differential tests. The checked-in scalar corpus is encoded
   and decoded independently by Go and Rust. Extend it to Go's optional,
   complex and registry forms; compare bytes when the format is canonical and
   decoded values otherwise.
5. Cluster fixtures. Capture raw Skiff streams from real jobs, covering table
   indexes, row/range indexes, key switches and multiple output descriptors.
   Open: `skiff_launch` (2 rows, 2 columns) and `format_compare` (412 554
   rows, nine columns) each have one input table and one output descriptor. `format_compare` was scoped to one output so a
   failure in this open ground could not be measured as slowness. Next is a
   two-input, two-output Skiff shape, as `cat --tables 2` is for YSON, which
   needs no Go on the cluster.
6. Regression. Binary-YSON unit, offline e2e and cluster e2e tests stay
   green; Skiff must not alter their byte-exact behavior.

## Attribution

[@AzazKamaz](https://gist.github.com/AzazKamaz/711234fde6c17cfe04c83702bced19d9)
provided the initial source-job framing example. Its `u16` table selector and
little-endian fixed/length-prefixed fields are a useful practical vector; a
complete implementation must also carry and validate the protocol schema,
format attributes and job system fields.
