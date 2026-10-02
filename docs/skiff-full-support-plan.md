# Plan: full Skiff support

What "full Skiff support" means against the C++ implementation, in build
order, with the test that closes each piece;
[`skiff-compatibility.md`](./skiff-compatibility.md) records what is done.
"Full" is the surface `library/cpp/skiff`, `yt/yt/library/skiff_ext` and
`yt/yt/library/formats/skiff_*` expose: a cluster can hand a job or client any
of it.

## The audit

On 2026-08-16 the crate was compared against the C++ implementation using
`ytsaurus-yson` 0.4.10, the PyPI wheel whose `load_skiff` / `dump_skiff` are
compiled `library/cpp/skiff`: the real writer and parser, with no Arcadia
build. The harness is [`tests/skiff-cpp-interop/`](../tests/skiff-cpp-interop/),
and its Rust half runs with the workspace suite.

Everything compared was byte-identical: `int64`, `uint64`, `boolean`, `double`
and `string32` at their boundaries (including `-0.0` and a non-UTF-8 payload);
`variant8<nothing; T>` optionals on both tags; `repeated_variant8` list items
and nested `tuple` structs; `$sparse_columns` as a `repeated_variant16` with
none, one and several fields set; `$other_columns` carrying C++-written binary
YSON; the `$row_index` / `$range_index` / `$key_switch` control columns; two
table schemas multiplexed by the `Variant16` prefix; and a `$name` reference
resolved through `skiff_schema_registry`. So nothing in Phases 1–3 changes the
bytes this crate writes today: every item adds a type, a rule or a framing
tag.

The bindings, not the format, limit the audit; the details are in
[its README](../tests/skiff-cpp-interop/README.md#two-doors-because-neither-reaches-everything).
`int8`/`int16`/`int32` and their unsigned twins are unreachable through either
door although the C++ converters implement them, and
`int128`/`uint128`/`int256`/`uint256` appear nowhere in `yt/yt/python/`;
building the bindings from source would not help. `$remaining_row_bytes` and
the end-of-stream tag live in `yt/yt/library/formats/`, so the bindings never
emit them. A cluster reaches all of it.

## Where the work sits

| Layer | Upstream | Here | What breaks when it is wrong |
| --- | --- | --- | --- |
| Serialization | `library/cpp/skiff` | `ytsaurus-skiff` `wire.rs` | Bytes. A stream is misread or unreadable. |
| Table schema matching | `yt/yt/library/skiff_ext/schema_match.cpp` | partly `ytsaurus-job` `skiff.rs` | Columns. A valid schema is refused, or a column lands in the wrong field. |
| Logical type mapping | `yt/yt/library/formats/skiff_yson_converter.cpp` | absent | Types. A `decimal` or a `list<T>` column cannot be expressed at all. |

Each layer fails differently, so each phase below has its own gate.

## Phase 1: serialization parity

Divergences the 2026-08-16 C++ run found, most costly first.

1. `uint128` and `uint256`. Add to `WireType`, `Value` and both codecs.
   `uint128` is sixteen little-endian bytes, `uint256` thirty-two, matching
   `ParseUint128`/`ParseUint256`, which read two and four little-endian `ui64`
   words, low word first. `uuid` columns travel as `uint128`, so today there is
   no `uuid` path.
2. Strict `boolean`. `TUncheckedSkiffParser::ParseBoolean` throws above 1.
   Decode `0x02` as an error, not as `true`. The Go SDK is lax here too, so
   only the C++ corpus catches a regression.
3. The `0xFFFF` end-of-stream tag. `TSkiffWriter::Close` writes it when
   `EnableEndOfStream` is set. `Decoder::next_row` should read it as a clean
   end of stream rather than as table index 65535, and `Encoder` should grow an
   opt-in `finish_with_end_of_stream`: upstream the tag is a control
   attribute, not part of the framing.
4. Schema validation parity. Reject a variant with no children, as
   `CreateVariant8Schema` does. Keep this crate's child-count ceilings; C++ has
   none, and a `variant8` with 300 children is unreadable.
5. `int256`/`uint256` value accessors. The bytes are carried verbatim, which
   is correct; add a four-`u64` view that hides the word order.

Gate: Rust round trips and boundary tests for the new types, plus a cluster
fixture with a `uuid` column. Neither corpus can close these (Phase 5).

## Phase 2: table schema matching

What `schema_match.cpp` owns. It belongs in `ytsaurus-skiff` as a
`TableDescription`, not in `ytsaurus-job`, because the client's table I/O needs
the same rules.

1. The dense/sparse/other split. `$other_columns` must be `yson32` and last;
   `$sparse_columns` must be `repeated_variant16` and second-to-last if
   `$other_columns` is present, last otherwise; every other root child is a
   dense field and must be named; names are unique across dense and sparse.
2. Control columns anywhere in the dense part. Upstream records where
   `$key_switch`, `$row_index`, `$range_index` and `$remaining_row_bytes` sit;
   `SkiffJobReader` requires a contiguous prefix, refusing a schema the
   cluster accepts.
3. `$sparse_columns` as a named map: `name -> value`, skipping absent and null
   fields, terminated by `0xFFFF`. The wire shape is verified; the naming is
   missing.
4. `$other_columns` as a YSON map: split into named values on read, gather
   unnamed columns on write, and refuse a row with an unnamed column when there
   is no `$other_columns` field.
5. `$row_index` / `$range_index` modes. `variant8<nothing;int64>` is
   `Incremental`: the value is written only when it does not continue the
   previous one, and tag 0 means "one more than the last". The three-child
   `variant8<nothing;int64;nothing>` is `IncrementalWithError`, where tag 2
   means the index is unavailable. Only the first is understood today, and
   only as a plain optional.
6. `$remaining_row_bytes`. Declared `int32`, written as a `u32` byte count
   then the rest of the row (`StartBlob`/`FinishBlob` in the C++ writer).
   Last, because it needs a two-pass or buffered encode.

Gate: cluster fixtures, a job whose input schema carries each shape.
`tests/skiff-cpp-interop/` pins the wire shape of items 1, 3 and 4, but not
item 5's incremental form or item 6, which
`yt/yt/library/formats/skiff_writer.cpp` writes.

## Phase 3: YT logical types

`skiff_yson_converter.cpp` maps logical types onto Skiff; without it a caller
can describe `int64` columns and nothing else.

1. Composite types. `optional<T>` → `variant8<nothing; T>`; `list<T>` →
   `repeated_variant8<T>` with tag 0 per item and the `0xFF` terminator;
   `struct` and `tuple` → `tuple`; `variant` → `variant8` or `variant16`;
   `dict<K,V>` → `repeated_variant8<tuple<K,V>>`; `tagged<T>` → `T`. Any type
   at all may instead be carried as `yson32`, the fallback every converter
   accepts. The `composite.hex` vector pins the first three against C++ bytes;
   `variant`, `dict` and `tagged` have no vector.
2. `decimal`. `int32`, `int64`, `int128` or `int256` by precision, holding YT's
   binary decimal representation.
3. `uuid`. `uint128`, with both halves byte-swapped and the high word first:
   `TUuidParser` writes `HostToInet(High)` then `HostToInet(Low)`. It needs its
   own vector.
4. Time-zone types. `tuple<inner; uint16>`, the second element being the
   time-zone index.
5. Schema inference: a Skiff format from a YT table schema, Go's
   `InferFormat`, so reading a typed table needs no hand-written Skiff schema.

Gate: cluster round trips per type: write with YSON, read as Skiff, compare.
A local corpus settles only the shape a composite maps onto; the mapping is
meaningful only against a real table schema.

## Phase 4: typed rows

1. Serde `SkiffRow`: typed `Scan`/`Write` over the dynamic `Value`, so a job
   declares a struct instead of indexing a tuple. Last: it is ergonomics over
   a codec that has to be right first.

## Phase 5: a standing gate against C++

Go stays the always-on reference: one `go test`, no wheels, and the surface
most jobs use. It has no `int128`/`int256` codec, no `uint128`/`uint256`, no
repeated-variant path, no `$sparse_columns` or `$other_columns` handling, and
the same lax boolean, so a green Go gate says little about Phases 1–3.
`tests/skiff-cpp-interop/` covers part of that at no build cost, but exposes
none of those wide types. Neither reaches:

1. Cluster fixtures: open gate 5 in the compatibility document, and the only
   thing that can settle Phases 2 and 3, which are meaningful only against a
   real table schema. A cluster also reaches the narrow and wide integers.
2. A compiled C++ harness. `library/cpp/skiff` is four `.cpp` files but links
   Arcadia's `util`, so this means a Docker builder over the pinned submodule
   that writes vectors for the wide types and reads Rust's back. Last: it is
   the only item that takes on an Arcadia build, and it is needed only if a
   cluster cannot settle Phase 1's types.

## Order

Phase 1 first: it is small, byte-level, and every later phase encodes through
it. Phase 2 next, because a control column in an unexpected place is the
likeliest cluster-written schema to be refused today. Phase 3 is the largest
and depends on both. Phase 4 can slip without blocking anything.
