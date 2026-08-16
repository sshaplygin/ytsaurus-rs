# Changelog

All notable changes to `ytsaurus-skiff` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**. It is on crates.io because `ytsaurus-job` and
`ytsaurus-client` depend on it and could not be published otherwise; the ship
gates in [docs/skiff-compatibility.md](../../docs/skiff-compatibility.md) are
still not all green, and the API may change in a patch release.

## 0.3.0 - 2026-08-16

No change to what this crate encodes or decodes — **not one byte moved**, and no
public API changed. What it gained is the evidence for that claim.

- **Added** a comparison against the C++ implementation, in
  [`tests/cpp_interop.rs`](tests/cpp_interop.rs) and
  [`tests/skiff-cpp-interop/`](../../tests/skiff-cpp-interop/). Go had been the
  only reference here, and it is a narrower one than it looks: `yt/go/skiff` has
  no `int128`/`int256` codec, no `uint128`/`uint256` at all, and no
  `$sparse_columns` or `$other_columns` handling anywhere, so a green Go gate
  says much less than the compatibility document implied. The new suite runs
  against `library/cpp/skiff` itself, through the `ytsaurus-yson` PyPI wheel,
  without building Arcadia.

  Everything compared came out **byte-identical**: `int64`, `uint64`, `boolean`,
  `double` and `string32` at their boundaries — including `-0.0` and a non-UTF-8
  payload; `variant8<nothing; T>` optionals on both tags; `repeated_variant8`
  list items and nested `tuple` structs; `$sparse_columns` with none, one and
  several fields set; `$other_columns` carrying C++-written binary YSON; the
  `$row_index` / `$range_index` / `$key_switch` control columns; two table
  schemas multiplexed by the `Variant16` prefix; and a `$name` reference
  resolved through `skiff_schema_registry`.

  What it *found* is not fixed here — it is the numbered work in
  [docs/skiff-full-support-plan.md](../../docs/skiff-full-support-plan.md):
  `uint128`/`uint256` are missing outright, which is the whole `uuid` path;
  `boolean` accepts any non-zero byte where C++ throws above 1; the `0xFFFF`
  end-of-stream tag reads as table index 65535; and control columns are required
  to be a contiguous prefix where upstream accepts them anywhere in the dense
  part.

- **Added** `benches/codec_throughput.rs`, a Criterion benchmark for the encode
  and decode paths, so a change to the codec has a number attached rather than
  an opinion.

## 0.2.6

Never released. The version was bumped in the workspace and the tag was never
cut; these changes reached crates.io in 0.3.0. This crate had none of its own.

## 0.2.5 - 2026-08-10

First release. Published as **pre-release**, by the human decision Hard rule 1
asks for, because `ytsaurus-job` and `ytsaurus-client` depend on this crate and
could not reach crates.io while it was `publish = false`.

Skiff schema model, wire format and bounded streaming codec: the dense and
sparse parts, `$other_columns`, the control columns, `Variant8`/`Variant16`
table multiplexing and a schema registry with `$name` references.
