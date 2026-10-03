# Changelog

All notable changes to `ytsaurus-skiff` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**. It is on crates.io because `ytsaurus-job` and
`ytsaurus-client` depend on it; the ship gates in
[docs/skiff-compatibility.md](../../docs/skiff-compatibility.md) are still not
all green, and the API may change in a patch release.

## 0.3.1 - 2026-08-16

### Fixed

- `tests/wire.rs` is no longer published. It `include_str!`s vectors from
  `tests/skiff-go-interop/`, outside this crate, so `cargo test` inside an
  unpacked 0.3.0 failed; depending on the crate was unaffected.
  `scripts/check_package_includes.py` now checks this in CI.

## 0.3.0 - 2026-08-16

No change to the bytes this crate encodes or decodes, and no public API change.

- Added a byte-level comparison against the C++ `library/cpp/skiff`:
  [`tests/cpp_interop.rs`](tests/cpp_interop.rs) and
  [`tests/skiff-cpp-interop/`](../../tests/skiff-cpp-interop/). Its coverage and
  findings: [docs/skiff-full-support-plan.md](../../docs/skiff-full-support-plan.md).
- Added `benches/codec_throughput.rs`, a Criterion benchmark of encode and
  decode.

## 0.2.6

Never released: the tag was never cut, and the workspace's 0.2.6 changes reached
crates.io in 0.3.0. This crate had none of its own.

## 0.2.5 - 2026-08-10

First release, as pre-release, because `ytsaurus-job` and `ytsaurus-client`
depend on this crate.

Skiff schema model, wire format and bounded streaming codec: the dense and
sparse parts, `$other_columns`, the control columns, `Variant8`/`Variant16`
table multiplexing and a schema registry with `$name` references.
