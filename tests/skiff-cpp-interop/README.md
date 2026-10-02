# C++ Skiff reference vectors

The C++ code under `library/cpp/skiff` is the reference Skiff implementation,
the one a YTsaurus cluster links, the job proxy writes with, and that reads a
job's output back; the Go SDK is a second one. This directory holds byte
vectors produced by the C++ code, and
[`crates/ytsaurus-skiff/tests/cpp_interop.rs`](../../crates/ytsaurus-skiff/tests/cpp_interop.rs)
asserts both directions against them.

## How the C++ code is reached, without building it

`ytsaurus-yson` on PyPI ships `yt_yson_bindings`, a compiled extension built
from `library/cpp/skiff` plus `yt/yt/python/yson/skiff`. `dump_skiff` runs
`TUncheckedSkiffWriter`; `load_skiff` runs `TCheckedSkiffParser` behind the same
`TSkiffSchema` matcher (`yt/yt/library/skiff_ext/schema_match.cpp`) a cluster
uses. No part of the format is reimplemented in Python, and nothing here builds
Arcadia: it is a wheel.

```sh
uv run --no-project --with-requirements requirements.txt python cpp_reference.py
```

Run it from this directory. `uv` resolves the pinned wheels into a throwaway
environment for that one command, with no `.venv` and nothing installed into
the interpreter first on `PATH`. `--no-project` is needed because the root
`pyproject.toml` configures `ruff` and declares no Python package; without it
`uv` looks for one and stops.

That rewrites every `*.hex`, and fails if the C++ writer and the C++ parser
disagree about any vector.

Regeneration is manual: CI does not install the bindings. CI runs
`cargo test --workspace --all-targets`, which includes the Rust side, so every
pull request asserts the crate against these bytes in both directions. Re-run
the script whenever the pin in `requirements.txt` moves, and commit what it
changes.

## Two doors, because neither reaches everything

The bindings expose the C++ code twice, with different control. The one script
generates both families of vectors.

Record vectors (`scalars`, `optional`, `sparse`, `other_columns`,
`system_columns`, `multi_table`) go through `SkiffRecord`. You hand it a Skiff
schema, so the wire shape is exactly what you wrote, but its field types are
limited to `int64`, `uint64`, `boolean`, `double`, `string32` and `yson32`.
`record.cpp:163` calls `YT_ABORT()` on anything else, killing the process: a
deliberate assertion in the binding, not a crash in the format code.

Structured vectors (`composite`) go through the typed-dataclass layer, which
derives the Skiff schema from Python annotations. It gives up control of the
leaf types (the inference widens every integer to `int64`, and a hand-built
Skiff schema passed alongside does not override it), but it is the only door
that emits `repeated_variant8` and a nested `tuple`, the shapes YT's `list` and
`struct` map onto. The record layer cannot build them, and the Go SDK's codec
has no repeated-variant path in either direction.

## Coverage

Asserted byte-for-byte by the Rust tests:

- `int64`, `uint64`, `boolean`, `double`, `string32`, `yson32`
- `variant8` optionals, both tags
- `tuple` rows and nested `tuple`
- `repeated_variant8` (list items, tag 0 each, `0xFF` terminator)
- `repeated_variant16` under `$sparse_columns`, `0xFFFF` terminator
- `$other_columns`, carrying C++-written binary YSON
- `$row_index` / `$range_index` / `$key_switch` control columns
- a multiplexed two-table input stream
- a `$name` reference resolved through `skiff_schema_registry`

Not reachable through the bindings at all, and needing a cluster:

- `int8`, `int16`, `int32`, `uint8`, `uint16`, `uint32`. The C++ converters
  support them (`converter_python_to_skiff.cpp`), but the typed layer's schema
  inference always widens to `int64`/`uint64`, and the record layer refuses them.
- `int128`, `uint128`, `int256`, `uint256`. These appear nowhere in
  `yt/yt/python/`; neither door exposes them. Building the bindings from source
  would not help; the limit is in the bindings' own C++.
- `$remaining_row_bytes` and the `0xFFFF` end-of-stream tag, which live in
  `yt/yt/library/formats/skiff_writer.cpp` rather than in the serialization
  library.
- `decimal`, `uuid`, tz types, `dict`, `variant`, which need a real table
  schema.

[`docs/skiff-compatibility.md`](../../docs/skiff-compatibility.md) tracks those
gaps; its cluster fixtures gate is for them.

## Pinning

`requirements.txt` pins both packages. Like the Go module version, it is part
of the contract: moving it is an intentional change to this directory, the
Rust tests and the compatibility document.
