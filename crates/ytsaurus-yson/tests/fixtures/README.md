# Interop fixtures

Copied verbatim from [ss123she/yson-interop-tests](https://github.com/ss123she/yson-interop-tests)
@ `175fc418d38a60d3b5c45ad71b5244424e43ef2e`, `data/` directory.

The `go_to_rust_*` files were produced by the **Go** YSON implementation
(`go.ytsaurus.tech/yt/go/yson`), which makes them an independent reference for
what YTsaurus itself emits. They cover int64 min/max, uint64 max, `%nan`/`%inf`,
escape sequences, non-UTF-8 byte strings, entity, nested lists, empty map and
attributes on both a string and a list.

Used by [`../interop_tests.rs`](../interop_tests.rs).
