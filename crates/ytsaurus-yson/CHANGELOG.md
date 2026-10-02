# Changelog

All notable changes to this fork are recorded here.

`ytsaurus-yson` is a fork of [ss123she/yson-rs](https://github.com/ss123she/yson-rs).
Recording modifications is a condition of the Apache-2.0 licence (section 4b), and the
list below is that record: everything that differs from upstream `ba2044c`.

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.3.0 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.2.6

Never released: the tag was never cut. This crate had no changes of its own.

## 0.2.5 - 2026-08-10

No changes to this crate beyond the version, which tracks the workspace.

## 0.2.0

No changes to this crate beyond the version, which tracks the workspace.

## 0.1.0 — the fork

Everything below shipped in the crate's first release and still applies.

### Forked

- Vendored `ss123she/yson-rs` at revision
  `ba2044c711cefa65259e25122fea21c36f451093` (2026-04-01, published as v0.1.3),
  `main@HEAD` when taken.
- Renamed the package `yson-rs` to `ytsaurus-yson`; the module layout is
  unchanged, so `yson_rs::x` becomes `ytsaurus_yson::x`.
- Licence: Apache-2.0. Upstream offers yson-rs under MIT or Apache-2.0 at the
  recipient's option; this project takes it under Apache-2.0, the licence of
  the whole repository. Both upstream licence texts (`LICENSE-APACHE`,
  `LICENSE-MIT`) are kept verbatim as notices received with the code, and
  [`NOTICE`](NOTICE) states the attribution and the derivation.
- Moved shared dependency versions to the workspace, and removed the crate-local
  `[profile.release]`: profiles are honoured only in the workspace root, and
  `panic = "abort"`, which must not apply to a library, now lives in the
  workspace `release-worker` profile.
- Renamed the fuzz crate `yson-fuzz` to `ytsaurus-yson-fuzz` and fixed its path
  dependency, which named a package (`yson`) that never existed.

### Fixed

- Three struct shapes serialized to unparseable output instead of valid YSON or
  an error. A struct field renamed to `@x` after a plain field put its `<` inside
  the open map body (`{a=1<x=2>}`, which this crate's parser rejects); it is now
  a serialization error, because attributes stand before the value they
  decorate, and `$value` beside plain fields errors for the same reason. An
  empty struct serialized to zero bytes and an all-attribute struct to `<x=1>`
  with no value node; they now produce `{}` and `<x=1>#`. Regression test:
  `struct_shapes_serialize_to_valid_yson_or_error`.

- `from_slice` ignored everything after the first value, so `42 garbage`
  answered `Ok(42)` and a truncated or concatenated document passed as healthy.
  It now requires the input to be exhausted, whitespace aside, and names the
  offset of the first trailing byte. `StreamDeserializer` reads a sequence of
  values.

- A varint longer than `u64` decoded to a wrong number instead of an error: a
  tenth byte carrying more than the top bit was shifted out silently. Ten-byte
  varints that fit, such as `u64::MAX`, still decode. Regression test:
  `a_ten_byte_varint_that_overflows_is_an_error`.

- A fixed-length visitor (a tuple, a tuple struct, an array) left its closing
  `]` unread: it stops after its length, where a `Vec`'s extra `None` consumes
  the bracket. At the top level it was left as trailing data; nested, it ended
  the enclosing container, so `[[1;2];3]` into `(Vec<i32>, i32)` lost the `3`
  without an error. The deserializer that opens a container now closes it if
  the visitor did not, and a list longer than its tuple is refused, not
  truncated. Text and binary alike. Regression test:
  `a_tuple_consumes_the_bracket_that_closes_it`.

- Decoding an attributed map into `YsonValue` dropped the map. The visitor sees
  the attributes as `@`-keys and the body as a `"$value"` entry (scalars) or as
  plain keys (maps), and knew only `"$value"`, so `<a=b>{x=10}`, the shape of
  every attributed cluster response, decoded to an attributed entity. Plain
  keys are now the body when no `"$value"` is present; `"$value"` beside plain
  keys is a deserialization error naming the extra key. Regression test:
  `an_attributed_map_keeps_its_body`.

- A stray `/` in text input hung the parser forever. In `skip_ignored`, a `/`
  followed by any byte other than `/` or `*` entered the comment branch and hit
  `continue` without advancing the cursor, so `/a` never returned. A `/` that
  opens no comment now ends the skip so the tokenizer rejects the byte, and an
  unterminated `/*` consumes to end-of-input. Found by
  `tests/fuzz_smoke_tests.rs`; regression tests
  `stray_slash_in_text_input_errors_instead_of_hanging` (thread-guarded
  against hanging) and `text_comments_are_still_skipped`.
  Binary mode (`<format=binary>yson`) never calls `skip_ignored` and was not
  exposed.

- Non-UTF-8 map keys were rejected. `YsonValue`'s visitor read keys through
  `String`, failing with `invalid value: byte array, expected a string`, though
  `YsonNode::Map` stores keys as `Vec<u8>` and the lexer decodes them correctly.
  Keys now go through an internal `MapKey` type accepting both string and bytes
  visitor calls. Regression test:
  `non_utf8_map_keys_survive_binary_round_trip`.

- Non-UTF-8 attribute names were replaced with an empty string.
  `FlatStructAccess` built attribute keys with
  `std::str::from_utf8(..).unwrap_or("")`, losing the name and colliding every
  such attribute. Attribute keys now reach the visitor as bytes through an
  internal `ByteKeyDeserializer`; `@`-prefixed struct fields are unaffected,
  since `#[derive(Deserialize)]` generates a `visit_bytes` arm for field
  identifiers. Regression test:
  `non_utf8_attribute_names_survive_binary_round_trip`.

  Both matter because YTsaurus string columns and attribute names are arbitrary
  byte strings, not text.

### Added

- `Serialize` for `YsonValue` and `YsonNode`. Upstream could only decode into
  the DOM, so a pass-through job could not encode it back. Valid UTF-8 takes
  the `serialize_str` path, so text output can use unquoted identifiers, and
  everything else `serialize_bytes`; in binary format both emit identical
  bytes. Attributes go through the existing `$__yson_attributes` marker, so
  `<attrs>value` comes back out. Maps round-trip as values, not bytes:
  `YsonNode::Map` is a `BTreeMap`, so keys come back sorted.

- `scan` module: `scan_value(input, format)` reports the byte length of the
  first complete value in a buffer, or `Scan::Incomplete` if more bytes are
  needed. Upstream's API takes a whole slice, so an input larger than memory
  could not be consumed without it. It walks the token stream without
  allocating or building values.

- `Serializer::with_buffer` / `Serializer::into_output`, so a caller writing
  many values can reuse one allocation. `Serializer::new` is now defined in
  terms of `with_buffer`.

- `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]` on `YsonFormat`, a
  fieldless enum every entry point takes by value, reusable across calls only
  with `Copy`.

- Test suites:
  - `tests/ytsaurus_protocol_tests.rs`: golden bytes for every control record in
    the YTsaurus docs (`table_index`, `row_index`, `range_index`, `key_switch`)
    in binary and text, the documented reduce input stream parsed as a list
    fragment, non-UTF-8 strings, keys and attribute names, strings larger than
    64 MiB, 10 000-column rows, and malformed and deeply nested input.
  - `tests/interop_tests.rs`: round trips against fixtures produced by the Go
    YSON implementation, vendored from
    [ss123she/yson-interop-tests](https://github.com/ss123she/yson-interop-tests).
  - `tests/fuzz_smoke_tests.rs`: a seeded, deterministic no-panic sweep (random
    corpus, truncation at every offset, single-bit corruption) that runs in CI,
    where `cargo fuzz` cannot.

### Known limitations

Carried over from upstream and not fixed here; see the crate README.
