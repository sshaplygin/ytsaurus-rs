# Changelog

All notable changes to this fork are recorded here.

`ytsaurus-yson` is a fork of [ss123she/yson-rs](https://github.com/ss123she/yson-rs).
Recording modifications is a condition of the Apache-2.0 licence (section 4b), and the
list below is that record: everything that differs from upstream `ba2044c`.

## 0.2.0

No changes to this crate beyond the version, which tracks the workspace. The
fork modifications below are unchanged and still apply.

## [Unreleased]

### Forked

- Vendored `ss123she/yson-rs` at revision
  `ba2044c711cefa65259e25122fea21c36f451093` (2026-04-01, published as v0.1.3),
  which was `main@HEAD` when it was taken.
- Renamed the package `yson-rs` → `ytsaurus-yson`; the module layout is unchanged,
  so `yson_rs::x` becomes `ytsaurus_yson::x`.
- **Licence: Apache-2.0.** Upstream offers yson-rs under *either* MIT *or*
  Apache-2.0, at the recipient's option; this project exercises that option and
  takes the code under Apache-2.0, which is the licence the whole repository is
  distributed under. Both upstream licence texts are kept verbatim
  (`LICENSE-APACHE`, `LICENSE-MIT`) as notices received with the code, and
  [`NOTICE`](NOTICE) states the attribution and the derivation.
- Moved shared dependency versions to the workspace, and removed the crate-local
  `[profile.release]` (profiles are only honoured in the workspace root, and
  `panic = "abort"` must not apply to a library — it now lives in the workspace
  `release-worker` profile).
- Renamed the fuzz crate `yson-fuzz` → `ytsaurus-yson-fuzz` and fixed its path
  dependency, which pointed at a package name (`yson`) that never existed and so
  could not have built.

### Fixed

- **A stray `/` in text input hung the parser forever.** In `skip_ignored`, a
  `/` followed by any byte other than `/` or `*` matched the "this might be a
  comment" branch but then hit `continue` without either branch having advanced
  the cursor — an unconditional infinite loop. Input as short as `/a` never
  returned. Because it consumes no memory it is not caught by a timeout on
  allocation: a job fed such input would spin until the operation was killed.
  A `/` that opens no comment now ends the skip so the tokenizer can reject the
  byte, and an unterminated `/*` consumes to end-of-input instead of leaving a
  stray byte behind. Found by `tests/fuzz_smoke_tests.rs`; regression tests are
  `stray_slash_in_text_input_errors_instead_of_hanging` (thread-guarded, so a
  regression fails rather than hangs CI) and `text_comments_are_still_skipped`.

  Binary mode never calls `skip_ignored`, so jobs using `<format=binary>yson`
  were not exposed; text-mode callers were.

- **Non-UTF-8 map keys were rejected.** `YsonValue`'s visitor read keys through
  `String`, so a map with a non-UTF-8 key failed with `invalid value: byte array,
  expected a string` — even though `YsonNode::Map` stores keys as `Vec<u8>` and
  the lexer decodes them correctly. Keys now go through an internal `MapKey`
  type that accepts both the string and the bytes visitor calls.
  Regression test: `non_utf8_map_keys_survive_binary_round_trip`.

- **Non-UTF-8 attribute names were silently replaced with an empty string.**
  `FlatStructAccess` built attribute keys with
  `std::str::from_utf8(..).unwrap_or("")`, so an attribute whose name was not
  valid UTF-8 was silently renamed to `""` — losing the name and colliding with
  any other such attribute. Attribute keys are now passed to the visitor as bytes
  via an internal `ByteKeyDeserializer`. `@`-prefixed struct fields are unaffected,
  because `#[derive(Deserialize)]` generates a `visit_bytes` arm for field
  identifiers. Regression test: `non_utf8_attribute_names_survive_binary_round_trip`.

  Both of these matter because YTsaurus string columns and attribute names are
  arbitrary byte strings, not text.

### Added

- **`Serialize` for `YsonValue` and `YsonNode`.** Upstream could only decode into
  the DOM, never encode from it, which made a decode → encode round trip
  impossible and left the type unusable for pass-through jobs. Non-UTF-8 strings
  and keys are preserved: valid UTF-8 takes the `serialize_str` path so that text
  output can use unquoted identifiers, everything else takes `serialize_bytes`;
  in binary format both emit identical bytes. Attributes are emitted through the
  existing `$__yson_attributes` marker, so `<attrs>value` comes back out.

  Note that maps round-trip as *values*, not byte-for-byte: `YsonNode::Map` is a
  `BTreeMap`, so keys come back in sorted order rather than input order.

- **`scan` module** — `scan_value(input, format)` reports the byte length of the
  first complete value in a buffer, or `Scan::Incomplete` if more bytes are
  needed. This is the primitive that makes streaming possible: upstream's API
  takes a whole slice, so consuming an input larger than memory was impossible
  without it. It walks the token stream without allocating or building values,
  so finding a record boundary costs far less than parsing the record.

- `Serializer::with_buffer` / `Serializer::into_output`, so a caller writing many
  values in sequence can reuse one allocation instead of paying for a fresh
  `Vec` per value. `Serializer::new` is now defined in terms of `with_buffer`.

- `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]` on `YsonFormat`. It is a
  fieldless enum that every entry point takes by value, so without `Copy` it could
  not be reused across two calls.

- Test suites for the risks this project actually cares about:
  - `tests/ytsaurus_protocol_tests.rs` — golden bytes for every control record in
    the YTsaurus docs (`table_index`, `row_index`, `range_index`, `key_switch`) in
    both binary and text, the documented reduce input stream parsed as a list
    fragment, non-UTF-8 strings/keys/attribute names, strings larger than 64 MiB,
    10 000-column rows, malformed and deeply-nested input.
  - `tests/interop_tests.rs` — round trips against fixtures produced by the **Go**
    YSON implementation, vendored from
    [ss123she/yson-interop-tests](https://github.com/ss123she/yson-interop-tests).
  - `tests/fuzz_smoke_tests.rs` — a seeded, deterministic no-panic sweep (random
    corpus, truncation at every offset, single-bit corruption) that runs in CI,
    where `cargo fuzz` cannot.

### Known limitations

Carried over from upstream and **not** fixed here; see the crate README.
