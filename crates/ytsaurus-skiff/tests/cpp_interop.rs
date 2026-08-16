//! Differential tests against the YTsaurus C++ Skiff implementation.
//!
//! The vectors under `tests/skiff-cpp-interop/` were produced by
//! `library/cpp/skiff` through its Python bindings and read back by the same
//! C++ parser. Each test asserts both directions: that this crate's encoder
//! produces those exact bytes, and that its decoder reads them back to the
//! values the C++ writer was given.
//!
//! What the bindings can express is narrower than what the C++ library
//! implements — no `int8`/`int16`/`int32`, none of the 128- or 256-bit types.
//! `tests/skiff-cpp-interop/README.md` records exactly where the edge is and
//! why it is a property of the bindings rather than of the format.

use std::collections::BTreeMap;
use std::io::Cursor;

use ytsaurus_skiff::{Decoder, Encoder, Format, Schema, SchemaRef, Value, Variant, WireType};

fn hex_fixture(input: &str) -> Vec<u8> {
    let digits: String = input
        .lines()
        .filter_map(|line| line.split('#').next())
        .flat_map(str::chars)
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    assert_eq!(
        digits.len() % 2,
        0,
        "fixture has an odd number of hex digits"
    );
    (0..digits.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&digits[index..index + 2], 16).unwrap())
        .collect()
}

fn format(schemas: Vec<Schema>) -> Format {
    Format::new(schemas.into_iter().map(SchemaRef::Inline).collect()).unwrap()
}

/// Encodes every row and compares the stream with the C++ one, then decodes
/// the C++ stream and compares the values.
///
/// Both directions matter and neither implies the other: a shared bug in the
/// encoder and decoder would round trip happily, and a decoder that ignores a
/// field would still reproduce a byte-exact stream it never truly read.
fn assert_matches_cpp(expected: &[u8], schemas: Vec<Schema>, rows: &[(usize, Value)]) {
    let mut encoded = Vec::new();
    for (table_index, row) in rows {
        assert_eq!(
            *table_index, 0,
            "the encoder writes one table per stream; multi-table vectors decode only"
        );
        let mut encoder = Encoder::new(&mut encoded, schemas[0].clone()).unwrap();
        encoder.write(row).unwrap();
        encoder.flush().unwrap();
    }
    assert_eq!(encoded, expected, "Rust encoder disagrees with C++ bytes");

    assert_decodes_cpp(expected, schemas, rows);
}

fn assert_decodes_cpp(expected: &[u8], schemas: Vec<Schema>, rows: &[(usize, Value)]) {
    let mut decoder = Decoder::new(Cursor::new(expected.to_vec()), format(schemas));
    for (table_index, row) in rows {
        assert_eq!(
            decoder.next_row().unwrap(),
            Some((*table_index, row.clone())),
            "Rust decoder disagrees with the values C++ was given"
        );
    }
    assert_eq!(decoder.next_row().unwrap(), None, "stream is not exhausted");
}

fn optional(name: &str, wire_type: WireType) -> Schema {
    Schema::named(name, wire_type).optional()
}

fn absent() -> Value {
    Value::Variant {
        tag: 0,
        value: Box::new(Value::Nothing),
    }
}

fn present(value: Value) -> Value {
    Value::Variant {
        tag: 1,
        value: Box::new(value),
    }
}

fn scalar_schema() -> Schema {
    Schema::tuple([
        Schema::named("i", WireType::Int64),
        Schema::named("u", WireType::Uint64),
        Schema::named("b", WireType::Boolean),
        Schema::named("d", WireType::Double),
        Schema::named("s", WireType::String32),
    ])
}

fn scalar_rows() -> [(usize, Value); 2] {
    [
        (
            0,
            Value::Tuple(vec![
                Value::Int64(i64::MIN),
                Value::Uint64(0),
                Value::Boolean(false),
                Value::Double(-0.0),
                Value::Bytes(vec![0xff, b'a']),
            ]),
        ),
        (
            0,
            Value::Tuple(vec![
                Value::Int64(i64::MAX),
                Value::Uint64(u64::MAX - 1),
                Value::Boolean(true),
                Value::Double(1.5),
                Value::Bytes(Vec::new()),
            ]),
        ),
    ]
}

#[test]
fn matches_the_cpp_scalar_corpus_in_both_directions() {
    assert_matches_cpp(
        &hex_fixture(include_str!("../../../tests/skiff-cpp-interop/scalars.hex")),
        vec![scalar_schema()],
        &scalar_rows(),
    );
}

#[test]
fn matches_the_cpp_optional_corpus_in_both_directions() {
    let schema = Schema::tuple([
        optional("oi", WireType::Int64),
        optional("os", WireType::String32),
    ]);
    let rows = [
        (0, Value::Tuple(vec![absent(), absent()])),
        (
            0,
            Value::Tuple(vec![
                present(Value::Int64(-5)),
                present(Value::Bytes(vec![0xff, b'a'])),
            ]),
        ),
    ];

    assert_matches_cpp(
        &hex_fixture(include_str!(
            "../../../tests/skiff-cpp-interop/optional.hex"
        )),
        vec![schema],
        &rows,
    );
}

#[test]
fn matches_the_cpp_sparse_column_corpus_in_both_directions() {
    let schema = Schema::tuple([
        Schema::named("dense", WireType::Int64),
        Schema {
            wire_type: WireType::RepeatedVariant16,
            name: Some("$sparse_columns".to_owned()),
            children: vec![
                Schema::named("sp1", WireType::Int64),
                Schema::named("sp2", WireType::String32),
            ],
        },
    ]);
    let rows = [
        (
            0,
            Value::Tuple(vec![Value::Int64(1), Value::RepeatedVariants(Vec::new())]),
        ),
        (
            0,
            Value::Tuple(vec![
                Value::Int64(2),
                Value::RepeatedVariants(vec![Variant {
                    tag: 1,
                    value: Value::Bytes(b"z".to_vec()),
                }]),
            ]),
        ),
        (
            0,
            Value::Tuple(vec![
                Value::Int64(3),
                Value::RepeatedVariants(vec![
                    Variant {
                        tag: 0,
                        value: Value::Int64(42),
                    },
                    Variant {
                        tag: 1,
                        value: Value::Bytes(b"q".to_vec()),
                    },
                ]),
            ]),
        ),
    ];

    assert_matches_cpp(
        &hex_fixture(include_str!("../../../tests/skiff-cpp-interop/sparse.hex")),
        vec![schema],
        &rows,
    );
}

#[test]
fn matches_the_cpp_other_columns_corpus_in_both_directions() {
    let schema = Schema::tuple([
        Schema::named("dense", WireType::Int64),
        Schema::named("$other_columns", WireType::Yson32),
    ]);
    // The binary YSON map `{extra="hello"; num=3}` exactly as the C++ writer
    // spells it. This crate carries the payload without interpreting it, so
    // the assertion is that the framing and the bytes both survive untouched.
    let other_columns = b"\x7b\x01\x0aextra=\x01\x0ahello;\x01\x06num=\x02\x06;\x7d";
    let rows = [(
        0,
        Value::Tuple(vec![Value::Int64(7), Value::Yson(other_columns.to_vec())]),
    )];

    assert_matches_cpp(
        &hex_fixture(include_str!(
            "../../../tests/skiff-cpp-interop/other_columns.hex"
        )),
        vec![schema],
        &rows,
    );
}

#[test]
fn matches_the_cpp_system_column_corpus_in_both_directions() {
    let schema = Schema::tuple([
        optional("$row_index", WireType::Int64),
        optional("$range_index", WireType::Int64),
        Schema::named("$key_switch", WireType::Boolean),
        Schema::named("a", WireType::Int64),
    ]);
    let rows = [
        (
            0,
            Value::Tuple(vec![
                absent(),
                absent(),
                Value::Boolean(false),
                Value::Int64(7),
            ]),
        ),
        (
            0,
            Value::Tuple(vec![
                present(Value::Int64(5)),
                present(Value::Int64(2)),
                Value::Boolean(true),
                Value::Int64(9),
            ]),
        ),
    ];

    assert_matches_cpp(
        &hex_fixture(include_str!(
            "../../../tests/skiff-cpp-interop/system_columns.hex"
        )),
        vec![schema],
        &rows,
    );
}

/// The composite shapes YT's `list`, `struct` and `optional` map onto.
///
/// This is the one vector nothing else in the repository can produce. The
/// Go SDK's codec has no repeated-variant path in either direction, and the
/// bindings' record layer cannot build one either — only the typed-dataclass
/// door emits `repeated_variant8`, with tag 0 per item and the `0xFF`
/// terminator. Phase 3 of the full-support plan builds on exactly this shape.
#[test]
fn matches_the_cpp_composite_corpus_in_both_directions() {
    let schema = Schema::tuple([
        Schema::named("id", WireType::Int64),
        Schema {
            wire_type: WireType::RepeatedVariant8,
            name: Some("tags".to_owned()),
            children: vec![Schema::leaf(WireType::String32)],
        },
        Schema {
            wire_type: WireType::Tuple,
            name: Some("nested".to_owned()),
            children: vec![
                Schema::named("x", WireType::Int64),
                Schema::named("y", WireType::String32),
            ],
        },
        optional("maybe", WireType::Int64),
    ]);

    let item = |text: &str| Variant {
        tag: 0,
        value: Value::Bytes(text.as_bytes().to_vec()),
    };
    let rows = [
        (
            0,
            Value::Tuple(vec![
                Value::Int64(1),
                Value::RepeatedVariants(Vec::new()),
                Value::Tuple(vec![Value::Int64(7), Value::Bytes(b"z".to_vec())]),
                absent(),
            ]),
        ),
        (
            0,
            Value::Tuple(vec![
                Value::Int64(2),
                Value::RepeatedVariants(vec![item("ab"), item("")]),
                Value::Tuple(vec![Value::Int64(-1), Value::Bytes(Vec::new())]),
                present(Value::Int64(-5)),
            ]),
        ),
    ];

    assert_matches_cpp(
        &hex_fixture(include_str!(
            "../../../tests/skiff-cpp-interop/composite.hex"
        )),
        vec![schema],
        &rows,
    );
}

#[test]
fn decodes_the_cpp_multiplexed_table_corpus() {
    // Decode only: the encoder writes one table per stream, exactly as the C++
    // writer does. Multiplexing is the reader's side of the protocol.
    let schemas = vec![
        Schema::tuple([Schema::named("a", WireType::Int64)]),
        Schema::tuple([Schema::named("b", WireType::String32)]),
    ];
    let rows = [
        (0, Value::Tuple(vec![Value::Int64(11)])),
        (1, Value::Tuple(vec![Value::Bytes(b"xy".to_vec())])),
        (0, Value::Tuple(vec![Value::Int64(12)])),
    ];

    assert_decodes_cpp(
        &hex_fixture(include_str!(
            "../../../tests/skiff-cpp-interop/multi_table.hex"
        )),
        schemas,
        &rows,
    );
}

#[test]
fn resolves_the_cpp_scalar_corpus_through_a_schema_registry() {
    // The C++ schema parser resolves `$name` against `skiff_schema_registry`
    // before any byte is read, so a registry reference must decode the same
    // stream as the inline schema it names.
    let mut registry = BTreeMap::new();
    registry.insert("scalars".to_owned(), scalar_schema());
    let format = Format::from_parts(vec![SchemaRef::Registry("scalars".to_owned())], registry)
        .expect("the registry reference resolves");

    let expected = hex_fixture(include_str!("../../../tests/skiff-cpp-interop/scalars.hex"));
    let mut decoder = Decoder::new(Cursor::new(expected), format);
    for (table_index, row) in scalar_rows() {
        assert_eq!(decoder.next_row().unwrap(), Some((table_index, row)));
    }
}
