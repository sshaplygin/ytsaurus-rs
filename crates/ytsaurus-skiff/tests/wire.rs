use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
};

use ytsaurus_skiff::{
    CodecError, Decoder, Encoder, Format, Schema, SchemaRef, Value, Variant, WireType,
};

fn go_reference_schema() -> Schema {
    Schema::tuple([
        Schema::named("found", WireType::Uint64),
        Schema::named("rcl", WireType::String32),
    ])
}

fn go_scalar_schema() -> Schema {
    Schema::tuple([
        Schema::named("bool", WireType::Boolean),
        Schema::named("i8", WireType::Int8),
        Schema::named("i16", WireType::Int16),
        Schema::named("i32", WireType::Int32),
        Schema::named("i64", WireType::Int64),
        Schema::named("u8", WireType::Uint8),
        Schema::named("u16", WireType::Uint16),
        Schema::named("u32", WireType::Uint32),
        Schema::named("u64", WireType::Uint64),
        Schema::named("f64", WireType::Double),
        Schema::named("bytes", WireType::String32),
    ])
}

fn go_scalar_row() -> Value {
    Value::Tuple(vec![
        Value::Boolean(true),
        Value::Int8(-8),
        Value::Int16(-16),
        Value::Int32(-32),
        Value::Int64(-64),
        Value::Uint8(8),
        Value::Uint16(16),
        Value::Uint32(32),
        Value::Uint64(64),
        Value::Double(-1.5),
        Value::Bytes(vec![0xff, b'a']),
    ])
}

fn optional_string(name: &str) -> Schema {
    Schema {
        wire_type: WireType::Variant8,
        name: Some(name.to_owned()),
        children: vec![
            Schema::leaf(WireType::Nothing),
            Schema::leaf(WireType::String32),
        ],
    }
}

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

fn format(schema: Schema) -> Format {
    Format::new(vec![SchemaRef::Inline(schema)]).unwrap()
}

#[test]
fn matches_the_pinned_go_sdk_encoder_vector() {
    let row = Value::Tuple(vec![Value::Uint64(7), Value::Bytes(b"abc".to_vec())]);
    let mut encoder = Encoder::new(Vec::new(), go_reference_schema()).unwrap();
    encoder.write(&row).unwrap();
    let bytes = encoder.into_inner().unwrap();

    let expected = vec![
        0x00, 0x00, // Variant16: table 0
        0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, b'a', b'b', b'c',
    ];
    assert_eq!(bytes, expected);

    let mut decoder = Decoder::new(Cursor::new(bytes), format(go_reference_schema()));
    assert_eq!(decoder.next_row().unwrap(), Some((0, row)));
    assert_eq!(decoder.next_row().unwrap(), None);
}

#[test]
fn matches_the_shared_go_scalar_corpus_in_both_directions() {
    let expected = hex_fixture(include_str!(
        "../../../tests/skiff-go-interop/scalar_row.hex"
    ));
    let row = go_scalar_row();

    let mut encoder = Encoder::new(Vec::new(), go_scalar_schema()).unwrap();
    encoder.write(&row).unwrap();
    assert_eq!(encoder.into_inner().unwrap(), expected);

    let mut decoder = Decoder::new(Cursor::new(expected), format(go_scalar_schema()));
    assert_eq!(decoder.next_row().unwrap(), Some((0, row)));
    assert_eq!(decoder.next_row().unwrap(), None);
}

#[test]
fn matches_the_shared_go_optional_field_corpus_in_both_directions() {
    let schema = Schema::tuple([optional_string("absent"), optional_string("present")]);
    let row = Value::Tuple(vec![
        Value::Variant {
            tag: 0,
            value: Box::new(Value::Nothing),
        },
        Value::Variant {
            tag: 1,
            value: Box::new(Value::Bytes(vec![0xff, b'a'])),
        },
    ]);
    let expected = hex_fixture(include_str!(
        "../../../tests/skiff-go-interop/optional_row.hex"
    ));

    let mut encoder = Encoder::new(Vec::new(), schema.clone()).unwrap();
    encoder.write(&row).unwrap();
    assert_eq!(encoder.into_inner().unwrap(), expected);

    let mut decoder = Decoder::new(Cursor::new(expected), format(schema));
    assert_eq!(decoder.next_row().unwrap(), Some((0, row)));
    assert_eq!(decoder.next_row().unwrap(), None);
}

#[test]
fn resolves_the_shared_go_scalar_corpus_through_a_schema_registry() {
    let mut registry = BTreeMap::new();
    registry.insert("scalar".to_owned(), go_scalar_schema());
    let format = Format::from_parts(vec![SchemaRef::Registry("scalar".to_owned())], registry)
        .expect("the registry reference resolves");
    let expected = hex_fixture(include_str!(
        "../../../tests/skiff-go-interop/scalar_row.hex"
    ));

    let mut decoder = Decoder::new(Cursor::new(expected), format);
    assert_eq!(decoder.next_row().unwrap(), Some((0, go_scalar_row())));
    assert_eq!(decoder.next_row().unwrap(), None);
}

#[test]
fn round_trips_every_wire_shape_through_one_byte_reads() {
    let schema = Schema::tuple([
        Schema::named("boolean", WireType::Boolean),
        Schema::named("int8", WireType::Int8),
        Schema::named("int16", WireType::Int16),
        Schema::named("int32", WireType::Int32),
        Schema::named("int64", WireType::Int64),
        Schema::named("int128", WireType::Int128),
        Schema::named("int256", WireType::Int256),
        Schema::named("uint8", WireType::Uint8),
        Schema::named("uint16", WireType::Uint16),
        Schema::named("uint32", WireType::Uint32),
        Schema::named("uint64", WireType::Uint64),
        Schema::named("double", WireType::Double),
        Schema::named("bytes", WireType::String32),
        Schema::named("any", WireType::Yson32),
        Schema {
            wire_type: WireType::Variant8,
            name: Some("optional".to_owned()),
            children: vec![
                Schema::leaf(WireType::Nothing),
                Schema::leaf(WireType::String32),
            ],
        },
        Schema {
            wire_type: WireType::Variant16,
            name: Some("choice".to_owned()),
            children: vec![
                Schema::leaf(WireType::Nothing),
                Schema::leaf(WireType::Uint64),
            ],
        },
        Schema {
            wire_type: WireType::RepeatedVariant8,
            name: Some("items8".to_owned()),
            children: vec![Schema::leaf(WireType::Int64)],
        },
        Schema {
            wire_type: WireType::RepeatedVariant16,
            name: Some("items16".to_owned()),
            children: vec![Schema::leaf(WireType::String32)],
        },
    ]);
    let row = Value::Tuple(vec![
        Value::Boolean(true),
        Value::Int8(-8),
        Value::Int16(-16),
        Value::Int32(-32),
        Value::Int64(-64),
        Value::Int128(-128),
        Value::Int256([0xA5; 32]),
        Value::Uint8(8),
        Value::Uint16(16),
        Value::Uint32(32),
        Value::Uint64(64),
        Value::Double(-1.5),
        Value::Bytes(vec![0, 0xFF]),
        Value::Yson(vec![b'#']),
        Value::Variant {
            tag: 1,
            value: Box::new(Value::Bytes(b"present".to_vec())),
        },
        Value::Variant {
            tag: 1,
            value: Box::new(Value::Uint64(42)),
        },
        Value::RepeatedVariants(vec![
            Variant {
                tag: 0,
                value: Value::Int64(1),
            },
            Variant {
                tag: 0,
                value: Value::Int64(2),
            },
        ]),
        Value::RepeatedVariants(vec![Variant {
            tag: 0,
            value: Value::Bytes(b"wide tag".to_vec()),
        }]),
    ]);

    let mut encoder = Encoder::new(Vec::new(), schema.clone()).unwrap();
    encoder.write(&row).unwrap();
    let bytes = encoder.into_inner().unwrap();
    let mut decoder = Decoder::new(OneByteReader::new(bytes), format(schema));

    assert_eq!(decoder.next_row().unwrap(), Some((0, row)));
    assert_eq!(decoder.next_row().unwrap(), None);
}

#[test]
fn truncated_go_reference_vectors_never_succeed() {
    let row = Value::Tuple(vec![Value::Uint64(7), Value::Bytes(b"abc".to_vec())]);
    let mut encoder = Encoder::new(Vec::new(), go_reference_schema()).unwrap();
    encoder.write(&row).unwrap();
    let complete = encoder.into_inner().unwrap();

    for cut in 1..complete.len() {
        let mut decoder = Decoder::new(
            Cursor::new(complete[..cut].to_vec()),
            format(go_reference_schema()),
        );
        assert!(
            matches!(decoder.next_row(), Err(CodecError::Truncated { .. })),
            "cut at {cut} must report truncation"
        );
    }
}

#[test]
fn rejects_a_blob_before_allocating_it() {
    let bytes = vec![0, 0, 5, 0, 0, 0];
    let schema = Schema::tuple([Schema::named("data", WireType::String32)]);
    let mut decoder = Decoder::new(Cursor::new(bytes), format(schema)).with_max_blob_bytes(4);

    assert!(matches!(
        decoder.next_row(),
        Err(CodecError::BlobTooLarge {
            wire_type: WireType::String32,
            length: 5,
            limit: 4,
        })
    ));
}

#[test]
fn rejects_an_unknown_variant_tag() {
    let schema = Schema::tuple([Schema {
        wire_type: WireType::Variant8,
        name: Some("choice".to_owned()),
        children: vec![Schema::leaf(WireType::Nothing)],
    }]);
    let mut decoder = Decoder::new(Cursor::new(vec![0, 0, 1]), format(schema));

    assert!(matches!(
        decoder.next_row(),
        Err(CodecError::InvalidVariantTag {
            wire_type: WireType::Variant8,
            tag: 1,
            children: 1,
        })
    ));
}

#[test]
fn malformed_stream_fuzz_smoke_never_panics_or_exceeds_the_blob_limit() {
    let schema = Schema::tuple([
        Schema::named("flag", WireType::Boolean),
        Schema::named("blob", WireType::String32),
        Schema {
            wire_type: WireType::Variant8,
            name: Some("choice".to_owned()),
            children: vec![
                Schema::leaf(WireType::Nothing),
                Schema::leaf(WireType::Uint64),
            ],
        },
        Schema {
            wire_type: WireType::RepeatedVariant8,
            name: Some("items".to_owned()),
            children: vec![Schema::tuple([
                Schema::leaf(WireType::Int16),
                Schema::leaf(WireType::String32),
            ])],
        },
        Schema {
            wire_type: WireType::Tuple,
            name: Some("nested".to_owned()),
            children: vec![
                Schema::leaf(WireType::Yson32),
                Schema::leaf(WireType::Double),
            ],
        },
    ]);
    let format = format(schema);
    let mut state = 0x7b69_5d3c_1f20_4a8e_u64;

    for _sample in 0..10_000 {
        let length = usize::try_from(next_random(&mut state) % 96).unwrap();
        let mut bytes = Vec::with_capacity(length);
        for _ in 0..length {
            bytes.push(next_random(&mut state) as u8);
        }

        let mut decoder = Decoder::new(Cursor::new(bytes), format.clone()).with_max_blob_bytes(64);
        for _ in 0..128 {
            match decoder.next_row() {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

#[derive(Debug)]
struct OneByteReader {
    input: Cursor<Vec<u8>>,
}

impl OneByteReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            input: Cursor::new(bytes),
        }
    }
}

impl Read for OneByteReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let length = output.len().min(1);
        self.input.read(&mut output[..length])
    }
}
