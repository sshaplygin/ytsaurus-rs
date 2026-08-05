use std::io::Cursor;

use ytsaurus_job::{JobError, SkiffJobReader};
use ytsaurus_skiff::{Encoder, Format, Schema, SchemaRef, Value, WireType};

fn optional_index(name: &str) -> Schema {
    Schema {
        wire_type: WireType::Variant8,
        name: Some(name.to_owned()),
        children: vec![
            Schema::leaf(WireType::Nothing),
            Schema::leaf(WireType::Int64),
        ],
    }
}

fn schema() -> Schema {
    Schema::tuple([
        Schema::named("$key_switch", WireType::Boolean),
        optional_index("$row_index"),
        optional_index("$range_index"),
        Schema::named("count", WireType::Uint64),
    ])
}

fn format(schema: Schema) -> Format {
    Format::new(vec![SchemaRef::Inline(schema)]).unwrap()
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

#[test]
fn extracts_skiff_system_columns_from_each_row() {
    let input = hex_fixture(include_str!(
        "../../../tests/skiff-go-interop/control_rows.hex"
    ));

    let mut reader = SkiffJobReader::new(Cursor::new(input), format(schema())).unwrap();
    let first = reader.next_row().unwrap().unwrap();
    assert_eq!(first.table_index, 0);
    assert_eq!(first.row_index, 2);
    assert_eq!(first.range_index, 0);
    assert!(!first.key_switch);
    assert_eq!(first.value(), &Value::Tuple(vec![Value::Uint64(7)]));

    let second = reader.next_row().unwrap().unwrap();
    assert_eq!(second.row_index, 3);
    assert_eq!(second.range_index, 3);
    assert!(second.key_switch);
    assert_eq!(second.into_value(), Value::Tuple(vec![Value::Uint64(11)]));
    assert_eq!(reader.next_row().unwrap(), None);
}

#[test]
fn rejects_a_system_column_after_data() {
    let schema = Schema::tuple([
        Schema::named("count", WireType::Uint64),
        Schema::named("$key_switch", WireType::Boolean),
    ]);

    assert!(matches!(
        SkiffJobReader::new(Cursor::new(Vec::new()), format(schema)),
        Err(JobError::BadSkiffSchema { table: 0, .. })
    ));
}

#[test]
fn tracks_the_go_sdk_row_index_rules_per_input_table() {
    let plain = Schema::tuple([Schema::named("count", WireType::Uint64)]);
    let keyed = Schema::tuple([
        Schema::named("$key_switch", WireType::Boolean),
        Schema::named("count", WireType::Uint64),
    ]);
    let input_format = Format::new(vec![
        SchemaRef::Inline(plain.clone()),
        SchemaRef::Inline(keyed.clone()),
    ])
    .unwrap();

    let mut first = Encoder::new(Vec::new(), plain.clone()).unwrap();
    first.write(&Value::Tuple(vec![Value::Uint64(1)])).unwrap();
    first.write(&Value::Tuple(vec![Value::Uint64(2)])).unwrap();
    let mut second = Encoder::new(Vec::new(), keyed).unwrap();
    second
        .write(&Value::Tuple(vec![Value::Boolean(true), Value::Uint64(3)]))
        .unwrap();

    let mut input = first.into_inner().unwrap();
    let mut keyed_bytes = second.into_inner().unwrap();
    // `Encoder` always tags its one table as zero. Change this row's framing
    // tag to table one, like YTsaurus does on a multiplexed input stream.
    keyed_bytes[..2].copy_from_slice(&1_u16.to_le_bytes());
    input.extend_from_slice(&keyed_bytes);

    let mut reader = SkiffJobReader::new(Cursor::new(input), input_format).unwrap();
    let first = reader.next_row().unwrap().unwrap();
    let second = reader.next_row().unwrap().unwrap();
    let third = reader.next_row().unwrap().unwrap();

    assert_eq!((first.table_index, first.row_index), (0, 1));
    assert_eq!((second.table_index, second.row_index), (0, 2));
    assert_eq!((third.table_index, third.row_index), (1, 0));
    assert!(third.key_switch);
}
