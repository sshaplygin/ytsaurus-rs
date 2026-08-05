use std::collections::BTreeMap;

use ytsaurus_skiff::{Format, Schema, SchemaError, SchemaRef, WireType};
use ytsaurus_yson::{YsonFormat, from_slice, to_string};

fn row_schema() -> Schema {
    Schema::tuple([
        Schema::named("found", WireType::Uint64),
        Schema::named("rcl", WireType::String32),
    ])
}

#[test]
fn renders_the_format_shape_ytsaurus_and_go_expect() {
    let format = Format::new(vec![SchemaRef::Inline(row_schema())]).unwrap();
    let rendered = to_string(&format.to_yson(), YsonFormat::Text).unwrap();

    assert_eq!(
        rendered,
        "<table_skiff_schemas=[{children=[{name=found;wire_type=uint64};{name=rcl;wire_type=string32}];wire_type=tuple}]>skiff"
    );
}

#[test]
fn round_trips_an_inline_schema_format() {
    let input = b"<table_skiff_schemas=[{wire_type=tuple;children=[{name=found;wire_type=uint64};{name=rcl;wire_type=string32}]}]>skiff";
    let value = from_slice(input, YsonFormat::Text).unwrap();

    let format = Format::from_yson(&value).unwrap();
    assert_eq!(format.table_schema(0).unwrap(), &row_schema());
}

#[test]
fn resolves_a_schema_registry_reference() {
    let registry = BTreeMap::from([("row".to_owned(), row_schema())]);
    let format = Format::from_parts(vec![SchemaRef::Registry("row".to_owned())], registry).unwrap();

    let parsed = Format::from_yson(&format.to_yson()).unwrap();
    assert_eq!(parsed.table_schema(0).unwrap(), &row_schema());
}

#[test]
fn rejects_an_unknown_registry_reference() {
    let err = Format::new(vec![SchemaRef::Registry("missing".to_owned())]).unwrap_err();
    assert_eq!(
        err,
        SchemaError::UnknownRegistryReference("missing".to_owned())
    );
}

#[test]
fn optional_column_is_the_go_sdk_variant8_shape() {
    let schema = Schema::named("found", WireType::Uint64).optional();
    assert_eq!(schema.wire_type, WireType::Variant8);
    assert_eq!(schema.name.as_deref(), Some("found"));
    assert_eq!(schema.children[0].wire_type, WireType::Nothing);
    assert_eq!(schema.children[1].wire_type, WireType::Uint64);
    schema.validate().unwrap();
}

#[test]
fn rejects_children_on_a_leaf() {
    let schema = Schema {
        wire_type: WireType::Uint64,
        name: None,
        children: vec![Schema::leaf(WireType::Nothing)],
    };

    assert_eq!(
        schema.validate(),
        Err(SchemaError::UnexpectedChildren {
            wire_type: WireType::Uint64,
            count: 1,
        })
    );
}

#[test]
fn table_schemas_are_named_tuples() {
    assert!(matches!(
        Format::new(vec![SchemaRef::Inline(Schema::leaf(WireType::Uint64))]),
        Err(SchemaError::TableSchemaRootMustBeTuple {
            found: WireType::Uint64,
        })
    ));

    assert!(matches!(
        Format::new(vec![SchemaRef::Inline(Schema::tuple([Schema::leaf(
            WireType::Uint64
        )]))]),
        Err(SchemaError::TableSchemaChildMissingName { index: 0 })
    ));
}

#[test]
fn supports_every_wire_type_named_by_go_sdk_v0_0_33() {
    let names = [
        (WireType::Nothing, "nothing"),
        (WireType::Boolean, "boolean"),
        (WireType::Int8, "int8"),
        (WireType::Int16, "int16"),
        (WireType::Int32, "int32"),
        (WireType::Int64, "int64"),
        (WireType::Int128, "int128"),
        (WireType::Int256, "int256"),
        (WireType::Uint8, "uint8"),
        (WireType::Uint16, "uint16"),
        (WireType::Uint32, "uint32"),
        (WireType::Uint64, "uint64"),
        (WireType::Double, "double"),
        (WireType::String32, "string32"),
        (WireType::Yson32, "yson32"),
        (WireType::Variant8, "variant8"),
        (WireType::Variant16, "variant16"),
        (WireType::RepeatedVariant8, "repeated_variant8"),
        (WireType::RepeatedVariant16, "repeated_variant16"),
        (WireType::Tuple, "tuple"),
    ];

    for (wire_type, expected) in names {
        assert_eq!(wire_type.as_str(), expected);
        assert_eq!(WireType::parse(expected), Some(wire_type));
    }
}
