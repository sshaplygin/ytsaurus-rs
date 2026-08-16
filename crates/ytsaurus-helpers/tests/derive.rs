//! What the derive actually produces.
//!
//! The macro's own tests check the type mapping in isolation; these check the
//! whole expansion — that it compiles, implements the trait, and renders the
//! YSON a cluster is given. `tests/cluster.rs` then checks that a cluster
//! takes it.

use std::borrow::Cow;

use ytsaurus_client::{ColumnType, SortOrder, TableRow, TableSchema};
// The macro under this crate's own name, the trait under the client's.
//
// They are separate namespaces, so both can be called `TableRow` — until
// `ytsaurus-client`'s `derive` feature is on, when the client re-exports this
// very macro as `TableRow` too and the two collide in the macro namespace. The
// feature is off in this crate's dev-dependency but on under `--all-features`,
// where cargo unifies it across the workspace. The alias is about that
// unification and nothing else.
use ytsaurus_helpers::TableRow as DeriveTableRow;
use ytsaurus_yson::{YsonFormat, YsonValue, to_string};

fn render(schema: &TableSchema) -> String {
    to_string(&schema.to_yson(), YsonFormat::Text).expect("encodes")
}

#[derive(DeriveTableRow)]
#[expect(dead_code)]
struct Visit<'a> {
    #[yt(key)]
    host: &'a str,
    size: i64,
    referrer: Option<&'a str>,
}

#[test]
fn a_struct_becomes_the_schema_its_fields_describe() {
    let schema = Visit::table_schema();
    let columns = schema.columns();

    assert_eq!(columns.len(), 3);

    assert_eq!(columns[0].name(), "host");
    assert_eq!(columns[0].column_type(), ColumnType::Utf8);
    assert!(columns[0].is_required());
    assert_eq!(columns[0].sort_order(), Some(SortOrder::Ascending));

    assert_eq!(columns[1].name(), "size");
    assert_eq!(columns[1].column_type(), ColumnType::Int64);
    assert!(columns[1].is_required());
    assert_eq!(columns[1].sort_order(), None);

    // The Rust type is the only thing that says a column is optional.
    assert_eq!(columns[2].name(), "referrer");
    assert!(!columns[2].is_required());

    assert_eq!(
        render(&schema),
        r#"<strict=%true;unique_keys=%false>[{name=host;required=%true;sort_order=ascending;type=utf8};{name=size;required=%true;type=int64};{name=referrer;required=%false;type=utf8}]"#
    );

    schema.validate().expect("a derived schema must be valid");
}

#[derive(DeriveTableRow)]
#[yt(unique_keys)]
#[expect(dead_code)]
struct Session {
    #[yt(key)]
    user_id: i64,
    #[yt(key)]
    started_at: i64,
    #[yt(name = "duration_s")]
    duration: i64,
    #[yt(skip)]
    cached: usize,
}

#[test]
fn keys_renames_and_skips_do_what_they_say() {
    let schema = Session::table_schema();
    let names: Vec<&str> = schema.columns().iter().map(|c| c.name()).collect();

    assert_eq!(names, ["user_id", "started_at", "duration_s"]);
    assert!(render(&schema).contains("unique_keys=%true"));
    assert_eq!(
        schema
            .columns()
            .iter()
            .filter(|c| c.sort_order().is_some())
            .count(),
        2,
        "both key fields are key columns"
    );

    schema.validate().expect("valid");
}

#[derive(DeriveTableRow)]
#[yt(non_strict)]
#[expect(dead_code)]
struct Loose {
    id: i64,
}

#[test]
fn non_strict_lets_a_row_carry_more_than_the_schema() {
    assert!(render(&Loose::table_schema()).contains("strict=%false"));
}

#[derive(DeriveTableRow)]
#[expect(dead_code)]
struct EveryType<'a> {
    tiny: i8,
    small: i16,
    medium: i32,
    big: i64,
    utiny: u8,
    usmall: u16,
    umedium: u32,
    ubig: u64,
    single: f32,
    dbl: f64,
    flag: bool,
    text: String,
    borrowed_text: &'a str,
    owned_bytes: Vec<u8>,
    borrowed_bytes: &'a [u8],
    cow_text: Cow<'a, str>,
    anything: YsonValue,
    missing: Option<i64>,
}

#[test]
fn every_supported_rust_type_maps_to_a_column() {
    let schema = EveryType::table_schema();
    let by_name = |name: &str| {
        schema
            .columns()
            .iter()
            .find(|c| c.name() == name)
            .unwrap_or_else(|| panic!("column {name}"))
            .clone()
    };

    assert_eq!(by_name("tiny").column_type(), ColumnType::Int8);
    assert_eq!(by_name("small").column_type(), ColumnType::Int16);
    assert_eq!(by_name("medium").column_type(), ColumnType::Int32);
    assert_eq!(by_name("big").column_type(), ColumnType::Int64);
    assert_eq!(by_name("utiny").column_type(), ColumnType::Uint8);
    assert_eq!(by_name("usmall").column_type(), ColumnType::Uint16);
    assert_eq!(by_name("umedium").column_type(), ColumnType::Uint32);
    assert_eq!(by_name("ubig").column_type(), ColumnType::Uint64);
    assert_eq!(by_name("single").column_type(), ColumnType::Float);
    assert_eq!(by_name("dbl").column_type(), ColumnType::Double);
    assert_eq!(by_name("flag").column_type(), ColumnType::Boolean);
    assert_eq!(by_name("text").column_type(), ColumnType::Utf8);
    assert_eq!(by_name("borrowed_text").column_type(), ColumnType::Utf8);
    assert_eq!(by_name("owned_bytes").column_type(), ColumnType::String);
    assert_eq!(by_name("borrowed_bytes").column_type(), ColumnType::String);
    assert_eq!(by_name("cow_text").column_type(), ColumnType::Utf8);
    assert_eq!(by_name("anything").column_type(), ColumnType::Any);

    // The cluster refuses a required `any` column, so the derive never
    // produces one.
    assert!(
        !by_name("anything").is_required(),
        "an any column cannot be required"
    );
    assert!(!by_name("missing").is_required());

    schema.validate().expect("valid");
}

#[derive(DeriveTableRow)]
#[expect(dead_code)]
struct Escaped {
    #[yt(column_type = "any")]
    payload: [u32; 4],
    #[yt(column_type = "timestamp")]
    when: u64,
}

#[test]
fn the_escape_hatch_names_a_type_the_derive_would_not_guess() {
    let schema = Escaped::table_schema();

    assert_eq!(schema.columns()[0].column_type(), ColumnType::Any);
    assert!(!schema.columns()[0].is_required());
    schema.validate().expect("valid");
}

/// A struct whose every field is skipped still has to compile: the generated
/// `TableSchema::new([])` must infer its element type from nothing.
#[derive(DeriveTableRow)]
#[expect(dead_code)]
struct AllSkipped {
    #[yt(skip)]
    scratch: usize,
    #[yt(skip)]
    also: String,
}

#[test]
fn a_schema_with_no_columns_is_still_a_schema() {
    let schema = AllSkipped::table_schema();
    assert!(schema.columns().is_empty());
    schema.validate().expect("an empty schema is valid");
}

/// Generics and where clauses have to survive the impl.
#[derive(DeriveTableRow)]
#[expect(dead_code)]
struct Generic<'a, T>
where
    T: Clone,
{
    #[yt(key)]
    id: i64,
    text: &'a str,
    #[yt(skip)]
    payload: T,
}

#[test]
fn a_generic_struct_gets_a_generic_impl() {
    let schema = <Generic<'_, String> as TableRow>::table_schema();
    assert_eq!(schema.columns().len(), 2);
    schema.validate().expect("valid");
}

/// A skipped field between two key fields must not break the key prefix: it is
/// not a column, so it cannot come between two of them.
#[derive(DeriveTableRow)]
#[expect(dead_code)]
struct SkipBetweenKeys {
    #[yt(key)]
    a: i64,
    #[yt(skip)]
    ignored: usize,
    #[yt(key)]
    b: i64,
    c: i64,
}

#[test]
fn a_skipped_field_does_not_end_the_key() {
    let schema = SkipBetweenKeys::table_schema();
    let keys: Vec<&str> = schema
        .columns()
        .iter()
        .filter(|c| c.sort_order().is_some())
        .map(|c| c.name())
        .collect();

    assert_eq!(keys, ["a", "b"]);
    schema.validate().expect("valid");
}
