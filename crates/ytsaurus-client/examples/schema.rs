//! `schema` — a derived schema, on a real cluster.
//!
//! The point of inferring a schema from a Rust struct is that the cluster then
//! enforces it, so the only test that means anything is whether the cluster
//! accepts what the derive produced — for every column type, not just the easy
//! ones — and whether it then rejects a row that breaks the promise.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example schema
//! ```

use std::process::ExitCode;

use ytsaurus_client::{
    Client, ClientError, Column, ColumnType, SortOrder, TableRow, TableSchema, yson_build,
};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, to_string, to_vec};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_schema";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nschema failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    step("Preparing Cypress");
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    done(BASE);

    step("Creating a table from the struct its rows have");
    // No schema written out by hand: this is `Visit`'s own fields.
    let visits = Visit::table_schema();
    println!("   {}", render(&visits.to_yson()));
    client.create_table(&format!("{BASE}/visits"), &visits)?;

    let stored = client.table_schema(&format!("{BASE}/visits"))?;
    check(
        "the cluster kept the columns as given",
        column_names(&stored) == ["host", "size", "referrer"],
    )?;
    check(
        "and marked the table sorted",
        matches!(
            client.get(&format!("{BASE}/visits/@sorted"))?.node,
            YsonNode::Boolean(true)
        ),
    )?;
    println!("   {}", render(&stored));

    step("Every column type the crate can name");
    let every = TableSchema::new(ALL_TYPES.iter().map(|(name, ty)| {
        let column = Column::new(*name, *ty);
        // `any`, `null` and `void` already mean "there may be nothing here";
        // the cluster refuses to also call them required.
        if ty.can_be_required() {
            column.required()
        } else {
            column
        }
    }));
    client.create_table(&format!("{BASE}/every_type"), &every)?;
    done(&format!("{} column types accepted", ALL_TYPES.len()));

    step("The schema is a promise the cluster keeps");
    // A row missing a required column must be refused. If this were accepted,
    // the schema would be decoration.
    let incomplete = yson_build::map([("host", yson_build::string("example.com"))]);
    let mut rows = to_vec(&incomplete, YsonFormat::Binary).map_err(|e| ClientError::Decode {
        command: "write_table".to_owned(),
        reason: e.to_string(),
    })?;
    rows.push(b';');

    match client.write_table(format!("{BASE}/visits"), &rows) {
        Ok(()) => {
            eprintln!("   FAIL the cluster accepted a row with no `size`");
            return Err(ClientError::Config(
                "a required column was not enforced".to_owned(),
            ));
        }
        Err(e) => {
            let message = e.to_string();
            check(
                "a row missing a required column is refused",
                message.contains("size") || message.to_lowercase().contains("required"),
            )?;
            println!("   {}", first_line(&message));
        }
    }

    step("Evolving the schema of a table that already has rows");
    // Its own table rather than the one above: the refused write left an upload
    // transaction holding an exclusive lock for a moment, and this example
    // should demonstrate schema evolution rather than wait out a lock.
    let visits_path = format!("{BASE}/evolving");
    client.create_table(&visits_path, &Visit::table_schema())?;
    client.write_table(&visits_path, &two_visits()?)?;
    done(&format!("{} rows written", client.row_count(&visits_path)?));

    // The struct gained a field, so the table does. Nothing is written out by
    // hand here either: this is `VisitV2`'s own fields.
    client.alter_table(&visits_path, &VisitV2::table_schema())?;
    check(
        "an optional column can be added to a table with rows in it",
        column_names(&client.table_schema(&visits_path)?) == ["host", "size", "referrer", "note"],
    )?;

    // Everything else asks more of the rows already written than they promised.
    for (what, schema) in incompatible_changes() {
        match client.alter_table(&visits_path, &schema) {
            Ok(()) => {
                eprintln!("   FAIL {what} was accepted");
                return Err(ClientError::Config(format!("{what} was accepted")));
            }
            Err(e) => println!("   ok {what}: {}", first_line(&e.to_string())),
        }
    }

    // The same change, on a table with nothing in it, is allowed. Which is why
    // trying a migration out on an empty table proves nothing.
    let empty = format!("{BASE}/empty_visits");
    client.create_table(&empty, &Visit::table_schema())?;
    client.alter_table(&empty, &incompatible_changes().swap_remove(0).1)?;
    check(
        "and an empty table accepts what a full one refuses",
        column_names(&client.table_schema(&empty)?) == ["host"],
    )?;

    step("What the client refuses before asking");
    // The same rules, caught locally: one sentence naming the column instead of
    // a round trip and a nested error document.
    for (what, schema) in local_refusals() {
        match schema.validate() {
            Ok(()) => {
                eprintln!("   FAIL {what} was not caught");
                return Err(ClientError::Config(format!("{what} was not caught")));
            }
            Err(reason) => println!("   ok {what}: {reason}"),
        }
    }

    step("And the one order this cluster will not take");
    // Documented on `SortOrder::Descending`. Checked rather than asserted, so
    // the day a cluster enables it, this says so instead of going stale.
    let descending = TableSchema::new([Column::new("k", ColumnType::Int64)
        .required()
        .sorted(SortOrder::Descending)]);
    match client.create_table(&format!("{BASE}/descending"), &descending) {
        Err(e) => println!("   as documented: {}", first_line(&e.to_string())),
        Ok(()) => println!(
            "   NOTE this cluster accepted a descending key column — \
             the doc on SortOrder::Descending is now out of date"
        ),
    }

    println!("\nA schema the cluster accepts, and enforces. Tables left at {BASE}");
    Ok(())
}

/// The rows the table holds. The schema is read off this and nothing else.
///
/// Spelled out rather than imported: with the client's `derive` feature on,
/// `TableRow` is both the trait and the macro, and importing the macro from
/// `ytsaurus-helpers` as well would be the same name twice.
#[derive(ytsaurus_helpers::TableRow)]
#[allow(dead_code)]
struct Visit<'a> {
    /// A key column: the table comes out sorted by it.
    #[yt(key)]
    host: &'a str,
    size: i64,
    /// Optional, because the Rust type says so.
    referrer: Option<&'a str>,
}

/// The same rows, a release later: the struct gained a field.
///
/// Optional, because the rows already in the table do not have it — which is
/// also the only kind of column a table with rows will accept.
#[derive(ytsaurus_helpers::TableRow)]
#[allow(dead_code)]
struct VisitV2<'a> {
    #[yt(key)]
    host: &'a str,
    size: i64,
    referrer: Option<&'a str>,
    note: Option<&'a str>,
}

/// Two rows the schema is happy with, in key order because the table is sorted.
fn two_visits() -> Result<Vec<u8>, ClientError> {
    let mut rows = Vec::new();
    for (host, size) in [("a.example", 1_i64), ("b.example", 2)] {
        let row = yson_build::map([
            ("host", yson_build::string(host)),
            ("size", yson_build::int(size)),
        ]);
        rows.extend_from_slice(&to_vec(&row, YsonFormat::Binary).map_err(|e| {
            ClientError::Decode {
                command: "write_table".to_owned(),
                reason: e.to_string(),
            }
        })?);
        rows.push(b';');
    }
    Ok(rows)
}

/// Schema changes a table with rows in it will not take.
///
/// The first is used twice: once here, and once against an empty table, where
/// the cluster allows it.
fn incompatible_changes() -> Vec<(&'static str, TableSchema)> {
    vec![
        (
            "dropping a column",
            TableSchema::new([Column::new("host", ColumnType::Utf8).required().key()]),
        ),
        (
            "adding a required column",
            TableSchema::new([
                Column::new("host", ColumnType::Utf8).required().key(),
                Column::new("size", ColumnType::Int64).required(),
                Column::new("referrer", ColumnType::Utf8),
                Column::new("note", ColumnType::Utf8),
                Column::new("must", ColumnType::Utf8).required(),
            ]),
        ),
        (
            "changing a column's type",
            TableSchema::new([
                Column::new("host", ColumnType::Utf8).required().key(),
                Column::new("size", ColumnType::String).required(),
                Column::new("referrer", ColumnType::Utf8),
                Column::new("note", ColumnType::Utf8),
            ]),
        ),
    ]
}

/// Every type the crate can name, in one table.
const ALL_TYPES: &[(&str, ColumnType)] = &[
    ("c_int8", ColumnType::Int8),
    ("c_int16", ColumnType::Int16),
    ("c_int32", ColumnType::Int32),
    ("c_int64", ColumnType::Int64),
    ("c_uint8", ColumnType::Uint8),
    ("c_uint16", ColumnType::Uint16),
    ("c_uint32", ColumnType::Uint32),
    ("c_uint64", ColumnType::Uint64),
    ("c_float", ColumnType::Float),
    ("c_double", ColumnType::Double),
    ("c_boolean", ColumnType::Boolean),
    ("c_string", ColumnType::String),
    ("c_utf8", ColumnType::Utf8),
    ("c_any", ColumnType::Any),
    ("c_date", ColumnType::Date),
    ("c_datetime", ColumnType::Datetime),
    ("c_timestamp", ColumnType::Timestamp),
    ("c_interval", ColumnType::Interval),
    ("c_date32", ColumnType::Date32),
    ("c_datetime64", ColumnType::Datetime64),
    ("c_timestamp64", ColumnType::Timestamp64),
    ("c_interval64", ColumnType::Interval64),
    ("c_json", ColumnType::Json),
    ("c_uuid", ColumnType::Uuid),
    ("c_void", ColumnType::Void),
    ("c_null", ColumnType::Null),
];

/// Schemas the client rejects without asking the cluster.
fn local_refusals() -> Vec<(&'static str, TableSchema)> {
    vec![
        (
            "a key column that is not a prefix",
            TableSchema::new([
                Column::new("value", ColumnType::Int64),
                Column::new("key", ColumnType::Utf8).key(),
            ]),
        ),
        (
            "a duplicate column name",
            TableSchema::new([
                Column::new("k", ColumnType::Int64),
                Column::new("k", ColumnType::Utf8),
            ]),
        ),
        (
            "a required any column",
            TableSchema::new([Column::new("blob", ColumnType::Any).required()]),
        ),
        (
            "unique_keys with no key column",
            TableSchema::new([Column::new("v", ColumnType::Int64)]).with_unique_keys(true),
        ),
        (
            "a column named after an attribute",
            TableSchema::new([Column::new("@id", ColumnType::Int64)]),
        ),
    ]
}

fn column_names(schema: &YsonValue) -> Vec<String> {
    let YsonNode::List(columns) = &schema.node else {
        return Vec::new();
    };
    columns
        .iter()
        .filter_map(|column| match &column.node {
            YsonNode::Map(fields) => fields
                .get(b"name".as_slice())
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            _ => None,
        })
        .collect()
}

fn render(value: &YsonValue) -> String {
    let text = to_string(value, YsonFormat::Text).unwrap_or_default();
    if text.len() > 220 {
        format!("{}…", &text[..220])
    } else {
        text
    }
}

fn first_line(message: &str) -> &str {
    message.lines().next().unwrap_or(message)
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn done(what: &str) {
    println!("   ok {what}");
}

fn check(what: &str, passed: bool) -> Result<(), ClientError> {
    if passed {
        done(what);
        return Ok(());
    }
    eprintln!("   FAIL {what}");
    Err(ClientError::Config(format!("check failed: {what}")))
}
