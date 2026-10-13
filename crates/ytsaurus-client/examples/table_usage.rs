//! `table_usage`: the Go SDK's table example, with rows as Rust values.
//!
//! `yt/go/examples/table-usage` infers a schema from a struct, creates a table
//! with it, writes a hundred structs, reads the row count back out of the
//! table's attributes and scans every row into the struct again. This does the
//! same, command for command, through `write_table_rows` and `read_table_rows`,
//! with no YSON in the example. It checks that the schema derived from
//! `Contact` is the one the cluster stored and enforces, that the rows come back
//! in order, and that `Name`, a struct naming one of the four columns, reads as
//! a projection. Tables are left under `//tmp/ytsaurus_rs_table_usage`.
//!
//! The Go example carries a dead branch that writes the same rows as Skiff.
//! Skiff table I/O here is `write_table_with_format` with `DataFormat::skiff`,
//! or `write_skiff_table`, and takes an encoded Skiff stream rather than typed
//! rows, so this example stays with YSON.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example table_usage
//! ```

use std::process::ExitCode;

use serde::{Deserialize, Serialize};
use ytsaurus_client::{Client, ClientError, TableRow};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, to_string};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_table_usage";

/// As many rows as the Go example writes.
const ROWS: usize = 100;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ntable_usage failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;
    let contacts = format!("{BASE}/contacts");

    step("Preparing Cypress");
    // A fixed path where Go uses a fresh guid; the remove makes reruns alike.
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    done(BASE);

    step("Creating a table from the struct its rows have");
    // `schema.Infer(Contact{})` in Go, the derive here: the column list is not
    // written out a second time, so it cannot disagree with the rows.
    let schema = Contact::table_schema();
    println!("   {}", render(&schema.to_yson()));
    client.create_table(&contacts, &schema)?;
    check(
        "the cluster stored the struct's four columns",
        column_names(&client.table_schema(&contacts)?) == ["name", "email", "phone", "age"],
    )?;

    step(&format!("Writing {ROWS} rows, as Rust values"));
    // Go pushes one struct at a time into a writer. This hands over an
    // iterator, encoded inside the request body, so the rows are never all in
    // memory.
    client.write_table_rows(&contacts, (0..ROWS).map(contact))?;
    done(&format!("{ROWS} contacts written"));

    step("Asking the cluster how many rows it has");
    let counted = client.row_count(&contacts)?;
    check(&format!("row_count is {counted}"), counted == ROWS as i64)?;

    // The same attribute, read the way Go reads it: `@` is the whole attribute
    // map, deserialised into a one-field struct that drops the rest.
    let attrs: Attrs = client.get_as(&format!("{contacts}/@"))?;
    check(
        "and the attribute map agrees, read into a one-field struct",
        attrs.row_count == counted,
    )?;

    step("Reading them back, as Rust values");
    let read = client.read_table_rows::<Contact>(&contacts)?;
    check(
        &format!("{} rows came back", read.len()),
        read.len() == ROWS,
    )?;

    // A static unsorted table keeps the order it was written in, so the round
    // trip must be the identity; a failure names the first differing row.
    let written: Vec<Contact> = (0..ROWS).map(contact).collect();
    if let Some(n) = read
        .iter()
        .zip(&written)
        .position(|(got, sent)| got != sent)
    {
        eprintln!(
            "   FAIL row {n} came back as {:?}, not {:?}",
            read[n], written[n]
        );
        return Err(ClientError::Config(
            "the round trip did not return what it was given".to_owned(),
        ));
    }
    check(
        "and every one is the row that went in, in order",
        read == written,
    )?;

    step("A struct naming one column is a projection");
    // Columns `Name` does not mention are ignored on the way in. The cluster
    // still sends all four: the projection is in the type, not the request.
    let names = client.read_table_rows::<Name>(&contacts)?;
    let first = names.first().map(|row| row.name.as_str());
    check(
        &format!("{} names came back, the first {first:?}", names.len()),
        names.len() == ROWS && first == Some(written[0].name.as_str()),
    )?;

    step("The same projection, in the other direction");
    // A projection does not write: the three columns it leaves out are
    // required by the schema. On its own table, because a refused write keeps
    // an upload transaction's lock for a moment afterwards.
    let partial = format!("{BASE}/partial");
    client.create_table(&partial, &Contact::table_schema())?;
    match client.write_table_rows(&partial, names.iter().take(1)) {
        Ok(()) => {
            eprintln!("   FAIL a row with three columns missing was accepted");
            return Err(ClientError::Config(
                "the derived schema was not enforced".to_owned(),
            ));
        }
        Err(e) => {
            let message = e.to_string();
            check(
                "a row missing the other three columns is refused",
                message.contains("email") || message.to_lowercase().contains("required"),
            )?;
            println!("   {}", first_line(&message));
        }
    }

    println!("\nA hundred Rust values in, and the same hundred out. Nothing here encodes");
    println!("YSON: the schema came off the struct, and the cluster holds the rows to it.");
    println!("A struct naming fewer columns reads them as a projection, and writes nothing.");
    println!("Tables left at {BASE}");
    Ok(())
}

/// The rows the table holds — the Go example's `Contact`, field for field.
///
/// The schema is derived from it, and rows are serialised from and
/// deserialised into it. The field names are the column names, where Go needs
/// a `yson:"name"` tag on each.
///
/// The derive path is spelled out: with the client's `derive` feature on,
/// `TableRow` is the trait and the macro, and importing both is a name clash.
#[derive(Serialize, Deserialize, PartialEq, Debug, ytsaurus_helpers::TableRow)]
struct Contact {
    name: String,
    email: String,
    phone: String,
    /// `int64`, because the Rust type is `i64` — as Go's `int` infers to.
    age: i64,
}

/// A projection: the same table, seen as one column.
///
/// Both `Deserialize` and `Serialize`: the write below shows the difference
/// between the two directions.
#[derive(Serialize, Deserialize)]
struct Name {
    name: String,
}

/// What Go reads with `GetNode(ctx, tablePath.Attrs(), &attrs, nil)`.
#[derive(Deserialize)]
struct Attrs {
    row_count: i64,
}

/// The `n`th contact.
///
/// The values vary, unlike Go's, so the round trip shows whether order
/// survived.
fn contact(n: usize) -> Contact {
    Contact {
        name: format!("Gopher {n}"),
        email: format!("gopher{n}@ytsaurus.tech"),
        phone: format!("+7{n:010}"),
        age: 27 + (n % 40) as i64,
    }
}

/// The column names of a schema as the cluster gives it back.
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
    to_string(value, YsonFormat::Text).unwrap_or_default()
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
