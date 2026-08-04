//! `table_usage` — the Go SDK's table example, with rows as Rust values.
//!
//! `yt/go/examples/table-usage` infers a schema from a struct, creates a table
//! with it, writes a hundred structs, reads the row count back out of the
//! table's attributes and scans every row into the struct again. This is the
//! same journey, command for command.
//!
//! What it does not contain is the point. Every example here used to build its
//! rows as YSON bytes and push a `;` between them — the same dozen lines,
//! eleven times over, which is how the loop came to be `write_table_rows`
//! instead. This is the example that exists to show the result: Rust values in,
//! Rust values out, and the encoding never appears. Two things the type system
//! is doing, both checked below: the schema is derived from `Contact`, so the
//! cluster is the one enforcing the shape `Contact` promises; and a struct
//! naming a subset of the columns is a projection, so `Name` reads one column
//! of a four-column table without mentioning the other three.
//!
//! The Go example carries a dead branch that would write the same rows as
//! Skiff rather than YSON; this crate has no Skiff, and `docs/benchmarking.md`
//! holds the measurements that keep that parked.
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
    // Go names its table after a fresh guid, so every run leaves a new one. A
    // fixed path is easier to go and look at afterwards, and the remove is
    // what makes a second run behave like the first.
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    done(BASE);

    step("Creating a table from the struct its rows have");
    // `schema.Infer(Contact{})` in Go, the derive here. Neither writes the
    // column list out a second time, which is the only arrangement in which it
    // cannot disagree with the rows.
    let schema = Contact::table_schema();
    println!("   {}", render(&schema.to_yson()));
    client.create_table(&contacts, &schema)?;
    check(
        "the cluster stored the struct's four columns",
        column_names(&client.table_schema(&contacts)?) == ["name", "email", "phone", "age"],
    )?;

    step(&format!("Writing {ROWS} rows, as Rust values"));
    // Go opens a writer and pushes one struct at a time, then commits. This
    // hands over an iterator: the encoder runs inside the request body, so the
    // hundred contacts are never all in memory, and no line of this file
    // produces a byte of YSON.
    client.write_table_rows(&contacts, (0..ROWS).map(contact))?;
    done(&format!("{ROWS} contacts written"));

    step("Asking the cluster how many rows it has");
    let counted = client.row_count(&contacts)?;
    check(&format!("row_count is {counted}"), counted == ROWS as i64)?;

    // The same attribute, read the way Go reads it: `@` is the whole attribute
    // map, `GetNode` deserialises it into a struct with one field, and the
    // dozens of attributes that struct does not name are dropped. A projection,
    // over attributes rather than columns.
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

    // Not "as many rows, near enough". A static unsorted table keeps the order
    // it was written in, so the round trip is either the identity or a
    // regression, and this says which row broke it.
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
    // Nothing declares `Name` to be a subset of `Contact`. Columns the type
    // does not mention are ignored on the way in, and that is the whole
    // mechanism. The cluster still sent all four: the projection is in the
    // type, not in the request.
    let names = client.read_table_rows::<Name>(&contacts)?;
    let first = names.first().map(|row| row.name.as_str());
    check(
        &format!("{} names came back, the first {first:?}", names.len()),
        names.len() == ROWS && first == Some(written[0].name.as_str()),
    )?;

    step("The same projection, in the other direction");
    // A projection reads and does not write: three of the columns it leaves
    // out were promised as required by the schema `Contact` derived, and the
    // cluster is holding that promise. On its own table, because a refused
    // write keeps an upload transaction's lock for a moment afterwards.
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
/// One struct doing three jobs: the schema is derived from it, the rows are
/// serialised from it, and what comes back is deserialised into it. Go needs a
/// `yson:"name"` tag on each field to get lower-case column names; the field
/// names here are already the column names.
///
/// The derive is spelled out rather than imported: with the client's `derive`
/// feature on, `TableRow` is the trait *and* the macro, and importing the
/// macro from `ytsaurus-helpers` as well would be the same name twice.
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
/// Deserialised from four columns and serialised back into one, which is why
/// it is both — the write below is what shows the difference between the two
/// directions.
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
/// Go fills all hundred with the same values. These vary, because a round trip
/// of a hundred identical rows cannot tell you the order survived it.
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
