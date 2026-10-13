//! `rich_path`: the columns and rows a path can name, checked against a
//! cluster.
//!
//! `TablePath::columns` and `TablePath::range` are attributes on the path, like
//! `<append=%true>`. `tests/request_shape.rs` pins the request shapes offline;
//! this example checks which rows come back. On a two-column key, `a..b` and
//! `a..=b` differ by a whole group of rows, because `key` and `key_bound`
//! compare a short key by different rules. It also checks the selections the
//! client refuses to send. Tables are left under `//tmp/ytsaurus_rs_rich_path`.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example rich_path
//! ```

use std::collections::BTreeMap;
use std::ops::Bound;
use std::process::ExitCode;

use serde::{Deserialize, Serialize};
use ytsaurus_client::{
    Client, ClientError, Column, ColumnType, Key, RowRange, SkiffFormat, SkiffSchema,
    SkiffSchemaRef, SkiffWireType, TablePath, TableSchema, yson_build,
};
use ytsaurus_skiff::Decoder as SkiffDecoder;

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_rich_path";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nrich_path failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;
    let visits = format!("{BASE}/visits");

    step("Preparing Cypress");
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    // Two key columns, because a *prefix* of the key is where the two key
    // selectors disagree, and a single-column key can never show it.
    let schema = TableSchema::new([
        Column::new("host", ColumnType::Utf8).required().key(),
        Column::new("path", ColumnType::Utf8).required().key(),
        Column::new("n", ColumnType::Int64).required(),
    ]);
    client.create_table(&visits, &schema)?;
    client.write_table_rows(&visits, rows())?;
    check(
        "5 rows on a table keyed (host, path): a/x a/y b/x b/y c/x",
        summary(&client.read_table_rows::<Visit>(&visits)?) == "a/x a/y b/x b/y c/x",
    )?;

    step("Naming columns");
    // Read into a map, which shows which keys each row carries; a struct
    // would fail on the missing fields instead.
    let only_n: Vec<BTreeMap<String, i64>> =
        client.read_table_rows(TablePath::new(&visits).columns(["n"]))?;
    check(
        "columns([n]) gives 5 rows of exactly one key",
        only_n.len() == 5 && only_n.iter().all(|row| row.keys().eq(["n"].iter())),
    )?;

    // An unknown column name is not an error, just absent, so a typo reads
    // clean and decodes short.
    let with_typo: Vec<BTreeMap<String, i64>> =
        client.read_table_rows(TablePath::new(&visits).columns(["n", "nosuch"]))?;
    check(
        "a column the table does not have is not an error, just absent",
        with_typo == only_n,
    )?;

    step("Naming rows by index");
    check(
        "range(0..2) is rows 0 and 1, as `&rows[0..2]` would be",
        summary(&client.read_table_rows::<Visit>(TablePath::new(&visits).range(0..2))?)
            == "a/x a/y",
    )?;
    // "The specified ranges will be read sequentially, in the order in which
    // they are specified", so the client must not sort them.
    check(
        "two ranges arrive in the order given, not in table order",
        summary(&client.read_table_rows::<Visit>(TablePath::new(&visits).range(3..4).range(0..1))?)
            == "b/y a/x",
    )?;

    step("Naming rows by key — where the two selectors disagree");
    // `keys(a..b)` sends `{key=[a]}` … `{key=[b]}`. A short key compares
    // component-wise against the row's full key, the shorter being smaller
    // when equal so far, so the `b` group is out.
    check(
        "keys(a..b) stops before host b: a/x a/y",
        summary(&client.read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::keys(Key::from("a")..Key::from("b"))),
        )?) == "a/x a/y",
    )?;

    // `keys(a..=b)` sends `{key=[a]}` … `{key_bound=["<=";[b]]}`, two limit
    // representations in one range entry, which the cluster accepts.
    // `key_bound` truncates the row's key to the bound's length, so every row
    // of host b compares equal to `[b]` and `<=` takes the whole group.
    check(
        "keys(a..=b) takes all of host b, and the mixed key/key_bound entry is accepted",
        summary(&client.read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::keys(Key::from("a")..=Key::from("b"))),
        )?) == "a/x a/y b/x b/y",
    )?;

    // The same truncation in the other direction: `>` on a prefix drops the
    // whole group.
    check(
        "keys((Excluded(a), Unbounded)) drops every row of host a, not one row",
        summary(
            &client.read_table_rows::<Visit>(TablePath::new(&visits).range(RowRange::keys((
                Bound::Excluded(Key::from("a")),
                Bound::Unbounded,
            ))))?,
        ) == "b/x b/y c/x",
    )?;

    // Two rows apart, and the difference is a whole prefix group.
    let half_open = client
        .read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::keys(Key::from("a")..Key::from("b"))),
        )?
        .len();
    let inclusive = client
        .read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::keys(Key::from("a")..=Key::from("b"))),
        )?
        .len();
    check(
        &format!("a..b is {half_open} rows and a..=b is {inclusive}: a group apart, not a row"),
        inclusive - half_open == 2,
    )?;

    step("The exact selector");
    check(
        "exact_key(a) is every row of host a",
        summary(&client.read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::exact_key(Key::from("a"))),
        )?) == "a/x a/y",
    )?;
    check(
        "and says the same as keys(a..=a), which is what its doc claims",
        client.read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::exact_key(Key::from("a"))),
        )? == client.read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::keys(Key::from("a")..=Key::from("a"))),
        )?,
    )?;
    // A full key selects one row.
    let full = Key::new([yson_build::string("a"), yson_build::string("/y")]);
    check(
        "a full key selects the single row it names",
        summary(
            &client.read_table_rows::<Visit>(
                TablePath::new(&visits).range(RowRange::exact_key(full)),
            )?,
        ) == "a/y",
    )?;

    step("Columns and rows together, including through Skiff");
    check(
        "columns and a range on one path narrow both ways",
        client
            .read_table_rows::<BTreeMap<String, i64>>(
                TablePath::new(&visits).columns(["n"]).range(1..3),
            )?
            .len()
            == 2,
    )?;
    // A Skiff read's columns are its format's fields, so the cluster gets
    // `<columns=[n];ranges=[…]>` on one path. The rows are counted: a range
    // accepted and then dropped would answer with the whole table.
    let skiff = client.read_skiff_table(TablePath::new(&visits).range(0..2), &n_only())?;
    check(
        "a Skiff read merges its schema's columns with a typed row range: 2 rows of the 5",
        skiff_rows(&skiff)? == 2,
    )?;

    step("A read still takes a string-spelled selection verbatim");
    // A string such as `//tmp/t[#0:#2]` is passed through unparsed, and the
    // cluster honours it.
    check(
        "//…[#0:#2] reads the first two rows, as it always did",
        summary(&client.read_table_rows::<Visit>(format!("{visits}[#0:#2]"))?) == "a/x a/y",
    )?;
    // Including on a Skiff read: the synthesised `<columns=[n]>` and a string
    // `[…]` select different things, so the cluster takes both.
    let ranged_skiff = client.read_skiff_table(format!("{visits}[#0:#2]"), &n_only())?;
    check(
        "a Skiff read takes a string-spelled range: 2 rows from the string, columns from the schema",
        skiff_rows(&ranged_skiff)? == 2,
    )?;
    // A string row range and typed columns compose too: the row count checks
    // the first, the keys of each row the second.
    let both: Vec<BTreeMap<String, i64>> =
        client.read_table_rows(TablePath::new(format!("{visits}[#0:#2]")).columns(["n"]))?;
    check(
        "a string-spelled range and a typed column selection combine: 2 rows of exactly n",
        both.len() == 2 && both.iter().all(|row| row.keys().eq(["n"].iter())),
    )?;

    step("An empty projection counts rows without reading any of them");
    // `columns([])` counts the rows of a range or key window cheaply;
    // `row_count` reads the whole-table `@row_count`.
    check(
        "columns([]) over a row range answers one empty record per row",
        client
            .read_table_rows::<BTreeMap<String, i64>>(
                TablePath::new(&visits)
                    .columns(Vec::<String>::new())
                    .range(1..3),
            )?
            .len()
            == 2,
    )?;
    check(
        "and over a key range, where @row_count has nothing to say",
        client
            .read_table_rows::<BTreeMap<String, i64>>(
                TablePath::new(&visits)
                    .columns(Vec::<String>::new())
                    .range(RowRange::keys(Key::from("a")..Key::from("c"))),
            )?
            .len()
            == 4,
    )?;

    step("And the shapes this client refuses to send");
    // A write with a read selection: the cluster would ignore the selection
    // and replace the whole table at 200. The refusal is local, and the table
    // is checked afterwards to show nothing was sent.
    refused(
        "a write that names a row range",
        client.write_table_rows(TablePath::new(&visits).range(0..2), rows()),
    )?;
    refused(
        "a write whose path string spells a range",
        client.write_table_rows(format!("{visits}[#0:#2]"), rows()),
    )?;
    check(
        "and the table is untouched: still 5 rows",
        client.row_count(&visits)? == 5,
    )?;

    // The same kind of selection spelled twice, `<ranges=[…]>"//tmp/t[#0:#2]"`:
    // the cluster lets the attribute win and discards the string's half at 200.
    // The client refuses that, so what is checked here is that a string block
    // on its own is honoured.
    check(
        "a path string's own `<columns=[n]>` is honoured when nothing is added",
        client
            .read_table_rows::<BTreeMap<String, i64>>(format!("<columns=[n]>{visits}"))?
            .iter()
            .all(|row| row.len() == 1 && row.contains_key("n")),
    )?;
    refused(
        "a read spelling a row selection twice, once in the string and once typed",
        client.read_table_rows::<Visit>(TablePath::new(format!("{visits}[#0:#2]")).range(0..2)),
    )?;
    refused(
        "a Skiff read whose path string names columns — its format already does",
        client.read_skiff_table(format!("{visits}{{n}}"), &n_only()),
    )?;
    // A path string that opens with an attribute block, whatever it holds:
    // the client does not parse the block, and if it names the attribute being
    // added the cluster discards the caller's silently.
    refused(
        "a typed range on a path whose string already opens with `<…>`",
        client
            .read_table_rows::<Visit>(TablePath::new(format!("<columns=[n]>{visits}")).range(0..2)),
    )?;

    // A backwards range comes back empty at 200; a negative row index is
    // clamped to 0 and comes back with real rows. `5..3` is built from
    // variables because clippy rejects the literal.
    let (from, to) = (5_i64, 3_i64);
    refused(
        "range(5..3), as `&rows[5..3]` would be",
        client.read_table_rows::<Visit>(TablePath::new(&visits).range(from..to)),
    )?;
    refused(
        "range(-5..2), which the cluster would have read as range(0..2)",
        client.read_table_rows::<Visit>(TablePath::new(&visits).range(-5..2)),
    )?;
    refused(
        "keys(b..a), the same mistake in the other selector",
        client.read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::keys(Key::from("b")..Key::from("a"))),
        ),
    )?;
    // What the refusal guards against: a negative lower limit reads from row 0.
    check(
        "and the cluster really does clamp: <ranges=[{lower_limit={row_index=-5}}]> is all 5 rows",
        client
            .read_table_rows::<Visit>(format!(
                "<ranges=[{{lower_limit={{row_index=-5}}}}]>{visits}"
            ))?
            .len()
            == 5,
    )?;

    println!(
        "\nThe rule to carry away: on a key *prefix*, `<=` takes the whole group and `>` drops it."
    );
    println!("Tables left at {BASE}");
    Ok(())
}

/// One row of the visits table.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Visit {
    host: String,
    path: String,
    n: i64,
}

/// The five rows every check above is measured against, in key order.
fn rows() -> impl Iterator<Item = Visit> {
    [
        ("a", "/x"),
        ("a", "/y"),
        ("b", "/x"),
        ("b", "/y"),
        ("c", "/x"),
    ]
    .into_iter()
    .enumerate()
    .map(|(n, (host, path))| Visit {
        host: host.to_owned(),
        path: path.to_owned(),
        n: n as i64 + 1,
    })
}

/// Rows as `host/path host/path …`, so a check reads as the table does.
fn summary(rows: &[Visit]) -> String {
    rows.iter()
        .map(|row| format!("{}{}", row.host, row.path))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A Skiff format naming one column of the table.
fn n_only() -> SkiffFormat {
    SkiffFormat::new(vec![SkiffSchemaRef::Inline(SkiffSchema::tuple([
        SkiffSchema::named("n", SkiffWireType::Int64),
    ]))])
    .expect("one named tuple is a valid format")
}

/// How many rows a Skiff answer holds.
///
/// Decoded rather than divided out of the byte count.
fn skiff_rows(stream: &[u8]) -> Result<usize, ClientError> {
    let mut decoder = SkiffDecoder::new(stream, n_only());
    let mut rows = 0;
    while decoder
        .skip_row()
        .map_err(|error| ClientError::Decode {
            command: "read_skiff_table".to_owned(),
            reason: error.to_string(),
        })?
        .is_some()
    {
        rows += 1;
    }
    Ok(rows)
}

/// Checks that a call was refused *locally*, before anything was sent.
///
/// `ClientError::Config` is the assertion: a cluster that answered would have
/// failed as `Transport` or `Cluster`.
fn refused<T>(what: &str, outcome: Result<T, ClientError>) -> Result<(), ClientError> {
    match outcome {
        Ok(_) => {
            eprintln!("   FAIL {what} was not refused");
            Err(ClientError::Config(format!("{what} was allowed through")))
        }
        Err(ClientError::Config(reason)) => {
            done(what);
            println!("   {}", first_line(&reason));
            Ok(())
        }
        Err(other) => {
            eprintln!("   FAIL {what} failed elsewhere: {other}");
            Err(other)
        }
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
