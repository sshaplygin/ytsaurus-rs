//! `rich_path` — the columns and rows a path can name, checked against a
//! cluster.
//!
//! `TablePath::columns` and `TablePath::range` are attributes on the path, the
//! same mechanism as `<append=%true>` and for the same reason. The shapes are
//! pinned offline by `tests/request_shape.rs`; what a wire test cannot say is
//! **which rows come back**, and for key ranges that is the whole question:
//! `key` and `key_bound` compare a short key by opposite rules, so `a..b` and
//! `a..=b` on a two-column key differ by a whole group of rows rather than by
//! one row. This example asks the cluster and checks the answers.
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
    // Every row comes back carrying only what was asked for. A map is what
    // shows it: a struct would fail on the missing fields instead, which is
    // the more useful failure but not the one being demonstrated.
    let only_n: Vec<BTreeMap<String, i64>> =
        client.read_table_rows(TablePath::new(&visits).columns(["n"]))?;
    check(
        "columns([n]) gives 5 rows of exactly one key",
        only_n.len() == 5 && only_n.iter().all(|row| row.keys().eq(["n"].iter())),
    )?;

    // The measured fact `TablePath::columns` documents: an unknown name is not
    // an error, it is simply absent. A typo reads clean and decodes short.
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
    // they are specified" — so a later range first really does come back
    // first, and the client must not sort them on the way out.
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

    // `keys(a..=b)` sends `{key=[a]}` … `{key_bound=["<=";[b]]}` — two
    // different limit representations inside one range entry, which the
    // reference documents separately and never together. The cluster takes
    // it. And `key_bound` truncates the row's key to the bound's length
    // before comparing, so every row of host b compares *equal* to `[b]` and
    // `<=` takes the whole group: four rows, not three.
    check(
        "keys(a..=b) takes all of host b, and the mixed key/key_bound entry is accepted",
        summary(&client.read_table_rows::<Visit>(
            TablePath::new(&visits).range(RowRange::keys(Key::from("a")..=Key::from("b"))),
        )?) == "a/x a/y b/x b/y",
    )?;

    // The same truncation in the other direction, and the one that costs
    // rows rather than adding them: `>` on a prefix drops the whole group.
    // There is no "the row just after a" for the cluster to start from.
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
    // A full key selects one row, which is the other half of "prefix".
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
    // A Skiff read's columns are its format's fields; a range says which
    // rows. The cluster gets both attributes on one path — `<columns=[n];
    // ranges=[…]>` — and this is the check that it takes them together.
    let skiff = client.read_skiff_table(TablePath::new(&visits).range(0..2), &n_only())?;
    check(
        "a Skiff read merges its schema's columns with a typed row range",
        !skiff.is_empty(),
    )?;

    step("A read still takes a string-spelled selection verbatim");
    // Code that read `//tmp/t[#0:#2]` before TablePath modelled ranges keeps
    // working: the string is never parsed, and the cluster honours it.
    check(
        "//…[#0:#2] reads the first two rows, as it always did",
        summary(&client.read_table_rows::<Visit>(format!("{visits}[#0:#2]"))?) == "a/x a/y",
    )?;

    step("And the shapes this client refuses to send");
    // A write with a read selection. The cluster's answer is to ignore the
    // selection and replace the whole table with a 200 — silent data loss —
    // so the refusal is local and the table is checked afterwards to prove
    // nothing left the process.
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

    // One selection per path, whichever way it is spelled.
    refused(
        "a read spelling a selection twice, once in the string and once typed",
        client.read_table_rows::<Visit>(TablePath::new(format!("{visits}[#0:#2]")).range(0..2)),
    )?;
    refused(
        "a Skiff read whose path string names columns — its format already does",
        client.read_skiff_table(format!("{visits}{{n}}"), &n_only()),
    )?;

    // Selections that ask for nothing. Every one of these is answered 200 by
    // the cluster and comes back empty, so they are refused here instead of
    // costing a round trip to learn nothing.
    refused(
        "columns([]), which the cluster answers with one empty map per row",
        client.read_table_rows::<Visit>(TablePath::new(&visits).columns(Vec::<String>::new())),
    )?;
    // From variables because clippy will not compile the literal `5..3`, and
    // for the same reason a caller only ever reaches it computed: an offset
    // that came out wrong, where a silently empty read is hardest to spot.
    let (from, to) = (5_i64, 3_i64);
    refused(
        "range(5..3), as `&rows[5..3]` would be",
        client.read_table_rows::<Visit>(TablePath::new(&visits).range(from..to)),
    )?;
    refused(
        "range(-5..0), since rows are numbered from 0",
        client.read_table_rows::<Visit>(TablePath::new(&visits).range(-5..0)),
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

/// Checks that a call was refused *locally*, before anything was sent.
///
/// `ClientError::Config` is the whole assertion: a cluster that answered would
/// have failed as `Transport` or `Cluster`, and a success is the regression
/// this exists to catch.
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
