//! `sort_reduce` — sort a table, then reduce over it.
//!
//! The shape a map-reduce is often used for by mistake. Once a table is sorted,
//! a plain reduce runs over it directly, without paying for a shuffle that has
//! already happened — and the sorted table can be reduced again and again.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh wordcount
//! cargo run -p ytsaurus-client --example sort_reduce
//! ```
//!
//! Reuses the `wordcount` worker's reduce phase: it sums `count` within each
//! `word` group.

use std::collections::BTreeMap;
use std::process::ExitCode;

use serde::{Deserialize, Serialize};
use ytsaurus_client::{Client, ClientError, ReduceSpec, SortSpec, yson_build};
use ytsaurus_yson::{YsonFormat, YsonValue};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_sort_reduce";

/// The worker this launches, as produced by `scripts/build-worker.sh wordcount`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/wordcount";

/// Deliberately out of order, and with each word split across several rows.
const INPUT: &[(&str, i64)] = &[
    ("delta", 1),
    ("alpha", 2),
    ("beta", 5),
    ("alpha", 3),
    ("beta", 1),
    ("gamma", 7),
    ("alpha", 1),
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nsort_reduce failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    if !std::path::Path::new(WORKER).exists() {
        eprintln!("worker not found at {WORKER}");
        eprintln!("build it first:  scripts/build-worker.sh wordcount");
        return Err(ClientError::Config(
            "the worker binary has not been built".to_owned(),
        ));
    }

    step("Preparing Cypress");
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    for table in ["input", "sorted", "counts"] {
        client.create("table", &format!("{BASE}/{table}"))?;
    }
    client.upload_worker(WORKER, &format!("{BASE}/wordcount"))?;
    client.write_table_rows(format!("{BASE}/input"), input_rows())?;
    done(&format!("{} unsorted rows", INPUT.len()));

    step("Sorting it");
    let sort = SortSpec::new(
        [format!("{BASE}/input")],
        format!("{BASE}/sorted"),
        ["word"],
    );
    let id = client.start_sort(&sort)?;
    client.wait_for_operation(&id)?;

    // The cluster records what a table is sorted by. A reduce refuses to start
    // otherwise, so this is the precondition made visible.
    let sorted_by = client.get(&format!("{BASE}/sorted/@sorted_by"))?;
    check(
        &format!("the table is now sorted by {}", render(&sorted_by)),
        render(&sorted_by) == "[word]",
    )?;

    step("Reducing over the sorted table");
    let reduce = ReduceSpec::new(
        "./wordcount reduce",
        [format!("{BASE}/sorted")],
        [format!("{BASE}/counts")],
        ["word"],
    )
    .with_local_file(format!("{BASE}/wordcount"))
    .with_memory_limit(512 * 1024 * 1024)
    .with_raw("max_failed_job_count", yson_build::int(1));

    let id = client.start_reduce(&reduce)?;
    client.wait_for_operation(&id)?;
    done(&format!(
        "{} rows",
        client.row_count(&format!("{BASE}/counts"))?
    ));

    step("Checking the totals");
    // Keyed by word, because what is being checked is what each group came to
    // and not the order the reduce emitted its groups in.
    let counts: BTreeMap<String, i64> = client
        .read_table_rows::<Total>(&format!("{BASE}/counts"))?
        .into_iter()
        .map(|row| (row.word, row.count))
        .collect();
    let expected = expected_totals();

    for (word, count) in &expected {
        check(
            &format!("{word} = {count}"),
            counts.get(word) == Some(count),
        )?;
    }
    check("no extra groups", counts.len() == expected.len())?;

    println!("\nA sorted table reduces without a shuffle. Tables left at {BASE}");
    Ok(())
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

fn render(value: &YsonValue) -> String {
    ytsaurus_yson::to_string(value, YsonFormat::Text).unwrap_or_default()
}

/// What the reduce should produce: the input summed per word.
fn expected_totals() -> BTreeMap<String, i64> {
    let mut expected = BTreeMap::new();
    for (word, count) in INPUT {
        *expected.entry((*word).to_owned()).or_insert(0) += count;
    }
    expected
}

/// Input rows, in the shape `wordcount reduce` consumes.
fn input_rows() -> impl Iterator<Item = Row> {
    INPUT.iter().map(|(word, count)| Row {
        word: word.as_bytes(),
        count: *count,
    })
}

/// A row of the input table: a word, and part of what it comes to.
///
/// `word` is bytes rather than a `&str` because that is how the worker reads
/// it: a string column holds arbitrary bytes, and a job that insisted on UTF-8
/// would fail on the first row that was not.
#[derive(Serialize)]
struct Row {
    #[serde(with = "serde_bytes")]
    word: &'static [u8],
    count: i64,
}

/// A row of the reduce's output: a word, and everything counted for it.
///
/// A `String` here rather than the bytes the worker wrote, because these words
/// came from `INPUT` and the totals are compared against it by name.
#[derive(Deserialize)]
struct Total {
    word: String,
    count: i64,
}
