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

use ytsaurus_client::{Client, ClientError, ReduceSpec, SortSpec, yson_build};
use ytsaurus_yson::{Scan, YsonFormat, YsonNode, YsonValue, from_slice, scan::scan_value, to_vec};

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
    client.write_table(&format!("{BASE}/input"), &input_rows())?;
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
    let counts = totals(&client.read_table(&format!("{BASE}/counts"))?)?;
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
fn input_rows() -> Vec<u8> {
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row<'a> {
        #[serde(with = "serde_bytes")]
        word: &'a [u8],
        count: i64,
    }

    let mut out = Vec::new();
    for (word, count) in INPUT {
        let row = Row {
            word: word.as_bytes(),
            count: *count,
        };
        out.extend_from_slice(&to_vec(&row, YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    out
}

/// Decodes an output table into `word -> count`.
fn totals(table: &[u8]) -> Result<BTreeMap<String, i64>, ClientError> {
    let mut out = BTreeMap::new();

    for record in records(table) {
        let value: YsonValue =
            from_slice(record, YsonFormat::Binary).map_err(|e| ClientError::Decode {
                command: "read_table".to_owned(),
                reason: e.to_string(),
            })?;

        let YsonNode::Map(row) = &value.node else {
            continue;
        };
        let word = match row.get(b"word".as_slice()).map(|v| &v.node) {
            Some(YsonNode::String(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            _ => continue,
        };
        let count = row.get(b"count".as_slice()).and_then(YsonValue::as_i64);
        if let Some(count) = count {
            out.insert(word, count);
        }
    }

    Ok(out)
}

/// Splits a binary YSON list fragment into records without decoding them.
fn records(mut data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();

    loop {
        while data.first() == Some(&b';') || data.first().is_some_and(u8::is_ascii_whitespace) {
            data = &data[1..];
        }
        if data.is_empty() {
            return out;
        }

        match scan_value(data, YsonFormat::Binary) {
            Ok(Scan::Complete { len }) => {
                out.push(&data[..len]);
                data = &data[len..];
            }
            // `read_table` already rejected a truncated stream; anything left
            // here is not worth a second error path.
            _ => return out,
        }
    }
}
