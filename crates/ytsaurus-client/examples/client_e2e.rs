//! `client_e2e` — everything `tests/cluster-e2e/run_e2e.sh` checks, with no Python involved.
//!
//! The shell script needs the `yt` CLI, which means a Python installation, which
//! is the one thing this stack exists to avoid. Every command it sends has a
//! method on [`Client`] — `remove --recursive --force` is `remove_tree`,
//! `create --recursive` is `create`, `write-table --format
//! '<format=binary>yson'` is `write_table` because binary YSON is the default,
//! and the two `--spec` fragments that actually matter
//! (`enable_input_table_index`, and `enable_key_switch` under **`reduce_job_io`**
//! rather than `job_io`) are modelled on the spec builders. So this is that
//! script, in the language of the thing it is testing.
//!
//! Three checks, in the order the script runs them:
//!
//! 1. **identity** — `cat` as a map, output compared to input byte-for-byte;
//! 2. **table switching** — two input and two output tables, each pair compared;
//! 3. **wordcount** — a map-reduce checked against a hand-computed reference.
//!
//! # Where the bytes come from, and why it matters
//!
//! The first two checks upload `tests/cluster-e2e/fixtures/table_rows_*.bin` **as
//! bytes**, exactly as the script pipes them into `yt write-table`. Those
//! fixtures are built by `generate_fixtures.py` straight from the binary YSON
//! specification and *deliberately not* by this project's own encoder — a
//! payload produced by the code under test would not prove the code under test
//! is right. Reading the file keeps that property; re-generating the rows here
//! would quietly throw it away.
//!
//! The wordcount input is written with [`Client::write_table_rows`], which does
//! go through this project's encoder — and that is fine, because what that check
//! asserts is a set of word counts, not a byte sequence. The distinction is the
//! point: use the foreign fixture where the claim is about bytes.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh cat wordcount
//! cargo run -p ytsaurus-client --example client_e2e
//! ```
//!
//! This binary is a *launcher*: it runs on your machine, built for the host. The
//! workers it uploads are the static musl builds.

use std::collections::BTreeMap;
use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError, MapReduceSpec, MapSpec};

/// Where the run keeps its tables. The same default the shell script uses, so
/// the two cannot both be run and then disagree about whose tree is whose.
const BASE: &str = "//tmp/ytsaurus_rs_e2e";

/// The workers, as `scripts/build-worker.sh cat wordcount` leaves them.
const WORKER_DIR: &str = "target/x86_64-unknown-linux-musl/release-worker";

/// Rows built from the YSON specification rather than by this project's codec.
const FIXTURES: &str = "tests/cluster-e2e/fixtures";

/// The sentences the map-reduce counts, and the counts they must produce.
///
/// Small enough to check by hand, which is the only kind of reference worth
/// comparing against: a total computed by the same code would agree with itself
/// however wrong it was.
const LINES: [&str; 4] = [
    "the quick brown fox",
    "jumps over the lazy dog",
    "the fox and the dog",
    "quick quick fox",
];

const EXPECTED: [(&str, i64); 9] = [
    ("and", 1),
    ("brown", 1),
    ("dog", 2),
    ("fox", 3),
    ("jumps", 1),
    ("lazy", 1),
    ("over", 1),
    ("quick", 3),
    ("the", 4),
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ne2e failed: {e}");
            // The cluster's actual complaint lives in the chain, not the head.
            let mut source = std::error::Error::source(&e);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    step("Preflight");
    for worker in ["cat", "wordcount"] {
        let path = format!("{WORKER_DIR}/{worker}");
        if !std::path::Path::new(&path).exists() {
            return Err(ClientError::Config(format!(
                "{path} is missing; build it with: scripts/build-worker.sh cat wordcount"
            )));
        }
    }
    // Not checked for being an ELF here: `upload_worker` refuses a binary built
    // for the host, and its message says so better than a check written twice
    // would.
    let rows0 = fixture("table_rows_0.bin")?;
    let rows1 = fixture("table_rows_1.bin")?;
    done(&format!(
        "workers built, fixtures read ({} and {} bytes)",
        rows0.len(),
        rows1.len()
    ));

    step("Preparing Cypress");
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    client.upload_worker(format!("{WORKER_DIR}/cat"), &format!("{BASE}/cat"))?;
    client.upload_worker(
        format!("{WORKER_DIR}/wordcount"),
        &format!("{BASE}/wordcount"),
    )?;
    done(&format!("{BASE}, with both workers uploaded"));

    identity(&client, &rows0)?;
    table_switching(&client, &rows0, &rows1)?;
    wordcount(&client)?;

    println!("\nAll end-to-end checks passed, and nothing Python ran.");
    println!("Cypress tree left at {BASE}; remove it with Client::remove_tree.");
    Ok(())
}

/// `cat` as a map: what goes in must come out unchanged.
fn identity(client: &Client, rows: &[u8]) -> Result<(), ClientError> {
    step("Running cat as a map operation");

    client.create("table", &format!("{BASE}/input"))?;
    client.write_table(format!("{BASE}/input"), rows)?;
    // The output table has to exist first. `yt map --dst` creates it for you,
    // and this crate does not: an operation that made its own outputs would
    // turn a mistyped destination into a stray table rather than an error.
    client.create("table", &format!("{BASE}/output"))?;

    let spec = MapSpec::new(
        "./cat",
        [format!("{BASE}/input")],
        [format!("{BASE}/output")],
    )
    .with_local_file(format!("{BASE}/cat"))
    .with_memory_limit(512 * 1024 * 1024);

    let id = client.start_map(&spec)?;
    client.wait_for_operation(&id)?;
    done(&format!("operation {id} finished"));

    step("Comparing input and output byte-for-byte");
    // Both sides are read back through the same path. Comparing the output
    // against the *uploaded file* instead would fail for a reason that has
    // nothing to do with the job: the cluster re-encodes rows on ingest, and
    // 309 676 bytes came back as 309 688.
    let before = client.read_table(format!("{BASE}/input"))?;
    let after = client.read_table(format!("{BASE}/output"))?;

    check(
        &format!("identical ({} bytes)", before.len()),
        before == after,
    )
}

/// Two inputs, two outputs, and rows routed by the table they came from.
fn table_switching(client: &Client, rows0: &[u8], rows1: &[u8]) -> Result<(), ClientError> {
    step("Two input tables, two output tables, with table switching");

    for (table, rows) in [("in0", rows0), ("in1", rows1)] {
        client.create("table", &format!("{BASE}/{table}"))?;
        client.write_table(format!("{BASE}/{table}"), rows)?;
    }
    for table in ["out0", "out1"] {
        client.create("table", &format!("{BASE}/{table}"))?;
    }

    // `with_input_table_index` is `enable_input_table_index=%true` on the
    // mapper. Without it every row arrives as table 0 and `cat --tables 2`
    // would send the whole input to one output — which the comparison below
    // would catch, but only as "table 1 diverged" rather than as the missing
    // spec option it is.
    let spec = MapSpec::new(
        "./cat --tables 2",
        [format!("{BASE}/in0"), format!("{BASE}/in1")],
        [format!("{BASE}/out0"), format!("{BASE}/out1")],
    )
    .with_local_file(format!("{BASE}/cat"))
    .with_memory_limit(512 * 1024 * 1024)
    .with_input_table_index();

    let id = client.start_map(&spec)?;
    client.wait_for_operation(&id)?;

    for i in 0..2 {
        let input = client.read_table(format!("{BASE}/in{i}"))?;
        let output = client.read_table(format!("{BASE}/out{i}"))?;
        check(
            &format!("table {i} identical ({} bytes)", input.len()),
            input == output,
        )?;
    }

    Ok(())
}

/// The canonical map-reduce, checked against counts done by hand.
fn wordcount(client: &Client) -> Result<(), ClientError> {
    step("Wordcount map-reduce");

    client.create("table", &format!("{BASE}/lines"))?;
    client.write_table_rows(
        format!("{BASE}/lines"),
        LINES.iter().map(|text| Line { text }),
    )?;
    client.create("table", &format!("{BASE}/counts"))?;

    // `enable_key_switch` is not passed here because the builder already sends
    // it, and — the part that is easy to get wrong — under `reduce_job_io`
    // rather than `job_io`. A map-reduce gives each job type its own I/O
    // section; `job_io` is accepted, silently ignored, and the reducer then
    // receives the whole input as one group, summing every word together.
    let spec = MapReduceSpec::new(
        "./wordcount reduce",
        [format!("{BASE}/lines")],
        [format!("{BASE}/counts")],
        ["word"],
    )
    .with_mapper("./wordcount map")
    .with_local_file(format!("{BASE}/wordcount"))
    .with_memory_limit(512 * 1024 * 1024);

    let id = client.start_map_reduce(&spec)?;
    client.wait_for_operation(&id)?;
    done(&format!("operation {id} finished"));

    // Read as typed rows: the shell script goes through `--format json` and a
    // Python one-liner to get here, and JSON is a format this crate has no need
    // of when serde can take the YSON straight into a struct.
    //
    // `word` as `String` is safe *here* because these are the ASCII sentences
    // written above; a YTsaurus string column holds arbitrary bytes, which is
    // why the worker itself reads it as `&[u8]`.
    let counted: BTreeMap<String, i64> = client
        .read_table_rows::<Total>(format!("{BASE}/counts"))?
        .into_iter()
        .map(|row| (row.word, row.count))
        .collect();

    let expected: BTreeMap<String, i64> = EXPECTED
        .iter()
        .map(|(word, count)| ((*word).to_owned(), *count))
        .collect();

    if counted != expected {
        eprintln!("     got      {counted:?}");
        eprintln!("     expected {expected:?}");
    }
    check(
        &format!("wordcount matches the reference ({} words)", counted.len()),
        counted == expected,
    )
}

/// A row of the input table the map-reduce reads.
#[derive(serde::Serialize)]
struct Line<'a> {
    text: &'a str,
}

/// What the reducer wrote, read back.
#[derive(serde::Deserialize)]
struct Total {
    word: String,
    count: i64,
}

/// Reads one of the fixtures the shell script pipes into `yt write-table`.
fn fixture(name: &str) -> Result<Vec<u8>, ClientError> {
    let path = format!("{FIXTURES}/{name}");
    std::fs::read(&path).map_err(|e| {
        ClientError::Config(format!(
            "cannot read {path}: {e}. Run from the repository root; the \
             fixtures are committed, and `tests/cluster-e2e/generate_fixtures.py` \
             rebuilds them."
        ))
    })
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
