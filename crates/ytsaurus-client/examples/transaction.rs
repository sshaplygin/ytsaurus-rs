//! `transaction` — several commands, one all-or-nothing outcome.
//!
//! A launcher that creates a table, uploads a worker and runs an operation has
//! three chances to fail halfway, and each one leaves something behind. This
//! runs that sequence inside a transaction and watches the cluster: nothing is
//! visible until the commit, and nothing survives an abort — including the one
//! a dropped handle sends on its way out of a failing function.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh cat
//! cargo run -p ytsaurus-client --example transaction
//! ```

use std::process::ExitCode;
use std::time::{Duration, Instant};

use ytsaurus_client::{Client, ClientError, MapSpec};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_transaction";

/// The worker this launches, as produced by `scripts/build-worker.sh cat`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/cat";

/// A transaction short enough that the ping thread is the only reason it lives.
const SHORT: Duration = Duration::from_secs(2);

/// How long to hold that transaction — several timeouts' worth.
const HELD: Duration = Duration::from_secs(6);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ntransaction failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    if !std::path::Path::new(WORKER).exists() {
        eprintln!("worker not found at {WORKER}");
        eprintln!("build it first:  scripts/build-worker.sh cat");
        return Err(ClientError::Config(
            "the worker binary has not been built".to_owned(),
        ));
    }

    let input = format!("{BASE}/input");
    let output = format!("{BASE}/output");
    let staging = format!("{BASE}/staging");

    step("Preparing Cypress");
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &input)?;
    client.write_table(&input, &sample_rows())?;
    client.create("table", &output)?;
    // Stands in for last week's result: what everyone reads until the new one
    // is published, and what they must go on reading if publishing fails.
    client.write_table(&output, &previous_result())?;
    done(&format!("{} rows in, an old result in place", 3));

    step("A table that exists only inside a transaction");
    let tx = client.start_transaction()?;
    println!("   transaction {}", tx.id());
    tx.create("table", &staging)?;
    check("the transaction sees it", tx.exists(&staging)?)?;
    check("and nothing outside does", !client.exists(&staging)?)?;

    step("Aborting it");
    tx.abort()?;
    check("nothing was left behind", !client.exists(&staging)?)?;

    step("A launcher that fails halfway");
    let half = format!("{BASE}/half_written");
    let failure = publish_and_fail(&client, &half).expect_err("this one fails on purpose");
    println!("   the launcher failed: {failure}");
    // No abort was written anywhere in that function. The `?` returned, the
    // handle dropped, and the drop is the abort.
    check(
        "the half-written table is gone with it",
        !client.exists(&half)?,
    )?;

    step("Publishing an operation's output atomically");
    let previous = client.read_table(&output)?;
    let tx = client.start_transaction()?;
    let worker = format!("{BASE}/cat");

    tx.upload_worker(WORKER, &worker)?;
    let spec = MapSpec::new("./cat", [input.clone()], [output.clone()])
        .with_local_file(&worker)
        .with_memory_limit(512 * 1024 * 1024);
    let id = tx.start_map(&spec)?;
    tx.wait_for_operation(&id)?;
    done(&format!("operation {id} completed"));

    // The operation has run, its rows are written, and none of it exists yet
    // for anybody else — the upload included.
    check(
        "outside the transaction the old result is still the result",
        client.read_table(&output)? == previous,
    )?;
    check(
        "and the worker is not in Cypress at all",
        !client.exists(&worker)?,
    )?;

    step("Committing");
    tx.commit()?;
    check(
        "the output is the operation's, all at once",
        client.read_table(&output)? == client.read_table(&input)?,
    )?;
    check("and the upload came with it", client.exists(&worker)?)?;

    step("What a transaction that is gone looks like");
    let orphan = {
        let tx = client.start_transaction()?;
        tx.id().to_owned()
    }; // aborted here, by the drop
    let rejoined = client.clone().with_transaction(&orphan);
    match rejoined.create("table", &format!("{BASE}/never")) {
        Ok(()) => return Err(ClientError::Config(
            "a command in an aborted transaction succeeded, which means the abort did not happen"
                .to_owned(),
        )),
        Err(e) => done(&format!("as expected: {e}")),
    }

    step(&format!(
        "Holding a {}s transaction for {}s",
        SHORT.as_secs(),
        HELD.as_secs()
    ));
    let slow = format!("{BASE}/slow");
    let started = Instant::now();
    let tx = client.start_transaction_with(SHORT)?;
    tx.create("table", &slow)?;
    std::thread::sleep(HELD);
    // Nothing in this function pinged anything. If the handle's thread were not
    // doing it, the cluster would have aborted this transaction four seconds
    // ago and the commit would fail with `No such transaction`.
    tx.commit()?;
    check(
        &format!("committed {:.0}s in", started.elapsed().as_secs_f64()),
        client.exists(&slow)?,
    )?;

    println!("\nEverything published in one step, or not at all.");
    println!("Tables left at {BASE}");
    Ok(())
}

/// A launcher that gets halfway and then fails, the way a real one does.
///
/// The interesting part is what is *not* written here: no abort, no cleanup, no
/// `if let Err`. The `?` returns, `tx` drops on the way out, and the table it
/// created never existed as far as the rest of the cluster is concerned.
fn publish_and_fail(client: &Client, path: &str) -> Result<(), ClientError> {
    let tx = client.start_transaction()?;

    tx.create("table", path)?;
    tx.write_table(path, &sample_rows())?;

    Err(ClientError::Config(
        "the step after the write did not work out".to_owned(),
    ))
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

/// The rows the map reads.
fn sample_rows() -> Vec<u8> {
    rows(&[("alpha", 1), ("beta", 2), ("gamma", 3)])
}

/// What the output table holds before this run publishes anything.
fn previous_result() -> Vec<u8> {
    rows(&[("last week", 0)])
}

fn rows(pairs: &[(&str, i64)]) -> Vec<u8> {
    use serde::Serialize;
    use ytsaurus_yson::{YsonFormat, to_vec};

    #[derive(Serialize)]
    struct Row<'a> {
        key: &'a str,
        count: i64,
    }

    let mut out = Vec::new();
    for (key, count) in pairs {
        let row = Row { key, count: *count };
        out.extend_from_slice(&to_vec(&row, YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    out
}
