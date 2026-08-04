//! `diagnose` — proves that a failed operation explains itself.
//!
//! Runs the `boom` worker, which panics on its first row, and checks that the
//! [`ClientError`] the launcher gets back carries the job's own stderr. Before
//! this existed, a failed operation gave you a state string and a trip to the
//! web UI.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh boom
//! cargo run -p ytsaurus-client --example diagnose
//! ```
//!
//! Exits non-zero if the operation *succeeds* — that would mean the test is no
//! longer testing anything.

use std::process::ExitCode;

use serde::Serialize;
use ytsaurus_client::{Client, ClientError, MapSpec, yson_build};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_diagnose";

/// The worker this launches, as produced by `scripts/build-worker.sh boom`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/boom";

/// What `boom` panics with. Asserting on it is what makes this a test of *this*
/// job's stderr rather than of any message that mentions a failure.
const MARKER: &str = "boom: this job fails on purpose";

/// A couple of rows, so the job has something to fail on.
const SAMPLE: [Row; 2] = [Row { key: "a", count: 1 }, Row { key: "b", count: 2 }];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ndiagnose failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    if !std::path::Path::new(WORKER).exists() {
        eprintln!("worker not found at {WORKER}");
        eprintln!("build it first:  scripts/build-worker.sh boom");
        return Err(ClientError::Config(
            "the worker binary has not been built".to_owned(),
        ));
    }

    step("Preparing Cypress");
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/input"))?;
    client.create("table", &format!("{BASE}/output"))?;
    client.upload_worker(WORKER, &format!("{BASE}/boom"))?;
    client.write_table_rows(&format!("{BASE}/input"), SAMPLE)?;
    done(&format!("{BASE} ready, worker uploaded"));

    step("Starting an operation that will fail");
    let spec = MapSpec::new(
        "./boom",
        [format!("{BASE}/input")],
        [format!("{BASE}/output")],
    )
    .with_local_file(format!("{BASE}/boom"))
    .with_memory_limit(512 * 1024 * 1024)
    // One attempt. The default lets the cluster retry a failing job several
    // times, which only makes the wait longer; the failure is deterministic.
    .with_raw("max_failed_job_count", yson_build::int(1));

    let id = client.start_map(&spec)?;
    done(&format!("operation {id}"));

    step("Waiting for it to fail");
    let Err(error) = client.wait_for_operation(&id) else {
        return Err(ClientError::Config(
            "the operation completed; `boom` is supposed to fail".to_owned(),
        ));
    };

    step("What the launcher was told");
    println!("{error}\n");

    let ClientError::OperationFailed { jobs, .. } = &error else {
        eprintln!("   expected an OperationFailed, got a different error");
        return Err(error);
    };

    check("a failed job was reported", !jobs.is_empty())?;
    check(
        "the job's stderr came back",
        jobs.iter().any(|j| j.stderr.is_some()),
    )?;
    check(
        "the stderr is the job's own panic",
        error.to_string().contains(MARKER),
    )?;
    check(
        "the job error explains the exit",
        jobs.iter().any(|j| j.error.is_some()),
    )?;

    println!("\nA failed operation now says why, without opening the web UI.");
    println!("Tables left at {BASE}");
    Ok(())
}

/// A row of the input table. `write_table_rows` serialises these, so the
/// example never spells out a row's bytes.
#[derive(Serialize)]
struct Row {
    key: &'static str,
    count: i64,
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
