//! `idempotent` — proves a repeated `start_operation` starts one operation.
//!
//! This is what makes a retry safe. Every mutating command the client sends
//! carries a `mutation_id`; when a request is repeated, the cluster recognises
//! the ID and hands back the first response instead of applying the change
//! twice. Without it, a transient 503 on the way *back* from a successful
//! `start_operation` would leave the retry starting a second operation over the
//! same tables.
//!
//! Here the duplicate is forced rather than waited for: the same
//! [`MutationId`] is used twice, which is exactly what a retry does on the
//! wire.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh cat
//! cargo run -p ytsaurus-client --example idempotent
//! ```

use std::process::ExitCode;

use serde::Serialize;
use ytsaurus_client::{Client, ClientError, MapSpec, MutationId, OperationType, yson_build};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_idempotent";

/// The worker this launches, as produced by `scripts/build-worker.sh cat`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/cat";

/// A couple of rows, so the operation has something to do.
const SAMPLE: [Row; 2] = [Row { key: "a", count: 1 }, Row { key: "b", count: 2 }];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nidempotent failed: {e}");
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

    step("Preparing Cypress");
    // These are mutating commands too, so they are already carrying mutation
    // IDs of their own — if the cluster disliked the format, this would fail.
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/input"))?;
    client.create("table", &format!("{BASE}/output"))?;
    client.create("table", &format!("{BASE}/control_output"))?;
    client.upload_worker(WORKER, &format!("{BASE}/cat"))?;
    client.write_table_rows(format!("{BASE}/input"), SAMPLE)?;
    done(&format!("{BASE} ready"));

    let spec = identity_map("output");

    step("Starting the operation twice under one mutation ID");
    let mutation = MutationId::new();
    println!("   mutation_id {mutation}");

    let first = client.start_operation_with(OperationType::Map, &spec, &mutation)?;
    done(&format!("first  -> {first}"));

    // Byte for byte the request a retry would send — including the `retry`
    // flag, without which the cluster refuses the duplicate rather than
    // deduplicating it: `Duplicate request is not marked as "retry"`.
    let second = client.start_operation_with(OperationType::Map, &spec, &mutation.as_retry())?;
    done(&format!("second -> {second}"));

    check("both calls returned the same operation", first == second)?;

    step("And a different ID really does start another one");
    // Its own output table: two operations writing one table would queue on an
    // exclusive lock, which would say nothing about mutation IDs.
    let control = identity_map("control_output");
    let other = client.start_operation_with(OperationType::Map, &control, &MutationId::new())?;
    check(
        "a fresh mutation ID starts a second operation",
        other != first,
    )?;

    step("Letting them finish");
    client.wait_for_operation(&first)?;
    client.wait_for_operation(&other)?;
    done("both completed");

    println!("\nA repeated start_operation is one operation, so a retry is safe.");
    println!("Tables left at {BASE}");
    Ok(())
}

/// The identity map, writing into `output`.
fn identity_map(output: &str) -> ytsaurus_yson::YsonValue {
    MapSpec::new(
        "./cat",
        [format!("{BASE}/input")],
        [format!("{BASE}/{output}")],
    )
    .with_local_file(format!("{BASE}/cat"))
    .with_memory_limit(512 * 1024 * 1024)
    .with_raw("max_failed_job_count", yson_build::int(1))
    .to_yson()
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
