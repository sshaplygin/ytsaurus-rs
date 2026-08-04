//! `statistics` — a job counts its own work, the operation adds it up.
//!
//! Runs the `counted` worker, which drops rows without a `key` column and
//! reports how many it read and dropped. Nothing else would tell you that rows
//! went missing: the operation succeeded, and the output table is simply
//! shorter than the input.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh counted
//! cargo run -p ytsaurus-client --example statistics
//! ```

use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError, MapSpec};
use ytsaurus_yson::{YsonFormat, to_string, to_vec};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_statistics";

/// The worker this launches, as produced by `scripts/build-worker.sh counted`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/counted";

/// Rows with a key, and rows without one that the job will drop.
const KEPT: i64 = 4;
const DROPPED: i64 = 3;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nstatistics failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    if !std::path::Path::new(WORKER).exists() {
        eprintln!("worker not found at {WORKER}");
        eprintln!("build it first:  scripts/build-worker.sh counted");
        return Err(ClientError::Config(
            "the worker binary has not been built".to_owned(),
        ));
    }

    step("Preparing Cypress");
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/input"))?;
    client.create("table", &format!("{BASE}/output"))?;
    let worker = client.upload_worker_cached(WORKER)?;
    client.write_table(&format!("{BASE}/input"), &sample_rows())?;
    done(&format!(
        "{} rows in, {DROPPED} of them without a key",
        KEPT + DROPPED
    ));

    step("Running the job");
    let spec = MapSpec::new(
        "./counted",
        [format!("{BASE}/input")],
        [format!("{BASE}/output")],
    )
    .with_local_file_named(&worker.path, &worker.name)
    .with_memory_limit(512 * 1024 * 1024)
    // One job, so the statistic is one job's count rather than a sum over
    // several — the sum is the same either way, which is the point of them.
    .with_job_count(1);

    let id = client.start_map(&spec)?;
    client.wait_for_operation(&id)?;
    done(&format!(
        "completed, {} rows written",
        client.row_count(&format!("{BASE}/output"))?
    ));

    step("What the job reported");
    let all = client.custom_statistics(&id)?;
    println!(
        "   {}",
        to_string(&all, YsonFormat::Text).unwrap_or_default()
    );

    check(
        &format!("rows/read is {}", KEPT + DROPPED),
        client.statistic_sum(&id, "rows/read")? == Some(KEPT + DROPPED),
    )?;
    check(
        &format!("rows/rejected is {DROPPED}"),
        client.statistic_sum(&id, "rows/rejected")? == Some(DROPPED),
    )?;
    check(
        "bytes/read is not zero",
        client.statistic_sum(&id, "bytes/read")?.unwrap_or(0) > 0,
    )?;
    check(
        "a statistic nobody reported is absent, not zero",
        client.statistic_sum(&id, "rows/invented")?.is_none(),
    )?;

    println!("\nThe operation succeeded and still told us rows were dropped.");
    println!("Tables left at {BASE}");
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

/// Rows with a `key`, and rows without one.
fn sample_rows() -> Vec<u8> {
    use serde::Serialize;

    #[derive(Serialize)]
    struct Kept<'a> {
        key: &'a str,
        count: i64,
    }

    #[derive(Serialize)]
    struct Dropped {
        count: i64,
    }

    let mut out = Vec::new();
    for i in 0..KEPT {
        let row = Kept {
            key: "alpha",
            count: i,
        };
        out.extend_from_slice(&to_vec(&row, YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    for i in 0..DROPPED {
        let row = Dropped { count: i };
        out.extend_from_slice(&to_vec(&row, YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    out
}
