//! `vanilla` — an operation with no input tables at all.
//!
//! Three jobs, no input, each computing its own slice of a sum and writing one
//! row; the rows must add up to the whole sum. The shape for gang jobs,
//! side-car computation, anything that is not a transformation of a table.
//!
//! It then reads back what the jobs printed, as the Go SDK's `vanilla-example`
//! does: a vanilla job often has no output table, so stderr is how it reports,
//! and the cluster keeps stderr for jobs that succeeded as well as failed.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh shards
//! cargo run -p ytsaurus-client --example vanilla
//! ```

use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError, VanillaSpec, VanillaTask, yson_build};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_vanilla";

/// The worker this launches, as produced by `scripts/build-worker.sh shards`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/shards";

/// How many jobs the task runs.
const JOBS: i64 = 3;

/// What the jobs are between them adding up: 1 + 2 + … + 1000.
const EXPECTED_SUM: i64 = 1_000 * 1_001 / 2;
const EXPECTED_COUNT: i64 = 1_000;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nvanilla failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    if !std::path::Path::new(WORKER).exists() {
        eprintln!("worker not found at {WORKER}");
        eprintln!("build it first:  scripts/build-worker.sh shards");
        return Err(ClientError::Config(
            "the worker binary has not been built".to_owned(),
        ));
    }

    step("Preparing Cypress");
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/results"))?;
    let worker = client.upload_worker_cached(WORKER)?;
    done(&format!("{BASE}/results, no input table anywhere"));

    step(&format!("Running {JOBS} jobs with nothing to read"));
    let spec = VanillaSpec::new(
        // The job count reaches the worker through its command line: the
        // cluster tells a job its own cookie but not how many siblings it has.
        VanillaTask::new("shards", format!("./shards {JOBS}"), JOBS)
            .with_local_file_named(&worker.path, &worker.name)
            .with_outputs([format!("{BASE}/results")])
            .with_memory_limit(512 * 1024 * 1024),
    )
    .with_raw("max_failed_job_count", yson_build::int(1));

    let id = client.start_vanilla(&spec)?;
    client.wait_for_operation(&id)?;
    done(&format!("operation {id} completed"));

    step("Reading back what the jobs printed");
    // Asked for right after the operation finishes: the controller agent
    // forgets its jobs soon afterwards, and a cluster with no job archive, such
    // as a local one, then has nothing left to tell.
    let jobs = client.list_jobs(&id, None, JOBS as u32)?;
    check(
        &format!("the cluster still lists all {JOBS} jobs"),
        jobs.len() == JOBS as usize,
    )?;

    let mut spoke = 0;
    for job in &jobs {
        let stderr = client.get_job_stderr(&id, &job.id)?;
        let text = String::from_utf8_lossy(&stderr);
        let text = text.trim();
        if !text.is_empty() {
            // A prefix of the id, through `get` because a job id need not be
            // eight characters.
            println!("   {}: {text}", job.id.get(..8).unwrap_or(&job.id));
            spoke += 1;
        }
    }
    // Stderr is kept for jobs that succeeded too, not only for post-mortems.
    check(
        &format!("all {JOBS} succeeded and their stderr survived"),
        spoke == JOBS as usize,
    )?;

    step("Checking what the jobs wrote");
    let rows: Vec<Shard> = client.read_table_rows(format!("{BASE}/results"))?;

    check(
        &format!("{JOBS} rows, one per job"),
        rows.len() == JOBS as usize,
    )?;

    let cookies: std::collections::BTreeSet<i64> = rows.iter().map(|r| r.cookie).collect();
    check(
        &format!("the jobs identified themselves as {cookies:?}"),
        cookies.len() == JOBS as usize,
    )?;

    let total: i64 = rows.iter().map(|r| r.sum).sum();
    let counted: i64 = rows.iter().map(|r| r.counted).sum();
    check(
        &format!("their slices add up to {EXPECTED_SUM}"),
        total == EXPECTED_SUM,
    )?;
    check(
        &format!("and cover every one of the {EXPECTED_COUNT} numbers exactly once"),
        counted == EXPECTED_COUNT,
    )?;

    println!("\nJobs with no input tables, split by nothing but their cookie.");
    println!("Rows left at {BASE}/results");
    Ok(())
}

/// One job's row, named as the worker writes it.
///
/// The worker's struct also has a `shards` column, which this does not name
/// and so does not read.
#[derive(serde::Deserialize)]
struct Shard {
    cookie: i64,
    sum: i64,
    counted: i64,
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
