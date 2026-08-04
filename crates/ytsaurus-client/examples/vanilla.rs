//! `vanilla` — an operation with no input tables at all.
//!
//! Three jobs, no input, each computing its own slice of a sum and writing one
//! row. That is a whole category this stack could not reach before: gang jobs,
//! side-car computation, anything that is not a transformation of a table.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh shards
//! cargo run -p ytsaurus-client --example vanilla
//! ```

use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError, VanillaSpec, VanillaTask, yson_build};
use ytsaurus_yson::{Scan, YsonFormat, YsonNode, YsonValue, from_slice, scan::scan_value};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_vanilla";

/// The worker this launches, as produced by `scripts/build-worker.sh shards`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/shards";

/// How many jobs the task runs. More than one is the point.
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
    client.remove(BASE)?;
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

    step("Checking what the jobs wrote");
    let rows = decode(&client.read_table(&format!("{BASE}/results"))?)?;

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

/// One job's row.
struct Shard {
    cookie: i64,
    sum: i64,
    counted: i64,
}

fn decode(table: &[u8]) -> Result<Vec<Shard>, ClientError> {
    let mut rows = Vec::new();
    let mut data = table;

    loop {
        while data.first() == Some(&b';') {
            data = &data[1..];
        }
        if data.is_empty() {
            return Ok(rows);
        }

        let len = match scan_value(data, YsonFormat::Binary) {
            Ok(Scan::Complete { len }) => len,
            other => {
                return Err(ClientError::Decode {
                    command: "read_table".to_owned(),
                    reason: format!("the result table is not a complete fragment: {other:?}"),
                });
            }
        };

        let value: YsonValue =
            from_slice(&data[..len], YsonFormat::Binary).map_err(|e| ClientError::Decode {
                command: "read_table".to_owned(),
                reason: e.to_string(),
            })?;
        data = &data[len..];

        let YsonNode::Map(row) = &value.node else {
            continue;
        };
        let field = |name: &str| row.get(name.as_bytes()).and_then(YsonValue::as_i64);

        rows.push(Shard {
            cookie: field("cookie").unwrap_or(-1),
            sum: field("sum").unwrap_or(0),
            counted: field("counted").unwrap_or(0),
        });
    }
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
