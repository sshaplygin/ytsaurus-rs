//! `selfrun` — one binary that is both the launcher and the job.
//!
//! The cluster starts a job by exec'ing an uploaded binary with `YT_JOB_ID` in
//! its environment. So a program can ask which role it is playing, and be both:
//!
//! ```text
//! your machine                          a cluster node
//! ────────────                          ──────────────
//! selfrun                               ./selfrun
//!   is_inside_job() -> false              is_inside_job() -> true
//!   uploads *itself*  ──────────────────► maps rows
//!   starts the operation                  writes the output table
//!   waits, reads the result back
//! ```
//!
//! The binary that runs on the cluster is the one you just built, because it is
//! the same file. There is no second artifact to forget to rebuild.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh selfrun     # the binary the cluster will run
//! cargo run -p ytsaurus-examples --bin selfrun
//! ```
//!
//! ## Two builds on macOS
//!
//! `upload_current_exe` uploads the running executable, which works when the
//! launcher is itself a Linux x86-64 static binary — build it with
//! `scripts/build-worker.sh` and run *that*. On macOS the running executable is
//! Mach-O, which no node can exec, and the client refuses it by inspecting the
//! ELF header rather than letting the job fail later. Set `YT_WORKER_BINARY` to
//! the musl build in that case; the source is still one file.
//!
//! The mapper counts hosts: `{url, size}` in, `{host, size}` out.

use serde::{Deserialize, Serialize};
use ytsaurus_client::{Client, ClientError, MapSpec, yson_build};
use ytsaurus_job::{Event, JobReader, JobWriter};
use ytsaurus_yson::{YsonFormat, to_vec};

/// Where the demo keeps its tables, and the worker.
const BASE: &str = "//tmp/ytsaurus_rs_selfrun";

/// Points at a cross-compiled build of this same source, for hosts that cannot
/// run a Linux binary themselves.
const WORKER_OVERRIDE: &str = "YT_WORKER_BINARY";

#[derive(Deserialize)]
struct Input<'a> {
    #[serde(borrow)]
    url: &'a str,
    size: i64,
}

#[derive(Serialize)]
struct Output<'a> {
    host: &'a str,
    size: i64,
}

fn main() -> std::process::ExitCode {
    // Inside a job this never returns: it runs the mapper and exits with the
    // status YTsaurus reads. On your machine it falls through.
    ytsaurus_job::run_if_inside_job(mapper);

    match launch() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nselfrun: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// The job. Runs on a node, once per chunk of input.
fn mapper() -> ytsaurus_job::Result<()> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(1)?;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };
        let input: Input = row.parse()?;
        let host = input.url.split('/').next().unwrap_or("");

        writer.write(
            0,
            &Output {
                host,
                size: input.size,
            },
        )?;
    }

    writer.finish()
}

/// The launcher. Runs on your machine.
fn launch() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    step("Preparing Cypress");
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/input"))?;
    client.create("table", &format!("{BASE}/output"))?;
    client.write_table(&format!("{BASE}/input"), &sample_rows())?;
    done(&format!(
        "{BASE}, {} rows",
        client.row_count(&format!("{BASE}/input"))?
    ));

    step("Uploading this very binary");
    let remote = format!("{BASE}/selfrun");
    match std::env::var(WORKER_OVERRIDE) {
        // A cross-compiled build of this same source, for a host whose own
        // binaries a cluster node cannot run.
        Ok(path) if !path.trim().is_empty() => {
            client.upload_worker(&path, &remote)?;
            done(&format!("{path} -> {remote}"));
        }
        _ => {
            client.upload_current_exe(&remote)?;
            done(&format!("{} -> {remote}", exe_name()));
        }
    }

    step("Starting the map operation");
    let spec = MapSpec::new(
        "./selfrun",
        [format!("{BASE}/input")],
        [format!("{BASE}/output")],
    )
    .with_local_file(&remote)
    .with_memory_limit(512 * 1024 * 1024)
    .with_raw("max_failed_job_count", yson_build::int(1));

    let id = client.start_map(&spec)?;
    done(&format!("operation {id}"));

    step("Waiting for it");
    client.wait_for_operation(&id)?;
    done("completed");

    step("Reading the result back");
    let rows = client.row_count(&format!("{BASE}/output"))?;
    let output = client.read_table(&format!("{BASE}/output"))?;
    done(&format!("{rows} rows, {} bytes", output.len()));

    println!("\nOne binary, two roles. Output at {BASE}/output");
    Ok(())
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn done(what: &str) {
    println!("   ok {what}");
}

fn exe_name() -> String {
    std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "this binary".to_owned())
}

/// Input rows, built with the codec rather than by hand.
fn sample_rows() -> Vec<u8> {
    #[derive(Serialize)]
    struct Row<'a> {
        url: &'a str,
        size: i64,
    }

    let rows = [
        Row {
            url: "example.com/a",
            size: 10,
        },
        Row {
            url: "example.com/b",
            size: 32,
        },
        Row {
            url: "ytsaurus.tech/docs",
            size: 7,
        },
    ];

    let mut out = Vec::new();
    for row in &rows {
        out.extend_from_slice(&to_vec(row, YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    out
}
