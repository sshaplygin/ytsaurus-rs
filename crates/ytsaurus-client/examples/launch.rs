//! `launch` — runs a whole operation with no Python on the machine.
//!
//! Does end to end what `tests/e2e/run_e2e.sh` does through the `yt` CLI:
//! creates tables, uploads the worker, writes input, starts a map, waits for
//! it, and reads the result back. The point is that the only executable
//! involved is this one and the worker it uploads.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh cat
//! cargo run -p ytsaurus-client --example launch
//! ```
//!
//! Note this binary is a *launcher*, not a worker: it runs on your machine and
//! is built for the host, not for musl.

use std::process::ExitCode;

use serde::Serialize;
use ytsaurus_client::{Client, MapSpec};
use ytsaurus_yson::{Scan, YsonFormat, YsonNode, YsonValue, from_slice, scan::scan_value};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_launch";

/// The worker this launches, as produced by `scripts/build-worker.sh cat`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/cat";

/// The rows the map reads.
///
/// Chosen for the values an encoder gets wrong: an empty string, a negative
/// count, the largest `i64` there is, and a blob with nothing in it.
const SAMPLE: [Row; 3] = [
    Row {
        key: "alpha",
        count: 1,
        blob: &[0xDE, 0xAD],
        ratio: 0.5,
        flag: true,
    },
    Row {
        key: "beta",
        count: -2,
        blob: &[0x00, 0xFF],
        ratio: -1.25,
        flag: false,
    },
    Row {
        key: "",
        count: i64::MAX,
        blob: &[],
        ratio: 0.0,
        flag: true,
    },
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nlaunch failed: {e}");
            // The chain is where the cluster's actual complaint lives.
            let mut source = std::error::Error::source(&e);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ytsaurus_client::ClientError> {
    let client = Client::from_env()?;

    if !std::path::Path::new(WORKER).exists() {
        eprintln!("worker not found at {WORKER}");
        eprintln!("build it first:  scripts/build-worker.sh cat");
        return Err(ytsaurus_client::ClientError::Config(
            "the worker binary has not been built".to_owned(),
        ));
    }

    step("Preparing Cypress");
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/input"))?;
    client.create("table", &format!("{BASE}/output"))?;
    done(BASE);

    step("Uploading the worker");
    client.upload_worker(WORKER, &format!("{BASE}/cat"))?;
    done(&format!("{BASE}/cat (executable)"));

    step("Writing input rows");
    client.write_table_rows(&format!("{BASE}/input"), SAMPLE)?;
    done(&format!(
        "{} rows",
        client.row_count(&format!("{BASE}/input"))?
    ));

    step("Starting the map operation");
    let spec = MapSpec::new(
        "./cat",
        [format!("{BASE}/input")],
        [format!("{BASE}/output")],
    )
    .with_local_file(format!("{BASE}/cat"))
    .with_memory_limit(512 * 1024 * 1024);

    let id = client.start_map(&spec)?;
    done(&format!("operation {id}"));

    step("Waiting for it to finish");
    client.wait_for_operation(&id)?;
    done("completed");

    step("Checking the result");
    // `cat` is the identity, so reading both tables back through the same path
    // must give identical bytes. Comparing against the rows this wrote would
    // fail for an unrelated reason: the cluster re-encodes them on ingest.
    let before = client.read_table(&format!("{BASE}/input"))?;
    let after = client.read_table(&format!("{BASE}/output"))?;

    if before != after {
        eprintln!(
            "   output differs from input: {} vs {} bytes",
            before.len(),
            after.len()
        );
        return Err(ytsaurus_client::ClientError::Config(
            "the identity map changed its input".to_owned(),
        ));
    }
    done(&format!("identical ({} bytes)", after.len()));

    step("Decoding a row, to prove it is real data");
    // Identical to the input is not the same as correct: two empty tables are
    // identical too. So the first record has to be there, and has to be a map
    // with the columns that went in.
    let first = first_record(&after).ok_or_else(|| {
        ytsaurus_client::ClientError::Config("the output table has no first row".to_owned())
    })?;
    let value: YsonValue = from_slice(first, YsonFormat::Binary).map_err(|e| {
        ytsaurus_client::ClientError::Decode {
            command: "read_table".to_owned(),
            reason: e.to_string(),
        }
    })?;
    let YsonNode::Map(m) = &value.node else {
        return Err(ytsaurus_client::ClientError::Config(
            "the first record of the output is not a row".to_owned(),
        ));
    };
    let columns: Vec<String> = m
        .keys()
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .collect();
    // Compared as a set, because the order columns come back in is the
    // cluster's name table talking and not anything this wrote.
    let mut found = columns.clone();
    found.sort();
    if found != ["blob", "count", "flag", "key", "ratio"] {
        eprintln!("   FAIL first row has columns {columns:?}");
        return Err(ytsaurus_client::ClientError::Config(
            "the row that came back is not the row that went in".to_owned(),
        ));
    }
    done(&format!("first row has columns {columns:?}"));

    println!("\nAll done — no Python was involved.");
    println!("Tables left at {BASE}");
    Ok(())
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn done(what: &str) {
    println!("   ok {what}");
}

/// Returns the first record of a binary YSON list fragment.
fn first_record(data: &[u8]) -> Option<&[u8]> {
    let trimmed = data.strip_prefix(b";").unwrap_or(data);
    match scan_value(trimmed, YsonFormat::Binary).ok()? {
        Scan::Complete { len } => Some(&trimmed[..len]),
        Scan::Incomplete => None,
    }
}

/// A row of the input table. These go in as values and the client serialises
/// them, which is why nothing here writes YSON by hand.
#[derive(Serialize)]
struct Row {
    key: &'static str,
    count: i64,
    #[serde(with = "serde_bytes")]
    blob: &'static [u8],
    ratio: f64,
    flag: bool,
}
