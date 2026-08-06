//! `cached_upload` — upload the worker once, launch as often as you like.
//!
//! A worker binary is tens of megabytes, and re-sending it on every launch is
//! the slowest part of a dev loop that changes nothing but the spec. The
//! cluster has a file cache keyed by MD5: `upload_worker_cached` asks it first
//! and only uploads on a miss.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh cat
//! cargo run -p ytsaurus-client --example cached_upload
//! ```
//!
//! Prints the time each call took, which is the whole point of the feature.
//!
//! On an installation whose cache is operator-managed there is no second call
//! to time: the client warns, uploads outside the cache and carries on — a
//! launch that works, and nothing to demonstrate. The example stops there and
//! says so, rather than failing later at a check whose cause has scrolled off.

use std::process::ExitCode;
use std::time::Instant;

use serde::Serialize;
use ytsaurus_client::{CachedFile, Client, ClientError, MapSpec};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_cached";

/// The worker this launches, as produced by `scripts/build-worker.sh cat`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/cat";

/// A couple of rows, so the operation has something to copy.
const SAMPLE: [Row; 2] = [Row { key: "a", count: 1 }, Row { key: "b", count: 2 }];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ncached_upload failed: {e}");
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
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/input"))?;
    client.create("table", &format!("{BASE}/output"))?;
    client.write_table_rows(format!("{BASE}/input"), SAMPLE)?;
    let size = std::fs::metadata(WORKER).map(|m| m.len()).unwrap_or(0);
    done(&format!("{BASE}, worker is {} KiB", size / 1024));

    // The cache is shared and persistent, so a previous run of this example
    // would leave nothing to miss on. Clearing it makes the first call a real
    // upload every time.
    step("Clearing this binary out of the cache, so the first call is a miss");
    let digest = md5_of(WORKER)?;
    if let Some(cached) = client.file_from_cache(&digest)? {
        client.remove(&cached)?;
        done(&format!("removed {cached}"));
    } else {
        done("nothing cached");
    }

    step("First upload");
    let (first, cold) = timed(|| client.upload_worker_cached(WORKER))?;
    describe(&first, cold);
    check("the first call uploaded it", first.uploaded)?;

    // `upload_worker_cached` is allowed to succeed *without* the cache, and on
    // an installation that keeps its cache to itself that is what happens. The
    // example has nothing to demonstrate then, and says so here rather than
    // three checks downstream where the symptom is "the second call uploaded
    // it too" and the cause is nowhere in the output.
    if !first.cached {
        return Err(nothing_to_demonstrate(&first));
    }

    step("Second upload of the same binary");
    let (second, warm) = timed(|| client.upload_worker_cached(WORKER))?;
    describe(&second, warm);

    check("the second call skipped the upload", !second.uploaded)?;
    check("and found it in the cache", second.cached)?;
    check("and found the same file", second.path == first.path)?;
    check(
        &format!(
            "and was quicker: {:.0} ms against {:.0} ms",
            warm.as_secs_f64() * 1000.0,
            cold.as_secs_f64() * 1000.0
        ),
        warm < cold,
    )?;

    step("Running the cached binary");
    // The cached node is named after the hash, so the sandbox name has to be
    // given explicitly or `./cat` would find nothing to run.
    let spec = MapSpec::new(
        "./cat",
        [format!("{BASE}/input")],
        [format!("{BASE}/output")],
    )
    .with_local_file_named(&second.path, &second.name)
    .with_memory_limit(512 * 1024 * 1024);

    let id = client.start_map(&spec)?;
    client.wait_for_operation(&id)?;

    let before = client.read_table(&format!("{BASE}/input"))?;
    let after = client.read_table(&format!("{BASE}/output"))?;
    check(
        "the identity map reproduced its input",
        before == after && !after.is_empty(),
    )?;

    println!("\nOne upload, any number of launches. Tables left at {BASE}");
    Ok(())
}

fn timed<T>(
    action: impl FnOnce() -> Result<T, ClientError>,
) -> Result<(T, std::time::Duration), ClientError> {
    let started = Instant::now();
    let value = action()?;
    Ok((value, started.elapsed()))
}

fn describe(file: &CachedFile, took: std::time::Duration) {
    println!(
        "   {} in {:.0} ms -> {}{}",
        if file.uploaded {
            "uploaded"
        } else {
            "cache hit"
        },
        took.as_secs_f64() * 1000.0,
        file.path,
        if file.cached { "" } else { "   (not cached)" }
    );
}

/// Why this example stops when the cache would not take the worker.
///
/// The client has already said what happened — it warns on stderr, names the
/// cache and quotes the cluster's refusal — and then uploaded outside the
/// cache, which is exactly right for a launcher and useless for a
/// demonstration of caching: nothing was put in the cache, so the second call
/// misses too and every check after it fails for a reason that is three steps
/// upstream of where it is reported. The example's non-zero exit is a real
/// finding about the installation, and this is it in words.
fn nothing_to_demonstrate(first: &CachedFile) -> ClientError {
    eprintln!("   FAIL nothing went into the cache, so there is no hit to demonstrate");
    eprintln!("        the worker went to {} instead", first.path);
    eprintln!("        every launch will send the whole binary again until that changes");
    eprintln!("        the warning printed above names the cache that refused it and quotes");
    eprintln!("        the cluster; Client::with_file_cache points this at a path you can");
    eprintln!("        write to, and then the example has something to show");
    ClientError::Config(
        "the file cache would not take the worker, so there is no cache hit to demonstrate"
            .to_owned(),
    )
}

/// The same digest the client computes, so the example can clear the entry.
fn md5_of(path: &str) -> Result<String, ClientError> {
    let bytes = std::fs::read(path).map_err(|source| ClientError::Io {
        path: path.to_owned(),
        source,
    })?;
    Ok(format!("{:x}", md5::compute(&bytes)))
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
