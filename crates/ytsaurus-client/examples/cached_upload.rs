//! `cached_upload`: upload the worker once, launch as often as you like.
//!
//! The cluster has a file cache keyed by MD5: `upload_worker_cached` asks it
//! first and uploads only on a miss. The example clears the worker's entry,
//! uploads twice and checks that the second call is a quicker cache hit for
//! the same file, then runs the cached binary as an identity map.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh cat
//! cargo run -p ytsaurus-client --example cached_upload
//! ```
//!
//! Prints the time each call took. The first call must be a real miss, so the
//! example uses a cache of its own that it may clear,
//! `//tmp/ytsaurus_rs_cached_cache`. The shared default
//! (`//tmp/yt_wrapper/file_storage/new_cache`, the Python wrapper's path) is
//! read-only to an ordinary user on a managed installation.
//!
//! `YT_FILE_CACHE` names another cache. If that cache refuses the upload, the
//! client warns and uploads outside it; if it refuses to remove the entry, the
//! first call is a hit. The example stops with an explanation in both cases.

use std::process::ExitCode;
use std::time::Instant;

use serde::Serialize;
use ytsaurus_client::{CachedFile, Client, ClientError, MapSpec};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_cached";

/// The cache this example brings with it.
///
/// Beside `BASE`, not inside it: `run` removes `BASE` whole, which would wipe
/// the cache before the clearing step could exercise `remove`.
const CACHE: &str = "//tmp/ytsaurus_rs_cached_cache";

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
    // `from_env` already applies `YT_FILE_CACHE`; it is read here as well to
    // print which cache the numbers came from.
    let cache = std::env::var("YT_FILE_CACHE")
        .map(|named| named.trim().to_owned())
        .ok()
        .filter(|named| !named.is_empty())
        .unwrap_or_else(|| CACHE.to_owned());
    let client = Client::from_env()?.with_file_cache(&cache);

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
    done(&format!("caching into {cache}"));

    // A cache persists across runs; clearing this entry makes the first call a
    // real upload.
    step("Clearing this binary out of the cache, so the first call is a miss");
    let digest = md5_of(WORKER)?;
    if let Some(cached) = client.file_from_cache(&digest)? {
        // An entry that stays makes the first call a hit, with no cold upload
        // to time.
        if let Err(refused) = client.remove(&cached) {
            return Err(nothing_to_clear(&cache, &cached, &refused));
        }
        done(&format!("removed {cached}"));
    } else {
        done("nothing cached");
    }

    step("First upload");
    let (first, cold) = timed(|| client.upload_worker_cached(WORKER))?;
    describe(&first, cold);
    check("the first call uploaded it", first.uploaded)?;

    // `upload_worker_cached` may succeed without the cache, when the cache
    // refuses the upload. Then there is nothing to demonstrate.
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

    let before = client.read_table(format!("{BASE}/input"))?;
    let after = client.read_table(format!("{BASE}/output"))?;
    check(
        "the identity map reproduced its input",
        before == after && !after.is_empty(),
    )?;

    println!("\nOne upload, any number of launches. Tables left at {BASE}, cache at {cache}");
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
/// The client has already warned on stderr, naming the cache and quoting the
/// cluster's refusal, and uploaded outside the cache, so the second call would
/// miss too.
fn nothing_to_demonstrate(first: &CachedFile) -> ClientError {
    eprintln!("   FAIL nothing went into the cache, so there is no hit to demonstrate");
    eprintln!("        the worker went to {} instead", first.path);
    eprintln!("        every launch will send the whole binary again until that changes");
    eprintln!("        the warning printed above names the cache that refused it and quotes");
    eprintln!("        the cluster; unset YT_FILE_CACHE to let the example use the one it");
    eprintln!("        brings ({CACHE}), or point it at a path you can write");
    eprintln!("        to, and then the example has something to show");
    ClientError::Config(
        "the file cache would not take the worker, so there is no cache hit to demonstrate"
            .to_owned(),
    )
}

/// Why this example stops when the cache holds the worker and will not let go.
///
/// Reached when `YT_FILE_CACHE` names a cache somebody else owns: `remove` is
/// refused (`Access denied … "remove" permission … is not allowed by any
/// matching ACE`, code 901), so the first call would be a hit and the timing
/// would compare two warm uploads.
fn nothing_to_clear(cache: &str, entry: &str, refused: &ClientError) -> ClientError {
    eprintln!("   FAIL {entry} is in the cache and this installation will not remove it");
    eprintln!("        {refused}");
    eprintln!("        so the first upload would be a hit, and there is no cold call to time");
    eprintln!("        unset YT_FILE_CACHE to use the cache this example brings ({CACHE}),");
    eprintln!("        or point it at one you can write to — {cache} is not that");
    ClientError::Config(format!(
        "the worker is already in {cache} and this caller may not clear it, \
         so there is no cold upload to measure"
    ))
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
