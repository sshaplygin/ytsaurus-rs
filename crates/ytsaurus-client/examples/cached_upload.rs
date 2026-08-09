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
//! The demonstration needs a cache it may **clear** — the first call has to be
//! a real miss — so it brings one of its own rather than using the
//! installation's. The shared default (`//tmp/yt_wrapper/file_storage/new_cache`,
//! the path the Python wrapper uses) is read-only for an ordinary user on a
//! managed installation, and the example's setup step died there with
//! `Access denied for user "…": "remove" permission … is not allowed by any
//! matching ACE` — a refusal about the example's own housekeeping, not about
//! anything the client cannot do.
//!
//! `YT_FILE_CACHE` points it back at a shared cache, and then one of two things
//! happens, both of which the example stops on rather than failing three checks
//! later where the cause has scrolled off: the cache refuses the *upload*, and
//! the client warns, uploads outside the cache and carries on — a launch that
//! works with nothing to demonstrate — or it refuses the *clearing*, and the
//! first call is bound to be a hit, so there is no cold upload to time.

use std::process::ExitCode;
use std::time::Instant;

use serde::Serialize;
use ytsaurus_client::{CachedFile, Client, ClientError, MapSpec};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_cached";

/// The cache this example brings with it.
///
/// It has to *clear* an entry to make the first upload a real miss, and a
/// shared cache is the one thing an ordinary user will never be allowed to
/// remove from. Bringing its own makes the demonstration self-contained.
///
/// **Beside `BASE` and not inside it.** `run` starts by removing `BASE` whole,
/// so a cache under there would be wiped before the clearing step could find
/// anything in it — the step would print "nothing cached" for ever and the
/// `remove` it exists to exercise would never run. A cache is a thing that
/// survives runs; that is the entire feature.
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
    // `YT_FILE_CACHE` is somebody asking for a particular cache on purpose, and
    // `from_env` has already applied it; naming it here as well is what lets
    // the example print which cache the numbers below came from.
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

    // A cache is persistent, so a previous run would leave nothing to miss on.
    // Clearing this one entry makes the first call a real upload every time —
    // and this is the step a shared cache refuses, which is why the example
    // brings its own unless told otherwise.
    step("Clearing this binary out of the cache, so the first call is a miss");
    let digest = md5_of(WORKER)?;
    if let Some(cached) = client.file_from_cache(&digest)? {
        // A managed cache is read-only to an ordinary user, `remove` included,
        // and this is the one thing the client cannot degrade around: an entry
        // that stays makes the first call a hit, so there is no cold upload to
        // time and every check below would be comparing two warm ones.
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
/// Reached only when `YT_FILE_CACHE` points at a cache somebody else owns: the
/// entry is there, `remove` is refused — `Access denied … "remove" permission …
/// is not allowed by any matching ACE`, code 901 — and so the first call is
/// bound to be a cache hit. Everything after this measures a warm upload against
/// a warm one, and the "and was quicker" check then passes or fails on which of
/// two identical operations the cluster felt like doing faster. Stopping here
/// says what happened; carrying on would report a coin toss.
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
