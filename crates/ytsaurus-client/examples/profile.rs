//! `profile` — what a job spends on decoding, measured by the cluster.
//!
//! The Skiff question is whether YSON decoding is a large enough share of job
//! cost to be worth a second wire format. `cargo bench -p ytsaurus-job` answers
//! it for a job that does nothing else, which is the worst case for YSON and
//! not a workload. This asks the same question of the pilot, on a cluster.
//!
//! The method is subtraction. The same mapper runs three times over one table,
//! stopped at three depths:
//!
//! | mode | does |
//! | --- | --- |
//! | `map-frames` | finds record boundaries, decodes nothing |
//! | `map-parse` | decodes each row into the mapper's own struct |
//! | `map` | the pilot: validates, routes, writes two tables |
//!
//! The scheduler's own `time/exec` for each is what they cost, and the
//! differences are what each phase costs.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! scripts/build-worker.sh sessionize
//! cargo run --release -p ytsaurus-client --example profile
//! ```
//!
//! `YT_PROFILE_MIB` sets the table size (default 48) and `YT_PROFILE_ROUNDS`
//! how many times to run each mode (default 3, of which the **fastest** counts
//! — a slow round is interference, a fast one cannot be).

use std::io::Read;
use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError, MapSpec, yson_build};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_profile";

/// The worker, as produced by `scripts/build-worker.sh sessionize`.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/sessionize";

/// The three depths, in the order they are reported.
const MODES: [(&str, &str); 3] = [
    ("frames", "map-frames"),
    ("parse", "map-parse"),
    ("full", "map"),
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nprofile failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    if !std::path::Path::new(WORKER).exists() {
        eprintln!("worker not found at {WORKER}");
        eprintln!("build it first:  scripts/build-worker.sh sessionize");
        return Err(ClientError::Config(
            "the worker binary has not been built".to_owned(),
        ));
    }

    let mib = number("YT_PROFILE_MIB", 48);
    let rounds = number("YT_PROFILE_ROUNDS", 3).max(1);
    let input = format!("{BASE}/events");

    step(&format!("Writing about {mib} MiB of access-log events"));
    client.remove(BASE)?;
    client.create("table", &input)?;
    client.create("table", &format!("{BASE}/events_out"))?;
    client.create("table", &format!("{BASE}/rejects"))?;
    let events = Events::of_at_least(mib * 1024 * 1024);
    let rows = events.rows_to_come();
    client.write_table_streaming(&input, events)?;
    done(&format!(
        "{rows} rows, {} MiB on the cluster",
        client
            .get(&format!("{input}/@uncompressed_data_size"))?
            .as_i64()
            .unwrap_or(0)
            / (1024 * 1024)
    ));

    let worker = client.upload_worker_cached(WORKER)?;

    let mut measured = Vec::new();
    for (label, argument) in MODES {
        step(&format!("Running `sessionize {argument}` {rounds}×"));
        let mut best = i64::MAX;

        for round in 1..=rounds {
            let spec = MapSpec::new(
                format!("./sessionize {argument}"),
                [input.clone()],
                [format!("{BASE}/events_out"), format!("{BASE}/rejects")],
            )
            .with_local_file_named(&worker.path, &worker.name)
            .with_memory_limit(2 * 1024 * 1024 * 1024)
            // One job, so `time/exec` is one number about one process rather
            // than a sum over however many the scheduler felt like starting.
            .with_raw("job_count", yson_build::int(1))
            .with_raw("max_failed_job_count", yson_build::int(1));

            let id = client.start_map(&spec)?;
            client.wait_for_operation(&id)?;

            match client.job_statistic_sum(&id, "time/exec")? {
                Some(ms) => {
                    println!("   round {round}: {ms} ms");
                    best = best.min(ms);
                }
                None => {
                    return Err(ClientError::Config(
                        "the cluster reported no time/exec for a completed job, so there \
                         is nothing to measure with here"
                            .to_owned(),
                    ));
                }
            }
        }

        done(&format!("{label}: {best} ms"));
        measured.push((label, best));
    }

    step("What each phase cost");
    let frames = measured[0].1;
    let parse = measured[1].1;
    let full = measured[2].1;

    if full <= 0 {
        println!("   the cluster reported {full} ms of exec time for the pilot's map,");
        println!("   so there is nothing here to divide into phases.");
        return Ok(());
    }

    // Three separate cluster runs, each the best of `rounds`. Scheduler noise
    // can still make a shallower mode measure slower than a deeper one, and a
    // phase cannot cost less than nothing: a difference that came out negative
    // means the modes were not separable on this run, not that the phase was
    // free. Clamped so the table stays readable, and flagged so the conclusion
    // below does not pretend the numbers held.
    let separable = frames <= parse && parse <= full;
    let decoding = (parse - frames).max(0);
    let writing = (full - parse).max(0);

    let share = |part: i64| 100.0 * part as f64 / full as f64;
    println!(
        "   being handed the rows      {frames:>6} ms   {:>5.1}%",
        share(frames)
    );
    println!(
        "   decoding them              {decoding:>6} ms   {:>5.1}%",
        share(decoding)
    );
    println!(
        "   validating and writing     {writing:>6} ms   {:>5.1}%",
        share(writing)
    );
    println!("   ————————————————————————————————————————");
    println!("   the pilot's map            {full:>6} ms   100.0%");

    // The threshold docs/benchmarking.md sets for taking Skiff seriously.
    let decoding_share = share(decoding);
    println!();
    if !separable {
        println!(
            "A shallower mode measured slower than a deeper one ({frames}, {parse}, {full} ms), \
             so this run cannot split the phases apart: the differences are inside the noise. \
             Raise YT_PROFILE_ROUNDS or YT_PROFILE_MIB and run it again — there is no answer \
             to the Skiff question in these numbers."
        );
    } else if decoding_share > 30.0 {
        println!(
            "Decoding is {decoding_share:.1}% of this job — above the 30% the Skiff \
             question turns on."
        );
    } else {
        println!(
            "Decoding is {decoding_share:.1}% of this job — below the 30% the Skiff \
             question turns on."
        );
    }
    println!(
        "One local cluster, x86-64 under emulation on this machine. A number to \
         start an argument with, not to end one: docs/benchmarking.md says what \
         settles it."
    );
    Ok(())
}

fn number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Access-log events, encoded on demand.
///
/// The same shape the pilot's `RawEvent` expects, including the byte columns
/// that are not text — measuring decoding on rows narrower or tidier than the
/// job's real ones would measure the wrong thing.
struct Events {
    remaining: u64,
    total: u64,
    buffer: Vec<u8>,
    position: usize,
}

impl Events {
    fn of_at_least(bytes: u64) -> Self {
        let per_row = encode(0).len() as u64;
        let count = bytes.div_ceil(per_row);
        Self {
            remaining: count,
            total: count,
            buffer: Vec::with_capacity(128 * 1024),
            position: 0,
        }
    }

    fn rows_to_come(&self) -> u64 {
        self.total
    }
}

impl Read for Events {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.position == self.buffer.len() {
            if self.remaining == 0 {
                return Ok(0);
            }
            self.buffer.clear();
            self.position = 0;
            while self.buffer.len() < 64 * 1024 && self.remaining > 0 {
                self.buffer
                    .extend_from_slice(&encode(self.total - self.remaining));
                self.remaining -= 1;
            }
        }

        let n = out.len().min(self.buffer.len() - self.position);
        out[..n].copy_from_slice(&self.buffer[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}

/// One event, as binary YSON.
fn encode(n: u64) -> Vec<u8> {
    use serde::Serialize;
    use ytsaurus_yson::{YsonFormat, to_vec};

    #[derive(Serialize)]
    struct Event<'a> {
        #[serde(with = "serde_bytes")]
        user_id: &'a [u8],
        timestamp: i64,
        url: &'a str,
        referer: Option<&'a str>,
        #[serde(with = "serde_bytes")]
        user_agent: &'a [u8],
        status: i64,
        bytes_sent: u64,
        is_mobile: bool,
        latency_ms: f64,
    }

    // A user agent that is not valid UTF-8, as real ones frequently are not.
    const AGENT: &[u8] = b"Mozilla/5.0 (\xff\xfe compatible) Gecko/20100101";
    const URLS: [&str; 4] = [
        "/index.html",
        "/search?q=ytsaurus&page=2",
        "/api/v1/items/48291",
        "/static/app.4f2c1d.js",
    ];

    let user = format!("user-{:06}", n % 5_000);
    let event = Event {
        user_id: user.as_bytes(),
        timestamp: 1_767_225_600_000_000 + (n as i64) * 1_000_000,
        url: URLS[(n % 4) as usize],
        referer: if n.is_multiple_of(3) {
            None
        } else {
            Some("https://example.com/from")
        },
        user_agent: AGENT,
        status: if n.is_multiple_of(17) { 500 } else { 200 },
        bytes_sent: 1_024 + n % 100_000,
        is_mobile: n.is_multiple_of(2),
        latency_ms: 12.5 + (n % 400) as f64 / 10.0,
    };

    let mut bytes = to_vec(&event, YsonFormat::Binary).expect("encodes");
    bytes.push(b';');
    bytes
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn done(what: &str) {
    println!("   ok {what}");
}
