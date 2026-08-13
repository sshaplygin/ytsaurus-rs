//! `format_compare` — a Rust worker against a YQL query, on one task.
//!
//! Phases 1 and 2 of `docs/format-comparison.md`. Three legs of its four:
//! **leg 1** twice — the `wordcount` worker on binary YSON, once summing within
//! a row and once summing within the job — and **leg 4**, the same computation
//! as a plain YQL query. The dynamic-YSON control and Skiff are not here yet.
//!
//! It is worth being exact about what this compares, because "YQL versus YSON"
//! is not a comparison that exists: YQL is a query engine and YSON is a wire
//! format. What is measured is *an idiomatic Rust worker, whose job I/O happens
//! to be YSON, against what an engineer who writes no code would run instead* —
//! and any difference decomposes into the runtime, the plan, and the format.
//!
//! The second worker leg exists because the first version of this comparison
//! could not make that decomposition and reported the sum. It found YQL ~1.8×
//! faster on summed job exec time, and the largest single cause was plan
//! shape: YQL's planner combines in the map stage, so 3 750 rows crossed its
//! shuffle where the worker's 3 114 964 did. `map-combine` is the worker doing
//! the same.
//!
//! **What the difference between the two worker legs is not.** It is not
//! "runtime and format, once plan is subtracted". Adversarial review of the
//! first result established that most of every number here is per-job process
//! startup — about 640 ms a job on this cluster, against 150–500 ms of actual
//! wordcount — and that the legs do not even run the same number of jobs: YQL's
//! spec sets `data_weight_per_job` to 1 GiB and gets one map job, the worker
//! takes the controller's default and gets three, while `time/exec` sums over
//! jobs. Charge YQL only what the worker's own reduce cost and its 4650 ms
//! becomes 3396 against 2787 — 1.22×, not 1.67×. On the stage that touches
//! every row the two are within a few per cent per byte.
//!
//! So the honest reading of a run of this harness is: **a job-level combiner
//! is worth about 3× on this workload, and the rest is plan shape and job
//! startup.** Nothing here is evidence about wire formats — the format is the
//! one thing held constant across all three legs.
//!
//! ```sh
//! tests/e2e/run_local_cluster.sh
//! scripts/build-worker.sh wordcount
//! export YT_PROXY=http://localhost:8000
//! cargo run --release -p ytsaurus-client --example format_compare
//! ```
//!
//! `YT_COMPARE_MIB` sets the input size (default 16) and `YT_COMPARE_ROUNDS`
//! how many timed rounds to run (default 5, of which the **fastest** counts —
//! a slow round is interference, a fast one cannot be). One warm-up round is
//! run first and discarded.
//!
//! ## What it will and will not tell you
//!
//! **Correctness before timing.** Both sides run once and their output tables
//! are compared word by word. A benchmark of two computations that disagree is
//! noise, so a disagreement stops the run.
//!
//! **Wall clock, not CPU.** A local cluster reports nothing under
//! `user_job/cpu`, so `time/exec` — which includes process start and the pipe —
//! is what there is. The harness asks for CPU anyway and prints whichever it
//! got, because on a cluster that reports it the same run answers the better
//! question.
//!
//! **Fixed costs dominate here.** `docs/benchmarking.md` §3 measured 2225 ms
//! merely to be handed rows on this kind of cluster against 474 ms on a real
//! one. Under emulation, on one CPU, a local result is a direction, not a
//! throughput claim.
//!
//! **The fairness rules, enforced rather than remembered:** one input table for
//! both sides; the same columns read, which here is automatic since the table
//! has one column; the query must contain an `INSERT`, so YQL pays the full
//! output cost rather than Query Tracker's first 10 000 rows; the query cache
//! is disabled, without which a repeated query completes having spawned no
//! operations at all; and both memory limits are printed — the query is given
//! 640 MB against the worker's 512 MB, a 1.25× asymmetry in the query's favour
//! that exists because 576 MB is where YQL fails on this cluster (`yql_smoke.rs`
//! measured it) and 512 MB is what every other example here gives a worker.
//!
//! **Rules it does not enforce, and should before anything is published:** the
//! two sides run different numbers of jobs (see above), the corpus's vocabulary
//! is capped at 3 750 words and does not grow with the input, which flatters a
//! combiner without bound, and the query tokenises with `Re2` where this
//! space-separated corpus would let it use the cheaper `String::SplitToList` —
//! and that sits in exactly the stage where the per-row work happens.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use ytsaurus_client::{
    Client, ClientError, Column, ColumnType, MapReduceSpec, MapSpec, Method, OperationFilter,
    Repeatable, TableSchema, yson_build,
};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice};

/// Where the comparison keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_compare";

/// Where `scripts/build-worker.sh` leaves the workers.
const WORKER_DIR: &str = "target/x86_64-unknown-linux-musl/release-worker";

/// Pinned so every leg runs the same number of jobs.
///
/// `time/exec` sums over jobs and a job start costs ~640 ms on this cluster, so
/// a leg left on the controller's default is compared on how it was scheduled
/// rather than on what it computed. 1 GiB is what YQL's own spec asks for, and
/// it puts every leg of a 16 MiB task in a single job.
const DATA_WEIGHT_PER_JOB: i64 = 1024 * 1024 * 1024;

/// What the worker is given, and what the query is told to match.
const WORKER_MEMORY: i64 = 512 * 1024 * 1024;

/// Both are load-bearing; `yql_smoke.rs` records how each was measured.
const PRAGMAS: &str = "PRAGMA yt.QueryCacheMode = \"disable\";\n\
                       PRAGMA yt.DefaultMemoryLimit = \"640M\";\n";

/// States a query does not leave.
const TERMINAL: [&str; 3] = ["completed", "failed", "aborted"];

/// How long to wait for one query.
const QUERY_TIMEOUT: Duration = Duration::from_secs(900);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nformat_compare failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    let mib = number("YT_COMPARE_MIB", 16);
    let rounds = number("YT_COMPARE_ROUNDS", 5).max(1);
    let task = std::env::var("YT_COMPARE_TASK").unwrap_or_else(|_| "wordcount".to_owned());

    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;

    let (input, mut legs) = match task.as_str() {
        "project" => prepare_project(&client, mib)?,
        "wordcount" => prepare_wordcount(&client, mib)?,
        other => {
            return Err(ClientError::Config(format!(
                "YT_COMPARE_TASK is \"wordcount\" or \"project\", not {other:?}"
            )));
        }
    };

    // -------------------------------------------------------- correctness

    step("Correctness — the same computation, or nothing to time");
    for leg in &legs {
        let measure = run_leg(&client, &input, leg)?;
        println!("   {:<18} {}", leg.label, measure.describe());
    }

    // Only legs that compute a result are compared, and they are compared
    // against the first of them. The shallow stops of a depth series write
    // nothing on purpose, so there is nothing of theirs to diff.
    let results: Vec<&Leg> = legs.iter().filter(|leg| leg.produces_rows()).collect();
    match results.as_slice() {
        [] => println!("   no leg produces rows; nothing to compare"),
        [only] => {
            let rows = client.row_count(&only.output)?;
            if rows == 0 {
                return Err(ClientError::Config(format!(
                    "{} wrote no rows, so there is nothing to time",
                    only.label
                )));
            }
            println!("   {} wrote {rows} rows", only.label);
        }
        [first, rest @ ..] => {
            let reference = counts(&client, &first.output)?;
            for leg in rest {
                let other = counts(&client, &leg.output)?;
                if let Some(reason) = disagreement(&reference, &other) {
                    return Err(ClientError::Config(format!(
                        "{} disagrees with {}, so there is nothing to time: {reason}",
                        leg.label, first.label
                    )));
                }
            }
            println!(
                "   all {} computing legs agree on {} distinct words",
                results.len(),
                reference.len()
            );
        }
    }

    // ------------------------------------------------------------ timings

    step(&format!(
        "Timing — one warm-up, then {rounds} rounds, fastest counts"
    ));
    println!("   worker memory limit {WORKER_MEMORY} B, query pragma 640M");

    let mut lost = 0;
    for round in 0..=rounds {
        // Interleaved rather than all of one leg then all of the next: whatever
        // the cluster is doing to one side, it is doing to the others at the
        // same time, which is the only defence a single-CPU emulated cluster
        // allows against drift.
        //
        // A round that fails is lost, not fatal. The first long run of this
        // ended on its last round with Query Tracker's own cell losing its
        // peers — the operations had run, only the bookkeeping was gone — and
        // twenty minutes of measurement went with it.
        let measured = legs
            .iter()
            .map(|leg| run_leg(&client, &input, leg))
            .collect::<Result<Vec<_>, _>>();

        let measures = match measured {
            Ok(measures) => measures,
            Err(e) => {
                lost += 1;
                println!("   round {round} lost: {e}");
                continue;
            }
        };

        if round == 0 {
            println!("   warm-up discarded");
            continue;
        }
        for (leg, measure) in legs.iter_mut().zip(measures) {
            println!("   round {round}: {:<18} {}", leg.label, measure.describe());
            leg.runs.push(measure);
        }
    }

    if legs.iter().any(|leg| leg.runs.is_empty()) {
        return Err(ClientError::Config(
            "every round was lost; nothing to report".to_owned(),
        ));
    }

    step("Result");
    if lost > 0 {
        println!(
            "   {lost} round(s) lost to cluster failures; {} counted\n",
            legs[0].runs.len()
        );
    }
    report(&legs);
    println!("\n   Left at {BASE}; remove with: yt remove {BASE} --recursive");
    Ok(())
}

// ------------------------------------------------------------------- tasks

/// Uploads a worker, refusing early if it was not built.
fn upload(client: &Client, name: &str) -> Result<(), ClientError> {
    let local = format!("{WORKER_DIR}/{name}");
    if !std::path::Path::new(&local).exists() {
        return Err(ClientError::Config(format!(
            "{local} is missing; build it with: scripts/build-worker.sh {name}"
        )));
    }
    client.upload_worker(&local, &format!("{BASE}/{name}"))
}

/// `wordcount`: two worker mappers and the query, all summing the same words.
///
/// Kept because it is what found the harness's own defects, but it is not the
/// task this comparison is for: it shuffles, so plan shape gets into every
/// number it produces.
fn prepare_wordcount(client: &Client, mib: usize) -> Result<(String, Vec<Leg>), ClientError> {
    let input = format!("{BASE}/lines");

    step(&format!("Writing about {mib} MiB of text"));
    client.create_table(
        &input,
        &TableSchema::new([Column::new("text", ColumnType::String).required()]),
    )?;
    let lines = corpus(mib);
    let words: usize = lines.iter().map(|line| line.text.split(' ').count()).sum();
    client.write_table_rows(&input, lines.iter())?;
    println!(
        "   {} rows, {words} words, {} distinct",
        client.row_count(&input)?,
        distinct_words(&lines)
    );
    upload(client, "wordcount")?;

    Ok((
        input,
        vec![
            Leg::new(
                "worker, per row",
                Kind::WordCount("map"),
                format!("{BASE}/counts_rust"),
            ),
            Leg::new(
                "worker, combining",
                Kind::WordCount("map-combine"),
                format!("{BASE}/counts_combine"),
            ),
            Leg::new("YQL", Kind::Query, format!("{BASE}/counts_yql")),
        ],
    ))
}

/// `project`: the pilot's map at three depths over one table.
///
/// The task `docs/format-comparison.md` chose, and for the reason the wordcount
/// run demonstrated: **no shuffle**, one output, so nothing about plan shape or
/// combiners can enter the measurement. The three stops differ only in how far
/// into each row they go, so their differences are what framing, decoding and
/// the work itself cost — the subtraction `profile.rs` established, now with a
/// format leg to hang off it.
fn prepare_project(client: &Client, mib: usize) -> Result<(String, Vec<Leg>), ClientError> {
    let input = format!("{BASE}/events");

    step(&format!("Writing about {mib} MiB of access-log events"));
    client.create_table(&input, &events_schema())?;
    let events = events(mib);
    client.write_table_rows(&input, events.iter())?;
    println!(
        "   {} rows, {} MiB on the cluster",
        client.row_count(&input)?,
        client
            .get(&format!("{input}/@data_weight"))?
            .as_i64()
            .unwrap_or(0) as f64
            / (1024.0 * 1024.0)
    );
    upload(client, "sessionize")?;

    Ok((
        input,
        vec![
            Leg::new(
                "frames only",
                Kind::Depth("map-frames"),
                format!("{BASE}/project_frames"),
            ),
            Leg::new(
                "framed + decoded",
                Kind::Depth("map-parse"),
                format!("{BASE}/project_parse"),
            ),
            Leg::new(
                "the whole map",
                Kind::Depth("map-one"),
                format!("{BASE}/project_full"),
            ),
        ],
    ))
}

/// The nine columns `sessionize`'s `RawEvent` expects.
///
/// Byte columns are `String` rather than `Utf8`: real user agents are not
/// valid UTF-8, and a schema that promised they were would be a lie the
/// cluster enforces.
fn events_schema() -> TableSchema {
    TableSchema::new([
        Column::new("user_id", ColumnType::String).required(),
        Column::new("timestamp", ColumnType::Int64).required(),
        Column::new("url", ColumnType::String).required(),
        Column::new("referer", ColumnType::String),
        Column::new("user_agent", ColumnType::String).required(),
        Column::new("status", ColumnType::Int64).required(),
        Column::new("bytes_sent", ColumnType::Uint64).required(),
        Column::new("is_mobile", ColumnType::Boolean).required(),
        Column::new("latency_ms", ColumnType::Double).required(),
    ])
}

/// One access-log event, wide and mixed-typed on purpose.
#[derive(serde::Serialize)]
struct EventRow {
    #[serde(with = "serde_bytes")]
    user_id: Vec<u8>,
    timestamp: i64,
    url: &'static str,
    referer: Option<&'static str>,
    #[serde(with = "serde_bytes")]
    user_agent: &'static [u8],
    status: i64,
    bytes_sent: u64,
    is_mobile: bool,
    latency_ms: f64,
}

/// Deterministic events, the same shape `profile.rs` measures the pilot on.
///
/// About 122 bytes of data weight a row, measured rather than guessed — the
/// first version of this assumed 190 and produced a table two fifths the size
/// asked for. The actual weight is still read back off the cluster and printed,
/// because an estimate that drifts is exactly how a benchmark ends up
/// comparing two different amounts of work.
fn events(mib: usize) -> Vec<EventRow> {
    /// Not valid UTF-8, as real user agents frequently are not.
    const AGENT: &[u8] = b"Mozilla/5.0 (\xff\xfe compatible) Gecko/20100101";
    const URLS: [&str; 4] = [
        "/index.html",
        "/search?q=ytsaurus&page=2",
        "/api/v1/items/48291",
        "/static/app.4f2c1d.js",
    ];

    let count = (mib * 1024 * 1024 / 122) as u64;
    (0..count)
        .map(|n| EventRow {
            user_id: format!("user-{:06}", n % 5_000).into_bytes(),
            timestamp: 1_767_225_600_000_000 + (n as i64) * 1_000_000,
            url: URLS[(n % 4) as usize],
            referer: if n % 3 == 0 {
                None
            } else {
                Some("https://example.com/from")
            },
            user_agent: AGENT,
            status: if n % 17 == 0 { 500 } else { 200 },
            bytes_sent: 1_024 + n % 100_000,
            is_mobile: n % 2 == 0,
            latency_ms: 12.5 + (n % 400) as f64 / 10.0,
        })
        .collect()
}

// ----------------------------------------------------------------- the legs

/// What one side of one round cost.
struct Measure {
    /// Wall clock as the launcher sees it, including waiting for the scheduler.
    wall: Duration,
    /// The cluster's own `time/exec`, summed over every operation this side ran.
    ///
    /// Per-job **wall** time including process start, summed over jobs — so a
    /// leg that splits its work into more jobs pays its startup once per job
    /// in this number while getting the wall-clock benefit of the parallelism.
    /// On this cluster a job's fixed cost is ~640 ms, which is most of what
    /// this metric contains for a 16 MiB task. Read it with `stages` beside it,
    /// never alone.
    exec_ms: Option<i64>,
    /// `time/prepare`, which `time/exec` does not include.
    prepare_ms: Option<i64>,
    /// `time/total` — the closest thing here to what a job really cost.
    total_ms: Option<i64>,
    /// `user_job/cpu/user`, where the cluster reports it. A local one does not.
    cpu_ms: Option<i64>,
    /// How many cluster operations this side took.
    operations: usize,
    input_bytes: Option<i64>,
    output_bytes: Option<i64>,
    /// Bytes across the job's input pipes — the encoded stream itself.
    pipe_in_bytes: Option<i64>,
    /// Bytes across the job's output pipes.
    pipe_out_bytes: Option<i64>,
    /// The same numbers split by job type.
    ///
    /// The reason this is collected at all: the first run of this comparison
    /// reported that YQL was 1.8× faster and left it there, which was true and
    /// misleading. The split showed the whole gap sitting in one place — the
    /// worker moved 3.1 M rows through the shuffle where the query moved 3 750,
    /// because YQL's planner put a combiner in the map stage and the worker
    /// aggregates within a row only. A total alone cannot say that, and a
    /// number nobody can decompose is a number nobody should quote.
    stages: Vec<Stage>,
}

/// What a leg does with the input.
enum Kind {
    /// `wordcount` as a map-reduce, with this mapper mode.
    WordCount(&'static str),
    /// `sessionize` as a map, with this command — one stop of the depth series.
    Depth(&'static str),
    /// The same computation as a query.
    Query,
}

/// One way of computing the answer, and what it cost in each round.
struct Leg {
    label: &'static str,
    kind: Kind,
    /// Where this leg writes, so the outputs can be compared where they exist.
    output: String,
    runs: Vec<Measure>,
}

impl Leg {
    fn new(label: &'static str, kind: Kind, output: String) -> Self {
        Self {
            label,
            kind,
            output,
            runs: Vec::new(),
        }
    }

    /// Whether this leg's output table is a result worth diffing.
    ///
    /// The shallow stops of the depth series write nothing by construction —
    /// that is what makes them shallow — so a diff against them would compare
    /// a computation with the absence of one.
    fn produces_rows(&self) -> bool {
        !matches!(self.kind, Kind::Depth(command) if command != "map-one")
    }
}

fn run_leg(client: &Client, input: &str, leg: &Leg) -> Result<Measure, ClientError> {
    match leg.kind {
        Kind::WordCount(mapper) => run_worker(client, input, &leg.output, mapper),
        Kind::Depth(command) => run_map(client, input, &leg.output, command),
        Kind::Query => run_query(client, &wordcount_query(input, &leg.output)),
    }
}

/// One stop of the depth series: `sessionize` as a plain map.
///
/// No shuffle and one output, so the three stops differ only by how deep into
/// the row each goes. `data_weight_per_job` is pinned so every leg runs the
/// same number of jobs: `time/exec` sums over jobs, and on this cluster a job
/// start is ~640 ms, so a leg that split differently would be compared on how
/// it was scheduled rather than on what it computed.
fn run_map(
    client: &Client,
    input: &str,
    output: &str,
    command: &str,
) -> Result<Measure, ClientError> {
    client.remove_tree(output)?;
    client.create("table", output)?;

    let spec = MapSpec::new(format!("./sessionize {command}"), [input], [output])
        .with_local_file(format!("{BASE}/sessionize"))
        .with_memory_limit(WORKER_MEMORY)
        .with_raw("data_weight_per_job", yson_build::int(DATA_WEIGHT_PER_JOB));

    let started = Instant::now();
    let id = client.start_map(&spec)?;
    client.wait_for_operation(&id)?;
    let wall = started.elapsed();

    Ok(measure(client, wall, &[id]))
}

/// One job type's share of one side.
struct Stage {
    job_type: String,
    jobs: i64,
    exec_ms: i64,
    input_rows: i64,
    input_bytes: i64,
}

impl Measure {
    fn describe(&self) -> String {
        let exec = self
            .exec_ms
            .map_or_else(|| "no time/exec".to_owned(), |ms| format!("{ms} ms exec"));
        let cpu = self
            .cpu_ms
            .map_or_else(String::new, |ms| format!(", {ms} ms cpu"));
        format!(
            "{:.1}s wall, {exec}{cpu}, {} operation(s)",
            self.wall.as_secs_f64(),
            self.operations
        )
    }
}

/// Leg 1: the `wordcount` worker, binary YSON in and out.
///
/// `mapper` selects which of the worker's two mappers runs — `map`, which sums
/// within a row, or `map-combine`, which sums within the job. They are the same
/// computation and produce the same output table; what differs is how much goes
/// through the shuffle, which is the whole of what the first version of this
/// comparison actually measured.
fn run_worker(
    client: &Client,
    input: &str,
    output: &str,
    mapper: &str,
) -> Result<Measure, ClientError> {
    client.remove_tree(output)?;
    client.create("table", output)?;

    let spec = MapReduceSpec::new("./wordcount reduce", [input], [output], ["word"])
        .with_mapper(format!("./wordcount {mapper}"))
        .with_local_file(format!("{BASE}/wordcount"))
        .with_memory_limit(WORKER_MEMORY);

    let started = Instant::now();
    let id = client.start_map_reduce(&spec)?;
    client.wait_for_operation(&id)?;
    let wall = started.elapsed();

    Ok(measure(client, wall, &[id]))
}

/// Leg 4: the same computation as a query.
///
/// The worker splits on any byte that is not an ASCII letter, digit or
/// apostrophe; `Re2::FindAndConsume` of the complementary class is the same
/// tokenisation written the other way round, and the corpus is ASCII so the two
/// cannot diverge on encoding. `CAST(… AS Int64)` because `COUNT(*)` is a
/// `Uint64` and the worker's column is an `int64`.
///
/// **The capture group is load-bearing.** `FindAndConsume` returns the capture
/// groups, not the whole match, so `"[A-Za-z0-9']+"` without the parentheses
/// yields an empty list per row: the query completes, writes nothing, and the
/// timing would be of a query that did no work. The correctness check catches
/// it — it did — which is why that check comes before any timing.
fn wordcount_query(input: &str, output: &str) -> String {
    format!(
        "{PRAGMAS}\
         $tokens = Re2::FindAndConsume(\"([A-Za-z0-9']+)\");\n\
         INSERT INTO `{output}` WITH TRUNCATE\n\
         SELECT word, CAST(COUNT(*) AS Int64) AS count\n\
         FROM (SELECT $tokens(text) AS words FROM `{input}`)\n\
         FLATTEN LIST BY words AS word\n\
         GROUP BY word;"
    )
}

fn run_query(client: &Client, query: &str) -> Result<Measure, ClientError> {
    assert!(
        query.contains("INSERT INTO"),
        "a query without an INSERT would be capped at Query Tracker's result \
         rows and would under-measure the output cost"
    );
    assert!(
        query.contains("QueryCacheMode = \"disable\""),
        "without the cache disabled a repeated query spawns no operations at \
         all, and the timing would be of a cache hit"
    );

    let params = yson_build::map([
        ("engine", yson_build::string("yql")),
        ("query", yson_build::string(query)),
    ]);

    let started = Instant::now();
    let body = client.raw_command(Method::Post, "start_query", &params, None)?;
    let id = field(&decode(&body, "start_query")?, "query_id")
        .as_ref()
        .and_then(text_of)
        .ok_or_else(|| ClientError::Decode {
            command: "start_query".to_owned(),
            reason: "no query_id in the answer".to_owned(),
        })?;

    let deadline = Instant::now() + QUERY_TIMEOUT;
    loop {
        let body = client.raw_command_with(
            Method::Get,
            "get_query",
            &yson_build::map([("query_id", yson_build::string(&id))]),
            None,
            Repeatable::Freely,
            None,
        )?;
        let answer = decode(&body, "get_query")?;
        let state = field(&answer, "state")
            .as_ref()
            .and_then(text_of)
            .unwrap_or_default();

        if TERMINAL.contains(&state.as_str()) {
            let wall = started.elapsed();
            if state != "completed" {
                let cause = field(&answer, "error")
                    .map(|e| error_messages(&e))
                    .and_then(|messages| messages.last().cloned())
                    .unwrap_or_else(|| "no message".to_owned());
                return Err(ClientError::Config(format!("the query {state}: {cause}")));
            }
            // YQL's operations are titled `YQL operation (<query id> by <user>)`
            // and the cluster's own filter matches that, so the modelled
            // command finds them. Summed, because one query is several
            // operations and the count is itself a number worth having.
            let operations = client.list_operations(&OperationFilter::new().with_substring(&id))?;
            let ids: Vec<String> = operations.operations.into_iter().map(|o| o.id).collect();
            return Ok(measure(client, wall, &ids));
        }

        if Instant::now() >= deadline {
            return Err(ClientError::Config(format!(
                "the query was still {state} after {}s",
                QUERY_TIMEOUT.as_secs()
            )));
        }
        sleep(Duration::from_millis(250));
    }
}

/// Everything the scheduler will say about a side's operations, summed.
fn measure(client: &Client, wall: Duration, ids: &[String]) -> Measure {
    // `None` when any operation's statistics could not be read, rather than the
    // sum of the ones that could: a leg is one or two operations, and a total
    // silently missing one of them is a plausible-looking undercount — the
    // worst kind of wrong number in a comparison.
    let total = |path: &str| {
        let mut sum = None;
        for id in ids {
            match client.job_statistic_sum(id, path) {
                Ok(Some(value)) => sum = Some(sum.unwrap_or(0) + value),
                Ok(None) => {}
                Err(_) => return None,
            }
        }
        sum
    };

    // Output volume is indexed by output table — `data/output/0/data_weight`,
    // not `data/output/data_weight`. Asking for the flat path returns `None`
    // for every leg, which this harness printed as a blank column for two full
    // measurement runs before anyone looked in the tree.
    let per_table = |path: &str, leaf: &str| {
        let mut sum = None;
        for id in ids {
            let Ok(statistics) = client.job_statistics(id) else {
                return None;
            };
            // Descend component by component: `field_ref` takes one key, and
            // asking it for "data/output" looks up a key spelled with a slash,
            // which no cluster has. That mistake is what kept this column blank
            // after the first attempt at fixing it.
            let mut subtree = &statistics;
            let mut found = true;
            for component in path.split('/') {
                match field_ref(subtree, component) {
                    Some(next) => subtree = next,
                    None => {
                        found = false;
                        break;
                    }
                }
            }
            if !found {
                continue;
            }
            let YsonNode::Map(indices) = &subtree.node else {
                continue;
            };
            for (name, index) in indices {
                // `user_job/pipes/output` carries a `total` beside its numeric
                // descriptors — the cluster's own aggregate — and adding it to
                // the descriptors it aggregates doubles the answer. This
                // harness published 171.4 MiB and a 4.1× ratio that way; both
                // were exactly twice the truth, and the shape of the error made
                // them look like a finding about the format.
                if name.as_slice() == b"total" {
                    continue;
                }
                for (_, (value, _)) in by_job_type_of(index, leaf) {
                    sum = Some(sum.unwrap_or(0) + value);
                }
            }
        }
        sum
    };

    let mut stages: BTreeMap<String, Stage> = BTreeMap::new();
    for id in ids {
        let Ok(statistics) = client.job_statistics(id) else {
            continue;
        };
        for (path, values) in [
            ("time/exec", by_job_type(&statistics, "time/exec")),
            (
                "data/input/row_count",
                by_job_type(&statistics, "data/input/row_count"),
            ),
            (
                "data/input/data_weight",
                by_job_type(&statistics, "data/input/data_weight"),
            ),
        ] {
            for (job_type, (sum, count)) in values {
                let stage = stages.entry(job_type.clone()).or_insert_with(|| Stage {
                    job_type,
                    jobs: 0,
                    exec_ms: 0,
                    input_rows: 0,
                    input_bytes: 0,
                });
                match path {
                    "time/exec" => {
                        stage.exec_ms += sum;
                        // Every leaf carries the same job count; taking it from
                        // one of them keeps it from being counted three times.
                        stage.jobs = count;
                    }
                    "data/input/row_count" => stage.input_rows += sum,
                    _ => stage.input_bytes += sum,
                }
            }
        }
    }

    Measure {
        wall,
        exec_ms: total("time/exec"),
        // Not in `time/exec` and worth 656–752 ms per job on this cluster,
        // which is the same order as the exec time of a whole stage here.
        prepare_ms: total("time/prepare"),
        total_ms: total("time/total"),
        cpu_ms: total("user_job/cpu/user"),
        operations: ids.len(),
        input_bytes: total("data/input/data_weight"),
        output_bytes: per_table("data/output", "data_weight"),
        // The bytes that actually crossed the job's pipes: the encoded stream,
        // which is the one number in here that a wire format decides. It is
        // what legs 2 and 3 will be compared on.
        pipe_in_bytes: total("user_job/pipes/input/bytes"),
        pipe_out_bytes: per_table("user_job/pipes/output", "bytes"),
        stages: stages.into_values().collect(),
    }
}

/// One built-in statistic, split by job type: `(sum, job count)` apiece.
///
/// `Client::job_statistic_sum` adds the job types together, which is the right
/// default and the wrong thing here — the whole question is which stage the
/// time went to. The tree nests by path component and then keys the aggregate
/// under `$$` (built-in) or `$` (custom), a state, and the job type.
fn by_job_type(statistics: &YsonValue, path: &str) -> BTreeMap<String, (i64, i64)> {
    let mut node = statistics;
    for component in path.split('/') {
        match field_ref(node, component) {
            Some(next) => node = next,
            None => return BTreeMap::new(),
        }
    }
    by_job_type_of(node, "")
}

/// As [`by_job_type`], from a subtree that is already at the right place.
///
/// `leaf` is one more path component to descend first, or `""` to stay put —
/// which is what reading `data/output/<index>/data_weight` needs, since the
/// index is discovered rather than named.
fn by_job_type_of(node: &YsonValue, leaf: &str) -> BTreeMap<String, (i64, i64)> {
    let node = if leaf.is_empty() {
        node
    } else {
        match field_ref(node, leaf) {
            Some(next) => next,
            None => return BTreeMap::new(),
        }
    };

    let Some(separated) = field_ref(node, "$$").or_else(|| field_ref(node, "$")) else {
        return BTreeMap::new();
    };
    let Some(completed) = field_ref(separated, "completed") else {
        return BTreeMap::new();
    };
    let YsonNode::Map(job_types) = &completed.node else {
        return BTreeMap::new();
    };

    job_types
        .iter()
        .filter_map(|(job_type, aggregate)| {
            let sum = field_ref(aggregate, "sum").and_then(YsonValue::as_i64)?;
            let count = field_ref(aggregate, "count")
                .and_then(YsonValue::as_i64)
                .unwrap_or(0);
            Some((String::from_utf8_lossy(job_type).into_owned(), (sum, count)))
        })
        .collect()
}

// ------------------------------------------------------------------ reporting

fn report(legs: &[Leg]) {
    row("", legs.iter().map(|leg| leg.label.to_owned()));

    let wall = |m: &Measure| Some(m.wall.as_millis() as i64);
    row(
        "wall, fastest",
        legs.iter().map(|leg| ms(fastest(&leg.runs, wall))),
    );
    row("wall, vs first", ratios(legs, wall));
    row(
        "wall, spread",
        legs.iter().map(|leg| spread(&leg.runs, wall)),
    );

    let exec = |m: &Measure| m.exec_ms;
    row(
        "time/exec, fastest",
        legs.iter().map(|leg| ms(fastest(&leg.runs, exec))),
    );
    row("time/exec, vs first", ratios(legs, exec));
    row(
        "time/exec, spread",
        legs.iter().map(|leg| spread(&leg.runs, exec)),
    );

    let cpu = |m: &Measure| m.cpu_ms;
    if legs.iter().any(|leg| fastest(&leg.runs, cpu).is_some()) {
        row(
            "job cpu, fastest",
            legs.iter().map(|leg| ms(fastest(&leg.runs, cpu))),
        );
        row("job cpu, vs first", ratios(legs, cpu));
    } else {
        println!("   job cpu               this cluster reports nothing under user_job/cpu");
    }

    row(
        "operations",
        legs.iter()
            .map(|leg| leg.runs.first().map_or(0, |m| m.operations).to_string()),
    );
    row(
        "bytes read",
        legs.iter()
            .map(|leg| bytes(leg.runs.first().and_then(|m| m.input_bytes))),
    );
    row(
        "bytes written",
        legs.iter()
            .map(|leg| bytes(leg.runs.first().and_then(|m| m.output_bytes))),
    );
    row(
        "pipe bytes in",
        legs.iter()
            .map(|leg| bytes(leg.runs.first().and_then(|m| m.pipe_in_bytes))),
    );
    row(
        "pipe bytes out",
        legs.iter()
            .map(|leg| bytes(leg.runs.first().and_then(|m| m.pipe_out_bytes))),
    );

    let total_time = |m: &Measure| m.total_ms;
    row(
        "time/total, fastest",
        legs.iter().map(|leg| ms(fastest(&leg.runs, total_time))),
    );
    row("time/total, vs first", ratios(legs, total_time));
    // Printed because it is not inside `time/exec` and is the same order of
    // magnitude as it: a comparison that quotes exec alone is quoting about
    // half of what the jobs cost.
    row(
        "time/prepare, extra",
        legs.iter()
            .map(|leg| ms(fastest(&leg.runs, |m| m.prepare_ms))),
    );

    // Where the time went, which is the part that decides whether the totals
    // above mean what they look like.
    for leg in legs {
        stage_table(leg.label, &leg.runs);
    }

    subtraction(legs);

    // The guard, per metric rather than once: on this cluster the rounds
    // scatter, and a difference smaller than the scatter is not a difference.
    // Applying it to the wall clock alone — which an earlier version did —
    // prints "no measurable difference" beside an exec column where the gap is
    // ten times the noise.
    guard(legs, "wall", wall);
    guard(legs, "time/exec", exec);
}

/// What each layer of the depth series cost, by subtraction.
///
/// Paired by round, not by minimum. The legs are interleaved precisely so that
/// round `i` of each stop met the same cluster, and subtracting one leg's
/// fastest round from another's throws that away — the two minima can come from
/// different rounds, and the difference then carries both rounds' noise with no
/// way to see it. The first version of this did exactly that and printed
/// "15.1 %" from minima taken 4 seconds apart; paired by round the same five
/// rounds say 13 % with a spread of 203–317 ms, which is the honest precision.
///
/// The method is `profile.rs`'s and so is the refusal: if the stops do not come
/// out in order, no share is reported. A decode share read off numbers that
/// came out backwards is not a small error, it is the whole quantity.
fn subtraction(legs: &[Leg]) {
    let [frames, parse, full] = legs else { return };
    if frames.label != "frames only" {
        return;
    }

    let rounds = frames.runs.len().min(parse.runs.len()).min(full.runs.len());
    let mut decode = Vec::new();
    let mut work = Vec::new();
    let mut whole = Vec::new();
    let mut handed = Vec::new();
    for i in 0..rounds {
        let (Some(f), Some(p), Some(w)) = (
            frames.runs[i].exec_ms,
            parse.runs[i].exec_ms,
            full.runs[i].exec_ms,
        ) else {
            continue;
        };
        if f > p || p > w {
            println!(
                "\n   Round {} came out {f}, {p}, {w} ms — out of order, so it is dropped.",
                i + 1
            );
            continue;
        }
        handed.push(f);
        decode.push(p - f);
        work.push(w - p);
        whole.push(w);
    }

    if decode.is_empty() {
        println!(
            "\n   No round separated the stops. No decode share is reported — that is the\n   \
             refusal working, not a missing number."
        );
        return;
    }

    let mean = |values: &[i64]| values.iter().sum::<i64>() / values.len() as i64;
    let range = |values: &[i64]| {
        let (min, max) = (
            values.iter().min().copied().unwrap_or(0),
            values.iter().max().copied().unwrap_or(0),
        );
        format!("{min}–{max}")
    };
    let total = mean(&whole);
    let share = |part: i64| format!("{:.0} %", 100.0 * part as f64 / total as f64);

    println!(
        "\n   By subtraction, paired by round ({} of {} rounds usable):",
        decode.len(),
        rounds
    );
    for (name, values) in [
        ("being handed the rows", &handed),
        ("decoding them", &decode),
        ("validating and writing", &work),
        ("the whole map", &whole),
    ] {
        println!(
            "     {name:<24} {:>6} ms   {:>5}   (rounds {} ms)",
            mean(values),
            share(mean(values)),
            range(values)
        );
    }

    println!(
        "\n   Read the first bucket before the second: it is not framing, it is framing\n   \
         plus process start plus waiting for the first batch, and on this cluster that\n   \
         fixed part is several hundred milliseconds of any job. The decode share is a\n   \
         share of a denominator that large, measured in per-job wall time — the 30 %\n   \
         threshold in docs/benchmarking.md is stated over job CPU, which this cluster\n   \
         does not report at all."
    );
}

fn row(label: &str, cells: impl IntoIterator<Item = String>) {
    let mut line = format!("   {label:<21}");
    for cell in cells {
        line.push_str(&format!("{cell:>18}"));
    }
    println!("{line}");
}

/// Each leg against the first, which is the one the repository already ships.
fn ratios(legs: &[Leg], of: impl Fn(&Measure) -> Option<i64> + Copy) -> Vec<String> {
    let base = fastest(&legs[0].runs, of);
    legs.iter()
        .map(|leg| match (base, fastest(&leg.runs, of)) {
            (Some(base), Some(value)) if base > 0 && value > 0 => {
                format!("{:.2}x", value as f64 / base as f64)
            }
            _ => String::new(),
        })
        .collect()
}

/// Says so when a gap is smaller than the noise it was measured against.
///
/// Every pair, not just each leg against the first: the number a reader quotes
/// is whichever two legs interest them, and a guard that vets only one column
/// leaves the interesting comparison unvetted. That is not hypothetical — the
/// headline this harness produced ("1.67x") was a pair the harness itself
/// never compared and never guarded.
fn guard(legs: &[Leg], metric: &str, of: impl Fn(&Measure) -> Option<i64> + Copy) {
    let noise = legs
        .iter()
        .map(|leg| scatter(&leg.runs, of))
        .max()
        .unwrap_or(0);

    for (index, left) in legs.iter().enumerate() {
        for right in &legs[index + 1..] {
            let (Some(l), Some(r)) = (fastest(&left.runs, of), fastest(&right.runs, of)) else {
                continue;
            };
            let gap = (l - r).abs();
            if gap < noise {
                println!(
                    "\n   {metric}: {} against {} differ by {gap} ms, which is less than the\n   \
                     scatter within one leg ({noise} ms). No measurable difference, not a winner.",
                    left.label, right.label
                );
            }
        }
    }
}

fn fastest(runs: &[Measure], of: impl Fn(&Measure) -> Option<i64>) -> Option<i64> {
    runs.iter().filter_map(of).min()
}

/// Where one leg's time and rows went, from its fastest round.
///
/// Rows matter as much as milliseconds here: a stage reading far more rows
/// than the input table holds is reading a shuffle, and that is the number
/// that says whether a difference in the totals is about the code or about
/// the plan.
fn stage_table(label: &str, runs: &[Measure]) {
    let Some(best) = runs
        .iter()
        .filter(|m| m.exec_ms.is_some())
        .min_by_key(|m| m.exec_ms.unwrap_or(i64::MAX))
    else {
        return;
    };
    if best.stages.is_empty() {
        return;
    }

    println!("\n   {label}, fastest round, by job type:");
    for stage in &best.stages {
        println!(
            "     {:<20} {:>7} ms  {:>12} rows  {:>9.1} MiB  ({} job(s))",
            stage.job_type,
            stage.exec_ms,
            stage.input_rows,
            stage.input_bytes as f64 / (1024.0 * 1024.0),
            stage.jobs
        );
    }
}

fn scatter(runs: &[Measure], of: impl Fn(&Measure) -> Option<i64> + Copy) -> i64 {
    let values: Vec<i64> = runs.iter().filter_map(of).collect();
    match (values.iter().min(), values.iter().max()) {
        (Some(min), Some(max)) => max - min,
        _ => 0,
    }
}

fn spread(runs: &[Measure], of: impl Fn(&Measure) -> Option<i64> + Copy) -> String {
    let values: Vec<i64> = runs.iter().filter_map(of).collect();
    match (values.iter().min(), values.iter().max()) {
        (Some(min), Some(max)) => format!("{min}-{max} ms"),
        _ => "-".to_owned(),
    }
}

fn ms(value: Option<i64>) -> String {
    value.map_or_else(|| "-".to_owned(), |ms| format!("{ms} ms"))
}

fn bytes(value: Option<i64>) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |b| format!("{:.1} MiB", b as f64 / (1024.0 * 1024.0)),
    )
}

// -------------------------------------------------------------------- corpus

#[derive(serde::Serialize)]
struct Line {
    text: String,
}

#[derive(serde::Deserialize)]
struct Total {
    word: String,
    count: i64,
}

/// Deterministic ASCII text, skewed so the word counts are not uniform.
///
/// Deterministic because a benchmark whose input changes between runs is
/// comparing two things at once, and ASCII because the two tokenisers only
/// have to agree on bytes they both understand.
fn corpus(mib: usize) -> Vec<Line> {
    const VOCABULARY: usize = 5000;
    let target = mib * 1024 * 1024;
    let mut lines = Vec::new();
    let mut written = 0usize;
    let mut seed = 0x5eed_1234_u64;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as usize
    };

    while written < target {
        let mut text = String::with_capacity(96);
        for word in 0..12 {
            if word > 0 {
                text.push(' ');
            }
            // Squaring the draw skews it: a few words are common, most are
            // rare, which is what a real corpus looks like and what makes the
            // shuffle do something.
            let draw = next() % VOCABULARY;
            let index = (draw * draw) / VOCABULARY;
            text.push('w');
            text.push_str(&index.to_string());
        }
        written += text.len() + 1;
        lines.push(Line { text });
    }
    lines
}

fn distinct_words(lines: &[Line]) -> usize {
    let mut seen = std::collections::BTreeSet::new();
    for line in lines {
        for word in line.text.split(' ') {
            seen.insert(word);
        }
    }
    seen.len()
}

fn counts(client: &Client, path: &str) -> Result<BTreeMap<String, i64>, ClientError> {
    Ok(client
        .read_table_rows::<Total>(path)?
        .into_iter()
        .map(|row| (row.word, row.count))
        .collect())
}

/// The first way the two tables differ, or `None` if they do not.
fn disagreement(left: &BTreeMap<String, i64>, right: &BTreeMap<String, i64>) -> Option<String> {
    if left.len() != right.len() {
        return Some(format!(
            "{} distinct words against {}",
            left.len(),
            right.len()
        ));
    }
    for (word, count) in left {
        match right.get(word) {
            None => return Some(format!("{word:?} is missing from the query's output")),
            Some(other) if other != count => {
                return Some(format!("{word:?}: worker {count}, query {other}"));
            }
            Some(_) => {}
        }
    }
    None
}

// ------------------------------------------------------------------ plumbing

fn error_messages(error: &YsonValue) -> Vec<String> {
    let mut found = Vec::new();
    collect_messages(error, &mut found);
    found
}

fn collect_messages(value: &YsonValue, found: &mut Vec<String>) {
    match &value.node {
        YsonNode::Map(entries) => {
            if let Some(message) = entries.get(b"message".as_slice()).and_then(text_of)
                && !message.is_empty()
            {
                found.push(message);
            }
            for (key, child) in entries {
                if key.as_slice() != b"attributes" {
                    collect_messages(child, found);
                }
            }
        }
        YsonNode::List(items) => {
            for child in items {
                collect_messages(child, found);
            }
        }
        _ => {}
    }
}

fn decode(body: &[u8], command: &str) -> Result<YsonValue, ClientError> {
    from_slice(body, YsonFormat::Text).map_err(|e| ClientError::Decode {
        command: command.to_owned(),
        reason: format!("{e}; body was {}", String::from_utf8_lossy(body)),
    })
}

fn field(value: &YsonValue, key: &str) -> Option<YsonValue> {
    field_ref(value, key).cloned()
}

fn field_ref<'value>(value: &'value YsonValue, key: &str) -> Option<&'value YsonValue> {
    match &value.node {
        YsonNode::Map(entries) => entries.get(key.as_bytes()),
        _ => None,
    }
}

fn text_of(value: &YsonValue) -> Option<String> {
    match &value.node {
        YsonNode::String(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

fn number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn step(what: &str) {
    println!("\n== {what}");
}

#[cfg(test)]
mod tests {
    use super::{corpus, disagreement};
    use std::collections::BTreeMap;

    #[test]
    fn corpus_is_deterministic_and_roughly_the_size_asked_for() {
        let first = corpus(1);
        let second = corpus(1);
        let bytes: usize = first.iter().map(|line| line.text.len() + 1).sum();

        assert_eq!(first.len(), second.len());
        assert_eq!(first[0].text, second[0].text);
        assert!(
            (1024 * 1024..1024 * 1024 + 200).contains(&bytes),
            "generated {bytes} bytes for 1 MiB"
        );
    }

    #[test]
    fn disagreement_names_the_first_difference() {
        let left = BTreeMap::from([("a".to_owned(), 2), ("b".to_owned(), 1)]);
        let same = left.clone();
        let fewer = BTreeMap::from([("a".to_owned(), 2)]);
        let wrong = BTreeMap::from([("a".to_owned(), 2), ("b".to_owned(), 9)]);

        assert!(disagreement(&left, &same).is_none());
        assert_eq!(
            disagreement(&left, &fewer).as_deref(),
            Some("2 distinct words against 1")
        );
        assert_eq!(
            disagreement(&left, &wrong).as_deref(),
            Some("\"b\": worker 1, query 9")
        );
    }
}
