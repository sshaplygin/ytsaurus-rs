//! `format_compare` — YSON, Skiff and a YQL query, on one task.
//!
//! Phases 1 and 2 of `docs/format-comparison.md`, and **two tasks** selected by
//! `YT_COMPARE_TASK`:
//!
//! - `project` (what the comparison is for) — eight legs over one nine-column
//!   table: the pilot's map at three depths on binary YSON through a typed
//!   serde struct, two of those depths again through `YsonValue`, two on Skiff,
//!   and the same computation as a YQL query. No shuffle and one output table,
//!   so plan shape cannot enter the numbers.
//! - `wordcount` (the default, and the one that found the harness's own
//!   defects) — the `wordcount` worker on binary YSON twice, once summing
//!   within a row and once within the job, against the query. It shuffles, so
//!   plan shape gets into everything it produces; the rest of these module docs
//!   is about that task and is kept because the lesson is what chose the other
//!   one.
//!
//! It is worth being exact about what this compares, because "YQL versus YSON"
//! is not a comparison that exists: YQL is a query engine and YSON is a wire
//! format. What is measured is *an idiomatic Rust worker, whose job I/O happens
//! to be YSON, against what an engineer who writes no code would run instead* —
//! and any difference decomposes into the runtime, the plan, and the format.
//!
//! On `wordcount`, the second worker leg exists because the first version of
//! this comparison could not make that decomposition and reported the sum. It
//! found YQL ~1.8× faster on summed job exec time, and the largest single cause
//! was plan shape: YQL's planner combines in the map stage, so 3 750 rows
//! crossed its shuffle where the worker's 3 114 964 did. `map-combine` is the
//! worker doing the same.
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
//! So the honest reading of a `wordcount` run is: **a job-level combiner is
//! worth about 3× on this workload, and the rest is plan shape and job
//! startup.** Nothing in that task is evidence about wire formats — the format
//! is the one thing held constant across its three legs. The `project` task is
//! where the formats differ; `docs/benchmarking.md` §5 is what its runs said.
//!
//! ```sh
//! tests/e2e/run_local_cluster.sh
//! export YT_PROXY=http://localhost:8000
//!
//! scripts/build-worker.sh wordcount
//! cargo run --release -p ytsaurus-client --example format_compare
//!
//! scripts/build-worker.sh sessionize
//! YT_COMPARE_TASK=project YT_COMPARE_MIB=48 YT_COMPARE_ROUNDS=9 \
//!     cargo run --release -p ytsaurus-client --example format_compare
//! ```
//!
//! `YT_COMPARE_MIB` sets the input size (default 16) and `YT_COMPARE_ROUNDS`
//! how many timed rounds to run (default 5). One warm-up round is run first and
//! discarded.
//!
//! The **fastest** round is what the absolute columns report — a slow round is
//! interference, a fast one cannot be. Ratios are not read off those minima:
//! two legs' fastest rounds can fall minutes apart and carry different weather,
//! so `paired_ratios` reports the ratio each round gave and calls a pair
//! inseparable when the sign flips between rounds. Read that block, not the
//! `vs first` columns.
//!
//! ## What it will and will not tell you
//!
//! **Correctness before timing.** Every leg that produces rows runs once and is
//! diffed against the first of them before any clock is read. A benchmark of
//! two computations that disagree is noise, so a disagreement stops the run.
//! What "the same answer" means is the task's: `wordcount` compares the
//! word-to-count map, and `project` compares the rows as a **multiset** —
//! canonical binary-YSON encodings, sorted — so a leg may order its output
//! differently but cannot hide a missing, extra or duplicated row.
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
//! every leg; every leg reads every column, which `wordcount` gets for free
//! from a one-column table and `project` gets by asking each leg for all nine —
//! so the query's usual projection advantage is off the table; the query must
//! contain an `INSERT`, so YQL pays the full output cost rather than Query
//! Tracker's first 10 000 rows; the query cache is disabled, without which a
//! repeated query completes having spawned no operations at all; the rounds are
//! interleaved, so whatever the cluster is doing to one leg it is doing to the
//! others at the same time; and both memory limits are printed — the query is
//! given 640 MB against the worker's 512 MB, a 1.25× asymmetry in the query's
//! favour that exists because 576 MB is where YQL fails on this cluster
//! (`yql_smoke.rs` measured it) and 512 MB is what every other example here
//! gives a worker.
//!
//! On `project`, `data_weight_per_job` is pinned as well, so no leg is compared
//! on how it was scheduled: `time/exec` sums over jobs and a job start is ~640
//! ms here.
//!
//! **Rules it does not enforce, and should before anything is published.** On
//! `wordcount`: the two sides run different numbers of jobs (see above), the
//! corpus's vocabulary is capped at 3 750 words and does not grow with the
//! input, which flatters a combiner without bound, and the query tokenises with
//! `Re2` where this space-separated corpus would let it use the cheaper
//! `String::SplitToList` — and that sits in exactly the stage where the per-row
//! work happens. On `project`: the Skiff legs are a *dynamic* API against a
//! *typed* YSON one, because Skiff has no typed rows yet, so no ratio between
//! them is a format ratio; `docs/benchmarking.md` §5 states how much of each
//! measured gap turned out to be the representation rather than the format.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use ytsaurus_client::{
    Client, ClientError, Column, ColumnType, DataFormat, MapReduceSpec, MapSpec, Method,
    OperationFilter, Repeatable, SkiffFormat, SkiffSchema, SkiffSchemaRef, SkiffWireType,
    TableSchema, error_summary, yson_build,
};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice, to_vec};

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
            // What "the same answer" means is the task's business: wordcount
            // produces a word-to-count map whose row order is not meaningful,
            // and the depth series produces the input's own rows in the input's
            // own order. Reading one as the other is how this check spent a run
            // failing on a missing `word` column.
            let rows = task == "project";
            let reference = answer(&client, &first.output, rows)?;
            for leg in rest {
                let other = answer(&client, &leg.output, rows)?;
                if let Some(reason) = reference.disagreement(&other) {
                    return Err(ClientError::Config(format!(
                        "{} disagrees with {}, so there is nothing to time: {reason}",
                        leg.label, first.label
                    )));
                }
            }
            println!(
                "   all {} computing legs agree, {}",
                results.len(),
                reference.describe()
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
            Leg::new(
                "YQL",
                Kind::Query(wordcount_query),
                format!("{BASE}/counts_yql"),
            ),
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
                "typed: frames",
                Kind::Depth("map-frames"),
                format!("{BASE}/project_frames"),
            ),
            Leg::new(
                "typed: decoded",
                Kind::Depth("map-parse"),
                format!("{BASE}/project_parse"),
            ),
            Leg::new(
                "typed: full",
                Kind::Depth("map-one"),
                format!("{BASE}/project_full"),
            ),
            Leg::new(
                "dynamic: decoded",
                Kind::Depth("map-parse-dynamic"),
                format!("{BASE}/project_parse_dyn"),
            ),
            Leg::new(
                "dynamic: full",
                Kind::Depth("map-one-dynamic"),
                format!("{BASE}/project_full_dyn"),
            ),
            Leg::new(
                "skiff: decoded",
                Kind::Skiff("map-parse-skiff"),
                format!("{BASE}/project_parse_skiff"),
            ),
            Leg::new(
                "skiff: full",
                Kind::Skiff("map-one-skiff"),
                format!("{BASE}/project_full_skiff"),
            ),
            Leg::new(
                "YQL",
                Kind::Query(project_query),
                format!("{BASE}/project_yql"),
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
    /// The same, with the operation's job I/O set to Skiff both ways.
    Skiff(&'static str),
    /// The same computation as a query, built by this function.
    Query(fn(&str, &str) -> String),
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
        match self.kind {
            Kind::Depth(command) => command == "map-one" || command == "map-one-dynamic",
            Kind::Skiff(command) => command == "map-one-skiff",
            _ => true,
        }
    }
}

fn run_leg(client: &Client, input: &str, leg: &Leg) -> Result<Measure, ClientError> {
    match leg.kind {
        Kind::WordCount(mapper) => run_worker(client, input, &leg.output, mapper),
        Kind::Depth(command) => run_map(client, input, &leg.output, command, false),
        Kind::Skiff(command) => run_map(client, input, &leg.output, command, true),
        Kind::Query(build) => run_query(client, &build(input, &leg.output)),
    }
}

/// The input table's nine columns as Skiff, spelled here a second time.
///
/// The worker declares the same schema in
/// `crates/ytsaurus-job/examples/sessionize.rs`, and that duplication is the
/// point: two independent spellings have to agree with each other, where one
/// shared constant would agree with itself however wrong it was. It is the same
/// reason `skiff_cat` and its test each write out their own, and the same
/// reason `run_e2e.sh` reads the tables back with somebody else's client.
///
/// Positional: a column is wherever the schema puts it. Swap two of the same
/// width here and the job reads one column as another in silence, which is what
/// this leg is measuring the price of.
fn skiff_input() -> DataFormat {
    DataFormat::skiff(
        SkiffFormat::new(vec![SkiffSchemaRef::Inline(SkiffSchema::tuple([
            SkiffSchema::named("user_id", SkiffWireType::String32),
            SkiffSchema::named("timestamp", SkiffWireType::Int64),
            SkiffSchema::named("url", SkiffWireType::String32),
            SkiffSchema::named("referer", SkiffWireType::String32).optional(),
            SkiffSchema::named("user_agent", SkiffWireType::String32),
            SkiffSchema::named("status", SkiffWireType::Int64),
            SkiffSchema::named("bytes_sent", SkiffWireType::Uint64),
            SkiffSchema::named("is_mobile", SkiffWireType::Boolean),
            SkiffSchema::named("latency_ms", SkiffWireType::Double),
        ]))])
        .expect("the input schema is a valid Skiff format"),
    )
}

/// What the mapper writes: the input's nine less `referer`, plus `is_external`.
fn skiff_output() -> DataFormat {
    DataFormat::skiff(
        SkiffFormat::new(vec![SkiffSchemaRef::Inline(SkiffSchema::tuple([
            SkiffSchema::named("user_id", SkiffWireType::String32),
            SkiffSchema::named("timestamp", SkiffWireType::Int64),
            SkiffSchema::named("url", SkiffWireType::String32),
            SkiffSchema::named("user_agent", SkiffWireType::String32),
            SkiffSchema::named("status", SkiffWireType::Int64),
            SkiffSchema::named("bytes_sent", SkiffWireType::Uint64),
            SkiffSchema::named("is_mobile", SkiffWireType::Boolean),
            SkiffSchema::named("latency_ms", SkiffWireType::Double),
            SkiffSchema::named("is_external", SkiffWireType::Boolean),
        ]))])
        .expect("the output schema is a valid Skiff format"),
    )
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
    skiff: bool,
) -> Result<Measure, ClientError> {
    client.remove_tree(output)?;
    client.create("table", output)?;

    let mut spec = MapSpec::new(format!("./sessionize {command}"), [input], [output])
        .with_local_file(format!("{BASE}/sessionize"))
        .with_memory_limit(WORKER_MEMORY)
        .with_raw("data_weight_per_job", yson_build::int(DATA_WEIGHT_PER_JOB));

    if skiff {
        spec = spec.with_formats(skiff_input(), skiff_output());
    }

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

/// Leg 4 for the depth-series task: the same map, as a query.
///
/// Sharper than the wordcount mirror was, and for one reason: there is no
/// grouping here, so the planner has nothing to shuffle and the query runs as
/// **one** map operation — the same shape as every worker leg. In wordcount it
/// needed two, and 42 % of its time went to the second.
///
/// The five rules are `sessionize`'s `validate`, and `is_external` is its
/// referer test. On this input nothing is rejected, so no leg pays for the
/// reject branch — which is a property of the fixture, not of the engines.
fn project_query(input: &str, output: &str) -> String {
    format!(
        "{PRAGMAS}INSERT INTO `{output}` WITH TRUNCATE\n\
         SELECT user_id, `timestamp`, url, user_agent, status, bytes_sent,\n\
         \x20      is_mobile, latency_ms,\n\
         \x20      IF(referer IS NULL, false,\n\
         \x20         referer != \"\" AND NOT StartsWith(referer, \"/\")) AS is_external\n\
         FROM `{input}`\n\
         WHERE user_id != \"\" AND `timestamp` > 0\n\
         \x20 AND status >= 100 AND status <= 599\n\
         \x20 AND latency_ms >= 0.0 AND url != \"\";"
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
                // The crate's own flattening, not a third copy of it. This
                // example carried one that kept the innermost cause and threw
                // the category away — `Memory limit exceeded` with no clue
                // which stage exceeded it — which is the same half-a-message
                // mistake, from the other end, that made `error_summary`
                // public in the first place.
                let cause = field(&answer, "error")
                    .as_ref()
                    .and_then(error_summary)
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
                        // Added, not assigned, and only under this one path.
                        // The three leaves of a single operation each carry the
                        // same job count, so summing all three would treble it —
                        // but a leg is one or two *operations*, and assigning
                        // reported the last operation's count beside an
                        // `exec_ms` summed over both. Routing it through the
                        // one arm gives each operation exactly one vote, which
                        // is what the other fields already get.
                        stage.jobs += count;
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

    paired_ratios(legs);
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
    let [frames, parse, full, ..] = legs else {
        return;
    };
    if frames.label != "typed: frames" {
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
        ("typed: full", &whole),
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

/// Every pair of legs, as the ratio each round gave rather than the ratio of
/// the minima.
///
/// This is the estimator the rig was built for and the one two runs proved
/// necessary. The absolute numbers on this cluster scatter by hundreds of
/// milliseconds — one leg's fastest round and another's can fall minutes apart
/// and carry different weather — but the two legs of a single round met the
/// same cluster, so their ratio is stable even when neither number is. A run
/// whose minima said "no pair is separable" gave ratios that held their sign in
/// all nine rounds.
///
/// A pair whose sign flips between rounds is reported as not separable, which
/// is the honest reading of a difference the noise can turn around.
fn paired_ratios(legs: &[Leg]) {
    println!("\n   Paired by round, on time/exec — the ratio each round gave:");

    let mut unstable = 0usize;
    for (index, left) in legs.iter().enumerate() {
        for right in &legs[index + 1..] {
            let rounds = left.runs.len().min(right.runs.len());
            let mut ratios: Vec<f64> = Vec::new();
            for round in 0..rounds {
                if let (Some(l), Some(r)) = (left.runs[round].exec_ms, right.runs[round].exec_ms)
                    && l > 0
                {
                    ratios.push(r as f64 / l as f64);
                }
            }
            if ratios.is_empty() {
                continue;
            }

            let faster_left = ratios.iter().all(|ratio| *ratio > 1.0);
            let faster_right = ratios.iter().all(|ratio| *ratio < 1.0);
            if !faster_left && !faster_right {
                unstable += 1;
                continue;
            }

            // Report it in the direction that reads as "how much slower":
            // the ratios are right-over-left, so when the right leg is the
            // quicker one every ratio is inverted, ends included.
            let mut sorted = ratios.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaN ratios"));
            if faster_right {
                sorted = sorted.iter().rev().map(|ratio| 1.0 / ratio).collect();
            }

            let (quicker, slower) = if faster_left {
                (left.label, right.label)
            } else {
                (right.label, left.label)
            };
            println!(
                "     {slower:<17} is {:.2}× {quicker:<17} ({:.2}–{:.2}, all {} rounds)",
                sorted[sorted.len() / 2],
                sorted[0],
                sorted[sorted.len() - 1],
                sorted.len()
            );
        }
    }

    if unstable > 0 {
        println!("     {unstable} pair(s) changed sign between rounds and are not separable.");
    }
}

fn row(label: &str, cells: impl IntoIterator<Item = String>) {
    let mut line = format!("   {label:<20}");
    for cell in cells {
        line.push_str(&format!("{cell:>17}"));
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

    let mut muddy = Vec::new();
    let mut pairs = 0usize;
    for (index, left) in legs.iter().enumerate() {
        for right in &legs[index + 1..] {
            let (Some(l), Some(r)) = (fastest(&left.runs, of), fastest(&right.runs, of)) else {
                continue;
            };
            pairs += 1;
            if (l - r).abs() < noise {
                muddy.push((left.label, right.label, (l - r).abs()));
            }
        }
    }

    if muddy.is_empty() {
        return;
    }

    // When a metric cannot separate any pair, saying so once is the finding;
    // saying it n² times buries every other line of the report. The wall clock
    // on this cluster does exactly that — it is quantised by the launcher's own
    // poll loop, so every leg reads the same and the scatter swamps the lot.
    if muddy.len() == pairs {
        println!(
            "\n   {metric}: no pair of legs differs by more than the scatter within one leg\n   \
             ({noise} ms). This metric separates nothing here; read another."
        );
        return;
    }

    for (left, right, gap) in muddy {
        println!(
            "\n   {metric}: {left} against {right} differ by {gap} ms, which is less than\n   \
             the scatter within one leg ({noise} ms). No measurable difference, not a winner."
        );
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

/// What a leg computed, in whichever shape the task's answer has.
enum Answer {
    /// wordcount: a word-to-count map, where row order carries nothing.
    Counts(BTreeMap<String, i64>),
    /// The depth series: canonical row encodings, sorted so table order carries
    /// nothing while duplicate rows still count.
    Rows(Vec<Vec<u8>>),
}

impl Answer {
    fn describe(&self) -> String {
        match self {
            Self::Counts(counts) => format!("{} distinct words", counts.len()),
            Self::Rows(rows) => format!("{} rows", rows.len()),
        }
    }

    /// The first way two answers differ, or `None` if they do not.
    fn disagreement(&self, other: &Self) -> Option<String> {
        match (self, other) {
            (Self::Counts(left), Self::Counts(right)) => disagreement(left, right),
            (Self::Rows(left), Self::Rows(right)) => {
                if left.len() != right.len() {
                    return Some(format!("{} rows against {}", left.len(), right.len()));
                }
                left.iter()
                    .zip(right)
                    .position(|(a, b)| a != b)
                    .map(|index| format!("normalized row {index} differs"))
            }
            _ => Some("the two answers are not even the same shape".to_owned()),
        }
    }
}

fn answer(client: &Client, path: &str, rows: bool) -> Result<Answer, ClientError> {
    if rows {
        Ok(Answer::Rows(canonical_rows(
            client.read_table_rows::<YsonValue>(path)?,
        )?))
    } else {
        Ok(Answer::Counts(counts(client, path)?))
    }
}

/// Turns rows into a comparison key independent of a table's physical order.
///
/// `YsonValue` keeps map keys in a `BTreeMap`, so binary YSON gives every row
/// a canonical representation. Sorting those representations compares the two
/// outputs as multisets: a query may reorder rows, but it cannot hide a
/// missing, extra, or duplicated one.
fn canonical_rows(rows: Vec<YsonValue>) -> Result<Vec<Vec<u8>>, ClientError> {
    let mut canonical = rows
        .into_iter()
        .map(|row| {
            to_vec(&row, YsonFormat::Binary).map_err(|e| ClientError::Decode {
                command: "read_table".to_owned(),
                reason: format!("could not canonicalize a comparison row: {e}"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    canonical.sort_unstable();
    Ok(canonical)
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
    use super::{Answer, canonical_rows, corpus, disagreement};
    use std::collections::BTreeMap;
    use ytsaurus_yson::{YsonNode, YsonValue};

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

    #[test]
    fn row_answers_ignore_table_order_but_preserve_duplicate_counts() {
        let row = |value| YsonValue {
            attributes: None,
            node: YsonNode::Int64(value),
        };
        let answer = |rows| Answer::Rows(canonical_rows(rows).expect("rows serialize"));

        let expected = answer(vec![row(1), row(2), row(2)]);
        let reordered = answer(vec![row(2), row(1), row(2)]);
        let different_multiplicity = answer(vec![row(1), row(1), row(2)]);

        assert!(expected.disagreement(&reordered).is_none());
        assert_eq!(
            expected.disagreement(&different_multiplicity).as_deref(),
            Some("normalized row 1 differs")
        );
    }
}
