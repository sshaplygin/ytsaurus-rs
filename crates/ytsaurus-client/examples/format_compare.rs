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
//! could not make that decomposition and reported the sum. It found YQL 1.84×
//! faster, and the whole of that gap turned out to be one thing: YQL's planner
//! combines in the map stage, so 3 750 rows crossed its shuffle where the
//! worker's 3 114 964 did. `map-combine` is the worker doing the same, and the
//! difference between the two worker legs is the price of that plan — leaving
//! whatever remains against YQL as the part that might be about the runtime.
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
//! operations at all; and both memory limits are printed, because YQL's default
//! (545 MB) and the worker's (512 MB) are close enough that neither side is
//! being flattered — see `yql_smoke.rs` for how that was measured.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use ytsaurus_client::{
    Client, ClientError, Column, ColumnType, MapReduceSpec, Method, OperationFilter, Repeatable,
    TableSchema, yson_build,
};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice};

/// Where the comparison keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_compare";

/// The worker, as `scripts/build-worker.sh wordcount` leaves it.
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/wordcount";

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

    if !std::path::Path::new(WORKER).exists() {
        return Err(ClientError::Config(format!(
            "{WORKER} is missing; build it with: scripts/build-worker.sh wordcount"
        )));
    }

    let mib = number("YT_COMPARE_MIB", 16);
    let rounds = number("YT_COMPARE_ROUNDS", 5).max(1);

    let input = format!("{BASE}/lines");
    let rust_out = format!("{BASE}/counts_rust");
    let combine_out = format!("{BASE}/counts_combine");
    let yql_out = format!("{BASE}/counts_yql");

    step(&format!("Writing about {mib} MiB of text"));
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
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
    client.upload_worker(WORKER, &format!("{BASE}/wordcount"))?;

    // -------------------------------------------------------- correctness

    step("Correctness — the same computation, or nothing to time");
    let mut legs = vec![
        Leg::worker("worker, per row", "map", rust_out.clone()),
        Leg::worker("worker, combining", "map-combine", combine_out.clone()),
        Leg::query("YQL", yql_out.clone()),
    ];
    for leg in &legs {
        let measure = run_leg(&client, &input, leg)?;
        println!("   {:<18} {}", leg.label, measure.describe());
    }

    // Everything is compared against the first leg, which is the one the
    // repository already ships and the one every other number is relative to.
    let reference = counts(&client, &legs[0].output)?;
    for leg in &legs[1..] {
        let other = counts(&client, &leg.output)?;
        if let Some(reason) = disagreement(&reference, &other) {
            return Err(ClientError::Config(format!(
                "{} disagrees with {}, so there is nothing to time: {reason}",
                leg.label, legs[0].label
            )));
        }
    }
    println!(
        "   all {} legs agree on {} distinct words",
        legs.len(),
        reference.len()
    );

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

// ----------------------------------------------------------------- the legs

/// What one side of one round cost.
struct Measure {
    /// Wall clock as the launcher sees it, including waiting for the scheduler.
    wall: Duration,
    /// The cluster's own `time/exec`, summed over every operation this side ran.
    exec_ms: Option<i64>,
    /// `user_job/cpu/user`, where the cluster reports it. A local one does not.
    cpu_ms: Option<i64>,
    /// How many cluster operations this side took.
    operations: usize,
    input_bytes: Option<i64>,
    output_bytes: Option<i64>,
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

/// One way of computing the answer, and what it cost in each round.
struct Leg {
    label: &'static str,
    /// Which of the worker's mappers to run, or `None` for the query.
    mapper: Option<&'static str>,
    /// Where this leg writes, so the outputs can be diffed against each other.
    output: String,
    runs: Vec<Measure>,
}

impl Leg {
    fn worker(label: &'static str, mapper: &'static str, output: String) -> Self {
        Self {
            label,
            mapper: Some(mapper),
            output,
            runs: Vec::new(),
        }
    }

    fn query(label: &'static str, output: String) -> Self {
        Self {
            label,
            mapper: None,
            output,
            runs: Vec::new(),
        }
    }
}

fn run_leg(client: &Client, input: &str, leg: &Leg) -> Result<Measure, ClientError> {
    match leg.mapper {
        Some(mapper) => run_worker(client, input, &leg.output, mapper),
        None => run_query(client, &wordcount_query(input, &leg.output)),
    }
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
    let total = |path: &str| {
        let mut sum = None;
        for id in ids {
            if let Ok(Some(value)) = client.job_statistic_sum(id, path) {
                sum = Some(sum.unwrap_or(0) + value);
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
        cpu_ms: total("user_job/cpu/user"),
        operations: ids.len(),
        input_bytes: total("data/input/data_weight"),
        output_bytes: total("data/output/data_weight"),
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

    // Where the time went, which is the part that decides whether the totals
    // above mean what they look like.
    for leg in legs {
        stage_table(leg.label, &leg.runs);
    }

    // The guard, per metric rather than once: on this cluster the rounds
    // scatter, and a difference smaller than the scatter is not a difference.
    // Applying it to the wall clock alone — which an earlier version did —
    // prints "no measurable difference" beside an exec column where the gap is
    // ten times the noise.
    guard(legs, "wall", wall);
    guard(legs, "time/exec", exec);
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
fn guard(legs: &[Leg], metric: &str, of: impl Fn(&Measure) -> Option<i64> + Copy) {
    let Some(base) = fastest(&legs[0].runs, of) else {
        return;
    };
    let noise = legs
        .iter()
        .map(|leg| scatter(&leg.runs, of))
        .max()
        .unwrap_or(0);

    for leg in &legs[1..] {
        let Some(value) = fastest(&leg.runs, of) else {
            continue;
        };
        let gap = (base - value).abs();
        if gap < noise {
            println!(
                "\n   {metric}: {} against {} differ by {gap} ms, which is less than the \n   \
                 scatter within one leg ({noise} ms). No measurable difference, not a winner.",
                legs[0].label, leg.label
            );
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
