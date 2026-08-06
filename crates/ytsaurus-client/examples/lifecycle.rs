//! `lifecycle` — the half of an operation's life that is not starting it.
//!
//! An operation used to be a string and four commands here: start it, ask its
//! state, wait, abort. Everything a supervised pipeline does *between* those —
//! pause it while the cluster is busy, give it more of the pool because it
//! turned out to matter, finish it early and keep what it made, find it again
//! after the launcher restarted — had no method to call. This runs all of it
//! against a cluster, and checks the answers rather than printing them.
//!
//! The second half runs two of the four operation types the enum could not name
//! until now: a sorted `merge` and an `erase`. `remote_copy` needs a second
//! cluster and is not exercised here; `join_reduce` has no spec builder, for the
//! reason [`OperationType::JoinReduce`](ytsaurus_client::OperationType) gives.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example lifecycle
//! ```

use std::process::ExitCode;
use std::time::{Duration, Instant};

use ytsaurus_client::{
    Client, ClientError, EraseSpec, MergeMode, MergeSpec, OperationFilter, OperationParameters,
    TableRow, VanillaSpec, VanillaTask, yson_build,
};

/// Where the tables of the second half live.
const BASE: &str = "//tmp/ytsaurus_rs_lifecycle";

/// How long the sleeping operation would run if nothing stopped it.
const SLEEP_SECONDS: u32 = 900;

/// How long to wait for a state before calling it a failure.
const PATIENCE: Duration = Duration::from_secs(120);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nlifecycle failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    // Unique per run: an alias belongs to at most one operation the scheduler
    // still holds, so two runs at once would collide over a fixed one.
    let alias = format!("*rs-lifecycle-{}", std::process::id());

    step(&format!("Starting something long, as {alias}"));
    let mut sleeper = Sleeper::start(&client, &alias)?;
    let id = sleeper.id.clone();
    println!("   operation {id}");
    wait_for_state(&client, &id, "running", PATIENCE)?;
    done("it is running");

    step("Attaching to it, the way a restarted supervisor would");
    // Nothing is sent: an id and a client is all a handle is. This is the
    // reattach door — C++'s `AttachOperation`, Go's `Track(id)` — and the
    // reason the id is the thing worth persisting.
    let op = client.attach_operation(&id);
    check("the handle carries the id it was given", op.id() == id)?;
    check(
        "and answers about the operation the id names",
        op.state()? == "running",
    )?;

    step("Finding it by the alias its spec gave it");
    // Until now an alias could be *set* through `with_raw` and then never
    // looked up: `get_operation` takes `operation_alias`, and refuses it
    // without `include_runtime`, which nothing here sent.
    let found = client.get_operation_by_alias(&alias, &["id", "state"])?;
    check(
        "the alias resolves to this operation",
        text_field(&found, "id").as_deref() == Some(id.as_str()),
    )?;

    step("Pausing it");
    let paused = Instant::now();
    op.suspend(false)?;
    done(&format!(
        "the scheduler took the request in {:.0} ms",
        paused.elapsed().as_secs_f64() * 1000.0
    ));
    check("and reports it suspended", op.suspended()?)?;
    // The one that catches people out. Suspension is not a state: a paused
    // operation still says `running`, so a loop watching the state alone waits
    // out a pause without ever saying why.
    check(
        "while its state is still `running` — suspension is not a state",
        op.state()? == "running",
    )?;

    step("Pausing it again");
    // Unlike abort and complete, this one forgives a repeat, which is why it
    // is the only mutating command here that the client retries.
    op.suspend(false)?;
    check("is accepted: suspend is idempotent", op.suspended()?)?;

    step("Letting it run again");
    op.resume()?;
    check("it is no longer suspended", !op.suspended()?)?;

    step("Resuming one that is not suspended");
    // The asymmetry worth knowing about before writing a retry: resume is not
    // idempotent, which is why the client sends it once and never repeats it.
    match op.resume() {
        Ok(()) => return fail("an operation that was not suspended accepted a resume"),
        Err(e) => {
            check(
                "is refused, because there is nothing to resume",
                e.to_string().contains("running"),
            )?;
            println!("   {}", first_line(&e.to_string()));
        }
    }

    step("Giving it more of the pool while it runs");
    // The one thing about a started operation that is not fixed. The weight
    // lands under the pool tree the operation is scheduled in, which is why it
    // is read back from there rather than from where it was sent.
    op.update_parameters(&OperationParameters::new().with_weight(2.5))?;
    check(
        "the cluster recorded the new weight",
        weight_in_first_tree(&op.get(&["runtime_parameters"])?) == Some(2.5),
    )?;

    // Assignment, not increment: the same update twice leaves the operation
    // where the first one put it. That is what makes this one safe to retry.
    op.update_parameters(&OperationParameters::new().with_weight(2.5))?;
    check(
        "and says the same thing when told twice",
        weight_in_first_tree(&op.get(&["runtime_parameters"])?) == Some(2.5),
    )?;

    step("Asking it to change nothing");
    // The cluster answers 200 and does nothing, so this is refused here — a
    // no-op that reports success is a mistake nobody finds.
    match op.update_parameters(&OperationParameters::new()) {
        Ok(()) => return fail("an empty update was sent to the cluster"),
        Err(e) => {
            check(
                "is refused before a request is sent",
                matches!(e, ClientError::Config(_)),
            )?;
            println!("   {}", first_line(&e.to_string()));
        }
    }

    step("Looking it up among the cluster's operations");
    let running = client.list_operations(
        &OperationFilter::new()
            .with_state("running")
            .with_kind(ytsaurus_client::OperationType::Vanilla)
            .with_limit(50),
    )?;
    check(
        &format!(
            "it is one of the {} running vanilla operations listed",
            running.operations.len()
        ),
        running.operations.iter().any(|o| o.id == id),
    )?;
    let listed = running
        .operations
        .iter()
        .find(|o| o.id == id)
        .expect("just checked");
    check(
        &format!(
            "and the row says what it is: {} {} started {}",
            listed.kind,
            listed.state,
            listed.start_time.as_deref().unwrap_or("?")
        ),
        listed.kind == "vanilla" && listed.finish_time.is_none(),
    )?;

    step("Reading one of its jobs by id");
    let jobs = op.jobs(Some("running"), 5)?;
    let first = jobs
        .first()
        .ok_or_else(|| ClientError::Config("the operation has no running job".to_owned()))?;
    let job = op.job(&first.id)?;
    check(
        &format!(
            "get_job answers about {} on {}",
            job.id,
            job.address.as_deref().unwrap_or("?")
        ),
        job.id == first.id,
    )?;

    step("Reading its event log");
    // Registered on every cluster and empty on one with no operations archive,
    // which is what a local cluster is. Reported rather than checked, because
    // an empty list here is the correct answer.
    let events = op.events()?;
    done(&format!(
        "list_operation_events answered with {} event(s){}",
        events.len(),
        if events.is_empty() {
            " — this cluster has no operations archive to keep them in"
        } else {
            ""
        }
    ));

    step("Finishing it early, and keeping what it made");
    // The difference from an abort: an aborted operation's output is discarded
    // and it ends as `aborted`; this ends as `completed`, so a launcher waiting
    // on it is told the work succeeded.
    op.complete()?;
    sleeper.finished();
    // `wait_for_state` is the check: it gives up on any other terminal state,
    // so reaching the next line is the fact being reported.
    let stopped = wait_for_state(&client, &id, "completed", PATIENCE)?;
    done(&format!(
        "it ends as `completed`, not `aborted` — {:.1}s",
        stopped.as_secs_f64()
    ));
    op.wait()?;
    done("and a wait on it returns Ok");

    step("Completing it again");
    match op.complete() {
        Ok(()) => return fail("a finished operation accepted a second completion"),
        Err(e) => {
            check(
                "is refused: the scheduler has let go of it",
                e.to_string().contains("No such operation"),
            )?;
            println!("   {}", first_line(&e.to_string()));
        }
    }

    merge_and_erase(&client)?;

    println!("\nAn operation is no longer a string and a wait: it can be paused,");
    println!("repriced, finished early, found by name, and picked up again by a");
    println!("process that did not start it.");
    Ok(())
}

/// The second half: two operation types this crate could not name until now.
fn merge_and_erase(client: &Client) -> Result<(), ClientError> {
    step("Two sorted tables, and a merge of them");
    client.remove_tree(BASE)?;
    let monday = format!("{BASE}/monday");
    let tuesday = format!("{BASE}/tuesday");
    let week = format!("{BASE}/week");

    client.create_table(&monday, &Visit::table_schema())?;
    client.create_table(&tuesday, &Visit::table_schema())?;
    // In key order: the table is sorted by `host`, and the cluster enforces it.
    client.write_table_rows(&monday, visits(&["alpha", "delta"]))?;
    client.write_table_rows(&tuesday, visits(&["beta", "gamma"]))?;
    // Unschematised on purpose: a merge infers the output's schema from its
    // inputs, and this is the shape a caller reaches for first.
    client.create("table", &week)?;
    done("4 rows across two tables sorted by host");

    let spec = MergeSpec::new([&monday, &tuesday], &week)
        .with_mode(MergeMode::Sorted)
        .with_merge_by(["host"])
        .with_combine_chunks(true);
    let id = client.start_merge(&spec)?;
    client.wait_for_operation(&id)?;

    let merged: Vec<Visit> = client.read_table_rows(&week)?;
    check(
        &format!("the merge wrote {} rows into one table", merged.len()),
        merged.len() == 4,
    )?;
    let hosts: Vec<&str> = merged.iter().map(|v| v.host.as_str()).collect();
    check(
        &format!("in sorted order across both inputs: {hosts:?}"),
        hosts == ["alpha", "beta", "delta", "gamma"],
    )?;

    step("A sorted merge that does not say what to merge by");
    // Refused here rather than by the cluster, which says the same thing later
    // and less clearly.
    match client.start_merge(&MergeSpec::new([&monday], &week).with_mode(MergeMode::Sorted)) {
        Ok(_) => return fail("a sorted merge with no merge_by was sent"),
        Err(e) => {
            check(
                "is refused before a request is sent",
                matches!(e, ClientError::Config(_)),
            )?;
            println!("   {}", first_line(&e.to_string()));
        }
    }

    step("Erasing the first two rows");
    // The range is part of the path — there is no parameter for it — and a
    // path with no range would erase every row and leave the table.
    let id =
        client.start_erase(&EraseSpec::new(format!("{week}[#0:#2]")).with_combine_chunks(true))?;
    client.wait_for_operation(&id)?;

    let left: Vec<Visit> = client.read_table_rows(&week)?;
    let hosts: Vec<&str> = left.iter().map(|v| v.host.as_str()).collect();
    check(
        &format!("two rows are gone and the rest are untouched: {hosts:?}"),
        hosts == ["delta", "gamma"],
    )?;

    client.remove_tree(BASE)?;
    Ok(())
}

/// A row of the tables the second half merges.
#[derive(ytsaurus_helpers::TableRow, serde::Serialize, serde::Deserialize)]
struct Visit {
    /// A key column, so the tables come out sorted and a sorted merge is legal.
    #[yt(key)]
    host: String,
    size: i64,
}

fn visits(hosts: &[&str]) -> Vec<Visit> {
    hosts
        .iter()
        .enumerate()
        .map(|(index, host)| Visit {
            host: (*host).to_owned(),
            size: index as i64 + 1,
        })
        .collect()
}

/// A sleeping operation that stops itself if this example gives up.
///
/// An example about not leaving operations running should not leave one running
/// when a check fails.
struct Sleeper<'a> {
    client: &'a Client,
    id: String,
    finished: bool,
}

impl<'a> Sleeper<'a> {
    fn start(client: &'a Client, alias: &str) -> Result<Self, ClientError> {
        let spec = VanillaSpec::new(
            VanillaTask::new("sleeper", format!("sleep {SLEEP_SECONDS}"), 1)
                .with_memory_limit(256 * 1024 * 1024),
        )
        // The alias is a spec field, and the leading `*` is the cluster's rule.
        // Setting one used to be a one-way trip: nothing here could look it up
        // again.
        .with_raw("alias", yson_build::string(alias))
        .with_raw("max_failed_job_count", yson_build::int(1));

        Ok(Self {
            id: client.start_vanilla(&spec)?,
            client,
            finished: false,
        })
    }

    /// Says the operation is accounted for, so `Drop` leaves it alone.
    fn finished(&mut self) {
        self.finished = true;
    }
}

impl Drop for Sleeper<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Best effort, and quiet: this runs while an error is on its way
            // out, and a failure to tidy up must not replace it.
            let _ = self
                .client
                .abort_operation(&self.id, Some("the lifecycle example gave up"));
        }
    }
}

/// Waits for an operation to reach `wanted`, and says how long it took.
fn wait_for_state(
    client: &Client,
    id: &str,
    wanted: &str,
    patience: Duration,
) -> Result<Duration, ClientError> {
    let started = Instant::now();

    while started.elapsed() < patience {
        let state = client.operation_state(id)?;
        if state == wanted {
            return Ok(started.elapsed());
        }
        if matches!(state.as_str(), "completed" | "failed" | "aborted") && state != wanted {
            return Err(ClientError::Config(format!(
                "operation {id} reached {state}, and was waiting for {wanted}"
            )));
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    Err(ClientError::Config(format!(
        "operation {id} did not reach {wanted} within {:.0}s",
        patience.as_secs_f64()
    )))
}

/// A string field of a YSON document, or `None` if it is not one.
fn text_field(document: &ytsaurus_yson::YsonValue, key: &str) -> Option<String> {
    match &document.node {
        ytsaurus_yson::YsonNode::Map(m) => match &m.get(key.as_bytes())?.node {
            ytsaurus_yson::YsonNode::String(bytes) => {
                Some(String::from_utf8_lossy(bytes).into_owned())
            }
            _ => None,
        },
        _ => None,
    }
}

/// The weight the scheduler recorded, from whichever pool tree it landed in.
///
/// A weight sent at the top level is spread across every tree the operation
/// runs in; a local cluster has exactly one, called `default`.
fn weight_in_first_tree(document: &ytsaurus_yson::YsonValue) -> Option<f64> {
    use ytsaurus_yson::YsonNode;

    let map = |value: &ytsaurus_yson::YsonValue, key: &str| match &value.node {
        YsonNode::Map(m) => m.get(key.as_bytes()).cloned(),
        _ => None,
    };

    let trees = map(
        &map(document, "runtime_parameters")?,
        "scheduling_options_per_pool_tree",
    )?;
    let YsonNode::Map(trees) = &trees.node else {
        return None;
    };
    let first = trees.values().next()?;

    match map(first, "weight")?.node {
        YsonNode::Double(w) => Some(w),
        _ => None,
    }
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or_default().to_owned()
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

fn fail(what: &str) -> Result<(), ClientError> {
    eprintln!("   FAIL {what}");
    Err(ClientError::Config(what.to_owned()))
}
