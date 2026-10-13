//! `abort`: stopping an operation, and what stopping one costs.
//!
//! A launcher that gives up on an operation should abort it rather than leave
//! it spending quota. This aborts a running operation and checks that its job
//! stops and that the reason is kept; aborts a finished one, which is an error;
//! aborts one before it runs, with a reason that needs escaping; and aborts one
//! from another thread while a wait is on it.
//!
//! It needs no worker binary: a vanilla task runs `sleep`.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example abort
//! ```

use std::process::ExitCode;
use std::time::{Duration, Instant};

use ytsaurus_client::{Client, ClientError, VanillaSpec, VanillaTask, yson_build};

/// How long the operation would run if nobody stopped it.
const SLEEP_SECONDS: u32 = 300;

/// How long to wait for the abort to take effect before calling it a failure.
const PATIENCE: Duration = Duration::from_secs(120);

/// What the abort says about itself.
const REASON: &str = "stopped by the abort example";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nabort failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    step(&format!(
        "Starting something that would run for {SLEEP_SECONDS}s"
    ));
    let mut sleeper = Sleeper::start(&client)?;
    let id = sleeper.id.clone();
    println!("   operation {id}");

    // An operation still in `starting` has no jobs to stop, so wait for
    // `running`. `wait_for_state` returns an error if the state never arrives.
    let waited = wait_for_state(&client, &id, "running", PATIENCE)?;
    done(&format!("it is running, {:.1}s in", waited.as_secs_f64()));

    // The job is what costs; the operation's state does not say whether the
    // abort reached it.
    let job = wait_for_a_running_job(&client, &id, PATIENCE)?;
    done(&format!(
        "with a job running on {}",
        job.address.as_deref().unwrap_or("?")
    ));

    step("Aborting it, with a reason");
    let asked = Instant::now();
    client.abort_operation(&id, Some(REASON))?;
    sleeper.finished();
    let acknowledged = asked.elapsed();
    done(&format!(
        "the scheduler took the request in {:.0} ms",
        acknowledged.as_secs_f64() * 1000.0
    ));

    // In practice the operation is already `aborted` when the call returns;
    // the `aborting` state in between is not observable from here.
    let stopped = wait_for_state(&client, &id, "aborted", PATIENCE)?;
    done(&format!(
        "and it was already `aborted` — {:.1}s of waiting",
        stopped.as_secs_f64()
    ));

    // The terminal state can come before the job stops: a node may still
    // report it `running` during teardown. A local scheduler drops aborted jobs
    // from `list_jobs` instead. Either way the check is that no job is running.
    let drained = wait_for_no_running_jobs(&client, &id, PATIENCE)?;
    done(&format!(
        "and its job stopped {:.1}s after the operation did",
        drained.as_secs_f64()
    ));

    step("Reading back why it stopped");
    // The reason is folded into the operation's error document, under the
    // cluster's own account, so whoever finds the operation later sees it.
    let error = client
        .operation_result_error(&id)?
        .unwrap_or_else(|| "(the cluster recorded no error at all)".to_owned());
    println!("   {error}");
    check(
        "the cluster recorded the abort as a user request",
        error.contains("aborted by user request"),
    )?;
    check(
        &format!("and kept the reason given: {REASON:?}"),
        error.contains(REASON),
    )?;

    step("Aborting it again");
    // Unlike a transaction, which accepts a second abort, an operation the
    // scheduler has finished with is gone, so aborting it is an error.
    match client.abort_operation(&id, None) {
        Ok(()) => {
            eprintln!("   FAIL a finished operation accepted a second abort");
            return Err(ClientError::Config(
                "abort_operation is idempotent after all, and the docs say it is not".to_owned(),
            ));
        }
        Err(e) => {
            check(
                "is refused, because the scheduler has let go of it",
                e.to_string().contains("No such operation"),
            )?;
            println!("   {e}");
        }
    }

    step("Aborting one that has not started running yet");
    // No jobs yet, but the scheduler must still be told, or it starts them.
    // The reason travels in an HTTP header as YSON, so a quote or a newline
    // must be escaped.
    let awkward = "he said \"stop\"\nand meant it";
    let mut sleeper = Sleeper::start(&client)?;
    let id = sleeper.id.clone();
    let state = client.operation_state(&id)?;
    client.abort_operation(&id, Some(awkward))?;
    sleeper.finished();
    let early = wait_for_state(&client, &id, "aborted", PATIENCE)?;
    done(&format!(
        "aborted from `{state}` in {:.1}s",
        early.as_secs_f64()
    ));
    let error = client.operation_result_error(&id)?.unwrap_or_default();
    check(
        "and the quote and the newline came back unharmed",
        error.contains(awkward),
    )?;

    step("Pulling an operation out from under a wait");
    // What a launcher sees when someone else stops the operation it is
    // waiting for: an error that says more than that the wait ended.
    let mut sleeper = Sleeper::start(&client)?;
    let id = sleeper.id.clone();
    sleeper.finished(); // the thread below is what stops this one
    let stopper = client.clone();
    let stopping = id.clone();
    let hand = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        stopper.abort_operation(&stopping, Some("stopped by somebody else"))
    });
    let waited = match client.wait_for_operation(&id) {
        Ok(()) => {
            eprintln!("   FAIL an aborted operation was waited for successfully");
            return Err(ClientError::Config(
                "wait_for_operation returned Ok for an operation that was aborted".to_owned(),
            ));
        }
        Err(e) => e,
    };
    hand.join().expect("the aborting thread finished")?;
    let reported = waited.to_string();
    check(
        "the wait fails, naming the state and the reason",
        reported.contains("aborted") && reported.contains("stopped by somebody else"),
    )?;
    println!("   {}", reported.replace('\n', "\n   "));

    println!("\nAn operation nobody is waiting for is an operation nobody should be paying");
    println!(
        "for. Asked at {:.0} ms, stopped {:.1}s later.",
        acknowledged.as_secs_f64() * 1000.0,
        stopped.as_secs_f64()
    );
    Ok(())
}

/// A sleeping operation that `Drop` aborts unless `finished` was called, so a
/// failed check does not leave it running.
struct Sleeper<'a> {
    client: &'a Client,
    id: String,
    finished: bool,
}

impl<'a> Sleeper<'a> {
    fn start(client: &'a Client) -> Result<Self, ClientError> {
        Ok(Self {
            id: start_sleeping(client)?,
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
            // Best effort and quiet: an error is already on its way out.
            let _ = self
                .client
                .abort_operation(&self.id, Some("the abort example gave up"));
        }
    }
}

/// Starts a vanilla operation whose one job sleeps.
fn start_sleeping(client: &Client) -> Result<String, ClientError> {
    let spec = VanillaSpec::new(
        // A vanilla task runs the command it is given; no worker is needed.
        VanillaTask::new("sleeper", format!("sleep {SLEEP_SECONDS}"), 1)
            .with_memory_limit(256 * 1024 * 1024),
    )
    .with_raw("max_failed_job_count", yson_build::int(1));

    client.start_vanilla(&spec)
}

/// Waits until the operation has a job actually running: the scheduler reports
/// the operation `running` before the first job has been handed to a node.
fn wait_for_a_running_job(
    client: &Client,
    id: &str,
    patience: Duration,
) -> Result<ytsaurus_client::JobInfo, ClientError> {
    let started = Instant::now();

    while started.elapsed() < patience {
        if let Some(job) = client
            .list_jobs(id, Some("running"), 10)?
            .into_iter()
            .next()
        {
            return Ok(job);
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    Err(ClientError::Config(format!(
        "operation {id} had no running job within {:.0}s",
        patience.as_secs_f64()
    )))
}

/// Waits until `list_jobs` no longer reports a running job; an operation is
/// `aborted` before every node has torn its job down. With the `running`
/// filter, a one-item answer is enough.
fn wait_for_no_running_jobs(
    client: &Client,
    id: &str,
    patience: Duration,
) -> Result<Duration, ClientError> {
    let started = Instant::now();

    while started.elapsed() < patience {
        if client.list_jobs(id, Some("running"), 1)?.is_empty() {
            return Ok(started.elapsed());
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    Err(ClientError::Config(format!(
        "operation {id} still had a running job within {:.0}s",
        patience.as_secs_f64()
    )))
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
        // A terminal state that is not the one wanted will never become it.
        if matches!(state.as_str(), "completed" | "failed") {
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
