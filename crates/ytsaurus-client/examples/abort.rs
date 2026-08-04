//! `abort` — stopping an operation, and what stopping one costs.
//!
//! A launcher that starts an operation and then gives up — an interrupted
//! wait, a failed step further down the script — used to leave that operation
//! running on the cluster, spending quota on a result nobody would read. This
//! is the other end of `start_map`: the one that says never mind.
//!
//! It needs no worker binary. A vanilla task runs whatever command it is
//! given, and `sleep` is the shortest way to have something worth stopping.
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

    // Aborting before the scheduler has started it would test a different
    // thing: an operation still in `starting` has no jobs to stop.
    // `wait_for_state` is the check: it returns an error if the state never
    // arrives, so reaching this line is the fact being reported.
    let waited = wait_for_state(&client, &id, "running", PATIENCE)?;
    done(&format!("it is running, {:.1}s in", waited.as_secs_f64()));

    // A running operation has a running job, and that job is the thing costing
    // money. Whether the abort reaches it is the question the state does not
    // answer.
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

    // Nothing is waited for here in practice: by the time the call has
    // returned the operation is already `aborted`, and the `aborting` state
    // that exists in between is not observable from this side of the request.
    let stopped = wait_for_state(&client, &id, "aborted", PATIENCE)?;
    done(&format!(
        "and it was already `aborted` — {:.1}s of waiting",
        stopped.as_secs_f64()
    ));

    // The jobs go with it. The scheduler drops an aborted operation's jobs from
    // `list_jobs` rather than showing them as aborted, so what is checked here
    // is that nothing is left running — which is the part that costs.
    let left = client.list_jobs(&id, None, 10)?;
    check(
        &format!(
            "and no job is still running ({} left in the list)",
            left.len()
        ),
        left.iter().all(|j| j.state != "running"),
    )?;

    step("Reading back why it stopped");
    // The reason is not kept beside the operation; it is folded into the error
    // document, under the cluster's own account of what happened. That is what
    // makes it worth passing: whoever finds this operation tomorrow is told who
    // stopped it, without having to find the launcher's logs.
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
    // Unlike a transaction, which forgives a second abort, an operation the
    // scheduler has finished with is gone from it. A launcher that aborts
    // defensively after a successful wait gets an error, not a shrug.
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
    // There is nothing to stop yet, which is a different thing from nothing to
    // do: the scheduler still has to be told, or it starts the jobs a moment
    // later. The reason goes along an HTTP header as YSON, so this one is also
    // the awkward-text case — a quote would end the string and a newline would
    // end the header if either reached the wire unescaped.
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
    // What a launcher sees when somebody else — an operator, a second copy of
    // the script — stops the operation it is waiting for. The error has to say
    // more than that the wait ended.
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

/// A sleeping operation that stops itself if this example gives up.
///
/// The thesis of the example is that an operation nobody is waiting for should
/// not be left running; an example that leaked a five-minute operation whenever
/// a check failed would be arguing against itself.
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
            // Best effort, and quiet: this runs while an error is on its way
            // out, and a failure to tidy up must not replace it.
            let _ = self
                .client
                .abort_operation(&self.id, Some("the abort example gave up"));
        }
    }
}

/// Starts a vanilla operation whose one job sleeps.
fn start_sleeping(client: &Client) -> Result<String, ClientError> {
    let spec = VanillaSpec::new(
        // No worker binary: a vanilla task runs the command it is given, and
        // this one needs nothing from this repository to be worth stopping.
        VanillaTask::new("sleeper", format!("sleep {SLEEP_SECONDS}"), 1)
            .with_memory_limit(256 * 1024 * 1024),
    )
    .with_raw("max_failed_job_count", yson_build::int(1));

    client.start_vanilla(&spec)
}

/// Waits until the operation has a job actually running.
///
/// `running` is the operation's state, not a job's: the scheduler reports it
/// before the first job has been handed to a node.
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
