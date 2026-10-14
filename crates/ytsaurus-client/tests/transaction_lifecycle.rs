//! The detach/attach lifecycle, on the wire.
//!
//! An aborted transaction and a detached one that expired both end, so the
//! tests read which requests the client sent: a detach and an attached
//! handle's drop send nothing, a started handle's drop sends the abort, and
//! the by-id commands carry the id they were given. A start whose answer was
//! lost is retried under the same mutation id and pings before it returns.
//!
//! The stub **reads every request in full before answering**: replying into a
//! body still being written closes the connection under `ureq`, which passes
//! on macOS and fails on a Linux runner. Parameters are asserted by decoding
//! `X-YT-Parameters`, never by matching a generated value's spelling.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ytsaurus_client::{Client, RetryPolicy};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice};

/// The id every stub transaction answers with, fixed so tests may assert it.
const TX: &str = "3-5bc70-10001-387a";

// ------------------------------------------------------------- the lifecycle

#[test]
fn a_started_handles_drop_still_aborts() {
    // Dropping a transaction this process started aborts it, which makes `?`
    // safe inside a transaction; examples/transaction.rs relies on it.
    let cluster = StubCluster::answering(Answers::default());
    {
        let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());
        let tx = client.start_transaction().expect("starts");
        assert_eq!(tx.id(), TX);
    } // dropped here, neither committed nor detached

    let aborts = cluster.heads_for("abort_transaction");
    assert_eq!(
        aborts.len(),
        1,
        "a dropped started handle must abort exactly once: {:?}",
        cluster.request_lines()
    );
    assert_eq!(
        str_param(&aborts[0], "transaction_id").as_deref(),
        Some(TX),
        "the abort named the wrong transaction:\n{}",
        aborts[0]
    );
}

#[test]
fn a_detached_transaction_is_neither_aborted_nor_pinged_again() {
    // A 3 s transaction is pinged every second, so 1.6 s of silence after
    // detach covers more than one interval.
    let cluster = StubCluster::answering(Answers::default());
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client
        .start_transaction_with(Duration::from_secs(3))
        .expect("starts");
    let id = tx.detach();
    assert_eq!(id, TX, "detach must hand back the transaction's own id");

    let settled = cluster.request_count();
    std::thread::sleep(Duration::from_millis(1600));

    assert!(
        cluster.heads_for("abort_transaction").is_empty(),
        "detach sent an abort: {:?}",
        cluster.request_lines()
    );
    assert_eq!(
        cluster.request_count(),
        settled,
        "something was sent after detach returned: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn detach_with_a_ping_in_flight_waits_for_it() {
    // `detach` joins a keep-alive thread that is mid-request. The stub answers
    // pings 500 ms late, and the test detaches once one has arrived, so the
    // ping is in flight while detach runs.
    //
    // **The assertion is the wall clock.** Request counts cannot see the join:
    // the in-flight ping is counted on arrival. Without the join, a ping
    // landing after `detach` returns restarts the expiry clock, so the
    // transaction outlives T+timeout, still holding its locks.
    const ANSWERED_AFTER: Duration = Duration::from_millis(500);

    let cluster = StubCluster::answering(Answers {
        ping_delay: ANSWERED_AFTER,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client
        .start_transaction_with(Duration::from_secs(3))
        .expect("starts");

    let deadline = Instant::now() + Duration::from_secs(5);
    while cluster.heads_for("ping_transaction").is_empty() {
        assert!(
            Instant::now() < deadline,
            "no ping arrived within 5 s of starting a 3 s transaction"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let waited = Instant::now();
    let id = tx.detach(); // the ping is being answered, 500 ms late
    let waited = waited.elapsed();
    assert_eq!(id, TX);

    // Half the delay: the polling loop sleeps 10 ms between checks, so it
    // notices the ping up to 10 ms after it arrived, more on a loaded machine.
    // Without the join this is single-digit milliseconds.
    assert!(
        waited >= ANSWERED_AFTER / 2,
        "detach returned in {waited:?} with a ping being answered {ANSWERED_AFTER:?} late: \
         it did not wait for the ping in flight"
    );

    let settled = cluster.request_count();
    std::thread::sleep(Duration::from_millis(1600));

    assert!(
        cluster.heads_for("abort_transaction").is_empty(),
        "detach under a ping in flight aborted: {:?}",
        cluster.request_lines()
    );
    assert_eq!(
        cluster.request_count(),
        settled,
        "a request started after detach returned: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn attach_reads_the_timeout_pings_and_its_drop_does_not_abort() {
    // A 3000 ms timeout means a ping every second, carrying the id. Dropping
    // the handle stops them without an abort: an attached handle detaches on
    // drop, as the C++ destructor does.
    let cluster = StubCluster::answering(Answers {
        timeout_ms: 3000,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client.attach_transaction(TX).expect("attaches");
    assert_eq!(tx.id(), TX);

    // The attach asked the object itself for its timeout.
    let gets = cluster.heads_for("/api/v4/get");
    assert_eq!(gets.len(), 1, "{:?}", cluster.request_lines());
    assert_eq!(
        str_param(&gets[0], "path").as_deref(),
        Some(format!("#{TX}/@timeout").as_str()),
        "the timeout was read from somewhere else:\n{}",
        gets[0]
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let ping = loop {
        if let Some(head) = cluster.heads_for("ping_transaction").into_iter().next() {
            break head;
        }
        assert!(
            Instant::now() < deadline,
            "an attached handle sent no ping within 5 s"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(str_param(&ping, "transaction_id").as_deref(), Some(TX));

    drop(tx);

    // The drop does not join the thread, so one ping may be in flight. A
    // second of grace covers its request budget (half the interval, 500 ms)
    // on a loaded machine; then silence for more than an interval.
    std::thread::sleep(Duration::from_millis(1000));
    let settled = cluster.request_count();
    std::thread::sleep(Duration::from_millis(1600));

    assert!(
        cluster.heads_for("abort_transaction").is_empty(),
        "an attached handle's drop aborted the owner's transaction: {:?}",
        cluster.request_lines()
    );
    assert_eq!(
        cluster.request_count(),
        settled,
        "the pings did not stop when the attached handle dropped: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn attaching_pings_before_it_returns() {
    // `@timeout` is the configured lifetime, not what is left of it, so a
    // handoff slower than two thirds of the timeout would leave the first
    // scheduled ping too late. The attach pings before it returns.
    //
    // With a 30 s timeout the thread's first ping is ten seconds away, so a
    // ping on record when `attach_transaction` returns is the attach's own.
    let cluster = StubCluster::answering(Answers {
        timeout_ms: 30_000,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client.attach_transaction(TX).expect("attaches");

    let pings = cluster.heads_for("ping_transaction");
    assert_eq!(
        pings.len(),
        1,
        "attach must restart the transaction's clock before handing back a handle: {:?}",
        cluster.request_lines()
    );
    assert_eq!(str_param(&pings[0], "transaction_id").as_deref(), Some(TX));
    drop(tx);
}

#[test]
fn attaching_to_a_transaction_that_dies_between_the_two_reads_fails_here() {
    // The attach's ping is also a probe. The stub answers the timeout read
    // but refuses the ping, as for a transaction that expired in the
    // handoff; the attach fails rather than return a handle that pings
    // nothing.
    let cluster = StubCluster::answering(Answers {
        ping_gone: true,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let error = client
        .attach_transaction(TX)
        .expect_err("the transaction is gone");
    let message = error.to_string();
    for expected in ["attach", TX, "No such transaction"] {
        assert!(
            message.contains(expected),
            "the error does not say {expected:?}: {message}"
        );
    }

    let before = cluster.request_count();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        cluster.request_count(),
        before,
        "a failed attach left a ping thread behind: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn a_handle_says_so_when_the_cluster_says_the_transaction_is_gone() {
    // The keep-alive thread stops only when the cluster says the transaction
    // is gone; `is_lost` makes that observable.
    let cluster = StubCluster::answering(Answers {
        ping_gone: true,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    // Started, not attached: attach refuses a transaction whose ping fails.
    let tx = client
        .start_transaction_with(Duration::from_secs(3))
        .expect("starts");
    assert!(!tx.is_lost(), "nothing has been answered yet");

    let deadline = Instant::now() + Duration::from_secs(5);
    while !tx.is_lost() {
        assert!(
            Instant::now() < deadline,
            "the handle never noticed: {:?}",
            cluster.request_lines()
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // And it stopped pinging.
    let settled = cluster.request_count();
    std::thread::sleep(Duration::from_millis(1600));
    assert_eq!(
        cluster.request_count(),
        settled,
        "the thread gave up and went on pinging: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn an_attached_handles_explicit_abort_still_aborts() {
    // Only the drop differs for an attached handle; an explicit abort reaches
    // the cluster from either origin.
    let cluster = StubCluster::answering(Answers {
        timeout_ms: 30_000,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client.attach_transaction(TX).expect("attaches");
    tx.abort().expect("aborts");

    let aborts = cluster.heads_for("abort_transaction");
    assert_eq!(
        aborts.len(),
        1,
        "an attached handle's explicit abort was swallowed: {:?}",
        cluster.request_lines()
    );
    assert_eq!(str_param(&aborts[0], "transaction_id").as_deref(), Some(TX));
}

#[test]
fn detaching_an_attached_handle_sends_nothing() {
    // Attach in the middle of a chain, then detach for the next holder. A 3 s
    // timeout is a ping every second, so 1.6 s of silence covers an interval.
    let cluster = StubCluster::answering(Answers {
        timeout_ms: 3000,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client.attach_transaction(TX).expect("attaches");
    assert_eq!(tx.detach(), TX, "detach must hand back the same id");

    let settled = cluster.request_count();
    std::thread::sleep(Duration::from_millis(1600));

    assert!(
        cluster.heads_for("abort_transaction").is_empty(),
        "detaching an attached handle aborted: {:?}",
        cluster.request_lines()
    );
    assert!(
        cluster.heads_for("commit_transaction").is_empty(),
        "detaching an attached handle committed: {:?}",
        cluster.request_lines()
    );
    assert_eq!(
        cluster.request_count(),
        settled,
        "something was sent after detach returned: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn an_attached_handle_commits_like_an_owner() {
    // An attached handle's explicit commit is the same commit, mutation ID
    // included. A 30 s timeout keeps the ping thread quiet for the test.
    let cluster = StubCluster::answering(Answers {
        timeout_ms: 30_000,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client.attach_transaction(TX).expect("attaches");
    tx.commit().expect("commits");

    let commits = cluster.heads_for("commit_transaction");
    assert_eq!(commits.len(), 1, "{:?}", cluster.request_lines());
    assert_eq!(
        str_param(&commits[0], "transaction_id").as_deref(),
        Some(TX)
    );
    assert!(
        param_of(&commits[0], "mutation_id").is_some(),
        "a commit is not idempotent and must carry a mutation id:\n{}",
        commits[0]
    );
    assert!(
        cluster.heads_for("abort_transaction").is_empty(),
        "an abort followed a successful commit: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn attaching_to_a_transaction_that_is_gone_is_a_clear_error() {
    // The timeout read fails before any handle exists, so no ping thread is
    // left behind. The error names the operation and the id, since the
    // cluster may not: a garbage id earns `Unknown cell tag 0`.
    let cluster = StubCluster::answering(Answers {
        missing: true,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let error = client
        .attach_transaction("0-0-0-1")
        .expect_err("there is nothing to attach to");
    let message = error.to_string();
    for expected in ["attach", "0-0-0-1", "No such object"] {
        assert!(
            message.contains(expected),
            "the error does not say {expected:?}: {message}"
        );
    }

    std::thread::sleep(Duration::from_millis(200));
    assert!(
        cluster.heads_for("ping_transaction").is_empty(),
        "a failed attach left a ping thread behind: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn finishing_someone_elses_transaction_takes_only_the_id() {
    // The by-id commands a process holding only the id sends: POSTs to the v4
    // names, carrying the id. The commit, not idempotent, carries a mutation
    // id whose presence is asserted, never its spelling.
    let cluster = StubCluster::answering(Answers::default());
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    client.ping_transaction(TX).expect("pings");
    client.commit_transaction(TX).expect("commits");
    client.abort_transaction(TX).expect("aborts");

    for command in [
        "ping_transaction",
        "commit_transaction",
        "abort_transaction",
    ] {
        let heads = cluster.heads_for(command);
        assert_eq!(heads.len(), 1, "{command}: {:?}", cluster.request_lines());
        assert!(
            heads[0].starts_with(&format!("POST /api/v4/{command} ")),
            "{command} used the wrong verb or path:\n{}",
            heads[0]
        );
        assert_eq!(
            str_param(&heads[0], "transaction_id").as_deref(),
            Some(TX),
            "{command} named the wrong transaction:\n{}",
            heads[0]
        );
    }

    let commit = &cluster.heads_for("commit_transaction")[0];
    assert!(
        param_of(commit, "mutation_id").is_some(),
        "a by-id commit must ride under a mutation id:\n{commit}"
    );

    // Ping and abort carry no mutation ID (nor `retry`, which goes with it):
    // a repeated ping is harmless, and aborting a transaction already gone
    // answers `{}`, so neither needs the cluster to remember an id.
    for freely in ["ping_transaction", "abort_transaction"] {
        let head = &cluster.heads_for(freely)[0];
        assert!(
            param_of(head, "mutation_id").is_none(),
            "{freely} is idempotent on its own and must carry no mutation id:\n{head}"
        );
        assert!(
            param_of(head, "retry").is_none(),
            "{freely} sent a retry flag with no mutation id to go with it:\n{head}"
        );
    }
}

// --------------------------------------------------------- a retried start

/// One lost start answer, retried once after 1.1 s: longer than the 1 s ping
/// interval of a 3 s transaction, so the first ping is due when `start`
/// returns.
fn retried_once() -> RetryPolicy {
    RetryPolicy::new(2, Duration::from_millis(1100), Duration::from_millis(1100)).quiet()
}

#[test]
fn a_retried_start_pings_before_it_returns() {
    // The retry carries the first attempt's mutation id, so the cluster hands
    // back the first attempt's transaction, whose clock started 1.1 s before
    // the answer: past the interval, so `start` pings before returning.
    let cluster = StubCluster::answering(Answers {
        lost_starts: AtomicUsize::new(1),
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(retried_once());

    let tx = client
        .start_transaction_with(Duration::from_secs(3))
        .expect("starts on the second attempt");
    // Read at once: the keep-alive's own first ping is a full second away.
    let lines = cluster.request_lines();
    assert_eq!(tx.id(), TX);

    let starts = cluster.heads_for("start_transaction");
    assert_eq!(starts.len(), 2, "one lost answer, one retry: {lines:?}");
    let first = str_param(&starts[0], "mutation_id").expect("the start carries a mutation id");
    assert_eq!(
        str_param(&starts[1], "mutation_id").as_deref(),
        Some(first.as_str()),
        "the retry must carry the first attempt's mutation id"
    );
    assert_eq!(
        param_of(&starts[0], "retry").map(|v| v.node),
        Some(YsonNode::Boolean(false))
    );
    assert_eq!(
        param_of(&starts[1], "retry").map(|v| v.node),
        Some(YsonNode::Boolean(true)),
        "the second start must say it is a retry"
    );

    let pings: Vec<_> = lines
        .iter()
        .filter(|line| line.contains("ping_transaction"))
        .collect();
    assert_eq!(
        pings.len(),
        1,
        "a start that took longer than the ping interval must ping before it returns: {lines:?}"
    );
    let ping_head = &cluster.heads_for("ping_transaction")[0];
    assert_eq!(str_param(ping_head, "transaction_id").as_deref(), Some(TX));
    let second_start = lines
        .iter()
        .rposition(|line| line.contains("start_transaction"))
        .expect("a start is on record");
    let ping = lines
        .iter()
        .position(|line| line.contains("ping_transaction"))
        .expect("a ping is on record");
    assert!(
        ping > second_start,
        "the ping preceded the start: {lines:?}"
    );
    drop(tx);
}

#[test]
fn a_start_whose_transaction_expired_during_the_retries_fails() {
    // The retried start returns an id, but the ping says the transaction is
    // gone, as for one that expired while the start was being retried.
    // `start` fails instead of returning a handle on nothing, and leaves no
    // ping thread behind.
    let cluster = StubCluster::answering(Answers {
        lost_starts: AtomicUsize::new(1),
        ping_gone: true,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(retried_once());

    let error = client
        .start_transaction_with(Duration::from_secs(3))
        .expect_err("the transaction is gone");
    let message = error.to_string();
    for expected in ["start", TX, "No such transaction"] {
        assert!(
            message.contains(expected),
            "the error does not say {expected:?}: {message}"
        );
    }

    let aborts = cluster.heads_for("abort_transaction");
    assert!(
        aborts.len() <= 1,
        "one bounded abort at most: {:?}",
        cluster.request_lines()
    );
    for abort in &aborts {
        assert_eq!(str_param(abort, "transaction_id").as_deref(), Some(TX));
    }

    let settled = cluster.request_count();
    std::thread::sleep(Duration::from_millis(1600));
    assert_eq!(
        cluster.request_count(),
        settled,
        "a failed start left a ping thread behind: {:?}",
        cluster.request_lines()
    );
}

#[test]
fn a_quick_start_sends_no_extra_ping() {
    // The common path costs one request: a start answered within the ping
    // interval leaves the first ping to the keep-alive, ten seconds away.
    let cluster = StubCluster::answering(Answers::default());
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let tx = client.start_transaction().expect("starts");

    assert!(
        cluster.heads_for("ping_transaction").is_empty(),
        "a quick start pinged before returning: {:?}",
        cluster.request_lines()
    );
    drop(tx);
}

#[test]
fn a_slow_starts_first_ping_counts_from_when_it_was_sent() {
    // The stub holds the start's answer for 700 ms, under the 1 s interval
    // of a 3 s transaction, so `start` sends no ping of its own, and the
    // keep-alive's first ping is due 1 s after the start was sent rather than
    // 1 s after it returned.
    //
    // The bound is an upper one, so load pushes toward it. Measured from the
    // call: 1.005-1.022 s over ten runs at load average 22-35 on ten cores,
    // 1.010-1.058 s over eight at 124-130; counted from the return instead,
    // 1.714-1.788 s.
    const HELD: Duration = Duration::from_millis(700);
    const BOUND: Duration = Duration::from_millis(1350);

    let cluster = StubCluster::answering(Answers {
        start_delay: HELD,
        ..Answers::default()
    });
    let client = Client::new(&cluster.url()).with_retries(RetryPolicy::none());

    let began = Instant::now();
    let tx = client
        .start_transaction_with(Duration::from_secs(3))
        .expect("starts");
    assert!(
        cluster.heads_for("ping_transaction").is_empty(),
        "a start within the interval pinged before returning: {:?}",
        cluster.request_lines()
    );

    let deadline = began + Duration::from_secs(5);
    while cluster.heads_for("ping_transaction").is_empty() {
        assert!(Instant::now() < deadline, "no ping within 5 s");
        std::thread::sleep(Duration::from_millis(5));
    }
    let first_ping = began.elapsed();
    drop(tx);

    assert!(
        first_ping < BOUND,
        "the first ping came {first_ping:?} after the start was sent: it was counted \
         from when the start returned"
    );
}

// ------------------------------------------------------------------ the stub

/// What the stub cluster answers with.
struct Answers {
    /// What `get #<id>/@timeout` answers, in milliseconds.
    timeout_ms: i64,
    /// How long a ping is held before it is answered, to keep it in flight.
    ping_delay: Duration,
    /// Whether the transaction is gone: `get` then answers the resolve error
    /// a local cluster gives for an id that names nothing.
    missing: bool,
    /// Whether a ping is refused with `No such transaction`, as for a
    /// transaction that expired or that somebody else finished. Separate from
    /// `missing`: the object may still resolve.
    ping_gone: bool,
    /// How many `start_transaction` requests are read, recorded and then
    /// left unanswered, the connection closed: an answer lost after the
    /// cluster received the request.
    lost_starts: AtomicUsize,
    /// How long a `start_transaction` is held before it is answered.
    start_delay: Duration,
}

impl Default for Answers {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            ping_delay: Duration::ZERO,
            missing: false,
            ping_gone: false,
            lost_starts: AtomicUsize::new(0),
            start_delay: Duration::ZERO,
        }
    }
}

impl Answers {
    /// The reply to one request; `None` closes the connection unanswered.
    fn answer(&self, head: &str) -> Option<Vec<u8>> {
        let path = head
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        let reply = match path.as_str() {
            "/api/v4/start_transaction" => {
                let lost = self
                    .lost_starts
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok();
                if lost {
                    return None;
                }
                std::thread::sleep(self.start_delay);
                ok(format!(r#"{{"transaction_id"="{TX}"}}"#).as_bytes())
            }
            "/api/v4/get" if self.missing => resolve_error(),
            "/api/v4/get" => ok(format!(r#"{{"value"={}}}"#, self.timeout_ms).as_bytes()),
            "/api/v4/ping_transaction" => {
                std::thread::sleep(self.ping_delay);
                if self.ping_gone {
                    no_such_transaction()
                } else {
                    ok(b"{}")
                }
            }
            _ => ok(b"{}"),
        };
        Some(reply)
    }
}

/// A stand-in cluster: stays up, reads every request in full before
/// answering, and remembers the request heads in arrival order.
struct StubCluster {
    address: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<String>>>,
}

impl StubCluster {
    fn answering(answers: Answers) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");
        let seen = Arc::new(Mutex::new(Vec::new()));

        let served = Arc::clone(&seen);
        let answers = Arc::new(answers);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let answers = Arc::clone(&answers);
                let seen = Arc::clone(&served);
                std::thread::spawn(move || serve(stream, &answers, &seen));
            }
        });

        Self { address, seen }
    }

    fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Full heads of the requests whose first line mentions `what`.
    fn heads_for(&self, what: &str) -> Vec<String> {
        self.seen
            .lock()
            .expect("nothing panicked holding it")
            .iter()
            .filter(|head| head.lines().next().is_some_and(|line| line.contains(what)))
            .cloned()
            .collect()
    }

    fn request_count(&self) -> usize {
        self.seen.lock().expect("nothing panicked holding it").len()
    }

    /// First lines only, for assertion messages.
    fn request_lines(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("nothing panicked holding it")
            .iter()
            .map(|head| head.lines().next().unwrap_or_default().to_owned())
            .collect()
    }
}

fn serve(mut stream: std::net::TcpStream, answers: &Answers, seen: &Arc<Mutex<Vec<String>>>) {
    let mut reader = BufReader::new(stream.try_clone().expect("clones"));

    loop {
        let mut head = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => return,
                Ok(_) if line == "\r\n" => break,
                Ok(_) => head.push_str(&line),
                Err(_) => return,
            }
        }
        if head.is_empty() {
            return;
        }

        // Head and body before any answer, or `ureq` reports a broken pipe on
        // a Linux runner.
        if let Some(length) = content_length(&head) {
            let mut body = vec![0_u8; length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
        } else if head.to_lowercase().contains("transfer-encoding: chunked") {
            drain_chunked(&mut reader);
        }

        // Recorded on arrival, so a test can act while a reply is held.
        seen.lock()
            .expect("nothing panicked holding it")
            .push(head.clone());

        let Some(answer) = answers.answer(&head) else {
            return;
        };
        if stream.write_all(&answer).is_err() {
            return;
        }
        stream.flush().ok();
    }
}

fn ok(body: &[u8]) -> Vec<u8> {
    let mut reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-yt-yson-text\r\n\r\n",
        body.len()
    )
    .into_bytes();
    reply.extend_from_slice(body);
    reply
}

/// A local cluster's answer to `get #<gone>/@timeout`, captured (noise
/// attributes trimmed): HTTP 200, `X-YT-Error` with the resolve error outside
/// and `No such object` inside, not the `No such transaction` a ping earns.
fn resolve_error() -> Vec<u8> {
    let document = r#"{"code":500,"message":"Error resolving path #0-0-0-1/@timeout","inner_errors":[{"code":500,"message":"No such object 0-0-0-1","attributes":{"missing_object_id":"0-0-0-1"}}]}"#;
    format!("HTTP/1.1 200 OK\r\nX-YT-Error: {document}\r\nContent-Length: 0\r\n\r\n").into_bytes()
}

/// A cluster's answer to a ping of a transaction that is gone: code 11000,
/// `NoSuchTransaction`. The client recognises both this and the resolve error.
fn no_such_transaction() -> Vec<u8> {
    let document = format!(
        r#"{{"code":11000,"message":"No such transaction {TX}","attributes":{{"transaction_id":"{TX}"}}}}"#
    );
    format!("HTTP/1.1 200 OK\r\nX-YT-Error: {document}\r\nContent-Length: 0\r\n\r\n").into_bytes()
}

fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .find(|line| line.to_lowercase().starts_with("content-length:"))
        .and_then(|line| line.split_once(':'))
        .and_then(|(_, value)| value.trim().parse().ok())
}

fn drain_chunked(reader: &mut BufReader<std::net::TcpStream>) {
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() {
            return;
        }
        let size = usize::from_str_radix(header.trim(), 16).unwrap_or(0);
        let mut chunk = vec![0_u8; size + 2];
        if reader.read_exact(&mut chunk).is_err() || size == 0 {
            return;
        }
    }
}

// ------------------------------------------------- reading what a head said

/// The `X-YT-Parameters` document of a request head, decoded.
///
/// Decoded, not matched as text: the text YSON writer quotes a string or not
/// depending on its first byte, so a mutation ID goes on the wire bare two
/// runs in five.
fn params_of(head: &str) -> YsonValue {
    let line = head
        .lines()
        .find(|line| {
            line.split_once(':')
                .is_some_and(|(name, _)| name.eq_ignore_ascii_case("x-yt-parameters"))
        })
        .unwrap_or_else(|| panic!("no X-YT-Parameters header in:\n{head}"));

    let value = line
        .split_once(':')
        .expect("the header has a value")
        .1
        .trim();
    from_slice(value.as_bytes(), YsonFormat::Text)
        .unwrap_or_else(|e| panic!("parameters are not text YSON ({e}): {value}"))
}

/// One decoded parameter, cloned out so the head can still be printed.
fn param_of(head: &str, key: &str) -> Option<YsonValue> {
    match params_of(head).node {
        YsonNode::Map(mut m) => m.remove(key.as_bytes()),
        _ => None,
    }
}

/// A string parameter's value.
fn str_param(head: &str, key: &str) -> Option<String> {
    match param_of(head, key)?.node {
        YsonNode::String(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
        _ => None,
    }
}
