//! The detach/attach lifecycle, on the wire.
//!
//! A cluster cannot distinguish a handle that aborted from one that detached
//! and expired — both transactions end. What separates them is *which requests
//! were sent*, so these tests serve the cluster's side from a socket
//! in-process and read what the client put on the wire: a detach must send
//! nothing, an attached handle's drop must send nothing, a started handle's
//! drop must still send the abort, and the by-id commands must carry the id
//! they were given.
//!
//! The stub is `combination.rs`'s, specialised: it stays up, **reads every
//! request in full before answering** (replying into a body still being
//! written closes the connection under `ureq`, which passes on macOS and fails
//! on a Linux runner), and remembers the request heads in order. Parameters
//! are asserted by decoding `X-YT-Parameters`, never by matching the rendered
//! text of a generated value — a mutation ID's spelling depends on its first
//! hex digit.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ytsaurus_client::{Client, RetryPolicy};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice};

/// The id every stub transaction answers with. A fixed literal, so asserting
/// on it is safe where asserting on a generated value's spelling is not.
const TX: &str = "3-5bc70-10001-387a";

// ------------------------------------------------------------- the lifecycle

#[test]
fn a_started_handles_drop_still_aborts() {
    // The behaviour that must survive this feature: dropping a transaction
    // this process started goes on aborting it. It is what makes `?` safe
    // inside a transaction, and examples/transaction.rs is built on it.
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
    // A 3 s transaction is pinged every second, so 1.6 s of post-detach
    // silence covers more than one interval: if the thread were still running,
    // a ping would land in it.
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
fn detach_with_a_ping_in_flight_neither_panics_nor_aborts() {
    // The race the join in `detach` exists for: the keep-alive thread is
    // mid-request when the handle detaches. The stub answers pings 500 ms
    // late, and the test waits until one has *arrived* before detaching, so
    // the ping is reliably in flight while detach runs. Detach must wait it
    // out — no request may start after it returns — and must send nothing.
    let cluster = StubCluster::answering(Answers {
        ping_delay: Duration::from_millis(500),
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

    let id = tx.detach(); // the ping is being answered, 500 ms late
    assert_eq!(id, TX);

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
    // 3000 ms of timeout means a ping every second; one must land within a
    // few, carrying the id. Dropping the handle must stop them and must not
    // abort — an attached handle detaches on drop, as the C++ destructor does.
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

    // The drop does not join the thread, so one ping may already be in
    // flight; let it land, then require silence for more than an interval.
    std::thread::sleep(Duration::from_millis(300));
    let settled = cluster.request_count();
    std::thread::sleep(Duration::from_millis(1300));

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
fn an_attached_handle_commits_like_an_owner() {
    // Only the drop differs between attached and started; an explicit commit
    // is the same commit, mutation ID included. 30 s of timeout keeps the
    // ping thread quiet for the duration of the test.
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
    // The timeout read is what fails — before any handle exists, so no ping
    // thread is left behind pinging a transaction that is not there. The
    // error must name the operation and the id itself, because the cluster's
    // answer does not always do either: a garbage id earns `Unknown cell tag
    // 0` on a real cluster, with no id and no mention of a transaction in it.
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
    // The by-id triple: what a process that holds nothing but the id sends.
    // All three are POSTs to the v4 names, all three carry the id they were
    // given, and the commit — the one that is not idempotent — carries a
    // mutation id whose *presence* is asserted, never its spelling.
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
}

// ------------------------------------------------------------------ the stub

/// What the stub cluster answers with.
struct Answers {
    /// What `get #<id>/@timeout` answers, in milliseconds.
    timeout_ms: i64,
    /// How long a ping is held before it is answered — how a ping is kept
    /// reliably in flight while the test does something else.
    ping_delay: Duration,
    /// Whether the transaction is gone: `get` then answers the resolve error
    /// a local cluster gives for an id that names nothing.
    missing: bool,
}

impl Default for Answers {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            ping_delay: Duration::ZERO,
            missing: false,
        }
    }
}

impl Answers {
    fn answer(&self, head: &str) -> Vec<u8> {
        let path = head
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        match path.as_str() {
            "/api/v4/start_transaction" => ok(format!(r#"{{"transaction_id"="{TX}"}}"#).as_bytes()),
            "/api/v4/get" if self.missing => resolve_error(),
            "/api/v4/get" => ok(format!(r#"{{"value"={}}}"#, self.timeout_ms).as_bytes()),
            "/api/v4/ping_transaction" => {
                std::thread::sleep(self.ping_delay);
                ok(b"{}")
            }
            _ => ok(b"{}"),
        }
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

        // The whole request before any answer — head *and* body — or `ureq`
        // reports a broken pipe instead of the reply on a Linux runner.
        if let Some(length) = content_length(&head) {
            let mut body = vec![0_u8; length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
        } else if head.to_lowercase().contains("transfer-encoding: chunked") {
            drain_chunked(&mut reader);
        }

        // Recorded before the answer is computed, so a test can see a ping
        // *arrive* and act while the stub is still holding the reply.
        seen.lock()
            .expect("nothing panicked holding it")
            .push(head.clone());

        let answer = answers.answer(&head);
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

/// What a local cluster answers `get #<gone>/@timeout` with, captured verbatim
/// (noise attributes trimmed): HTTP 200 carrying the structured error in
/// `X-YT-Error`, the resolve error outside, `No such object` inside — **not**
/// `No such transaction`, which is what a ping of the same id earns.
fn resolve_error() -> Vec<u8> {
    let document = r#"{"code":500,"message":"Error resolving path #0-0-0-1/@timeout","inner_errors":[{"code":500,"message":"No such object 0-0-0-1","attributes":{"missing_object_id":"0-0-0-1"}}]}"#;
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
/// Decoded rather than matched as text, because the spelling of a *generated*
/// value is not stable: the text YSON writer quotes a string or not depending
/// on its first byte, so a mutation ID goes on the wire bare two runs in five.
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
