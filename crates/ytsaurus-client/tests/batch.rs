//! What `execute_batch` puts on the wire, and what it makes of the answer.
//!
//! A cluster is too forgiving to notice most of what matters here: it merges
//! parameters from the header and the body, generates a mutation id when none
//! is sent, and answers a well-formed batch the same way however the request
//! was dressed. These tests serve the request from a socket in-process and
//! read the bytes the client sent — the verb, where the parts travelled, the
//! mutation id that must repeat across a retry and the one that must not be
//! there at all — and script answers a live cluster gave, including the one
//! where a part fails and the rest do not.
//!
//! The rule from `tests::sent_parameters` applies throughout: never assert on
//! the rendered text of a *generated* value. A mutation id goes on the wire
//! bare or quoted depending on its first hex digit, so every assertion here
//! decodes the YSON and compares values.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ytsaurus_client::{BatchRequest, Client, ClientError, MutationId, RetryPolicy, yson_build};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice};

/// One captured request: the head as text, the body as bytes.
type Seen = (String, Vec<u8>);

/// A stand-in proxy that answers from a script and remembers what it was
/// asked.
///
/// Every response is sent only after the **whole** request has been read —
/// head and body — because replying to a body still being written closes the
/// connection under `ureq`, which then reports a broken pipe instead of the
/// request under test; `request_shape.rs` learned that the hard way. When the
/// script runs out, the last entry keeps answering, so a client that sends
/// more requests than the test expects is caught by counting rather than by a
/// hang.
struct Stub {
    address: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Stub {
    fn serving(script: Vec<(u16, Vec<u8>)>) -> Self {
        assert!(!script.is_empty(), "a stub needs something to answer");
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let cursor = Arc::new(Mutex::new(0_usize));

        let recorded = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let script = script.clone();
                let seen = Arc::clone(&recorded);
                let cursor = Arc::clone(&cursor);
                std::thread::spawn(move || serve(stream, &script, &cursor, &seen));
            }
        });

        Self { address, seen }
    }

    fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Everything served so far, in arrival order.
    fn seen(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .expect("nothing panicked holding it")
            .clone()
    }
}

/// Answers requests on one connection until the client hangs up.
fn serve(
    mut stream: TcpStream,
    script: &[(u16, Vec<u8>)],
    cursor: &Mutex<usize>,
    seen: &Mutex<Vec<Seen>>,
) {
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

        // The whole body first — see the type-level comment.
        let Some(body) = read_body(&head, &mut reader) else {
            return;
        };

        seen.lock()
            .expect("nothing panicked holding it")
            .push((head, body));

        let (status, reply_body) = {
            let mut at = cursor.lock().expect("nothing panicked holding it");
            let entry = script[(*at).min(script.len() - 1)].clone();
            *at += 1;
            entry
        };

        let reply = format!(
            "HTTP/1.1 {status} .\r\nContent-Length: {}\r\nContent-Type: application/x-yt-yson-text\r\n\r\n",
            reply_body.len()
        );
        if stream.write_all(reply.as_bytes()).is_err() || stream.write_all(&reply_body).is_err() {
            return;
        }
        stream.flush().ok();
    }
}

/// Reads a request body to its end, however the head said it was framed.
///
/// `None` means the connection died mid-body and there is nothing to answer.
///
/// Both framings, though `execute_batch` only ever sends the first: it renders
/// the whole batch before sending, so the body is always `Payload::Bytes` and
/// always declares a `Content-Length`. Reading only that one would make a
/// chunked body silently arrive empty and be answered early — the failure the
/// type-level comment says closes the connection under `ureq` and reports a
/// broken pipe instead of the request under test. The proxy takes a chunked
/// body (see the streaming rule in AGENTS.md), so this is the shape a future
/// batch could grow, and a stub that could not read it would fail confusingly
/// rather than usefully.
fn read_body(head: &str, reader: &mut impl BufRead) -> Option<Vec<u8>> {
    if header(head, "transfer-encoding")
        .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
    {
        let mut body = Vec::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).ok()?;
            let size = usize::from_str_radix(line.trim().split(';').next()?, 16).ok()?;
            let mut chunk = vec![0_u8; size + 2]; // the chunk, then CRLF
            reader.read_exact(&mut chunk).ok()?;
            if size == 0 {
                return Some(body);
            }
            body.extend_from_slice(&chunk[..size]);
        }
    }

    let mut body = vec![0_u8; content_length(head).unwrap_or(0)];
    reader.read_exact(&mut body).ok()?;
    Some(body)
}

/// One header of a captured request head, lowercased for matching.
fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}

/// The declared body length of a request, if it declared one.
fn content_length(head: &str) -> Option<usize> {
    header(head, "content-length").and_then(|value| value.parse().ok())
}

/// The `X-YT-Parameters` header of a captured request, decoded.
fn header_parameters(head: &str) -> YsonValue {
    let value = header(head, "x-yt-parameters")
        .unwrap_or_else(|| panic!("no X-YT-Parameters header in:\n{head}"));
    from_slice(value.as_bytes(), YsonFormat::Text)
        .unwrap_or_else(|e| panic!("parameters are not text YSON ({e}): {value}"))
}

/// A captured request body, decoded as text YSON.
fn body_document(body: &[u8]) -> YsonValue {
    from_slice(body, YsonFormat::Text).unwrap_or_else(|e| {
        panic!(
            "the body is not text YSON ({e}): {}",
            String::from_utf8_lossy(body)
        )
    })
}

/// One entry of a decoded YSON dict, without the panicking `Index`.
fn field<'a>(value: &'a YsonValue, key: &str) -> Option<&'a YsonValue> {
    match &value.node {
        YsonNode::Map(m) => m.get(key.as_bytes()),
        _ => None,
    }
}

/// The `requests` list of a decoded batch body.
fn requests_of(body: &YsonValue) -> Vec<YsonValue> {
    match field(body, "requests").map(|value| &value.node) {
        Some(YsonNode::List(items)) => items.clone(),
        other => panic!("the body carries no requests list: {other:?}"),
    }
}

/// A stub answer: 200 with `{results=[…]}` around the given items.
fn results(items: &[&str]) -> (u16, Vec<u8>) {
    (
        200,
        format!("{{\"results\"=[{};]}}", items.join(";")).into_bytes(),
    )
}

/// A once-only client: no retries, so a request seen twice is the client's
/// doing and not the policy's.
fn once(stub: &Stub) -> Client {
    Client::new(&stub.url()).with_retries(RetryPolicy::none())
}

#[test]
fn the_stub_reads_a_body_however_it_is_framed() {
    // The stub answers only once the whole request is in, and until now
    // "whole" meant whatever `Content-Length` said. `execute_batch` always
    // sends that shape, so a chunked body would have arrived empty and been
    // answered early — a broken pipe in place of the request under test.
    // Neither framing is guessed at now, and this is what proves it.
    let head = "POST /api/v4/execute_batch HTTP/1.1\r\nContent-Length: 5\r\n";
    assert_eq!(
        read_body(head, &mut &b"hello and then some"[..]),
        Some(b"hello".to_vec())
    );

    let head = "POST /api/v4/execute_batch HTTP/1.1\r\nTransfer-Encoding: chunked\r\n";
    assert_eq!(
        read_body(head, &mut &b"5\r\nhello\r\n3\r\n th\r\n0\r\n\r\n"[..]),
        Some(b"hello th".to_vec()),
        "the terminating zero chunk ends the body, and no more is read"
    );

    // No framing at all is a bodiless request, not a read that blocks.
    let head = "GET /api/v4/get HTTP/1.1\r\n";
    assert_eq!(read_body(head, &mut &b""[..]), Some(Vec::new()));
}

#[test]
fn a_batch_is_one_post_with_its_parts_in_the_body() {
    let stub = Stub::serving(vec![results(&[
        r#"{"output"={"node_id"="1-2-3-4"}}"#,
        r#"{"output"={"value"=%true}}"#,
    ])]);

    let mut batch = BatchRequest::new();
    batch.create("table", "//tmp/t").exists("//tmp/t");
    once(&stub).execute_batch(&batch).expect("executes");

    let seen = stub.seen();
    assert_eq!(seen.len(), 1, "a batch is the round trip it saves");
    let (head, body) = &seen[0];

    // Volatile in the cluster's registry, so a POST — and a bare command
    // path, with the parts in the body rather than the parameter header,
    // which is where the C++ client puts them and where a batch's size
    // cannot outgrow a header.
    assert!(
        head.starts_with("POST /api/v4/execute_batch HTTP/1.1"),
        "{head}"
    );

    let sent = body_document(body);
    let requests = requests_of(&sent);
    assert_eq!(requests.len(), 2);

    // Each typed part sends exactly what its `Client` namesake sends.
    assert_eq!(
        field(&requests[0], "command").and_then(YsonValue::as_str),
        Some("create")
    );
    let create = field(&requests[0], "parameters").expect("create has parameters");
    assert_eq!(
        field(create, "path").and_then(YsonValue::as_str),
        Some("//tmp/t")
    );
    assert_eq!(
        field(create, "type").and_then(YsonValue::as_str),
        Some("table")
    );
    assert_eq!(
        field(create, "recursive").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );
    assert_eq!(
        field(create, "ignore_existing").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );

    assert_eq!(
        field(&requests[1], "command").and_then(YsonValue::as_str),
        Some("exists")
    );

    // Nothing the caller did not say: no concurrency was set, so none is
    // sent and the cluster's own default applies.
    assert!(
        field(&sent, "concurrency").is_none(),
        "concurrency appeared from nowhere"
    );
}

#[test]
fn a_mutating_batch_carries_a_mutation_id_and_a_read_only_one_does_not() {
    // The mutating half: the id in the header, the parts in the body, merged
    // by the proxy into one parameter set — and `retry=%false` on a first
    // attempt.
    let stub = Stub::serving(vec![results(&[r#"{"output"={"node_id"="1-2-3-4"}}"#])]);
    let mut mutating = BatchRequest::new();
    mutating.create("table", "//tmp/t");
    once(&stub).execute_batch(&mutating).expect("executes");

    let (head, _) = &stub.seen()[0];
    let sent = header_parameters(head);
    assert!(
        field(&sent, "mutation_id")
            .and_then(YsonValue::as_str)
            .is_some(),
        "a mutating batch must be replayable under an id:\n{head}"
    );
    assert_eq!(
        field(&sent, "retry").map(|v| &v.node),
        Some(&YsonNode::Boolean(false)),
        "{head}"
    );

    // The read-only half: such a batch mutates nothing — "mutating if the
    // set includes mutating commands" — so there is nothing for an id to
    // deduplicate and none is sent.
    let stub = Stub::serving(vec![results(&[
        r#"{"output"={"value"=%true}}"#,
        r#"{"output"={"value"={}}}"#,
    ])]);
    let mut reads = BatchRequest::new();
    reads.exists("//tmp/t").get("//tmp/t/@type");
    once(&stub).execute_batch(&reads).expect("executes");

    let (head, _) = &stub.seen()[0];
    let sent = header_parameters(head);
    assert!(field(&sent, "mutation_id").is_none(), "{head}");
    assert!(field(&sent, "retry").is_none(), "{head}");
}

#[test]
fn one_part_fails_and_the_rest_succeed_in_order() {
    // The answer a local cluster actually gave to a four-part batch —
    // create over an existing node, set with input, get, remove of nothing —
    // trimmed of its timestamps. A batch's parts fail individually, and the
    // vector must keep both the sides and the order.
    let stub = Stub::serving(vec![results(&[
        r#"{"error"={"code"=501;"message"="Node //tmp/impl-batch-a already exists";"attributes"={"host"="localhost"}}}"#,
        r#"{"output"={}}"#,
        r#"{"output"={"value"="table"}}"#,
        r#"{"error"={"code"=500;"message"="Node //tmp has no child with key \"impl-batch-nothing-here\"";"attributes"={"host"="localhost"}}}"#,
    ])]);

    let mut batch = BatchRequest::new();
    batch
        .create("table", "//tmp/impl-batch-a")
        .set_attribute("//tmp/impl-batch-b", "note", yson_build::string("hello"))
        .get("//tmp/impl-batch-b/@type")
        .remove("//tmp/impl-batch-nothing-here");

    let parts = once(&stub)
        .execute_batch(&batch)
        .expect("the envelope succeeded");
    assert_eq!(parts.len(), 4);

    let first = parts[0].as_ref().expect_err("the create failed");
    let ClientError::Cluster {
        command,
        code,
        message,
        ..
    } = first
    else {
        panic!("a part failure is a cluster error: {first:?}");
    };
    assert_eq!(command, "create");
    assert_eq!(*code, 501);
    assert!(message.contains("already exists"), "{message}");

    assert!(parts[1].is_ok(), "the set succeeded: {:?}", parts[1]);
    assert_eq!(
        parts[2].as_ref().expect("the get succeeded")["value"].as_str(),
        Some("table")
    );
    let last = parts[3].as_ref().expect_err("the remove failed");
    assert!(
        matches!(last, ClientError::Cluster { code: 500, .. }),
        "{last:?}"
    );

    // And the set's input travelled in the part, not in the request body's
    // own stream: `set` is a structured-input command, so its value is the
    // part's `input` field.
    let (_, body) = &stub.seen()[0];
    let requests = requests_of(&body_document(body));
    assert_eq!(
        field(&requests[1], "input").and_then(YsonValue::as_str),
        Some("hello")
    );
    assert_eq!(
        field(&requests[1], "command").and_then(YsonValue::as_str),
        Some("set")
    );
}

#[test]
fn a_retried_batch_keeps_its_mutation_id_and_admits_to_the_replay() {
    // What makes the retry safe is the cluster spreading the batch's id over
    // the volatile parts, and that only works if the replay carries the same
    // id — so the id must not change between attempts, and the second must
    // say `retry=%true`. The values are compared decoded, never as text: a
    // mutation id renders bare or quoted depending on its first hex digit.
    let stub = Stub::serving(vec![
        (503, Vec::new()),
        results(&[r#"{"output"={"node_id"="1-2-3-4"}}"#]),
    ]);

    let mut batch = BatchRequest::new();
    batch.create("table", "//tmp/t");

    let parts = Client::new(&stub.url())
        .with_retries(RetryPolicy::new(2, Duration::ZERO, Duration::ZERO))
        .execute_batch(&batch)
        .expect("the second attempt succeeded");
    assert_eq!(parts.len(), 1);
    assert!(parts[0].is_ok());

    let seen = stub.seen();
    assert_eq!(seen.len(), 2, "one failure, one retry");

    let first = header_parameters(&seen[0].0);
    let second = header_parameters(&seen[1].0);

    let id = field(&first, "mutation_id")
        .and_then(YsonValue::as_str)
        .expect("the first attempt carries an id");
    assert_eq!(
        field(&second, "mutation_id").and_then(YsonValue::as_str),
        Some(id),
        "the id is what the cluster deduplicates by, so it must not change"
    );
    assert_eq!(
        field(&first, "retry").map(|v| &v.node),
        Some(&YsonNode::Boolean(false))
    );
    assert_eq!(
        field(&second, "retry").map(|v| &v.node),
        Some(&YsonNode::Boolean(true)),
        "an unmarked duplicate is refused, not deduplicated"
    );

    // The parts travelled identically both times.
    assert_eq!(seen[0].1, seen[1].1, "a replay is the same request");
}

#[test]
fn a_batch_with_a_raw_part_is_sent_once_whatever_the_policy_says() {
    // A raw part may name a command no mutation cache covers, so a replay
    // could apply it twice. The stub would happily serve a second request —
    // the script repeats its last entry — so a retried batch shows up as a
    // second record, not as a hang.
    let stub = Stub::serving(vec![(503, Vec::new())]);

    let mut batch = BatchRequest::new();
    batch.create("table", "//tmp/t");
    batch
        .raw(
            "parse_ypath",
            yson_build::map([("path", yson_build::string("//tmp/t"))]),
            None,
        )
        .expect("a fine command name");

    let error = Client::new(&stub.url())
        .with_retries(RetryPolicy::new(5, Duration::ZERO, Duration::ZERO))
        .execute_batch(&batch)
        .expect_err("503 with nothing to retry into");
    assert!(
        matches!(error, ClientError::Http { status: 503, .. }),
        "{error:?}"
    );

    assert_eq!(
        stub.seen().len(),
        1,
        "a batch this client cannot classify must not be replayed"
    );

    // And no mutation id: an id would promise a deduplication nobody checked.
    let sent = header_parameters(&stub.seen()[0].0);
    assert!(field(&sent, "mutation_id").is_none());
}

#[test]
fn a_bound_transaction_reaches_the_parts_and_not_the_envelope() {
    // The envelope has no transaction to be in — a local cluster drops an
    // outer transaction_id in silence, and the part then lands outside the
    // transaction. So the id must be on every part that can take one, and
    // not on the envelope, where it would only dress the request up in a
    // parameter known to mean nothing.
    let stub = Stub::serving(vec![results(&[
        r#"{"output"={"node_id"="1-2-3-4"}}"#,
        r#"{"output"={"value"=%true}}"#,
    ])]);

    let mut batch = BatchRequest::new();
    batch.create("table", "//tmp/t").exists("//tmp/t");

    once(&stub)
        .with_transaction("3-5d231-10001-db88")
        .execute_batch(&batch)
        .expect("executes");

    let (head, body) = &stub.seen()[0];
    for request in requests_of(&body_document(body)) {
        let parameters = field(&request, "parameters").expect("every part has parameters");
        // A fixed literal, so comparing the value is comparing the spelling.
        assert_eq!(
            field(parameters, "transaction_id").and_then(YsonValue::as_str),
            Some("3-5d231-10001-db88"),
            "a part escaped the transaction:\n{head}"
        );
    }

    let envelope = header_parameters(head);
    assert!(
        field(&envelope, "transaction_id").is_none(),
        "the envelope wore a transaction_id the cluster is known to drop:\n{head}"
    );
}

#[test]
fn a_big_batch_is_split_and_the_results_stitched_back_in_order() {
    // Five parts, at most two per request: the C++ client's BatchPartMaxSize
    // behaviour. Three requests, each carrying the concurrency the caller
    // set, each under its own mutation id — and one vector back, in part
    // order, as if nothing had been split.
    let stub = Stub::serving(vec![
        results(&[
            r#"{"output"={"node_id"="0-0-0-0"}}"#,
            r#"{"output"={"node_id"="0-0-0-1"}}"#,
        ]),
        results(&[
            r#"{"output"={"node_id"="0-0-0-2"}}"#,
            r#"{"error"={"code"=501;"message"="Node //tmp/t3 already exists"}}"#,
        ]),
        results(&[r#"{"output"={"node_id"="0-0-0-4"}}"#]),
    ]);

    let mut batch = BatchRequest::new()
        .with_concurrency(2)
        .with_max_part_size(2);
    for index in 0..5 {
        batch.create("table", &format!("//tmp/t{index}"));
    }

    let parts = once(&stub).execute_batch(&batch).expect("executes");

    let seen = stub.seen();
    assert_eq!(seen.len(), 3, "five parts at two per request");
    let sizes: Vec<usize> = seen
        .iter()
        .map(|(_, body)| requests_of(&body_document(body)).len())
        .collect();
    assert_eq!(sizes, [2, 2, 1]);

    // Each request says the concurrency the caller set.
    for (_, body) in &seen {
        assert_eq!(
            field(&body_document(body), "concurrency").and_then(YsonValue::as_i64),
            Some(2)
        );
    }

    // Each request is its own replayable unit, so the ids must differ —
    // one id across two different part sets would deduplicate the second
    // request into the first's answer.
    let ids: Vec<String> = seen
        .iter()
        .map(|(head, _)| {
            field(&header_parameters(head), "mutation_id")
                .and_then(YsonValue::as_str)
                .expect("every chunk carries an id")
                .to_owned()
        })
        .collect();
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
    // Pairwise-adjacent is not distinct: three ids of A, B, A would pass the
    // two above and still deduplicate the third request into the first's
    // answer.
    assert_ne!(ids[0], ids[2]);
    // And the property stated once, over all of them, so that no single
    // deleted line can take the guarantee with it: an A,B,A recycling
    // implementation has to fail here too.
    let distinct: std::collections::HashSet<&String> = ids.iter().collect();
    assert_eq!(
        distinct.len(),
        ids.len(),
        "every request needs its own mutation id, got {ids:?}"
    );

    // Stitched back in part order, failure and all.
    assert_eq!(parts.len(), 5);
    for (index, part) in parts.iter().enumerate() {
        if index == 3 {
            assert!(part.is_err(), "part 3 failed on the cluster");
        } else {
            assert_eq!(
                part.as_ref().expect("created")["node_id"].as_str(),
                Some(format!("0-0-0-{index}").as_str())
            );
        }
    }
}

#[test]
fn a_split_batch_that_stops_hands_back_the_parts_that_already_applied() {
    // The failure the split makes possible and no rollback undoes: chunk one
    // commits, chunk two exhausts itself on a 503. Reporting only the 503
    // would tell the caller nothing about the two tables that now exist —
    // and re-running the batch is not a recovery, because a second execution
    // mints fresh mutation ids and the parts land a second time.
    let stub = Stub::serving(vec![
        results(&[
            r#"{"output"={"node_id"="0-0-0-0"}}"#,
            r#"{"output"={"node_id"="0-0-0-1"}}"#,
        ]),
        (503, Vec::new()),
    ]);

    let mut batch = BatchRequest::new().with_max_part_size(2);
    for index in 0..5 {
        batch.create("table", &format!("//tmp/t{index}"));
    }

    let error = once(&stub)
        .execute_batch(&batch)
        .expect_err("the second request failed");

    let ClientError::BatchInterrupted {
        answered,
        parts,
        cause,
    } = &error
    else {
        panic!("a stopped split batch must carry its prefix: {error:?}");
    };

    assert_eq!(*parts, 5, "the batch held five parts");
    assert_eq!(answered.len(), 2, "one request's worth had been answered");
    for (index, part) in answered.iter().enumerate() {
        assert_eq!(
            part.as_ref().expect("created")["node_id"].as_str(),
            Some(format!("0-0-0-{index}").as_str()),
            "the prefix keeps the parts' own answers"
        );
    }
    assert!(
        matches!(**cause, ClientError::Http { status: 503, .. }),
        "{cause:?}"
    );
    // The message says both halves: where it stopped, and why.
    let said = error.to_string();
    assert!(said.contains("2 of 5"), "{said}");
    assert!(said.contains("503"), "{said}");
    // And it must not claim the count is a line the cluster honours. This is
    // the sentence that reaches a log and an unwrap() panic, and `answered`
    // holds Err entries that applied nothing while the request that failed
    // ran every part it never answered for. A message asserting the prefix is
    // "already applied" is the one that gets a caller to corrupt state.
    assert!(
        !said.contains("already applied"),
        "the one-liner must not claim the answered prefix is what applied: {said}"
    );
    assert!(
        said.contains("not where the effects"),
        "the one-liner must say what the count is and is not: {said}"
    );

    // The third request was never sent — this is a stop, not a skip.
    assert_eq!(stub.seen().len(), 2);

    // And a batch that fits one request keeps the plain error: there is no
    // prefix to report, so there is nothing to wrap.
    let stub = Stub::serving(vec![(503, Vec::new())]);
    let mut single = BatchRequest::new();
    single.create("table", "//tmp/t");
    let error = once(&stub)
        .execute_batch(&single)
        .expect_err("the only request failed");
    assert!(
        matches!(error, ClientError::Http { status: 503, .. }),
        "{error:?}"
    );
}

#[test]
fn every_request_of_a_split_batch_gets_its_own_mutation_id() {
    // Stated on its own, over more chunks than the split test uses, because
    // the property had exactly one assertion covering it: an implementation
    // that recycled ids as A,B,A passed the whole workspace with that single
    // line deleted. Recycling any id across requests would have the cluster
    // answer the later request with the earlier request's results — the
    // per-part ids are derived by incrementing the batch's, so a repeat is a
    // collision across every part of both.
    let answers = vec![r#"{"output"={"node_id"="0-0-0-0"}}"#; 2];
    let stub = Stub::serving(vec![results(&answers); 5]);

    let mut batch = BatchRequest::new().with_max_part_size(2);
    for index in 0..10 {
        batch.create("table", &format!("//tmp/t{index}"));
    }
    once(&stub).execute_batch(&batch).expect("executes");

    let seen = stub.seen();
    assert_eq!(seen.len(), 5, "ten parts at two per request");

    let ids: Vec<String> = seen
        .iter()
        .map(|(head, _)| {
            field(&header_parameters(head), "mutation_id")
                .and_then(YsonValue::as_str)
                .expect("every chunk carries an id")
                .to_owned()
        })
        .collect();

    let distinct: std::collections::HashSet<&String> = ids.iter().collect();
    assert_eq!(
        distinct.len(),
        ids.len(),
        "every request needs its own mutation id, got {ids:?}"
    );
}

#[test]
fn a_dozen_creates_are_one_request() {
    // The example's headline claim — "a dozen tables in one round trip" — is
    // a wire fact, and a program on a cluster cannot see it. This can: the
    // default part size is the C++ client's concurrency × 5, so a dozen parts
    // are nowhere near a split, and the stub counts what arrived.
    let answers = vec![r#"{"output"={"node_id"="0-0-0-0"}}"#; 12];
    let stub = Stub::serving(vec![results(&answers)]);

    let mut batch = BatchRequest::new();
    for index in 0..12 {
        batch.create("table", &format!("//tmp/ytsaurus_rs_batch/t{index}"));
    }

    let made = once(&stub).execute_batch(&batch).expect("executes");
    assert_eq!(made.len(), 12, "twelve parts, twelve answers");

    let seen = stub.seen();
    assert_eq!(seen.len(), 1, "twelve commands, one round trip");
    assert_eq!(requests_of(&body_document(&seen[0].1)).len(), 12);
}

#[test]
fn a_caller_supplied_mutation_id_is_the_one_that_goes_out() {
    // The crash-replay guarantee, expressible through the batch API rather
    // than only through `raw_command_with`: persist the id, and the replay is
    // deduplicated against the send that may already have landed.
    let stub = Stub::serving(vec![results(&[r#"{"output"={"node_id"="1-2-3-4"}}"#])]);

    let mut batch = BatchRequest::new();
    batch.create("table", "//tmp/t");

    let id = MutationId::new();
    let client = once(&stub);
    client
        .execute_batch_with(&batch, Some(&id))
        .expect("executes");
    client
        .execute_batch_with(&batch, Some(&id.as_retry()))
        .expect("executes");

    let seen = stub.seen();
    assert_eq!(seen.len(), 2);
    for (head, _) in &seen {
        assert_eq!(
            field(&header_parameters(head), "mutation_id").and_then(YsonValue::as_str),
            Some(id.as_str()),
            "the caller's id is what went on the wire:\n{head}"
        );
    }
    // The first send is not a replay and the second says that it is — an
    // unmarked duplicate is refused rather than deduplicated.
    assert_eq!(
        field(&header_parameters(&seen[0].0), "retry").map(|v| &v.node),
        Some(&YsonNode::Boolean(false))
    );
    assert_eq!(
        field(&header_parameters(&seen[1].0), "retry").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );

    // An id would be a lie across a split: the cluster derives each part's id
    // by incrementing the batch's, so a second request under the same id
    // collides with the first request's parts. Refused before anything is
    // sent — nothing listens on this address.
    let mut split = BatchRequest::new().with_max_part_size(1);
    split.create("table", "//tmp/a").create("table", "//tmp/b");
    let error = Client::new("http://127.0.0.1:1")
        .with_retries(RetryPolicy::none())
        .execute_batch_with(&split, Some(&MutationId::new()))
        .expect_err("one id cannot cover two requests");
    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
    assert!(error.to_string().contains("with_max_part_size"), "{error}");
}

#[test]
fn a_raw_read_a_caller_vouches_for_leaves_the_batch_retriable() {
    // `raw` alone means send-once, and one raw part decides the whole batch —
    // so an all-read batch with a `check_permission` in it was demoted to a
    // single attempt. A caller who knows the command's registry bits says so,
    // and the batch is retried like the reads it is made of. The 503 here is
    // retriable, so a send-once batch shows up as one request and a `Freely`
    // one as two.
    let stub = Stub::serving(vec![
        (503, Vec::new()),
        results(&[
            r#"{"output"={"value"=%true}}"#,
            r#"{"output"={"action"="allow"}}"#,
        ]),
    ]);

    let mut batch = BatchRequest::new();
    batch.exists("//tmp/t");
    batch
        .raw_with(
            "check_permission",
            yson_build::map([
                ("user", yson_build::string("root")),
                ("path", yson_build::string("//tmp/t")),
                ("permission", yson_build::string("read")),
            ]),
            None,
            ytsaurus_client::Repeatable::Freely,
        )
        .expect("a fine command name");

    let parts = Client::new(&stub.url())
        .with_retries(RetryPolicy::new(2, Duration::ZERO, Duration::ZERO))
        .execute_batch(&batch)
        .expect("the second attempt succeeded");
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(Result::is_ok), "{parts:?}");

    assert_eq!(stub.seen().len(), 2, "the batch is a read and was retried");

    // Still no mutation id: nothing here mutates, so there is nothing for one
    // to deduplicate.
    let sent = header_parameters(&stub.seen()[0].0);
    assert!(field(&sent, "mutation_id").is_none());
}

#[test]
fn an_empty_batch_is_refused_before_anything_is_sent() {
    // Nothing listens on this address, so a Config error is proof the check
    // ran first. The cluster would answer an empty batch with 200 and no
    // results, which is a no-op reported as work done.
    let client = Client::new("http://127.0.0.1:1").with_retries(RetryPolicy::none());
    let error = client
        .execute_batch(&BatchRequest::new())
        .expect_err("an empty batch is not a request worth sending");
    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
}

#[test]
fn an_answer_shaped_like_nothing_known_fails_the_call_loudly() {
    // The envelope traps documented in AGENTS.md — `exists` under `value`,
    // the file cache answering a bare string — earn the batch parser its
    // paranoia: a shape it does not recognise is refused, not read as an
    // empty success.
    let stub = Stub::serving(vec![(200, br#"{"results"=[{"outcome"={}}]}"#.to_vec())]);

    let mut batch = BatchRequest::new();
    batch.exists("//tmp/t");

    let error = once(&stub)
        .execute_batch(&batch)
        .expect_err("an unknown result shape must not pass");
    assert!(matches!(error, ClientError::Decode { .. }), "{error:?}");
    assert!(error.to_string().contains("outcome"), "{error}");
}
