//! What `execute_batch` puts on the wire, and what it makes of the answer.
//!
//! A cluster merges header and body parameters, generates a missing mutation
//! id, and answers a well-formed batch however it was sent, so these tests
//! read the bytes the client sent (the verb, where the parts travelled, which
//! mutation id is sent and repeated) and script answers a live cluster gave,
//! including one where a part fails and the rest do not.
//!
//! A mutation id goes on the wire bare or quoted depending on its first hex
//! digit, so every assertion decodes the YSON and compares values.

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
/// Every response is sent only after the **whole** request has been read,
/// head and body: replying to a body still being written closes the
/// connection under `ureq`, which then reports a broken pipe. When the script
/// runs out, the last entry keeps answering, so an extra request is caught by
/// counting rather than by a hang.
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
/// `execute_batch` always sends a `Content-Length` (`Payload::Bytes`), but the
/// proxy takes a chunked body too (*Streaming table I/O* in
/// docs/protocol-reference.md); a chunked body read as empty would be answered
/// early, a broken pipe.
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
    // The stub must read a whole body, declared by `Content-Length` or
    // chunked, before it answers.
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

    // Volatile in the registry, so a POST, with the parts in the body as the
    // C++ client sends them, where a batch cannot outgrow a header.
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

    // No concurrency was set, so none is sent.
    assert!(
        field(&sent, "concurrency").is_none(),
        "concurrency appeared from nowhere"
    );
}

#[test]
fn a_mutating_batch_carries_a_mutation_id_and_a_read_only_one_does_not() {
    // A mutating batch: the id in the header, the parts in the body, and
    // `retry=%false` on a first attempt.
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

    // A batch is mutating only if a part is, so a read-only one sends no id.
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
    // A local cluster's answer to a four-part batch (create over an existing
    // node, set with input, get, remove of nothing), timestamps trimmed. Parts
    // fail individually; the vector keeps each outcome in order.
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

    // `set` takes structured input, so its value is the part's `input` field.
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
    // The cluster spreads the batch's id over the volatile parts, so a retry
    // is safe only with the same id, and the second attempt says
    // `retry=%true`.
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
    // A raw part may name a command no mutation cache covers, so the batch is
    // sent once. The script repeats its last entry, so a retry would show as
    // a second record.
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

    // No mutation id: nothing checked that the command deduplicates.
    let sent = header_parameters(&stub.seen()[0].0);
    assert!(field(&sent, "mutation_id").is_none());
}

#[test]
fn a_bound_transaction_reaches_the_parts_and_not_the_envelope() {
    // A local cluster silently drops an envelope `transaction_id`, and the
    // part lands outside the transaction, so the id goes on every part that
    // takes one and not on the envelope.
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
        // A fixed literal, so its spelling may be compared.
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
    // Five parts, at most two per request, as the C++ client's
    // BatchPartMaxSize splits: three requests, each with the caller's
    // concurrency and its own mutation id, and one vector back in part order.
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

    // A shared id would deduplicate a later request into an earlier one's
    // answer, so every id differs.
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
    // A, B, A passes the two above.
    assert_ne!(ids[0], ids[2]);
    // And over all of them at once.
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
    // Chunk one commits, chunk two fails on a 503, and nothing rolls back.
    // The error carries the answered prefix: re-running the batch mints fresh
    // mutation ids, so its parts would land a second time.
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
    // The message says where it stopped, and why.
    let said = error.to_string();
    assert!(said.contains("2 of 5"), "{said}");
    assert!(said.contains("503"), "{said}");
    // It must not call the prefix "already applied": `answered` holds Err
    // entries that applied nothing, and the failed request may have run
    // parts it never answered for.
    assert!(
        !said.contains("already applied"),
        "the one-liner must not claim the answered prefix is what applied: {said}"
    );
    assert!(
        said.contains("not where the effects"),
        "the one-liner must say what the count is and is not: {said}"
    );

    // The third request was never sent: a stop, not a skip.
    assert_eq!(stub.seen().len(), 2);

    // A batch that fits one request keeps the plain error.
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
    // Over five chunks. Per-part ids are derived by incrementing the batch's,
    // so a repeated id across requests collides on every part, and the
    // cluster answers the later request with the earlier one's results.
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
    // The example's "a dozen tables in one round trip", counted on the wire:
    // the default part size is the C++ client's concurrency × 5.
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
    // A persisted id makes a replay after a crash deduplicate against the
    // send that may already have landed.
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
    // The second send says it is a replay; an unmarked duplicate is refused
    // rather than deduplicated.
    assert_eq!(
        field(&header_parameters(&seen[0].0), "retry").map(|v| &v.node),
        Some(&YsonNode::Boolean(false))
    );
    assert_eq!(
        field(&header_parameters(&seen[1].0), "retry").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );

    // Each part's id increments the batch's, so one id over two requests
    // collides. Refused before sending: nothing listens on this address.
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
    // A `raw` part makes the whole batch send-once; `raw_with` and
    // `Repeatable::Freely` keep a read batch retriable. The 503 is retriable,
    // so a send-once batch shows one request and this one two.
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

    // Nothing here mutates, so no mutation id.
    let sent = header_parameters(&stub.seen()[0].0);
    assert!(field(&sent, "mutation_id").is_none());
}

#[test]
fn an_empty_batch_is_refused_before_anything_is_sent() {
    // A cluster answers an empty batch 200 with no results. Nothing listens on
    // this address, so a Config error shows the check ran first.
    let client = Client::new("http://127.0.0.1:1").with_retries(RetryPolicy::none());
    let error = client
        .execute_batch(&BatchRequest::new())
        .expect_err("an empty batch is not a request worth sending");
    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
}

#[test]
fn an_answer_shaped_like_nothing_known_fails_the_call_loudly() {
    // Answer envelopes vary (`exists` answers under `value`, the file cache a
    // bare string), so a shape the parser does not recognise is refused, not
    // read as an empty success.
    let stub = Stub::serving(vec![(200, br#"{"results"=[{"outcome"={}}]}"#.to_vec())]);

    let mut batch = BatchRequest::new();
    batch.exists("//tmp/t");

    let error = once(&stub)
        .execute_batch(&batch)
        .expect_err("an unknown result shape must not pass");
    assert!(matches!(error, ClientError::Decode { .. }), "{error:?}");
    assert!(error.to_string().contains("outcome"), "{error}");
}
