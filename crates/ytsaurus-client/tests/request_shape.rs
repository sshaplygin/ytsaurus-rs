//! What the client actually puts on the wire.
//!
//! Everything else in this crate is checked against a cluster, which answers
//! the same whether or not the request was well made. These serve the request
//! from a socket in-process and read the bytes the client sent, which is the
//! only way to pin the things a cluster is too forgiving to notice:
//! compression the client asks for, the token it carries, and the header the
//! parameters travel in.
//!
//! The last section is a second question — not what a request looked like but
//! *which address* it went to, which no single listener can answer. It uses
//! two, and [`Proxy`] rather than [`capture`].

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;

use ytsaurus_client::{
    Client, ClientError, DataFormat, Method, OperationFilter, OperationParameters, RetryPolicy,
    SkiffFormat, SkiffSchema, SkiffSchemaRef, SkiffWireType, TablePath, TraceContext, yson_build,
};

/// Serves exactly one request and returns its headers as text.
///
/// The reply is a valid `exists` answer, so the client finishes normally and
/// nothing retries into a second connection this listener would never accept.
fn capture(request_from: impl FnOnce(&str)) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let address = listener.local_addr().expect("has an address");

    let served = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accepts");
        let mut reader = BufReader::new(stream.try_clone().expect("clones"));

        let mut head = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) if line == "\r\n" => break,
                Ok(_) => head.push_str(&line),
                Err(_) => break,
            }
        }

        // A command with a body — a table write — is only finished sending
        // when its body is read. Replying before then leaves the client writing
        // into a socket nobody is reading, which surfaces as a broken pipe
        // rather than as the request under test.
        if let Some(length) = content_length(&head) {
            let mut body = vec![0_u8; length];
            reader.read_exact(&mut body).ok();
        } else if head.to_lowercase().contains("transfer-encoding: chunked") {
            // The row-by-row writers stream, so their length is only known
            // when the terminating chunk arrives.
            drain_chunked(&mut reader);
        }

        let body = br#"{"value"=%true}"#;
        let mut reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-yt-yson-text\r\n\r\n",
            body.len()
        )
        .into_bytes();
        reply.extend_from_slice(body);
        stream.write_all(&reply).expect("replies");
        stream.flush().ok();

        head
    });

    request_from(&format!("http://{address}"));
    served.join().expect("the listener thread finished")
}

/// Consumes a chunked body up to its terminating zero-length chunk.
fn drain_chunked(reader: &mut BufReader<std::net::TcpStream>) {
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() {
            return;
        }
        let size = usize::from_str_radix(header.trim(), 16).unwrap_or(0);
        if size == 0 {
            let mut trailer = String::new();
            reader.read_line(&mut trailer).ok();
            return;
        }
        let mut chunk = vec![0_u8; size + 2]; // the chunk and its CRLF
        if reader.read_exact(&mut chunk).is_err() {
            return;
        }
    }
}

/// The declared body length of a request, if it declared one.
fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .find(|line| line.to_lowercase().starts_with("content-length:"))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|value| value.trim().parse().ok())
}

/// One header of a captured request, exactly as it was sent.
///
/// The name is matched case-insensitively, because HTTP says a header name is;
/// the value comes back untouched, because some of them are case-sensitive and
/// lowercasing the whole request head — which the tests here otherwise do —
/// would hide that.
fn header_value(head: &str, name: &str) -> Option<String> {
    head.lines()
        .find(|line| {
            line.to_lowercase()
                .starts_with(&format!("{}:", name.to_lowercase()))
        })
        .map(|line| line[line.find(':').unwrap_or(0) + 1..].trim().to_owned())
}

/// The `X-YT-Parameters` header of a captured request.
fn parameters(head: &str) -> String {
    head.lines()
        .find(|line| line.to_lowercase().starts_with("x-yt-parameters:"))
        .map(|line| line[line.find(':').unwrap_or(0) + 1..].trim().to_owned())
        .unwrap_or_default()
}

#[test]
fn a_plain_write_replaces_and_says_nothing_about_it() {
    // The shape every version of this crate has sent. A path that grew
    // `<append=%false>` would be a new request for an unchanged meaning, and
    // the day it changed nobody would know which release did it.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        client.write_table("//tmp/out", b"").expect("writes");
    });

    assert!(
        parameters(&head).contains(r#"path="//tmp/out""#),
        "the path is not a bare string:\n{head}"
    );
    assert!(
        !parameters(&head).contains("append"),
        "a replacing write mentioned append:\n{head}"
    );
}

#[test]
fn an_appending_write_carries_the_attribute_on_the_path() {
    // The attribute goes on the *path*, not beside it as a parameter of its
    // own. A cluster given `{path="//tmp/out";append=%true}` replaces the
    // table and reports success, which is the failure this pins.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        client
            .write_table(TablePath::new("//tmp/out").append(), b"")
            .expect("writes");
    });

    assert!(
        parameters(&head).contains(r#"path=<append=%true>"//tmp/out""#),
        "the path does not carry the attribute:\n{head}"
    );
}

#[test]
fn all_three_writers_can_append() {
    // `write_table`, `write_table_rows` and `write_table_streaming` build the
    // same parameter block three times over, and only one of them is exercised
    // by an example. A path that lost its attribute on the streaming route
    // would replace a table the caller meant to add to, and the caller would
    // find out by losing rows.
    let row = std::collections::BTreeMap::from([("n", 1_i64)]);

    let heads = [
        (
            "write_table",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                client
                    .write_table(TablePath::new("//tmp/out").append(), b"")
                    .expect("writes");
            }),
        ),
        (
            "write_table_rows",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                client
                    .write_table_rows(TablePath::new("//tmp/out").append(), [row])
                    .expect("writes");
            }),
        ),
        (
            "write_table_streaming",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                client
                    .write_table_streaming(
                        TablePath::new("//tmp/out").append(),
                        std::io::Cursor::new(Vec::new()),
                    )
                    .expect("writes");
            }),
        ),
    ];

    for (command, head) in heads {
        assert!(
            parameters(&head).contains(r#"path=<append=%true>"//tmp/out""#),
            "{command} sent a path without the attribute:\n{head}"
        );
    }
}

#[test]
fn an_abort_reason_cannot_break_out_of_its_header() {
    // The reason is caller's text and it travels in an HTTP header, so the
    // YSON encoder is the only thing between a chatty message and a forged
    // request. A raw newline here would end the header — and start whatever
    // came after it as a header of its own.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        client
            .abort_operation(
                "1-2-3-4",
                Some("he said \"stop\"\r\nX-Forged: yes\nand meant it"),
            )
            .expect("aborts");
    });

    let params = parameters(&head);
    assert!(
        params.contains(r#"abort_message="he said \"stop\"\r\nX-Forged: yes\nand meant it""#),
        "the reason was not escaped as YSON text:\n{head}"
    );
    assert!(
        !head.lines().any(|line| line.starts_with("X-Forged")),
        "the reason smuggled a header into the request:\n{head}"
    );
    // One `X-YT-Parameters` line, holding the whole parameter block: the
    // cluster reads the header, not the lines under it.
    assert_eq!(
        head.lines()
            .filter(|line| line.to_lowercase().starts_with("x-yt-parameters:"))
            .count(),
        1,
        "{head}"
    );
}

#[test]
fn an_abort_carries_its_reason_and_no_mutation_id() {
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        client
            .abort_operation("1-2-3-4", Some("stopped by the test"))
            .expect("aborts");
    });

    let params = parameters(&head);
    assert!(head.starts_with("POST /api/v4/abort_operation"), "{head}");
    assert!(params.contains(r#"operation_id="1-2-3-4""#), "{params}");
    // The reason is what tells whoever finds the aborted operation later who
    // stopped it; dropping it silently would be the easy mistake here.
    assert!(
        params.contains(r#"abort_message="stopped by the test""#),
        "{params}"
    );
    // Deliberately absent, though this is a mutating command. The master's
    // mutation cache does not cover a scheduler command: a resend of the same
    // ID is answered `No such operation` rather than with the first response,
    // so a retry would report a successful abort as a failed one. Verified
    // against the cluster before this was changed.
    assert!(!params.contains("mutation_id"), "{params}");
    assert!(!params.contains("retry="), "{params}");
}

#[test]
fn an_abort_without_a_reason_sends_no_empty_message() {
    // An empty `abort_message` is a different statement from none, and it is
    // the one that would show up in the operation's error document as a blank
    // line under "aborted by user request".
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        client.abort_operation("1-2-3-4", None).expect("aborts");
    });

    assert!(
        !parameters(&head).contains("abort_message"),
        "{}",
        parameters(&head)
    );
}

#[test]
fn every_request_asks_for_a_compressed_answer() {
    // The proxy compresses when asked — a 67.7 MiB table came back as 400 KiB
    // — and `ureq` asks on its own because this crate turns its `gzip` feature
    // on. Nothing else here would notice if that feature were dropped: the
    // cluster answers the same either way, just larger.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        assert_eq!(client.exists("//tmp").ok(), Some(true));
    });

    let lowercase = head.to_lowercase();
    assert!(
        lowercase.contains("accept-encoding: gzip"),
        "the request did not ask for compression:\n{head}"
    );
}

#[test]
fn the_parameters_travel_as_a_header_and_not_a_query_string() {
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.exists("//tmp/some/path");
    });

    assert!(
        head.starts_with("GET /api/v4/exists HTTP/1.1"),
        "the command is the path, with nothing appended:\n{head}"
    );
    // The body is where a data stream goes, so parameters cannot live there;
    // the query string is not where API v4 looks for them either.
    assert!(
        head.contains(r#"x-yt-parameters: {path="//tmp/some/path"}"#)
            || head.contains(r#"X-YT-Parameters: {path="//tmp/some/path"}"#),
        "parameters are not in the header the protocol names:\n{head}"
    );
    assert!(
        head.to_lowercase()
            .contains("x-yt-header-format: <format=text>yson"),
        "the header format must say how the other headers are encoded:\n{head}"
    );
}

#[test]
fn a_token_is_carried_as_an_oauth_authorization() {
    let head = capture(|proxy| {
        let client = Client::with_token(proxy, "secret-token").with_retries(RetryPolicy::none());
        let _ = client.exists("//tmp");
    });

    assert!(
        head.contains("authorization: OAuth secret-token")
            || head.contains("Authorization: OAuth secret-token"),
        "the token is not on the request:\n{head}"
    );
}

#[test]
fn an_unauthenticated_client_sends_no_authorization_at_all() {
    // Not an empty one: a header that is present and empty is a different
    // statement to a proxy than a header that is absent.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.exists("//tmp");
    });

    assert!(
        !head.to_lowercase().contains("authorization:"),
        "an unauthenticated client sent an authorization header:\n{head}"
    );
}

#[test]
fn a_trace_context_travels_as_a_traceparent_header() {
    // The cluster traces itself, and a request that names a trace has its
    // proxy-side span put inside that one. The header is the W3C spelling,
    // which is what `TryParseTraceParent` in the proxy reads and what all three
    // official clients send; a header we spelled our own way would be dropped
    // in silence and the launch would simply not appear in the trace.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent");

    let head = capture(|proxy| {
        let client = Client::new(proxy)
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);

        // What will be sent, before it is sent — the read-back a caller logs
        // so the trace can be found again.
        assert_eq!(
            client.traceparent(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );

        let _ = client.exists("//tmp");
    });

    // The header *name* is matched case-insensitively because HTTP says it is,
    // and the *value* case-sensitively because the standard says the hex is
    // lowercase. Lowercasing the whole line, as the tests around this one do,
    // would accept an uppercased id the standard does not allow.
    let value = header_value(&head, "traceparent");
    assert_eq!(
        value.as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        "the trace context is not on the request as sent:\n{head}"
    );
}

#[test]
fn a_tracestate_travels_beside_the_traceparent() {
    // The standard pairs the two, and a participant that forwards one is
    // required to forward the other unmodified. The proxy ignores `tracestate`;
    // the caller's own backend is what keys off it, so dropping it here would
    // cost the sampling decision or the correlation key of everything
    // downstream of this hop and nothing would say so.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent")
        .with_tracestate("vendora=t61rcWkgMzE,vendorb=x9");

    let head = capture(|proxy| {
        let client = Client::new(proxy)
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);

        assert_eq!(client.tracestate(), Some("vendora=t61rcWkgMzE,vendorb=x9"));

        let _ = client.exists("//tmp");
    });

    assert_eq!(
        header_value(&head, "tracestate").as_deref(),
        Some("vendora=t61rcWkgMzE,vendorb=x9"),
        "the tracestate was dropped on the way to the cluster:\n{head}"
    );
    assert_eq!(
        header_value(&head, "traceparent").as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        "a tracestate must not displace the traceparent it belongs to:\n{head}"
    );
}

#[test]
fn a_traced_client_sends_no_tracestate_it_was_not_given() {
    // A `tracestate` naming no vendor is not a smaller version of one; it is a
    // header the caller's backend has to decide what to do with.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent");

    let head = capture(|proxy| {
        let client = Client::new(proxy)
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);
        let _ = client.exists("//tmp");
    });

    assert!(
        !head.to_lowercase().contains("tracestate"),
        "a tracestate appeared from nowhere:\n{head}"
    );
}

#[test]
fn a_transaction_inherits_the_clients_trace() {
    // The doc on `with_trace_context` promises this, and it holds only because
    // `Transaction::start` clones the client rather than rebuilding one from
    // its parts — which is a plausible refactor, since it immediately overrides
    // the retries and the timeout on the ping client. A commit or a ping that
    // hung is named in that doc as the thing the trace is for, so the claim is
    // load-bearing and nothing else here would notice it breaking.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent");

    let head = capture(|proxy| {
        let client = Client::new(proxy)
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);
        // The reply is an `exists` answer rather than a transaction id, so the
        // start fails to decode and no ping thread outlives this. The request
        // is what is under test.
        let _ = client.start_transaction();
    });

    assert!(
        head.starts_with("POST /api/v4/start_transaction"),
        "the captured request is not the transaction start:\n{head}"
    );
    assert_eq!(
        header_value(&head, "traceparent").as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        "a transaction started from a traced client left the trace:\n{head}"
    );
}

#[test]
fn a_client_without_a_trace_context_sends_no_traceparent() {
    // Sampling costs the cluster something, so a client that was not asked to
    // join a trace must not start one on its own.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.exists("//tmp");
    });

    assert!(
        !head.to_lowercase().contains("traceparent"),
        "an untraced client sent a trace context:\n{head}"
    );
}

#[test]
fn the_hosts_lookup_carries_the_trace_like_a_command_does() {
    // `/hosts` is not a command and builds its own request, which is how it
    // once came to carry neither the token nor the timeout. The trace context
    // is one more thing it would miss, and a heavy-proxy lookup slow enough to
    // matter is exactly the one worth seeing in the trace.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent");

    let head = capture(|proxy| {
        let client = Client::with_token(proxy, "secret-token")
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);
        // The reply is an `exists` answer rather than a host list, so this
        // fails to decode; the request is what is under test.
        let _ = client.heavy_proxy();
    });

    assert!(
        head.starts_with("GET /hosts HTTP/1.1"),
        "the lookup is not the documented one:\n{head}"
    );
    assert_eq!(
        header_value(&head, "traceparent").as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        "the hosts lookup dropped the trace context:\n{head}"
    );
    assert_eq!(
        header_value(&head, "authorization").as_deref(),
        Some("OAuth secret-token"),
        "the hosts lookup dropped the token:\n{head}"
    );
}

#[test]
fn a_raw_command_is_dressed_like_every_other_command() {
    // The reason the escape hatch is a `Client` method and not a bare `ureq`
    // agent handed to the caller. A raw command has to arrive looking like a
    // command — the token, the header format, the parameters header, the
    // compression — or every user of it reimplements this crate's transport
    // badly. A cluster answers the same either way, so nothing but this would
    // notice a regression.
    let head = capture(|proxy| {
        let client = Client::with_token(proxy, "secret-token").with_retries(RetryPolicy::none());
        let _ = client.raw_command(
            Method::Get,
            "get_supported_features",
            &yson_build::empty_map(),
            None,
        );
    });

    let lowercase = head.to_lowercase();
    assert!(
        head.starts_with("GET /api/v4/get_supported_features HTTP/1.1"),
        "the command name is the path, with nothing appended:\n{head}"
    );
    assert!(
        lowercase.contains("authorization: oauth secret-token"),
        "a raw command must carry the token:\n{head}"
    );
    assert!(
        lowercase.contains("x-yt-header-format: <format=text>yson")
            && lowercase.contains("x-yt-parameters: {}"),
        "a raw command must encode its parameters the way the protocol says:\n{head}"
    );
    assert!(
        lowercase.contains("accept-encoding: gzip"),
        "a raw command must ask for compression like the rest:\n{head}"
    );
}

/// A refusal that has to happen before the socket does.
#[test]
fn a_multi_table_skiff_write_is_refused_before_anything_is_sent() {
    // Nothing listens on this port, so reaching the transport at all would be
    // a connection error. A Config error is therefore proof that the format
    // was checked first, and that the caller is told what is actually wrong
    // with the request rather than what the stream looked like to a decoder
    // holding the wrong schema.
    let client = Client::new("http://127.0.0.1:1").with_retries(RetryPolicy::none());
    let two_tables = SkiffFormat::new(vec![
        SkiffSchemaRef::Inline(SkiffSchema::tuple([SkiffSchema::named(
            "a",
            SkiffWireType::Uint64,
        )])),
        SkiffSchemaRef::Inline(SkiffSchema::tuple([SkiffSchema::named(
            "b",
            SkiffWireType::Uint64,
        )])),
    ])
    .expect("two named tuples are a valid format");

    let error = client
        .write_table_with_format(
            // Truncated on purpose: with the checks the other way round this
            // is the byte that produces the misleading answer.
            TablePath::from("//tmp/out"),
            b"\x00",
            &DataFormat::skiff(two_tables),
        )
        .expect_err("direct table I/O takes exactly one table schema");

    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
    assert!(
        error.to_string().contains("exactly one table schema"),
        "{error}"
    );
}

// ------------------------------------------------- the operation lifecycle

#[test]
fn a_suspend_says_what_to_do_with_the_running_jobs() {
    // Sent either way round, never left out. The two are different requests:
    // one lets the jobs that have started finish, the other throws their work
    // away — and a caller who asked for the second and got the first would
    // find out much later, from a cluster bill.
    for abort_running_jobs in [false, true] {
        let head = capture(|proxy| {
            let client = Client::new(proxy).with_retries(RetryPolicy::none());
            client
                .suspend_operation("1-2-3-4", abort_running_jobs)
                .expect("suspends");
        });

        let params = parameters(&head);
        assert!(head.starts_with("POST /api/v4/suspend_operation"), "{head}");
        assert!(params.contains(r#"operation_id="1-2-3-4""#), "{params}");
        assert!(
            params.contains(&format!("abort_running_jobs=%{abort_running_jobs}")),
            "{params}"
        );
    }
}

#[test]
fn the_scheduler_commands_carry_no_mutation_id() {
    // The master's mutation cache does not cover a scheduler command, which is
    // the fact `abort_operation` was built on and these three inherit: a
    // resend under the same ID is answered `No such operation` rather than
    // with the first response. Suspend is retried, but on its own idempotency
    // — a second suspend of a suspended operation is simply accepted — and not
    // by asking the cluster to deduplicate it.
    let heads = [
        (
            "suspend_operation",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                client
                    .suspend_operation("1-2-3-4", false)
                    .expect("suspends");
            }),
        ),
        (
            "resume_operation",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                client.resume_operation("1-2-3-4").expect("resumes");
            }),
        ),
        (
            "complete_operation",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                client.complete_operation("1-2-3-4").expect("completes");
            }),
        ),
    ];

    for (command, head) in heads {
        let params = parameters(&head);
        assert!(
            head.starts_with(&format!("POST /api/v4/{command}")),
            "a mutating command is a POST: {head}"
        );
        assert!(!params.contains("mutation_id"), "{command}: {params}");
        assert!(!params.contains("retry="), "{command}: {params}");
    }
}

#[test]
fn updated_parameters_travel_in_the_header_and_not_in_the_body() {
    // The command reference calls this command's input "structured", which
    // reads like a request body. The cluster's own registry says `null`, and
    // the registry is what the proxy implements: the parameters go in
    // `X-YT-Parameters` like every other command's.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        client
            .update_operation_parameters(
                "1-2-3-4",
                &OperationParameters::new()
                    .with_pool("fast")
                    .with_weight(2.5),
            )
            .expect("updates");
    });

    let params = parameters(&head);
    assert!(
        head.starts_with("POST /api/v4/update_operation_parameters"),
        "{head}"
    );
    assert!(params.contains(r#"operation_id="1-2-3-4""#), "{params}");
    assert!(
        params.contains("parameters={pool=fast;weight=2.5}"),
        "the parameters are one nested dict, and the weight is a double: {params}"
    );
    assert!(
        !head.to_lowercase().contains("content-length: ")
            || head.to_lowercase().contains("content-length: 0"),
        "the command takes no body:\n{head}"
    );
}

#[test]
fn an_update_that_changes_nothing_is_refused_before_it_is_sent() {
    // Nothing listens on this port, so a Config error proves the check ran
    // first. The cluster answers an empty update with 200 and does nothing,
    // which is the shape of mistake that survives every test but the one that
    // reads the pool afterwards.
    let client = Client::new("http://127.0.0.1:1").with_retries(RetryPolicy::none());
    let error = client
        .update_operation_parameters("1-2-3-4", &OperationParameters::new())
        .expect_err("an empty update is not a request worth sending");

    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
}

#[test]
fn an_alias_lookup_asks_for_runtime_information() {
    // Without it the cluster refuses outright: "Operation alias cannot be
    // resolved without using runtime information". A lookup that forgot this
    // would fail every time, and only against a real cluster.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.get_operation_by_alias("*nightly", &["state"]);
    });

    let params = parameters(&head);
    assert!(head.starts_with("GET /api/v4/get_operation"), "{head}");
    assert!(params.contains(r#"operation_alias="*nightly""#), "{params}");
    assert!(params.contains("include_runtime=%true"), "{params}");
    assert!(params.contains("attributes=[state]"), "{params}");
    assert!(
        !params.contains("operation_id"),
        "an alias lookup names no id: {params}"
    );
}

#[test]
fn the_whole_operation_document_is_asked_for_by_naming_no_attributes() {
    // `attributes=[]` is a request for *no* attributes, which the cluster
    // answers with `{}`. Leaving the parameter out is what asks for
    // everything, so an empty slice must not be sent as an empty list.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.get_operation("1-2-3-4", &[]);
    });

    let params = parameters(&head);
    assert_eq!(params, r#"{operation_id="1-2-3-4"}"#);
}

#[test]
fn a_filtered_listing_sends_its_filter_and_nothing_else() {
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.list_operations(
            &OperationFilter::new()
                .with_user("robot-loader")
                .with_state("running")
                .with_limit(20),
        );
    });

    assert!(head.starts_with("GET /api/v4/list_operations"), "{head}");
    assert_eq!(
        parameters(&head),
        "{limit=20;state=running;user=robot-loader}"
    );
}

#[test]
fn a_job_is_asked_for_by_operation_and_job() {
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        // The stub answers `{value=%true}`, which names no job; the request is
        // what this is about.
        let _ = client.get_job("1-2-3-4", "5-6-7-8");
    });

    assert!(head.starts_with("GET /api/v4/get_job "), "{head}");
    assert_eq!(
        parameters(&head),
        r#"{job_id="5-6-7-8";operation_id="1-2-3-4"}"#
    );
}

// ------------------------------------------- where a command is sent, and why
//
// A cluster answers a heavy command the same way whichever of its proxies was
// asked — that is the point of the roles — so nothing about the *answers* here
// could tell a routed client from an unrouted one. Two listeners and a record
// of which one was spoken to can.

/// A stand-in proxy that keeps serving, and remembers what it was asked.
///
/// [`capture`] serves exactly one request, which is all a wire *shape* needs.
/// Routing is about which address a request went to and how many there were,
/// so this keeps a list and stays up.
struct Proxy {
    address: std::net::SocketAddr,
    seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl Proxy {
    /// A proxy that answers `/hosts` with `hosts`, or with 404 when given
    /// `None` — the shape of a cluster that has no such endpoint.
    fn new(hosts: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let served = std::sync::Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let hosts = hosts.clone();
                let seen = std::sync::Arc::clone(&served);
                // One thread per connection: `ureq` pools them, and a client
                // that opened a second while the first sat idle would deadlock
                // against a server that answered them in turn.
                std::thread::spawn(move || serve(stream, hosts, seen));
            }
        });

        Self { address, seen }
    }

    /// The address to configure a client with.
    fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// The address as `/hosts` names one: a bare host and port, no scheme.
    fn host(&self) -> String {
        self.address.to_string()
    }

    /// The request line of everything served so far.
    fn requests(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("nothing panicked holding it")
            .iter()
            .map(|head| head.lines().next().unwrap_or_default().to_owned())
            .collect()
    }

    /// Everything served so far, headers and all.
    fn heads(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("nothing panicked holding it")
            .clone()
    }
}

/// Answers requests on one connection until the client hangs up.
fn serve(
    mut stream: std::net::TcpStream,
    hosts: Option<String>,
    seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
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

        // The body first, for the reason `capture` gives: a request is only
        // finished being sent when its body has been read.
        if let Some(length) = content_length(&head) {
            let mut body = vec![0_u8; length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
        } else if head.to_lowercase().contains("transfer-encoding: chunked") {
            drain_chunked(&mut reader);
        }

        let asked_where_to_go = head.starts_with("GET /hosts ");
        seen.lock().expect("nothing panicked holding it").push(head);

        let reply = match (asked_where_to_go, &hosts) {
            (true, None) => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
            (true, Some(list)) => reply(list.as_bytes()),
            (false, _) => reply(br#"{"value"=%true}"#),
        };

        if stream.write_all(&reply).is_err() {
            return;
        }
        stream.flush().ok();
    }
}

/// A 200 carrying `body`, on a connection that stays open.
fn reply(body: &[u8]) -> Vec<u8> {
    let mut reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-yt-yson-text\r\n\r\n",
        body.len()
    )
    .into_bytes();
    reply.extend_from_slice(body);
    reply
}

/// An address nothing is listening on, for a proxy that has gone away.
fn nowhere() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let address = listener.local_addr().expect("has an address");
    drop(listener);
    address.to_string()
}

/// A client that discovers, though it is talking to a listener on loopback.
///
/// Discovery is off for a loopback address by default — a local cluster cannot
/// be improved on and a tunnelled one cannot be followed — and every listener
/// in this file is on loopback. So the tests that are *about* discovery say so
/// explicitly, and the one that is about the default does not.
fn discovering(proxy: &Proxy) -> Client {
    Client::new(&proxy.url())
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
}

#[test]
fn a_heavy_command_goes_to_the_proxy_the_cluster_names() {
    // The bug this file is the regression test for: every heavy command went
    // to whatever `YT_PROXY` held, which on an installation that separates
    // proxy roles is a control proxy, and a control proxy refuses one.
    let heavy = Proxy::new(None);
    let control = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));

    Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1"],
        "the configured address served the upload itself"
    );
    assert_eq!(heavy.requests(), ["PUT /api/v4/write_table HTTP/1.1"]);

    // The other half of sending it elsewhere: it has to arrive dressed as a
    // command. A heavy proxy that is handed a request with no token answers by
    // blaming the caller's credentials.
    let head = &heavy.heads()[0];
    assert_eq!(
        header_value(head, "authorization").as_deref(),
        Some("OAuth secret-token"),
        "the upload reached the heavy proxy without its token:\n{head}"
    );
    assert!(
        parameters(head).contains(r#"path="//tmp/t""#),
        "the upload lost its parameters on the way:\n{head}"
    );
}

#[test]
fn every_heavy_shape_goes_there_and_the_cluster_is_asked_once() {
    // Buffered, streamed in, streamed out, and a job's stderr: four routes
    // through the transport — `call`, `upload`, `open` — that each had their
    // own way of choosing an address. And one lookup between them, because the
    // answer is kept for the client's lifetime.
    let heavy = Proxy::new(None);
    let control = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client = discovering(&control);

    client.write_table("//tmp/t", b"").expect("buffered write");
    client
        .write_table_rows(
            "//tmp/t",
            [std::collections::BTreeMap::from([("n", 1_i64)])],
        )
        .expect("streamed write");
    // The stub answers `{value=%true}`, which is not a table: these fail on
    // the answer, having asked the question this is about.
    let _ = client.read_table("//tmp/t");
    let _ = client.read_table_streaming("//tmp/t");
    let _ = client.get_job_stderr("1-2-3-4", "5-6-7-8");

    assert_eq!(control.requests(), ["GET /hosts HTTP/1.1"]);
    assert_eq!(
        heavy.requests(),
        [
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "GET /api/v4/read_table HTTP/1.1",
            "GET /api/v4/read_table HTTP/1.1",
            "GET /api/v4/get_job_stderr HTTP/1.1",
        ]
    );
}

#[test]
fn a_light_command_stays_where_the_client_was_pointed() {
    // Cypress, the scheduler and the master are the control proxy's own work.
    // Sending them to a heavy proxy would be the same mistake pointing the
    // other way, and asking `/hosts` before a `get` would put a round trip in
    // front of every one of them.
    let heavy = Proxy::new(None);
    let control = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client = discovering(&control);

    let _ = client.exists("//tmp");
    let _ = client.create("table", "//tmp/t");
    let _ = client.abort_operation("1-2-3-4", None);

    assert_eq!(
        control.requests(),
        [
            "GET /api/v4/exists HTTP/1.1",
            "POST /api/v4/create HTTP/1.1",
            "POST /api/v4/abort_operation HTTP/1.1",
        ]
    );
    assert!(
        heavy.requests().is_empty(),
        "a light command was routed away: {:?}",
        heavy.requests()
    );
}

#[test]
fn a_cluster_that_names_no_heavy_proxy_keeps_serving_the_uploads_itself() {
    // The fallback that keeps a single-node installation working, and every
    // deployment that does not separate the roles. Asked once and then not
    // again: an empty answer is an answer.
    let control = Proxy::new(Some("[]".to_owned()));
    let client = discovering(&control);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_file("//tmp/f", b"x").expect("writes");

    assert_eq!(
        control.requests(),
        [
            "GET /hosts HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_file HTTP/1.1",
        ]
    );
}

#[test]
fn a_cluster_with_no_hosts_endpoint_is_not_asked_before_every_upload() {
    // `absent`, not merely empty: 404 is deterministic, so asking again would
    // cost a round trip per upload and buy nothing. A failure that might pass —
    // a timeout, a restarting proxy — is judged the other way, by the same
    // rule the retry policy uses.
    let control = Proxy::new(None);
    let client = discovering(&control);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        control.requests(),
        [
            "GET /hosts HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
        ]
    );
}

#[test]
fn a_heavy_proxy_that_cannot_be_reached_is_asked_about_again() {
    // A proxy that is down, drained or banned stays broken for every upload
    // that follows, so the answer is thrown away rather than kept for the
    // client's lifetime. The failed command itself is *not* re-sent: heavy
    // commands are not retried, and a streamed body is gone by then.
    let control = Proxy::new(Some(format!(r#"["{}"]"#, nowhere())));
    let client = discovering(&control);

    let first = client.write_table("//tmp/t", b"");
    let second = client.write_table("//tmp/t", b"");

    assert!(first.is_err() && second.is_err(), "nothing was listening");
    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1", "GET /hosts HTTP/1.1"],
        "the client kept an address it could not reach"
    );
}

#[test]
fn a_local_cluster_is_never_asked_where_to_send_a_heavy_command() {
    // The no-regression test, and the reason the others have to ask for
    // discovery explicitly. A cluster on loopback is this machine's own or a
    // tunnel to one: the address such a cluster publishes for itself is not
    // reachable from here, and following it would break every upload that
    // works today. So the default sends the request straight there, with no
    // lookup in front of it.
    let control = Proxy::new(Some(r#"["heavy.example.net"]"#.to_owned()));

    Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert_eq!(control.requests(), ["PUT /api/v4/write_table HTTP/1.1"]);
}
