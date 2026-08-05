//! What the client actually puts on the wire.
//!
//! Everything else in this crate is checked against a cluster, which answers
//! the same whether or not the request was well made. These serve one request
//! from a socket in-process and read the bytes the client sent, which is the
//! only way to pin the things a cluster is too forgiving to notice:
//! compression the client asks for, the token it carries, and the header the
//! parameters travel in.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;

use ytsaurus_client::{
    Client, ClientError, DataFormat, RetryPolicy, SkiffFormat, SkiffSchema, SkiffSchemaRef,
    SkiffWireType, TablePath,
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
