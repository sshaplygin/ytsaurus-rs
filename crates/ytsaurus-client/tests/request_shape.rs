//! What the client actually puts on the wire.
//!
//! A cluster answers the same whether or not a request was well made, so these
//! tests serve the request in-process and read the bytes the client sent: the
//! compression it asks for, the token, the header the parameters travel in.
//!
//! The last section asks *which address* a request went to, which needs two
//! listeners: [`Proxy`] rather than [`capture`].

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};

use ytsaurus_client::{
    Client, ClientError, DataFormat, Key, Method, OperationFilter, OperationParameters,
    RetryPolicy, RowRange, SkiffFormat, SkiffSchema, SkiffSchemaRef, SkiffWireType, TablePath,
    TraceContext, yson_build,
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

        // The body is read before the reply, or the client, still writing it,
        // reports a broken pipe instead of the request under test.
        if let Some(length) = content_length(&head) {
            let mut body = vec![0_u8; length];
            reader.read_exact(&mut body).ok();
        } else if head.to_lowercase().contains("transfer-encoding: chunked") {
            // The row-by-row writers stream a chunked body.
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
/// The name is matched case-insensitively; the value comes back untouched,
/// since some values are case-sensitive.
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
    // A replacing write sends a bare path, with no `<append=%false>`.
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
    // The attribute goes on the path: a cluster given
    // `{path="//tmp/out";append=%true}` replaces the table and reports success.
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
    // The three writers build their parameter blocks separately; a path that
    // lost its attribute on one route would replace the table.
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

// The reads below ignore their result: the stub's reply is not binary YSON, so
// the read fails after the request under test is on the wire.

#[test]
fn a_column_selection_travels_as_an_attribute_on_the_path() {
    // `columns` is an attribute on the path, as the rich YPath reference lists
    // it for `read_table`; a sibling parameter would be silently dropped.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_table(TablePath::new("//tmp/wide").columns(["host", "status"]));
    });

    assert!(
        head.starts_with("GET /api/v4/read_table"),
        "not a read_table:\n{head}"
    );
    assert!(
        parameters(&head).contains(r#"path=<columns=[host;status]>"//tmp/wide""#),
        "the selection is not on the path:\n{head}"
    );
}

#[test]
fn a_row_range_travels_in_the_documented_limits() {
    // `0..2` is rows 0 and 1 on both sides: every limit but key_bound is
    // inclusive below and exclusive above, as Rust's `..` is.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_table(TablePath::new("//tmp/t").range(0..2));
    });

    assert!(
        parameters(&head).contains(
            r#"path=<ranges=[{lower_limit={row_index=0};upper_limit={row_index=2}}]>"//tmp/t""#
        ),
        "the range is not on the path:\n{head}"
    );
}

#[test]
fn key_bounds_travel_in_the_clusters_representation() {
    // Inclusive-below, exclusive-above bounds go as `key`; the other two go as
    // `key_bound=[relation;prefix]`. Every literal is fixed, so the rendered
    // text may be asserted.
    let plain = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_table(
            TablePath::new("//tmp/sorted")
                .range(RowRange::keys(Key::from("alice")..Key::from("bob"))),
        );
    });
    assert!(
        parameters(&plain).contains(
            r#"path=<ranges=[{lower_limit={key=[alice]};upper_limit={key=[bob]}}]>"//tmp/sorted""#
        ),
        "an inclusive..exclusive key range is not the plain key form:\n{plain}"
    );

    let bounds = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_table(TablePath::new("//tmp/sorted").range(RowRange::keys((
            std::ops::Bound::Excluded(Key::from("alice")),
            std::ops::Bound::Included(Key::from("bob")),
        ))));
    });
    assert!(
        parameters(&bounds).contains(
            r#"path=<ranges=[{lower_limit={key_bound=[">";[alice]]};upper_limit={key_bound=["<=";[bob]]}}]>"//tmp/sorted""#
        ),
        "the other two inclusivities are not key_bound:\n{bounds}"
    );
}

#[test]
fn an_exact_key_read_asks_in_the_clusters_word_for_it() {
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_table(
            TablePath::new("//tmp/sorted").range(RowRange::exact_key(Key::from("alice"))),
        );
    });

    assert!(
        parameters(&head).contains(r#"path=<ranges=[{exact={key=[alice]}}]>"//tmp/sorted""#),
        "the exact selector is not on the path:\n{head}"
    );
}

#[test]
fn all_three_readers_carry_the_selection() {
    // The three readers build their parameter blocks separately; a selection
    // lost on one route would read the whole table.
    let selected = || TablePath::new("//tmp/wide").columns(["host"]).range(0..100);
    let expected = r#"path=<columns=[host];ranges=[{lower_limit={row_index=0};upper_limit={row_index=100}}]>"//tmp/wide""#;

    let heads = [
        (
            "read_table",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                let _ = client.read_table(selected());
            }),
        ),
        (
            "read_table_rows",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                let _ =
                    client.read_table_rows::<std::collections::BTreeMap<String, i64>>(selected());
            }),
        ),
        (
            "read_table_streaming",
            capture(|proxy| {
                let client = Client::new(proxy).with_retries(RetryPolicy::none());
                let _ = client.read_table_streaming(selected());
            }),
        ),
    ];

    for (command, head) in heads {
        assert!(
            parameters(&head).contains(expected),
            "{command} sent a path without the selection:\n{head}"
        );
    }
}

#[test]
fn a_skiff_read_merges_a_row_range_with_its_schema_columns() {
    // The Skiff fields select columns and a typed range selects rows; both
    // attributes must arrive on the path.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_skiff_table(TablePath::new("//tmp/t").range(0..2), &one_column());
    });

    // The whole rendering, so `columns` moving to a sibling parameter fails.
    // Attributes are in the encoder's BTreeMap order.
    assert!(
        parameters(&head).contains(
            r#"path=<columns=[n];ranges=[{lower_limit={row_index=0};upper_limit={row_index=2}}]>"//tmp/t""#
        ),
        "the Skiff read lost half the selection, or moved it off the path:\n{head}"
    );
}

#[test]
fn a_skiff_read_refuses_a_column_selection_spelled_into_the_string() {
    // A Skiff read synthesises `columns` from the format's fields, so
    // `//tmp/t{n}` would be two column selections; the cluster keeps the
    // attribute and discards the string's at 200 (docs/protocol-reference.md).
    // A leading `<…>` is refused because the client cannot parse it to know
    // whether it names `columns`.
    let client = Client::new(&nowhere()).with_retries(RetryPolicy::none());

    for path in [
        "//tmp/t{n}",
        "<columns=[n]>//tmp/t",
        "<primary_medium=default>//tmp/t",
    ] {
        let error = client
            .read_skiff_table(path, &one_column())
            .expect_err("refused");
        assert!(
            matches!(&error, ClientError::Config(_)),
            "{path} was not refused locally: {error}"
        );
    }
}

#[test]
fn a_skiff_read_sends_a_string_spelled_row_range() {
    // A string-spelled row range composes with the synthesised `columns`:
    // `<columns=["n"]>"//tmp/t[#3:#5]"` answered rows 3-4 with only `n`.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_skiff_table("//tmp/t[#0:#2]", &one_column());
    });

    assert!(
        parameters(&head).contains(r#"path=<columns=[n]>"//tmp/t[#0:#2]""#),
        "the string-spelled range did not reach the cluster intact:\n{head}"
    );
}

#[test]
fn a_skiff_read_refuses_a_second_column_selection() {
    // The schema's fields and `TablePath::columns` are two projections over
    // one positional format, so the read is refused before anything is sent.
    let client = Client::new(&nowhere()).with_retries(RetryPolicy::none());
    let error = client
        .read_skiff_table(TablePath::new("//tmp/t").columns(["n"]), &one_column())
        .expect_err("refused");

    assert!(
        matches!(&error, ClientError::Config(reason) if reason.contains("columns")),
        "not the local refusal: {error}"
    );
}

#[test]
fn a_write_with_a_read_selection_is_refused_before_anything_is_sent() {
    // A cluster ignores columns and ranges on a write and replaces the whole
    // table with a 200 (docs/protocol-reference.md), so they are refused
    // locally. Nothing answers at this address, so an attempted request would
    // be a Transport error, not Config.
    let client = Client::new(&nowhere()).with_retries(RetryPolicy::none());
    let row = std::collections::BTreeMap::from([("n", 1_i64)]);

    let refusals: Vec<(&str, ClientError)> = vec![
        (
            "write_table with columns",
            client
                .write_table(TablePath::new("//tmp/out").columns(["a"]), b"")
                .expect_err("refused"),
        ),
        (
            "write_table with a range",
            client
                .write_table(TablePath::new("//tmp/out").range(0..2), b"")
                .expect_err("refused"),
        ),
        (
            "write_table_rows with a range",
            client
                .write_table_rows(TablePath::new("//tmp/out").range(0..2), [row])
                .expect_err("refused"),
        ),
        (
            "write_table_streaming with columns",
            client
                .write_table_streaming(
                    TablePath::new("//tmp/out").columns(["a"]),
                    std::io::Cursor::new(Vec::new()),
                )
                .expect_err("refused"),
        ),
        (
            "write_skiff_table with a range",
            client
                .write_skiff_table(TablePath::new("//tmp/out").range(0..2), b"", &one_column())
                .expect_err("refused"),
        ),
    ];

    for (writer, error) in refusals {
        assert!(
            matches!(&error, ClientError::Config(_)),
            "{writer} did not refuse locally: {error}"
        );
    }
}

#[test]
fn a_write_path_string_spelling_a_selection_is_refused() {
    // `write_table_rows("//tmp/t[#0:#2]", rows)` replaced the whole table and
    // succeeded, so a write path string must be a bare path.
    let client = Client::new(&nowhere()).with_retries(RetryPolicy::none());

    for path in ["//tmp/t[#0:#2]", "//tmp/t{a,b}", "<append=%true>//tmp/t"] {
        let error = client.write_table(path, b"").expect_err("refused");
        assert!(
            matches!(&error, ClientError::Config(_)),
            "{path} was not refused locally: {error}"
        );
    }
}

#[test]
fn an_escaped_bracket_is_a_node_name_and_still_writable() {
    // Rich YPath escapes a literal bracket as `\[`: this is a table named
    // `t[x]`, not a range.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        client.write_table(r"//tmp/t\[x\]", b"").expect("writes");
    });

    assert!(
        head.starts_with("PUT /api/v4/write_table"),
        "the write was not sent:\n{head}"
    );
}

#[test]
fn a_read_keeps_passing_a_string_spelled_path_through() {
    // The cluster reads a string-spelled range correctly, so a read path
    // travels as a bare string, not parsed or rewritten.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_table("//tmp/t[#0:#2]");
    });

    assert!(
        parameters(&head).contains(r#"path="//tmp/t[#0:#2]""#),
        "the string-spelled path was rewritten:\n{head}"
    );
}

#[test]
fn a_read_refuses_a_selection_spelled_twice() {
    // A string carrying `[…]` plus a typed range: the cluster keeps the
    // attribute and discards the string's range at 200 without a word.
    //
    // Every reader builds its parameters separately, so each must refuse.
    // Nothing answers at this address, so an attempted request would be a
    // Transport error, not Config.
    let client = Client::new(&nowhere()).with_retries(RetryPolicy::none());
    let doubled = || TablePath::new("//tmp/t[#0:#2]").range(0..2);

    let refusals: Vec<(&str, ClientError)> = vec![
        (
            "read_table",
            client.read_table(doubled()).expect_err("refused"),
        ),
        (
            "read_table_rows",
            client
                .read_table_rows::<std::collections::BTreeMap<String, i64>>(doubled())
                .expect_err("refused"),
        ),
        (
            "read_table_streaming",
            // `expect_err` needs a `Debug` success value and a `TableReader`
            // has none, so the success case is spelled out.
            match client.read_table_streaming(doubled()) {
                Ok(_) => panic!("read_table_streaming did not refuse a doubled selection"),
                Err(error) => error,
            },
        ),
        (
            "read_skiff_table",
            client
                .read_skiff_table(TablePath::new("//tmp/t[#0:#2]").range(0..2), &one_column())
                .expect_err("refused"),
        ),
    ];

    for (reader, error) in refusals {
        assert!(
            matches!(&error, ClientError::Config(_)),
            "{reader} did not refuse locally: {error}"
        );
    }
}

#[test]
fn a_read_refuses_a_range_asking_for_rows_no_table_has() {
    // A backwards range is answered 200 with no rows, and a negative
    // `row_index` is clamped to 0 (docs/protocol-reference.md); neither is
    // reported, so both are refused.
    let client = Client::new(&nowhere()).with_retries(RetryPolicy::none());
    // From variables, since clippy rejects the literal `5..3`.
    let (from, to) = (5_i64, 3_i64);

    let refusals: Vec<(&str, ClientError)> = vec![
        (
            "rows(5..3)",
            client
                .read_table(TablePath::new("//tmp/t").range(from..to))
                .expect_err("refused"),
        ),
        (
            "rows(-5..2), which the cluster would have read as rows(0..2)",
            client
                .read_table(TablePath::new("//tmp/t").range(-5..2))
                .expect_err("refused"),
        ),
        (
            "keys(b..a), the same mistake in the other selector",
            client
                .read_table(
                    TablePath::new("//tmp/t").range(RowRange::keys(Key::from("b")..Key::from("a"))),
                )
                .expect_err("refused"),
        ),
    ];

    for (selection, error) in refusals {
        assert!(
            matches!(&error, ClientError::Config(_)),
            "{selection} did not refuse locally: {error}"
        );
    }
}

#[test]
fn a_read_sends_an_empty_column_selection() {
    // `<columns=[]>` answers one empty map per row and composes with a range,
    // counting a range's rows with no column bytes, which `Client::row_count`
    // (the whole-table `@row_count`) cannot. So it is sent as an empty list.
    let head = capture(|proxy| {
        let client = Client::new(proxy).with_retries(RetryPolicy::none());
        let _ = client.read_table(
            TablePath::new("//tmp/t")
                .columns(Vec::<String>::new())
                .range(0..2),
        );
    });

    assert!(
        parameters(&head).contains(
            r#"path=<columns=[];ranges=[{lower_limit={row_index=0};upper_limit={row_index=2}}]>"//tmp/t""#
        ),
        "the empty projection did not travel as an empty `columns` attribute:\n{head}"
    );
}

#[test]
fn an_abort_reason_cannot_break_out_of_its_header() {
    // The reason is caller's text in an HTTP header: a raw newline would end
    // the header and start a forged one. The YSON encoder escapes it.
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
    // One `X-YT-Parameters` line holds the whole parameter block.
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
    // The reason tells whoever finds the aborted operation who stopped it.
    assert!(
        params.contains(r#"abort_message="stopped by the test""#),
        "{params}"
    );
    // No mutation ID, though the command mutates: the master's mutation cache
    // does not cover scheduler commands, and a resend under the same ID is
    // answered `No such operation` (verified on a cluster).
    assert!(!params.contains("mutation_id"), "{params}");
    assert!(!params.contains("retry="), "{params}");
}

#[test]
fn an_abort_without_a_reason_sends_no_empty_message() {
    // An empty `abort_message` would show as a blank line under "aborted by
    // user request" in the operation's error document.
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
    // `ureq` asks for compression because its `gzip` feature is on
    // (docs/protocol-reference.md); a cluster answers either way, only larger.
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
    // The body carries a data stream, and API v4 does not read parameters
    // from the query string.
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
    // Absent, not empty: a proxy treats the two differently.
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
    // The proxy puts its span inside a trace named by the W3C `traceparent`
    // header, which `TryParseTraceParent` reads and all three official
    // clients send.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent");

    let head = capture(|proxy| {
        let client = Client::new(proxy)
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);

        // The read-back a caller logs so the trace can be found again.
        assert_eq!(
            client.traceparent(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );

        let _ = client.exists("//tmp");
    });

    // The value is matched case-sensitively: the standard requires lowercase
    // hex.
    let value = header_value(&head, "traceparent");
    assert_eq!(
        value.as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        "the trace context is not on the request as sent:\n{head}"
    );
}

#[test]
fn a_tracestate_travels_beside_the_traceparent() {
    // The standard requires forwarding `tracestate` unmodified with its
    // `traceparent`. The proxy ignores it; the caller's backend keys off it.
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
    // No `tracestate` is sent unless one was given.
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
    // `with_trace_context` promises this. It holds because `Transaction::start`
    // clones the client rather than rebuilding it from parts.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent");

    let head = capture(|proxy| {
        let client = Client::new(proxy)
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);
        // The reply is not a transaction id, so the start fails to decode and
        // no ping thread outlives this.
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
    // Sampling costs the cluster, so an untraced client starts no trace.
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
    // `/hosts` is not a command and builds its own request; it carries the
    // trace context and the token as a command does.
    let context = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .expect("a W3C traceparent");

    let head = capture(|proxy| {
        let client = Client::with_token(proxy, "secret-token")
            .with_retries(RetryPolicy::none())
            .with_trace_context(&context);
        // The reply is not a host list, so this fails to decode.
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
    // A raw command carries the token, the header format, the parameters
    // header and the compression request, like every other command.
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
    // Nothing listens on this port, so a Config error proves the format was
    // checked before the transport was reached.
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
            // Truncated: with the checks reversed, this byte would produce a
            // misleading decode error.
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
    // `abort_running_jobs` is always sent, either way: one value lets started
    // jobs finish, the other throws their work away.
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
    // The master's mutation cache does not cover scheduler commands: a resend
    // under the same ID is answered `No such operation`. Suspend is retried on
    // its own idempotency, since a second suspend is accepted.
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
    // The command reference calls the input "structured", but the registry
    // the proxy implements says `null`: the parameters go in
    // `X-YT-Parameters`.
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
    // A cluster answers an empty update 200 and does nothing. Nothing listens
    // on this port, so a Config error proves the check ran first.
    let client = Client::new("http://127.0.0.1:1").with_retries(RetryPolicy::none());
    let error = client
        .update_operation_parameters("1-2-3-4", &OperationParameters::new())
        .expect_err("an empty update is not a request worth sending");

    assert!(matches!(error, ClientError::Config(_)), "{error:?}");
}

#[test]
fn an_alias_lookup_asks_for_runtime_information() {
    // Without `include_runtime` the cluster refuses: "Operation alias cannot
    // be resolved without using runtime information".
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
    // `attributes=[]` asks for no attributes and gets `{}`; leaving it out
    // asks for everything, so an empty slice is not sent.
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
        // The stub's answer names no job; only the request is checked.
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
// A cluster answers a heavy command the same whichever proxy was asked, so
// only a record of which listener was spoken to tells routing apart.

/// A stand-in proxy that keeps serving, and remembers what it was asked.
///
/// Unlike [`capture`], it stays up and records every request, since routing
/// is about which address each request went to.
struct Proxy {
    address: std::net::SocketAddr,
    seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

/// What a stand-in proxy does with `GET /hosts`.
#[derive(Clone)]
enum Hosts {
    /// Answers 200 with this JSON body.
    List(String),
    /// Answers 404: a cluster with no such endpoint.
    Absent,
    /// Answers a "not now" status, as a restarting proxy does.
    Status(u16),
    /// Accepts the question and never answers it.
    Hang,
    /// Answers this body after the delay: a cluster slower than the budget.
    Slow(std::time::Duration, String),
    /// Answers each ask with the next `(status, body)`; the last one repeats.
    /// A `/hosts` whose answer changes over time, for the refresh tests.
    Sequence(Vec<(u16, String)>),
}

/// What a stand-in proxy does with a **heavy** command.
///
/// `Any` serves everything, like a single-node installation. A real control
/// proxy refuses a heavy request, and `Control` does too, so a client that
/// stopped routing fails.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    /// Serves every command, whatever its weight.
    Any,
    /// Refuses a heavy command the way a real control proxy does.
    Control,
}

/// The commands a control proxy will not serve: the `isHeavy` column of
/// `REGISTER_ALL` in `driver.cpp`.
const HEAVY_COMMANDS: &[&str] = &[
    "write_table",
    "write_file",
    "read_table",
    "read_file",
    "get_job_input",
    "get_job_stderr",
];

impl Proxy {
    /// A proxy that answers `/hosts` with `hosts`, or 404 for `None`.
    fn new(hosts: Option<String>) -> Self {
        Self::answering(hosts.map_or(Hosts::Absent, Hosts::List), 200)
    }

    /// A proxy that answers `/hosts` one way and commands with `commands`.
    fn answering(hosts: Hosts, commands: u16) -> Self {
        Self::in_role(hosts, commands, Role::Any)
    }

    /// A **control** proxy: it names heavy proxies and serves none of their
    /// work itself.
    fn control(hosts: String) -> Self {
        Self::in_role(Hosts::List(hosts), 200, Role::Control)
    }

    fn in_role(hosts: Hosts, commands: u16, role: Role) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let served = std::sync::Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let hosts = hosts.clone();
                let seen = std::sync::Arc::clone(&served);
                // One thread per connection: a client that opened a second
                // while the first sat pooled would deadlock against a server
                // answering them in turn.
                std::thread::spawn(move || serve(stream, hosts, commands, role, seen));
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
    hosts: Hosts,
    commands: u16,
    role: Role,
    seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) {
    let mut reader = BufReader::new(stream.try_clone().expect("clones"));
    // Held open and never answered, for `Hosts::Hang`: dropping the socket
    // would be a connection reset instead.
    let mut hung = Vec::new();

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

        // The body first, as in `capture`.
        if let Some(length) = content_length(&head) {
            let mut body = vec![0_u8; length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
        } else if head.to_lowercase().contains("transfer-encoding: chunked") {
            drain_chunked(&mut reader);
        }

        let asked_where_to_go = head.starts_with("GET /hosts ");
        let heavy = HEAVY_COMMANDS
            .iter()
            .any(|command| head.contains(&format!("/api/v4/{command} ")));
        let asked_so_far = {
            let mut seen = seen.lock().expect("nothing panicked holding it");
            seen.push(head);
            seen.iter()
                .filter(|head| head.starts_with("GET /hosts "))
                .count()
        };

        let answer = if asked_where_to_go {
            match &hosts {
                Hosts::List(list) => reply(200, list.as_bytes()),
                Hosts::Absent => reply(404, b""),
                Hosts::Status(status) => reply(*status, b""),
                Hosts::Hang => {
                    hung.push(stream.try_clone().expect("clones"));
                    continue;
                }
                Hosts::Slow(delay, list) => {
                    std::thread::sleep(*delay);
                    reply(200, list.as_bytes())
                }
                Hosts::Sequence(answers) => {
                    let (status, body) = &answers[(asked_so_far - 1).min(answers.len() - 1)];
                    reply(*status, body.as_bytes())
                }
            }
        } else if role == Role::Control && heavy {
            refusal()
        } else {
            reply(commands, br#"{"value"=%true}"#)
        };

        if stream.write_all(&answer).is_err() {
            return;
        }
        stream.flush().ok();
    }
}

/// A response carrying `body`, on a connection that stays open.
fn reply(status: u16, body: &[u8]) -> Vec<u8> {
    let mut reply = format!(
        "HTTP/1.1 {status} .\r\nContent-Length: {}\r\nContent-Type: application/x-yt-yson-text\r\n\r\n",
        body.len()
    )
    .into_bytes();
    reply.extend_from_slice(body);
    reply
}

/// What a control proxy answers a heavy request with input data.
///
/// The cluster's shape, from `TContext::TryRedirectHeavyRequests`: 503 with
/// `Retry-After` and the reason in `X-YT-Error`, which the client reports.
fn refusal() -> Vec<u8> {
    let error =
        r#"{"code":1,"message":"Control proxy may not serve heavy requests with input data"}"#;
    format!("HTTP/1.1 503 .\r\nContent-Length: 0\r\nRetry-After: 60\r\nX-YT-Error: {error}\r\n\r\n")
        .into_bytes()
}

/// An address nothing is listening on, for a proxy that has gone away.
///
/// A connection to it must be refused at once, and no other stub in the suite
/// may ever be bound to it. Binding `127.0.0.1:0` and dropping the listener
/// fails the second rule: across a suite that binds thousands of sockets the
/// OS hands the freed port to another test's `TcpListener::bind`.
///
/// A **privileged** port cannot be assigned that way: `bind("127.0.0.1:0")`
/// draws from the ephemeral range only (32768+ on Linux, 49152+ on macOS). The
/// refusal is verified, so a port something on the machine listens on is
/// skipped.
fn nowhere() -> String {
    for port in 1u16..=16 {
        let address = format!("127.0.0.1:{port}");
        let socket = address.parse().expect("a valid loopback address");
        match TcpStream::connect_timeout(&socket, std::time::Duration::from_millis(200)) {
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => return address,
            // A listener answered, or the connect timed out: try the next.
            _ => continue,
        }
    }
    panic!("no reserved loopback port refused a connection; cannot address nowhere");
}

/// A client that discovers, though it is talking to a listener on loopback.
///
/// Discovery is off by default for a loopback address, where every listener
/// here is, so the tests about discovery turn it on explicitly.
fn discovering(proxy: &Proxy) -> Client {
    Client::new(&proxy.url())
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
}

#[test]
fn a_heavy_command_goes_to_the_proxy_the_cluster_names() {
    // A heavy command goes to the proxy `/hosts` names, not to the configured
    // address (`YT_PROXY`), which with separate roles is a control proxy that
    // refuses it.
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

    // It arrives with its token and parameters.
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

/// One table of one named column — enough to make a Skiff call well formed.
fn one_column() -> SkiffFormat {
    SkiffFormat::new(vec![SkiffSchemaRef::Inline(SkiffSchema::tuple([
        SkiffSchema::named("n", SkiffWireType::Int64),
    ]))])
    .expect("one named tuple is a valid format")
}

#[test]
fn every_heavy_shape_goes_there_and_the_cluster_is_asked_once() {
    // Every route through the transport (`call`, `upload`, `open`): tables,
    // files and Skiff in both directions, and a job's stderr. One lookup
    // serves them all, since the answer is kept for the refresh interval, a
    // minute by default (pinned in
    // `a_stale_answer_is_refreshed_by_the_next_heavy_command_and_a_fresh_one_is_not`).
    //
    // The pool has one host, so the random pick leaves the order as called;
    // the reads fail on the answer after a 200, which drops nobody.
    //
    // Each call site is one `Repeatable::Heavy` away from the control proxy;
    // `write_file` carries every worker upload.
    let heavy = Proxy::new(None);
    let control = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client = discovering(&control);
    let skiff = one_column();

    client.write_table("//tmp/t", b"").expect("buffered write");
    client
        .write_table_rows(
            "//tmp/t",
            [std::collections::BTreeMap::from([("n", 1_i64)])],
        )
        .expect("streamed write");
    client.write_file("//tmp/f", b"x").expect("file write");
    client
        .write_skiff_table("//tmp/t", b"", &skiff)
        .expect("skiff write");
    // The stub's answer is not a table: these fail after the request.
    let _ = client.read_table("//tmp/t");
    let _ = client.read_table_streaming("//tmp/t");
    let _ = client.read_skiff_table("//tmp/t", &skiff);
    let _ = client.read_file("//tmp/f");
    let _ = client.read_file_streaming("//tmp/f");
    let _ = client.get_job_stderr("1-2-3-4", "5-6-7-8");

    // The `get` is the buffered `read_file`'s size check, a light command, so
    // it stays on the configured address.
    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1", "GET /api/v4/get HTTP/1.1"],
        "a heavy command was served by the control proxy"
    );
    assert_eq!(
        heavy.requests(),
        [
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_file HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "GET /api/v4/read_table HTTP/1.1",
            "GET /api/v4/read_table HTTP/1.1",
            "GET /api/v4/read_table HTTP/1.1",
            "GET /api/v4/read_file HTTP/1.1",
            "GET /api/v4/read_file HTTP/1.1",
            "GET /api/v4/get_job_stderr HTTP/1.1",
        ]
    );
}

#[test]
fn a_light_command_stays_where_the_client_was_pointed() {
    // Cypress, scheduler and master commands are the control proxy's work, and
    // no `/hosts` lookup goes in front of them.
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
    // A single-node installation, or one without separate roles: the empty
    // answer is kept, and the configured address serves the uploads.
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
    // A 404 is deterministic, so it is not asked again before each upload. A
    // failure that might pass is judged by the retry policy's rule.
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
fn a_heavy_proxy_that_cannot_be_reached_gives_the_configured_address_back() {
    // A single-node container reached at `172.17.0.2` is not local, so
    // discovery runs, and `/hosts` names a container-internal host nothing
    // outside can dial. The first upload fails and is not re-sent (heavy
    // commands are not retried); the dead host is dropped, and later uploads
    // go to the configured address without asking again.
    let dead = nowhere();
    let control = Proxy::new(Some(format!(r#"["{dead}"]"#)));
    let client = discovering(&control);

    let first = client.write_table("//tmp/t", b"");
    let second = client.write_table("//tmp/t", b"");

    assert!(first.is_err(), "nothing was listening on {dead}");
    assert!(
        second.is_ok(),
        "the second upload was sent to the dead host too: {second:?}"
    );
    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1", "PUT /api/v4/write_table HTTP/1.1"],
        "the client kept resolving an address it could not reach"
    );
}

#[test]
fn a_failure_at_a_discovered_proxy_says_which_one() {
    // A discovered address appears nowhere in the caller's code, so the error
    // names it: `write_table at <host>:`.
    let dead = nowhere();
    let control = Proxy::new(Some(format!(r#"["{dead}"]"#)));

    let error = discovering(&control)
        .write_table("//tmp/t", b"")
        .expect_err("nothing was listening");

    assert!(
        error
            .to_string()
            .starts_with(&format!("write_table at {dead}:")),
        "the failure did not name the proxy it went to: {error}"
    );
}

#[test]
fn a_failure_at_the_configured_address_is_not_dressed_up_as_a_routed_one() {
    // A failure at the configured address is not named as a routed one. Both
    // uploads fail with the same 503; only the first went where the client
    // chose.
    let heavy = Proxy::answering(Hosts::Absent, 503);
    let control = Proxy::answering(Hosts::List(format!(r#"["{}"]"#, heavy.host())), 503);
    let client = discovering(&control);

    let chosen = client.write_table("//tmp/t", b"").expect_err("503");
    let given = client.write_table("//tmp/t", b"").expect_err("503");

    assert!(
        chosen
            .to_string()
            .starts_with(&format!("write_table at {}:", heavy.host())),
        "{chosen}"
    );
    assert!(
        given.to_string().starts_with("write_table:"),
        "a failure at the address the caller gave was reported as a routed one: {given}"
    );
}

#[test]
fn a_settled_lookup_survives_a_failed_upload() {
    // A 404 `/hosts` settles that the cluster serves its own heavy commands,
    // and a 503 at that address says nothing about the lookup.
    //
    // **The retry window is zero**: at the default ten seconds a forgotten
    // answer and a settled one look identical for the whole test.
    let control = Proxy::answering(Hosts::Absent, 503);
    let client = discovering(&control).with_hosts_retry_after(std::time::Duration::ZERO);

    assert!(client.write_table("//tmp/t", b"").is_err());
    assert!(client.write_table("//tmp/t", b"").is_err());

    assert_eq!(
        control.requests(),
        [
            "GET /hosts HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
        ],
        "a settled lookup was restarted by a failure that had nothing to do with it"
    );
}

#[test]
fn a_lookup_that_did_not_settle_is_asked_again_and_a_settled_one_is_not() {
    // `Configured` and `FellBack` differ only once the retry window has
    // passed, so the window is zero here. 404 is deterministic and settles the
    // question; 503 says only "not now".
    let settled = Proxy::answering(Hosts::Absent, 200);
    let client = discovering(&settled).with_hosts_retry_after(std::time::Duration::ZERO);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        settled.requests(),
        [
            "GET /hosts HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
        ],
        "a settled answer was asked about again"
    );

    // An answer declined in full is settled too: the names and the domain rule
    // will be the same next time.
    let refused = Proxy::new(Some(r#"["n0132-sas.somewhere-else.net"]"#.to_owned()));
    let client = discovering(&refused).with_hosts_retry_after(std::time::Duration::ZERO);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        refused.requests(),
        [
            "GET /hosts HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
        ],
        "an answer that was declined in full was asked for again"
    );

    let unsettled = Proxy::answering(Hosts::Status(503), 200);
    let client = discovering(&unsettled).with_hosts_retry_after(std::time::Duration::ZERO);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        unsettled.requests(),
        [
            "GET /hosts HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
            "GET /hosts HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
        ],
        "a lookup that failed for a reason that might pass was never repeated"
    );
}

#[test]
fn the_control_proxy_in_these_tests_refuses_a_heavy_command_as_a_real_one_does() {
    // The routing tests below depend on this: a stub serving every upload
    // cannot tell a client that routed from one that did not.
    let control = Proxy::control("[]".to_owned());

    let error = Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .write_table("//tmp/t", b"")
        .expect_err("a control proxy refuses a heavy request with input data");

    assert!(
        error
            .to_string()
            .contains("Control proxy may not serve heavy requests with input data"),
        "{error}"
    );
    // And the refusal names the switch that would have routed it.
    assert!(
        error.to_string().contains("with_proxy_discovery"),
        "the refusal says nothing about the routing that would have avoided it: {error}"
    );
}

#[test]
fn a_proxy_that_stumbles_is_dropped_from_the_pool_not_for_the_control_proxy() {
    // A data proxy answering one transient 503 is dropped from the pool; the
    // client does not fall back to the configured address, which with
    // separate roles is a balancer in front of control proxies that refuse
    // heavy commands.
    //
    // The pick is random, so the bad host costs exactly one command: writes
    // land on the good proxy until one lands on the bad and drops it. The
    // bound is 64 tries, which a fair pick does not miss by chance.
    let bad = Proxy::answering(Hosts::Absent, 503);
    let good = Proxy::new(None);
    let control = Proxy::control(format!(r#"["{}", "{}"]"#, bad.host(), good.host()));
    let client = discovering(&control);

    let mut failures = 0;
    for _ in 0..64 {
        if client.write_table("//tmp/t", b"").is_err() {
            failures += 1;
            break;
        }
    }
    assert_eq!(failures, 1, "the bad host was never picked in 64 writes");

    for _ in 0..8 {
        client
            .write_table("//tmp/t", b"")
            .expect("a write after the drop still reached the dropped host");
    }

    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1"],
        "an upload went back to the control proxy, which is what refuses one"
    );
    assert_eq!(
        bad.requests(),
        ["PUT /api/v4/write_table HTTP/1.1"],
        "the host that failed was not dropped after its first failure"
    );
}

#[test]
fn heavy_commands_spread_across_the_pool_rather_than_piling_onto_the_first() {
    // Always taking `[0]` of the load-ordered `/hosts` never rebalances, so
    // each command picks at random, as both official clients do.
    //
    // Over 64 commands and three hosts, a fair pick leaves a host unvisited
    // with probability 3·(2/3)^64 ≈ 1.6e-11; a pick pinned to a subset fails.
    let (a, b, c) = (Proxy::new(None), Proxy::new(None), Proxy::new(None));
    let control = Proxy::control(format!(
        r#"["{}", "{}", "{}"]"#,
        a.host(),
        b.host(),
        c.host()
    ));
    let client = discovering(&control);

    std::thread::scope(|scope| {
        for _ in 0..64 {
            scope.spawn(|| client.write_table("//tmp/t", b"").expect("writes"));
        }
    });

    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1"],
        "a fresh answer was re-asked, or an upload went to the control proxy"
    );
    for (name, proxy) in [("first", &a), ("second", &b), ("third", &c)] {
        assert!(
            !proxy.requests().is_empty(),
            "64 uploads over a pool of three never once landed on the {name} host"
        );
    }
}

#[test]
fn a_stale_answer_is_refreshed_by_the_next_heavy_command_and_a_fresh_one_is_not() {
    // The documentation says "re-query the /hosts list every minute or every
    // few queries". The refresh is lazy, as in the C++ SDK: the heavy command
    // that finds the answer stale asks first; no background thread asks.
    //
    // An interval of zero makes every answer stale, so the second write
    // re-asks; an hour-long interval does not. The default interval is
    // pinned in `every_heavy_shape_goes_there_and_the_cluster_is_asked_once`.
    let heavy = Proxy::new(None);
    let stale = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client = discovering(&stale).with_host_list_refresh_interval(std::time::Duration::ZERO);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        stale.requests(),
        ["GET /hosts HTTP/1.1", "GET /hosts HTTP/1.1"],
        "a stale answer was not refreshed before the next heavy command"
    );
    assert_eq!(heavy.requests().len(), 2);

    let heavy = Proxy::new(None);
    let fresh = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client =
        discovering(&fresh).with_host_list_refresh_interval(std::time::Duration::from_secs(3600));

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        fresh.requests(),
        ["GET /hosts HTTP/1.1"],
        "an answer well inside its interval was asked about again"
    );
    assert_eq!(heavy.requests().len(), 2);
}

/// An interval long enough to act inside and short enough to sleep past (each
/// sleep adds ~0.4 s). Zero could not tell "put off for another interval" from
/// "retried before the next upload".
const TEST_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

/// Sleeps `TEST_INTERVAL` out, with margin.
fn outlive_the_interval() {
    std::thread::sleep(TEST_INTERVAL + std::time::Duration::from_millis(100));
}

#[test]
fn a_refresh_that_fails_keeps_the_pool_and_waits_out_another_interval() {
    // A refresh that fails (a coordinator restarting) keeps the pool, and the
    // next try waits a whole interval, not the short retry window: nothing
    // waits on the answer, and an earlier retry would stall heavy traffic.
    // With the retry window at zero, a refresh retried on the wrong clock
    // asks before the third write.
    let heavy = Proxy::new(None);
    let flaky = Proxy::answering(
        Hosts::Sequence(vec![
            (200, format!(r#"["{}"]"#, heavy.host())),
            (503, String::new()),
        ]),
        200,
    );
    let client = discovering(&flaky)
        .with_host_list_refresh_interval(TEST_INTERVAL)
        .with_hosts_retry_after(std::time::Duration::ZERO);

    client.write_table("//tmp/t", b"").expect("writes");
    outlive_the_interval();
    client
        .write_table("//tmp/t", b"")
        .expect("a failed refresh dropped the pool");
    client
        .write_table("//tmp/t", b"")
        .expect("a failed refresh dropped the pool");

    assert_eq!(
        flaky.requests(),
        ["GET /hosts HTTP/1.1", "GET /hosts HTTP/1.1"],
        "a refresh that failed was retried in front of the very next upload"
    );
    assert_eq!(
        heavy.requests().len(),
        3,
        "an upload left the pool while the refresh was the only thing failing"
    );
}

#[test]
fn a_refresh_adopts_the_answer_it_fetched() {
    // A fresh answer replaces the pool: the cluster names one host, then
    // another, and the traffic moves. This also restores dropped hosts.
    let (first, second) = (Proxy::new(None), Proxy::new(None));
    let moving = Proxy::answering(
        Hosts::Sequence(vec![
            (200, format!(r#"["{}"]"#, first.host())),
            (200, format!(r#"["{}"]"#, second.host())),
        ]),
        200,
    );
    let client = discovering(&moving).with_host_list_refresh_interval(std::time::Duration::ZERO);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        first.requests(),
        ["PUT /api/v4/write_table HTTP/1.1"],
        "the first answer went unused, or was never given up"
    );
    assert_eq!(
        second.requests(),
        ["PUT /api/v4/write_table HTTP/1.1"],
        "the refreshed answer was fetched and thrown away"
    );
}

#[test]
fn a_dropped_host_is_restored_by_the_next_refresh() {
    // Without a ban list, a persistently bad host (a misissued certificate)
    // costs one failed command per interval, and a host that was draining
    // returns as soon as the cluster lists it again. A second failure after
    // the interval shows the host was restored.
    let bad = Proxy::answering(Hosts::Absent, 503);
    let good = Proxy::new(None);
    let control = Proxy::control(format!(r#"["{}", "{}"]"#, bad.host(), good.host()));
    let client = discovering(&control).with_host_list_refresh_interval(TEST_INTERVAL);

    let mut failures = 0;
    for _ in 0..64 {
        if client.write_table("//tmp/t", b"").is_err() {
            failures += 1;
            break;
        }
    }
    assert_eq!(failures, 1, "the bad host was never picked before the drop");

    outlive_the_interval();

    for _ in 0..64 {
        if client.write_table("//tmp/t", b"").is_err() {
            failures += 1;
            break;
        }
    }
    assert_eq!(
        failures, 2,
        "the dropped host was never restored by the refresh"
    );
    assert_eq!(
        bad.requests().len(),
        2,
        "restoration reached the bad host more (or less) than the drop accounts for"
    );
}

#[test]
fn a_refresh_that_answers_nobody_keeps_the_pool_in_hand() {
    // A fleet mid-rotation can briefly answer `[]`. The pool in hand is kept
    // for another interval, and uploads keep reaching the discovered host.
    let heavy = Proxy::new(None);
    let briefly_empty = Proxy::answering(
        Hosts::Sequence(vec![
            (200, format!(r#"["{}"]"#, heavy.host())),
            (200, "[]".to_owned()),
        ]),
        200,
    );
    let client =
        discovering(&briefly_empty).with_host_list_refresh_interval(std::time::Duration::ZERO);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        heavy.requests().len(),
        3,
        "an empty refresh answer took the pool down with it"
    );
}

#[test]
fn an_emptied_pool_asks_the_cluster_again_after_the_window() {
    // A pool with every host dropped uses the configured address for the
    // retry window; the first heavy command after it asks again.
    let dead = nowhere();
    let control = Proxy::new(Some(format!(r#"["{dead}"]"#)));
    let client = discovering(&control).with_hosts_retry_after(std::time::Duration::ZERO);

    let first = client.write_table("//tmp/t", b"");
    assert!(first.is_err(), "nothing was listening on {dead}");
    let _ = client.write_table("//tmp/t", b"");

    let asked = control
        .requests()
        .iter()
        .filter(|line| line.starts_with("GET /hosts"))
        .count();
    assert_eq!(asked, 2, "the fallback never ended");
}

#[test]
fn an_interval_of_forever_disables_the_refresh() {
    // `Duration::MAX` never elapses and does not panic: the pool compares
    // `elapsed()` with the interval rather than adding it to an `Instant`.
    let heavy = Proxy::new(None);
    let control = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client = discovering(&control).with_host_list_refresh_interval(std::time::Duration::MAX);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1"],
        "an interval of forever still refreshed"
    );
    assert_eq!(heavy.requests().len(), 2);
}

#[test]
fn a_cluster_that_named_nobody_is_asked_again_an_interval_later() {
    // An empty answer, as during a rolling restart, expires with the refresh
    // interval like a pool: an empty answer, then a real one, and the third
    // upload routes.
    let heavy = Proxy::new(None);
    let recovering = Proxy::answering(
        Hosts::Sequence(vec![
            (200, "[]".to_owned()),
            (200, format!(r#"["{}"]"#, heavy.host())),
        ]),
        200,
    );
    let client = discovering(&recovering).with_host_list_refresh_interval(TEST_INTERVAL);

    client.write_table("//tmp/t", b"").expect("writes");
    client.write_table("//tmp/t", b"").expect("writes");
    assert!(
        heavy.requests().is_empty(),
        "an answer well inside its interval was given up early"
    );

    outlive_the_interval();
    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(
        heavy.requests(),
        ["PUT /api/v4/write_table HTTP/1.1"],
        "a cluster that named nobody was never asked again"
    );
}

#[test]
fn a_proxy_that_refuses_heavy_work_is_given_up_like_one_that_cannot_be_reached() {
    // `/hosts` lists what `default_role_filter`, an operator-set coordinator
    // parameter, says, so it may name a control proxy. That refuses with
    // cluster code 1, not retriable but the host's fault, so it is dropped
    // like an unreachable host: at most one command reaches it.
    let good = Proxy::new(None);
    let wrong_role = Proxy::control("[]".to_owned());
    let control = Proxy::control(format!(r#"["{}", "{}"]"#, wrong_role.host(), good.host()));
    let client = discovering(&control);

    let mut failures = 0;
    for _ in 0..64 {
        if client.write_table("//tmp/t", b"").is_err() {
            failures += 1;
            break;
        }
    }
    assert_eq!(failures, 1, "the wrong-role host was never picked");

    for _ in 0..8 {
        client
            .write_table("//tmp/t", b"")
            .expect("a write after the drop still reached the refusing host");
    }

    assert_eq!(
        wrong_role.requests().len(),
        1,
        "the refusing host was not dropped after its first refusal"
    );
    assert_eq!(control.requests(), ["GET /hosts HTTP/1.1"]);
}

#[test]
fn a_refusal_at_the_configured_address_says_what_would_have_routed_it() {
    // A name declined under the domain rule leaves the control proxy's
    // refusal; the error names `with_heavy_proxies_anywhere`, the builder call
    // that would have used it.
    //
    // A literal `127.0.0.1` admits only itself, so `localhost` is refused.
    let elsewhere = Proxy::new(None);
    let named = format!("localhost:{}", elsewhere.address.port());
    let control = Proxy::control(format!(r#"["{named}"]"#));

    let error = discovering(&control)
        .write_table("//tmp/t", b"")
        .expect_err("a control proxy refuses a heavy request with input data");

    assert!(
        error.to_string().contains("with_heavy_proxies_anywhere"),
        "the refusal does not say that a name was declined: {error}"
    );
    assert!(
        elsewhere.requests().is_empty(),
        "the token went to a host the caller never named: {:?}",
        elsewhere.requests()
    );
}

#[test]
fn a_list_of_proxies_written_out_by_hand_is_what_is_used() {
    // An explicit list is a boundary: `anywhere` is all-or-nothing, and the
    // domain rule is a typo guard that admits every tenant of a shared parent
    // domain.
    let allowed = Proxy::new(None);
    let refused = Proxy::new(None);
    let control = Proxy::control(format!(
        r#"["localhost:{}", "localhost:{}"]"#,
        refused.address.port(),
        allowed.address.port()
    ));

    Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_heavy_proxies_in([format!("localhost:{}", allowed.address.port())])
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert_eq!(control.requests(), ["GET /hosts HTTP/1.1"]);
    assert_eq!(allowed.requests(), ["PUT /api/v4/write_table HTTP/1.1"]);
    assert!(
        refused.requests().is_empty(),
        "a name outside the list was used: {:?}",
        refused.requests()
    );
}

#[test]
fn the_lookup_budget_can_be_raised_and_not_only_lowered() {
    // The default lookup budget gives up on a 1.2 s `/hosts`;
    // `with_hosts_timeout` raises it. The first heavy command is often the
    // client's first request, with DNS, TCP and TLS inside the budget.
    let heavy = Proxy::new(None);
    let slow = Hosts::Slow(
        std::time::Duration::from_millis(1200),
        format!(r#"["{}"]"#, heavy.host()),
    );
    let control = Proxy::answering(slow, 200);

    Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert!(
        heavy.requests().is_empty(),
        "the default budget waited out a cluster it is meant to give up on"
    );

    Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_hosts_timeout(std::time::Duration::from_secs(3))
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert_eq!(
        heavy.requests(),
        ["PUT /api/v4/write_table HTTP/1.1"],
        "the budget could not be raised, so this cluster is unroutable"
    );
}

#[test]
fn a_mistake_at_the_heavy_proxy_does_not_send_the_client_back_to_ask() {
    // A missing table is missing at every proxy. Only a failure another proxy
    // might not have (a refused connection, a 503, a banned proxy) gives up a
    // resolved address.
    let heavy = Proxy::answering(Hosts::Absent, 404);
    let control = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client = discovering(&control);

    assert!(client.write_table("//tmp/t", b"").is_err());
    assert!(client.write_table("//tmp/t", b"").is_err());

    assert_eq!(control.requests(), ["GET /hosts HTTP/1.1"]);
    assert_eq!(
        heavy.requests(),
        [
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
        ],
        "the client threw away a working address because a command was wrong"
    );
}

#[test]
fn a_discovery_off_client_never_touches_the_routing_state() {
    // `with_proxy_discovery(false)` turns the whole feature off: a heavy
    // failure costs no lock and no lookup.
    let control = Proxy::answering(Hosts::List(r#"["heavy.example.net"]"#.to_owned()), 503);

    let client = Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(false);

    assert!(client.write_table("//tmp/t", b"").is_err());
    assert!(client.write_table("//tmp/t", b"").is_err());

    assert_eq!(
        control.requests(),
        [
            "PUT /api/v4/write_table HTTP/1.1",
            "PUT /api/v4/write_table HTTP/1.1",
        ]
    );
}

#[test]
fn a_blank_name_in_the_answer_is_passed_over_rather_than_believed() {
    // A blank name is one of the shapes `heavy_base` refuses; the rest of the
    // list is still used.
    let heavy = Proxy::new(None);
    let control = Proxy::new(Some(format!(r#"["", "   ", "{}"]"#, heavy.host())));
    let client = discovering(&control);

    client.write_table("//tmp/t", b"").expect("writes");

    assert_eq!(control.requests(), ["GET /hosts HTTP/1.1"]);
    assert_eq!(heavy.requests(), ["PUT /api/v4/write_table HTTP/1.1"]);
}

#[test]
fn an_answer_that_is_not_a_list_of_host_names_settles_the_question() {
    // An unreadable body is settled like an empty list: the configured
    // address serves the uploads, and no lookup goes in front of them.
    for nonsense in [
        "not json at all",
        r#"{"hosts": ["n0132-sas.example.net"]}"#,
        "[1, 2, 3]",
        "null",
    ] {
        let control = Proxy::answering(Hosts::List(nonsense.to_owned()), 200);
        let client = discovering(&control);

        client.write_table("//tmp/t", b"").expect("writes");
        client.write_table("//tmp/t", b"").expect("writes");

        assert_eq!(
            control.requests(),
            [
                "GET /hosts HTTP/1.1",
                "PUT /api/v4/write_table HTTP/1.1",
                "PUT /api/v4/write_table HTTP/1.1",
            ],
            "{nonsense:?} was asked about twice, or routed somewhere"
        );
    }
}

#[test]
fn a_host_outside_the_configured_domain_is_refused() {
    // A heavy command carries the token, and on plain http the `/hosts` body
    // is as easy to forge as a `Location` header, so a name from another
    // domain is passed over and the upload goes to the configured address.
    //
    // A literal `127.0.0.1` admits only itself, so `localhost` is refused.
    let elsewhere = Proxy::new(None);
    let named = format!("localhost:{}", elsewhere.address.port());
    let control = Proxy::new(Some(format!(r#"["{named}"]"#)));

    discovering(&control)
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert_eq!(
        control.requests(),
        ["GET /hosts HTTP/1.1", "PUT /api/v4/write_table HTTP/1.1"],
    );
    assert!(
        elsewhere.requests().is_empty(),
        "the token went to a host the caller never named: {:?}",
        elsewhere.requests()
    );
}

#[test]
fn an_installation_that_really_does_answer_elsewhere_can_opt_in() {
    // An installation whose `/hosts` names another domain (a vanity address,
    // data proxies in another zone) opts in with one builder call.
    let heavy = Proxy::new(None);
    let named = format!("localhost:{}", heavy.address.port());
    let control = Proxy::new(Some(format!(r#"["{named}"]"#)));

    Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_heavy_proxies_anywhere(true)
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert_eq!(control.requests(), ["GET /hosts HTTP/1.1"]);
    assert_eq!(heavy.requests(), ["PUT /api/v4/write_table HTTP/1.1"]);
}

#[test]
fn the_lookup_has_its_own_budget_and_a_heavy_command_does_not_wait_out_the_clients() {
    // The lookup has its own budget, not the client's retry policy (five
    // attempts, backoff, two minutes each), under which a 503 `/hosts` was
    // measured to cost a heavy command 15 s and a hung one 615 s. The 5 s
    // bound is loose; the 615 s it rules out is two orders of magnitude away.
    for (what, hosts) in [
        ("503", Hosts::Status(503)),
        ("no answer at all", Hosts::Hang),
    ] {
        let control = Proxy::answering(hosts, 200);
        // The default policy, not `none()`: the lookup must not inherit it.
        let client = Client::new(&control.url()).with_proxy_discovery(true);

        let started = std::time::Instant::now();
        client.write_table("//tmp/t", b"").expect("writes");
        let elapsed = started.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "a heavy command waited {elapsed:?} on a /hosts that answered {what}"
        );
    }
}

#[test]
fn waiting_threads_do_not_queue_up_behind_a_failing_lookup() {
    // Eight threads, one hung `/hosts`. A failed lookup leaves a verdict (the
    // configured address, for the retry window), so the seven behind the
    // first do not repeat it. With the lookup under the client's five-attempt
    // policy and a failure leaving the state unasked, this was measured at
    // 8 x 5 = 40 lookups and 240 s.
    let control = Proxy::answering(Hosts::Hang, 200);
    let client = Client::new(&control.url()).with_proxy_discovery(true);

    let started = std::time::Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| client.write_table("//tmp/t", b"").expect("writes"));
        }
    });
    let elapsed = started.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "eight threads took {elapsed:?}, which is a queue rather than one lookup"
    );
    let asked = control
        .requests()
        .iter()
        .filter(|line| line.starts_with("GET /hosts"))
        .count();
    assert!(asked <= 2, "{asked} lookups for one question");
}

#[test]
fn eight_threads_against_a_healthy_cluster_still_ask_exactly_once() {
    // The lock is held across the lookup so that the seven threads behind the
    // first read its answer instead of asking again.
    let heavy = Proxy::new(None);
    let control = Proxy::new(Some(format!(r#"["{}"]"#, heavy.host())));
    let client = discovering(&control);

    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| client.write_table("//tmp/t", b"").expect("writes"));
        }
    });

    assert_eq!(control.requests(), ["GET /hosts HTTP/1.1"]);
    assert_eq!(heavy.requests().len(), 8);
}

#[test]
fn a_local_cluster_is_never_asked_where_to_send_a_heavy_command() {
    // A cluster on loopback is local or tunnelled, and the address it
    // publishes is not reachable from here, so by default no lookup is made.
    let control = Proxy::new(Some(r#"["heavy.example.net"]"#.to_owned()));

    Client::new(&control.url())
        .with_retries(RetryPolicy::none())
        .write_table("//tmp/t", b"")
        .expect("writes");

    assert_eq!(control.requests(), ["PUT /api/v4/write_table HTTP/1.1"]);
}
