//! What the client actually puts on the wire.
//!
//! Everything else in this crate is checked against a cluster, which answers
//! the same whether or not the request was well made. These serve one request
//! from a socket in-process and read the bytes the client sent, which is the
//! only way to pin the things a cluster is too forgiving to notice:
//! compression the client asks for, the token it carries, and the header the
//! parameters travel in.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

use ytsaurus_client::{Client, RetryPolicy};

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
