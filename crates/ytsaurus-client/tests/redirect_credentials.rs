//! What becomes of a request that is redirected while carrying a token.
//!
//! A control proxy answers a heavy read with a **cross-host** `307`, naming a
//! data proxy. `ureq` follows that and drops the `Authorization` header on the
//! way, so the request arrives unauthenticated and the cluster reports `Client
//! is missing credentials` about a token that is perfectly valid.
//!
//! None of this can be reproduced against the local cluster this repository
//! tests with: it runs one proxy and redirects nothing. So it is reproduced
//! here, the way `request_shape.rs` reproduces the rest of the wire — with
//! sockets in-process. There are two stubs: one that redirects, and one
//! standing in for the data proxy, which answers exactly the error the issue
//! reports and **must never be reached**.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use ytsaurus_client::{Client, ClientError, RetryPolicy};

/// A listener that answers every request with `reply` and remembers what it was
/// asked.
///
/// Detached rather than joined: half of what these prove is that nobody
/// connected at all, and there is nothing to wait for in that case. The thread
/// ends with the test process.
fn stub(reply: String) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let address = listener.local_addr().expect("has an address");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);

    std::thread::spawn(move || {
        while let Ok((mut stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().expect("clones"));
            let head = read_head(&mut reader);
            // Recorded before the reply goes out, so a client that has been
            // answered has already been counted: the assertions run after the
            // call returns, and would otherwise race it.
            recorded.lock().expect("not poisoned").push(head);
            stream.write_all(reply.as_bytes()).ok();
            stream.flush().ok();
        }
    });

    (format!("http://{address}"), seen)
}

/// Reads one request head. Every request here is a GET, so there is no body.
fn read_head(reader: &mut BufReader<TcpStream>) -> String {
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
    head
}

/// The answer a control proxy gives a heavy read: go to that other host.
fn redirect_to(location: &str) -> String {
    format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n")
}

/// The answer the data proxy gives a request that arrived without its token —
/// the misleading one this whole issue is about.
fn missing_credentials() -> String {
    let error = r#"{"code":111,"message":"Client is missing credentials"}"#;
    format!("HTTP/1.1 401 Unauthorized\r\nX-YT-Error: {error}\r\nContent-Length: 0\r\n\r\n")
}

/// An ordinary `exists` answer, for the stub that is allowed to be reached.
fn exists_answer() -> String {
    let body = r#"{"value"=%true}"#;
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-yt-yson-text\r\n\r\n{body}",
        body.len()
    )
}

fn heads(seen: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    seen.lock().expect("not poisoned").clone()
}

#[test]
fn a_redirected_read_goes_nowhere_and_says_where_it_was_sent() {
    // The exact shape of the bug: `read_table` against a control proxy, which
    // answers 307 pointing at a data proxy on another host.
    let (data_proxy, data_seen) = stub(missing_credentials());
    let target = format!("{data_proxy}/api/v4/read_table?path=//tmp/t");
    let (control_proxy, control_seen) = stub(redirect_to(&target));

    let client =
        Client::with_token(&control_proxy, "secret-token").with_retries(RetryPolicy::none());
    let error = client
        .read_table("//tmp/t")
        .expect_err("a redirect carrying credentials is refused");

    let ClientError::Redirected {
        status, location, ..
    } = &error
    else {
        panic!("a refused redirect must not arrive as anything else: {error:?}");
    };
    assert_eq!(*status, 307);
    assert_eq!(location, &target, "the caller is not told where it pointed");

    // The token went to the proxy the caller named, and to nowhere else. This
    // is the assertion the fix exists for: a bearer token must not follow a
    // `Location` header to a host nobody chose.
    let asked = heads(&control_seen);
    assert_eq!(asked.len(), 1, "{asked:?}");
    assert!(
        asked[0].starts_with("GET /api/v4/read_table"),
        "{}",
        asked[0]
    );
    assert!(
        asked[0].to_lowercase().contains("authorization: oauth"),
        "{}",
        asked[0]
    );
    assert!(
        heads(&data_seen).is_empty(),
        "the request followed the redirect: {:?}",
        heads(&data_seen)
    );
}

#[test]
fn the_message_points_at_the_redirect_and_not_at_the_token() {
    // The second half of the report, and the more expensive half: `cluster
    // error 111: Client is missing credentials` sends a user to check their
    // token, their token file and their permissions, none of which is wrong.
    let (data_proxy, _) = stub(missing_credentials());
    let target = format!("{data_proxy}/api/v4/read_table");
    let (control_proxy, _) = stub(redirect_to(&target));

    let client =
        Client::with_token(&control_proxy, "secret-token").with_retries(RetryPolicy::none());
    let message = client
        .read_table("//tmp/t")
        .expect_err("refused")
        .to_string();

    assert!(message.contains("307"), "{message}");
    assert!(message.contains("redirected to"), "{message}");
    assert!(message.contains(&target), "{message}");
    assert!(
        !message.contains("missing credentials") && !message.contains("111"),
        "the failure is still being blamed on the token: {message}"
    );
    // And what to do instead, since the caller has to do something.
    assert!(message.contains("heavy proxy"), "{message}");
}

#[test]
fn the_hosts_lookup_refuses_a_redirect_too() {
    // `/hosts` builds its own request, which is how it once came to carry
    // neither the token nor the timeout. It carries the token now, so it has
    // one to lose — and it is the lookup a caller reaches for *because* of this
    // issue, which would be a poor moment for it to leak the credential it was
    // finally given.
    let (elsewhere, elsewhere_seen) = stub(missing_credentials());
    let target = format!("{elsewhere}/hosts");
    let (proxy, _) = stub(redirect_to(&target));

    let client = Client::with_token(&proxy, "secret-token").with_retries(RetryPolicy::none());
    let error = client.heavy_proxy().expect_err("refused");

    assert!(
        matches!(&error, ClientError::Redirected { location, .. } if location == &target),
        "{error:?}"
    );
    assert!(heads(&elsewhere_seen).is_empty(), "the token was forwarded");
}

#[test]
fn a_client_with_no_token_still_follows_a_redirect() {
    // The rule is about credentials, not about redirects: a request with
    // nothing to lose keeps the behaviour every release so far has had. Making
    // this conditional is what keeps the change to the thing the report is
    // about — and this test is what says so out loud, so that a later reader
    // does not read the refusal as "this client hates redirects".
    let (elsewhere, elsewhere_seen) = stub(exists_answer());
    let target = format!("{elsewhere}/api/v4/exists");
    let (proxy, proxy_seen) = stub(redirect_to(&target));

    let client = Client::new(&proxy).with_retries(RetryPolicy::none());
    assert_eq!(client.exists("//tmp").ok(), Some(true));

    assert_eq!(heads(&proxy_seen).len(), 1);
    let followed = heads(&elsewhere_seen);
    assert_eq!(
        followed.len(),
        1,
        "the redirect was not followed: {followed:?}"
    );
    assert!(
        !followed[0].to_lowercase().contains("authorization:"),
        "there was no token to send: {}",
        followed[0]
    );
}
