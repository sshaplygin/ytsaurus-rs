//! What becomes of a request that is redirected.
//!
//! A control proxy answers a heavy read with a **cross-host** `307`, naming a
//! data proxy. `ureq` follows that and drops the `Authorization` header on the
//! way, so the request arrives unauthenticated and the cluster reports `Client
//! is missing credentials` about a token that is perfectly valid.
//!
//! That is the report these begin with, and not all of what they hold. This
//! client follows redirects itself now, so the rest of the policy is here too:
//! a redirect that stays on the same origin **is** followed, token and all; one
//! on a request carrying a body is refused whether there is a token or not,
//! because a redirect drops the body and a write that wrote nothing comes back
//! looking like a write that worked.
//!
//! None of this can be reproduced against the local cluster this repository
//! tests with: it runs one proxy and redirects nothing. So it is reproduced
//! here, the way `request_shape.rs` reproduces the rest of the wire — with
//! sockets in-process. Two stubs, mostly: one that redirects, and one standing
//! in for the host it points at, which answers exactly the error the issue
//! reports and **must never be reached**.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use ytsaurus_client::{Client, ClientError, RedirectRefusal, RetryPolicy};

/// A listener that answers every request with `reply` and remembers what it was
/// asked.
fn stub(reply: String) -> (String, Arc<Mutex<Vec<String>>>) {
    stub_answering(vec![reply])
}

/// As [`stub`], answering `replies` in order and repeating the last one.
///
/// Detached rather than joined: half of what these prove is that nobody
/// connected at all, and there is nothing to wait for in that case. The thread
/// ends with the test process.
fn stub_answering(replies: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let address = listener.local_addr().expect("has an address");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);

    std::thread::spawn(move || {
        let mut answered = 0_usize;
        while let Ok((mut stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().expect("clones"));
            let head = read_head(&mut reader);
            let reply = &replies[answered.min(replies.len() - 1)];
            answered += 1;
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

/// Reads one request head, and stops there.
///
/// The one request with a body is small enough to sit in the socket buffer
/// while the reply goes out, and it is a request that must never be answered
/// on its merits anyway.
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
    redirect_with(307, "Temporary Redirect", location, "")
}

/// A redirect of any status, optionally carrying extra headers.
fn redirect_with(status: u16, reason: &str, location: &str, extra: &str) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nLocation: {location}\r\n{extra}Content-Length: 0\r\n\r\n"
    )
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

/// What a proxy answers a write with — and, if a redirect were followed, what
/// it would answer the emptied `GET` that arrived instead.
fn empty_answer() -> String {
    let body = "{}";
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
    // Not a bare `111`: the target carries an ephemeral port, and one in a
    // hundred of those has those three digits in it.
    assert!(
        !message.contains("missing credentials") && !message.contains("cluster error 111"),
        "the failure is still being blamed on the token: {message}"
    );
    // What the client is actually certain of, rather than a verdict on a token
    // it never got an answer about.
    assert!(
        message.contains("was not sent to the host that answered"),
        "{message}"
    );
    // And what to do instead, since the caller has to do something.
    assert!(message.contains("heavy proxy"), "{message}");
}

#[test]
fn a_redirected_light_command_is_not_sent_to_a_heavy_proxy() {
    // The other half of the message: `create` is not a heavy command, there is
    // no heavy proxy that would have answered it, and telling its caller to go
    // and find one is advice that cannot be taken.
    let (elsewhere, elsewhere_seen) = stub(missing_credentials());
    let (proxy, _) = stub(redirect_to(&format!("{elsewhere}/api/v4/create")));

    let client = Client::with_token(&proxy, "secret-token").with_retries(RetryPolicy::none());
    let message = client
        .create("map_node", "//tmp/thing")
        .expect_err("refused")
        .to_string();

    assert!(message.contains("redirected to"), "{message}");
    assert!(
        !message.contains("heavy proxy"),
        "a light command was told to go to a heavy proxy: {message}"
    );
    assert!(heads(&elsewhere_seen).is_empty(), "{message}");
}

#[test]
fn a_redirect_is_read_before_the_clusters_own_error() {
    // A proxy is free to send both, and one that does must not turn the
    // refusal back into the misleading report it exists to replace: `407`
    // arrives on the redirect itself here, and the answer is still that this
    // client went nowhere.
    let error = r#"{"code":111,"message":"Client is missing credentials"}"#;
    let (elsewhere, elsewhere_seen) = stub(missing_credentials());
    let target = format!("{elsewhere}/api/v4/read_table");
    let (proxy, _) = stub(redirect_with(
        307,
        "Temporary Redirect",
        &target,
        &format!("X-YT-Error: {error}\r\n"),
    ));

    let client = Client::with_token(&proxy, "secret-token").with_retries(RetryPolicy::none());
    let error = client.read_table("//tmp/t").expect_err("refused");

    assert!(
        matches!(
            &error,
            ClientError::Redirected {
                refusal: RedirectRefusal::Credentials,
                location,
                ..
            } if location == &target
        ),
        "the cluster's error header was read first: {error:?}"
    );
    assert!(heads(&elsewhere_seen).is_empty(), "{error:?}");
}

#[test]
fn a_302_is_refused_like_a_307() {
    // 307 is the one the reference names for heavy routing, and it is not the
    // only redirect a request meets: a balancer in front of the cluster sends
    // 301 and 302, and a credential does not care which digit moved it.
    let (elsewhere, elsewhere_seen) = stub(exists_answer());
    let target = format!("{elsewhere}/api/v4/exists");
    let (proxy, _) = stub(redirect_with(302, "Found", &target, ""));

    let client = Client::with_token(&proxy, "secret-token").with_retries(RetryPolicy::none());
    let error = client.exists("//tmp").expect_err("refused");

    assert!(
        matches!(&error, ClientError::Redirected { status: 302, .. }),
        "{error:?}"
    );
    assert!(heads(&elsewhere_seen).is_empty(), "the token was forwarded");
}

#[test]
fn a_redirect_that_stays_on_the_host_is_followed_with_the_token() {
    // The rule is about the host the credentials reach, not about redirects. A
    // balancer canonicalising its own paths sends a relative `Location` back to
    // itself; refusing that would break every command against such an
    // installation, and would protect nothing — the token is already going
    // there.
    let (proxy, seen) = stub_answering(vec![
        redirect_with(301, "Moved Permanently", "/api/v4/exists?path=//tmp", ""),
        exists_answer(),
    ]);

    let client = Client::with_token(&proxy, "secret-token").with_retries(RetryPolicy::none());
    assert_eq!(client.exists("//tmp").ok(), Some(true));

    let asked = heads(&seen);
    assert_eq!(asked.len(), 2, "the redirect was not followed: {asked:?}");
    for head in &asked {
        assert!(
            head.to_lowercase().contains("authorization: oauth"),
            "the token was dropped on a host it was already addressed to: {head}"
        );
    }
    assert!(asked[1].starts_with("GET /api/v4/exists"), "{}", asked[1]);
}

#[test]
fn a_redirect_that_never_arrives_anywhere_is_a_loop_and_not_a_route() {
    // The cost of following redirects here rather than leaving them to `ureq`
    // is that the bound is ours to keep. A balancer pointing at itself would
    // otherwise be an unbounded loop inside one attempt, under a timeout that
    // each hop resets.
    let (proxy, seen) = stub(redirect_with(
        301,
        "Moved Permanently",
        "/api/v4/exists?path=//tmp",
        "",
    ));

    let client = Client::with_token(&proxy, "secret-token").with_retries(RetryPolicy::none());
    let error = client.exists("//tmp").expect_err("a loop is refused");

    assert!(
        matches!(
            &error,
            ClientError::Redirected {
                refusal: RedirectRefusal::TooMany,
                ..
            }
        ),
        "{error:?}"
    );
    // Ten hops, plus the request that started it.
    assert_eq!(heads(&seen).len(), 11);
}

#[test]
fn a_write_is_not_redirected_into_a_successful_nothing() {
    // No token, so nothing to leak — and the expensive failure anyway. A
    // redirect rewrites a PUT into a GET and drops the body; the cluster
    // answers the empty request, and `write_table` returns `Ok(())` having
    // written no rows at all. A write that quietly wrote nothing is worse than
    // one that failed.
    let (elsewhere, elsewhere_seen) = stub(empty_answer());
    let (proxy, _) = stub(redirect_with(
        302,
        "Found",
        &format!("{elsewhere}/api/v4/write_table"),
        "",
    ));

    let client = Client::new(&proxy).with_retries(RetryPolicy::none());
    let error = client
        .write_table("//tmp/t", b"{a=1};")
        .expect_err("a redirect that would drop the rows is refused");

    assert!(
        matches!(
            &error,
            ClientError::Redirected {
                refusal: RedirectRefusal::Body,
                status: 302,
                ..
            }
        ),
        "{error:?}"
    );
    assert!(
        heads(&elsewhere_seen).is_empty(),
        "the rows were dropped on the way: {:?}",
        heads(&elsewhere_seen)
    );
}

#[test]
fn a_redirected_streaming_read_goes_nowhere() {
    // `read_table_streaming` is one of exactly two paths that reconfigure the
    // request before sending it — the streaming timeout override — and so one
    // of two where the redirect policy is inherited rather than stated. A
    // `max_redirects` that crept back into that override would leak the token
    // here and nowhere else.
    let (data_proxy, data_seen) = stub(missing_credentials());
    let target = format!("{data_proxy}/api/v4/read_table?path=//tmp/t");
    let (control_proxy, control_seen) = stub(redirect_to(&target));

    let client =
        Client::with_token(&control_proxy, "secret-token").with_retries(RetryPolicy::none());
    let error = client
        .read_table_streaming("//tmp/t")
        .expect_err("a redirect carrying credentials is refused");

    assert!(
        matches!(&error, ClientError::Redirected { location, .. } if location == &target),
        "{error:?}"
    );
    assert_eq!(heads(&control_seen).len(), 1);
    assert!(
        heads(&data_seen).is_empty(),
        "the streaming read followed the redirect: {:?}",
        heads(&data_seen)
    );
}

#[test]
fn a_redirected_job_input_goes_nowhere() {
    // The other request built through the streaming override, and the one a
    // launcher reaches for while diagnosing a failure — the worst moment to
    // hand a token to whichever host answered.
    let (data_proxy, data_seen) = stub(missing_credentials());
    let target = format!("{data_proxy}/api/v4/get_job_input");
    let (control_proxy, _) = stub(redirect_to(&target));

    let client =
        Client::with_token(&control_proxy, "secret-token").with_retries(RetryPolicy::none());
    let error = client
        .get_job_input("1-2-3-4", "5-6-7-8")
        .expect_err("a redirect carrying credentials is refused");

    assert!(
        matches!(&error, ClientError::Redirected { location, .. } if location == &target),
        "{error:?}"
    );
    assert!(
        heads(&data_seen).is_empty(),
        "the job input followed the redirect: {:?}",
        heads(&data_seen)
    );
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
