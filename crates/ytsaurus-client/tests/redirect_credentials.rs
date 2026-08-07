//! What becomes of a request that is redirected.
//!
//! A control proxy answers a heavy read with a **cross-host** `307`, naming a
//! data proxy. `ureq` follows that and drops the `Authorization` header on the
//! way, so the request arrives unauthenticated and the cluster reports `Client
//! is missing credentials` about a token that is perfectly valid.
//!
//! That is the report these begin with, and not all of what they hold. This
//! client follows redirects itself now, so the rest of the policy is here too.
//! A redirect that stays on the **same origin** is followed — token, method and
//! body — so a `POST create` met by a balancer's `301` arrives as a `POST
//! create`, and a `write_table` sends its rows on rather than losing them.
//!
//! Crossing an origin is where things are withheld, and there are two of them:
//! the **token**, and the request's **data**. Neither waits on the other — a
//! tokenless `write_table` does not get to send a table to whichever host a
//! `Location` names — and a body of length zero is not data, so a bodiless
//! command still goes. Separately, a body this client cannot send a second time
//! (a *stream*) is refused wherever it points: by the time the `3xx` arrives
//! some of it has gone, and a write that arrived with no rows comes back
//! looking like a write that worked.
//!
//! And the whole chain is bounded twice over — by ten hops, and by the
//! command's own timeout, which the hops share rather than each being handed a
//! copy of.
//!
//! None of this can be reproduced against the local cluster this repository
//! tests with: it runs one proxy and redirects nothing. So it is reproduced
//! here, the way `request_shape.rs` reproduces the rest of the wire — with
//! sockets in-process. Two stubs, mostly: one that redirects, and one standing
//! in for the host it points at, which answers exactly the error the issue
//! reports and **must never be reached**.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ytsaurus_client::{Client, ClientError, RedirectRefusal, RetryPolicy};

/// A listener that answers every request with `reply` and remembers what it was
/// asked.
fn stub(reply: String) -> (String, Arc<Mutex<Vec<String>>>) {
    stub_answering(vec![reply])
}

/// As [`stub`], answering `replies` in order and repeating the last one.
fn stub_answering(replies: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
    stub_answering_after(Duration::ZERO, replies)
}

/// As [`stub_answering`], taking `delay` to think about each request.
///
/// Detached rather than joined: half of what these prove is that nobody
/// connected at all, and there is nothing to wait for in that case. The thread
/// ends with the test process.
fn stub_answering_after(
    delay: Duration,
    replies: Vec<String>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let address = listener.local_addr().expect("has an address");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);

    std::thread::spawn(move || {
        let mut answered = 0_usize;
        while let Ok((mut stream, _)) = listener.accept() {
            // So a client that goes away mid-request fails this stub rather
            // than hanging the suite behind a read that will never finish.
            stream.set_read_timeout(Some(STUB_PATIENCE)).ok();
            let mut reader = BufReader::new(stream.try_clone().expect("clones"));
            let request = read_request(&mut reader);
            let reply = &replies[answered.min(replies.len() - 1)];
            answered += 1;
            // Recorded before the reply goes out, so a client that has been
            // answered has already been counted: the assertions run after the
            // call returns, and would otherwise race it.
            recorded.lock().expect("not poisoned").push(request);
            std::thread::sleep(delay);
            stream.write_all(reply.as_bytes()).ok();
            stream.flush().ok();
        }
    });

    (format!("http://{address}"), seen)
}

/// How long the stub waits on a request that has stopped arriving.
const STUB_PATIENCE: Duration = Duration::from_secs(10);

/// Reads one request **whole** — head and body — and only then returns.
///
/// Whole, and that is the load-bearing word. Answering a request whose body is
/// still being written closes the connection under the client, and what it sees
/// then is `EPIPE` rather than the reply: the answer never gets read, and the
/// decision the test is about is never reached. A body with a `Content-Length`
/// is small enough here to sit in the socket buffer and survive that; a
/// **chunked** one is written by `ureq` as the reader produces it, and a Linux
/// runner loses the race that macOS won. `write_table_rows` sends exactly that,
/// so the fix is to stop racing rather than to hope for a buffer — the stub
/// reads to the end of the body in both framings before it says anything.
///
/// Reading it also makes it evidence: whether the rows arrived at the second
/// host is then a question with an answer rather than something taken on trust.
fn read_request(reader: &mut BufReader<TcpStream>) -> String {
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

    let header = |name: &str| {
        head.lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_owned())
    };

    let body = if header("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        read_chunked(reader)
    } else {
        let length = header("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0; length];
        if reader.read_exact(&mut body).is_err() {
            body.clear();
        }
        body
    };

    format!("{head}\r\n{}", String::from_utf8_lossy(&body))
}

/// Drains a chunked body to its terminating zero-length chunk.
///
/// Bails on anything it cannot parse rather than looping: a stub that hung on
/// a malformed chunk would hang the test, and there is nothing here it could
/// usefully say about one.
fn read_chunked(reader: &mut BufReader<TcpStream>) -> Vec<u8> {
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        // `size[;extension]`, in hex.
        let size = line.trim().split(';').next().unwrap_or("");
        let Ok(size) = usize::from_str_radix(size, 16) else {
            break;
        };
        if size == 0 {
            // The trailer section — empty in practice — ends at a blank line.
            loop {
                let mut end = String::new();
                match reader.read_line(&mut end) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if end == "\r\n" => break,
                    Ok(_) => {}
                }
            }
            break;
        }

        let mut chunk = vec![0; size];
        if reader.read_exact(&mut chunk).is_err() {
            break;
        }
        body.extend_from_slice(&chunk);
        // The CRLF that closes the chunk.
        if reader.read_exact(&mut [0; 2]).is_err() {
            break;
        }
    }
    body
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
    // otherwise go round until the command's deadline ran out — two minutes by
    // default, spent on a route that was never going to arrive.
    //
    // The assertions below are what keep it, and they are the only thing that
    // does. Measured with `MAX_REDIRECTS` raised out of the way: **nothing
    // hangs**. The stub gives out after about eight seconds of connections it
    // cannot keep up with, and the run fails with a transport error — `Peer
    // disconnected` — rather than with the refusal that should have arrived
    // after ten hops. So read a failure here as "the bound is gone", not as
    // "the stub is flaky", and do not expect a missing bound to announce
    // itself by making the suite sit still.
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
fn a_buffered_write_is_sent_again_rather_than_emptied() {
    // The expensive failure this rule exists for: a redirect that rewrote the
    // PUT into a GET and dropped the body, so `write_table` returned `Ok(())`
    // having written no rows at all.
    //
    // Following the redirect *ourselves* answers it outright — the same method
    // and the same rows go where the proxy pointed. On the **same origin**,
    // which is the case the rule is for: a balancer canonicalising its own
    // paths. The rows were already going to that host. Crossing to another one
    // is the test below, and is refused.
    let (proxy, seen) = stub_answering(vec![
        redirect_with(307, "Temporary Redirect", "/api/v4/write_table", ""),
        empty_answer(),
    ]);

    let client = Client::new(&proxy).with_retries(RetryPolicy::none());
    client
        .write_table("//tmp/t", b"{a=1};")
        .expect("the rows are sent again rather than dropped");

    let asked = heads(&seen);
    assert_eq!(asked.len(), 2, "the redirect was not followed: {asked:?}");
    for head in &asked {
        assert!(
            head.starts_with("PUT /api/v4/write_table"),
            "the method did not survive the hop: {head}"
        );
        assert!(
            head.ends_with("{a=1};"),
            "the rows were dropped on the way: {head}"
        );
    }
}

#[test]
fn a_buffered_write_does_not_take_the_rows_to_another_host() {
    // The other half, and the one following redirects at all put at risk: with
    // no token there is no credential to refuse over, and the rows are
    // re-sendable — so nothing but this rule stops a `Location` header from
    // redirecting a table's contents to whichever host it names. That is not
    // the silent nothing the rule above prevents; it is the caller's data going
    // somewhere the caller never chose, which is the same objection the token
    // gets and deserves the same answer.
    let (elsewhere, elsewhere_seen) = stub(empty_answer());
    let (proxy, proxy_seen) = stub(redirect_with(
        302,
        "Found",
        &format!("{elsewhere}/api/v4/write_table"),
        "",
    ));

    let client = Client::new(&proxy).with_retries(RetryPolicy::none());
    let error = client
        .write_table("//tmp/t", b"{a=1};")
        .expect_err("rows do not cross an origin on a header's say-so");

    assert!(
        matches!(
            &error,
            ClientError::Redirected {
                refusal: RedirectRefusal::Payload,
                status: 302,
                ..
            }
        ),
        "{error:?}"
    );
    // Asked of the second stub rather than of the error: what matters is that
    // the bytes did not arrive, not that a `Result` said so.
    assert!(
        heads(&elsewhere_seen).is_empty(),
        "the rows went to a host nobody named: {:?}",
        heads(&elsewhere_seen)
    );
    assert_eq!(heads(&proxy_seen).len(), 1);
}

#[test]
fn a_bodiless_post_may_still_cross_an_origin() {
    // The line the data rule draws is around *data*, not around bodies. A
    // command with no payload — most of API v4 — sends `Content-Length: 0` and
    // gives nothing away by going, so it keeps the behaviour a tokenless client
    // has always had. Drawing the line at "has a body" instead would refuse
    // every POST again, which is the bug this branch spent a commit fixing.
    let (elsewhere, elsewhere_seen) = stub(empty_answer());
    let (proxy, _) = stub(redirect_with(
        307,
        "Temporary Redirect",
        &format!("{elsewhere}/api/v4/create"),
        "",
    ));

    let client = Client::new(&proxy).with_retries(RetryPolicy::none());
    client
        .create("map_node", "//tmp/thing")
        .expect("an empty body has nothing to give away");

    let followed = heads(&elsewhere_seen);
    assert_eq!(followed.len(), 1, "the redirect was not followed");
    assert!(
        followed[0].starts_with("POST /api/v4/create"),
        "{}",
        followed[0]
    );
    assert!(
        !followed[0].to_lowercase().contains("authorization:"),
        "there was no token to send: {}",
        followed[0]
    );
}

#[test]
fn a_streamed_write_is_refused_because_it_cannot_be_sent_twice() {
    // The half of the old rule that survives, and the only half that was ever
    // true. `write_table_rows` encodes as it sends: by the time the `3xx`
    // arrives some of the body has gone, and a reader cannot be rewound. There
    // is no request left to send to the second host, so nothing is sent.
    //
    // Rows enough that the body cannot sit in a socket buffer, which is what
    // the stub's draining is for: answering a chunked request that is still
    // being written closes the connection under `ureq`, and it reports the
    // broken pipe rather than the refusal. A small body wins that race on one
    // platform and loses it on another — this one loses it everywhere unless
    // `read_request` really does read to the end of the body before replying.
    let (elsewhere, elsewhere_seen) = stub(empty_answer());
    let (proxy, _) = stub(redirect_with(
        307,
        "Temporary Redirect",
        &format!("{elsewhere}/api/v4/write_table"),
        "",
    ));

    let client = Client::new(&proxy).with_retries(RetryPolicy::none());
    let error = client
        .write_table_rows(
            "//tmp/t",
            (0..200_000).map(|i| BTreeMap::from([("a", i as i64)])),
        )
        .expect_err("a body that cannot be replayed is refused");

    assert!(
        matches!(
            &error,
            ClientError::Redirected {
                refusal: RedirectRefusal::Body,
                status: 307,
                ..
            }
        ),
        "{error:?}"
    );
    // And the reason it gives is the reason it has: nothing about a `GET`, and
    // nothing about a body that does not exist.
    let message = error.to_string();
    assert!(message.contains("read as it is sent"), "{message}");
    assert!(
        heads(&elsewhere_seen).is_empty(),
        "the rows were dropped on the way: {:?}",
        heads(&elsewhere_seen)
    );
}

#[test]
fn a_bodiless_post_follows_a_canonicalising_balancer() {
    // The promise the same-origin rule makes — "a balancer canonicalising its
    // own host does not break every command" — was kept for `GET` only: the
    // refusal tested the *method*, so every POST in the crate met a `301` with
    // `RedirectRefusal::Body`, about a body that was not there. The head says
    // `content-length: 0`.
    //
    // `307` because it is the code that settles the argument outright: it
    // preserves the method and the body by definition, so there was never a
    // body to be rewritten away.
    let (proxy, seen) = stub_answering(vec![
        redirect_with(307, "Temporary Redirect", "/api/v4/create", ""),
        empty_answer(),
    ]);

    let client = Client::with_token(&proxy, "secret-token").with_retries(RetryPolicy::none());
    client
        .create("map_node", "//tmp/thing")
        .expect("a bodiless POST has nothing to lose to a redirect");

    let asked = heads(&seen);
    assert_eq!(asked.len(), 2, "the redirect was not followed: {asked:?}");
    for head in &asked {
        assert!(
            head.starts_with("POST /api/v4/create"),
            "the command's verb did not survive the hop: {head}"
        );
        // The head that says there is no body to lose, on both hops. It is
        // also what the refusal used to be reported about.
        assert!(head.to_lowercase().contains("content-length: 0"), "{head}");
        // Same origin, so the token goes: there is no host here it was not
        // already addressed to.
        assert!(
            head.to_lowercase().contains("authorization: oauth"),
            "{head}"
        );
    }
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

/// A proxy that redirects to itself, slowly.
///
/// Every hop costs `HOP`, and the client is given a budget it cannot spend
/// twice. The point of both timeout tests below: the deadline belongs to the
/// command, not to the request, so a chain of hops shares one.
const HOP: Duration = Duration::from_millis(300);
/// Less than two hops, so a second one cannot finish inside it.
const BUDGET: Duration = Duration::from_millis(400);
/// Comfortably more than the budget and comfortably less than what eleven hops
/// would cost — 3.3 s of them, plus whatever the machine is busy with.
const PATIENCE: Duration = Duration::from_millis(1_500);

#[test]
fn a_redirect_chain_spends_the_commands_timeout_and_not_one_each() {
    // `with_timeout` promises a limit that is end to end for a buffered
    // command. Following redirects inside the transport is where that can be
    // quietly lost: give each hop the full timeout and the real limit becomes
    // eleven times the one the caller asked for — 22 minutes at the default
    // two, on an `exists`.
    let (proxy, seen) = stub_answering_after(
        HOP,
        vec![redirect_with(
            301,
            "Moved Permanently",
            "/api/v4/exists?path=//tmp",
            "",
        )],
    );

    let client = Client::with_token(&proxy, "secret-token")
        .with_retries(RetryPolicy::none())
        .with_timeout(BUDGET);

    let started = Instant::now();
    let error = client.exists("//tmp").expect_err("the budget runs out");
    let took = started.elapsed();

    assert!(
        took < PATIENCE,
        "the command outlived its own timeout: {took:?} against a {BUDGET:?} budget, \
         {} requests",
        heads(&seen).len()
    );
    // And it says so as a timeout, which is what it is — not as a redirect
    // refusal, which is what it would become if the chain were allowed to run
    // to its bound.
    let message = error.to_string();
    assert!(
        message.contains("timeout"),
        "the deadline was reported as something else: {message}"
    );
}

#[test]
fn the_hosts_lookup_shares_one_budget_across_its_hops_too() {
    // `/hosts` builds its own request and follows its own redirects, which is
    // how it has come to miss things the command path had. It is also the
    // lookup a caller reaches for *because* of a redirect, so a caller already
    // waiting on one proxy should not wait eleven times over on the next.
    let (proxy, _) = stub_answering_after(
        HOP,
        vec![redirect_with(301, "Moved Permanently", "/hosts", "")],
    );

    let client = Client::with_token(&proxy, "secret-token")
        .with_retries(RetryPolicy::none())
        .with_timeout(BUDGET);

    let started = Instant::now();
    let error = client.heavy_proxy().expect_err("the budget runs out");
    let took = started.elapsed();

    assert!(took < PATIENCE, "{took:?} against a {BUDGET:?} budget");
    assert!(error.to_string().contains("timeout"), "{error}");
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
