//! What becomes of a request that is redirected.
//!
//! A control proxy answers a heavy read with a **cross-host** `307` naming a
//! data proxy. Following it would drop the `Authorization` header, and the
//! cluster would report `Client is missing credentials` about a valid token.
//!
//! The client follows redirects itself. A redirect on the **same origin** is
//! followed with token, method and body, so a `POST create` met by a
//! balancer's `301` arrives as a `POST create`. Crossing an origin withholds
//! the **token** and the request's **data**, each on its own: a tokenless
//! `write_table` does not send its rows to whichever host a `Location` names,
//! and a zero-length body is not data, so a bodiless command still goes. A
//! streamed body, which cannot be sent twice, is refused wherever it points.
//! The chain is bounded by ten hops and by the command's own timeout, which
//! the hops share.
//!
//! The local cluster runs one proxy and redirects nothing, so the stubs here
//! are in-process: one that redirects, and one standing in for the host it
//! points at, which answers `Client is missing credentials` and, in most
//! tests, **must never be reached**.

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
/// Detached rather than joined: many tests prove nobody connected at all. The
/// thread ends with the test process.
///
/// **It keeps a connection open, because the client does.** No reply says
/// `Connection: close`, so `ureq` pools the socket and reuses it for the next
/// request to the same origin, a same-origin redirect's second hop included.
/// A stub that went back to `accept` after one answer would close that socket
/// under the client, and the hop would fail on `Connection reset by peer`
/// when it lost the race.
///
/// **A thread per connection**: a client may open a second connection while
/// holding the first, and serving them in turn would leave the second waiting
/// for [`STUB_PATIENCE`] and then for ever.
fn stub_answering_after(
    delay: Duration,
    replies: Vec<String>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let address = listener.local_addr().expect("has an address");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let replies = Arc::new(replies);
    // Shared, so the nth request gets the nth reply across connections.
    let answered = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    std::thread::spawn(move || {
        while let Ok((mut stream, _)) = listener.accept() {
            let replies = Arc::clone(&replies);
            let recorded = Arc::clone(&recorded);
            let answered = Arc::clone(&answered);

            std::thread::spawn(move || {
                // A client gone mid-request must not hang the suite.
                stream.set_read_timeout(Some(STUB_PATIENCE)).ok();
                let mut reader = BufReader::new(stream.try_clone().expect("clones"));

                loop {
                    let request = read_request(&mut reader);
                    // The client has closed its end.
                    if request.trim().is_empty() {
                        break;
                    }

                    let nth = answered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let reply = &replies[nth.min(replies.len() - 1)];
                    // Recorded before the reply, so assertions made after the
                    // call returns do not race it.
                    recorded.lock().expect("not poisoned").push(request);
                    std::thread::sleep(delay);
                    if stream.write_all(reply.as_bytes()).is_err() {
                        break;
                    }
                    stream.flush().ok();
                }
            });
        }
    });

    (format!("http://{address}"), seen)
}

/// How long the stub waits on a request that has stopped arriving.
const STUB_PATIENCE: Duration = Duration::from_secs(10);

/// Reads one request **whole** — head and body — and only then returns.
///
/// Answering a body still being written closes the connection under the
/// client, which then sees `EPIPE` instead of the reply. A small
/// `Content-Length` body fits the socket buffer and survives; a **chunked**
/// one, as `write_table_rows` sends, is written as it is produced, and a Linux
/// runner loses the race that macOS wins. So both framings are read to the
/// end, which also records whether the rows arrived.
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
/// Bails on anything it cannot parse rather than hang the test.
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
            // The trailer section, empty in practice, ends at a blank line.
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

/// The data proxy's answer to a request that arrived without its token.
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

/// What a proxy answers a write with, or an emptied `GET` in its place.
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
    // `read_table` against a control proxy that answers 307 pointing at a
    // data proxy on another host.
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

    // The token went to the proxy the caller named, and nowhere else: a
    // bearer token does not follow a `Location` to a host nobody chose.
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
    // `cluster error 111: Client is missing credentials` would send a user to
    // check a token that is not wrong; the message names the redirect.
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
    // Not a bare `111`: the target's ephemeral port may contain it.
    assert!(
        !message.contains("missing credentials") && !message.contains("cluster error 111"),
        "the failure is still being blamed on the token: {message}"
    );
    // What the client is certain of, not a verdict on the token.
    assert!(
        message.contains("was not sent to the host that answered"),
        "{message}"
    );
    // And what to do instead.
    assert!(message.contains("heavy proxy"), "{message}");
}

#[test]
fn a_redirected_light_command_is_not_sent_to_a_heavy_proxy() {
    // `create` is not a heavy command, so the message does not send its
    // caller to a heavy proxy.
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
    // A redirect that also carries `X-YT-Error` (code 111 here) is still
    // reported as a refused redirect, not as the cluster's error.
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
    // A balancer sends 301 and 302 as well as the 307 of heavy routing, and
    // each is refused alike.
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
    // A balancer canonicalising its own paths sends a relative `Location`
    // back to itself. The token is already going there, so it is followed.
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
    // A balancer pointing at itself is refused after `MAX_REDIRECTS` (ten) hops
    // rather than followed until the command's deadline (two minutes by
    // default).
    //
    // Without the bound **nothing hangs**: measured with `MAX_REDIRECTS`
    // raised, the stub gives out after about eight seconds and the run fails
    // with `Peer disconnected`. Read that failure as a missing bound, not a
    // flaky stub.
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
    // A redirect that rewrote the PUT into a GET and dropped the body would
    // make `write_table` return `Ok(())` with no rows written. On the **same
    // origin** the client sends the same method and rows again; crossing to
    // another origin is refused (next test).
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
    // With no token and re-sendable rows, only the data rule stops a
    // `Location` header from sending a table's contents to whichever host it
    // names.
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
    // Asked of the second stub: the bytes did not arrive.
    assert!(
        heads(&elsewhere_seen).is_empty(),
        "the rows went to a host nobody named: {:?}",
        heads(&elsewhere_seen)
    );
    assert_eq!(heads(&proxy_seen).len(), 1);
}

#[test]
fn a_bodiless_post_may_still_cross_an_origin() {
    // The data rule is about data, not bodies. A command with no payload,
    // most of API v4, sends `Content-Length: 0` and gives nothing away, so a
    // tokenless client follows the redirect.
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
    // `write_table_rows` encodes as it sends: by the time the `3xx` arrives
    // some of the body has gone and cannot be rewound, so nothing is sent on.
    //
    // Enough rows that the body cannot sit in a socket buffer: this passes
    // only if `read_request` reads the whole chunked body before replying,
    // otherwise `ureq` reports a broken pipe instead of the refusal.
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
    // The reason given is the stream, not a method or a missing body.
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
    // A same-origin redirect of a bodiless POST is followed, not refused as
    // `RedirectRefusal::Body`: the head says `content-length: 0`. A `307`
    // preserves the method and body by definition.
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
        // No body to lose, on both hops.
        assert!(head.to_lowercase().contains("content-length: 0"), "{head}");
        // Same origin, so the token goes.
        assert!(
            head.to_lowercase().contains("authorization: oauth"),
            "{head}"
        );
    }
}

#[test]
fn a_redirected_streaming_read_goes_nowhere() {
    // `read_table_streaming` reconfigures the request (the streaming timeout
    // override), so it inherits the redirect policy rather than stating it;
    // a `max_redirects` in that override would leak the token here only.
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
    // The other request built through the streaming override.
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
    // `/hosts` builds its own request and carries the token, so it refuses a
    // cross-origin redirect too.
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
/// Every hop costs `HOP`, and the deadline belongs to the command, so a chain
/// of hops shares one budget.
const HOP: Duration = Duration::from_millis(300);
/// Less than two hops, so a second one cannot finish inside it.
const BUDGET: Duration = Duration::from_millis(400);
/// Well above the budget and well below eleven hops' 3.3 s.
const PATIENCE: Duration = Duration::from_millis(1_500);

#[test]
fn a_redirect_chain_spends_the_commands_timeout_and_not_one_each() {
    // `with_timeout` is end to end for a buffered command; a fresh timeout per
    // hop would allow eleven times it (docs/protocol-reference.md).
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
    // Reported as a timeout, not as a redirect refusal.
    let message = error.to_string();
    assert!(
        message.contains("timeout"),
        "the deadline was reported as something else: {message}"
    );
}

#[test]
fn the_hosts_lookup_shares_one_budget_across_its_hops_too() {
    // `/hosts` builds its own request and follows its own redirects, under
    // the same one budget.
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
    // The refusal is about credentials, not redirects: a tokenless request
    // with no data follows a cross-origin redirect.
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
