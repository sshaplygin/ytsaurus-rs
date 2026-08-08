//! What the four branches do **together**, on the round-3 heads.
//!
//! Each of #36, #37, #38 and #39 is tested on its own branch. These are the
//! questions that only exist once two of them are in the same tree — where one
//! PR's new error meets another PR's new decision about what an error means.
//! The headline is the #36 × #38 interaction that round 2 found permanently
//! disabled heavy routing; round 3 is where the fixes are supposed to have
//! closed it.
//!
//! The stubs are `request_shape.rs`'s, generalised: a listener that stays up,
//! remembers every request, and answers `/hosts` however the test needs it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ytsaurus_client::{Client, ClientError, RetryPolicy};

// --------------------------------------------------------------- the headline

#[test]
fn a_cross_origin_hosts_redirect_no_longer_permanently_disables_routing() {
    // #36 × #38, round 3. A balancer answers `/hosts` with a cross-origin 307,
    // which #36 refuses (the token would be dropped) as `ClientError::Redirected`.
    //
    // Round 2: that error reached `base_for`, was classified non-retriable, and
    // cached `Configured` **permanently** — `/hosts` was asked exactly once for
    // the client's whole life. Round 3: #38's `worth_asking_again` now counts
    // `Redirected` as worth asking again, so it caches `FellBack` (temporary).
    // With the fallback window set to zero, every heavy command asks again.
    let control = Proxy::new(Hosts::RedirectCrossOrigin);

    let client = Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_hosts_retry_after(Duration::ZERO);

    for _ in 0..3 {
        // The upload still succeeds — it falls back to the configured address.
        client
            .write_table("//tmp/t", b"")
            .expect("the upload still succeeds");
    }

    let hosts_lookups = control
        .requests()
        .iter()
        .filter(|line| line.starts_with("GET /hosts"))
        .count();

    // The heart of the fix: the cluster is asked again rather than once ever.
    // Round 2 asserted exactly one lookup across all three commands; here there
    // is one per command, so routing is *retried*, not disabled for life.
    assert_eq!(
        hosts_lookups,
        3,
        "the redirect was cached as a permanent verdict again: {:?}",
        control.requests()
    );
}

#[test]
fn the_same_permanence_is_gone_with_a_ten_second_default_too() {
    // The same point without leaning on a zero window: with the default
    // HOSTS_RETRY_AFTER the client asks once, falls back, and does *not* ask
    // again within the window — but the state is `FellBack`, not `Configured`,
    // which is what round 2 got wrong. Proven by the contrast test above (zero
    // window → re-asks) and this one (default window → one ask, still serving).
    let control = Proxy::new(Hosts::RedirectCrossOrigin);

    let client = Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true);

    for _ in 0..3 {
        client
            .write_table("//tmp/t", b"")
            .expect("still succeeds via fallback");
    }

    // One lookup within the window, then the configured address serves the
    // rest — the command never fails, which is what makes the fallback correct.
    let hosts_lookups = control
        .requests()
        .iter()
        .filter(|line| line.starts_with("GET /hosts"))
        .count();
    assert_eq!(hosts_lookups, 1, "{:?}", control.requests());
}

#[test]
fn a_same_origin_hosts_redirect_is_now_followed_and_routing_works() {
    // The other half of #36's new model: a same-origin redirect — a balancer
    // canonicalising its own `/hosts` URL — is *followed*, token and all. So
    // the lookup succeeds, the client learns the heavy proxy, and the upload
    // reaches it. Round 2 refused every redirect and this case broke too.
    let heavy = Proxy::new(Hosts::naming_itself());
    let control = Proxy::new(Hosts::RedirectSameOriginTo(format!(
        r#"["{}"]"#,
        heavy.host()
    )));

    Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .write_table("//tmp/t", b"")
        .expect("writes");

    // The control proxy served the lookup and its same-origin redirect; the
    // heavy proxy served the upload.
    assert!(
        control
            .requests()
            .iter()
            .any(|l| l.starts_with("GET /hosts")),
        "the lookup did not happen: {:?}",
        control.requests()
    );
    assert!(
        heavy
            .requests()
            .iter()
            .any(|l| l.contains("/api/v4/write_table")),
        "the upload did not reach the heavy proxy: heavy={:?} control={:?}",
        heavy.requests(),
        control.requests()
    );
}

#[test]
fn the_redirected_lookup_is_still_reported_as_a_redirect_when_asked_directly() {
    // #36 still works: `heavy_proxy()` surfaces the cross-origin refusal as
    // `ClientError::Redirected`, rather than swallowing it. The difference from
    // round 2 is only what the *router* does with the same error.
    let control = Proxy::new(Hosts::RedirectCrossOrigin);

    let error = Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .heavy_proxy()
        .expect_err("the balancer redirected cross-origin");

    assert!(
        matches!(error, ClientError::Redirected { .. }),
        "the lookup failed as something other than a redirect: {error:?}"
    );
}

// ----------------------------------------------- #38 × #39, the second edge

#[test]
fn a_discovered_proxy_that_fails_non_retriably_is_not_re_resolved() {
    // #38 × #39, and the seam #40 then split. Dropping a host from the pool
    // keys on `attributable_to_the_host` — `worth_asking_again` PLUS a
    // rejected certificate — precisely so the per-host verdict a certificate
    // is (`NotValidForName` names one host) drops that host where it used to
    // pin the client. What this test stands up is the class that stays on the
    // other side of the line: a failure about the *request* — cluster code
    // 500, a resolve error — is not the host's fault, the table is missing
    // everywhere, and the discovered proxy is rightly kept and not
    // re-resolved. The certificate side of the line is pinned in
    // `http::tests::a_rejected_certificate_drops_the_host_and_a_wrong_command_does_not`,
    // where the error can be built without the TLS handshake a stub cannot
    // stage.
    let heavy = Proxy::new(Hosts::naming_itself().failing_commands_with(500));
    let control = Proxy::new(Hosts::listing(&heavy.host()));

    let client = Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_hosts_retry_after(Duration::ZERO);

    for _ in 0..3 {
        let _ = client.write_table("//tmp/t", b"");
    }

    // Asked once; the non-retriable failure did not send it back to `/hosts`.
    let hosts_lookups = control
        .requests()
        .iter()
        .filter(|l| l.starts_with("GET /hosts"))
        .count();
    assert_eq!(
        hosts_lookups,
        1,
        "a non-retriable failure re-resolved: {:?}",
        control.requests()
    );
    // And every upload went to the one discovered proxy, never elsewhere.
    assert_eq!(
        heavy
            .requests()
            .iter()
            .filter(|l| l.contains("/api/v4/write_table"))
            .count(),
        3,
        "{:?}",
        heavy.requests()
    );
}

#[test]
fn a_discovered_proxy_that_fails_retriably_is_re_resolved() {
    // The contrast that shows the split is about classification: a banned
    // proxy (cluster code 2100) is retriable, so `worth_asking_again` is true
    // and the client re-asks.
    let heavy = Proxy::new(Hosts::naming_itself().failing_commands_with(2100));
    let control = Proxy::new(Hosts::listing(&heavy.host()));

    let client = Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_hosts_retry_after(Duration::ZERO);

    for _ in 0..3 {
        let _ = client.write_table("//tmp/t", b"");
    }

    let hosts_lookups = control
        .requests()
        .iter()
        .filter(|l| l.starts_with("GET /hosts"))
        .count();
    assert!(
        hosts_lookups >= 2,
        "a retriable failure was treated as settled: {:?}",
        control.requests()
    );
}

// ----------------------------------------- WP5 option B: the credential path

#[test]
fn the_token_reaches_a_validated_discovered_heavy_proxy() {
    // Option B, half (a): the token *does* travel to a `/hosts`-named host —
    // intended, matching the C++/Go/Python clients. The safety is that the host
    // was validated first (half b), not that the token is withheld.
    let heavy = Proxy::new(Hosts::naming_itself());
    let control = Proxy::new(Hosts::listing(&heavy.host()));

    Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .write_table("//tmp/t", b"")
        .expect("writes");

    let head = heavy
        .heads()
        .into_iter()
        .find(|h| h.starts_with("PUT /api/v4/write_table"))
        .expect("the upload reached the heavy proxy");
    assert!(
        head.to_lowercase()
            .contains("authorization: oauth secret-token"),
        "the upload reached the discovered host without its token:\n{head}"
    );
}

#[test]
fn an_at_smuggled_host_is_refused_so_the_token_is_not_sent_to_it() {
    // Option B, half (b): the discovered host is validated. A `/hosts` answer
    // whose entry is `real@evil` is a URL whose *host* is `evil`; #38 refuses
    // it (the `@`), falls back to the configured address, and the token is
    // never dialled at `evil`. The upload still succeeds against the configured
    // address, so the refusal is safe rather than fatal.
    let evil = Proxy::new(Hosts::naming_itself());
    // The configured cluster; the /hosts answer smuggles `evil` behind userinfo.
    let control = Proxy::new(Hosts::listing(&format!(
        "{}@{}",
        "127.0.0.1:1",
        evil.host()
    )));

    Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_hosts_retry_after(Duration::ZERO)
        .write_table("//tmp/t", b"")
        .expect("falls back to the configured address");

    // Nothing was ever dialled at the smuggled host.
    assert!(
        evil.requests().is_empty(),
        "the token-bearing upload reached the @-smuggled host: {:?}",
        evil.requests()
    );
    // The configured address served the upload itself.
    assert!(
        control
            .requests()
            .iter()
            .any(|l| l.contains("/api/v4/write_table")),
        "the upload did not fall back to the configured address: {:?}",
        control.requests()
    );
}

#[test]
fn a_plain_http_discovered_host_is_refused_from_an_https_client() {
    // The downgrade guard, still behavioural: an `https://` client will not be
    // steered onto an `http://` heavy proxy by a `/hosts` body, which would put
    // the token on the wire in cleartext. `heavy_base` refuses a name that
    // carries its own scheme, so this falls back to the configured address.
    // (Configured over https here means the command then fails at the TLS
    // guard on a stub socket — what matters is only that it did not dial the
    // http host.)
    let evil = Proxy::new(Hosts::naming_itself());
    let control = Proxy::new(Hosts::listing(&format!("http://{}", evil.host())));

    let client = Client::with_token(&control.url(), "secret-token")
        .with_retries(RetryPolicy::none())
        .with_proxy_discovery(true)
        .with_hosts_retry_after(Duration::ZERO);
    let _ = client.write_table("//tmp/t", b"");

    assert!(
        evil.requests().is_empty(),
        "an https client was downgraded onto an http heavy proxy: {:?}",
        evil.requests()
    );
}

// ------------------------------------------------------------------ the stub

/// How a stub proxy answers, given its own address.
enum Hosts {
    /// `/hosts` names `host`; commands succeed.
    Listing(String),
    /// `/hosts` names this listener itself; commands succeed.
    NamingItself,
    /// `/hosts` names this listener; every command fails with this cluster code.
    NamingItselfFailing(i64),
    /// `/hosts` answers 307 to a *different* origin — refused by #36 with a
    /// token.
    RedirectCrossOrigin,
    /// `/hosts` answers 307 to the same origin, then serves this list — followed
    /// by #36.
    RedirectSameOriginTo(String),
}

impl Hosts {
    fn naming_itself() -> Self {
        Self::NamingItself
    }

    fn listing(host: &str) -> Self {
        Self::Listing(host.to_owned())
    }

    fn failing_commands_with(self, code: i64) -> Self {
        match self {
            Self::NamingItself => Self::NamingItselfFailing(code),
            other => other,
        }
    }

    fn answer(&self, head: &str, me: &str) -> Vec<u8> {
        let hosts = head.starts_with("GET /hosts");
        match self {
            Self::Listing(host) if hosts => ok_body(format!(r#"["{host}"]"#).as_bytes()),
            Self::NamingItself if hosts => ok_body(format!(r#"["{me}"]"#).as_bytes()),
            Self::NamingItselfFailing(_) if hosts => ok_body(format!(r#"["{me}"]"#).as_bytes()),
            Self::NamingItselfFailing(code) => cluster_error(*code),
            Self::RedirectCrossOrigin if hosts => {
                // A different origin: port 1 on loopback, which no client of
                // ours is bound to, so it is unambiguously cross-origin.
                redirect(307, "http://127.0.0.1:1/hosts")
            }
            Self::RedirectSameOriginTo(list) if head.starts_with("GET /hosts ") => {
                // Same-origin canonicalisation: an absolute-path Location on the
                // host that answered.
                redirect(307, &format!("http://{me}/hosts/"))
            }
            Self::RedirectSameOriginTo(list) if head.starts_with("GET /hosts/") => {
                ok_body(list.as_bytes())
            }
            _ => ok_body(br#"{"value"=%true}"#),
        }
    }
}

/// A stand-in proxy that keeps serving and remembers what it was asked.
struct Proxy {
    address: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Proxy {
    fn new(serving: Hosts) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");
        let seen = Arc::new(Mutex::new(Vec::new()));

        let served = Arc::clone(&seen);
        let serving = Arc::new(serving);
        let me = address.to_string();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let serving = Arc::clone(&serving);
                let seen = Arc::clone(&served);
                let me = me.clone();
                std::thread::spawn(move || serve(stream, &serving, &me, &seen));
            }
        });

        Self { address, seen }
    }

    fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// The address as `/hosts` names one: a bare host and port, no scheme.
    fn host(&self) -> String {
        self.address.to_string()
    }

    fn requests(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("nothing panicked holding it")
            .iter()
            .map(|head| head.lines().next().unwrap_or_default().to_owned())
            .collect()
    }

    fn heads(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("nothing panicked holding it")
            .clone()
    }
}

fn serve(
    mut stream: std::net::TcpStream,
    serving: &Hosts,
    me: &str,
    seen: &Arc<Mutex<Vec<String>>>,
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

        if let Some(length) = content_length(&head) {
            let mut body = vec![0_u8; length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
        } else if head.to_lowercase().contains("transfer-encoding: chunked") {
            drain_chunked(&mut reader);
        }

        let answer = serving.answer(&head, me);
        seen.lock().expect("nothing panicked holding it").push(head);

        if stream.write_all(&answer).is_err() {
            return;
        }
        stream.flush().ok();
    }
}

fn ok_body(body: &[u8]) -> Vec<u8> {
    let mut reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-yt-yson-text\r\n\r\n",
        body.len()
    )
    .into_bytes();
    reply.extend_from_slice(body);
    reply
}

fn redirect(status: u16, location: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n"
    )
    .into_bytes()
}

/// A cluster error in the `X-YT-Error` header, HTTP 200 the way the proxy sends
/// most of them.
fn cluster_error(code: i64) -> Vec<u8> {
    let document = format!(r#"{{"code":{code},"message":"stub failure {code}"}}"#);
    format!("HTTP/1.1 200 OK\r\nX-YT-Error: {document}\r\nContent-Length: 0\r\n\r\n").into_bytes()
}

fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .find(|line| line.to_lowercase().starts_with("content-length:"))
        .and_then(|line| line.split_once(':'))
        .and_then(|(_, value)| value.trim().parse().ok())
}

fn drain_chunked(reader: &mut BufReader<std::net::TcpStream>) {
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() {
            return;
        }
        let size = usize::from_str_radix(header.trim(), 16).unwrap_or(0);
        let mut chunk = vec![0_u8; size + 2];
        if reader.read_exact(&mut chunk).is_err() || size == 0 {
            return;
        }
    }
}
