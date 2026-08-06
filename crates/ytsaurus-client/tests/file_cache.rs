//! What a cached upload does against a cache the caller may not write to.
//!
//! No cluster here answers `Access denied`: a local one in Docker is a cluster
//! where the caller is `root` and every path is writable, which is exactly why
//! the failure this pins was found on a real installation and not before. So
//! the cluster is a socket in-process that answers each command as told —
//! `request_shape.rs` does the same for one request, and this does it for the
//! seven a cached upload sends, because what is under test is the *sequence*:
//! which command was refused, and what the client did next.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ytsaurus_client::{Client, ClientError, RetryPolicy};

/// The default cache, which is the path an installation manages.
const CACHE: &str = "//tmp/yt_wrapper/file_storage/new_cache";

/// Where a cached file ends up: named after its hash, under a fan-out
/// directory, and not where the upload put it.
const IN_THE_CACHE: &str = "//tmp/yt_wrapper/file_storage/new_cache/ab/abcdef";

/// A cache miss, which the cluster spells as an empty string rather than as an
/// error or an entity.
const MISS: &str = r#""""#;

// ------------------------------------------------------------------ the tests

#[test]
fn a_cache_the_installation_keeps_to_itself_still_launches_the_worker() {
    // The reported failure: `//tmp/yt_wrapper/file_storage` is operator-managed
    // and an ordinary user may read it and nothing else, so the `create` on the
    // miss branch is refused and the launch dies at its first upload. It is an
    // optimisation that failed, and the worker still has somewhere to go.
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(MISS.to_owned()),
        // Everything else aimed at the cache, whichever of the writes into it
        // this installation checks first.
        _ if sent.mentions(CACHE) => Answer::Denied,
        _ => Answer::Body("{}".to_owned()),
    });

    let worker = worker_file("managed");
    let uploaded = cluster
        .client()
        .upload_worker_cached(&worker)
        .expect("a cache that refuses this caller is not a failed upload");

    assert!(
        uploaded.uploaded,
        "the fallback is an upload, and says so: {uploaded:?}"
    );
    assert!(
        !uploaded.path.starts_with(CACHE) && uploaded.path.starts_with("//tmp/"),
        "the worker went somewhere the caller cannot write: {}",
        uploaded.path
    );
    // The sandbox name still comes from the file, not from the path it landed
    // at — the whole point of `CachedFile::name`.
    assert_eq!(uploaded.name, "managed");

    let sent = cluster.sent();
    assert_eq!(
        commands(&sent),
        [
            "get_file_from_cache",
            "create",
            "create",
            "write_file",
            "set"
        ],
        "the lookup, the refused cache directory, then a plain upload: {sent:?}"
    );
    // Nothing was staged in the cache and nothing was handed to it: the client
    // stopped asking at the first refusal rather than working through the rest
    // of the sequence collecting the same answer.
    assert!(
        !sent.iter().any(|s| s.command == "put_file_to_cache"),
        "the cache was still asked to take the file: {sent:?}"
    );
    for wrote in sent.iter().filter(|s| s.command == "write_file") {
        assert!(
            wrote.mentions(&uploaded.path) && !wrote.mentions(CACHE),
            "the bytes went into the cache after all: {wrote:?}"
        );
    }
}

#[test]
fn a_cache_that_takes_the_bytes_and_refuses_the_handover_falls_back_too() {
    // The other half, and the more expensive one: `put_file_to_cache` is the
    // last call of the sequence, so by the time it is refused the whole binary
    // is already on the cluster. Sending it again is worth it — the alternative
    // is a launch that fails having done all the work.
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(MISS.to_owned()),
        "put_file_to_cache" => Answer::Denied,
        _ => Answer::Body("{}".to_owned()),
    });

    let worker = worker_file("handover");
    let uploaded = cluster
        .client()
        .upload_worker_cached(&worker)
        .expect("a handover the cache refuses is not a failed upload");

    assert!(
        !uploaded.path.starts_with(CACHE),
        "the path is the cache's, and the cache refused it: {}",
        uploaded.path
    );

    let sent = cluster.sent();
    assert_eq!(
        commands(&sent),
        [
            "get_file_from_cache",
            "create",
            "create",
            "write_file",
            "set",
            "put_file_to_cache",
            // The staging node goes whichever way the handover went. It is
            // inside the cache, and cache expiry collects what the cache itself
            // created, not what was left beside it.
            "remove",
            "create",
            "write_file",
            "set",
        ],
        "{sent:?}"
    );

    let writes: Vec<&Sent> = sent.iter().filter(|s| s.command == "write_file").collect();
    assert_eq!(writes.len(), 2, "the bytes are sent twice: {sent:?}");
    assert!(writes[0].mentions(CACHE), "{:?}", writes[0]);
    assert!(
        writes[1].mentions(&uploaded.path) && !writes[1].mentions(CACHE),
        "the second upload went back into the cache: {:?}",
        writes[1]
    );
}

#[test]
fn a_cluster_that_allows_the_cache_still_uses_it() {
    // The path nothing above exercises, and the one every cluster in CI takes.
    // A fallback that fired whatever the cluster answered would pass both tests
    // above and quietly stop caching for everybody.
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(MISS.to_owned()),
        "put_file_to_cache" => Answer::Body(format!("{IN_THE_CACHE:?}")),
        _ => Answer::Body("{}".to_owned()),
    });

    let worker = worker_file("ordinary");
    let uploaded = cluster
        .client()
        .upload_worker_cached(&worker)
        .expect("uploads");

    assert_eq!(uploaded.path, IN_THE_CACHE);
    assert!(uploaded.uploaded);

    let sent = cluster.sent();
    assert_eq!(
        commands(&sent),
        [
            "get_file_from_cache",
            "create",
            "create",
            "write_file",
            "set",
            "put_file_to_cache",
            "remove",
            // The executable bit on the cached path, which is a different node
            // from the one that was written: whether the attribute survives the
            // cache's move decides whether the job can exec at all.
            "set",
        ],
        "{sent:?}"
    );
    assert!(
        sent.last()
            .expect("something was sent")
            .mentions(IN_THE_CACHE),
        "the last thing done is to the cached node: {sent:?}"
    );
    for wrote in sent.iter().filter(|s| s.command == "write_file") {
        assert!(wrote.mentions(CACHE), "an upload left the cache: {wrote:?}");
    }
}

#[test]
fn a_cache_hit_uploads_nothing_and_falls_back_to_nothing() {
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(format!("{IN_THE_CACHE:?}")),
        _ => Answer::Body("{}".to_owned()),
    });

    let worker = worker_file("hit");
    let uploaded = cluster
        .client()
        .upload_worker_cached(&worker)
        .expect("finds it");

    assert_eq!(uploaded.path, IN_THE_CACHE);
    assert!(!uploaded.uploaded, "a hit uploaded something: {uploaded:?}");
    assert_eq!(commands(&cluster.sent()), ["get_file_from_cache"]);
}

#[test]
fn a_create_that_failed_for_some_other_reason_is_not_a_cache_to_give_up_on() {
    // A resolve error is not a permission error, and no amount of uploading
    // elsewhere addresses it. Catching by command alone — "the create failed,
    // so there is no cache" — would turn every such failure into a silent
    // second upload and a launch that reports success.
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(MISS.to_owned()),
        "create" => Answer::Failed(500, "Error resolving path //tmp/yt_wrapper"),
        _ => Answer::Body("{}".to_owned()),
    });

    let worker = worker_file("resolve");
    let error = cluster
        .client()
        .upload_worker_cached(&worker)
        .expect_err("a resolve error is the caller's to hear about");

    assert!(
        matches!(&error, ClientError::Cluster { code: 500, command, .. } if command == "create"),
        "{error:?}"
    );
    assert!(
        !cluster.sent().iter().any(|s| s.command == "write_file"),
        "something was uploaded anyway: {:?}",
        cluster.sent()
    );
}

#[test]
fn an_access_denied_that_is_not_the_caches_is_not_swallowed() {
    // 901 on the bytes themselves, at a node this caller has just created. That
    // is not the installation withholding its cache — it is this write being
    // refused, and the same bytes sent to another path would be refused the
    // same way. A fallback here uploads twice and then fails anyway, having
    // hidden the first answer.
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(MISS.to_owned()),
        "write_file" => Answer::Denied,
        _ => Answer::Body("{}".to_owned()),
    });

    let worker = worker_file("denied-write");
    let error = cluster
        .client()
        .upload_worker_cached(&worker)
        .expect_err("a denial that is not about the cache is still a denial");

    assert!(
        matches!(&error, ClientError::Cluster { code: 901, command, .. } if command == "write_file"),
        "{error:?}"
    );

    let sent = cluster.sent();
    assert_eq!(
        sent.iter().filter(|s| s.command == "write_file").count(),
        1,
        "the refused upload was tried again somewhere else: {sent:?}"
    );
    assert!(
        !sent
            .iter()
            .any(|s| s.command == "create" && !s.mentions(CACHE)),
        "a fallback upload was started outside the cache: {sent:?}"
    );
}

// -------------------------------------------------------------- the stub

/// One request, as the stub saw it.
#[derive(Clone, Debug)]
struct Sent {
    /// The last segment of `/api/v4/<command>`.
    command: String,
    /// The `X-YT-Parameters` header, verbatim.
    parameters: String,
}

impl Sent {
    /// Whether the parameters name `text` — a path, in every use here.
    ///
    /// A substring rather than an equality: `set` addresses `<path>/@executable`
    /// and `get_file_from_cache` carries a `cache_path` beside its `md5`.
    fn mentions(&self, text: &str) -> bool {
        self.parameters.contains(text)
    }
}

/// What the stub answers with.
enum Answer {
    /// 200, and this text-YSON body.
    Body(String),
    /// The refusal this whole file is about: cluster error 901, in the
    /// `X-YT-Error` header the client reads it from.
    Denied,
    /// Any other cluster error.
    Failed(i64, &'static str),
}

impl Answer {
    /// The status line and the body to send back.
    fn parts(&self) -> (&'static str, Option<String>, String) {
        match self {
            Answer::Body(body) => ("200 OK", None, body.clone()),
            Answer::Denied => (
                "403 Forbidden",
                Some(document(
                    901,
                    "Access denied for user \"tester\": \"write | modify_children\" \
                     permission for node //tmp/yt_wrapper/file_storage/new_cache \
                     is not allowed by any matching ACE",
                )),
                String::new(),
            ),
            Answer::Failed(code, message) => (
                "400 Bad Request",
                Some(document(*code, message)),
                String::new(),
            ),
        }
    }
}

/// An `X-YT-Error` document, which is JSON even though everything else is YSON.
fn document(code: i64, message: &str) -> String {
    let escaped = message.replace('\\', r"\\").replace('"', "\\\"");
    format!(r#"{{"code":{code},"message":"{escaped}"}}"#)
}

type Answering = Arc<dyn Fn(&Sent) -> Answer + Send + Sync>;

/// A cluster that answers as told and remembers what it was asked.
struct Cluster {
    proxy: String,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl Cluster {
    /// A client pointed at it, sending each command once — the stub answers
    /// every request itself, so a retry could only confuse the record.
    fn client(&self) -> Client {
        Client::new(&self.proxy).with_retries(RetryPolicy::none())
    }

    fn sent(&self) -> Vec<Sent> {
        self.sent.lock().expect("not poisoned").clone()
    }
}

fn commands(sent: &[Sent]) -> Vec<&str> {
    sent.iter().map(|s| s.command.as_str()).collect()
}

fn cluster(answer: impl Fn(&Sent) -> Answer + Send + Sync + 'static) -> Cluster {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let proxy = format!("http://{}", listener.local_addr().expect("has an address"));
    let sent = Arc::new(Mutex::new(Vec::new()));

    let answering: Answering = Arc::new(answer);
    let log = Arc::clone(&sent);
    // A thread per connection, because the client opens a second one whenever
    // the first is not returned to its pool — which is what a response whose
    // body goes unread does, and every error answered here is one of those.
    // Serving connections in turn would leave the second waiting on the first,
    // which never ends.
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            let answering = Arc::clone(&answering);
            let log = Arc::clone(&log);
            std::thread::spawn(move || serve(&stream, &answering, &log));
        }
    });

    Cluster { proxy, sent }
}

/// Answers every request on one connection, until the client goes away.
fn serve(stream: &TcpStream, answer: &Answering, log: &Mutex<Vec<Sent>>) {
    // So a connection the client keeps open but never uses again does not hold
    // a thread for the lifetime of the test binary.
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("sets a timeout");
    let mut writer = stream.try_clone().expect("clones");
    let mut reader = BufReader::new(stream.try_clone().expect("clones"));

    while let Some(request) = read_request(&mut reader) {
        log.lock().expect("not poisoned").push(request.clone());

        let (status, error, body) = answer(&request).parts();
        let mut reply = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", body.len());
        if let Some(error) = error {
            // The cluster's own error, which is where the client looks first —
            // and reads whatever the status says.
            reply.push_str(&format!("X-YT-Error: {error}\r\n"));
        }
        reply.push_str("Content-Type: application/x-yt-yson-text\r\n\r\n");
        reply.push_str(&body);

        if writer.write_all(reply.as_bytes()).is_err() {
            return;
        }
        writer.flush().ok();
    }
}

/// One request off the wire, or `None` when there are no more.
fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Sent> {
    let mut head = String::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return None,
            Ok(_) if line == "\r\n" => break,
            Ok(_) => head.push_str(&line),
        }
    }

    // The body has to be consumed before the reply, or a client still writing
    // one finds the socket closed and reports a broken pipe instead of the
    // answer this stub was asked for. Everything here declares its length:
    // `write_file` and `set` carry bytes, the rest carry nothing.
    if let Some(length) = header(&head, "content-length").and_then(|v| v.parse().ok()) {
        let mut body = vec![0_u8; length];
        reader.read_exact(&mut body).ok()?;
    }

    Some(Sent {
        // "POST /api/v4/create HTTP/1.1"
        command: head
            .lines()
            .next()?
            .split_whitespace()
            .nth(1)?
            .rsplit('/')
            .next()?
            .to_owned(),
        parameters: header(&head, "x-yt-parameters").unwrap_or_default(),
    })
}

/// One header of a request, matched case-insensitively as HTTP says it is.
fn header(head: &str, name: &str) -> Option<String> {
    head.lines()
        .find(|line| {
            line.to_lowercase()
                .starts_with(&format!("{}:", name.to_lowercase()))
        })
        .map(|line| line[line.find(':').unwrap_or(0) + 1..].trim().to_owned())
}

/// A "worker binary" on disk, named after the test that wants one.
///
/// Its contents are arbitrary: `upload_worker_cached` uploads what it is given
/// and hashes it, and only `upload_current_exe` checks for an ELF a node could
/// run.
fn worker_file(name: &str) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("ytsaurus-rs-cache-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("creates");
    let path = directory.join(name);
    std::fs::write(&path, format!("not really a worker: {name}")).expect("writes");
    path
}
