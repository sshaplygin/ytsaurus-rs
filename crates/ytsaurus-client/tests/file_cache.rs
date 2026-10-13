//! What a cached upload does against a cache the caller may not write to.
//!
//! A local Docker cluster runs as `root`, where every path is writable and
//! nothing answers `Access denied`, so the cluster here is an in-process socket
//! that answers each command as told. What is under test is the sequence of up
//! to seven commands: which one was refused, and what the client did next.

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
    // `//tmp/yt_wrapper/file_storage` may be operator-managed and read-only to
    // an ordinary user, so the `create` on a miss is refused. The cache is an
    // optimisation: the worker is uploaded outside it instead.
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(MISS.to_owned()),
        // Every write into the cache, whichever one is checked first.
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
    // `uploaded` is true on both paths; `cached` is what tells a caller not to
    // delete the path afterwards, since a cached one is the shared entry.
    assert!(
        !uploaded.cached,
        "the fallback claims the cache took it: {uploaded:?}"
    );
    assert!(
        !uploaded.path.starts_with(CACHE) && uploaded.path.starts_with("//tmp/"),
        "the worker went somewhere the caller cannot write: {}",
        uploaded.path
    );
    // `CachedFile::name` comes from the file, not from where it landed.
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
    // The client stopped at the first refusal: nothing staged in or handed to
    // the cache.
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
fn a_refusal_the_cluster_wrapped_is_still_the_cache_refusing() {
    // An outer error is often a category (`Error creating node`) with the
    // reason in `inner_errors`. The refusal is recognised one level down, as
    // the crate's other error classifiers do.
    let cluster = cluster(|sent| match sent.command.as_str() {
        "get_file_from_cache" => Answer::Body(MISS.to_owned()),
        _ if sent.mentions(CACHE) => Answer::DeniedInside,
        _ => Answer::Body("{}".to_owned()),
    });

    let worker = worker_file("wrapped");
    let uploaded = cluster
        .client()
        .upload_worker_cached(&worker)
        .expect("a refusal one level down is the same refusal");

    assert!(!uploaded.cached, "{uploaded:?}");
    assert_eq!(
        commands(&cluster.sent()),
        [
            "get_file_from_cache",
            "create",
            "create",
            "write_file",
            "set"
        ],
        "the wrapped refusal was reported rather than fallen back from: {:?}",
        cluster.sent()
    );
}

#[test]
fn a_cache_that_takes_the_bytes_and_refuses_the_handover_falls_back_too() {
    // `put_file_to_cache` is refused after the whole binary is staged. The
    // client uploads it again outside the cache rather than fail the launch.
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
    assert!(
        uploaded.uploaded && !uploaded.cached,
        "the bytes went up and the cache did not keep them: {uploaded:?}"
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
            // The staging node is removed either way: cache expiry does not
            // collect what was left beside the cache's own entries.
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
    // The path CI clusters take: a fallback that fired on any answer would pass
    // the tests above and stop caching for everybody.
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
    // Uploaded and cached are both true only on this path.
    assert!(uploaded.uploaded && uploaded.cached, "{uploaded:?}");

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
            // The executable bit, set on the cached node rather than the
            // written one, so that the job can exec it.
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
    // Nothing was sent, and the file is the cache's: `cached` must not merely
    // echo `uploaded`.
    assert!(uploaded.cached, "a hit is not cached: {uploaded:?}");
    assert_eq!(commands(&cluster.sent()), ["get_file_from_cache"]);
}

#[test]
fn a_create_that_failed_for_some_other_reason_is_not_a_cache_to_give_up_on() {
    // A resolve error is not a permission error, and uploading elsewhere does
    // not address it. Falling back on any failed `create` would hide it.
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
    // The stub refuses every `create`, the fallback's too, so a client falling
    // back on any failed `create` would fail with the same error. Only the
    // absent second `create` tells the two apart.
    assert_eq!(
        commands(&cluster.sent()),
        ["get_file_from_cache", "create"],
        "the failure was carried on from rather than reported: {:?}",
        cluster.sent()
    );
}

#[test]
fn an_access_denied_that_is_not_the_caches_is_not_swallowed() {
    // 901 on `write_file` to a node this caller just created is not the cache
    // being withheld; a fallback would upload twice and hide the first error.
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
    /// Whether the parameters contain `text`, a path in every use here: `set`
    /// addresses `<path>/@executable`, and `get_file_from_cache` carries a
    /// `cache_path` beside its `md5`.
    fn mentions(&self, text: &str) -> bool {
        self.parameters.contains(text)
    }
}

/// The cluster's own words for the refusal this whole file is about.
const ACCESS_DENIED: &str = "Access denied for user \"tester\": \"write | modify_children\" \
                             permission for node //tmp/yt_wrapper/file_storage/new_cache \
                             is not allowed by any matching ACE";

/// What the stub answers with.
enum Answer {
    /// 200, and this text-YSON body.
    Body(String),
    /// Cluster error 901, `Access denied`, in the `X-YT-Error` header.
    Denied,
    /// The same refusal nested under a category error, as a master that wraps
    /// its answer sends it.
    DeniedInside,
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
                Some(document(901, ACCESS_DENIED)),
                String::new(),
            ),
            Answer::DeniedInside => (
                "403 Forbidden",
                Some(wrapping(
                    1,
                    "Error creating node //tmp/yt_wrapper/file_storage/new_cache",
                    &document(901, ACCESS_DENIED),
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
    format!(r#"{{"code":{code},"message":"{}"}}"#, escape(message))
}

/// One error document carrying another, as `inner_errors` does.
fn wrapping(code: i64, message: &str, inner: &str) -> String {
    format!(
        r#"{{"code":{code},"message":"{}","inner_errors":[{inner}]}}"#,
        escape(message)
    )
}

fn escape(message: &str) -> String {
    message.replace('\\', r"\\").replace('"', "\\\"")
}

type Answering = Arc<dyn Fn(&Sent) -> Answer + Send + Sync>;

/// A cluster that answers as told and remembers what it was asked.
struct Cluster {
    proxy: String,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl Cluster {
    /// A client pointed at it, with no retries to confuse the record.
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
    // A thread per connection: an error response's body goes unread, so its
    // connection leaves the pool and the client opens another, which would
    // wait forever if connections were served in turn.
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
    // A pooled connection never used again must not hold a thread forever.
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
            // The client reads this header first, whatever the status.
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

    // The body is read before the reply, or a client still writing it reports
    // a broken pipe. Every request here declares its length.
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

/// A "worker binary" on disk, named after the test that wants one. Its
/// contents are arbitrary: only `upload_current_exe` checks for an ELF.
fn worker_file(name: &str) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("ytsaurus-rs-cache-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("creates");
    let path = directory.join(name);
    std::fs::write(&path, format!("not really a worker: {name}")).expect("writes");
    path
}
