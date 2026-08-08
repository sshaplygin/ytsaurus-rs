//! `read_file` on the wire, and the check that makes its answer a file.
//!
//! A file's bytes carry no framing, so unlike a table — whose truncation
//! leaves a record that does not parse — a `read_file` body cut short by a
//! mid-stream failure looks exactly like a shorter file. The proxy says so in
//! a trailer `ureq` cannot read (rechecked against its source, where the word
//! does not appear), and the buffered path compensates by comparing the body
//! against the size Cypress records for the node. These tests serve the file
//! from a socket in-process, which is the only place a wrong-length body can
//! be produced on demand: a cluster sends one only while something is
//! genuinely failing mid-stream.
//!
//! The stub follows the rule `request_shape.rs` learned the hard way: the
//! whole request is read before the answer is written, or the client reports a
//! broken pipe instead of the request under test.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ytsaurus_client::{Client, ClientError, RetryPolicy};

/// The path every test reads. Fixed, so the tests may assert its rendered
/// spelling — a fixed literal cannot change, where a generated value's can.
const PATH: &str = "//tmp/f";

/// Bytes that are not text: every value a byte can take, cycled past the
/// length of any one buffer, so an accidental `String` on the way through
/// would corrupt them and a comparison would say so.
fn file_of(len: usize) -> Vec<u8> {
    (0..len).map(|n| (n % 256) as u8).collect()
}

#[test]
fn a_file_comes_back_byte_for_byte_and_is_checked_against_the_recorded_size() {
    let contents = file_of(3000);
    let stub = FileStub::serving(contents.clone(), Size::Bytes(3000));

    let read = stub.client().read_file(PATH).expect("reads");
    assert_eq!(read, contents, "the bytes are not what the proxy sent");

    // The read, then the check: one heavy `read_file` and one light `get` for
    // the node's recorded size — asked *after* the body, so the size compared
    // is the size the file had when the proxy finished sending it.
    assert_eq!(
        stub.request_lines(),
        ["GET /api/v4/read_file HTTP/1.1", "GET /api/v4/get HTTP/1.1",]
    );
    let heads = stub.heads();
    assert!(
        parameters(&heads[1]).contains(r#"path="//tmp/f/@uncompressed_data_size""#),
        "the check asks about a different attribute than the documented one:\n{}",
        heads[1]
    );

    // Both requests on one connection. `ureq` pools a connection only when its
    // response body was consumed, so a second connection here would mean the
    // read left its body unread — the mistake that once put 11 623 sockets in
    // TIME_WAIT (see AGENTS.md, *Connections*).
    assert_eq!(
        stub.connections(),
        1,
        "the size check opened a fresh connection, so the file's body was not consumed"
    );
}

#[test]
fn the_read_is_a_get_with_the_path_and_nothing_else() {
    // `read_file` is registered with no input stream and as non-mutating, so
    // the verb is GET by the proxy's own rule — and its answer is raw bytes,
    // not rows, so there is no `output_format` to send. A format parameter
    // appearing here would be this crate guessing at a shape it never
    // verified; exact equality is what pins that, and the absence of a
    // `mutation_id` besides.
    let stub = FileStub::serving(b"x".to_vec(), Size::Bytes(1));

    stub.client().read_file(PATH).expect("reads");

    let heads = stub.heads();
    assert!(
        heads[0].starts_with("GET /api/v4/read_file HTTP/1.1"),
        "{}",
        heads[0]
    );
    assert_eq!(parameters(&heads[0]), r#"{path="//tmp/f"}"#);
}

#[test]
fn a_clean_short_body_is_an_error_rather_than_a_shorter_file() {
    // The failure the size check exists for. A mid-stream failure ends the
    // chunked body cleanly and puts the reason in a trailer this client cannot
    // read, so at the HTTP layer nothing is wrong — the body is simply shorter
    // than the file. Returning it as success would hand the caller a silently
    // truncated file, which for a worker binary is an exec that fails on the
    // node, at the worst possible distance from the cause.
    let stub = FileStub::serving(file_of(100), Size::Bytes(4096));

    let error = stub
        .client()
        .read_file(PATH)
        .expect_err("a 100-byte body is not a 4096-byte file");

    assert!(matches!(error, ClientError::Decode { .. }), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.contains("4096") && rendered.contains("100"),
        "the error names neither size: {rendered}"
    );
    assert!(
        rendered.contains("trailer"),
        "the error does not say why the client cannot know more: {rendered}"
    );
}

#[test]
fn a_size_the_cluster_cannot_answer_fails_the_read_rather_than_skipping_the_check() {
    // The check is promised, so it must refuse loudly rather than quietly not
    // happen: a `read_file` that skipped it on a bad answer would be exactly
    // as truncatable as one with no check at all, and nothing would say so.
    let stub = FileStub::serving(file_of(100), Size::NotAnInteger);

    let error = stub
        .client()
        .read_file(PATH)
        .expect_err("a size that is not an integer cannot check anything");

    assert!(matches!(error, ClientError::Decode { .. }), "{error:?}");
    assert!(
        error.to_string().contains("uncompressed_data_size"),
        "the error does not name the attribute that failed it: {error}"
    );
}

#[test]
fn an_empty_file_reads_back_empty() {
    // Zero bytes recorded, zero bytes served: a legitimate file, not an edge
    // the check may refuse.
    let stub = FileStub::serving(Vec::new(), Size::Bytes(0));

    let read = stub.client().read_file(PATH).expect("reads");
    assert!(
        read.is_empty(),
        "{} bytes appeared from nowhere",
        read.len()
    );
}

#[test]
fn the_streaming_read_hands_back_the_bytes_and_asks_nothing_else() {
    // The streaming half never has the whole body, so it cannot run the size
    // check — and must not pay for one it cannot run: no `get` beside the
    // read. The caller who needs certainty compares `bytes_read` against the
    // size themselves, which is what the doc on `FileReader` sends them to do.
    let contents = file_of(3000);
    let stub = FileStub::serving(contents.clone(), Size::Bytes(3000));

    let mut reader = stub
        .client()
        .read_file_streaming(PATH)
        .expect("opens the stream");
    let mut read = Vec::new();
    reader.read_to_end(&mut read).expect("reads");

    assert_eq!(read, contents, "the bytes are not what the proxy sent");
    assert_eq!(reader.bytes_read(), contents.len() as u64);
    assert_eq!(
        stub.request_lines(),
        ["GET /api/v4/read_file HTTP/1.1"],
        "the streaming path asked something the buffered path asks"
    );
}

// -------------------------------------------------------------- the stub

/// What the stub answers a `get` for the file's size with.
enum Size {
    /// `{"value"=N}` — the envelope a cluster wraps an attribute in.
    Bytes(i64),
    /// `{"value"=%true}` — well-formed, and not a size.
    NotAnInteger,
}

impl Size {
    fn body(&self) -> String {
        match self {
            Size::Bytes(n) => format!(r#"{{"value"={n}}}"#),
            Size::NotAnInteger => r#"{"value"=%true}"#.to_owned(),
        }
    }
}

/// A proxy that serves one file and the size beside it, and remembers what it
/// was asked — including how many connections the asking took.
struct FileStub {
    address: std::net::SocketAddr,
    heads: Arc<Mutex<Vec<String>>>,
    connections: Arc<Mutex<usize>>,
}

impl FileStub {
    fn serving(file: Vec<u8>, size: Size) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");
        let heads = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(Mutex::new(0_usize));

        let file = Arc::new(file);
        let size = Arc::new(size.body());
        let seen = Arc::clone(&heads);
        let counted = Arc::clone(&connections);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                *counted.lock().expect("not poisoned") += 1;
                let file = Arc::clone(&file);
                let size = Arc::clone(&size);
                let seen = Arc::clone(&seen);
                // A thread per connection: a client whose pool declined a
                // connection opens another, and serving them in turn would
                // leave it waiting forever.
                std::thread::spawn(move || serve(&stream, &file, &size, &seen));
            }
        });

        Self {
            address,
            heads,
            connections,
        }
    }

    /// A client pointed at it, sending each command once: the stub answers
    /// every request, so a retry could only confuse the record.
    fn client(&self) -> Client {
        Client::new(&format!("http://{}", self.address)).with_retries(RetryPolicy::none())
    }

    /// Everything served so far, headers and all.
    fn heads(&self) -> Vec<String> {
        self.heads.lock().expect("not poisoned").clone()
    }

    /// The request line of everything served so far.
    fn request_lines(&self) -> Vec<String> {
        self.heads()
            .iter()
            .map(|head| head.lines().next().unwrap_or_default().to_owned())
            .collect()
    }

    /// How many connections the client opened to ask what it asked.
    fn connections(&self) -> usize {
        *self.connections.lock().expect("not poisoned")
    }
}

/// Answers requests on one connection until the client hangs up.
fn serve(stream: &TcpStream, file: &[u8], size: &str, seen: &Mutex<Vec<String>>) {
    // So a connection the client keeps pooled but never uses again does not
    // hold a thread for the lifetime of the test binary.
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("sets a timeout");
    let mut writer = stream.try_clone().expect("clones");
    let mut reader = BufReader::new(stream.try_clone().expect("clones"));

    while let Some(head) = read_request(&mut reader) {
        let body: &[u8] = if head.starts_with("GET /api/v4/read_file ") {
            file
        } else {
            size.as_bytes()
        };
        seen.lock().expect("not poisoned").push(head);

        let reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
            body.len()
        );
        if writer.write_all(reply.as_bytes()).is_err() || writer.write_all(body).is_err() {
            return;
        }
        writer.flush().ok();
    }
}

/// One request head off the wire — body drained first — or `None` at the end.
fn read_request(reader: &mut BufReader<TcpStream>) -> Option<String> {
    let mut head = String::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return None,
            Ok(_) if line == "\r\n" => break,
            Ok(_) => head.push_str(&line),
        }
    }

    // Both commands here are GETs and carry nothing, but the rule is the rule:
    // a request is only finished being sent when its body has been read.
    if let Some(length) = header(&head, "content-length").and_then(|v| v.parse().ok()) {
        let mut body = vec![0_u8; length];
        reader.read_exact(&mut body).ok()?;
    }

    Some(head)
}

/// The `X-YT-Parameters` header of a captured request.
fn parameters(head: &str) -> String {
    header(head, "x-yt-parameters").unwrap_or_default()
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
