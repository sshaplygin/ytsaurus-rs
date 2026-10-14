//! `read_file` on the wire, and the check that makes its answer a file.
//!
//! A file's bytes carry no framing, so a body cut short by a mid-stream failure
//! looks like a shorter file; the proxy's verdict is in a trailer `ureq` cannot
//! read. The buffered path compares the body with the size Cypress records.
//! The stub serves the file in-process, the only place a wrong-length body can
//! be produced on demand.
//!
//! The stub reads the whole request before it answers, or the client reports a
//! broken pipe instead of the request under test.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ytsaurus_client::{Client, ClientError, RetryPolicy};

/// The path every test reads, fixed so the tests may assert its spelling.
const PATH: &str = "//tmp/f";

/// Every byte value, cycled: an accidental `String` on the way through would
/// corrupt them.
fn file_of(len: usize) -> Vec<u8> {
    (0..len).map(|n| (n % 256) as u8).collect()
}

#[test]
fn a_file_comes_back_byte_for_byte_and_is_checked_against_the_recorded_size() {
    let contents = file_of(3000);
    let stub = FileStub::serving(contents.clone(), Size::Bytes(3000));

    let read = stub.client().read_file(PATH).expect("reads");
    assert_eq!(read, contents, "the bytes are not what the proxy sent");

    // One `read_file`, then one `get` of the recorded size, asked after the
    // body so the size is the one the file had when the proxy finished.
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

    // One connection: `ureq` pools a connection only when its body was
    // consumed (docs/protocol-reference.md, *Connections*).
    assert_eq!(
        stub.connections(),
        1,
        "the size check opened a fresh connection, so the file's body was not consumed"
    );
}

#[test]
fn the_read_is_a_get_with_the_path_and_nothing_else() {
    // `read_file` has no input stream and does not mutate, so the verb is GET;
    // its answer is raw bytes, so no `output_format` is sent. Exact equality
    // of the parameters also pins that there is no `mutation_id`.
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
    // A mid-stream failure ends the chunked body cleanly with the reason in an
    // unreadable trailer, so only the size says the body is short. The error
    // names both sizes and the trailer.
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
    // A size that is not an integer fails the read: skipping the check on a
    // bad answer would leave truncation undetected.
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
fn a_size_check_that_never_got_an_answer_is_reported_as_the_read_it_belongs_to() {
    // The size `get` fails after the file arrived. The error starts with
    // `read_file`, the command the caller sent, and quotes the `get`'s 500
    // rather than swallowing it.
    let stub = FileStub::serving(file_of(100), Size::Unanswerable);

    let error = stub
        .client()
        .read_file(PATH)
        .expect_err("the size the body was to be checked against never arrived");

    let rendered = error.to_string();
    assert!(
        rendered.starts_with("read_file"),
        "the caller is sent after a command they never sent: {rendered}"
    );
    assert!(
        rendered.contains("500"),
        "the underlying failure was swallowed rather than quoted: {rendered}"
    );
}

#[test]
fn an_empty_file_reads_back_empty() {
    // Zero bytes recorded and served is a legitimate file.
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
    // The streaming read never has the whole body, so it sends no size `get`;
    // `FileReader`'s doc tells the caller to compare `bytes_read` instead.
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
    /// `{"value"=N}`, the envelope a cluster wraps an attribute in.
    Bytes(i64),
    /// `{"value"=%true}`: well-formed, and not a size.
    NotAnInteger,
    /// A 500 with no error document, as a master hiccup between the two
    /// requests looks from here.
    Unanswerable,
}

impl Size {
    /// The status line and body the stub answers a size `get` with.
    fn reply(&self) -> (&'static str, String) {
        match self {
            Size::Bytes(n) => ("200 OK", format!(r#"{{"value"={n}}}"#)),
            Size::NotAnInteger => ("200 OK", r#"{"value"=%true}"#.to_owned()),
            Size::Unanswerable => ("500 Internal Server Error", String::new()),
        }
    }
}

/// A proxy serving one file and its size, recording requests and connections.
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
        let size = Arc::new(size.reply());
        let seen = Arc::clone(&heads);
        let counted = Arc::clone(&connections);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                *counted.lock().expect("not poisoned") += 1;
                let file = Arc::clone(&file);
                let size = Arc::clone(&size);
                let seen = Arc::clone(&seen);
                // A thread per connection: serving a second connection in
                // turn would leave the client waiting forever.
                std::thread::spawn(move || serve(&stream, &file, &size, &seen));
            }
        });

        Self {
            address,
            heads,
            connections,
        }
    }

    /// A client pointed at it, with no retries to confuse the record.
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
fn serve(
    stream: &TcpStream,
    file: &[u8],
    size: &(&'static str, String),
    seen: &Mutex<Vec<String>>,
) {
    // A pooled connection never used again must not hold a thread forever.
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("sets a timeout");
    let mut writer = stream.try_clone().expect("clones");
    let mut reader = BufReader::new(stream.try_clone().expect("clones"));

    while let Some(head) = read_request(&mut reader) {
        let (status, body): (&str, &[u8]) = if head.starts_with("GET /api/v4/read_file ") {
            ("200 OK", file)
        } else {
            (size.0, size.1.as_bytes())
        };
        seen.lock().expect("not poisoned").push(head);

        let reply = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
            body.len()
        );
        if writer.write_all(reply.as_bytes()).is_err() || writer.write_all(body).is_err() {
            return;
        }
        writer.flush().ok();
    }
}

/// One request head off the wire, body drained, or `None` at the end.
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

    // A request is fully sent only once its body has been read.
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
