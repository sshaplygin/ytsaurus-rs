//! What the client costs to move typed rows, with the cluster taken out of it.
//!
//! `examples/append.rs` times a write against a real cluster, which is mostly
//! the cluster. This times the client's part: Rust values into a request body,
//! and a response body back into Rust values. The server is a loopback socket
//! that reads the body and discards it, so what is timed is serialisation,
//! HTTP framing and one loopback round trip. It checks two claims:
//!
//! - `write_table_rows` encodes inside the request body, a bufferful at a
//!   time, at no cost against encoding the whole table into a `Vec` first;
//! - reading rows into a type is not so much dearer than taking the bytes that
//!   a caller should hesitate over it.
//!
//! ```sh
//! cargo bench -p ytsaurus-client
//! ```

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use ytsaurus_client::{Client, RetryPolicy};
use ytsaurus_yson::{YsonFormat, to_vec};

/// How many rows each measurement moves.
const SIZES: [usize; 3] = [1_000, 10_000, 100_000];

#[derive(Serialize, Deserialize, Clone)]
struct Row {
    n: i64,
    name: String,
    payload: String,
    ratio: f64,
    flag: bool,
}

fn row(n: usize) -> Row {
    Row {
        n: n as i64,
        name: format!("row-{n:08}"),
        payload: "0123456789abcdef0123456789abcdef".to_owned(),
        ratio: n as f64 / 7.0,
        flag: n.is_multiple_of(2),
    }
}

/// The whole table encoded up front: one `Vec` per row, then a copy into the
/// buffer. The baseline the streaming encoder must match.
fn encoded_by_hand(rows: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for n in 0..rows {
        out.extend_from_slice(&to_vec(&row(n), YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    out
}

/// A socket that answers every request the same way, until told to stop.
/// Connections are kept alive and serve any number of requests. With a fresh
/// connection per iteration, the TCP handshake and teardown were about half the
/// time at a thousand rows, and one arm at criterion's defaults left tens of
/// thousands of sockets in `TIME_WAIT`, exhausting the ephemeral port range on
/// macOS.
struct NullCluster {
    address: String,
    stop: Arc<AtomicBool>,
}

impl NullCluster {
    fn new(reply_body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = format!("http://{}", listener.local_addr().expect("has an address"));
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);

        std::thread::spawn(move || {
            for connection in listener.incoming() {
                if stopping.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(stream) = connection else { return };
                // One thread per connection: `ureq`'s pool keeps one open, and
                // a second would otherwise wait behind it for the whole run.
                let body = reply_body.clone();
                std::thread::spawn(move || serve(stream, &body));
            }
        });

        Self { address, stop }
    }
}

impl Drop for NullCluster {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Unblock the accept, so the thread notices and exits.
        let _ = TcpStream::connect(self.address.trim_start_matches("http://"));
    }
}

/// Answers requests on one connection until the client goes away.
fn serve(mut stream: TcpStream, body: &[u8]) {
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

        // The body is read whole before replying: an early reply would time a
        // broken pipe, and a partial read leaves the next request mid-body.
        let lowercase = head.to_lowercase();
        if let Some(length) = lowercase
            .lines()
            .find(|line| line.starts_with("content-length:"))
            .and_then(|line| line.split(':').nth(1))
            .and_then(|value| value.trim().parse::<usize>().ok())
        {
            let mut sink = vec![0_u8; length];
            if reader.read_exact(&mut sink).is_err() {
                return;
            }
        } else if lowercase.contains("transfer-encoding: chunked") {
            // `write_table_rows` streams, so its body arrives chunked and its
            // length is only known when the terminating chunk shows up.
            if !drain_chunked(&mut reader) {
                return;
            }
        }

        let mut reply =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        reply.extend_from_slice(body);
        if stream.write_all(&reply).is_err() {
            return;
        }
        stream.flush().ok();
    }
}

/// Consumes a chunked body up to its terminating zero-length chunk. `false` if
/// it could not be read to its end; the caller then drops the connection.
fn drain_chunked(reader: &mut BufReader<TcpStream>) -> bool {
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() {
            return false;
        }
        // A chunk size may carry extensions after a `;`; treating one as a
        // terminator would truncate the body.
        let size = header.trim().split(';').next().unwrap_or("");
        let Ok(size) = usize::from_str_radix(size, 16) else {
            return false;
        };
        if size == 0 {
            let mut trailer = String::new();
            reader.read_line(&mut trailer).ok();
            return true;
        }
        let mut chunk = vec![0_u8; size + 2]; // the chunk and its CRLF
        if reader.read_exact(&mut chunk).is_err() {
            return false;
        }
    }
}

fn client_for(cluster: &NullCluster) -> Client {
    Client::new(&cluster.address).with_retries(RetryPolicy::none())
}

fn writing(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");

    for rows in SIZES {
        let bytes = encoded_by_hand(rows).len() as u64;
        group.throughput(Throughput::Bytes(bytes));

        // The one this crate ships: rows go in as values, and the encoder runs
        // inside the request body.
        group.bench_with_input(
            BenchmarkId::new("write_table_rows", rows),
            &rows,
            |b, &n| {
                let cluster = NullCluster::new(br#"{}"#.to_vec());
                let client = client_for(&cluster);
                b.iter(|| {
                    client
                        .write_table_rows("//tmp/bench", (0..n).map(row))
                        .expect("writes");
                });
            },
        );

        // The whole table encoded into a `Vec`, then sent. If this were faster,
        // the streaming encoder would not be worth having.
        group.bench_with_input(
            BenchmarkId::new("encode_then_write_table", rows),
            &rows,
            |b, &n| {
                let cluster = NullCluster::new(br#"{}"#.to_vec());
                let client = client_for(&cluster);
                b.iter(|| {
                    let encoded = encoded_by_hand(n);
                    client.write_table("//tmp/bench", &encoded).expect("writes");
                });
            },
        );
    }

    group.finish();
}

fn reading(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    for rows in SIZES {
        let table = encoded_by_hand(rows);
        group.throughput(Throughput::Bytes(table.len() as u64));

        // Rows as values: the decode is what is being measured.
        group.bench_with_input(BenchmarkId::new("read_table_rows", rows), &rows, |b, _| {
            let cluster = NullCluster::new(table.clone());
            let client = client_for(&cluster);
            b.iter(|| {
                let rows: Vec<Row> = client.read_table_rows("//tmp/bench").expect("reads");
                assert!(!rows.is_empty());
            });
        });

        // The floor: the same bytes, undecoded. The gap is what a type costs.
        group.bench_with_input(BenchmarkId::new("read_table", rows), &rows, |b, _| {
            let cluster = NullCluster::new(table.clone());
            let client = client_for(&cluster);
            b.iter(|| {
                let bytes = client.read_table("//tmp/bench").expect("reads");
                assert!(!bytes.is_empty());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, writing, reading);
criterion_main!(benches);
