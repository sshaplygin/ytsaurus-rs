//! `streaming` — a table larger than the program that moves it.
//!
//! `read_table` and `write_table` hold a whole table at once, which is right
//! for a launcher inspecting a result and wrong for anything the size of the
//! data. This writes a table from a generator, reads it back as a stream, and
//! then reads the same table the buffered way — watching peak RSS through all
//! three, because that is the only number that settles the question.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run --release -p ytsaurus-client --example streaming
//! ```
//!
//! `YT_STREAM_MIB` sets how much to move; the default is 64 MiB.

use std::io::Read;
use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError};
use ytsaurus_job::{Event, JobReader};

/// Where the demo keeps its table.
const BASE: &str = "//tmp/ytsaurus_rs_streaming";

/// How much to move, unless `YT_STREAM_MIB` says otherwise.
const DEFAULT_MIB: u64 = 64;

/// What the generator hands the transport at a time.
///
/// The whole point of the exercise: this, and not the table, is what the
/// writing side holds.
const CHUNK: usize = 64 * 1024;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nstreaming failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;
    let path = format!("{BASE}/rows");

    let mib = std::env::var("YT_STREAM_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MIB);
    let target = mib * 1024 * 1024;

    step("Preparing Cypress");
    client.remove_tree(BASE)?;
    client.create("table", &path)?;
    let baseline = peak_rss();
    done(&format!("{path}, peak RSS so far {}", megabytes(baseline)));

    step(&format!("Writing about {mib} MiB from a generator"));
    let rows = Rows::of_at_least(target);
    let expected = rows.rows_to_come();
    client.write_table_streaming(&path, rows)?;
    let after_write = peak_rss();
    done(&format!(
        "{expected} rows, {} on the cluster, peak RSS {}",
        megabytes(
            client
                .get(&format!("{path}/@uncompressed_data_size"))?
                .as_i64()
                .unwrap_or(0) as u64
        ),
        megabytes(after_write)
    ));

    step("Reading it back as a stream");
    // The client hands over bytes; the job runtime decodes them. Same decoder
    // as a job on a cluster node, on the same wire format.
    let mut reader = JobReader::binary(client.read_table_streaming(&path)?);
    let mut counted = 0_u64;
    let mut total = 0_i64;
    while let Some(event) = reader.next_event().map_err(decoding)? {
        if let Event::Row(row) = event {
            counted += 1;
            // Only the column this checks: serde ignores the rest, so the
            // payload is never copied out of the reader's buffer.
            total += row.parse::<Counted>().map_err(decoding)?.n;
        }
    }
    let after_stream = peak_rss();

    check(
        &format!(
            "{counted} rows counted, peak RSS {}",
            megabytes(after_stream)
        ),
        counted == expected,
    )?;
    check(
        "and their values add up to what was written",
        total == expected_total(expected),
    )?;

    step("The same table, read into memory");
    let whole = client.read_table(&path)?;
    let after_buffered = peak_rss();
    done(&format!(
        "{} in hand, peak RSS {}",
        megabytes(whole.len() as u64),
        megabytes(after_buffered)
    ));

    // The high-water mark is what a program is actually charged for, so this is
    // the comparison that matters. Not an assertion: RSS is the operating
    // system's business and a run under a profiler or an allocator with
    // different habits could move it.
    println!(
        "\nStreaming the {} table cost {} of peak RSS; reading it in cost {}.",
        megabytes(whole.len() as u64),
        megabytes(after_stream.saturating_sub(baseline)),
        megabytes(after_buffered.saturating_sub(after_stream))
    );
    println!("Table left at {path}");
    Ok(())
}

/// The one column the reading side looks at.
#[derive(serde::Deserialize)]
struct Counted {
    n: i64,
}

/// Rows encoded on demand, never all at once.
///
/// A `Read` rather than a `Vec`, because handing `write_table_streaming` a
/// `Vec` would defeat the exercise. Each fill produces whole records: a
/// record split across two fills would still be correct on the wire, but this
/// keeps what the buffer holds easy to reason about.
struct Rows {
    remaining: u64,
    total: u64,
    buffer: Vec<u8>,
    position: usize,
}

impl Rows {
    /// Enough rows to make a table of at least `bytes`.
    fn of_at_least(bytes: u64) -> Self {
        let per_row = encode(0).len() as u64;
        let count = bytes.div_ceil(per_row);
        Self {
            remaining: count,
            total: count,
            buffer: Vec::with_capacity(CHUNK + 128),
            position: 0,
        }
    }

    fn rows_to_come(&self) -> u64 {
        self.total
    }
}

impl Read for Rows {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.position == self.buffer.len() {
            if self.remaining == 0 {
                return Ok(0);
            }
            self.buffer.clear();
            self.position = 0;
            while self.buffer.len() < CHUNK && self.remaining > 0 {
                self.buffer
                    .extend_from_slice(&encode(self.total - self.remaining));
                self.remaining -= 1;
            }
        }

        let n = out.len().min(self.buffer.len() - self.position);
        out[..n].copy_from_slice(&self.buffer[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}

/// One row, as the binary YSON a job would write.
fn encode(n: u64) -> Vec<u8> {
    use serde::Serialize;
    use ytsaurus_yson::{YsonFormat, to_vec};

    #[derive(Serialize)]
    struct Row<'a> {
        n: i64,
        payload: &'a str,
    }

    let row = Row {
        n: n as i64,
        // Fixed width, so the row count follows from the size asked for.
        payload: "0123456789abcdef0123456789abcdef",
    };
    let mut bytes = to_vec(&row, YsonFormat::Binary).expect("encodes");
    bytes.push(b';');
    bytes
}

/// 0 + 1 + … + (count - 1), which is what the `n` column adds up to.
fn expected_total(count: u64) -> i64 {
    let count = count as i64;
    count * (count - 1) / 2
}

fn decoding(e: ytsaurus_job::JobError) -> ClientError {
    ClientError::Decode {
        command: "read_table".to_owned(),
        reason: e.to_string(),
    }
}

fn megabytes(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// The high-water mark of this process's resident memory.
///
/// `getrusage(RUSAGE_SELF).ru_maxrss` is a high-water mark, so a spike cannot
/// hide from it between two readings. Linux reports kilobytes, macOS bytes.
#[cfg(unix)]
fn peak_rss() -> u64 {
    #[repr(C)]
    #[derive(Default)]
    struct Timeval {
        tv_sec: i64,
        tv_usec: i64,
    }

    #[repr(C)]
    #[derive(Default)]
    struct Rusage {
        ru_utime: Timeval,
        ru_stime: Timeval,
        ru_maxrss: i64,
        rest: [i64; 13],
    }

    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut Rusage) -> i32;
    }

    let mut usage = Rusage::default();
    // SAFETY: `getrusage` fills the caller-provided struct; the prefix above
    // matches the platform layout for the one field this reads, and the tail is
    // sized to cover the rest of it.
    let rc = unsafe { getrusage(0, &raw mut usage) };
    if rc != 0 {
        return 0;
    }

    let scale = if cfg!(target_os = "macos") { 1 } else { 1024 };
    (usage.ru_maxrss.max(0) as u64) * scale
}

#[cfg(not(unix))]
fn peak_rss() -> u64 {
    0
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn done(what: &str) {
    println!("   ok {what}");
}

fn check(what: &str, passed: bool) -> Result<(), ClientError> {
    if passed {
        done(what);
        return Ok(());
    }
    eprintln!("   FAIL {what}");
    Err(ClientError::Config(format!("check failed: {what}")))
}
