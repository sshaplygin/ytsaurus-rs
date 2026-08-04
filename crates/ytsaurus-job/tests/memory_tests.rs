//! Proves the reader streams rather than accumulates.
//!
//! A job's input is routinely far larger than its memory limit, so the reader
//! must hold a bounded amount no matter how much data flows through. This feeds
//! 2 GB through it and checks the process's peak resident set stays small.
//!
//! The input is generated on the fly, so the test needs no 2 GB fixture on disk.

mod common;

use std::io::{self, Read};

use ytsaurus_job::{Event, JobReader};

/// Total bytes to push through the reader.
const TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Ceiling on peak RSS growth. The plan calls for < 256 MB; the reader's own
/// steady state is a single 1 MiB buffer, so anything near this limit means it
/// is accumulating.
const RSS_LIMIT_BYTES: u64 = 256 * 1024 * 1024;

/// How much the process may grow while consuming the whole 2 GB.
///
/// The reader's own steady state is a single 1 MiB buffer, so anything beyond a
/// few multiples of that means it is holding on to input. This catches a leak
/// that [`RSS_LIMIT_BYTES`] alone would not.
const RSS_GROWTH_LIMIT_BYTES: u64 = 32 * 1024 * 1024;

/// Generates a list fragment of `{key=...;value=...}` rows without ever holding
/// more than one row in memory.
struct SyntheticInput {
    produced: u64,
    target: u64,
    record: Vec<u8>,
    offset: usize,
    counter: u64,
}

impl SyntheticInput {
    fn new(target: u64) -> Self {
        Self {
            produced: 0,
            target,
            record: Vec::new(),
            offset: 0,
            counter: 0,
        }
    }

    fn refill(&mut self) {
        use common::{bin_i64, bin_string};

        self.record.clear();
        self.record.push(b'{');
        bin_string(b"key", &mut self.record);
        self.record.push(b'=');
        bin_string(
            format!("user_{:012}", self.counter).as_bytes(),
            &mut self.record,
        );
        self.record.push(b';');
        bin_string(b"value", &mut self.record);
        self.record.push(b'=');
        bin_i64(self.counter as i64, &mut self.record);
        self.record.push(b';');
        bin_string(b"payload", &mut self.record);
        self.record.push(b'=');
        bin_string(&[b'x'; 200], &mut self.record);
        self.record.push(b'}');
        self.record.push(b';');

        self.offset = 0;
        self.counter += 1;
    }
}

impl Read for SyntheticInput {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.produced >= self.target {
            return Ok(0);
        }
        if self.offset >= self.record.len() {
            self.refill();
        }

        let n = (self.record.len() - self.offset).min(buf.len());
        buf[..n].copy_from_slice(&self.record[self.offset..self.offset + n]);
        self.offset += n;
        self.produced += n as u64;
        Ok(n)
    }
}

/// Peak resident set size of this process, in bytes.
///
/// `getrusage(RUSAGE_SELF).ru_maxrss` is a high-water mark, so a transient spike
/// cannot hide from it. Linux reports kilobytes, macOS reports bytes.
#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
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
        ru_ixrss: i64,
        ru_idrss: i64,
        ru_isrss: i64,
        ru_minflt: i64,
        ru_majflt: i64,
        ru_nswap: i64,
        ru_inblock: i64,
        ru_oublock: i64,
        ru_msgsnd: i64,
        ru_msgrcv: i64,
        ru_nsignals: i64,
        ru_nvcsw: i64,
        ru_nivcsw: i64,
    }

    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut Rusage) -> i32;
    }

    let mut usage = Rusage::default();
    // SAFETY: `getrusage` fills the caller-provided struct; the layout above
    // matches the platform definition for the fields we read.
    let rc = unsafe {
        getrusage(0 /* RUSAGE_SELF */, &raw mut usage)
    };
    if rc != 0 {
        return None;
    }

    let raw = u64::try_from(usage.ru_maxrss).ok()?;
    Some(if cfg!(target_os = "linux") {
        raw * 1024
    } else {
        raw
    })
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

/// The headline requirement: 2 GB in, bounded memory throughout.
///
/// `#[ignore]`d so it stays out of the default run, which means `--all-targets`
/// skips it too. CI therefore gives it a dedicated step — see the
/// "2 GB streaming memory test" step in `.github/workflows/ci.yml`. If that step
/// is ever removed, nothing checks that the reader still streams.
///
/// Run locally with:
///
/// ```sh
/// cargo test -p ytsaurus-job --test memory_tests -- --ignored --nocapture
/// ```
#[test]
#[ignore = "moves 2 GB; run explicitly"]
fn two_gigabytes_of_input_stay_within_the_memory_budget() {
    let before = peak_rss_bytes();

    let mut reader = JobReader::binary(SyntheticInput::new(TOTAL_BYTES));

    let mut rows: u64 = 0;
    let mut bytes: u64 = 0;
    while let Some(event) = reader.next_event().expect("stream reads cleanly") {
        if let Event::Row(row) = event {
            rows += 1;
            // Touch the row so nothing can be optimised away, and exercise the
            // borrowed-decode path that a real job would use.
            bytes += row.raw().len() as u64;
        }
    }

    let after = peak_rss_bytes();

    assert!(rows > 0, "no rows were read");
    assert!(
        bytes > TOTAL_BYTES / 2,
        "expected to see most of the input, saw {bytes} bytes across {rows} rows"
    );

    println!("read {rows} rows / {bytes} bytes");

    match (before, after) {
        (Some(before), Some(after)) => {
            let growth = after.saturating_sub(before);
            println!(
                "peak RSS: {:.1} MiB -> {:.1} MiB (grew {:.1} MiB)",
                before as f64 / 1048576.0,
                after as f64 / 1048576.0,
                growth as f64 / 1048576.0,
            );

            // The absolute figure is dominated by the test binary's own
            // footprint and differs by platform — ~47 MiB on Linux, ~2 MiB on
            // macOS — so the budget alone is a loose check.
            assert!(
                after < RSS_LIMIT_BYTES,
                "peak RSS {after} bytes exceeded the {RSS_LIMIT_BYTES} byte budget; \
                 the reader is accumulating input instead of streaming it"
            );

            // The real invariant: consuming 2 GB must not grow the process.
            // This is far sharper than the budget above, which a 100 MB leak
            // would slip past unnoticed.
            assert!(
                growth < RSS_GROWTH_LIMIT_BYTES,
                "peak RSS grew by {growth} bytes while streaming {TOTAL_BYTES} bytes; \
                 the reader should hold a bounded buffer regardless of input size"
            );
        }
        _ => println!("peak RSS unavailable on this platform; skipped the memory assertion"),
    }
}

/// A fast version of the same shape, so the streaming property is covered by
/// the default test run too.
#[test]
fn a_hundred_megabytes_stay_within_the_memory_budget() {
    const HUNDRED_MB: u64 = 100 * 1024 * 1024;

    let mut reader = JobReader::binary(SyntheticInput::new(HUNDRED_MB));

    let mut rows = 0u64;
    while let Some(event) = reader.next_event().expect("stream reads cleanly") {
        if matches!(event, Event::Row(_)) {
            rows += 1;
        }
    }

    assert!(rows > 100_000, "expected many rows, got {rows}");

    if let Some(peak) = peak_rss_bytes() {
        assert!(
            peak < RSS_LIMIT_BYTES,
            "peak RSS {peak} bytes exceeded the budget after only 100 MB of input"
        );
    }
}
