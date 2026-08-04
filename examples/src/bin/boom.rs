//! `boom` — a worker that fails on purpose.
//!
//! It exists for the failure path: an operation that fails should hand the user
//! what the job printed, not just the word `failed`. Everything here is chosen
//! to make that testable — it logs an ordinary line first, so the stderr the
//! launcher reports is not only the panic, and the panic carries a marker
//! string a test can look for.
//!
//! ```sh
//! scripts/build-worker.sh boom
//! cargo run -p ytsaurus-client --example diagnose
//! ```

use ytsaurus_job::{Event, JobReader, JobWriter};

/// Printed by the panic. A test asserts on this exact string, so that it is
/// checking this job's stderr and not some other diagnostic that happens to
/// mention a failure.
const MARKER: &str = "boom: this job fails on purpose";

fn main() {
    ytsaurus_job::run(|| {
        let mut reader = JobReader::from_stdin();
        let mut writer = JobWriter::descriptors(1)?;

        eprintln!("boom: started, reading input");

        let mut rows = 0_u64;
        while let Some(event) = reader.next_event()? {
            let Event::Row(row) = event else { continue };

            // Fail on a row, not before reading anything: a job that dies at
            // startup exercises the exec path, while this exercises the path a
            // real bug takes.
            rows += 1;
            if rows == 1 {
                panic!("{MARKER} (row {rows}, {} bytes)", row.raw().len());
            }
        }

        writer.finish()
    })
}
