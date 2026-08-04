//! `cat` — an identity map.
//!
//! Copies every input row to an output table without decoding it, so the output
//! table should be byte-for-byte identical to the input. That makes it the
//! sharpest end-to-end check available: any discrepancy is a bug in the protocol
//! handling, not in the job's logic.
//!
//! Rows are routed by input table index, so with several input and output tables
//! it also exercises table switching. Run it with `--tables N` to declare N
//! output tables; rows from input table `i` go to output table `min(i, N-1)`.
//!
//! ```sh
//! scripts/build-worker.sh cat
//! yt map './cat' --src //tmp/in --dst //tmp/out \
//!     --format '<format=binary>yson' \
//!     --spec '{job_io={control_attributes={enable_table_index=%true}}}'
//! ```

use ytsaurus_job::{Event, JobReader, JobWriter};

fn main() {
    let tables = output_table_count();

    ytsaurus_job::run(move || {
        let mut reader = JobReader::from_stdin();
        let mut writer = JobWriter::descriptors(tables)?;

        while let Some(event) = reader.next_event()? {
            let Event::Row(row) = event else {
                // A key switch carries no data, so an identity map drops it.
                continue;
            };

            // Route by input table, clamped so a job configured with fewer
            // output tables than inputs still runs instead of failing.
            let table = usize::try_from(row.table_index.max(0))
                .unwrap_or(0)
                .min(tables - 1);

            // `raw()` forwards the original bytes. Decoding and re-encoding
            // would reorder map keys and so would not be byte-exact.
            writer.write_raw(table, row.raw())?;
        }

        writer.finish()
    })
}

/// Reads `--tables N` from the command line, defaulting to a single table.
fn output_table_count() -> usize {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--tables" {
            return args
                .next()
                .and_then(|n| n.parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(1);
        }
    }
    1
}
