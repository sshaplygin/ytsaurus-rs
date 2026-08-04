//! `counted` — an identity map that reports what it saw.
//!
//! The cluster measures a job from the outside: CPU, memory, rows in and out.
//! What it cannot see is anything about the work itself. This job counts what
//! it read and what it rejected, and reports both as custom statistics, which
//! the operation aggregates across all its jobs.
//!
//! A row is "rejected" if it has no `key` column. Rejected rows are dropped
//! rather than failing the job — which is exactly the situation where a
//! statistic earns its keep, since nothing else would tell you it happened.
//!
//! ```sh
//! scripts/build-worker.sh counted
//! cargo run -p ytsaurus-client --example statistics
//! ```

use ytsaurus_job::{Event, JobReader, JobStatistics, JobWriter};

fn main() {
    ytsaurus_job::run(|| {
        let mut reader = JobReader::from_stdin();
        let mut writer = JobWriter::descriptors(1)?;
        let mut stats = JobStatistics::new();

        while let Some(event) = reader.next_event()? {
            let Event::Row(row) = event else { continue };
            let raw = row.raw();

            stats.add("rows/read", 1)?;
            stats.add("bytes/read", raw.len() as i64)?;

            if !has_key(raw) {
                stats.add("rows/rejected", 1)?;
                continue;
            }

            writer.write_raw(0, raw)?;
        }

        // Both are explicit. Buffered output that is never flushed is missing
        // rows, and statistics that are never sent are a silent job.
        writer.finish()?;
        stats.finish()
    })
}

/// Whether the row has a `key` column, without decoding it.
///
/// Byte-level on purpose: this runs per row, and the job's whole point is to
/// count cheaply.
fn has_key(raw: &[u8]) -> bool {
    // Binary YSON strings are `\x01<zigzag length>bytes`, so a three-byte key
    // is preceded by `\x01\x06`.
    raw.windows(5).any(|w| w == b"\x01\x06key")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::{YsonFormat, to_vec};

    #[derive(serde::Serialize)]
    struct WithKey<'a> {
        key: &'a str,
        count: i64,
    }

    #[derive(serde::Serialize)]
    struct WithoutKey {
        count: i64,
    }

    #[test]
    fn a_row_with_a_key_is_recognised() {
        let row = to_vec(&WithKey { key: "a", count: 1 }, YsonFormat::Binary).unwrap();
        assert!(has_key(&row));
    }

    #[test]
    fn a_row_without_one_is_not() {
        let row = to_vec(&WithoutKey { count: 1 }, YsonFormat::Binary).unwrap();
        assert!(!has_key(&row));
    }

    #[test]
    fn a_column_merely_ending_in_key_does_not_count() {
        #[derive(serde::Serialize)]
        struct Other<'a> {
            monkey: &'a str,
        }

        let row = to_vec(&Other { monkey: "x" }, YsonFormat::Binary).unwrap();
        assert!(
            !has_key(&row),
            "the length prefix is what separates `key` from `monkey`"
        );
    }
}
