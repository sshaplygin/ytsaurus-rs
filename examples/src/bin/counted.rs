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
    // A row is a binary YSON map: `{` name `=` value `;` … `}`. Names are
    // strings — `\x01<zigzag length>bytes`, so a three-byte one is preceded by
    // `\x01\x06` — but so are string *values*, encoded identically. What tells
    // the two apart is position: a name follows `{` or `;` and is followed by
    // `=`. Matching the bytes alone would count `{name="key"}` as having the
    // column, and the statistic this job exists to report would be short by
    // however many rows happened to hold that word.
    raw.windows(7)
        .any(|w| matches!(w[0], b'{' | b';') && &w[1..6] == b"\x01\x06key" && w[6] == b'=')
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

    #[test]
    fn a_row_whose_value_is_the_word_key_does_not_count() {
        #[derive(serde::Serialize)]
        struct Named<'a> {
            name: &'a str,
            count: i64,
        }

        // `name="key"` encodes the value exactly as the column name `key`
        // encodes. Only its position says which it is.
        let row = to_vec(
            &Named {
                name: "key",
                count: 1,
            },
            YsonFormat::Binary,
        )
        .unwrap();
        assert!(
            !has_key(&row),
            "a value that reads `key` is not a `key` column"
        );
    }

    #[test]
    fn the_scan_matches_what_the_reader_hands_over() {
        // `has_key` anchors on `{` and `;`, which only holds if a row's raw
        // bytes are the whole map and not its contents. They are — but that is
        // the reader's promise rather than this job's, so it is worth pinning
        // here, through the reader the job actually uses.
        let mut fragment = Vec::new();
        for row in [
            to_vec(&WithKey { key: "a", count: 1 }, YsonFormat::Binary).unwrap(),
            to_vec(&WithoutKey { count: 1 }, YsonFormat::Binary).unwrap(),
        ] {
            fragment.extend_from_slice(&row);
            fragment.push(b';');
        }

        let mut reader = JobReader::binary(std::io::Cursor::new(fragment));
        let mut seen = Vec::new();
        while let Some(event) = reader.next_event().expect("reads") {
            if let Event::Row(row) = event {
                seen.push(has_key(row.raw()));
            }
        }

        assert_eq!(seen, vec![true, false]);
    }

    #[test]
    fn the_key_column_is_found_wherever_it_sits() {
        // Not only first: serde writes fields in declaration order, and a row
        // whose `key` comes second follows a `;` rather than the opening `{`.
        #[derive(serde::Serialize)]
        struct CountFirst<'a> {
            count: i64,
            key: &'a str,
        }

        let row = to_vec(&CountFirst { count: 1, key: "a" }, YsonFormat::Binary).unwrap();
        assert!(has_key(&row));
    }
}
