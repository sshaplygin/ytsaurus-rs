//! `wordcount` — the canonical MapReduce job, as a map and a reduce phase.
//!
//! One binary, two modes, so there is only one file to upload:
//!
//! ```sh
//! scripts/build-worker.sh wordcount
//!
//! yt map-reduce \
//!     --mapper './wordcount map' --reducer './wordcount reduce' \
//!     --reduce-by word \
//!     --src //tmp/lines --dst //tmp/counts \
//!     --format '<format=binary>yson' \
//!     --map-local-file target/x86_64-unknown-linux-musl/release-worker/wordcount \
//!     --reduce-local-file target/x86_64-unknown-linux-musl/release-worker/wordcount \
//!     --spec '{reduce_job_io={control_attributes={enable_key_switch=%true}}}'
//! ```
//!
//! The reduce phase needs `enable_key_switch`, which is what
//! [`JobReader::groups`](ytsaurus_job::JobReader::groups) splits on. Without it
//! the whole input arrives as one group and every word would be summed together.
//!
//! The `text` column is read as bytes rather than as a `String`: YTsaurus string
//! columns hold arbitrary bytes, and a row containing invalid UTF-8 would fail a
//! whole job that insisted on `String`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use ytsaurus_job::{Event, JobError, JobReader, JobWriter};

/// A row of the input table.
#[derive(Deserialize)]
struct Line<'a> {
    /// Free text. Bytes, not `String` — see the module docs.
    #[serde(with = "serde_bytes", borrow)]
    text: &'a [u8],
}

/// What the mapper emits and the reducer consumes.
#[derive(Serialize, Deserialize)]
struct WordCount<'a> {
    #[serde(with = "serde_bytes", borrow)]
    word: &'a [u8],
    count: i64,
}

/// The reducer's output. Owned, because it is built after the input row is gone.
#[derive(Serialize)]
struct Total {
    #[serde(with = "serde_bytes")]
    word: Vec<u8>,
    count: i64,
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();

    match mode.as_str() {
        "map" => ytsaurus_job::run(map),
        "reduce" => ytsaurus_job::run(reduce),
        other => {
            eprintln!("usage: wordcount <map|reduce>   (got {other:?})");
            std::process::exit(2);
        }
    }
}

/// Splits each line into words and emits `{word=...; count=1}`.
///
/// Counts are pre-aggregated per row, which cuts the volume the shuffle has to
/// move without changing the result.
fn map() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(1)?;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };
        let line: Line = row.parse()?;

        // The map lives inside the loop on purpose. Its keys borrow from `row`,
        // which borrows the reader's buffer, so hoisting it out would keep the
        // reader borrowed across iterations and would not compile. That is the
        // zero-copy design working as intended: a borrow cannot outlive the row
        // it points into.
        let mut counts: HashMap<&[u8], i64> = HashMap::new();
        for word in split_words(line.text) {
            *counts.entry(word).or_insert(0) += 1;
        }

        for (word, count) in &counts {
            writer.write(
                0,
                &WordCount {
                    word,
                    count: *count,
                },
            )?;
        }
    }

    writer.finish()
}

/// Sums the counts within each `reduce_by` group.
fn reduce() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(1)?;

    let mut groups = reader.groups();
    while let Some(mut group) = groups.next_group()? {
        let mut word: Option<Vec<u8>> = None;
        let mut total: i64 = 0;

        while let Some(row) = group.next_row()? {
            let entry: WordCount = row.parse()?;
            if word.is_none() {
                // Every row in a group shares the reduce key, so the first one
                // decides the word.
                word = Some(entry.word.to_vec());
            }
            total += entry.count;
        }

        // A group always has at least one row, but a `None` here would mean an
        // empty group; skipping it is better than emitting a row with no word.
        if let Some(word) = word {
            writer.write(0, &Total { word, count: total })?;
        }
    }

    writer.finish()
}

/// Splits on anything that is not an ASCII letter, digit or apostrophe.
///
/// Deliberately byte-oriented: the input is not required to be UTF-8, and a
/// naive `str::split_whitespace` would need a lossy conversion first.
fn split_words(text: &[u8]) -> impl Iterator<Item = &[u8]> {
    text.split(|b| !(b.is_ascii_alphanumeric() || *b == b'\''))
        .filter(|w| !w.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_punctuation_and_whitespace() {
        let words: Vec<&[u8]> = split_words(b"the quick, brown  fox!").collect();
        assert_eq!(
            words,
            vec![
                b"the".as_slice(),
                b"quick".as_slice(),
                b"brown".as_slice(),
                b"fox".as_slice()
            ]
        );
    }

    #[test]
    fn keeps_apostrophes_and_digits() {
        let words: Vec<&[u8]> = split_words(b"it's 42 o'clock").collect();
        assert_eq!(
            words,
            vec![b"it's".as_slice(), b"42".as_slice(), b"o'clock".as_slice()]
        );
    }

    #[test]
    fn handles_empty_and_non_utf8_input() {
        assert_eq!(split_words(b"").count(), 0);
        assert_eq!(split_words(b"   ,,, ").count(), 0);

        // Invalid UTF-8 must not be dropped or panic.
        let words: Vec<&[u8]> = split_words(&[b'a', 0xFF, b'b']).collect();
        assert_eq!(words, vec![b"a".as_slice(), b"b".as_slice()]);
    }
}
