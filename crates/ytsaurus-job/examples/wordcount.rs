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
//!
//! ## `map` and `map-combine`
//!
//! There are two mappers, and the difference between them is worth more than
//! the code in it. `map` sums within a **row**; `map-combine` sums within the
//! **job**, which is what a combiner is.
//!
//! Measured on the local cluster over 16 MiB of text — `cargo run -p
//! ytsaurus-client --example format_compare` — `map` sent **3 114 964 rows**
//! into the shuffle where the same computation in YQL sent **3 750**, because
//! YQL's planner puts a combiner in its map stage as a matter of course. At
//! that size the query finished in about **1.8×** less summed job exec time;
//! on wall clock the two were indistinguishable, and at 1 MiB the plain `map`
//! was already 2.5× *faster* than the query. The direction is a property of
//! the input size, not of the two implementations.
//!
//! With `map-combine` the worker's total exec falls 8432 → 2787 ms against the
//! query's 4650. Where the 5645 ms went: 1294 ms is the mapper no longer
//! encoding 3.1 M output rows, 4351 ms is the reducer no longer reading 39.7
//! MiB of them back. **This is a result about plan shape**, and the same is
//! true of the gap it closes — neither number says anything about the wire
//! format or the language, and both are dominated by per-job process startup
//! (~640 ms per job here, against 150–500 ms of actual wordcount).
//!
//! The reason it was not written this way to begin with is visible in the
//! signature: `map`'s keys are `&[u8]` borrowed from the reader's buffer, and
//! a borrow cannot outlive the row it points into — which is the zero-copy
//! design working exactly as intended, and also the reason a mapper cannot
//! accumulate across rows without owning its keys. `map-combine` pays one
//! allocation per **distinct** word rather than one per occurrence, and bounds
//! the map so a high-cardinality input cannot exhaust the job's memory.

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
        "map-combine" => ytsaurus_job::run(map_combine),
        "reduce" => ytsaurus_job::run(reduce),
        other => {
            eprintln!("usage: wordcount <map|map-combine|reduce>   (got {other:?})");
            std::process::exit(2);
        }
    }
}

/// Splits each line into words and emits `{word=...; count=1}`.
///
/// Counts are pre-aggregated per row. On short lines that is worth almost
/// nothing — on the measured corpus 3 126 408 words became 3 114 964 shuffle
/// rows, a **0.4 %** reduction, because a word rarely repeats inside one line.
/// That is why [`map_combine`] exists.
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

/// How many distinct words one job accumulates before flushing.
///
/// A combiner has to be bounded, because the number of distinct keys is a
/// property of the data rather than of the job: an input of unique words would
/// otherwise grow this map until the job is killed for exceeding its memory
/// limit. Flushing early costs nothing but a partially summed key, which the
/// reducer sums again anyway — that is exactly what makes a combiner safe to
/// interrupt.
const COMBINE_LIMIT: usize = 1 << 20;

/// Splits each line into words and sums them **within the job**.
///
/// The difference from [`map`] is one `HashMap` that outlives the row, which
/// is why its keys are owned. Measured, the shuffle carried **277× fewer
/// rows** — 11 250 against 3 114 964. That 11 250 is 3 map jobs × the corpus's
/// 3 750 distinct words, each job flushing the whole vocabulary; YQL reached
/// 3 750 because its map ran as a single job, so the remaining factor of 3 is
/// job count rather than combining.
///
/// The corpus is also the friendliest case a combiner can have: its vocabulary
/// is small enough that every job sees every word, and it does not grow with
/// the input. On a real corpus, where distinct words keep appearing, the
/// saving is smaller and the flush below starts to matter.
fn map_combine() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(1)?;
    let mut counts: HashMap<Vec<u8>, i64> = HashMap::new();

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };
        let line: Line = row.parse()?;

        for word in split_words(line.text) {
            // Looked up by the borrowed key and only allocated when the word
            // is new: one allocation per distinct word, not per occurrence.
            match counts.get_mut(word) {
                Some(count) => *count += 1,
                None => {
                    counts.insert(word.to_vec(), 1);
                }
            }
        }

        if counts.len() >= COMBINE_LIMIT {
            flush(&mut writer, &mut counts)?;
        }
    }

    flush(&mut writer, &mut counts)?;
    writer.finish()
}

/// Emits what has been summed so far and empties the map.
fn flush(writer: &mut JobWriter, counts: &mut HashMap<Vec<u8>, i64>) -> Result<(), JobError> {
    for (word, count) in counts.drain() {
        writer.write(0, &WordCount { word: &word, count })?;
    }
    Ok(())
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
