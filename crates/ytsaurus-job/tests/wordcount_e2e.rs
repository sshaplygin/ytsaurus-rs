//! End-to-end test of the `wordcount` worker's map and reduce phases.
//!
//! Runs the **actual compiled binary** for both phases and does the cluster's
//! job in between: sort the mapper output by the reduce key and insert a
//! `<key_switch=%true>#` record at each group boundary. That is exactly what
//! `map-reduce` with `--reduce-by word` delivers to a reducer, so the reduce
//! phase — including [`JobReader::groups`] — is exercised for real.

use std::collections::BTreeMap;
use std::io::Write;
use std::process::{Command, Stdio};

use ytsaurus_yson::{Scan, YsonFormat, YsonNode, YsonValue, from_slice, scan::scan_value, to_vec};

mod common;

// ------------------------------------------------------------ YSON helpers

fn uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn bin_string(s: &[u8], out: &mut Vec<u8>) {
    out.push(0x01);
    uvarint(((s.len() as i64) << 1) as u64, out);
    out.extend_from_slice(s);
}

/// `{text="..."}`
fn line_row(text: &[u8]) -> Vec<u8> {
    let mut out = vec![b'{'];
    bin_string(b"text", &mut out);
    out.push(b'=');
    bin_string(text, &mut out);
    out.push(b'}');
    out
}

fn fragment(records: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in records {
        out.extend_from_slice(r);
        out.push(b';');
    }
    out
}

/// `<key_switch=%true>#`
fn key_switch() -> Vec<u8> {
    let mut out = vec![b'<'];
    bin_string(b"key_switch", &mut out);
    out.push(b'=');
    out.push(0x05);
    out.extend_from_slice(b">#");
    out
}

/// Splits a list fragment into its records.
fn split_records(mut data: &[u8]) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    loop {
        while data.first() == Some(&b';') {
            data = &data[1..];
        }
        if data.is_empty() {
            return records;
        }
        match scan_value(data, YsonFormat::Binary).expect("mapper output must be valid YSON") {
            Scan::Complete { len } => {
                records.push(data[..len].to_vec());
                data = &data[len..];
            }
            Scan::Incomplete => panic!("mapper produced a truncated record"),
        }
    }
}

// ---------------------------------------------------------------- the phases

/// Runs one phase of the worker, feeding `input` on stdin and returning fd 1.
fn run_phase(mode: &str, input: &[u8]) -> Vec<u8> {
    let mut child = Command::new(common::example("wordcount"))
        .arg(mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn wordcount");

    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input)
        .expect("write input");

    let output = child.wait_with_output().expect("wait");
    assert!(
        output.status.success(),
        "wordcount {mode} failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

/// Stands in for the cluster's shuffle: sort by the reduce key, then separate
/// adjacent keys with a key-switch record.
fn shuffle(mapper_output: &[u8]) -> Vec<u8> {
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = split_records(mapper_output)
        .into_iter()
        .map(|record| {
            let value: YsonValue =
                from_slice(&record, YsonFormat::Binary).expect("mapper row parses");
            let YsonNode::Map(ref map) = value.node else {
                panic!("mapper emitted a non-map row");
            };
            let word = match &map.get(b"word".as_slice()).expect("word column").node {
                YsonNode::String(bytes) => bytes.clone(),
                other => panic!("word must be a string, got {other:?}"),
            };
            (word, record)
        })
        .collect();

    // Stable sort by key, exactly as `--reduce-by word` orders the reducer input.
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = Vec::new();
    let mut previous: Option<&[u8]> = None;
    for (word, record) in &rows {
        if previous.is_some_and(|p| p != word.as_slice()) {
            out.extend_from_slice(&key_switch());
            out.push(b';');
        }
        out.extend_from_slice(record);
        out.push(b';');
        previous = Some(word);
    }
    out
}

/// Decodes the reducer's output into a word -> count map.
fn totals(reducer_output: &[u8]) -> BTreeMap<String, i64> {
    split_records(reducer_output)
        .into_iter()
        .map(|record| {
            let value: YsonValue =
                from_slice(&record, YsonFormat::Binary).expect("reducer row parses");
            let YsonNode::Map(ref map) = value.node else {
                panic!("reducer emitted a non-map row");
            };
            let word = match &map.get(b"word".as_slice()).expect("word column").node {
                YsonNode::String(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                other => panic!("word must be a string, got {other:?}"),
            };
            let count = map
                .get(b"count".as_slice())
                .and_then(YsonValue::as_i64)
                .expect("count column");
            (word, count)
        })
        .collect()
}

/// Full pipeline: map, shuffle, reduce.
fn word_count(lines: &[&[u8]]) -> BTreeMap<String, i64> {
    let input = fragment(&lines.iter().map(|l| line_row(l)).collect::<Vec<_>>());
    let mapped = run_phase("map", &input);
    let shuffled = shuffle(&mapped);
    let reduced = run_phase("reduce", &shuffled);
    totals(&reduced)
}

fn expect(pairs: &[(&str, i64)]) -> BTreeMap<String, i64> {
    pairs.iter().map(|(w, c)| ((*w).to_string(), *c)).collect()
}

// -------------------------------------------------------------------- tests

#[test]
fn wordcount_matches_the_reference_result() {
    let got = word_count(&[
        b"the quick brown fox",
        b"jumps over the lazy dog",
        b"the fox and the dog",
        b"quick quick fox",
    ]);

    assert_eq!(
        got,
        expect(&[
            ("and", 1),
            ("brown", 1),
            ("dog", 2),
            ("fox", 3),
            ("jumps", 1),
            ("lazy", 1),
            ("over", 1),
            ("quick", 3),
            ("the", 4),
        ])
    );
}

/// Every row shares one key, so the reducer sees a single group spanning the
/// whole input — the case where a bug in group termination shows up as a
/// silently dropped tail.
#[test]
fn a_single_group_spanning_the_whole_input() {
    let got = word_count(&[b"word", b"word", b"word", b"word word"]);
    assert_eq!(got, expect(&[("word", 5)]));
}

/// The opposite extreme: every group has exactly one row.
#[test]
fn single_row_groups() {
    let got = word_count(&[b"alpha beta gamma delta"]);
    assert_eq!(
        got,
        expect(&[("alpha", 1), ("beta", 1), ("delta", 1), ("gamma", 1)])
    );
}

#[test]
fn empty_input_produces_no_output() {
    assert_eq!(word_count(&[]), BTreeMap::new());
}

/// Rows containing no words at all must not produce output or crash.
#[test]
fn input_with_no_words() {
    assert_eq!(word_count(&[b"", b"   ", b",,, ... !!!"]), BTreeMap::new());
}

/// The mapper reads `text` as bytes, so a column that is not valid UTF-8 must
/// not fail the job. A mapper insisting on `String` would abort the whole
/// operation over one bad row.
#[test]
fn non_utf8_input_is_processed() {
    let got = word_count(&[&[b'a', b'b', 0xFF, 0xFE, b'c', b'd'], b"ab"]);
    assert_eq!(got, expect(&[("ab", 2), ("cd", 1)]));
}

/// Many distinct keys, to be sure grouping holds up past the reader's buffer.
#[test]
fn many_groups() {
    const WORDS: usize = 5_000;

    let line: Vec<u8> = (0..WORDS)
        .map(|i| format!("w{i:06}"))
        .collect::<Vec<_>>()
        .join(" ")
        .into_bytes();

    let got = word_count(&[&line, &line]);

    assert_eq!(got.len(), WORDS, "expected one group per distinct word");
    assert!(
        got.values().all(|c| *c == 2),
        "every word appeared twice, so every count must be 2"
    );
}

/// The reducer must also work when the mapper's pre-aggregation produced counts
/// greater than one, which is the normal case for repeated words in a row.
#[test]
fn pre_aggregated_counts_are_summed() {
    let got = word_count(&[b"a a a a a", b"a a"]);
    assert_eq!(got, expect(&[("a", 7)]));
}

/// A reducer run with no key switches at all (the operation forgot
/// `enable_key_switch`) sees one group. This documents the failure mode rather
/// than pretending it cannot happen: everything is summed under the first word.
#[test]
fn without_key_switches_everything_collapses_into_one_group() {
    let input = fragment(&[line_row(b"alpha beta")]);
    let mapped = run_phase("map", &input);

    // Feed the mapper output straight to the reducer, with no key switches.
    let reduced = run_phase("reduce", &mapped);
    let got = totals(&reduced);

    assert_eq!(got.len(), 1, "no key switches must yield exactly one group");
    assert_eq!(got.values().sum::<i64>(), 2);
}

/// Sanity check on the harness itself: the shuffle must place exactly one key
/// switch between adjacent distinct keys, and none before the first group.
#[test]
fn the_shuffle_harness_inserts_boundaries_correctly() {
    let input = fragment(&[line_row(b"b a c")]);
    let mapped = run_phase("map", &input);
    let shuffled = shuffle(&mapped);

    let records = split_records(&shuffled);
    let switches = records
        .iter()
        .filter(|r| {
            let v: YsonValue = from_slice(r, YsonFormat::Binary).expect("parses");
            matches!(v.node, YsonNode::Entity)
        })
        .count();

    // Three distinct words -> three groups -> two boundaries.
    assert_eq!(switches, 2, "records: {records:?}");
    assert!(
        !matches!(
            from_slice::<YsonValue>(&records[0], YsonFormat::Binary)
                .expect("parses")
                .node,
            YsonNode::Entity
        ),
        "a key switch must not precede the first group"
    );
}

/// Round-tripping through `to_vec` keeps the harness honest about the format it
/// feeds the reducer.
#[test]
fn key_switch_record_matches_the_documented_encoding() {
    let encoded = key_switch();
    let value: YsonValue = from_slice(&encoded, YsonFormat::Binary).expect("parses");

    assert_eq!(value.node, YsonNode::Entity);
    assert_eq!(
        value.attr("key_switch").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );
    assert_eq!(
        to_vec(&value, YsonFormat::Binary).expect("re-encodes"),
        encoded
    );
}
