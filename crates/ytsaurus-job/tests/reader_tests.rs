//! Reader tests, driven by the control-record stream from the YTsaurus docs.

mod common;

use common::*;
use serde::Deserialize;
use ytsaurus_job::{Event, JobError, JobReader};

/// Collects every event as an owned description, so assertions do not have to
/// juggle the borrows the streaming API hands out.
#[derive(Debug, PartialEq)]
enum Seen {
    Row {
        table_index: i64,
        row_index: Option<i64>,
        range_index: Option<i64>,
        raw: Vec<u8>,
    },
    KeySwitch,
}

fn drain<R: std::io::Read>(reader: &mut JobReader<R>) -> Result<Vec<Seen>, JobError> {
    let mut seen = Vec::new();
    while let Some(event) = reader.next_event()? {
        seen.push(match event {
            Event::Row(row) => Seen::Row {
                table_index: row.table_index,
                row_index: row.row_index,
                range_index: row.range_index,
                raw: row.raw().to_vec(),
            },
            Event::KeySwitch => Seen::KeySwitch,
        });
    }
    Ok(seen)
}

fn rows_only(seen: &[Seen]) -> Vec<&Vec<u8>> {
    seen.iter()
        .filter_map(|s| match s {
            Seen::Row { raw, .. } => Some(raw),
            Seen::KeySwitch => None,
        })
        .collect()
}

#[test]
fn reads_a_plain_row_stream() {
    let rows = vec![
        bin_row_str(b"a", b"1"),
        bin_row_str(b"a", b"2"),
        bin_row_str(b"a", b"3"),
    ];
    let input = fragment(&rows);

    let mut reader = JobReader::binary(input.as_slice());
    let seen = drain(&mut reader).expect("stream reads");

    assert_eq!(rows_only(&seen), rows.iter().collect::<Vec<_>>());
}

#[test]
fn empty_input_produces_no_events() {
    let mut reader = JobReader::binary(&[][..]);
    assert_eq!(drain(&mut reader).expect("empty is fine"), vec![]);
}

/// A fragment may or may not end with a separator; both are legal.
#[test]
fn trailing_separator_is_optional() {
    let row = bin_row_str(b"a", b"1");

    for input in [fragment(std::slice::from_ref(&row)), row.clone()] {
        let mut reader = JobReader::binary(input.as_slice());
        let seen = drain(&mut reader).expect("reads");
        assert_eq!(rows_only(&seen), vec![&row]);
    }
}

/// The exact control-record sequence from the io-configuration docs.
#[test]
fn applies_every_documented_control_record() {
    let row_a = bin_row_str(b"a", b"2");
    let row_b = bin_row_str(b"a", b"3");
    let row_c = bin_row_str(b"a", b"1");

    let input = fragment(&[
        bin_control_i64(b"table_index", 0),
        bin_control_i64(b"range_index", 0),
        bin_control_i64(b"row_index", 2),
        row_a.clone(),
        bin_control_bool(b"key_switch", true),
        row_b.clone(),
        bin_control_bool(b"key_switch", true),
        bin_control_i64(b"row_index", 0),
        row_c.clone(),
    ]);

    let mut reader = JobReader::binary(input.as_slice());
    let seen = drain(&mut reader).expect("stream reads");

    assert_eq!(
        seen,
        vec![
            Seen::Row {
                table_index: 0,
                row_index: Some(2),
                range_index: Some(0),
                raw: row_a,
            },
            Seen::KeySwitch,
            // No control record before this row: consecutive rows advance the
            // index implicitly, which is why the stream re-emits `row_index`
            // before the third row — that one is not consecutive.
            Seen::Row {
                table_index: 0,
                row_index: Some(3),
                range_index: Some(0),
                raw: row_b,
            },
            Seen::KeySwitch,
            Seen::Row {
                table_index: 0,
                row_index: Some(0),
                range_index: Some(0),
                raw: row_c,
            },
        ]
    );
}

/// A `row_index` control record numbers the *next* row; every row after it
/// advances the index by one without any further control record.
#[test]
fn row_index_advances_across_consecutive_rows() {
    let rows: Vec<_> = (0..4).map(|i| bin_row_str(b"n", &[b'0' + i])).collect();

    let input = fragment(&[
        bin_control_i64(b"row_index", 7),
        rows[0].clone(),
        rows[1].clone(),
        rows[2].clone(),
        bin_control_i64(b"row_index", 40),
        rows[3].clone(),
    ]);

    let mut reader = JobReader::binary(input.as_slice());
    let seen = drain(&mut reader).expect("reads");

    let indices: Vec<Option<i64>> = seen
        .iter()
        .filter_map(|s| match s {
            Seen::Row { row_index, .. } => Some(*row_index),
            Seen::KeySwitch => None,
        })
        .collect();
    assert_eq!(indices, vec![Some(7), Some(8), Some(9), Some(40)]);
}

/// A table switch restarts both row and range numbering; stale values from the
/// previous table must not leak onto the new table's rows.
#[test]
fn table_switch_drops_stale_row_and_range_indices() {
    let r0 = bin_row_str(b"t", b"0");
    let r1 = bin_row_str(b"t", b"1");

    let input = fragment(&[
        bin_control_i64(b"row_index", 5),
        bin_control_i64(b"range_index", 2),
        r0.clone(),
        bin_control_i64(b"table_index", 1),
        r1.clone(),
    ]);

    let mut reader = JobReader::binary(input.as_slice());
    let seen = drain(&mut reader).expect("reads");

    assert_eq!(
        seen,
        vec![
            Seen::Row {
                table_index: 0,
                row_index: Some(5),
                range_index: Some(2),
                raw: r0,
            },
            Seen::Row {
                table_index: 1,
                row_index: None,
                range_index: None,
                raw: r1,
            },
        ]
    );
}

#[test]
fn tracks_table_index_across_switches() {
    let r0 = bin_row_str(b"t", b"0");
    let r1 = bin_row_str(b"t", b"1");
    let r2 = bin_row_str(b"t", b"2");

    let input = fragment(&[
        bin_control_i64(b"table_index", 0),
        r0.clone(),
        bin_control_i64(b"table_index", 2),
        r1.clone(),
        r2.clone(),
    ]);

    let mut reader = JobReader::binary(input.as_slice());
    let seen = drain(&mut reader).expect("reads");

    let indices: Vec<i64> = seen
        .iter()
        .filter_map(|s| match s {
            Seen::Row { table_index, .. } => Some(*table_index),
            Seen::KeySwitch => None,
        })
        .collect();
    assert_eq!(indices, vec![0, 2, 2]);
}

/// `<key_switch=%false>#` is not a group boundary.
#[test]
fn false_key_switch_is_not_a_boundary() {
    let row = bin_row_str(b"a", b"1");
    let input = fragment(&[
        row.clone(),
        bin_control_bool(b"key_switch", false),
        row.clone(),
    ]);

    let mut reader = JobReader::binary(input.as_slice());
    let seen = drain(&mut reader).expect("reads");

    assert_eq!(seen.iter().filter(|s| **s == Seen::KeySwitch).count(), 0);
    assert_eq!(rows_only(&seen).len(), 2);
}

/// Unknown control attributes must be ignored, not fatal: YTsaurus may add new
/// ones, and a job built today should survive meeting them.
#[test]
fn unknown_control_records_are_ignored() {
    let row = bin_row_str(b"a", b"1");
    let input = fragment(&[bin_control_i64(b"some_future_attribute", 7), row.clone()]);

    let mut reader = JobReader::binary(input.as_slice());
    let seen = drain(&mut reader).expect("reads");
    assert_eq!(rows_only(&seen), vec![&row]);
}

/// The reader must not care how the stream is chopped up by the OS. Feeding it
/// one byte at a time exercises every possible split point, including splits in
/// the middle of a varint length prefix.
#[test]
fn records_split_across_reads_are_reassembled() {
    let rows: Vec<Vec<u8>> = (0..40)
        .map(|i| bin_row_str(format!("key_{i:03}").as_bytes(), &vec![b'v'; i * 7]))
        .collect();
    let input = fragment(&rows);

    for chunk in [1usize, 2, 3, 7, 64, 4096] {
        let mut reader =
            JobReader::binary(ChunkedReader::new(input.clone(), chunk)).with_buffer_size(64);
        let seen = drain(&mut reader).unwrap_or_else(|e| panic!("chunk {chunk}: {e}"));
        assert_eq!(
            rows_only(&seen),
            rows.iter().collect::<Vec<_>>(),
            "chunk size {chunk}"
        );
    }
}

/// A record larger than the whole buffer must grow it rather than fail.
#[test]
fn records_larger_than_the_buffer_grow_it() {
    let big = bin_row_str(b"payload", &vec![b'x'; 300_000]);
    let input = fragment(&[bin_row_str(b"a", b"1"), big.clone()]);

    let mut reader = JobReader::binary(ChunkedReader::new(input, 1024)).with_buffer_size(128);
    let seen = drain(&mut reader).expect("reads");

    assert_eq!(rows_only(&seen).len(), 2);
    assert_eq!(rows_only(&seen)[1], &big);
}

#[test]
fn interrupted_reads_are_retried() {
    let rows = vec![bin_row_str(b"a", b"1"), bin_row_str(b"a", b"2")];
    let input = fragment(&rows);

    let mut reader = JobReader::binary(InterruptingReader::new(input, 3)).with_buffer_size(64);
    let seen = drain(&mut reader).expect("EINTR must be retried, not reported");
    assert_eq!(rows_only(&seen), rows.iter().collect::<Vec<_>>());
}

/// A stream that stops mid-record is an error, not a silent short read. A job
/// that quietly processed a truncated input would produce a wrong output table.
#[test]
fn truncated_input_is_an_error() {
    let rows = vec![bin_row_str(b"a", b"1"), bin_row_str(b"key", b"value")];
    let full = fragment(&rows);

    // Cut somewhere inside the second record.
    let cut = full.len() - 4;
    let mut reader = JobReader::binary(&full[..cut]);

    let mut events = 0;
    let err = loop {
        match reader.next_event() {
            Ok(Some(_)) => events += 1,
            Ok(None) => panic!("truncated stream reported a clean end after {events} events"),
            Err(e) => break e,
        }
    };

    assert!(
        matches!(err, JobError::TruncatedRecord { .. }),
        "expected TruncatedRecord, got {err:?}"
    );
}

#[test]
fn malformed_input_is_an_error() {
    // 0x07 is not a valid binary YSON marker.
    let input = vec![0x07, 0x07];
    let mut reader = JobReader::binary(input.as_slice());

    let err = reader.next_event().expect_err("must reject");
    assert!(
        matches!(err, JobError::Yson { .. }),
        "expected Yson, got {err:?}"
    );
}

/// A corrupt length prefix must not be chased into an out-of-memory abort.
///
/// The record claims ~2 GiB and is backed by enough real bytes that the reader
/// keeps growing its buffer rather than hitting end-of-stream first, so the
/// limit — not truncation — is what stops it.
#[test]
fn absurd_record_length_hits_the_limit() {
    const LIMIT: usize = 1 << 20;

    let mut input = vec![b'{', 0x01];
    uvarint(zigzag(2 * 1024 * 1024 * 1024), &mut input);
    input.extend(std::iter::repeat_n(b'x', 2 * LIMIT));

    let mut reader = JobReader::binary(ChunkedReader::new(input, 64 * 1024))
        .with_buffer_size(64 * 1024)
        .with_max_record_bytes(LIMIT);

    let err = reader.next_event().expect_err("must refuse");
    assert!(
        matches!(err, JobError::RecordTooLarge { .. }),
        "expected RecordTooLarge, got {err:?}"
    );
}

/// A record that stops short is truncation, distinct from the limit above.
#[test]
fn short_oversized_record_reports_truncation() {
    let mut input = vec![b'{', 0x01];
    uvarint(zigzag(2 * 1024 * 1024 * 1024), &mut input);
    input.extend_from_slice(b"abc");

    let mut reader = JobReader::binary(ChunkedReader::new(input, 64))
        .with_buffer_size(64)
        .with_max_record_bytes(1 << 20);

    let err = reader.next_event().expect_err("must refuse");
    assert!(
        matches!(err, JobError::TruncatedRecord { .. }),
        "expected TruncatedRecord, got {err:?}"
    );
}

#[test]
fn rows_deserialize_into_typed_structs() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct Record<'a> {
        #[serde(borrow)]
        key: &'a str,
        count: i64,
    }

    let mut row = vec![b'{'];
    bin_string(b"key", &mut row);
    row.push(b'=');
    bin_string(b"alpha", &mut row);
    row.push(b';');
    bin_string(b"count", &mut row);
    row.push(b'=');
    bin_i64(7, &mut row);
    row.push(b'}');

    let input = fragment(&[row]);
    let mut reader = JobReader::binary(input.as_slice());

    let Some(Event::Row(r)) = reader.next_event().expect("reads") else {
        panic!("expected a row");
    };
    let parsed: Record = r.parse().expect("parses");
    assert_eq!(
        parsed,
        Record {
            key: "alpha",
            count: 7
        }
    );
}

/// Byte columns are not UTF-8. The raw bytes must reach the job intact.
#[test]
fn non_utf8_columns_survive() {
    let payload: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];
    let row = bin_row_str(b"blob", payload);
    let input = fragment(std::slice::from_ref(&row));

    let mut reader = JobReader::binary(input.as_slice());
    let Some(Event::Row(r)) = reader.next_event().expect("reads") else {
        panic!("expected a row");
    };

    assert_eq!(r.raw(), row.as_slice());

    #[derive(Deserialize)]
    struct Blob {
        #[serde(with = "serde_bytes")]
        blob: Vec<u8>,
    }
    let parsed: Blob = r.parse().expect("parses");
    assert_eq!(parsed.blob, payload);
}

#[test]
fn text_format_streams_too() {
    let input = b"{a=1};{a=2};{a=3}";
    let mut reader = JobReader::text(&input[..]).with_buffer_size(8);
    let seen = drain(&mut reader).expect("reads");
    assert_eq!(rows_only(&seen).len(), 3);
}

#[test]
fn row_offsets_advance_through_the_stream() {
    let rows = vec![bin_row_str(b"a", b"1"), bin_row_str(b"a", b"2")];
    let input = fragment(&rows);

    let mut reader = JobReader::binary(input.as_slice());

    let Some(Event::Row(first)) = reader.next_event().expect("reads") else {
        panic!("expected a row");
    };
    let first_offset = first.offset();

    let Some(Event::Row(second)) = reader.next_event().expect("reads") else {
        panic!("expected a row");
    };
    assert_eq!(first_offset, 0);
    assert_eq!(second.offset(), (rows[0].len() + 1) as u64);
}

// ------------------------------------------------------------------- groups

fn collect_groups(input: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let mut reader = JobReader::binary(input);
    let mut groups = reader.groups();
    let mut out = Vec::new();

    while let Some(mut group) = groups.next_group().expect("group reads") {
        let mut rows = Vec::new();
        while let Some(row) = group.next_row().expect("row reads") {
            rows.push(row.raw().to_vec());
        }
        out.push(rows);
    }
    out
}

#[test]
fn groups_split_on_key_switch() {
    let a = bin_row_str(b"k", b"a");
    let b = bin_row_str(b"k", b"b");
    let c = bin_row_str(b"k", b"c");

    let input = fragment(&[
        a.clone(),
        a.clone(),
        bin_control_bool(b"key_switch", true),
        b.clone(),
        bin_control_bool(b"key_switch", true),
        c.clone(),
        c.clone(),
        c.clone(),
    ]);

    assert_eq!(
        collect_groups(&input),
        vec![vec![a.clone(), a], vec![b], vec![c.clone(), c.clone(), c],]
    );
}

/// A single group spanning the whole input (no key switches at all).
#[test]
fn one_group_when_there_are_no_key_switches() {
    let row = bin_row_str(b"k", b"a");
    let input = fragment(&[row.clone(), row.clone(), row.clone()]);
    assert_eq!(
        collect_groups(&input),
        vec![vec![row.clone(), row.clone(), row]]
    );
}

#[test]
fn single_row_groups() {
    let row = bin_row_str(b"k", b"a");
    let input = fragment(&[
        row.clone(),
        bin_control_bool(b"key_switch", true),
        row.clone(),
    ]);
    assert_eq!(collect_groups(&input), vec![vec![row.clone()], vec![row]]);
}

#[test]
fn empty_input_has_no_groups() {
    assert_eq!(collect_groups(b""), Vec::<Vec<Vec<u8>>>::new());
}

/// Skipping past rows the caller did not read must not lose the group boundary.
#[test]
fn unread_rows_are_skipped_when_advancing_groups() {
    let a = bin_row_str(b"k", b"a");
    let b = bin_row_str(b"k", b"b");

    let input = fragment(&[
        a.clone(),
        a.clone(),
        a.clone(),
        bin_control_bool(b"key_switch", true),
        b.clone(),
        b.clone(),
    ]);

    let mut reader = JobReader::binary(input.as_slice());
    let mut groups = reader.groups();

    // Read only the first row of group 1, leaving two unread. The scopes end
    // each group's borrow so the next `next_group` call can proceed.
    {
        let mut group = groups.next_group().expect("reads").expect("a group");
        let first = group
            .next_row()
            .expect("reads")
            .expect("a row")
            .raw()
            .to_vec();
        assert_eq!(first, a);
    }

    // Group 2 must still start in the right place.
    {
        let mut group = groups.next_group().expect("reads").expect("a second group");
        let mut rows = Vec::new();
        while let Some(row) = group.next_row().expect("reads") {
            rows.push(row.raw().to_vec());
        }
        assert_eq!(rows, vec![b.clone(), b]);
    }

    assert!(groups.next_group().expect("reads").is_none());
}

#[test]
fn groups_work_when_split_across_reads() {
    let a = bin_row_str(b"k", b"aaaaaaaaaaaaaaaa");
    let b = bin_row_str(b"k", b"bbbbbbbbbbbbbbbb");

    let input = fragment(&[
        a.clone(),
        a.clone(),
        bin_control_bool(b"key_switch", true),
        b.clone(),
    ]);

    for chunk in [1usize, 2, 5, 17] {
        let mut reader =
            JobReader::binary(ChunkedReader::new(input.clone(), chunk)).with_buffer_size(16);
        let mut groups = reader.groups();
        let mut collected = Vec::new();
        while let Some(mut group) = groups.next_group().expect("reads") {
            let mut rows = Vec::new();
            while let Some(row) = group.next_row().expect("reads") {
                rows.push(row.raw().to_vec());
            }
            collected.push(rows);
        }
        assert_eq!(
            collected,
            vec![vec![a.clone(), a.clone()], vec![b.clone()]],
            "chunk {chunk}"
        );
    }
}

/// Control records interleaved inside a group must not end it.
#[test]
fn control_records_inside_a_group_do_not_split_it() {
    let a = bin_row_str(b"k", b"a");

    let input = fragment(&[
        bin_control_i64(b"table_index", 0),
        a.clone(),
        bin_control_i64(b"row_index", 5),
        a.clone(),
        bin_control_bool(b"key_switch", true),
        a.clone(),
    ]);

    assert_eq!(
        collect_groups(&input),
        vec![vec![a.clone(), a.clone()], vec![a]]
    );
}

// ------------------------------------------------------- reduce keys (#2)

#[test]
fn groups_by_decodes_the_reduce_key() {
    let a1 = bin_row_str(b"user", b"alice");
    let a2 = bin_row_str(b"user", b"alice");
    let b1 = bin_row_str(b"user", b"bob");

    let input = fragment(&[
        a1.clone(),
        a2.clone(),
        bin_control_bool(b"key_switch", true),
        b1.clone(),
    ]);

    let mut reader = JobReader::binary(input.as_slice());
    let mut groups = reader.groups_by(["user"]);

    let mut seen = Vec::new();
    while let Some(mut group) = groups.next_group().expect("reads") {
        let key = group.key().bytes("user").expect("key present").to_vec();
        let mut rows = 0;
        while group.next_row().expect("reads").is_some() {
            rows += 1;
        }
        seen.push((key, rows));
    }

    assert_eq!(seen, vec![(b"alice".to_vec(), 2), (b"bob".to_vec(), 1)]);
}

/// The key must survive being a byte string that is not UTF-8 — reduce keys
/// routinely are.
#[test]
fn a_non_utf8_reduce_key_is_preserved() {
    let key: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
    let input = fragment(&[bin_row_str(b"id", key)]);

    let mut reader = JobReader::binary(input.as_slice());
    let mut groups = reader.groups_by(["id"]);
    let group = groups.next_group().expect("reads").expect("a group");

    assert_eq!(group.key().bytes("id"), Some(key));
    assert_eq!(group.key().str("id"), None, "must not claim it is UTF-8");
}

#[test]
fn a_multi_column_key_keeps_every_column() {
    let mut row = vec![b'{'];
    bin_string(b"shard", &mut row);
    row.push(b'=');
    bin_i64(7, &mut row);
    row.push(b';');
    bin_string(b"user", &mut row);
    row.push(b'=');
    bin_string(b"carol", &mut row);
    row.push(b'}');

    let input = fragment(&[row]);
    let mut reader = JobReader::binary(input.as_slice());
    let mut groups = reader.groups_by(["shard", "user"]);
    let group = groups.next_group().expect("reads").expect("a group");

    assert_eq!(group.key().i64("shard"), Some(7));
    assert_eq!(group.key().str("user"), Some("carol"));
    assert_eq!(group.key().columns().len(), 2);
}

/// A key column absent from the row must not fail the job.
#[test]
fn a_missing_key_column_is_simply_absent() {
    let input = fragment(&[bin_row_str(b"other", b"x")]);
    let mut reader = JobReader::binary(input.as_slice());
    let mut groups = reader.groups_by(["user"]);
    let group = groups.next_group().expect("reads").expect("a group");

    assert!(group.key().is_empty());
    assert_eq!(group.key().bytes("user"), None);
}

/// `groups()` keeps its old behaviour: no key, no cost.
#[test]
fn plain_groups_carry_no_key() {
    let input = fragment(&[bin_row_str(b"user", b"alice")]);
    let mut reader = JobReader::binary(input.as_slice());
    let mut groups = reader.groups();
    let group = groups.next_group().expect("reads").expect("a group");
    assert!(group.key().is_empty());
}

/// Reading the key must not consume the row it was read from.
#[test]
fn reading_the_key_leaves_every_row_available() {
    let row = bin_row_str(b"user", b"alice");
    let input = fragment(&[row.clone(), row.clone(), row.clone()]);

    let mut reader = JobReader::binary(input.as_slice());
    let mut groups = reader.groups_by(["user"]);
    let mut group = groups.next_group().expect("reads").expect("a group");

    assert_eq!(group.key().str("user"), Some("alice"));

    let mut rows = 0;
    while group.next_row().expect("reads").is_some() {
        rows += 1;
    }
    assert_eq!(rows, 3, "the row the key came from must still be delivered");
}

#[test]
fn keys_survive_being_split_across_reads() {
    let a = bin_row_str(b"user", b"aaaaaaaaaaaaaaaa");
    let b = bin_row_str(b"user", b"bbbbbbbbbbbbbbbb");
    let input = fragment(&[a, bin_control_bool(b"key_switch", true), b]);

    for chunk in [1usize, 3, 17] {
        let mut reader =
            JobReader::binary(ChunkedReader::new(input.clone(), chunk)).with_buffer_size(16);
        let mut groups = reader.groups_by(["user"]);
        let mut keys = Vec::new();
        while let Some(mut group) = groups.next_group().expect("reads") {
            keys.push(group.key().bytes("user").expect("key").to_vec());
            while group.next_row().expect("reads").is_some() {}
        }
        assert_eq!(
            keys,
            vec![b"aaaaaaaaaaaaaaaa".to_vec(), b"bbbbbbbbbbbbbbbb".to_vec()],
            "chunk {chunk}"
        );
    }
}

// --------------------------------------------- error classification (#1)

/// A job that quarantines bad rows needs a cheap, stable reason and a way to
/// tell "this row is bad" from "the stream is bad".
#[test]
fn errors_classify_themselves_without_formatting() {
    // A row that is not valid YSON: bad row, keep going.
    let input = fragment(&[bin_row_str(b"a", b"1")]);
    let mut reader = JobReader::binary(input.as_slice());
    let Some(Event::Row(row)) = reader.next_event().expect("reads") else {
        panic!("expected a row");
    };

    #[derive(serde::Deserialize, Debug)]
    struct Mismatch {
        #[allow(dead_code)]
        missing_column: i64,
    }
    let err = row.parse::<Mismatch>().expect_err("must not parse");
    assert_eq!(err.kind(), "invalid_yson");
    assert!(err.is_row_local(), "a bad row must not stop the job");

    // A truncated stream: not row-local, the job must stop.
    let full = fragment(&[bin_row_str(b"key", b"value")]);
    let mut reader = JobReader::binary(&full[..full.len() - 4]);
    let err = loop {
        match reader.next_event() {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("expected an error"),
            Err(e) => break e,
        }
    };
    assert_eq!(err.kind(), "truncated_record");
    assert!(
        !err.is_row_local(),
        "a truncated stream means every later row is suspect"
    );
}

/// `kind()` must not allocate or change shape — it goes in an output column.
#[test]
fn error_kinds_are_stable_identifiers() {
    let err = JobError::RecordTooLarge {
        offset: 0,
        limit: 1,
    };
    assert_eq!(err.kind(), "record_too_large");
    assert!(
        err.kind()
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '_'),
        "a kind is written into a table; keep it a plain identifier"
    );
}
