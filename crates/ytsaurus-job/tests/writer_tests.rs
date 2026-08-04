//! Writer tests: descriptor routing, table switches and round trips.

mod common;

use common::*;
use serde::{Deserialize, Serialize};
use ytsaurus_job::{Event, JobError, JobReader, JobWriter, table_descriptor, yson::YsonFormat};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Row {
    key: String,
    count: i64,
}

/// Builds a writer over in-memory sinks, returning the handles to inspect.
fn writer_with_tables(count: usize, format: YsonFormat) -> (JobWriter, Vec<SharedBuffer>) {
    let buffers: Vec<SharedBuffer> = (0..count).map(|_| SharedBuffer::new()).collect();
    let sinks: Vec<Box<dyn std::io::Write>> = buffers
        .iter()
        .map(|b| Box::new(b.clone()) as Box<dyn std::io::Write>)
        .collect();
    (JobWriter::from_writers(sinks, format), buffers)
}

/// The descriptor numbering rule from the table-switch docs: table k is fd 3k+1.
#[test]
fn descriptor_numbering_follows_the_documented_rule() {
    assert_eq!(table_descriptor(0), 1);
    assert_eq!(table_descriptor(1), 4);
    assert_eq!(table_descriptor(2), 7);
    assert_eq!(table_descriptor(3), 10);
}

#[test]
fn rows_go_to_the_right_table() {
    let (mut writer, buffers) = writer_with_tables(3, YsonFormat::Binary);

    writer
        .write(
            1,
            &Row {
                key: "b".into(),
                count: 2,
            },
        )
        .expect("writes");
    writer
        .write(
            0,
            &Row {
                key: "a".into(),
                count: 1,
            },
        )
        .expect("writes");
    writer.finish().expect("flushes");

    assert!(!buffers[0].contents().is_empty(), "table 0 got no rows");
    assert!(!buffers[1].contents().is_empty(), "table 1 got no rows");
    assert!(
        buffers[2].contents().is_empty(),
        "table 2 should have stayed empty"
    );
}

#[test]
fn writing_to_a_missing_table_is_an_error() {
    let (mut writer, _buffers) = writer_with_tables(2, YsonFormat::Binary);

    let err = writer
        .write(
            5,
            &Row {
                key: "x".into(),
                count: 0,
            },
        )
        .expect_err("must reject");

    assert!(
        matches!(err, JobError::UnknownTable { index: 5, count: 2 }),
        "got {err:?}"
    );
    writer.finish().expect("flushes");
}

/// Everything the writer emits must read back through the reader unchanged.
#[test]
fn written_rows_round_trip_through_the_reader() {
    let rows = vec![
        Row {
            key: "alpha".into(),
            count: 1,
        },
        Row {
            key: "beta".into(),
            count: -2,
        },
        Row {
            key: String::new(),
            count: i64::MAX,
        },
    ];

    let (mut writer, buffers) = writer_with_tables(1, YsonFormat::Binary);
    for row in &rows {
        writer.write(0, row).expect("writes");
    }
    writer.finish().expect("flushes");

    let encoded = buffers[0].contents();
    let mut reader = JobReader::binary(encoded.as_slice());

    let mut decoded = Vec::new();
    while let Some(event) = reader.next_event().expect("reads") {
        if let Event::Row(row) = event {
            decoded.push(row.parse::<Row>().expect("parses"));
        }
    }

    assert_eq!(decoded, rows);
}

/// `write_raw` must reproduce input bytes exactly — this is what makes an
/// identity job byte-faithful, which decode-then-re-encode is not.
#[test]
fn raw_rows_are_forwarded_byte_for_byte() {
    let originals = vec![
        bin_row_str(b"a", b"1"),
        bin_row_str(b"blob", &[0xDE, 0xAD, 0xBE, 0xEF]),
        bin_row_i64(b"n", -42),
    ];
    let input = fragment(&originals);

    let (mut writer, buffers) = writer_with_tables(1, YsonFormat::Binary);
    let mut reader = JobReader::binary(input.as_slice());
    while let Some(event) = reader.next_event().expect("reads") {
        if let Event::Row(row) = event {
            writer.write_raw(0, row.raw()).expect("writes");
        }
    }
    writer.finish().expect("flushes");

    assert_eq!(buffers[0].contents(), input);
}

/// In switch mode every table shares one descriptor, and a `<table_index=N>#`
/// record marks each change of destination — the mechanism the table-switch docs
/// describe.
#[test]
fn table_switches_are_emitted_only_when_the_destination_changes() {
    let buffer = SharedBuffer::new();
    let mut writer =
        JobWriter::from_writer_with_switches(Box::new(buffer.clone()), 3, YsonFormat::Text);

    let row = |n: i64| Row {
        key: format!("r{n}"),
        count: n,
    };

    writer.write(0, &row(0)).expect("writes");
    writer.write(0, &row(1)).expect("writes"); // same table: no switch
    writer.write(2, &row(2)).expect("writes"); // switch to 2
    writer.write(2, &row(3)).expect("writes"); // same table: no switch
    writer.write(0, &row(4)).expect("writes"); // switch back to 0
    writer.finish().expect("flushes");

    let text = String::from_utf8(buffer.contents()).expect("UTF-8");

    // Table 0 is the initial destination, so the first write needs no switch.
    assert_eq!(
        text,
        "{key=r0;count=0};{key=r1;count=1};\
         <table_index=2>#;{key=r2;count=2};{key=r3;count=3};\
         <table_index=0>#;{key=r4;count=4};",
        "unexpected switch placement"
    );
}

/// The same routing in binary, read back through the reader: rows must land in
/// the tables they were addressed to.
#[test]
fn binary_table_switches_route_rows_correctly() {
    let buffer = SharedBuffer::new();
    let mut writer =
        JobWriter::from_writer_with_switches(Box::new(buffer.clone()), 2, YsonFormat::Binary);

    writer
        .write(
            1,
            &Row {
                key: "to-one".into(),
                count: 1,
            },
        )
        .expect("writes");
    writer
        .write(
            0,
            &Row {
                key: "to-zero".into(),
                count: 0,
            },
        )
        .expect("writes");
    writer.finish().expect("flushes");

    // Reading the stream back applies the switches, so each row reports the
    // table it was addressed to.
    let encoded = buffer.contents();
    let mut reader = JobReader::binary(encoded.as_slice());

    let mut routed = Vec::new();
    while let Some(event) = reader.next_event().expect("reads") {
        if let Event::Row(row) = event {
            routed.push((row.table_index, row.parse::<Row>().expect("parses").key));
        }
    }

    assert_eq!(
        routed,
        vec![(1, "to-one".to_string()), (0, "to-zero".to_string())]
    );
}

#[test]
fn text_format_output_is_readable() {
    let (mut writer, buffers) = writer_with_tables(1, YsonFormat::Text);
    writer
        .write(
            0,
            &Row {
                key: "hello".into(),
                count: 7,
            },
        )
        .expect("writes");
    writer.finish().expect("flushes");

    let text = String::from_utf8(buffers[0].contents()).expect("UTF-8");
    assert_eq!(text, "{key=hello;count=7};");
}

/// Byte columns must survive the writer as well as the reader.
#[test]
fn non_utf8_columns_round_trip_through_the_writer() {
    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Blob {
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    }

    let original = Blob {
        data: vec![0x00, 0xFF, 0xDE, 0xAD, 0x80],
    };

    let (mut writer, buffers) = writer_with_tables(1, YsonFormat::Binary);
    writer.write(0, &original).expect("writes");
    writer.finish().expect("flushes");

    let encoded = buffers[0].contents();
    let mut reader = JobReader::binary(encoded.as_slice());
    let Some(Event::Row(row)) = reader.next_event().expect("reads") else {
        panic!("expected a row");
    };
    assert_eq!(row.parse::<Blob>().expect("parses"), original);
}

/// Many rows must not be lost to buffering.
#[test]
fn every_row_survives_buffering() {
    const COUNT: i64 = 50_000;

    let (mut writer, buffers) = writer_with_tables(1, YsonFormat::Binary);
    for i in 0..COUNT {
        writer
            .write(
                0,
                &Row {
                    key: format!("key_{i}"),
                    count: i,
                },
            )
            .expect("writes");
    }
    writer.finish().expect("flushes");

    let encoded = buffers[0].contents();
    let mut reader = JobReader::binary(encoded.as_slice());
    let mut seen = 0i64;
    while let Some(event) = reader.next_event().expect("reads") {
        if let Event::Row(row) = event {
            let parsed: Row = row.parse().expect("parses");
            assert_eq!(parsed.count, seen);
            seen += 1;
        }
    }
    assert_eq!(seen, COUNT);
}

/// A writer that is dropped without `finish` still flushes, so a job that
/// forgets does not silently truncate its output table.
///
/// The sink is wrapped in a `BufWriter` here so there really is buffered data to
/// lose — exactly the arrangement `JobWriter::descriptors` sets up.
#[test]
fn dropping_without_finish_still_flushes() {
    let buffer = SharedBuffer::new();
    {
        let buffered = std::io::BufWriter::with_capacity(64 * 1024, buffer.clone());
        let mut writer = JobWriter::from_writers(vec![Box::new(buffered)], YsonFormat::Text);
        writer
            .write(
                0,
                &Row {
                    key: "a".into(),
                    count: 1,
                },
            )
            .expect("writes");
        assert!(
            buffer.contents().is_empty(),
            "test is not exercising buffering: bytes arrived before the flush"
        );
        // no finish() — the Drop impl must still push bytes through
    }
    assert_eq!(
        String::from_utf8(buffer.contents()).expect("UTF-8"),
        "{key=a;count=1};",
        "dropping the writer lost buffered output"
    );
}

/// `finish` is the supported path and must flush everything.
#[test]
fn finish_flushes_buffered_output() {
    let buffer = SharedBuffer::new();
    let buffered = std::io::BufWriter::with_capacity(64 * 1024, buffer.clone());
    let mut writer = JobWriter::from_writers(vec![Box::new(buffered)], YsonFormat::Text);

    writer
        .write(
            0,
            &Row {
                key: "a".into(),
                count: 1,
            },
        )
        .expect("writes");
    assert!(buffer.contents().is_empty(), "flushed too early");

    writer.finish().expect("flushes");
    assert_eq!(
        String::from_utf8(buffer.contents()).expect("UTF-8"),
        "{key=a;count=1};"
    );
}
