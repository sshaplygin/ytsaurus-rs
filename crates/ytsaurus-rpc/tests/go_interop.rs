//! The row wire format and CRC-64, checked against the Go SDK's bytes.
//!
//! The vectors in `tests/rpc-go-interop/` are **produced** by the reference
//! implementation, not written by hand: a Go program pinned to yt/go v0.0.33
//! encodes each rowset and dumps the bytes. That is the same arrangement
//! `tests/skiff-go-interop/` uses, and it exists because a binary format
//! checked only against our own reading of the specification is checked
//! against itself.
//!
//! Regenerate them with `cd tests/rpc-go-interop && go test ./...`.
//!
//! Both directions are checked for each vector: our encoder must produce the
//! reference bytes, and our decoder must read them back to the values they
//! encode. Encoding alone would miss a decoder that is wrong in the same way.

use std::path::PathBuf;

use bytes::Bytes;
use ytsaurus_rpc::crc64;
use ytsaurus_rpc::wire::{self, MaybeRow, UnversionedValue, Value};

fn vectors_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/rpc-go-interop")
}

/// Reads one `.hex` vector, ignoring the `#` comment lines the generator writes.
fn read_vector(name: &str) -> Bytes {
    let path = vectors_directory().join(format!("{name}.hex"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}\nRun `cd tests/rpc-go-interop && go test ./...`",
            path.display()
        )
    });

    let mut bytes = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        for pair in line.as_bytes().chunks(2) {
            let pair = std::str::from_utf8(pair).unwrap();
            bytes.push(u8::from_str_radix(pair, 16).expect("hex digits"));
        }
    }
    Bytes::from(bytes)
}

fn string(id: u16, text: &str) -> UnversionedValue {
    UnversionedValue::new(id, Value::String(Bytes::copy_from_slice(text.as_bytes())))
}

/// The rowsets in `go_test.go`, rebuilt with this crate's types.
///
/// Kept in the same order and under the same names, so a mismatch names the Go
/// case that disagrees.
fn cases() -> Vec<(&'static str, Vec<MaybeRow>)> {
    vec![
        ("rowset_empty", vec![]),
        ("rowset_null_and_empty_rows", vec![None, Some(vec![])]),
        (
            "rowset_scalars",
            vec![Some(vec![
                UnversionedValue::new(0, Value::Null),
                UnversionedValue::new(1, Value::Boolean(true)),
                UnversionedValue::new(2, Value::Boolean(false)),
                UnversionedValue::new(3, Value::Int64(-42)),
                UnversionedValue::new(4, Value::Uint64(42)),
                UnversionedValue::new(5, Value::Double(1.25)),
            ])],
        ),
        (
            "rowset_extremes",
            vec![Some(vec![
                UnversionedValue::new(0, Value::Int64(i64::MIN)),
                UnversionedValue::new(1, Value::Int64(i64::MAX)),
                UnversionedValue::new(2, Value::Uint64(u64::MAX)),
                UnversionedValue::new(3, Value::Double(0.0)),
                UnversionedValue::new(4, Value::Double(-0.0)),
            ])],
        ),
        (
            "rowset_strings",
            vec![Some(vec![
                string(0, ""),
                string(1, "a"),
                string(2, "ab"),
                string(3, "abc"),
                string(4, "abcd"),
                string(5, "abcde"),
                string(6, "abcdef"),
                string(7, "abcdefg"),
                string(8, "abcdefgh"),
                string(9, "abcdefghi"),
            ])],
        ),
        (
            "rowset_any",
            vec![Some(vec![
                string(0, "key"),
                UnversionedValue::new(1, Value::Any(Bytes::from_static(b"[1;2;3]"))),
            ])],
        ),
        (
            "rowset_many_rows",
            vec![
                Some(vec![
                    UnversionedValue::new(0, Value::Int64(1)),
                    string(1, "one"),
                ]),
                Some(vec![
                    UnversionedValue::new(0, Value::Int64(2)),
                    string(1, "two"),
                ]),
                None,
                Some(vec![
                    UnversionedValue::new(0, Value::Int64(3)),
                    string(1, "three"),
                ]),
            ],
        ),
        (
            "rowset_non_utf8",
            vec![Some(vec![UnversionedValue::new(
                0,
                Value::String(Bytes::from_static(&[0xff, 0xfe, 0x00, 0x80])),
            )])],
        ),
    ]
}

#[test]
fn the_encoder_reproduces_the_go_sdk_bytes() {
    for (name, rows) in cases() {
        let expected = read_vector(name);
        let encoded = wire::encode_rowset(&rows).expect("the reference cases are all valid");
        assert_eq!(
            encoded,
            expected,
            "{name}: this encoder disagrees with the Go SDK\n  ours: {}\n  theirs: {}",
            hex(&encoded),
            hex(&expected)
        );
    }
}

#[test]
fn the_decoder_reads_the_go_sdk_bytes() {
    for (name, rows) in cases() {
        let bytes = read_vector(name);
        let decoded = wire::decode_rowset(&bytes)
            .unwrap_or_else(|error| panic!("{name}: could not decode the Go SDK's bytes: {error}"));
        assert_eq!(decoded, rows, "{name}: decoded to the wrong values");
    }
}

/// The null row must survive a trip through the reference bytes as a null row.
///
/// It is the one distinction a lookup depends on — a key with no row — and the
/// easiest to flatten into an empty row by accident.
#[test]
fn the_null_row_survives_the_reference_bytes() {
    let decoded = wire::decode_rowset(&read_vector("rowset_null_and_empty_rows")).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0], None, "the first row is null in the Go vector");
    assert_eq!(
        decoded[1],
        Some(vec![]),
        "the second row is present but empty"
    );
}

#[test]
fn checksums_match_the_go_sdk_over_bus_shaped_inputs() {
    let path = vectors_directory().join("crc64_vectors.txt");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

    let mut checked = 0;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split(' ');
        let name = fields.next().expect("a name");
        let input_hex = fields.next().expect("an input");
        let expected_hex = fields.next().expect("a checksum");

        let input: Vec<u8> = (0..input_hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&input_hex[at..at + 2], 16).unwrap())
            .collect();
        let expected = u64::from_str_radix(expected_hex, 16).unwrap();

        assert_eq!(
            crc64::checksum(&input),
            expected,
            "{name}: checksum disagrees with the Go SDK"
        );
        checked += 1;
    }

    assert!(
        checked >= 5,
        "expected the generated vectors, found {checked}"
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
