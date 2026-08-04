//! Interop tests against fixtures produced by the **Go** YSON implementation.
//!
//! These are the strongest correctness signal available offline: the bytes in
//! `fixtures/go_to_rust_binary.bin` were written by `go.ytsaurus.tech/yt/go/yson`,
//! i.e. by the same family of code that a real YTsaurus cluster runs. See
//! `fixtures/README.md` for provenance.

use std::collections::BTreeMap;
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice, to_vec};

const GO_BINARY: &[u8] = include_bytes!("fixtures/go_to_rust_binary.bin");
const GO_TEXT: &[u8] = include_bytes!("fixtures/go_to_rust_text.txt");
const RUST_BINARY: &[u8] = include_bytes!("fixtures/rust_to_go_binary.bin");
const RUST_TEXT: &[u8] = include_bytes!("fixtures/rust_to_go_text.txt");

fn parse(bytes: &[u8], format: YsonFormat) -> YsonValue {
    from_slice(bytes, format).expect("fixture must parse")
}

fn fields(value: &YsonValue) -> &BTreeMap<Vec<u8>, YsonValue> {
    match &value.node {
        YsonNode::Map(m) => m,
        other => panic!("expected a map at the top level, got {other:?}"),
    }
}

fn field<'a>(value: &'a YsonValue, key: &str) -> &'a YsonValue {
    fields(value)
        .get(key.as_bytes())
        .unwrap_or_else(|| panic!("missing key {key:?}"))
}

/// Structural equality that treats `NaN == NaN`.
///
/// `f64::NAN != f64::NAN`, so the derived `PartialEq` on `YsonValue` can never
/// report two documents containing `%nan` as equal. Every other value compares
/// bit-for-bit as usual (including `-0.0` vs `0.0`, which stay distinct).
fn eq_nan_aware(a: &YsonValue, b: &YsonValue) -> bool {
    fn attrs_eq(
        a: Option<&BTreeMap<Vec<u8>, YsonValue>>,
        b: Option<&BTreeMap<Vec<u8>, YsonValue>>,
    ) -> bool {
        match (a, b) {
            (None, None) => true,
            (Some(x), Some(y)) => {
                x.len() == y.len()
                    && x.iter()
                        .zip(y.iter())
                        .all(|((ka, va), (kb, vb))| ka == kb && eq_nan_aware(va, vb))
            }
            _ => false,
        }
    }

    if !attrs_eq(a.attributes.as_ref(), b.attributes.as_ref()) {
        return false;
    }

    match (&a.node, &b.node) {
        (YsonNode::Double(x), YsonNode::Double(y)) => {
            (x.is_nan() && y.is_nan()) || x.to_bits() == y.to_bits()
        }
        (YsonNode::List(x), YsonNode::List(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(i, j)| eq_nan_aware(i, j))
        }
        (YsonNode::Map(x), YsonNode::Map(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((ka, va), (kb, vb))| ka == kb && eq_nan_aware(va, vb))
        }
        (x, y) => x == y,
    }
}

#[test]
fn go_binary_fixture_decodes_to_expected_values() {
    let v = parse(GO_BINARY, YsonFormat::Binary);

    assert_eq!(field(&v, "int_min").node, YsonNode::Int64(i64::MIN));
    assert_eq!(
        field(&v, "int_max").node,
        YsonNode::Int64(9223372036854775806)
    );
    assert_eq!(
        field(&v, "uint_max").node,
        YsonNode::Uint64(18446744073709551614)
    );
    assert_eq!(field(&v, "int_zero").node, YsonNode::Int64(0));
    assert_eq!(field(&v, "int_neg_one").node, YsonNode::Int64(-1));

    match field(&v, "float_nan").node {
        YsonNode::Double(d) => assert!(d.is_nan(), "expected NaN, got {d}"),
        ref other => panic!("expected double, got {other:?}"),
    }
    assert_eq!(field(&v, "float_inf").node, YsonNode::Double(f64::INFINITY));
    assert_eq!(
        field(&v, "float_neg_inf").node,
        YsonNode::Double(f64::NEG_INFINITY)
    );
    assert_eq!(field(&v, "float_zero").node, YsonNode::Double(0.0));

    assert_eq!(field(&v, "empty_str").node, YsonNode::String(Vec::new()));
    assert_eq!(
        field(&v, "special_str").node,
        YsonNode::String(b"Line1\nLine2\t\0\"\\_modified".to_vec())
    );

    // The whole point of the exercise: bytes that are not valid UTF-8 must
    // survive decoding untouched.
    assert_eq!(
        field(&v, "byte_array").node,
        YsonNode::String(vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x42])
    );

    assert_eq!(field(&v, "none_val").node, YsonNode::Entity);
    assert_eq!(field(&v, "empty_map").node, YsonNode::Map(BTreeMap::new()));

    // nested_list = [[]; [1;2;3;4]; [-100]]
    match &field(&v, "nested_list").node {
        YsonNode::List(outer) => {
            assert_eq!(outer.len(), 3);
            assert_eq!(outer[0].node, YsonNode::List(Vec::new()));
            match &outer[1].node {
                YsonNode::List(inner) => {
                    let got: Vec<i64> = inner.iter().filter_map(YsonValue::as_i64).collect();
                    assert_eq!(got, vec![1, 2, 3, 4]);
                }
                other => panic!("expected list, got {other:?}"),
            }
            match &outer[2].node {
                YsonNode::List(inner) => {
                    assert_eq!(inner.len(), 1);
                    assert_eq!(inner[0].node, YsonNode::Int64(-100));
                }
                other => panic!("expected list, got {other:?}"),
            }
        }
        other => panic!("expected list, got {other:?}"),
    }

    // Attributes on a scalar: <description=...;timestamp=...>"Hello ..."
    let attributed = field(&v, "attributed_str");
    assert_eq!(
        attributed.node,
        YsonNode::String(b"Hello with attributes_from_go".to_vec())
    );
    assert_eq!(
        attributed.attr("description").and_then(YsonValue::as_str),
        Some("Just a string")
    );
    assert_eq!(
        attributed.attr("timestamp").map(|t| &t.node),
        Some(&YsonNode::Uint64(999999))
    );

    // Attributes on a list: <list_id="list-x">[1.1; 2.2]
    let attributed_list = field(&v, "attributed_list");
    assert_eq!(
        attributed_list.attr("list_id").and_then(YsonValue::as_str),
        Some("list-x")
    );
    assert_eq!(
        attributed_list.node,
        YsonNode::List(vec![
            YsonValue {
                attributes: None,
                node: YsonNode::Double(1.1)
            },
            YsonValue {
                attributes: None,
                node: YsonNode::Double(2.2)
            },
        ])
    );
}

/// Binary and text encodings of the same Go document must decode identically.
#[test]
fn go_binary_and_text_fixtures_agree() {
    let from_binary = parse(GO_BINARY, YsonFormat::Binary);
    let from_text = parse(GO_TEXT, YsonFormat::Text);
    assert!(
        eq_nan_aware(&from_binary, &from_text),
        "binary and text fixtures decoded differently"
    );
}

#[test]
fn rust_binary_and_text_fixtures_agree() {
    let from_binary = parse(RUST_BINARY, YsonFormat::Binary);
    let from_text = parse(RUST_TEXT, YsonFormat::Text);
    assert!(
        eq_nan_aware(&from_binary, &from_text),
        "binary and text fixtures decoded differently"
    );
}

/// decode -> encode -> decode must be a fixed point, in both formats, for every
/// fixture. This exercises `Serialize for YsonValue` against real-world data
/// including non-UTF-8 strings, NaN/Inf and attributes.
#[test]
fn fixtures_survive_a_reencode_round_trip() {
    let cases: [(&str, &[u8], YsonFormat); 4] = [
        ("go_to_rust_binary", GO_BINARY, YsonFormat::Binary),
        ("go_to_rust_text", GO_TEXT, YsonFormat::Text),
        ("rust_to_go_binary", RUST_BINARY, YsonFormat::Binary),
        ("rust_to_go_text", RUST_TEXT, YsonFormat::Text),
    ];

    for (name, bytes, format) in cases {
        let once = parse(bytes, format);
        let reencoded = to_vec(&once, format).unwrap_or_else(|e| panic!("{name}: encode: {e}"));
        let twice: YsonValue = from_slice(&reencoded, format)
            .unwrap_or_else(|e| panic!("{name}: re-decode: {e}\nbytes: {reencoded:?}"));
        assert!(eq_nan_aware(&once, &twice), "{name}: round trip diverged");
    }
}

/// Re-encoding to *binary* must be byte-stable: encoding the same value twice
/// gives the same bytes, and decoding those bytes gives the same value again.
#[test]
fn binary_reencoding_is_byte_stable() {
    let value = parse(GO_BINARY, YsonFormat::Binary);
    let first = to_vec(&value, YsonFormat::Binary).unwrap();
    let decoded: YsonValue = from_slice(&first, YsonFormat::Binary).unwrap();
    let second = to_vec(&decoded, YsonFormat::Binary).unwrap();
    assert_eq!(first, second, "binary encoding is not stable");
}

/// A document decoded from binary must re-encode into text and back unchanged,
/// and vice versa — the two formats are interchangeable representations.
#[test]
fn values_survive_a_format_change() {
    let from_binary = parse(GO_BINARY, YsonFormat::Binary);

    let as_text = to_vec(&from_binary, YsonFormat::Text).unwrap();
    let via_text: YsonValue = from_slice(&as_text, YsonFormat::Text).unwrap();
    assert!(
        eq_nan_aware(&from_binary, &via_text),
        "binary -> text -> value diverged"
    );

    let as_binary = to_vec(&via_text, YsonFormat::Binary).unwrap();
    let via_binary: YsonValue = from_slice(&as_binary, YsonFormat::Binary).unwrap();
    assert!(
        eq_nan_aware(&from_binary, &via_binary),
        "text -> binary -> value diverged"
    );
}
