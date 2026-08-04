//! Tests for the parts of YSON that the YTsaurus job protocol actually leans on.
//!
//! Golden byte sequences here are derived from the official documentation:
//!   - <https://ytsaurus.tech/docs/en/user-guide/storage/yson> (binary markers)
//!   - <https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration> (control records)
//!
//! Binary markers, per the spec:
//!   `0x01` string (`sint32` zigzag length + raw bytes), `0x02` int64 (`sint64`
//!   zigzag), `0x03` double (8 bytes little-endian), `0x04` false, `0x05` true,
//!   `0x06` uint64 (unsigned varint), and the literal ASCII bytes
//!   `# < > [ ] { } = ;` for entity, attributes, list, map and separators.

use ytsaurus_yson::{StreamDeserializer, YsonFormat, YsonNode, YsonValue, from_slice, to_vec};

// --- binary encoding helpers, written out by hand so the tests do not depend
// --- on the serializer they are meant to check.

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn bin_string(s: &[u8], out: &mut Vec<u8>) {
    out.push(0x01);
    uvarint(zigzag(s.len() as i64), out);
    out.extend_from_slice(s);
}

fn bin_i64(v: i64, out: &mut Vec<u8>) {
    out.push(0x02);
    uvarint(zigzag(v), out);
}

/// `<key=value>#` — the shape of every YTsaurus control record.
fn bin_control_i64(key: &[u8], value: i64) -> Vec<u8> {
    let mut out = vec![b'<'];
    bin_string(key, &mut out);
    out.push(b'=');
    bin_i64(value, &mut out);
    out.push(b'>');
    out.push(b'#');
    out
}

fn bin_control_true(key: &[u8]) -> Vec<u8> {
    let mut out = vec![b'<'];
    bin_string(key, &mut out);
    out.push(b'=');
    out.push(0x05); // %true
    out.push(b'>');
    out.push(b'#');
    out
}

fn attr_i64(value: &YsonValue, key: &str) -> Option<i64> {
    value.attr(key).and_then(YsonValue::as_i64)
}

// ---------------------------------------------------------------- golden bytes

#[test]
fn control_record_golden_bytes_match_the_spec() {
    // <table_index=0>#  — "table_index" is 11 bytes, zigzag(11) = 22 = 0x16.
    let mut expected = vec![b'<', 0x01, 0x16];
    expected.extend_from_slice(b"table_index");
    expected.extend_from_slice(&[b'=', 0x02, 0x00, b'>', b'#']);
    assert_eq!(bin_control_i64(b"table_index", 0), expected);

    // <key_switch=%true>#  — "key_switch" is 10 bytes, zigzag(10) = 20 = 0x14.
    let mut expected = vec![b'<', 0x01, 0x14];
    expected.extend_from_slice(b"key_switch");
    expected.extend_from_slice(&[b'=', 0x05, b'>', b'#']);
    assert_eq!(bin_control_true(b"key_switch"), expected);

    // <row_index=2>#  — "row_index" is 9 bytes, zigzag(9) = 18 = 0x12;
    // the value 2 encodes as zigzag(2) = 4.
    let mut expected = vec![b'<', 0x01, 0x12];
    expected.extend_from_slice(b"row_index");
    expected.extend_from_slice(&[b'=', 0x02, 0x04, b'>', b'#']);
    assert_eq!(bin_control_i64(b"row_index", 2), expected);
}

#[test]
fn each_documented_control_record_parses_in_binary() {
    for (key, value) in [("table_index", 2), ("row_index", 7), ("range_index", 0)] {
        let bytes = bin_control_i64(key.as_bytes(), value);
        let parsed: YsonValue = from_slice(&bytes, YsonFormat::Binary)
            .unwrap_or_else(|e| panic!("{key}: parse failed: {e}"));

        assert_eq!(parsed.node, YsonNode::Entity, "{key}: body must be entity");
        assert_eq!(attr_i64(&parsed, key), Some(value), "{key}: wrong value");
    }

    let bytes = bin_control_true(b"key_switch");
    let parsed: YsonValue = from_slice(&bytes, YsonFormat::Binary).expect("key_switch parses");
    assert_eq!(parsed.node, YsonNode::Entity);
    assert_eq!(
        parsed.attr("key_switch").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );
}

#[test]
fn each_documented_control_record_parses_in_text() {
    // Exactly the spellings used in the io-configuration docs.
    let cases: [(&[u8], &str, i64); 3] = [
        (br#"<"table_index"=0;>#"#, "table_index", 0),
        (br#"<"range_index"=0;>#"#, "range_index", 0),
        (br#"<"row_index"=2;>#"#, "row_index", 2),
    ];
    for (input, key, value) in cases {
        let parsed: YsonValue = from_slice(input, YsonFormat::Text)
            .unwrap_or_else(|e| panic!("{key}: parse failed: {e}"));
        assert_eq!(parsed.node, YsonNode::Entity);
        assert_eq!(attr_i64(&parsed, key), Some(value));
    }

    let parsed: YsonValue =
        from_slice(br#"<"key_switch"=%true;>#"#, YsonFormat::Text).expect("key_switch parses");
    assert_eq!(parsed.node, YsonNode::Entity);
    assert_eq!(
        parsed.attr("key_switch").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );
}

/// The complete reduce-job input stream printed in the YTsaurus docs, parsed as
/// a list fragment. This is the exact byte sequence a reducer sees on fd 0 when
/// all four control attributes are enabled.
#[test]
fn documented_reduce_stream_parses_as_a_list_fragment() {
    let input = br#"<"table_index"=0;>#;
<"range_index"=0;>#;
<"row_index"=2;>#;
{"a"="2";};
<"key_switch"=%true;>#;
{"a"="3";};
<"key_switch"=%true;>#;
<"row_index"=0;>#;
{"a"="1";};
"#;

    let mut stream = StreamDeserializer::<YsonValue>::new(input, false);
    let mut items = Vec::new();
    while let Some(item) = stream.next_item().expect("stream parses") {
        items.push(item);
    }

    assert_eq!(items.len(), 9, "expected 9 records");

    assert_eq!(attr_i64(&items[0], "table_index"), Some(0));
    assert_eq!(attr_i64(&items[1], "range_index"), Some(0));
    assert_eq!(attr_i64(&items[2], "row_index"), Some(2));

    // Data rows are plain maps with no attributes.
    for (i, expected) in [(3usize, "2"), (5, "3"), (8, "1")] {
        assert!(
            items[i].attributes.is_none(),
            "row {i} should have no attrs"
        );
        match &items[i].node {
            YsonNode::Map(m) => assert_eq!(
                m.get(b"a".as_slice()).and_then(YsonValue::as_str),
                Some(expected)
            ),
            other => panic!("row {i}: expected map, got {other:?}"),
        }
    }

    for i in [4usize, 6] {
        assert_eq!(
            items[i].attr("key_switch").map(|v| &v.node),
            Some(&YsonNode::Boolean(true)),
            "record {i} should be a key_switch"
        );
    }
    assert_eq!(attr_i64(&items[7], "row_index"), Some(0));
}

/// The same stream in binary, which is what `<format=binary>yson` actually delivers.
#[test]
fn documented_reduce_stream_parses_in_binary() {
    let mut input = Vec::new();
    let mut push = |record: Vec<u8>| {
        input.extend_from_slice(&record);
        input.push(b';');
    };

    push(bin_control_i64(b"table_index", 0));
    push(bin_control_i64(b"range_index", 0));
    push(bin_control_i64(b"row_index", 2));
    push({
        let mut row = vec![b'{'];
        bin_string(b"a", &mut row);
        row.push(b'=');
        bin_string(b"2", &mut row);
        row.push(b'}');
        row
    });
    push(bin_control_true(b"key_switch"));
    push({
        let mut row = vec![b'{'];
        bin_string(b"a", &mut row);
        row.push(b'=');
        bin_string(b"3", &mut row);
        row.push(b'}');
        row
    });

    let mut stream = StreamDeserializer::<YsonValue>::new(&input, true);
    let mut items = Vec::new();
    while let Some(item) = stream.next_item().expect("binary stream parses") {
        items.push(item);
    }

    assert_eq!(items.len(), 6);
    assert_eq!(attr_i64(&items[0], "table_index"), Some(0));
    assert_eq!(attr_i64(&items[1], "range_index"), Some(0));
    assert_eq!(attr_i64(&items[2], "row_index"), Some(2));
    assert_eq!(items[3].node, {
        let mut m = std::collections::BTreeMap::new();
        m.insert(
            b"a".to_vec(),
            YsonValue {
                attributes: None,
                node: YsonNode::String(b"2".to_vec()),
            },
        );
        YsonNode::Map(m)
    });
    assert_eq!(
        items[4].attr("key_switch").map(|v| &v.node),
        Some(&YsonNode::Boolean(true))
    );
}

/// A trailing `;` after the last record is legal (and is what YT emits).
#[test]
fn list_fragment_tolerates_a_trailing_separator() {
    for (input, binary) in [
        (b"{a=1};{a=2};".to_vec(), false),
        (b"{a=1};{a=2}".to_vec(), false),
    ] {
        let mut stream = StreamDeserializer::<YsonValue>::new(&input, binary);
        let mut n = 0;
        while stream.next_item().expect("parses").is_some() {
            n += 1;
        }
        assert_eq!(n, 2, "input {:?}", String::from_utf8_lossy(&input));
    }
}

#[test]
fn empty_input_yields_no_records() {
    let mut stream = StreamDeserializer::<YsonValue>::new(b"", true);
    assert!(
        stream
            .next_item()
            .expect("empty input is not an error")
            .is_none()
    );
}

// ------------------------------------------------------------ non-UTF-8 bytes

/// YTsaurus string columns are arbitrary byte strings, not UTF-8. Losing bytes
/// here would silently corrupt user data.
#[test]
fn non_utf8_strings_survive_binary_round_trip() {
    let payloads: [&[u8]; 5] = [
        &[0xFF],
        &[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x42],
        &[0x80, 0x81, 0x82], // continuation bytes with no lead byte
        &[0xC3],             // truncated 2-byte sequence
        &[0xED, 0xA0, 0x80], // encoded UTF-16 surrogate half
    ];

    for payload in payloads {
        let mut bytes = Vec::new();
        bin_string(payload, &mut bytes);

        let parsed: YsonValue = from_slice(&bytes, YsonFormat::Binary)
            .unwrap_or_else(|e| panic!("{payload:?}: parse failed: {e}"));
        assert_eq!(
            parsed.node,
            YsonNode::String(payload.to_vec()),
            "{payload:?}: bytes were altered"
        );

        let reencoded = to_vec(&parsed, YsonFormat::Binary).expect("re-encode");
        assert_eq!(reencoded, bytes, "{payload:?}: re-encoded bytes differ");
    }
}

#[test]
fn non_utf8_strings_survive_text_round_trip() {
    let payload: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x42];

    // Raw non-UTF-8 bytes inside a quoted text string.
    let mut raw = vec![b'"'];
    raw.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    raw.extend_from_slice(br"\0\377B");
    raw.push(b'"');
    let parsed: YsonValue = from_slice(&raw, YsonFormat::Text).expect("raw bytes parse");
    assert_eq!(parsed.node, YsonNode::String(payload.to_vec()));

    // The same value written with hex escapes.
    let escaped = br#""\xDE\xAD\xBE\xEF\x00\xFF\x42""#;
    let parsed: YsonValue = from_slice(escaped, YsonFormat::Text).expect("hex escapes parse");
    assert_eq!(parsed.node, YsonNode::String(payload.to_vec()));

    // And it must survive our own text encoder.
    let reencoded = to_vec(&parsed, YsonFormat::Text).expect("re-encode");
    let reparsed: YsonValue = from_slice(&reencoded, YsonFormat::Text).expect("re-parse");
    assert_eq!(reparsed.node, YsonNode::String(payload.to_vec()));
}

#[test]
fn non_utf8_map_keys_survive_binary_round_trip() {
    let key: &[u8] = &[0xFF, 0xFE, 0x00];

    let mut bytes = vec![b'{'];
    bin_string(key, &mut bytes);
    bytes.push(b'=');
    bin_i64(1, &mut bytes);
    bytes.push(b'}');

    let parsed: YsonValue = from_slice(&bytes, YsonFormat::Binary).expect("parses");
    match &parsed.node {
        YsonNode::Map(m) => {
            assert!(m.contains_key(key), "non-UTF-8 key was mangled: {m:?}");
        }
        other => panic!("expected map, got {other:?}"),
    }

    let reencoded = to_vec(&parsed, YsonFormat::Binary).expect("re-encode");
    assert_eq!(reencoded, bytes, "non-UTF-8 key changed on re-encode");
}

#[test]
fn non_utf8_attribute_names_survive_binary_round_trip() {
    let name: &[u8] = &[0xFF, 0xFE];

    // <\xFF\xFE=1>#
    let mut bytes = vec![b'<'];
    bin_string(name, &mut bytes);
    bytes.push(b'=');
    bin_i64(1, &mut bytes);
    bytes.push(b'>');
    bytes.push(b'#');

    let parsed: YsonValue = from_slice(&bytes, YsonFormat::Binary).expect("parses");
    assert_eq!(parsed.node, YsonNode::Entity);

    let attrs = parsed.attributes.as_ref().expect("attributes present");
    assert!(
        attrs.contains_key(name),
        "non-UTF-8 attribute name was mangled: {attrs:?}"
    );
    assert_eq!(attrs.get(name).and_then(YsonValue::as_i64), Some(1));

    let reencoded = to_vec(&parsed, YsonFormat::Binary).expect("re-encode");
    assert_eq!(reencoded, bytes, "non-UTF-8 attribute name changed");
}

// ----------------------------------------------------------- large payloads

/// Length prefixes are `sint32` zigzag varints, so a string larger than the
/// single-byte / two-byte varint ranges exercises multi-byte length decoding.
/// 64 MiB is the size called out as a risk in the research doc.
#[test]
fn strings_larger_than_64_mib_round_trip() {
    const SIZE: usize = 64 * 1024 * 1024 + 1;

    let payload = vec![b'x'; SIZE];
    let mut bytes = Vec::with_capacity(SIZE + 16);
    bin_string(&payload, &mut bytes);

    // zigzag(67108865) = 134217730, which is under 2^28 and so needs a 4-byte
    // varint — well past the single-byte range the small tests cover.
    let mut len_bytes = Vec::new();
    uvarint(zigzag(SIZE as i64), &mut len_bytes);
    assert_eq!(len_bytes.len(), 4, "expected a 4-byte length varint");

    let parsed: YsonValue = from_slice(&bytes, YsonFormat::Binary).expect("large string parses");
    match &parsed.node {
        YsonNode::String(s) => {
            assert_eq!(s.len(), SIZE);
            assert!(s.iter().all(|&b| b == b'x'));
        }
        other => panic!("expected string, got {other:?}"),
    }

    let reencoded = to_vec(&parsed, YsonFormat::Binary).expect("re-encode");
    assert_eq!(reencoded, bytes, "large string changed on re-encode");
}

/// A row wider than the read buffer used by the job runtime, with many columns.
#[test]
fn wide_rows_round_trip() {
    const COLUMNS: usize = 10_000;

    let mut bytes = vec![b'{'];
    for i in 0..COLUMNS {
        if i > 0 {
            bytes.push(b';');
        }
        bin_string(format!("column_{i:05}").as_bytes(), &mut bytes);
        bytes.push(b'=');
        bin_i64(i as i64, &mut bytes);
    }
    bytes.push(b'}');

    let parsed: YsonValue = from_slice(&bytes, YsonFormat::Binary).expect("wide row parses");
    match &parsed.node {
        YsonNode::Map(m) => {
            assert_eq!(m.len(), COLUMNS);
            assert_eq!(
                m.get(b"column_09999".as_slice())
                    .and_then(YsonValue::as_i64),
                Some(9999)
            );
        }
        other => panic!("expected map, got {other:?}"),
    }
}

// ------------------------------------------------------------ malformed input

/// The parser must reject rather than panic on damaged input; a job that reads
/// a truncated stream should fail cleanly.
#[test]
fn malformed_binary_input_errors_without_panicking() {
    let cases: Vec<Vec<u8>> = vec![
        vec![0x01],                         // string marker, no length
        vec![0x01, 0x10],                   // length 8, no data
        vec![0x02],                         // int marker, no varint
        vec![0x03, 0x00],                   // double marker, short payload
        vec![0x06, 0x80],                   // unterminated varint
        vec![b'{'],                         // unterminated map
        vec![b'['],                         // unterminated list
        vec![b'<'],                         // unterminated attributes
        vec![0x07],                         // undefined marker
        vec![0xFF, 0xFF],                   // garbage
        vec![b'{', 0x01, 0x02, b'a', b'='], // key with no value
    ];

    for case in cases {
        let result: Result<YsonValue, _> = from_slice(&case, YsonFormat::Binary);
        assert!(
            result.is_err(),
            "expected an error for {case:?}, got {:?}",
            result.ok()
        );
    }
}

/// Regression: a `/` that opens neither a `//` nor a `/*` comment used to spin
/// forever in `skip_ignored`, because no branch advanced the cursor. Any text
/// input containing such a byte would hang the parser rather than fail it.
///
/// Guarded by a worker thread so a regression fails the test instead of hanging CI.
#[test]
fn stray_slash_in_text_input_errors_instead_of_hanging() {
    use std::{sync::mpsc, thread, time::Duration};

    let cases: [&[u8]; 6] = [
        b"/a",
        b"/",
        b"/x{a=1}",
        b"{a=/b}",
        b"[/]",
        b"/*unterminated",
    ];

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for case in cases {
            let _: Result<YsonValue, _> = from_slice(case, YsonFormat::Text);
        }
        let _ = tx.send(());
    });

    assert!(
        rx.recv_timeout(Duration::from_secs(10)).is_ok(),
        "parser hung on a stray '/' in text input"
    );
}

/// Comments are still skipped correctly after the fix above.
#[test]
fn text_comments_are_still_skipped() {
    let cases: [(&[u8], i64); 4] = [
        (b"// leading comment\n42", 42),
        (b"/* block */ 42", 42),
        (b"42 // trailing", 42),
        (b"/* a */ /* b */ 42", 42),
    ];
    for (input, expected) in cases {
        let parsed: YsonValue = from_slice(input, YsonFormat::Text)
            .unwrap_or_else(|e| panic!("{:?}: {e}", String::from_utf8_lossy(input)));
        assert_eq!(parsed.node, YsonNode::Int64(expected));
    }
}

/// Deeply nested input must hit the recursion limit instead of blowing the stack.
#[test]
fn deeply_nested_input_is_rejected() {
    let depth = 10_000;
    let mut bytes = vec![b'['; depth];
    bytes.extend(std::iter::repeat_n(b']', depth));

    let result: Result<YsonValue, _> = from_slice(&bytes, YsonFormat::Binary);
    assert!(result.is_err(), "deep nesting should be rejected");
}
