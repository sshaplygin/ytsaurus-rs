//! End-to-end test of the `sessionize` pilot, driving the real binaries.
//!
//! Runs map and reduce as the cluster does — input on fd 0, table 0 on fd 1,
//! table 1 on fd 4 — with the shuffle simulated in between. The point is to put
//! the runtime under a production-shaped load: wide mixed-type rows, non-UTF-8
//! byte columns, two output tables per phase, a reduce over a realistic key, and
//! input that is deliberately part-corrupt.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

use ytsaurus_yson::{Scan, YsonFormat, YsonNode, YsonValue, from_slice, scan::scan_value};

const MINUTE_US: i64 = 60 * 1_000_000;

/// Epoch microseconds for 2026-01-01T00:00:00Z.
///
/// Fixtures are offset from a realistic instant rather than from zero: the
/// validator rejects a non-positive timestamp, so minute 0 would be quarantined
/// and the test would be measuring the wrong thing.
const BASE_US: i64 = 1_767_225_600 * 1_000_000;

// ------------------------------------------------------------ YSON building

fn uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn s(bytes: &[u8], out: &mut Vec<u8>) {
    out.push(0x01);
    uvarint(zigzag(bytes.len() as i64), out);
    out.extend_from_slice(bytes);
}

fn i64v(v: i64, out: &mut Vec<u8>) {
    out.push(0x02);
    uvarint(zigzag(v), out);
}

fn u64v(v: u64, out: &mut Vec<u8>) {
    out.push(0x06);
    uvarint(v, out);
}

/// A column value, so rows can be built declaratively.
enum V<'a> {
    Bytes(&'a [u8]),
    Int(i64),
    Uint(u64),
    Double(f64),
    Bool(bool),
    Entity,
}

fn row(columns: &[(&[u8], V<'_>)]) -> Vec<u8> {
    let mut out = vec![b'{'];
    for (i, (key, value)) in columns.iter().enumerate() {
        if i > 0 {
            out.push(b';');
        }
        s(key, &mut out);
        out.push(b'=');
        match value {
            V::Bytes(b) => s(b, &mut out),
            V::Int(v) => i64v(*v, &mut out),
            V::Uint(v) => u64v(*v, &mut out),
            V::Double(v) => {
                out.push(0x03);
                out.extend_from_slice(&v.to_le_bytes());
            }
            V::Bool(v) => out.push(if *v { 0x05 } else { 0x04 }),
            V::Entity => out.push(b'#'),
        }
    }
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

fn key_switch() -> Vec<u8> {
    let mut out = vec![b'<'];
    s(b"key_switch", &mut out);
    out.push(b'=');
    out.push(0x05);
    out.extend_from_slice(b">#");
    out
}

fn split_records(mut data: &[u8]) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    loop {
        while data.first() == Some(&b';') {
            data = &data[1..];
        }
        if data.is_empty() {
            return records;
        }
        match scan_value(data, YsonFormat::Binary).expect("worker emitted valid YSON") {
            Scan::Complete { len } => {
                records.push(data[..len].to_vec());
                data = &data[len..];
            }
            Scan::Incomplete => panic!("worker emitted a truncated record"),
        }
    }
}

fn field<'a>(record: &'a YsonValue, key: &str) -> &'a YsonValue {
    match &record.node {
        YsonNode::Map(m) => m
            .get(key.as_bytes())
            .unwrap_or_else(|| panic!("missing column {key:?} in {:?}", m.keys())),
        other => panic!("expected a map, got {other:?}"),
    }
}

fn as_bytes(v: &YsonValue) -> Vec<u8> {
    match &v.node {
        YsonNode::String(b) => b.clone(),
        other => panic!("expected a string, got {other:?}"),
    }
}

// -------------------------------------------------------------- the harness

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "ytsaurus-rs-pilot-{}-{tag}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("create temp dir");
        Self(p)
    }
    fn join(&self, n: &str) -> PathBuf {
        self.0.join(n)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs one phase with the real descriptor layout, returning (table0, table1).
fn run_phase(mode: &str, input: &[u8], tag: &str) -> (Vec<u8>, Vec<u8>) {
    let dir = TempDir::new(tag);
    let stdin_path = dir.join("in.bin");
    let t0 = dir.join("t0.bin");
    let t1 = dir.join("t1.bin");
    std::fs::write(&stdin_path, input).expect("write input");

    let script = format!(
        "{:?} {mode} <{:?} 1>{:?} 4>{:?}",
        env!("CARGO_BIN_EXE_sessionize"),
        stdin_path.display().to_string(),
        t0.display().to_string(),
        t1.display().to_string(),
    );

    let out = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .output()
        .expect("run sessionize");

    assert!(
        out.status.success(),
        "sessionize {mode} failed: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    (
        std::fs::read(&t0).unwrap_or_default(),
        std::fs::read(&t1).unwrap_or_default(),
    )
}

/// Stands in for the cluster shuffle: sort by `user_id`, insert key switches.
fn shuffle(mapper_output: &[u8]) -> Vec<u8> {
    let mut rows: Vec<(Vec<u8>, i64, Vec<u8>)> = split_records(mapper_output)
        .into_iter()
        .map(|record| {
            let v: YsonValue = from_slice(&record, YsonFormat::Binary).expect("row parses");
            let user = as_bytes(field(&v, "user_id"));
            let ts = field(&v, "timestamp").as_i64().expect("timestamp");
            (user, ts, record)
        })
        .collect();

    // `--reduce-by user_id --sort-by user_id,timestamp`
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut out = Vec::new();
    let mut previous: Option<&[u8]> = None;
    for (user, _, record) in &rows {
        if previous.is_some_and(|p| p != user.as_slice()) {
            out.extend_from_slice(&key_switch());
            out.push(b';');
        }
        out.extend_from_slice(record);
        out.push(b';');
        previous = Some(user);
    }
    out
}

// ------------------------------------------------------------------ fixtures

/// A well-formed event. `user_agent` is deliberately not valid UTF-8.
fn event(user: &[u8], ts_minutes: i64, url: &[u8], status: i64) -> Vec<u8> {
    row(&[
        (b"user_id", V::Bytes(user)),
        (b"timestamp", V::Int(BASE_US + ts_minutes * MINUTE_US)),
        (b"url", V::Bytes(url)),
        (b"referer", V::Bytes(b"https://example.org/")),
        (b"user_agent", V::Bytes(&[b'M', b'o', 0xFF, 0xFE, b'z'])),
        (b"status", V::Int(status)),
        (b"bytes_sent", V::Uint(1024)),
        (b"is_mobile", V::Bool(false)),
        (b"latency_ms", V::Double(12.5)),
    ])
}

// --------------------------------------------------------------------- tests

#[test]
fn the_pilot_runs_map_then_reduce_end_to_end() {
    // u1: two sessions, split by a 45-minute gap. u2: one session.
    let input = fragment(&[
        event(b"u1", 0, b"/home", 200),
        event(b"u1", 5, b"/search", 200),
        event(b"u1", 10, b"/item", 404),
        event(b"u1", 55, b"/home", 200), // +45 min -> new session
        event(b"u1", 57, b"/cart", 200),
        event(b"u2", 3, b"/home", 200),
        event(b"u2", 4, b"/pay", 500),
    ]);

    let (events, rejects) = run_phase("map", &input, "map");
    assert!(rejects.is_empty(), "clean input produced rejects");
    assert_eq!(split_records(&events).len(), 7);

    let (sessions, users) = run_phase("reduce", &shuffle(&events), "reduce");

    let sessions: Vec<YsonValue> = split_records(&sessions)
        .iter()
        .map(|r| from_slice(r, YsonFormat::Binary).expect("session parses"))
        .collect();
    let users: Vec<YsonValue> = split_records(&users)
        .iter()
        .map(|r| from_slice(r, YsonFormat::Binary).expect("user parses"))
        .collect();

    // u1 -> 2 sessions, u2 -> 1.
    assert_eq!(sessions.len(), 3, "expected three sessions");
    assert_eq!(users.len(), 2, "expected two users");

    let by_user: BTreeMap<Vec<u8>, &YsonValue> = users
        .iter()
        .map(|u| (as_bytes(field(u, "user_id")), u))
        .collect();

    let u1 = by_user.get(b"u1".as_slice()).expect("u1 summary");
    assert_eq!(field(u1, "sessions").as_i64(), Some(2));
    assert_eq!(field(u1, "hits").as_i64(), Some(5));
    assert_eq!(field(u1, "errors").as_i64(), Some(1)); // the 404

    let u2 = by_user.get(b"u2".as_slice()).expect("u2 summary");
    assert_eq!(field(u2, "sessions").as_i64(), Some(1));
    assert_eq!(field(u2, "hits").as_i64(), Some(2));
    assert_eq!(field(u2, "errors").as_i64(), Some(1)); // the 500

    // The first u1 session spans 0..10 minutes and has three hits.
    let mut u1_sessions: Vec<&YsonValue> = sessions
        .iter()
        .filter(|s| as_bytes(field(s, "user_id")) == b"u1")
        .collect();
    u1_sessions.sort_by_key(|s| field(s, "started_at").as_i64());
    assert_eq!(field(u1_sessions[0], "hits").as_i64(), Some(3));
    assert_eq!(
        field(u1_sessions[0], "duration_us").as_i64(),
        Some(10 * MINUTE_US)
    );
    assert_eq!(field(u1_sessions[1], "hits").as_i64(), Some(2));
}

/// The headline requirement for a pilot: bad rows are quarantined, never fatal.
#[test]
fn malformed_rows_are_quarantined_without_failing_the_job() {
    let bad = [
        // empty user_id
        row(&[
            (b"user_id", V::Bytes(b"")),
            (b"timestamp", V::Int(MINUTE_US)),
            (b"url", V::Bytes(b"/x")),
            (b"user_agent", V::Bytes(b"a")),
            (b"status", V::Int(200)),
            (b"bytes_sent", V::Uint(1)),
            (b"is_mobile", V::Bool(false)),
            (b"latency_ms", V::Double(1.0)),
        ]),
        // status out of range
        row(&[
            (b"user_id", V::Bytes(b"u")),
            (b"timestamp", V::Int(MINUTE_US)),
            (b"url", V::Bytes(b"/x")),
            (b"user_agent", V::Bytes(b"a")),
            (b"status", V::Int(9999)),
            (b"bytes_sent", V::Uint(1)),
            (b"is_mobile", V::Bool(false)),
            (b"latency_ms", V::Double(1.0)),
        ]),
        // latency is NaN
        row(&[
            (b"user_id", V::Bytes(b"u")),
            (b"timestamp", V::Int(MINUTE_US)),
            (b"url", V::Bytes(b"/x")),
            (b"user_agent", V::Bytes(b"a")),
            (b"status", V::Int(200)),
            (b"bytes_sent", V::Uint(1)),
            (b"is_mobile", V::Bool(false)),
            (b"latency_ms", V::Double(f64::NAN)),
        ]),
        // a column is missing entirely
        row(&[
            (b"user_id", V::Bytes(b"u")),
            (b"timestamp", V::Int(MINUTE_US)),
        ]),
        // a column has the wrong type
        row(&[
            (b"user_id", V::Bytes(b"u")),
            (b"timestamp", V::Bytes(b"not-a-number")),
            (b"url", V::Bytes(b"/x")),
            (b"user_agent", V::Bytes(b"a")),
            (b"status", V::Int(200)),
            (b"bytes_sent", V::Uint(1)),
            (b"is_mobile", V::Bool(false)),
            (b"latency_ms", V::Double(1.0)),
        ]),
        // a null where a value is required
        row(&[
            (b"user_id", V::Entity),
            (b"timestamp", V::Int(MINUTE_US)),
            (b"url", V::Bytes(b"/x")),
            (b"user_agent", V::Bytes(b"a")),
            (b"status", V::Int(200)),
            (b"bytes_sent", V::Uint(1)),
            (b"is_mobile", V::Bool(false)),
            (b"latency_ms", V::Double(1.0)),
        ]),
    ];

    let mut records = vec![event(b"good", 1, b"/ok", 200)];
    records.extend_from_slice(&bad);
    records.push(event(b"good", 2, b"/ok2", 200));

    let (events, rejects) = run_phase("map", &fragment(&records), "quarantine");

    assert_eq!(split_records(&events).len(), 2, "good rows should survive");
    assert_eq!(
        split_records(&rejects).len(),
        bad.len(),
        "every bad row should be quarantined"
    );

    // Each reject must carry a reason and the original bytes.
    for record in split_records(&rejects) {
        let v: YsonValue = from_slice(&record, YsonFormat::Binary).expect("reject parses");
        let reason = as_bytes(field(&v, "reason"));
        assert!(!reason.is_empty(), "reject has no reason");
        assert!(
            !as_bytes(field(&v, "raw")).is_empty(),
            "reject lost the row"
        );
    }
}

/// Non-UTF-8 byte columns must survive both phases untouched.
#[test]
fn non_utf8_columns_survive_the_pipeline() {
    let weird_user: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
    let input = fragment(&[
        event(weird_user, 1, b"/a", 200),
        event(weird_user, 2, b"/b", 200),
    ]);

    let (events, rejects) = run_phase("map", &input, "utf8");
    assert!(rejects.is_empty());

    let (_, users) = run_phase("reduce", &shuffle(&events), "utf8-reduce");
    let users = split_records(&users);
    assert_eq!(users.len(), 1);

    let v: YsonValue = from_slice(&users[0], YsonFormat::Binary).expect("parses");
    assert_eq!(
        as_bytes(field(&v, "user_id")),
        weird_user,
        "non-UTF-8 user_id was mangled"
    );
}

#[test]
fn empty_input_produces_empty_tables() {
    let (events, rejects) = run_phase("map", b"", "empty");
    assert!(events.is_empty() && rejects.is_empty());

    let (sessions, users) = run_phase("reduce", b"", "empty-reduce");
    assert!(sessions.is_empty() && users.is_empty());
}

/// A single user generating many sessions — checks the reduce path holds up
/// past the reader's buffer and that session indices stay dense.
#[test]
fn many_sessions_for_one_user() {
    const SESSIONS: i64 = 200;

    // Each event is an hour apart, so every one starts its own session.
    let records: Vec<Vec<u8>> = (0..SESSIONS)
        .map(|i| event(b"solo", i * 60, b"/page", 200))
        .collect();

    let (events, _) = run_phase("map", &fragment(&records), "many");
    let (sessions, users) = run_phase("reduce", &shuffle(&events), "many-reduce");

    let sessions = split_records(&sessions);
    assert_eq!(sessions.len() as i64, SESSIONS);

    let indices: Vec<i64> = sessions
        .iter()
        .map(|r| {
            let v: YsonValue = from_slice(r, YsonFormat::Binary).expect("parses");
            field(&v, "session_index").as_i64().expect("index")
        })
        .collect();
    let mut sorted = indices.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        (0..SESSIONS).collect::<Vec<_>>(),
        "session indices should be dense and unique"
    );

    let users = split_records(&users);
    assert_eq!(users.len(), 1);
    let v: YsonValue = from_slice(&users[0], YsonFormat::Binary).expect("parses");
    assert_eq!(field(&v, "sessions").as_i64(), Some(SESSIONS));
}
