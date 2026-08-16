//! End-to-end job throughput, measured the way a real job pays for it.
//!
//! The microbenchmarks in `ytsaurus-yson` and `ytsaurus-skiff` parse one big
//! slice; a job instead streams a pipe, finds record boundaries, and decodes
//! row by row. This measures that job path for both formats.
//!
//! The cases are chosen to separate the costs:
//!
//! - `pass_through`  — read + find boundaries, never decode. The floor: what an
//!   identity job costs, and the share of time that is pure framing.
//! - `parse_borrowed` — decode into a struct borrowing `&str`/`&[u8]`. What a
//!   well-written job costs.
//! - `parse_owned`    — decode into `String`, forcing a copy per string column.
//! - `parse_dynamic`  — decode into `YsonValue`, allocating a whole DOM per row.
//! - `skiff_dynamic`  — decode the equivalent Skiff schema into its current
//!   dynamic `Value` representation.
//!
//! `pass_through` versus `parse_borrowed` is the answer to "how much of job cpu
//! is YSON parsing". `skiff_dynamic` is deliberately separate: Skiff currently
//! exposes dynamic rows, so comparing it to a borrowed Serde struct would hide
//! the allocation cost of its public job API.
//!
//! The `YSON vs Skiff dynamic job API` group makes the like-for-like comparison
//! explicit. Both cases decode these exact rows, then read `duration` through
//! their public dynamic representations. It reports rows/sec rather than
//! bytes/sec because the two wire encodings have different sizes.
//!
//! `YSON vs Skiff dynamic encoding` does the complementary write comparison.
//! Both cases build the same logical dynamic rows and encode them into one table
//! stream, including each format's row separator or table tag.

use std::{
    collections::BTreeMap,
    hint::black_box,
    io::{BufReader, Cursor},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use ytsaurus_job::{Event, JobReader, SkiffJobReader};
use ytsaurus_skiff::{Encoder, Format, Schema, SchemaRef, Value, WireType};
use ytsaurus_yson::{YsonNode, YsonValue, ser::Serializer};

const ROWS: usize = 100_000;
const INPUT_BUFFER_BYTES: usize = 1024 * 1024;

/// A plausible table row: a few string columns, a few numeric ones.
#[derive(Deserialize)]
#[expect(dead_code)]
struct BorrowedRow<'a> {
    #[serde(borrow)]
    user_id: &'a str,
    #[serde(borrow)]
    url: &'a str,
    #[serde(borrow)]
    referer: &'a str,
    timestamp: i64,
    duration: i64,
    is_mobile: bool,
    score: f64,
}

#[derive(Deserialize)]
#[expect(dead_code)]
struct OwnedRow {
    user_id: String,
    url: String,
    referer: String,
    timestamp: i64,
    duration: i64,
    is_mobile: bool,
    score: f64,
}

// --- hand-rolled binary YSON, so the benchmark input does not depend on the
// --- serializer whose cost we are trying to isolate.

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

fn string(s: &[u8], out: &mut Vec<u8>) {
    out.push(0x01);
    uvarint(zigzag(s.len() as i64), out);
    out.extend_from_slice(s);
}

fn int64(v: i64, out: &mut Vec<u8>) {
    out.push(0x02);
    uvarint(zigzag(v), out);
}

fn column(key: &[u8], out: &mut Vec<u8>, first: &mut bool) {
    if !*first {
        out.push(b';');
    }
    *first = false;
    string(key, out);
    out.push(b'=');
}

/// 100 000 rows, about 17.7 MiB — enough to dwarf warm-up and to cross the
/// reader's 1 MiB buffer many times over.
fn generate_yson_input() -> Vec<u8> {
    let mut out = Vec::with_capacity(32 * 1024 * 1024);
    for i in 0..ROWS {
        out.push(b'{');
        let mut first = true;

        column(b"user_id", &mut out, &mut first);
        string(format!("user-{i:08}").as_bytes(), &mut out);

        column(b"url", &mut out, &mut first);
        string(
            format!("https://example.com/page/{}/section/{}", i % 997, i % 31).as_bytes(),
            &mut out,
        );

        column(b"referer", &mut out, &mut first);
        string(
            format!("https://referer.example.org/{}", i % 613).as_bytes(),
            &mut out,
        );

        column(b"timestamp", &mut out, &mut first);
        int64(1_700_000_000 + i as i64, &mut out);

        column(b"duration", &mut out, &mut first);
        int64((i % 3600) as i64, &mut out);

        column(b"is_mobile", &mut out, &mut first);
        out.push(if i % 3 == 0 { 0x05 } else { 0x04 });

        column(b"score", &mut out, &mut first);
        out.push(0x03);
        out.extend_from_slice(&(i as f64 / 7.0).to_le_bytes());

        out.push(b'}');
        out.push(b';');
    }
    out
}

fn skiff_schema() -> Schema {
    Schema::tuple([
        Schema::named("user_id", WireType::String32),
        Schema::named("url", WireType::String32),
        Schema::named("referer", WireType::String32),
        Schema::named("timestamp", WireType::Int64),
        Schema::named("duration", WireType::Int64),
        Schema::named("is_mobile", WireType::Boolean),
        Schema::named("score", WireType::Double),
    ])
}

fn skiff_row(i: usize) -> Value {
    Value::Tuple(vec![
        Value::Bytes(format!("user-{i:08}").into_bytes()),
        Value::Bytes(
            format!("https://example.com/page/{}/section/{}", i % 997, i % 31).into_bytes(),
        ),
        Value::Bytes(format!("https://referer.example.org/{}", i % 613).into_bytes()),
        Value::Int64(1_700_000_000 + i as i64),
        Value::Int64((i % 3600) as i64),
        Value::Boolean(i.is_multiple_of(3)),
        Value::Double(i as f64 / 7.0),
    ])
}

fn yson_value(node: YsonNode) -> YsonValue {
    YsonValue {
        attributes: None,
        node,
    }
}

fn yson_dynamic_row(i: usize) -> YsonValue {
    yson_value(YsonNode::Map(BTreeMap::from([
        (
            b"user_id".to_vec(),
            yson_value(YsonNode::String(format!("user-{i:08}").into_bytes())),
        ),
        (
            b"url".to_vec(),
            yson_value(YsonNode::String(
                format!("https://example.com/page/{}/section/{}", i % 997, i % 31).into_bytes(),
            )),
        ),
        (
            b"referer".to_vec(),
            yson_value(YsonNode::String(
                format!("https://referer.example.org/{}", i % 613).into_bytes(),
            )),
        ),
        (
            b"timestamp".to_vec(),
            yson_value(YsonNode::Int64(1_700_000_000 + i as i64)),
        ),
        (
            b"duration".to_vec(),
            yson_value(YsonNode::Int64((i % 3600) as i64)),
        ),
        (
            b"is_mobile".to_vec(),
            yson_value(YsonNode::Boolean(i.is_multiple_of(3))),
        ),
        (
            b"score".to_vec(),
            yson_value(YsonNode::Double(i as f64 / 7.0)),
        ),
    ])))
}

fn encode_yson_dynamic() -> Vec<u8> {
    let mut serializer = Serializer::new(true);
    for i in 0..ROWS {
        yson_dynamic_row(i).serialize(&mut serializer).unwrap();
        serializer.output.push(b';');
    }
    serializer.output
}

fn generate_skiff_input(schema: &Schema) -> Vec<u8> {
    let mut encoder = Encoder::new(Vec::with_capacity(16 * 1024 * 1024), schema.clone()).unwrap();
    for i in 0..ROWS {
        encoder.write(&skiff_row(i)).unwrap();
    }
    encoder.into_inner().unwrap()
}

fn skiff_duration(row: &Value) -> i64 {
    match row {
        Value::Tuple(fields) => match fields.get(4) {
            Some(Value::Int64(duration)) => *duration,
            _ => unreachable!("the benchmark schema fixes duration at tuple index four"),
        },
        _ => unreachable!("the benchmark table schema has a tuple root"),
    }
}

fn yson_dynamic_duration(input: &[u8]) -> i64 {
    let mut reader = JobReader::binary(input);
    let mut total = 0i64;
    while let Some(event) = reader.next_event().unwrap() {
        if let Event::Row(row) = event {
            let value: YsonValue = row.value().unwrap();
            total += value["duration"]
                .as_i64()
                .expect("the benchmark fixture gives duration an int64 value");
        }
    }
    total
}

fn skiff_dynamic_duration(input: &[u8], format: &Format) -> i64 {
    let input = BufReader::with_capacity(INPUT_BUFFER_BYTES, Cursor::new(input));
    let mut reader = SkiffJobReader::new(input, format.clone()).unwrap();
    let mut total = 0i64;
    while let Some(row) = reader.next_row().unwrap() {
        total += skiff_duration(row.value());
    }
    total
}

fn benchmark(c: &mut Criterion) {
    let yson_input = generate_yson_input();

    let mut group = c.benchmark_group("YSON job throughput");
    group.throughput(Throughput::Bytes(yson_input.len() as u64));
    group.sample_size(20);

    // Framing only: what an identity job pays.
    group.bench_function("pass_through", |b| {
        b.iter(|| {
            let mut reader = JobReader::binary(black_box(yson_input.as_slice()));
            let mut total = 0usize;
            while let Some(event) = reader.next_event().unwrap() {
                if let Event::Row(row) = event {
                    total += row.raw().len();
                }
            }
            black_box(total)
        });
    });

    group.bench_function("parse_borrowed", |b| {
        b.iter(|| {
            let mut reader = JobReader::binary(black_box(yson_input.as_slice()));
            let mut total = 0i64;
            while let Some(event) = reader.next_event().unwrap() {
                if let Event::Row(row) = event {
                    let parsed: BorrowedRow = row.parse().unwrap();
                    total += parsed.duration;
                }
            }
            black_box(total)
        });
    });

    group.bench_function("parse_owned", |b| {
        b.iter(|| {
            let mut reader = JobReader::binary(black_box(yson_input.as_slice()));
            let mut total = 0i64;
            while let Some(event) = reader.next_event().unwrap() {
                if let Event::Row(row) = event {
                    let parsed: OwnedRow = row.parse().unwrap();
                    total += parsed.duration;
                }
            }
            black_box(total)
        });
    });

    group.bench_function("parse_dynamic", |b| {
        b.iter(|| {
            let mut reader = JobReader::binary(black_box(yson_input.as_slice()));
            let mut rows = 0usize;
            while let Some(event) = reader.next_event().unwrap() {
                if let Event::Row(row) = event {
                    let value: YsonValue = row.value().unwrap();
                    rows += usize::from(!matches!(value.node, ytsaurus_yson::YsonNode::Entity));
                }
            }
            black_box(rows)
        });
    });

    group.finish();

    let skiff_schema = skiff_schema();
    let skiff_format = Format::new(vec![SchemaRef::Inline(skiff_schema.clone())]).unwrap();
    let skiff_input = generate_skiff_input(&skiff_schema);

    let mut group = c.benchmark_group("Skiff job throughput");
    group.throughput(Throughput::Bytes(skiff_input.len() as u64));
    group.sample_size(20);
    group.bench_function("skiff_dynamic", |b| {
        b.iter(|| {
            black_box(skiff_dynamic_duration(
                black_box(skiff_input.as_slice()),
                &skiff_format,
            ))
        });
    });
    group.finish();

    // Row throughput, rather than wire-byte throughput: the same logical rows
    // are deliberately different sized byte streams in the two formats.
    let mut group = c.benchmark_group("YSON vs Skiff dynamic job API");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(20);
    group.bench_function("yson_dynamic", |b| {
        b.iter(|| black_box(yson_dynamic_duration(black_box(yson_input.as_slice()))));
    });
    group.bench_function("skiff_dynamic", |b| {
        b.iter(|| {
            black_box(skiff_dynamic_duration(
                black_box(skiff_input.as_slice()),
                &skiff_format,
            ))
        });
    });
    group.finish();

    // Both loops include dynamic row construction: a YSON map in one case and
    // a positional Skiff tuple in the other. That is the public encoding work a
    // caller does before the bytes can reach a job output stream.
    let mut group = c.benchmark_group("YSON vs Skiff dynamic encoding");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(20);
    group.bench_function("yson_dynamic", |b| {
        b.iter(|| black_box(encode_yson_dynamic()));
    });
    group.bench_function("skiff_dynamic", |b| {
        b.iter(|| black_box(generate_skiff_input(&skiff_schema)));
    });
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
