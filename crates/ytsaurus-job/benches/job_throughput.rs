//! End-to-end job throughput, measured the way a real job pays for it.
//!
//! The microbenchmark in `ytsaurus-yson` parses one big slice; a job instead
//! streams a pipe, finds record boundaries, and decodes row by row. The gap
//! between those two numbers is what the phase-5 Skiff decision turns on, so
//! this measures the job path specifically.
//!
//! The four cases are chosen to separate the costs:
//!
//! - `pass_through`  — read + find boundaries, never decode. The floor: what an
//!   identity job costs, and the share of time that is pure framing.
//! - `parse_borrowed` — decode into a struct borrowing `&str`/`&[u8]`. What a
//!   well-written job costs.
//! - `parse_owned`    — decode into `String`, forcing a copy per string column.
//! - `parse_dynamic`  — decode into `YsonValue`, allocating a whole DOM per row.
//!
//! `pass_through` versus `parse_borrowed` is the answer to "how much of job cpu
//! is YSON parsing": if the difference is small, Skiff would buy little.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::Deserialize;
use ytsaurus_job::{Event, JobReader};
use ytsaurus_yson::YsonValue;

/// A plausible table row: a few string columns, a few numeric ones.
#[derive(Deserialize)]
#[allow(dead_code)]
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
#[allow(dead_code)]
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
fn generate_input() -> Vec<u8> {
    const ROWS: usize = 100_000;

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

fn benchmark(c: &mut Criterion) {
    let input = generate_input();

    let mut group = c.benchmark_group("job throughput");
    group.throughput(Throughput::Bytes(input.len() as u64));
    group.sample_size(20);

    // Framing only: what an identity job pays.
    group.bench_function("pass_through", |b| {
        b.iter(|| {
            let mut reader = JobReader::binary(black_box(input.as_slice()));
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
            let mut reader = JobReader::binary(black_box(input.as_slice()));
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
            let mut reader = JobReader::binary(black_box(input.as_slice()));
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
            let mut reader = JobReader::binary(black_box(input.as_slice()));
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
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
