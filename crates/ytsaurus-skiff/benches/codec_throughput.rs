//! Skiff codec throughput on the same seven-column rows as the job benchmark.
//!
//! `decode_dynamic` reflects the public `Value` API. `validate_and_skip` is
//! intentionally separate: it parses exactly the same framing and schema, but
//! does not construct a value tree. The difference is the cost of dynamic-row
//! allocation, not an imaginary zero-cost Skiff decoder.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ytsaurus_skiff::{Decoder, Encoder, Format, Schema, SchemaRef, Value, WireType};

const ROWS: usize = 100_000;

fn schema() -> Schema {
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

fn row(i: usize) -> Value {
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

fn rows() -> Vec<Value> {
    (0..ROWS).map(row).collect()
}

fn encode_rows(schema: &Schema, rows: &[Value]) -> Vec<u8> {
    let mut encoder = Encoder::new(Vec::with_capacity(16 * 1024 * 1024), schema.clone()).unwrap();
    for row in rows {
        encoder.write(row).unwrap();
    }
    encoder.into_inner().unwrap()
}

fn duration(row: &Value) -> i64 {
    match row {
        Value::Tuple(fields) => match fields.get(4) {
            Some(Value::Int64(duration)) => *duration,
            _ => unreachable!("the benchmark schema fixes duration at tuple index four"),
        },
        _ => unreachable!("the benchmark table schema has a tuple root"),
    }
}

fn benchmark(c: &mut Criterion) {
    let schema = schema();
    let format = Format::new(vec![SchemaRef::Inline(schema.clone())]).unwrap();
    let rows = rows();
    let input = encode_rows(&schema, &rows);

    let mut group = c.benchmark_group("Skiff codec throughput");
    group.throughput(Throughput::Bytes(input.len() as u64));
    group.sample_size(20);

    group.bench_function("encode_dynamic", |b| {
        b.iter(|| {
            let mut encoder =
                Encoder::new(Vec::with_capacity(input.len()), schema.clone()).unwrap();
            for row in black_box(rows.as_slice()) {
                encoder.write(row).unwrap();
            }
            black_box(encoder.into_inner().unwrap())
        });
    });

    group.bench_function("decode_dynamic", |b| {
        b.iter(|| {
            let mut decoder = Decoder::new(black_box(input.as_slice()), format.clone());
            let mut total = 0i64;
            while let Some((table, row)) = decoder.next_row().unwrap() {
                total += table as i64 + duration(&row);
            }
            black_box(total)
        });
    });

    group.bench_function("validate_and_skip", |b| {
        b.iter(|| {
            let mut decoder = Decoder::new(black_box(input.as_slice()), format.clone());
            let mut tables = 0usize;
            while let Some(table) = decoder.skip_row().unwrap() {
                tables += table;
            }
            black_box(tables)
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
