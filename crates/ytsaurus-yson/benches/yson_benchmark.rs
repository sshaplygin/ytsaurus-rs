use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use std::hint::black_box;
use ytsaurus_yson::{de::Deserializer, ser::Serializer};

use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone)]
struct BenchData<'a> {
    id: u64,
    #[serde(borrow)]
    name: &'a str,
    #[serde(borrow)]
    tags: Vec<&'a str>,
    #[serde(borrow)]
    properties: HashMap<&'a str, f64>,
}

fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn generate_data() -> Vec<BenchData<'static>> {
    (0..10_000)
        .map(|i| {
            let mut props = HashMap::new();
            props.insert("x", 10.5);
            props.insert("y", 20.1);
            props.insert("velocity", 99.9);

            BenchData {
                id: i,
                name: leak_str(format!("Item-{i}")),
                tags: vec!["fast", "rust", "serde"],
                properties: props,
            }
        })
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    let data = generate_data();

    let mut ser_bin = Serializer::new(true);
    data.serialize(&mut ser_bin).unwrap();
    let bin_bytes = ser_bin.output;

    let mut ser_text = Serializer::new(false);
    data.serialize(&mut ser_text).unwrap();
    let text_bytes = ser_text.output;

    let mut group = c.benchmark_group("YSON Throughput");

    // Bench: Serialize Binary
    group.throughput(Throughput::Bytes(bin_bytes.len() as u64));
    group.bench_function("Serialize Binary", |b| {
        b.iter(|| {
            let mut ser = Serializer::new(true);
            black_box(&data).serialize(&mut ser).unwrap();
        });
    });

    // Bench: Deserialize Binary
    group.bench_function("Deserialize Binary", |b| {
        b.iter(|| {
            let mut de = Deserializer::from_bytes(black_box(&bin_bytes), true);
            let _val: Vec<BenchData> = Vec::deserialize(&mut de).unwrap();
        });
    });

    // Bench: Serialize Text
    group.throughput(Throughput::Bytes(text_bytes.len() as u64));
    group.bench_function("Serialize Text", |b| {
        b.iter(|| {
            let mut ser = Serializer::new(false);
            black_box(&data).serialize(&mut ser).unwrap();
        });
    });

    // Bench: Deserialize Text
    group.bench_function("Deserialize Text", |b| {
        b.iter(|| {
            let mut de = Deserializer::from_bytes(black_box(&text_bytes), false);
            let _val: Vec<BenchData> = Vec::deserialize(&mut de).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
