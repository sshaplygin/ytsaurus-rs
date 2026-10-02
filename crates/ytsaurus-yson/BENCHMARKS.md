# ytsaurus-yson performance baseline

Captured immediately after vendoring
[ss123she/yson-rs](https://github.com/ss123she/yson-rs) @ `ba2044c` and applying the
fork changes listed in [CHANGELOG.md](CHANGELOG.md), to make later regressions
visible and as input to the [Skiff decision](../../docs/benchmarking.md).

## How to reproduce

```sh
cargo bench -p ytsaurus-yson
```

The harness is [`benches/yson_benchmark.rs`](benches/yson_benchmark.rs) (criterion,
inherited from upstream). The payload is 10 000 records of a small struct
(`u64`, `&str`, `Vec<&str>`, `HashMap<&str, f64>`), roughly 1.2 MB binary / 1.5 MB
text, deserialised into borrowed types.

## Results

Measured 2026-08-04 on an Apple M1 Max (10 cores, macOS 26.2), rustc 1.94.0,
`[profile.bench]` = `opt-level 3`, `lto = "fat"`, `codegen-units = 1`.

| Format | Operation | Throughput (median) | Time (median) |
| :--- | :--- | ---: | ---: |
| Binary | Serialize | 1.51 GiB/s | 770 µs |
| Binary | Deserialize | 263 MiB/s | 4.53 ms |
| Text | Serialize | 249 MiB/s | 3.44 ms |
| Text | Deserialize | 146 MiB/s | 5.85 ms |

Criterion's low/high bounds were within ±1 % of the median for every case except
text serialisation (±3 %).

### Comparison with the upstream README

Upstream reports numbers from an Intel Core i5-11400. Different machines, so the
columns are not directly comparable; the shape matches, which suggests vendoring
did not perturb anything.

| Case | Upstream (i5-11400) | Here (M1 Max) |
| :--- | ---: | ---: |
| Binary serialize | 1.71 GiB/s | 1.51 GiB/s |
| Binary deserialize | 255 MiB/s | 263 MiB/s |
| Text serialize | 339 MiB/s | 249 MiB/s |
| Text deserialize | 129 MiB/s | 146 MiB/s |

## What these numbers bound

Binary deserialisation at ~263 MiB/s is the ceiling on how fast a Rust job can
consume its input before any user logic runs, under two caveats:

1. The benchmark deserialises into borrowed types (`&str`), the best case.
   Owned `String`s will be slower.
2. It measures a whole slice at once. The streaming reader in `ytsaurus-job`
   re-parses at record boundaries and copies across chunk edges, so end-to-end
   job throughput will be lower. [docs/benchmarking.md](../../docs/benchmarking.md)
   measures the job path.

A YTsaurus job typically gets a fraction of a core, so ~263 MiB/s of parsing is
unlikely to bottleneck I/O-bound work, but it is well short of what Skiff, a
fixed-layout format with no per-field tags, can do. This table does not decide
the format; real measurements on a real workload do.
