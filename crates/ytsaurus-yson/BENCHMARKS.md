# ytsaurus-yson — performance baseline

Baseline captured immediately after vendoring
[ss123she/yson-rs](https://github.com/ss123she/yson-rs) @ `ba2044c` and applying the
fork changes listed in [CHANGELOG.md](CHANGELOG.md). Its purpose is to make any later
regression visible, and to feed the [Skiff go/no-go
decision](../../docs/benchmarking.md).

## How to reproduce

```sh
cargo bench -p ytsaurus-yson
```

The harness is [`benches/yson_benchmark.rs`](benches/yson_benchmark.rs) (criterion,
inherited from upstream). The payload is 10 000 records of a small struct
(`u64`, `&str`, `Vec<&str>`, `HashMap<&str, f64>`) — roughly 1.2 MB binary / 1.5 MB
text, deserialised into borrowed types.

## Results

Measured 2026-08-04 on an **Apple M1 Max** (10 cores, macOS 26.2), rustc 1.94.0,
`[profile.bench]` = `opt-level 3`, `lto = "fat"`, `codegen-units = 1`.

| Format | Operation | Throughput (median) | Time (median) |
| :--- | :--- | ---: | ---: |
| Binary | Serialize | **1.51 GiB/s** | 770 µs |
| Binary | Deserialize | **263 MiB/s** | 4.53 ms |
| Text | Serialize | **249 MiB/s** | 3.44 ms |
| Text | Deserialize | **146 MiB/s** | 5.85 ms |

Criterion's low/high bounds were within ±1 % of the median for every case except
text serialisation (±3 %).

### Comparison with the upstream README

Upstream reports numbers from an Intel Core i5-11400. Same code, different machine —
the columns are not directly comparable, but the *shape* matches, which is a decent
sanity check that vendoring did not perturb anything.

| Case | Upstream (i5-11400) | Here (M1 Max) |
| :--- | ---: | ---: |
| Binary serialize | 1.71 GiB/s | 1.51 GiB/s |
| Binary deserialize | 255 MiB/s | 263 MiB/s |
| Text serialize | 339 MiB/s | 249 MiB/s |
| Text deserialize | 129 MiB/s | 146 MiB/s |

## Reading these numbers for the Skiff decision

The number that matters is **binary deserialisation at ~263 MiB/s**.
That is the ceiling on how fast a Rust job can consume its input, before any user
logic runs.

Two caveats before drawing conclusions from it:

1. **This benchmark deserialises into borrowed types** (`&str`), which is the
   best case. Rows deserialised into owned `String`s will be slower.
2. **It measures a whole slice at once.** The streaming reader in `ytsaurus-job`
   re-parses at record boundaries and copies across chunk edges, so end-to-end job
   throughput will be lower. Judge the job, not this microbenchmark — see
   [docs/benchmarking.md](../../docs/benchmarking.md), which measures the job path.

A YTsaurus job typically gets a fraction of a core, so ~263 MiB/s of parsing is
unlikely to be the bottleneck for I/O-bound work — but it is well short of what
Skiff (a fixed-layout format with no per-field tags) can do. Decide with real
measurements on a real workload, not with this table.
