# Benchmarking and the Skiff decision

This document answers one question:

> Is YSON parsing enough of the job's CPU (> ~30 %) to justify implementing
> `ytsaurus-skiff`?

This document holds the measurements that exist, the ones that do not, and the
criteria for answering. **The go/no-go is a human decision** — this is the
evidence, not the verdict.

## What has been measured

Two layers, on an Apple M1 Max, rustc 1.94.0, `lto = "fat"`, `codegen-units = 1`.

### 1. Codec microbenchmark

`cargo bench -p ytsaurus-yson` — parses one whole slice, no streaming.
Baseline in [`crates/ytsaurus-yson/BENCHMARKS.md`](../crates/ytsaurus-yson/BENCHMARKS.md).
Binary deserialisation: **263 MiB/s**.

### 2. Job-path benchmark

`cargo bench -p ytsaurus-job` — the real path: streaming reads, record framing,
per-row decoding. Four cases isolate where the time goes.

| Case | What it does |
| --- | --- |
| `pass_through` | frame records, never decode — the identity-job floor |
| `parse_borrowed` | decode into `&str` / `&[u8]` fields |
| `parse_owned` | decode into `String` fields, copying every string column |
| `parse_dynamic` | decode into `YsonValue`, a DOM per row |

Measured on 100 000 rows (~17.7 MiB) with a realistic seven-column schema:

| Case | Time | Throughput |
| --- | ---: | ---: |
| `pass_through` (framing only) | 17.43 ms | **1014 MiB/s** |
| `parse_borrowed` (`&str` / `&[u8]`) | 51.96 ms | **340 MiB/s** |
| `parse_owned` (`String`) | 59.89 ms | **295 MiB/s** |
| `parse_dynamic` (`YsonValue`) | 92.18 ms | **192 MiB/s** |

`pass_through` versus `parse_borrowed` is the quantity that matters: it is the
share of job CPU that Skiff could actually remove. Framing cost (`pass_through`)
does not go away with Skiff — a fixed-layout format still has to find record
boundaries — and user logic does not either.

### Reading these numbers

For a job that does nothing but decode, field decoding is
`51.96 − 17.43 = 34.5 ms`, i.e. **66 % of job CPU** — well above the ~30 %
threshold. But that is the *worst case for YSON*: it assumes zero user logic.
Any real job does work per row, and the parse share falls in proportion. So the
threshold question **cannot be answered without a real workload**, which is
exactly why this is a joint decision rather than a benchmark result.

Two findings are actionable regardless of how Skiff goes:

- Borrowed decoding is **15 % faster** than owned, for a one-line change in the
  row struct. [The guide](writing-a-job.md) leads with it.
- `YsonValue` costs **1.8×** what a typed struct costs. Avoid it on hot paths.

## What has *not* been measured

The part that really settles it, because it needs a cluster:

- a ≥ 10 GB table with a realistic schema,
- the same job in **C++** (`yt/cpp/mapreduce`) and **Python** for comparison,
- **job cpu time**, **operation wall time** and **RSS** as YTsaurus reports them.

The local benchmark is a proxy, and an optimistic one: it reads from memory, not
from a pipe fed by a node, and it runs on a full core rather than the fraction a
job is usually allotted.

## Running the cluster comparison

With a cluster available:

```sh
# 1. A realistic table. Substitute a real one if you have it.
yt --proxy "$YT_PROXY" create table //tmp/bench_input --force

# 2. The Rust job.
scripts/build-worker.sh cat
yt --proxy "$YT_PROXY" map './cat' \
    --src //tmp/bench_input --dst //tmp/bench_rust \
    --format '<format=binary>yson' \
    --local-file target/x86_64-unknown-linux-musl/release-worker/cat

# 3. Read the statistics the scheduler recorded.
yt --proxy "$YT_PROXY" get //sys/operations/<op-id>/@progress/job_statistics
```

The fields to record, per job and summed:

| Statistic | Meaning |
| --- | --- |
| `user_job/cpu/user` | CPU the job itself burned |
| `user_job/cpu/system` | syscall time — mostly reading the pipe |
| `user_job/max_memory` | peak RSS |
| `time/total` | wall clock |
| `data/input/data_weight` | bytes in, for normalising |

Repeat with an equivalent C++ job (`yt/cpp/mapreduce`) and a Python one
(`ytsaurus-client`) over the same table. Compare `user_job/cpu/user` per byte.

## Decision criteria

Implement `ytsaurus-skiff` if **both** hold:

1. **Parsing is the bottleneck.** `parse_borrowed - pass_through`, or the
   equivalent measured on the cluster, exceeds ~30 % of job CPU. Below that,
   Skiff optimises something that is not the problem.
2. **The Rust job is not already fast enough.** If it already beats the C++
   baseline on CPU per byte, the remaining headroom is unlikely to justify a
   second wire format, its schema negotiation, and the ongoing compatibility
   burden.

Arguments the other way, worth weighing explicitly:

- Skiff needs the operation spec to carry a **schema**, which is a real increase
  in the API surface a job author has to understand. YSON needs none.
- Skiff is **positional**: adding a column changes the wire layout, so job and
  table schema must be upgraded together. YSON tolerates schema drift.
- The reference implementation is the `skiff` package in the
  [Go SDK](https://pkg.go.dev/go.ytsaurus.tech/yt/go), which is a good model but
  still a full format to port and keep correct.

If the answer is no, the cheaper wins are worth doing first:

- decode into borrowed types everywhere (already the largest single lever —
  see `parse_owned` versus `parse_borrowed`),
- avoid `YsonValue` on hot paths (`parse_dynamic` shows what it costs),
- raise the read buffer for wide rows.

## Reproducing the local numbers

```sh
cargo bench -p ytsaurus-yson     # codec
cargo bench -p ytsaurus-job      # job path

# Streaming memory behaviour: 2 GB through the reader.
cargo test -p ytsaurus-job --release --test memory_tests -- --ignored --nocapture
```
