# Benchmarking and the Skiff decision

This document answers one question:

> Is YSON parsing enough of the job's CPU (> ~30 %) to justify implementing
> `ytsaurus-skiff`?

This document holds the measurements that exist, the ones that do not, and the
criteria for answering. **The go/no-go is a human decision** — this is the
evidence, not the verdict.

## What has been measured

Two layers, on an Apple M1 Max, rustc 1.94.0, `lto = "fat"`, `codegen-units = 1`.

### 1. Codec microbenchmarks

`cargo bench -p ytsaurus-yson` — parses one whole slice, no streaming.
Baseline in [`crates/ytsaurus-yson/BENCHMARKS.md`](../crates/ytsaurus-yson/BENCHMARKS.md).
Binary deserialisation: **263 MiB/s**.

`cargo bench -p ytsaurus-skiff --bench codec_throughput` measures the same
realistic seven-column rows that the job benchmark uses. Its three cases make
the current dynamic API visible: `encode_dynamic`, `decode_dynamic`, and
`validate_and_skip`. The last validates the stream's framing and schema without
building a `Value` tree; it is a codec baseline, not a job API. Do not compare
the dynamic Skiff result directly with YSON's borrowed-Serde result: Skiff does
not expose typed or borrowing rows yet.

### 2. Job-path benchmark

`cargo bench -p ytsaurus-job` — the real path: streaming reads, record framing,
per-row decoding. The YSON cases isolate where the time goes, and the Skiff case
uses the equivalent schema through `SkiffJobReader`. Its dedicated **YSON vs
Skiff dynamic job API** group compares the two formats directly: each decodes
the same 100 000 logical rows and reads the `duration` field through its public
dynamic value type.

| Case | What it does |
| --- | --- |
| `pass_through` | frame records, never decode — the identity-job floor |
| `parse_borrowed` | decode into `&str` / `&[u8]` fields |
| `parse_owned` | decode into `String` fields, copying every string column |
| `parse_dynamic` | decode into `YsonValue`, a DOM per row |
| `skiff_dynamic` | decode the equivalent schema into Skiff's dynamic `Value` tree |
| `YSON vs Skiff dynamic job API/{yson,skiff}_dynamic` | directly compare those dynamic APIs, reported in rows/sec |

The direct-comparison group intentionally uses **rows/sec**, not bytes/sec:
Skiff and YSON are different-sized streams by design, while the logical row
work is identical. It compares the current dynamic public APIs — positional
Skiff values against keyed YSON values — and is not a claim about a future
typed or borrowing Skiff interface.

The recorded results below are binary YSON, measured on 100 000 rows (~17.7
MiB) with a realistic seven-column schema:

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

The pilot on a cluster (§3) is the other end of that range: a job that does
something with its rows spends **~10 %** on decoding, not 66 % — on the local
Docker cluster. The same pilot on a production cluster spent **36 %** (§4), so
the other end of the range is not one number. The true answer for any given
workload sits between 10 % and 66 %, and where in there depends on the machine
as much as on the job.

Two findings are actionable regardless of how Skiff goes:

- Borrowed decoding is **15 % faster** than owned, for a one-line change in the
  row struct. [The guide](writing-a-job.md) leads with it.
- `YsonValue` costs **1.8×** what a typed struct costs. Avoid it on hot paths.

### 3. The pilot, on a cluster

The two benchmarks above measure a job that does nothing but decode, which is
the worst case for YSON and not a workload. This one asks the same question of
the **pilot** — access-log sessionization, wide mixed-type rows, two output
tables — with the cluster doing the timing:

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh sessionize
cargo run --release -p ytsaurus-client --example profile
```

The method is subtraction. The same mapper runs three times over one table,
stopped at three depths — `map-frames` finds record boundaries and decodes
nothing, `map-parse` decodes each row into the mapper's own struct, `map` is the
pilot — and the scheduler's `time/exec` for each is what it cost. One job per
operation, three rounds per mode, fastest round counted. (Three was the default
when this was run. It is five now, because three did not survive contact with a
production cluster — see §4.)

48 MiB of generated events, 245 521 rows, on the local Docker cluster:

| | | |
| --- | ---: | ---: |
| being handed the rows | 2225 ms | 45.8 % |
| **decoding them** | **514 ms** | **10.6 %** |
| validating and writing | 2120 ms | 43.6 % |
| the pilot's map | 4859 ms | 100 % |

At 16 MiB the decode share was 6.6 %; the rise with size is process startup
being amortised, since that sits in the first bucket.

**Decoding is not what this job spends its time on, here.** ~10 %, against
~46 % to be handed the rows and ~44 % to validate and write them — the last of
which is mostly *output* encoding, since the validation is a handful of
comparisons. On these numbers the write path is the more interesting place to
look.

Three reasons this is a reading and not a verdict:

- **`time/exec` is wall time, not CPU.** It includes process start and the pipe.
- **The cluster runs under emulation** here — x86-64 on arm64 — so absolute
  numbers mean nothing and even ratios are only indicative.
- **The noise is larger than the quantity.** Rounds of the same mode ranged 2225
  to 3509 ms; the decode bucket is 514 ms. Taking the fastest round of three
  helps, since a slow round is interference and a fast one cannot be, but a
  514 ms difference read off numbers that scatter by a second is a direction,
  not a measurement. Two single-round runs of the identical 16 MiB job, minutes
  apart, reported 1776 ms and 897 ms — the same work, twice the time. Anything
  read off one round is worthless, and three is the least that is not.

What it establishes is a bound **on this cluster**: decoding is not 30 % of this
job there, and it is not close. It would take a very different workload — or a
very much faster cluster making the fixed costs smaller — to change that.

A very much faster cluster is exactly what the next section is.

### 4. The same pilot, on a production cluster

Run on 2026-08-09 against a managed multi-node installation — a shared
production cluster with separate proxy roles, not named here — and it
disagrees.

At the then-default three rounds the example **refused to answer**: a shallower
mode measured slower than a deeper one (1507, 1107, 2752 ms), which is the
guard working — on a shared cluster the scheduler's noise is larger than the
quantity being measured, and the run said so instead of reporting a number.
That refusal is why the default is now five rounds rather than three.

At `YT_PROFILE_ROUNDS=7`, same 48 MiB:

| | | |
| --- | ---: | ---: |
| being handed the rows | 474 ms | 34.0 % |
| **decoding them** | **505 ms** | **36.2 %** |
| validating and writing | 415 ms | 29.8 % |
| the pilot's map | 1394 ms | 100 % |

**36.2 % against 10.6 %**, and on the far side of the 30 % threshold the whole
question is framed around. The job is the same job; what changed is the machine
under it. The fixed costs — process start, the pipe, being handed the rows —
are a fifth of what they were on the emulated local cluster — 474 ms against
2225 ms — and decoding is
the part that did not shrink with them. That is the shape you would predict, and
it is the reason the local reading is the one least like production.

**Believe it about as far as the last one.** The rounds still scatter badly —
round 6 took 3331 ms and round 7 took 1395 ms — and the estimator takes the
minimum per mode, so a shared production cluster is a noisier instrument than a
quiet local one, not a quieter one. Two readings that disagree by 3.4× mean the
question is **open**, not settled either way:

- the 10.6 % came from one local cluster, x86-64 under emulation, which is not
  the environment any of this is for;
- the 36.2 % came from one production cluster, on rounds that scatter by 2×;
- neither has been repeated enough times to record a spread rather than a
  number, and doing that is the next measurement this document wants.

Until then, no conclusion about Skiff rests on either figure. See
[skiff-compatibility.md](skiff-compatibility.md) for what the format itself
still has open, which is a separate question from whether it would pay.

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
   Skiff optimises something that is not the problem. *The one workload measured
   this way so far — the pilot — came out at ~10 % on a local cluster (§3) and
   36 % on a production one (§4). This criterion is therefore **not met and not
   missed**: it is unmeasured, and a spread across repeated production runs is
   what would settle it.*
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

## A different question: what the client costs

Everything above is about a **job**, and about whether YSON is the wrong format
for one. This section is about the **launcher**, and it answers a question with
no bearing on Skiff: when a launcher moves rows to and from a cluster, how much
of the time is this crate's doing rather than the cluster's?

```sh
cargo bench -p ytsaurus-client --bench rows
```

The benchmark serves the requests itself, from a socket on loopback that reads
the body and discards it, so what is timed is serialisation, HTTP framing and
one loopback round trip. Apple M1 Max, `--release`, with the Docker cluster
running on the same machine — which is the caveat for every number here.

**Writing.** `write_table_rows` encodes inside the request body, a bufferful at
a time. The alternative — the shape every example in this repository used
before it existed — is to encode the whole table into a `Vec` and send that:

| rows | `write_table_rows` | encode, then `write_table` | |
| ---: | ---: | ---: | --- |
| 1 000 | 275 µs | 338 µs | 1.23× |
| 10 000 | 1.96 ms | 2.29 ms | 1.17× |
| 100 000 | 18.5 ms | 22.8 ms | 1.24× |

The streaming encoder is about **20 % faster**, not merely no worse: it avoids a
`Vec` per row and the copy of the whole table that follows. Bounded memory was
the reason it was written that way; being quicker as well settles the question
of whether it costs anything.

**Reading.** Asking for a type costs about **2–2.6×** taking the bytes:

| rows | `read_table_rows` | `read_table` | |
| ---: | ---: | ---: | --- |
| 1 000 | 606 µs | 317 µs | 1.9× |
| 10 000 | 4.65 ms | 1.81 ms | 2.6× |
| 100 000 | 46.6 ms | 19.2 ms | 2.4× |

That is the honest reason `read_table_streaming` exists for anything large.

**What the benchmark found in the crate.** The first version of it opened a
connection per iteration, and the reason turned out not to be the benchmark: a
table write never read its response body, so `ureq` could not return the
connection to its pool. A few seconds of writing left **11 623** sockets in
`TIME_WAIT`. Reading and discarding the answer fixed it — 46 for the whole suite
afterwards — and took **23 %** off `write_table_rows` at a thousand rows, which
had been paying for a TCP handshake it did not need. Every table write against a
real cluster was doing the same.

**Appending, against the real cluster.** `examples/append.rs` writes the same
rows in the same number of pieces, both ways. 60 000 rows in 12 pieces on the
local cluster:

```text
appending     0.60s       60000 rows sent
rewriting     1.03s      390000 rows sent   (6.5× the data)
```

The data ratio is arithmetic rather than measurement — rewriting `k` pieces
sends `(k+1)/2` times the rows — and the clock only says whether the cluster
charges for it. On this one it does, at roughly half the ratio, since a write
is not purely bytes on the wire.

**Aborting.** `examples/abort.rs` measures the one number there is: the
scheduler accepted the abort in **321–405 ms**, and the operation was *already*
`aborted` when it did. There is an `aborting` state in between, but the HTTP call
outlives it, so no caller of `abort_operation` can observe it.

## Reproducing the local numbers

```sh
cargo bench -p ytsaurus-yson     # codec
cargo bench -p ytsaurus-skiff --bench codec_throughput  # Skiff codec
cargo bench -p ytsaurus-job      # job path
cargo bench -p ytsaurus-client   # the launcher's own cost, over loopback

# Streaming memory behaviour: 2 GB through the reader.
cargo test -p ytsaurus-job --release --test memory_tests -- --ignored --nocapture

# Against a cluster:
export YT_PROXY=http://localhost:8000
cargo run --release -p ytsaurus-client --example append   # append against rewrite
cargo run -p ytsaurus-client --example abort              # how long stopping takes
```
