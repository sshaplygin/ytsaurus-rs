# Benchmarking and the Skiff decision

This document holds the measurements behind one question:

> Is YSON parsing enough of the job's CPU (> ~30 %) to justify making Skiff,
> through `ytsaurus-skiff`, the default job format?

The go/no-go is a human decision.

## The verdict

Undecided. For the pilot job, decoding is 10.6 % of the job's `time/exec` on
the local Docker cluster (§3) and 36.2 % on a production cluster (§4), against
the ~30 % threshold. Both readings scatter by about 2× across rounds, and
neither has been repeated enough to record a spread rather than a number. A
spread from repeated production runs is still needed (#70). Do not quote the
10 % alone.

The format comparison (§5) did not decide it. The comparison that would, typed
YSON against typed Skiff, cannot be run while Skiff has no typed rows.
[`skiff-compatibility.md`](skiff-compatibility.md)'s required test 4 is still
open; see [Decision criteria](#decision-criteria).

## Pull-request comparison

Every pull request that changes Rust code runs the four Criterion suites at its
current `main` base commit and at its head. The suites run in parallel; each
`main`/PR pair runs sequentially on one GitHub-hosted VM with the PR's pinned
Rust toolchain. Raw logs are kept as an artifact for 14 days. One comment on a
same-repository PR carries every benchmark's middle time estimate and relative
change; a fork PR, whose read-only token cannot post a comment, gets the same in
the job summary.

Time is lower-is-better. A change of 20% or more is flagged as an improvement or
regression but does not fail the PR, since shared runners are noisy; re-run
before trusting a borderline result.

## What has been measured

§1 and §2 run in-process on an Apple M1 Max, rustc 1.94.0, `lto = "fat"`,
`codegen-units = 1`. §3 to §5 run on a cluster.

### 1. Codec microbenchmarks

`cargo bench -p ytsaurus-yson` parses one whole slice, no streaming. Baseline in
[`crates/ytsaurus-yson/BENCHMARKS.md`](../crates/ytsaurus-yson/BENCHMARKS.md).
Binary deserialisation: 263 MiB/s.

`cargo bench -p ytsaurus-skiff --bench codec_throughput` measures the job
benchmark's seven-column rows in three cases: `encode_dynamic`,
`decode_dynamic` and `validate_and_skip`. The last validates framing and schema
without building a `Value` tree; it is a codec baseline, not a job API. Do not
compare the dynamic Skiff result with YSON's borrowed-Serde result: Skiff does
not expose typed or borrowing rows yet.

### 2. Job-path benchmark

`cargo bench -p ytsaurus-job` measures streaming reads, record framing and
per-row decoding. The YSON cases isolate where the time goes; the Skiff case
reads the equivalent schema through `SkiffJobReader`.

| Case | What it does |
| --- | --- |
| `pass_through` | frame records, never decode: the identity-job floor |
| `parse_borrowed` | decode into `&str` / `&[u8]` fields |
| `parse_owned` | decode into `String` fields, copying every string column |
| `parse_dynamic` | decode into `YsonValue`, a DOM per row |
| `skiff_dynamic` | decode the equivalent schema into Skiff's dynamic `Value` tree |
| `YSON vs Skiff dynamic job API/{yson,skiff}_dynamic` | directly compare those dynamic APIs, reported in rows/sec |
| `YSON vs Skiff dynamic encoding/{yson,skiff}_dynamic` | compare dynamic row construction and encoding, reported in rows/sec |

The direct-comparison groups use the same 100 000 logical rows in each format
and report rows/sec, since the streams differ in size by design. They compare
the current dynamic APIs, positional Skiff values against keyed YSON values, not
a future typed Skiff. The decode group reads `duration` through each dynamic
value; the encode group includes row construction and times one complete table
stream, record separator or table tag included.

#### Direct dynamic comparisons

The 20-sample Criterion run, 100 000 identical rows per iteration, row
construction included in the encode rows:

| Path | Time | Throughput |
| --- | ---: | ---: |
| decode: binary YSON → `YsonValue` + keyed `duration` lookup | 97.94 ms | 1.021 M rows/s |
| decode: Skiff → `Value` + positional `duration` lookup | 30.67 ms | 3.261 M rows/s |
| encode: binary YSON `YsonValue` map → table stream | 79.16 ms | 1.263 M rows/s |
| encode: Skiff `Value` tuple → table stream | 29.19 ms | 3.426 M rows/s |

Skiff was 3.19× faster decoding and 2.71× faster encoding. These are results
about the current Rust implementations and their dynamic values, including
their different row representations, not a protocol-wide claim or a prediction
for a job that uses typed YSON rows.

Most of the decode gap is the row representation. On the whole map,
off-cluster, the same pair of APIs stands at 3.14×; giving the Skiff side a map
keyed by the same column names, with format, codec and wire bytes unchanged,
brings it to 1.66×
([format-comparison.md](format-comparison.md#off-cluster-reproduction)). The
3.19× has not been re-measured under that change, which bounds it rather than
replacing it. **Quote this pair as "keyed DOM against positional tuple", never
as "YSON against Skiff".**

#### Binary YSON, by decoding depth

100 000 rows (~17.7 MiB) with a realistic seven-column schema:

| Case | Time | Throughput |
| --- | ---: | ---: |
| `pass_through` (framing only) | 17.43 ms | 1014 MiB/s |
| `parse_borrowed` (`&str` / `&[u8]`) | 51.96 ms | 340 MiB/s |
| `parse_owned` (`String`) | 59.89 ms | 295 MiB/s |
| `parse_dynamic` (`YsonValue`) | 92.18 ms | 192 MiB/s |

`pass_through` against `parse_borrowed` is the share of job CPU that Skiff could
remove. Framing does not go away with Skiff, since a fixed-layout format still
has to find record boundaries, and neither does user logic.

### Reading these numbers

For a job that does nothing but decode, field decoding is
`51.96 − 17.43 = 34.5 ms`, 66 % of job CPU. That is the worst case for YSON: it
assumes zero user logic. The pilot does something with its rows and spends
10.6 % locally (§3) and 36.2 % on production (§4). For a given workload the
share sits between 10 % and 66 %, and the machine moves it as much as the job
does.

Two results apply regardless of Skiff:

- Borrowed decoding is 15 % faster than owned, for a one-line change in the row
  struct. [The guide](writing-a-job.md) leads with it.
- `YsonValue` costs 1.8× what a typed struct costs. Avoid it on hot paths.

### 3. The pilot, on a cluster

The pilot: access-log sessionization, wide mixed-type rows, two output tables.

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh sessionize
cargo run --release -p ytsaurus-client --example profile
```

The method is subtraction: one mapper over one table, stopped at three depths.
`map-frames` finds record boundaries and decodes nothing, `map-parse` decodes
each row into the mapper's struct, `map` is the pilot. The scheduler's
`time/exec` for each is its cost. One job per operation, three rounds per mode
(the default then; five now, see §4), fastest round counted.

48 MiB of generated events, 245 521 rows, on the local Docker cluster:

| | | |
| --- | ---: | ---: |
| being handed the rows | 2225 ms | 45.8 % |
| decoding them | 514 ms | 10.6 % |
| validating and writing | 2120 ms | 43.6 % |
| the pilot's map | 4859 ms | 100 % |

At 16 MiB the decode share was 6.6 %; the rise with size is process startup
being amortised in the first bucket. Validating and writing is mostly output
encoding, since the validation is a handful of comparisons, so on these numbers
the write path is the larger target.

Limits:

- `time/exec` is wall time, not CPU, including process start and the pipe.
- The cluster runs x86-64 images under arm64 emulation: absolute numbers mean
  nothing and ratios are only indicative.
- The noise is larger than the quantity. Rounds of one mode ranged 2225 to
  3509 ms against a 514 ms decode bucket, so the result is a direction, not a
  measurement. The fastest of three counts, since a slow round is interference
  and a fast one cannot be. Two single-round runs of the identical 16 MiB job,
  minutes apart, took 1776 ms and 897 ms; three rounds is the minimum.

On this cluster, decoding is not 30 % of this job, nor close to it. A very
different workload, or a much faster cluster with smaller fixed costs, could
change that; §4 is the second.

### 4. The same pilot, on a production cluster

Run on 2026-08-09 against a managed multi-node installation, a shared
production cluster with separate proxy roles, not named here.

At three rounds the example refused to answer: a shallower mode measured slower
than a deeper one (1507, 1107, 2752 ms), so the noise exceeded the quantity. The
default is five rounds for that reason. At `YT_PROFILE_ROUNDS=7`, same 48 MiB:

| | | |
| --- | ---: | ---: |
| being handed the rows | 474 ms | 34.0 % |
| decoding them | 505 ms | 36.2 % |
| validating and writing | 415 ms | 29.8 % |
| the pilot's map | 1394 ms | 100 % |

Same job, different machine, other side of the 30 % threshold. The fixed costs
(process start, the pipe, being handed the rows) are a fifth of the emulated
local cluster's, 474 ms against 2225 ms; decoding did not shrink with them.

The rounds still scatter: round 6 took 3331 ms and round 7 took 1395 ms. The
estimator takes the minimum per mode, and a shared production cluster is a
noisier instrument than a quiet local one. The two readings differ by 3.4×,
and the 10.6 % came from an emulated x86-64 cluster, not the environment any of
this is for. No conclusion about Skiff rests on either figure.
[skiff-compatibility.md](skiff-compatibility.md) covers what the format itself
still has open, a separate question from whether it would pay.

### 5. The same map, in three formats and a query

Three formats and a query on one task. The plan, the method and the
off-cluster reproduction are in [format-comparison.md](format-comparison.md).

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh sessionize
YT_COMPARE_TASK=project YT_COMPARE_MIB=48 YT_COMPARE_ROUNDS=9 \
    cargo run --release -p ytsaurus-client --example format_compare
```

The task is the pilot's map without the rejects table: nine mixed-type columns,
five validation rules, one derived column, one output table, no shuffle, over
412 554 rows / 48 MiB. `data_weight_per_job` pins every leg to one job, and
rounds are interleaved so each round's legs meet the same cluster. Before any
clock is read, each run diffs every leg's rows against the first leg's; all
four computing legs agree row for row
([method](format-comparison.md#method)).

| leg | reads | stops at |
| --- | --- | --- |
| `typed: frames` | binary YSON | record boundaries, decoding nothing |
| `typed: decoded` | binary YSON | a borrowed serde struct, writing nothing |
| `typed: full` | binary YSON | the whole map, one output table |
| `dynamic: decoded` | binary YSON | a `YsonValue` DOM, writing nothing |
| `dynamic: full` | binary YSON | the whole map, `YsonValue` in and out |
| `skiff: decoded` | Skiff | a positional `Value` tuple, writing nothing |
| `skiff: full` | Skiff | the whole map, `Value` in and out |
| `YQL` | the query's projection | `INSERT INTO`, the same computation |

Skiff has no frames-only stop: with no self-describing record boundaries,
finding the end of a row is decoding it.

Everything ran on a single-node local Docker cluster, x86-64 images under arm64
emulation, one job per leg: the environment least like production. A
production run is still owed (#70).

#### Results

Three nine-round runs, every ratio paired within its round:

| paired by round | run 1 | run 2 | run 3 |
| --- | ---: | ---: | ---: |
| Skiff against the dynamic YSON leg, whole map | 1.85× | 1.88× | 1.93× |
| Skiff against the dynamic leg, read only | 1.65× | 1.61× | — |
| Skiff against the typed YSON leg, whole map | 1.10× | 1.16× | 1.20× |
| Skiff against the query | 1.44× | 1.47× | 1.49× |
| typed YSON against the dynamic leg | 1.61× | 1.62× | — |
| decoding, by subtraction | 322 ms, 13 % | 281 ms, 13 % | 308 ms, 10 % |

- Every ratio is `time/exec`, per-job wall time summed over jobs, the only
  timing this cluster offers. It excludes `time/prepare`, a further 650–800 ms
  a job for every leg, which makes each ratio larger than the whole-job one.
  Folded back in, by arithmetic over medians rather than a measured pairing,
  1.20× becomes about 1.15× and 1.93× about 1.7×.
- Run 3 is not a third sample for any Skiff row. Before it,
  *stop the Skiff leg handicapping itself* removed eight `Value` clones a row
  that the Skiff mapper made where `SkiffRow::into_value` exists and the row
  already owns them (three allocations a row, 12 % of the leg). So 1.93×, 1.20×
  and 1.49× are partly that fix, the drift across runs is not a spread, and the
  run 1 and 2 Skiff figures are a floor. The one row measuring an unchanged
  program, typed YSON against the dynamic leg, has values for two runs only.
- The `—` cells are blanks in the harness's output, of an unrecorded kind:
  `paired_ratios` prints nothing for a pair whose sign flips between rounds,
  which would make a blank a result, and the run output was not kept. The sign
  claims below apply to the whole-map rows, where the sign held in all nine
  rounds of all three runs.
- Rounds of the first row fall between 1.68× and 2.18×. The decode row is a
  mean over only the rounds that came out in order, whose decode bucket ranged
  100 to 731 ms in the noisiest run. It is an upper estimate: the harness drops
  any round where a shallower stop measured slower than a deeper one, which can
  only remove rounds where noise made the difference small or negative.

#### What the wire carries

| | in | out |
| --- | ---: | ---: |
| Skiff | 54.6 MiB | 47.2 MiB |
| binary YSON | 91.1 MiB | 85.7 MiB |
| YQL | 55.0 MiB | 47.6 MiB |

Identical in all three runs: the solid result. Reproduced byte for byte off the
cluster from the generator and the two encodings: Skiff 54.6 in and 47.2
out, YSON 85.7 out, and YSON 90.7 in against the cluster's 91.1, the 0.4 MiB
being control records. The input row streams, 90.7 MiB against 54.6 MiB over
412 554 rows, differ by about 92 bytes a row, as a net:

- YSON adds the nine column names, 71 bytes, on every row.
- It adds about 38 bytes of map syntax: a type marker and a length for each of
  the nine keys, nine `=`, the eight `;` between pairs, two braces and the
  record separator.
- It gets about 17 bytes back. Its varint integers and one-byte length prefixes
  are smaller than Skiff's fixed-width `int64`/`uint64` and four-byte `string32`
  prefixes (`status` costs 3 bytes in YSON against 8 in Skiff, `bytes_sent`
  about 4 against 8), and Skiff pays a two-byte table tag on every row.

71 + 38 − 17 ≈ 92, against the measured 91.8. The row's payload is ~122 bytes,
the data weight the cluster reports for this table and what summing the nine
values by hand gives, so a YSON row is ~231 bytes on the wire and a Skiff row
~139.

This depends on the row: the names weigh 71 bytes against 122 of payload
because the nine columns are short, and five wide blobs would show almost none
of it; the 17-byte credit grows on a table of small integers, and the direction
of the comparison is not guaranteed. "Skiff is 40 % smaller" describes this
schema, not the format.

YQL's own job I/O is Skiff, read off the operation spec. Its schema carries
`$row_index` as `variant8<nothing;int64>` and writes `is_external` as an
optional boolean where the worker writes a plain one: the whole difference
between its 55.0/47.6 and the hand-written schema's 54.6/47.2. An independently
written positional schema matching the engine's to within its system columns is
the strongest evidence so far that the Skiff leg is right.

#### Skiff against the dynamic YSON leg

The 1.85–1.93× is a representation difference, not a format one. 79–88 % of the
gap on the cluster (88 %, 82 % and 79 % in the three runs, moving with the Skiff
leg's fix) and 98 % of it off the cluster lies between the two YSON legs, which
move identical streams through the same reader and serializer and still differ
by 1.61× and 1.62×. With the representation equalised, the format accounts for at
most 1.66×, and on this evidence less
([format-comparison.md](format-comparison.md#off-cluster-reproduction)). The read-only row moves different bytes, 54.6
MiB against 91.1 in, so it is not a clean representation comparison either.

#### Skiff against typed YSON

The typed leg, a borrowed serde struct allocating nothing per row, is what a
job author writes today. Skiff is ahead by 1.10×, 1.16× and 1.20×, in all nine
rounds of every run and off the cluster. It is not a format measurement:

- the Skiff decoder `Box::new()`s the `referer` variant on every row, including
  the ones where it is absent;
- the Skiff path reads through a `BufReader` with about 14 `read_exact` calls a
  row, where the YSON legs parse in place;
- with the output bytes discarded, the two legs' in-process time differs by
  1.01× to 1.13×, smaller than and in the same direction as the cluster's gap.

The cluster-side advantage is consistent with being mostly the 38.5 MiB of
output that Skiff does not push through the pipe: wire volume arriving as time,
not a faster codec, and a job with small output would not collect it. The
deciding comparison cannot be run, since
[`skiff-compatibility.md`](skiff-compatibility.md) lists typed rows, schema
inference and typed `Scan`/`Write` as planned; 1.10–1.20× is dynamic Skiff
against typed YSON, the representation confound above with its sign reversed.

#### Against the query

Skiff against the query: 1.44×, 1.47×, 1.49×. Typed worker against the query
was printed in each run but not recorded; the quotient of recorded pairs,
1.44/1.10, 1.47/1.16 and 1.49/1.20, gives 1.24× to 1.31× in the worker's
favour. That is arithmetic over medians of different pairs, not a ratio any
round produced, and a re-run should replace it with the measured pairing before
anything is decided on it. YQL held 640 MB against the worker's 512, an
asymmetry the harness prints by design and one that favours the query. YQL is
not a C++ SDK job: it brings an optimizer and a vectorized runtime. Its usual
projection advantage does not apply, since every leg reads the same nine
columns.

#### What §5 does not close

The Skiff leg exercised nine columns, mixed types and an optional variant, more
shape than `skiff_launch` covered on 2026-08-09, but one input table, one
output table and no key switch. Required test 4 of
[`skiff-compatibility.md`](skiff-compatibility.md) stays open.

## What has *not* been measured

- A ≥ 10 GB table with a realistic schema.
- The same job in C++ (`yt/cpp/mapreduce`) and Python. §5's YQL leg runs YQL's
  C++ runtime, which is not the C++ SDK job. Python is unmeasured.
- Job CPU time, operation wall time and RSS as YTsaurus reports them. Job CPU is
  missing wherever the format comparison ran: that cluster reports nothing
  under `user_job/cpu`.
- A typed Skiff leg. Every Skiff figure on record is a dynamic-API figure.

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
| `user_job/cpu/system` | syscall time, mostly reading the pipe |
| `user_job/max_memory` | peak RSS |
| `time/total` | wall clock |
| `data/input/data_weight` | bytes in, for normalising |

Repeat with an equivalent C++ job (`yt/cpp/mapreduce`) and a Python one
(`ytsaurus-client`) over the same table. Compare `user_job/cpu/user` per row,
not per byte (criterion 2 below says why). Record `data/input/data_weight` as
the format's size difference, not as a denominator.

## Decision criteria

Make Skiff the default if both hold:

1. Parsing is the bottleneck: `parse_borrowed - pass_through`, or the
   equivalent measured on the cluster, exceeds ~30 % of job CPU. Below that,
   Skiff optimises something that is not the problem.

   Unmeasured in its unit, so neither met nor missed: the pilot's ~10 % (§3) and
   36 % (§4) are wall time. The local cluster reports nothing under
   `user_job/cpu`, and roughly half its wall-time denominator is process start
   and waiting for the first batch: a job start costs ~640 ms there and
   `latency/input/time_to_first_read_batch` a further ~504 ms of a ~2000 ms
   job, with `time/prepare` another 650–800 ms that `time/exec` excludes. §5's
   decode bucket (13 %, 13 %, 10 % of the whole map) is in §3's territory on
   the same class of machine, but not over the same denominator: §3's job wrote
   two output tables and subtracted minima rather than pairing rounds. It says
   nothing about §4's 36.2 %. Only a cluster that reports CPU can move this
   criterion.
2. The Rust job is not already fast enough: if it already beats the C++
   baseline on the same logical work, the remaining headroom is unlikely to
   justify a second wire format, its schema negotiation, and the ongoing
   compatibility burden.

   The unit was CPU per byte; it is now the same logical rows through the same
   pipeline, because per byte ranks formats backwards once a baseline reads a
   different one. YQL's job I/O is Skiff, 55.0/47.6 MiB against the worker's
   91.1/85.7. With Skiff at `T`, typed YSON is 1.20× slower and better per input
   byte, 1.20/91.1 = 0.0132·T against 1/54.6 = 0.0183·T. Against YQL the ranking
   holds: the worker is faster (1.24–1.31×) and cheaper per byte (1/91.1 =
   0.0110·T against 1.24/55.0 = 0.0226·T), so per byte is unsafe rather than
   always wrong. Use CPU per row where the cluster reports CPU, else per-job wall
   time paired by round with the legs interleaved.

   §5 is the first evidence for this criterion, and it points away from
   spending the format budget: typed YSON is ahead of YQL on this task, on
   per-job wall time on one local cluster, by 1.24× to 1.31× derived from
   medians, with YQL's 640 MB against 512 MB in the query's favour; Skiff's
   margin over typed YSON is 1.10–1.20×, an unknown but large part of it the
   pipe rather than the codec. No C++ SDK job has been run, and YQL, with its
   optimizer and Skiff job I/O, is not a YSON baseline.

Against Skiff:

- Skiff needs the operation spec to carry a schema, a real increase in the API
  surface a job author has to understand. YSON needs none.
- Skiff is positional: adding a column changes the wire layout, so job and table
  schema must be upgraded together. YSON tolerates schema drift.
- The reference implementation is the `skiff` package in the
  [Go SDK](https://pkg.go.dev/go.ytsaurus.tech/yt/go), a good model but still a
  full format to keep correct.

If the answer is no, the cheaper wins come first:

- decode into borrowed types everywhere (the largest single lever: see
  `parse_owned` against `parse_borrowed`),
- avoid `YsonValue` on hot paths (`parse_dynamic` shows what it costs),
- raise the read buffer for wide rows.

## A different question: what the client costs

This section is about the launcher, not Skiff: how much of a launcher's row
transfer time is this crate's?

```sh
cargo bench -p ytsaurus-client --bench rows
```

A loopback socket serves the requests and discards the body, so the timing is
serialisation, HTTP framing and one round trip. Apple M1 Max, `--release`, with
the Docker cluster on the same machine.

Writing. `write_table_rows` encodes inside the request body, a bufferful at a
time, against encoding the whole table into a `Vec` and sending that:

| rows | `write_table_rows` | encode, then `write_table` | |
| ---: | ---: | ---: | --- |
| 1 000 | 275 µs | 338 µs | 1.23× |
| 10 000 | 1.96 ms | 2.29 ms | 1.17× |
| 100 000 | 18.5 ms | 22.8 ms | 1.24× |

The streaming encoder is about 20 % faster as well as bounded in memory: it
avoids a `Vec` per row and a copy of the whole table.

Reading. Asking for a type costs about 2–2.6× taking the bytes:

| rows | `read_table_rows` | `read_table` | |
| ---: | ---: | ---: | --- |
| 1 000 | 606 µs | 317 µs | 1.9× |
| 10 000 | 4.65 ms | 1.81 ms | 2.6× |
| 100 000 | 46.6 ms | 19.2 ms | 2.4× |

For anything large, `read_table_streaming` avoids that cost.

Connections. A write must read its response body for `ureq` to pool the
connection ([protocol-reference.md](protocol-reference.md#connections)): without
that, a few seconds of writing left 11 623 sockets in `TIME_WAIT`; with it, 46
for the whole suite, and 23 % less time for `write_table_rows` at a thousand
rows.

Appending. `examples/append.rs` writes the same rows in the same number of
pieces, both ways. 60 000 rows in 12 pieces on the local cluster:

```text
appending     0.60s       60000 rows sent
rewriting     1.03s      390000 rows sent   (6.5× the data)
```

The data ratio is arithmetic: rewriting `k` pieces sends `(k+1)/2` times the
rows. The clock shows the cluster charging for it, though less than in
proportion, since a write is not purely bytes on the wire.

Aborting. `examples/abort.rs`: the scheduler accepted the abort in 321–405 ms,
and the operation was already `aborted` when it did. There is an `aborting`
state in between, but the HTTP call outlives it, so no caller of
`abort_operation` can observe it.

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
