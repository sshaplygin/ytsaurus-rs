# Benchmarking and the Skiff decision

## Pull-request comparison

Every pull request that changes Rust code runs the four Criterion suites twice:
once at the pull request's current `main` base commit and once at the PR head.
The suites run in parallel, but each `main`/PR pair runs sequentially on one
GitHub-hosted VM with the PR's pinned Rust toolchain. That preserves a useful
per-suite delta without making the overall report wait for all four suites in
series. The workflow keeps its raw logs as an artifact for 14 days and updates
one comment on same-repository PRs with every benchmark's middle time estimate
and relative change. Fork PRs receive the same result in the job summary, where
the read-only token cannot post a comment.

Time is lower-is-better. A change of 20% or more is called out as an improvement
or regression, but does not fail the PR: shared GitHub runners are noisy and a
performance result is evidence to review, not a replacement for a controlled
measurement. Re-run the workflow before drawing a conclusion from a borderline
result.

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
dynamic value type. The accompanying **YSON vs Skiff dynamic encoding** group
constructs those same dynamic rows and emits one output-table stream in each
format.

| Case | What it does |
| --- | --- |
| `pass_through` | frame records, never decode — the identity-job floor |
| `parse_borrowed` | decode into `&str` / `&[u8]` fields |
| `parse_owned` | decode into `String` fields, copying every string column |
| `parse_dynamic` | decode into `YsonValue`, a DOM per row |
| `skiff_dynamic` | decode the equivalent schema into Skiff's dynamic `Value` tree |
| `YSON vs Skiff dynamic job API/{yson,skiff}_dynamic` | directly compare those dynamic APIs, reported in rows/sec |
| `YSON vs Skiff dynamic encoding/{yson,skiff}_dynamic` | compare dynamic row construction and encoding, reported in rows/sec |

The direct-comparison group intentionally uses **rows/sec**, not bytes/sec:
Skiff and YSON are different-sized streams by design, while the logical row
work is identical. It compares the current dynamic public APIs — positional
Skiff values against keyed YSON values — and is not a claim about a future
typed or borrowing Skiff interface.

The encoding comparison includes dynamic row construction as well as encoding:
YSON builds a keyed `YsonValue` map, and Skiff builds a positional `Value`
tuple. In both cases the timed output is one complete table stream, including
the format's record separator or table tag.

#### Direct dynamic API comparison

On the same Apple M1 Max / rustc 1.94.0 setup, the 20-sample Criterion run
decoded 100 000 identical rows per iteration:

| Format | Time | Throughput |
| --- | ---: | ---: |
| binary YSON → `YsonValue` + keyed `duration` lookup | 97.94 ms | **1.021 M rows/s** |
| Skiff → `Value` + positional `duration` lookup | 30.67 ms | **3.261 M rows/s** |

For this dynamic-job-API comparison, Skiff was **3.19×** faster. This is a
result about the current Rust implementations and their public dynamic values,
not a protocol-wide claim or a prediction for a job that uses typed YSON rows.

**And most of it is the row representation, not the format.** Measured on
2026-08-14 over 412 554 rows off-cluster, **on the whole map rather than on this
benchmark**: that pair of APIs stands at 3.14× end to end and 2.56× on decode
alone, and giving the Skiff side a `BTreeMap<Vec<u8>, Value>` keyed by the same
column names — same format, same codec, same wire bytes — takes it from 315 ms
to 594 ms, collapsing the 3.14× to 1.66×, which is itself an upper bound because
the named-Skiff leg still does less work than the `YsonValue` one. The 3.19×
above has not been re-measured under the swap; the swap bounds it rather than
replacing it. Per row the two APIs differ by 26.67 allocations against 11.67,
~87 byte-slice key comparisons against 0, and 12 `str::from_utf8` scans on the
YSON write path against none — all twelve wasted, since `ByteString::serialize`
validates so that *text* output can use the unquoted-identifier form and binary
emits the same bytes either way. Quote this pair as "keyed DOM against
positional tuple", never as "YSON against Skiff".

#### Direct dynamic encoding comparison

The same 20-sample Criterion run encoded 100 000 identical dynamic rows per
iteration, including construction of the map or tuple that the respective
encoder accepts:

| Format | Time | Throughput |
| --- | ---: | ---: |
| binary YSON `YsonValue` map → table stream | 79.16 ms | **1.263 M rows/s** |
| Skiff `Value` tuple → table stream | 29.19 ms | **3.426 M rows/s** |

For this dynamic encode path, Skiff was **2.71×** faster. As above, this is a
measurement of the current dynamic Rust APIs, including their different row
representations, rather than a protocol-wide claim.

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

### 5. The same map, in three formats and a query

§3 and §4 measured one implementation at three depths. This section puts three
formats and an outside implementation on the same task, in one harness, with the
cluster doing the timing:

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh sessionize
YT_COMPARE_TASK=project YT_COMPARE_MIB=48 YT_COMPARE_ROUNDS=9 \
    cargo run --release -p ytsaurus-client --example format_compare
```

The task is the pilot's map with the rejects table dropped — nine mixed-type
columns, five validation rules, one derived column, **one** output table, no
shuffle — over one table of 412 554 rows / 48 MiB. `data_weight_per_job` is
pinned so every leg runs in exactly one job, the rounds are interleaved so each
round's legs met the same cluster, and every leg that produces rows is diffed
against the first, once at the start of each run, before any clock is read. All
four computing legs agree row for row.

The diff is a **multiset** comparison: each row is re-encoded as canonical
binary YSON and the encodings are sorted, so a leg may write its rows in a
different order but cannot hide a missing, extra or duplicated one. The three
runs below were made against an order-sensitive version of that check, which
these legs also passed — the sort was added afterwards, because ordering is a
property no leg here is required to preserve and a check that demands it would
fail a correct leg for a reason that is not about correctness.

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

Skiff has no frames-only stop, and that is the format rather than an omission in
the harness: its stream carries no self-describing record boundaries, so finding
the end of a row *is* decoding it against the schema. The subtraction the YSON
legs support does not exist on that side.

**Where this ran, and what that settles in advance.** A single-node local Docker
cluster running x86-64 images under arm64 emulation, one job per leg — which §3
calls the environment least like production.
[`format-comparison.md`](format-comparison.md) predicted before the run, from
numbers already in this document, that a local cluster could not resolve the
Skiff delta. It was half right, and the half matters: pairing legs by round did
hold a sign in all nine rounds of all three runs, which the prediction said
would be impossible; what a local run still cannot do is turn that sign into a
decode share in criterion 1's unit. §1 of that document records the verdict
against the prediction. So what follows is organised around the three questions
these legs can actually answer, in descending order of how much of each survives
its caveats. **A production run is still owed**, and nothing here replaces it.

Three nine-round runs are reported. Every ratio is paired *within* its round:
two legs' fastest rounds can fall minutes apart and carry different weather, and
an earlier version of this that compared minimum against minimum produced ratios
the rounds themselves did not support.

**Run 3 is not a third weather sample for any Skiff row.** It ran after
`2debf16` removed eight `Value` clones a row from the Skiff mapper, so 1.93×,
1.20× and 1.49× below are all partly that fix rather than run-to-run variation,
and the drift across the three columns is not a spread. The only row here that
is three measurements of one program is `typed YSON against the dynamic leg`.

#### What the wire carries

The only part of this that needs no cluster and survives whole.

| | in | out |
| --- | ---: | ---: |
| Skiff | 54.6 MiB | 47.2 MiB |
| binary YSON | 91.1 MiB | 85.7 MiB |
| YQL | 55.0 MiB | 47.6 MiB |

**Identical in all three runs** — this is the one quantity here with no spread at
all. It was then reproduced off the cluster, byte for byte, from the generator
and the two encodings: Skiff 54.6 in and 47.2 out exactly, YSON 85.7 out
exactly, and YSON 90.7 in against the cluster's 91.1, the 0.4 MiB being control
records the cluster's stream also carries.

The two input **row** streams differ by about **92 bytes a row** — 90.7 MiB of
YSON against 54.6 MiB of Skiff over 412 554 rows, since the cluster's extra
0.4 MiB is control records and not row bytes. That figure is a **net**, and its
parts pull both ways:

- YSON adds the nine column names — **71 bytes**, repeated on every row.
- It adds about **38 bytes** of map syntax: a type marker and a length for each
  of the nine keys, nine `=`, the eight `;` between pairs, the two braces and
  the record separator.
- It gets about **17 bytes back**. Its varint integers and one-byte length
  prefixes are smaller than Skiff's fixed-width `int64`/`uint64` and its
  four-byte `string32` prefixes — `status` costs 3 bytes in YSON against 8 in
  Skiff, `bytes_sent` about 4 against 8 — and Skiff pays a two-byte table tag on
  every row that YSON does not.

71 + 38 − 17 ≈ 92, against the measured 91.8. The row's payload is **~122
bytes** — the data weight the cluster itself reports for this table, and what
summing the nine values by hand gives — so a YSON row is ~231 bytes on the wire
and a Skiff row ~139.

**Two limits on generalising, both inside that arithmetic.** The names are 71
bytes against 122 of payload because this row's nine columns are short and
numerous; a table of five wide blobs would show almost none of it. And the
17-byte credit is Skiff's fixed-width encoding *losing* to YSON on small
integers and short strings — on a table of small integers it grows, and the
direction of the whole comparison is not guaranteed. "Skiff is 40 % smaller"
quoted without the shape of the row is a statement about this schema, not about
the format.

YQL is in that table because it is the same kind of measurement: **YQL's own job
I/O is Skiff**, read off the operation spec rather than inferred. Its schema
carries `$row_index` as `variant8<nothing;int64>` and writes `is_external` as an
optional boolean where the worker writes a plain one — and that is the whole of
the difference between its 55.0/47.6 and the hand-written schema's 54.6/47.2. An
independently written positional schema landing within its system columns of the
engine's is the strongest evidence so far that the Skiff leg is right.

What none of this does is explain the timings below. Two of the legs move
byte-identical streams and differ from each other by more than either differs
from Skiff, so **bytes cannot account for most of the 1.85–1.93×**, and "and
that is the format" is not available as a reading of *that* number. Where bytes
do turn up as time is the typed-against-Skiff comparison further down, and it is
worth keeping the two apart.

#### What the format costs at equal API — and why that phrase is a trap

The dynamic-YSON leg exists so that the format could be compared at an equal API
level, `YsonValue` against `ytsaurus_skiff::Value`. It says:

| Skiff against dynamic YSON, paired by round | run 1 | run 2 | run 3 |
| --- | ---: | ---: | ---: |
| whole map | 1.85× | 1.88× | 1.93× |
| read only | 1.65× | 1.61× | — |

Every ratio in this section is `time/exec`: the cluster's per-job wall time,
summed over jobs, which is the only timing this cluster offers. It does not
include `time/prepare` — a further 650–800 ms a job, the same for every leg —
and a constant omitted from both sides makes a ratio larger than the whole-job
comparison would be. Fold prepare back in on these figures and 1.20× is about
1.15×, 1.93× about 1.7×. That arithmetic is over medians, not a measured
pairing, and it is the direction every number here would move if the metric
included the whole job.

The read-only row is not a clean representation comparison either: those two
legs move different bytes, 54.6 MiB against 91.1 through the input pipe, which
is the same 36 MiB argument that turns up on the output side below. What is
clean is the two YSON legs against each other — identical bytes, identical
reader — and that is where the argument that follows lives.

The `—` is a blank in the harness's output, and it is not recorded which kind.
`paired_ratios` prints nothing for a pair whose sign flips between rounds and
counts it as not separable, which would make the blank a result rather than a
gap; the run's output was not kept, so that cannot be confirmed here. Read the
sign claim below as scoped to the whole-map rows, where it did hold in all nine
rounds of all three runs.

Individual rounds of the whole-map row fall between 1.68× and 2.18×, and the
sign holds in every round of every run.

**That is not the format, and the phrase "at equal API" is what went wrong.**
Three independent reviews took the claim apart — by reading the code, and by
reproducing every leg off the cluster over the same 412 554 rows under a
counting global allocator:

- **79–88 % of the dynamic-versus-Skiff gap on the cluster** — 88 %, 82 % and
  79 % in the three runs, moving with the Skiff leg's own fix — and **98 % of it
  off the cluster** ((989 − 328) / (989 − 315) on the medians below) sits
  **between the two YSON legs**: identical format, identical bytes, identical
  reader, identical serializer. This document's own standing advice — that
  `YsonValue` costs about 1.8× a typed struct — is most of the answer, and the
  harness measured typed against dynamic YSON at 1.61× and 1.62× in the two runs
  that recorded it.
- The decisive experiment gave the Skiff leg the dynamic YSON leg's
  *representation* — a `BTreeMap<Vec<u8>, Value>` keyed by the same column names
  — changing nothing about the format, the codec, or the bytes on the wire. The
  leg went from 315 ms to 594 ms, **+89 %**, and the ratio collapsed from 3.14×
  to 1.66×. That 1.66× is still an **upper bound** on the format's share, since
  the named-Skiff leg does less work than the dynamic YSON one even after the
  change.

So [`format-comparison.md`](format-comparison.md)'s claim that the dynamic leg
"stops the whole thing from being a comparison of APIs wearing format labels" is
wrong, and wrong in the *other* direction: it removes one confound — borrowed
rows against owned — and introduces a larger one, a keyed map against a
positional tuple. The three legs do not do the same work per row, and the counts
say how much they differ:

| per row | typed YSON | Skiff | dynamic YSON |
| --- | ---: | ---: | ---: |
| allocations | 0.00 | 11.67 | 26.67 |
| clones | 0 | 8 | 8 |
| byte-slice key comparisons | ~9–18 | 0 | ~87 |

11.67 is what the review counted; the Skiff leg is at 8.67 since one clone came
out (below). A fourth count belongs to the crate rather than to any leg: the
dynamic write path runs **12 `str::from_utf8` scans a row, all of them wasted**.
`ByteString::serialize` validates every key and every byte-string value so that
*text* output can use the unquoted-identifier form, and in binary both branches
emit exactly the same bytes. Nine of the twelve are the column names. Eleven
succeed and one — `user_agent`, deliberately not UTF-8 — falls through to the
bytes branch, and the scan is thrown away either way.

The reproduction that produced those counts also produced timings, and they are
**in-process elapsed times on the development machine** — native arm64, reading
from memory, no pipe, no process start, no scheduler. This document calls that
shape of measurement a proxy and an optimistic one, and it is here to separate
code from cluster, not to stand in for either. No figure in this subsection is
CPU; this cluster reports no CPU and neither does an `Instant`. Medians of nine:

| | ms |
| --- | ---: |
| `typed: frames` | 96 |
| `typed: decoded` | 270 |
| `typed: full` | 328 |
| `skiff: decoded` | 205 |
| `skiff: full` | 315 |
| `dynamic: decoded` | 524 |
| `dynamic: full` | 989 |

The same subtraction §3 taught gives 174 of 328 ms on these rows — a 53 % decode
share — and that number is not criterion 1's either. It is in-process time with
no pipe, no process start and no scheduler, on a native machine: the *worst case
for YSON* that §1 and §2 already describe, arriving a third time. Criterion 1 is
a share of job CPU on a cluster that reports job CPU, and nothing in this
section is that.

The honest statement of the 1.85–1.93×: it is a measurement of **two Rust row
representations**, of which the format is responsible for at most 1.66× and on
this evidence rather less.

#### What it costs against the path a job author writes today

The typed leg is what anyone would actually write — a borrowed serde struct,
which the counts above show allocating nothing per row.

| Skiff against typed YSON, whole map, paired by round | run 1 | run 2 | run 3 |
| --- | ---: | ---: | ---: |
| | 1.10× | 1.16× | 1.20× |

The third figure is a fix, not a better day. The Skiff mapper had been cloning
eight `Value`s a row where `SkiffRow::into_value` exists and the row already owns
them — three allocations a row, 12 % of the leg — so 1.10× and 1.16× were a
floor rather than a measurement, and removing the handicap moved it to 1.20×.

**The sign survives everything**: Skiff was ahead in all nine rounds of every
run, and off the cluster too. The number does not survive as a statement about
formats:

- the Skiff decoder `Box::new()`s the `referer` variant on **every** row,
  including the ones where it is absent;
- the Skiff path reads through a `BufReader` with about 14 `read_exact` calls a
  row, where the YSON legs parse in place;
- and with the output bytes discarded, the two legs' in-process time differs by
  1.01× to 1.13× — a gap smaller than, and in the same direction as, the one the
  cluster reports.

That last one points somewhere specific: the cluster-side advantage is
consistent with being mostly the **38.5 MiB of output that Skiff does not push
through the pipe**. That is a real advantage, and it is the wire-volume section
arriving as time rather than as bytes — but it is not "the codec is faster", and
a job whose output is small would not collect it.

**The comparison that would decide the format question cannot be run by anyone
yet.** Skiff has no typed rows: [`skiff-compatibility.md`](skiff-compatibility.md)
lists typed rows, schema inference and typed `Scan`/`Write` as planned. So
1.10–1.20× is a *dynamic* Skiff leg against a *typed* YSON one, carrying the
whole of the previous section's representation confound with its sign reversed.
Today's typed YSON against a future typed Skiff is the measurement that would
settle it, and it does not exist to be run.

The outside opinion, on the same rounds:

| Skiff against the query | 1.44× | 1.47× | 1.49× |
| --- | ---: | ---: | ---: |

The harness pairs every leg against every other, and the typed-worker-against-query
pairing was printed in each run but not recorded here. What can be said from the
medians above is a quotient of two pairs — 1.44/1.10, 1.47/1.16, 1.49/1.20, so
**1.24× to 1.31×** in the worker's favour — which is arithmetic over medians of
different pairs, not a ratio any round produced, and is worth exactly that much.
The measured pairing is one `grep` away in a re-run and should replace this
before anything is decided on it. It also comes with YQL holding 640 MB against
the worker's 512 — an asymmetry the harness prints by design, and one that
flatters the query rather than the worker. YQL is also not a C++ SDK job: it
brings an optimizer and a vectorized runtime. Its usual projection advantage is
off the table here, because the harness makes every leg read the same nine
columns.

#### What this does to the decision criteria

**Criterion 1 is untouched, and cannot be moved by anything above.** It is a
decode share against a ~30 % threshold stated over **job CPU**, and this cluster
reports nothing under `user_job/cpu`; every figure in this section is per-job
wall time. The decode bucket these legs do produce — `map-parse` minus
`map-frames` — came out at 322, 281 and 308 ms across the three runs, which is
13 %, 13 % and 10 % of the whole map, on rounds ranging from 100 to 731 ms in the
noisiest run. Read it as an **upper estimate rather than a central one**: it is
a mean over only the rounds that came out in order, since the harness drops any
round where a shallower stop measured slower than a deeper one. That refusal is
`profile.rs`'s and it is right, but it is also a filter that can only remove
rounds where the noise made the difference small or negative. It sits in the
same territory as §3's 10.6 % on the same class of machine, though not over the
same denominator — §3's job wrote two output tables and subtracted minima rather
than pairing rounds — and it says nothing whatever about §4's 36.2 %. The
denominator is not job work either: roughly half of it is process start and
waiting for the first batch — a job start costs ~640 ms on this cluster and
`latency/input/time_to_first_read_batch` is a further ~504 ms of a ~2000 ms job,
with `time/prepare` another 650–800 ms that `time/exec` does not include at all.
The spread from repeated production runs that §4 asked for is still the
measurement that would settle this, and this is not it.

**Criterion 2's unit stopped working the moment a baseline existed.** It read
"if it already beats the C++ baseline on CPU per byte" until this run; the
criterion below now says *on the same logical work*, and this is why. Per byte is harmless
while both sides read the same format — bytes and rows are then proportional. It
fails as soon as the comparison spans formats, and it now does: the first
baseline this criterion has ever had is a YQL query whose **own job I/O is
Skiff**, moving 55.0/47.6 MiB where the worker legs move 91.1/85.7 for the
identical rows. Per byte the verbose format wins by being verbose — on these
figures the YSON leg is 1.20× slower on the clock and *better* per byte
(0.0132·T against 0.0183·T) — so the ranking inverts. The comparable unit is
**the same logical rows through the same pipeline**: CPU per row where the
cluster reports CPU, and per-job wall time paired by round where it does not.
Even then this section has no CPU to put in the numerator. What criterion 2 now
has, for the first time, is any evidence at all, and it points **away** from
spending the format budget: the typed YSON path is ahead of YQL's vectorized
runtime on this task, on per-job wall time on one local cluster, by something
between 1.24× and 1.31× derived from medians — it is **not** ahead of a C++ SDK
job, because no C++ SDK job has been run — and Skiff's margin over that typed
path is 1.10–1.20×, of which an unknown but large part is the pipe rather than
the codec.

Believe all of it about as far as §3 and §4:

- a single-node local Docker cluster running x86-64 images under arm64
  emulation, one job per leg — the environment least like production, and the
  reason a production run is still owed;
- rounds that scatter: the decode bucket ranged 100 to 731 ms in the noisiest
  run against a median of 308, and the whole-map ratios ranged 1.68–2.18 around
  medians of 1.85–1.93;
- and the harness was itself wrong once, in a way worth recording:
  `user_job/pipes/output` carries a `total` beside its numeric descriptors, and
  summing both doubled every pipe figure it printed for two runs before that was
  found.

**What it does not close.** The Skiff leg exercised nine columns, mixed types and
an optional variant — more shape than `skiff_launch` covered on 2026-08-09 — but
it is still one input table, one output table and no key switch.
[`skiff-compatibility.md`](skiff-compatibility.md)'s **required test 4 stays
open**, and none of this should be read as if it did not.

## What has *not* been measured

The part that really settles it, because it needs a cluster:

- a ≥ 10 GB table with a realistic schema,
- the same job in **C++** (`yt/cpp/mapreduce`) and **Python** for comparison.
  *Partly supplied, 2026-08-14*: [`format-comparison.md`](format-comparison.md)
  runs the pilot's map as a YQL query, whose jobs are YQL's own C++ runtime, on
  the same table and agreeing with the workers row for row (§5 states what that
  diff checks) — the first outside opinion this document has had. It is **not** the C++ SDK job asked for here: it has an
  optimizer, a vectorized runtime and Skiff job I/O of its own. Python remains
  entirely unmeasured.
- **job cpu time**, **operation wall time** and **RSS** as YTsaurus reports them.
  Job CPU is still missing everywhere the format comparison ran — that cluster
  reports nothing under `user_job/cpu` — which is why criterion 1 above cannot
  be moved by it.
- **a typed Skiff leg.** Skiff has no typed rows
  ([`skiff-compatibility.md`](skiff-compatibility.md) lists them planned), so the
  comparison that would decide this document's question — today's typed YSON
  against a typed Skiff — is unmeasurable by anyone today. Every Skiff figure on
  record is a dynamic-API figure.

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
(`ytsaurus-client`) over the same table. Compare `user_job/cpu/user` **per
row**, not per byte. Per byte is harmless only while every job in the comparison
reads the same format; the moment one of them does not — as YQL, the first
baseline that actually arrived, does not — a per-byte figure rewards whichever
job moves more bytes and ranks the formats backwards. Per row is the unit that
survives both cases. `data/input/data_weight` is still worth recording — it is
what makes the format's size difference a number — but it is a reported
quantity, not a denominator.

## Decision criteria

Implement `ytsaurus-skiff` if **both** hold:

1. **Parsing is the bottleneck.** `parse_borrowed - pass_through`, or the
   equivalent measured on the cluster, exceeds ~30 % of job CPU. Below that,
   Skiff optimises something that is not the problem. *The one workload measured
   this way so far — the pilot — came out at ~10 % on a local cluster (§3) and
   36 % on a production one (§4). This criterion is therefore **not met and not
   missed**: it is unmeasured, and a spread across repeated production runs is
   what would settle it.*

   **The threshold's unit is the obstacle, and it is now explicit.** It is
   stated over **job CPU**; the local cluster reports nothing under
   `user_job/cpu`, so everything measured there — §3's 10.6 % and every figure
   in [`format-comparison.md`](format-comparison.md) — is per-job **wall** time
   over a denominator roughly half of which is process start and waiting for
   the first batch: a job start costs ~640 ms on that cluster and
   `latency/input/time_to_first_read_batch` a further ~504 ms of a ~2000 ms job,
   with `time/prepare` another 650–800 ms that `time/exec` does not even
   include. Three nine-round runs of the format comparison put the decode bucket
   at 13 % / 13 % / 10 % of the whole map in that unit, and **none of those
   numbers can move this criterion**, in either direction. Only a cluster that
   reports CPU can.
2. **The Rust job is not already fast enough.** If it already beats the C++
   baseline on the same logical work, the remaining headroom is unlikely to
   justify a second wire format, its schema negotiation, and the ongoing
   compatibility burden.

   **Restated, because the unit stopped working the moment a baseline existed.**
   This criterion used to say *CPU per byte*. Per byte is harmless while both
   sides read the same format — bytes and rows are then proportional. It fails
   as soon as the comparison spans formats, and it now does: the first baseline
   this criterion has ever had is a YQL query whose **own job I/O is Skiff**,
   moving 55.0/47.6 MiB where the worker legs move 91.1/85.7 for the identical
   rows. Per byte the verbose format wins by being verbose — on these figures
   the YSON leg is 1.20× slower on the clock and *better* per byte, 0.0132·T
   against 0.0183·T — so the ranking inverts. The comparable unit is **the same
   logical rows through the same pipeline**: CPU per row where the cluster
   reports CPU, and per-job wall time paired by round where it does not, with
   the legs interleaved so each round meets the same cluster.
   [`format-comparison.md`](format-comparison.md) is the first evidence this
   criterion has ever had — a YQL query whose jobs run YQL's C++ runtime, on the
   pilot's map, agreeing row for row with the workers under the diff §5
   describes. What it says is that the
   typed YSON path is ahead of YQL's vectorized runtime on this task, on per-job
   wall time on one local cluster, by something between 1.24× and 1.31× derived
   from medians of different pairs rather than from a measured pairing — with
   YQL holding 640 MB against the worker's 512 MB, an asymmetry the harness
   prints and does not correct — and one that favours the query, so it is not
   what put the worker ahead. It is
   **not** ahead of a C++ SDK job; no C++ SDK job has been run. Read it with two
   further caveats: YQL brings an optimizer and a vectorized runtime, and its
   own job I/O turned out to be Skiff, so it is not a YSON baseline either.

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
