# Comparing formats on a cluster: YSON, Skiff and YQL

*Planned on 13 August 2026 from the v1.0 YQL brief written the day before, at
repository state `8861036` / 0.2.6, and run on 13–14 August 2026. Phase 0 is
`yql_smoke`; phases 1 and 2 are `format_compare`. The cluster tables are in
[benchmarking.md §5](benchmarking.md#5-the-same-map-in-three-formats-and-a-query),
and the verdict on the Skiff default is
[benchmarking.md's](benchmarking.md#the-verdict).*

## Results

One task (the pilot's map, one output table, 412 554 rows / 48 MiB), eight
legs, three nine-round runs on a single-node local Docker cluster running x86-64
images under arm64 emulation, one job per leg. Every ratio is paired by round
and is `time/exec`, per-job wall time. Three independent reviews then read the
code and reproduced every leg off the cluster under a counting global
allocator. This comparison did not decide whether Skiff should be the default.

- Skiff against typed YSON, whole map: 1.10×, 1.16×, 1.20×, with the sign held
  in every round and off the cluster. With output bytes discarded, the two legs'
  in-process time differs by only 1.01–1.13×, so most of the cluster gap is
  consistent with the 38.5 MiB of output Skiff does not push through the pipe.
- Skiff against dynamic YSON (`YsonValue`), whole map: 1.85×, 1.88×, 1.93×.
  79–88 % of that gap on the cluster (88 %, 82 %, 79 % by run) and 98 % of it
  off the cluster lies between the two YSON legs, which share format, bytes,
  reader and serializer. It is a representation difference, keyed map against
  positional tuple, and must not be quoted as a format ratio.
- Wire volume is the solid result: Skiff moves 54.6 MiB in and 47.2 out, binary
  YSON 91.1 and 85.7, identical in all three runs. Off the cluster: 54.6/47.2
  and 90.7/85.7, the 0.4 MiB on the YSON input being control records rather
  than row bytes. Column names are 71 bytes of a ~122-byte payload on this row,
  which does not generalise
  ([benchmarking.md](benchmarking.md#what-the-wire-carries)).
- The comparison that would decide the default, typed YSON against typed Skiff,
  cannot be run: Skiff has no typed rows
  ([§ Skiff in a job](#skiff-in-a-job-today)).
- The decode share in the threshold's unit, job CPU, is not measured: the local
  cluster reports nothing under `user_job/cpu`. A production run is still owed
  (#70), and so is required test 5 (cluster fixtures) of
  [`skiff-compatibility.md`](skiff-compatibility.md).
- The prediction recorded before the runs was refuted in three of its four
  parts.

### The prediction and the outcome

Arithmetic over earlier results, recorded on 13 August before any run.

| Predicted | Measured |
| --- | --- |
| Skiff-dynamic decodes 3.19× faster than YSON-dynamic and `YsonValue` costs about 1.8× a typed struct, so against leg 1 Skiff's decode advantage lands near 1.7×. | Not measured as stated, and wrong as far as it can be checked. The decode-adjacent pairing off the cluster is 270 ms against 205 ms, 1.32×; the whole map is 1.10× / 1.16× / 1.20×. Both input factors were measured between representations, so their quotient does not isolate a format. |
| With decode at 10.6 % locally and 36.2 % on production, a 1.7× decode gain removes about 4 % of local job time and 15 % of production job time. | The premise held: the local decode share came out at 13 % / 13 % / 10 %. The conclusion did not: Skiff's 1.10–1.20× over the typed leg removes 9–17 % of job time, not 4 %, consistent with the output pipe, not decode. These figures are wall time; the 30 % threshold is stated over job CPU. |
| Local rounds scatter by 2×, so 4 % is not observable there: the local cluster can produce the YQL comparison but cannot resolve the Skiff delta, which is a production-cluster measurement or nothing. | Refuted in part. Pairing legs by round held the sign in all nine rounds of all three runs, locally. A local run still cannot turn that sign into a format number. |
| The write side: the recorded encode comparison is dynamic-to-dynamic while leg 1 writes through serde, and the pilot spends 43.6 % locally on validate-and-write, so the output path is where a local run may see something. | Held. Skiff pushes 38.5 MiB less output through the pipe (47.2 MiB against 85.7), and off the cluster, with output discarded, the two legs differ by only 1.01–1.13×. |

### Off-cluster reproduction

Work per row, over the same 412 554 rows:

| per row | typed YSON | Skiff | dynamic YSON |
| --- | ---: | ---: | ---: |
| allocations | 0.00 | 11.67 | 26.67 |
| clones | 0 | 8 | 8 |
| byte-slice key comparisons | ~9–18 | 0 | ~87 |

The Skiff column was counted before *stop the Skiff leg handicapping itself*
removed the eight `Value` clones; after it, 8.67 allocations and 0 clones. The
dynamic write path also runs 12 `str::from_utf8` scans a row, all wasted:
`ByteString::serialize` validates every key and every byte-string value so that
text output can use the unquoted-identifier form, and in binary both branches
emit the same bytes. Nine of the twelve are the column names; eleven succeed,
and one, `user_agent`, deliberately not UTF-8, falls through to the bytes
branch.

Medians of nine, in ms. These are in-process elapsed times on the development
machine (native arm64, reading from memory, no pipe, no process start, no
scheduler), the shape of measurement `benchmarking.md` calls a proxy and an
optimistic one. None is CPU.

| | ms |
| --- | ---: |
| `typed: frames` | 96 |
| `typed: decoded` | 270 |
| `typed: full` | 328 |
| `skiff: decoded` | 205 |
| `skiff: full` | 315 |
| `dynamic: decoded` | 524 |
| `dynamic: full` | 989 |

- Typed YSON's decode share by subtraction is 174 of 328 ms, 53 %: in-process
  time on a native machine, the worst case for YSON that `benchmarking.md` §1
  and §2 describe, not the threshold's job CPU.
- Dynamic YSON against Skiff is 3.14× end to end and 2.56× on decode alone. 98 %
  of the gap, (989 − 328) / (989 − 315), lies between the two YSON legs.
- The Skiff leg, given the dynamic YSON leg's representation, a
  `BTreeMap<Vec<u8>, Value>` keyed by the same column names, with format, codec
  and wire bytes unchanged, went from 315 ms to 594 ms, +89 %, and the ratio
  from 3.14× to 1.66×. 1.66× is still an upper bound on the format's share,
  since the named-Skiff leg does less work than the dynamic YSON one.
- The 3.19× Criterion figure in `benchmarking.md` §2 was not re-run under that
  change, which bounds it rather than replacing it.

## Method

### The four legs

| # | Leg | Reads | Writes | Isolates |
| --- | --- | --- | --- | --- |
| 1 | worker, YSON typed | borrowed serde struct | serde struct | what a job author writes today, and what §3/§4 of `benchmarking.md` measured |
| 2 | worker, YSON dynamic | `YsonValue` | `YsonValue` | same format as leg 1, nominally the same API level as leg 3, but not the same representation |
| 3 | worker, Skiff dynamic | `ytsaurus_skiff::Value` | `Value` | the format under decision, at the only API it has |
| 4 | YQL | the query's projection | `INSERT INTO` | a C++ runtime with an optimizer |

Legs 1 against 3 are the choice a job author faces today; legs 1 and 3 against
4 are the outside opinion. Leg 2 was meant as the equal-API control, since
`SkiffJobReader` yields an owned `Value` while YSON rows are borrowed. It
removes that confound and adds a larger one: a `YsonValue` map is keyed by
column name, a Skiff `Value` is positional. Both dynamic legs pay for owning
their values; leg 2 also pays for its keys, ~87 key comparisons and 12 dead
UTF-8 scans a row. Describe legs 2 and 3 as "two row representations,
one of which also changes format", never as "the format delta at equal API".
Leg 2 measures what a `YsonValue` job costs, not what YSON costs, and Skiff
numbers are interpretable only against leg 1.

YQL is a third implementation, where Criterion and the pilot compare this crate
with itself. The YQL agent splits a query into ordinary YT operations whose jobs
run YQL's own C++ compute runtime: a mature C++ baseline that costs nothing to
stand up, unlike `yt/cpp/mapreduce` built outside Arcadia. It brings an optimizer
(column projection, stage fusion) and a vectorized runtime, so it is not a C++
SDK job and does not replace an SDK-against-SDK benchmark on a real cluster.

No custom UDFs. `String::`, `Unicode::` and `Re2::` are UDF modules the YQL
agent already loads, and using them is allowed; writing a C++ UDF is a plugin
ABI and a separate project. A task that cannot be expressed with what the agent
loads is changed, not the rule.

### What the plan builds on

Each premise, checked against the tree:

| Assumed | Verdict | Where |
| --- | --- | --- |
| Query Tracker and a YQL agent run in the local cluster by default | observed on 13 August 2026: `SELECT 1` and a table-to-table `INSERT` both ran on `ghcr.io/ytsaurus/local:stable` | [`examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs) |
| `start_query` / `get_query` need no new client surface | holds | `Client::raw_command`, `raw_command_with` |
| Workers exist to mirror | holds, moved in `16915ab` | `crates/ytsaurus-job/examples/{wordcount,sessionize}.rs` |
| A job can read and write Skiff | holds, dynamic only | `WorkerReader` / `WorkerWriter` / `WorkerRow` in [`crates/ytsaurus-job/src/worker.rs`](../crates/ytsaurus-job/src/worker.rs), shown by [`skiff_cat.rs`](../crates/ytsaurus-job/examples/skiff_cat.rs) |
| An operation can be told to use Skiff | holds | `MapSpec::with_formats(DataFormat::skiff(…), …)`, see [`skiff_launch.rs`](../crates/ytsaurus-client/examples/skiff_launch.rs) |
| Rich-path `columns` for projection fairness | holds | `TablePath::columns` |
| `job_statistics` / `statistic_sum` for metrics | holds as API, fails as metric (see [The metric](#the-metric)) | `Client::job_statistics`, `job_statistic_sum` |
| Operations findable by filter | holds, but `OperationFilter::with_archive` needs an archive, which a local cluster does not have | `OperationFilter` |

### Skiff in a job today

From [`docs/skiff-compatibility.md`](skiff-compatibility.md), the contract:

| | |
| --- | --- |
| dynamic rows, reader and writer | implemented |
| typed rows, schema inference, typed `Scan`/`Write` | planned, so leg 3 is positional `Value::Tuple` with a hand-written schema, and there is no "Skiff typed" leg |
| indexes and key switch, decoding | implemented offline |
| on a real cluster | one input table, one output descriptor, a map: what `skiff_launch` settled on 2026-08-09 |
| table indexes, row/range indexes, key switches, multiple output descriptors on a cluster | open, as required test 5 (cluster fixtures) |

So the task is one input table, one output table, no key switch: a Skiff leg
outside that shape would run inside that open ground, where a failure cannot be
told from a slow result. Extending the single-table task to two inputs and two
outputs is the cheapest way to close required test 5 (cluster fixtures); it is
not part of this plan.

### The task: the pilot's map, one output

The map phase of the pilot (nine mixed-type columns, five validation rules, one
derived column) restricted to one output table. §3 and §4 of `benchmarking.md`
measured that exact job, so the new numbers join a series.

The rejects table is dropped because stock YQL cannot produce it: a rejects row
carries the offending input row's raw bytes, which a query cannot see. Bad rows
are counted, not kept.

| Stage | Legs 1–3 | Leg 4 (YQL) | Compared? |
| --- | --- | --- | --- |
| projection of 9 columns | via `TablePath::columns` / the format's schema | the `SELECT` list | yes |
| 5 validation rules | `validate()` | the same 5 as a `WHERE` | yes, on surviving rows and their count |
| derived `is_external` | referer test | the same expression | yes |
| quarantine with raw bytes | exists in the worker, out of this task | not expressible | no: a capability difference |

`wordcount` and the full `sessionize` stay as YSON-against-YQL only (map-reduce
and a key-switch reduce are outside the Skiff envelope above), used in phase 1
to prove semantic agreement cheaply.

### The metric

v1.0 collected "total job CPU time". From
[`docs/protocol-reference.md`](protocol-reference.md#custom-job-statistics):

> A local cluster reports nothing under `user_job/cpu`, so job-CPU
> comparisons cannot be run here; `time/exec` is what it does report.
> — [`docs/protocol-reference.md`](protocol-reference.md), *Custom job statistics*

Locally the results are wall clock under emulation, never CPU.
`user_job/cpu/user` is collected where the cluster offers it (the production
installation of `benchmarking.md` §4), and the harness prints which it got.
Rows and bytes are exact everywhere.

| Collected | From | Note |
| --- | --- | --- |
| operation wall time | `time/total`, `time/exec` | the metric that exists locally |
| job CPU | `user_job/cpu/user`, `user_job/cpu/system` | `None` locally; the harness prints which was obtained |
| rows and bytes in/out | `data/input/*`, `data/output/*` | where projection and format size show up |
| operations spawned | count them | one or two for a worker; for YQL, summed across all operations a query spawned |

### The estimator, and three rules

v1.0 asked for medians of 5; the plan then adopted `profile.rs`'s fastest of 5,
with its guard that refuses to report rounds in an impossible order, plus the
spread. The runs replaced both with pairing: every published ratio is paired
within its round, and the fastest-of-N survives only in the absolute columns.
Rounds are interleaved so that round *i* of every leg meets the same cluster.
`format_compare`'s module docs say to quote the paired block, not the
`vs first` columns. One warm-up per leg is discarded.

- Pair by round, never minimum against minimum. Two legs' fastest rounds can
  fall minutes apart and meet different load; the two legs of one round cannot.
  A run whose minima said "no pair is separable" gave ratios that held their
  sign in all nine rounds.
- Pin the job count. `time/exec` sums over jobs and a job start costs several
  hundred milliseconds here, so a leg left on the controller's default is
  compared on how it was scheduled.
- Read the statistics tree before summing it. `user_job/pipes/output` carries a
  `total` beside its numeric descriptors; adding both doubled every pipe figure
  the harness printed for two runs.

### Correctness before timing

Before any timing, every leg runs on the same input and every output is diffed
against leg 1's. Legs 2 and 3 must agree with leg 1 exactly: they are the same
computation in another representation, so a difference is a bug in this crate.
The diff re-encodes each row as canonical binary YSON and compares the sorted
multiset: it ignores order but catches presence, absence and multiplicity. The
three runs were taken under the earlier order-sensitive check, and the legs
passed both; no leg is required to preserve order.

YQL forces three more things on the harness:

- Input tables need a strict schema. The e2e fixtures land on schemaless
  tables, where YQL would need `WeakField` or a pragma, and Skiff needs a schema
  anyway. `Client::create_table(path, &TableSchema)` with the schema derived from
  the row struct, as [`examples/schema.rs`](../crates/ytsaurus-client/examples/schema.rs)
  does, gives all four legs one typed input table.
- `COUNT(*)` is `Uint64`; the workers emit `Int64`. Cast in the query, or the
  diff fails on type rather than on value.
- Float sums are not bit-identical: `latency_ms` accumulates in stream order in
  the worker and in YQL's chosen order. A task that sums a float needs a
  relative tolerance (`1e-9`) on float columns, which is not built.
  `canonical_rows` compares the byte-exact encoding of every column, and the
  `project` task passes `latency_ms` through unchanged, so all four legs were
  compared exactly and agreed.

### Fairness

- One input table, one schema, one sorted state, all four legs.
- Every leg reads the same columns, or YQL wins on projection alone. The query
  names all nine columns rather than the workers being narrowed with
  `TablePath::columns`; this is a property of the query text, which the harness
  does not check.
- The harness refuses query text without an `INSERT`: a bare `SELECT` is capped
  at Query Tracker's result rows and would under-measure output cost.
- It refuses query text without `PRAGMA yt.QueryCacheMode = "disable"`: phase 0
  saw a repeated query complete with zero operations.
- Memory limits are comparable and both printed: YQL needs
  `PRAGMA yt.DefaultMemoryLimit = "640M"` on this cluster, just above its own
  545 MB default, and the worker legs get 512 MB. Neither is raised to a round
  number.
- Not built: a check that a Skiff leg's schema matches the input table's. The
  schema is written by hand in two places that must agree, and a mismatch
  surfaces as a decode failure rather than as slowness.
- Not built: printing the image tag, crate versions and a hash of the query
  texts. Only the memory limits are printed, so the three runs are told apart
  in prose, not by their output.

## Driving YQL

Four commands through the escape hatch, observed on 13 August 2026 against
`ghcr.io/ytsaurus/local:stable` by
[`examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs),
which prints the bodies:

| | Command | Verb | `Repeatable` | Parameters | Answer |
| --- | --- | --- | --- | --- | --- |
| 1 | `start_query` | POST | `Never` | `{engine=yql; query=<text>; settings={…}}` | the query id |
| 2 | `get_query` | GET | `Freely` | `{query_id=…}`, optionally `attributes` | state, progress, error |
| 3 | `abort_query` | POST | `Never` | `{query_id=…}` | — |
| 4 | `list_queries` | GET | `Freely` | filters | only if step 2 does not carry the operation ids |

The verbs and `Repeatable` follow [`examples/raw.rs`](../crates/ytsaurus-client/examples/raw.rs):
pick the verb from the proxy's rule, state `Repeatable` deliberately, and decode
the body with `from_slice(body, YsonFormat::Text)`. Poll step 2 as
`Client::wait_for_operation` polls an operation. States seen:
`pending → running → completing → completed`, and `running → failing → failed`
when it does not work; only `completed`, `failed` and `aborted` are terminal.
Print a failed query's error verbatim by collecting its `message` fields: the
messages sit at the bottom of a tree of pids and trace ids, and the difference
is `Column reference '_'` against forty lines of attributes.

The spawned operation IDs are in the `get_query` answer twice: under
`progress/yql_progress/<node>/remoteId`, spelled `<proxy>/<operation id>`, and
under `progress/yql_statistics/ExecutionStatistics/yt/<node>/_id`. They are
empty for plan nodes that are not YT operations, which is every node of
`SELECT 1`. The `Operations` inside `yql_plan` (`YtMap!`, `YtMapReduce!`,
`YtPublish!`) are plan nodes, not operations. From the other end, YQL titles
each operation `YQL operation (<query id> by <user>)`, which `list_operations`'
`filter` matches, so the modelled `Client::list_operations` with
`OperationFilter::with_substring(query_id)` finds them. Read them promptly: a
local cluster has no operations archive. A first draft said neither location
held the ids; it had checked only queries that spawned no operations
(`SELECT 1`, and a repeat served from the query cache).

Two pragmas are mandatory:

```sql
PRAGMA yt.QueryCacheMode = "disable";   -- or a repeat is free and spawns nothing
PRAGMA yt.DefaultMemoryLimit = "640M";  -- or the map_reduce stage dies
```

**Without the first, the same `INSERT` run twice completes the second time
having started no operations at all, and a benchmark would report a cache hit
as a fast runtime.** For the second, YQL's own default is
`reducer.memory_limit = 545523360` (512 MB plus overhead), and the stage fails
just above it: 576M fails, 640M passes. The 512 MB the other examples give a
worker is therefore comparable. 2G, the figure in an earlier draft, would have
given YQL 3.2× the memory it needs.

`get_query_result` and `read_query_result` are not used. Results reach a table
through `INSERT INTO`; reading them through Query Tracker would measure the
display path and cap at its result-row limit. `abort_query` is there so that a
harness killed mid-run does not leave a query running on the cluster.

Query Tracker keeps its state in dynamic tables, a recorded non-goal; these
ordinary HTTP commands do not touch that path. Whether it becomes client API is
not this plan's decision: [`docs/sdk-comparison.md`](sdk-comparison.md) records
it as undecided, not excluded. For scale: `read_file` was 1824 lines across 15
files (#47), transaction detach 1864 (#45), batch requests 3371 (#44). A minimal
query surface (`start_query`, `get_query`, `wait_for_query`, `abort_query`, a
`QueryState` enum, a parsed `QueryInfo`) would be roughly 1.5–2 k lines with
wire-shape tests, a cluster example and the CHANGELOG; `list_queries` with a
filter builder and the `get_query_result` / `read_query_result` path roughly
doubles it. The harness's raw helpers are under 150 lines. Phase 0 proved the
`get_query` wire shape, as `examples/raw.rs` did for `Client::read_file` and
`read_file_streaming`; a modelled surface still lacks a second caller.
`OperationInfo` has no `brief_spec`, which nothing here needs.

## Phases

### Phase 0: smoke and plumbing, passed 13 August 2026

Run by [`examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs)
against `ghcr.io/ytsaurus/local:stable` on Docker/arm64 under emulation, plus
`skiff_launch` for the Skiff half:

| Question | Answer |
| --- | --- |
| Does this installation run YQL? | yes: `SELECT 1` and a table-to-table `INSERT` both complete |
| What is the cluster called to YQL? | `locasaurus`, and no `USE` is needed: a backtick-quoted absolute path resolves on its own. `USE locasaurus;` also works; `` `locasaurus.//tmp/…` `` does not |
| Which UDF modules load? | `Re2`, `String` and `Unicode`, so phase 1's `Re2::FindAndConsume` tokenisation stands |
| Where are the spawned operation IDs? | in `get_query`, under `progress/yql_progress/<node>/remoteId` and `…/yql_statistics/…/_id`, and through `Client::list_operations` with `with_substring(query_id)`. Two operations per `INSERT`: a `map` and a `map_reduce` |
| Does the dynamic Skiff map path work here? | yes: `skiff_launch` green, 2 rows through a real map |

The query cache and the job memory limit ([Driving YQL](#driving-yql)) came
out of phase 0. The gate, for re-running elsewhere; nothing in phases 1–3
proceeds until it passes:

1. `raw_command(Method::Post, "start_query", …)` with `engine=yql` and
   `SELECT 1`; poll `get_query` to a terminal state.
2. Record next to the observed body: the `USE <cluster>;` name (`primary` is a
   guess); where the operation IDs live, testing `remoteId` and
   `OperationFilter::with_substring(query_id)` on a query that spawns
   operations; and which UDF modules load, via a one-line `SELECT` of
   `Re2::FindAndConsume` and `String::SplitToList`.
3. `INSERT INTO … WITH TRUNCATE SELECT … FROM …` over a schematized table the
   harness created.
4. Run `skiff_cat` through `cargo run -p ytsaurus-client --example skiff_launch`
   on the same cluster, the only cluster-verified Skiff path.

Done when one command prints the query id, the ids and states of the operations
it spawned, the answers above, and a green `skiff_launch`.

### Phase 1: the queries and the modes

`sessionize` gained single-output map modes beside its `map-frames` /
`map-parse` stops:

| mode | leg |
| --- | --- |
| `map-one` | 1: typed serde, one output |
| `map-one-dynamic` | 2: `YsonValue` in and out |
| `map-one-skiff` | 3: `WorkerReader`/`WorkerWriter` on a hand-written Skiff schema |

The Skiff schema is written by hand (there is no inference) and must match the
input table's column for column: `string32` for the two byte columns, `int64`,
`uint64`, `double`, `boolean`, and a `Variant8` optional for `referer`.

Every query writes its full result with `INSERT INTO … WITH TRUNCATE`, so YQL
pays the same output cost and nothing is capped at Query Tracker's display
limit.

*project-and-filter* mirrors the modes above: the `SELECT` list is the nine
columns, the `WHERE` is the five rules, `is_external` is the referer
expression.

*wordcount* mirrors [`wordcount.rs`](../crates/ytsaurus-job/examples/wordcount.rs),
whose tokenization is runs of ASCII alphanumerics and apostrophes,
byte-oriented. It matches the runs rather than splitting, because the worker
splits on a character class and `String::SplitToList` takes a literal
separator. Sketch, to be validated against phase 0 step 2:

```sql
INSERT INTO `…/counts_yql` WITH TRUNCATE
SELECT word, CAST(COUNT(*) AS Int64) AS count
FROM (SELECT Re2::FindAndConsume(_, "[A-Za-z0-9']+")(text) AS words
      FROM `…/lines`)
FLATTEN LIST BY words AS word
GROUP BY word;
```

Without `Re2`: `Unicode::SplitToList`, then a corpus on which a
single-separator split is equivalent; the last weakens the comparison and the
report must say so.

*sessionize* mirrors the reduce over the `events` table. Semantics read off the
worker, each to be checked:

| Worker | YQL |
| --- | --- |
| new session when `timestamp - ended_at > 30 min` (µs) | `SessionWindow(timestamp, 1800000000)`: verify the boundary is `>`, not `>=` |
| `session_index`, 0-based, per user | `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start) - 1` |
| `started_at` / `ended_at` = min / max | `MIN` / `MAX` |
| `entry_url` = the row that opened the session | `MIN_BY(url, timestamp)`: check the tie-break; the worker takes stream order |
| `errors` = count of `status >= 400` | `SUM(IF(status >= 400, 1, 0))` |
| `is_mobile` = OR over the session | `BOOL_OR(is_mobile)` |
| `mean_latency_ms` = `sum / hits` | `SUM(latency_ms) / COUNT(*)`, needing the float tolerance, which does not exist yet |
| `users` table | a second `INSERT` from the sessions relation |

The e2e corpus (four lines of text, 60 synthetic users) serves the diff, not
timing. Done when all four legs agree on the e2e fixtures and every
disagreement found is written down.

### Phase 2: the harness

One Rust example that, per task, creates the schematized input once and runs
each leg over it, as under [Method](#method). Done when one command produces
one table of numbers, reproducible across two consecutive runs within the
spread it reports.

### Phase 3: where the results went

- [`docs/benchmarking.md`](benchmarking.md) §5, after "The same pilot, on a
  production cluster", with tables of its own; it had no comparison table to
  add a column to.
- Decision criterion 1: legs 2 and 3 were expected to say what changing the
  format would recover, and could not, since the criterion is over job CPU,
  which a local cluster does not report. It stands where §3 and §4 left it.
- Decision criterion 2 (the Rust job beats the C++ baseline): leg 4 is its
  first evidence, entered whichever way it came out.
- "What has *not* been measured": what YQL supplies and what it does not.
- The Skiff entry under *Status* in [`AGENTS.md`](../AGENTS.md#status), which
  says a spread from repeated production runs is still needed (#70). This plan
  owes that spread too and adds no third single number.
- [`skiff-compatibility.md`](skiff-compatibility.md) required test 5 (cluster fixtures): one input,
  one output, no key switch, so the gate stays open.

Any YQL advantage has to be decomposed into projection, runtime and stage
structure: a query reading 3 of 9 columns and winning is not a runtime result.
A single-node Docker cluster under emulation measures fixed costs with some
computation attached.

## Limits and what is still owed

- The production run on the installation of `benchmarking.md` §4 is owed
  (#70); the go/no-go is a human decision.
- Skiff on a cluster is verified for one shape only; stepping outside it turns
  a benchmark into debugging.
- Version drift: the YQL agent rides `ghcr.io/ytsaurus/local:stable`, and the
  Go SDK reference for Skiff is pinned at v0.0.33. The harness does not print
  the image tag.
- Semantic drift between implementations (session boundaries, tie breaks,
  float accumulation) is caught only by phase 1's diff, run before any timing.

## Deliverables

| | |
| --- | --- |
| [`crates/ytsaurus-client/examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs) | done: phase 0's gate and its answers, plus `YT_YQL_QUERY` for running one query verbatim |
| [`crates/ytsaurus-client/examples/format_compare.rs`](../crates/ytsaurus-client/examples/format_compare.rs) | done, all four legs, on two tasks: `wordcount` (which shuffles, and whose numbers are about plan shape) and `project` (the pilot's map at three depths, the dynamic-YSON leg, Skiff and the query). Phase 1's diff and phase 2's timings |
| [`crates/ytsaurus-job/examples/sessionize.rs`](../crates/ytsaurus-job/examples/sessionize.rs) | done: `map-one`, `map-one-dynamic`, `map-parse-dynamic`, `map-one-skiff`, `map-parse-skiff` beside `map-frames` / `map-parse` |
| the query texts | not built as files: v1.0 wanted `tests/cluster-e2e/yql/*.sql`; they are `format!` strings in `format_compare.rs`, beside the leg that checks them and reachable only from Rust |
| [`tests/cluster-e2e/README.md`](../tests/cluster-e2e/README.md) | done: a `yql_smoke` section and a `format_compare` one |
| [`docs/benchmarking.md`](benchmarking.md) | §5 and the edits listed under phase 3 |

## Changes from v1.0

| | |
| --- | --- |
| Scope | Rust-versus-YQL → four legs on one task, so the YQL numbers enter the Skiff decision |
| Added | legs 2 and 3 and their worker modes; leg 2 added a larger confound than it removed |
| Added | the pre-registered prediction, refuted in three of four parts |
| Task | wordcount + full sessionize → *project-and-filter* (the pilot's map, one output) for the four-way comparison; the other two stay YSON-versus-YQL, inside phase 1 |
| Constraint | one input, one output, no key switch: the only Skiff shape verified on a cluster |
| Metric | job CPU → `time/exec` locally, `user_job/cpu/*` only where the cluster reports it |
| Estimator | median of 5 → fastest of 5 plus the spread → paired by round; fastest-of-N only in the absolute columns |
| Sessionize scope | whole pilot → clean path; the rejects table is not expressible in stock YQL |
| Correctness bar | "byte-comparable" → exact between legs 1–3, float tolerance against leg 4 where a float is summed |
| Report target | "a third column" → `benchmarking.md` §5 plus the phase 3 edits |
| Layout | `tests/yql-comparison/` → queries in `tests/cluster-e2e/yql/`, harness as a `ytsaurus-client` example; shipped as the example, with the queries inside it |
| Worker paths | repo root → `crates/ytsaurus-job/examples/`, after `16915ab` |
| UDF rule | "no UDFs" → no *custom* UDFs; which modules load is a phase 0 question |
| Phase 0 | added the cluster's YQL name, the loaded UDF modules, a `skiff_launch` run, and polling for operation ids during the run |

## Sources

[Query Tracker](https://ytsaurus.tech/docs/ru/user-guide/query-tracker/about) ·
[YQL](https://ytsaurus.tech/docs/ru/yql/) ·
[YQL execution stages](https://ytsaurus.tech/docs/ru/yql/misc/exec_steps) ·
[Skiff](https://ytsaurus.tech/docs/en/user-guide/storage/skiff) ·
[run_local_cluster.sh](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/local/run_local_cluster.sh)
· in-tree: [`benchmarking.md`](benchmarking.md),
[`skiff-compatibility.md`](skiff-compatibility.md), [`tests/cluster-e2e/`](../tests/cluster-e2e/),
[`examples/profile.rs`](../crates/ytsaurus-client/examples/profile.rs),
[`examples/raw.rs`](../crates/ytsaurus-client/examples/raw.rs),
[`examples/skiff_launch.rs`](../crates/ytsaurus-client/examples/skiff_launch.rs)
