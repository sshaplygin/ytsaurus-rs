# Comparing formats on a cluster: YSON, Skiff and YQL

*A plan. Adapted on 13 August 2026 from the v1.0 YQL brief written the day
before, then widened the same day so the YQL numbers land **inside** the Skiff
decision rather than beside it. Repository state: `8861036` / 0.2.6. §8 lists
every change from v1.0 and why. No new crates and no protocol work — worker
modes, queries, a harness, and a report.*

## 0. The question

[`docs/benchmarking.md`](benchmarking.md) exists to answer one thing: is YSON
decoding a large enough share of job cost to justify Skiff. Today its evidence
is of two kinds, and neither settles it:

- **In-process Criterion benchmarks** — Skiff decodes 3.19× and encodes 2.71×
  faster than YSON *through the dynamic APIs*. Real, reproducible, and about
  two Rust libraries rather than about a job — and, as the 14 August adversarial
  pass showed, about two *row representations* rather than two formats. That
  pass did not re-run this benchmark. It ran the whole map, nine columns,
  412 554 rows, off-cluster, where the same pair of APIs stands at 3.14× end to
  end and 2.56× on decode alone; giving the Skiff side a
  `BTreeMap<Vec<u8>, Value>` keyed by the same column names — format, codec and
  wire bytes unchanged — took it from 315 ms to 594 ms and collapsed that 3.14×
  to 1.66×. The 3.19× above has not itself been re-measured under the swap;
  treat the swap as bounding it, not as replacing it.
- **The pilot on a cluster** — decoding is 10.6 % of the job locally and
  36.2 % on a production installation, either side of the 30 % threshold, on
  rounds that scatter by 2×. The document's own words: the question is **open**.

Neither has ever been checked against a **third implementation**. Criterion
compares this crate with itself; the pilot compares this crate with itself at
three depths. That is the gap YQL fills: the YQL agent splits a query into
ordinary YT operations whose jobs run YQL's own C++ compute runtime, so it is a
mature C++ baseline that costs nothing to stand up — against the known pain of
building `yt/cpp/mapreduce` outside Arcadia.

So the comparison is not "Rust versus YQL". It is **one task, one input table,
one cluster, four ways of reading and writing it**, of which two are the formats
under decision and one is the outside opinion.

**What this is not.** YQL brings an optimizer — column projection, stage fusion
— and a vectorized runtime, so it is not "a C++ SDK job". It does not replace
the SDK-versus-SDK benchmark on a real cluster, and the report must say so.

**No custom UDFs, by decision.** `String::`, `Unicode::` and `Re2::` are UDF
modules the YQL agent already loads, and using them is inside the rule. Writing
a C++ UDF is a plugin ABI and a separate project. If a task cannot be expressed
with what the agent loads, **the task changes, not the rule**.

## 1. The four legs

| # | Leg | Reads | Writes | Isolates |
| --- | --- | --- | --- | --- |
| 1 | **worker, YSON typed** | borrowed serde struct | serde struct | what a job author writes today, and what §3/§4 of `benchmarking.md` already measured |
| 2 | **worker, YSON dynamic** | `YsonValue` | `YsonValue` | the intended control: same format as leg 1, nominally the same API level as leg 3 — but **not the same representation**, which is what the run found (see below) |
| 3 | **worker, Skiff dynamic** | `ytsaurus_skiff::Value` | `Value` | the format under decision, at the only API it has |
| 4 | **YQL** | the query's projection | `INSERT INTO` | a C++ runtime with an optimizer |

Legs **1 against 3** are the decision a job author would actually face today.
Legs **1 and 3 against 4** are the outside opinion. Leg **2 against 3** was
designed as the format delta at equal API — the cluster-side version of the
Criterion numbers.

**That design is refuted, and the refutation is the finding.** The argument for
leg 2 was that `SkiffJobReader` yields an **owned** `Value` while YSON rows are
borrowed and byte-exact, so without leg 2 every Skiff result is confounded — and
that `benchmarking.md` says so in its own words, *"do not compare the dynamic
Skiff result directly with YSON's borrowed-Serde result"*. Leg 2 does remove the
borrowed-versus-owned confound. It introduces a **larger** one in its place: a
`YsonValue` map is keyed by column name and a Skiff `Value` is positional, so
leg 2 versus leg 3 also compares ~87 byte-slice key comparisons a row against 0,
26.67 allocations a row against 11.67, and 12 wasted `str::from_utf8` scans a
row on the write path against none — `ByteString::serialize` validates every key
and every byte-string value so that *text* output can use the
unquoted-identifier form, and in binary both branches emit the same bytes, so
the eleven that succeed are as wasted as the one that fails. Measured:
**79–88 % of the leg-2-to-leg-3 gap on the cluster — 88 %, 82 % and 79 % across
the three runs — and 98 % of it off-cluster, sits between the two YSON legs** —
identical format, identical bytes, identical reader and serializer. The decisive
experiment gave the Skiff leg the dynamic YSON leg's representation and changed
nothing else: 315 ms → 594 ms, and 3.14× → 1.66×, itself still an upper bound
because the named-Skiff leg does less work than leg 2 does.

So leg 2 is not the control this section claimed. It is a **second, differently
handicapped API**, and the sentence to use about legs 2 and 3 is "two row
representations, one of which also changes format" — never "the format delta at
equal API". The leg that answers the question this document asks is **leg 1
against leg 3**, and the one that would settle it — today's typed YSON against a
typed Skiff — cannot be run by anyone, because Skiff has no typed rows (§3.2).

### What the existing numbers predict

Worth writing down **before** the run, so the measurement can disagree with it.
This is arithmetic over recorded results, not a measurement.

*Pre-registered 13 August 2026 and kept verbatim. The verdicts were added on
14 August, after three nine-round runs and an adversarial pass. Not one of the
four came through as written; that is what pre-registering is for, and none of
it is deleted.*

- Skiff-dynamic decodes 3.19× faster than YSON-dynamic; `YsonValue` costs about
  1.8× a typed struct. So against **leg 1** — the path anyone would actually
  write — Skiff's decode advantage should land near **1.7×**, not 3.19×.

  **Not measured as stated, and wrong as far as it can be checked.** The
  prediction is about decode; the decode-adjacent pairing off-cluster is 270 ms
  against 205 ms, **1.32×**, not 1.7×. On the whole map the measured figures are
  1.10× / 1.16× / 1.20×. The arithmetic was invalid in a way worth naming: both
  input factors were measured between *representations*, so dividing one by the
  other does not leave a format behind.
- The pilot's decode share is 10.6 % locally and 36.2 % on production. A 1.7×
  decode improvement therefore removes roughly **4 % of local job time** and
  **15 % of production job time**.

  **Its input survived; its conclusion did not, and its unit was wrong.** The
  local decode share came out at 13 % / 13 % / 10 %, which is the 10.6 % this
  bullet assumed, so the 4 % follows validly from its own premises. What the run
  found is that Skiff's whole-map advantage over the typed leg is 1.10–1.20× —
  it removes 9–17 % of job time, not 4 % — and that the gain is consistent with
  the output pipe rather than with decode. The prediction under-shot the size
  and mis-named the cause. And the unit does not carry: this is per-job **wall**
  time, because the cluster reports nothing under `user_job/cpu`, while the
  10.6 %/36.2 % figures and the 30 % threshold are stated over job CPU.
- Local rounds scatter by 2×. **4 % is not observable there.** The Skiff leg is
  a production-cluster measurement or it is nothing, and the local run's job is
  to prove the harness works and to produce the YQL comparison.

  **Refuted in part, by a better estimator.** Pairing legs by round — the legs
  interleaved so round *i* of each met the same cluster — held the sign in all
  nine rounds of all three runs, locally. What the local run cannot do is turn
  the sign into a format number; a production run is still owed.
- The write side is weaker still as a prediction: the recorded encode
  comparison is dynamic-to-dynamic, and leg 1 writes through serde. Locally the
  pilot spends 43.6 % on validate-and-write, so the output path is where the
  local run may still see something — which is the one local result worth
  waiting for.

  **Stands, and it is where the leg-1-versus-leg-3 gap now looks likely to
  live**: Skiff pushes 38.5 MiB less output through the pipe (47.2 MiB against
  binary YSON's 85.7), and off-cluster, with output discarded, the two legs'
  in-process time differs by only 1.01–1.13×.

If the run contradicts any of this, the contradiction is the finding.

## 2. What is already here

Every premise re-checked against the tree:

| Assumed | Verdict | Where |
| --- | --- | --- |
| Query Tracker and a YQL agent run in the local cluster by default | **observed, 13 August 2026** — `SELECT 1` and a table-to-table `INSERT` both ran on `ghcr.io/ytsaurus/local:stable` | [`examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs) |
| `start_query` / `get_query` need no new client surface | **holds** | `Client::raw_command`, `raw_command_with` |
| Workers exist to mirror | **holds, moved** in `16915ab` | `crates/ytsaurus-job/examples/{wordcount,sessionize}.rs` |
| A job can read and write Skiff | **holds, dynamic only** | `WorkerReader` / `WorkerWriter` / `WorkerRow` in [`crates/ytsaurus-job/src/worker.rs`](../crates/ytsaurus-job/src/worker.rs), demonstrated by [`skiff_cat.rs`](../crates/ytsaurus-job/examples/skiff_cat.rs) |
| An operation can be told to use Skiff | **holds** | `MapSpec::with_formats(DataFormat::skiff(…), …)`, see [`skiff_launch.rs`](../crates/ytsaurus-client/examples/skiff_launch.rs) |
| Rich-path `columns` for projection fairness | **holds** | `TablePath::columns` |
| `job_statistics` / `statistic_sum` for metrics | **holds as API, fails as metric** — §3.1 | `Client::job_statistics`, `job_statistic_sum` |
| Operations findable by filter | **holds, with a catch** — `OperationFilter::with_archive` needs an archive, *which a local cluster does not have* | `OperationFilter` |
| "A third column in `docs/benchmarking.md`" | **there was no column to be third of** — four narrative sections, no comparison table. Phase 3 added a fifth section with tables of its own rather than a column to an existing one | phase 3 |

Two shapes to copy rather than reinvent:
[`examples/profile.rs`](../crates/ytsaurus-client/examples/profile.rs) — generate
input, run N rounds, read the scheduler's own numbers, and **refuse to report**
when the rounds cannot separate what is being measured; and
[`examples/raw.rs`](../crates/ytsaurus-client/examples/raw.rs) — pick the verb
from the proxy's rule, state `Repeatable` deliberately, decode the body with
`from_slice(body, YsonFormat::Text)` yourself. That fixes both new calls:
`start_query` is **POST** and `Repeatable::Never`; `get_query` is **GET** and
`Repeatable::Freely`.

### Driving YQL: the loop, and what stays out of it

The whole of the YQL side is four commands through the escape hatch. These were
**observed** on 13 August 2026 against `ghcr.io/ytsaurus/local:stable` by
[`examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs),
which prints the bodies rather than only what it makes of them:

| | Command | Verb | `Repeatable` | Parameters | Answer |
| --- | --- | --- | --- | --- | --- |
| 1 | `start_query` | POST | `Never` | `{engine=yql; query=<text>; settings={…}}` | the query id |
| 2 | `get_query` | GET | `Freely` | `{query_id=…}`, optionally `attributes` | state, progress, error |
| 3 | `abort_query` | POST | `Never` | `{query_id=…}` | — |
| 4 | `list_queries` | GET | `Freely` | filters | only if step 2 does not carry the operation ids |

Poll step 2 the way `Client::wait_for_operation` polls an operation. The states
seen: `pending → running → completing → completed`, and `running → failing →
failed` when it does not work — so `failing` and `completing` are on the way and
only `completed` / `failed` / `aborted` are terminal. A **failed query's error
must be printed verbatim** — a YQL error is the most likely way phase 1 fails,
and the messages sit at the bottom of a tree of pids and trace ids, so the
harness collects `message` fields rather than dumping the tree. That is the
difference between `Column reference '_'` and forty lines of attributes.

**Where the operation IDs are.** In the `get_query` answer, twice: under
`progress/yql_progress/<node>/remoteId`, spelled `<proxy>/<operation id>`, and
under `progress/yql_statistics/ExecutionStatistics/yt/<node>/_id`. They are
empty for plan nodes that are not YT operations, which is every node of
`SELECT 1`. The `Operations` inside `yql_plan` are a different thing — plan
nodes, `YtMap!`, `YtMapReduce!`, `YtPublish!` — and are not what to read.

And they are findable from the other end: YQL titles each operation `YQL
operation (<query id> by <user>)`, and `list_operations`' `filter` matches it,
so the **modelled** `Client::list_operations` with
`OperationFilter::with_substring(query_id)` is the lookup. No `raw_command`
detour, and nothing for the harness to scan by hand. Read soon rather than
later: a local cluster has no operations archive.

*Both of those were wrong in the first draft of this document, in the same
way, and the way is worth keeping: every check behind them was run against a
query that had **spawned no operations** — `SELECT 1`, which runs inside the
agent, and a repeat served from the query cache. "There is no operation id
here" was a true statement about a query that had none. Re-checked against a
query whose operations existed, both reversed.*

**Two pragmas are mandatory, and one of them decides whether the benchmark is
real at all:**

```sql
PRAGMA yt.QueryCacheMode = "disable";   -- or a repeat is free and spawns nothing
PRAGMA yt.DefaultMemoryLimit = "640M";  -- or the map_reduce stage dies
```

The first was found the hard way: run the same `INSERT` twice and the second
completes having started **no operations at all**. A benchmark without it would
have measured a cache hit and reported it as a fast runtime.

The second is a smaller correction than it first looked. YQL's own default is
`reducer.memory_limit = 545523360` — 512 MB plus overhead — and the stage fails
just above it: **576M fails, 640M passes**. So the fairness requirement is not
"give the Rust side gigabytes to match"; the two sides already sit at the same
order, and the 512 MB the other examples give a worker is comparable. An
earlier draft of this document said 2G, which would have handed YQL 3.2× the
memory it needs and called the result a comparison.

Two commands are deliberately **not** used: `get_query_result` and
`read_query_result`. Results reach a table through `INSERT INTO`, so reading
them back through Query Tracker would measure the display path and cap at its
result-row limit — the exact under-measurement §4 phase 2 refuses queries
without an `INSERT` to prevent. `abort_query` exists in the list for one reason:
a harness killed mid-run must not leave a query burning cluster.

**What this does not touch.** Query Tracker keeps its own state in dynamic
tables, and [`docs/go-parity.md`](go-parity.md) excludes the Go SDK's
`query-tracker` example on exactly that basis — the dynamic-table data path is a
recorded non-goal. Nothing here goes near it: these are ordinary HTTP commands
to the proxy, and the non-goal stays intact.

**Whether this ever becomes client API is not this plan's decision.**
[`docs/sdk-comparison.md`](sdk-comparison.md) records Query Tracker as
**"undecided, not excluded"**, and hard rule 5 keeps it that way until a human
says otherwise. `raw_command` is precisely what the escape hatch is for, and it
is enough for everything here. If the numbers — or a user — later make a
modelled surface worth having, it is about five commands and a state enum, and
it is a separate decision made on purpose rather than one that arrives as a
side effect of a benchmark.

What it would cost, measured against what this repository has actually shipped:
`read_file` was **1824 lines** across 15 files (#47), transaction detach 1864
(#45), batch requests 3371 (#44). A minimal query surface — `start_query`,
`get_query`, `wait_for_query`, `abort_query`, a `QueryState` enum and a parsed
`QueryInfo` — lands in the same bracket, roughly **1.5–2 k lines** including the
wire-shape tests, a self-checking cluster example and the CHANGELOG this house
requires; adding `list_queries` with a filter builder and the
`get_query_result` / `read_query_result` read path roughly doubles it. The
harness's raw helpers are **under 150 lines** by comparison.

Which is why the order matters more than the total. `Client::read_file` and
`read_file_streaming` **grew out of `examples/raw.rs`** — the raw call proved
the wire shape first, and the modelled API was written against an observed
answer rather than a guessed one, which is what [`examples/raw.rs`](../crates/ytsaurus-client/examples/raw.rs)
says about itself. Phase 0 has now done that for `get_query`, so the shapes are
on record; what is still missing before a modelled surface is worth writing is a
second caller.

**What phase 0 says about the API decision.** Less than it first appeared. An
earlier draft reported a gap — that identifying a query's operations needs the
operation title, which `OperationInfo` does not carry — and that was wrong:
the cluster's `filter` matches the title server-side, so the modelled
`Client::list_operations` does the job unchanged. `OperationInfo` still has no
`brief_spec`, and that is still a real absence, but nothing here needs it.

What the escape hatch actually made awkward is smaller and more specific, and
it is the list a modelled surface would have to get right: a terminal-state
predicate kept separate from "the wait ran out" (conflating them made this
example report a successful query as a failure); the crate's
outer-plus-innermost error flattening, which **was** `pub(crate)` in `jobs.rs`
and which every raw caller otherwise reinvented worse — it is `error_summary` on
the crate root as of this branch; and a poll interval and
timeout as parameters rather than constants. Four commands and a state enum,
none of it urgent.

## 3. Adaptations

### 3.1 The headline metric is not available where the plan runs

v1.0 collects "total job CPU time". This repository already knows better:

> A local cluster reports **nothing under `user_job/cpu`**, so job-CPU
> comparisons cannot be run here; `time/exec` is what it does report.
> — [`AGENTS.md`](../AGENTS.md), *Built-in statistics*

So: locally the harness collects `time/exec` and `time/total` and the report
says *wall clock under emulation*, never "CPU". `user_job/cpu/user` is collected
**where the cluster offers it** — the production installation of §4 — and the
harness prints which of the two it got. Rows and bytes (`data/input/*`,
`data/output/*`) are exact everywhere and are what makes YQL's projection
advantage a number rather than an inference.

Combined with §1's arithmetic, this is the plan's central scheduling fact:
**the local cluster can produce the YQL comparison but cannot resolve the Skiff
delta.** Do not let a local run be written up as if it had.

### 3.2 What Skiff can and cannot do in a job today

From [`docs/skiff-compatibility.md`](skiff-compatibility.md), which is the
contract and is explicit:

| | |
| --- | --- |
| dynamic rows, reader and writer | **implemented** |
| typed rows, schema inference, typed `Scan`/`Write` | **planned** — so leg 3 is positional `Value::Tuple` with a hand-written schema, and there is no "Skiff typed" leg to have |
| indexes and key switch, decoding | implemented **offline** |
| on a real cluster | **one input table, one output descriptor, a map** — that is the whole of what `skiff_launch` settled on 2026-08-09 |
| table indexes, row/range indexes, key switches, multiple output descriptors **on a cluster** | **open**, and named as required test 4 |

This decides the task. A Skiff leg that needs two outputs or a key switch would
be running inside required test 4's open ground, where a failure is
indistinguishable from a slow result. **The comparison task must therefore be
one input table, one output table, no key switch.**

That is not a loss. It has a second payoff: a two-in/two-output Skiff shape is
exactly what gate 4 wants, so once the single-table comparison works, extending
it is the cheapest way anyone will ever close that gate. Say so in the report;
do not do it inside this plan.

### 3.3 The task: the pilot's map, one output

The workload is the **map phase of the pilot** — nine mixed-type columns, five
validation rules, one derived column — restricted to a single output table.
Chosen over inventing a new one because §3 and §4 of `benchmarking.md` already
measured that exact job, so the new numbers join a series instead of starting
one.

The rejects table is dropped for this task, and that is a scoped decision with a
reason beyond convenience: **stock YQL cannot produce it at all.** The rejects
row carries the offending input row's raw bytes, which is not a value a query
can see. Bad rows are counted, not kept.

| Stage | Legs 1–3 | Leg 4 (YQL) | Compared? |
| --- | --- | --- | --- |
| projection of 9 columns | via `TablePath::columns` / the format's schema | the `SELECT` list | **yes** |
| 5 validation rules | `validate()` | the same 5 as a `WHERE` | **yes**, on surviving rows and their count |
| derived `is_external` | referer test | the same expression | **yes** |
| quarantine with raw bytes | exists in the worker, out of this task | **not expressible** | **no** — recorded as a capability difference |

`wordcount` and the full `sessionize` stay in the plan as **YSON-versus-YQL
only** (map-reduce and a key-switch reduce are outside §3.2's envelope), and
they are what phase 1 uses to prove semantic agreement cheaply.

### 3.4 What YQL forces on the harness

- **Input tables need a strict schema.** The e2e fixtures land on schemaless
  tables; YQL would need `WeakField` or a pragma, and Skiff needs a schema
  anyway. `Client::create_table(path, &TableSchema)` with the schema derived
  from the row struct — as [`examples/schema.rs`](../crates/ytsaurus-client/examples/schema.rs)
  does — gives all four legs one properly typed input table.
- **`COUNT(*)` is `Uint64`; the workers emit `Int64`.** Cast in the query, or
  the diff fails on type rather than on value.
- **Float sums will not be bit-identical.** `latency_ms` accumulates in stream
  order in the worker and in whatever order YQL chooses. The diff would need a
  relative tolerance (`1e-9`) on float columns and exact equality elsewhere.
  v1.0's "byte-comparable" is the wrong requirement for exactly one column.

  **Not built, and not needed by the task that ran.** `canonical_rows` compares
  the byte-exact encoding of every column. The `project` task passes
  `latency_ms` through unchanged rather than summing it, so no leg ever
  accumulates a float and the four agreed exactly — the tolerance was designed
  for the session aggregation of §5's table, which no leg computes. Anything
  that does sum a float has to add it before it can be compared.

### 3.5 The estimator and the layout the repo already uses

- v1.0 says medians of 5. The house estimator is **the fastest of 5** with a
  guard that refuses to report when rounds come out in an impossible order —
  `profile.rs`, whose default moved from 3 to 5 because 3 did not survive a
  shared cluster. **Report the spread as well as the minimum**: two readings
  that disagree by 2× are what §3 and §4 of `benchmarking.md` are about.

  **Superseded by what the runs found.** The fastest-of-N survives only in the
  absolute columns. Every ratio the harness publishes is paired *within* its
  round, because two legs' fastest rounds can fall minutes apart and carry
  different weather while the two legs of one round cannot; a run whose minima
  said "no pair is separable" gave ratios that held their sign in all nine
  rounds. `format_compare`'s module docs tell a reader to quote that block and
  not the `vs first` columns.
- v1.0 proposes `tests/yql-comparison/`. The repo's habit is cluster-driving
  code as a Rust example under `crates/ytsaurus-client/examples/`, fixtures and
  shell under `tests/cluster-e2e/`, and every item ending in a cluster example that
  checks itself. Follow it — §7.

## 4. Phases

### Phase 0 — smoke and plumbing (gate) — **passed, 13 August 2026**

Run by [`examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs)
against `ghcr.io/ytsaurus/local:stable` on Docker/arm64 under emulation, plus
`skiff_launch` for the Skiff half. What it answered:

| Question | Answer |
| --- | --- |
| Does this installation run YQL? | **yes** — `SELECT 1` and a table-to-table `INSERT` both complete |
| What is the cluster called to YQL? | it is `locasaurus`, and **no `USE` is needed** — a backtick-quoted absolute path resolves on its own. `USE locasaurus;` also works; `` `locasaurus.//tmp/…` `` does **not** |
| Which UDF modules load? | **`Re2`, `String` and `Unicode`** — so phase 1's `Re2::FindAndConsume` tokenisation stands |
| Where are the spawned operation IDs? | **in `get_query`**, under `progress/yql_progress/<node>/remoteId` and `…/yql_statistics/…/_id`; and findable from the other end through the modelled `Client::list_operations` with `with_substring(query_id)`. Two operations per `INSERT`: a `map` and a `map_reduce` |
| Does the dynamic Skiff map path work here? | **yes** — `skiff_launch` green, 2 rows through a real map |

Two findings changed the plan rather than confirming it: the **query cache**
(§"Driving YQL"), which makes a repeated query free and would have silently
falsified every timing, and the **job memory limit**, which now belongs to the
fairness checklist.

The original gate, kept for anyone re-running this elsewhere:

**Nothing in phases 1–3 may be built before this passes.** If the local image
does not run queries, the plan stops and gets reported, not worked around.

1. `raw_command(Method::Post, "start_query", …)` with `engine=yql` and
   `SELECT 1`; poll `get_query` to a terminal state.
2. Record, in the harness, next to the observed body:
   - **what the cluster is called to YQL** — the `USE <cluster>;` name.
     `primary` is the guess, not the answer.
   - **where the spawned operation IDs live** — `remoteId` in the query's
     progress, and `OperationFilter::with_substring(query_id)` from the other
     end (§"Driving YQL"). Test both rather than trusting either: the first
     draft of this document got both wrong by checking them against a query
     that spawned nothing. The archive is not an option on a local cluster.
   - **which UDF modules the agent loads** — one-line `SELECT` of
     `Re2::FindAndConsume` and `String::SplitToList`. Phase 1's tokenization
     depends on the answer.
3. `INSERT INTO … WITH TRUNCATE SELECT … FROM …` over a schematized table the
   harness created.
4. **The Skiff half of the gate**: run `skiff_cat` through
   `cargo run -p ytsaurus-client --example skiff_launch` on the same cluster.
   It is the only cluster-verified Skiff path; if it does not pass here, leg 3
   is not blocked by this plan's code and knowing that early is worth the two
   minutes.

**DoD**: one command prints the query id, the ids and states of the operations
it spawned, the three answers above, and a green `skiff_launch`.

### Where this stands, 14 August 2026

Phases 1 and 2 are built and have run, three times, and the numbers below **have
now been through an adversarial pass** — three independent agents, code-reading
plus an off-cluster reproduction of every leg over the same 412 554 rows under a
counting global allocator. It refuted more than it confirmed, and the
refutations are recorded here and in §1 rather than in a phase-3 report that
would have carried the wrong claims into `benchmarking.md`.

**On the `project` task** — the pilot's map, 412 554 rows / 48 MiB, one job per
leg (`data_weight_per_job` pinned), one output table, no shuffle, rounds
interleaved, outputs diffed row for row once at the start of each run and before
any clock was read, all four computing legs agreeing. **Three nine-round runs**,
every ratio paired by round:

*The diff these runs passed compared the rows exactly and in order. The harness
now sorts canonical binary-YSON encodings and compares the multiset instead —
strictly weaker on ordering, identical on presence, absence and multiplicity —
because no leg here is required to preserve the input's order, and a check that
demands it would fail a correct leg for a reason that is not about correctness.
Re-running these numbers gets the weaker check; the numbers themselves were
taken under the stronger one.*

| paired by round | run 1 | run 2 | run 3 |
| --- | ---: | ---: | ---: |
| Skiff against the **dynamic** YSON leg, whole map | 1.85× | 1.88× | 1.93× |
| Skiff against the dynamic leg, read only | 1.65× | 1.61× | — |
| Skiff against the **typed** YSON leg, whole map | 1.10× | 1.16× | **1.20×** |
| Skiff against the query | 1.44× | 1.47× | 1.49× |
| typed YSON against the dynamic leg | 1.61× | 1.62× | — |
| decoding, by subtraction | 322 ms, 13 % | 281 ms, 13 % | 308 ms, 10 % |

**The metric.** Every ratio here is `time/exec`: the cluster's per-job wall
time, summed over jobs, which is the only timing this cluster offers. It does
not include `time/prepare` — a further 650–800 ms a job, the same for every leg
— and a constant omitted from both sides makes a ratio larger than the whole-job
comparison would be. Fold prepare back in on these figures and 1.20× is about
1.15×, 1.93× about 1.7×. That arithmetic is over medians, not a measured
pairing, and it is the direction every number here would move if the metric
included the whole job.

**Run 3 is not a third weather sample for any Skiff row.** It ran after
*stop the Skiff leg handicapping itself* removed eight `Value` clones a row
from the Skiff mapper, so 1.93×,
1.20× and 1.49× are all partly that fix rather than run-to-run variation, and
the drift across the three columns is not a spread. The only row here that is
three measurements of one program is `typed YSON against the dynamic leg`. The
`—` cells are blanks in the harness's output and it is not recorded which kind:
`paired_ratios` prints nothing for a pair whose sign flips between rounds, which
would make them results rather than gaps, but the run output was not kept.

Individual rounds of the first row range 1.68–2.18, and the decode bucket's
rounds range 100–731 ms in the noisiest run. Read the decode row as an **upper
estimate rather than a central one**: it is a mean over only the rounds that
came out in order, and dropping the out-of-order ones can only remove rounds
where noise made the difference small or negative. The 1.20× is the 1.16×
measurement with a handicap removed: the Skiff leg had been cloning eight
`Value`s a row where `SkiffRow::into_value` exists — three allocations a row,
12 % of the leg, fixed in *stop the Skiff leg handicapping itself* — so every
earlier Skiff figure was a floor
for the Skiff side rather than an estimate of it.

**What survives as a measurement, and what does not.** The adversarial pass
reproduced all of it off-cluster and left the table meaning less than it looks:

- **The 1.85–1.93× is not "the format at equal API".** 79–88 % of that gap on
  the cluster — 88 %, 82 % and 79 % across the three runs — and 98 % off it
  ((989 − 328) / (989 − 315) on the medians below), sits **between the two YSON
  legs** — same format, same bytes, same reader, same serializer (§1).
- **The three legs do not do the same work.** Per row: allocations 0.00 typed /
  11.67 Skiff (8.67 after the clone fix) / 26.67 dynamic; clones 0 / 8 / 8;
  byte-slice key comparisons ~9–18 / 0 / ~87; and 12 `str::from_utf8` scans a
  row on the dynamic *write* path, all of them wasted — `ByteString::serialize`
  validates every key and every byte-string value so that text output can use
  the unquoted-identifier form, and in binary both branches emit the same bytes,
  so the eleven that succeed are as wasted as the one that fails on the
  deliberately non-UTF-8 `user_agent`. Off-cluster medians of 9, in ms — these
  are **in-process elapsed times on the development machine**, native arm64,
  reading from memory, with no pipe, no process start and no scheduler, which
  `benchmarking.md` calls a proxy and an optimistic one: typed frames 96, typed
  decoded 270, typed full 328; dynamic decoded 524, dynamic full 989; Skiff
  decoded 205, Skiff full 315. None of these is CPU.
- **The 1.10–1.20× keeps its sign and loses its unit.** The sign held in all
  nine rounds of all three runs and reproduces off-cluster. The number is not a
  format measurement: the Skiff decoder `Box::new()`s the `referer` variant on
  every row including the absent ones, and the Skiff path reads through
  `BufReader` with ~14 `read_exact` a row where the YSON legs parse in place.
  With output bytes discarded the two legs' in-process time differs by only
  1.01–1.13×, so the cluster's advantage is consistent with being mostly the
  38.5 MiB of output Skiff does not push through the pipe.

Wire volume, which does not scatter at all and was identical in all three runs:
Skiff moves **54.6 MiB in and 47.2 out** where binary YSON moves 91.1 and 85.7.
Reproduced byte for byte off-cluster from the generator and the encodings — 54.6
/ 47.2 and 90.7 / 85.7, the cluster's extra 0.4 MiB on the YSON input being the
control records its stream also carries. Comparing the two **row** streams —
90.7 against 54.6 over 412 554 rows, since control records are not row bytes —
the difference is **~92 bytes a row**, and it is a net of three terms: YSON adds
71 bytes of repeated column names and ~38 bytes of map syntax (a marker and a
length for each of nine keys, nine `=`, eight `;`, two braces, one record
separator), and gets ~17 bytes back, because its varint integers and one-byte
length prefixes beat Skiff's fixed-width `int64`/`uint64` and four-byte
`string32` prefixes — `status` costs 3 bytes against 8 — and because Skiff pays
a two-byte table tag a row that YSON does not. 71 + 38 − 17 ≈ 92, against a
measured 91.8. The row's payload is ~122 bytes, the data weight the cluster
reports, so a YSON row is ~231 bytes and a Skiff row ~139.

**Two limits on generalising, both inside that arithmetic:** the names are 71
bytes against 122 of payload because this row's nine columns are short and
numerous, and a table of five wide blobs would show almost none of it; and the
17-byte credit is Skiff's fixed-width encoding *losing* to YSON on small
integers and short strings, so on a table of small integers it grows and the
direction of the whole comparison is not guaranteed.

**Three things the runs settled about the method itself**, each of which had
already produced a wrong number:

- **Pair by round, never minimum against minimum.** Two legs' fastest rounds
  can fall minutes apart and carry different weather; the two legs of one round
  cannot. A run whose minima said "no pair is separable" gave ratios that held
  their sign in all nine rounds.
- **Pin the job count.** `time/exec` sums over jobs and a job start is several
  hundred milliseconds here, so a leg left on the controller's default is
  compared on how it was scheduled.
- **Read the statistics tree before summing it.** `user_job/pipes/output`
  carries a `total` beside its numeric descriptors, and adding both doubled
  every pipe figure this harness printed for two runs.

**Two findings about the formats**, as opposed to about the measurement:

- **Skiff cannot have a frames-only stop.** Its stream has no self-describing
  record boundaries: finding the end of a row is decoding it against the
  schema. The subtraction the YSON legs support does not exist on that side.
- **YQL's own job I/O is Skiff**, confirmed from the operation's spec. Its
  schema carries `$row_index` as a `variant8<nothing;int64>` and writes
  `is_external` as an optional boolean where the worker writes a plain one —
  which is the whole of the difference between its 55.0/47.6 MiB and the
  hand-written schema's 54.6/47.2. An independently written positional schema
  matching the engine's to within its system columns is the strongest evidence
  so far that the Skiff leg is right.

**What none of it settles**: the decode share against the 30 % threshold, which
[`benchmarking.md`](benchmarking.md) states over **job CPU**. This cluster
reports nothing under `user_job/cpu`; roughly half the denominator here is
process start and waiting for the first batch — a job start costs ~640 ms on
this cluster and `latency/input/time_to_first_read_batch` is a further ~504 ms
of a ~2000 ms job, with `time/prepare` another 650–800 ms that `time/exec` does
not include at all; and every figure above is per-job wall time on a single-node
local Docker cluster running x86-64 images under arm64 emulation, one job per
leg.

**And what none of it can settle, by construction**: whether Skiff is the
faster *format* for a job that reads it well. That comparison is today's typed
YSON against a typed Skiff, and Skiff has no typed rows — they are listed as
planned in [`skiff-compatibility.md`](skiff-compatibility.md), so no one can
measure it yet. Bytes cannot stand in for it either: they cannot explain a
difference between two legs that move identical bytes, which is where most of
the 1.85–1.93× turned out to be. A production run is still owed, and so is
required test 4 (§3.2).

### Phase 1 — the queries and the modes, correctness before timing

**Worker modes.** Extend `sessionize` with three single-output map modes beside
its existing `map-frames` / `map-parse` — the precedent for exactly this kind of
depth-controlled variant is already in that file:

| mode | leg |
| --- | --- |
| `map-one` | 1 — typed serde, one output |
| `map-one-dynamic` | 2 — `YsonValue` in and out |
| `map-one-skiff` | 3 — `WorkerReader`/`WorkerWriter` on a hand-written Skiff schema |

The Skiff schema is written out by hand (there is no inference — §3.2) and
must match the input table's schema column for column: `string32` for the two
byte columns, `int64`, `uint64`, `double`, `boolean`, and a `Variant8` optional
for `referer`.

**Queries.** All write their full result with `INSERT INTO … WITH TRUNCATE`, so
YQL pays the same output cost and nothing is capped at Query Tracker's display
limit.

*project-and-filter* — the leg-4 mirror of the modes above: the `SELECT` list is
the nine columns, the `WHERE` is the five rules, `is_external` is the referer
expression.

*wordcount* — mirrors [`wordcount.rs`](../crates/ytsaurus-job/examples/wordcount.rs),
whose tokenization is runs of ASCII alphanumerics and apostrophes, byte-oriented.
Written the complement way round on purpose, because the worker splits on a
character class and `String::SplitToList` takes a literal separator. Sketch, to
be validated against phase 0 step 2:

```sql
INSERT INTO `…/counts_yql` WITH TRUNCATE
SELECT word, CAST(COUNT(*) AS Int64) AS count
FROM (SELECT Re2::FindAndConsume(_, "[A-Za-z0-9']+")(text) AS words
      FROM `…/lines`)
FLATTEN LIST BY words AS word
GROUP BY word;
```

If `Re2` is not loaded: `Unicode::SplitToList`, then changing the corpus so a
single-separator split is equivalent — the last weakens the comparison and must
be said in the report if used.

*sessionize* — mirrors the reduce over the `events` table. Semantics read off
the worker, all of which must be checked rather than assumed:

| Worker | YQL |
| --- | --- |
| new session when `timestamp - ended_at > 30 min` (µs) | `SessionWindow(timestamp, 1800000000)` — verify the boundary is `>`, not `>=` |
| `session_index`, 0-based, per user | `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start) - 1` |
| `started_at` / `ended_at` = min / max | `MIN` / `MAX` |
| `entry_url` = the row that opened the session | `MIN_BY(url, timestamp)` — check the tie-break; the worker takes stream order |
| `errors` = count of `status >= 400` | `SUM(IF(status >= 400, 1, 0))` |
| `is_mobile` = OR over the session | `BOOL_OR(is_mobile)` |
| `mean_latency_ms` = `sum / hits` | `SUM(latency_ms) / COUNT(*)`, needing §3.4's tolerance, which does not exist yet |
| `users` table | a second `INSERT` from the sessions relation |

**Before any timing**, every leg runs on the same input and every output is
diffed against leg 1's, row order normalized. Legs 2 and 3 must agree with leg 1
**exactly** — they are the same computation in a different representation, and
any difference there is a bug in this crate, which is a better find than a
benchmark. Leg 4 would agree only within §3.4's float tolerance, on a task that
sums a float; the `project` task the harness runs does not, so all four legs are
compared exactly and do agree.

The e2e corpus — four lines of text, 60 synthetic users — is right for the diff
and useless for timing.

**DoD**: all four legs agree on the e2e fixtures; every disagreement found on
the way is written down, since a semantic difference between two implementations
is a result whether or not it is fixable.

### Phase 2 — the harness

One Rust example that, per task: creates the schematized input once, then runs
each leg over it and collects the same numbers from each.

| Collected | From | Note |
| --- | --- | --- |
| operation wall time | `time/total`, `time/exec` | the metric that exists locally (§3.1) |
| job CPU | `user_job/cpu/user`, `user_job/cpu/system` | `None` locally; print which was obtained |
| rows and bytes in/out | `data/input/*`, `data/output/*` | where projection and format size show up |
| operations spawned | count them | one or two for a worker, unknown for YQL, and itself interesting |

Sum across **all** operations a query spawned. Repeat 5 times, take the fastest,
print the spread, discard one warm-up per leg.

Fairness enforced by the harness rather than by discipline:

- one input table, one schema, one sorted state, all four legs;
- every leg reads the same columns, otherwise YQL wins on projection alone and
  the number means nothing. *Built the other way round*: rather than narrowing
  the workers with `TablePath::columns`, the query was written to name all nine,
  so every leg reads the whole row. Same fairness, and nothing to keep in step —
  but it is a property of the query text, not something the harness checks;
- **refuse any query text without an `INSERT`**: a stray `SELECT` is silently
  capped at Query Tracker's result rows and would under-measure output cost,
  which is the failure this design exists to prevent;
- **refuse any query text without `PRAGMA yt.QueryCacheMode = "disable"`** —
  phase 0 measured a repeated query completing with zero operations, and a
  benchmark that reports a cache hit as a runtime is worse than no benchmark;
- **comparable memory limits on both sides**: YQL needs
  `PRAGMA yt.DefaultMemoryLimit = "640M"` on this cluster, which is just above
  its own 545 MB default, so the worker legs' 512 MB is already comparable —
  print both rather than assuming, and do not raise either to a round number
  that flatters one side;
- ~~refuse a Skiff leg whose schema does not match the input table's schema,
  rather than letting a mismatch be measured as slowness~~ — **not built**. The
  schema is written out by hand in two places that have to agree with each
  other, and a mismatch surfaces as a decode failure rather than as slowness,
  so this bought less than it looked like it would;
- ~~print the pinned versions — cluster image tag, crate versions, a hash of the
  query texts~~ — **not built**. It prints the two memory limits and nothing
  else, so a run's provenance lives in whoever ran it. Still worth having: the
  three runs recorded in §1 are distinguished in prose rather than by anything
  the output carries.

**DoD**: one command, one table of numbers, reproducible across two consecutive
runs within the spread it reports.

### Phase 3 — the report, inside the Skiff decision

The results go into [`docs/benchmarking.md`](benchmarking.md) as a new **§5**
after "The same pilot, on a production cluster", and — this is the point of the
widening — into the parts of that document that decide things:

- **Decision criteria #1** ("parsing is the bottleneck … exceeds ~30 % of job
  CPU") rests on leg 1 measured at three depths. Legs 2 and 3 were expected to
  give it the other half — not just what decoding costs, but what changing the
  format would recover. **They did not, and could not.** The criterion is stated
  over job CPU; a local cluster reports none, so everything measured here is
  per-job wall time, and about half of the denominator is process start and
  waiting for the first batch. §5 of `benchmarking.md` says so in those words,
  and criterion 1 stands where §3 and §4 left it.
- **Decision criteria #2** ("the Rust job is not already fast enough … if it
  already beats the C++ baseline") has never had any evidence at all. Leg 4 is
  its first. If leg 1 already matches or beats YQL, that is a serious argument
  against spending the format budget, and it must be entered even — especially —
  if it is unwelcome.
- **"What has *not* been measured"** lists a C++ and a Python baseline; say what
  YQL now supplies and what it still does not.
- **The parked Skiff entry** in [`AGENTS.md`](../AGENTS.md), which currently ends
  "what is owed is a spread from repeated production runs, not a third single
  number" — this plan owes that spread too, and should not add a third single
  number to it.
- **[`skiff-compatibility.md`](skiff-compatibility.md) required test 4** — note
  what the Skiff leg did and did not exercise. One input, one output, no key
  switch: it does **not** close the gate, and the report must not be readable as
  if it did.

Required interpretation, whichever way it comes out: what any YQL advantage
decomposes into — projection, runtime, stage structure, since a query reading
3 of 9 columns and winning is not a runtime result; what the leg 2 → 3 delta
does to the Skiff question; and the standing caveat that a single-node Docker
cluster under emulation measures fixed costs with some computation attached.

**DoD**: `benchmarking.md` updated; modes, queries and harness in-tree so anyone
with Docker reproduces the local half.

## 5. Risks and limits

- **The local cluster cannot see the thing this is now for** (§1, §3.1). The
  predicted Skiff gain is ~4 % of local job time against rounds that scatter by
  2×. Locally this plan produces a YQL comparison and a working harness; the
  Skiff answer needs the production installation.
- **Both dynamic legs are handicapped by their APIs, and leg 2 more than leg 3
  — the opposite of what this bullet assumed.** Leg 3 pays for owning its
  values; leg 2 pays for owning them *and* for keying them by name, ~87 key
  comparisons and 12 dead UTF-8 scans a row. Leg 2 is worth keeping as a
  measured artefact, not as a control: the honest use of it is "what a
  `YsonValue` job costs", never "what YSON costs". The Skiff numbers are
  interpretable only against **leg 1**.
- **Skiff on a cluster is verified for one shape only** (§3.2). Staying inside
  it is a constraint on the task, and stepping outside it turns a benchmark into
  debugging.
- **The QT result-row limit** is avoided by `INSERT INTO`; the harness's refusal
  to run a query without one keeps it avoided.
- **Version drift.** The YQL agent rides `ghcr.io/ytsaurus/local:stable`; the
  Go SDK reference for Skiff is pinned at v0.0.33. The tags printed in the
  report are what make two runs comparable over time.
- **Semantic drift between implementations** — session boundaries, tie breaks,
  float accumulation. Phase 1's diff is the whole defence, which is why it comes
  before any timing.
- **The gate may fail.** If the local image ships Query Tracker without a
  working YQL agent, this plan produces one honest paragraph, and that is a
  legitimate outcome.

## 6. Needed from a human

- **Docker running**, for phase 0. It was not, on the machine this was adapted
  on.
- **The production installation** used for `benchmarking.md` §4 — with §1's
  arithmetic in hand, this is no longer a nice-to-have: it is where the Skiff
  half of the question is decided.
- At phase 3: whether the numbers change any priority — the Skiff go/no-go in
  particular, which is a human decision by the standing rule in that document.

## 7. Deliverables

| | |
| --- | --- |
| [`crates/ytsaurus-client/examples/yql_smoke.rs`](../crates/ytsaurus-client/examples/yql_smoke.rs) | **done** — phase 0's gate and the four answers, plus `YT_YQL_QUERY` for running one query verbatim |
| [`crates/ytsaurus-client/examples/format_compare.rs`](../crates/ytsaurus-client/examples/format_compare.rs) | **done, all four legs**, on two tasks: `wordcount` (which shuffles, and whose numbers turned out to be about plan shape) and `project` — the pilot's map at three depths, plus the dynamic-YSON control, plus Skiff, plus the query. Phase 1's diff and phase 2's timings |
| [`crates/ytsaurus-job/examples/sessionize.rs`](../crates/ytsaurus-job/examples/sessionize.rs) | **done**: `map-one`, `map-one-dynamic`, `map-parse-dynamic`, `map-one-skiff`, `map-parse-skiff` beside the `map-frames` / `map-parse` stops that already existed |
| the query texts | **not built as files.** v1.0 wanted `tests/cluster-e2e/yql/*.sql` versioned beside the workers they mirror; they are `format!` strings in `format_compare.rs` instead, which keeps a query and the leg that checks it in one place and puts them out of reach of anything but Rust |
| [`tests/cluster-e2e/README.md`](../tests/cluster-e2e/README.md) | **done** — a `yql_smoke` section and a `format_compare` one, so both commands that reproduce any of this are documented where a person looks for them rather than only here |
| [`docs/benchmarking.md`](benchmarking.md) | §5 and the four edits in phase 3 |
| this file | kept current as the phases land |

## 8. Changes from v1.0

| | |
| --- | --- |
| **Scope** | Rust-versus-YQL → four legs on one task, so the YQL numbers enter the Skiff decision instead of sitting next to it |
| **Added** | legs 2 and 3, the worker modes that implement them, and the control leg — which turned out to introduce a larger confound than it removed, and is recorded as such in §1 |
| **Added** | the pre-registered prediction in §1 — and its consequence, that the local cluster cannot resolve the Skiff delta |
| **Task** | wordcount + full sessionize → *project-and-filter* (the pilot's map, one output) for the four-way comparison; the other two stay YSON-versus-YQL, inside phase 1 |
| **Constraint** | one input, one output, no key switch — the only Skiff shape verified on a cluster |
| **Metric** | job CPU → `time/exec` locally, `user_job/cpu/*` only where the cluster reports it |
| **Estimator** | median of 5 → fastest of 5 plus the spread, matching `profile.rs` → **paired by round**, which is what the runs showed was needed; the fastest-of-N survives only in the absolute columns (§3.5) |
| **Sessionize scope** | whole pilot → clean path; the rejects table is not expressible in stock YQL, recorded as a capability difference |
| **Correctness bar** | "byte-comparable" → exact between legs 1–3, float tolerance against leg 4 |
| **Report target** | "a third column" → §5 plus named edits to both decision criteria, the not-measured list, the parked entry, and required test 4 |
| **Layout** | `tests/yql-comparison/` → queries in `tests/cluster-e2e/yql/`, harness as a `ytsaurus-client` example. Shipped as the example; the queries stayed inside it (§7) |
| **Worker paths** | repo root → `crates/ytsaurus-job/examples/`, after `16915ab` |
| **UDF rule** | "no UDFs" → no *custom* UDFs; which modules the agent loads is a phase 0 question |
| **Phase 0** | added the cluster's YQL name, the loaded UDF modules, a `skiff_launch` run, and the note that the operation-id fallback must poll during the run |

## 9. Sources

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
