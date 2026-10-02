# Writing a YTsaurus job in Rust

From an empty file to a running operation. Assumes you can reach a cluster:
`export YT_PROXY=http://localhost:8000` for the local one in
[`tests/cluster-e2e/README.md`](../tests/cluster-e2e/README.md).

The goal is one static binary that is both launcher and job: it uploads
itself, starts the operation, and is what the cluster runs. A `cargo run`
launcher uploads a separately built static worker instead; the `yt` CLI can
also launch (§5).

## 0. What a job actually is

A YTsaurus job is an ordinary executable. The cluster copies it to a node, runs
it, and:

- feeds it input rows on fd 0,
- collects output table `k` from fd `3k + 1`: table 0 is fd 1 (stdout),
  table 1 is fd 4, table 2 is fd 7,
- decides whether the job succeeded from its exit code,
- shows its stderr in the operation UI.

The rows are [YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson), in
binary, as a `;`-separated list fragment. `ytsaurus-job` handles all of that.

Two consequences:

- **stdout belongs to the protocol.** A stray `println!` corrupts output table
  0. Print diagnostics with `eprintln!`.
- **A job can be restarted.** YTsaurus reruns failed and speculative jobs, so a
  job must be a pure function of its input. Do not write to external state.

## 1. Set up

Add a binary to `examples/` (or your own crate):

```toml
[dependencies]
ytsaurus-job = "0.3"
serde = { version = "1", features = ["derive"] }
serde_bytes = "0.11"

# Only if the same binary should also launch the operation; see §3.
ytsaurus-client = { version = "0.3", default-features = false }
```

`default-features = false` drops TLS, whose stack needs a C toolchain to
cross-compile to musl; a local cluster is plain HTTP. For an `https://`
cluster, turn the `tls` feature back on and build the worker on Linux or with a
musl cross-compiler.

`ytsaurus-job` re-exports the codec as `ytsaurus_job::yson`, so a direct
dependency on `ytsaurus-yson` is needed only without the job runtime.

## 2. Write a mapper

```rust
use serde::{Deserialize, Serialize};
use ytsaurus_job::{Event, JobReader, JobWriter};

#[derive(Deserialize)]
struct Input<'a> {
    #[serde(borrow)]
    url: &'a str,
    size: i64,
}

#[derive(Serialize)]
struct Output<'a> {
    host: &'a str,
    size: i64,
}

fn mapper() -> ytsaurus_job::Result<()> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(1)?;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };
        let input: Input = row.parse()?;
        let host = input.url.split('/').next().unwrap_or("");
        writer.write(0, &Output { host, size: input.size })?;
    }

    // Buffered rows that are never flushed are rows missing from the output
    // table. `finish` is not optional.
    writer.finish()
}

fn main() {
    ytsaurus_job::run(mapper)
}
```

`run` installs a panic hook and turns any error into a non-zero exit with the
message on stderr, where the UI shows it. §3 replaces that `main` with one
that also launches.

### Choosing column types

| Column | Use | Why |
| --- | --- | --- |
| string, known to be text | `&str` / `String` | borrows from the read buffer with `&str` |
| string, arbitrary bytes | `#[serde(with = "serde_bytes")] &'a [u8]` | YTsaurus strings are byte strings: a `String` field fails the whole job on one non-UTF-8 row |
| int64 / uint64 | `i64` / `u64` | |
| double | `f64` | |
| boolean | `bool` | |
| any nullable column | `Option<T>` | a missing or `#` value |
| anything at all | `ytsaurus_yson::YsonValue` | dynamic access |

Prefer borrowed types (`&'a str`, `&'a [u8]`): they read straight out of the
reader's buffer and cost nothing to decode, where owned types copy every row.
Borrowed fields cannot outlive the row, so copy what you keep across rows; the
compiler points at the spot.

### Output rows can borrow too

A row is serialized before the borrow ends, so an output struct may hold
references to the input:

```rust
use serde::Serialize;

#[derive(Serialize)]
struct Reject<'a> {
    #[serde(with = "serde_bytes")]
    raw: &'a [u8],          // borrows the input row: no copy
    reason: &'a str,
}

// writer.write(rejects, &Reject { raw: row.raw(), reason })?;
```

`raw: Vec<u8>` with `row.raw().to_vec()` is correct too, but copies every
rejected row.

### Reporting why a row was rejected

A row can fail to decode (`JobError`) or break your own rule. Fold both into
one cheap `&'static str`:

```rust
let outcome: Result<Clean, &'static str> = match row.parse::<Raw>() {
    // Not this row's fault: a truncated stream or a failed write means every
    // later row is suspect, so stop rather than quarantine.
    Err(e) if !e.is_row_local() => return Err(e),
    // A bad row: `kind()` is allocation-free and stable enough to group by.
    Err(e) => Err(e.kind()),
    Ok(raw) => validate(&raw).map(|()| Clean::from(raw)),
};
```

`JobError::kind()` returns values like `invalid_yson` and `truncated_record`.
Formatting the error would allocate per bad row and give a message that can
change between versions.

## 3. One binary, two roles

The cluster starts a job by exec'ing the uploaded binary with `YT_JOB_ID` in its
environment, so a program can ask which role it is playing:

```rust
fn main() {
    // Inside a job this never returns: it runs the mapper and exits with the
    // status YTsaurus reads.
    ytsaurus_job::run_if_inside_job(mapper);

    // Only your machine gets here.
    if let Err(e) = launch() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn launch() -> Result<(), ytsaurus_client::ClientError> {
    let client = ytsaurus_client::Client::from_env()?;

    // Uploads this static binary, so what runs on the cluster is what you just
    // built.
    client.upload_current_exe("//tmp/my_job")?;

    let spec = ytsaurus_client::MapSpec::new("./my_job", ["//tmp/input"], ["//tmp/output"])
        .with_local_file("//tmp/my_job")
        .with_memory_limit(512 * 1024 * 1024);

    let id = client.start_map(&spec)?;
    client.wait_for_operation(&id)
}
```

For a static launcher that is the whole pattern, with no second artifact to
forget to rebuild. `upload_current_exe` checks the executable's ELF header
(Linux, x86-64, statically linked) first, because anything else would fail on
the node minutes later with an error that names no cause:

```text
/…/target/debug/selfrun cannot run on a cluster node: it is not an ELF binary,
so a Linux node cannot exec it. Build the worker with scripts/build-worker.sh …
```

**A launcher built with `cargo run` cannot be the uploaded file**: it is
Mach-O on macOS and dynamically linked on a typical Linux host. Build the
static musl worker from the same source and point the host launcher at it:

```sh
scripts/build-worker.sh my_job
# Rebuild this worker whenever its source changes.
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/my_job \
    cargo run -p my-workers --bin my_job
```

where the launcher chooses:

```rust
match std::env::var("YT_WORKER_BINARY") {
    Ok(path) if !path.trim().is_empty() => client.upload_worker(&path, remote)?,
    _ => client.upload_current_exe(remote)?,
}
```

On Linux x86-64 you can run the musl build itself instead, and
`upload_current_exe` needs no help. [`crates/ytsaurus-job/examples/selfrun.rs`](../crates/ytsaurus-job/examples/selfrun.rs)
is the runnable version of both forms.

### Uploading it only when it changed

A worker is tens of megabytes. The cluster's file cache is keyed by MD5, so an
unchanged binary is found rather than uploaded:

```rust
let worker = client.upload_worker_cached("target/…/my_job")?;
let spec = MapSpec::new("./my_job", ["//tmp/in"], ["//tmp/out"])
    .with_local_file_named(&worker.path, &worker.name);
```

Pass the name along: a cached node is named after its hash, and `./my_job`
would find nothing to run without it. `worker.uploaded` says whether this call
was a miss.

`worker.cached` is different. Where `//tmp/yt_wrapper/file_storage` is
read-only to an ordinary user, the upload into it is answered `Access denied`;
the client warns, uploads the worker under `//tmp` instead and goes on.
`cached` is false there, and true both for a hit and for an upload the cache
accepted. **Check `cached`, not `uploaded`, before removing the node
afterwards**: on an ordinary cluster that node is the shared cache entry, and
deleting it evicts the binary for everybody. `Client::with_file_cache` points
the cache somewhere writable.

### What the cluster puts in a job's environment

Captured from a job on a local cluster:

| Variable | Example | |
| --- | --- | --- |
| `YT_JOB_ID` | `f5627254-f0da1b44-10384-1000001` | what `is_inside_job` tests; `job_id()` returns it |
| `YT_OPERATION_ID` | `94709bab-331f4beb-103e8-609dd076` | the operation this job belongs to |
| `YT_JOB_COOKIE` | `0` | stable across a restart of the same job; how vanilla jobs identify themselves |
| `YT_JOB_INDEX`, `YT_TASK_JOB_INDEX` | `0` | position within the operation and within its task |
| `YT_START_ROW_INDEX` | `0` | first input row index this job was given |
| `YT_FIRST_OUTPUT_TABLE_FD` | `1` | the `3k + 1` descriptor rule, stated by the cluster |
| `YT_NODE_HOST`, `YT_POOL_TREE` | `localhost`, `default` | where it ran |

The job's argv is exactly the spec's command (`["./selfrun"]`), so a binary can
also take its role from an argument: `wordcount map` / `wordcount reduce`
serves both phases of a map-reduce this way. `YT_JOB_ID` is the better signal
for launcher versus job, because nothing has to remember to pass it.

## 4. Build it for the cluster

Jobs run on Linux x86_64 nodes that may not have your libc. Build a fully static
musl binary:

```sh
scripts/build-worker.sh my_job
file target/x86_64-unknown-linux-musl/release-worker/my_job
# ELF 64-bit LSB pie executable, x86-64, ..., static-pie linked, stripped
```

This works on Linux and on macOS, where it links with the toolchain's bundled
`rust-lld`; no cross-toolchain is needed. You want `static-pie linked` with no
interpreter; a dynamic binary fails on the node with an obscure missing-loader
error.

## 5. Run it

With the `main` from §3, that is just the binary:

```sh
export YT_PROXY=http://localhost:8000
cargo run -p my-workers --bin my_job
```

### Typed output tables

With a schema the cluster checks every row, and the schema is the struct the
job already serialises:

```rust
use ytsaurus_client::TableRow;

#[derive(TableRow)]
struct Output<'a> {
    #[yt(key)]
    host: &'a str,               // utf8, required, and the table comes out sorted
    size: i64,                   // int64, required
    referrer: Option<&'a str>,   // optional, because the Rust type says so
}

client.create_table("//tmp/output", &Output::table_schema())?;
```

Needs `ytsaurus-client` with `features = ["derive"]`. A row that leaves out a
required column is then refused by the cluster with
`Required column "size" cannot have "null" value` instead of landing in the
table.

`String` becomes `utf8` and `Vec<u8>` becomes `string`, the same text-versus-
bytes distinction as §2. A Rust type the derive cannot place is a compile
error, not a guess; name the column type yourself with
`#[yt(column_type = "timestamp")]` for types no Rust type implies.

When the struct gains a field, `client.alter_table(path, &Output::table_schema())?`
widens the table to match. A table that already holds rows accepts only changes
that ask less of them: a new optional column is fine; dropping one or adding a
required one is refused by name. An empty table accepts anything, so trying the
migration on one proves nothing about the real table.

### Publishing the result all at once

A launch that fails partway leaves some of its work behind: the output table
exists, the worker is uploaded, half the rows are replaced. A transaction makes
it one event:

```rust
let tx = client.start_transaction()?;

tx.upload_worker(WORKER, "//tmp/my_job")?;
let id = tx.start_map(&spec)?;
tx.wait_for_operation(&id)?;

tx.commit()?;                     // until this line, none of it exists
```

Nothing outside sees any of it, the upload included, until the commit, and
dropping the handle aborts, so each `?` above leaves the cluster as it was,
with no cleanup code. The handle pings the transaction while it lives, so it
can wrap an hour-long operation; unpinged, the cluster drops it after 30
seconds.

### Or with the `yt` CLI

The CLI needs two packages; `ytsaurus-client` alone fails on binary YSON with
`YSON bindings required`:

```sh
pip install ytsaurus-client ytsaurus-yson
```

```sh
yt map './my_job' \
    --src //tmp/input --dst //tmp/output \
    --format '<format=binary>yson' \
    --local-file target/x86_64-unknown-linux-musl/release-worker/my_job
```

`--local-file` uploads the binary; `'./my_job'` is the command the node runs.
`--format '<format=binary>yson'` sets both input and output format, as
`JobReader::from_stdin` and `JobWriter::descriptors` expect.

Two CLI details:

- `--spec` is YSON, not JSON: `{mapper={memory_limit=536870912}}`, with `=`
  for key/value, `;` between entries, `%true`/`%false` for booleans. A JSON
  spec fails with `Unexpected token ":"`.
- `map-reduce` uses `--map-local-file` and `--reduce-local-file`, not
  `--local-file`.

## 6. Multiple output tables

Declare them by name and address them by handle:

```rust
let (mut writer, [good, bad]) = JobWriter::named(["good", "bad"])?;
writer.write(good, &kept)?;
writer.write(bad, &rejected)?;
```

`JobWriter::descriptors(2)` and `writer.write(0, …)` still work, but
transposing `0` and `1` runs happily and fills each table with the other's
rows.

```sh
yt map './my_job' --src //tmp/in --dst //tmp/good --dst //tmp/bad ...
```

Table `k` goes to fd `3k + 1`. To send everything down one descriptor,
`JobWriter::table_switches(n)` writes `<table_index=N>#` records instead. Do
not mix the two: YTsaurus does not define the order of rows reaching one table
through two descriptors.

## 7. Knowing which input table a row came from

Ask for it in the spec, then read `row.table_index`:

```sh
--spec '{mapper={enable_input_table_index=%true}}'
```

`row_index` and `range_index` work the same way, via
`job_io.control_attributes.enable_row_index` / `enable_range_index`. Without
these the fields stay at their defaults: `table_index` is `0` and the others
are `None`.

## 8. Reduce

A reducer's input is grouped by the `--reduce-by` columns, with a
`<key_switch=%true>#` record between groups. **Enable it, or the whole input
arrives as one group and every key is silently summed together:**

```sh
# `reduce` operation: one job type, so the section is `job_io`
--spec '{job_io={control_attributes={enable_key_switch=%true}}}'

# `map-reduce` operation: several job types, each with its own section
--spec '{reduce_job_io={control_attributes={enable_key_switch=%true}}}'
```

**On a map-reduce, `job_io` is silently ignored**: the reducer sees no key
switches and every key is summed into one row.

`ReduceSpec` and `MapReduceSpec` each put it in the right place, on by default:

```rust
use ytsaurus_client::{ReduceSpec, SortSpec};

// A reduce needs sorted input, so sort first, once. The sorted table can then
// be reduced as often as you like, without paying for a shuffle each time.
let sort = SortSpec::new(["//tmp/lines"], "//tmp/sorted", ["word"]);
client.wait_for_operation(&client.start_sort(&sort)?)?;

let reduce = ReduceSpec::new("./wordcount reduce", ["//tmp/sorted"], ["//tmp/counts"], ["word"])
    .with_local_file("//tmp/wordcount");
client.wait_for_operation(&client.start_reduce(&reduce)?)?;
```

Use map-reduce for unsorted data in one pass; sort-then-reduce when the data
is sorted or reduced more than once. The cluster refuses a reduce whose input
is not sorted by a column set beginning with `reduce_by`.

```rust
// Pass the same columns the operation was given as `reduce_by`.
let mut groups = reader.groups_by(["word"]);

while let Some(mut group) = groups.next_group()? {
    let word = group.key().bytes("word").unwrap_or_default().to_vec();

    let mut total = 0i64;
    while let Some(row) = group.next_row()? {
        total += row.parse::<Entry>()?.count;
    }

    writer.write(totals, &Total { word, count: total })?;
}
```

YTsaurus does not transmit the key (`key_switch` carries no payload), so
`groups_by` reads it from the group's first row. `groups()` without columns
still works and leaves `group.key()` empty.

A full map-reduce, with one binary serving both phases:

```sh
yt map-reduce \
    --mapper './wordcount map' --reducer './wordcount reduce' \
    --reduce-by word \
    --src //tmp/lines --dst //tmp/counts \
    --format '<format=binary>yson' \
    --map-local-file target/x86_64-unknown-linux-musl/release-worker/wordcount \
    --reduce-local-file target/x86_64-unknown-linux-musl/release-worker/wordcount \
    --spec '{reduce_job_io={control_attributes={enable_key_switch=%true}}}'
```

See [`crates/ytsaurus-job/examples/wordcount.rs`](../crates/ytsaurus-job/examples/wordcount.rs).

## 9. Jobs with no input

A vanilla operation runs jobs with nothing on fd 0: a distributed process, a
side-car computation, or a job that fetches its own input.

```rust
use ytsaurus_client::{VanillaSpec, VanillaTask};

let spec = VanillaSpec::new(
    VanillaTask::new("shards", "./my_job 3", 3)     // three jobs
        .with_local_file_named(&worker.path, &worker.name)
        .with_outputs(["//tmp/results"]),
);
client.wait_for_operation(&client.start_vanilla(&spec)?)?;
```

The job side is the same runtime minus the reader:

```rust
fn main() {
    ytsaurus_job::run(|| {
        let mut writer = JobWriter::descriptors(1)?;      // output still works
        let shard = ytsaurus_job::job_cookie().unwrap_or(0);
        // ... do this shard's work
        writer.finish()
    })
}
```

The cluster keeps `job_count` jobs running; dividing the work is yours. Divide
by `job_cookie()`: it counts from zero and is stable across a restart, so a
retried job redoes its own share.

The cluster tells a job its cookie but not how many siblings it has, so pass
that in the command (`"./my_job 3"` above). A task may declare output tables or
none; `gang_options` and the rest go through `VanillaTask::with_raw`.

See [`crates/ytsaurus-job/examples/shards.rs`](../crates/ytsaurus-job/examples/shards.rs) and
`cargo run -p ytsaurus-client --example vanilla`.

## 10. Reporting your own numbers

The cluster measures CPU, memory and rows in and out, not rows that failed
validation, dictionary load time or lookup misses. Report those as custom
statistics. A job that drops rows should, or the operation succeeds and the
output table is simply shorter.

```rust
use ytsaurus_job::JobStatistics;

let mut stats = JobStatistics::new();

while let Some(event) = reader.next_event()? {
    let Event::Row(row) = event else { continue };
    stats.add("rows/read", 1)?;

    if !valid(&row) {
        stats.add("rows/rejected", 1)?;
        continue;
    }
    // ...
}

writer.finish()?;
stats.finish()?;      // nothing is sent until this
```

They go to fd 5, which YTsaurus reserves for them. A job may report at most
128 distinct names; adding to a recorded one is always fine, and the 129th
name is refused locally rather than by the cluster rejecting all of them.

Outside a job nothing is written: fd 5 belongs to whoever opened it, which
with §3's one-binary pattern may be the launcher's connection to the cluster.

Reading them back:

```rust
let rejected = client.statistic_sum(&operation_id, "rows/rejected")?;   // Some(3)
```

The name keeps its slash (the cluster stores `rows/rejected` as one key), and
the total is over `completed` jobs, since an aborted job's work is redone by
its replacement. `Client::custom_statistics` returns the whole tree, with the
per-job-type breakdown.

See [`crates/ytsaurus-job/examples/counted.rs`](../crates/ytsaurus-job/examples/counted.rs) and
`cargo run -p ytsaurus-client --example statistics`.

## 11. Test without a cluster

A job is a program that reads a pipe, so run it as one:

```sh
./my_job < input.bin > table0.bin 4> table1.bin
```

[`crates/ytsaurus-job/tests/cat_e2e.rs`](../crates/ytsaurus-job/tests/cat_e2e.rs)
works this way and catches most protocol mistakes. For the reduce path, [`crates/ytsaurus-job/tests/wordcount_e2e.rs`](../crates/ytsaurus-job/tests/wordcount_e2e.rs)
simulates the shuffle by sorting the mapper output and inserting key switches.

For a real cluster run, see [`tests/cluster-e2e/README.md`](../tests/cluster-e2e/README.md).

## 12. When something goes wrong

| Symptom | Likely cause |
| --- | --- |
| `exec format error` on the node | binary is not Linux x86_64; check `file` |
| output table has garbage rows | something wrote to stdout; use `eprintln!` |
| output table is short | `writer.finish()` was not called |
| every reduce key summed together | `enable_key_switch` was not set; on `map-reduce` it goes under `reduce_job_io`, not `job_io` |
| job fails on some rows only | a `String` column that is not valid UTF-8; use `serde_bytes` |
| `table_index` always 0 | `enable_input_table_index` was not set |
| job killed on memory | you are accumulating rows; the reader itself holds ~1 MiB |
| `YSON bindings required` from the CLI | `pip install ytsaurus-yson` |
| `Unexpected token ":"` from `--spec` | the spec is JSON; it must be YSON |
| `Table values cannot have top-level attributes` on write | a column value carries `<...>` attributes; tables cannot store those |
| output differs from input in an identity job | you decoded and re-encoded, and map keys come back sorted; use `Row::raw()` |
| `cannot run on a cluster node` from `upload_current_exe` | the launcher is not a Linux x86-64 static binary; build the worker separately and point `YT_WORKER_BINARY` at it (§3) |
| the job re-runs the launcher on the node | `run_if_inside_job` is not the first thing `main` does, or the uploaded binary is a different build |
| rows vanish and the operation still succeeds | a mapper is dropping them; count them with a custom statistic (§10) |
| `not running as a job, so N statistic(s) were not sent` | `JobStatistics::finish` was called outside a job, where fd 5 is not the cluster's |

Job stderr appears in the operation UI. Set `RUST_BACKTRACE` through the spec to
get backtraces from a panicking job:

```sh
--spec '{mapper={environment={RUST_BACKTRACE="1"}}}'
```

### Reading the failure without the UI

With [`ytsaurus-client`](../crates/ytsaurus-client/), when an operation
fails, `wait_for_operation` asks the cluster which jobs failed and what they
printed, and puts that in the error.

```text
operation 1ba94195-3142e068-103e8-ffe93efc finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
  job 24c164af-a273b7fd-10384-1000001 on localhost:24403: User job failed: Process terminated by signal 6
  stderr:
    boom: started, reading input
    ytsaurus-job: the job panicked and will fail.
    thread 'main' panicked at crates/ytsaurus-job/examples/boom.rs:37:17:
    boom: this job fails on purpose (row 1, 23 bytes)
```

`Process terminated by signal 6` is a Rust panic seen from the cluster: worker
binaries are built with `panic = "abort"`, so the panic aborts rather than
unwinds. The message is in the stderr above it.

Only the tail of each job's stderr is included, up to a few kilobytes; for the
whole thing, or for a job that succeeded, use `Client::get_job_stderr`. The
report costs one `list_jobs` and a few `get_job_stderr` calls per failed
operation; `Client::with_job_diagnostics(false)` turns it off on installations
where `list_jobs` is not welcome.

`cargo run -p ytsaurus-client --example diagnose` shows the whole path on a
local cluster with the `boom` worker.

## Reference

- [YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson)
- [Input/output settings](https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration): control attributes
- [Table switching](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/table-switch): descriptor numbering
- [Operation options](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options)
