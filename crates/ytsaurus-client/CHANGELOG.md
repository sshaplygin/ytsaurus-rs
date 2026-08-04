# Changelog

## Unreleased

### Published all at once, or not at all

- **Added** `Transaction`, `Client::start_transaction`,
  `Client::start_transaction_with` and `Client::with_transaction`. Everything
  sent through a transaction is invisible to everything else until it commits,
  and is discarded if it does not — so a launcher that dies halfway leaves no
  empty table, no stale worker and no half-replaced result.
- **Fixed** `Client::exists`, which read the answer out of an `exists` key the
  cluster does not send and so failed **every** call with a decode error. It
  reads `value`, as `get` does. Nothing in the crate called it until now, which
  is how it survived two releases; a captured response is a test now.

`Transaction` derefs to a `Client` bound to it, so `tx.write_table(…)` writes
inside the transaction and `tx.start_map(…)` runs the operation inside it. The
transaction ID is stamped onto every command in one place — the transport —
because a command that forgot it would quietly do its work outside the
transaction, which is the failure a transaction exists to prevent. A command
that names a transaction itself keeps the one it named, so committing a nested
transaction commits the one meant.

**Dropping the handle aborts it.** That is what makes `?` safe inside a
transaction: a failure returns from the function, the handle drops on the way
out, and the cluster is left as it was. Only `commit` publishes.

Two facts about the cluster, both watched rather than assumed:

- **A transaction expires 30 seconds after its last ping.** Verified: one with a
  two-second timeout, left alone for four, answers a ping with `Transaction …
  has expired or was aborted`. So the handle keeps a thread pinging three times
  per timeout for as long as it lives, which is what makes a transaction usable
  around an operation that runs for an hour. Without it the feature would work
  in an example and fail on anything real.
- **Committing twice is an error**, not a no-op: `No such transaction`, which
  reads like the commit failed when it succeeded. The commit therefore carries a
  mutation ID, so a retry after a lost answer is the same commit rather than a
  second one.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
transaction`: a table visible only inside its transaction and gone after the
abort; a launcher that fails halfway and leaves nothing, with no cleanup code in
it; a map operation whose worker upload *and* output table appear only at the
commit; a command in an aborted transaction refused with `No such transaction`;
and a two-second transaction committed six seconds in, which only the ping
thread makes possible.

### A table can be told what its rows look like

- **Added** the `schema` module — `TableSchema`, `Column`, `ColumnType`,
  `SortOrder` and the `TableRow` trait — plus `Client::create_table` and
  `Client::table_schema`.
- **Added** the `derive` feature, which re-exports `#[derive(TableRow)]` from
  the new [`ytsaurus-helpers`](../ytsaurus-helpers/) crate. Off by default: it
  is a compiler plugin, and a crate that only launches operations should not pay
  to build one.

A schematised table is checked on every write. The example run against a local
cluster ends with the cluster refusing a row that left a required column out —
`Required column "size" cannot have "null" value` — which is the whole point of
saying what the rows look like.

`TableSchema::validate` catches locally what the cluster answers with error 314
a round trip later: key columns that are not a prefix, duplicate names, names
starting with `@`, `unique_keys` without a key, and a required `any`. Each
becomes one sentence naming the column.

Four protocol facts behind this, all watched on a cluster rather than taken from
the documentation:

- **A schema passed as a top-level `schema` on `create` is silently ignored.**
  The request returns 200 and a node id, and the table comes back with an empty
  weak schema. It has to go inside `attributes`. This is the single worst
  mistake the command allows, and it is why `create_table` exists rather than a
  `schema` argument on `create`.
- `create_table` deliberately **fails if the path exists**: the cluster ignores
  the attributes of a create it skips, so an `ignore_existing` version would
  quietly leave the old schema in place and report success.
- **`boolean`/`any` are the `type` spellings; `bool`/`yson` are the `type_v3`
  ones.** Those two names are the only ones that differ between the
  vocabularies, and mixing them is refused —
  `Error parsing ESimpleLogicalValueType value "bool"`.
- **Three types can never be required** — `any`, `null` and `void`. Each already
  means "there may be nothing here".

All 26 column types the crate can name were created on a local cluster and
accepted. Descending sort order was *not*: `Descending sort order is not
available in this context yet`, so `SortOrder::Descending` says as much on
itself and the example checks it rather than asserting it, so the day a cluster
enables it the run says so.

### Operations with no input tables

- **Added** `VanillaSpec`, `VanillaTask` and `Client::start_vanilla`. A vanilla
  operation runs jobs that are not a transformation of a table — a distributed
  process, a side-car computation, a job that fetches its own input — which is a
  whole category this stack could not reach.

A task says how many jobs of its kind to run and, optionally, which tables they
write; the scheduler keeps that many going. Everything else — `gang_options` for
a coordinated process, `stderr_table_path` — goes through `with_raw`.
`output_table_paths` is always sent, even empty: not sending it is a different
statement from "there are none".

Coordination between the jobs is the user's problem, and
[`ytsaurus_job::job_cookie`](../ytsaurus-job/CHANGELOG.md) is what to divide the
work by.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
vanilla`: three jobs with nothing to read, identifying themselves as 0, 1 and 2,
whose slices of a sum add up to the whole and cover every number exactly once.

### Reading back what the jobs reported

- **Added** `Client::custom_statistics` and `Client::statistic_sum`, the other
  half of [`JobStatistics`](../ytsaurus-job/CHANGELOG.md).

The tree the cluster files them in is deeper than the name suggests, and the
shape was taken from a live cluster rather than guessed:

```text
{"rows/rejected"={"$"={completed={map={count=1;max=3;min=3;sum=3}}}}}
```

The statistic's name keeps its slash as **one key** — it does not nest, so a
path-walking lookup finds nothing. Below it sit `$`, the job state, and the job
type. `statistic_sum` totals `completed` jobs across job types: a map-reduce
reporting one name from both phases gives the operation's total, while an
aborted job's work is redone by its replacement and counting it would double.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
statistics`: a job that drops rows without a `key` column reports having read
seven and rejected three, and the operation — which succeeded, with a shorter
output table and no other sign anything was dropped — reports the same.

### The worker is uploaded once, not once per launch

- **Added** `Client::upload_worker_cached`, `Client::file_from_cache`,
  `Client::put_file_to_cache` and `Client::with_file_cache`. The cluster keeps a
  file cache keyed by MD5; an unchanged binary is now found there instead of
  being re-sent, which is the slowest part of a dev loop that changes only the
  spec.
- **Added** `with_local_file_named` to all three spec builders. A cached node is
  named after its hash, so `./my_job` would find nothing to run without a
  `file_name` attribute on the path. `file_paths` entries are YSON values now
  rather than plain strings, which is what makes such attributes expressible.
- **Added** a dependency on `md5` (0.8), chosen for having no dependencies of
  its own: this crate is linked into worker binaries that cross-compile to musl
  with nothing but the Rust toolchain.

The cache defaults to `//tmp/yt_wrapper/file_storage/new_cache`, the path the
Python wrapper uses, so an installation that already expires entries there
expires ours too.

Two things the cluster settled, both now handled: `get_file_from_cache` and
`put_file_to_cache` answer with a **bare string** rather than the usual
`{path=…}` envelope, and a cache miss is an **empty string**, not an error or an
entity.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
cached_upload`: first call uploads (166 ms), second is a hit (32 ms) on the same
path, and the cached binary runs as a job — so both the `executable` attribute
and the sandbox name survive the trip through the cache.

### A transient failure no longer kills the run

A shared cluster produces failures that pass on their own — a restarting proxy,
a scheduler that has lost the master. One of those used to end the run. Light
commands are now repeated, following the
[documented rules](https://ytsaurus.tech/docs/en/api/commands#retry).

- **Added** `RetryPolicy` and `Client::with_retries`. Five attempts by default,
  with a delay that doubles from one second to ten. `RetryPolicy::none()` turns
  it off.
- **Added** `MutationId` and `Client::start_operation_with`. Every mutating
  command the client sends now carries a `mutation_id`, so a repeated request is
  deduplicated by the cluster rather than applied twice — without it, a 503 on
  the way *back* from a successful `start_operation` would leave the retry
  starting a second operation over the same tables.
- **Heavy commands are still sent once**, whatever the policy says: the
  documentation is explicit that they cannot be retried, and a transaction is
  the way to make an upload atomic.
- Retriable failures are transport errors, HTTP 429/500/502/503/504, and
  YTsaurus codes 3, 100, 105, 108, 904 and 2100 — the same set the Python
  client retries on. A retriable code is looked for throughout the error
  document, because the outer error is often a `Request retries failed` wrapper
  with the real reason nested inside. Codes that mean the request was wrong
  (500 resolve, 501 already exists) are never retried.

**A replay must admit to being one.** The cluster refuses a repeated
`mutation_id` sent without the `retry` flag — `Duplicate request is not marked
as "retry"` — rather than deduplicating it. So the flag travels with the ID:
`MutationId::as_retry()` marks a send as a replay, which is what a
crash-and-restart needs when it reuses a persisted ID.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
idempotent`: the same ID twice returns one operation, a fresh ID starts a
second. The retry classification is unit-tested, including on the exact error
document a local cluster produced when its scheduler could not reach the master.

### Reduce and sort as operations of their own

- **Added** `ReduceSpec` / `Client::start_reduce` and `SortSpec` /
  `Client::start_sort`. Reduce over an already-sorted table is one of the most
  common operation shapes, and reaching for map-reduce instead pays for a
  shuffle that has already happened. Sort is what produces the sorted table, and
  it can then be reduced again and again.

- A reduce's `key_switch` goes under **`job_io`**, not `reduce_job_io`. That is
  the map-reduce trap in the other direction — one job type, one I/O section —
  and the wrong spelling is accepted and silently ignored, leaving the reducer
  to fold every key into one group. Both spellings are now pinned by tests.

- `sort_by` is only sent when asked for: the cluster defaults it to `reduce_by`,
  and stating it turns on a sortedness check the caller did not request.

- `SortSpec` renders **`output_table_path`** — singular, and a string rather
  than a list. Sort writes exactly one table however many it reads, and the
  plural spelling every other operation uses is rejected.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
sort_reduce`: seven unsorted rows sorted (`@sorted_by` becomes `[word]`), then
reduced to four correct per-word totals. Four rows rather than one is itself the
proof that `key_switch` reached the reducer.

### The binary can upload itself

- **Added** `Client::upload_current_exe`, which uploads the running executable.
  Together with
  [`ytsaurus_job::is_inside_job`](../ytsaurus-job/CHANGELOG.md) this is the
  one-binary pattern: the same program launches the operation and runs as its
  job, so what the cluster runs is what you just built.

- **Added** `ClientError::NotAWorker`. The running executable is often not
  something a node can exec — Mach-O on macOS, dynamically linked on a
  developer's Linux — and both fail on the node minutes later with an error that
  names no cause. `upload_current_exe` reads the ELF header first (Linux,
  x86-64, no interpreter) and refuses with an error that says what to build
  instead. `upload_worker` is unchanged and stays permissive: a job command can
  legitimately be a shell script.

- **Added** the `tls` feature, on by default. Turning it off drops `rustls`,
  which drags in `ring`, which needs a C toolchain to reach musl — and a binary
  that is both launcher and job has to reach musl. With it off, an `https://`
  proxy fails with an error that names the feature rather than a confusing
  connection error. Defaults are unchanged for existing users.

  This is what lets `scripts/build-worker.sh` keep its promise of needing
  nothing but the Rust toolchain, verified by cross-compiling a worker that
  contains the whole client to static musl on macOS.

Verified end to end on the local cluster from both sides: the launcher refusing
a Mach-O binary, and the musl build of the same source uploading *itself* from
inside a Linux container and being run as the job.

### Failed operations explain themselves

`wait_for_operation` now reports *why* an operation failed. On a terminal
`failed` or `aborted` it asks the cluster which jobs failed and what each wrote
to stderr, and puts both in the error. Before this, a failed operation gave you
a state string and a trip to the web UI.

- **Added** `Client::list_jobs` and `Client::get_job_stderr`, plus the `JobInfo`
  and `JobFailure` types they return.
- **Added** `Client::with_job_diagnostics`, to turn the report off. The
  YTsaurus documentation asks that `list_jobs` not be used without an
  administrator's approval; this is the way to respect that.
- **Changed** the operation error is now the flattened message
  (`Failed jobs limit exceeded: Process terminated by signal 6`) rather than a
  truncated raw document, falling back to the raw document if the shape moves.
- **Breaking** `ClientError::OperationFailed` gained a `jobs` field. Code that
  matches the variant by name is unaffected; code that destructures every field
  needs `..`.

Collecting the report is best-effort throughout: it runs while an error is being
built, and a diagnostic that replaces the failure it was explaining would be
worse than no diagnostic.

Verified on the local cluster with the new `boom` worker, which panics on its
first row, driven by `cargo run -p ytsaurus-client --example diagnose`. The
`list_jobs` response it produced is kept as a test fixture in
`tests/fixtures/list_jobs_failed.yson`.

Two things that capture taught us, both now pinned by tests:

- `stderr_size` is a hint, not a length — the cluster reported `1` for a job
  whose stderr was several hundred bytes, so the client asks for stderr whatever
  the field says.
- The useful part of a job error is the innermost one. `User job failed` is a
  category; `Process terminated by signal 6` — a Rust panic under
  `panic = "abort"` — is the answer.

## 0.2.0

First release of this crate. Version tracks the workspace.

A thin HTTP API v4 client: enough to run a Rust worker with no Python
installation. Covers Cypress (`create`, `remove`, `exists`, `get`, `row_count`),
data (`upload_worker`, `write_file`, `write_table`, `read_table`,
`set_attribute`) and operations (`start_map`, `start_map_reduce`,
`start_operation`, `operation_state`, `wait_for_operation`), with `MapSpec` and
`MapReduceSpec` builders.

Verified against a local cluster with nothing Python on `PATH`.

Two limits are documented rather than hidden: heavy commands are not routed via
`/hosts`, and `ureq` 3.3 exposes no trailers, so a failure the proxy reports
mid-stream cannot be seen. `read_table` compensates by rejecting a response that
is not a complete YSON list fragment.
