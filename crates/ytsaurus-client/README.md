# ytsaurus-client

[![crates.io](https://img.shields.io/crates/v/ytsaurus-client.svg)](https://crates.io/crates/ytsaurus-client)
[![docs.rs](https://img.shields.io/docsrs/ytsaurus-client)](https://docs.rs/ytsaurus-client)
[![CI](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)

A thin [YTsaurus](https://ytsaurus.tech) HTTP API v4 client: enough to run a Rust
worker **without a Python installation**.

```toml
[dependencies]
ytsaurus-client = "0.2"
```

```rust
use ytsaurus_client::{Client, MapSpec};

# fn demo() -> Result<(), ytsaurus_client::ClientError> {
let client = Client::from_env()?;                  // YT_PROXY, and the CLI's token

client.upload_worker("target/…/my_job", "//tmp/my_job")?;

let spec = MapSpec::new("./my_job", ["//tmp/in"], ["//tmp/out"])
    .with_local_file("//tmp/my_job")
    .with_memory_limit(512 * 1024 * 1024);

let id = client.start_map(&spec)?;
client.wait_for_operation(&id)?;
# Ok(())
# }
```

A runnable version is [`examples/launch.rs`](examples/launch.rs), which creates
tables, uploads a worker, writes rows, runs a map, waits for it and verifies the
result:

```sh
export YT_PROXY=http://localhost:8000
cargo run -p ytsaurus-client --example launch
```

## What it covers

| | |
| --- | --- |
| Cypress | `create`, `create_table`, `alter_table`, `remove`, `exists`, `get`, `list`, `row_count`, `table_schema` |
| Naming | `copy`, `move_node`, `link` — each with a `_replacing` twin that overwrites |
| Locks | `lock`, `lock_waiting` |
| Data | `upload_worker`, `upload_worker_cached`, `upload_current_exe`, `write_file`, `write_table`, `read_table`, `set_attribute` |
| Streaming | `read_table_streaming`, `write_table_streaming` |
| File cache | `file_from_cache`, `put_file_to_cache` |
| Operations | `start_map`, `start_reduce`, `start_sort`, `start_map_reduce`, `start_vanilla`, `start_operation`, `operation_state`, `wait_for_operation` |
| Jobs | `list_jobs`, `get_job_stderr`, `custom_statistics`, `statistic_sum` |
| Transactions | `start_transaction`, `with_transaction`, `Transaction::{commit, abort, ping}` |

Specs are built with [`MapSpec`] / [`ReduceSpec`] / [`SortSpec`] /
[`MapReduceSpec`] / [`VanillaSpec`], which model what launching a
`ytsaurus-job` worker needs and expose `with_raw` for everything else.

Two defaults exist because getting them wrong is quiet rather than loud:

- **both formats are binary YSON**, which is what `JobReader` and `JobWriter`
  expect;
- **`key_switch` is on** for both grouping operations, and lands in the right
  section for each: `reduce_job_io` for map-reduce, which has several job types
  and so an I/O section per type, and plain `job_io` for reduce, which has one.
  The wrong spelling is accepted and then ignored, leaving the reducer to fold
  every key into one group.

Reduce needs sorted input; `SortSpec` is what produces it, and its
`output_table_path` is singular — sort writes one table however many it reads.
[`examples/sort_reduce.rs`](examples/sort_reduce.rs) runs both against a
cluster.

`upload_worker` sets the `executable` attribute. Without it the cluster copies
the binary and then refuses to exec it, with an error that never mentions the
attribute.

## Typed tables

A table with no schema takes whatever a job writes and finds out later. The
schema is already written, though — it is the struct the rows have:

```rust
use ytsaurus_client::TableRow;

#[derive(TableRow)]
struct Visit<'a> {
    #[yt(key)]
    host: &'a str,               // utf8, required, and the table comes out sorted
    size: i64,                   // int64, required
    referrer: Option<&'a str>,   // optional, because the Rust type says so
}

client.create_table("//tmp/visits", &Visit::table_schema())?;
```

Needs `features = ["derive"]`, which re-exports the macro from
[`ytsaurus-helpers`](../ytsaurus-helpers/). The cluster then refuses a row that
leaves out a required column — `Required column "size" cannot have "null"
value` — which is the whole point of saying what the rows look like.

`TableSchema::validate` catches locally what the cluster answers with error 314
a round trip later: key columns that are not a prefix, duplicate names, a
required `any`, `unique_keys` with no key.

**`create_table` fails if the path exists.** The cluster ignores the attributes
of a create it skips, so a version that tolerated an existing table would
quietly leave the old schema in place and report success.

### Changing it afterwards

The struct gains a field; `alter_table` widens the table to match. **A table
with rows takes only changes that ask less of the rows already written**: an
optional column may be added, a required one may be relaxed, `strict` may be
dropped. Removing a column, adding a required one, changing a type or making the
table sorted are each refused, by name — `Cannot insert a new required column
"must" into a non-empty table`.

Two things to know before either becomes permanent:

- **An empty table accepts all of it**, so a migration rehearsed on an empty
  table has proved nothing about the real one.
- **A non-strict schema can never gain a named column.** Relaxing `strict` is a
  one-way door out of schema evolution.

The schema is a top-level parameter here and an attribute in `create` — the two
commands are opposites, and only `alter_table` complains when you get it wrong.

## All at once, or not at all

A launcher creates a table, uploads a worker and runs an operation. Each of
those is a chance to fail halfway, and each failure leaves something behind: an
empty table, a stale binary, an output table holding neither the old result nor
the new one. A transaction makes the whole sequence one event:

```rust
fn publish(client: &Client) -> Result<(), ClientError> {
    let tx = client.start_transaction()?;

    tx.upload_worker(WORKER, "//tmp/my_job")?;
    let id = tx.start_map(&spec)?;
    tx.wait_for_operation(&id)?;

    tx.commit()                       // and only now does any of it exist
}
```

`Transaction` derefs to a `Client` bound to it, so every command above happens
inside the transaction. **Dropping it aborts it** — which is what makes those
`?`s safe: a failure returns from the function, the handle drops on the way out,
and the cluster is left exactly as it was. There is no cleanup code to write and
none to forget.

Two things the cluster insists on, and one the client does about them:

- **A transaction expires 30 seconds after its last ping.** The handle keeps a
  thread pinging it three times per timeout for as long as it lives, so a
  transaction wrapped around an hour-long operation survives. Without that the
  scheduler would abort the operation halfway.
- **Nothing outside the transaction sees its work** — that is the point, and also
  the trap. A `read_table` from a client that is not in the transaction reads
  what was there before, and a second writer blocks on the lock the first took.

[`examples/transaction.rs`](examples/transaction.rs) watches all of it on a
cluster, including a launcher that fails halfway and leaves nothing behind.

## Naming what you produced

A pipeline's results need names — yesterday's run beside today's, and something
that always points at the newest:

```rust
client.move_replacing(&staging, &format!("//tmp/runs/{today}"))?;
client.link_replacing(&format!("//tmp/runs/{today}"), "//tmp/runs/latest")?;
```

Readers following `latest` see the previous run until that second line and the
new one after it, and never a half-written table. Three things about this that
are easy to get wrong, all watched on a cluster:

- **`list` is not sorted.** Three dated tables came back as the second, the third
  and then the first.
- **A truncated listing is an attribute, not an error**: `<incomplete=%true>[…]`.
  `list` refuses one rather than handing back a listing quietly missing entries.
- **A link resolves to its target, attributes included.** `latest/@type` answers
  `table`; `latest&/@type` answers `link`. The `&` is the difference between
  asking *about* the link and asking *through* it.

Locks are the other half — a lock belongs to a transaction, so `lock` refuses
before sending anything if the client is not in one:

```rust
let tx = client.start_transaction()?;
tx.lock("//tmp/runs/latest", LockMode::Exclusive)?;   // or wait: lock_waiting
```

**A waitable lock is granted later, or never.** The cluster answers immediately
with a lock that is `pending`, and treating that as held is the mistake the
command invites; `lock_waiting` returns only when the cluster says `acquired`.
Its deadline is not a nicety: a transaction that already holds a snapshot lock
on the node is *refused* an exclusive one, but the waitable version of that same
request queues behind a lock only that transaction's end will release, and
waits forever without a word.

[`examples/cypress.rs`](examples/cypress.rs) runs all of it, ending with three
transactions competing for one lock.

## One binary, two roles

`upload_current_exe` uploads the *running* executable, so the same program can
launch the operation and be the job it runs:

```rust
fn main() {
    ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job
    launch().unwrap();                         // only your machine gets here
}
```

There is no second artifact to forget to rebuild. The running executable has to
be something a node can exec, so its ELF header is checked first — Linux,
x86-64, statically linked — and refused with `ClientError::NotAWorker` when it
is not, rather than failing on the node minutes later. On macOS the launcher is
Mach-O and cannot be the uploaded file: build the worker with
`scripts/build-worker.sh` and upload that with `upload_worker`. The source is
still one file — see
[`examples/src/bin/selfrun.rs`](../../examples/src/bin/selfrun.rs).

## Talking to a real installation

`Client::from_env` reads `YT_PROXY`, and finds a token the way the `yt` CLI
does: `YT_TOKEN`, then the file named by `YT_TOKEN_PATH`, then `~/.yt/token`. A
machine where the CLI already works needs nothing else. A token read from a file
is trimmed — `echo token > ~/.yt/token` leaves a newline, and sending it fails
authentication with an error that never mentions a newline.

**Responses are compressed.** Every request carries `Accept-Encoding: gzip` and
every answer is decompressed on the way in, including a streamed table read: on
a local cluster 67.7 MiB of table arrived as 400 KiB. Uploads are not
compressed, though the proxy would accept it — that costs a compression
dependency in a crate that gets cross-compiled to musl.

## Features

`tls` (default) brings in `rustls`, and with it `https://` proxies. Turning it
off leaves a client that speaks plain HTTP and needs no C toolchain — which is
how a binary that is both launcher and job gets cross-compiled to musl. Without
it, an `https://` proxy fails with an error naming the feature.

## When an operation fails

`wait_for_operation` does not stop at the state. It asks which jobs failed and
what they printed, so the error carries the job's own words:

```text
operation 1ba94195-… finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
  job 24c164af-… on localhost:24403: User job failed: Process terminated by signal 6
  stderr:
    thread 'main' panicked at examples/src/bin/boom.rs:37:17:
    boom: this job fails on purpose (row 1, 23 bytes)
```

That costs one `list_jobs` and a few `get_job_stderr` calls, on failure only.
The YTsaurus documentation asks that `list_jobs` not be used without an
administrator's approval, so `Client::with_job_diagnostics(false)` turns the
report off. Failing to collect it never replaces the failure being reported.

[`examples/diagnose.rs`](examples/diagnose.rs) runs the whole path against a
local cluster.

## Tables bigger than memory

`read_table` and `write_table` hold a whole table at once. The streaming pair
moves the same bytes without ever holding more than a buffer of them:

```rust
let mut reader = JobReader::binary(client.read_table_streaming("//tmp/big")?);
while let Some(event) = reader.next_event()? { /* … */ }

client.write_table_streaming("//tmp/big", File::open("rows.yson")?)?;
```

The bytes are the same binary YSON list fragment a job reads on fd 0, so the
same decoder handles a table read on a laptop and a table read inside a job.
Measured on a local cluster by
[`examples/streaming.rs`](examples/streaming.rs), which writes a table from a
generator and then reads it back both ways:

```text
Writing about 64 MiB from a generator     1242757 rows, peak RSS 2.9 MiB
Reading it back as a stream               1242757 rows counted, peak RSS 3.8 MiB
The same table, read into memory          67.7 MiB in hand, peak RSS 74.7 MiB
```

Two things streaming gives up, on purpose:

- **No completeness check.** `read_table` verifies the response is a whole YSON
  list fragment — the only defence against a mid-stream failure this client
  cannot see. Streaming has no whole thing to check, so the defence moves to the
  decoder, which fails on the record that was cut in half.
- **No retry, ever.** A reader that has been consumed cannot be sent again, so a
  streaming write is one attempt in principle rather than by policy.

## Upload the worker once

Re-sending tens of megabytes on every launch is the slowest part of a dev loop
that changes only the spec. The cluster's file cache is keyed by MD5:

```rust
let worker = client.upload_worker_cached("target/…/my_job")?;   // uploaded, or found

let spec = MapSpec::new("./my_job", ["//tmp/in"], ["//tmp/out"])
    .with_local_file_named(&worker.path, &worker.name);
```

`worker.uploaded` says which it was. The name has to be passed along because the
cached node is named after the hash — `./my_job` would find nothing to run
otherwise. The cache defaults to the path the Python wrapper uses, so it is
shared with everything else on the installation;
`Client::with_file_cache` moves it.

## Retries

A shared cluster produces failures that pass on their own. Light commands are
repeated — five attempts by default, with a delay doubling from one second to
ten; `Client::with_retries(RetryPolicy::none())` turns that off.

Mutating commands carry a `mutation_id`, so a repeated request is deduplicated
by the cluster instead of being applied twice. Two things follow from how the
cluster implements that:

- a replay must be **marked** as one. Re-sending a known ID without the `retry`
  flag is refused — `Duplicate request is not marked as "retry"` — so
  `MutationId::as_retry()` is what a restarted process uses with a persisted ID
  (`Client::start_operation_with`);
- IDs are remembered for five to ten minutes, so this guards against a crash and
  restart, not forever.

**Heavy commands are not retried**, whatever the policy says: the documentation
is explicit that they cannot be, and [a transaction](#all-at-once-or-not-at-all)
is the way to make an upload atomic.

## Limits worth knowing

**Heavy commands go to the address you gave it.** Large installations separate
light and heavy proxies and answer an upload on a light proxy with 503. Use
[`Client::heavy_proxy`] to discover one and point a second client at it. A local
cluster needs none of this.

**Trailers are not read.** The proxy reports a failure discovered mid-stream in
an `X-YT-Error` trailer, and `ureq` 3.3 exposes none — rechecked against its
source, where the word does not appear. `read_table` compensates by checking the
response is a complete YSON list fragment, so a truncated read is caught; a
mid-stream failure that still yields well-formed output would not be.

**`read_table` and `write_table` hold the whole table.** They are for results a
launcher inspects; `read_table_streaming` and `write_table_streaming` are for
everything larger.

## Why not JSON

Parameters and specs are encoded with [`ytsaurus-yson`](../ytsaurus-yson/), this
project's own codec, rather than JSON. It keeps the dependency list short and
means every request exercises the codec against a real cluster.

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](../../NOTICE).

[`MapSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.MapSpec.html
[`MapReduceSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.MapReduceSpec.html
[`ReduceSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.ReduceSpec.html
[`SortSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.SortSpec.html
[`VanillaSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.VanillaSpec.html
[`Client::heavy_proxy`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.Client.html#method.heavy_proxy
