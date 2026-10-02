# ytsaurus-client

[![crates.io](https://img.shields.io/crates/v/ytsaurus-client.svg)](https://crates.io/crates/ytsaurus-client)
[![docs.rs](https://img.shields.io/docsrs/ytsaurus-client)](https://docs.rs/ytsaurus-client)
[![CI](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)

A thin [YTsaurus](https://ytsaurus.tech) HTTP API v4 client: enough to run a Rust
worker without a Python installation.

```toml
[dependencies]
ytsaurus-client = "0.3"
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

Cluster behaviour, with evidence:
[protocol reference](../../docs/protocol-reference.md).

## What it covers

| | |
| --- | --- |
| Cypress | `create`, `create_table`, `alter_table`, `remove`, `exists`, `get`, `list`, `row_count`, `table_schema` |
| Naming | `copy`, `move_node`, `link`, each with a `_replacing` twin that overwrites |
| Locks | `lock`, `lock_waiting` |
| Data | `upload_worker`, `upload_worker_cached`, `upload_current_exe`, `write_file`, `read_file`, `write_table`, `read_table`, `set_attribute` |
| Formats | `write_table_with_format`, `read_table_with_format`, `write_skiff_table`, `read_skiff_table` |
| Typed | `write_table_rows`, `read_table_rows`, `get_as` |
| Streaming | `read_table_streaming`, `write_table_streaming`, `read_file_streaming` |
| File cache | `file_from_cache`, `put_file_to_cache` |
| Operations | `start_map`, `start_reduce`, `start_sort`, `start_map_reduce`, `start_vanilla`, `start_merge`, `start_erase`, `start_remote_copy`, `start_operation`, `operation_state`, `wait_for_operation`, `operation_result_error` |
| Lifecycle | `abort_operation`, `suspend_operation`, `resume_operation`, `complete_operation`, `update_operation_parameters`, `operation_suspended`, `operation_status`, `attach_operation` → `Operation` |
| Finding one | `list_operations`, `get_operation`, `get_operation_by_alias`, `list_operation_events` |
| Jobs | `list_jobs`, `get_job`, `get_job_stderr`, `get_job_input`, `custom_statistics`, `statistic_sum`, `job_statistics`, `job_statistic_sum` |
| Transactions | `start_transaction`, `with_transaction`, `Transaction::{commit, abort, ping}` |
| Anything else | `raw_command`, `raw_command_with`, `raw_command_streaming`, `raw_command_upload` |

[`MapSpec`], [`ReduceSpec`], [`SortSpec`], [`MapReduceSpec`], [`VanillaSpec`],
[`MergeSpec`], [`EraseSpec`] and [`RemoteCopySpec`] model what a `ytsaurus-job`
worker needs; `with_raw` sets anything else. `OperationType` names all nine types the cluster
registers; `join_reduce` has no builder, because the current documentation
describes the same work as a reduce with `join_by` and
`enable_key_guarantee=%false`.

`DataFormat` (binary or text YSON, or validated dynamic Skiff) is taken by
`MapSpec::with_formats`, the map-reduce equivalents, and the `_with_format`
table methods. Skiff table I/O derives the column projection from the format,
as the Go SDK does; the `*_skiff_*` methods are wrappers. Skiff has no typed rows or schema inference
yet ([compatibility contract](../../docs/skiff-compatibility.md)).
[`skiff_launch.rs`](examples/skiff_launch.rs) runs a Skiff map over non-UTF-8
`string32` data with the `skiff_cat` worker:

```sh
./scripts/build-worker.sh skiff_cat
export YT_PROXY=http://localhost:8000
cargo run -p ytsaurus-client --example skiff_launch
```

Defaults that prevent mistakes the cluster does not report:

- Both formats are binary YSON, which `JobReader` and `JobWriter` expect.
- `key_switch` is on for both grouping operations: in `reduce_job_io` for
  map-reduce (a section per job type) and `job_io` for reduce. The wrong section
  is accepted and ignored, and the reducer sees every key as one group.
- `upload_worker` sets the `executable` attribute, without which the cluster
  refuses to exec the binary with an error that does not mention it.

`SortSpec` produces the sorted input reduce needs; its `output_table_path` is
singular, since sort writes one table
([`examples/sort_reduce.rs`](examples/sort_reduce.rs)).

## Configuration

`Client::from_env` reads:

| Variable | What it does |
| --- | --- |
| `YT_PROXY` | The cluster address. A bare host name means `https://`. Required. |
| `YT_TOKEN`, `YT_TOKEN_PATH` | The token, found as the `yt` CLI finds it: `YT_TOKEN`, then the file named by `YT_TOKEN_PATH`, then `~/.yt/token`. A token read from a file is trimmed, so a trailing newline does not fail authentication. |
| `YT_CA_BUNDLE` | A PEM file of roots to trust instead of the compiled-in Mozilla bundle. See [TLS](#tls). |
| `YT_PROXY_SUFFIX` | Completes a bare cluster name: `YT_PROXY=hume` with `YT_PROXY_SUFFIX=.yt.example.net` addresses `hume.yt.example.net`. Applied only to a name with no dot, no colon and no `localhost`, the Go SDK's gate. No suffix is compiled in. |
| `YT_HEAVY_PROXY_DOMAINS` | `Client::with_heavy_proxies_under`, comma- or space-separated. |
| `YT_HEAVY_PROXIES_ANYWHERE` | `1`, `true` or `yes` for `Client::with_heavy_proxies_anywhere`. Applied after the domains, so the wider rule wins. |
| `YT_FILE_CACHE` | `Client::with_file_cache`, for an installation whose shared worker cache is read-only to you. |

The last four are inert when unset, so a machine that sets none gets exactly
what `Client::new` gives. A variable set to the empty string counts as
unset (`export YT_FILE_CACHE=` turns one off), `YT_PROXY` included.

### TLS

TLS is `rustls` with the Mozilla roots compiled in, so a cluster whose
certificate chains to a private CA fails with
`invalid peer certificate: UnknownIssuer` even where `curl` succeeds. The error
names both remedies: `YT_CA_BUNDLE`, or the `platform-verifier` feature, which
trusts the operating system's store. The bundle wins when both are set.

```sh
export YT_PROXY=cluster.example.net
export YT_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
```

Every certificate in the bundle is a root; other PEM sections are skipped. A
bundle is refused, naming the file, if it yields no certificates, cannot be
read, is not a regular file, or is larger than 16 MB. **One `BEGIN CERTIFICATE`
block that is not X.509 refuses the whole file**: a PKCS#7 `.p7b` under that
label is the usual case, and `openssl pkcs7 -print_certs` converts it.

An unknown issuer or a certificate that does not cover the requested host is
reported at once and not retried, since neither changes between attempts.
Other TLS errors are retried: an expired certificate (a fleet mid-rotation may
answer with a renewed one), and a platform verifier's `Other(…)`, which is how
`rustls-platform-verifier` reports a failed revocation lookup or a briefly
unavailable trust store. So is a reset connection.

Responses are gzip-compressed. Uploads are not: that needs a compression
dependency in a crate cross-compiled to musl.

## Features

| Feature | Default | |
| --- | --- | --- |
| `tls` | on | `rustls` and `https://` proxies. Without it the client needs no C toolchain, so a launcher-and-job binary cross-compiles to musl; an `https://` proxy then fails, naming the feature. |
| `platform-verifier` | off | Trust the operating system's store instead of the Mozilla bundle. Off because it costs `rustls-platform-verifier`, and the compiled-in bundle is safer for a client running outside the network it talks to. |
| `rpc` | off | The RPC proxy as a second transport: `create_rpc_client` beside `create_client`, both returning [`ytsaurus_api::TableClient`](https://docs.rs/ytsaurus-api). It is for latency and throughput under concurrency (one connection multiplexes many requests); HTTP v4 already reaches the dynamic-table commands. |
| `tracing` | off | A span per attempt; see [Seeing what it did](#seeing-what-it-did). Adds `tracing`, `pin-project-lite`, `tracing-core` and `once_cell` (already in a default build); no `#[instrument]`, so no `attributes`. |
| `derive` | off | `#[derive(TableRow)]`, a table schema read off the row struct. |

**`rpc` and `tracing` must stay off for worker builds.** `ytsaurus-job`'s
examples are the static musl workers, and they take this crate with
`default-features = false`. `rpc` reaches `tokio` and `prost`; CI asserts the
worker graph carries neither.

## Rows are Rust values

```rust
client.write_table_rows("//tmp/contacts", (0..100).map(contact))?;

let back: Vec<Contact> = client.read_table_rows("//tmp/contacts")?;
let root: ClusterInfo = client.get_as("//@")?;
```

`write_table_rows` encodes from an iterator inside the request body, so a
million rows cost one buffer. `read_table_rows` returns the whole table as owned
rows; a struct naming three of twenty columns is a projection. For larger tables
see [`read_table_streaming`](#tables-bigger-than-memory).
[`docs/go-parity.md`](../../docs/go-parity.md) compares the API with the Go
SDK's examples.

## Typed tables

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

Needs `features = ["derive"]` (the macro is from
[`ytsaurus-helpers`](../ytsaurus-helpers/)). The cluster then refuses a row
missing a required column: `Required column "size" cannot have "null" value`.
`TableSchema::validate` catches locally what the cluster would answer with
error 314: key columns that are not a prefix, duplicate names, a required
`any`, `unique_keys` with no key. `create_table` fails if the path exists,
because the cluster ignores a skipped create's attributes and would keep the old
schema.

On a table with rows, `alter_table` may add an optional column, relax a
required one or drop `strict`; removing a column, adding a required one,
changing a type or sorting the table is refused, naming the column
([full list](../../docs/protocol-reference.md#changing-a-schema)). **An empty
table accepts every change**, so a migration rehearsed on one proves nothing.
**A non-strict schema can never gain a named column**, so relaxing `strict`
cannot be undone.

## All at once, or not at all

A launcher that fails between steps leaves an empty table, a stale binary, or an
output table holding neither the old result nor the new one. A transaction
makes the sequence one event:

```rust
fn publish(client: &Client) -> Result<(), ClientError> {
    let tx = client.start_transaction()?;

    tx.upload_worker(WORKER, "//tmp/my_job")?;
    let id = tx.start_map(&spec)?;
    tx.wait_for_operation(&id)?;

    tx.commit()                       // and only now does any of it exist
}
```

`Transaction` derefs to a `Client` bound to it. Dropping it aborts it, so an
early `?` leaves the cluster as it was. A transaction expires 30
seconds after its last ping; the handle pings from a thread three times per
timeout while it lives, so it can span an hour-long operation. Nothing outside
the transaction sees its work, and a second writer blocks on its locks.
See [`examples/transaction.rs`](examples/transaction.rs).

## Naming what you produced

```rust
client.move_replacing(&staging, &format!("//tmp/runs/{today}"))?;
client.link_replacing(&format!("//tmp/runs/{today}"), "//tmp/runs/latest")?;
```

Readers of `latest` never see a half-written table. `list` is not sorted, and refuses a truncated
(`<incomplete=%true>[…]`) listing. A link resolves to its target, attributes
included: `latest/@type` is `table`, `latest&/@type` is `link`.

`lock` needs a transaction, and refuses before sending anything without one:

```rust
let tx = client.start_transaction()?;
tx.lock("//tmp/runs/latest", LockMode::Exclusive)?;   // or wait: lock_waiting
```

**A waitable lock is answered as `pending`, not held.** `lock_waiting` returns
when the cluster reports `acquired`, and has a deadline because a waitable
request can queue forever ([`examples/cypress.rs`](examples/cypress.rs)).

## One static binary, two roles

When the running executable is a static Linux x86-64 binary,
`upload_current_exe` uploads it, so one program launches the operation and is
its job:

```rust
fn main() {
    ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job
    launch().unwrap();                         // only your machine gets here
}
```

Anything else is refused with `ClientError::NotAWorker` before upload. A
launcher from `cargo run` is Mach-O on macOS and normally dynamically linked on
Linux; build the worker with `scripts/build-worker.sh` and upload it with
`upload_worker`. See
[`crates/ytsaurus-job/examples/selfrun.rs`](../../crates/ytsaurus-job/examples/selfrun.rs).

## Seeing what it did

A `traceparent` header puts the proxy's span for a request into the caller's
trace, with no dependency:

```rust
// A service passing on the trace it was called in.
let client = Client::from_env()?
    .with_trace_context(&TraceContext::parse(incoming_traceparent)?);
```

`TraceContext::new()` starts a trace; `yt_trace_id()` prints its id as the proxy
log, the `X-YT-Trace-Id` header and the UI spell it:
`8e9bcc43-5c2be9b4-56f18c4e-117ea314`. `parse` refuses a malformed header,
which the proxy would drop silently. `with_tracestate()` forwards a
`tracestate` for the caller's backend; the proxy ignores it.

The `tracing` feature adds a span per attempt (command, attempt, elapsed time)
and makes the retry message a `WARN` event. Retries are announced either
way, except inside a job, where stderr is the cluster's bounded diagnostic
buffer; `RetryPolicy::loud()` turns that back on. With no subscriber installed
the stderr line is still printed, since another crate in the graph may have
enabled the feature.

## When an operation fails

`wait_for_operation` fetches the failed jobs' stderr into the error:

```text
operation 1ba94195-… finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
  job 24c164af-… on localhost:24403: User job failed: Process terminated by signal 6
  stderr:
    thread 'main' panicked at crates/ytsaurus-job/examples/boom.rs:37:17:
    boom: this job fails on purpose (row 1, 23 bytes)
```

That costs one `list_jobs` and a few `get_job_stderr` calls, on failure only.
The YTsaurus documentation asks that `list_jobs` not be used without an
administrator's approval; `Client::with_job_diagnostics(false)` turns the
report off. Failing to collect it never hides the original failure
([`examples/diagnose.rs`](examples/diagnose.rs)).

## Adding rows instead of replacing them

`<append=%true>` is an attribute on the path, which is a YSON value:

```rust
client.write_table_rows(TablePath::new("//tmp/log").append(), entries)?;
```

Without it every write replaces the table. The table must exist, and a sorted
one stays sorted: a key smaller than the last is refused with
`Sort order violation: [0#9] > [0#1]`. Appends take a shared lock, so
concurrent ones all land. **Appending nothing is a no-op; writing nothing
truncates.** Rewriting a table in twelve pieces sends 6.5× the rows;
[`examples/append.rs`](examples/append.rs) measures it.

## Reading part of a table

`columns` and `ranges` are path attributes too, so only the selection crosses the
wire:

```rust
let head = client.read_table_rows::<Visit>(
    TablePath::new("//tmp/log").columns(["host", "status"]).range(0..100),
)?;
```

Row ranges are Rust ranges (`0..100`, `100..`, `..`): inclusive below,
exclusive above, like `row_index` limits. Key ranges take the same
shape, `RowRange::keys(Key::from("a")..Key::from("b"))`;
`RowRange::exact_key` is the `exact` selector. Checked by
[`examples/rich_path.rs`](examples/rich_path.rs); the cluster's answers are in
the [protocol reference](../../docs/protocol-reference.md#selecting-columns-and-rows-on-a-path):

- **A write to a path carrying a selection is refused** with
  `ClientError::Config`: the cluster would replace the whole table and answer
  200, for `write_table_rows("//tmp/t[#0:#2]", rows)` or a typed `ranges`
  attribute alike.
- **On a key prefix, `<=` takes a whole group and `>` drops one.** `keys(a..b)`
  and `keys(a..=b)` differ by every row of `b`, because `key_bound` compares
  only the bound's length of the row's key.
- An unknown column is not an error: the key is absent, so a struct fails and a
  map reads clean.
- A path string that already selects (`//tmp/t[#0:#2]`, `//tmp/t{a}`) composes
  with the other kind: `TablePath::new("//tmp/t[#0:#2]").columns(["n"])` reads
  two rows of one column, and `read_skiff_table("//tmp/t[#0:#2]", fmt)` takes
  columns from the format. The same kind twice is refused, since the cluster
  would discard the string's half at 200, so a Skiff read of `//tmp/t{a}` is
  refused. So is a string opening with `<…>`, which the client does not parse.

## Stopping an operation

```rust
client.abort_operation(&id, Some("the input turned out to be yesterday's"))?;
```

`operation_result_error` reads the reason back. The operation is already
`aborted` when the call returns, in about 350 ms.
**Aborting is not idempotent**, unlike `Transaction::abort`: the scheduler then
answers `No such operation`. It is sent once and never retried, because the
mutation cache does not cover scheduler commands.

## Pausing one, repricing it, and picking it up again

```rust
let op = client.attach_operation(id);   // an id from anywhere: a file, a log, another process

op.suspend(false)?;                     // stop scheduling; let running jobs finish
op.resume()?;
op.update_parameters(&OperationParameters::new().with_pool("interactive").with_weight(2.0))?;
op.complete()?;                         // finish early and keep the output
```

`attach_operation` is C++'s `AttachOperation` and Go's `Track(id)`. It sends
nothing, so persist the id; dropping the handle does nothing. Every method is
also on `Client`.

**Suspension is not a state**: a suspended operation reports `running`.
`operation_suspended` and `operation_status` read the flag, and
`wait_for_operation` reports `running, suspended`. Suspend is idempotent and
retried; resuming an operation that is not suspended is refused (code 201).
Complete is not idempotent, and ends the operation as `completed`, publishing
its output. An update that changes nothing is refused, since the cluster would
accept it and do nothing. A sorted merge takes its key from the inputs' sort
columns without `merge_by`.

`get_operation_by_alias("*nightly-load", &["state"])` finds an operation by a
spec alias; `list_operations` takes an `OperationFilter`. See
[`examples/lifecycle.rs`](examples/lifecycle.rs).

## Tables bigger than memory

The streaming pair holds a buffer, not the table:

```rust
let mut reader = JobReader::binary(client.read_table_streaming("//tmp/big")?);
while let Some(event) = reader.next_event()? { /* … */ }

client.write_table_streaming("//tmp/big", File::open("rows.yson")?)?;
```

The stream is what a job reads on fd 0, so one decoder serves both. On a local
cluster, [`examples/streaming.rs`](examples/streaming.rs) streamed a
67.7 MiB table for 1.0 MiB of peak RSS, against 70.9 MiB to read it into memory
([results](../../tests/cluster-e2e/README.md)). A stream has no completeness
check (the decoder fails on a record cut short instead), and a streaming write
is never retried, because a consumed reader cannot be resent.

## Upload the worker once

The file cache is keyed by MD5, so an unchanged worker is not resent:

```rust
let worker = client.upload_worker_cached("target/…/my_job")?;   // uploaded, or found

let spec = MapSpec::new("./my_job", ["//tmp/in"], ["//tmp/out"])
    .with_local_file_named(&worker.path, &worker.name);
```

The cached node is named after the hash, so pass `worker.name` on. The cache
defaults to the Python wrapper's path, shared across the installation;
`Client::with_file_cache` moves it. If the cluster answers the upload with
`Access denied`, the worker goes to `//tmp` and a warning (a `WARN` event with
`tracing`) names `with_file_cache`, since every launch then resends the binary. Any other failure is still a failure.

**Check `worker.cached`, not `worker.uploaded`, before deleting anything.**
`uploaded` is true on both paths, and deleting on it evicts the shared cache
entry for everyone. Where `cached` is false the node is this launch's own and
nothing else removes it.

## Retries

Light commands get five attempts by default, the delay doubling from one
second to ten; `Client::with_retries(RetryPolicy::none())` turns that off.
Mutating commands carry a `mutation_id`, so the cluster deduplicates a repeat.
A replay must be marked: a known ID without the `retry` flag is refused with
`Duplicate request is not marked as "retry"`, so a restarted process sends a
persisted ID with `MutationId::as_retry()` through `Client::start_operation_with`.
IDs are remembered for five to ten minutes. **Heavy commands are not retried**;
use [a transaction](#all-at-once-or-not-at-all) to make an upload atomic.

## Where a heavy command goes

Table and file data (`write_table`, `read_table`, `write_file`, `read_file`,
`upload_worker`, their streaming forms) are heavy commands, which a large
installation serves on separate proxies. The first heavy command asks `/hosts`;
the answer becomes a pool, and each heavy command picks a member at random, as
both official SDKs do. The pool is refreshed lazily when older than
`Client::with_host_list_refresh_interval` (one minute by default); a failed
refresh keeps the old one. Light commands stay on the configured address.

A failure attributable to a host drops it from the pool until a refresh names
it again. Only an empty pool falls back to the configured address, usually a
control proxy, for ten seconds (`Client::with_hosts_retry_after`). A cluster
that names no heavy proxy is served at the configured address and asked again a
refresh interval later; one at `localhost` is not asked, since a published
address is not reachable through a port mapping. `Client::with_proxy_discovery`
overrides both; [`Client::heavy_proxy`] shows the choice. The lookup gets one
attempt and 800 ms, not the client's five attempts and two minutes;
`Client::with_hosts_timeout` changes that.

**A discovered host must share the configured address's domain.**
`https://cluster.example.net` follows `n0132-sas.example.net`, not
`n0132-sas.somewhere-else.net`; a bare `YT_PROXY=hume` matches as a label, so
it follows `n0008-sas.hume.yt.example.net`. Scheme and port come from the
configured address; a name with `://`, `/`, `@` or whitespace is rejected, and
a declined answer is reported once, with what and why. It guards against typos,
not token theft: whoever can steer `/hosts` already sees the token, and without a
public-suffix list `yt-1234.us-east-1.elb.amazonaws.com` shares a domain with
every load balancer in its region.

| | Allows | From the environment |
| --- | --- | --- |
| default | the domain rule above | |
| `Client::with_heavy_proxies_under([…])` | that domain and the ones named, for heavy proxies in a zone of their own | `YT_HEAVY_PROXY_DOMAINS` |
| `Client::with_heavy_proxies_in([…])` | exactly these names; the only boundary, and settable only in Rust | |
| `Client::with_heavy_proxies_anywhere(true)` | wherever `/hosts` says, as the official Go SDK does | `YT_HEAVY_PROXIES_ANYWHERE=1` |

Needing one shows as `cluster error 1: Control proxy may not serve heavy
requests with input data` while `heavy_proxy()` names a proxy the client
declined; the error names all three options. A named domain survives proxy
rotation where a list of seventy-nine names does not. HTTP statuses and the
balancer case:
[protocol reference](../../docs/protocol-reference.md#where-a-heavy-command-goes).
Comparison with the C++ and Go clients:
[docs/sdk-comparison.md](../../docs/sdk-comparison.md).

## Limits

**Trailers are not read**: `ureq` 3.3 does not expose the `X-YT-Error` trailer
that reports a mid-stream failure. `read_table` checks the response is a
complete YSON list fragment, which catches truncation but not a failure that
still yields well-formed output. `read_file` compares the bytes with the node's
`@uncompressed_data_size`; `read_file_streaming` cannot, so compare its
`bytes_read()` with that attribute yourself.

**A buffered response is capped at 512 MiB of decoded bytes** (`read_table`,
`write_table`, their `_with_format` and `_skiff_table` variants, `read_file`).
A larger one fails with `ClientError::ResponseTooLarge`, naming the streaming
method. The cap is not a process budget: the buffer grows by doubling, so peak
residency can reach about 1.5× of it. Two error paths, the non-2xx branch of a
streaming open and the `/hosts` lookup, are bounded only on the wire.
Measurements: [protocol reference](../../docs/protocol-reference.md#response-size-limits).

## A command this crate does not model

The table above is roughly a quarter of API v4. The rest is reachable without
forking the crate:

```rust
use ytsaurus_client::{Client, Method, yson_build};

let client = Client::from_env()?;

// Not modelled here, and needs no parameters: what this cluster's build can do.
let body = client.raw_command(
    Method::Get,
    "get_supported_features",
    &yson_build::empty_map(),
    None,
)?;
```

`raw_command_streaming` and `raw_command_upload` stream the answer or the
request (`read_blob_table`). The client still supplies the token, timeout,
TLS, header encoding, `X-YT-Error` check and transaction. A raw command is sent
once whatever the retry policy; `raw_command_with` takes a `Repeatable`. A
command name with `/` or `?` is refused, since it goes into `/api/v4/{command}`.
`Method` documents the proxy's verb rule.
`cargo run -p ytsaurus-client --example raw` exercises all four entry points.

## Why not JSON

Parameters and specs are encoded with this project's own codec,
[`ytsaurus-yson`](../ytsaurus-yson/). That keeps the dependency list short, and
every request exercises the codec against a real cluster.

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](../../NOTICE).

[`MapSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.MapSpec.html
[`MapReduceSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.MapReduceSpec.html
[`ReduceSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.ReduceSpec.html
[`SortSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.SortSpec.html
[`VanillaSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.VanillaSpec.html
[`MergeSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.MergeSpec.html
[`EraseSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.EraseSpec.html
[`RemoteCopySpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.RemoteCopySpec.html
[`Client::heavy_proxy`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.Client.html#method.heavy_proxy
