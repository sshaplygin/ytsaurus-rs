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

Specs are built with [`MapSpec`] / [`ReduceSpec`] / [`SortSpec`] /
[`MapReduceSpec`] / [`VanillaSpec`] / [`MergeSpec`] / [`EraseSpec`] /
[`RemoteCopySpec`], which model what launching a `ytsaurus-job` worker needs and
expose `with_raw` for everything else. `OperationType` names all nine types the
cluster registers; the ninth, `join_reduce`, has no builder because the current
documentation describes the same work as a reduce with `join_by` and
`enable_key_guarantee=%false`.

`DataFormat` is the common public format choice: use `MapSpec::with_formats`,
the map-reduce phase equivalents, and `write_table_with_format` /
`read_table_with_format`. It supports binary/text YSON and validated **dynamic**
Skiff today. Direct Skiff table I/O derives the rich-path column projection from
the format, as the Go SDK does. The former `*_skiff_*` methods remain convenience
wrappers. Typed rows and schema inference are not available yet; see the
[compatibility contract](../../docs/skiff-compatibility.md).

The runnable [`skiff_launch.rs`](examples/skiff_launch.rs) example pairs those
methods with the `skiff_cat` worker and checks non-UTF-8 `string32` data:

```sh
./scripts/build-worker.sh skiff_cat
export YT_PROXY=http://localhost:8000
cargo run -p ytsaurus-client --example skiff_launch
```

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

## Rows are Rust values

```rust
client.write_table_rows("//tmp/contacts", (0..100).map(contact))?;

let back: Vec<Contact> = client.read_table_rows("//tmp/contacts")?;
let root: ClusterInfo = client.get_as("//@")?;
```

An iterator rather than a slice, because the encoder runs *inside* the request
body: rows are serialised a bufferful at a time as the connection asks for
bytes, so a million rows cost one buffer. Reading is the launcher-shaped
direction — owned rows, whole table — and a struct naming three of twenty
columns is a projection rather than an error. For tables that do not fit,
[`read_table_streaming`](#tables-bigger-than-memory) feeds
`ytsaurus_job::JobReader`.

This exists because of the Go SDK: going through its twelve examples one at a
time showed that writing structs and scanning them back is the thing it does
that this client made you do yourself.
[`docs/go-parity.md`](../../docs/go-parity.md) is the whole comparison —
what matches, what is deliberately absent, and what is still missing.

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

## One static binary, two roles

When the running executable is a static Linux x86-64 binary,
`upload_current_exe` uploads it, so the same program can launch the operation
and be the job it runs:

```rust
fn main() {
    ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job
    launch().unwrap();                         // only your machine gets here
}
```

That direct-static path has no second artifact to forget to rebuild. The running
executable has to be something a node can exec, so its ELF header is checked
first — Linux, x86-64, statically linked — and refused with
`ClientError::NotAWorker` when it is not, rather than failing on the node
minutes later. The default launcher built with `cargo run` is Mach-O on macOS
or normally dynamically linked on Linux, so it cannot be the uploaded file.
Build the worker with `scripts/build-worker.sh` and upload that with
`upload_worker`. The source is still one file — see
[`crates/ytsaurus-job/examples/selfrun.rs`](../../crates/ytsaurus-job/examples/selfrun.rs).

## Talking to a real installation

`Client::from_env` reads `YT_PROXY`, and finds a token the way the `yt` CLI
does: `YT_TOKEN`, then the file named by `YT_TOKEN_PATH`, then `~/.yt/token`. A
machine where the CLI already works needs nothing else. A token read from a file
is trimmed — `echo token > ~/.yt/token` leaves a newline, and sending it fails
authentication with an error that never mentions a newline.

**Table and file data goes to a proxy that will accept it**, which is the
difference between a real installation and a local one that costs the most time
— see [where a heavy command goes](#where-a-heavy-command-goes).

Four more variables exist because every example ships built on `from_env`, and a
policy settable only in Rust is a policy those examples cannot be run under.
Each is inert when unset, so a machine that sets none gets exactly what
`Client::new` gives:

| | |
| --- | --- |
| `YT_PROXY_SUFFIX` | Completes a bare cluster name — `YT_PROXY=hume` with `YT_PROXY_SUFFIX=.yt.example.net` addresses `hume.yt.example.net`. Applied only to a name with no dot, no colon and no `localhost` in it, the gate the Go SDK uses. No suffix is compiled in: this client is not one installation's. |
| `YT_HEAVY_PROXY_DOMAINS` | `Client::with_heavy_proxies_under`, comma- or space-separated. |
| `YT_HEAVY_PROXIES_ANYWHERE` | `1`, `true` or `yes` for `Client::with_heavy_proxies_anywhere`. Applied after the domains, so the wider of the two wins rather than whichever was exported last. |
| `YT_FILE_CACHE` | `Client::with_file_cache`, for an installation whose shared worker cache is read-only to you. |

A variable set to nothing counts as unset — `export YT_FILE_CACHE=` is how a
shell profile turns one back off — and that goes for `YT_PROXY` too, which then
earns the same message as a missing one.

**A cluster behind a private CA needs its CA named.** A bare host name in
`YT_PROXY` means `https://`, and TLS here is `rustls` with the Mozilla root
bundle compiled in — so an installation whose certificate chains to a corporate
CA is refused with `invalid peer certificate: UnknownIssuer`, however happily
`curl` talks to it. That refusal now carries both ways out with it, because
nothing else about it suggests whose roots were consulted. `YT_CA_BUNDLE` names
a PEM file to trust instead:

```sh
export YT_PROXY=cluster.example.net
export YT_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
```

Every certificate in the file becomes a root; anything else in it — a private
key, a comment — is not one, and is skipped. A file that yields no certificates
at all is **refused**, with an error naming it, rather than quietly falling back
to the Mozilla roots: that fallback would answer a deliberate request with the
very handshake failure the variable exists to end. So is a file that cannot be
read, one that is not a regular file, and one larger than 16 MB.

**A `BEGIN CERTIFICATE` block that is not an X.509 certificate refuses the whole
file**, naming it and saying how many blocks were wrong. PEM is only an
envelope, and `rustls` discards a block it cannot parse without telling anyone —
so a PKCS#7 `.p7b` re-armoured under that label used to be accepted, produce an
empty root store, and fail every request with the same `UnknownIssuer` this
variable exists to end. `openssl pkcs7 -print_certs` converts one properly.

The other way is the `platform-verifier` feature, which trusts whatever the
operating system trusts and so needs nothing set — see *Features*. The bundle
wins when both are there: it is the more specific answer, and the one the caller
went out of their way to give.

**An unknown issuer, or a certificate that does not cover the host asked for, is
reported once rather than retried.** Neither is transient: both are decided by
this client's own roots and its own URL, which are the same on the fifth attempt,
and retrying only put fifteen seconds of doubling backoff in front of the same
message. Every other TLS complaint is still retried — an expired certificate,
because a round-robin fleet mid-rotation may answer with a renewed one next
time, and a platform verifier's `Other(…)`, because that is how
`rustls-platform-verifier` reports a revocation lookup or a trust store that was
briefly unavailable. So is an ordinary reset connection.

**Responses are compressed.** Every request carries `Accept-Encoding: gzip` and
every answer is decompressed on the way in, including a streamed table read: on
a local cluster 67.7 MiB of table arrived as 400 KiB. Uploads are not
compressed, though the proxy would accept it — that costs a compression
dependency in a crate that gets cross-compiled to musl.

## Seeing what it did

The cluster traces itself: its proxy opens a span for every request it serves.
A request that carries a `traceparent` has its span put inside the caller's
trace rather than starting an orphan, so naming the trace is the whole of it —
no dependency, one header:

```rust
// A service passing on the trace it was called in.
let client = Client::from_env()?
    .with_trace_context(&TraceContext::parse(incoming_traceparent)?);
```

`TraceContext::new()` starts a trace for a program nobody called;
`yt_trace_id()` prints its id the way the cluster does —
`8e9bcc43-5c2be9b4-56f18c4e-117ea314` — which is the spelling in the proxy log,
in the `X-YT-Trace-Id` response header and in the UI. A header that is not a
traceparent is refused rather than sent: the proxy drops one it cannot parse
without saying so, and the trace would then be quietly missing the part that
mattered.

A `tracestate` that arrived beside the header goes on too, via
`with_tracestate()` — the standard pairs the two and asks a forwarder to pass
the second on unmodified. The proxy ignores it; the caller's own backend is
what reads it.

That is the cluster's side. For this process's own side there is the `tracing`
feature, off by default, which puts every attempt in a span carrying the
command, the attempt number and the elapsed time, and turns the retry message
into a `WARN` event. If nothing is subscribed the stderr line is printed after
all: Cargo unifies features across the graph, so another crate can turn this on
for a program that never asked, and a feature should not take away the only
sign a launcher had that anything was retrying.

A retry announces itself in whichever of the two forms is compiled — a launcher
that pauses for fifteen seconds should say why — and in both it goes quiet
inside a job, where stderr is the cluster's own bounded diagnostic buffer.
`RetryPolicy::loud()` puts it back. With the feature on the announcement is an
event and not a line, so it goes wherever your subscriber sends it, and nowhere
at all if you install none.

## Features

`tls` (default) brings in `rustls`, and with it `https://` proxies. Turning it
off leaves a client that speaks plain HTTP and needs no C toolchain — which is
how a binary that is both launcher and job gets cross-compiled to musl. Without
it, an `https://` proxy fails with an error naming the feature.

`platform-verifier` (off) verifies the cluster against the operating system's
own trust store rather than the Mozilla bundle, so an on-premises installation
works with nothing set — the machine already trusts its CA, which is why `curl`
and the `yt` CLI reach it. Off by default because it costs
`rustls-platform-verifier`, and because the compiled-in bundle is the safer
default for a client that may be running outside the network it is talking to.
`YT_CA_BUNDLE` covers the same ground with no dependency at all and is checked
first.

`tracing` (off) adds the spans above. Off for the same reason: a worker binary
should carry only what it runs on, and `examples/` — what `build-worker.sh`
cross-compiles — takes this crate with `default-features = false`. It costs
three more crates to compile (`tracing`, its `pin-project-lite`, and
`tracing-core`) plus `once_cell`, which a default build already has through
`rustls`. The facade is taken without `attributes`, since `#[instrument]` is a
proc macro and these spans are opened by hand.

`derive` (off) brings `#[derive(TableRow)]`, which reads a table schema off the
struct the rows already have.

## When an operation fails

`wait_for_operation` does not stop at the state. It asks which jobs failed and
what they printed, so the error carries the job's own words:

```text
operation 1ba94195-… finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
  job 24c164af-… on localhost:24403: User job failed: Process terminated by signal 6
  stderr:
    thread 'main' panicked at crates/ytsaurus-job/examples/boom.rs:37:17:
    boom: this job fails on purpose (row 1, 23 bytes)
```

That costs one `list_jobs` and a few `get_job_stderr` calls, on failure only.
The YTsaurus documentation asks that `list_jobs` not be used without an
administrator's approval, so `Client::with_job_diagnostics(false)` turns the
report off. Failing to collect it never replaces the failure being reported.

[`examples/diagnose.rs`](examples/diagnose.rs) runs the whole path against a
local cluster.

## Adding rows instead of replacing them

A YTsaurus path is a YSON value, not a string, and `<append=%true>` is an
attribute on it:

```rust
client.write_table_rows(TablePath::new("//tmp/log").append(), entries)?;
```

Every write replaces the table unless the path says otherwise, which is the
cluster's own default. Two things worth knowing before relying on it:

- **A sorted table stays sorted, and the cluster checks.** A key smaller than
  the last is refused with `Sort order violation: [0#9] > [0#1]`, so an append
  to a sorted table is a continuation of it rather than an addition to it.
- **The table has to exist.** Appending to a path that does not is refused with
  `Error getting basic attributes of user objects`.
- **Appends do not fight each other.** An append takes a *shared* lock where a
  replace takes an exclusive one: four concurrent appends all land, where four
  concurrent replaces leave one winner and three failures.
- **Appending nothing is a no-op; writing nothing truncates.** One `.append()`
  apart.

Worth it because the alternative is quadratic: writing a table in twelve pieces
by rewriting it each time sends 6.5× the rows.
[`examples/append.rs`](examples/append.rs) measures exactly that.

## Reading part of a table

The same type, the read side: `columns` and `ranges` are attributes on the path
too, so three columns of a hundred rows cost three columns of a hundred rows on
the wire rather than the whole table.

```rust
let head = client.read_table_rows::<Visit>(
    TablePath::new("//tmp/log").columns(["host", "status"]).range(0..100),
)?;
```

Row ranges are plain Rust ranges — `0..100`, `100..`, `..` — because Rust's `..`
and the cluster's `row_index` limits agree: inclusive below, exclusive above.
Key ranges on a sorted table take the same shape,
`RowRange::keys(Key::from("a")..Key::from("b"))`, and `RowRange::exact_key` is
the cluster's `exact` selector.

Four things worth knowing, all measured on a cluster by
[`examples/rich_path.rs`](examples/rich_path.rs):

- **A *write* to a path carrying a selection is refused**, as
  `ClientError::Config`, before anything is sent. The cluster ignores a
  selection on a write and replaces the whole table with a 200 — silent data
  loss in both spellings, `write_table_rows("//tmp/t[#0:#2]", rows)` and a typed
  `ranges` attribute alike.
- **On a key *prefix*, `<=` takes a whole group and `>` drops one.** `keys(a..b)`
  and `keys(a..=b)` differ by every row of `b`, not by one row: the second sends
  `key_bound`, and `key_bound` truncates the row's key to the bound's length
  before comparing. An exclusive lower bound on a prefix likewise excludes the
  whole prefix — there is no "the row just after `a`".
- **A column the table does not have is not an error.** The key is simply absent
  from every row, so a typo reads clean; a struct fails on the missing field,
  a map does not.
- **A selection is said once *per kind*.** A path whose *string* already spells
  one (`//tmp/t[#0:#2]`, `//tmp/t{a}`) still reads verbatim, as it always did,
  and the other kind on top of it composes and is sent:
  `TablePath::new("//tmp/t[#0:#2]").columns(["n"])` reads the two rows the
  string names carrying the one column the attribute names, and
  `read_skiff_table("//tmp/t[#0:#2]", fmt)` takes its rows from the string and
  its columns from the format's fields. What is refused is the *same* kind
  twice: measured, the attribute this client adds wins and the string's half is
  discarded at 200 with nothing said. So a Skiff read of `//tmp/t{a}` is
  refused — the format's fields are already a column selection — where the same
  read of `//tmp/t[#0:#2]` goes through. A string that *opens* with `<…>` is
  refused whatever it holds: this client does not parse the block to see which
  attribute it names.

## Stopping an operation

```rust
client.abort_operation(&id, Some("the input turned out to be yesterday's"))?;
```

The reason is folded into the operation's error document, where
`operation_result_error` reads it back, so whoever finds the aborted operation
tomorrow is told who stopped it. By the time the call returns — about 350 ms —
the operation is **already** `aborted`.

**This is not idempotent**, unlike `Transaction::abort`. The scheduler lets go of
an operation as soon as the first abort is accepted, and then answers `No such
operation`, so a defensive second abort is an error rather than a no-op. It is
sent once and never retried for the same reason: the master's mutation cache does
not cover a scheduler command, so a retry after a lost answer would report a
successful abort as a failed one.

## Pausing one, repricing it, and picking it up again

```rust
let op = client.attach_operation(id);   // an id from anywhere: a file, a log, another process

op.suspend(false)?;                     // stop scheduling; let running jobs finish
op.resume()?;
op.update_parameters(&OperationParameters::new().with_pool("interactive").with_weight(2.0))?;
op.complete()?;                         // finish early and keep the output
```

`attach_operation` is the reattach door — C++'s `AttachOperation`, Go's
`Track(id)`. Nothing is sent by it: an id and a client is all an `Operation` is,
which is why the id is the thing worth persisting. **Dropping the handle does
nothing**, unlike a `Transaction`: an operation is meant to outlive the process
that started it.

Everything on the handle is also on `Client`, taking the id — the handle is for
passing an operation around, not for reaching anything the flat API cannot.

Five things about this the cluster does not document and this crate measured:

- **Suspension is not a state.** A suspended operation still reports `running`.
  `operation_suspended` is the question that gets a straight answer, and
  `operation_status` asks it together with the state in one request — which is
  what `wait_for_operation` polls with, so a wait on a paused operation says
  `running, suspended` rather than nothing at all.
- **Suspend is idempotent; resume is not.** A second suspend is accepted, so it
  is the one mutating scheduler command here that is retried. A resume of
  something that is not suspended is refused with code 201.
- **Complete is not idempotent**, exactly as abort is not — and it ends the
  operation as `completed`, so its output is published and a waiting launcher is
  told the work succeeded.
- **An update that changes nothing is refused here**, because the cluster
  accepts one with 200 and does nothing.
- **A sorted merge does not need `merge_by`.** Sent without one it is accepted,
  and the key comes from the sort columns the inputs already carry.

An alias set in the spec can now be looked up:
`get_operation_by_alias("*nightly-load", &["state"])`, which sends the
`include_runtime` the cluster insists on. `list_operations` takes an
`OperationFilter`. [`examples/lifecycle.rs`](examples/lifecycle.rs) runs all of
it against a cluster.

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

**A cache you may not write to costs speed, not the launch.** On an installation
that maintains that shared path itself, an ordinary user may read it and no
more, and the cluster answers the upload into it with `Access denied`. The
worker then goes up outside the cache, under `//tmp`, and a warning on stderr —
a `WARN` event with the `tracing` feature on — says so and names
`with_file_cache`, because every launch re-sends the whole binary until the
cache is pointed somewhere writable. Any other failure is still a failure.

**`worker.cached` is which of the two happened, and it is the field to check
before deleting anything.** `uploaded` is true on both paths, so it cannot tell
them apart: a launcher that cleans up after itself on that signal removes the
installation's *shared* cache entry on an ordinary cluster and evicts the binary
for everyone. Where `cached` is false the node is this launch's own — and
nothing collects it, so a launcher that never cleans up leaks one per launch
there. That, and the re-sent bytes, is what the warning is about.

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

## Where a heavy command goes

Table and file data — `write_table`, `read_table`, `write_file`, `read_file`,
`upload_worker`, and the streaming form of each — is what YTsaurus calls a
*heavy* command, and a large installation serves those on a separate set of
proxies. **The client routes them itself**: the first heavy command asks
`/hosts`, the whole answer becomes a pool, and every heavy command goes to a
member **picked at random** — the way both official SDKs pick, because a
client that kept one pick for its lifetime would never rebalance. The answer
is **refreshed** when it outlives `Client::with_host_list_refresh_interval` —
one minute by default, the documentation's own "re-query every minute" —
lazily, by the heavy command that finds it stale; there is no background
thread, and a refresh that fails keeps the previous answer in use. Light
commands stay on the address you gave.

**A proxy a command fails at is dropped from the pool, not committed to.** A
failure attributable to the host it went to — a refused connection, a 503, a
certificate that does not match that host's own name — takes it out of the
pool, and the next command picks from what remains; the next refresh that
still names it puts it back. Only a pool with nobody left falls back to the
address you configured — and then only until it asks the cluster again ten
seconds later (`Client::with_hosts_retry_after`). The order matters: with
separate proxy roles, the address you configured is usually a control proxy,
so falling back on the first hiccup would answer a transient 503 with ten
seconds of guaranteed refusals.

A cluster that names no heavy proxy is answered by using that address — asked
about again one refresh interval later, so an unlucky first lookup during a
rolling restart is not a verdict for life — and a single-node installation is
unaffected. One reached at `localhost` is not asked at all, because the
address a proxy publishes for itself is not reachable from the other end of a
port mapping or a tunnel. `Client::with_proxy_discovery` overrides both, and
[`Client::heavy_proxy`] still answers the question directly.

**The lookup has its own budget** — one attempt, 800 ms — rather than the
client's five attempts and two-minute timeout. It sits in front of your first
upload, and not getting an answer costs nothing worse than the routing this
crate had none of a release ago. `Client::with_hosts_timeout` moves that budget
in **both** directions: it used to be the smaller of 800 ms and your own
timeout, so a cluster answering `/hosts` in 900 ms could not be routed to at
all.

**A discovered host has to share your address's domain.** So
`https://cluster.example.net` will follow `n0132-sas.example.net` and will not
follow `n0132-sas.somewhere-else.net`; a bare cluster name — `YT_PROXY=hume`,
the usual spelling — is matched as a *label*, so it follows
`n0008-sas.hume.yt.example.net`; the scheme and the port come from your address,
not from the answer; and a name carrying `://`, `/`, `@` or whitespace is not a
name. When a whole answer is declined the client says so, once, naming what it
refused and why.

Read that rule for what it is: **a guard against a typo in your configuration
and against an obviously foreign name.** It is not what keeps your token where
you put it. To steer a heavy command with a `/hosts` body somebody has to
control that body — over `https://` that means owning the proxy, which has your
token already, and over `http://` it means being a man-in-the-middle, who reads
your token out of every light command without coming near this. The case it does
cover is a proxy registering itself in the cluster's coordinator under a name
its operators did not intend; and even there the rule is coarse, because a
suffix rule with no public-suffix list behind it treats
`yt-1234.us-east-1.elb.amazonaws.com` as sharing a domain with every other load
balancer in that region.

Four ways to say which names are allowed, then:

| | | From the environment |
| --- | --- | --- |
| default | the domain rule above | |
| `Client::with_heavy_proxies_under([…])` | that domain **and** the ones you name — for an installation whose heavy proxies live in a zone of their own | `YT_HEAVY_PROXY_DOMAINS` |
| `Client::with_heavy_proxies_in([…])` | exactly these names — the one that is a boundary, because you wrote it | |
| `Client::with_heavy_proxies_anywhere(true)` | wherever `/hosts` says — which is what the official Go SDK does | `YT_HEAVY_PROXIES_ANYWHERE=1` |

The symptom of needing any of the last three is an upload refused at your own
address — `Control proxy may not serve heavy requests with input data` — while
`heavy_proxy()` shows a perfectly good one the client declined to use. That
refusal now says as much itself, and names all three.

The middle one is the one a large installation usually wants. Naming a domain
survives a proxy rotation, where a written-out list of seventy-nine names does
not, and it is still a boundary rather than the absence of one. The environment
can only ever **widen** this rule: `with_heavy_proxies_in` is the mode that is a
boundary, and a boundary a variable could set is a boundary a variable could
move.

Without any of this, the failure is not obvious. The refusal arrives as
`cluster error 1: Control proxy may not serve heavy requests with input data` —
this crate's errors do not print the HTTP status beside the cluster's own, so
the status is not what to look for. The cluster splits on whether the request
carries input data: a heavy **write** is refused with 503, a heavy **read** is
answered with a 307 to a data proxy. And a deployment behind a balancer is the
case that breaks rather than the case that works — the balancer fronts the
control proxies.

The C++ and Go clients re-query `/hosts` periodically, as [the documentation
recommends](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload); so
does this one, lazily and on the C++ client's schedule rather than in a
background thread. See
[docs/sdk-comparison.md](../../docs/sdk-comparison.md) for where the three
still differ.

## Limits worth knowing

**Trailers are not read.** The proxy reports a failure discovered mid-stream in
an `X-YT-Error` trailer, and `ureq` 3.3 exposes none — rechecked against its
source, where the word does not appear. `read_table` compensates by checking the
response is a complete YSON list fragment, so a truncated read is caught; a
mid-stream failure that still yields well-formed output would not be. A file's
bytes carry no such framing — a body cut short just ends, looking like a
shorter file — so `read_file` compares what arrived against the node's
`@uncompressed_data_size` instead, one light `get` after the read.
`read_file_streaming` cannot do either, and says so: compare its
`bytes_read()` against the same attribute yourself.

**`read_table` and `write_table` hold the whole table**, as do their
`_with_format` and `_skiff_table` variants, and `read_file` holds the whole
file. They are for results a launcher inspects; `read_table_streaming`,
`write_table_streaming` and `read_file_streaming` are for everything larger.
**A buffered response is capped at 512 MiB of decoded bytes** — past that the
read is refused rather than truncated, with a `ClientError::ResponseTooLarge`
naming the cap and the streaming half. Decoded, not transferred: responses
arrive gzipped (see above), and a 600 MiB file of zeros crosses the wire in
611 522 bytes, so a cap counted there would bound nothing that matters.

Two things the cap is not. It is not a **process** budget: the bytes land in a
`Vec` that grows by doubling and copies as it grows, so peak residency runs
above the ceiling — measured, a 600 MiB read refused by the cap peaked at
611 385 344 bytes of resident set, and one that held 512 MiB peaked at
544 178 176 — with about 1.5× as the worst case the growth implies. Size for
that, not for the cap. And it is not **crate-wide**: it covers the buffered
commands and uploads, which read through the guard. Two error paths still take
`ureq`'s own wire-only default — the non-2xx branch of a streaming open, and
the `/hosts` discovery lookup — and a gzipped body on either is bounded on the
wire rather than in memory.

## A command this crate does not model

The table above is roughly a quarter of API v4, and the rest is reachable
without forking the crate:

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

`raw_command_streaming` and `raw_command_upload` are the same door for a
command whose answer is the data (`read_blob_table`) or whose request is, so
neither has to fit in memory. `read_file` goes through it in the `raw` example
because that shape is verified against a cluster — it has had methods of its
own, `read_file` and `read_file_streaming`, since #10.

What you give up is the parameters and the answer — the crate has no opinion
about either. What you keep is everything else: the token, the timeout, TLS,
the header encoding, the `X-YT-Error` check, and **the client's transaction**,
so a raw command sent through a `Transaction` is in it rather than beside it.

Two deliberate defaults. A raw command is **sent once**, whatever the retry
policy says, because a command the crate does not model cannot be assumed
idempotent — `raw_command_with` takes a `Repeatable` from a caller who knows
better. And the **command name is checked** before the URL is built: it goes
into `/api/v4/{command}` as it is, so a name carrying `/` or `?` is refused
rather than allowed to address something else. `Method` documents the proxy's
own rule for choosing a verb.

`cargo run -p ytsaurus-client --example raw` exercises all four entry points.

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
