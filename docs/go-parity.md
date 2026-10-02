# Parity with the Go SDK's examples

There is no official Rust SDK for YTsaurus, but there is an official
[Go one](https://pkg.go.dev/go.ytsaurus.tech/yt/go), and its `yt/go/examples`
directory is twelve programs that between them define what an SDK for this
cluster is expected to do. This document maps each onto this workspace: what is
covered, what is deliberately not, and what the comparison added.

## The map

| Go example | Here | What it is about |
| --- | --- | --- |
| `compute-email-example` | [`crates/ytsaurus-job/examples/selfrun.rs`](../crates/ytsaurus-job/examples/selfrun.rs) | one binary that is both launcher and job |
| `count-names-example` | [`sort_reduce.rs`](../crates/ytsaurus-client/examples/sort_reduce.rs) | sort a table, then reduce over its key groups |
| `cypress-example` | [`cluster_info.rs`](../crates/ytsaurus-client/examples/cluster_info.rs) | connect, and read a node into a Rust type |
| `schema` | [`schema.rs`](../crates/ytsaurus-client/examples/schema.rs) | infer a schema from a struct, create, read back, alter |
| `table-usage` | [`table_usage.rs`](../crates/ytsaurus-client/examples/table_usage.rs) | typed rows in, typed rows out |
| `vanilla-example` | [`vanilla.rs`](../crates/ytsaurus-client/examples/vanilla.rs) | jobs with no input table, and reading their stderr |
| `admin` | none | cluster maintenance requests |
| `discovery-client` | none | discovery service, over the bus protocol |
| `dynamic-table` | none | dynamic tables |
| `ordered-dynamic-table` | none | dynamic tables, without keys |
| `query-tracker` | none | Query Tracker, over a dynamic table |
| `tracing` | none | OpenTelemetry spans around client calls |

Six are covered. Each of the other six is a decision, recorded below.

`schema.rs` goes further than its Go counterpart: it creates all 26 column
types the crate can name and checks that the cluster refuses what it should,
where the Go example is twenty lines with every error discarded. `cypress.rs`,
a tour of `list`/`copy`/`move`/`link`/`lock`, has no Go counterpart.

## What the Go examples asked for

Three of the twelve needed something this client did not have. All three are
built.

Typed table rows (`table-usage`). Go writes structs to a table
(`writer.Write(v)`) and reads structs back (`reader.Scan(&c)`). With only
`write_table(&[u8])` and `read_table() -> Vec<u8>`, eleven of this
repository's twelve examples hand-rolled the same YSON encode loop, so the loop
moved into the library:

```rust
client.write_table_rows("//tmp/contacts", contacts.iter())?;
let back: Vec<Contact> = client.read_table_rows("//tmp/contacts")?;
```

`write_table_rows` takes an iterator because the encoder sits inside the
request body: rows are serialised a bufferful at a time as the connection asks
for bytes, so a million rows cost one buffer. Nine examples dropped their
encode loop. Three keep one: `schema.rs` needs a row the derive cannot produce
(one with a required column missing) to see the cluster refuse it, and
`streaming.rs` and `profile.rs` generate raw bytes because they measure the
byte stream.

Typed nodes (`cypress-example`). Go reads `//@` into a struct of the three
attributes it needs. `Client::get` returns a `YsonValue`; `Client::get_as::<T>`
returns the struct.

Job stderr on the success path (`vanilla-example`). Go lists an operation's
jobs after it succeeds and prints what each wrote. `list_jobs` and
`get_job_stderr` existed here, called only by the failure report inside
`wait_for_operation`; the examples now call both. Established against a
cluster:

- Stderr is kept for successful jobs, with no spec option needed. The cluster
  returned a completed job's stderr verbatim.
- `list_jobs` answers with an empty list for an operation that finished a
  while ago: the controller agent forgets its jobs, and a cluster with no job
  archive (a local one) has nothing left to say. Harvest right after
  `wait_for_operation`, as both examples do.

## What is deliberately absent

The decisions are in [AGENTS.md](../AGENTS.md).

Dynamic tables (`dynamic-table`, `ordered-dynamic-table`, and the data path of
`query-tracker`). Lookup, select, insert and delete are implemented,
pre-release, through `ytsaurus_api::TableClient` over HTTP (`create_client`)
and over the RPC proxy (`create_rpc_client`, `ytsaurus-rpc`);
[rpc-compatibility.md](rpc-compatibility.md) is the contract, and
`both_transports.rs` runs the same code over both. `mount_table` is not
modelled; `Client::raw_command` reaches it untyped. The two Go examples have no
Rust counterpart.

The discovery service (`discovery-client`). It speaks the bus protocol with
protobuf bodies, not HTTP. `ytsaurus-rpc` speaks that protocol and wraps
`DiscoverProxies`; the standalone discovery-client surface, a different
service from the proxy's, is absent.

Tracing (`tracing`). Built. `TraceContext` and `Client::with_trace_context`
emit the same `traceparent` the Go example's `ytotel.TraceFn` produces, with no
dependency, plus the `tracestate` the standard pairs with it, which the Go
example does not carry. The `tracing` feature, which spans this client's own
attempts, is off by default and kept out of musl worker builds. There is no
cluster example: it would have to read a span from the cluster's trace store,
which this client cannot read and the local Docker cluster does not run a
collector for. The check is a wire test, `tests/request_shape.rs`, which reads
the bytes off a socket and pins the header, including on the `/hosts` lookup,
which builds its own request. The exporter for this process's spans is the
user's choice.

Cluster maintenance (`admin`). Not listed in the non-goals, so this is an open
decision. It is outside the client's stated charter ("It does what launching a
job needs … and nothing else"), and it is the only surface in the set where a
mistake on a shared cluster is destructive. It would be two commands and two
enums if the charter widens.

Query Tracker (`query-tracker`). A non-goal, as AGENTS.md lists it. The Go
example's data path is a dynamic table; the Query Tracker itself is plain HTTP
API v4, and `Client::raw_command` reaches it untyped (`yql_smoke.rs`; see
[sdk-comparison.md](sdk-comparison.md#query-tracker-through-raw_command)).

## What the Go examples showed missing

Found while checking the twelve programs against the API. None blocked an
example.

1. Abort: built. `Client::abort_operation(id, reason)` stops an operation and
   puts the reason in its error document, where
   `Client::operation_result_error` reads it back. Suspend, resume,
   `complete_operation` and `list_operations` are built too
   ([sdk-comparison.md](sdk-comparison.md#operations-and-the-job-model)).
2. Append: built. `TablePath::new(p).append()` carries the `<append=%true>`
   attribute, and the three write methods take `impl Into<TablePath>`, so a
   `&str` still works. Go's `ypath.Rich` also carries `Columns` and `Ranges`;
   those are modelled as `TablePath::columns` / `::range`, and the read methods
   take `impl Into<TablePath>`. A *write* with a read selection is refused
   locally, because the cluster ignores the selection and replaces the table
   with a 200. `examples/rich_path.rs` checks all of it against a cluster,
   including what no wire test can show: `key` and `key_bound` compare a short
   key by opposite rules, so `keys(a..b)` and `keys(a..=b)` differ by a whole
   prefix group rather than by one row.
3. Escape hatch: built. `Client::raw_command(method, command, params,
   payload)` sends a command this crate does not model, with
   `raw_command_streaming` and `raw_command_upload` for the two heavy shapes,
   and `Method`/`Repeatable` public so a caller can classify an unknown
   command. It generalises `Client::start_operation`'s raw spec to every
   command. It keeps the token, the timeout, TLS, the `X-YT-Error` check and
   the client's transaction, and gives up only the parameters and the answer.
   `cargo run -p ytsaurus-client --example raw`.
4. Web UI links: not built. Three Go examples print `yt.WebUIOperationURL(...)`.
   The URL is `https://<host>/<cluster>/operations/<id>`, and the cluster name
   is not derivable from a proxy address, so the client would have to ask the
   caller for it or print a link that might be wrong.

Building the first three found three things neither Go example mentions:

- Aborting is not idempotent: an operation the scheduler has finished with
  answers `No such operation`, where `abort_transaction` accepts it.
- Appending to a sorted table is checked: a key out of order is refused with
  `Sort order violation`.
- `whoami` is not an API v4 command. The Go SDK sends it as an auth call to a
  different endpoint, so the raw door cannot reach it; the examples use
  `get_supported_features` instead.

## Running the Rust side

Every example checks itself and exits non-zero when a check fails:

```sh
export YT_PROXY=http://localhost:8000
tests/cluster-e2e/run_local_cluster.sh
scripts/build-worker.sh

cargo run -p ytsaurus-client --example cluster_info
cargo run -p ytsaurus-client --example raw
cargo run -p ytsaurus-client --example table_usage
cargo run -p ytsaurus-client --example schema
cargo run -p ytsaurus-client --example sort_reduce
cargo run -p ytsaurus-client --example vanilla
# `cargo run` is the host launcher; upload the static worker built above.
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-job --example selfrun
```

[`tests/cluster-e2e/README.md`](../tests/cluster-e2e/README.md) holds the
results of each run that was made.

## Interop below the examples

[`crates/ytsaurus-yson/tests/interop_tests.rs`](../crates/ytsaurus-yson/tests/interop_tests.rs)
runs against fixtures written by `go.ytsaurus.tech/yt/go/yson`, in both
directions and both formats. Example parity says the two SDKs can do the same
things; those fixtures say they agree on the bytes.
