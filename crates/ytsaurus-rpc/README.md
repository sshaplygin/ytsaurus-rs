# ytsaurus-rpc

A Rust client for the YTsaurus RPC proxy: bus framing, the RPC envelope, and
the row wire format dynamic tables speak.

**Pre-release, published from 0.3.0.** The ship gates are not all green and the
API may change in a patch release. What works, what does not, and what has been
run against a real cluster is [docs/rpc-compatibility.md](../../docs/rpc-compatibility.md).

## Why this exists

HTTP API v4, which [`ytsaurus-client`](../ytsaurus-client) speaks, already
reaches `select_rows`, `lookup_rows`, `insert_rows` and `delete_rows`. This
crate is for latency and throughput under concurrency: one connection
multiplexes many in-flight requests, where HTTP pays its per-request cost every
time. Tablet transactions are RPC-only.

If that is not your bottleneck, use the HTTP client, which has none of the
gates in the compatibility document.

## The protocol is four layers

| Layer | What it is | Module |
| --- | --- | --- |
| 1 | Bus: framed, CRC-64-checksummed packets over TCP | `bus` |
| 2 | RPC envelope: request and response headers, `TError` | `rpc` |
| 3 | API surface: generated protobuf | `ytsaurus-proto` |
| 4 | Row wire format: rows in attachments, not protobuf fields | `wire` |

Rows do not travel as protobuf: `api_service.proto` says "actual data is
passed via attachments in the wire protocol", and the request carries only a
descriptor naming the columns. That format is neither YSON nor Skiff; it is a
third one, mandatory for every dynamic-table read and write.

## Shape

The parsers are sans-io: `crc64`, `bus::packet`, `rpc` and `wire` are pure
functions from bytes to values with no `async`, so each is tested without a
runtime. That also makes them fuzzable; they are not fuzzed yet (gate E).
`async` appears only at the I/O edges, `bus::Bus` and `connection::Connection`.

A connection is an actor: a writer task drains a bounded channel with at most
256 calls in flight, which gives backpressure, and a reader task routes each
response to the `oneshot` waiting on it. A timed-out call sends the protocol's
cancellation message, because a client-side-only timeout leaves the proxy
working on a result nobody will read.

Unlike the rest of this workspace, this crate is async, on tokio: multiplexed
in-flight requests are the reason to speak this protocol.

## Use

```rust,no_run
use ytsaurus_rpc::client::{Client, LookupOptions, StartTransactionOptions, TransactionType};
use ytsaurus_rpc::wire::{UnversionedValue, Value};

# async fn example() -> ytsaurus_rpc::error::Result<()> {
let client = Client::connect("localhost:8011").await?;

let transaction = client
    .start_transaction(TransactionType::Tablet, StartTransactionOptions::default())
    .await?;
transaction
    .insert_rows("//tmp/table", &["key", "value"], &[vec![
        UnversionedValue::new(0, Value::Int64(1)),
        UnversionedValue::new(1, Value::String("hello".into())),
    ]])
    .await?;
transaction.commit().await?;

let key = vec![UnversionedValue::new(0, Value::Int64(1))];
let rows = client
    .lookup_rows("//tmp/table", &["key"], &[key], LookupOptions::default())
    .await?;
// One answer per key asked for, in order; `None` where the key had no row.
assert!(rows[0].is_some());
# Ok(())
# }
```

A method this crate does not wrap is still reachable:
`client.connection().invoke_raw(..)` takes any service, method and protobuf
body, and `ytsaurus-proto` has the generated type for all 158 of them.

## Building

`cargo build`. The protobuf bindings come from
[`ytsaurus-proto`](https://crates.io/crates/ytsaurus-proto), which ships them
generated, so building needs neither the YTsaurus `.proto` submodule nor
`protoc`. Regenerating them is done in a repository checkout:
`./scripts/init-protos.sh`, then `cargo xtask generate-protos`.

## Tests

```sh
cargo test -p ytsaurus-rpc                     # unit tests + golden vectors
cd tests/rpc-go-interop && go test ./...       # regenerate the vectors
cargo run -p ytsaurus-rpc --example rpc_e2e    # against a live RPC proxy
```

The golden vectors are produced by the pinned Go SDK, not written by hand, as
in `tests/skiff-go-interop/`, so the format is not checked only against this
crate's own reading of the specification.

`tests/cluster-e2e/run_local_cluster.sh` starts a local cluster with an RPC
proxy on `localhost:8011`; run it before `rpc_e2e`. The example also runs in
the post-merge `Cluster E2E` workflow.
