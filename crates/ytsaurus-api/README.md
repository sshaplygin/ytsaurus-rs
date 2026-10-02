# ytsaurus-api

The transport-independent YTsaurus client interface: one API, two transports.

**Pre-release, published from 0.3.0.** The interface has not settled, and it
is the part of this workspace most expensive to change later: the version is
0.x and it may change in a patch release. `ytsaurus-client`'s `create_client`
and `create_rpc_client` return this crate's `TableClient`, backed for RPC by
[`ytsaurus-rpc`](../ytsaurus-rpc).

## Why

YTsaurus reaches its dynamic tables over two transports, and the C++ client
gives both one interface with two constructors:

```cpp
IClientPtr CreateClient   (const TString& serverName, ...);  // HTTP
IClientPtr CreateRpcClient(const TString& serverName, ...);  // RPC proxy
```

This crate is the Rust equivalent of what those return, with the C++ layering:

| C++ | here |
| --- | --- |
| `yt/yt/client/api`: the interface | this crate |
| `yt/yt/client/api/rpc_proxy`: one implementation | [`ytsaurus-rpc`](../ytsaurus-rpc) |
| `yt/cpp/mapreduce`: the wrapper with both constructors | [`ytsaurus-client`](../ytsaurus-client) |

The constructors live in `ytsaurus-client`, which depends on both, so choosing
a transport is one line:

```rust,no_run
use ytsaurus_api::{LookupOptions, Row, TableClient};

# fn main() -> Result<(), ytsaurus_api::Error> {
let client = ytsaurus_client::create_client("http://localhost:8000")?;   // HTTP
// or, with the `rpc` feature:
// let client = ytsaurus_client::create_rpc_client("localhost:8011")?;   // RPC

let key = Row::new().with("key", 1i64);
let rows = client.lookup_rows("//tmp/table", &[key], &LookupOptions::default())?;
# Ok(())
# }
```

## The interface is synchronous

The C++ wrapper blocks, every other crate in this workspace is synchronous, and
a MapReduce job is a synchronous, single-purpose process. Async callers use
`ytsaurus_rpc::Client` directly, still the only way to get concurrent in-flight
requests. This interface gives portability between transports, not
concurrency: the blocking facade drives one call at a time.

## The row model

Columns by name, not id. The RPC wire format numbers its values and resolves
them through a name table, HTTP names them directly, and a caller need not know
which.

```rust
use ytsaurus_api::{Row, Value};

let row = Row::new().with("key", 1i64).with("value", "hello");
assert_eq!(row.get("key"), Some(&Value::Int64(1)));
```

Rows keep the order their columns were added in, because a key row's column
order is the table's key order; sorting would ask for a different row. Strings
are bytes, not `String`: a YTsaurus column may hold something that is not
UTF-8.

## What it covers, and what it does not

The dynamic-table surface both transports implement: `lookup_rows`,
`select_rows`, `insert_rows`, `delete_rows`, and tablet transactions.

Cypress, operations and file I/O stay on `ytsaurus-client`. The RPC crate does
not implement them, and an interface with half its methods unavailable on one
transport would be worse than two separate APIs.

## One asymmetry

**Tablet transactions are RPC-only.** They are sticky: a transaction belongs to
the proxy that created it, and every later call in it must reach that proxy,
while an HTTP client routes each request independently. Asked for one over
HTTP, a real cluster answers:

> Sticky transaction … is not found, this usually means that you use tablet
> transactions within HTTP API; consider using RPC API instead

So the HTTP implementation refuses up front with `Error::Unsupported`, quoting
that advice, rather than failing on the second call. Everything else (reads,
and writes outside a transaction) works on both.

## Checked against a real cluster

```sh
cargo run -p ytsaurus-client --features rpc --example both_transports
```

Runs the same code over each transport against one cluster and compares the
results row for row. The two share nothing on the wire (YSON over HTTP, the
row wire protocol over bus), so this is a differential test between two
independent implementations of the same operations.
