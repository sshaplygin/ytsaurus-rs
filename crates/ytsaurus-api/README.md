# ytsaurus-api

The transport-independent YTsaurus client interface: one API, two transports.

**Pre-release, published from 0.3.0.** The interface is the one thing here that
is expensive to change later and it has **not** settled: the version is 0.x and
it may change in a patch release. It is on crates.io because
[`ytsaurus-rpc`](../ytsaurus-rpc) and `ytsaurus-client`'s `create_client` /
`create_rpc_client` return this crate's `TableClient` and could not be published
otherwise.

## Why

YTsaurus reaches its dynamic tables two ways, and the C++ client does not make
callers pick an API to go with the transport. It has one interface and two
constructors:

```cpp
IClientPtr CreateClient   (const TString& serverName, ...);  // HTTP
IClientPtr CreateRpcClient(const TString& serverName, ...);  // RPC proxy
```

This crate is the Rust equivalent of what those return, and the layering copies
the C++ because that layering is the reason it works:

| C++ | here |
| --- | --- |
| `yt/yt/client/api` — the interface | **this crate** |
| `yt/yt/client/api/rpc_proxy` — one implementation | [`ytsaurus-rpc`](../ytsaurus-rpc) |
| `yt/cpp/mapreduce` — the wrapper with both constructors | [`ytsaurus-client`](../ytsaurus-client) |

So the constructors live in `ytsaurus-client`, which depends on both, and
choosing a transport is one line:

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

Deliberately, and it is the decision here worth arguing about. The C++ wrapper
blocks, every other crate in this workspace is synchronous, and a MapReduce job
is a synchronous, single-purpose process.

**Async callers lose nothing.** `ytsaurus_rpc::Client` is untouched and is still
the only way to get concurrent in-flight requests — which is the entire reason
the RPC proxy exists. What this interface buys is portability between
transports, not concurrency; the blocking facade drives one call at a time.

## The row model

Column **names**, not ids. The RPC wire format numbers its values and resolves
them through a name table, HTTP names them directly, and a caller should not
have to know which.

```rust
use ytsaurus_api::{Row, Value};

let row = Row::new().with("key", 1i64).with("value", "hello");
assert_eq!(row.get("key"), Some(&Value::Int64(1)));
```

Rows keep the order their columns were added in, because a key row's column
order is the table's key order — sorting them would ask for a different row.
Strings are bytes rather than `String`: a YTsaurus column may legitimately hold
something that is not UTF-8.

## What it covers, and what it does not

The dynamic-table surface both transports implement: `lookup_rows`,
`select_rows`, `insert_rows`, `delete_rows`, and tablet transactions.

Cypress, operations and file I/O stay on `ytsaurus-client`. The RPC crate
deliberately does not implement them, and an interface with half its methods
unavailable on one transport would be worse than two honest APIs.

## One asymmetry, and the cluster reports it

**Tablet transactions are RPC-only.** They are *sticky* — a transaction belongs
to the proxy that created it, and every later call in it must reach that same
proxy — and an HTTP client routes each request independently. Ask an HTTP client
for one and a real cluster answers:

> Sticky transaction … is not found, this usually means that you use tablet
> transactions within HTTP API; consider using RPC API instead

So the HTTP implementation refuses up front with `Error::Unsupported` and quotes
that advice, rather than failing on the second call. This is one of the reasons
`CreateRpcClient` exists in the C++ at all.

Everything else — reads, and writes outside a transaction — works on both.

## Checked against a real cluster

```sh
cargo run -p ytsaurus-client --features rpc --example both_transports
```

Runs the same code over each transport against one cluster and compares the
results row for row. The two are wire-level unrelated — YSON over HTTP, the row
wire protocol over bus — so "they behave the same" is a claim that needs
running, and this is the first differential test in this repository between two
independent implementations of the same operations.
