# The three clients: C++, Go, and this one

YTsaurus ships two official clients: the C++ MapReduce wrapper
(`yt/cpp/mapreduce`), which jobs have historically been written against, and
the newer Go SDK (`yt/go`), the only one with a published reference interface.
A feature only the native C++ client (`yt/yt/client/api`, the RPC one used
inside the cluster) has is marked *native*.

This compares both against `ytsaurus-client` (and, for dynamic tables,
`ytsaurus-api` and `ytsaurus-rpc`), to say what "production-ready" would mean
here. [`go-parity.md`](go-parity.md) covers the Go SDK's twelve *examples*;
this covers the API surface behind them. Everything below was read from
source; where a claim could not be checked it says so.

## How C++ and Go differ

Little in what they can do, a lot in how a job is written. Both cover nine
operation types, dynamic tables, tablet transactions and administration.

- Row formats: C++ has five families (`TNode`, protobuf, YaMR, Skiff, and raw
  at any `TFormat`); Go has YSON and Skiff. Protobuf is the only way a C++
  program gets a schema from a type (`CreateTableSchema<T>()` reads the
  descriptor); Go reflects over `yson` struct tags at run time.
- Job code: both serialise the job object, C++ through `Y_SAVELOAD_JOB`, Go by
  registering the struct with `gob` and encrypting it into the operation's
  `SecureVault`, so a job carries its state from the launcher. The Rust client
  ships a plain executable with argv and environment: its largest difference
  from both.
- The operation: C++ returns an `IOperationPtr` that owns the lifecycle
  (`Watch`, `GetBriefState`, `GetFailedJobInfo`, `Suspend`, `UpdateParameters`,
  `GetWebInterfaceUrl`). Go's `mapreduce.Operation` is `ID()` and `Wait()`;
  the rest is on `yt.Client`.

`TrimRows`, `GetTabletInfos`, `ExplainQuery`, blob-table readers and a
multi-threaded parallel reader (`library/parallel_io`) exist in C++ and not in
Go. Neither is a superset.

## Transport and configuration

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Protocol | HTTP and RPC | HTTP and RPC | HTTP API v4; the RPC proxy for dynamic-table rows only (`create_rpc_client`, `rpc` feature, pre-release) |
| Token lookup | `Token`, `TokenPath`, `~/.yt/token` | `YT_TOKEN`; a file only with `ReadTokenFromFile` | `YT_TOKEN`, `YT_TOKEN_PATH`, `~/.yt/token` |
| Other credentials | TVM, service tickets, impersonation | 5 implementations, swappable per call | OAuth only |
| TLS | `UseTLS` | `UseTLS` + caller CA bundle | `tls` feature: Mozilla roots, `YT_CA_BUNDLE`, or the OS store with `platform-verifier` |
| Heavy-proxy routing | automatic (`THostManager`) | automatic, plus a 5-minute ban on failure | automatic, random per command, refreshed every minute, constrained by domain; see below |
| Compression | configurable, off by default | zstd both ways | gzip inbound only |
| Timeouts | connect and socket separately | 5 min light, none for heavy | one, 120 s by default, `Client::with_timeout` |
| Batching several commands | `CreateBatchRequest` → futures | `NewBatchRequest` → `BatchResponse[T]` | `BatchRequest` → `Vec<Result<…>>`; no per-part retry (C++ re-queues a retriable part); a split batch that stops reports the prefix it was answered for (C++ throws and loses it) |
| Retries | three policies by request class | interceptor chain | one policy × `Repeatable` |
| Client logging | global `ILogger` | `Config.Logger`, structured | optional `tracing` feature, off by default |
| Distributed tracing | `EnableClientTracing` | `TraceFn` + Jaeger and OTel adapters | `TraceContext` → `traceparent`, no dependency |

All three send the same W3C `traceparent`, and the cluster records the span.
Go's `ytjaeger` and `ytotel` adapters read it from an ambient
`context.Context`; Rust has none, so it is passed to the client, and an
OpenTelemetry application formats its current span into a `traceparent`.

### Heavy proxies

Both official clients pick a host at random per command (`THostManager` with
`RandomNumber`, Go's `ProxySet.PickRandom`) and never commit to one. C++
refreshes `/hosts` lazily on access; Go refreshes in the background and bans a
failing proxy for five minutes. This client:

- picks at random using the crate's id source (*unique, not unpredictable*),
  so no new dependency;
- refreshes the C++ way: the heavy command that finds the answer older than
  `Client::with_host_list_refresh_interval` (one minute by default, the
  [proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)'s
  "re-query every minute") asks `/hosts` first. No background thread; a failed
  refresh keeps the previous answer;
- drops a host whose failure is attributable to it, a rejected certificate
  included, until a refresh names it again: a persistently bad host costs one
  failed command per interval. A pool with nobody left falls back to the
  configured address for ten seconds before `/hosts` is asked again. With separate roles that address is a
  control proxy, which answers uploads with `Control proxy may not serve heavy
  requests with input data`.

Deviation from both: a discovered name is used only if it shares the
configured address's domain, with the scheme and port of the configured
address. It guards against typos and foreign names; it is not a token
boundary, because steering the `/hosts` body takes the proxy or the wire, and
either already has the token. Go filters nothing (`listHeavyProxies` returns
the list verbatim; `proxy_set.go` adds every entry). A managed installation
answered `/hosts` with 79 heavy proxies under a domain the configured address
does not share; the default rule refused all of them and no heavy command
could be sent. Settings:

- `Client::with_heavy_proxies_anywhere(true)`: no filter, as the official
  clients;
- `Client::with_heavy_proxies_under([…])`: the configured domain plus those
  named; survives a proxy rotation;
- `Client::with_heavy_proxies_in([…])`: only the names listed; of the four
  settings, the only boundary.

Logging is thinner: a span per attempt and an event per retry, where the
official clients log request bodies, proxy choices and connection lifecycles.

Only this client mutes retry logging inside a job (`YT_JOB_ID`) and works from
inside a job at all; Go refuses unless `AllowRequestsFromJob` is set, guarding
against a hundred thousand jobs finding the master at once. The one-binary
launcher-and-worker pattern depends on it.

## Cypress and paths

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Core verbs | 11 | 11 | 9 |
| `SetNode` on any node | yes | yes | attributes only (`set_attribute`) |
| `MultisetAttributes` | yes | yes | no |
| `CreateObject` (accounts, users) | yes | yes | no |
| Path: `append` | yes | yes | yes: `TablePath::new(p).append()` |
| Path: columns, ranges, key bounds | `TRichYPath` | `ypath.Rich` | yes: `TablePath::columns` / `::range`, `RowRange`, `Key`; a *write* with a read selection is refused locally, because the cluster silently ignores it there |
| Dynamic value | `TNode` | `yson.RawValue` | `YsonValue` |
| Read a node into a native type | protobuf/`TNode` | `GetNode(&out)` | `get_as::<T>()` |

## Tables, formats and schemas

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Row formats | 5: TNode, protobuf, Skiff, YaMR, raw | 2: YSON, Skiff | binary YSON by default; text YSON and dynamic Skiff (pre-release) through `DataFormat` |
| Schema from a native type | protobuf descriptors | reflection at run time | `#[derive(TableRow)]`, at compile time |
| Schema validated before sending | no | no | `TableSchema::validate()` |
| Whole table as typed rows in one call | no, loop a reader | no, loop a reader | `read_table_rows` / `write_table_rows` |
| Streaming row cursor | `TTableReader<T>` | `TableReader.Next/Scan` | `read_table_streaming`, read with `ytsaurus-job`'s reader |
| Read a file back | `CreateFileReader` | `ReadFile` | `read_file`, and `read_file_streaming` for one that does not fit |
| Partitioned reads | `GetTablePartitions` | `PartitionTables` | no |
| Parallel reader | `library/parallel_io` | no | no |
| Blob tables | `CreateBlobTableReader` | no | no |
| Retries inside a table write | yes, resumable upload | yes, own transaction and 512 MB batches | no: one attempt |
| Table created from the first row | `InferSchema` option | the default | no: `create_table` first |

Whether Skiff becomes the default format is undecided; see
[`benchmarking.md`](benchmarking.md). Protobuf rows are not implemented.

## Operations and the job model

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Operation types | 9 | 9 | 9, spec builders for 8 |
| What you get back | `IOperationPtr`, 12+ methods | thin `Operation`: `ID`, `Wait` | a `String` id, and an `Operation` handle over it |
| Suspend / resume / complete | yes | yes | yes, with idempotency measured per command |
| Update parameters while running | yes | yes | yes |
| List operations, look up by alias | yes | yes | yes |
| Reattach to another process's operation | `AttachOperation` | `Track(id)` | `attach_operation(id)` |
| Abort | yes | yes | yes, documented as not idempotent |
| One job by id, and its input | yes | yes | yes: `get_job`, `get_job_input` |
| How job code reaches the node | `Y_SAVELOAD_JOB` | `gob` + `SecureVault` | argv and environment |
| Binary upload | automatic | automatic, md5-cached | manual, md5-cached |
| Failure explains itself with stderr | yes | only when the message matches | always, up to 3 jobs |
| Custom job statistics | yes | no | yes |

`OperationType` names all nine; eight have builders, including `MergeSpec`,
`EraseSpec` and `RemoteCopySpec`. `join_reduce` has none: the current
documentation no longer lists it under `start_operation` and describes the work
as a reduce with `join_by` and `enable_key_guarantee=%false`, which
`ReduceSpec::with_raw` builds.

Which of these may be repeated was measured on a cluster: a second suspend is
accepted, so suspend is retried; a second resume is refused with code 201, and
a second complete or abort is answered `No such operation`, so those are sent
once.

## Transactions and locks

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Timeout / ping period | 120 s / 5 s | 15 s / 3 s | 30 s / timeout ÷ 3 |
| Handle doubles as a client | `ITransaction : IClientBase` | `Tx` embeds the interfaces | `Deref<Target = Client>` |
| Attach to one started elsewhere | yes, fully | yes | yes: `attach_transaction`, pinging included |
| `Detach`: stop pinging, leave it alive | yes | partial | yes; dropping an *attached* handle detaches too |
| Learn it was lost without a command | no | `Tx.Finished()` channel | `is_lost()`, polled, or `ping()` |
| Prerequisite transaction ids | yes | yes | no |
| Wait for a waitable lock | `GetAcquiredFuture()` | no helper | yes, with a mandatory deadline |
| Unlock | yes | yes | no |
| Child-key / attribute locks | yes | yes | no: whole-node only |

`Transaction::detach` stops the keep-alive and leaves the transaction running;
`attach_transaction` turns the id into a pinging handle elsewhere, reading the
interval from `#<id>/@timeout`; `ping_transaction`, `commit_transaction` and
`abort_transaction` work from the id alone. `Drop` follows the C++ destructor:
a handle this process *started* aborts, which makes `?` safe inside a
transaction; an *attached* one detaches. Go's `AttachTx(id, {AutoPingable:
false})` maps onto `with_transaction` plus the by-id commands, so
`attach_transaction` always pings, and pings before returning, which neither
official client does: `@timeout` is the configured lifetime, not what a
handoff has left. Go's `Tx.Finished()` is pushed; `Transaction::is_lost` must
be polled.

## Dynamic tables, administration, the rest

Dynamic-table rows are narrow and pre-release
([rpc-compatibility.md](rpc-compatibility.md)); the rest is absent. Non-goals:
[AGENTS.md](../AGENTS.md#non-goals).

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Lookup, select, insert, delete | yes | yes | yes, pre-release: `ytsaurus_api::TableClient` over HTTP (`create_client`) or RPC (`create_rpc_client`) |
| Mount, unmount | yes | yes | no; `raw_command` reaches them untyped |
| Tablet transactions | native | yes, first class | RPC only; over HTTP, `Error::Unsupported` |
| Wait for a tablet state | no | `migrate.MountAndWait` | no |
| Queues and consumers | native | yes | non-goal |
| Query Tracker | native | yes | non-goal; reachable through `raw_command`, see below |
| `WhoAmI`, `CheckPermission` | yes | yes | no |
| Maintenance, users, tokens | native | yes | no; outside the client's charter, no decision recorded |
| Test fixture | `TTestFixture` | `yttest`, dockertest | a shell script and self-checking examples |
| Code generation | protobuf row classes | `yt-gen-client` emits ~8 000 lines | `#[derive(TableRow)]` |

### Query Tracker through `raw_command`

Query Tracker is a non-goal and no method models it, but
`crates/ytsaurus-client/examples/yql_smoke.rs` drives it end to end through
`Client::raw_command` on a local cluster: `start_query` (POST,
`Repeatable::Never`), `get_query` (GET, `Freely`), `abort_query` on a timeout.
YQL titles each operation `YQL operation (<query id> by <user>)`, so
`Client::list_operations` with `OperationFilter::with_substring` finds what a
query spawned. A raw caller supplies a terminal-state predicate separate from
"the wait ran out" (conflating them reports a completed query as a failure),
a poll interval and a timeout, and uses `error_summary` from the crate root to
report the cause at the bottom of the error tree. The Go example reads Query
Tracker's state from dynamic tables; this path uses ordinary HTTP commands.

## Where this client sits

Roughly a quarter of `yt.Client`'s ~101 methods, not the quarter either
library would pick. Better than both:

- the schema comes off the type at compile time;
- the schema is validated before the request is sent, turning cluster error 314
  into one sentence naming the column;
- a failed operation always explains itself with its jobs' stderr, where Go
  does it only when the error message happens to match a string;
- typed whole-table I/O in one call, which neither has.

The six gaps the
[parity issue](https://github.com/sshaplygin/ytsaurus-rs/issues) listed are
built: logging and tracing (a `traceparent`, and the optional
`tracing` feature); the operation object and lifecycle; read-side
`TablePath::columns` / `::range`; `read_file` and its streaming half; batch
requests (`BatchRequest`, `Client::execute_batch`, per-part `Result`s, the C++
client's `Concurrency` and `BatchPartMaxSize` options); and transaction
`Detach` with attach and the by-id commands.

`Client::raw_command`, with `raw_command_streaming` and `raw_command_upload`
for heavy shapes, sends any command this crate does not model, so each
remaining gap is ergonomics, not capability. It sends once unless the caller
classifies the command, and refuses a command name that would change the
request URL; `cargo run -p ytsaurus-client --example raw` reads a file through
it. Whether either official client has such a door was not checked.

## What was not verified

- Nothing was run against a cluster for this document; the Rust column is source
  plus its own doc comments, several of which cite cluster-observed errors.
- C++ was read from `interface/{client,operation,io,cypress,config}.h` and
  `yt/yt/client/api/client_common.h`: headers, not implementations.
- Go was read from `yt/go/yt/interface.go`, `config.go`, the retry interceptors
  and `mapreduce/registry.go`. Its RPC client was not examined, so a capability
  reachable only over RPC and not declared in `interface.go` is outside this.
- Method counts for C++ include overloads and are not comparable to Go's verb
  count as a number.
