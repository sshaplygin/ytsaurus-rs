# The three clients: C++, Go, and this one

YTsaurus ships two official clients. The **C++ MapReduce wrapper**
(`yt/cpp/mapreduce`) is what jobs have historically been written against; the
**Go SDK** (`yt/go`) is the newer one and the only one with a published
reference interface. There is also a native C++ client (`yt/yt/client/api`),
which is the RPC one used inside the cluster — where it is the one that has a
feature, this document says *native*.

This compares both against `ytsaurus-client`, and it exists for two reasons: to
say what "production-ready" would mean here in concrete terms, and to stop the
same research being redone. [`go-parity.md`](go-parity.md) covers the Go SDK's
twelve *examples*; this covers the API surface behind them.

Everything below was read from source. Where a claim could not be checked it
says so.

## How different are C++ and Go, really?

**Barely, in what they can do; enormously, in how a job is written.**

Both cover the whole cluster — nine operation types, dynamic tables, tablet
transactions, administration. The differences are not where a feature list would
put them:

**Row formats.** C++ has five families — `TNode`, protobuf, YaMR, Skiff, and raw
at any `TFormat`. Go has two: YSON and Skiff. This is not cosmetic. Protobuf is
the only way a C++ program gets a schema out of a type (`CreateTableSchema<T>()`
reads the descriptor), where Go infers one by reflecting over `yson` struct tags
at run time.

**How job code reaches the node.** Both serialise **the job object itself**:
C++ through the `Y_SAVELOAD_JOB` macro, Go by registering the struct with
`gob` and encrypting it into the operation's `SecureVault`. A job carries its
state from the launcher. That is the deepest single difference from this Rust
client, which ships a plain executable and gives it argv and environment.

**The operation.** C++ hands back an `IOperationPtr` that owns the lifecycle —
`Watch`, `GetBriefState`, `GetFailedJobInfo`, `Suspend`, `UpdateParameters`,
`GetWebInterfaceUrl`. Go's `mapreduce.Operation` is `ID()` and `Wait()`, with
everything else on a flat `yt.Client`.

And the surprise runs the other way too: **`TrimRows`, `GetTabletInfos`,
`ExplainQuery`, blob-table readers and a multi-threaded parallel reader
(`library/parallel_io`) exist in C++ and not in Go.** Neither is a superset.

## Transport and configuration

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Protocol | HTTP and RPC | HTTP and RPC | **HTTP API v4 only** — recorded non-goal |
| Token lookup | `Token`, `TokenPath`, `~/.yt/token` | `YT_TOKEN`; a file only with `ReadTokenFromFile` | `YT_TOKEN`, `YT_TOKEN_PATH`, `~/.yt/token` |
| Other credentials | TVM, service tickets, impersonation | 5 implementations, swappable per call | OAuth only |
| TLS | `UseTLS` | `UseTLS` + caller CA bundle | `tls` feature, system roots |
| Heavy-proxy routing | automatic (`THostManager`) | automatic, plus a 5-minute ban on failure | **manual** — `heavy_proxy()` returns one host |
| Compression | configurable, off by default | zstd both ways | gzip **inbound only** |
| Timeouts | connect and socket separately | 5 min light, none for heavy | **one, 120 s, not settable** |
| Batching several commands | `CreateBatchRequest` | `NewBatchRequest` | **none** |
| Retries | three policies by request class | interceptor chain | one policy × `Repeatable` |
| Client logging | global `ILogger` | `Config.Logger`, structured | **no logging dependency at all** |
| Distributed tracing | `EnableClientTracing` | `TraceFn` + Jaeger and OTel adapters | **none** |

Two things this client has that neither other does: retry logging that mutes
itself inside a job (`YT_JOB_ID`), and being usable from inside a job at all —
Go refuses unless `AllowRequestsFromJob` is set, because it is guarding against
a hundred thousand jobs finding the master at once. Here the one-binary
launcher-and-worker pattern depends on it.

## Cypress and paths

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Core verbs | 11 | 11 | 9 |
| `SetNode` on any node | yes | yes | **attributes only** (`set_attribute`) |
| `MultisetAttributes` | yes | yes | no |
| `CreateObject` (accounts, users) | yes | yes | no |
| Path: `append` | yes | yes | **yes** — `TablePath::new(p).append()` |
| Path: columns, ranges, key bounds | `TRichYPath` | `ypath.Rich` | **no** |
| Dynamic value | `TNode` | `yson.RawValue` | `YsonValue` |
| Read a node into a native type | protobuf/`TNode` | `GetNode(&out)` | **`get_as::<T>()`** |

## Tables, formats and schemas

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Row formats | **5** — TNode, protobuf, Skiff, YaMR, raw | 2 — YSON, Skiff | **1** — binary YSON |
| Schema from a native type | protobuf descriptors | reflection at run time | **`#[derive(TableRow)]`, at compile time** |
| Schema validated before sending | no | no | **`TableSchema::validate()`** |
| Whole table as typed rows in one call | no, loop a reader | no, loop a reader | **`read_table_rows` / `write_table_rows`** |
| Streaming row cursor | `TTableReader<T>` | `TableReader.Next/Scan` | in `ytsaurus-job`, not the client |
| Read a file back | `CreateFileReader` | `ReadFile` | **no — `write_file` has no counterpart** |
| Partitioned reads | `GetTablePartitions` | `PartitionTables` | no |
| Parallel reader | `library/parallel_io` | **no** | no |
| Blob tables | `CreateBlobTableReader` | **no** | no |
| Retries inside a table write | yes, resumable upload | yes, own transaction and 512 MB batches | **no — one attempt** |
| Table created from the first row | `InferSchema` option | **the default** | no — `create_table` first |

Skiff is parked here pending the benchmark in
[`benchmarking.md`](benchmarking.md); protobuf rows are a recorded non-goal.

## Operations and the job model

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Operation types | **9** | 9 | **5** — map, reduce, sort, map-reduce, vanilla |
| What you get back | `IOperationPtr`, 12+ methods | thin `Operation` — `ID`, `Wait` | **a `String` id** |
| Suspend / resume / complete | yes | yes | **no** |
| Update parameters while running | yes | yes | no |
| List operations, look up by alias | yes | yes | no |
| Reattach to another process's operation | `AttachOperation` | `Track(id)` | no object to attach to |
| Abort | yes | yes | **yes** — and documented as not idempotent |
| How job code reaches the node | `Y_SAVELOAD_JOB` | `gob` + `SecureVault` | **argv and environment** |
| Binary upload | automatic | automatic, md5-cached | manual, md5-cached |
| Failure explains itself with stderr | yes | only when the message matches | **always, up to 3 jobs** |
| Custom job statistics | yes | **no** | **yes** |

The Rust `OperationType` enum has no variant for merge, erase, remote-copy or
join-reduce, so those four cannot be named even through the raw-spec door.

## Transactions and locks

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Timeout / ping period | 120 s / 5 s | 15 s / 3 s | 30 s / timeout ÷ 3 |
| Handle doubles as a client | `ITransaction : IClientBase` | `Tx` embeds the interfaces | `Deref<Target = Client>` |
| Attach to one started elsewhere | yes, fully | yes | **binds commands only** — cannot ping, commit or abort |
| `Detach` — stop pinging, leave it alive | **yes** | partial | **no — dropping always aborts** |
| Learn it was lost without a command | no | **`Tx.Finished()` channel** | manual `ping()` |
| Prerequisite transaction ids | yes | yes | no |
| Wait for a waitable lock | `GetAcquiredFuture()` | **no helper** | **yes, with a mandatory deadline** |
| Unlock | yes | yes | no |
| Child-key / attribute locks | yes | yes | no — whole-node only |

`Detach` is the sharpest of these: there is no way to hand a live transaction to
another process from Rust.

## Dynamic tables, administration, the rest

Absent from the Rust client entirely. Most of it by [recorded
decision](../AGENTS.md); the exceptions are marked.

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Mount, select, lookup, insert | yes | yes | non-goal |
| Tablet transactions | native | **yes, first class** | non-goal |
| Wait for a tablet state | **no** | `migrate.MountAndWait` | — |
| Queues and consumers | native | yes | non-goal |
| Query Tracker | native | yes | **undecided**, not excluded |
| `WhoAmI`, `CheckPermission` | yes | yes | no |
| Maintenance, users, tokens | native | yes | out of charter, open decision |
| Test fixture | `TTestFixture` | `yttest`, dockertest | a shell script and self-checking examples |
| Code generation | protobuf row classes | `yt-gen-client` emits ~8 000 lines | `#[derive(TableRow)]` |

## Where this client sits

Roughly a quarter of `yt.Client`'s ~101 methods, and not the same quarter
either library would have picked. Three things here are better than in both:

- **the schema comes off the type at compile time**, not by reflection or from a
  protobuf descriptor;
- **the schema is validated before the request is sent**, turning cluster error
  314 into one sentence naming the column;
- **a failed operation always explains itself** with its jobs' stderr, where Go
  does it only when the error message happens to match a string.

Add typed whole-table I/O in one call, which neither has.

What is missing, in the order it would matter for production use, is tracked in
the [parity issue](https://github.com/sshaplygin/ytsaurus-rs/issues) — logging
and tracing first, then the operation object and its lifecycle, `read_file`,
batch requests, read-side column and range selection, and transaction `Detach`.

Behind all of them sits one structural gap: **`Transport::call` is
`pub(crate)`**, so a command this crate does not model cannot be sent at all.
Every entry above is unreachable even as a workaround until there is a public
raw-command door — `Client::start_operation` taking a raw spec already sets the
precedent that one is acceptable here.

## What was not verified

- Nothing was run against a cluster for this document; the Rust column is source
  plus its own doc comments, several of which cite cluster-observed errors.
- C++ was read from `interface/{client,operation,io,cypress,config}.h` and
  `yt/yt/client/api/client_common.h` — headers, not implementations.
- Go was read from `yt/go/yt/interface.go`, `config.go`, the retry interceptors
  and `mapreduce/registry.go`. Its RPC client was not examined, so a capability
  reachable only over RPC and not declared in `interface.go` is outside this.
- Method counts for C++ include overloads and are not comparable to Go's verb
  count as a number.
