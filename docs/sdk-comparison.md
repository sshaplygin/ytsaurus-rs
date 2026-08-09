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
| Heavy-proxy routing | automatic (`THostManager`) | automatic, plus a 5-minute ban on failure | automatic — a pool picked at random, refreshed lazily every minute, a failed host dropped until a refresh restores it; constrained to the configured domain, or to a list you write |
| Compression | configurable, off by default | zstd both ways | gzip **inbound only** |
| Timeouts | connect and socket separately | 5 min light, none for heavy | **one, 120 s, not settable** |
| Batching several commands | `CreateBatchRequest` → futures | `NewBatchRequest` → `BatchResponse[T]` | `BatchRequest` → `Vec<Result<…>>`, per-part; **no per-part retry**, where C++ re-queues a retriable part; a split batch that stops reports the prefix it applied, where C++ throws with it lost |
| Retries | three policies by request class | interceptor chain | one policy × `Repeatable` |
| Client logging | global `ILogger` | `Config.Logger`, structured | optional `tracing` feature, off by default |
| Distributed tracing | `EnableClientTracing` | `TraceFn` + Jaeger and OTel adapters | `TraceContext` → `traceparent`, no dependency |

The tracing row is closer than it looks: all three send the same W3C
`traceparent` header, and the cluster is what records the span. What Go's
`ytjaeger` and `ytotel` adapters add is reading the context out of an ambient
`context.Context`; here it is passed to the client, because Rust has no ambient
one to read. An application already exporting OpenTelemetry spans formats its
current one into a `traceparent` and hands that over, which is the same picture
by a shorter road.

The heavy-proxy row recorded this table's one deliberate divergence for a
release — "one answer, never refreshed, walked host by host as it fails" —
and #40 retired it. The official clients disagree on trimmings (C++ refreshes
lazily on access; Go refreshes in the background and bans a failing proxy for
five minutes) but agree on the core, **never commit to one host**, and that
property earned its keep here the hard way: a certificate valid for every
proxy but one pinned the client to the one bad host for as long as it lived,
and a fleet of pinned clients never rebalanced — whichever host a client's
one lookup happened to name kept that client through every drain and load
shift afterwards. Now:

**Selection is random per command**, from the pool the answer named, as both
official clients pick — `THostManager` with `RandomNumber`, Go's
`ProxySet.PickRandom`. The entropy is the crate's existing id source, whose
contract — *unique, not unpredictable* — is the right bar for load-spreading,
so no new dependency.

**The list is refreshed lazily on access**, the C++ way rather than Go's: the
heavy command that finds the answer older than
`Client::with_host_list_refresh_interval` — one minute by default, the
[proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)'s
own "re-query every minute" — asks `/hosts` first. No background thread, so a
client that stops uploading stops asking; a refresh that fails keeps the
previous answer in use.

**A failing host is dropped, not walked to the next and not pinned.** The ban
is shorter-lived than Go's five minutes: a dropped host stays out until a
refresh names it again, at most one interval away — so a host that was merely
draining comes back within a minute of the cluster vouching for it, and a
*persistently* bad one (the misissued certificate that motivated #40) is
re-learned at one failed command per interval until an operator fixes it,
the price of not keeping a second clock. The drop turns on "was this failure
attributable to the host" — deliberately including a rejected certificate,
the per-host condition the old walk's predicate gated out — and a pool with
nobody left falls back to the configured address for ten seconds before the
cluster is asked again. Falling back any earlier was itself a bug once: the
fallback address on a deployment with separate roles is a *control* proxy, so
a single transient 503 answered ten seconds of uploads with `Control proxy
may not serve heavy requests with input data` — the very failure the routing
was written for.

**And a discovered host is constrained**, which neither other client does: a
name is used only if it shares the configured address's domain, and the scheme
and port come from the configured address rather than from the answer. Read that
as a guard against a typo and against an obviously foreign name rather than as a
promise about the token, which is a promise a suffix rule cannot make — steering
the `/hosts` body means controlling the proxy or the wire, and either already
has the token. `Client::with_heavy_proxies_anywhere(true)` restores the other
clients' behaviour; `Client::with_heavy_proxies_in([…])` goes the other way and
is the only one of the four that is a boundary, because it is a list somebody
wrote on purpose.

**A large installation showed what that constraint costs**, and it is not
hypothetical: a managed installation answered `/hosts` with 79 heavy proxies in
a zone of its own, under a domain the configured address does not share, so the
rule refused every one of them and no heavy command could be sent at all. The
Go SDK sends the token to whatever `/hosts` names — `listHeavyProxies` returns
the list verbatim and `proxy_set.go` adds every entry without a filter — so on
that cluster the stricter client is the one that does not work.
`Client::with_heavy_proxies_under([…])` is the answer this client grew for it:
the domain rule plus the domains an operator names, which is narrower than the
official clients' behaviour and survives a proxy rotation, which a written-out
list does not. The deviation from both official
clients stands — it is a guard the others do not have — but it is now a guard
with a setting between "as shipped" and "off".

Logging is a smaller row than it was and still the softer of the two: this
client has one span per attempt and one event per retry, where the official
clients log request bodies, proxy choices and connection lifecycles.

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
| Path: columns, ranges, key bounds | `TRichYPath` | `ypath.Rich` | **yes** — `TablePath::columns` / `::range`, `RowRange`, `Key`; a *write* with a read selection is refused locally, because the cluster silently ignores it there |
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
| Read a file back | `CreateFileReader` | `ReadFile` | **yes — `read_file`, and `read_file_streaming` for one that does not fit** |
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
| Operation types | 9 | 9 | **9** — spec builders for 8 of them |
| What you get back | `IOperationPtr`, 12+ methods | thin `Operation` — `ID`, `Wait` | **a `String` id, and an `Operation` handle over it** |
| Suspend / resume / complete | yes | yes | **yes** — and which of them are idempotent is measured |
| Update parameters while running | yes | yes | **yes** |
| List operations, look up by alias | yes | yes | **yes** |
| Reattach to another process's operation | `AttachOperation` | `Track(id)` | **`attach_operation(id)`** |
| Abort | yes | yes | **yes** — and documented as not idempotent |
| One job by id, and its input | yes | yes | **yes** — `get_job`, `get_job_input` |
| How job code reaches the node | `Y_SAVELOAD_JOB` | `gob` + `SecureVault` | **argv and environment** |
| Binary upload | automatic | automatic, md5-cached | manual, md5-cached |
| Failure explains itself with stderr | yes | only when the message matches | **always, up to 3 jobs** |
| Custom job statistics | yes | **no** | **yes** |

`OperationType` now names all nine, `MergeSpec`, `EraseSpec` and
`RemoteCopySpec` join the five that had builders, and `join_reduce` deliberately
has none: the current documentation no longer lists it under `start_operation`
and describes the same work as a reduce with `join_by` and
`enable_key_guarantee=%false`, which `ReduceSpec::with_raw` builds.

Where this now goes further than either official client is in **saying which of
these commands may be repeated, and why**. Suspend is retried and resume is not,
because a second suspend is accepted and a second resume is refused with code
201; complete and abort are sent once, because the second is answered `No such
operation`. Each of those was measured against a cluster rather than assumed
from the fact that all four are "mutating and light".

## Transactions and locks

| | C++ | Go | Rust |
| --- | --- | --- | --- |
| Timeout / ping period | 120 s / 5 s | 15 s / 3 s | 30 s / timeout ÷ 3 |
| Handle doubles as a client | `ITransaction : IClientBase` | `Tx` embeds the interfaces | `Deref<Target = Client>` |
| Attach to one started elsewhere | yes, fully | yes | yes — `attach_transaction`, pinging included |
| `Detach` — stop pinging, leave it alive | **yes** | partial | yes — and dropping an *attached* handle detaches too |
| Learn it was lost without a command | no | **`Tx.Finished()` channel** | `is_lost()`, polled — or `ping()` |
| Prerequisite transaction ids | yes | yes | no |
| Wait for a waitable lock | `GetAcquiredFuture()` | **no helper** | **yes, with a mandatory deadline** |
| Unlock | yes | yes | no |
| Child-key / attribute locks | yes | yes | no — whole-node only |

`Detach` used to be the sharpest of these — there was no way to hand a live
transaction to another process from Rust. There is now: `Transaction::detach`
stops the keep-alive and leaves the transaction running, `attach_transaction`
turns the id back into a pinging handle elsewhere (reading the interval from
`#<id>/@timeout`, as the attacher must), and `ping_transaction` /
`commit_transaction` / `abort_transaction` finish one from nothing but the id.
What `Drop` does follows the C++ destructor's line: a handle this process
*started* still aborts on drop — that is what makes `?` safe inside a
transaction — where an *attached* one detaches. Go's
`AttachTx(id, {AutoPingable: false})` maps onto `with_transaction` plus the
by-id commands; the Rust `attach_transaction` always pings, because a
non-pinging handle would duplicate exactly that pair. It also pings *before*
returning, which neither of the others does: `@timeout` is the configured
lifetime and the id says nothing about how much of it a handoff has already
spent.

The remaining Go advantage in that table is `Tx.Finished()`, which is pushed
rather than polled. `Transaction::is_lost` answers the same question — the
keep-alive stops for exactly one reason, and this is that verdict made
visible — but a holder has to ask.

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
the [parity issue](https://github.com/sshaplygin/ytsaurus-rs/issues). The first
all six on that list are **now built**: logging and tracing — a `traceparent`
the cluster joins, and an optional `tracing` feature — the operation object and
its lifecycle, which is what the table above came to, read-side column and
range selection (`TablePath::columns` / `::range`), `read_file` with its
streaming half (#10), batch requests (`BatchRequest` and
`Client::execute_batch`, per-part `Result`s with the C++ client's
`Concurrency` and `BatchPartMaxSize` options, plus one thing neither official
client offers — a split batch that stops names the prefix it already applied),
and transaction `Detach`, with attach and the by-id commands beside it.

Behind all of them used to sit one structural gap: `Transport::call` was
`pub(crate)`, so a command this crate does not model could not be sent at all,
and every entry above was unreachable even as a workaround. **That is now
`Client::raw_command`** — with `raw_command_streaming` and
`raw_command_upload` for the heavy shapes — so each remaining entry is a
question of ergonomics rather than of capability. `read_file` was the clearest
case, reachable for a release only as
`raw_command_streaming(Method::Get, "read_file", …)`: that is the door its
methods grew out of — the wire shape was verified through it before it was
modelled — and the `raw` example still reads a file that way, to show the door
working on a command whose shape is known. See
`cargo run -p ytsaurus-client --example raw`.

Whether either official client offers the same door was not checked for this
document, and it matters less to them: both model far more of the API, so the
set of commands a user has to reach around them for is much smaller. The door
here is deliberately narrow — sent once unless the caller classifies the
command, and refusing a command name that would change the request URL.

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
