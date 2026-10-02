# Changelog

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.
`ytsaurus-skiff` and `ytsaurus-job` shipped a test file each that could not
compile from their tarballs; this crate had no such file.

## 0.3.0 - 2026-08-16

- Added typed dynamic-table commands `lookup_rows_dynamic`,
  `select_rows_dynamic`, `insert_rows_dynamic` and `delete_rows_dynamic`,
  previously reachable only through `Client::raw_command`. Insert and delete
  send a tabular input stream, select returns one, lookup does both; all four
  are heavy.
- Added `create_client` and `create_rpc_client`, which return the same
  `ytsaurus_api::TableClient`, so choosing a transport is one line.
- Added the `rpc` feature, **off by default and required to stay off**: it pulls
  in tokio and prost, and this crate is a dev-dependency of `ytsaurus-job`,
  whose examples are the static musl workers. CI asserts the worker graph has
  neither.
- Tablet transactions are not available over HTTP: the client returns
  `Error::Unsupported` instead of failing on the second call. They are sticky
  to the proxy that created them, and HTTP routes each request independently;
  the cluster's own message recommends the RPC API.
- Renamed the end-to-end example from `e2e` to `client_e2e`; both packages
  built `target/.../examples/e2e` and overwrote each other. The `ytsaurus-rpc`
  counterpart is `rpc_e2e`.
- Added `error_summary(&YsonValue) -> Option<String>`, previously `pub(crate)`,
  which `JobInfo::error` and the operation errors are built from: the outer
  message and the innermost cause (`Failed to run query: Memory limit
  exceeded`), without the attributes between. Use it on the error documents
  `Client::raw_command` returns.

## 0.2.6

Never released. The version was bumped in the workspace and the tag was never
cut; these changes reached crates.io in 0.3.0. This crate had none of its own.

## 0.2.5 - 2026-08-10

### Breaking changes

- **Breaking** `ClientError` is `#[non_exhaustive]`: a `match` over it needs a
  `_` arm. Naming, constructing and destructuring a variant are unaffected.
- **Breaking** `Repeatable` gained a variant, `Heavy`, and
  `#[non_exhaustive]`: an exhaustive `match` needs a `_` arm.
- **Breaking** `Client::remove` sends the cluster's defaults (the node must
  exist, a map node must be empty) instead of `recursive=%true; force=%true`.
  To delete a subtree or tolerate absence, call `Client::remove_tree`.
- **Breaking** `CachedFile` gained a `cached` field. Code that destructures
  every field needs `..`; matching by name or reading fields is unaffected.
- **Breaking** a write to a path carrying a read selection is refused locally
  with `ClientError::Config`, before anything is sent; the cluster would
  replace the whole table and answer 200. This covers `TablePath::columns` and
  `TablePath::range` and selection syntax in the path string: a leading `<…>`
  block, or an unescaped `[` or `{` (an escaped `\[` in a node name is still
  writable). So `write_table("<append=%true>//tmp/t", rows)`, which appended,
  now fails: use `TablePath::append()`, or `Client::raw_command` for any other
  write attribute.
- **Breaking** `TablePath` no longer derives `Eq`, since a key bound may hold a
  double; `PartialEq` remains. Neither `TablePath` nor `RowRange` derives
  `Default`. `read_skiff_table` refuses a path whose columns are selected twice,
  by `TablePath::columns` or by `{…}` in the path string, because the Skiff
  format adds a `columns` attribute itself; a row range, typed or in the
  string, is allowed.
- **Breaking** a row range that runs backwards (`rows(5..3)`, `keys(b..a)`) or
  has a negative row index (`rows(-5..2)`) is refused rather than sent. The
  cluster answers the first with no rows and clamps the second to 0. An empty
  range (`rows(5..5)`, `keys(a..a)`) is still sent.

### Configuration from the environment

- Added `Client::with_heavy_proxies_under(domains)`: heavy proxies may be in the
  configured address's domain or in any domain named, for an installation whose
  heavy proxies sit in a zone of their own. Entries are normalised (whitespace,
  a leading or trailing dot, a leading `*`, a scheme, a port), and one with no
  dot is dropped, since it would admit a whole top-level domain. It is a typo
  guard like the default; `with_heavy_proxies_in` is the boundary.
- Added four variables to `Client::from_env`, each inert when unset:

  | Variable | Effect |
  | --- | --- |
  | `YT_PROXY_SUFFIX` | Completes a bare cluster name: `YT_PROXY=hume` with `YT_PROXY_SUFFIX=.yt.example.net` addresses `hume.yt.example.net`. Only for a name with no colon, no dot and not `localhost`, as in the Go SDK. No suffix is compiled in. |
  | `YT_HEAVY_PROXY_DOMAINS` | Comma- or space-separated, into `with_heavy_proxies_under`. |
  | `YT_HEAVY_PROXIES_ANYWHERE` | `1`, `true` or `yes`, into `with_heavy_proxies_anywhere`. Applied after the domains, so the wider of the two wins. |
  | `YT_FILE_CACHE` | Into `with_file_cache`, for an installation whose shared cache is read-only. |

  The environment can widen the heavy-proxy rule and cannot narrow it:
  `with_heavy_proxies_in` is settable only in Rust.
- Changed `Client::from_env` to trim every variable it reads, `YT_PROXY`
  included, and to treat a variable set to nothing as unset. Previously
  `YT_PROXY=" http://localhost:8000 "` failed as a malformed URL and `YT_PROXY=`
  addressed `https://`.
- Changed the `UnknownIssuer` error: `ClientError::Transport` now names
  `YT_CA_BUNDLE` and the `platform-verifier` feature when the root store
  rejected the certificate. Not for `NotValidForName`, and not for a transient
  platform-verifier failure such as `Other(OtherError("UnknownIssuer lookup
  failed"))`, which is still retried.
- Changed both messages about a declined `/hosts` answer (the one-time
  announcement, and the sentence added to the cluster's refusal of each heavy
  command) to offer `with_heavy_proxies_under`, `with_heavy_proxies_in` and
  `with_heavy_proxies_anywhere` with their environment spellings, and to name a
  domain dropped for having no dot.
- Changed the `cached_upload` example to bring its own file cache, since a
  managed shared cache refuses its clearing `remove` with code 901.
  `YT_FILE_CACHE` points it at a shared one; it then stops with an explanation.
- Changed the `profile` example to caption its result from the run
  (`//sys/@cluster_name`, rounds, size), and raised the default
  `YT_PROFILE_ROUNDS` from 3 to 5.

### Handing a transaction to another process (#13)

- Added `Transaction::detach`, C++'s `ITransaction::Detach()`: stops the
  keep-alive thread and returns the id, leaving the transaction running until
  its timeout (30 s by default) after the last ping, unless the receiver keeps
  it alive. It waits up to five seconds for the thread to stop; at most one ping
  is left outstanding, and the thread exits after it. **Above a 30 s timeout a
  stalled ping can outlast that wait, reach the master after `detach` returns
  and restart the expiry clock.** At or below 30 s the wait always ends with the
  thread's exit.
- Added `Client::attach_transaction(id)`: a full `Transaction` (bound client,
  ping thread, `commit`, `abort`, `ping`) for an id from elsewhere. It reads
  `#<id>/@timeout` for the ping interval and pings once before returning, so a
  transaction that is gone fails here, naming the id. **Dropping an attached
  handle detaches rather than aborts.** It always pings; Go's
  `AttachTx(id, {AutoPingable: false})` maps onto `with_transaction` and the
  by-id commands. Two attaches to one id both ping, and the first to finish the
  transaction decides it.
- Added `Transaction::is_lost`: true once a ping was answered "no such
  transaction" and the keep-alive stopped. Also false when the thread never
  started or panicked. It takes `&self`; after `detach`, probe with
  `Client::ping_transaction`.
- Added `Client::ping_transaction`, `Client::commit_transaction` and
  `Client::abort_transaction`, taking a bare id. Commit carries a mutation ID,
  abort is retried freely, and a ping doubles as a liveness probe.
- Unchanged: dropping a transaction this process started still aborts it.
  Added the `detach` example. Documented that `mem::forget` on a `Transaction`
  leaks its keep-alive thread, which holds the transaction and its locks open
  for the life of the process; `detach` is the way to hand one on.

### Reading a file

- Added `Client::read_file` and `Client::read_file_streaming` (#10), buffered
  and streaming like the table reads, and routed to a heavy proxy like them.
- `read_file` checks the body against the node's `@uncompressed_data_size` with
  one light `get` after the read, and fails, naming both numbers, on a mismatch
  or a non-integer answer. The streaming read cannot check: compare
  `FileReader::bytes_read` with the same attribute.
- Added `FileReader`, the streaming read's type: `ResponseReader` under the
  name the file path uses, as `TableReader` is for tables.

### The 512 MiB cap on a buffered response

- Fixed the cap counting wire bytes instead of decoded ones. Responses are
  gzip, so it bounded almost nothing. **A buffered response that decodes to more
  than 512 MiB now fails**; a body of exactly 512 MiB now passes.
  `read_table_streaming`, `read_file_streaming` and `write_table` are
  unaffected. The cap is on bytes held, not on peak memory (a growing `Vec`
  peaks near 1.5×), and the non-2xx branch of a streaming open and the `/hosts`
  lookup keep `ureq`'s wire-only default.
- Changed the error for a buffered response over the cap from
  `ClientError::Transport` to a new `ClientError::ResponseTooLarge { command,
  limit }`, which is never retried and does not drop a heavy proxy from the
  pool. It reaches `read_table`, `read_table_with_format`, `read_skiff_table`,
  `read_table_rows`, a buffered `raw_command` and any light command answered
  that large. A caller matching `Transport` to retry stops matching it. The
  message names the cap and the streaming form of the command.

### Selecting columns and rows on a read

- Added `TablePath::columns(…)` and `TablePath::range(…)`, with `RowRange` and
  `Key`, sent as the `columns` and `ranges` attributes on the path
  ([rich YPath](https://ytsaurus.tech/docs/en/user-guide/storage/ypath)).
- Row ranges are Rust ranges: `path.range(0..100)`, `range(100..)`,
  `range(..)`. Key ranges are `RowRange::keys(Key::from("a")..Key::from("b"))`:
  an included lower and excluded upper bound are sent as `key`, `..=` and
  `Bound::Excluded` below as `key_bound`. `RowRange::exact_key` is the `exact`
  selector. `keys(a..b)` and `keys(a..=b)` differ by every row whose key starts
  with `b` ([protocol reference](../../docs/protocol-reference.md#selecting-columns-and-rows-on-a-path)).
- Added `yson_build::uint`, for `uint64` keys above `i64::MAX`.
- Changed `read_table`, `read_table_with_format`, `read_skiff_table`,
  `read_table_rows` and `read_table_streaming` to take `impl Into<TablePath>`.
  `&str`, `String`, `&String`, `&&str` and `Cow<str>` compile unchanged; a call
  relying on inference (`path.as_ref()`) may need `&str` spelled once.
- A read still sends a path string verbatim, but refuses a typed selection
  added to a string that already spells the same kind, where the cluster would
  silently discard the string's half, or to a string opening with `<…>`. Rows
  and columns from different sources compose and are sent.
- `columns([])` is sent: it returns one empty map per row, counting a range's
  rows with no column bytes on the wire.

### Heavy proxies: a refreshed pool (#40)

- Changed heavy-command routing to match the C++ and Go SDKs: the whole
  `/hosts` answer is a pool, each heavy command picks a member at random, and
  the first heavy command to find the answer older than the refresh interval
  asks again. A failed refresh keeps the previous answer for another interval.
  No background thread. A heavy command may now pay one `/hosts` round trip
  mid-life (bounded by `with_hosts_timeout`, at most once per interval), and
  "the cluster named no heavy proxy" expires after one interval instead of
  lasting for the client's life. `with_proxy_discovery(false)`, `heavy_proxy()`
  (the cluster's first pick) and `with_hosts_retry_after` are unchanged.
- Fixed a failed heavy command dropping its host only on failures worth asking
  `/hosts` again about. Any failure attributable to the host now drops it until
  a refresh names it again: a refused connection, a 503, a wrong-role refusal
  or a rejected certificate. An emptied pool falls back to the configured
  address for `with_hosts_retry_after`, then asks afresh.
- Added `Client::with_host_list_refresh_interval(Duration)`, default one minute.
  `Duration::ZERO` asks before every heavy command; `Duration::MAX` never
  refreshes, though a failed host is still dropped and an emptied pool still
  falls back and asks again.

### Batches (#11)

- Added `BatchRequest` and `Client::execute_batch`, the cluster's
  `execute_batch` (C++ `CreateBatchRequest`, Go `NewBatchRequest`). The answer
  is a `Vec` of per-part results in part order: `Ok` holds the part's envelope
  (`{node_id=…}` for a create, `{value=…}` for an exists), `Err` a
  `ClientError::Cluster` named after the part's command.
- `BatchRequest` builds parts with `create`, `create_table`, `exists`, `get`,
  `list`, `remove`, `remove_tree` and `set_attribute`, which send what their
  `Client` namesakes send, and `raw`. A `raw` part naming a command the cluster
  will not batch (output type `tabular` or `binary`, or input `binary`; 21
  names, `select_rows` and `lookup_rows` among them) is refused locally. That
  list is one cluster's registry and may be incomplete.
- Added `with_concurrency` (the server-side cap, cluster default 50) and
  `with_max_part_size` (C++ `BatchPartMaxSize`). A larger batch is split into
  consecutive requests, `concurrency × 5` parts each by default, with results in
  part order. Nothing is rolled back across requests, and unlike the C++ client
  a retriable part is not re-queued ([sdk-comparison](../../docs/sdk-comparison.md)).
- Added `ClientError::BatchInterrupted { answered, parts, cause }`, for a split
  batch that stops partway: `answered` holds the per-part results of every
  request that completed. A one-request batch returns the underlying error as
  before. `answered` is what came back, not what was applied: **a request that
  failed while executing may have run all of its parts.**
- Added `Client::execute_batch_with`, taking a caller's `MutationId`, so a
  batch replayed after a crash under `id.as_retry()` is deduplicated part by
  part. A batch that would be split is refused with `ClientError::Config` when
  given an id.
- Added `BatchRequest::raw_with`, taking a `Repeatable`, so a raw read does not
  make the whole batch send-once. `Repeatable::Heavy` is refused.
- A mutating batch is retried under one mutation id, which the cluster derives
  per part. A batch of reads carries no id; a batch with a `raw` part is sent
  once. A client bound to a transaction stamps each part, not the envelope,
  whose `transaction_id` the cluster drops; a part naming its own transaction
  keeps it, and a command with no transaction is left alone.
- A batch sends its parameters in the POST body, not `X-YT-Parameters`; the
  proxy merges the two (`TContext::CaptureParameters`; measured: `requests` in
  the body and `mutation_id` in the header arrive as one set). A cross-origin
  redirect on a batch is therefore refused with `RedirectRefusal::Payload` even
  without a token, where the same commands sent singly would follow it.
- Parts run in parallel, so a part and its consequence belong in two batches.
  A part naming an unknown command fails the whole request with HTTP 400 after
  the other parts have run.
- The parser refuses a `results` count that does not match the parts, and an
  item that is not `{output=…}`, `{error=…}` or, for `set`, `remove` or a `raw`
  part, `{}`. A part's success must carry its key: `node_id` for `create`,
  `value` for `exists`, `get` and `list`. A `create` answered `{}` used to pass
  and panic the caller at `answer["node_id"]`.
- Added the `batch` example. `tests/batch.rs` pins the single request and the
  wire shape.

### A file cache that refuses writes (#32)

- Fixed `Client::upload_worker_cached` failing on an installation that manages
  `//tmp/yt_wrapper/file_storage` itself. A code-901 refusal of the cache's own
  writes (creating the cache directory, creating the staging node,
  `put_file_to_cache`) now uploads the worker under `//tmp` and carries on. A
  901 on the bytes themselves, or any other error, still fails. The code is
  found anywhere in the error document. Not verified against a cluster that
  denies access; `tests/file_cache.rs` scripts the refusals.
- `CachedFile::cached` is true for a hit and an accepted upload, false only for
  the fallback; branch on it, not on `uploaded`, which is true for both, or a
  cleanup deletes the shared cache entry.
- Added a warning when the fallback is taken, on stderr or as a `WARN` event
  under the `tracing` feature: the refused path, the cluster's message, and
  `Client::with_file_cache`.
- Documented the fallback node: an ordinary `//tmp` node with `//tmp`'s ACL, no
  expiry and a name that is unique but not unpredictable, so a co-tenant can
  rewrite it before the job runs. Point `with_file_cache` at your own directory.

### Redirects

- Fixed a credential-carrying request following a redirect and arriving
  without its token (`cluster error 111: Client is missing credentials`).
  `ureq` follows no redirect now (`max_redirects(0)`); the client decides:
  - same origin: followed, token included, `Location` resolved against the
    request's address ([RFC 3986 §4.2](https://www.rfc-editor.org/rfc/rfc3986#section-4.2));
  - another origin, with credentials: refused with `ClientError::Redirected`,
    naming the status and target without vouching for the token, and pointing
    to `Client::heavy_proxy` only for a command that could use one;
  - another origin, with a non-empty body, token or not:
    `RedirectRefusal::Payload`;
  - a body that cannot be sent twice (`write_table_rows`,
    `raw_command_upload`): refused anywhere, `RedirectRefusal::Body`. This
    fixes `write_table` returning `Ok(())` with no rows written after a
    redirect;
  - more than ten hops: refused as a loop.
- Changed a followed redirect to resend the same method and body, whatever the
  status, so a bodiless `POST` follows a balancer's `301`.
- Fixed the request timeout restarting on every hop; a redirect chain shares
  one deadline per attempt (a retry still gets a fresh one), in
  `Client::heavy_proxy` too.
- Fixed `Location: ?path=//other` resolving against the request's directory
  instead of its path
  ([RFC 3986 §5.3](https://www.rfc-editor.org/rfc/rfc3986#section-5.3)); a bare
  `#fragment` also keeps the query.
- Added `RedirectRefusal` (`Credentials`, `Body`, `Payload`, `TooMany`;
  non-exhaustive), a field of `ClientError::Redirected`.
- Fixed `get_job_stderr` missing from the heavy-command list, so a redirected
  stderr fetch got no advice. `read_file` and `read_blob_table` stay on it.

### A cluster behind a private CA (#29)

- Added `YT_CA_BUNDLE`, a PEM file of roots used instead of the compiled-in
  Mozilla bundle, and the `platform-verifier` feature, which trusts the OS
  store. Both off by default; the bundle wins where both are set. Both sit
  behind `tls`: no new direct dependency, and the musl worker graph is
  unchanged (CI also checks it for `rustls-platform-verifier`).
- The bundle is read once per process and must be a regular file of at most
  16 MB. One that cannot be read, holds no certificate, or holds a
  `BEGIN CERTIFICATE` block that is not X.509 (a re-armoured PKCS#7 `.p7b`) is
  refused at the first request that needs it, naming the file and the number of
  bad blocks. A plain-HTTP cluster is unaffected.
- Fixed `UnknownIssuer` and `NotValidForName` being retried five times, about
  15 s; they are reported at once. Every other TLS failure, a reset or refused
  connection and a timeout are still retried.

### Heavy commands go to a heavy proxy (#30)

- Added automatic routing. Heavy commands (`write_table`, `write_file`,
  `read_table`, `upload_worker`, `write_table_rows` and the streaming forms)
  went to `YT_PROXY`, which on a role-separated installation is a control proxy
  that refuses them. The first heavy command now asks `/hosts`, and every clone
  of the client shares the answer, refreshed as in the pool entry above. A
  failed heavy command is not re-sent.
- A failed heavy command drops the host it used, and the next name takes over.
  So does a proxy that refuses heavy work for its role, or cannot be reached.
  Only when every name has failed does the client fall back to the configured
  address, for ten seconds, then ask again.
- Added `Client::with_heavy_proxies_in`, an explicit list of proxies, compared
  without case and with a port only where both sides name one.
- Added `Client::with_hosts_timeout` (default 800 ms, one attempt, independent
  of `with_timeout`) and `Client::with_hosts_retry_after` (default ten
  seconds). The lookup no longer runs the client's retry policy while holding
  the lock other heavy commands wait on.
- A `/hosts` answer declined in full is announced once, on stderr or as a
  `WARN` event under `tracing`, muted inside a job; a heavy command then refused
  at the configured address carries a sentence saying so and naming the builder
  call that changes it.
- A discovered name is used only with the configured address's domain (or as
  the configured host itself), its scheme and port, a numeric port, no `://`,
  `/`, `@` or whitespace, and brackets only around an IPv6 literal. A
  configured name with no dot matches as a non-leftmost label: `hume` follows
  `n0008-sas.hume.yt.example.net`, not `hume.evil.com`. A refused name is
  skipped. The domain rule is a typo guard, not a token boundary.
- Added `Client::with_heavy_proxies_anywhere`, which relaxes only the domain
  rule.
- A cluster that names no heavy proxy, answers `/hosts` with 404 or with
  something other than host names, or has every name refused, is served at the
  configured address, and that answer is kept. A cluster on loopback is not
  asked. Added `Client::with_proxy_discovery` to force discovery on or off.
- A routed failure names its host: `write_table at n0132-sas.example.net:9013: …`.
- Corrected the documented control-proxy refusal: a heavy write gets 503 with
  `Retry-After: 60`, a heavy read a 307; it had said HTTP 200. `heavy_proxy`
  remains for reading the chosen address.
- Added `Repeatable::Heavy`, split from `Repeatable::Never`. `Transport::open`
  and `Transport::upload` are heavy, so raw streaming commands are routed too.

### The operation lifecycle

- Added `suspend_operation`, `resume_operation`, `complete_operation`,
  `update_operation_parameters`, `list_operations`, `list_operation_events`,
  `get_operation`, `get_operation_by_alias`, `operation_suspended`,
  `operation_status`, `get_job` and `get_job_input`, and `Operation`, a handle
  over a client and an id with the same commands. `Client::attach_operation(id)`
  makes one from an id. Dropping an `Operation` does nothing. `start_map` and
  its siblings still return a `String`.
- `operation_status` reads the state and the suspension together, and
  `wait_for_operation` polls it, so a wait on a paused operation reports
  `running, suspended`.
- `suspend_operation` and `update_operation_parameters` are retried;
  `resume_operation` and `complete_operation` are not. An update that would
  change nothing is refused locally.
- `operation_state`, `job_statistics` and `operation_result_error` now read
  through `get_operation`.
- Added `OperationType::Merge`, `Erase`, `RemoteCopy` and `JoinReduce`, with
  `MergeSpec`, `EraseSpec`, `RemoteCopySpec`, `start_merge`, `start_erase` and
  `start_remote_copy`. A sorted merge without `merge_by` is sent. `JoinReduce`
  has no builder: use `ReduceSpec::with_raw` with `join_by` and
  `enable_key_guarantee=%false`.
- Added the `lifecycle` example.

### Tracing

- Added `TraceContext` and `Client::with_trace_context`: every request, `/hosts`
  included, carries a W3C `traceparent`, with no new dependency.
  `TraceContext::parse` continues a trace (the version field may be absent; a
  later version's extra fields are ignored) and refuses a malformed header;
  `TraceContext::new` starts one. The caller's span id is sent unchanged.
- Added `TraceContext::with_tracestate`, forwarded unmodified, and
  `TraceContext::yt_trace_id`, the cluster's spelling of the id
  (`8e9bcc43-5c2be9b4-56f18c4e-117ea314`).
- Added the `tracing` feature, off by default: a span per attempt (command,
  attempt, duration), and retry messages as `WARN` events. It adds `tracing`,
  `tracing-core`, `pin-project-lite`, and `once_cell` where TLS is off. **With
  no subscriber installed, the stderr line is still printed.** `attempt` is the
  try that failed and `of` the number allowed, in event and span alike.
- Unchanged: retry reporting is muted inside a job, in both forms;
  `RetryPolicy::loud` turns it on.

### Raw commands and smaller changes

- Added the `e2e` example (renamed `client_e2e` in 0.3.0): the three checks of
  `tests/cluster-e2e/run_e2e.sh`, run through this crate with no Python. It
  creates its destination tables, which operations here never do.
- Added `Client::raw_command(method, command, params, payload)`,
  `raw_command_with(…, repeatable, mutation_id)`,
  `raw_command_streaming(method, command, params)`, which returns the response
  unread, and `raw_command_upload(method, command, params, body)`, which streams
  the body. `raw_command` is sent once whatever the retry policy, and is
  stamped with the client's transaction (`NO_TRANSACTION` exceptions apply). A
  command name containing `/`, `?`, `#` or whitespace is refused, as is a
  payload with `Method::Get`.
- Added `Method` and `Repeatable` to the public API, `ResponseReader` (which
  `TableReader` now names), and `yson_build::empty_map`.
- Fixed transaction pings: a ping is one attempt bounded by half the ping
  interval instead of the full retry pipeline, and an answer that the
  transaction is gone stops the ping thread; transient failures do not.
- Fixed `Drop` of an uncommitted `Transaction` blocking for minutes: its abort
  is one attempt bounded by five seconds. An explicit `abort()` keeps the full
  retries.
- Fixed a connection failing mid-body being reported as `Decode`, which is
  never retried; it is `Transport`, so a `Repeatable::Freely` command retries it.
- Fixed `MapReduceSpec::with_local_file`, `with_local_file_named` and
  `with_memory_limit` reaching the mapper only when `with_mapper` came first.
- Fixed the 120-second request timeout cutting off `read_table_streaming`,
  `write_table_rows` and `write_table_streaming`: for streaming it bounds
  resolve, connect, sending the request and the response headers, not the
  data. Buffered commands keep it end to end. Added `Client::with_timeout`
  (default two minutes).

### Aborting an operation, appending to a table

- Added `Client::abort_operation(id, reason)`, sent once
  (`Repeatable::Never`), and `Client::operation_result_error`, which reads the
  error document the reason is folded into.
- Added `TablePath`; `write_table`, `write_table_rows` and
  `write_table_streaming` take `impl Into<TablePath>`, so `&str` call sites are
  unaffected. `TablePath::new(p).append()` appends instead of replacing.
- Added the `cargo bench -p ytsaurus-client` benchmark
  ([benchmarking](../../docs/benchmarking.md)).
- Fixed every `write_table_rows` and `write_table_streaming` opening a new
  connection: the response is now read, so the connection is pooled.

### Rows are Rust values

- **Added** `Client::write_table_rows` and `Client::read_table_rows`. A table is
  written from anything that yields serialisable values and read back into
  anything that deserialises, so the encoding is this crate's problem rather
  than every caller's.
- **Added** `Client::get_as`, which reads a node — or one attribute — into a
  Rust type instead of a `YsonValue` to walk.

This came out of going through the **Go SDK's twelve examples** one by one and
asking what each would need here; the answer is recorded in
[`docs/go-parity.md`](../../docs/go-parity.md). Go writes structs to a table and
scans structs back, and the SDK does the encoding. This client had bytes in and
bytes out, and the consequence was measurable: **eleven of its twelve examples
hand-rolled the same YSON encode loop**. Nine of them no longer do.

```rust
client.write_table_rows("//tmp/contacts", (0..100).map(contact))?;
let back: Vec<Contact> = client.read_table_rows("//tmp/contacts")?;
```

An iterator rather than a slice, because the encoder runs **inside** the request
body: rows are serialised a bufferful at a time as the connection asks for
bytes, so a million rows cost one buffer, and the caller never has to hold them
either. A row that will not serialise fails the write with the row's number
rather than sending the rows before it — a short table reported as a successful
write is the failure worth preventing.

Reading is the launcher-shaped direction: owned rows, whole table. A struct
naming three of twenty columns is a projection rather than an error, which is
what makes it worth asking with a type at all. For tables that do not fit,
`read_table_streaming` feeding `ytsaurus_job::JobReader` is still the answer,
and now says so.

Two cluster facts came out of the same exercise, both from the Go
`vanilla-example`, which reads its jobs' stderr **after they succeed**:

- **Stderr is kept for successful jobs**, with no spec option needed.
- **Ask promptly.** `list_jobs` answers with an empty list for an operation that
  finished a while ago — the controller agent forgets its jobs, and a cluster
  with no job archive then has nothing left to say. Both examples that harvest
  stderr do it immediately after `wait_for_operation`.

`Client::list_jobs` and `Client::get_job_stderr` had no example calling either
until now; they were reachable only through the automatic failure report.

### Reading what the scheduler recorded

- **Added** `Client::job_statistics` and `Client::job_statistic_sum`, the
  built-in counterpart to `custom_statistics` / `statistic_sum`.

The two trees are stored differently, which is why they are read differently: a
custom name keeps its slash as **one key**, while a built-in statistic **nests**
by path component. The separator differs too — `$$` rather than `$` — and both
are now accepted, since that is not something a caller should have to know.

```text
custom:    {"rows/rejected" = {"$"  = {completed = {map = {sum=3}}}}}
built-in:  {time = {exec    = {"$$" = {completed = {map = {sum=744}}}}}}
```

**A local cluster reports nothing under `user_job/cpu`**, so the CPU comparison
[`docs/benchmarking.md`](../../docs/benchmarking.md) describes cannot be run
here at all. `time/exec` is what it does report, and that is what the new
`profile` example measures with.

### Pointing it at a real installation

- **Changed** `Client::from_env` to find a token the way the `yt` CLI does:
  `YT_TOKEN`, then the file named by `YT_TOKEN_PATH`, then `~/.yt/token`. A
  machine where the CLI already works now needs nothing else. The token is
  **trimmed**: `echo token > ~/.yt/token` leaves a newline, and sending that
  fails authentication with an error that never mentions a newline. An
  unreadable file means no token rather than an error — which is what it means
  on a cluster that wants none.

**Responses were already compressed** and nothing said so. `ureq`'s `gzip`
feature is on in this crate, so every request carries `Accept-Encoding: gzip`
and every answer is decompressed on the way in; the proxy honours it, including
for a streamed table read — 67.7 MiB of table arrived as 400 KiB on the wire.
Nothing in the crate would have noticed if that feature were dropped, because a
cluster answers the same either way, just larger. A new test serves one request
from a socket in-process and reads what the client actually sent, which also
pins the token header, the absence of one when there is no token, and that
parameters travel in `X-YT-Parameters` rather than a query string.

**The proxy also accepts a gzipped request body** (`Content-Encoding: gzip`),
verified on the local cluster. Compressing uploads is not implemented: it costs
a compression dependency in a crate that is linked into worker binaries and
cross-compiled to musl, and that is a trade worth making deliberately rather
than in passing.

TLS remains the one part of this that a local cluster cannot exercise: the `tls`
feature is there, on by default, and only an `https://` installation will prove
it.

### A table bigger than the program that moves it

- **Added** `Client::read_table_streaming` and `Client::write_table_streaming`,
  with the `TableReader` the first returns. The buffered pair holds a whole
  table at once, which is right for a launcher inspecting a result and wrong
  for anything the size of the data.

Both carry the same bytes as the buffered pair — a binary YSON list fragment —
so a streamed table is exactly what a job reads on fd 0, and
`ytsaurus_job::JobReader::binary` decodes it unchanged. The client sends bytes
and the job runtime decodes them; that direction stays one-way (`ytsaurus-job`
is a dev-dependency here, so the example that says so is compiled rather than
asserted).

Measured on the local cluster with `cargo run --release -p ytsaurus-client
--example streaming`, which writes a table from a generator and reads it back
both ways:

```text
Writing about 64 MiB from a generator     1242757 rows, peak RSS 2.9 MiB
Reading it back as a stream               1242757 rows counted, peak RSS 3.8 MiB
The same table, read into memory          67.7 MiB in hand, peak RSS 74.7 MiB

Streaming the 67.7 MiB table cost 1.0 MiB of peak RSS; reading it in cost 70.9 MiB.
```

Two things this gives up, both deliberate:

- **No completeness check on the streaming read.** `read_table` verifies the
  response is a whole YSON list fragment, which is the client's only defence
  against a mid-stream failure it cannot see. Streaming cannot: the point is
  not to have the whole thing. The defence moves to the decoder, where a
  fragment cut short leaves a record that does not parse — the same protection,
  applied where it still can be.
- **No retry, ever.** A reader that has been consumed cannot be sent again, so
  a streaming write is one attempt in principle and not just by policy. That
  agrees with the documented rule for heavy commands, and a transaction is what
  makes such a write safe to fail.

The `X-YT-Error` trailer question the backlog attached to this item was
rechecked rather than assumed: **`ureq` 3.3 still exposes no trailers** — the
word does not appear in its source — so the gap documented in the `http` module
stands.

Internally the transport now builds a request in one place and differs only in
how the response is consumed: into a `Vec`, as a reader, or with a reader as
the request body.

### A schema can change after the table exists

- **Added** `Client::alter_table`, the other half of `create_table`. A table
  outlives the program that made it, and the struct its rows have gains fields.

**A table with rows accepts only changes that ask less of the rows already
written.** Watched on a cluster, on a table holding two rows:

| Change | |
| --- | --- |
| add an **optional** column, anywhere in the order | allowed |
| make a required column optional | allowed |
| `strict` → non-strict | allowed |
| add a **required** column | `Cannot insert a new required column "must" into a non-empty table` |
| remove a column | `Cannot remove column "size" from a strict schema` |
| change a column's type | `Type … is modified in non backward compatible manner` |
| rename a column | read as a removal, and refused as one |
| make the table sorted | `Cannot change schema from unsorted to sorted` |
| non-strict → `strict` | `Changing "strict" from "false" to "true" is not allowed` |

Two of those deserve to be known before either becomes permanent:

- **An empty table accepts all of it.** Dropping columns, changing types,
  becoming sorted — all fine while there is nothing to break. So a migration
  rehearsed on an empty table has proved nothing about the real one.
- **A non-strict schema can never gain a named column** —
  `Cannot insert a new column "note" into non-strict schema`. Relaxing `strict`
  is a one-way door out of schema evolution.

Here the schema is a **top-level parameter**, where `create` wants it inside
`attributes`. The two commands are exact opposites on this, and only one of them
says so: `create` ignores the top-level spelling in silence.

No local compatibility checking, deliberately: error 316 carries an inner error
naming the column and the reason, and the client's error flattening — written
for failed jobs — surfaces it as one sentence. A local rule set could only add a
way to refuse something the cluster would have allowed.

Verified on the local cluster in `cargo run -p ytsaurus-client --example
schema`, which now writes rows, widens the table by deriving the schema from a
struct that gained a field, and watches the cluster refuse each incompatible
change in turn — then make the same change on an empty table.

### The rest of the Cypress tree

- **Added** `Client::list`, `copy` / `copy_replacing`, `move_node` /
  `move_replacing`, `link` / `link_replacing`, and `lock` / `lock_waiting` with
  `LockMode` and `Lock`. Between them these are what a pipeline needs to *name*
  its results: yesterday's run beside today's, a `latest` link pointing at the
  newest, and a lock so two launchers do not publish over each other.

The `_replacing` half of each pair overwrites the destination and the plain one
refuses it, which is the cluster's own default. `move_node` carries the odd name
because `move` is a Rust keyword and `client.r#move` at every call site would
cost more than four characters do.

What the cluster taught us here:

- **`list` is not sorted.** Three dated tables came back as the second, the
  third and then the first. The order is the cluster's own and means nothing.
- **A truncated listing is an attribute, not an error.** The answer comes back
  as `<incomplete=%true>[…]`, so a caller who does not look gets a listing
  quietly missing entries. `list` refuses one instead of returning it.
- **Listing a table is an error** — `"List" method is not supported` — rather
  than an empty list.
- **A link resolves to its target, including for attributes.** `latest/@type`
  answers `table`; `latest&/@type` answers `link`. The `&` is the whole
  difference between asking about the link and asking through it.
- **A lock needs a transaction**, so `lock` refuses locally rather than sending
  a request the cluster answers with `A valid master transaction is required`.
- **A waitable lock is granted later, or never.** It comes back `pending`, and
  treating that as held is the mistake the command invites; `lock_waiting` polls
  until the cluster says `acquired`. The deadline is not a nicety: a transaction
  that already holds a *snapshot* lock on the node is refused an exclusive one
  outright, but the waitable version of that request queues behind a lock only
  that transaction's own end will release. It waits forever, silently.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
cypress`, which builds a small tree of dated runs, publishes over the live table
by moving a staging one across inside a transaction, and finishes with three
transactions competing for one lock.

### Published all at once, or not at all

- **Added** `Transaction`, `Client::start_transaction`,
  `Client::start_transaction_with` and `Client::with_transaction`. Everything
  sent through a transaction is invisible to everything else until it commits,
  and is discarded if it does not — so a launcher that dies halfway leaves no
  empty table, no stale worker and no half-replaced result.
- **Fixed** `Client::exists`, which read the answer out of an `exists` key the
  cluster does not send and so failed **every** call with a decode error. It
  reads `value`, as `get` does. Nothing in the crate called it until now, which
  is how it survived two releases; a captured response is a test now.

`Transaction` derefs to a `Client` bound to it, so `tx.write_table(…)` writes
inside the transaction and `tx.start_map(…)` runs the operation inside it. The
transaction ID is stamped onto every command in one place — the transport —
because a command that forgot it would quietly do its work outside the
transaction, which is the failure a transaction exists to prevent. A command
that names a transaction itself keeps the one it named, so committing a nested
transaction commits the one meant.

**Dropping the handle aborts it.** That is what makes `?` safe inside a
transaction: a failure returns from the function, the handle drops on the way
out, and the cluster is left as it was. Only `commit` publishes.

Two facts about the cluster, both watched rather than assumed:

- **A transaction expires 30 seconds after its last ping.** Verified: one with a
  two-second timeout, left alone for four, answers a ping with `Transaction …
  has expired or was aborted`. So the handle keeps a thread pinging three times
  per timeout for as long as it lives, which is what makes a transaction usable
  around an operation that runs for an hour. Without it the feature would work
  in an example and fail on anything real.
- **Committing twice is an error**, not a no-op: `No such transaction`, which
  reads like the commit failed when it succeeded. The commit therefore carries a
  mutation ID, so a retry after a lost answer is the same commit rather than a
  second one.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
transaction`: a table visible only inside its transaction and gone after the
abort; a launcher that fails halfway and leaves nothing, with no cleanup code in
it; a map operation whose worker upload *and* output table appear only at the
commit; a command in an aborted transaction refused with `No such transaction`;
and a two-second transaction committed six seconds in, which only the ping
thread makes possible.

### A table can be told what its rows look like

- **Added** the `schema` module — `TableSchema`, `Column`, `ColumnType`,
  `SortOrder` and the `TableRow` trait — plus `Client::create_table` and
  `Client::table_schema`.
- **Added** the `derive` feature, which re-exports `#[derive(TableRow)]` from
  the new [`ytsaurus-helpers`](../ytsaurus-helpers/) crate. Off by default: it
  is a compiler plugin, and a crate that only launches operations should not pay
  to build one.

A schematised table is checked on every write. The example run against a local
cluster ends with the cluster refusing a row that left a required column out —
`Required column "size" cannot have "null" value` — which is the whole point of
saying what the rows look like.

`TableSchema::validate` catches locally what the cluster answers with error 314
a round trip later: key columns that are not a prefix, duplicate names, names
starting with `@`, `unique_keys` without a key, and a required `any`. Each
becomes one sentence naming the column.

Four protocol facts behind this, all watched on a cluster rather than taken from
the documentation:

- **A schema passed as a top-level `schema` on `create` is silently ignored.**
  The request returns 200 and a node id, and the table comes back with an empty
  weak schema. It has to go inside `attributes`. This is the single worst
  mistake the command allows, and it is why `create_table` exists rather than a
  `schema` argument on `create`.
- `create_table` deliberately **fails if the path exists**: the cluster ignores
  the attributes of a create it skips, so an `ignore_existing` version would
  quietly leave the old schema in place and report success.
- **`boolean`/`any` are the `type` spellings; `bool`/`yson` are the `type_v3`
  ones.** Those two names are the only ones that differ between the
  vocabularies, and mixing them is refused —
  `Error parsing ESimpleLogicalValueType value "bool"`.
- **Three types can never be required** — `any`, `null` and `void`. Each already
  means "there may be nothing here".

All 26 column types the crate can name were created on a local cluster and
accepted. Descending sort order was *not*: `Descending sort order is not
available in this context yet`, so `SortOrder::Descending` says as much on
itself and the example checks it rather than asserting it, so the day a cluster
enables it the run says so.

### Operations with no input tables

- **Added** `VanillaSpec`, `VanillaTask` and `Client::start_vanilla`. A vanilla
  operation runs jobs that are not a transformation of a table — a distributed
  process, a side-car computation, a job that fetches its own input — which is a
  whole category this stack could not reach.

A task says how many jobs of its kind to run and, optionally, which tables they
write; the scheduler keeps that many going. Everything else — `gang_options` for
a coordinated process, `stderr_table_path` — goes through `with_raw`.
`output_table_paths` is always sent, even empty: not sending it is a different
statement from "there are none".

Coordination between the jobs is the user's problem, and
[`ytsaurus_job::job_cookie`](../ytsaurus-job/CHANGELOG.md) is what to divide the
work by.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
vanilla`: three jobs with nothing to read, identifying themselves as 0, 1 and 2,
whose slices of a sum add up to the whole and cover every number exactly once.

### Reading back what the jobs reported

- **Added** `Client::custom_statistics` and `Client::statistic_sum`, the other
  half of [`JobStatistics`](../ytsaurus-job/CHANGELOG.md).

The tree the cluster files them in is deeper than the name suggests, and the
shape was taken from a live cluster rather than guessed:

```text
{"rows/rejected"={"$"={completed={map={count=1;max=3;min=3;sum=3}}}}}
```

The statistic's name keeps its slash as **one key** — it does not nest, so a
path-walking lookup finds nothing. Below it sit `$`, the job state, and the job
type. `statistic_sum` totals `completed` jobs across job types: a map-reduce
reporting one name from both phases gives the operation's total, while an
aborted job's work is redone by its replacement and counting it would double.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
statistics`: a job that drops rows without a `key` column reports having read
seven and rejected three, and the operation — which succeeded, with a shorter
output table and no other sign anything was dropped — reports the same.

### The worker is uploaded once, not once per launch

- **Added** `Client::upload_worker_cached`, `Client::file_from_cache`,
  `Client::put_file_to_cache` and `Client::with_file_cache`. The cluster keeps a
  file cache keyed by MD5; an unchanged binary is now found there instead of
  being re-sent, which is the slowest part of a dev loop that changes only the
  spec.
- **Added** `with_local_file_named` to all three spec builders. A cached node is
  named after its hash, so `./my_job` would find nothing to run without a
  `file_name` attribute on the path. `file_paths` entries are YSON values now
  rather than plain strings, which is what makes such attributes expressible.
- **Added** a dependency on `md5` (0.8), chosen for having no dependencies of
  its own: this crate is linked into worker binaries that cross-compile to musl
  with nothing but the Rust toolchain.

The cache defaults to `//tmp/yt_wrapper/file_storage/new_cache`, the path the
Python wrapper uses, so an installation that already expires entries there
expires ours too.

Two things the cluster settled, both now handled: `get_file_from_cache` and
`put_file_to_cache` answer with a **bare string** rather than the usual
`{path=…}` envelope, and a cache miss is an **empty string**, not an error or an
entity.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
cached_upload`: first call uploads (166 ms), second is a hit (32 ms) on the same
path, and the cached binary runs as a job — so both the `executable` attribute
and the sandbox name survive the trip through the cache.

### A transient failure no longer kills the run

A shared cluster produces failures that pass on their own — a restarting proxy,
a scheduler that has lost the master. One of those used to end the run. Light
commands are now repeated, following the
[documented rules](https://ytsaurus.tech/docs/en/api/commands#retry).

- **Added** `RetryPolicy` and `Client::with_retries`. Five attempts by default,
  with a delay that doubles from one second to ten. `RetryPolicy::none()` turns
  it off.
- **Added** `MutationId` and `Client::start_operation_with`. Every mutating
  command the client sends now carries a `mutation_id`, so a repeated request is
  deduplicated by the cluster rather than applied twice — without it, a 503 on
  the way *back* from a successful `start_operation` would leave the retry
  starting a second operation over the same tables.
- **Heavy commands are still sent once**, whatever the policy says: the
  documentation is explicit that they cannot be retried, and a transaction is
  the way to make an upload atomic.
- Retriable failures are transport errors, HTTP 429/500/502/503/504, and
  YTsaurus codes 3, 100, 105, 108, 904 and 2100 — the same set the Python
  client retries on. A retriable code is looked for throughout the error
  document, because the outer error is often a `Request retries failed` wrapper
  with the real reason nested inside. Codes that mean the request was wrong
  (500 resolve, 501 already exists) are never retried.

**A replay must admit to being one.** The cluster refuses a repeated
`mutation_id` sent without the `retry` flag — `Duplicate request is not marked
as "retry"` — rather than deduplicating it. So the flag travels with the ID:
`MutationId::as_retry()` marks a send as a replay, which is what a
crash-and-restart needs when it reuses a persisted ID.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
idempotent`: the same ID twice returns one operation, a fresh ID starts a
second. The retry classification is unit-tested, including on the exact error
document a local cluster produced when its scheduler could not reach the master.

### Reduce and sort as operations of their own

- **Added** `ReduceSpec` / `Client::start_reduce` and `SortSpec` /
  `Client::start_sort`. Reduce over an already-sorted table is one of the most
  common operation shapes, and reaching for map-reduce instead pays for a
  shuffle that has already happened. Sort is what produces the sorted table, and
  it can then be reduced again and again.

- A reduce's `key_switch` goes under **`job_io`**, not `reduce_job_io`. That is
  the map-reduce trap in the other direction — one job type, one I/O section —
  and the wrong spelling is accepted and silently ignored, leaving the reducer
  to fold every key into one group. Both spellings are now pinned by tests.

- `sort_by` is only sent when asked for: the cluster defaults it to `reduce_by`,
  and stating it turns on a sortedness check the caller did not request.

- `SortSpec` renders **`output_table_path`** — singular, and a string rather
  than a list. Sort writes exactly one table however many it reads, and the
  plural spelling every other operation uses is rejected.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
sort_reduce`: seven unsorted rows sorted (`@sorted_by` becomes `[word]`), then
reduced to four correct per-word totals. Four rows rather than one is itself the
proof that `key_switch` reached the reducer.

### The binary can upload itself

- **Added** `Client::upload_current_exe`, which uploads the running executable.
  Together with
  [`ytsaurus_job::is_inside_job`](../ytsaurus-job/CHANGELOG.md) this is the
  one-binary pattern: the same program launches the operation and runs as its
  job, so what the cluster runs is what you just built.

- **Added** `ClientError::NotAWorker`. The running executable is often not
  something a node can exec — Mach-O on macOS, dynamically linked on a
  developer's Linux — and both fail on the node minutes later with an error that
  names no cause. `upload_current_exe` reads the ELF header first (Linux,
  x86-64, no interpreter) and refuses with an error that says what to build
  instead. `upload_worker` is unchanged and stays permissive: a job command can
  legitimately be a shell script.

- **Added** the `tls` feature, on by default. Turning it off drops `rustls`,
  which drags in `ring`, which needs a C toolchain to reach musl — and a binary
  that is both launcher and job has to reach musl. With it off, an `https://`
  proxy fails with an error that names the feature rather than a confusing
  connection error. Defaults are unchanged for existing users.

  This is what lets `scripts/build-worker.sh` keep its promise of needing
  nothing but the Rust toolchain, verified by cross-compiling a worker that
  contains the whole client to static musl on macOS.

Verified end to end on the local cluster from both sides: the launcher refusing
a Mach-O binary, and the musl build of the same source uploading *itself* from
inside a Linux container and being run as the job.

### Failed operations explain themselves

`wait_for_operation` now reports *why* an operation failed. On a terminal
`failed` or `aborted` it asks the cluster which jobs failed and what each wrote
to stderr, and puts both in the error. Before this, a failed operation gave you
a state string and a trip to the web UI.

- **Added** `Client::list_jobs` and `Client::get_job_stderr`, plus the `JobInfo`
  and `JobFailure` types they return.
- **Added** `Client::with_job_diagnostics`, to turn the report off. The
  YTsaurus documentation asks that `list_jobs` not be used without an
  administrator's approval; this is the way to respect that.
- **Changed** the operation error is now the flattened message
  (`Failed jobs limit exceeded: Process terminated by signal 6`) rather than a
  truncated raw document, falling back to the raw document if the shape moves.
- **Breaking** `ClientError::OperationFailed` gained a `jobs` field. Code that
  matches the variant by name is unaffected; code that destructures every field
  needs `..`.

Collecting the report is best-effort throughout: it runs while an error is being
built, and a diagnostic that replaces the failure it was explaining would be
worse than no diagnostic.

Verified on the local cluster with the new `boom` worker, which panics on its
first row, driven by `cargo run -p ytsaurus-client --example diagnose`. The
`list_jobs` response it produced is kept as a test fixture in
`tests/fixtures/list_jobs_failed.yson`.

Two things that capture taught us, both now pinned by tests:

- `stderr_size` is a hint, not a length — the cluster reported `1` for a job
  whose stderr was several hundred bytes, so the client asks for stderr whatever
  the field says.
- The useful part of a job error is the innermost one. `User job failed` is a
  category; `Process terminated by signal 6` — a Rust panic under
  `panic = "abort"` — is the answer.

## 0.2.0

First release of this crate. Version tracks the workspace.

A thin HTTP API v4 client: enough to run a Rust worker with no Python
installation. Covers Cypress (`create`, `remove`, `exists`, `get`, `row_count`),
data (`upload_worker`, `write_file`, `write_table`, `read_table`,
`set_attribute`) and operations (`start_map`, `start_map_reduce`,
`start_operation`, `operation_state`, `wait_for_operation`), with `MapSpec` and
`MapReduceSpec` builders.

Verified against a local cluster with nothing Python on `PATH`.

Two limits are documented rather than hidden: heavy commands are not routed via
`/hosts`, and `ureq` 3.3 exposes no trailers, so a failure the proxy reports
mid-stream cannot be seen. `read_table` compensates by rejecting a response that
is not a complete YSON list fragment.
