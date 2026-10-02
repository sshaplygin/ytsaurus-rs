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
- **Breaking** `ClientError::OperationFailed` gained a `jobs` field. Code that
  destructures every field needs `..`; matching the variant by name is
  unaffected.
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
  transaction" and the keep-alive stopped. False does not prove pinging: it is
  also false when the thread never started or has panicked. It takes `&self`;
  after `detach`, probe with `Client::ping_transaction`.
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
  with `b`, and `Excluded(a)` below skips every row starting with `a`
  ([protocol reference](../../docs/protocol-reference.md#selecting-columns-and-rows-on-a-path)).
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
  No background thread and no new dependency. A heavy command may now pay one
  `/hosts` round trip mid-life (bounded by `with_hosts_timeout`, at most once
  per interval), and
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
  (`/api/v4/?path=//other`) instead of its path (`/api/v4/exists?path=//other`,
  [RFC 3986 §5.3](https://www.rfc-editor.org/rfc/rfc3986#section-5.3)); a bare
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
- A failed heavy command drops the host it used, and the next name takes over;
  likewise a proxy that refuses heavy work for its role or cannot be reached.
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

### Typed rows and table I/O

- Added `Client::write_table_rows`, from any iterator of serialisable values,
  encoded a buffer at a time as the body is sent; a row that will not serialise
  fails the write, naming the row. Added `Client::read_table_rows`, into any
  deserialisable type; a struct naming some of the columns reads just those.
- Added `Client::get_as`, which reads a node or attribute into a Rust type.
- Added `Client::read_table_streaming`, returning `TableReader`, and
  `Client::write_table_streaming`. They carry the same binary YSON list
  fragment as the buffered pair, so `ytsaurus_job::JobReader::binary` decodes
  it. The streaming read has no completeness check (a truncated fragment fails
  in the decoder) and a streaming write is never retried.
- Added `Client::job_statistics` and `Client::job_statistic_sum`, for the
  built-in statistics; both the `$` and `$$` separators are read.
- Changed `Client::from_env` to look for a token as the `yt` CLI does:
  `YT_TOKEN`, then the file in `YT_TOKEN_PATH`, then `~/.yt/token`, trimmed. An
  unreadable file means no token.

### Schemas, Cypress and transactions

- Added the `schema` module (`TableSchema`, `Column`, `ColumnType`, `SortOrder`,
  the `TableRow` trait), `Client::create_table`, which sends the schema inside
  `attributes` and fails if the path exists, and `Client::table_schema`.
  `TableSchema::validate` refuses non-prefix key columns, duplicate names, names
  starting with `@`, `unique_keys` without a key and a required `any`, naming
  the column. `SortOrder::Descending` documents that clusters refuse it.
- Added the `derive` feature, off by default, re-exporting
  `#[derive(TableRow)]` from [`ytsaurus-helpers`](../ytsaurus-helpers/).
- Added `Client::alter_table`, which sends `schema` as a top-level parameter
  and checks nothing locally; the cluster's error 316 names the column.
- Added `Client::list`, which refuses a listing marked `<incomplete=%true>`;
  `copy` / `copy_replacing`, `move_node` / `move_replacing` (`move` is a
  keyword) and `link` / `link_replacing`, where the `_replacing` form
  overwrites; and `lock` / `lock_waiting` with `LockMode` and `Lock`. `lock`
  without a transaction is refused locally, and `lock_waiting` polls until
  `acquired`, with a deadline.
- Added `Transaction`, `Client::start_transaction`,
  `Client::start_transaction_with` and `Client::with_transaction`. A
  `Transaction` derefs to a `Client` bound to it (`tx.write_table(…)`,
  `tx.start_map(…)`), and every command it sends carries the id unless the
  command names a transaction itself. A thread pings three times per timeout.
  Only `commit` publishes, under a mutation ID; dropping an uncommitted handle
  aborts.
- Fixed `Client::exists`, which failed every call by reading the key `exists`
  instead of `value`.

### Operations

- Added `VanillaSpec`, `VanillaTask` and `Client::start_vanilla`.
  `output_table_paths` is always sent, even empty; anything else, such as
  `gang_options` or `stderr_table_path`, goes through `with_raw`.
- Added `Client::custom_statistics` and `Client::statistic_sum`, which totals
  the `completed` jobs across job types.
- Added `Client::upload_worker_cached`, `Client::file_from_cache`,
  `Client::put_file_to_cache` and `Client::with_file_cache`: an unchanged worker
  is found in the cluster's MD5-keyed file cache instead of re-sent. The cache
  defaults to `//tmp/yt_wrapper/file_storage/new_cache`, the Python wrapper's.
  Added `with_local_file_named` to all three spec builders, since a cached file
  is named by its hash; `file_paths` entries are YSON values. Added a
  dependency on `md5` 0.8.
- Added `RetryPolicy` and `Client::with_retries`: light commands are retried,
  five attempts by default, the delay doubling from one second to ten;
  `RetryPolicy::none()` turns it off. Retried: transport errors, HTTP
  429/500/502/503/504, and YTsaurus codes 3, 100, 105, 108, 904 and 2100
  anywhere in the error document (the outer error is often a
  `Request retries failed` wrapper), as the Python client does; never 500
  (resolve) or 501 (already exists). Heavy commands are sent once.
- Added `MutationId` and `Client::start_operation_with`; every mutating command
  carries a `mutation_id`, and `MutationId::as_retry()` marks a replay of a
  persisted one.
- Added `ReduceSpec` / `Client::start_reduce` and `SortSpec` /
  `Client::start_sort`. A reduce's `key_switch` goes under `job_io`. `sort_by`
  is sent only when set; the cluster defaults it to `reduce_by`. `SortSpec`
  sends a single `output_table_path`.
- Added `Client::upload_current_exe`: with `ytsaurus_job::is_inside_job`, one
  binary launches the operation and runs as its job. Added
  `ClientError::NotAWorker`, which refuses an executable that is not a Linux
  x86-64 ELF without an interpreter, saying what to build instead.
  `upload_worker` still accepts anything, a shell script included.
- Added the `tls` feature, on by default. Without it an `https://` proxy fails
  with an error naming the feature, and a binary that is both launcher and job
  builds for musl with only the Rust toolchain.

### Failed operations explain themselves

- Changed `wait_for_operation` to put the failed jobs and their stderr into
  the error when an operation ends `failed` or `aborted`, best effort. Stderr is
  fetched whatever `stderr_size` says.
- Added `Client::list_jobs`, `Client::get_job_stderr`, `JobInfo` and
  `JobFailure`, and `Client::with_job_diagnostics` to turn the report off: the
  documentation asks that `list_jobs` be used only with an administrator's
  approval.
- Changed the operation error to the flattened message (`Failed jobs limit
  exceeded: Process terminated by signal 6`), falling back to the raw document.

### Examples

- Added the cluster examples `diagnose` (with the `boom` worker), `idempotent`,
  `sort_reduce`, `cached_upload`, `statistics`, `vanilla`, `schema`,
  `transaction`, `cypress`, `streaming`, `profile` and `rich_path`. Each checks
  its own result.

## 0.2.0

First release of this crate. Version tracks the workspace.

A thin HTTP API v4 client, enough to run a Rust worker with no Python
installation. Covers Cypress (`create`, `remove`, `exists`, `get`, `row_count`),
data (`upload_worker`, `write_file`, `write_table`, `read_table`,
`set_attribute`) and operations (`start_map`, `start_map_reduce`,
`start_operation`, `operation_state`, `wait_for_operation`), with `MapSpec` and
`MapReduceSpec` builders. Verified against a local cluster with nothing Python
on `PATH`.

Limits: heavy commands are not routed via `/hosts`, and `ureq` 3.3 exposes no
trailers, so a failure the proxy reports mid-stream cannot be seen; `read_table`
rejects a response that is not a complete YSON list fragment instead.
