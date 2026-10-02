# Protocol reference

How the YTsaurus protocol and cluster behave, where this repository depends on it.
Each fact names its evidence. "Observed" means on a local Docker cluster; "a real installation" means a multi-node one.

## Binary YSON markers

| Marker | Type | Payload |
| --- | --- | --- |
| `0x01` | string | zigzag varint length (`sint32`), then that many raw bytes |
| `0x02` | int64 | zigzag varint (`sint64`) |
| `0x03` | double | 8 bytes little-endian |
| `0x04` / `0x05` | boolean | false / true |
| `0x06` | uint64 | unsigned varint |
| `0x23` `#` | entity | none |
| `< >` `[ ]` `{ }` `=` `;` | attributes, list, map, key/value separator, item separator | literal ASCII |

## Descriptors

Output table `k` is fd `3k + 1`: table 0 is fd 1, table 1 is fd 4, table 2 is fd 7. A job's environment carries `YT_FIRST_OUTPUT_TABLE_FD=1`.

## The job environment

Observed in a job: `YT_JOB_ID`, `YT_OPERATION_ID`, `YT_JOB_COOKIE`, `YT_JOB_INDEX`, `YT_TASK_JOB_INDEX`, `YT_START_ROW_INDEX`, `YT_FIRST_OUTPUT_TABLE_FD`, `YT_NODE_HOST`, `YT_POOL_TREE`, `YT_COLLECTIVE_ID`/`YT_COLLECTIVE_MEMBER_RANK`, `YT_JOB_PROXY_{GRPC,HTTP}_SOCKET_PATH`.

- `is_inside_job()` tests `YT_JOB_ID`, as Go's `mapreduce.InsideJob` does.
- argv is exactly the spec's command, `["./selfrun"]`. Role dispatch by argv (`wordcount map`) works if the call site passes the role.

## Custom job statistics

- A job writes them to fd 5 as a YSON list fragment holding one map, `{"rows/read"=7};`, as the Python wrapper's `write_statistics` does. At most 128 names per job.
- Filed under `progress/job_statistics/custom`. Observed:

  ```text
  {"rows/rejected"={"$"={completed={map={count=1;max=3;min=3;sum=3}}}}}
  ```

  The name is one key, slash included; below it `$`, job state, job type, aggregate.
- `Client::statistic_sum` totals the `completed` state across job types; an aborted job's work is redone by its replacement.
- `JobStatistics` writes fd 5 only when `is_inside_job()`; in a launcher fd 5 may be a socket.
- Built-in statistics are stored differently, hence `Client::job_statistic_sum`: the name nests by path component (`time` → `exec`) and the state separator is `$$`. A local cluster reports nothing under `user_job/cpu`; it reports `time/exec`.

## Table schemas

A schema is a YSON list of column dicts, with `strict` and `unique_keys` as attributes on the list:

```text
<strict=%true;unique_keys=%false>[{name=host;required=%true;sort_order=ascending;type=utf8};…]
```

All observed:

- On `create`, the schema goes inside `attributes`. **A top-level `schema` gets 200 and a node id and is ignored**, leaving an empty weak schema. `set //table/@schema` is refused.
- `create` with `ignore_existing` ignores the attributes too, so `create_table` fails on an existing path.
- `boolean`/`any` are the `type` spelling, `bool`/`yson` the `type_v3` one; only these differ. Mixing is refused: `Error parsing ESimpleLogicalValueType value "bool"`.
- `any`, `null` and `void` cannot be required. `required` defaults to `%false`.
- A read-back has `required`, `type` and `type_v3` on every column, keys alphabetical. Unknown keys are dropped silently.
- Key columns must be a contiguous prefix. `unique_keys=%true` needs a key column. Names must be non-empty, ≤256 bytes, unique, and not start with `@`.
- `sort_order=descending` is refused on this build: `Descending sort order is not available in this context yet`, gated by `//sys/@config/enable_descending_sort_order`.

`TableSchema::validate` checks these locally, naming the column, instead of error 314 from a create.

### Changing a schema

`alter_table` takes `schema` as a top-level parameter. On a table with rows, a change that asks more of existing rows fails with error 316, `Table schemas are incompatible`; the inner error names the column, so the client has no local rules. Observed:

| Change | On a table with rows |
| --- | --- |
| add an optional column, at any position | allowed |
| required → optional | allowed |
| strict → non-strict | allowed |
| add a required column | `Cannot insert a new required column "must" into a non-empty table` |
| remove (or rename) a column | `Cannot remove column "size" from a strict schema` |
| change a type | `Type of "" field is modified in non backward compatible manner` |
| unsorted → sorted | `Cannot change schema from unsorted to sorted` |
| non-strict → strict | `Changing "strict" from "false" to "true" is not allowed` |

- An empty table accepts all of them.
- A non-strict schema can never gain a named column: `Cannot insert a new column "note" into non-strict schema`. Relaxing `strict` cannot be undone.
- A failed `write_table` leaves its upload transaction briefly holding an exclusive lock; until it clears, the next command on that path fails with the concurrent-transaction error.

## Transactions

Commands: `start_transaction`, `commit_transaction`, `abort_transaction`, `ping_transaction` (`start_tx` is not registered). `start_transaction` answers `{transaction_id="3-5bc70-10001-387a"}`; the other three take `transaction_id` and answer `{}` or a commit timestamp. Other commands join through a `transaction_id` parameter, stamped in `Transport::in_transaction`.

Observed:

- A transaction expires 30 000 ms after its last ping (default `@timeout`; `Transaction` reads it to pick its ping interval). A 2 s timeout left for 4 s: `Transaction … has expired or was aborted`.
- A commit is not idempotent: a second one fails with `No such transaction`. Commits carry a mutation ID.
- An abort of a committed or nonexistent transaction answers `{}`, so aborting from `Drop` is safe.
- `get_operation`, `list_jobs`, `get_job_stderr` and the file-cache commands accept `transaction_id` and ignore it. The client omits it for the scheduler and job commands listed in `NO_TRANSACTION` (`http.rs`; no `TTransactionalOptions`), in case a version refuses unknown parameters: `Transaction` derefs to `Client`, so `wait_for_operation` runs inside one. `start_operation` is not listed, since an operation inside a transaction keeps its output invisible until commit. A command naming a transaction itself keeps its own.
- `start_transaction` under a transaction makes a nested one.
- An aborted or expired transaction fails with `No such transaction` nested inside `Error resolving path …`.
- `ping_ancestor_transactions=%true` is accepted and not needed: each handle pings its own transaction.

### Handing a transaction to another process

`detach` / `attach_transaction` (#13). Observed:

- `@timeout` is in milliseconds and is `Int64`: `get #<id>/@timeout` on a 30 s transaction answers `{"value"=30000;}` in text YSON. `Transaction::attach` also accepts `Uint64`.
- The attribute is the configured timeout, not the time remaining. `attach` pings before returning; otherwise a handoff longer than `timeout × 2/3` yields a handle whose first ping arrives after expiry.
- Three absences, three errors. `transaction_is_gone` looks for both "gone" spellings in the whole document:

  | Case | Error |
  | --- | --- |
  | garbage id (`1-2-3-4`) | `cluster error 1: Unknown cell tag 0`, rewritten by `attach_failed` |
  | expired or aborted id, as an object | `Error resolving path #<id>/@timeout` wrapping `No such object <id>` |
  | the same id, pinged | `No such transaction`, code 11000 |

- A detached transaction differs from a held one only in the pings that stop. `crates/ytsaurus-client/tests/transaction_lifecycle.rs` checks that against an in-process stub, timing `detach`'s join.
- `detach` waits out an in-flight ping only up to a 30 s timeout. Its join is bounded at five seconds; a ping's budget is `clamp(interval / 2, 1 s, 120 s)` with `interval` = `max(timeout / 3, 1 s)`. The master keeps the requested timeout verbatim (`#<id>/@timeout` read back `3600000`, `30000`, `20000`; budgets 120 s, 5 s, 3.3 s), so above the default a stalled ping can outlive `detach` and restart the cluster's clock. `transaction.rs` unit tests pin both directions; the one asserting the wait ends is the only test that fails on a `drop(alive)` in the ping thread.

## Commands and verbs

- The proxy documents the verb rule: *"If the command has an input data stream, then PUT. If the command is mutating, then POST. Otherwise GET."* `yt/yt/client/driver/driver.cpp` declares both per command, `REGISTER_ALL(command, name, inDataType, outDataType, isVolatile, isHeavy)`: `write_table` (`Tabular` in, volatile) is PUT, `create` (volatile) POST, `get` and `read_table` GET.
- `isVolatile` and `isHeavy` are the two bits `Repeatable` encodes; `Client::raw_command` defaults to `Repeatable::Never`.
- `whoami` is not an API v4 command: Go's `WhoAmI` uses `newAuthCall`, not `/api/v4/`. The doctest and the `raw` example use `get_supported_features` (`Null` in, `Structured` out, non-volatile, non-heavy).
- `check_permission` and `get_supported_features` are registered and not modelled. `list_operations` and `read_file` are modelled (`Client::read_file`, `Client::read_file_streaming`).
- `get_supported_features` answers `{features=…}`. Observed keys: `compression_codecs` (71 of them), `erasure_codecs`, `node_flavors`, `operation_statistics_descriptions`, `primitive_types`, `query_memory_limit_in_tablet_nodes`, `require_password_in_authentication_commands`, `structured_web_json`, `user_tokens_metadata`.

### Reading and writing files

- `read_file` streams and `write_file` takes a chunked body: a 4 MB round trip through `Client::raw_command_streaming` and `raw_command_upload` held the file in neither direction.
- The buffered `read_file` compares the body with `@uncompressed_data_size`: a truncated body looks like a shorter file, and the proxy's verdict is in a trailer `ureq` cannot read. On `compression_codec=zlib_6` nodes of 1 000 000 bytes, `@compressed_data_size` was 4 214 for cycling `i % 256` bytes, 999 for zeros and 1 000 324 for `os.urandom`. `@file_size` does not exist (`Attribute "file_size" is not found`).
- A rich path does nothing to a file read. On a 1000-byte file, `<lower_limit={offset=0};upper_limit={offset=10}>//tmp/f` reads all 1000 bytes; `//tmp/f[#0:#10]` also returns 1000, then fails the size check because `//tmp/f[#0:#10]/@uncompressed_data_size` does not parse (`Error reading parameter /path: Unexpected token "/" of type "slash"`). Files are sliced by the `offset` and `length` parameters; selection is #12.

### Response size limits

- `ureq`'s `limit()` bounds transferred bytes, not memory: `BodyWithConfig::do_build` puts the gzip decoder on top of a `LimitReader`. Measured: a `read_file` of 600 MiB of zeros arrived in 611 522 wire bytes, and `.limit(536870912)` returned all 629 145 600. The memory cap, `http::CapReader`, sits above the decoder.
- A wire limit is still needed underneath: a chunked stream of empty deflate stored blocks (`00 00 00 ff ff`) decodes to nothing, and `flate2` loops inside one `read`.
- The wire limit cannot be the memory cap: 4 096 incompressible bytes gzip to 4 119. `http::wire_budget` is zlib's `deflateBound` plus the gzip wrapper.
- The memory cap is not a process budget: `read_to_end` doubles a `Vec` and copies, so both buffers are resident during a copy (about 1.5× where the allocator cannot extend in place). Measured (release build, local listener): a read of 536 870 911 bytes peaked at 544 178 176 bytes resident, a 600 MiB read refused by the 512 MiB cap at 611 385 344. Quote the cap as what is held, never as what to size a container for.

## Authentication and compression

- The token is looked up as the `yt` CLI does: `YT_TOKEN`, then `YT_TOKEN_PATH`, then `~/.yt/token`, and trimmed (`echo token > ~/.yt/token` leaves a newline that fails authentication).
- `ureq`'s `gzip` feature is on, so every request sends `Accept-Encoding: gzip`: a 67.7 MiB table read came back as 400 KiB on the wire. `tests/request_shape.rs` pins it.
- The proxy accepts a gzipped request body (`Content-Encoding: gzip`). Uploads are not compressed: that adds a dependency to musl workers, which is a human's call.
- A local cluster accepts any token; the file lookup is unit-tested.

### Redirects

- A heavy read through a control proxy gets a cross-host `307` to a data proxy ([Where a heavy command goes](#where-a-heavy-command-goes)). `ureq` drops `Authorization` on it (`RedirectAuthHeaders::Never`), giving `cluster error 111: Client is missing credentials`; `redirect_auth_headers(RedirectAuthHeaders::SameHost)` does not cover cross-host.
- `ureq` follows nothing (`max_redirects(0)` on every transport); the client decides:

  | Redirect | Result |
  | --- | --- |
  | same origin | followed, token included |
  | other origin (scheme, host or port), with a token | refused: `ClientError::Redirected`, naming the target |
  | other origin, no token, with data | refused: `RedirectRefusal::Payload` |
  | other origin, no token, no data (`Content-Length: 0` included) | followed (a bodiless `POST` carries no data) |
  | body not resendable (`Transport::upload`'s reader: `write_table_rows`, `raw_command_upload`) | refused anywhere; following would write no rows and return `Ok(())` |
  | longer than `MAX_REDIRECTS` | refused as a loop |

- Method and body are kept on every hop, whatever the status (307/308 require it; a v4 command's verb is fixed): a bodiless `POST create` follows a balancer's `301`, a same-origin `write_table` resends its rows.
- The chain shares the command's one deadline. A fresh `timeout_global` per hop would allow `(MAX_REDIRECTS + 1)×` the requested time: 22 minutes at the default two, on an `exists`.
- `Location` is resolved against the request's address (RFC 3986 §4.2); a reference with no path (`?path=…`, `#frag`) keeps the request's path (§5.3).
- Tested offline in `crates/ytsaurus-client/tests/redirect_credentials.rs` (a local cluster redirects nothing). A stub must read the whole request (head and body, `Content-Length` or chunked) before answering, as `request_shape.rs` does; otherwise `ureq` reports a broken pipe (a small body survives on macOS, not on a Linux runner).
- The `HEAVY` list in `http.rs` only decides whether a refusal says "go to a heavy proxy". It is the cluster's `isHeavy` bit, covering commands only `raw_command` can send, and must be reconciled with `Repeatable::Heavy` when #38 merges (marked in the source).

### TLS

- Default trust is the Mozilla bundle, `webpki-roots`, via `ureq`'s `rustls` feature. A bare host name in `YT_PROXY` means `https://`. Behind a corporate CA this failed with `invalid peer certificate: UnknownIssuer` where `curl` (OS trust store) worked.
- `YT_CA_BUNDLE` names a PEM file; the `platform-verifier` feature trusts the OS store; the bundle wins when both are set. A bundle with no certificates is refused, naming the file. Verified on a real installation with a three-deep, self-signed chain (#29); a local cluster is plain HTTP.
- `ureq::tls::parse_pem` only splits and base64-decodes (its `Certificate` is unvalidated), and `rustls`' `add_parsable_certificates` silently drops what it cannot parse: a PKCS#7 `.p7b` under a `BEGIN CERTIFICATE` label gave an empty root store and `UnknownIssuer`. `http::is_x509` checks the DER skeleton (`SEQUENCE { SEQUENCE, SEQUENCE, BIT STRING }` and the head of the `TBSCertificate`), and one bad block refuses the file. A `ContentInfo` fails at its first member, an OBJECT IDENTIFIER where the `tbsCertificate` sequence belongs.
- The bundle is `stat`ed before opening: a FIFO blocks forever, and `Client::new` is infallible.
- A rejected certificate is `ureq::Error::Io` of kind `InvalidData` wrapping a `rustls::Error`, rendered `invalid peer certificate: <CertificateError>`; retried as a transport failure it cost five attempts and ~15 s. `UnknownIssuer` and the name mismatch depend only on this client's roots and URL, and are not retried.
- Match the rendering, not the variant name. `Display for CertificateError` writes prose for context-carrying variants and `Debug` otherwise: `UnknownIssuer` by name, a hostname mismatch as `certificate not valid for name "…"; certificate is only valid for …`. The webpki verifier builds only `NotValidForNameContext`, so `NotValidForName` alone matches nothing in the default build; both spellings are listed.
- The rest stays retriable: `rustls-platform-verifier` maps a failed revocation lookup or unreadable trust store to `Other(…)` under the same prefix, and `Expired` or `Revoked` belongs to one fleet member that may be renewed mid-rotation.

## The operation lifecycle

Observed:

- `abort_operation` takes `operation_id` and an optional `abort_message` (added to the error document under `Operation aborted by user request`) and answers `{}` in ~350 ms, by which time the operation is `aborted`; `aborting` is never seen.
- Abort is not idempotent: the scheduler drops the operation once the first abort is accepted, and then answers code 200, `No such operation`. The rule is "the scheduler has dropped it", not "it is terminal": an operation that finished by itself can be aborted for the short while it is kept.
- Never send an abort under a mutation ID: the master's mutation cache does not cover scheduler commands, and a flagged retry is answered `No such operation`. `Repeatable::Never`.

### Suspend, resume, complete, update

Observed:

| Command | Repeat | Notes |
| --- | --- | --- |
| `suspend_operation` | idempotent, `{}`; retried on that basis, not under a mutation ID | state stays `running`; the cluster sets a separate `suspended` attribute, read by `Client::operation_suspended` |
| `resume_operation` | refused when not suspended: code 201, `Operation is in "running" state` | |
| `complete_operation` | second call: code 200, `No such operation` | ends as `completed`, so output is published and a waiting launcher sees success |
| `update_operation_parameters` | assigns; repeated freely | parameters go in the header; answers with Content-Length: 0 |

- Polling `operation_state` alone never shows a pause. Once the scheduler has let go, all four answer `No such operation`.
- `update_operation_parameters`: the registry declares its input `null`; the command reference says "structured". A top-level `{weight=2.5}` lands in every pool tree, at `runtime_parameters/scheduling_options_per_pool_tree/<tree>/weight`. An empty `parameters={}` gets 200 and changes nothing; the client refuses it.

### Finding an operation again

Observed:

- `get_operation` accepts `operation_alias` only with `include_runtime=%true`, otherwise *"Operation alias cannot be resolved without using runtime information"*. A stale alias falls through to `//sys/operations_archive/operation_aliases`, absent locally. An alias is a spec field starting with `*`.
- `attributes=[]` gets `{}`. Omitting it gets everything: 119 KB for a one-job vanilla operation, mostly spec and progress.
- `list_operations` answers a flat multi-key document: `{operations=[…]; incomplete=%false; pool_tree_counts={}; …; failed_jobs_count=0}`. `progress` carries `job_statistics` beside `job_statistics_v2`.
- `list_operation_events` answers a bare list, empty without an operations archive (as locally). Only the empty list has been seen; the parser also reads `{events=[…]}` and refuses other shapes.
- A sorted merge needs no `merge_by`: two tables sorted by `host`, merged with `mode=sorted`, complete with output `sorted_by=[host]`. `start_merge` no longer refuses it.
- `get_job` answers unwrapped with `job_id`; `list_jobs` wraps in `{jobs=[…]}` with `id`. One parser reads both.
- `get_job_input` never answers for a vanilla job: 30 s, zero bytes.

## Appending to a table

Observed:

- `<append=%true>` is a path attribute: `{path=<append=%true>"//tmp/t"}`. **Sent as a sibling parameter it is ignored and the table is replaced, with a 200.** `TablePath` builds it; a wire-level test pins the shape.
- A bare path and `<append=%false>` both replace.
- The table must exist; otherwise `Error getting basic attributes of user objects`.
- A sorted table stays sorted: a key smaller than the last is refused with error 301, `Sort order violation: [0#9] > [0#1]`.
- Rewriting in `k` pieces sends `(k+1)/2` times the rows: 6.5× for 12 ([benchmarking.md](benchmarking.md)).
- Appends take a shared lock, replaces an exclusive one. Four concurrent appends all land; four concurrent replaces leave one winner and three `Cannot take "exclusive" lock` failures.
- An append of zero rows is a no-op; a write of zero rows truncates the table.
- A reader never sees a partial append: `@row_count` changes when the upload transaction commits.

## Selecting columns and rows on a path

`columns` and `ranges` are path attributes too. All observed; checked by `examples/rich_path.rs`.

- **A read selection on a write is ignored and the whole table replaced, with a 200**, in both spellings: `write_table_rows("//tmp/t[#0:#2]", rows)`, and `ranges` as a typed attribute (three rows replaced by one). `TablePath::write_refusal` refuses both.
- An unknown name in `columns` gets 200 with the key absent from every row.
- `columns=[]` gets 200 and one empty map per row, and composes with a range: `<columns=[];ranges=[{lower_limit={row_index=0}; upper_limit={row_index=2}}]>` gave two empty maps, the same range with `key` bounds three. It counts a range's rows with no column bytes, which `@row_count` cannot, so the client sends it. (Once refused by analogy with `update_operation_parameters({})`; that is a no-op mutation, this a correct read.)
- A negative `row_index` is clamped to 0: `{lower_limit={row_index=-5}}` returned all five rows of a five-row table, `-5..2` rows 0 and 1, `{upper_limit={row_index=-2}}` none. ("`-5..0` is answered 200 and no rows", recorded earlier, was `upper_limit=0`.) The client refuses negative bounds anyway.
- A backwards range gets 200 and no rows in either selector (`{lower_limit={row_index=5};upper_limit={row_index=3}}`, `{lower_limit={key=[3]};upper_limit={key=[1]}}`). The client refuses both.
- The client sends `path` as a YSON string node with attributes outside, `<columns=[n]>"//tmp/t{k}"`. A JSON-parameter `curl` sends the flat text `<columns=[n]>//tmp/t{k}`, where the string's `{k}` wins instead of the attribute. Reproduce the client's shape with `-H 'X-YT-Header-Format: <format=text>yson'` and `{path=<columns=["n"]>"//tmp/t{k}";output_format=json}`.
- When the attribute and the string spell the same kind of selection, the attribute wins silently, at 200; different kinds compose. On a table `k,n`, YSON shape:

  | Sent | Read |
  | --- | --- |
  | `<columns=["n"]>"//tmp/t{k}"` | column `n` |
  | `<ranges=[…0:2]>"//tmp/t[#3:#5]"` | rows 0–1 |
  | `<columns=["k"]>"<columns=["n"]>//tmp/t"` | column `k` |
  | `<columns=["n"]>"//tmp/t[#3:#5]"` | rows 3–4, only `n` |
  | `<ranges=[…0:2]>"//tmp/t{k}"` | rows 0–1, only `k` |
  | `"<columns=["n"]>//tmp/t"` | column `n` |
  | `<ranges=[…0:2]>"<columns=["n"]>//tmp/t"` | rows 0–1, only `n` |

  No combination gets a 400. The client refuses a doubled kind, sends the pairing, and refuses a string opening with `<…>`, whose attribute it cannot parse. (A recorded "two blocks → 400, *does not start with a valid root-designator*" was flat text, which this client cannot send.)
- A `uint64` key column takes `{exact={key=[42]}}` and `{exact={key=[42u]}}` alike. For ranges, `Key::from(i64)` stops at `i64::MAX`: the row keyed `18446744073709551615u` came back only via `yson_build::uint`.
- `key` and `key_bound` compare a short key differently. `key` compares the row's whole key component-wise, shorter tuple smaller when equal so far. `key_bound` truncates the row's key to the bound's length, so every row sharing the prefix compares equal. On a table keyed `(host, path)` holding `(a,/x) (a,/y) (b,/x) (b,/y) (c,/x)`:

  | Sent | Rows back |
  | --- | --- |
  | `{key=[a]}` … `{key=[b]}` | `(a,/x) (a,/y)` |
  | `{key=[a]}` … `{key_bound=["<=";[b]]}` | `(a,/x) (a,/y) (b,/x) (b,/y)` |
  | `{key_bound=[">";[a]]}` | `(b,/x) (b,/y) (c,/x)` |
  | `{exact={key=[a]}}` | `(a,/x) (a,/y)` |

  `a..b` and `a..=b` differ by a whole prefix group, and `>` on a prefix skips all of its rows.
- A range with `key` on one side and `key_bound` on the other is accepted (undocumented); `keys(a..=b)` sends that.

## Tracing

From the cluster's source (the HTTP reference does not mention it), then observed through the response's `X-YT-Trace-Id`, which carries the adopted trace id.

- The proxy joins a caller's trace through the W3C `traceparent` header, `00-<32 hex trace>-<16 hex span>-<2 hex flags>`, parsed by `TryParseTraceParent` in `yt/yt/core/http/helpers.cpp`. Flags: bit 0 sampled, bit 1 debug.
- All three official clients send it: C++ `FormatTraceParentHeader` (hard-coded `00-…-01`), Go `injectTracing`, Python `generate_traceparent` (on every request, with its own id).
- The version may be omitted (`4bf92f35…-00f067aa0ba902b7-01`, as the Go SDK sends) and uppercase hex is accepted. This client parses both and sends lowercase, four groups.
- A malformed header is ignored (200, a made-up trace id); `TraceContext::parse` refuses one.
- The cluster's GUID is the trace id's four 32-bit groups, dashed, without leading zeros; an all-zero group keeps one digit. `TraceContext::yt_trace_id` reproduces it; its test cases:

  | Sent in `traceparent` | Echoed in `X-YT-Trace-Id` |
  | --- | --- |
  | `4bf92f3577b34da6a3ce929d0e0e4736` | `4bf92f35-77b34da6-a3ce929d-e0e4736` |
  | `00000001000000020000000300000004` | `1-2-3-4` |
  | `00000000000000010000000000000002` | `0-1-0-2` |

- `X-YT-Correlation-Id` (request) and `X-YT-Request-Id` / `X-YT-Proxy` (response) are the documented way to find a request in the proxy log. Neither is sent or read (no method returns response headers).

## Where a heavy command goes

Observed on a real installation ([#30](https://github.com/sshaplygin/ytsaurus-rs/issues/30)); docs and source cited per fact.

- A control proxy does not serve heavy requests; the rule is `TContext::TryRedirectHeavyRequests` in [`yt/yt/server/http_proxy/context.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/server/http_proxy/context.cpp):

  | Request | Answer |
  | --- | --- |
  | heavy with input data: `write_table`, `write_file` | 503 + `Retry-After: 60`, `Control proxy may not serve heavy requests with input data` |
  | heavy without: `read_table`, `read_file`, `get_job_input`, `get_job_stderr` | 307 to a data proxy |
  | heavy read, no data proxy available | 503, `There are no data proxies available` |
  | any of them with `X-YT-Suppress-Redirect` | served by the control proxy |

  The split is the registry's `inDataType` column.
- This file recorded the refusal as an HTTP 200; the source says 503. Only the error string was observed: `ClientError::Cluster` renders `{command}: cluster error {code}: {message}`, without the status.
- The documentation gives each half: the [`/hosts` section](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts) says "When you try to execute a heavy command, light proxies return code 503"; the [return-code table](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#return_codes) says "307 — Redirecting heavy queries from light to heavy proxies".
- Only the role `control` refuses: `TCoordinator::CanHandleHeavyRequests` is `Role != "control"`, so `default` serves heavy commands.
- A balancer fronts the control proxies, so every upload through it reaches one; with `YT_PROXY` set to an address from `/hosts`, the same examples passed.

### `/hosts`

- `/hosts` answers a JSON list of bare host names, `["n0008-sas.cluster-name", …]` ([HTTP proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)), "ordered by load … the very first proxy in the resulting list is the least loaded" ([reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)); `TCoordinator::ListProxies` shuffles the better half. No scheme and usually no port: both come from the configured address.
- The `data` role default is undocumented: `default_role_filter`, a coordinator config parameter defaulted in `TCoordinatorConfig::Register` to `NApi::DefaultHttpProxyRole`, `"data"` in [`yt/yt/client/api/public.h`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/api/public.h). An operator can change it; the client validates what it gets. `?role=`, `/hosts/all` (the only form listing banned and dead proxies) and the plain-text form (exact `Accept: text/plain`) are also source-only.
- The [proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload): "A good strategy is to re-query the `/hosts` list every minute or every few queries and change the current proxy to which queries are made." The client keeps the answer as a pool (`Transport::base_for`); each heavy command picks a member at random, and re-asks first, lazily, when the answer is older than `with_host_list_refresh_interval` (one minute by default), as the C++ `THostManager` does. A failed refresh keeps the previous answer for another interval; an empty answer expires after one interval too. This replaced the ask-once pin recorded in [sdk-comparison.md](sdk-comparison.md) (#40).
- A failed heavy command drops its host from the pool; falling back to the configured address would reach a control proxy (#30). Only an empty pool falls back, for `HOSTS_RETRY_AFTER`; a refresh restores dropped hosts. The drop uses `retry::attributable_to_the_host`, not `worth_asking_again`: they differ on a rejected certificate, and `NotValidForName` is about one host (#40). A proxy refusing heavy work for its role is also the host's fault, which `is_retriable` cannot express; hence three predicates.
- An empty or absent list means the configured address serves everything. A loopback cluster is not asked, since it would publish an address behind the port mapping or tunnel (reasoning only; no local `/hosts` answer was captured).

### Which names from `/hosts` are used

The documentation does not say which hosts may receive the token (`Authorization: OAuth`). The default rule: same domain as the configured address, its scheme and port, no `://`, `/`, `@` or whitespace, and brackets only around an IPv6 literal (`ureq` 3.3 passes `[not.an.ip]` to the resolver).

- The domain rule is a typo guard: whoever can steer `/hosts` already sees the token, and with no public-suffix list it trusts every tenant of a hosting platform. `Client::with_heavy_proxies_in` is a boundary; `Client::with_heavy_proxies_anywhere` removes the rule.
- A dotless configured name (`YT_PROXY=hume`) must appear as a non-leftmost label of the discovered name; before that, `["n0008-sas.hume.yt.example.net"]` was refused in full. It needs `YT_PROXY_SUFFIX`, since `https://hume` resolves only with a resolver search list.
- `with_heavy_proxies_under` adds named domains to the configured one: a managed installation answered `/hosts` with 79 heavy proxies in another zone, and the default rule refused all of them (`Control proxy may not serve heavy requests with input data`).
- The Go SDK does not filter `/hosts` (`listHeavyProxies` returns it verbatim, `proxy_set.go` adds every name); `with_heavy_proxies_anywhere` matches it.
- The examples use only `Client::from_env`, so per-cluster settings have environment variables, inert when unset: `YT_PROXY_SUFFIX`, `YT_HEAVY_PROXY_DOMAINS`, `YT_HEAVY_PROXIES_ANYWHERE`, `YT_FILE_CACHE`.

## Connections

- Read every response body, or `ureq` does not pool the connection. Ignoring answers left 11 623 sockets in `TIME_WAIT` after a few seconds of table writes; reading and discarding took 23 % off a small write.

## Jobs, listed and read

Observed:

- Stderr is kept for successful jobs too, with no spec option.
- `list_jobs` answers an empty list for an operation finished a while ago: the controller agent drops its jobs and a local cluster has no job archive (`get_job_stderr` on an old job says `Job archive is unavailable`). Harvest right after `wait_for_operation`.
- `list_jobs(op, None, limit)` lists every state; the failure report passes `Some("failed")`.

## Streaming table I/O

- The proxy accepts a chunked request body for `write_table`, so a table can be written from a `Read`.
- `ureq` 3.3 caps `read_to_vec` at 10 MB by default and leaves a reader uncapped. The buffered path enforces its own limit ([Response size limits](#response-size-limits)), the streaming path none.
- `ureq` 3.3 exposes no trailers (checked in its source), so the mid-stream `X-YT-Error` trailer is unreadable; `read_table` checks completeness instead, and a truncated stream fails in the decoder.
- Measured: writing 64 MiB from a generator and streaming it back cost 1.0 MiB of peak RSS; `read_table` on the same table cost 70.9 MiB.

## Cypress: naming and locks

Observed:

- `list` is not sorted: three dated tables came back second, third, first.
- A truncated listing (also from `max_size`) is `<incomplete=%true>[…]`, not an error. `Client::list` refuses one.
- Listing a non-map node is error 103, `"List" method is not supported`.
- `copy`/`move` need `force` to overwrite (else error 501 `already exists`) and `recursive` to create parents. Both take `source_path`/`destination_path`; `link` takes `target_path`/`link_path`.
- A link resolves to its target, attributes included: `latest/@type` is `table`, `latest&/@type` is `link`.
- `lock` requires a transaction (`A valid master transaction is required`) and answers `{lock_id, node_id, revision}`.
- A conflicting lock names the winner: error 402, `… since "exclusive" lock is taken by concurrent transaction 4-dac2-10001-eb1b`, with a `winner_transaction` attribute.
- `waitable=%true` returns a `pending` lock with `revision=0`; `#<lock_id>/@state` becomes `acquired` when the queue clears.
- A waitable lock can wait forever: a transaction holding a snapshot lock is refused an exclusive one on the same node (error 400, `already taken by same transaction`), but the waitable form queues forever. `lock_waiting` has a deadline.
- `unlock` is not modelled; its rules after the node was modified were not verified.

## Batched commands

`execute_batch` sends several light commands in one request (`BatchRequest`, `Client::execute_batch`; `cargo run -p ytsaurus-client --example batch`). Observed:

- Twelve `create`s took one request and 9.16 ms; one at a time, 140.77 ms (through a counting TCP relay). `tests/batch.rs` pins the request count in-process.
- An envelope `transaction_id` is dropped silently (`TExecuteBatchOptions : TMutatingOptions` has no transactional half): the node was created outside the transaction and survived the abort. The client stamps each part; `execute_batch` is on the `NO_TRANSACTION` list.
- A part naming an unknown command fails the whole batch: HTTP 400, `Unknown command "frobnicate"`, no per-part results (`TRequestExecutor::Run` resolves every descriptor first).
- **That refused batch still ran every other part.** `TExecuteBatchCommand` runs all parts through `CancelableRunWithBoundedConcurrency`, then calls `.ValueOrThrow()` on the collected list; dispatch is never aborted. Ordering and `concurrency=1` do not help:

  | Batch (each answered 400, no results) | Applied |
  | --- | --- |
  | `[create a1, frobnicate]` | `a1` exists |
  | `[frobnicate, create b1]` | `b1` exists |
  | `[create c1, frobnicate, create c2]` | both exist |
  | `concurrency=1`, `[frobnicate, create d1, create d2]` | both exist |
  | `concurrency=1`, 8 creates then `frobnicate` | all 8 exist |

  `Client::execute_batch` reports the prefix it was answered for; `ClientError::BatchInterrupted` does not call it "applied".
- A batch refused while parsing parameters runs nothing; one that reaches execution runs everything. The message tells which:

  | Probe (each with a `create`; nothing applied) | Message |
  | --- | --- |
  | `concurrency=0` | `Validation failed at /concurrency` |
  | part missing `command`; part `parameters` not a dict; `requests` not a list | `Error loading parameter /requests` |
  | `requests` missing | `Missing required parameter /requests` |

- Parts run in parallel: `create` and `exists` on one node got `%false`. Put a dependent part in a second batch.
- A replay under one mutation id is deduplicated per part. Part *k* gets the batch's id plus *k* (`NRpc::GenerateNextBatchMutationId`, `++id.Parts32[0]`), and every volatile part gets the batch's `retry` flag. Measured with `BatchRequest::create_table` (no `ignore_existing`):

  ```text
  first  (id)          : ["2-2e82-10191-d4fdeff4", "2-2e83-10191-b0f0b0cd"]
  replay (id, retry)   : ["2-2e82-10191-d4fdeff4", "2-2e83-10191-b0f0b0cd"]   IDENTICAL
  fresh  (new id)      : [501 "already exists", 501 "already exists"]
  ```

  **Do not test this with `BatchRequest::create`**: its `ignore_existing` returns the old node ids under a fresh id too. Per-part ids are derived by incrementing, so the client refuses to split a batch carrying a caller's id.
- Data types, not `isHeavy`, decide what can be a part: output `tabular` or `binary`, or input `binary`, is refused before any part runs with `Command %Qv cannot be part of a batch since it has inappropriate output type %Qlv`. That is 21 of the 190 commands in `GET /api/v4`, against 7 on the crate's `HEAVY` list. Accepted: `get_job_spec` (`is_heavy: true`), and `write_table` (`is_heavy: true`, input `tabular`, output `structured`) with rows in the part's `input`, which the crate refuses by policy. Refused: `alter_query` and `push_queue_producer` (`is_heavy: false`), `lookup_rows`, and `select_rows`: `[create x1, select_rows]` got 400 `inappropriate output type "tabular"` with `x1` created.
- Also refused whole: a part whose command needs input and has none (`Command %Qv requires input`; `insert_rows`, `write_table` and seven others).
- No modelled command answers a bare `{}` under API v4: `create` → `{"output":{"node_id":…}}`, `set` and `remove` → `{"output":{}}`, `exists` → `{"output":{"value":false}}`. The reference's bare `{}` for `set` is v3 (`GET /api/v4` lists `remove` and `set` as `output_type: structured`, `/api/v3` as `null`). The parser checks the key: `node_id` for `create`, `value` for `exists`/`get`/`list`.

## Control records

Attributed entities interleaved with the data: `table_index`, `row_index`, `range_index` (int64) and `key_switch` (boolean). A data row is a map, which lets the runtime tell them apart without decoding rows.

YTsaurus writes a trailing `;` inside the attribute block:

```text
<\x01\x16table_index=\x02\x00;>#;
```

Enabled in the operation spec:

- `job_io.control_attributes.{enable_table_index,enable_row_index,enable_range_index,enable_key_switch}`
- `mapper.enable_input_table_index` overrides `enable_table_index`.
- For map-reduce, the reducer's section is `reduce_job_io`, not `job_io`: each job type has its own I/O section.

## Cluster gotchas

- `--spec` is YSON: `{mapper={memory_limit=536870912}}`, with `=`, `;` and `%true`. JSON fails with `Unexpected token ":"`.
- `map-reduce` uses `--map-local-file` / `--reduce-local-file`, not `--local-file`.
- Binary YSON in Python needs the `ytsaurus-client` and `ytsaurus-yson` packages; without the second, `YSON bindings required`. `ytsaurus-yson` includes the compiled `library/cpp/skiff`, the C++ reference for `tests/skiff-cpp-interop/`.
- A duplicate `mutation_id` without `retry=%true` is refused, `Duplicate request is not marked as "retry"`. The flag is not inferred from a known id.
- The file-cache commands answer with a bare string, not the `{path=…}` envelope, and a miss is an empty string.
- A `cache_path` that does not exist is also a miss: with no `//tmp/yt_wrapper` at all, `get_file_from_cache` answers 200 and `""` (verified by removing the tree and re-running `cached_upload`). The lookup needs no `create`, which a read-only managed cache would refuse; `upload_worker_cached` creates the directory on the miss branch.
- On a managed cache that create is refused with code 901, `Access denied … "write | modify_children" … not allowed by any matching ACE` (real installation, #32). A 901 on the cache's own writes (the directory, the staging node, `put_file_to_cache`) makes `upload_worker_cached` upload under `//tmp` and warn, naming `Client::with_file_cache`; any other error fails the upload. Neither branch has run against a denying cluster; `crates/ytsaurus-client/tests/file_cache.rs` scripts the refusals in-process.
- `CachedFile::cached` says which node the caller holds; `uploaded` is also true for the `//tmp` fallback, and deleting on it evicts a shared entry. The fallback has `//tmp`'s ACL, no expiry and a name that is unique but not unpredictable, so a co-tenant listing `//tmp` can rewrite the worker before exec. Prefer `with_file_cache` on your own directory.
- A managed cache allows an ordinary user only `read` (`check_permission` on `//tmp/yt_wrapper/file_storage/new_cache`: `read allow`; `write`, `remove`, `create` deny), so `cached_upload`'s clearing step got 901. The example brings its own cache beside `BASE`, which each run removes; `YT_FILE_CACHE` overrides it.
- A cached file is named by its hash. Reference it as `<file_name="my_job">//tmp/.../ab/cdef…` in `file_paths`, or the job's command finds nothing to run.
- Two operations writing one output table serialise on an exclusive lock, and the loser fails to prepare. Give concurrent operations separate outputs.
- A column value cannot carry attributes: `Table values cannot have top-level attributes`.
- The cluster re-encodes rows on ingest: 309 676 bytes uploaded came back as 309 688. Compare read-back against read-back.
- `stderr_size` from `list_jobs` is a hint: several hundred bytes of stderr reported as `1`.
- A Rust panic reaches the cluster as `Process terminated by signal 6` (`panic = "abort"`); the message is in the job's stderr, which `wait_for_operation` fetches.
- A job error's outer message is a category (`User job failed`); the cause is at the bottom of `inner_errors`. Both `ClientError` paths flatten outer plus innermost.
- A v4 answer is keyed by what it returns; for `exists` the key is `value`, not `exists` (the wrong key failed every call for two releases). Every command whose result is read needs a call site.
- Never assert on the rendered text of a generated value. The text YSON writer leaves a string unquoted when its first byte is a letter or `_` and the rest alphanumeric or `_-.` (`ser::is_safe_unquoted`), so a mutation ID goes bare (`ebd6e011-…`) or quoted (`3f2a1b-…`) by its first hex digit: 39.8 % unquoted over 100 000 IDs, and a test matching `mutation_id="…"` failed two runs in five. Decode `X-YT-Parameters` and compare values (`tests::sent_parameters`); a fixed literal like `transaction_id="3-5d231-…"` is safe. The cluster accepts both forms.
