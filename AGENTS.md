# AGENTS.md

Context for coding agents working on **ytsaurus-rs**. Read this before changing
anything.

## What this is

A Rust stack for writing [YTsaurus](https://ytsaurus.tech) MapReduce workers, so
jobs can be written in Rust instead of C++. A YTsaurus job is an arbitrary
executable: it reads input rows from fd 0 and writes output tables to fds 1, 4,
7…; the wire format is binary YSON. There is no official Rust SDK, so this
repository builds the minimal stack — a YSON codec and a job runtime.

## Layout

| Path | What it is |
| --- | --- |
| `crates/ytsaurus-yson/` | YSON codec (text + binary). Fork of [ss123she/yson-rs](https://github.com/ss123she/yson-rs) @ `ba2044c`. |
| `crates/ytsaurus-job/` | Job runtime: streaming reader, control records, multi-table output. Reads and writes YSON or Skiff. |
| `crates/ytsaurus-skiff/` | Skiff schema, format and bounded streaming codec. Pre-release, `publish = false`; see [docs/skiff-compatibility.md](docs/skiff-compatibility.md). |
| `crates/ytsaurus-format/` | `DataFormat`: the one format selection shared by the launcher and the worker, so the two cannot drift. Pre-release, `publish = false`. |
| `crates/ytsaurus-client/` | HTTP API v4 launcher: upload a worker, start an operation, wait for it, and say why it failed. No Python needed. |
| `crates/ytsaurus-helpers/` | Derive macros for the client: `#[derive(TableRow)]` infers a table schema from a struct. Proc-macro crate, so it can hold nothing else. |
| `examples/` | Worker binaries (`cat`, `wordcount`, `hello`, `sessionize`, `boom`, `selfrun`, `counted`, `shards`, `skiff_cat`) plus their e2e tests. |
| `docs/` | [writing-a-job.md](docs/writing-a-job.md) (the user guide), [benchmarking.md](docs/benchmarking.md) (measurements + the Skiff decision), [skiff-compatibility.md](docs/skiff-compatibility.md) (what "compatible with the Go SDK" means, and every gap), [go-parity.md](docs/go-parity.md) (every Go SDK example mapped onto this repo), [sdk-comparison.md](docs/sdk-comparison.md) (the C++ and Go clients side by side with this one). |
| `tests/e2e/` | Cluster scripts and captured golden fixtures. |
| `scripts/build-worker.sh` | Static musl worker builds. |

## Fixed decisions — do not revisit without a human

| Decision | Value |
| --- | --- |
| Repository name | **ytsaurus-rs** |
| Crate names | `ytsaurus-*` prefix: `ytsaurus-yson`, `ytsaurus-job`; later `ytsaurus-skiff`, `ytsaurus-client` if needed |
| YSON foundation | fork of ss123she/yson-rs pinned to `ba2044c711cefa65259e25122fea21c36f451093` (2026-04-01, v0.1.3) |
| Licence | **Apache-2.0** for this project. Upstream yson-rs is MIT OR Apache-2.0; we elect Apache-2.0 and keep upstream's licence files and notices. |
| Job data format | binary YSON (`<format=binary>yson`) remains the **default** everywhere. Skiff is implemented and selectable through `DataFormat`, and is pre-release: making it the default is still the benchmark question below, not something this decision has been changed to allow. |
| Worker builds | `x86_64-unknown-linux-musl`, fully static; `lto = "fat"`, `codegen-units = 1`, `strip = "symbols"`, `panic = "abort"` — the last **only** for worker binaries, never for library crates |
| Operation launch | `ytsaurus-client` (this repo), or the `yt` CLI. |
| Repo layout | single Cargo workspace |

## Hard rules

1. **Publish nothing to crates.io** without explicit human approval. Never claim
   the `yson-rs` crate name — it belongs to the upstream author.
2. `ytsaurus-yson` is **vendored**, not a git dependency. This project is
   Apache-2.0, so its own §4 obligations apply to the vendored code:
   - keep upstream's `LICENSE-APACHE` and `LICENSE-MIT` where they are, unedited
     — they are notices received with the code, not ours to rewrite;
   - keep [`NOTICE`](NOTICE) and
     [`crates/ytsaurus-yson/NOTICE`](crates/ytsaurus-yson/NOTICE) accurate, and
     credit the source repo and revision in the README and Cargo `description`;
   - record **every** change in
     [`crates/ytsaurus-yson/CHANGELOG.md`](crates/ytsaurus-yson/CHANGELOG.md).
     §4(b) requires stating modifications; the changelog is that statement.
3. Protocol facts are verified against the official YTsaurus documentation and
   against a real cluster. If code and docs disagree, **re-read the docs first**,
   then change the code. Cite the doc at the point of use.
4. Every change ends with green CI: `cargo fmt --check`, `cargo clippy
   --all-targets -D warnings`, `cargo test`, `cargo test --doc`.
5. **No scope creep.** RPC proxy, protobuf row format, dynamic tables, non-Linux
   targets are out of scope until a human decides otherwise. *(Custom job
   statistics were on this list until the backlog ranked them P1 #7; that is the
   human decision, and they now ship as `JobStatistics`.)*

## Commands

```sh
cargo test --workspace            # 427 tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/build-worker.sh         # static musl workers -> target/x86_64-unknown-linux-musl/release-worker/
./scripts/build-worker.sh cat     # just one

cargo bench -p ytsaurus-yson      # codec microbenchmark
cargo bench -p ytsaurus-job       # job-path throughput

# 2 GB streaming memory test (ignored by default)
cargo test -p ytsaurus-job --release --test memory_tests -- --ignored --nocapture
```

`build-worker.sh` works on Linux and macOS. On macOS it links with the `rust-lld`
bundled with the toolchain, because Apple's `cc` cannot produce Linux ELF — no
cross-toolchain or Docker needed.

`panic = "abort"` is a whole-graph profile setting and cannot be scoped to one
crate. It lives in the workspace `release-worker` profile so library crates never
inherit it.

`ytsaurus-client`'s **`tls` feature is on by default and off in `examples/`**.
TLS means `rustls`, which means `ring`, which needs a C cross-compiler to reach
musl — and `examples/` is what `build-worker.sh` cross-compiles. Turning it off
there is what keeps that script working with nothing but the Rust toolchain,
even for a worker that contains the whole client. The dependency is spelled out
in `examples/Cargo.toml` rather than inherited, because cargo does not let an
inherited dependency disable default features.

## Protocol reference

Verified against the docs **and** against a live cluster.

### Binary YSON markers

| Marker | Type | Payload |
| --- | --- | --- |
| `0x01` | string | zigzag varint length (`sint32`), then that many raw bytes |
| `0x02` | int64 | zigzag varint (`sint64`) |
| `0x03` | double | 8 bytes little-endian |
| `0x04` / `0x05` | boolean | false / true |
| `0x06` | uint64 | unsigned varint |
| `0x23` `#` | entity | — |
| `< >` `[ ]` `{ }` `=` `;` | attributes, list, map, key/value separator, item separator | literal ASCII |

### Descriptors

Output table `k` is fd `3k + 1`: table 0 → fd 1, table 1 → fd 4, table 2 → fd 7.
The cluster agrees in writing: a job's environment carries
`YT_FIRST_OUTPUT_TABLE_FD=1`.

### The job environment

Captured from a job on a local cluster:

`YT_JOB_ID` · `YT_OPERATION_ID` · `YT_JOB_COOKIE` · `YT_JOB_INDEX` ·
`YT_TASK_JOB_INDEX` · `YT_START_ROW_INDEX` · `YT_FIRST_OUTPUT_TABLE_FD` ·
`YT_NODE_HOST` · `YT_POOL_TREE` · `YT_COLLECTIVE_ID` / `YT_COLLECTIVE_MEMBER_RANK`
· `YT_JOB_PROXY_{GRPC,HTTP}_SOCKET_PATH`

`YT_JOB_ID` is what `is_inside_job()` tests, matching Go's
`mapreduce.InsideJob`. argv is exactly the spec's command — `["./selfrun"]` —
so argv-based role dispatch (`wordcount map`) also works, but has to be
remembered at the call site.

### Custom job statistics

A job writes them to **fd 5** as a YSON list fragment holding one map —
`{"rows/read"=7};` — matching the Python wrapper's `write_statistics`. At most
**128 names** per job.

The cluster files them under `progress/job_statistics/custom`, and the shape is
deeper than it looks. Captured from a local cluster:

```text
{"rows/rejected"={"$"={completed={map={count=1;max=3;min=3;sum=3}}}}}
```

- the name keeps its slash as **one key**; it does not nest;
- `$` → job **state** → job **type** → the aggregate.

`Client::statistic_sum` totals the `completed` state across job types: an
aborted job's work is redone by its replacement, so counting it would double.

`JobStatistics` refuses to touch fd 5 unless `is_inside_job()`. With one binary
serving as both launcher and job, fd 5 in the launcher is as likely to be an
open socket to the cluster as to be nothing.

**Built-in statistics are stored differently from custom ones**, so
`Client::job_statistic_sum` exists beside `statistic_sum`: a built-in name
**nests** by path component (`time` → `exec`) where a custom one keeps its slash
as one key, and the state separator is **`$$`** rather than `$`. A local cluster
reports **nothing under `user_job/cpu`**, so job-CPU comparisons cannot be run
here; `time/exec` is what it does report.

### Table schemas

A schema is a YSON **list** of column dicts with `strict` and `unique_keys` as
attributes **on the list**:

```text
<strict=%true;unique_keys=%false>[{name=host;required=%true;sort_order=ascending;type=utf8};…]
```

Cluster facts, all watched rather than read:

- **On `create`, the schema goes inside `attributes`.** A top-level `schema` is
  answered with 200 and a node id and then **silently ignored** — the table comes
  back with an empty weak schema. `alter_table` inverts this: there `schema` is a
  top-level parameter. `set //table/@schema` is refused outright.
- `create` with `ignore_existing` ignores the attributes too, so `create_table`
  fails on an existing path rather than reporting success with the old schema.
- **`boolean`/`any` are the `type` spelling; `bool`/`yson` are `type_v3`.** Those
  two are the only names that differ; mixing them is refused
  (`Error parsing ESimpleLogicalValueType value "bool"`).
- **`any`, `null` and `void` can never be required.**
- `required` **defaults to `%false`**, so a column dict without it is optional.
- A read-back always carries `required`, `type` *and* `type_v3` on every column,
  with the keys in **alphabetical** order. Unknown keys in a column dict are
  silently dropped.
- Key columns must be a **contiguous prefix**; `unique_keys=%true` needs at least
  one key column; names must be non-empty, ≤256 bytes, not start with `@`, and
  be unique.
- **`sort_order=descending` is refused** on this build: `Descending sort order is
  not available in this context yet`, gated by
  `//sys/@config/enable_descending_sort_order`.

`TableSchema::validate` mirrors these so the error arrives as a sentence naming
the column rather than as error 314 from a create.

#### Changing one afterwards

`alter_table` takes `schema` as a **top-level** parameter. A table **with rows**
takes only changes that ask less of the rows already written — error 316,
`Table schemas are incompatible`, with an inner error naming the column:

| Change | On a table with rows |
| --- | --- |
| add an **optional** column, at any position | allowed |
| required → optional | allowed |
| strict → non-strict | allowed |
| add a **required** column | `Cannot insert a new required column "must" into a non-empty table` |
| remove (or rename) a column | `Cannot remove column "size" from a strict schema` |
| change a type | `Type of "" field is modified in non backward compatible manner` |
| unsorted → sorted | `Cannot change schema from unsorted to sorted` |
| non-strict → strict | `Changing "strict" from "false" to "true" is not allowed` |

- **An empty table accepts every one of them.** A migration rehearsed on an
  empty table proves nothing about the real one.
- **A non-strict schema can never gain a named column**: `Cannot insert a new
  column "note" into non-strict schema`. Relaxing `strict` is a one-way door.
- No local compatibility rules in the client: the cluster's inner error already
  names the column, and a local rule set could only refuse what the cluster
  would allow.
- A **failed** `write_table` leaves its upload transaction holding an exclusive
  lock on the table for a moment; the next command on that path fails with the
  concurrent-transaction error until it clears.

### Transactions

Commands: `start_transaction`, `commit_transaction`, `abort_transaction`,
`ping_transaction` — the v4 names; `start_tx` and friends are **not registered**.
`start_transaction` answers `{transaction_id="3-5bc70-10001-387a"}`; the other
three take `transaction_id` and answer with `{}` or a commit timestamp.

Every other command joins a transaction through a `transaction_id` parameter,
which the client stamps in one place (`Transport::in_transaction`) rather than
per command. Cluster facts:

- **A transaction expires 30 000 ms after its last ping** — that is the default
  `@timeout`, and asking for one is how `Transaction` knows how often to ping.
  Verified: 2 s timeout, left 4 s, `Transaction … has expired or was aborted`.
- **A commit is not idempotent.** The second commit fails with
  `No such transaction`, which reads like the *first* one failed. Hence the
  mutation ID on commit.
- **An abort is forgiving**: aborting a committed transaction, or one that never
  existed, answers `{}`. So aborting from `Drop` is always safe.
- **`get_operation`, `list_jobs`, `get_job_stderr` and the file-cache commands
  accept `transaction_id`** and ignore it, so the blanket stamp costs nothing
  *here*. The client stops sending it to them anyway: `NO_TRANSACTION` in
  `http.rs` lists the scheduler and job commands, which go to the scheduler and
  the controller agents rather than the master and take no
  `TTransactionalOptions`. That is hardening, not a fix — it buys nothing on
  this cluster, and holds only for as long as a proxy quietly drops parameters
  it does not recognise. It is worth having because `Transaction` derefs to
  `Client`, so a launcher's first `wait_for_operation` inside a transaction is
  what would break on a version that refuses them. **`start_operation` is
  deliberately not on the list**: an operation genuinely can run inside a
  transaction, which is how its output tables stay invisible until the launcher
  commits. A command that names a transaction itself keeps its own.
- `start_transaction` under a transaction makes a **nested** one, which is what a
  bound client naturally does.
- Using an aborted or expired transaction fails with `No such transaction` nested
  inside `Error resolving path …`.
- `ping_ancestor_transactions=%true` is accepted; unnecessary here, since every
  handle pings its own transaction.

### Picking a verb, and what is a command at all

- **The proxy documents the rule outright**, so no command's verb is a guess:
  *"If the command has an input data stream, then PUT. If the command is
  mutating, then POST. Otherwise GET."* Both properties are declared per
  command in the cluster's own registry —
  `yt/yt/client/driver/driver.cpp`, `REGISTER_ALL(command, name, inDataType,
  outDataType, isVolatile, isHeavy)`. Cross-checked against what this crate
  already sends: `write_table` is `Tabular` in and volatile → PUT, `create` is
  volatile → POST, `get` and `read_table` are neither → GET.
- That registry is also where "is this command retriable, and is it heavy?"
  is answered: `isVolatile` and `isHeavy` are the two bits `Repeatable`
  encodes. `Client::raw_command` defaults to `Repeatable::Never` because it
  cannot read them for a command it does not model.
- **`whoami` is not an API v4 command.** It is easy to assume it is, and the
  Go SDK's `WhoAmI` goes through `newAuthCall` to the proxy's auth endpoint,
  not to `/api/v4/`. `get_supported_features` is the real "no parameters,
  small answer" command — `Null` in, `Structured` out, non-volatile,
  non-heavy — and is what the doctest and the `raw` example use.
- `list_operations`, `check_permission`, `read_file` and
  `get_supported_features` are all registered and none is modelled here; they
  are the natural first users of the raw door.
- **`get_supported_features` answers `{features=…}`**, not `{value=…}` — the
  envelope is keyed by what the command returns, the same trap that made
  `exists` read the wrong key for two releases. Captured from a local cluster:
  `compression_codecs` (71 of them), `erasure_codecs`, `node_flavors`,
  `operation_statistics_descriptions`, `primitive_types`,
  `query_memory_limit_in_tablet_nodes`,
  `require_password_in_authentication_commands`, `structured_web_json`,
  `user_tokens_metadata`.
- **`read_file` streams and `write_file` takes a chunked body**, verified with
  a 4 MB round trip through `Client::raw_command_streaming` and
  `raw_command_upload` — neither direction holds the file.

### Authentication and compression

- The token is looked for as the `yt` CLI looks for it: **`YT_TOKEN`, then
  `YT_TOKEN_PATH`, then `~/.yt/token`**, and it is **trimmed** — `echo token >
  ~/.yt/token` leaves a newline that fails authentication with an error saying
  nothing about newlines.
- **Responses are already compressed**: `ureq`'s `gzip` feature is on, so every
  request carries `Accept-Encoding: gzip` and every answer is decompressed on
  arrival. A 67.7 MiB table read came back as **400 KiB** on the wire. Nothing
  but `tests/request_shape.rs` would notice if the feature were dropped — a
  cluster answers the same either way, just larger.
- **The proxy accepts a gzipped request body** (`Content-Encoding: gzip`),
  verified. Uploads are *not* compressed: that costs a compression dependency in
  a crate linked into musl worker binaries, which is a human's call.
- A local cluster **accepts any token**, so the file lookup is unit-tested and
  whether a real installation likes the token cannot be checked here.

### Stopping an operation

- `abort_operation` takes `operation_id` and an optional `abort_message`, and
  answers `{}`. The message is folded into the operation's **error document**,
  under the cluster's own `Operation aborted by user request`.
- **It is not idempotent.** Once the scheduler has let go of an operation it
  answers code 200, `No such operation` — where `abort_transaction` forgives
  exactly that. It lets go as soon as the first abort is accepted, so even an
  operation that was running a moment ago refuses the second one. The rule is
  "the scheduler has dropped it", not "it is terminal": an operation that
  finished *by itself* can still be aborted for the short while it is kept.
- **Never send it under a mutation ID.** The master's mutation cache does not
  cover a scheduler command: a resend of the same ID, flagged as a retry, is
  answered `No such operation` rather than with the first response, so a retry
  turns an abort that worked into an error the caller believes. `Repeatable::Never`.
- The call is acknowledged in ~350 ms and the operation is **already `aborted`**
  by then. The `aborting` state exists but the request outlives it.
- `suspend_operation`, `resume_operation`, `complete_operation` and
  `update_operation_parameters` exist in the API and are not modelled.

### Appending to a table

- **`<append=%true>` is an attribute on the path**, not a parameter beside it:
  `{path=<append=%true>"//tmp/t"}`. Sent as a sibling parameter it is ignored
  and the table is **replaced**, with a 200 — silent data loss, which is why
  `TablePath` exists and why a wire-level test pins the shape.
- A bare path replaces; `<append=%false>` also replaces.
- **The table must exist.** Otherwise `Error getting basic attributes of user
  objects`.
- **A sorted table stays sorted and the cluster enforces it**: a key smaller
  than the last is refused with error 301, `Sort order violation: [0#9] > [0#1]`.
- Rewriting a table in `k` pieces sends `(k+1)/2` times the rows; measured at
  6.5× for 12 pieces in [`docs/benchmarking.md`](docs/benchmarking.md).
- **Appends take a *shared* lock; replaces take an exclusive one.** Four
  concurrent appends to one table all land; four concurrent replaces leave one
  winner and three `Cannot take "exclusive" lock` failures. This is most of why
  append is worth having, beyond the wire saving.
- **Zero rows is asymmetric**: an append of nothing is a no-op, a *write* of
  nothing truncates the table.
- A reader never sees a partial append — `@row_count` holds its old value until
  the upload transaction commits.

### Connections

- **A response body must be read or the connection is not pooled.** `ureq` only
  returns a connection it knows is finished, so a command that ignored its
  answer opened a fresh one every time: a few seconds of table writes left
  11 623 sockets in `TIME_WAIT`. Reading and discarding took 23 % off a small
  write. Any new command must consume its response.

### Jobs, listed and read

- **Stderr is kept for jobs that succeeded**, not only failed ones, and no spec
  option is needed. Verified with a vanilla job that echoed to stderr and
  completed.
- **`list_jobs` forgets.** It answers with an empty list for an operation that
  finished a while ago: the controller agent drops its jobs, and this local
  cluster has no job archive to fall back on (`get_job_stderr` on an old job
  says `Job archive is unavailable`). Harvest immediately after
  `wait_for_operation` or not at all.
- `list_jobs(op, None, limit)` lists every state; the failure report passes
  `Some("failed")`.

### Streaming table I/O

- The proxy **accepts a chunked request body** for `write_table`, so a table can
  be written from a `Read` that never has all of it.
- `ureq` 3.3 caps `read_to_vec` at 10 MB unless told otherwise but leaves a
  **reader uncapped**, which is the right way round: the buffered path passes an
  explicit limit, the streaming path passes none.
- **`ureq` 3.3 still exposes no trailers** — rechecked in its source, where the
  word does not appear. So the `X-YT-Error` trailer a proxy uses to report a
  mid-stream failure remains unreadable, and the completeness check on
  `read_table` remains the only compensation. On the streaming path there is
  nothing to check up front; a fragment cut short fails in the decoder instead.
- Measured on a local cluster: writing 64 MiB from a generator and streaming it
  back cost **1.0 MiB** of peak RSS; `read_table` on the same table cost
  **70.9 MiB**.

### Cypress: naming and locks

- **`list` is not sorted.** Three dated tables came back as the second, the third
  and then the first. The order is the cluster's own.
- **A truncated listing is `<incomplete=%true>[…]`** — an attribute on the list,
  not an error. `Client::list` refuses one rather than returning a short list.
  `max_size` produces the same marker.
- **Listing a non-map node is error 103**, `"List" method is not supported`, not
  an empty list.
- **`copy`/`move` need `force` to overwrite** (else error 501 `already exists`)
  and `recursive` to create parents. Both are `source_path`/`destination_path`;
  `link` is `target_path`/`link_path`.
- **A link resolves to its target, attributes included.** `latest/@type` is
  `table`; `latest&/@type` is `link`. The `&` suffix asks about the link itself.
- **`lock` requires a transaction**: `A valid master transaction is required`.
  It answers `{lock_id, node_id, revision}`.
- **A conflicting lock names the winner**: error 402, `… since "exclusive" lock is
  taken by concurrent transaction 4-dac2-10001-eb1b`, with a `winner_transaction`
  attribute.
- **`waitable=%true` returns a `pending` lock, not a held one**, with
  `revision=0`; `#<lock_id>/@state` becomes `acquired` when the queue clears.
- **A waitable lock can wait for something that will never happen.** A
  transaction holding a *snapshot* lock is refused an exclusive lock on the same
  node (error 400, `already taken by same transaction`) — but the waitable form
  of that request queues forever instead. Hence the deadline on `lock_waiting`.
- `unlock` exists and is not modelled: its rules about a transaction that has
  already modified the node were not verified here.

### Control records

Attributed **entities** interleaved with the data: `table_index`, `row_index`,
`range_index` (int64) and `key_switch` (boolean). A data row is a **map**; that
is how the runtime tells them apart without decoding rows.

YTsaurus emits them with a trailing `;` *inside* the attribute block:

```text
<\x01\x16table_index=\x02\x00;>#;
```

Enable them in the operation spec:

- `job_io.control_attributes.{enable_table_index,enable_row_index,enable_range_index,enable_key_switch}`
- `mapper.enable_input_table_index` overrides `enable_table_index`
- For **map-reduce**, the reducer's section is **`reduce_job_io`**, not `job_io` —
  an operation with several job types gives each type its own I/O section.

### Cluster facts learned the hard way

These cost time once. They are recorded so they do not cost it again.

- **`--spec` is YSON, not JSON.** `{mapper={memory_limit=536870912}}`, with `=`,
  `;` and `%true`. A JSON spec fails with `Unexpected token ":"`.
- **`map-reduce` uses `--map-local-file` / `--reduce-local-file`**, not
  `--local-file`.
- **Binary YSON needs two pip packages**: `ytsaurus-client` *and* `ytsaurus-yson`.
  Without the second: `YSON bindings required`.
- **A duplicate mutation must admit to being one.** Re-sending a `mutation_id`
  without `retry=%true` is refused with `Duplicate request is not marked as
  "retry"`, not deduplicated. The flag is not inferred from the ID being known.
- **The file-cache commands answer with a bare string**, not the `{path=…}`
  envelope the rest of API v4 uses, and a **miss is an empty string** rather
  than an error or an entity.
- **A `cache_path` that does not exist is a miss, not a resolve error.**
  `get_file_from_cache` against a cluster with no `//tmp/yt_wrapper` at all
  answers **200 and `""`** — the same empty string as any other miss, not the
  error 500 a missing path usually earns. So the lookup needs no `create` to
  guard it, which matters because the cache is often installation-managed and
  read-only to the caller: a lookup that mutated it would fail exactly where the
  cache is worth the most. `upload_worker_cached` creates the directory on the
  miss branch instead. Verified by removing the whole tree and re-running
  `cached_upload`.
- **A cached file keeps its name from the hash, not from the upload.** Reference
  it in `file_paths` as `<file_name="my_job">//tmp/.../ab/cdef…` or the job's
  command finds nothing to run.
- **Two operations writing one output table serialise on an exclusive lock**,
  and the loser fails to prepare rather than waiting. Give concurrent
  operations their own outputs.
- **A column value cannot carry attributes.** YTsaurus rejects it on write with
  `Table values cannot have top-level attributes`, so a job can never receive one.
- **The cluster re-encodes rows on ingest.** 309 676 bytes uploaded came back as
  309 688. Compare read-back against read-back, never against the uploaded file.
- **`stderr_size` from `list_jobs` is a hint, not a length.** A job whose stderr
  was several hundred bytes was reported as `1`. Never use it to decide whether
  there is stderr to fetch.
- **A Rust panic reaches the cluster as `Process terminated by signal 6`.**
  Worker binaries are built with `panic = "abort"`, so a panic aborts. The
  message itself is only in the job's stderr, which is why
  `wait_for_operation` fetches it.
- **A job error's outer message is a category.** `User job failed` says nothing;
  the cause is at the bottom of `inner_errors`. Both `ClientError` paths flatten
  outer-plus-innermost.
- **A v4 answer is an envelope keyed by what it returns, and for `exists` that
  key is `value`, not `exists`.** Reading the wrong key failed every call for two
  releases, because nothing in the crate called `exists` until transactions did.
  Every command whose result is read needs a call site, or the shape is a guess.

## Architecture

### `ytsaurus-yson`

Vendored upstream plus a `scan` module. `scan_value(input, format)` returns the
byte length of the first complete value or `Scan::Incomplete`. It walks the token
stream without allocating, which is what makes streaming possible — upstream's
API takes a whole slice, so an input larger than memory could not be consumed
without it.

### `ytsaurus-job`

- `JobReader::next_event()` is a **lending iterator**, not `Iterator`. Rows borrow
  the read buffer, so the borrow must end before the next call. This is what makes
  zero-copy decoding safe, and the compiler enforces it.
- The reader holds **one buffer** (1 MiB default), compacts it, and grows it only
  when a single record does not fit. Streaming 2 GB does not raise peak RSS:
  46.6 MiB before and after on Linux CI, 1.9 -> 2.0 MiB on macOS.
- **Unknown control records are skipped, not surfaced as rows.** A control record
  is an attributed entity, and YTsaurus may add attributes this version has never
  seen. Handing one to the job as a row would silently corrupt the output table.
- **Output descriptors are never closed** (`ManuallyDrop<File>`). Table 0 is fd 1,
  which `std::io::stdout()` also refers to; closing it would leave later
  `println!` writing to a closed or recycled descriptor.
- **`finish()` is explicit.** Output is buffered; unflushed rows are missing rows.
  `Drop` makes a last-ditch flush and complains on stderr but cannot fail the job,
  which is why `run()` exists.
- A corrupt length prefix is capped by `max_record_bytes` (256 MiB) rather than
  chased into an OOM abort.
- **`Row::raw()` is byte-exact**; decode-then-re-encode is not, because
  `YsonNode::Map` is a `BTreeMap` and sorts keys. Identity jobs must use `raw()`.

## Fork status

Three real bugs were found in upstream while vendoring, all fixed here and all
recorded in the changelog. They matter because YTsaurus strings and attribute
names are **arbitrary byte strings, not text**:

1. **Infinite loop on a stray `/` in text input.** A `/` followed by anything
   other than `/` or `*` entered the comment branch without advancing the cursor,
   then `continue`d. `/a` never returned. Allocates nothing, so no memory
   watchdog catches it.
2. **Non-UTF-8 map keys were rejected**, though `YsonNode::Map` stores `Vec<u8>`.
3. **Non-UTF-8 attribute names were silently replaced with `""`** — a literal
   `unwrap_or("")`, which loses the name and collides every such attribute.

Also added: `Serialize` for `YsonValue`/`YsonNode`, `Copy` on `YsonFormat`,
`Serializer::with_buffer`/`into_output`, and the `scan` module.

Known limitations are documented in
[`crates/ytsaurus-yson/README.md`](crates/ytsaurus-yson/README.md). The two that
bite most: maps round-trip as values not bytes, and decoding into `String` fails
on non-UTF-8 columns (use `serde_bytes`).

**Worth doing:** report the three bugs upstream to `ss123she/yson-rs`. The hang is
a denial of service in any text-mode parser and the fixes are small.

## Testing

Three layers:

1. **Unit and integration** — 305 tests. Control records driven by the exact
   stream from the docs; chunked readers down to **one byte per `read`**, which
   exercises every split point including mid-varint.
2. **Offline e2e** — runs the real compiled worker with real fd 1 / fd 4
   redirection. Its golden fixtures are **captured from a live cluster**
   (`tests/e2e/capture_fixtures.sh`), so it is not our reading of the spec checked
   against itself.
3. **Cluster e2e** — `tests/e2e/run_e2e.sh` against a local YTsaurus in Docker.
   Not in CI (needs a multi-GB image). See
   [`tests/e2e/README.md`](tests/e2e/README.md).

Fuzzing: `cargo +nightly fuzz run fuzz_target_{1,2}` from `crates/ytsaurus-yson/`.
`tests/fuzz_smoke_tests.rs` gives CI a deterministic no-panic signal without
nightly.

When adding a fixture, prefer capturing from a cluster over hand-building one.
The synthetic fixture was wrong in two ways only the cluster revealed.

## Status

The codec, the job runtime and the example workers are implemented and verified,
including against a real cluster. Measurements:
[`docs/benchmarking.md`](docs/benchmarking.md) for the job path,
[`crates/ytsaurus-yson/BENCHMARKS.md`](crates/ytsaurus-yson/BENCHMARKS.md) for the
codec.

Verified: identity map reproduces 309 688 bytes byte-for-byte; table switching
across two input and two output tables; wordcount map-reduce matching a
hand-computed result; 2 GB streamed with no growth in peak RSS; 6.5 M fuzz
iterations without a crash.

### Shipped

- **GitHub**: [sshaplygin/ytsaurus-rs](https://github.com/sshaplygin/ytsaurus-rs),
  public, CI green, tagged `v0.1.0` with a release.
- **crates.io**: [`ytsaurus-yson` 0.1.0](https://crates.io/crates/ytsaurus-yson)
  and [`ytsaurus-job` 0.1.0](https://crates.io/crates/ytsaurus-job); docs.rs
  built both.
- **Upstream courtesy**: the fork and the three fixed defects are filed as
  [ss123she/yson-rs#1](https://github.com/ss123she/yson-rs/issues/1). The fork
  is licence-compliant on its own, so nothing waits on a reply.

### Roadmap

**1. Pilot worker on the local cluster.** Port one representative,
production-shaped task to `ytsaurus-job`: wide rows with mixed types and byte
columns, several output tables, a reduce with realistic keys, malformed-input
handling. Run it end to end on the Docker local cluster. Keep a friction log —
every place the API forces a workaround becomes an issue. *DoD: pilot runs e2e
locally; friction filed as issues.*

**2. API stabilization from pilot feedback.** Apply the accepted changes, update
[`docs/writing-a-job.md`](docs/writing-a-job.md), record everything in the
CHANGELOGs, bump to 0.2.0. *DoD: pilot issues closed or explicitly rejected; the
guide reflects the final API.*

**3. ~~`ytsaurus-client`~~ — done.** Verified against the local cluster: creates
tables, uploads the worker, writes rows, runs a map, waits, reads back and
compares, with nothing Python on `PATH`. Run it with
`cargo run -p ytsaurus-client --example launch`.

**4. The ranked backlog — worked top to bottom, P0 through P3.** Each item ends
with a cluster example that checks itself, so `tests/e2e/README.md` is the list
of what has actually been run:

| | | |
| --- | --- | --- |
| P0 | job diagnostics · one binary as launcher and job · vanilla | `diagnose`, `selfrun`, `vanilla` |
| P1 | reduce/sort · retries and `mutation_id` · worker cache · custom statistics | `sort_reduce`, `idempotent`, `cached_upload`, `statistics` |
| P2 | schema from a struct · transactions · Cypress and locks · `alter_table` | `schema`, `transaction`, `cypress` |
| P3 | streaming table I/O · the pilot's decode share · token file and compression | `streaming`, `profile`, `tests/request_shape.rs` |

**5. Parity with the Go SDK's examples**, mapped in
[`docs/go-parity.md`](docs/go-parity.md). All twelve were read and classified:
six have a Rust counterpart that runs on a cluster, six are a decision recorded
here not to. Three asked for something the client could not do — typed rows,
typed nodes, and a successful job's stderr — and got it. **Read that document
before adding client API**: it also lists what the Go SDK can do and this
cannot, which is where the next real gap is. Three of the four it listed are
now built — `abort_operation`, `<append=%true>`, and the escape hatch
(`Client::raw_command`); only the web UI links remain, and those are a
deliberate no.

**What is left of the backlog is not code.** P3 #15 (tracing spans) is written
down as "only worth doing if a user asks" — **and one has**: tracing, together
with logging, is now the first item of the pinned parity issue, ahead of
everything else, because a production deployment needs to see what the client is
doing. That supersedes the P3 ranking. TLS is the one part
of P3 #14 a local cluster cannot exercise. Everything else needs a human — see
below.

### Parked — needs a human and a real cluster

- **Skiff go/no-go — now about the default, not about the crate.**
  `ytsaurus-skiff` and `ytsaurus-format` exist and Skiff is selectable end to
  end: worker I/O, operation specs and direct table I/O. Binary YSON is still
  what every spec renders unless a caller asks otherwise, both crates are
  `publish = false`, and which compatibility gates are still open is
  [`docs/skiff-compatibility.md`](docs/skiff-compatibility.md). The reference
  implementation is the `skiff` package in the
  [Go SDK](https://pkg.go.dev/go.ytsaurus.tech/yt/go), pinned at v0.0.33.
  Making it the default is what still needs a human: job-path benchmarks exist
  ([`docs/benchmarking.md`](docs/benchmarking.md)) but that decision needs a
  ≥ 10 GB table and C++/Python baselines. Decoding is 66 % of job CPU for a job
  that does nothing else, which is the worst case for YSON, not a verdict — and
  **~10 % for the pilot**, a job that does something with its rows, measured on
  the local cluster by `cargo run -p ytsaurus-client --example profile`. That is
  well under the 30 % threshold, so the question has lost urgency without being
  settled.
- **Upstreaming** to
  [ytsaurus/ytsaurus-rust-sdk](https://github.com/ytsaurus/ytsaurus-rust-sdk) —
  the maintainers' stance in ytsaurus#6 is "PRs welcome". **Do not start without
  a go-ahead.**
- **Contacting the yson-rs author** about co-ownership or publishing.

A test cluster with real data is needed from a human for the Skiff comparison;
a local Docker cluster is enough for everything else.

## Non-goals

RPC proxy (custom binary protocol), protobuf row format, dynamic tables,
non-Linux targets, publishing to crates.io. *(Custom job statistics were on this
list until the backlog ranked them P1 #7 — a human decision, and they ship now
as `JobStatistics`.)*

## Reference

[YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson) ·
[control attributes](https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration) ·
[table switch](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/table-switch) ·
[operation options](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options) ·
[Try YTsaurus](https://ytsaurus.tech/docs/en/overview/try-yt) ·
[ss123she/yson-rs](https://github.com/ss123she/yson-rs) ·
[interop-tests](https://github.com/ss123she/yson-interop-tests)
