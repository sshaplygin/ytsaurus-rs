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
| `crates/ytsaurus-skiff/` | Skiff schema, format and bounded streaming codec. **Pre-release, and published from 0.2.5** — the ship gates in [docs/skiff-compatibility.md](docs/skiff-compatibility.md) are still not all green, and the API may change in a patch release. It is on crates.io because `ytsaurus-job` and `ytsaurus-client` depend on it and could not be published otherwise. |
| `crates/ytsaurus-format/` | `DataFormat`: the one format selection shared by the launcher and the worker, so the two cannot drift. Pre-release, and published from 0.2.5 with `ytsaurus-skiff`, whose status it inherits. |
| `crates/ytsaurus-client/` | HTTP API v4 launcher: upload a worker, start an operation, wait for it, and say why it failed. No Python needed. |
| `crates/ytsaurus-helpers/` | Derive macros for the client: `#[derive(TableRow)]` infers a table schema from a struct. Proc-macro crate, so it can hold nothing else. |
| `crates/ytsaurus-proto/` | Generated protobuf bindings for the RPC proxy, built from the upstream `.proto` files in the `third_party/ytsaurus` submodule — not from a copy. Pre-release, unpublished. |
| `crates/ytsaurus-rpc/` | RPC proxy client: bus framing, the RPC envelope and the dynamic-table row wire format. **Async on tokio**, unlike everything above it. Pre-release, unpublished; see [docs/rpc-compatibility.md](docs/rpc-compatibility.md). |
| `docs/` | [writing-a-job.md](docs/writing-a-job.md) (the user guide), [benchmarking.md](docs/benchmarking.md) (measurements + the Skiff decision), [skiff-compatibility.md](docs/skiff-compatibility.md) (what "compatible with the Go SDK" means, and every gap), [go-parity.md](docs/go-parity.md) (every Go SDK example mapped onto this repo), [sdk-comparison.md](docs/sdk-comparison.md) (the C++ and Go clients side by side with this one), [rpc-compatibility.md](docs/rpc-compatibility.md) (what the RPC client implements, every deliberate divergence from the reference clients, and the gates still open). |
| `tests/e2e/` | Cluster scripts and captured golden fixtures. |
| `tests/rpc-go-interop/` | Version-pinned Go program that *produces* byte vectors for the RPC row wire format and CRC-64, which the Rust tests consume. Same shape as `tests/skiff-go-interop/`. |
| `third_party/ytsaurus` | Submodule: the YTsaurus monorepo, sparse-checked-out for its `.proto` files only. `./scripts/init-protos.sh`. |
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
   **Client behaviour the protocol does not dictate — retries, routing, proxy
   selection, banning, refresh — is checked against the official clients'
   source before it is designed here**: C++ (`yt/cpp/mapreduce`) and Go
   (`yt/go`), and the Python wrapper where it is the reference (the retry
   list). Where the two disagree, say which was followed and why; where this
   client deviates from both, the deviation is a deliberate decision recorded
   in [docs/sdk-comparison.md](docs/sdk-comparison.md) — that record is what
   turned the heavy-proxy lifetime pin from a silent bug into #40, a filed
   divergence with a known fix. [docs/go-parity.md](docs/go-parity.md) is the
   same rule for API surface: read it before adding client API, because a
   feature list written by the people who built the thing is worth more than
   one written by the people reimplementing it.
4. Every change ends with green CI: `cargo fmt --check`, `cargo clippy
   --all-targets -D warnings`, `cargo test`, `cargo test --doc`.
5. **No scope creep.** Non-Linux targets are out of scope until a human decides
   otherwise. *(Custom job statistics were on this list until the backlog ranked
   them P1 #7; that is the human decision, and they now ship as `JobStatistics`.
   **The RPC proxy, the protobuf row format and dynamic tables came off it the
   same way** — a human asked for the RPC protocol to be implemented, and it
   ships pre-release as `ytsaurus-rpc`. The scope there is deliberately narrow:
   transactions, `lookup_rows`, `select_rows` and `modify_rows`, not the other
   150 request types. What is in and what is out is
   [docs/rpc-compatibility.md](docs/rpc-compatibility.md).)*

## Commands

```sh
./scripts/init-protos.sh          # once after cloning: the .proto submodule, shallow and sparse

cargo test --workspace            # 759 tests: 685 unit and integration, 74 doc
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/build-worker.sh         # static musl workers -> target/x86_64-unknown-linux-musl/release-worker/
./scripts/build-worker.sh cat     # just one

cargo bench -p ytsaurus-yson      # codec microbenchmark
cargo bench -p ytsaurus-job       # job-path throughput

cd tests/rpc-go-interop && go test ./...   # regenerate the RPC wire-format vectors
cargo run -p ytsaurus-rpc --example e2e    # RPC client against a live RPC proxy

# 2 GB streaming memory test (ignored by default)
cargo test -p ytsaurus-job --release --test memory_tests -- --ignored --nocapture
```

`build-worker.sh` works on Linux and macOS. On macOS it links with the `rust-lld`
bundled with the toolchain, because Apple's `cc` cannot produce Linux ELF — no
cross-toolchain or Docker needed.

`panic = "abort"` is a whole-graph profile setting and cannot be scoped to one
crate. It lives in the workspace `release-worker` profile so library crates never
inherit it.

`ytsaurus-client`'s **`tls` feature is on by default and off where the workers
are built**. TLS means `rustls`, which means `ring`, which needs a C
cross-compiler to reach musl — and the workers are what `build-worker.sh`
cross-compiles. Turning it off is what keeps that script working with nothing
but the Rust toolchain, even for `selfrun`, which contains the whole client. The
dependency is spelled out in `crates/ytsaurus-job/Cargo.toml` rather than
inherited, because cargo does not let an inherited dependency disable default
features.

**A dev-dependency is a worker's dependency.** Cargo compiles a package's
dev-dependencies whenever it builds that package's *examples*, and the workers
are examples of `ytsaurus-job`. So anything added to that crate's
`[dev-dependencies]` lands in the musl build: that is why `ytsaurus-client` is
there with `default-features = false`, why it carries a **path and no version**
(a version would make it cyclic with the client, which dev-depends on this crate
in turn, and deadlock both releases), and why the throughput bench lives in
**criterion pinned below 0.8** — 0.8 added a dependency on `alloca`, whose
build script wants exactly the C cross-compiler this build is meant not to
need, and `cargo bench` is not where that would have been noticed.

Its **`tracing` and `platform-verifier` features are off by default** and must
stay that way, for the second half of the same reason: a worker binary should
carry only what it runs on, and `default-features = false` in
`crates/ytsaurus-job/Cargo.toml` is what keeps them out of the musl build.
`platform-verifier` is gated on `tls` and so cannot reach a worker even if
something asked for it; `YT_CA_BUNDLE`, which answers the same need with no
dependency at all, sits behind `tls` for the same reason. CI asserts all of
this rather than trusting it — the musl job lists the worker's dependency graph
with `cargo tree -p ytsaurus-job --target x86_64-unknown-linux-musl
--prefix none` and fails if `tracing`, `rustls`, `ring` or
`rustls-platform-verifier` is in it. Listed and searched rather than probed with
`-i <crate>`: `-i` exits non-zero both when the crate is absent (the pass) and
when cargo could not run at all, and it resolves `-i` before `-p`, so even a
misspelled package prints the same "did not match any packages". Reading that
exit code turned every cargo failure into a silent pass. **Do not go back to
it** — a guard that cannot fail asserts nothing.
The observability that costs no dependency — the `traceparent` header — is
always compiled in.

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

Handing one to another process (`detach` / `attach_transaction`, #13):

- **`@timeout` is in milliseconds and comes back `Int64`.** `get
  #<id>/@timeout` on a 30 s transaction answers `{"value"=30000;}` in text
  YSON — no `u`, so not `Uint64`. `Transaction::attach` reads both anyway: a
  duration in milliseconds is exactly the field a master could spell unsigned,
  and this crate has been surprised by that class of thing before.
- **The attribute says nothing about how much life is left.** It is the
  configured timeout, not the remaining one, and the id carries no last-ping
  time. That is why `attach` pings before it returns: without it, a handoff
  taking longer than `timeout × 2/3` produces a handle whose own first ping —
  one interval away — lands after the cluster has already expired the
  transaction.
- **Three different absences, three different errors**, all observed on a local
  cluster:
  - a garbage id (`1-2-3-4`): `cluster error 1: Unknown cell tag 0` — names
    neither the id nor a transaction, which is what `attach_failed` rebrands;
  - an expired or aborted id, addressed as an object: `Error resolving path
    #<id>/@timeout` wrapping `No such object <id>` — **not** `No such
    transaction`;
  - the same id *pinged*: `No such transaction`, code 11000. Both spellings are
    why `transaction_is_gone` looks for each, in the whole document.
- **A detached transaction is indistinguishable from a held one**, so the only
  evidence a test can read is which requests stop arriving — which is what
  `crates/ytsaurus-client/tests/transaction_lifecycle.rs` does, against a stub
  cluster in-process, plus wall-clock timing for the join `detach` does.
- **`detach`'s wait covers the ping only up to a 30 s timeout.** The join is
  bounded at five seconds and a ping's request budget is
  `clamp(interval / 2, 1 s, 120 s)` on an `interval` of `max(timeout / 3, 1 s)`
  — so the budget fits inside the bound while the timeout is under 30 s, equals
  it at the 30 s default, and exceeds it above. The master honours the asked-for
  timeout verbatim, so that arithmetic is the caller's to do: `#<id>/@timeout`
  read back `3600000`, `30000` and `20000` for transactions started at each,
  observed on a local cluster — budgets of 120 s, 5 s and 3.3 s. Above the
  default a stalled ping outlives the detach and can restart the cluster's
  clock afterwards; the docs say so, and this is why they cannot say "no ping
  is in flight" flatly. Both directions are pinned in `transaction.rs`'s unit
  tests — one asserts the wait happens, one asserts it ends — and the second is
  what a `drop(alive)` in the ping thread's body fails; nothing else in the
  workspace does.

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
- `check_permission` and `get_supported_features` are registered and not
  modelled here; they are the natural first users of the raw door.
  *(`list_operations` and `read_file` were both on this list and both came off
  it — the first with the operation lifecycle, the second as `Client::read_file`
  and `Client::read_file_streaming`. The `raw` example still reads a file, now
  because that wire shape is verified rather than because nothing else could.)*
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
  `raw_command_upload` — neither direction holds the file. `Client::read_file`
  and `Client::read_file_streaming` are that round trip written down, and the
  buffered half needs a check the streaming half cannot have: a file's bytes
  carry no framing, so a body cut short by a mid-stream failure looks exactly
  like a shorter file, and the proxy's verdict is in a trailer `ureq` cannot
  read. It compares the body against the node's `@uncompressed_data_size` —
  the *logical* size, measured against `compression_codec=zlib_6` nodes of
  1 000 000 bytes each: `@compressed_data_size` is 4 214 for the cycling
  `i % 256` bytes the tests write, 999 for all zeros and 1 000 324 for
  `os.urandom` — none of them the logical size, and the read passes against
  all three. There is no `@file_size`: asked for one, the cluster answers
  `Attribute "file_size" is not found`.
- **`ureq`'s `limit()` does not bound memory.** `BodyWithConfig::do_build`
  wraps the raw source in a `LimitReader` and builds the gzip decoder on top,
  so the number bounds *transferred* bytes — and every request this crate
  sends carries `Accept-Encoding: gzip`. Measured: a `read_file` of 600 MiB of
  zeros arrives in 611 522 wire bytes, and `.limit(536870912)` on it returned
  all 629 145 600. A ceiling that counts memory has to sit above the decoder,
  which is what `http::CapReader` is for. **A wire limit is still needed
  underneath it** — a chunked stream of empty deflate stored blocks
  (`00 00 00 ff ff`) decodes to nothing, so a cap on decoded bytes never spends
  a byte against it and `flate2` loops inside a single `read`; with the wire
  limit taken away, that read does not return. **And the wire limit cannot be
  the memory cap**: deflate expands what it cannot compress, so the largest
  permitted body can arrive larger than the cap — 4 096 incompressible bytes
  gzip to 4 119. `http::wire_budget` is zlib's `deflateBound` plus the gzip
  wrapper, for that reason.
- **A memory cap is not a process budget.** `read_to_end` grows a `Vec` by
  doubling and copies as it grows, so both buffers are resident for the length
  of a copy — about 1.5× where the allocator cannot extend in place. Measured
  in a release build against a local listener: a read handing back
  536 870 911 bytes peaks at 544 178 176 of resident set, and a 600 MiB read
  *refused* by the 512 MiB cap peaks at 611 385 344. Quote the cap as what is
  held, never as what to size a container for.
- **A rich path does nothing to a file read**, measured on a 1000-byte file:
  `<lower_limit={offset=0};upper_limit={offset=10}>//tmp/f` reads back all
  1000 bytes and says nothing — a file is sliced by the command's `offset` and
  `length` parameters, never by limits on the path — and `//tmp/f[#0:#10]`
  likewise returns all 1000, then fails `read_file`'s size check, because
  `//tmp/f[#0:#10]/@uncompressed_data_size` is not a path the cluster parses
  (`Error reading parameter /path: Unexpected token "/" of type "slash"`).
  So `read_file` documents a plain node path; #12 is where selection goes.

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
- **A heavy read through a control proxy is answered with a cross-host `307`**
  naming a data proxy. The
  [HTTP proxy reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#return_codes)
  gives the row outright — *"307 | Redirecting heavy queries from light to
  heavy proxies"* — so this is documented routing rather than a balancer's
  quirk. `ureq` drops the `Authorization` header when it follows one
  (`RedirectAuthHeaders::Never`). The request then arrives unauthenticated and
  the cluster blames the token: `cluster error 111: Client is missing
  credentials`, about a token that is fine.
  **`redirect_auth_headers(RedirectAuthHeaders::SameHost)` is not the fix**:
  the redirect is deliberately cross-host, which is precisely what that setting
  does not cover.
- **`ureq` therefore follows nothing — `max_redirects(0)` for every transport —
  and the client follows redirects itself**, because the answer turns on
  several things at once that no combination of `ureq` settings expresses.
  **Same origin: everything goes.** **Crossing one: the token and the data stay
  behind.** In detail:
  - a redirect that **changes origin** (scheme, host or port) is refused when
    the request carries credentials, with `ClientError::Redirected`, naming
    where it pointed. One that stays on the same origin is followed, token and
    all: nothing new learns it, and refusing would break every command against
    a balancer that canonicalises its own host.
  - a redirect that changes origin is **also** refused when the request carries
    **data**, token or no token (`RedirectRefusal::Payload`). The same
    objection as the token, about the other thing a caller picks a host for: a
    tokenless `write_table` must not send a table's rows to whichever host a
    `Location` header names. A body of **length zero** is not data —
    `Content-Length: 0` gives nothing away — so a bodiless `POST` still goes.
  - **the method and the body survive the hop** — the request is sent again,
    not rewritten into a `GET`. That is what 307/308 require by definition, and
    what an API v4 command needs whatever the digit: a command's verb is fixed
    by the command (mutating → POST, input stream → PUT), and no `Location`
    changes it. So a bodiless `POST create` follows a balancer's `301`, and a
    same-origin `write_table` sends its rows on rather than losing them.
  - a body this client **cannot send again** is refused wherever it points:
    `Transport::upload`'s reader — `write_table_rows`, `raw_command_upload` —
    has already begun to drain into the first request and cannot be rewound.
    Refused with or without a token, because it costs data rather than a
    credential: a `write_table` that arrived with no rows came back `Ok(())`
    having written none.
  - a chain longer than `MAX_REDIRECTS` is a loop, not a route — and the whole
    chain shares **one** deadline, the command's own. Handing each hop a fresh
    `timeout_global` made the real limit `(MAX_REDIRECTS + 1)×` the one the
    caller asked for: 22 minutes at the default two, on an `exists`.

  `Location` is resolved against the address the request went to (RFC 3986
  §4.2), so a relative one still names a host in the error and in the origin
  comparison — and §5.3, so a reference with no path of its own (`?path=…`,
  `#frag`) keeps the request's path rather than falling back to its directory.
  Reproduced offline in
  `crates/ytsaurus-client/tests/redirect_credentials.rs`; a local cluster runs
  one proxy and redirects nothing. **A stub there must read the whole request
  before it answers** — head *and* body, `Content-Length` or chunked, as
  `request_shape.rs` already did. Replying to a body still being written closes
  the connection under `ureq`, which then reports a broken pipe instead of the
  answer; a small body survives that on macOS and not on a Linux runner, which
  is a test that passes locally and fails in CI.

  The `HEAVY` list in `http.rs` decides only whether a refusal ends with "go to
  a heavy proxy". It is the cluster's `isHeavy` bit, so it covers commands only
  `raw_command` can send; **it must be reconciled with `Repeatable::Heavy` when
  #38 merges**, and the source carries that marker.
- **TLS trusts the Mozilla bundle unless told otherwise**, which is
  `webpki-roots` compiled in through `ureq`'s `rustls` feature. A bare host name
  in `YT_PROXY` means `https://`, so an on-premises installation behind a
  corporate CA was unreachable — `invalid peer certificate: UnknownIssuer`,
  where `curl` succeeds by reading the OS trust store. `YT_CA_BUNDLE` names a
  PEM file instead; the `platform-verifier` feature trusts what the OS trusts;
  the bundle wins where both are set. A **bundle that parses to no certificates
  is refused**, naming the file — the fallback would be the same silent
  `UnknownIssuer`. Verified against a real multi-node installation with a
  three-deep, self-signed chain (#29); a local cluster is plain HTTP and cannot
  exercise any of it.
- **PEM is an envelope and proves nothing about what is inside it.**
  `ureq::tls::parse_pem` splits the sections and base64-decodes them; the
  `Certificate` it hands back is documented as unvalidated, and `rustls`'
  `add_parsable_certificates` then **discards what it cannot parse and reports
  the count to nobody**. So a PKCS#7 `.p7b` re-armoured under a
  `BEGIN CERTIFICATE` label — how a Windows-born bundle usually arrives — was
  accepted, produced an empty root store, and failed every request with the
  same `UnknownIssuer` the variable exists to end. `http::is_x509` checks the
  DER skeleton (`SEQUENCE { SEQUENCE, SEQUENCE, BIT STRING }`, and the head of
  the `TBSCertificate` inside it) and **one bad block refuses the whole file**;
  a `ContentInfo` parts company at its first member, which is an OBJECT
  IDENTIFIER rather than the `tbsCertificate` sequence. The bundle is also
  `stat`ed before it is opened, because opening a FIFO blocks for ever and
  `Client::new` is infallible with nothing above it to time a file read out.
- **A rejected certificate is not retried — for two of the reasons, not all of
  them.** It arrives as `ureq::Error::Io` of kind `InvalidData` wrapping a
  `rustls::Error`, rendered `invalid peer certificate: <CertificateError>`, and
  was retried five times as an ordinary transport failure, which put ~15 s of
  backoff in front of a verdict that cannot change. Only `UnknownIssuer` and
  the name mismatch are that verdict: both are decided by *this client's* roots
  and *this client's* URL, which the next attempt does not change.

  **Match the rendering, not the variant name.** `rustls` renders that error
  with `Display`, and `Display for CertificateError` writes prose for the
  context-carrying variants while falling back to `Debug` for the rest. So
  `UnknownIssuer` arrives under its own name, but a hostname mismatch arrives
  as `certificate not valid for name "…"; certificate is only valid for …` —
  and the webpki verifier builds *only* `NotValidForNameContext`, never the
  bare variant, so matching `NotValidForName` alone settles nothing in the
  default build. Both spellings are listed for that reason. Everything else stays retriable, and deliberately so —
  `rustls-platform-verifier` maps a failed revocation lookup or an unreadable
  trust store to `Other(…)` under the same prefix, and classifying those would
  make enabling `platform-verifier` a way of turning a transient OS condition
  into a permanent failure; `Expired` and `Revoked` are properties of the fleet
  member that answered, and a round-robin set mid-rotation may answer with a
  renewed one next time.

### The operation lifecycle

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

#### Pausing, repricing and finishing one — all measured

- **Suspension is not a state.** A suspended operation still reports `running`;
  the cluster keeps it in a separate `suspended` attribute. A poll loop that
  watches the state alone will never learn that an operation is paused, which is
  why `Client::operation_suspended` exists beside `operation_state`.
- **`suspend_operation` is idempotent, `resume_operation` is not.** Suspending a
  suspended operation answers `{}`; resuming one that is not suspended is
  refused with code 201, `Operation is in "running" state`. So suspend is the
  one mutating scheduler command here that is **retried** — and on its own
  idempotency, not under a mutation ID, which the master's cache would not cover
  anyway. The distinction from abort is what makes that safe: an abort *causes*
  the scheduler to let go, so its retry is guaranteed to fail, where a repeated
  suspend simply says the same thing twice.
- **`complete_operation` is not idempotent** — the second is answered code 200,
  `No such operation`, exactly as a second abort is. It ends the operation as
  `completed` rather than `aborted`, so its output is published and a waiting
  launcher is told the work succeeded.
- Once the scheduler has let go, *every* one of these answers `No such
  operation`. The rule is "the scheduler still has it", not "it has not
  finished".
- **`update_operation_parameters` takes its parameters in the header**, not in a
  body: the cluster's registry declares its input as `null`, though the command
  reference says "structured". It answers with **Content-Length: 0** — an empty
  body, where its neighbours send `{}`. It **assigns**, so the same update twice
  is the same as once; the client repeats it freely on that basis.
- A top-level `{weight=2.5}` is **spread into every pool tree**, landing at
  `runtime_parameters/scheduling_options_per_pool_tree/<tree>/weight`. An empty
  `parameters={}` is accepted with 200 and changes nothing, so the client
  refuses one rather than reporting success for a no-op.

#### Finding an operation again

- **`get_operation` accepts `operation_alias`** and refuses it without
  `include_runtime=%true`: *"Operation alias cannot be resolved without using
  runtime information"*. With it, a live alias resolves; a stale one falls
  through to `//sys/operations_archive/operation_aliases`, which a local cluster
  does not have. An alias is a spec field and must start with `*`.
- **`attributes=[]` asks for nothing** and is answered `{}`. Leaving the
  parameter out is what asks for everything — and everything is large: the full
  document for a one-job vanilla operation measured **119 KB**, mostly the
  resolved spec and the progress tree.
- **`list_operations` answers a flat multi-key document**, not the one-key
  envelope: `{operations=[…]; incomplete=%false; pool_tree_counts={}; …;
  failed_jobs_count=0}`. `progress` still carries `job_statistics` beside the
  newer `job_statistics_v2`, so the statistics readers are unaffected.
- **`list_operation_events` answers a bare list**, with no envelope at all — the
  same surprise the file-cache commands hold — and it is **empty on a cluster
  with no operations archive**, which is what a local one is. Only the *empty*
  bare list has ever been seen here, then: the shape of a non-empty answer is a
  guess, so the parser reads `{events=[…]}` too and refuses anything else rather
  than reporting a shape it does not know as "no events".
- **A sorted merge does not need `merge_by`.** Measured: two tables sorted by
  `host`, merged with `mode=sorted` and no `merge_by`, are accepted and the
  operation completes with the output `sorted_by=[host]` — the cluster takes the
  key from the inputs' own sort columns. `start_merge` used to refuse this spec
  on the assumption that the cluster would; it does not, and the refusal blocked
  an ordinary operation.
- **`get_job` answers the job document unwrapped** and calls the id `job_id`,
  where `list_jobs` wraps in `{jobs=[…]}` and calls it `id`. One parser reads
  both.
- **`get_job_input` never answers for a vanilla job**: 30 s, zero bytes. A
  vanilla operation has no input tables, and the cluster does not say so.

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

### Selecting columns and rows on a path

The read-side half of the same mechanism, and the sibling-parameter trap
generalised: `columns` and `ranges` are attributes **on the path** too, and the
cluster's answer to one in the wrong place is again a 200.

- **A read selection on a *write* is ignored and the whole table is replaced,
  with a 200.** Measured in both spellings: `write_table_rows("//tmp/t[#0:#2]",
  rows)` replaced everything and reported success, and a `write_table` whose
  path carried `ranges` as a typed *attribute* did exactly the same — 200, three
  rows replaced by one. Same shape as the append trap: an attribute in the wrong
  place costs a table and says nothing. Hence `TablePath::write_refusal`.
- **An unknown name in `columns` is not an error**: 200, with the key simply
  absent from every row. A typo reads clean and decodes short — loudly into a
  struct, silently into a map.
- **`columns=[]` is answered 200 with one empty map per row**, and it composes
  with a range: `<columns=[];ranges=[{lower_limit={row_index=0};
  upper_limit={row_index=2}}]>` came back as two empty maps, and the same range
  spelled with `key` bounds as three. That counts the rows of a *range* with no
  column bytes on the wire — something `@row_count` cannot do, speaking as it
  does for a whole static table — so the client **sends** it. The first round
  refused it by analogy with `update_operation_parameters({})`; the analogy was
  false, because that one is a mutation silently no-op'd and reported as
  success while this is a read that returns one correct record per row.
- **A negative `row_index` is clamped to 0, not rejected** — the correction that
  cost the most here, because the first measurement did not isolate the
  variable. `{lower_limit={row_index=-5}}` returned **all five rows** of a
  five-row table and `-5..2` returned rows 0 and 1: a negative lower limit reads
  exactly as `0` would. Only a negative *upper* limit comes back empty
  (`{upper_limit={row_index=-2}}` → 200, no rows), and that is the clamp too.
  The earlier "`-5..0` is answered 200 and no rows" was true of `upper_limit=0`,
  not of the negative bound. The client still refuses it — a bound arriving only
  from arithmetic that went wrong, silently replaced by one that reads the whole
  table, is worth an error — but for that reason, not the false one.
- **A backwards range is answered 200 with no rows, in either selector**:
  `{lower_limit={row_index=5};upper_limit={row_index=3}}` and
  `{lower_limit={key=[3]};upper_limit={key=[1]}}` both came back empty. The
  client refuses both, which it did not at first — it checked only row indices.
- **Measure the shape the client actually sends, not the flat text.** This one
  cost two wrong rounds. The client sends `path` as a **YSON string node with
  its attributes hung outside** — `<columns=[n]>"//tmp/t{k}"` — while a `curl`
  with JSON parameters sends the *flat text* `<columns=[n]>//tmp/t{k}` as one
  string. **They parse differently and give opposite answers.** In flat text
  the string's `{k}` wins; in the YSON shape the attribute wins. Reproduce the
  real one with `-H 'X-YT-Header-Format: <format=text>yson'` and parameters
  `{path=<columns=["n"]>"//tmp/t{k}";output_format=json}`. A JSON-parameter
  `curl` is not evidence about this client's behaviour.
- **The attribute beats the string when both spell the same kind of selection,
  silently, at 200.** Measured in the YSON shape, on a table `k,n`:
  `<columns=["n"]>"//tmp/t{k}"` → column `n` (the `{k}` discarded);
  `<ranges=[…0:2]>"//tmp/t[#3:#5]"` → rows 0–1 (the `[#3:#5]` discarded);
  `<columns=["k"]>"<columns=["n"]>//tmp/t"` → column `k`. **Different kinds
  compose**: `<columns=["n"]>"//tmp/t[#3:#5]"` → rows 3–4 carrying only `n`,
  and `<ranges=[…0:2]>"//tmp/t{k}"` → rows 0–1 carrying only `k`. So the client
  refuses the doubled *kind* and sends the pairing. Nothing here is corrupted —
  the read is exactly what the attribute asked for — the loss is that the
  caller's own half is thrown away with no mention, which is what the refusal
  is for.
- **There is no 400 in any of this.** Every combination above answers 200,
  including a string that opens with `<…>`: `"<columns=["n"]>//tmp/t"` alone
  → column `n`, and `<ranges=[…0:2]>"<columns=["n"]>//tmp/t"` → rows 0–1
  carrying only `n`, composing like any other different kinds. A leading `<…>`
  is still refused, but for the honest reason: **the client cannot parse the
  block to know which attribute it names**, and if it names the one being added
  the caller's is discarded silently. (The earlier "two blocks → 400, *does not
  start with a valid root-designator*" was the flat-text artefact — two `<…>`
  concatenated into one string — not anything this client can send.)
- **A `uint64` key column does not insist on the `u` suffix**: on a
  `uint64`-keyed table `{exact={key=[42]}}` and `{exact={key=[42u]}}` both
  returned the row. `yson_build::uint` earns its place on *range* instead —
  `Key::from(i64)` stops at `i64::MAX`, and the row keyed
  `18446744073709551615u` came back only for the uint spelling.
- **`key` and `key_bound` compare a short key by opposite rules**, which is the
  finding worth the most here. Under `key` the row's whole key is compared
  component-wise, the shorter tuple being smaller when equal so far. Under
  `key_bound` the row's key is **truncated** to the bound's length first, so
  every row sharing the prefix compares *equal* to it. On a table keyed
  `(host, path)` holding `(a,/x) (a,/y) (b,/x) (b,/y) (c,/x)`:

  | sent | rows back |
  | --- | --- |
  | `{key=[a]}` … `{key=[b]}` | `(a,/x) (a,/y)` |
  | `{key=[a]}` … `{key_bound=["<=";[b]]}` | `(a,/x) (a,/y) (b,/x) (b,/y)` |
  | `{key_bound=[">";[a]]}` | `(b,/x) (b,/y) (c,/x)` |
  | `{exact={key=[a]}}` | `(a,/x) (a,/y)` |

  So `a..b` and `a..=b` differ by a whole prefix group, and `>` on a prefix
  drops every row of that prefix — there is no "the row just after `a`".
- **A range entry mixing `key` on one side with `key_bound` on the other is
  accepted**, which is what `keys(a..=b)` sends. The reference documents the two
  selectors separately and never together; the cluster takes the mixture.
- All of the above is checked by `examples/rich_path.rs`.

### Tracing

Read out of the cluster's source — the HTTP reference does not mention the
header at all — and then **watched on a local cluster**, which answers every
question at once: the proxy puts the trace id it adopted in the `X-YT-Trace-Id`
of the response, so one `curl` per case says what it did with the header.

- **The proxy joins a caller's trace through a `traceparent` header**, the W3C
  one: `00-<32 hex trace>-<16 hex span>-<2 hex flags>`. Its parser is
  `TryParseTraceParent` in `yt/yt/core/http/helpers.cpp`. The flags are a byte:
  **bit 0 sampled, bit 1 debug**.
- All three official clients send it: C++ `FormatTraceParentHeader` (hard-coded
  `00-…-01`), Go `injectTracing`, Python `generate_traceparent` — the last on
  **every** request, with an id it generated itself.
- **The version may be left off.** `4bf92f35…-00f067aa0ba902b7-01`, three groups
  rather than four, is adopted exactly like the four-group form; the parser
  says so in a comment and the cluster agrees. That is what the Go SDK sends.
  **Uppercase hex is accepted** too. This client is liberal in and strict out:
  it parses both and always sends lowercase, four groups.
- **A malformed header is ignored in silence** — answered 200, with a trace id
  the proxy made up. Hence `TraceContext::parse` refusing one instead: a trace
  missing the half that mattered looks exactly like a trace nobody asked for.
- **The header's trace id and the cluster's GUID spelling are the same four
  32-bit groups in the same order**, differing only in the dashes and in the
  leading zeros the cluster drops. Sent and echoed, on a local cluster:

  | sent in `traceparent` | echoed in `X-YT-Trace-Id` |
  | --- | --- |
  | `4bf92f3577b34da6a3ce929d0e0e4736` | `4bf92f35-77b34da6-a3ce929d-e0e4736` |
  | `00000001000000020000000300000004` | `1-2-3-4` |
  | `00000000000000010000000000000002` | `0-1-0-2` |

  A group that is all zeros keeps **one** digit, never none.
  `TraceContext::yt_trace_id` reproduces this, and those are its test cases.
- `X-YT-Correlation-Id` (request) and `X-YT-Request-Id` / `X-YT-Proxy`
  (response) are the documented, non-trace way to find a request in the proxy
  log. Not sent or read yet — and reading either would mean handing response
  headers back, which no method does today.

### Where a heavy command goes

Observed on a real multi-node installation rather than a local Docker one — see
[#30](https://github.com/sshaplygin/ytsaurus-rs/issues/30) — and then read back
out of the documentation and the cluster's own source, because the observation
and the documentation disagreed and the source is what settles it.

- **A control proxy will not serve a heavy request, and what it does instead
  depends on whether the request carries input data.** The rule is
  `TContext::TryRedirectHeavyRequests` in
  [`yt/yt/server/http_proxy/context.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/server/http_proxy/context.cpp):

  | Request | Answer |
  | --- | --- |
  | heavy **with** input data — `write_table`, `write_file` | **503** + `Retry-After: 60`, carrying `Control proxy may not serve heavy requests with input data` |
  | heavy **without** — `read_table`, `read_file`, `get_job_input`, `get_job_stderr` | **307** to a data proxy |
  | heavy read, no data proxy available | **503**, `There are no data proxies available` |
  | any of them with `X-YT-Suppress-Redirect` | served by the control proxy after all |

  The `inDataType` column of the driver registry is exactly that test, so the
  split is mechanical rather than a judgement.

  **This crate recorded the refusal as an HTTP 200 and that was wrong.** The
  status was never observed: `ClientError::Cluster` renders as `{command}:
  cluster error {code}: {message}` and does not print the status at all, so a
  503 carrying an `X-YT-Error` header looks exactly like a 200 carrying one.
  Only the error string is first-hand here.

  The documentation gives both halves of the rule and neither whole. The
  [`/hosts` section](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)
  says "When you try to execute a heavy command, light proxies return code
  503"; the return-code table on the same page lists "307 — Redirecting heavy
  queries from light to heavy proxies".
- **The role that refuses is spelled `control`, exactly.**
  `TCoordinator::CanHandleHeavyRequests` is `Role != "control"`, so a proxy
  with any other role — including `default` — serves heavy commands.
- **A deployment behind a balancer is the case that breaks**, not the case that
  works: the balancer fronts the *control* proxies, so every upload arrives at
  one. Pointing `YT_PROXY` at an address from `/hosts` makes the same examples
  pass unchanged, which is what proved the cause.
- **`/hosts` answers a JSON list of bare host names, best first.** The
  [HTTP proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)
  shows the wire form — `["n0008-sas.cluster-name", …]` — and the
  [reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)
  the ordering: "ordered by load … the very first proxy in the resulting list
  is the least loaded". No scheme and usually no port, so both come from the
  address the caller configured. (`TCoordinator::ListProxies` shuffles the
  better half of the list before returning it, so "first" means "one of the
  good ones", not "the best".)
- **"Defaults to the `data` role" is not in the documentation** — it was
  asserted here without one for a release. It is `default_role_filter`, a
  coordinator **config parameter**, defaulted in `TCoordinatorConfig::Register`
  to `NApi::DefaultHttpProxyRole`, which
  [`yt/yt/client/api/public.h`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/api/public.h)
  spells `"data"`. A compiled-in default an operator can change, then, not a
  protocol guarantee — which is why this client validates what it is handed
  rather than trusting the role. `?role=`, `/hosts/all` (which alone shows
  banned and dead proxies) and the plain-text form selected by an exact
  `Accept: text/plain` are all source-only too.
- **The documentation asks for the list to be re-queried, and this client now
  does.** From the [proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload):
  "A good strategy is to re-query the `/hosts` list every minute or every few
  queries and change the current proxy to which queries are made." The whole
  answer is kept as a pool (`Transport::base_for`), each heavy command picks a
  member **at random** — `/hosts` is ordered by load (better half shuffled,
  see above), and a client that keeps its one pick for life never rebalances —
  and the command that finds the answer older than
  `with_host_list_refresh_interval` (one minute by default) re-asks first,
  lazily, the way the C++ `THostManager` does; no background thread, and a
  failed refresh keeps the previous answer in use and waits out another
  interval. "The cluster named nobody" expires on the same interval rather
  than settling for ever, so a first lookup that lands during a rolling
  restart is not a verdict for life. The ask-once lifetime pin this replaced
  was recorded as a deliberate deviation in
  [docs/sdk-comparison.md](docs/sdk-comparison.md) and retired by #40.
- **A failed heavy command drops the host it used from the pool, not back to
  the configured address.** The distinction is the whole feature: with separate
  roles the configured address is a control proxy, so falling back there
  answers one transient 503 with a window of guaranteed refusals — issue #30
  reproduced on demand. Only a pool with nobody left in it falls back, and
  then for `HOSTS_RETRY_AFTER` before the cluster is asked again; a refresh
  is what restores a dropped host. The drop turns on
  `retry::attributable_to_the_host`, **not** on `worth_asking_again`: the two
  agree except about a rejected certificate, and that gap was #40's pinning
  failure — `NotValidForName` is a verdict about *one host's name*, not about
  the coordinator's list, so gating the drop on the lookup's predicate left
  the client pinned to the one bad proxy in the fleet. A proxy that refuses
  heavy work *because of its role* counts as the host's fault too, which
  `is_retriable` alone cannot say; that is why the predicates are three.
- An empty or absent list means "the configured address serves everything",
  which is what a single-node cluster is. **A cluster on loopback is not asked
  at all**: what it publishes for itself is an address behind the port mapping
  or tunnel that `localhost` stands for, and following it would break every
  upload that works today. That last point is reasoning, not a measurement —
  no local cluster's `/hosts` answer has been captured here.
- **A name from `/hosts` is checked before it is used.** The documentation says
  nothing at all about which hosts a token may be sent to — the `/hosts` flow
  and the `Authorization: OAuth` header are documented on the same page and
  never connected — so the answer is this client's to choose: same domain as the
  configured address, scheme and port from the configured address, no `://`,
  `/`, `@` or whitespace, and a bracketed name only for an IPv6 literal (`ureq`
  3.3 hands `[not.an.ip]` straight to the resolver, so accepting it buys a
  permanently unresolvable address).
  **The domain rule is a typo guard, not a token boundary** — say so when
  writing about it. Steering the `/hosts` body means owning the proxy (which
  has the token) or the wire (which reads it off every light command), and a
  suffix rule with no public-suffix list treats every tenant of a hosting
  platform as a neighbour. `Client::with_heavy_proxies_in` is the version that
  is a boundary; `Client::with_heavy_proxies_anywhere` removes the rule.
- **A configured name with no dot is matched as a label.** `YT_PROXY=hume` is
  the ordinary spelling and has no parent domain, so the domain rule
  degenerated to "the name itself" and refused
  `["n0008-sas.hume.yt.example.net"]` in full and for good. It now has to appear
  as a label of the discovered name and not as its leftmost one. That rule was
  also **out of reach without a resolver search list** until `YT_PROXY_SUFFIX`
  existed: it only fires for a dotless `YT_PROXY`, and a dotless `YT_PROXY`
  became `https://hume`, which resolves nowhere unless the machine's own DNS
  configuration completes it. Two features that each worked alone depended on a
  third thing neither of them owned.
- **The domain rule has a middle setting, because a real installation needed
  one.** A managed installation answered `/hosts` with 79 heavy proxies in a
  zone of its own — a domain the configured address does not share — so the
  default rule refused every one of them and no heavy command could be sent at
  all: `Control proxy may not serve heavy requests with input data`. Listing 79 names by hand goes stale the moment one
  rotates, and removing the rule is the whole rule. `with_heavy_proxies_under`
  is the third answer: the configured address's domain **plus** the ones named.
  It is still a suffix rule and still worth what a suffix rule is worth — the
  boundary is `with_heavy_proxies_in`. Note for anyone weighing the default: the
  **Go SDK filters `/hosts` not at all** (`listHeavyProxies` returns the list
  verbatim, `proxy_set.go` adds every name), so `with_heavy_proxies_anywhere` is
  not a weakening relative to the official client — it *is* the official
  client's behaviour, and this client is the stricter of the two.
- **`Client::from_env` is the only constructor the examples use**, so anything a
  cluster can differ in has to be reachable from the environment or it cannot be
  run against. `YT_PROXY_SUFFIX`, `YT_HEAVY_PROXY_DOMAINS`,
  `YT_HEAVY_PROXIES_ANYWHERE` and `YT_FILE_CACHE` exist for that reason and for
  no other; each is inert unset. Adding a `with_…` knob without one is how the
  suite came to need a source patch to run on a managed installation at all.

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
  **reader uncapped**, which is the right way round: the buffered path enforces
  a limit of its own, the streaming path none. `ureq`'s number is not that
  limit and cannot be — see the API-shape note above on where `limit()` sits.
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

### Batched commands

`execute_batch` sends several light commands in one request — `BatchRequest` +
`Client::execute_batch`, exercised by
`cargo run -p ytsaurus-client --example batch`. All of the following was watched
on the local cluster, not read:

- **The saving is real and it is the whole point.** Twelve `create`s went as
  **one** request in **9.16 ms**, against **140.77 ms** for the same twelve sent
  one at a time — measured through a counting TCP relay in front of the proxy,
  because a client has no way to count its own round trips. `tests/batch.rs`
  pins the count against an in-process socket; a program on a cluster cannot,
  which is why the example does not claim it.
- **An envelope `transaction_id` is silently dropped.** `TExecuteBatchOptions :
  TMutatingOptions` has no transactional half, so a batch stamped with one
  created its node **outside** the transaction — visible at once, untouched by
  the abort. The silent escape a transaction exists to prevent. The client
  stamps each **part** instead, which the cluster honours, and `execute_batch`
  is on the `NO_TRANSACTION` list so the blanket stamp cannot dress the envelope
  up in a parameter known to mean nothing.
- **A part naming an unknown command fails the whole batch** — HTTP 400,
  `Unknown command "frobnicate"`, and **no per-part results at all**, because
  the driver resolves each command's descriptor before per-part error handling
  begins (`TRequestExecutor::Run` throws first).
- **…and that refused batch ran every part. It is not a race.** The failure
  destroys the *answers*, not the work. `TExecuteBatchCommand` collects the
  sub-requests into callbacks, runs them all through
  `CancelableRunWithBoundedConcurrency`, and only then calls `.ValueOrThrow()`
  on the collected list, which discards every result together as soon as one is
  the unknown-name throw — **dispatch is never aborted**. Probed five ways, and
  none of the obvious mitigations mitigate:

  | batch | HTTP | applied |
  | --- | --- | --- |
  | `[create a1, frobnicate]` | 400, no results | `a1` exists |
  | `[frobnicate, create b1]` — bad part **first** | 400 | `b1` exists |
  | `[create c1, frobnicate, create c2]` | 400 | **both** exist |
  | `concurrency=1`, `[frobnicate, create d1, create d2]` | 400 | **both** exist |
  | `concurrency=1`, 8 creates then `frobnicate` | 400 | **all 8** exist |

  Putting the bad part first does not limit the damage and neither does
  `concurrency=1`. So a wholesale failure says nothing about what was applied,
  and there are no per-part results to ask — which is why
  `Client::execute_batch` reports the prefix it was *answered* for and does not
  claim to know what landed, and why `ClientError::BatchInterrupted`'s own
  one-liner refuses to call that prefix "applied".
- **The one bound that does hold: parameter parsing versus execution.** A batch
  refused while its parameters are being read runs **nothing**; a batch that
  reaches execution runs **all of it**. Measured with a `create` sitting in each
  refused request:

  | probe | message | applied |
  | --- | --- | --- |
  | `concurrency=0` + create | `Validation failed at /concurrency` | **nothing** |
  | part missing `command` | `Error loading parameter /requests` | **nothing** |
  | part `parameters` not a dict | `Error loading parameter /requests` | **nothing** |
  | `requests` not a list | `Error loading parameter /requests` | **nothing** |
  | `requests` missing | `Missing required parameter /requests` | n/a |

  This is the only fact that lets a caller reason about a 400 at all: the
  message tells you which side of the line you are on.
- **Parts run in parallel, and the documentation means it.** A batch that
  created a node and asked `exists` about it in the same breath was answered
  `%false`: both parts succeeded, and the read simply ran first. A part and its
  consequence belong in two batches.
- **A batch replayed under one mutation id is deduplicated per part.** The
  driver hands part *k* the batch's id plus *k*
  (`NRpc::GenerateNextBatchMutationId`, `++id.Parts32[0]`) and stamps the
  batch's `retry` flag into every volatile part. Measured with parts that carry
  **no `ignore_existing`** — `BatchRequest::create_table`, not
  `BatchRequest::create` — because that is the only spelling where the result
  means anything:

  ```text
  first  (id)          : ["2-2e82-10191-d4fdeff4", "2-2e83-10191-b0f0b0cd"]
  replay (id, retry)   : ["2-2e82-10191-d4fdeff4", "2-2e83-10191-b0f0b0cd"]   IDENTICAL
  fresh  (new id)      : [501 "already exists", 501 "already exists"]
  ```

  **Do not run this check with `BatchRequest::create`.** It sends
  `ignore_existing`, so a second send answers with the *old* node's id whether
  or not a replay was recognised — measured, a two-`create` batch under a
  **fresh** id returned ids identical to the first send's, which looks exactly
  like a deduplicated replay and is not one. One id covers **one** request,
  though: because the per-part ids are derived by incrementing, a second request
  under anything derived from the same id would collide with the first request's
  parts, so a split batch carrying a caller's id is refused.
- **`isHeavy` is not what decides whether a command can be a part — the data
  types are.** The driver throws `Command %Qv cannot be part of a batch since it
  has inappropriate output type %Qlv` before any part runs, so one such name
  fails the whole request. Measured against the registry the cluster serves at
  `GET /api/v4` (190 commands) and confirmed name by name: a part is refused
  when its **output type** is `tabular` or `binary`, or its **input type** is
  `binary` — 21 names, against the 7 on the crate's `HEAVY` list. The two lists
  differ in *both* directions: `get_job_spec` is `is_heavy: true` and was
  **accepted** as a part (ordinary per-part error), while `alter_query` and
  `push_queue_producer` are `is_heavy: false` and are refused. And `write_table`
  — `is_heavy: true`, input `tabular`, output `structured` — was **accepted and
  applied**: a `write_table` part with its rows in the part's `input` wrote
  them, so the crate's refusal of it is the crate's own policy (bulk data does
  not belong inline in a batch body headed for a light proxy) and not the
  cluster's. `select_rows` and `lookup_rows` are the names a caller would
  plausibly try; both are refused, and `[create x1, select_rows]` was answered
  400 `inappropriate output type "tabular"` **with `x1` created anyway**. Two
  more whole-batch refusals no name list can catch: a part whose command takes
  input and is given none (`Command %Qv requires input`, measured for
  `insert_rows`, `write_table` and seven others), and an unknown name.
- **No modelled command answers a bare `{}` under API v4.** Measured one part
  apiece: `create` → `{"output":{"node_id":…}}`, `set` → `{"output":{}}`,
  `remove` → `{"output":{}}`, `exists` → `{"output":{"value":false}}`. The bare
  `{}` the command reference's example shows for a `set` belongs to **v3**: the
  registry the cluster serves at `GET /api/v4` lists `remove` and `set` as
  `output_type: structured`, and only `/api/v3` lists them `null`. So the
  registry's output-type bit does not separate `set` from `create` on the
  version this crate speaks, and a guard built on it — refusing only a bare `{}`
  — is dead against its own motivating scenario, because the shape a v4 cluster
  would really produce is `{"output":{}}`, and that is a legitimate `set`
  success the parser cannot tell from a broken `create`. The parser therefore
  checks **the key the answer carries** (`node_id` for `create`, `value` for
  `exists`/`get`/`list`), which catches a `create` with no `node_id` however it
  is wrapped.

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
- **The create on that miss branch is the half a managed cache refuses**, with
  **code 901**, `Access denied … "write | modify_children" … not allowed by any
  matching ACE` — found on a real multi-node installation (#32) and invisible on
  a local one, where the caller is root. `upload_worker_cached` treats a 901 on
  the cache's own writes — creating the directory, creating the staging node in
  it, `put_file_to_cache` — as an unusable cache, uploads under `//tmp` instead
  and warns, naming `Client::with_file_cache`. A 901 anywhere else, and any
  other error, still fails the upload. Neither branch has been run against a
  cluster that denies anything; `crates/ytsaurus-client/tests/file_cache.rs`
  scripts the refusals a socket in-process can.
- **`CachedFile::cached`, not `uploaded`, is which node the caller is holding.**
  `uploaded` is true both for a file the cache accepted and for one that went to
  `//tmp` because the cache would not, so a launcher tidying up on that signal
  deletes the installation's *shared* cache entry and evicts the binary for
  everyone. The fallback node is an ordinary `//tmp` node — whatever ACL `//tmp`
  carries, no expiry, and a name whose entropy is documented as *unique, not
  unpredictable* — so a co-tenant who can list `//tmp` can rewrite the worker
  between the upload and the exec. That is the ordinary exposure of `//tmp`, and
  the reason `with_file_cache` pointed at a directory of your own beats
  accepting the fallback as a settled state.
- **A managed cache is read-only to an ordinary user, `remove` included** —
  which the client survives and an *example* need not. On
  a managed installation, `check_permission` on
  `//tmp/yt_wrapper/file_storage/new_cache` answers `read allow`, and `write`,
  `remove` and `create` all `deny`. `upload_worker_cached` degrades to a plain
  upload, as above; `cached_upload`'s setup step, which clears its own entry so
  the first call is a real miss, is refused with code 901 and nothing degrades
  it. The example therefore brings a cache of its own — **beside** its `BASE`
  and not under it, since it removes that tree whole on every run and a cache
  inside would be gone before the clearing step could find anything in it — and
  `YT_FILE_CACHE` points it back at a shared one. A demonstration that has to
  clear a cache has to own one, and has to let it outlive the run.
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
- **Never assert on the rendered text of a *generated* value.** The text YSON
  writer omits the quotes when a string looks like an identifier — first byte a
  letter or `_`, the rest alphanumeric or `_-.` (`ser::is_safe_unquoted`). A
  mutation ID is a hex GUID with no leading zeros, so `ebd6e011-…` goes on the
  wire bare and `3f2a1b-…` goes quoted, decided by its first hex digit:
  **measured at 39.8 % unquoted over 100 000 IDs**. A wire-level test that
  matched `mutation_id="…"` therefore passed the first run and failed two in
  five afterwards. Decode the `X-YT-Parameters` header and compare values, as
  `tests::sent_parameters` does; matching a *fixed* literal like
  `transaction_id="3-5d231-…"` is safe because its spelling cannot change.
  Both forms are valid YSON and the cluster takes either.

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
3. **Cluster e2e** — against a local YTsaurus in Docker, two readings of the
   same three checks. `cargo run -p ytsaurus-client --example e2e` drives them
   through this crate and needs no Python; `tests/e2e/run_e2e.sh` drives them
   through the official Python client and so checks the worker's output against
   a **different implementation** rather than against ourselves. Keep both — the
   second is the only place anything here is read by code we did not write.
   Neither is in CI (needs a multi-GB image). See
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
  public, CI green, tagged `v0.2.6`.
- **crates.io**: all six crates at **0.2.6**, released together —
  [`ytsaurus-yson`](https://crates.io/crates/ytsaurus-yson),
  [`ytsaurus-skiff`](https://crates.io/crates/ytsaurus-skiff),
  [`ytsaurus-format`](https://crates.io/crates/ytsaurus-format),
  [`ytsaurus-helpers`](https://crates.io/crates/ytsaurus-helpers),
  [`ytsaurus-job`](https://crates.io/crates/ytsaurus-job) and
  [`ytsaurus-client`](https://crates.io/crates/ytsaurus-client). The version is
  the workspace's, so they move as one. `ytsaurus-skiff` and `ytsaurus-format`
  were `publish = false` until 0.2.5 and are **still pre-release**; they are on
  the registry only because the two crates above them depend on them.
  `ytsaurus-yson` and `ytsaurus-job` were on crates.io at 0.1.0 and 0.2.0
  before this, and `ytsaurus-client` at 0.2.0.
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

**What is left of the backlog is not code.** P3 #15 (tracing spans) was written
down as "only worth doing if a user asks" — **one did, and it is built**.
Logging and tracing were ranked first of the pinned parity issue (#8), ahead of
everything else, because a production deployment needs to see what the client is
doing; **that supersedes the P3 #15 ranking**, which should not be read as the
current order of anything.

What shipped is two halves with different prices. `TraceContext` and
`Client::with_trace_context` send a W3C `traceparent`, which costs no
dependency and is always compiled in — the cluster is the instrumented party,
and this only tells it which trace to join. The `tracing` feature adds a span
per attempt and is **off by default and absent from musl worker builds**, for
the reason `tls` is — but it *adds*: with it on and no subscriber installed,
the stderr line is still printed, because Cargo unifies features across the
graph and a launcher does not get to decide alone whether this is on. The retry
reporting goes through whichever is compiled and still mutes itself inside a
job; that muting is load-bearing and covers both.
See *Tracing* under *Protocol reference* for what was read out of the cluster's
source.

TLS is the one part of P3 #14 a local cluster cannot exercise. Everything else
needs a human — see below.

### Parked — needs a human and a real cluster

- **Skiff go/no-go — now about the default, not about the crate.**
  `ytsaurus-skiff` and `ytsaurus-format` exist and Skiff is selectable end to
  end: worker I/O, operation specs and direct table I/O. Binary YSON is still
  what every spec renders unless a caller asks otherwise, both crates are
  published-but-pre-release from 0.2.5, and which compatibility gates are still
  open is
  [`docs/skiff-compatibility.md`](docs/skiff-compatibility.md). The reference
  implementation is the `skiff` package in the
  [Go SDK](https://pkg.go.dev/go.ytsaurus.tech/yt/go), pinned at v0.0.33.
  Making it the default is what still needs a human: job-path benchmarks exist
  ([`docs/benchmarking.md`](docs/benchmarking.md)) but that decision needs a
  ≥ 10 GB table and C++/Python baselines. Decoding is 66 % of job CPU for a job
  that does nothing else, which is the worst case for YSON, not a verdict — and
  for the pilot, a job that does something with its rows, `cargo run -p
  ytsaurus-client --example profile` says **~10 % on the local Docker cluster
  and 36 % on a production one**. The threshold is 30 %, and the two
  readings sit either side of it: **the question is open, not settled and not
  lost**. The production cluster's fixed costs are a fifth of the emulated
  local one's — 474 ms against 2225 ms to be handed the rows — and decoding is
  the part that did not shrink with them, which is
  the shape to expect — but both readings scatter by 2× across rounds, so what
  is owed is a spread from repeated production runs, not a third single number.
  Do not quote the 10 % on its own again.
- **Upstreaming** to
  [ytsaurus/ytsaurus-rust-sdk](https://github.com/ytsaurus/ytsaurus-rust-sdk) —
  the maintainers' stance in ytsaurus#6 is "PRs welcome". **Do not start without
  a go-ahead.**
- **Contacting the yson-rs author** about co-ownership or publishing.

A test cluster with real data is needed from a human for the Skiff comparison;
a local Docker cluster is enough for everything else.

## Non-goals

Non-Linux targets. *(The RPC proxy, the protobuf row format and dynamic tables
were here until a human asked for them; `ytsaurus-rpc` implements a deliberately
narrow slice of all three and is pre-release. Streaming table I/O over RPC, the
gRPC proxy, chaos/replication, queues and Query Tracker remain non-goals.)* *(Custom job statistics were on this list until the backlog
ranked them P1 #7 — a human decision, and they ship now as `JobStatistics`.
Publishing to crates.io was on it too, and is now done, at 0.2.5, by the same
kind of decision; Hard rule 1 still governs every release after it.)*

## Reference

[YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson) ·
[control attributes](https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration) ·
[table switch](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/table-switch) ·
[operation options](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options) ·
[Try YTsaurus](https://ytsaurus.tech/docs/en/overview/try-yt) ·
[ss123she/yson-rs](https://github.com/ss123she/yson-rs) ·
[interop-tests](https://github.com/ss123she/yson-interop-tests)
