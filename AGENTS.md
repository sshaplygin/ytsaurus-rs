# AGENTS.md

Context for coding agents working on ytsaurus-rs. Read this before changing
anything.

## What this is

Rust clients for [YTsaurus](https://ytsaurus.tech), which has no official Rust
SDK. The main product is working with tables from Rust. `ytsaurus-client`
speaks the HTTP API: static tables, Cypress, transactions, files, batched
commands and operations. Dynamic tables are reachable over HTTP and over the
RPC proxy (`ytsaurus-rpc`) through one interface, `ytsaurus-api`'s
`TableClient`. The HTTP client's wire formats are YSON and Skiff; the RPC
client wraps requests in protobuf and sends rows as attachments in the YTsaurus
row wire format, which is neither. `ytsaurus-job`, a runtime for writing
MapReduce workers in Rust, is secondary to the clients.

## Layout

| Path | What it is |
| --- | --- |
| `crates/ytsaurus-client/` | HTTP API v4 client: static tables, dynamic tables through `ytsaurus-api`'s `TableClient`, Cypress, transactions, files, batched commands, and operations (upload a worker, start it, wait, report why it failed). |
| `crates/ytsaurus-api/` | Transport-independent client interface: `TableClient` and the row model both transports speak. Mirrors `yt/yt/client/api` in the C++; lets `create_client` (HTTP) and `create_rpc_client` (RPC) return the same thing. Published from 0.3.0, pre-release: the interface has not settled and is the most expensive thing here to change later. |
| `crates/ytsaurus-rpc/` | RPC proxy client: bus framing, the RPC envelope, the dynamic-table row wire format. Async on tokio, unlike the other crates. Published from 0.3.0, pre-release; gates in [docs/rpc-compatibility.md](docs/rpc-compatibility.md). |
| `crates/ytsaurus-proto/` | Protobuf bindings for the RPC proxy, generated from the upstream `.proto` files in the `third_party/ytsaurus` submodule. The generated Rust is committed and there is no build script, because `cargo package` does not walk into a submodule. Regenerate with `cargo xtask generate-protos`; CI fails on a diff. Published from 0.3.0. |
| `crates/ytsaurus-helpers/` | Proc-macro crate: `#[derive(TableRow)]` infers a table schema from a struct. |
| `crates/ytsaurus-yson/` | YSON codec (text + binary). Fork of [ss123she/yson-rs](https://github.com/ss123she/yson-rs) @ `ba2044c`. |
| `crates/ytsaurus-skiff/` | Skiff schema, format and bounded streaming codec. Pre-release, published from 0.2.5 because `ytsaurus-job` and `ytsaurus-client` depend on it. The ship gates in [docs/skiff-compatibility.md](docs/skiff-compatibility.md) are not all green; the API may change in a patch release. |
| `crates/ytsaurus-format/` | `DataFormat`: the one format selection shared by launcher and worker. Pre-release, published from 0.2.5 with `ytsaurus-skiff`. |
| `crates/ytsaurus-job/` | Job runtime: streaming reader, control records, multi-table output. Reads and writes YSON or Skiff. |
| `docs/` | [protocol-reference.md](docs/protocol-reference.md) (protocol and cluster behaviour), [writing-a-job.md](docs/writing-a-job.md) (user guide), [benchmarking.md](docs/benchmarking.md) (measurements and the Skiff decision), [skiff-compatibility.md](docs/skiff-compatibility.md) (Go SDK compatibility and every gap), [go-parity.md](docs/go-parity.md) (every Go SDK example mapped onto this repo), [sdk-comparison.md](docs/sdk-comparison.md) (the C++ and Go clients beside this one), [rpc-compatibility.md](docs/rpc-compatibility.md) (what the RPC client implements and its divergences), [format-comparison.md](docs/format-comparison.md) (YSON, Skiff and YQL on one task: plan, three nine-round cluster runs, a refuted pre-registered prediction, an adversarial review). |
| `tests/cluster-e2e/` | Cluster scripts and captured golden fixtures. |
| `tests/rpc-go-interop/` | Version-pinned Go program that produces byte vectors for the RPC row wire format and CRC-64, consumed by the Rust tests. Same shape as `tests/skiff-go-interop/`. |
| `third_party/ytsaurus` | Submodule: the YTsaurus monorepo, sparse-checked-out for its `.proto` files (`./scripts/init-protos.sh`). Needed only to regenerate `ytsaurus-proto`, never to build it. |
| `xtask/` | Repository tasks, `cargo xtask <task>`. Holds `generate-protos`. Never published. |
| `scripts/build-worker.sh` | Static musl worker builds. |

## Fixed decisions: do not revisit without a human

| Decision | Value |
| --- | --- |
| Repository name | ytsaurus-rs |
| Crate names | `ytsaurus-*` prefix: `ytsaurus-yson`, `ytsaurus-job`; later `ytsaurus-skiff`, `ytsaurus-client` if needed |
| YSON foundation | fork of ss123she/yson-rs pinned to `ba2044c711cefa65259e25122fea21c36f451093` (2026-04-01, v0.1.3) |
| Licence | Apache-2.0 for this project. Upstream yson-rs is MIT OR Apache-2.0; we elect Apache-2.0 and keep upstream's licence files and notices. |
| Job data format | Binary YSON (`<format=binary>yson`) is the default everywhere. Skiff is implemented, selectable through `DataFormat`, and pre-release; making it the default is the open question under Status, not something this decision allows. |
| Worker builds | `x86_64-unknown-linux-musl`, fully static; `lto = "fat"`, `codegen-units = 1`, `strip = "symbols"`, `panic = "abort"`, the last only for worker binaries, never for library crates |
| Operation launch | `ytsaurus-client` (this repo), or the `yt` CLI. |
| Repo layout | single Cargo workspace |
| Python tooling | ruff lints and formats; ty type-checks; uv runs (all three from astral). No `venv`, no bare `pip install`, no second formatter. The root `pyproject.toml` configures ruff and ty and declares no package; pass `--no-project` to `uv run`. |
| Language for automation | Python for everything that computes, parses or asserts. bash for glue only: sequencing external commands, with no source of another language embedded in it and none inlined into a workflow. Rust (`xtask`) only where required: `generate-protos`, because `prost-build` is a Rust library API; nothing else qualifies. Go only in `tests/*-go-interop/`, as an oracle this project did not write. `tests/skiff-cpp-interop/cpp_reference.py` stays Python: `yt_yson_bindings` is a compiled C extension over upstream's own Skiff. No new language without a human; JavaScript is not coming back. |

## Hard rules

1. **Publish nothing to crates.io** without explicit human approval. Never claim
   the `yson-rs` crate name; it belongs to the upstream author.
2. `ytsaurus-yson` is vendored, not a git dependency. This project is
   Apache-2.0, so its own §4 obligations apply to the vendored code:
   - keep upstream's `LICENSE-APACHE` and `LICENSE-MIT` where they are,
     unedited: they are notices received with the code;
   - keep [`NOTICE`](NOTICE) and
     [`crates/ytsaurus-yson/NOTICE`](crates/ytsaurus-yson/NOTICE) accurate, and
     credit the source repo and revision in the README and Cargo `description`;
   - record every change in
     [`crates/ytsaurus-yson/CHANGELOG.md`](crates/ytsaurus-yson/CHANGELOG.md),
     which is the statement of modifications §4(b) requires.
3. Protocol facts are verified against the official YTsaurus documentation and
   against a real cluster. If code and docs disagree, re-read the docs first,
   then change the code. Cite the doc at the point of use. Client behaviour the
   protocol does not dictate (retries, routing, proxy selection, banning,
   refresh) is checked against the official clients' source before it is
   designed here: C++ (`yt/cpp/mapreduce`), Go (`yt/go`), and the Python
   wrapper where it is the reference (the retry list). Where they disagree, say
   which was followed and why; a deviation from both is a deliberate decision
   recorded in [docs/sdk-comparison.md](docs/sdk-comparison.md). Read
   [docs/go-parity.md](docs/go-parity.md) before adding client API.
4. Every change ends with green CI: `cargo fmt --check`, `cargo clippy
   --all-targets -D warnings`, `cargo test`, `cargo test --doc`.
5. No scope creep. Non-Linux targets are out of scope until a human decides
   otherwise. A human added custom job statistics (`JobStatistics`). A human
   also brought the RPC proxy, the protobuf row format and dynamic tables into
   scope, for transactions, `lookup_rows`, `select_rows` and `modify_rows`
   only, not the other 150 request types; see
   [docs/rpc-compatibility.md](docs/rpc-compatibility.md). `ytsaurus-rpc`
   implements those; dynamic tables are also served over HTTP; the protobuf
   row format is not implemented.

## Writing

Applies to everything committed or published: docs, rustdoc, comments,
CHANGELOGs, commit messages and PR bodies. `scripts/check_prose.py` enforces
the countable part in CI; the rest is on review.

1. One home per fact. Protocol and cluster behaviour:
   [docs/protocol-reference.md](docs/protocol-reference.md). Measurements and
   method: `docs/benchmarking.md` and `docs/format-comparison.md`. What changed
   for a caller: the crate's CHANGELOG. An item's contract: its rustdoc.
   History: git. Open work: the issue tracker; this file names no issue
   numbers, because they go stale. Anywhere else, link to the home instead of
   restating it.
2. State results as they are. A null or negative result is written as one:
   "X did not decide Y; Z is still needed." Do not present it as a
   finding, a success, or a question that is still "not lost".
3. A reversal is one line: what was believed, what is true, the evidence. No
   defence of the earlier position.
4. No narration of the process ("this cost two rounds", "the first measurement
   did not…") and no aphorisms or morals. If a rule matters, write the rule.
5. Evidence once, attached to the fact: "observed on a local cluster",
   "measured: 611 522 bytes". Not as rhetoric, and never when it was not done.
6. Code comments describe the code as it is: no issue numbers, no "used to",
   no pull-request history, nothing addressed to a reviewer. A known gap goes
   into an issue.
7. Length. A CHANGELOG entry is at most four lines. An item's rustdoc is its
   contract (what it does, `# Errors`, `# Panics`, an example); past 25 lines
   the rest belongs in `docs/`. Bold marks a warning, not emphasis.

`scripts/prose_budget.json` holds the counts for files still over the
defaults. Budgets only go down: `--tighten` lowers them after a cleanup.
Raising one, by hand or with `--bootstrap`, shows in the diff of that file
and is for a reviewer to accept.

## Commands

```sh
./scripts/init-protos.sh          # only to regenerate protos: the submodule, shallow and sparse
cargo xtask generate-protos       # rewrite crates/ytsaurus-proto/src/generated/ from it

python3 scripts/check_package_includes.py   # no published file may include_str!
                                            # data from outside its own crate
python3 scripts/check_prose.py              # the Writing rules' countable part;
                                            # --tighten after a cleanup
python3 scripts/check_readme_example.py     # README.md's quick start matches
                                            # crates/ytsaurus-client/examples/quickstart.rs

cargo test --workspace            # 938 tests: 863 unit and integration, 75 doc
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/build-worker.sh         # static musl workers -> target/x86_64-unknown-linux-musl/release-worker/
./scripts/build-worker.sh cat     # just one

cargo bench -p ytsaurus-yson      # codec microbenchmark
cargo bench -p ytsaurus-job       # job-path throughput

uvx ruff check .                  # the Python scripts
uvx ruff format --check .         # drop --check to rewrite

cd tests/rpc-go-interop && go test ./...   # regenerate the RPC wire-format vectors
cd tests/skiff-cpp-interop && uv run --no-project \
    --with-requirements requirements.txt python cpp_reference.py
                                           # regenerate the C++ Skiff vectors
cargo run -p ytsaurus-rpc --example rpc_e2e # RPC client against a live RPC proxy
cargo run -p ytsaurus-client --features rpc --example both_transports
                                           # the same code over HTTP and over RPC, compared

# 2 GB streaming memory test (ignored by default)
cargo test -p ytsaurus-job --release --test memory_tests -- --ignored --nocapture
```

### Worker builds and features

- `build-worker.sh` works on Linux and macOS. On macOS it links with the
  toolchain's bundled `rust-lld`, because Apple's `cc` cannot produce Linux
  ELF; no cross-toolchain or Docker is needed.
- `panic = "abort"` is a whole-graph profile setting and cannot be scoped to
  one crate. It lives in the workspace `release-worker` profile so library
  crates never inherit it.
- `ytsaurus-client`'s `tls` feature is on by default and off where workers are
  built. TLS means `rustls` and `ring`, and `ring` needs a C cross-compiler for
  musl; with `tls` off, `build-worker.sh` needs only the Rust toolchain, even
  for `selfrun`, which contains the whole client. The dependency is spelled out
  in `crates/ytsaurus-job/Cargo.toml` because cargo does not let an inherited
  dependency disable default features.
- Anything in `ytsaurus-job`'s `[dev-dependencies]` lands in the musl build: cargo compiles dev-dependencies when it builds examples, and the
  workers are examples of `ytsaurus-job`. So `ytsaurus-client` is there with `default-features = false`
  and a path with no version (a version makes it cyclic with the client, which
  dev-depends on `ytsaurus-job`, and deadlocks both releases), and the
  throughput bench's criterion is pinned below 0.8 (0.8 depends on `alloca`,
  whose build script needs a C cross-compiler).
- The `tracing` and `platform-verifier` features are off by default and must
  stay off; `default-features = false` in `crates/ytsaurus-job/Cargo.toml`
  keeps them out of the musl build. `platform-verifier` and `YT_CA_BUNDLE` are
  gated on `tls`. The `traceparent` header needs no dependency and is always
  compiled in. With `tracing` on and no subscriber installed, the retry line on
  stderr is still printed, because Cargo unifies features across the graph;
  retry reporting mutes itself inside a job.
- CI's musl job lists the worker graph with `cargo tree -p ytsaurus-job
  --target x86_64-unknown-linux-musl --prefix none` and fails if `tracing`,
  `rustls`, `ring` or `rustls-platform-verifier` is in it.
  **Do not switch to `cargo tree -i <crate>`**: `-i` exits non-zero both when
  the crate is absent and when cargo fails (it resolves `-i` before `-p`, so a
  misspelled package prints the same "did not match any packages"), so a failed
  run reads as a pass.

## Protocol reference

[docs/protocol-reference.md](docs/protocol-reference.md) records the YTsaurus
protocol and cluster behaviour this repository depends on, each fact with its
evidence: binary YSON markers, descriptors, the job environment, statistics,
schemas, transactions, verbs, authentication, redirects, TLS, heavy-proxy
routing, the operation lifecycle, rich paths, tracing, batches, control records
and cluster gotchas.

Read it before touching the wire protocol. New protocol or cluster facts go
there, not here.

## Architecture

### `ytsaurus-yson`

Vendored upstream plus a `scan` module. `scan_value(input, format)` returns the
byte length of the first complete value or `Scan::Incomplete`, walking the
token stream without allocating. Upstream's API takes a whole slice; `scan` is
what lets an input larger than memory be streamed.

### `ytsaurus-job`

- `JobReader::next_event()` is a lending iterator, not `Iterator`. Rows borrow
  the read buffer, so the borrow must end before the next call; the compiler
  enforces this, which makes zero-copy decoding safe.
- The reader holds one buffer (1 MiB default), compacts it, and grows it only
  when a single record does not fit. Streaming 2 GB does not raise peak RSS:
  46.6 MiB before and after on Linux CI, 1.9 -> 2.0 MiB on macOS.
- Unknown control records are skipped, not surfaced as rows. YTsaurus may add
  attributes this version has not seen, and handing one to the job as a row
  would corrupt the output table.
- Output descriptors are never closed (`ManuallyDrop<File>`). Table 0 is
  fd 1, which `std::io::stdout()` also refers to; closing it would leave later
  `println!` writing to a closed or recycled descriptor.
- `finish()` is explicit. Output is buffered and unflushed rows are lost.
  `Drop` makes a last flush and complains on stderr but cannot fail the job;
  `run()` exists for that reason.
- A corrupt length prefix is capped by `max_record_bytes` (256 MiB) rather than
  chased into an OOM abort.
- `Row::raw()` is byte-exact; decode-then-re-encode is not, because
  `YsonNode::Map` is a `BTreeMap` and sorts keys. Identity jobs must use
  `raw()`.

## Fork status

Three upstream bugs were found while vendoring, all fixed here and recorded in
the changelog. YTsaurus strings and attribute names are arbitrary byte strings,
not text:

1. Infinite loop on a stray `/` in text input. A `/` followed by anything other
   than `/` or `*` entered the comment branch without advancing the cursor, so
   `/a` never returned. It allocates nothing, so no memory watchdog catches it.
2. Non-UTF-8 map keys were rejected, though `YsonNode::Map` stores `Vec<u8>`.
3. Non-UTF-8 attribute names were replaced with `""` (a literal
   `unwrap_or("")`), losing the name and colliding every such attribute.

Also added: `Serialize` for `YsonValue`/`YsonNode`, `Copy` on `YsonFormat`,
`Serializer::with_buffer`/`into_output`, and the `scan` module.

The fork and the three defects are
[reported upstream](https://github.com/ss123she/yson-rs/issues/1). Known
limitations are in [`crates/ytsaurus-yson/README.md`](crates/ytsaurus-yson/README.md);
the two that matter most: maps round-trip as values not bytes, and decoding
into `String` fails on non-UTF-8 columns (use `serde_bytes`).

## Testing

Three layers:

1. Unit and integration. Control records are driven by the exact stream from
   the docs; chunked readers go down to one byte per `read`, which exercises
   every split point including mid-varint.
2. Offline e2e: runs the real compiled worker with real fd 1 / fd 4
   redirection, against golden fixtures captured from a live cluster
   (`tests/cluster-e2e/capture_fixtures.sh`).
3. Cluster e2e, against a local YTsaurus in Docker, the same three checks
   driven two ways. `cargo run -p ytsaurus-client --example client_e2e` drives
   them through this crate with no Python; `tests/cluster-e2e/run_e2e.sh`
   drives them through the official Python client, the only place code this
   project did not write reads the worker's output. Keep both. `run_e2e.sh`
   runs on every push to main through `.github/workflows/cluster-e2e.yml`;
   `run_pilot.sh` is not in CI (it needs a multi-GB image). See
   [`tests/cluster-e2e/README.md`](tests/cluster-e2e/README.md).

Fuzzing: `cargo +nightly fuzz run fuzz_target_{1,2}` from
`crates/ytsaurus-yson/`. `tests/fuzz_smoke_tests.rs` gives CI a deterministic
no-panic signal without nightly.

Prefer capturing a fixture from a cluster over building one by hand: the
synthetic fixture was wrong in two ways that only the cluster showed.

## Status

All nine crates are on crates.io and share the workspace version; the current
one is on the badges in [README.md](README.md). Four are pre-release:
`ytsaurus-skiff` and `ytsaurus-format` (published since 0.2.5), `ytsaurus-api`
and `ytsaurus-rpc` (since 0.3.0). Their compatibility gates are not all green
and their APIs may change in a patch release. Release history is in each
crate's CHANGELOG; measurements are in [docs/benchmarking.md](docs/benchmarking.md)
and [`crates/ytsaurus-yson/BENCHMARKS.md`](crates/ytsaurus-yson/BENCHMARKS.md).

Whether Skiff becomes the default job format is undecided; the readings, the
threshold and what is still owed are in
[docs/benchmarking.md](docs/benchmarking.md#the-verdict). A local Docker cluster
is enough for everything else.

Open work is in the [issue tracker](https://github.com/sshaplygin/ytsaurus-rs/issues).
Three things need a human decision. **Do not start them without one.** They
are: upstreaming to [ytsaurus-rust-sdk](https://github.com/ytsaurus/ytsaurus-rust-sdk),
whose maintainers have
[said PRs are welcome](https://github.com/ytsaurus/ytsaurus/issues/6);
convergence with the yson-rs author (co-ownership, publishing, the patches);
and a public write-up of 0.3.

## Non-goals

Non-Linux targets. Streaming table I/O over RPC, the gRPC proxy,
chaos/replication, queues and Query Tracker. Hard rule 1 governs every release.

## Reference

[YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson) ·
[control attributes](https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration) ·
[table switch](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/table-switch) ·
[operation options](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options) ·
[Try YTsaurus](https://ytsaurus.tech/docs/en/overview/try-yt) ·
[ss123she/yson-rs](https://github.com/ss123she/yson-rs) ·
[interop-tests](https://github.com/ss123she/yson-interop-tests)
