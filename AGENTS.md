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
| `crates/ytsaurus-job/` | Job runtime: streaming reader, control records, multi-table output. |
| `crates/ytsaurus-client/` | HTTP API v4 launcher: upload a worker, start an operation, wait for it. No Python needed. |
| `examples/` | Worker binaries (`cat`, `wordcount`, `hello`) plus their e2e tests. |
| `docs/` | [writing-a-job.md](docs/writing-a-job.md) (the user guide), [benchmarking.md](docs/benchmarking.md) (measurements + the Skiff decision). |
| `tests/e2e/` | Cluster scripts and captured golden fixtures. |
| `scripts/build-worker.sh` | Static musl worker builds. |

## Fixed decisions — do not revisit without a human

| Decision | Value |
| --- | --- |
| Repository name | **ytsaurus-rs** |
| Crate names | `ytsaurus-*` prefix: `ytsaurus-yson`, `ytsaurus-job`; later `ytsaurus-skiff`, `ytsaurus-client` if needed |
| YSON foundation | fork of ss123she/yson-rs pinned to `ba2044c711cefa65259e25122fea21c36f451093` (2026-04-01, v0.1.3) |
| Licence | **Apache-2.0** for this project. Upstream yson-rs is MIT OR Apache-2.0; we elect Apache-2.0 and keep upstream's licence files and notices. |
| Job data format | binary YSON (`<format=binary>yson`); Skiff only after benchmarks |
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
5. **No scope creep.** RPC proxy, protobuf row format, dynamic tables, custom job
   statistics, non-Linux targets are out of scope until a human decides
   otherwise.

## Commands

```sh
cargo test --workspace            # 158 tests
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
- **A column value cannot carry attributes.** YTsaurus rejects it on write with
  `Table values cannot have top-level attributes`, so a job can never receive one.
- **The cluster re-encodes rows on ingest.** 309 676 bytes uploaded came back as
  309 688. Compare read-back against read-back, never against the uploaded file.

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

1. **Unit and integration** — 158 tests. Control records driven by the exact
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

### Parked — needs a human and a real cluster

- **Skiff go/no-go.** Job-path benchmarks exist
  ([`docs/benchmarking.md`](docs/benchmarking.md)) but the decision needs a
  ≥ 10 GB table and C++/Python baselines. Decoding is 66 % of job CPU for a job
  that does nothing else, which is the worst case for YSON, not a verdict.
- **Upstreaming** to
  [ytsaurus/ytsaurus-rust-sdk](https://github.com/ytsaurus/ytsaurus-rust-sdk) —
  the maintainers' stance in ytsaurus#6 is "PRs welcome". **Do not start without
  a go-ahead.**
- **Contacting the yson-rs author** about co-ownership or publishing.
- **`ytsaurus-skiff`**, only if the benchmarks justify it — see
  [`docs/benchmarking.md`](docs/benchmarking.md). Reference implementation: the
  `skiff` package in the [Go SDK](https://pkg.go.dev/go.ytsaurus.tech/yt/go).

A test cluster with real data is needed from a human for the Skiff comparison;
a local Docker cluster is enough for everything else.

## Non-goals

RPC proxy (custom binary protocol), protobuf row format, dynamic tables, custom
job statistics, non-Linux targets, publishing to crates.io.

## Reference

[YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson) ·
[control attributes](https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration) ·
[table switch](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/table-switch) ·
[operation options](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options) ·
[Try YTsaurus](https://ytsaurus.tech/docs/en/overview/try-yt) ·
[ss123she/yson-rs](https://github.com/ss123she/yson-rs) ·
[interop-tests](https://github.com/ss123she/yson-interop-tests)
