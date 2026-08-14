# ytsaurus-rs

[![CI](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/tag/sshaplygin/ytsaurus-rs?label=release&sort=semver)](https://github.com/sshaplygin/ytsaurus-rs/releases)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)
[![rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](rust-toolchain.toml)

Six of the eight crates are on crates.io at **0.2.6**, released together — the
two RPC crates are pre-release and unpublished. The version is
the workspace's, so they move as one.

| Crate | Version | Docs | What it is |
| --- | --- | --- | --- |
| [`ytsaurus-yson`](crates/ytsaurus-yson/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-yson.svg)](https://crates.io/crates/ytsaurus-yson) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-yson)](https://docs.rs/ytsaurus-yson) | YSON codec, text and binary |
| [`ytsaurus-job`](crates/ytsaurus-job/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-job.svg)](https://crates.io/crates/ytsaurus-job) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-job)](https://docs.rs/ytsaurus-job) | Job runtime |
| [`ytsaurus-client`](crates/ytsaurus-client/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-client.svg)](https://crates.io/crates/ytsaurus-client) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-client)](https://docs.rs/ytsaurus-client) | HTTP API v4 launcher |
| [`ytsaurus-helpers`](crates/ytsaurus-helpers/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-helpers.svg)](https://crates.io/crates/ytsaurus-helpers) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-helpers)](https://docs.rs/ytsaurus-helpers) | `#[derive(TableRow)]` for schemas |
| [`ytsaurus-skiff`](crates/ytsaurus-skiff/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-skiff.svg)](https://crates.io/crates/ytsaurus-skiff) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-skiff)](https://docs.rs/ytsaurus-skiff) | Skiff schema and codec. **Pre-release** — [gates still open](docs/skiff-compatibility.md) |
| [`ytsaurus-format`](crates/ytsaurus-format/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-format.svg)](https://crates.io/crates/ytsaurus-format) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-format)](https://docs.rs/ytsaurus-format) | `DataFormat`, shared by launcher and worker. Pre-release with the above |
| [`ytsaurus-api`](crates/ytsaurus-api/) | — | — | The transport-independent client interface: one API, HTTP or RPC. Unpublished |
| [`ytsaurus-rpc`](crates/ytsaurus-rpc/) | — | — | RPC proxy client: bus, the RPC envelope and the dynamic-table row wire format. **Pre-release and unpublished** — [gates still open](docs/rpc-compatibility.md) |
| [`ytsaurus-proto`](crates/ytsaurus-proto/) | — | — | Generated protobuf for the RPC proxy, built from the upstream `.proto` files. Unpublished |

Write [YTsaurus](https://ytsaurus.tech) MapReduce workers in Rust instead of C++.

```toml
[dependencies]
ytsaurus-job = "0.2"
```

A YTsaurus job is just an executable: it reads input rows from fd 0 and writes output
tables to fds 1, 4, 7… in binary [YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson).
There is no official Rust SDK, so this workspace provides the minimal stack needed —
a YSON codec and a job runtime — plus example workers that build as fully static
`x86_64-unknown-linux-musl` binaries you can upload straight to a cluster.

## Layout

| Path | What it is |
| --- | --- |
| [crates/ytsaurus-yson/](crates/ytsaurus-yson/) | YSON serializer/deserializer (text + binary). Fork of [ss123she/yson-rs](https://github.com/ss123she/yson-rs) @ `ba2044c`. |
| [crates/ytsaurus-skiff/](crates/ytsaurus-skiff/) | Schema model and compatibility suite for YTsaurus Skiff. **Pre-release**: published so the crates above can be, with [gates still open](docs/skiff-compatibility.md). |
| [crates/ytsaurus-format/](crates/ytsaurus-format/) | Shared `DataFormat` selection used by client specs/table I/O and worker I/O. |
| [crates/ytsaurus-job/](crates/ytsaurus-job/) | Job runtime: streaming row reader, control records, multi-table output. Its [examples/](crates/ytsaurus-job/examples/) are the nine runnable worker binaries. |
| [crates/ytsaurus-client/](crates/ytsaurus-client/) | HTTP API v4 launcher: run an operation without the Python SDK. |
| [crates/ytsaurus-helpers/](crates/ytsaurus-helpers/) | Derive macros: a table schema read off the struct the rows have. |
| [crates/ytsaurus-api/](crates/ytsaurus-api/) | One interface over both transports, so `create_client` and `create_rpc_client` return the same thing. |
| [crates/ytsaurus-rpc/](crates/ytsaurus-rpc/) | The RPC proxy, for dynamic tables under concurrency. Async on tokio, unlike everything above. |
| [crates/ytsaurus-proto/](crates/ytsaurus-proto/) | Protobuf bindings, generated from the `third_party/ytsaurus` submodule. |
| [docs/](docs/) | Guides: writing a job, benchmarks, and how this compares to the official C++ and Go clients. |
| [tests/e2e/](tests/e2e/) | End-to-end scripts against a local YTsaurus cluster. |

## A job in full

```rust
use ytsaurus_job::{Event, JobReader, JobWriter};

fn main() {
    ytsaurus_job::run(|| {
        let mut reader = JobReader::from_stdin();
        let mut writer = JobWriter::descriptors(1)?;

        while let Some(event) = reader.next_event()? {
            let Event::Row(row) = event else { continue };
            writer.write_raw(0, row.raw())?;
        }

        writer.finish()
    })
}
```

Then:

```sh
./scripts/build-worker.sh my_job
yt map './my_job' --src //tmp/in --dst //tmp/out \
    --format '<format=binary>yson' \
    --local-file target/x86_64-unknown-linux-musl/release-worker/my_job
```

## Or let a static binary launch itself

The cluster starts a job with `YT_JOB_ID` in its environment, so a static Linux
x86-64 binary can be both the launcher and the job — and upload *itself*, which
means the cluster can never be running a stale worker:

```rust
fn main() {
    ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job

    let client = ytsaurus_client::Client::from_env().unwrap();
    client.upload_current_exe("//tmp/my_job").unwrap();
    // ...start the operation and wait for it
}
```

If the launcher comes from `cargo run`, build a static worker separately and
set `YT_WORKER_BINARY` so it uploads that artifact; rebuild the worker whenever
its source changes. If it fails, the error carries the job's own stderr rather
than a state string. See [crates/ytsaurus-job/examples/selfrun.rs](crates/ytsaurus-job/examples/selfrun.rs);
the full walkthrough is [docs/writing-a-job.md](docs/writing-a-job.md).

```sh
# a local cluster is plain HTTP
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-job --example selfrun

# an https cluster needs the launcher to have TLS
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-job --example selfrun --features example-tls
```

The flag changes the **launcher** only. `ytsaurus-job` takes `ytsaurus-client`
as a `default-features = false` dev-dependency, for this one example, which is
what lets `build-worker.sh` cross-compile to musl with nothing but the Rust
toolchain; the musl worker carries no TLS either way. It is spelled
`example-tls` rather than `tls` because it changes nothing about the library —
`ytsaurus-job` has no HTTP in it at all.

**Against a cluster that is not a local one** — a private CA, heavy proxies in
another domain, a shared file cache — see [the runbook in
tests/e2e/README.md](tests/e2e/README.md#against-a-cluster-that-is-not-the-local-one).

## Environment

Everything this workspace reads. Three groups, because they are set by three
different people: you, the cluster, and whoever is running an example.

**The client** — [`Client::from_env`](https://docs.rs/ytsaurus-client) reads
these. `YT_PROXY` is the only one that is required; every other is inert when
unset, so a machine that sets none behaves exactly as `Client::new` does. One
**set to nothing counts as unset** throughout — `export YT_FILE_CACHE=` is how a
knob gets turned back off — and all but `YT_CA_BUNDLE` are trimmed. That one is
read as a path rather than as text, so it keeps whatever spelling it was given.

| Variable | Default | What it does |
| --- | --- | --- |
| `YT_PROXY` | — | The cluster address. A bare host means `https://`; a local cluster is `http://localhost:8000`. **Required.** |
| `YT_TOKEN` | — | The token. Looked for the way the `yt` CLI looks for it, stopping at the first that has one. |
| `YT_TOKEN_PATH` | `~/.yt/token` | A file holding the token instead, tried after `YT_TOKEN` and before the default path. Trimmed, so a trailing newline from `echo` does not fail authentication. |
| `YT_CA_BUNDLE` | Mozilla roots | A PEM file of root certificates, for a cluster whose chain ends in a private CA. Without it such a cluster fails its first request with `invalid peer certificate: UnknownIssuer`. |
| `YT_PROXY_SUFFIX` | off | Completes a bare cluster name: `YT_PROXY=hume` plus `.yt.example.net` addresses `hume.yt.example.net`. Applied only to a name with no dot, no colon and no `localhost` in it. No suffix is compiled in. |
| `YT_HEAVY_PROXY_DOMAINS` | — | One more domain, or several comma- or space-separated, that `/hosts` may name a heavy proxy under — for an installation that publishes them in a zone of its own. `Client::with_heavy_proxies_under`. |
| `YT_HEAVY_PROXIES_ANYWHERE` | off | `1`, `true` or `yes` removes the domain rule altogether, which is what the official Go SDK does with `/hosts`. Applied after the domains, so the wider of the two wins. |
| `YT_FILE_CACHE` | `//tmp/yt_wrapper/file_storage/new_cache` | Where `upload_worker_cached` keeps its files, for an installation whose shared cache is read-only to you. |

The heavy-proxy rule can be **widened** from the environment and deliberately not
narrowed: `Client::with_heavy_proxies_in` is the one mode that is a boundary
rather than a heuristic, and it is written in Rust. See [the client
README](crates/ytsaurus-client/README.md#where-a-heavy-command-goes).

**The cluster, inside a job** — set by YTsaurus when it execs the worker, read by
`ytsaurus-job`. `YT_JOB_ID` is what `is_inside_job` tests, and is why one binary
can be both launcher and job. The full table, with what each is worth, is in
[docs/writing-a-job.md](docs/writing-a-job.md#what-the-cluster-puts-in-a-jobs-environment).

**The examples and scripts** — knobs for the things that measure something, so a
run can be made bigger without editing code:

| Variable | Default | Used by |
| --- | --- | --- |
| `YT_WORKER_BINARY` | the running executable | `selfrun` — the static musl worker to upload when the launcher itself came from `cargo run` |
| `YT_PROFILE_MIB` / `YT_PROFILE_ROUNDS` | 48 / 5 | `profile`. Raise the rounds on a busy cluster; at 3 it could not separate the phases at all |
| `YT_STREAM_MIB` | 64 | `streaming` |
| `YT_APPEND_ROWS` / `YT_APPEND_CHUNKS` | 60000 / 12 | `append` |
| `YT_LOCAL_DIR` | `~/yt-local` | `tests/e2e/run_local_cluster.sh` |
| `YT_PILOT_BASE` | `//tmp/ytsaurus_rs_pilot` | `tests/e2e/run_pilot.sh` |

## Build and test

```sh
./scripts/init-protos.sh        # once after cloning: the .proto submodule
cargo test --workspace          # 892 tests
./scripts/build-worker.sh       # static musl worker binaries
cargo bench -p ytsaurus-job     # job-path throughput
```

`build-worker.sh` produces `target/x86_64-unknown-linux-musl/release-worker/<name>`,
statically linked and stripped. It works on Linux and on macOS (where it links via the
`rust-lld` bundled with the Rust toolchain, so no cross-toolchain install is needed).

`panic = "abort"` is set only in the `release-worker` profile, never in the library
crates — see the comment in [Cargo.toml](Cargo.toml).

## Status

The codec, the job runtime, the client and the example workers are implemented
and verified against a cluster. The ranked backlog has been worked top to bottom:
job diagnostics, one binary that is both launcher and job, vanilla operations,
reduce and sort, retries, the worker cache, custom statistics, schemas derived
from a struct, transactions, the rest of Cypress with locks, `alter_table`,
streaming table I/O, and the whole operation lifecycle — pause, resume, reprice,
finish early, look one up by alias, and reattach to one after a restart. Every
item ends with an example that checks itself on a cluster — [`tests/e2e/README.md`](tests/e2e/README.md) is the list of what has
actually been run, with its output.

**Measured against the official clients.** There is no official Rust SDK, but
there are a C++ one and a Go one, and what they do is what an SDK for this
cluster is expected to do.

- [`docs/sdk-comparison.md`](docs/sdk-comparison.md) puts all three side by side,
  area by area — transport, Cypress, tables, operations, transactions — and says
  where this client is ahead, where it is behind, and where the two official
  ones differ more from each other than either does from this one.
- [`docs/go-parity.md`](docs/go-parity.md) maps the Go SDK's twelve examples onto
  this workspace: six have a Rust counterpart that runs on a cluster, six are a
  recorded decision not to. Three of the twelve asked for something this client
  could not do — typed rows in and out, typed nodes, and reading a successful
  job's stderr — and it can now.

**Skiff.** Implementation has started against a pinned Go SDK compatibility
baseline; the supported surface and the test gates are in [the Skiff
compatibility contract](docs/skiff-compatibility.md). `DataFormat` is the common
public selector for binary/text YSON and dynamic Skiff across launchers and
workers; the existing format-specific methods remain convenience APIs.

What is still needed to match the official clients, in the order it matters for
production use, is tracked in the pinned parity issue. What remains open needs a
human: publishing, an API review, and upstreaming. All are described in
[AGENTS.md](AGENTS.md), which is also the project context for contributors and
coding agents.

**Verified against a real cluster.** A local YTsaurus in Docker ran the identity
map (output table byte-identical to the input, 309 688 bytes), a two-input /
two-output run exercising table switching, and a `wordcount` map-reduce matching a
hand-computed result. The offline test's golden fixtures are **captured from that
cluster** — `cat_input.bin` is literally the stream a job was handed on fd 0 — so
CI keeps a meaningful signal without Docker. See
[`tests/e2e/README.md`](tests/e2e/README.md).

Running it for real caught four things no offline test could: `--spec` is YSON and
not JSON, `map-reduce` needs `--map-local-file`/`--reduce-local-file`, a column
value may not carry attributes, and YTsaurus emits `<table_index=0;>#` with a
trailing semicolon inside the attribute block.

Other verified numbers: streaming 2 GB through the reader **does not raise peak
RSS at all** — 46.6 MiB before and after on Linux CI, 1.9 → 2.0 MiB on macOS
(the absolute figure is the test binary's own footprint, which differs by
platform; the invariant is that it does not grow). Streaming a 67.7 MiB table
out of a cluster costs **1.0 MiB** of peak RSS against 70.9 MiB to read it into
memory. Decoding YSON is **~10 %** of the pilot job's time on the local Docker
cluster and **36 %** of the same job on a production one, against 66 % for a job
that does nothing but decode — two readings 3.4× apart, which is why the Skiff
question is recorded as **open** rather than answered; see
[docs/benchmarking.md](docs/benchmarking.md). Fuzzing ran 6.5 M iterations
across both YSON formats without a crash.

Vendoring `yson-rs` turned up three real bugs, including an input that hangs the
text parser forever — see [the changelog](crates/ytsaurus-yson/CHANGELOG.md).

All six crates are on crates.io at 0.2.6, and **no further release happens
without explicit human approval** — versions, yanks and new crates alike. In
particular the `yson-rs` name belongs to its upstream author and will never be
claimed here.

Every protocol fact in this repository is taken from the official YTsaurus
documentation and cited at the point of use, then checked against a real cluster.

## Acknowledgements

[@AzazKamaz](https://gist.github.com/AzazKamaz/711234fde6c17cfe04c83702bced19d9)
shared the initial job-level Skiff framing example that prompted this work. It
is retained as a useful reference vector; compatibility is defined by the
official protocol, the pinned Go SDK, and cluster tests.

## Licence

[Apache-2.0](LICENSE). Attributions for vendored third-party code are in
[NOTICE](NOTICE).

`crates/ytsaurus-yson` derives from [ss123she/yson-rs](https://github.com/ss123she/yson-rs),
which its author offers under *either* MIT *or* Apache-2.0. This project takes it
under Apache-2.0 — a choice that licence explicitly permits — and retains the
upstream notices alongside the vendored code.
