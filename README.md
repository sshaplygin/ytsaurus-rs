# ytsaurus-rs

[![CI](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml)
[![cluster-e2e](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/cluster-e2e.yml/badge.svg?branch=main)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/cluster-e2e.yml?query=branch%3Amain)
[![release](https://img.shields.io/github/v/tag/sshaplygin/ytsaurus-rs?label=release&sort=semver)](https://github.com/sshaplygin/ytsaurus-rs/releases)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)
[![rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](rust-toolchain.toml)

Rust clients for [YTsaurus](https://ytsaurus.tech), which has no official
Rust SDK: `ytsaurus-client` for the HTTP API, and dynamic tables over HTTP or
the RPC proxy through one interface, `ytsaurus-api`'s `TableClient`.
`ytsaurus-job`, a runtime for MapReduce workers in Rust, is secondary; see
[Writing workers](#writing-workers).

All nine crates are on crates.io, released together under the workspace
version shown on the badges. Four of them, `ytsaurus-skiff`, `ytsaurus-format`,
`ytsaurus-api` and `ytsaurus-rpc`, are pre-release: the version is 0.x, their
compatibility gates are not all green, and their APIs may change in a patch
release.

| Crate | Version | Docs | What it is |
| --- | --- | --- | --- |
| [`ytsaurus-client`](crates/ytsaurus-client/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-client.svg)](https://crates.io/crates/ytsaurus-client) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-client)](https://docs.rs/ytsaurus-client) | HTTP API v4 client: static tables, Cypress, transactions, files, batched commands, operations; dynamic tables through `ytsaurus-api` |
| [`ytsaurus-api`](crates/ytsaurus-api/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-api.svg)](https://crates.io/crates/ytsaurus-api) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-api)](https://docs.rs/ytsaurus-api) | The transport-independent client interface: one API, HTTP or RPC. **Pre-release**: the interface has not settled |
| [`ytsaurus-rpc`](crates/ytsaurus-rpc/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-rpc.svg)](https://crates.io/crates/ytsaurus-rpc) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-rpc)](https://docs.rs/ytsaurus-rpc) | RPC proxy client: bus, the RPC envelope and the dynamic-table row wire format. **Pre-release**: [gates still open](docs/rpc-compatibility.md) |
| [`ytsaurus-proto`](crates/ytsaurus-proto/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-proto.svg)](https://crates.io/crates/ytsaurus-proto) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-proto)](https://docs.rs/ytsaurus-proto) | Generated protobuf for the RPC proxy, generated from the upstream `.proto` files and committed |
| [`ytsaurus-helpers`](crates/ytsaurus-helpers/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-helpers.svg)](https://crates.io/crates/ytsaurus-helpers) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-helpers)](https://docs.rs/ytsaurus-helpers) | `#[derive(TableRow)]` for schemas |
| [`ytsaurus-yson`](crates/ytsaurus-yson/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-yson.svg)](https://crates.io/crates/ytsaurus-yson) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-yson)](https://docs.rs/ytsaurus-yson) | YSON codec, text and binary |
| [`ytsaurus-skiff`](crates/ytsaurus-skiff/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-skiff.svg)](https://crates.io/crates/ytsaurus-skiff) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-skiff)](https://docs.rs/ytsaurus-skiff) | Skiff schema and codec. **Pre-release**: [gates still open](docs/skiff-compatibility.md) |
| [`ytsaurus-format`](crates/ytsaurus-format/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-format.svg)](https://crates.io/crates/ytsaurus-format) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-format)](https://docs.rs/ytsaurus-format) | `DataFormat`, shared by launcher and worker. Pre-release with the above |
| [`ytsaurus-job`](crates/ytsaurus-job/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-job.svg)](https://crates.io/crates/ytsaurus-job) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-job)](https://docs.rs/ytsaurus-job) | Job runtime |

## Quick start

```sh
cargo add ytsaurus-client ytsaurus-helpers serde --features serde/derive
cargo add ytsaurus-client --features rpc   # for the RPC proxy
```

CI checks this block against
[quickstart.rs](crates/ytsaurus-client/examples/quickstart.rs).

<!-- quickstart.rs -->
```rust
use serde::{Deserialize, Serialize};
use ytsaurus_client::{Client, TableRow};

#[derive(Serialize, Deserialize, Debug, PartialEq, ytsaurus_helpers::TableRow)]
struct Contact {
    name: String,
    age: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A static table over HTTP, as Rust values.
    let client = Client::from_env()?;
    let path = "//tmp/ytsaurus_rs_quickstart";
    client.remove_tree(path)?;
    client.create_table(path, &Contact::table_schema())?;
    let rows = vec![Contact {
        name: "Ann".into(),
        age: 31,
    }];
    client.write_table_rows(path, &rows)?;
    assert_eq!(client.read_table_rows::<Contact>(path)?, rows);
    println!("wrote and read back {path}");

    // A dynamic table.
    let table = std::env::var("YT_DYNAMIC_TABLE").unwrap_or_default();
    if !table.trim().is_empty() {
        let tables = ytsaurus_client::create_client(std::env::var("YT_PROXY")?.trim())?;
        // With the `rpc` feature: ytsaurus_client::create_rpc_client(&address)?
        let found =
            tables.select_rows(&format!("* from [{table}] limit 10"), &Default::default())?;
        println!("{} rows", found.len());
    }
    Ok(())
}
```

In a clone of this repository:

```sh
export YT_PROXY=http://localhost:8000
cargo run -p ytsaurus-client --example quickstart
```

The dynamic read needs `YT_DYNAMIC_TABLE` to name a mounted table; mounting
takes `Client::raw_command`, as in
[both_transports.rs](crates/ytsaurus-client/examples/both_transports.rs).
`create_client` and `create_rpc_client` send no token, not even the one
`from_env` found; `create_client_with_token` and `create_rpc_client_with_token`
do.

## Layout

| Path | What it is |
| --- | --- |
| [crates/](crates/) | The nine crates above. [`ytsaurus-yson`](crates/ytsaurus-yson/) is a fork of [ss123she/yson-rs](https://github.com/ss123she/yson-rs) @ `ba2044c`; `ytsaurus-job` is a streaming row reader, control records and multi-table output, and [its examples](crates/ytsaurus-job/examples/) are the nine runnable worker binaries; `ytsaurus-rpc`, for dynamic tables under concurrency, is async on tokio, unlike the rest; `ytsaurus-proto` is generated from the `third_party/ytsaurus` submodule and committed. |
| [xtask/](xtask/) | `cargo xtask generate-protos`, the one task that has to be Rust (`prost-build` is a Rust library). Never published. |
| [scripts/](scripts/) | The rest of the automation, in Python: the CI checks below and the benchmark comparison posted on pull requests. |
| [docs/](docs/) | Guides: writing a job, benchmarks, the protocol reference, and comparisons with the official C++ and Go clients. |
| [tests/cluster-e2e/](tests/cluster-e2e/) | End-to-end scripts against a local YTsaurus cluster. |

## Environment

The client reads these through [`Client::from_env`](https://docs.rs/ytsaurus-client).
Only `YT_PROXY` is required; the rest are inert when unset, so a machine that
sets none behaves as `Client::new` does. A variable set to the empty string
counts as unset (`export YT_FILE_CACHE=` turns one off). All but `YT_CA_BUNDLE`
are trimmed; that one is a path and keeps its spelling.

| Variable | Default | What it does |
| --- | --- | --- |
| `YT_PROXY` | none | The cluster address. A bare host means `https://`; a local cluster is `http://localhost:8000`. Required. |
| `YT_TOKEN` | none | The token, looked for as the `yt` CLI does, stopping at the first source that has one. |
| `YT_TOKEN_PATH` | `~/.yt/token` | A file holding the token, tried after `YT_TOKEN` and before the default path. Trimmed, so a trailing newline from `echo` does not fail authentication. |
| `YT_CA_BUNDLE` | Mozilla roots | A PEM file of root certificates, for a cluster whose chain ends in a private CA. Without it such a cluster fails its first request with `invalid peer certificate: UnknownIssuer`. Read by every client, `Client::new` included. |
| `YT_PROXY_SUFFIX` | off | Completes a bare cluster name: `YT_PROXY=hume` plus `.yt.example.net` addresses `hume.yt.example.net`. Applied only to a name with no dot, no colon and no `localhost` in it. No suffix is compiled in. |
| `YT_HEAVY_PROXY_DOMAINS` | none | More domains, comma- or space-separated, under which `/hosts` may name a heavy proxy. `Client::with_heavy_proxies_under`. |
| `YT_HEAVY_PROXIES_ANYWHERE` | off | `1`, `true` or `yes` removes the domain rule, as the official Go SDK does with `/hosts`. Applied after the domains, so the wider wins. |
| `YT_FILE_CACHE` | `//tmp/yt_wrapper/file_storage/new_cache` | Where `upload_worker_cached` keeps its files, for an installation whose shared cache is read-only to you. |

The environment can widen the heavy-proxy rule but not narrow it; see [the client
README](crates/ytsaurus-client/README.md#where-a-heavy-command-goes).

For a cluster that is not a local one (a private CA, heavy proxies in another
domain, a shared file cache), see [the
runbook](tests/cluster-e2e/README.md#against-a-cluster-that-is-not-the-local-one).

Inside a job, YTsaurus sets variables that `ytsaurus-job` reads; `YT_JOB_ID` is
what `is_inside_job` tests; full list in
[docs/writing-a-job.md](docs/writing-a-job.md#what-the-cluster-puts-in-a-jobs-environment).

Examples and scripts that measure something take these:

| Variable | Default | Used by |
| --- | --- | --- |
| `YT_WORKER_BINARY` | the running executable | `selfrun`: the static musl worker to upload when the launcher came from `cargo run` |
| `YT_PROFILE_MIB` / `YT_PROFILE_ROUNDS` | 48 / 5 | `profile`. Raise the rounds on a busy cluster; at 3 it could not separate the phases at all |
| `YT_STREAM_MIB` | 64 | `streaming` |
| `YT_APPEND_ROWS` / `YT_APPEND_CHUNKS` | 60000 / 12 | `append` |
| `YT_LOCAL_DIR` | `~/yt-local` | `tests/cluster-e2e/run_local_cluster.sh` |
| `YT_PILOT_BASE` | `//tmp/ytsaurus_rs_pilot` | `tests/cluster-e2e/run_pilot.sh` |

## Writing workers

A YTsaurus job is an executable that reads input rows from fd 0 and writes
output tables to fds 1, 4, 7… in binary
[YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson). `ytsaurus-job`
is the runtime for writing one in Rust instead of C++; its examples build as
fully static `x86_64-unknown-linux-musl` binaries ready to upload.

```sh
cargo add ytsaurus-job
```

### A job in full

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

### Or let a static binary launch itself

The cluster starts a job with `YT_JOB_ID` in its environment, so a static Linux
x86-64 binary can be both the launcher and the job, and upload itself, so the
cluster never runs a stale worker:

```rust
fn main() {
    ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job

    let client = ytsaurus_client::Client::from_env().unwrap();
    client.upload_current_exe("//tmp/my_job").unwrap();
    // ...start the operation and wait for it
}
```

A launcher from `cargo run` is not a static Linux binary, so build the worker
separately, set `YT_WORKER_BINARY` to it, and rebuild it when its source
changes. A failed job's error carries its own stderr. See
[crates/ytsaurus-job/examples/selfrun.rs](crates/ytsaurus-job/examples/selfrun.rs)
and the walkthrough in [docs/writing-a-job.md](docs/writing-a-job.md).

```sh
# a local cluster is plain HTTP
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-job --example selfrun

# an https cluster needs the launcher to have TLS
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-job --example selfrun --features example-tls
```

The flag changes only the launcher: `ytsaurus-job` takes `ytsaurus-client` as a
`default-features = false` dev-dependency, so `build-worker.sh` cross-compiles
to musl with only the Rust toolchain, and the musl worker has no TLS either way.
It is `example-tls`, not `tls`, because `ytsaurus-job` has no HTTP.

## Build and test

```sh
cargo test --workspace          # 941 tests
./scripts/build-worker.sh       # static musl worker binaries
cargo bench -p ytsaurus-job     # job-path throughput
```

Building does not need the `.proto` submodule: `ytsaurus-proto` ships its
generated bindings. Regenerating them is `./scripts/init-protos.sh` once, then
`cargo xtask generate-protos`; CI regenerates and fails on a diff.

CI's other checks, runnable locally:

```sh
python3 scripts/check_package_includes.py   # no published file may include_str!
                                            # data from outside its own crate
python3 scripts/check_worker_graph.py       # no tracing, TLS, tokio or prost
                                            # reaches the musl worker build
python3 scripts/check_worker_binaries.py    # after build-worker.sh: every worker
                                            # is statically linked (needs ldd,
                                            # so Linux)
```

`build-worker.sh` produces `target/x86_64-unknown-linux-musl/release-worker/<name>`,
statically linked and stripped, on Linux and on macOS, where it links with the
Rust toolchain's bundled `rust-lld` and needs no cross-toolchain.
`panic = "abort"` is set only in the `release-worker` profile, never for the
library crates; see the comment in [Cargo.toml](Cargo.toml).

## Status

The ranked backlog is done, from job diagnostics to the full operation
lifecycle; each item has an example that checks itself on a cluster, and
[`tests/cluster-e2e/README.md`](tests/cluster-e2e/README.md) lists what ran and
what it reported.

[`docs/sdk-comparison.md`](docs/sdk-comparison.md) compares this client with
the official C++ and Go ones area by area; [`docs/go-parity.md`](docs/go-parity.md)
maps the Go SDK's twelve examples onto this workspace: six have a Rust
counterpart that runs on a cluster, six are a recorded decision not to.

Skiff support is pre-release, built against a pinned Go SDK baseline;
[the compatibility contract](docs/skiff-compatibility.md) lists the supported
surface and open gates. `DataFormat` selects binary or text YSON or dynamic
Skiff. Whether Skiff becomes the default is undecided:
[docs/benchmarking.md](docs/benchmarking.md).

The pinned parity issue tracks what is still needed to match the official
clients. Open work that needs a human: a public write-up (#71),
upstreaming (#72), and convergence with yson-rs (#73), all listed under
[Status in AGENTS.md](AGENTS.md#status), the project context for contributors
and coding agents.

Verified on a local YTsaurus in Docker: the identity map (output byte-identical
to the input, 309 688 bytes), a two-input, two-output run with table switching,
and a `wordcount` map-reduce matching a hand-computed result. The offline
test's golden fixtures are captured from that cluster, so CI keeps the signal
without Docker. Running on a cluster caught four things no offline test could:
`--spec` is YSON and not JSON, `map-reduce` needs
`--map-local-file`/`--reduce-local-file`, a column value may not carry
attributes, and YTsaurus emits `<table_index=0;>#` with a trailing semicolon
inside the attribute block.

Streaming 2 GB through the reader does not raise peak RSS: 46.6 MiB before and
after on Linux CI, 1.9 → 2.0 MiB on macOS (the test binary's own footprint
differs by platform). Streaming a 67.7 MiB table out of a cluster costs 1.0 MiB
of peak RSS, against 70.9 MiB to read it into memory. Fuzzing ran 6.5 M
iterations across both YSON formats without a crash. Vendoring `yson-rs` turned
up three bugs, including an input that hangs the text parser forever; see
[the changelog](crates/ytsaurus-yson/CHANGELOG.md).

**No further release happens without explicit human approval**: versions, yanks
and new crates alike. The `yson-rs` crate name belongs to its upstream author
and will not be claimed here.

Every protocol fact in this repository is taken from the official YTsaurus
documentation, cited at the point of use, and checked against a real cluster;
[docs/protocol-reference.md](docs/protocol-reference.md) collects them.

## Acknowledgements

[@AzazKamaz](https://gist.github.com/AzazKamaz/711234fde6c17cfe04c83702bced19d9)
shared the initial job-level Skiff framing example that prompted this work. It
is kept as a reference vector; compatibility is defined by the official
protocol, the pinned Go SDK, and cluster tests.

## Licence

[Apache-2.0](LICENSE). Attributions for vendored third-party code are in
[NOTICE](NOTICE).

`crates/ytsaurus-yson` derives from [ss123she/yson-rs](https://github.com/ss123she/yson-rs),
which its author offers under either MIT or Apache-2.0. This project takes it
under Apache-2.0, as that licence permits, and keeps the upstream notices
beside the vendored code.
