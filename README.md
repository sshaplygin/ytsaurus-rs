# ytsaurus-rs

[![CI](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)
[![rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](rust-toolchain.toml)

| Crate | Version | Docs | What it is |
| --- | --- | --- | --- |
| [`ytsaurus-yson`](crates/ytsaurus-yson/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-yson.svg)](https://crates.io/crates/ytsaurus-yson) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-yson)](https://docs.rs/ytsaurus-yson) | YSON codec, text and binary |
| [`ytsaurus-job`](crates/ytsaurus-job/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-job.svg)](https://crates.io/crates/ytsaurus-job) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-job)](https://docs.rs/ytsaurus-job) | Job runtime |
| [`ytsaurus-client`](crates/ytsaurus-client/) | [![crates.io](https://img.shields.io/crates/v/ytsaurus-client.svg)](https://crates.io/crates/ytsaurus-client) | [![docs.rs](https://img.shields.io/docsrs/ytsaurus-client)](https://docs.rs/ytsaurus-client) | HTTP API v4 launcher |
| [`ytsaurus-helpers`](crates/ytsaurus-helpers/) | unpublished | — | `#[derive(TableRow)]` for schemas |

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
| [crates/ytsaurus-job/](crates/ytsaurus-job/) | Job runtime: streaming row reader, control records, multi-table output. |
| [crates/ytsaurus-client/](crates/ytsaurus-client/) | HTTP API v4 launcher: run an operation without the Python SDK. |
| [crates/ytsaurus-helpers/](crates/ytsaurus-helpers/) | Derive macros: a table schema read off the struct the rows have. |
| [examples/](examples/) | Worker binaries built on `ytsaurus-job`. |
| [docs/](docs/) | Guides, including how to write and launch a job. |
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

## Or let the binary launch itself

The cluster starts a job with `YT_JOB_ID` in its environment, so one binary can
be both the launcher and the job — and upload *itself*, which means the cluster
can never be running a stale worker:

```rust
fn main() {
    ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job

    let client = ytsaurus_client::Client::from_env().unwrap();
    client.upload_current_exe("//tmp/my_job").unwrap();
    // ...start the operation and wait for it
}
```

If it fails, the error carries the job's own stderr rather than a state string.
See [examples/src/bin/selfrun.rs](examples/src/bin/selfrun.rs); the full
walkthrough is [docs/writing-a-job.md](docs/writing-a-job.md).

## Build and test

```sh
cargo test --workspace          # 301 tests
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
from a struct, transactions, the rest of Cypress with locks, `alter_table`, and
streaming table I/O. Every item ends with an example that checks itself on a
cluster — [`tests/e2e/README.md`](tests/e2e/README.md) is the list of what has
actually been run, with its output.

**Measured against the Go SDK.** There is no official Rust SDK, but there is an
official Go one, and its twelve examples are what an SDK for this cluster is
expected to do. [`docs/go-parity.md`](docs/go-parity.md) maps every one of them
onto this workspace: six have a Rust counterpart that runs on a cluster, six are
a recorded decision not to. Three of the twelve asked for something this client
could not do — typed rows in and out, typed nodes, and reading a successful
job's stderr — and it can now.

What remains open needs a human: publishing, an API review, upstreaming, and
whether to build a Skiff codec. All are described in [AGENTS.md](AGENTS.md),
which is also the project context for contributors and coding agents.

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
memory. Decoding YSON is **~10 %** of the pilot job's time on a cluster, against
66 % for a job that does nothing but decode — which is why the Skiff question is
parked rather than pressing. Fuzzing ran 6.5 M iterations across both YSON
formats without a crash.

Vendoring `yson-rs` turned up three real bugs, including an input that hangs the
text parser forever — see [the changelog](crates/ytsaurus-yson/CHANGELOG.md).

All three crates are on crates.io, and **no further release happens without
explicit human approval** — versions, yanks and new crates alike. In particular
the `yson-rs` name belongs to its upstream author and will never be claimed here.

Every protocol fact in this repository is taken from the official YTsaurus
documentation and cited at the point of use, then checked against a real cluster.

## Licence

[Apache-2.0](LICENSE). Attributions for vendored third-party code are in
[NOTICE](NOTICE).

`crates/ytsaurus-yson` derives from [ss123she/yson-rs](https://github.com/ss123she/yson-rs),
which its author offers under *either* MIT *or* Apache-2.0. This project takes it
under Apache-2.0 — a choice that licence explicitly permits — and retains the
upstream notices alongside the vendored code.
