# ytsaurus-client

A thin [YTsaurus](https://ytsaurus.tech) HTTP API v4 client: enough to run a Rust
worker **without a Python installation**.

```toml
[dependencies]
ytsaurus-client = "0.2"
```

```rust
use ytsaurus_client::{Client, MapSpec};

# fn demo() -> Result<(), ytsaurus_client::ClientError> {
let client = Client::from_env()?;                  // YT_PROXY, YT_TOKEN

client.upload_worker("target/…/my_job", "//tmp/my_job")?;

let spec = MapSpec::new("./my_job", ["//tmp/in"], ["//tmp/out"])
    .with_local_file("//tmp/my_job")
    .with_memory_limit(512 * 1024 * 1024);

let id = client.start_map(&spec)?;
client.wait_for_operation(&id)?;
# Ok(())
# }
```

A runnable version is [`examples/launch.rs`](examples/launch.rs), which creates
tables, uploads a worker, writes rows, runs a map, waits for it and verifies the
result:

```sh
export YT_PROXY=http://localhost:8000
cargo run -p ytsaurus-client --example launch
```

## What it covers

| | |
| --- | --- |
| Cypress | `create`, `remove`, `exists`, `get`, `row_count` |
| Data | `upload_worker`, `upload_worker_cached`, `upload_current_exe`, `write_file`, `write_table`, `read_table`, `set_attribute` |
| File cache | `file_from_cache`, `put_file_to_cache` |
| Operations | `start_map`, `start_reduce`, `start_sort`, `start_map_reduce`, `start_operation`, `operation_state`, `wait_for_operation` |
| Jobs | `list_jobs`, `get_job_stderr`, `custom_statistics`, `statistic_sum` |

Specs are built with [`MapSpec`] / [`ReduceSpec`] / [`SortSpec`] /
[`MapReduceSpec`], which model what launching a `ytsaurus-job` worker needs and
expose `with_raw` for everything else.

Two defaults exist because getting them wrong is quiet rather than loud:

- **both formats are binary YSON**, which is what `JobReader` and `JobWriter`
  expect;
- **`key_switch` is on** for both grouping operations, and lands in the right
  section for each: `reduce_job_io` for map-reduce, which has several job types
  and so an I/O section per type, and plain `job_io` for reduce, which has one.
  The wrong spelling is accepted and then ignored, leaving the reducer to fold
  every key into one group.

Reduce needs sorted input; `SortSpec` is what produces it, and its
`output_table_path` is singular — sort writes one table however many it reads.
[`examples/sort_reduce.rs`](examples/sort_reduce.rs) runs both against a
cluster.

`upload_worker` sets the `executable` attribute. Without it the cluster copies
the binary and then refuses to exec it, with an error that never mentions the
attribute.

## One binary, two roles

`upload_current_exe` uploads the *running* executable, so the same program can
launch the operation and be the job it runs:

```rust
fn main() {
    ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job
    launch().unwrap();                         // only your machine gets here
}
```

There is no second artifact to forget to rebuild. The running executable has to
be something a node can exec, so its ELF header is checked first — Linux,
x86-64, statically linked — and refused with `ClientError::NotAWorker` when it
is not, rather than failing on the node minutes later. On macOS the launcher is
Mach-O and cannot be the uploaded file: build the worker with
`scripts/build-worker.sh` and upload that with `upload_worker`. The source is
still one file — see
[`examples/src/bin/selfrun.rs`](../../examples/src/bin/selfrun.rs).

## Features

`tls` (default) brings in `rustls`, and with it `https://` proxies. Turning it
off leaves a client that speaks plain HTTP and needs no C toolchain — which is
how a binary that is both launcher and job gets cross-compiled to musl. Without
it, an `https://` proxy fails with an error naming the feature.

## When an operation fails

`wait_for_operation` does not stop at the state. It asks which jobs failed and
what they printed, so the error carries the job's own words:

```text
operation 1ba94195-… finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
  job 24c164af-… on localhost:24403: User job failed: Process terminated by signal 6
  stderr:
    thread 'main' panicked at examples/src/bin/boom.rs:37:17:
    boom: this job fails on purpose (row 1, 23 bytes)
```

That costs one `list_jobs` and a few `get_job_stderr` calls, on failure only.
The YTsaurus documentation asks that `list_jobs` not be used without an
administrator's approval, so `Client::with_job_diagnostics(false)` turns the
report off. Failing to collect it never replaces the failure being reported.

[`examples/diagnose.rs`](examples/diagnose.rs) runs the whole path against a
local cluster.

## Upload the worker once

Re-sending tens of megabytes on every launch is the slowest part of a dev loop
that changes only the spec. The cluster's file cache is keyed by MD5:

```rust
let worker = client.upload_worker_cached("target/…/my_job")?;   // uploaded, or found

let spec = MapSpec::new("./my_job", ["//tmp/in"], ["//tmp/out"])
    .with_local_file_named(&worker.path, &worker.name);
```

`worker.uploaded` says which it was. The name has to be passed along because the
cached node is named after the hash — `./my_job` would find nothing to run
otherwise. The cache defaults to the path the Python wrapper uses, so it is
shared with everything else on the installation;
`Client::with_file_cache` moves it.

## Retries

A shared cluster produces failures that pass on their own. Light commands are
repeated — five attempts by default, with a delay doubling from one second to
ten; `Client::with_retries(RetryPolicy::none())` turns that off.

Mutating commands carry a `mutation_id`, so a repeated request is deduplicated
by the cluster instead of being applied twice. Two things follow from how the
cluster implements that:

- a replay must be **marked** as one. Re-sending a known ID without the `retry`
  flag is refused — `Duplicate request is not marked as "retry"` — so
  `MutationId::as_retry()` is what a restarted process uses with a persisted ID
  (`Client::start_operation_with`);
- IDs are remembered for five to ten minutes, so this guards against a crash and
  restart, not forever.

**Heavy commands are not retried**, whatever the policy says: the documentation
is explicit that they cannot be, and a transaction is the way to make an upload
atomic.

## Limits worth knowing

**Heavy commands go to the address you gave it.** Large installations separate
light and heavy proxies and answer an upload on a light proxy with 503. Use
[`Client::heavy_proxy`] to discover one and point a second client at it. A local
cluster needs none of this.

**Trailers are not read.** The proxy reports a failure discovered mid-stream in
an `X-YT-Error` trailer, and `ureq` 3.3 exposes none. `read_table` compensates by
checking the response is a complete YSON list fragment, so a truncated read is
caught; a mid-stream failure that still yields well-formed output would not be.
This is a launcher, not a bulk export tool — for that, use the `yt` CLI.

**Tables are read into memory.** `read_table` is for results a launcher
inspects.

## Why not JSON

Parameters and specs are encoded with [`ytsaurus-yson`](../ytsaurus-yson/), this
project's own codec, rather than JSON. It keeps the dependency list short and
means every request exercises the codec against a real cluster.

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](../../NOTICE).

[`MapSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.MapSpec.html
[`MapReduceSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.MapReduceSpec.html
[`ReduceSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.ReduceSpec.html
[`SortSpec`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.SortSpec.html
[`Client::heavy_proxy`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.Client.html#method.heavy_proxy
