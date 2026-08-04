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
| Data | `upload_worker`, `write_file`, `write_table`, `read_table`, `set_attribute` |
| Operations | `start_map`, `start_map_reduce`, `start_operation`, `operation_state`, `wait_for_operation` |

Specs are built with [`MapSpec`] / [`MapReduceSpec`], which model what launching
a `ytsaurus-job` worker needs and expose `with_raw` for everything else.

Two defaults exist because getting them wrong is quiet rather than loud:

- **both formats are binary YSON**, which is what `JobReader` and `JobWriter`
  expect;
- **`key_switch` is on** for map-reduce, and goes under `reduce_job_io` — an
  operation with several job types gives each type its own I/O section, and the
  plain `job_io` spelling is accepted and then ignored, leaving the reducer to
  fold every key into one group.

`upload_worker` sets the `executable` attribute. Without it the cluster copies
the binary and then refuses to exec it, with an error that never mentions the
attribute.

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
[`Client::heavy_proxy`]: https://docs.rs/ytsaurus-client/latest/ytsaurus_client/struct.Client.html#method.heavy_proxy
