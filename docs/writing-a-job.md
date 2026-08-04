# Writing a YTsaurus job in Rust

From an empty file to a running operation. Assumes you can already reach a
cluster with the `yt` CLI.

## 0. What a job actually is

A YTsaurus job is an ordinary executable. The cluster copies it to a node, runs
it, and:

- feeds it input rows on **fd 0**,
- collects output table `k` from **fd `3k + 1`** — so table 0 is fd 1 (stdout),
  table 1 is fd 4, table 2 is fd 7,
- decides whether the job succeeded from its **exit code**,
- shows its **stderr** in the operation UI.

The rows are [YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson), in
binary, as a `;`-separated list fragment. `ytsaurus-job` handles all of that.

Two consequences worth internalising early:

- **stdout belongs to the protocol.** A stray `println!` corrupts output table 0.
  Print diagnostics with `eprintln!`.
- **A job can be restarted.** YTsaurus reruns failed and speculative jobs, so a
  job must be a pure function of its input. Do not write to external state.

## 1. Set up

Add a binary to `examples/` (or your own crate):

```toml
[dependencies]
ytsaurus-job = { path = "../crates/ytsaurus-job" }
serde = { version = "1", features = ["derive"] }
serde_bytes = "0.11"
```

## 2. Write a mapper

```rust
use serde::{Deserialize, Serialize};
use ytsaurus_job::{Event, JobReader, JobWriter};

#[derive(Deserialize)]
struct Input<'a> {
    #[serde(borrow)]
    url: &'a str,
    size: i64,
}

#[derive(Serialize)]
struct Output<'a> {
    host: &'a str,
    size: i64,
}

fn main() {
    ytsaurus_job::run(|| {
        let mut reader = JobReader::from_stdin();
        let mut writer = JobWriter::descriptors(1)?;

        while let Some(event) = reader.next_event()? {
            let Event::Row(row) = event else { continue };
            let input: Input = row.parse()?;
            let host = input.url.split('/').next().unwrap_or("");
            writer.write(0, &Output { host, size: input.size })?;
        }

        // Buffered rows that are never flushed are rows missing from the
        // output table. `finish` is not optional.
        writer.finish()
    })
}
```

`run` installs a panic hook and turns any error into a non-zero exit with the
message on stderr, which is what makes a failure diagnosable in the UI.

### Choosing column types

| Column | Use | Why |
| --- | --- | --- |
| string, known to be text | `&str` / `String` | borrows from the read buffer with `&str` |
| string, arbitrary bytes | `#[serde(with = "serde_bytes")] &'a [u8]` | **YTsaurus strings are byte strings.** A `String` field fails the whole job on one non-UTF-8 row |
| int64 / uint64 | `i64` / `u64` | |
| double | `f64` | |
| boolean | `bool` | |
| any nullable column | `Option<T>` | a missing or `#` value |
| anything at all | `ytsaurus_yson::YsonValue` | dynamic access |

Prefer borrowed types (`&'a str`, `&'a [u8]`). They read straight out of the
reader's buffer and cost nothing to decode; owned types copy every row.

Because borrowed fields point into the reader's buffer, they cannot outlive the
row. If you need to accumulate across rows, copy what you keep — the compiler
will tell you exactly where.

## 3. Build it for the cluster

Jobs run on Linux x86_64 nodes that may not have your libc. Build a fully static
musl binary:

```sh
scripts/build-worker.sh my_job
file target/x86_64-unknown-linux-musl/release-worker/my_job
# ELF 64-bit LSB pie executable, x86-64, ..., static-pie linked, stripped
```

This works on Linux and on macOS (it links with the `rust-lld` bundled with the
Rust toolchain, so there is no cross-toolchain to install).

`static-pie linked` with no interpreter is what you want. A dynamically linked
binary will fail on the node with a missing-loader error that is hard to read.

## 4. Run it

The CLI needs **two** packages — `ytsaurus-client` alone fails on binary YSON
with `YSON bindings required`:

```sh
pip install ytsaurus-client ytsaurus-yson
```

```sh
yt map './my_job' \
    --src //tmp/input --dst //tmp/output \
    --format '<format=binary>yson' \
    --local-file target/x86_64-unknown-linux-musl/release-worker/my_job
```

`--local-file` uploads the binary; `'./my_job'` is the command the node runs.
`--format '<format=binary>yson'` sets both input and output format and is what
`JobReader::from_stdin` and `JobWriter::descriptors` expect.

Two CLI details that are easy to get wrong:

- **`--spec` is YSON, not JSON.** `{mapper={memory_limit=536870912}}` — `=` for
  key/value, `;` between entries, `%true`/`%false` for booleans. A JSON spec
  fails with `Unexpected token ":"`.
- **`map-reduce` uses `--map-local-file` and `--reduce-local-file`**, not
  `--local-file`.

## 5. Multiple output tables

Declare the count and address tables by index:

```rust
let mut writer = JobWriter::descriptors(2)?;
writer.write(0, &kept)?;
writer.write(1, &rejected)?;
```

```sh
yt map './my_job' --src //tmp/in --dst //tmp/good --dst //tmp/bad ...
```

Table `k` goes to fd `3k + 1`. If you would rather send everything down one
descriptor, `JobWriter::table_switches(n)` writes `<table_index=N>#` records
instead. Do not mix the two: YTsaurus does not define the order of rows reaching
one table through two descriptors.

## 6. Knowing which input table a row came from

Ask for it in the spec, then read `row.table_index`:

```sh
--spec '{mapper={enable_input_table_index=%true}}'
```

`row_index` and `range_index` work the same way, via
`job_io.control_attributes.enable_row_index` / `enable_range_index`. Without
these the fields stay at their defaults — `table_index` is `0` and the others
are `None`.

## 7. Reduce

A reducer's input is grouped by the `--reduce-by` columns, with a
`<key_switch=%true>#` record between groups. **You must enable it**, or the whole
input arrives as one group and every key is silently summed together:

```sh
# `reduce` operation — one job type, so the section is `job_io`
--spec '{job_io={control_attributes={enable_key_switch=%true}}}'

# `map-reduce` operation — several job types, each with its own section
--spec '{reduce_job_io={control_attributes={enable_key_switch=%true}}}'
```

Getting this wrong is quiet, not loud: `job_io` on a map-reduce is simply
ignored, the reducer sees no key switches, and every key is summed into one row.

```rust
let mut groups = reader.groups();
while let Some(mut group) = groups.next_group()? {
    let mut total = 0i64;
    let mut key = None;

    while let Some(row) = group.next_row()? {
        let entry: Entry = row.parse()?;
        // Every row in a group shares the reduce key.
        if key.is_none() {
            key = Some(entry.word.to_vec());
        }
        total += entry.count;
    }

    if let Some(key) = key {
        writer.write(0, &Total { word: key, count: total })?;
    }
}
```

A full map-reduce, with one binary serving both the map and reduce phases:

```sh
yt map-reduce \
    --mapper './wordcount map' --reducer './wordcount reduce' \
    --reduce-by word \
    --src //tmp/lines --dst //tmp/counts \
    --format '<format=binary>yson' \
    --map-local-file target/x86_64-unknown-linux-musl/release-worker/wordcount \
    --reduce-local-file target/x86_64-unknown-linux-musl/release-worker/wordcount \
    --spec '{reduce_job_io={control_attributes={enable_key_switch=%true}}}'
```

See [`examples/src/bin/wordcount.rs`](../examples/src/bin/wordcount.rs).

## 8. Test without a cluster

A job is a program that reads a pipe, so you can run it as one:

```sh
./my_job < input.bin > table0.bin 4> table1.bin
```

That is exactly how [`examples/tests/cat_e2e.rs`](../examples/tests/cat_e2e.rs)
works, and it catches most protocol mistakes without a cluster. For the reduce
path, [`examples/tests/wordcount_e2e.rs`](../examples/tests/wordcount_e2e.rs)
simulates the shuffle by sorting the mapper output and inserting key switches.

For a real cluster run, see [`tests/e2e/README.md`](../tests/e2e/README.md).

## 9. When something goes wrong

| Symptom | Likely cause |
| --- | --- |
| `exec format error` on the node | binary is not Linux x86_64 — check `file` |
| output table has garbage rows | something wrote to stdout; use `eprintln!` |
| output table is short | `writer.finish()` was not called |
| every reduce key summed together | `enable_key_switch` was not set — on `map-reduce` it goes under `reduce_job_io`, not `job_io` |
| job fails on some rows only | a `String` column that is not valid UTF-8 — use `serde_bytes` |
| `table_index` always 0 | `enable_input_table_index` was not set |
| job killed on memory | you are accumulating rows; the reader itself holds ~1 MiB |
| `YSON bindings required` from the CLI | `pip install ytsaurus-yson` |
| `Unexpected token ":"` from `--spec` | the spec is JSON; it must be YSON |
| `Table values cannot have top-level attributes` on write | a column value carries `<...>` attributes; tables cannot store those |
| output differs from input in an identity job | you decoded and re-encoded — map keys come back sorted. Use `Row::raw()` |

Job stderr appears in the operation UI. Set `RUST_BACKTRACE` through the spec to
get backtraces from a panicking job:

```sh
--spec '{mapper={environment={RUST_BACKTRACE="1"}}}'
```

## Reference

- [YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson)
- [Input/output settings](https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration) — control attributes
- [Table switching](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/table-switch) — descriptor numbering
- [Operation options](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options)
