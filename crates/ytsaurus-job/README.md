# ytsaurus-job

[![crates.io](https://img.shields.io/crates/v/ytsaurus-job.svg)](https://crates.io/crates/ytsaurus-job)
[![docs.rs](https://img.shields.io/docsrs/ytsaurus-job)](https://docs.rs/ytsaurus-job)
[![CI](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)

Runtime for writing [YTsaurus](https://ytsaurus.tech) MapReduce jobs in Rust.

A job is an executable: rows arrive on fd 0 as binary
[YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson), output tables go
to fds 1, 4, 7…, and the exit code decides whether the job passed. This crate
turns that into a loop over rows.

`WorkerReader` and `WorkerWriter` take the shared `DataFormat` enum: binary or
text YSON, or experimental dynamic, schema-driven Skiff. Rows stay explicit
(`WorkerRow::YsonRaw` or `WorkerRow::Skiff`), so pass-through is byte-exact
and Skiff is schema-checked. `JobReader` / `JobWriter` and `SkiffJobReader` /
`SkiffJobWriter` are the format-specific forms. Typed Skiff rows are not in
the public job API yet; see the
[compatibility contract](../../docs/skiff-compatibility.md).

Start with the guide: [docs/writing-a-job.md](../../docs/writing-a-job.md).

```rust
use ytsaurus_job::{Event, JobReader, JobWriter};

# fn demo() -> Result<(), ytsaurus_job::JobError> {
let mut reader = JobReader::from_stdin();
let mut writer = JobWriter::descriptors(1)?;

while let Some(event) = reader.next_event()? {
    let Event::Row(row) = event else { continue };
    writer.write_raw(0, row.raw())?;
}

writer.finish()
# }
```

## What it handles

- Streaming input. The reader holds one buffer (1 MiB by default) however much
  data flows through; a test streams 2 GB without raising peak RSS (measured:
  46.6 MiB before and after on Linux CI, 1.9 → 2.0 MiB on macOS).
- Control records. `table_index`, `row_index` and `range_index` are applied
  and reported on each row; `key_switch` becomes `Event::KeySwitch` or, via
  `groups()`, per-key iterators for reduce.
- Byte-exact pass-through. `Row::raw()` hands back the original bytes, so an
  identity job reproduces its input exactly. Decoding and re-encoding does not:
  YSON maps come back with sorted keys.
- Multi-table output. One descriptor per table, or a single stream with
  `<table_index=N>#` switch records.
- Failing usefully. Truncated input, corrupt records and write errors are all
  fatal and explain themselves on stderr, where the operation UI shows them.
- Custom statistics. `JobStatistics` sends them on the descriptor YTsaurus
  reserves, and the operation aggregates them across jobs:

  ```rust
  let mut stats = JobStatistics::new();
  stats.add("rows/rejected", 1)?;
  stats.finish()?;
  ```

  Nothing else tells you a mapper dropped rows: the operation succeeds and the
  output table is simply shorter.
- Knowing it is a job. The cluster sets `YT_JOB_ID`, so `is_inside_job()` and
  `run_if_inside_job()` let one binary be both the launcher and the job:

  ```rust
  fn main() {
      ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job
      launch();                                  // only your machine gets here
  }
  ```

  When that launcher is a static Linux x86-64 binary, `ytsaurus-client`'s
  `upload_current_exe` uploads it. A `cargo run` launcher needs a separately
  built static worker.

## Design notes

Rows borrow the read buffer. `Row::parse::<T>()` can decode into types holding
`&'a str` and `&'a [u8]`, which costs nothing beyond validation. The borrow
cannot outlive the row; to accumulate across rows, copy what you keep, and the
compiler points at the spot.

**`finish()` is not optional.** Output is buffered; rows never flushed are
rows missing from the table. `Drop` makes a last-ditch attempt and complains on
stderr, but cannot fail the job, which is why `run()` calls `finish()` through
you.

Output descriptors are never closed. Table 0 is fd 1, which `std::io::stdout()`
also refers to; closing it would leave later `println!` calls writing to a
closed or recycled descriptor. Process exit closes them.

Unknown control records are skipped, not surfaced. A control record is an
attributed entity, and YTsaurus may add attributes this version has not seen;
handing one to the job as a row would silently corrupt the output table.

A corrupt length prefix cannot exhaust memory. The read buffer grows on demand
but stops at `max_record_bytes` (256 MiB by default) and fails with
`RecordTooLarge` rather than chasing an implausible length into an abort.

## Testing a job without a cluster

A job is a program that reads a pipe:

```sh
./my_job < input.bin > table0.bin 4> table1.bin
```

See [`crates/ytsaurus-job/tests/cat_e2e.rs`](../../crates/ytsaurus-job/tests/cat_e2e.rs)
for that pattern applied to a real binary, and
[`tests/cluster-e2e/README.md`](../../tests/cluster-e2e/README.md) for the cluster test.

## Benchmarks

```sh
cargo bench -p ytsaurus-job
```

Measures the job path (streaming, framing and decoding), unlike the
whole-slice microbenchmark in `ytsaurus-yson`. See
[docs/benchmarking.md](../../docs/benchmarking.md).

## Licence

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
