# ytsaurus-job

Runtime for writing [YTsaurus](https://ytsaurus.tech) MapReduce jobs in Rust.

A job is an executable: rows arrive on fd 0 as binary
[YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson), output tables go
to fds 1, 4, 7…, and the exit code decides whether the job passed. This crate
turns that into a loop over rows.

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

- **Streaming input.** The reader holds one buffer (1 MiB by default) no matter
  how much data flows through. 2 GB of input runs at under 2 MiB peak RSS —
  there is a test for exactly that.
- **Control records.** `table_index`, `row_index` and `range_index` are applied
  and reported on each row; `key_switch` becomes `Event::KeySwitch` or, via
  `groups()`, per-key iterators for reduce.
- **Byte-exact pass-through.** `Row::raw()` hands back the original bytes, so an
  identity job reproduces its input exactly. Decoding and re-encoding does not:
  YSON maps come back with sorted keys.
- **Multi-table output.** One descriptor per table, or a single stream with
  `<table_index=N>#` switch records.
- **Failing usefully.** Truncated input, corrupt records and write errors are all
  fatal and explain themselves on stderr, where the operation UI shows them.
- **Reporting its own numbers.** `JobStatistics` sends custom statistics on the
  descriptor YTsaurus reserves for them, and the operation aggregates them
  across jobs:

  ```rust
  let mut stats = JobStatistics::new();
  stats.add("rows/rejected", 1)?;
  stats.finish()?;
  ```

  Nothing else would tell you a mapper dropped rows: the operation succeeds and
  the output table is simply shorter.
- **Knowing it is a job.** The cluster sets `YT_JOB_ID`, so `is_inside_job()` and
  `run_if_inside_job()` let one binary be both the launcher and the job it runs:

  ```rust
  fn main() {
      ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job
      launch();                                  // only your machine gets here
  }
  ```

  With `ytsaurus-client`'s `upload_current_exe`, the binary uploads itself —
  there is no second artifact to forget to rebuild.

## Design notes

**Rows borrow the read buffer.** `Row::parse::<T>()` can decode into types
holding `&'a str` and `&'a [u8]`, which costs nothing beyond validation. The
borrow cannot outlive the row — if you need to accumulate across rows, copy what
you keep, and the compiler will point at the spot.

**`finish()` is not optional.** Output is buffered; rows that are never flushed
are rows missing from the table. `Drop` makes a last-ditch attempt and complains
on stderr, but it cannot fail the job, which is why `run()` calls `finish()`
through you.

**Output descriptors are never closed.** Table 0 is fd 1, which
`std::io::stdout()` also refers to; closing it would leave later `println!`
calls writing to a closed or recycled descriptor. The process exiting closes
them, which is the right time.

**Unknown control records are skipped, not surfaced.** A control record is an
attributed entity, and YTsaurus may add attributes this version has never heard
of. Skipping is the safe reading — handing one to the job as a row would
silently corrupt the output table.

**A corrupt length prefix cannot OOM the job.** The read buffer grows on demand
but stops at `max_record_bytes` (256 MiB by default) and fails with
`RecordTooLarge` rather than chasing an implausible length into an abort.

## Testing a job without a cluster

A job is a program that reads a pipe:

```sh
./my_job < input.bin > table0.bin 4> table1.bin
```

See [`examples/tests/cat_e2e.rs`](../../examples/tests/cat_e2e.rs) for that
pattern applied to a real binary, and
[`tests/e2e/README.md`](../../tests/e2e/README.md) for the cluster test.

## Benchmarks

```sh
cargo bench -p ytsaurus-job
```

Measures the job path — streaming, framing and decoding — as opposed to the
whole-slice microbenchmark in `ytsaurus-yson`. See
[docs/benchmarking.md](../../docs/benchmarking.md).

## Licence

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
