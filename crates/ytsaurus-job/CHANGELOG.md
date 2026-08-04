# Changelog

## Unreleased

### One binary can be both the launcher and the job

- **Added** `is_inside_job`, `run_if_inside_job` and `job_id`. The cluster
  starts a job with `YT_JOB_ID` in its environment, so a program can tell which
  role it is playing:

  ```rust
  fn main() {
      ytsaurus_job::run_if_inside_job(mapper);   // never returns inside a job
      launch();                                  // only your machine gets here
  }
  ```

  With `Client::upload_current_exe` on the other side, the binary uploads
  itself, and "the cluster is running last week's worker" stops being possible.
  This is the shape of Go's `mapreduce.InsideJob` / `JobMain`.

  Verified on a cluster rather than assumed: a job printed its environment, and
  `YT_JOB_ID` was in it, alongside `YT_OPERATION_ID`, `YT_JOB_COOKIE`,
  `YT_JOB_INDEX`, `YT_START_ROW_INDEX` and `YT_FIRST_OUTPUT_TABLE_FD=1` — the
  `3k + 1` descriptor rule from the cluster's own mouth. The full list is in
  [`docs/writing-a-job.md`](../../docs/writing-a-job.md) §3.

  An empty `YT_JOB_ID` does not count as a job. `YT_JOB_ID=` in a shell would
  otherwise run the job body on a developer's machine, reading their terminal as
  an input stream.

## 0.2.0

Everything here came from writing a production-shaped pilot
([`sessionize`](../../examples/src/bin/sessionize.rs)) and a launcher
([`ytsaurus-client`](../ytsaurus-client/)) against the API, then filing what got
in the way. Each change closes a numbered issue.

### Added

- **`JobReader::groups_by`, `Group::key` and `GroupKey`** ([#2]). A reducer no
  longer has to re-derive the reduce key by parsing its first row, copy it out,
  and write a dead branch for the "no rows yet" case that cannot happen:

  ```rust
  let mut groups = reader.groups_by(["user_id"]);
  while let Some(mut group) = groups.next_group()? {
      let user = group.key().bytes("user_id").unwrap_or_default();
      // ...
  }
  ```

  YTsaurus does not transmit the key — `key_switch` carries no payload — so this
  reads it from the group's first row. Same work, done once instead of in every
  reducer. `groups()` is unchanged and leaves `Group::key` empty.

  `GroupKey` accessors are byte-first (`bytes`, `str`, `i64`, `get`) because
  reduce keys are routinely not UTF-8. A key column missing from a row is absent
  rather than fatal.

- **`JobError::kind` and `JobError::is_row_local`** ([#1]). A validating mapper
  has to fold two error types — the runtime's `JobError` and its own validation
  reason — into one path. Previously that meant `to_string()`, which allocates
  per bad row and produces a message that can change between versions, which is
  awkward for a column you intend to group by.

  `kind()` returns a stable, allocation-free identifier (`invalid_yson`,
  `truncated_record`, …). `is_row_local()` says whether to quarantine the row or
  stop: a truncated stream or a failed write means every later row is suspect.

- **`JobWriter::named` and `TableId`** ([#4]). Output tables can be declared by
  name and addressed by handle:

  ```rust
  let (mut writer, [events, rejects]) = JobWriter::named(["events", "rejects"])?;
  writer.write(rejects, &row)?;
  ```

  A bare `usize` still converts, so 0.1 code keeps compiling. The point is the
  call site: a job with two output tables of different meaning is where
  transposing `0` and `1` yields something that runs happily and fills each table
  with the other's rows.

  `JobWriter::table_name` and `named_writers` (for tests) come with it.

### Changed

- **`JobWriter::write` and `write_raw` take `impl Into<TableId>`** instead of
  `usize`. Source-compatible: `write(0, &row)` still resolves.

- **`JobError::UnknownTable` gained a `names` field**, so an out-of-range write
  reports `output table 9 does not exist; this job has 2 output table(s): events,
  rejects` rather than a bare index. **Breaking** for anyone matching the variant
  exhaustively; add `..` to the pattern.

### Documentation

- The guide now covers the **output** side ([#3]). An output row may borrow from
  the input row — the value is serialized before the borrow ends — so a rejects
  table does not need `row.raw().to_vec()`. The copying version compiles and is
  correct, which is exactly why nothing pushed you off it; the cost only shows up
  on a large run. This was filed as an API gap and turned out to be a
  documentation gap, verified by compiling the borrowing form against 0.1.0.
- Added a section on reporting why a row was rejected, using the new `kind()` and
  `is_row_local()`.

[#1]: https://github.com/sshaplygin/ytsaurus-rs/issues/1
[#2]: https://github.com/sshaplygin/ytsaurus-rs/issues/2
[#3]: https://github.com/sshaplygin/ytsaurus-rs/issues/3
[#4]: https://github.com/sshaplygin/ytsaurus-rs/issues/4

## 0.1.0

First release. Streaming reader with control records and reduce grouping,
multi-table output over descriptors or table switches, panic-to-stderr wrapper.
Verified against a real cluster; 2 GB of input streams without growing the
process.
