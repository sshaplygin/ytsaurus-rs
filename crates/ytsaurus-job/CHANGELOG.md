# Changelog

## 0.2.6 - 2026-08-10

### The worker examples live here now

- **Added** eight runnable workers under `examples/` — `cat`, `wordcount`,
  `hello`, `sessionize`, `boom`, `counted`, `shards` and `skiff_cat` — which
  were a separate package in the repository before and are now published with
  this crate. `cargo run -p ytsaurus-job --example wordcount`, and on docs.rs
  they sit beside the API they demonstrate.

  A ninth, `selfrun`, stays in the repository and is **excluded from the
  package**. It is launcher and job in one binary, so it needs
  `ytsaurus-client` — which dev-depends on this crate in turn, so the
  dependency here carries a path and no version to keep two published crates
  from becoming cyclic. Cargo drops a version-less dev-dependency on publish,
  and an example importing a crate the manifest no longer names is an example
  nobody could build.

- **Added** the `example-tls` feature, which gives that same `selfrun` example
  TLS for a cluster reached over https. It affects nothing else: this crate's
  library has no HTTP in it. Off by default, because the workers cross-compile
  to musl and `rustls` reaches `ring`, which wants a C cross-compiler.

- **Changed** the criterion dev-dependency to `0.7`. Cargo compiles a package's
  dev-dependencies whenever it builds that package's examples, and criterion
  0.8 reaches `alloca`, whose build script wants the same cross-compiler.
  Nothing in the bench used a 0.8 feature.

## 0.2.5 - 2026-08-10

No changes to this crate beyond the version, which tracks the workspace.

### An empty reduce group stays empty

- **Fixed** back-to-back key switches producing a "empty" group that was
  actually live: it handed out the *next* group's rows under the empty
  group's (absent) key, and that group was never seen at all. An empty group
  now comes out with no rows, and the group after it keeps its rows and its
  key. YTsaurus does not emit consecutive switches today, which is why this
  had not bitten; the iterator no longer depends on that staying true.

### A late row is refused, not lost

- **Fixed** `JobWriter` accepting rows after `finish()`. Such a row went into
  the buffer, `Drop` saw a finished writer and flushed nothing, and the job
  exited zero with a short table — the exact outcome `finish` exists to rule
  out. Writing after `finish` now fails with the new
  `JobError::WriteAfterFinish`. **Breaking** for anyone matching `JobError`
  exhaustively; add `..` or a `_` arm.

### Row numbering matches the cluster's

- **Fixed** `Row::row_index` standing still between control records. YTsaurus
  emits `<row_index=N>#` only at discontinuities — the start of a range or a
  chunk — and every row after it implicitly advances the index; the reader now
  counts rows the way the Go, C++ and Python SDKs do, instead of stamping every
  row of a run with the same `N`. Rows skipped by `Groups` draining a group
  advance the index too, since they are still rows of the table.
- **Fixed** a table switch leaving `range_index` stale: `<table_index=…>#`
  now drops the previous table's range index along with its row index, so a row
  of the new table never reports a range it was not read from.

### A job can report its own numbers

- **Added** `JobStatistics`. The cluster measures a job from the outside — CPU,
  memory, rows in and out — but nothing tells you how many rows a job *rejected*
  unless the job says so. Statistics go to the descriptor YTsaurus reserves for
  them (fd 5) as a YSON list fragment, matching the Python wrapper's
  `write_statistics`, and the operation aggregates them across jobs.

  ```rust
  let mut stats = JobStatistics::new();
  stats.add("rows/rejected", 1)?;
  stats.finish()?;
  ```

  Values accumulate and are sent once, by `finish` — the cluster has no defined
  behaviour for one name arriving twice. `Drop` makes a last-ditch attempt and
  complains on stderr, as with `JobWriter`.

- **Added** `JobError::TooManyStatistics` and `JobError::Statistics`. A job may
  report at most 128 distinct names, so the 129th is refused locally rather than
  by the cluster rejecting the lot. The second is separate from `JobError::Write`
  because fd 5 is not an output table, and reporting it as "output table 5"
  would send the reader looking for a table that does not exist. **Breaking**
  for anyone matching `JobError` exhaustively; add `..` or a `_` arm.

**Nothing is written unless `is_inside_job()`.** Outside a job, fd 5 is not the
cluster's — and with one binary serving as both launcher and job, it is as
likely to be an open socket to the cluster as to be nothing at all. Writing YSON
into that would be worse than losing a statistic.

### Knowing which job of the task this is

- **Added** `job_cookie`, the job's index within its task (`YT_JOB_COOKIE`),
  counting from zero and stable across a restart. A map job rarely needs it —
  its share of the work arrives on fd 0 — but a **vanilla** job has no input at
  all, so this is how it takes its own share, and how a retried job redoes that
  share rather than someone else's.

The entry point for a job with no input is the same `run`: a `JobReader` was
never mandatory. See `ytsaurus-client`'s `VanillaSpec` for the other side.

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
([`sessionize`](../../crates/ytsaurus-job/examples/sessionize.rs)) and a launcher
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
