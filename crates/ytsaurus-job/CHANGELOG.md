# Changelog

## 0.3.1 - 2026-08-16

### Fixed

- `tests/skiff_reader_tests.rs` is no longer published. It `include_str!`s a
  control-row vector from `tests/skiff-go-interop/`, outside this crate, so
  `cargo test` inside an unpacked 0.3.0 failed; depending on the crate was
  unaffected. `scripts/check_package_includes.py` now checks this in CI.

## 0.3.0 - 2026-08-16

No library change since 0.2.5. This release ships the 0.2.6 changes below, a
rewritten throughput benchmark, and end-to-end tests that drive the example
workers.

- Excluded `tests/cat_e2e.rs` from the package: it `include_bytes!`s golden
  fixtures from outside this crate and could not build from the tarball. It
  still runs in the repository.

## 0.2.6 — never released

The tag was never cut; nothing in this section reached crates.io until 0.3.0.

- Added eight example workers, previously a separate package in the repository
  and now published here: `cat`, `wordcount`, `hello`, `sessionize`, `boom`,
  `counted`, `shards` and `skiff_cat` (`cargo run -p ytsaurus-job --example
  wordcount`). A ninth, `selfrun`, needs `ytsaurus-client` and is excluded from
  the package.
- Added the `example-tls` feature: TLS for the `selfrun` example and nothing
  else, off by default. It is repository-only; on crates.io it reads
  `example-tls = []` and does nothing.
- Changed the criterion dev-dependency to `0.7`: 0.8 reaches `alloca`, whose
  build script needs a C cross-compiler that the musl worker build lacks.

## 0.2.5 - 2026-08-10

- Fixed back-to-back key switches: an empty reduce group now has no rows, and
  the group after it keeps its rows and its key. Before, the empty group handed
  out the next group's rows. YTsaurus does not emit consecutive switches today.
- Fixed `JobWriter` accepting rows after `finish()`, which were then lost while
  the job exited zero. Writing after `finish` now fails with
  `JobError::WriteAfterFinish`. **Breaking** for anyone matching `JobError`
  exhaustively; add `..` or a `_` arm.
- Fixed `Row::row_index` standing still between control records: after each
  `<row_index=N>#` it now advances by one per row, as the Go, C++ and Python
  SDKs count, including rows skipped by `Groups` draining a group.
- Fixed a table switch leaving `range_index` stale: `<table_index=…>#` now drops
  the previous table's range index along with its row index.
- Added `JobStatistics`: custom statistics written to fd 5 as a YSON list
  fragment, as the Python wrapper's `write_statistics` does. Values accumulate
  and are sent once, by `finish`; `Drop` makes a last-ditch attempt and
  complains on stderr. Nothing is written unless `is_inside_job()`.
- Added `JobError::TooManyStatistics`, for a 129th distinct name (the limit is
  128 per job), and `JobError::Statistics`, for a failed write to fd 5, apart
  from `JobError::Write`. **Breaking** for anyone matching `JobError`
  exhaustively; add `..` or a `_` arm.
- Added `job_cookie`, the job's index within its task (`YT_JOB_COOKIE`),
  counting from zero and stable across a restart: how a vanilla job, which has
  no input, takes its share. Such a job uses the same `run`; see
  `ytsaurus-client`'s `VanillaSpec`.
- Added `is_inside_job`, `run_if_inside_job` and `job_id`, so one binary can be
  both launcher and job (with `Client::upload_current_exe`), in the shape of
  Go's `mapreduce.InsideJob` / `JobMain`. A job is detected by a non-empty
  `YT_JOB_ID`; an empty one does not count.

## 0.2.0

Changes from writing the
[`sessionize`](../../crates/ytsaurus-job/examples/sessionize.rs) pilot and a
launcher against the API. Each closes the numbered issue.

### Added

- `JobReader::groups_by`, `Group::key` and `GroupKey` ([#2]): a reducer gets
  its reduce key, read from the group's first row since YTsaurus does not
  transmit it. `GroupKey` accessors are byte-first (`bytes`, `str`, `i64`,
  `get`); a missing key column is absent, not an error. `groups()` is unchanged
  and leaves `Group::key` empty.
- `JobError::kind` and `JobError::is_row_local` ([#1]): a stable,
  allocation-free identifier (`invalid_yson`, `truncated_record`, …), and
  whether to quarantine the row or stop.
- `JobWriter::named` and `TableId` ([#4]): output tables declared by name and
  addressed by handle. `JobWriter::table_name` and `named_writers` (for tests)
  come with it.

### Changed

- `JobWriter::write` and `write_raw` take `impl Into<TableId>` instead of
  `usize`. Source-compatible: `write(0, &row)` still resolves.
- `JobError::UnknownTable` gained a `names` field, so an out-of-range write
  names the job's output tables. **Breaking** for anyone matching the variant
  exhaustively; add `..` to the pattern.

### Documentation

- The guide covers the output side ([#3]): an output row may borrow from the
  input row, so a rejects table does not need `row.raw().to_vec()`.
- The guide covers reporting why a row was rejected, with `kind()` and
  `is_row_local()`.

[#1]: https://github.com/sshaplygin/ytsaurus-rs/issues/1
[#2]: https://github.com/sshaplygin/ytsaurus-rs/issues/2
[#3]: https://github.com/sshaplygin/ytsaurus-rs/issues/3
[#4]: https://github.com/sshaplygin/ytsaurus-rs/issues/4

## 0.1.0

First release. Streaming reader with control records and reduce grouping,
multi-table output over descriptors or table switches, panic-to-stderr wrapper.
