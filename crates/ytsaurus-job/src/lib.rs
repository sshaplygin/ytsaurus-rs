//! Write [YTsaurus](https://ytsaurus.tech) MapReduce jobs in Rust.
//!
//! A YTsaurus job is an ordinary executable. The cluster runs it once per chunk
//! of input, feeds it rows on fd 0, and collects output tables from fds 1, 4, 7…
//! The wire format is [YSON](https://ytsaurus.tech/docs/en/user-guide/storage/yson),
//! normally binary. This crate handles that protocol so a job can be written as
//! a loop over rows.
//!
//! # A complete mapper
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use ytsaurus_job::{Event, JobReader, JobWriter};
//!
//! #[derive(Deserialize)]
//! struct Input<'a> {
//!     #[serde(borrow)]
//!     url: &'a str,
//!     size: i64,
//! }
//!
//! #[derive(Serialize)]
//! struct Output<'a> {
//!     host: &'a str,
//!     size: i64,
//! }
//!
//! fn main() {
//!     ytsaurus_job::run(|| {
//!         let mut reader = JobReader::from_stdin();
//!         let mut writer = JobWriter::descriptors(1)?;
//!
//!         while let Some(event) = reader.next_event()? {
//!             let Event::Row(row) = event else { continue };
//!             let input: Input = row.parse()?;
//!             let host = input.url.split('/').next().unwrap_or("");
//!             writer.write(0, &Output { host, size: input.size })?;
//!         }
//!
//!         writer.finish()
//!     })
//! }
//! ```
//!
//! Build it for the cluster with `scripts/build-worker.sh`, then launch it with
//! the `yt` CLI — see `docs/writing-a-job.md`.
//!
//! # Memory
//!
//! The input stream is usually much larger than the job's memory limit.
//! [`JobReader`] never accumulates it: it holds one buffer (1 MiB by default)
//! and hands out rows that borrow from it. A row is only copied if you ask for
//! an owned type when decoding it.
//!
//! # Control records
//!
//! When the operation enables them, YTsaurus interleaves control records with
//! the data: `<table_index=N>#`, `<row_index=N>#`, `<range_index=N>#` and
//! `<key_switch=%true>#`. [`JobReader`] consumes the first three and reflects
//! them on each [`Row`]; `key_switch` surfaces as [`Event::KeySwitch`], or is
//! turned into per-key iterators by [`JobReader::groups`].

#![warn(missing_docs)]

/// Errors a job can fail with.
pub mod error;
/// Reading the input stream.
pub mod reader;
/// Writing output tables.
pub mod writer;

pub use crate::error::{JobError, Result};
pub use crate::reader::{Event, Group, Groups, JobReader, Row};
pub use crate::writer::{JobWriter, table_descriptor};

pub use ytsaurus_yson as yson;

/// Installs a panic hook that reports panics in a form a human can act on.
///
/// A job's stderr is shown in the operation UI, so this is where a failing job
/// explains itself. The default hook already prints the message and location;
/// this one labels it so it is obvious in the UI that the job — not the
/// infrastructure — is at fault, and reminds the reader that backtraces need
/// `RUST_BACKTRACE`, which cannot be set after the fact on a cluster.
///
/// [`run`] calls this for you.
pub fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("─────────────────────────────────────────────");
        eprintln!("ytsaurus-job: the job panicked and will fail.");
        if std::env::var_os("RUST_BACKTRACE").is_none() {
            eprintln!("Set RUST_BACKTRACE=1 in the operation spec's environment for a backtrace.");
        }
        eprintln!("─────────────────────────────────────────────");
        default_hook(info);
    }));
}

/// Runs a job body, reporting failures the way YTsaurus expects.
///
/// Installs [`install_panic_hook`], runs `job`, and on error prints the whole
/// error chain to stderr and exits with a non-zero status. YTsaurus decides
/// whether a job succeeded from its exit code, and shows stderr in the
/// operation UI, so this is the difference between a diagnosable failure and a
/// job that just says "exit code 1".
///
/// Note that `job` is responsible for calling [`JobWriter::finish`]; buffered
/// output that is never flushed is missing output.
pub fn run<F, E>(job: F) -> !
where
    F: FnOnce() -> std::result::Result<(), E>,
    E: std::fmt::Display,
{
    install_panic_hook();

    match job() {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("ytsaurus-job: the job failed: {e}");
            std::process::exit(1);
        }
    }
}
