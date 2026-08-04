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
pub use crate::reader::{Event, Group, GroupKey, Groups, JobReader, Row};
pub use crate::writer::{JobWriter, TableId, table_descriptor};

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

/// The variable YTsaurus sets in every job's environment.
///
/// Verified on a cluster, not assumed: a job printed its environment and this
/// was in it. The Go SDK's `mapreduce.InsideJob` tests the same variable.
const JOB_ID_ENV: &str = "YT_JOB_ID";

/// Whether this process is running as a job on a cluster.
///
/// This is what lets one binary be both the launcher and the job: the cluster
/// starts the same executable with `YT_JOB_ID` set, so the program can tell
/// which role it is playing.
///
/// ```no_run
/// fn main() {
///     // Inside a job this never returns.
///     ytsaurus_job::run_if_inside_job(my_mapper);
///
///     // Only reached on your machine: upload this binary and start the
///     // operation that will run it.
/// }
/// # fn my_mapper() -> ytsaurus_job::Result<()> { Ok(()) }
/// ```
#[must_use]
pub fn is_inside_job() -> bool {
    inside_job(std::env::var_os(JOB_ID_ENV))
}

/// The job's ID, when running inside one.
///
/// Worth putting in a log line: it is how a message on stderr is tied back to a
/// job in the operation's UI.
#[must_use]
pub fn job_id() -> Option<String> {
    std::env::var(JOB_ID_ENV).ok().filter(|id| !id.is_empty())
}

/// Runs `job` if this process is a job, and returns otherwise.
///
/// The whole of the one-binary pattern:
///
/// ```no_run
/// use ytsaurus_job::{Event, JobReader, JobWriter};
///
/// fn main() {
///     ytsaurus_job::run_if_inside_job(mapper);
///     launch();   // only your machine gets here
/// }
///
/// fn mapper() -> ytsaurus_job::Result<()> {
///     let mut reader = JobReader::from_stdin();
///     let mut writer = JobWriter::descriptors(1)?;
///     while let Some(event) = reader.next_event()? {
///         let Event::Row(row) = event else { continue };
///         writer.write_raw(0, row.raw())?;
///     }
///     writer.finish()
/// }
/// # fn launch() {}
/// ```
///
/// Inside a job this behaves exactly like [`run`] and never returns; the
/// process exits with the job's status.
pub fn run_if_inside_job<F, E>(job: F)
where
    F: FnOnce() -> std::result::Result<(), E>,
    E: std::fmt::Display,
{
    if is_inside_job() {
        run(job);
    }
}

/// The decision itself, split out so it can be tested without touching the
/// process environment — which is global, and in edition 2024 unsafe to write.
fn inside_job(job_id: Option<std::ffi::OsString>) -> bool {
    job_id.is_some_and(|id| !id.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_id_means_we_are_inside_a_job() {
        assert!(inside_job(Some("55aff293-7ef14284-3fe0384-3e07".into())));
    }

    #[test]
    fn no_job_id_means_we_are_not() {
        assert!(!inside_job(None));
    }

    #[test]
    fn an_empty_job_id_does_not_count() {
        // `YT_JOB_ID=` in a shell is not a job. Treating it as one would run
        // the job body on a developer's machine, reading their terminal as if
        // it were an input stream.
        assert!(!inside_job(Some(std::ffi::OsString::new())));
    }
}
