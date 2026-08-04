//! Custom job statistics: numbers a job reports for the operation to aggregate.
//!
//! The system already measures a job from the outside — CPU, memory, rows in
//! and out. What it cannot see is anything about the *work*: how many rows were
//! rejected, how long loading a dictionary took, how many lookups missed. Those
//! are custom statistics, and this is how a job sends them.
//!
//! A job writes them to **file descriptor 5**, which YTsaurus reserves for the
//! purpose, as a YSON list fragment containing one map. The reference
//! implementation is the Python wrapper's `write_statistics`. The cluster
//! aggregates them across jobs and files them under `custom` in the operation's
//! `job_statistics`.
//!
//! ```no_run
//! use ytsaurus_job::JobStatistics;
//!
//! # fn main() -> ytsaurus_job::Result<()> {
//! let mut stats = JobStatistics::new();
//!
//! // ... while processing rows ...
//! stats.add("rows/rejected", 1)?;
//! stats.set("dictionary_bytes", 4096)?;
//!
//! // Nothing is sent until this is called.
//! stats.finish()?;
//! # Ok(())
//! # }
//! ```
//!
//! **A job may report at most 128 of them**, so `set` and `add` refuse a 129th
//! name rather than letting the cluster reject the lot.
//!
//! Reference:
//! <https://ytsaurus.tech/docs/en/user-guide/problems/jobstatistics>

use std::collections::BTreeMap;
use std::io::Write;

use crate::error::{JobError, Result};

/// The descriptor YTsaurus reserves for user statistics.
const STATISTICS_FD: i32 = 5;

/// How many distinct statistics one job may report.
const MAX_STATISTICS: usize = 128;

/// Numbers a job reports about its own work.
///
/// Values accumulate in memory and are written once, by [`JobStatistics::finish`]
/// — the cluster has no defined behaviour for the same name arriving twice, so
/// this never sends it twice.
#[derive(Debug, Default)]
pub struct JobStatistics {
    values: BTreeMap<String, i64>,
    finished: bool,
}

impl JobStatistics {
    /// An empty set of statistics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `name` to `value`, replacing whatever it held.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::TooManyStatistics`] if this is a new name and the
    /// job already reports 128.
    pub fn set(&mut self, name: impl Into<String>, value: i64) -> Result<()> {
        let name = name.into();
        self.reserve(&name)?;
        self.values.insert(name, value);
        Ok(())
    }

    /// Adds `delta` to `name`, starting from zero.
    ///
    /// This is the counter case: call it per row and report the total once.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::TooManyStatistics`] if this is a new name and the
    /// job already reports 128.
    pub fn add(&mut self, name: impl Into<String>, delta: i64) -> Result<()> {
        let name = name.into();
        self.reserve(&name)?;
        let slot = self.values.entry(name).or_insert(0);
        *slot = slot.saturating_add(delta);
        Ok(())
    }

    /// What `name` currently holds, before it is sent.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<i64> {
        self.values.get(name).copied()
    }

    /// How many distinct statistics are set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Sends everything recorded so far to the cluster.
    ///
    /// Outside a job this does nothing but say so on stderr. That is not
    /// timidity: descriptor 5 belongs to YTsaurus only inside a job, and in a
    /// launcher — which, with the one-binary pattern, is the same program —
    /// it is as likely to be an open socket to the cluster as to be nothing at
    /// all. Writing YSON into that would be worse than losing a statistic.
    ///
    /// # Errors
    ///
    /// Returns [`JobError`] if the statistics cannot be encoded or written.
    pub fn finish(&mut self) -> Result<()> {
        if self.finished || self.values.is_empty() {
            return Ok(());
        }
        self.finished = true;

        if !crate::is_inside_job() {
            eprintln!(
                "ytsaurus-job: not running as a job, so {} statistic(s) were not sent",
                self.values.len()
            );
            return Ok(());
        }

        let encoded = self.encode()?;
        write_to_statistics_fd(&encoded)
    }

    /// The bytes a job puts on descriptor 5: one map, as a list fragment.
    fn encode(&self) -> Result<Vec<u8>> {
        let map = ytsaurus_yson::YsonValue {
            attributes: None,
            node: ytsaurus_yson::YsonNode::Map(
                self.values
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_bytes().to_vec(),
                            ytsaurus_yson::YsonValue {
                                attributes: None,
                                node: ytsaurus_yson::YsonNode::Int64(*value),
                            },
                        )
                    })
                    .collect(),
            ),
        };

        // Text, not binary: this is the format the reference implementation
        // sends, and descriptor 5 carries no format negotiation to say
        // otherwise.
        let mut encoded =
            ytsaurus_yson::to_vec(&map, ytsaurus_yson::YsonFormat::Text).map_err(|e| {
                JobError::Statistics {
                    reason: format!("could not encode them: {e}"),
                }
            })?;
        // A list fragment: the separator is part of the record.
        encoded.push(b';');
        Ok(encoded)
    }

    /// Refuses a new name once the job's allowance is used up.
    fn reserve(&self, name: &str) -> Result<()> {
        if self.values.len() >= MAX_STATISTICS && !self.values.contains_key(name) {
            return Err(JobError::TooManyStatistics {
                limit: MAX_STATISTICS,
                name: name.to_owned(),
            });
        }
        Ok(())
    }
}

impl Drop for JobStatistics {
    /// Last-ditch send, so statistics are not silently lost when `finish` was
    /// forgotten. It cannot report a failure, which is why `finish` exists.
    fn drop(&mut self) {
        if self.finished || self.values.is_empty() {
            return;
        }
        if let Err(e) = self.finish() {
            eprintln!("ytsaurus-job: could not send job statistics: {e}");
        }
    }
}

/// Writes to descriptor 5 without taking ownership of it.
///
/// The descriptor belongs to the job proxy: closing it — which dropping a
/// `File` would do — would take the statistics channel with it, exactly as
/// with the output tables.
fn write_to_statistics_fd(bytes: &[u8]) -> Result<()> {
    use std::mem::ManuallyDrop;
    use std::os::fd::FromRawFd;

    // SAFETY: descriptor 5 is opened by the job proxy for this purpose, and
    // `ManuallyDrop` keeps this from closing it.
    let mut file = ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(STATISTICS_FD) });

    file.write_all(bytes)
        .and_then(|()| file.flush())
        .map_err(|source| JobError::Statistics {
            reason: format!("writing to descriptor {STATISTICS_FD}: {source}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_accumulate_and_replace() {
        let mut stats = JobStatistics::new();

        stats.add("rows", 1).unwrap();
        stats.add("rows", 2).unwrap();
        assert_eq!(stats.get("rows"), Some(3));

        stats.set("rows", 10).unwrap();
        assert_eq!(stats.get("rows"), Some(10));
        assert_eq!(stats.len(), 1);
    }

    #[test]
    fn a_counter_that_runs_away_saturates_instead_of_panicking() {
        let mut stats = JobStatistics::new();
        stats.set("rows", i64::MAX).unwrap();
        stats.add("rows", 1).unwrap();
        assert_eq!(stats.get("rows"), Some(i64::MAX));
    }

    #[test]
    fn the_hundred_and_twenty_ninth_name_is_refused() {
        let mut stats = JobStatistics::new();
        for i in 0..MAX_STATISTICS {
            stats.add(format!("stat_{i}"), 1).unwrap();
        }

        // An existing name still works — the limit is on names, not on writes.
        stats.add("stat_0", 1).unwrap();
        assert_eq!(stats.get("stat_0"), Some(2));

        let err = stats.add("one_too_many", 1).expect_err("must refuse");
        assert!(err.to_string().contains("128"), "{err}");
        assert!(err.to_string().contains("one_too_many"), "{err}");
    }

    #[test]
    fn the_encoding_is_a_yson_list_fragment_of_one_map() {
        let mut stats = JobStatistics::new();
        stats.set("rows/rejected", 7).unwrap();
        stats.set("bytes", 4096).unwrap();

        let encoded = String::from_utf8(stats.encode().unwrap()).unwrap();

        // Keys sort, which is what a BTreeMap gives and what makes this
        // assertion stable.
        assert_eq!(encoded, r#"{bytes=4096;"rows/rejected"=7};"#);
    }

    #[test]
    fn nothing_is_encoded_for_no_statistics() {
        let mut stats = JobStatistics::new();
        assert!(stats.is_empty());
        // No descriptor is touched, which is what makes this safe to call in a
        // job that recorded nothing.
        stats.finish().unwrap();
    }
}
