//! `sessionize` — a production-shaped pilot: access-log sessionization.
//!
//! This exists to put `ytsaurus-job` under the kind of load a real task
//! generates, rather than the tidy shape an example usually has. Specifically it
//! wants: wide rows with mixed types, byte columns that are not UTF-8, several
//! output tables, a reduce over a realistic key, and input that is partly
//! malformed.
//!
//! ## Map phase
//!
//! Validates each event and routes it:
//!
//! - table 0 `events`   — well-formed events, keyed for the reduce
//! - table 1 `rejects`  — rows that failed validation, with the reason
//!
//! A bad row must never fail the job. In a real pipeline one corrupt row in a
//! billion should not cost the whole operation, so the mapper quarantines
//! instead of panicking.
//!
//! ## Reduce phase
//!
//! Groups by `user_id`, splits each user's events into sessions on a 30-minute
//! inactivity gap, and emits:
//!
//! - table 0 `sessions` — one row per session
//! - table 1 `users`    — one row per user, aggregating their sessions
//!
//! ```sh
//! scripts/build-worker.sh sessionize
//! # see tests/e2e/run_pilot.sh for the full operation invocation
//! ```

use std::borrow::Cow;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use ytsaurus_job::{Event, JobError, JobReader, JobWriter, Row};

/// Inactivity gap that starts a new session, in microseconds.
const SESSION_GAP_US: i64 = 30 * 60 * 1_000_000;

/// A raw access-log event, as it arrives.
///
/// Deliberately wide and mixed-typed. `user_id` and `user_agent` are byte
/// columns: user agents in real logs are routinely not valid UTF-8, and a job
/// that declared them `String` would fail on those rows.
#[derive(Deserialize)]
struct RawEvent<'a> {
    #[serde(with = "serde_bytes", borrow)]
    user_id: &'a [u8],
    timestamp: i64,
    #[serde(borrow)]
    url: &'a str,
    #[serde(default, borrow)]
    referer: Option<&'a str>,
    #[serde(with = "serde_bytes", borrow)]
    user_agent: &'a [u8],
    status: i64,
    bytes_sent: u64,
    is_mobile: bool,
    latency_ms: f64,
}

/// A validated event, as handed to the reducer.
#[derive(Serialize, Deserialize)]
struct CleanEvent<'a> {
    #[serde(with = "serde_bytes", borrow)]
    user_id: &'a [u8],
    timestamp: i64,
    #[serde(borrow)]
    url: &'a str,
    #[serde(with = "serde_bytes", borrow)]
    user_agent: &'a [u8],
    status: i64,
    bytes_sent: u64,
    is_mobile: bool,
    latency_ms: f64,
    /// Whether the request arrived from another site.
    is_external: bool,
}

/// A row that failed validation, kept for inspection rather than dropped.
///
/// Borrows the offending row rather than copying it. The rejects path exists to
/// be cheap when a fraction of a huge input is corrupt, and `raw: Vec<u8>` would
/// copy every bad row for no reason — the value is serialized before the borrow
/// ends.
#[derive(Serialize)]
struct Reject<'a> {
    #[serde(with = "serde_bytes")]
    raw: &'a [u8],
    reason: &'a str,
    row_index: Option<i64>,
}

#[derive(Serialize)]
struct Session {
    #[serde(with = "serde_bytes")]
    user_id: Vec<u8>,
    session_index: i64,
    started_at: i64,
    ended_at: i64,
    duration_us: i64,
    hits: i64,
    bytes_sent: u64,
    errors: i64,
    is_mobile: bool,
    mean_latency_ms: f64,
    #[serde(with = "serde_bytes")]
    entry_url: Vec<u8>,
}

#[derive(Serialize)]
struct UserSummary {
    #[serde(with = "serde_bytes")]
    user_id: Vec<u8>,
    sessions: i64,
    hits: i64,
    bytes_sent: u64,
    errors: i64,
    total_duration_us: i64,
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    match mode.as_str() {
        "map" => ytsaurus_job::run(map),
        "reduce" => ytsaurus_job::run(reduce),
        other => {
            eprintln!("usage: sessionize <map|reduce>   (got {other:?})");
            std::process::exit(2);
        }
    }
}

/// Why a row is unusable. Returning a reason rather than a bool means the
/// rejects table explains itself.
fn validate(event: &RawEvent<'_>) -> Result<(), &'static str> {
    if event.user_id.is_empty() {
        return Err("empty user_id");
    }
    if event.timestamp <= 0 {
        return Err("non-positive timestamp");
    }
    if !(100..=599).contains(&event.status) {
        return Err("status outside 100..=599");
    }
    if !event.latency_ms.is_finite() || event.latency_ms < 0.0 {
        return Err("latency_ms is negative or not finite");
    }
    if event.url.is_empty() {
        return Err("empty url");
    }
    Ok(())
}

fn map() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(2)?;

    let mut kept = 0u64;
    let mut rejected = 0u64;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };

        // FRICTION 1: "row did not parse" and "row parsed but is invalid" call
        // for identical handling, but they arrive as different error types --
        // `JobError` from the runtime, our own reason from `validate`. Folding
        // them means stringifying, which loses the structure of `JobError`.
        //
        // `Cow` keeps the cost off the common path at least: a validation
        // failure borrows a `&'static str` and allocates nothing; only a decode
        // failure, which should be rare, pays for a `String`.
        let outcome: Result<CleanEvent, Cow<'static, str>> = row
            .parse::<RawEvent>()
            .map_err(|e| Cow::Owned(e.to_string()))
            .and_then(|event| {
                validate(&event).map_err(Cow::Borrowed)?;
                Ok(CleanEvent {
                    is_external: event
                        .referer
                        .is_some_and(|r| !r.is_empty() && !r.starts_with('/')),
                    user_id: event.user_id,
                    timestamp: event.timestamp,
                    url: event.url,
                    user_agent: event.user_agent,
                    status: event.status,
                    bytes_sent: event.bytes_sent,
                    is_mobile: event.is_mobile,
                    latency_ms: event.latency_ms,
                })
            });

        match outcome {
            Ok(clean) => {
                writer.write(0, &clean)?;
                kept += 1;
            }
            Err(reason) => {
                quarantine(&mut writer, &row, &reason)?;
                rejected += 1;
            }
        }
    }

    // stderr shows up in the operation UI, so this is the job explaining itself.
    eprintln!("sessionize map: kept {kept}, rejected {rejected}");
    writer.finish()
}

/// Writes a bad row to the rejects table, preserving the original bytes.
fn quarantine(writer: &mut JobWriter, row: &Row<'_>, reason: &str) -> Result<(), JobError> {
    writer.write(
        1,
        &Reject {
            raw: row.raw(),
            reason,
            row_index: row.row_index,
        },
    )
}

fn reduce() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(2)?;

    let mut groups = reader.groups();
    let mut users = 0u64;
    let mut sessions_emitted = 0u64;

    while let Some(mut group) = groups.next_group()? {
        // FRICTION 2: the reduce key is only discoverable by parsing the first
        // row of the group. `Group` knows a boundary was crossed but carries no
        // key, so every reducer re-derives it and must handle the "no rows yet"
        // case that cannot actually happen.
        let mut user_id: Option<Vec<u8>> = None;

        let mut current: Option<SessionAcc> = None;
        let mut finished: Vec<Session> = Vec::new();

        while let Some(row) = group.next_row()? {
            let event: CleanEvent = row.parse()?;

            if user_id.is_none() {
                user_id = Some(event.user_id.to_vec());
            }

            // A gap longer than the threshold closes the current session.
            let starts_new_session = match &current {
                Some(acc) => event.timestamp - acc.ended_at > SESSION_GAP_US,
                None => true,
            };

            if starts_new_session {
                if let Some(acc) = current.take() {
                    finished.push(acc.finish(finished.len() as i64));
                }
                current = Some(SessionAcc::start(&event));
            } else if let Some(acc) = &mut current {
                acc.push(&event);
            }
        }

        let Some(user_id) = user_id else { continue };
        if let Some(acc) = current {
            finished.push(acc.finish(finished.len() as i64));
        }

        let mut summary = UserSummary {
            user_id: user_id.clone(),
            sessions: finished.len() as i64,
            hits: 0,
            bytes_sent: 0,
            errors: 0,
            total_duration_us: 0,
        };

        for session in &finished {
            summary.hits += session.hits;
            summary.bytes_sent += session.bytes_sent;
            summary.errors += session.errors;
            summary.total_duration_us += session.duration_us;
            writer.write(0, session)?;
            sessions_emitted += 1;
        }

        writer.write(1, &summary)?;
        users += 1;
    }

    eprintln!("sessionize reduce: {users} users, {sessions_emitted} sessions");
    writer.finish()
}

/// A session being accumulated.
///
/// Owned rather than borrowed: it outlives the row it started from, which is
/// exactly the case the zero-copy reader cannot serve.
struct SessionAcc {
    user_id: Vec<u8>,
    started_at: i64,
    ended_at: i64,
    hits: i64,
    bytes_sent: u64,
    errors: i64,
    is_mobile: bool,
    latency_total: f64,
    entry_url: Vec<u8>,
}

impl SessionAcc {
    fn start(event: &CleanEvent<'_>) -> Self {
        Self {
            user_id: event.user_id.to_vec(),
            started_at: event.timestamp,
            ended_at: event.timestamp,
            hits: 1,
            bytes_sent: event.bytes_sent,
            errors: i64::from(event.status >= 400),
            is_mobile: event.is_mobile,
            latency_total: event.latency_ms,
            entry_url: event.url.as_bytes().to_vec(),
        }
    }

    fn push(&mut self, event: &CleanEvent<'_>) {
        // Events within a group are not guaranteed sorted unless the operation
        // sorts by timestamp, so track the extremes rather than assuming order.
        self.started_at = self.started_at.min(event.timestamp);
        self.ended_at = self.ended_at.max(event.timestamp);
        self.hits += 1;
        self.bytes_sent += event.bytes_sent;
        self.errors += i64::from(event.status >= 400);
        self.latency_total += event.latency_ms;
        self.is_mobile |= event.is_mobile;
    }

    fn finish(self, index: i64) -> Session {
        Session {
            user_id: self.user_id,
            session_index: index,
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_us: self.ended_at - self.started_at,
            hits: self.hits,
            bytes_sent: self.bytes_sent,
            errors: self.errors,
            is_mobile: self.is_mobile,
            mean_latency_ms: self.latency_total / self.hits as f64,
            entry_url: self.entry_url,
        }
    }
}

/// Unused today, but the shape a per-user cache would take. Kept out of the hot
/// path deliberately: see FRICTION 3 in the friction log.
#[allow(dead_code)]
type UserCache = HashMap<Vec<u8>, u64>;

#[cfg(test)]
mod tests {
    use super::*;

    fn event(ts: i64, status: i64) -> CleanEvent<'static> {
        CleanEvent {
            user_id: b"u1",
            timestamp: ts,
            url: "/a",
            user_agent: b"agent",
            status,
            bytes_sent: 10,
            is_mobile: false,
            latency_ms: 1.0,
            is_external: false,
        }
    }

    #[test]
    fn a_gap_longer_than_the_threshold_splits_the_session() {
        let mut acc = SessionAcc::start(&event(0, 200));
        acc.push(&event(SESSION_GAP_US - 1, 200));
        let s = acc.finish(0);
        assert_eq!(s.hits, 2);
        assert_eq!(s.duration_us, SESSION_GAP_US - 1);
    }

    #[test]
    fn errors_are_counted_from_status() {
        let mut acc = SessionAcc::start(&event(0, 200));
        acc.push(&event(1, 404));
        acc.push(&event(2, 500));
        acc.push(&event(3, 302));
        assert_eq!(acc.finish(0).errors, 2);
    }

    #[test]
    fn out_of_order_events_still_bound_the_session() {
        let mut acc = SessionAcc::start(&event(100, 200));
        acc.push(&event(50, 200));
        acc.push(&event(150, 200));
        let s = acc.finish(0);
        assert_eq!(s.started_at, 50);
        assert_eq!(s.ended_at, 150);
        assert_eq!(s.duration_us, 100);
    }

    #[test]
    fn mean_latency_is_averaged_over_hits() {
        let mut acc = SessionAcc::start(&event(0, 200));
        acc.push(&event(1, 200));
        acc.push(&event(2, 200));
        assert!((acc.finish(0).mean_latency_ms - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn validation_rejects_what_it_should() {
        let ok = RawEvent {
            user_id: b"u",
            timestamp: 1,
            url: "/x",
            referer: None,
            user_agent: b"a",
            status: 200,
            bytes_sent: 1,
            is_mobile: false,
            latency_ms: 1.0,
        };
        assert!(validate(&ok).is_ok());

        let cases: [(RawEvent, &str); 5] = [
            (
                RawEvent {
                    user_id: b"",
                    ..ok_like()
                },
                "empty user_id",
            ),
            (
                RawEvent {
                    timestamp: 0,
                    ..ok_like()
                },
                "non-positive timestamp",
            ),
            (
                RawEvent {
                    status: 999,
                    ..ok_like()
                },
                "status outside 100..=599",
            ),
            (
                RawEvent {
                    latency_ms: f64::NAN,
                    ..ok_like()
                },
                "latency_ms is negative or not finite",
            ),
            (
                RawEvent {
                    url: "",
                    ..ok_like()
                },
                "empty url",
            ),
        ];
        for (event, expected) in cases {
            assert_eq!(validate(&event), Err(expected));
        }
    }

    fn ok_like() -> RawEvent<'static> {
        RawEvent {
            user_id: b"u",
            timestamp: 1,
            url: "/x",
            referer: None,
            user_agent: b"a",
            status: 200,
            bytes_sent: 1,
            is_mobile: false,
            latency_ms: 1.0,
        }
    }
}
