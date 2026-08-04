//! Job-level diagnostics for a failed operation.
//!
//! An operation that fails reports a state and a category — `failed`, "User job
//! failed". The reason is in what the job itself printed before it died, and
//! that takes two more commands: `list_jobs` names the jobs that failed, and
//! `get_job_stderr` returns what each one wrote. Without them the only way to
//! learn anything is the web UI.
//!
//! Both are documented in the
//! [command reference](https://ytsaurus.tech/docs/en/api/commands): `list_jobs`
//! is light and returns a structured `{jobs=[…]}`, `get_job_stderr` is heavy and
//! returns the stderr as raw bytes.

use ytsaurus_yson::{YsonNode, YsonValue};

/// One job of an operation, as `list_jobs` reports it.
///
/// A subset of the cluster's `TJob`: the fields needed to name a job and say
/// why it failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInfo {
    /// Job ID, in the form [`Client::get_job_stderr`](crate::Client::get_job_stderr) expects.
    pub id: String,
    /// Job state: `failed`, `completed`, `running`, `aborted`, …
    pub state: String,
    /// The exec node that ran it, as `host:port`.
    pub address: Option<String>,
    /// The error that ended the job, flattened to one line.
    pub error: Option<String>,
    /// How much stderr the cluster says it saved.
    ///
    /// A hint, not a fact: a local cluster reported `1` for a job whose stderr
    /// was several hundred bytes, so this is not a length to size anything by.
    /// `None` means the field was absent.
    pub stderr_size: Option<u64>,
}

/// A failed job and what it printed, as carried by
/// [`ClientError::OperationFailed`](crate::ClientError::OperationFailed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFailure {
    /// Job ID, for `get_job_stderr` or the web UI.
    pub id: String,
    /// The exec node that ran it, as `host:port`.
    pub address: Option<String>,
    /// The error that ended the job, flattened to one line.
    pub error: Option<String>,
    /// The tail of the job's stderr, bounded and decoded lossily.
    pub stderr: Option<String>,
}

/// Reads the `jobs` list of a `list_jobs` response.
///
/// A job whose ID is missing or unreadable is dropped: there is nothing to ask
/// the cluster about it, and an anonymous entry in an error message is noise.
pub(crate) fn parse_jobs(jobs: &YsonValue) -> Vec<JobInfo> {
    let YsonNode::List(items) = &jobs.node else {
        return Vec::new();
    };
    items.iter().filter_map(parse_job).collect()
}

fn parse_job(job: &YsonValue) -> Option<JobInfo> {
    // `list_jobs` calls it `id`, `get_job` calls it `job_id`. Same value.
    let id = text(field(job, "id").or_else(|| field(job, "job_id"))?)?;

    Some(JobInfo {
        id,
        state: field(job, "state").and_then(text).unwrap_or_default(),
        address: field(job, "address").and_then(text),
        error: field(job, "error").and_then(error_summary),
        stderr_size: field(job, "stderr_size").and_then(count),
    })
}

/// Flattens a YTsaurus error document to one line.
///
/// The outer message is a category ("User job failed"); the cause is at the
/// bottom of `inner_errors` ("Process exited with code 1"). Both are useful, so
/// both are kept.
pub(crate) fn error_summary(error: &YsonValue) -> Option<String> {
    let top = text(field(error, "message")?)?;
    match innermost_message(error) {
        Some(inner) if inner != top => Some(format!("{top}: {inner}")),
        _ => Some(top),
    }
}

fn innermost_message(error: &YsonValue) -> Option<String> {
    let YsonNode::List(inner) = &field(error, "inner_errors")?.node else {
        return None;
    };
    let first = inner.first()?;
    innermost_message(first).or_else(|| field(first, "message").and_then(text))
}

/// A dict entry, without the panic `YsonValue`'s `Index` would give.
pub(crate) fn field<'a>(value: &'a YsonValue, key: &str) -> Option<&'a YsonValue> {
    match &value.node {
        YsonNode::Map(m) => m.get(key.as_bytes()),
        _ => None,
    }
}

/// A string field. YSON strings are byte strings, so this decodes lossily
/// rather than refusing a name the cluster is happy with.
fn text(value: &YsonValue) -> Option<String> {
    match &value.node {
        YsonNode::String(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

/// A byte count, which the cluster sends unsigned but which nothing forbids
/// arriving signed.
fn count(value: &YsonValue) -> Option<u64> {
    match value.node {
        YsonNode::Int64(v) => u64::try_from(v).ok(),
        YsonNode::Uint64(v) => Some(v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::{YsonFormat, from_slice};

    fn parse(text: &str) -> YsonValue {
        from_slice(text.as_bytes(), YsonFormat::Text).expect("valid YSON")
    }

    /// The response shape from the command reference, trimmed to the fields
    /// this client reads.
    const LIST_JOBS_RESPONSE: &str = r#"{
        "jobs" = [
            {
                "id" = "55aff293-7ef14284-3fe0384-3e07";
                "type" = "map";
                "state" = "failed";
                "address" = "hostname.net:9012";
                "fail_context_size" = 973230u;
                "stderr_size" = 1024u;
                "error" = {
                    "code" = 1205;
                    "message" = "User job failed";
                    "inner_errors" = [
                        {
                            "code" = 10000;
                            "message" = "Process exited with code 101";
                        };
                    ];
                };
            };
            {
                "id" = "69ae20a7-887b25ab-3fe0384-3cff";
                "type" = "map";
                "state" = "running";
                "address" = "hostname.net:9012";
            };
        ];
        "state_counts" = {"running" = 1; "failed" = 1};
    }"#;

    #[test]
    fn reads_the_documented_list_jobs_response() {
        let response = parse(LIST_JOBS_RESPONSE);
        let jobs = parse_jobs(field(&response, "jobs").expect("has jobs"));

        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].id, "55aff293-7ef14284-3fe0384-3e07");
        assert_eq!(jobs[0].state, "failed");
        assert_eq!(jobs[0].address.as_deref(), Some("hostname.net:9012"));
        assert_eq!(jobs[0].stderr_size, Some(1024));
        assert_eq!(
            jobs[0].error.as_deref(),
            Some("User job failed: Process exited with code 101")
        );

        // A running job has no error and no saved stderr, and the absence must
        // stay distinguishable from "the cluster saved nothing".
        assert_eq!(jobs[1].state, "running");
        assert_eq!(jobs[1].error, None);
        assert_eq!(jobs[1].stderr_size, None);
    }

    /// A real `list_jobs` response, captured from the local cluster after
    /// running `cargo run -p ytsaurus-client --example diagnose`. The
    /// documented shape above is what the reference promises; this is what a
    /// cluster actually sends, which is not the same thing — it carries
    /// `attributes` maps full of `u64`s, an entity `cypress_job_count`, and a
    /// `brief_statistics` with YSON attributes on it.
    const CAPTURED: &str = include_str!("../tests/fixtures/list_jobs_failed.yson");

    #[test]
    fn reads_a_response_captured_from_a_cluster() {
        let response = parse(CAPTURED);
        let jobs = parse_jobs(field(&response, "jobs").expect("has jobs"));

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "3dc650de-c17d51d2-10384-1000001");
        assert_eq!(jobs[0].state, "failed");
        assert_eq!(jobs[0].address.as_deref(), Some("localhost:24403"));
        assert_eq!(
            jobs[0].error.as_deref(),
            Some("User job failed: Process terminated by signal 6"),
            "the signal is the whole point — signal 6 is a Rust panic under \
             panic=abort, and only the inner error names it"
        );

        // The cluster said one byte; the job's stderr was several hundred. The
        // client asks for stderr regardless, and this is why.
        assert_eq!(jobs[0].stderr_size, Some(1));
    }

    #[test]
    fn a_job_without_an_id_is_dropped() {
        let response = parse(r#"{"jobs" = [{"state" = "failed"}; {"id" = "a-b-c-d"}]}"#);
        let jobs = parse_jobs(field(&response, "jobs").expect("has jobs"));

        assert_eq!(jobs.len(), 1, "the entry with no id must not be reported");
        assert_eq!(jobs[0].id, "a-b-c-d");
    }

    #[test]
    fn a_response_without_a_job_list_yields_nothing() {
        assert!(parse_jobs(&parse(r#"{"jobs" = #}"#)["jobs"]).is_empty());
        assert!(parse_jobs(&parse(r#""not a list""#)).is_empty());
    }

    #[test]
    fn the_summary_reaches_the_innermost_message() {
        let error = parse(
            r#"{
                "message" = "Operation failed";
                "inner_errors" = [
                    {
                        "message" = "User job failed";
                        "inner_errors" = [{"message" = "Process exited with code 1"}];
                    };
                ];
            }"#,
        );
        assert_eq!(
            error_summary(&error).as_deref(),
            Some("Operation failed: Process exited with code 1")
        );
    }

    #[test]
    fn a_flat_error_is_not_repeated() {
        let error = parse(r#"{"message" = "User job failed"; "inner_errors" = []}"#);
        assert_eq!(error_summary(&error).as_deref(), Some("User job failed"));
    }

    #[test]
    fn an_error_without_a_message_has_no_summary() {
        assert_eq!(error_summary(&parse(r#"{"code" = 1}"#)), None);
    }

    #[test]
    fn a_non_utf8_field_is_kept_lossily() {
        // A job address the cluster is happy with but Rust would not accept as
        // a `&str`. Dropping the job over it would lose the failure.
        let job = YsonValue {
            attributes: None,
            node: YsonNode::Map(
                [
                    (b"id".to_vec(), string_value(b"a-b-c-d")),
                    (b"address".to_vec(), string_value(&[0xFF, 0xFE])),
                ]
                .into_iter()
                .collect(),
            ),
        };

        let parsed = parse_job(&job).expect("an id is all it takes");
        assert_eq!(parsed.address.as_deref(), Some("\u{FFFD}\u{FFFD}"));
    }

    fn string_value(bytes: &[u8]) -> YsonValue {
        YsonValue {
            attributes: None,
            node: YsonNode::String(bytes.to_vec()),
        }
    }

    #[test]
    fn a_signed_byte_count_is_accepted_and_a_negative_one_is_not() {
        assert_eq!(count(&parse("1024")), Some(1024));
        assert_eq!(count(&parse("1024u")), Some(1024));
        assert_eq!(count(&parse("-1")), None);
        assert_eq!(count(&parse(r#""1024""#)), None);
    }
}
