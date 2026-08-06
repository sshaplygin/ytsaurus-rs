//! Errors the client can fail with.

use thiserror::Error;

use crate::jobs::JobFailure;

/// Shorthand for a client result.
pub type Result<T, E = ClientError> = std::result::Result<T, E>;

/// Something went wrong talking to the cluster.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The request could not be made, or the connection failed.
    #[error("{command}: transport error: {source}")]
    Transport {
        /// The API command being attempted.
        command: String,
        /// The underlying HTTP error.
        #[source]
        source: Box<ureq::Error>,
    },

    /// The cluster reported an error.
    ///
    /// YTsaurus returns a structured error in the `X-YT-Error` header; the
    /// message and code are lifted out of it so the common case reads well,
    /// and the whole thing is kept in `raw` because the nested `inner_errors`
    /// are often where the real cause is.
    #[error("{command}: cluster error {code}: {message}")]
    Cluster {
        /// The API command that failed.
        command: String,
        /// YTsaurus error code.
        code: i64,
        /// Top-level error message.
        message: String,
        /// The full error document, as returned.
        raw: String,
    },

    /// The cluster answered with an unexpected HTTP status and no usable error.
    #[error("{command}: unexpected HTTP {status}{}", body_hint(.body))]
    Http {
        /// The API command that failed.
        command: String,
        /// The HTTP status returned.
        status: u16,
        /// Whatever body came back, truncated.
        body: String,
    },

    /// A request carrying credentials was redirected, and refused rather than
    /// followed.
    ///
    /// A control proxy does not refuse a heavy *read*: it answers `307
    /// Temporary Redirect` naming a data proxy on another host. Following that
    /// with the `Authorization` header attached hands a bearer token to a host
    /// the caller never chose; following it *without* — which is what `ureq`
    /// does by default — makes the request arrive unauthenticated, and the
    /// cluster then reports `Client is missing credentials` about a token that
    /// is perfectly valid. This error is the third answer: go nowhere, and say
    /// where the proxy pointed.
    #[error(
        "{command}: the proxy answered HTTP {status} and redirected to {location}, \
         which this client did not follow because the request carries \
         credentials — a redirect chooses the host they would be sent to, and \
         the caller did not. The token is not at fault. Heavy commands belong \
         on a heavy proxy: ask the cluster for one (`Client::heavy_proxy`) and \
         address it directly."
    )]
    Redirected {
        /// The API command that was redirected.
        command: String,
        /// The redirect status the proxy answered with — `307` in practice.
        status: u16,
        /// Where it pointed. Usually a data proxy on a different host.
        location: String,
    },

    /// A response could not be decoded.
    #[error("{command}: could not decode the response: {reason}")]
    Decode {
        /// The API command whose response was unreadable.
        command: String,
        /// What went wrong.
        reason: String,
    },

    /// Reading a local file failed.
    #[error("reading {path}: {source}")]
    Io {
        /// The path that could not be read.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// An operation finished in a state other than `completed`.
    #[error("operation {id} finished as {state}{}{}", failure_hint(.error), jobs_hint(.jobs))]
    OperationFailed {
        /// The operation's ID.
        id: String,
        /// Its terminal state — `failed`, `aborted`, …
        state: String,
        /// The operation's error document, when it has one.
        error: Option<String>,
        /// The jobs that failed, with what they printed.
        ///
        /// Empty if the cluster reported none, if job diagnostics are turned
        /// off (see
        /// [`Client::with_job_diagnostics`](crate::Client::with_job_diagnostics)),
        /// or if asking for them failed — collecting them must never replace
        /// the failure being reported.
        jobs: Vec<JobFailure>,
    },

    /// A binary that a cluster node could not run was about to be uploaded.
    #[error("{path} cannot run on a cluster node: {reason}")]
    NotAWorker {
        /// The binary that was refused.
        path: String,
        /// What is wrong with it, and what to do instead.
        reason: String,
    },

    /// The environment did not describe a cluster to talk to.
    #[error("{0}")]
    Config(String),
}

fn body_hint(body: &str) -> String {
    if body.trim().is_empty() {
        String::new()
    } else {
        format!(": {}", body.trim())
    }
}

fn failure_hint(error: &Option<String>) -> String {
    match error {
        Some(e) if !e.trim().is_empty() => format!(": {}", e.trim()),
        _ => String::new(),
    }
}

/// Renders the failed jobs under the operation's own line.
///
/// Deliberately multi-line: a job's stderr is what the user came for, and
/// squeezing a panic message onto one line is how it becomes unreadable.
fn jobs_hint(jobs: &[JobFailure]) -> String {
    let mut out = String::new();

    for job in jobs {
        out.push_str("\n  job ");
        out.push_str(&job.id);
        if let Some(address) = &job.address {
            out.push_str(&format!(" on {address}"));
        }
        if let Some(error) = &job.error {
            out.push_str(&format!(": {}", error.trim()));
        }

        if let Some(stderr) = &job.stderr
            && !stderr.trim().is_empty()
        {
            out.push_str("\n  stderr:");
            for line in stderr.lines() {
                out.push_str("\n    ");
                out.push_str(line);
            }
        }
    }

    out
}

impl ClientError {
    /// Builds a [`ClientError::Cluster`] from an `X-YT-Error` document.
    ///
    /// Falls back to [`ClientError::Http`] if the document is not the shape
    /// YTsaurus documents — better a slightly clumsy error than a panic while
    /// reporting one.
    pub(crate) fn from_yt_error(command: &str, status: u16, raw: &str) -> Self {
        let parsed: Option<serde_json::Value> = serde_json::from_str(raw).ok();

        match parsed {
            Some(value) => {
                let code = value
                    .get("code")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1);
                let message = value
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("(no message)")
                    .to_owned();

                // The useful detail is usually one level down.
                let message = match innermost_message(&value) {
                    Some(inner) if inner != message => format!("{message}: {inner}"),
                    _ => message,
                };

                ClientError::Cluster {
                    command: command.to_owned(),
                    code,
                    message,
                    raw: raw.to_owned(),
                }
            }
            None => ClientError::Http {
                command: command.to_owned(),
                status,
                body: truncate(raw, 400),
            },
        }
    }
}

/// Walks `inner_errors` to the deepest message, which is where YTsaurus tends
/// to put the actual cause.
fn innermost_message(value: &serde_json::Value) -> Option<String> {
    let inner = value.get("inner_errors")?.as_array()?;
    let first = inner.first()?;
    innermost_message(first).or_else(|| {
        first
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    })
}

pub(crate) fn truncate(s: &str, limit: usize) -> String {
    if s.len() <= limit {
        return s.to_owned();
    }
    let mut end = limit;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({} bytes total)", &s[..end], s.len())
}

/// Keeps the **last** `limit` bytes of `s`, saying what was dropped.
///
/// The tail rather than the head, because this is used on a job's stderr: a job
/// that logs as it works and then dies puts the reason last, and cutting from
/// the front would keep the startup chatter and throw away the panic.
pub(crate) fn tail(s: &str, limit: usize) -> String {
    if s.len() <= limit {
        return s.to_owned();
    }
    let mut start = s.len() - limit;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    format!(
        "… ({} bytes total, last {} shown)\n{}",
        s.len(),
        s.len() - start,
        &s[start..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(jobs: Vec<JobFailure>) -> ClientError {
        ClientError::OperationFailed {
            id: "1-2-3-4".to_owned(),
            state: "failed".to_owned(),
            error: Some("Operation failed: User job failed".to_owned()),
            jobs,
        }
    }

    #[test]
    fn a_failed_operation_reports_what_the_job_printed() {
        let message = failure(vec![JobFailure {
            id: "a-b-c-d".to_owned(),
            address: Some("node.local:9012".to_owned()),
            error: Some("User job failed: Process exited with code 101".to_owned()),
            stderr: Some("boom: refusing row 7\nthread 'main' panicked".to_owned()),
        }])
        .to_string();

        assert!(
            message.contains("operation 1-2-3-4 finished as failed"),
            "{message}"
        );
        assert!(
            message.contains("job a-b-c-d on node.local:9012"),
            "{message}"
        );
        assert!(
            message.contains("Process exited with code 101"),
            "{message}"
        );
        // The point of the whole feature: the job's own words, indented under it.
        assert!(
            message.contains("\n    thread 'main' panicked"),
            "{message}"
        );
    }

    #[test]
    fn a_failure_with_no_job_information_stays_one_line() {
        let message = failure(Vec::new()).to_string();
        assert_eq!(
            message,
            "operation 1-2-3-4 finished as failed: Operation failed: User job failed"
        );
    }

    #[test]
    fn a_job_with_empty_stderr_gets_no_stderr_block() {
        let message = failure(vec![JobFailure {
            id: "a-b-c-d".to_owned(),
            address: None,
            error: None,
            stderr: Some("   \n".to_owned()),
        }])
        .to_string();

        assert!(message.ends_with("job a-b-c-d"), "{message}");
    }

    #[test]
    fn tail_keeps_the_end_and_says_how_much_it_dropped() {
        let long = format!("{}the panic", "chatter\n".repeat(100));
        let kept = tail(&long, 20);

        assert!(kept.ends_with("the panic"), "{kept}");
        assert!(
            kept.contains(&format!("{} bytes total", long.len())),
            "{kept}"
        );
        assert_eq!(tail("short", 20), "short");
    }

    #[test]
    fn tail_does_not_cut_a_character_in_half() {
        // Cut at every byte offset, so multi-byte characters are crossed
        // mid-character. A job's stderr is arbitrary bytes; this must not
        // panic, and what it keeps must still be the end of the text.
        let text = "ошибка в джобе";
        for limit in 0..=text.len() {
            let kept = tail(text, limit);
            // Everything after the "…" header, or the whole thing if it fits.
            let suffix = kept.rsplit_once('\n').map_or(kept.as_str(), |(_, s)| s);

            assert!(text.ends_with(suffix), "limit {limit}: {kept:?}");
            assert!(suffix.len() <= limit.max(text.len()), "limit {limit}");
        }
    }
}
