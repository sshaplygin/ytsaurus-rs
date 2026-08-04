//! Errors the client can fail with.

use thiserror::Error;

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
    #[error("operation {id} finished as {state}{}", failure_hint(.error))]
    OperationFailed {
        /// The operation's ID.
        id: String,
        /// Its terminal state — `failed`, `aborted`, …
        state: String,
        /// The operation's error document, when it has one.
        error: Option<String>,
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
