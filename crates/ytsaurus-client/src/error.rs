//! Errors the client can fail with.

use thiserror::Error;

use crate::jobs::JobFailure;

/// Shorthand for a client result.
pub type Result<T, E = ClientError> = std::result::Result<T, E>;

/// Something went wrong talking to the cluster. Each variant says when it is
/// returned.
///
/// Non-exhaustive: a `match` needs a `_` arm, since the ways a cluster can
/// refuse are the cluster's to add.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The request could not be made, or the connection failed.
    #[error("{command}: transport error: {source}{}", certificate_advice(.source))]
    Transport {
        /// The API command being attempted.
        command: String,
        /// The underlying HTTP error.
        #[source]
        source: Box<ureq::Error>,
    },

    /// The cluster answered with an error document: the `X-YT-Error` header,
    /// or a batch part's `error`. `code` and `message` are lifted out of it,
    /// the message joined with the innermost of `inner_errors`; `raw` keeps it
    /// whole.
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

    /// A redirect was refused rather than followed; `refusal` says which rule
    /// it met.
    ///
    /// A control proxy answers a heavy read with a cross-host `307` to a data
    /// proxy ([HTTP proxy reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#return_codes)).
    /// Followed without `Authorization`, it ends in `Client is missing
    /// credentials` about a token that may be fine, so this client goes nowhere
    /// and says where the proxy pointed. A redirect that stays on the request's
    /// origin is followed, credentials and all. See [Redirects](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#redirects).
    #[error(
        "{command}: the proxy answered HTTP {status} and redirected to {location}, \
         which this client did not follow: {refusal}{}",
        redirect_advice(.heavy)
    )]
    Redirected {
        /// The API command that was redirected.
        command: String,
        /// The redirect status the proxy answered with — `307` in practice.
        status: u16,
        /// Where it pointed, resolved against the address the request went to,
        /// so a relative `Location` still names a host. Usually a data proxy on
        /// a different one.
        location: String,
        /// Which rule the redirect met.
        refusal: RedirectRefusal,
        /// Whether the command reads or writes a data stream; only then does the
        /// message advise a heavy proxy.
        heavy: bool,
    },

    /// A response could not be decoded.
    #[error("{command}: could not decode the response: {reason}")]
    Decode {
        /// The API command whose response was unreadable.
        command: String,
        /// What went wrong.
        reason: String,
    },

    /// A buffered response ran past what this client will hold in memory,
    /// `limit` bytes after decompression. Refused rather than truncated, and
    /// never retried or blamed on the host; the message names the streaming
    /// method where the command has one.
    ///
    /// Not a [`ClientError::Decode`], which means bytes were read and had the
    /// wrong shape. `limit` is what is held, not what the process needs: the
    /// buffer grows by doubling, so peak residency runs above it
    /// ([Response size limits](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#response-size-limits)).
    #[error(
        "{command}: the response ran past the {} this client will hold in \
         memory{}",
        cap_size(.limit),
        streaming_advice(.command)
    )]
    ResponseTooLarge {
        /// The API command whose response was too large.
        command: String,
        /// The ceiling it ran past, in decoded bytes.
        limit: u64,
    },

    /// A split batch stopped part of the way through, and the requests before
    /// the failure have already run on the cluster.
    ///
    /// Returned only when [`Client::execute_batch`](crate::Client::execute_batch)
    /// split a batch larger than
    /// [`BatchRequest::with_max_part_size`](crate::BatchRequest::with_max_part_size)
    /// and a later request failed wholesale; an unsplit batch fails with the
    /// underlying error. There is no rollback, and re-running the
    /// [`BatchRequest`](crate::BatchRequest) applies the landed parts again
    /// under fresh mutation ids.
    ///
    /// `answered` holds the per-part results of every completed request, in
    /// part order, so the parts never attempted are `batch[answered.len()..]`.
    /// That is where the answers stop, not the effects: the failed request
    /// still ran its parts, and an `Err` among the answers applied nothing. Use
    /// a transaction, or one request, where partial application matters.
    #[error(
        "execute_batch: {} of {parts} parts were answered for before the batch stopped — \
         that is where the answers stop, not where the effects do: the request that failed \
         still ran its parts, and an Err among the answers applied nothing: {cause}",
        .answered.len()
    )]
    BatchInterrupted {
        /// The parts already answered, in part order — every part of every
        /// request that completed, `Ok` and `Err` alike.
        answered: Vec<Result<ytsaurus_yson::YsonValue>>,
        /// How many parts the batch held in all.
        parts: usize,
        /// Why the rest never went.
        #[source]
        cause: Box<ClientError>,
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
        /// The failed jobs, with what they printed. Empty if none were reported,
        /// if [`Client::with_job_diagnostics`](crate::Client::with_job_diagnostics)
        /// is off, or if fetching them failed.
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

    /// Refused by this client before anything was sent: the environment did
    /// not describe a cluster, or a call or its arguments failed a local check.
    #[error("{0}")]
    Config(String),
}

/// Why a redirect was refused. Each variant renders the clause the error
/// message carries.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedirectRefusal {
    /// The request carries credentials, and the redirect leaves the origin they
    /// were addressed to.
    #[error(
        "the request carries credentials and the redirect leaves the host they \
         were addressed to. Following it drops the `Authorization` header — \
         `ureq` does that by default — and the cluster then answers with a \
         credentials failure about a token that may be perfectly good. The \
         token was not sent to the host that answered, so start with the \
         redirect rather than with the token."
    )]
    Credentials,

    /// The request body is a stream already partly sent (`write_table` from an
    /// iterator, `raw_command_upload`), so it cannot be sent again, wherever the
    /// redirect points. A body held in memory is resent.
    #[error(
        "the request body is read as it is sent, so this client cannot send it \
         to the address the redirect named — a reader that has already begun \
         to drain cannot be rewound. A write that arrived carrying no rows is \
         answered much like one that succeeded, which is worse than failing. \
         Send the body from memory, or address the host you meant to reach."
    )]
    Body,

    /// The request carries data, and the redirect leaves the origin it was
    /// addressed to, token or not. A zero-length body is not data.
    #[error(
        "the request carries data and the redirect leaves the host it was \
         addressed to. Sending it on would hand the body to a host the caller \
         never named, on the say-so of a header that arrived mid-flight. A \
         redirect that stays on the same host is followed, body and all; to \
         reach another one on purpose, ask the cluster for it and address it \
         yourself."
    )]
    Payload,

    /// The redirects did not end: past a bounded number of hops it is a loop.
    #[error(
        "the redirects did not end. This client follows a bounded number of \
         them and that bound was reached, which is a loop rather than a route."
    )]
    TooMany,
}

/// The advice an `UnknownIssuer` rejection needs: this client trusts the
/// compiled-in Mozilla bundle, not the machine's store, so it names
/// `YT_CA_BUNDLE` and the `platform-verifier` feature.
///
/// Classified by [`crate::retry::settled_certificate_verdict`], not by a
/// substring: `rustls-platform-verifier` can report a retriable
/// `Other(… "UnknownIssuer …")`. `NotValidForName` gets no advice, since no
/// root store mends a certificate for another host.
fn certificate_advice(source: &ureq::Error) -> &'static str {
    if crate::retry::settled_certificate_verdict(source) == Some("UnknownIssuer") {
        " The chain does not end in a root this client trusts, which is the \
         Mozilla bundle compiled in and not what the machine trusts: point \
         YT_CA_BUNDLE at a PEM file of roots (the `yt` CLI reads the same \
         variable; on Linux the system bundle is usually \
         /etc/ssl/certs/ca-certificates.crt), or build with the \
         `platform-verifier` feature to trust whatever the operating system \
         does."
    } else {
        ""
    }
}

/// The sentence only a heavy command can act on. See [`ClientError::Redirected`].
fn redirect_advice(heavy: &bool) -> &'static str {
    if *heavy {
        " Heavy commands belong on a heavy proxy: ask the cluster for one \
         (`Client::heavy_proxy`) and address it directly."
    } else {
        ""
    }
}

/// The cap as `512 MiB (536870912 bytes)` when it is a whole number of
/// mebibytes, else in bytes.
fn cap_size(limit: &u64) -> String {
    const MIB: u64 = 1024 * 1024;

    if *limit >= MIB && limit.is_multiple_of(MIB) {
        format!("{} MiB ({limit} bytes)", limit / MIB)
    } else {
        format!("{limit} bytes")
    }
}

/// The streaming method that avoids the cap, for a command that has one. See
/// [`ClientError::ResponseTooLarge`].
fn streaming_advice(command: &str) -> &'static str {
    match command {
        // Every `read_table` variant sends this command name.
        "read_table" => " — Client::read_table_streaming moves the same bytes without holding them",
        "read_file" => " — Client::read_file_streaming moves the same bytes without holding them",
        _ => "",
    }
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

/// Renders the failed jobs under the operation's line, multi-line so that a
/// job's stderr stays readable.
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
    /// Builds a [`ClientError::Cluster`] from an `X-YT-Error` document, or a
    /// [`ClientError::Http`] if it is not the documented shape.
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

/// Keeps the last `limit` bytes of `s`, saying what was dropped: a job's
/// stderr puts the reason last.
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

    /// A transport failure carrying the `io::Error` `ureq` would have carried.
    fn transport(message: &str) -> ClientError {
        ClientError::Transport {
            command: "get".to_owned(),
            source: Box::new(ureq::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                message.to_owned(),
            ))),
        }
    }

    #[test]
    fn an_untrusted_root_names_the_two_things_that_change_it() {
        // Verbatim what a cluster behind a private CA answers on the very first
        // request, before any YTsaurus logic runs. On its own it says nothing
        // about whose roots were consulted or how to change them, and the
        // machine it fails on is usually one where `curl` works.
        let message = transport("invalid peer certificate: UnknownIssuer").to_string();

        assert!(message.contains("UnknownIssuer"), "{message}");
        assert!(message.contains("YT_CA_BUNDLE"), "{message}");
        assert!(message.contains("platform-verifier"), "{message}");
    }

    #[test]
    fn other_transport_failures_are_left_alone() {
        // A certificate that does not cover the host asked for is not a root
        // store problem, and a connection refused is not a TLS problem at all.
        // Advising a CA bundle for either sends the reader to rewrite the one
        // part of the configuration that is working.
        for message in [
            "invalid peer certificate: certificate not valid for name \
             \"cluster.example.net\"",
            "connection refused",
            // The one that a `contains` would get wrong, and the reason this
            // goes through `retry`'s classifier rather than looking for the
            // word: `Other(..)` is `rustls-platform-verifier` reporting a
            // passing condition of this machine — a revocation lookup that
            // timed out, a trust store briefly unreadable — which `retry`
            // treats as worth another attempt. Only that verifier produces it,
            // so advising `platform-verifier` here would be advice to enable
            // what is already on.
            "invalid peer certificate: Other(OtherError(\"UnknownIssuer lookup failed\"))",
        ] {
            let rendered = transport(message).to_string();
            // `ureq` renders an `Error::Io` with an `io: ` of its own, so this
            // is the whole message and nothing has been appended to it.
            assert_eq!(rendered, format!("get: transport error: io: {message}"));
        }
    }
}
