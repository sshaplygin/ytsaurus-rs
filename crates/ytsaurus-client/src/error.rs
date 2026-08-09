//! Errors the client can fail with.

use thiserror::Error;

use crate::jobs::JobFailure;

/// Shorthand for a client result.
pub type Result<T, E = ClientError> = std::result::Result<T, E>;

/// Something went wrong talking to the cluster.
///
/// **Non-exhaustive.** A `match` over this must carry a `_` arm: the ways a
/// cluster can refuse are the cluster's to add, not this crate's to freeze, and
/// every release so far has added one. Naming a variant, constructing one and
/// destructuring one all work as before.
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

    /// A redirect was refused rather than followed.
    ///
    /// A control proxy does not refuse a heavy *read*: it answers `307
    /// Temporary Redirect` naming a data proxy on another host — the
    /// [HTTP proxy reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#return_codes)
    /// gives that row as *"307 | Redirecting heavy queries from light to heavy
    /// proxies"*. Following it *without* the `Authorization` header — which is
    /// what `ureq` does by default — makes the request arrive unauthenticated,
    /// and the cluster then reports `Client is missing credentials` about a
    /// token that may be perfectly valid. Re-attaching it and going would
    /// follow an instruction the client never asked for, on a request already
    /// addressed elsewhere. This error is the third answer: go nowhere, and say
    /// where the proxy pointed.
    ///
    /// The message stops short of declaring the token good. It cannot know
    /// that — a gateway in front of the cluster may answer an expired token
    /// with a redirect of its own — so it reports the one thing this client is
    /// certain of: the credentials never reached the host that answered.
    ///
    /// Not every redirect ends here. One that stays on the origin the request
    /// was addressed to is followed, credentials and all, because nothing new
    /// learns the token by it; `refusal` says which rule this redirect met.
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
        /// Whether the redirected command reads or writes a data stream.
        ///
        /// Only those belong on a heavy proxy, so only those are told to go to
        /// one: a `create` that met a balancer's `301` cannot use that advice
        /// and is not given it.
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

    /// A buffered response ran past what this client will hold in memory.
    ///
    /// Its own variant rather than a [`ClientError::Decode`], which is what it
    /// was first written as. Every other `Decode` in this crate means *the
    /// bytes were read and were not the shape expected* — a YSON document that
    /// does not parse, a Skiff frame that ends early, an envelope missing the
    /// key the command answers under. This body was never read at all, and the
    /// difference is the whole of what the caller can do next: a `Decode`
    /// invites a look at the data, and this invites the streaming half of the
    /// same command, which the message names.
    ///
    /// Refused rather than truncated, and never retried — no amount of waiting
    /// shrinks a response, and the host that served it did nothing wrong. That
    /// second half is not this caller's concern alone: a heavy read blamed on
    /// its host takes a healthy data proxy out of the pool, and enough of them
    /// empty it. See `http::body_failure`.
    ///
    /// `limit` counts bytes **after** decompression, which is where they are
    /// actually held — and it is what this client *holds*, not what the
    /// process needs: the buffer grows by doubling and copies, so peak
    /// residency runs above the number. See `http::RESPONSE_LIMIT`.
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
    /// the failure have **already run on the cluster**.
    ///
    /// [`Client::execute_batch`](crate::Client::execute_batch) sends a batch
    /// larger than [`BatchRequest::with_max_part_size`](crate::BatchRequest::with_max_part_size)
    /// as several `execute_batch` requests. There is no rollback: when a later
    /// request fails wholesale, the earlier ones have run and whichever of
    /// their parts succeeded have taken effect. Reporting only the failure
    /// would hide that, and re-running the same
    /// [`BatchRequest`](crate::BatchRequest) is not a recovery either — a
    /// second execution mints fresh mutation ids, so the parts that already
    /// landed are applied a second time rather than deduplicated.
    ///
    /// So the prefix comes back with the failure: `answered` holds one entry
    /// per part of every request that completed, in part order, with exactly
    /// the per-part `Ok`/`Err` split [`Client::execute_batch`](crate::Client::execute_batch)
    /// would have handed back. `answered.len()` is where the batch stopped, and
    /// `parts` is how many there were, so the parts never attempted are
    /// `batch[answered.len()..]`.
    ///
    /// Only for a batch that **was** split: a batch that fits in one request
    /// fails with the underlying error itself, since there is no prefix to
    /// report. Put the sequence in a transaction, or keep it inside one
    /// request, if a partial application is not something the caller can act
    /// on.
    ///
    /// The rendered message says the same thing, deliberately. It is the
    /// sentence that reaches a log line and an `unwrap()` panic, so it must not
    /// draw a line the cluster does not honour: `answered.len()` is where the
    /// *answers* stop, not where the effects stop. The request that failed runs
    /// its parts whatever it answers — measured — and `answered` itself holds
    /// `Err` entries, which applied nothing at all.
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

/// Why a redirect was refused rather than followed.
///
/// The rules live with the transport that applies them; this is the half of
/// them a caller can branch on. Each variant renders the clause the error
/// message carries, so `refusal.to_string()` is what the user is told.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedirectRefusal {
    /// The request carries credentials, and the redirect leaves the origin
    /// they were addressed to.
    ///
    /// The one this crate exists to report. A same-origin redirect is followed
    /// instead: the token reaches no host it was not already going to.
    #[error(
        "the request carries credentials and the redirect leaves the host they \
         were addressed to. Following it drops the `Authorization` header — \
         `ureq` does that by default — and the cluster then answers with a \
         credentials failure about a token that may be perfectly good. The \
         token was not sent to the host that answered, so start with the \
         redirect rather than with the token."
    )]
    Credentials,

    /// The request body cannot be sent a second time.
    ///
    /// A redirect is followed by sending the same request to the address it
    /// named: same method, same payload. So a body is no reason to refuse one
    /// — a bodiless `POST`, which is most of API v4, goes wherever it is
    /// pointed, and a body held in memory goes with it.
    ///
    /// A body that is a **stream** cannot. `write_table` from an iterator and
    /// every `raw_command_upload` read their body as it is sent, so by the
    /// time the `3xx` arrives some of it has already gone and there is nothing
    /// to rewind. Sending what is left would be a different request; sending
    /// nothing is the expensive failure this refuses — a `write_table` that
    /// arrived carrying no rows is answered much like one that succeeded.
    #[error(
        "the request body is read as it is sent, so this client cannot send it \
         to the address the redirect named — a reader that has already begun \
         to drain cannot be rewound. A write that arrived carrying no rows is \
         answered much like one that succeeded, which is worse than failing. \
         Send the body from memory, or address the host you meant to reach."
    )]
    Body,

    /// The request carries data, and the redirect leaves the origin it was
    /// addressed to.
    ///
    /// [`RedirectRefusal::Credentials`] asked again about the other thing a
    /// caller chooses a host for. A token is not the only thing worth
    /// withholding from a host nobody named: a table's rows are the caller's
    /// own data, and a `Location` header is the far end of the connection
    /// asking for them to be sent somewhere else. So this one does not wait
    /// for a token to be present.
    ///
    /// A redirect that stays on the origin is followed, body and all — the
    /// bytes were already going there. And a body of length zero is not data:
    /// `Content-Length: 0` gives nothing away, so most of API v4 is unaffected.
    #[error(
        "the request carries data and the redirect leaves the host it was \
         addressed to. Sending it on would hand the body to a host the caller \
         never named, on the say-so of a header that arrived mid-flight. A \
         redirect that stays on the same host is followed, body and all; to \
         reach another one on purpose, ask the cluster for it and address it \
         yourself."
    )]
    Payload,

    /// The redirects did not end.
    ///
    /// This client follows a bounded number of same-origin hops; a balancer
    /// pointing at itself is a loop, not a route.
    #[error(
        "the redirects did not end. This client follows a bounded number of \
         them and that bound was reached, which is a loop rather than a route."
    )]
    TooMany,
}

/// The sentence a rejected root store needs, and nothing else does.
///
/// `invalid peer certificate: UnknownIssuer` is the whole of what a cluster
/// behind a private CA says on its first request, and it names neither the two
/// things that fix it nor the fact that this client's roots are not the
/// machine's. Every internal installation begins there — the `yt` CLI and the Go
/// SDK read the system store, so the machine where `curl` works is exactly the
/// machine where this fails — and the message that arrives is one word about a
/// certificate.
///
/// Classified by [`crate::retry::settled_certificate_verdict`] rather than by
/// looking for the word here. That function narrows three times — an
/// `ureq::Error::Io` of kind `InvalidData`, carrying `rustls`'s `invalid peer
/// certificate: ` prefix, whose reason **starts with** a settled verdict — and
/// every one of them matters to this message. A plain `contains("UnknownIssuer")`
/// would fire on `Other(OtherError("UnknownIssuer lookup failed"))`, which
/// `retry` deliberately treats as retriable: it is `rustls-platform-verifier`
/// reporting a passing condition of *this machine*, so the advice would tell a
/// build that already has the platform verifier to go and enable it.
///
/// Only `UnknownIssuer` gets this. `NotValidForName` is a certificate that does
/// not cover the host asked for, which no root store mends, and pointing its
/// reader at a CA bundle would send them to rewrite the one thing that is
/// working.
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

/// The cap, written the way the caller thinks about it.
///
/// `536870912` is the number a matcher wants and not the one a reader wants;
/// `512 MiB` is the reverse. Both, then — the round one first, because the
/// question the message answers is *how big is too big*, and nobody sizes a
/// machine in bytes. Only a whole number of mebibytes gets the treatment: a
/// test's cap of 4 096 reads better as itself than as `0.00390625 MiB`.
fn cap_size(limit: &u64) -> String {
    const MIB: u64 = 1024 * 1024;

    if *limit >= MIB && limit.is_multiple_of(MIB) {
        format!("{} MiB ({limit} bytes)", limit / MIB)
    } else {
        format!("{limit} bytes")
    }
}

/// The way past the cap, for a command that has one. See
/// [`ClientError::ResponseTooLarge`].
///
/// `the response body is larger than request limit: 536870912` — what `ureq`
/// says — names neither the number a caller can plan around nor the method
/// that makes the number irrelevant, and the streaming half of a read is a
/// method a caller may not know exists. A command with no streaming half
/// promises nothing.
fn streaming_advice(command: &str) -> &'static str {
    match command {
        // Every `read_table` shape — `_with_format`, `_skiff_table`, `_rows` —
        // sends this one command name.
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
