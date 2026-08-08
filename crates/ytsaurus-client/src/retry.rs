//! Repeating a request that failed for a reason that will pass.
//!
//! The rules come from the
//! [HTTP command reference](https://ytsaurus.tech/docs/en/api/commands#retry):
//!
//! - a **non-mutating light** command can simply be repeated;
//! - a **mutating light** command must carry a `mutation_id` — a GUID — in both
//!   the original request and the retries, with `retry=%false` on the first and
//!   `retry=%true` afterwards. The cluster keeps the first response for five to
//!   ten minutes and hands it back instead of applying the change twice;
//! - a **heavy** command cannot be retried at all. The documented way to make
//!   one atomic is a transaction.
//!
//! Which failures are worth repeating follows the Python client's HTTP retry
//! list (`get_retriable_errors` in `yt/python/yt/wrapper/http_helpers.py`):
//! transport failures, request timeouts, an unavailable or overloaded proxy,
//! and a banned one.
//!
//! With one exception, which no cluster reports and only a client can know: a
//! transport failure that is the TLS layer **rejecting the cluster's
//! certificate for a reason this client's own configuration decided** is a
//! settled verdict, not a passing condition. It is reported at the first
//! attempt — see [`rejected_the_certificate`], and [`SETTLED_REJECTIONS`] for
//! how few of the TLS layer's complaints that actually is.

use std::time::Duration;

use crate::error::{ClientError, Result};

/// How a command may be repeated — and, for a heavy one, where it goes.
///
/// The classification is the cluster's, not this crate's: each command declares
/// whether it mutates and whether it is heavy, and the rules at the top of this
/// module follow from those two bits. A modelled command has its answer written
/// into its call site; [`Client::raw_command_with`](crate::Client::raw_command_with)
/// is where a caller supplies one for a command this crate does not model.
///
/// **[`Repeatable::Never`] is the safe answer and the default there.** A retry
/// of something that turned out to be mutating applies it twice, and a
/// `mutation_id` only prevents that where the master's mutation cache covers
/// the command — it does not cover the scheduler, which is why
/// [`Client::abort_operation`](crate::Client::abort_operation) is `Never`
/// despite being both light and mutating.
///
/// The enum is `#[non_exhaustive]`: the cluster's registry has more shapes than
/// this crate has needed so far, and a caller that matches on it exhaustively
/// would break the next time one of them earns a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Repeatable {
    /// Safe to repeat unchanged, with no mutation ID to deduplicate by.
    ///
    /// A non-mutating light command, which is the common case — and also a
    /// mutating one the cluster answers the same way however many times it
    /// arrives, where the mutation cache would not have covered it anyway.
    /// [`Client::suspend_operation`](crate::Client::suspend_operation) and
    /// [`Client::update_operation_parameters`](crate::Client::update_operation_parameters)
    /// are the two: suspending a suspended operation is accepted, and setting
    /// a pool assigns rather than increments.
    ///
    /// **Idempotent is not the same as consequence-free.** A retry sent after
    /// the scheduler has let the operation go is answered `No such operation`,
    /// so a change that was applied can still be reported as an error. Each of
    /// those two commands says so; a mutating command classified here needs the
    /// same reasoning written down beside it.
    Freely,
    /// Mutating and light: repeat it tagged with a `mutation_id`.
    ///
    /// The cluster keeps the first response for five to ten minutes and hands
    /// it back rather than applying the change twice. See [`MutationId`].
    WithMutationId,
    /// Mutating outside the master's mutation cache. Sent once, whatever the
    /// policy says, because there is nothing that would deduplicate a second
    /// send and the first may already have been applied.
    Never,
    /// **Heavy**: table and file data, in either direction.
    ///
    /// Sent once, like [`Repeatable::Never`] and for the documented reason —
    /// the way to make a heavy command atomic is a transaction, not a retry.
    ///
    /// It also decides **where** the command goes. A large installation gives
    /// its proxies roles and refuses a heavy request on a control proxy, so
    /// the client asks `/hosts` for one that will take it, once per client
    /// and only if a heavy command needs it. Nothing about the call site
    /// changes: this is the same `isHeavy` bit of the cluster's command
    /// registry that says the command cannot be repeated, and both answers
    /// follow from writing it down once. The discovered host is constrained to
    /// the configured address's own domain — see
    /// [`Client::with_heavy_proxies_anywhere`](crate::Client::with_heavy_proxies_anywhere).
    ///
    /// `write_table`, `read_table`, `write_file`, `get_job_input` and
    /// `get_job_stderr` are the modelled ones — every one of them declared
    /// `isHeavy = true` in `REGISTER_ALL`/`REGISTER` in the cluster's
    /// [driver registry](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/driver.cpp),
    /// whose argument order is `(command, name, inDataType, outDataType,
    /// isVolatile, isHeavy)`. A raw command that streams in either direction is
    /// sent this way whatever the caller says, because streaming *is* the heavy
    /// shape.
    Heavy,
}

/// YTsaurus error codes worth a second attempt.
///
/// Deliberately short. Codes that mean "your request was wrong" — 500 is a
/// resolve error, 501 an already-existing node — must never end up here: a
/// retry cannot fix them and only delays the report.
const RETRIABLE_CODES: &[i64] = &[
    3,    // request timed out
    100,  // transport error
    105,  // RPC unavailable — the scheduler could not reach the master
    108,  // request queue size limit exceeded
    904,  // request rate limit exceeded
    2100, // proxy banned
];

/// HTTP statuses worth a second attempt, when the cluster sent no error
/// document to judge by.
const RETRIABLE_STATUSES: &[u16] = &[429, 500, 502, 503, 504];

/// How `rustls` 0.23 introduces a verdict on the peer's certificate.
///
/// Read out of its `Display for Error` rather than collected from messages as
/// they were seen: `InvalidCertificate(reason)` renders as `invalid peer
/// certificate: ` followed by the `Debug` of a `CertificateError`. That reason
/// is what decides whether waiting could help, so it is read rather than
/// discarded — see [`SETTLED_REJECTIONS`].
const CERTIFICATE_VERDICT: &str = "invalid peer certificate: ";

/// The certificate verdicts a second attempt cannot change.
///
/// Deliberately two, and both decided **here** rather than at the cluster:
///
/// - `UnknownIssuer` — the chain does not end in a root this client trusts. The
///   root store is the same one a second later; only `YT_CA_BUNDLE` or the
///   `platform-verifier` feature changes it.
/// - `NotValidForName` — the certificate does not cover the host that was
///   asked for. The host is the same one a second later too.
/// - `certificate not valid for name ` — the **same** verdict, spelled the way
///   it actually arrives. `rustls` renders `InvalidCertificate` with `Display`
///   rather than `Debug` (`Error::fmt`), and `Display for CertificateError`
///   gives the context-carrying variants prose instead of their variant name.
///   The webpki verifier only ever builds `NotValidForNameContext` for a
///   hostname mismatch — the bare `NotValidForName` above is unreachable in the
///   default build — so matching the variant name alone would settle nothing
///   and quietly cost five attempts for a certificate naming another host.
///
/// Everything else stays retriable, and the reason is the same in each case:
/// the answer might genuinely differ next time.
///
/// - `Other(..)` is what `rustls-platform-verifier` — the whole point of the
///   `platform-verifier` feature — maps a *platform* failure to: a revocation
///   lookup that timed out, a trust store that could not be opened. Those are
///   transient conditions of this machine, and classifying them here would
///   turn the feature into a way of making the OS's bad afternoon permanent.
/// - `Expired` and `NotValidYet` are a property of the certificate that
///   answered, not of the fleet: a round-robin proxy set mid-rotation has some
///   members already renewed, and the next connection may reach one of them.
/// - `invalid certificate revocation list` is a CRL that could not be fetched
///   or parsed — the same transient class as `Other`.
/// - `peer sent no certificates` is a proxy that answered wrong once.
const SETTLED_REJECTIONS: &[&str] = &[
    "UnknownIssuer",
    "NotValidForName",
    "certificate not valid for name ",
];

/// How often, and how patiently, a failed request is repeated.
///
/// The default is five attempts with a doubling delay from one second, capped
/// at ten — about fifteen seconds of patience, which covers a proxy restart or
/// a scheduler reconnect without making a genuinely broken cluster feel hung.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    /// Whether a retry announces itself at all — on stderr, or as a `WARN`
    /// event where the `tracing` feature is on. See [`RetryPolicy::quiet`].
    report: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(10),
            report: true,
        }
    }
}

impl RetryPolicy {
    /// `attempts` tries in total, waiting `initial_backoff` after the first
    /// failure and doubling up to `max_backoff`.
    ///
    /// `attempts` is clamped to at least one: zero attempts would mean never
    /// sending the request at all.
    #[must_use]
    pub fn new(attempts: u32, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            attempts: attempts.max(1),
            initial_backoff,
            max_backoff,
            report: true,
        }
    }

    /// Send once, report whatever comes back.
    #[must_use]
    pub fn none() -> Self {
        Self::new(1, Duration::ZERO, Duration::ZERO)
    }

    /// The same policy, retrying without saying so.
    ///
    /// A retry normally announces itself on stderr, so a launcher that pauses
    /// for fifteen seconds says why rather than looking hung. Inside a **job**
    /// that same stream is the cluster's diagnostic channel — a bounded buffer
    /// the operation UI shows, and the one the job's own messages go to — so a
    /// worker that talks to the cluster while a proxy is flaky would fill it
    /// with retry chatter.
    ///
    /// With the `tracing` feature on the announcement is a `WARN` event rather
    /// than a line on stderr, and this mutes that too. Same reason: a
    /// subscriber installed inside a job is, more often than not, writing to
    /// the very buffer this exists to protect. [`RetryPolicy::loud`] puts the
    /// messages back whichever form they take.
    ///
    /// A [`Client`](crate::Client) built inside a job is quiet already; this is
    /// for choosing it anywhere else:
    ///
    /// ```
    /// use ytsaurus_client::{Client, RetryPolicy};
    ///
    /// let client = Client::new("http://localhost:8000")
    ///     .with_retries(RetryPolicy::default().quiet());
    /// ```
    #[must_use]
    pub fn quiet(mut self) -> Self {
        self.report = false;
        self
    }

    /// The same policy, announcing each retry.
    ///
    /// The default outside a job, and what puts the messages back inside one —
    /// a job whose stderr nobody else is using may well want them, and so does
    /// one whose subscriber ships them somewhere other than stderr.
    #[must_use]
    pub fn loud(mut self) -> Self {
        self.report = true;
        self
    }

    /// Whether this policy says anything out loud at all.
    ///
    /// Read by the transport as well as by [`run`]: the one thing the client
    /// announces that is not a retry — a `/hosts` answer it declined, see
    /// [`crate::observe::declined`] — has to be muted by the same switch, for
    /// the same reason. A job's stderr is the cluster's bounded diagnostic
    /// buffer whatever the client is talking about.
    pub(crate) fn reports(self) -> bool {
        self.report
    }

    /// How long to wait after the `attempt`-th failure, counting from one.
    fn backoff(self, attempt: u32) -> Duration {
        let doubled = self
            .initial_backoff
            .checked_mul(1_u32.checked_shl(attempt - 1).unwrap_or(u32::MAX))
            .unwrap_or(self.max_backoff);
        doubled.min(self.max_backoff)
    }
}

/// A GUID the cluster deduplicates a repeated mutation by.
///
/// The client generates one for every mutating command it may have to repeat,
/// so retries never apply a change twice. Passing your own is for a stronger
/// guarantee than one process can give itself: persist the ID, and replaying
/// the command after a crash returns the original result rather than starting
/// a second operation. See
/// [`Client::start_operation_with`](crate::Client::start_operation_with).
///
/// **A replay must say that it is one.** The ID carries that flag, because the
/// cluster does not infer it: sending a known ID again without it is refused
/// with `Duplicate request is not marked as "retry"` rather than deduplicated.
///
/// ```
/// use ytsaurus_client::MutationId;
///
/// let first = MutationId::new();       // the original request
/// let again = first.as_retry();        // the same mutation, sent again
///
/// assert_eq!(first.as_str(), again.as_str());
/// assert!(!first.is_retry() && again.is_retry());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationId {
    id: String,
    retry: bool,
}

impl MutationId {
    /// A fresh ID, for an original request.
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: generate(),
            retry: false,
        }
    }

    /// The same ID, marked as a replay of a request already sent.
    ///
    /// This is what makes the cluster return the first response instead of
    /// refusing the duplicate.
    #[must_use]
    pub fn as_retry(&self) -> Self {
        Self {
            id: self.id.clone(),
            retry: true,
        }
    }

    /// The ID, as YTsaurus spells a GUID.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.id
    }

    /// Whether this send is a replay.
    #[must_use]
    pub fn is_retry(&self) -> bool {
        self.retry
    }
}

impl Default for MutationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MutationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.id)
    }
}

/// Builds a GUID: four 32-bit numbers in hex, separated by `-`, as the command
/// reference describes them and as the cluster's own IDs are printed —
/// `b4ef546-e730447d-103e8-20cfe65`, with no leading zeros.
///
/// The bits come from [`crate::unique::word`], which is also where a
/// [`TraceContext`](crate::TraceContext) draws its ids: the argument for why
/// they do not repeat is the same one, and it is made once.
fn generate() -> String {
    let mut parts = [0_u32; 4];
    for (i, pair) in parts.chunks_mut(2).enumerate() {
        let value = crate::unique::word(i as u64);
        pair[0] = (value >> 32) as u32;
        pair[1] = value as u32;
    }

    format!(
        "{:x}-{:x}-{:x}-{:x}",
        parts[0], parts[1], parts[2], parts[3]
    )
}

/// Whether **waiting** and sending the same request again could plausibly
/// succeed.
///
/// This is the question the retry loop asks, and only that one. "Would asking
/// somewhere else help?" is a different question with a different answer —
/// see [`worth_asking_again`].
pub(crate) fn is_retriable(error: &ClientError) -> bool {
    match error {
        // The request never got an answer: a refused connection, a reset, a
        // timeout. Nothing about it says the command was wrong — unless it was
        // the certificate that was refused, which no amount of waiting mends.
        ClientError::Transport { source, .. } => !rejected_the_certificate(source),
        ClientError::Http { status, .. } => RETRIABLE_STATUSES.contains(status),
        ClientError::Cluster { code, raw, .. } => {
            RETRIABLE_CODES.contains(code) || raw_contains_code(raw, RETRIABLE_CODES)
        }
        _ => false,
    }
}

/// Whether the TLS layer refused the cluster's certificate.
///
/// A rejected chain is a settled question: the same roots will reject the same
/// certificate a second later, and a third time after that. Retrying it turns a
/// configuration mistake into fifteen seconds of doubling backoff before the
/// same sentence — which is what a cluster behind a private CA cost, five
/// attempts at a time, until `YT_CA_BUNDLE` existed to answer it.
///
/// It arrives as `ureq::Error::Io` rather than as one of `ureq`'s TLS variants:
/// `rustls` wraps its own error in an `io::Error` of kind `InvalidData`
/// (`ConnectionCommon::complete_io`) and `ureq` passes it through untouched.
/// Reading the `rustls::Error` back out would mean depending on `rustls`
/// directly, which this crate deliberately does not — `ureq` is its only door
/// to TLS, and the whole `tls` feature is one line in a manifest because of it.
/// So the kind narrows the error to the TLS layer (neither `ureq` nor
/// `ureq-proto` produces `InvalidData`, and a failed decompression has a
/// variant of its own) and the text says which TLS failure it was.
///
/// **Deliberately narrow, in three ways.** The kind confines it to the TLS
/// layer; the prefix confines it to a verdict about the certificate rather than
/// about the protocol — a disagreement mid-handshake may well be one busy proxy
/// out of several; and the reason itself confines it to the two verdicts this
/// client's own configuration decides, rather than to every unhappy thing a
/// verifier can say. See [`SETTLED_REJECTIONS`], which is where that last
/// narrowing is argued: an `Other(..)` from `rustls-platform-verifier` is a
/// passing condition of this machine, and reading it as a verdict would make
/// enabling the platform verifier a way of turning the operating system's bad
/// afternoon into a permanent failure.
fn rejected_the_certificate(error: &ureq::Error) -> bool {
    let ureq::Error::Io(io) = error else {
        return false;
    };

    if io.kind() != std::io::ErrorKind::InvalidData {
        return false;
    }

    let message = io.to_string();
    let Some((_, reason)) = message.split_once(CERTIFICATE_VERDICT) else {
        return false;
    };

    // `starts_with` and not `contains`: `Other(..)` wraps a message this crate
    // did not write, and one that happened to quote `UnknownIssuer` would
    // otherwise be read as one.
    SETTLED_REJECTIONS
        .iter()
        .any(|settled| reason.starts_with(settled))
}

/// Looks for one of `wanted` anywhere in an error document.
///
/// The outer code is often a wrapper — `Request retries failed`, `Error
/// resolving path` — while the code that decides anything sits in
/// `inner_errors`. Every classifier in this crate that reads a cluster code
/// has to walk the document for that reason, so the walk is written once and
/// the list of codes is the caller's: [`is_retriable`] passes the retriable
/// ones, and `Client::upload_worker_cached` passes `Access denied`.
///
/// A document that is not JSON at all answers `false` rather than failing: the
/// outer code has already been consulted by then, and a classifier that
/// returned an error would only be asked to guess again.
pub(crate) fn raw_contains_code(raw: &str, wanted: &[i64]) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    contains_code(&value, wanted)
}

/// The walk itself: this error's own code, then every error nested under it.
fn contains_code(value: &serde_json::Value, wanted: &[i64]) -> bool {
    if let Some(code) = value.get("code").and_then(serde_json::Value::as_i64)
        && wanted.contains(&code)
    {
        return true;
    }

    value
        .get("inner_errors")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|inner| inner.iter().any(|error| contains_code(error, wanted)))
}

/// Whether putting the **question** to the cluster again could plausibly get a
/// different answer.
///
/// Not the same question as [`is_retriable`], and the difference is the whole
/// reason this exists. `is_retriable` asks *would waiting help?* — it decides
/// whether to send the same request to the same place after a pause. This one
/// asks *would asking again ever help?*, and its callers are the two places
/// that decide whether the client keeps or discards what `/hosts` told it:
/// `Transport::base_for` and `Transport::after_heavy`. Neither is going to
/// re-send anything; both are choosing between "remember this answer" and "ask
/// once more later".
///
/// They differ for exactly the failures where the *addressee* is what was
/// wrong rather than the moment. Every reason to wait is also a reason to ask
/// again — a proxy that was restarting is one the coordinator may well name
/// differently in a minute — so this starts from `is_retriable` and adds to it.
///
/// **Two arms belong here that this branch cannot yet write**, because the
/// variants they name are introduced by sibling pull requests. Each is one
/// line, and this function is shaped so that it is:
///
/// - **`ClientError::Redirected` → `true`** (#36, redirect credentials). A
///   balancer that answered with a `Location` this client refuses to follow is
///   not a permanent verdict on heavy routing: its routing may be different for
///   the next request, and the thing that must not happen is *following* the
///   redirect, not *asking* again. Left as `is_retriable`'s `false`, one such
///   answer would disable heavy routing for the client's whole life.
/// - **a rejected certificate → `false`** (#39, TLS CA bundle). A host this
///   process does not trust will not become trusted by being asked twice, and
///   the fix is a CA bundle rather than another question. That arm needs no
///   line here: #39 narrows `is_retriable` to answer `false` for one, and
///   `false` is the right answer on this side too.
///
/// Both sibling PRs have now landed, so both arms this function was shaped for
/// are in place. #39 narrowed [`is_retriable`] to answer `false` for a rejected
/// certificate, which is the right answer here too and needs no line of its own.
/// #36's [`ClientError::Redirected`] is the line below: a balancer that answered
/// `/hosts` with a `Location` this client refuses to follow is not a permanent
/// verdict on heavy routing — its routing may differ for the next request — so
/// the coordinator is worth asking again. Left at `is_retriable`'s `false`, one
/// such answer would disable heavy routing for the client's whole life (#30
/// behind a new message), which the merge integration test in
/// `tests/combination.rs` guards against.
pub(crate) fn worth_asking_again(error: &ClientError) -> bool {
    is_retriable(error)
        || refused_for_being_the_wrong_proxy(error)
        || matches!(error, ClientError::Redirected { .. })
}

/// Whether the proxy refused this because of the **role it has**.
///
/// The purest case of "waiting would not help and asking somewhere else would",
/// and the reason the two predicates are separate. `Control proxy may not serve
/// heavy requests with input data` arrives as an ordinary cluster error with
/// code 1, which [`is_retriable`] correctly judges hopeless: sending it there
/// again will be refused again, forever. Asking the *coordinator* again, on the
/// other hand, is the entire fix — and a client that has been routed onto a
/// control proxy (an operator can change `default_role_filter`, and `/hosts`
/// then lists proxies that refuse this) would otherwise keep that address for
/// its whole life and fail every heavy command with it.
fn refused_for_being_the_wrong_proxy(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Cluster { message, .. } if message.contains(crate::http::CONTROL_REFUSAL)
    )
}

/// Whether a fresh client should announce its retries.
///
/// Not inside a job. `YT_JOB_ID` is set by the node that starts one, and a
/// job's stderr is the cluster's diagnostic channel rather than a terminal: a
/// bounded buffer the operation UI shows, shared with whatever the job wanted
/// to say. This crate is linked into worker binaries — that is the whole point
/// of the one-binary pattern — so the same `Client::from_env()` runs in both
/// roles, and the default has to be the one the caller cannot easily choose
/// for itself. [`RetryPolicy::loud`] puts the messages back.
pub(crate) fn report_by_default() -> bool {
    !inside_job(std::env::var_os("YT_JOB_ID"))
}

/// The decision itself, split out so it can be tested without touching the
/// process environment — which is global, and in edition 2024 unsafe to write.
fn inside_job(job_id: Option<std::ffi::OsString>) -> bool {
    job_id.is_some_and(|id| !id.is_empty())
}

/// Runs `action` until it succeeds, gives up, or fails for a reason a retry
/// cannot fix.
///
/// `action` is told whether this is a retry, which is what a mutating command
/// puts in its `retry` parameter. Each attempt is timed and named — see
/// `observe::attempt` — and progress is reported unless the policy is
/// [`RetryPolicy::quiet`]: a run that pauses for fifteen seconds should say why
/// rather than look hung.
pub(crate) fn run<T>(
    policy: RetryPolicy,
    repeatable: Repeatable,
    command: &str,
    mut action: impl FnMut(bool) -> Result<T>,
) -> Result<T> {
    let allowed = match repeatable {
        Repeatable::Never | Repeatable::Heavy => 1,
        _ => policy.attempts,
    };

    let mut attempt = 1;
    loop {
        match crate::observe::attempt(command, attempt, || action(attempt > 1)) {
            Ok(value) => return Ok(value),
            Err(error) => {
                if attempt >= allowed || !is_retriable(&error) {
                    return Err(error);
                }

                let wait = policy.backoff(attempt);
                if policy.report {
                    // `allowed`, not `allowed - 1`: the announcement counts
                    // attempts, because the span beside it does. Reporting the
                    // retry *number* against a retry total meant the same
                    // field name carried two different counters — an event
                    // saying `attempt=4, of=4` sat next to a span saying
                    // `attempt=5`, and anything keying on `attempt == of` to
                    // mean "the last try" fired one attempt early.
                    crate::observe::retrying(command, &error, wait, attempt, allowed);
                }
                std::thread::sleep(wait);
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Zero backoff, so the tests do not sleep.
    fn instant(attempts: u32) -> RetryPolicy {
        RetryPolicy::new(attempts, Duration::ZERO, Duration::ZERO)
    }

    fn cluster_error(code: i64, raw: &str) -> ClientError {
        ClientError::Cluster {
            command: "get".to_owned(),
            code,
            message: "boom".to_owned(),
            raw: raw.to_owned(),
        }
    }

    #[test]
    fn an_unavailable_cluster_is_worth_retrying() {
        // Exactly what a local cluster answered while its scheduler was
        // reconnecting to the master.
        assert!(is_retriable(&cluster_error(105, r#"{"code":105}"#)));
    }

    #[test]
    fn a_wrapper_error_is_judged_by_what_is_inside_it() {
        // "Request retries failed" is a wrapper; the reason is one level down.
        let raw = r#"{"code":1,"message":"Request retries failed",
                      "inner_errors":[{"code":105,"message":"Master is not connected"}]}"#;
        assert!(is_retriable(&cluster_error(1, raw)));
    }

    #[test]
    fn a_mistake_is_not_retried() {
        // 500 is a resolve error and 501 an already-existing node: repeating
        // either just delays the report.
        assert!(!is_retriable(&cluster_error(500, r#"{"code":500}"#)));
        assert!(!is_retriable(&cluster_error(501, r#"{"code":501}"#)));
        assert!(!is_retriable(&cluster_error(1, r#"{"code":1}"#)));
    }

    #[test]
    fn an_unparseable_error_document_is_not_retried() {
        assert!(!is_retriable(&cluster_error(1, "not json at all")));
    }

    #[test]
    fn http_statuses_are_split_by_whether_waiting_helps() {
        let http = |status| ClientError::Http {
            command: "get".to_owned(),
            status,
            body: String::new(),
        };

        assert!(is_retriable(&http(503)));
        assert!(is_retriable(&http(429)));
        assert!(!is_retriable(&http(404)));
        assert!(!is_retriable(&http(401)));
    }

    /// A transport failure carrying the `io::Error` `ureq` would have carried.
    fn transport_error(kind: std::io::ErrorKind, message: &str) -> ClientError {
        ClientError::Transport {
            command: "get".to_owned(),
            source: Box::new(ureq::Error::Io(std::io::Error::new(kind, message))),
        }
    }

    #[test]
    fn a_rejected_certificate_is_not_retried() {
        // Exactly what a cluster behind a corporate CA answered with, before
        // there was any way to name that CA: `rustls` wraps its own error in an
        // `io::Error` of kind `InvalidData`, and `ureq` hands it through. Five
        // attempts of this is fifteen seconds spent proving that the same roots
        // still do not contain the same issuer.
        assert!(!is_retriable(&transport_error(
            std::io::ErrorKind::InvalidData,
            "invalid peer certificate: UnknownIssuer"
        )));

        for rejection in [
            "invalid peer certificate: NotValidForName",
            // The form this verdict actually arrives in, and the one that
            // matters: `rustls` renders `InvalidCertificate` with `Display`,
            // and `Display for CertificateError` writes prose for the
            // context-carrying variants rather than their variant name. The
            // webpki verifier builds *only* `NotValidForNameContext` for a
            // hostname mismatch, so this string — not the one above — is what
            // a cluster whose certificate names another host produces.
            "invalid peer certificate: certificate not valid for name \
             \"cluster.example.net\"; certificate is only valid for \
             DnsName(\"other.example.net\")",
        ] {
            assert!(
                !is_retriable(&transport_error(std::io::ErrorKind::InvalidData, rejection)),
                "{rejection}"
            );
        }
    }

    #[test]
    fn a_platform_verifier_that_had_a_bad_afternoon_is_retried() {
        // `rustls-platform-verifier` — which is what the `platform-verifier`
        // feature turns on — maps every failure of the operating system's own
        // machinery to `CertificateError::Other`, and that renders under the
        // same `invalid peer certificate:` prefix as a verdict. A revocation
        // lookup that timed out or a trust store that was momentarily
        // unreadable is a condition, not a judgement, and reading it as one
        // would make enabling the feature a way of turning the OS's bad
        // afternoon into a permanent failure.
        for message in [
            "invalid peer certificate: Other(OtherError(TrustStoreUnavailable))",
            "invalid peer certificate: Other(OtherError(RevocationLookupTimedOut))",
            // Nor does quoting a settled reason inside one make it settled.
            "invalid peer certificate: Other(OtherError(\"UnknownIssuer lookup failed\"))",
        ] {
            assert!(
                is_retriable(&transport_error(std::io::ErrorKind::InvalidData, message)),
                "{message}"
            );
        }
    }

    #[test]
    fn a_certificate_that_may_be_one_proxy_out_of_several_is_retried() {
        // A fleet answers round-robin, so these are properties of the member
        // that happened to answer rather than of the installation. Mid-rotation
        // some members are renewed and some are not; the next connection may
        // reach a renewed one, and fifteen seconds is a cheap price for that
        // against reporting a working cluster as broken.
        for message in [
            "invalid peer certificate: Expired",
            "invalid peer certificate: NotValidYet",
            "invalid peer certificate: Revoked",
            // A revocation list that could not be fetched or parsed is the
            // same transient class.
            "invalid certificate revocation list: ParseError",
            "peer sent no certificates",
        ] {
            assert!(
                is_retriable(&transport_error(std::io::ErrorKind::InvalidData, message)),
                "{message}"
            );
        }
    }

    #[test]
    fn every_other_transport_failure_is_still_retried() {
        // The narrowness is the point. A reset connection is the ordinary case
        // this whole module exists for, and a TLS error that is not about the
        // certificate may well be one busy proxy out of several.
        for (kind, message) in [
            (
                std::io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            ),
            (std::io::ErrorKind::ConnectionRefused, "connection refused"),
            (std::io::ErrorKind::TimedOut, "operation timed out"),
            (std::io::ErrorKind::UnexpectedEof, "unexpected end of file"),
            (
                std::io::ErrorKind::InvalidData,
                "received corrupt message of type Handshake",
            ),
            (
                std::io::ErrorKind::InvalidData,
                "peer misbehaved: TooManyEmptyFragments",
            ),
            // The right words, the wrong layer: a body that decompressed to
            // nonsense is not a handshake.
            (
                std::io::ErrorKind::Other,
                "invalid peer certificate: UnknownIssuer",
            ),
        ] {
            assert!(is_retriable(&transport_error(kind, message)), "{message}");
        }

        // And a failure that never reached the TLS layer at all.
        assert!(is_retriable(&ClientError::Transport {
            command: "get".to_owned(),
            source: Box::new(ureq::Error::HostNotFound),
        }));
    }

    #[test]
    fn a_rejected_certificate_costs_one_attempt_and_not_five() {
        let calls = std::cell::Cell::new(0);

        let result: Result<()> = run(instant(5), Repeatable::Freely, "get", |_| {
            calls.set(calls.get() + 1);
            Err(transport_error(
                std::io::ErrorKind::InvalidData,
                "invalid peer certificate: UnknownIssuer",
            ))
        });

        assert!(result.is_err());
        assert_eq!(
            calls.get(),
            1,
            "a certificate is no likelier to be accepted on the fifth try"
        );
    }

    #[test]
    fn asking_again_is_a_different_question_from_waiting() {
        // Two predicates, two questions. Everything worth waiting for is worth
        // asking about again — a proxy that was restarting is one the
        // coordinator may name differently in a minute — so this direction of
        // the implication is the one that must hold on every branch.
        for worth_waiting in [
            ClientError::Transport {
                command: "write_table".to_owned(),
                source: Box::new(ureq::Error::HostNotFound),
            },
            ClientError::Http {
                command: "hosts".to_owned(),
                status: 503,
                body: String::new(),
            },
            cluster_error(2100, r#"{"code":2100}"#),
        ] {
            assert!(is_retriable(&worth_waiting), "{worth_waiting}");
            assert!(worth_asking_again(&worth_waiting), "{worth_waiting}");
        }

        // And the case that makes the split earn its keep: a proxy refusing a
        // heavy command because of the role it has. Waiting cannot help — it
        // will refuse the next one identically, forever — and asking the
        // coordinator for another proxy is the entire fix. `/hosts` lists
        // whatever `default_role_filter` says, which an operator can change, so
        // a control proxy really can turn up in the answer.
        let wrong_proxy = ClientError::Cluster {
            command: "write_table".to_owned(),
            code: 1,
            message: "Control proxy may not serve heavy requests with input data".to_owned(),
            raw: r#"{"code":1}"#.to_owned(),
        };
        assert!(!is_retriable(&wrong_proxy), "{wrong_proxy}");
        assert!(worth_asking_again(&wrong_proxy), "{wrong_proxy}");

        // And a settled answer is settled for both. A cluster with no `/hosts`
        // endpoint answers 404 every time, so the lookup is remembered as
        // "this cluster serves its own heavy commands" rather than repeated
        // before every upload.
        for settled in [
            ClientError::Http {
                command: "hosts".to_owned(),
                status: 404,
                body: String::new(),
            },
            ClientError::Decode {
                command: "hosts".to_owned(),
                reason: "not a list of host names".to_owned(),
            },
            ClientError::Config("no proxy".to_owned()),
            cluster_error(500, r#"{"code":500}"#),
        ] {
            assert!(!is_retriable(&settled), "{settled}");
            assert!(!worth_asking_again(&settled), "{settled}");
        }
    }

    #[test]
    fn decode_and_config_errors_are_never_retried() {
        assert!(!is_retriable(&ClientError::Config("no proxy".to_owned())));
        assert!(!is_retriable(&ClientError::Decode {
            command: "get".to_owned(),
            reason: "not yson".to_owned(),
        }));
    }

    #[test]
    fn a_transient_failure_is_survived() {
        let calls = RefCell::new(Vec::new());

        let result = run(instant(5), Repeatable::Freely, "get", |is_retry| {
            calls.borrow_mut().push(is_retry);
            if calls.borrow().len() < 3 {
                Err(cluster_error(105, r#"{"code":105}"#))
            } else {
                Ok(42)
            }
        });

        assert_eq!(result.ok(), Some(42));
        // The first attempt is not a retry; the ones after it are, which is
        // exactly what goes into the `retry` parameter.
        assert_eq!(*calls.borrow(), vec![false, true, true]);
    }

    #[test]
    fn attempts_are_bounded() {
        let calls = std::cell::Cell::new(0);

        let result: Result<()> = run(instant(3), Repeatable::Freely, "get", |_| {
            calls.set(calls.get() + 1);
            Err(cluster_error(105, r#"{"code":105}"#))
        });

        assert!(result.is_err());
        assert_eq!(calls.get(), 3, "three attempts, not three retries");
    }

    #[test]
    fn a_heavy_command_is_sent_once() {
        for once in [Repeatable::Heavy, Repeatable::Never] {
            let calls = std::cell::Cell::new(0);

            let result: Result<()> = run(instant(5), once, "write_table", |_| {
                calls.set(calls.get() + 1);
                Err(cluster_error(105, r#"{"code":105}"#))
            });

            assert!(result.is_err());
            assert_eq!(
                calls.get(),
                1,
                "{once:?}: heavy commands cannot be retried, whatever the policy says"
            );
        }
    }

    #[test]
    fn a_hopeless_error_stops_immediately() {
        let calls = std::cell::Cell::new(0);

        let result: Result<()> = run(instant(5), Repeatable::Freely, "get", |_| {
            calls.set(calls.get() + 1);
            Err(cluster_error(500, r#"{"code":500}"#))
        });

        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn no_retries_means_one_attempt() {
        let calls = std::cell::Cell::new(0);

        let result: Result<()> = run(RetryPolicy::none(), Repeatable::Freely, "get", |_| {
            calls.set(calls.get() + 1);
            Err(cluster_error(105, r#"{"code":105}"#))
        });

        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn backoff_doubles_and_then_stops_growing() {
        let policy = RetryPolicy::new(10, Duration::from_secs(1), Duration::from_secs(8));

        assert_eq!(policy.backoff(1), Duration::from_secs(1));
        assert_eq!(policy.backoff(2), Duration::from_secs(2));
        assert_eq!(policy.backoff(3), Duration::from_secs(4));
        assert_eq!(policy.backoff(4), Duration::from_secs(8));
        assert_eq!(policy.backoff(5), Duration::from_secs(8));
        // A shift wide enough to overflow must saturate, not panic.
        assert_eq!(policy.backoff(64), Duration::from_secs(8));
        assert_eq!(policy.backoff(u32::MAX), Duration::from_secs(8));
    }

    #[test]
    fn a_job_gets_a_quiet_client_and_a_terminal_a_talkative_one() {
        // A worker's stderr is the cluster's bounded diagnostic buffer, shared
        // with whatever the job itself writes. A launcher's is a terminal.
        assert!(inside_job(Some("55aff293-7ef14284-3fe0384-3e07".into())));
        assert!(!inside_job(None));
        // An empty variable is not a job, the same reading `ytsaurus-job` takes.
        assert!(!inside_job(Some(String::new().into())));
    }

    #[test]
    fn quiet_changes_the_reporting_and_nothing_else() {
        let policy = RetryPolicy::default();

        assert!(policy.report);
        assert!(!policy.quiet().report);
        assert!(policy.quiet().loud().report);

        // Same patience either way: this is about the messages, not the waiting.
        assert_eq!(policy.quiet().attempts, policy.attempts);
        assert_eq!(policy.quiet().backoff(3), policy.backoff(3));
    }

    #[test]
    fn a_policy_always_sends_the_request_at_least_once() {
        assert_eq!(
            RetryPolicy::new(0, Duration::ZERO, Duration::ZERO).attempts,
            1
        );
    }

    #[test]
    fn a_replay_keeps_the_id_and_says_it_is_one() {
        // The cluster refuses a duplicate that does not admit to being one:
        // "Duplicate request is not marked as \"retry\"". So the flag travels
        // with the ID rather than being inferred.
        let original = MutationId::new();
        let replay = original.as_retry();

        assert_eq!(original.as_str(), replay.as_str());
        assert!(!original.is_retry());
        assert!(replay.is_retry());
        assert_eq!(replay.as_retry().as_str(), original.as_str());
    }

    #[test]
    fn mutation_ids_are_unique_and_shaped_like_guids() {
        let ids: std::collections::HashSet<String> =
            (0..10_000).map(|_| MutationId::new().id).collect();
        assert_eq!(
            ids.len(),
            10_000,
            "a repeated ID would deduplicate two different mutations"
        );

        for id in ids.iter().take(100) {
            let groups: Vec<&str> = id.split('-').collect();
            assert_eq!(groups.len(), 4, "{id}");
            for group in groups {
                assert!(!group.is_empty(), "{id}");
                assert!(group.len() <= 8, "{id}");
                assert!(group.chars().all(|c| c.is_ascii_hexdigit()), "{id}");
            }
        }
    }
}
