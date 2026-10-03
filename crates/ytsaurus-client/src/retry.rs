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
//! and a banned one. The exception is a transport failure that is the TLS
//! layer rejecting the certificate for a reason this client's configuration
//! decided ([`SETTLED_REJECTIONS`]): that is reported at the first attempt.

use std::time::Duration;

use crate::error::{ClientError, Result};

/// How a command may be repeated — and, for a heavy one, where it goes.
///
/// The classification follows the two bits each command declares in the
/// cluster's registry: whether it mutates and whether it is heavy. Modelled
/// commands carry theirs; [`Client::raw_command_with`](crate::Client::raw_command_with)
/// takes one for a command this crate does not model.
///
/// **[`Repeatable::Never`] is the safe answer and the default there.** A retry
/// of a mutating command applies it twice unless the master's mutation cache
/// covers it, and the cache does not cover the scheduler: that is why
/// [`Client::abort_operation`](crate::Client::abort_operation) is `Never`
/// though light and mutating.
///
/// `#[non_exhaustive]`, so that more of the registry's shapes can be added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Repeatable {
    /// Safe to repeat unchanged, with no mutation ID to deduplicate by.
    ///
    /// A non-mutating light command, or a mutating one the cluster answers the
    /// same way however often it arrives:
    /// [`Client::suspend_operation`](crate::Client::suspend_operation) and
    /// [`Client::update_operation_parameters`](crate::Client::update_operation_parameters).
    ///
    /// **Idempotent is not consequence-free.** A retry sent after the scheduler
    /// has let the operation go is answered `No such operation`, so an applied
    /// change can still be reported as an error.
    Freely,
    /// Mutating and light: repeat it tagged with a `mutation_id`.
    ///
    /// The cluster keeps the first response for five to ten minutes and hands
    /// it back rather than applying the change twice. See [`MutationId`].
    WithMutationId,
    /// Mutating outside the master's mutation cache. Sent once, whatever the
    /// policy says: nothing would deduplicate a second send.
    Never,
    /// **Heavy**: table and file data, in either direction.
    ///
    /// Sent once, like [`Repeatable::Never`]: the documented way to make a
    /// heavy command atomic is a transaction.
    ///
    /// It also decides where the command goes. An installation with proxy roles
    /// refuses heavy requests on a control proxy, so the client asks `/hosts`
    /// for a pool of proxies when a heavy command needs one, and again when the
    /// answer outlives its refresh interval. Discovered hosts must be in the
    /// configured address's domain; see
    /// [`Client::with_heavy_proxies_anywhere`](crate::Client::with_heavy_proxies_anywhere)
    /// and [where a heavy command goes](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#where-a-heavy-command-goes).
    ///
    /// The modelled heavy commands are `write_table`, `read_table`,
    /// `write_file`, `read_file`, `get_job_input` and `get_job_stderr`, each
    /// `isHeavy` in the cluster's
    /// [driver registry](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/driver.cpp).
    /// A raw command that streams in either direction is sent this way whatever
    /// the caller says.
    Heavy,
}

/// YTsaurus error codes worth a second attempt.
///
/// Codes that mean "your request was wrong", such as 500 (resolve error) or
/// 501 (node exists), do not belong here: a retry only delays the report.
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
/// From its `Display for Error`: `InvalidCertificate(reason)` renders as this
/// prefix followed by the reason, which [`SETTLED_REJECTIONS`] reads.
const CERTIFICATE_VERDICT: &str = "invalid peer certificate: ";

/// The certificate verdicts a second attempt cannot change.
///
/// Both depend only on this client's configuration:
///
/// - `UnknownIssuer`: the chain does not end in a trusted root. Only
///   `YT_CA_BUNDLE` or the `platform-verifier` feature changes the roots.
/// - `NotValidForName`, and `certificate not valid for name `, the same
///   verdict as `Display for CertificateError` actually renders it: the webpki
///   verifier builds only `NotValidForNameContext`, so the variant name alone
///   matches nothing in the default build.
///
/// Everything else stays retriable, because the next answer may differ:
/// `Other(..)` is how `rustls-platform-verifier` reports a failed revocation
/// lookup or an unreadable trust store; `Expired` and `NotValidYet` belong to
/// one proxy of a fleet that may be mid-rotation; a bad CRL and `peer sent no
/// certificates` are transient. See the
/// [protocol reference](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#tls).
const SETTLED_REJECTIONS: &[&str] = &[
    "UnknownIssuer",
    "NotValidForName",
    "certificate not valid for name ",
];

/// How often, and how patiently, a failed request is repeated.
///
/// The default is five attempts with a doubling delay from one second, capped
/// at ten: about fifteen seconds, enough for a proxy restart or a scheduler
/// reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    /// Whether a retry is announced. See [`RetryPolicy::quiet`].
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
    /// `attempts` is clamped to at least one.
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
    /// A retry is normally announced on stderr, or as a `WARN` event with the
    /// `tracing` feature; this mutes both. Inside a job, stderr is the
    /// cluster's bounded diagnostic buffer, so a [`Client`](crate::Client)
    /// built there is quiet already. [`RetryPolicy::loud`] undoes it.
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
    /// The default outside a job.
    #[must_use]
    pub fn loud(mut self) -> Self {
        self.report = true;
        self
    }

    /// Whether this policy says anything out loud at all.
    ///
    /// Also mutes [`crate::observe::declined`], for the same reason.
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
/// The client generates one for every mutating command it may repeat. Persist
/// your own to survive a crash: replaying the command returns the original
/// result rather than starting a second operation. See
/// [`Client::start_operation_with`](crate::Client::start_operation_with).
///
/// **A replay must say that it is one.** Without the retry flag a known ID is
/// refused with `Duplicate request is not marked as "retry"`.
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
    /// The cluster then returns the first response instead of refusing it.
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

/// Builds a GUID as the cluster prints one: four 32-bit numbers in hex, no
/// leading zeros, `b4ef546-e730447d-103e8-20cfe65`. The bits come from
/// [`crate::unique::word`].
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
/// The retry loop's question. Whether asking somewhere else would help is
/// [`worth_asking_again`].
pub(crate) fn is_retriable(error: &ClientError) -> bool {
    match error {
        // No answer at all, which says nothing against the command, unless
        // the certificate was refused.
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
/// Only for a settled verdict: see [`settled_certificate_verdict`].
fn rejected_the_certificate(error: &ureq::Error) -> bool {
    settled_certificate_verdict(error).is_some()
}

/// Which settled verdict the TLS layer returned, if it returned one.
///
/// `rustls` wraps its error in an `io::Error` of kind `InvalidData`, which
/// `ureq` passes through as `ureq::Error::Io`; neither `ureq` nor `ureq-proto`
/// produces that kind itself. This crate does not depend on `rustls`, so the
/// rendered text is matched, narrowed three ways: the kind (the TLS layer), the
/// [`CERTIFICATE_VERDICT`] prefix (the certificate, not the handshake), and
/// [`SETTLED_REJECTIONS`]. It answers which verdict because
/// `error::certificate_advice` treats `UnknownIssuer` and
/// `NotValidForName` differently; that caller shares this match rather than
/// writing its own.
pub(crate) fn settled_certificate_verdict(error: &ureq::Error) -> Option<&'static str> {
    let ureq::Error::Io(io) = error else {
        return None;
    };

    if io.kind() != std::io::ErrorKind::InvalidData {
        return None;
    }

    let message = io.to_string();
    let (_, reason) = message.split_once(CERTIFICATE_VERDICT)?;

    // `starts_with`: an `Other(..)` message quoting `UnknownIssuer` is not
    // that verdict.
    SETTLED_REJECTIONS
        .iter()
        .find(|settled| reason.starts_with(*settled))
        .copied()
}

/// Looks for one of `wanted` anywhere in an error document.
///
/// The outer code is often a wrapper (`Request retries failed`, `Error
/// resolving path`), and the deciding code sits in `inner_errors`. A document
/// that is not JSON answers `false`: the outer code was already consulted.
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
/// [`is_retriable`] asks whether waiting would help; this asks whether asking
/// again ever would. Its caller is `Transport::base_for`, judging a failed
/// `/hosts` lookup or refresh: ask again soon, or an interval from now.
///
/// It is `is_retriable` plus two cases where the addressee, not the moment, was
/// wrong: a proxy refusing heavy work for its role, and a
/// [`ClientError::Redirected`] (a balancer's routing may differ next time;
/// following the redirect is what must not happen). A rejected certificate
/// stays `false`: asking twice does not make a host trusted.
pub(crate) fn worth_asking_again(error: &ClientError) -> bool {
    is_retriable(error)
        || refused_for_being_the_wrong_proxy(error)
        || matches!(error, ClientError::Redirected { .. })
}

/// Whether a heavy command's failure is plausibly about the **host** it went
/// to rather than about the request itself.
///
/// Asked by `Transport::after_heavy`, deciding whether to drop a discovered
/// proxy from the pool. It is [`worth_asking_again`] plus a settled
/// certificate rejection: `NotValidForName`, or `UnknownIssuer` from a
/// misissued chain, is about one host, and other proxies may present good
/// certificates. A request's own fault (a resolve error, a schema mismatch)
/// keeps the host.
pub(crate) fn attributable_to_the_host(error: &ClientError) -> bool {
    matches!(error, ClientError::Transport { source, .. } if rejected_the_certificate(source))
        || worth_asking_again(error)
}

/// Whether the proxy refused this because of the **role it has**.
///
/// `Control proxy may not serve heavy requests with input data` is a cluster
/// error with code 1: hopeless to resend to the same proxy, which
/// [`is_retriable`] answers, but fixed by asking `/hosts` again (an operator
/// can change `default_role_filter` so that it lists control proxies).
fn refused_for_being_the_wrong_proxy(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Cluster { message, .. } if message.contains(crate::http::CONTROL_REFUSAL)
    )
}

/// Whether a fresh client should announce its retries.
///
/// Not inside a job (`YT_JOB_ID` set), whose stderr is the cluster's bounded
/// diagnostic buffer. [`RetryPolicy::loud`] puts the messages back.
pub(crate) fn report_by_default() -> bool {
    !inside_job(std::env::var_os("YT_JOB_ID"))
}

/// Split out so a test need not write the process environment.
fn inside_job(job_id: Option<std::ffi::OsString>) -> bool {
    job_id.is_some_and(|id| !id.is_empty())
}

/// Runs `action` until it succeeds, gives up, or fails for a reason a retry
/// cannot fix.
///
/// `action` is told whether this is a retry, for a mutating command's `retry`
/// parameter. Each attempt runs in `observe::attempt`; retries are announced
/// unless the policy is [`RetryPolicy::quiet`].
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
                    // Counts attempts, as the span does, so `attempt == of`
                    // means the last try.
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
    fn a_rejected_certificate_is_the_hosts_fault_though_not_worth_waiting_or_asking() {
        // The three predicates part company exactly here. Waiting cannot mend
        // a verdict this client's own roots and URL
        // decided, so `is_retriable` says no; the coordinator's list is not
        // what was wrong, so `worth_asking_again` inherits the no. But
        // `NotValidForName` is a verdict about *one host's name* — the rest
        // of the fleet matches its own names fine — so the pool must drop
        // that host and pick another. Gating the drop on either other
        // predicate is the mutation this test exists to fail.
        for spelling in [
            "invalid peer certificate: UnknownIssuer",
            "invalid peer certificate: certificate not valid for name \"n0132.example.net\"; \
             certificate is only valid for [\"cluster.example.net\"]",
            "invalid peer certificate: NotValidForName",
        ] {
            let rejected = transport_error(std::io::ErrorKind::InvalidData, spelling);
            assert!(!is_retriable(&rejected), "{spelling}");
            assert!(!worth_asking_again(&rejected), "{spelling}");
            assert!(attributable_to_the_host(&rejected), "{spelling}");
        }

        // Everything worth asking the coordinator about again is also the
        // host's fault — the implication only runs one way, and this is the
        // direction that must hold on every branch.
        for hosts_fault in [
            ClientError::Transport {
                command: "write_table".to_owned(),
                source: Box::new(ureq::Error::HostNotFound),
            },
            ClientError::Http {
                command: "write_table".to_owned(),
                status: 503,
                body: String::new(),
            },
            ClientError::Cluster {
                command: "write_table".to_owned(),
                code: 1,
                message: "Control proxy may not serve heavy requests with input data".to_owned(),
                raw: r#"{"code":1}"#.to_owned(),
            },
        ] {
            assert!(worth_asking_again(&hosts_fault), "{hosts_fault}");
            assert!(attributable_to_the_host(&hosts_fault), "{hosts_fault}");
        }

        // And a failure about the request keeps the host: the same command
        // will be exactly as wrong at every other proxy in the pool.
        for requests_fault in [
            ClientError::Http {
                command: "write_table".to_owned(),
                status: 404,
                body: String::new(),
            },
            cluster_error(500, r#"{"code":500}"#),
            ClientError::Decode {
                command: "read_table".to_owned(),
                reason: "cut short".to_owned(),
            },
        ] {
            assert!(
                !attributable_to_the_host(&requests_fault),
                "{requests_fault}"
            );
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
