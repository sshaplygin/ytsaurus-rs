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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::{ClientError, Result};

/// How a command may be repeated.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Heavy, or mutating outside the master's mutation cache. Sent once,
    /// whatever the policy says.
    Never,
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
    /// Whether a retry announces itself on stderr. See [`RetryPolicy::quiet`].
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

    /// The same policy, announcing each retry on stderr.
    ///
    /// The default outside a job, and what puts the messages back inside one —
    /// a job whose stderr nobody else is using may well want them.
    #[must_use]
    pub fn loud(mut self) -> Self {
        self.report = true;
        self
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

/// Counts calls, so two IDs made in the same nanosecond still differ.
static MUTATIONS: AtomicU64 = AtomicU64::new(0);

/// Builds a GUID: four 32-bit numbers in hex, separated by `-`, as the command
/// reference describes them and as the cluster's own IDs are printed —
/// `b4ef546-e730447d-103e8-20cfe65`, with no leading zeros.
///
/// The entropy comes from `RandomState`, which the standard library seeds from
/// the OS once per process, mixed with a counter and the clock. A mutation ID
/// needs to be *unique*, not unpredictable, and that is a poor reason to add a
/// random-number crate to a dependency list this short.
fn generate() -> String {
    use std::hash::{BuildHasher, Hasher, RandomState};

    let counter = MUTATIONS.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);

    let mut parts = [0_u32; 4];
    for (i, pair) in parts.chunks_mut(2).enumerate() {
        // A fresh `RandomState` per half: its keys differ between instances,
        // so the two halves are not two views of one 64-bit value.
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(counter);
        hasher.write_u64(nanos);
        hasher.write_usize(i);
        let value = hasher.finish();

        pair[0] = (value >> 32) as u32;
        pair[1] = value as u32;
    }

    format!(
        "{:x}-{:x}-{:x}-{:x}",
        parts[0], parts[1], parts[2], parts[3]
    )
}

/// Whether repeating the request could plausibly succeed.
pub(crate) fn is_retriable(error: &ClientError) -> bool {
    match error {
        // The request never got an answer: a refused connection, a reset, a
        // timeout. Nothing about it says the command was wrong.
        ClientError::Transport { .. } => true,
        ClientError::Http { status, .. } => RETRIABLE_STATUSES.contains(status),
        ClientError::Cluster { code, raw, .. } => {
            RETRIABLE_CODES.contains(code) || raw_contains_retriable_code(raw)
        }
        _ => false,
    }
}

/// Looks for a retriable code anywhere in the error document.
///
/// The outer code is often a wrapper — `Request retries failed` — while the
/// reason that decides retriability sits in `inner_errors`.
fn raw_contains_retriable_code(raw: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    contains_retriable_code(&value)
}

fn contains_retriable_code(value: &serde_json::Value) -> bool {
    if let Some(code) = value.get("code").and_then(serde_json::Value::as_i64)
        && RETRIABLE_CODES.contains(&code)
    {
        return true;
    }

    value
        .get("inner_errors")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|inner| inner.iter().any(contains_retriable_code))
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
/// puts in its `retry` parameter. Progress goes to stderr unless the policy is
/// [`RetryPolicy::quiet`]: a run that pauses for fifteen seconds should say why
/// rather than look hung.
pub(crate) fn run<T>(
    policy: RetryPolicy,
    repeatable: Repeatable,
    command: &str,
    mut action: impl FnMut(bool) -> Result<T>,
) -> Result<T> {
    let allowed = match repeatable {
        Repeatable::Never => 1,
        _ => policy.attempts,
    };

    let mut attempt = 1;
    loop {
        match action(attempt > 1) {
            Ok(value) => return Ok(value),
            Err(error) => {
                if attempt >= allowed || !is_retriable(&error) {
                    return Err(error);
                }

                let wait = policy.backoff(attempt);
                if policy.report {
                    eprintln!(
                        "ytsaurus-client: {command} failed ({error}); \
                         retrying in {:.1}s ({attempt}/{})",
                        wait.as_secs_f64(),
                        allowed - 1
                    );
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
        let calls = std::cell::Cell::new(0);

        let result: Result<()> = run(instant(5), Repeatable::Never, "write_table", |_| {
            calls.set(calls.get() + 1);
            Err(cluster_error(105, r#"{"code":105}"#))
        });

        assert!(result.is_err());
        assert_eq!(
            calls.get(),
            1,
            "heavy commands cannot be retried, whatever the policy says"
        );
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
