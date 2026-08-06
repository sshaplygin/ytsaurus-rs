//! HTTP transport for the YTsaurus API v4.
//!
//! The protocol, from
//! <https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference>:
//!
//! - commands live at `/api/v4/<command>`;
//! - `X-YT-Header-Format` says how the other `X-YT-*` headers are encoded; this
//!   client uses text YSON for all of them;
//! - command parameters go in `X-YT-Parameters`, not the query string or body,
//!   which keeps the body free for the data stream;
//! - the body is the input stream for commands that take one;
//! - failures are reported in `X-YT-Error`.
//!
//! # A known gap: trailers
//!
//! The proxy can only discover some failures *after* it has begun streaming a
//! 200 response, and reports those in an `X-YT-Error` **trailer** rather than a
//! header. `ureq` 3.3 exposes no trailers, so this client cannot read them.
//!
//! Rechecked against `ureq` 3.3's own source rather than carried forward as an
//! assumption: the string "trailer" does not appear in it.
//!
//! Rather than pretend the gap does not exist, the client checks what it can:
//! a truncated data stream is caught by validating that the response is a
//! complete YSON list fragment (see `Client::read_table`), and on the streaming
//! path — which never has the whole thing to validate — by the decoder failing
//! on the record that was cut in half. A mid-stream failure that still produces
//! well-formed output would go unnoticed either way: in practice a partial read
//! reported as success.
//!
//! # Where a command is sent
//!
//! Not every command goes to the address the client was configured with. A
//! large installation gives its proxies roles, and a *control* proxy will not
//! serve a heavy request — so the heavy ones ask `/hosts` where they should
//! go. [`Transport::base_for`] is that decision and [`HeavyProxy`] is what it
//! remembers.
//!
//! What a control proxy does with a heavy request depends on whether the
//! request carries **input data**, and this client's own error rendering hides
//! the difference — the status is not in the message, only the cluster's error
//! document is. From `TContext::TryRedirectHeavyRequests` in
//! [`yt/yt/server/http_proxy/context.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/server/http_proxy/context.cpp):
//!
//! - a heavy command **with** an input stream — `write_table`, `write_file` —
//!   is refused with **503** and `Retry-After: 60`, carrying the error
//!   `Control proxy may not serve heavy requests with input data`;
//! - a heavy command **without** one — `read_table`, `read_file`,
//!   `get_job_input`, `get_job_stderr` — is answered with a **307** to a data
//!   proxy, or with 503 and `There are no data proxies available` if there is
//!   none.
//!
//! The documentation gives both halves and neither whole: the
//! [`/hosts` section](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)
//! says "light proxies return code 503", and the return-code table on the same
//! page lists "307 — Redirecting heavy queries from light to heavy proxies".
//! The input-data test above is what decides which.
//!
//! Discovery is what keeps either from happening. The lookup gets its **own**
//! budget rather than the client's — see [`HOSTS_TIMEOUT`] — because it sits in
//! front of the first heavy command and a proxy that cannot answer it in that
//! time has not earned the wait.

use std::borrow::Cow;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use ureq::http::HeaderMap;
use ureq::{AsSendBody, SendBody};
use ytsaurus_yson::{YsonFormat, YsonValue, to_string};

use crate::error::{ClientError, Result, truncate};
use crate::retry::{MutationId, Repeatable, RetryPolicy};
use crate::yson_build::{boolean, insert, string};

const HEADER_FORMAT: &str = "X-YT-Header-Format";
const PARAMETERS: &str = "X-YT-Parameters";
const ERROR: &str = "X-YT-Error";
/// The W3C trace context, in the spelling the proxy parses. See
/// [`TraceContext`](crate::TraceContext).
const TRACEPARENT: &str = "traceparent";
/// The vendor state the standard pairs with `traceparent`. The proxy has no
/// opinion about it; a caller's own backend may well have one, and a
/// participant that forwards the one header is required to forward the other.
const TRACESTATE: &str = "tracestate";

/// The parameter that puts a command inside a transaction.
const TRANSACTION_ID: &str = "transaction_id";

/// Commands that have no transaction to be in.
///
/// These go to the scheduler and the controller agents rather than to the
/// master, and take no `TTransactionalOptions`. Stamping them works only for as
/// long as the proxy quietly drops parameters it does not recognise; on a
/// cluster or a version that refuses them instead, every transaction-scoped
/// launcher would fail at its first `wait_for_operation`.
///
/// `start_operation` is deliberately *not* here: an operation genuinely can run
/// inside a transaction, which is how its output tables stay invisible until
/// the launcher commits.
const NO_TRANSACTION: &[&str] = &[
    "get_operation",
    "list_operations",
    "list_operation_events",
    "abort_operation",
    "complete_operation",
    "suspend_operation",
    "resume_operation",
    "update_operation_parameters",
    "list_jobs",
    "get_job",
    "get_job_stderr",
    "get_job_input",
    "abort_job",
    "poll_job_shell",
];

/// Applies a header list to either builder flavour.
///
/// `ureq` gives requests with and without a body distinct builder types, so a
/// plain function cannot decorate both. A macro can.
macro_rules! with_headers {
    ($request:expr $(, $headers:expr)* $(,)?) => {{
        let mut request = $request;
        $(
            for (name, value) in $headers {
                request = request.header(*name, value.as_str());
            }
        )*
        request
    }};
}

/// How the command's payload is carried.
pub(crate) enum Payload<'a> {
    /// No request body.
    None,
    /// Raw bytes, for commands like `write_file`.
    Bytes(&'a [u8]),
}

/// How long the whole `/hosts` lookup may take.
///
/// **Not the client's request timeout, and not its retry policy.** This
/// question sits in front of the first heavy command, its answer is a few
/// hundred bytes from a proxy the client is already talking to, and failing to
/// get one is not fatal — the command goes where it would have gone before
/// there was a lookup at all. Under the client's own policy it was five
/// attempts of up to two minutes with fifteen seconds of backoff between them,
/// so a `/hosts` that answered 503 cost a heavy command **fifteen seconds** and
/// one that hung cost it **ten minutes**, all of it under the mutex.
///
/// One attempt, then. The retry is [`HOSTS_RETRY_AFTER`] rather than a second
/// attempt inside the lock: spreading it out is what keeps a client whose
/// `/hosts` is down from paying for the answer over and over.
const HOSTS_TIMEOUT: Duration = Duration::from_millis(800);

/// How long the configured address serves heavy commands after a lookup that
/// did not settle, before the cluster is asked again.
///
/// Two failures share this. A lookup that failed for a reason that might pass,
/// and a discovered proxy a heavy command could not reach: both mean "use the
/// address the caller gave, and ask again in a moment" rather than "ask again
/// now", which is what turned eight threads into eight lookups and one dead
/// host into every upload failing.
///
/// Short, because it is also how quickly routing comes back once the cluster
/// does. Long enough that a client uploading in a loop against a broken
/// `/hosts` pays [`HOSTS_TIMEOUT`] a handful of times a minute rather than
/// once per upload.
const HOSTS_RETRY_AFTER: Duration = Duration::from_secs(10);

/// Where the cluster wants heavy commands sent.
///
/// Asked once per client and kept for its lifetime — see
/// [`Transport::base_for`]. Shared by every clone, because
/// `Client::with_transaction`, `Operation` and the diagnostics client are all
/// clones of one client, and a lookup each would be a lookup per command.
#[derive(Debug)]
enum HeavyProxy {
    /// The cluster has not been asked yet.
    Unasked,
    /// The address `/hosts` named, already a base URL.
    At(String),
    /// The cluster was asked and named none this client may use, so the
    /// configured address serves heavy commands too. A single-node cluster, any
    /// installation that does not separate the roles, and a `/hosts` whose
    /// answer was refused — see [`heavy_base`].
    Configured,
    /// The question did not settle, or the answer did not work.
    ///
    /// The configured address serves heavy commands until `until`, and then the
    /// cluster is asked once more. This is what a *waiting* thread finds rather
    /// than an invitation to perform the same failing lookup itself, and what a
    /// heavy command finds after the proxy it was routed to refused the
    /// connection.
    FellBack {
        /// When to ask again. See [`HOSTS_RETRY_AFTER`].
        until: Instant,
    },
}

/// A configured connection to one cluster.
#[derive(Clone)]
pub(crate) struct Transport {
    agent: ureq::Agent,
    /// The address the caller gave. Every light command goes here, and so does
    /// a heavy one until the cluster names somewhere better.
    base: String,
    /// Where heavy commands go, once asked. See [`HeavyProxy`].
    heavy: Arc<Mutex<HeavyProxy>>,
    /// Whether to ask at all. Off for a cluster on loopback — see
    /// [`is_local`] — and settable either way by the caller.
    discovery: bool,
    /// Whether a discovered host may leave the configured address's own
    /// domain. Off by default — see [`heavy_base`].
    anywhere: bool,
    token: Option<String>,
    retries: RetryPolicy,
    /// End-to-end limit for buffered commands; per-phase limit for streaming
    /// ones — see [`Transport::dispatch`].
    timeout: Duration,
    /// Stamped onto every command, when the client is bound to a transaction.
    transaction: Option<String>,
    /// The `traceparent` header, when the client was given a trace to belong
    /// to.
    trace: Option<String>,
    /// The companion `tracestate`, carried unmodified when the context that
    /// was joined had one. See [`TraceContext::tracestate`].
    tracestate: Option<String>,
    /// The headers that say who is asking, rendered once — see
    /// [`Transport::render_caller_headers`]. None of them changes between
    /// requests, so none of them is worth building again for each one.
    caller: Vec<(&'static str, String)>,
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("base", &self.base)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Transport {
    pub(crate) fn new(proxy: &str, token: Option<String>, timeout: Duration) -> Self {
        let base = if proxy.starts_with("http://") || proxy.starts_with("https://") {
            proxy.trim_end_matches('/').to_owned()
        } else {
            // A bare host means a real cluster, which is always TLS. Only an
            // explicit `http://` opts out, which is how a local cluster is
            // addressed.
            format!("https://{}", proxy.trim_end_matches('/'))
        };

        // Quiet inside a job, where stderr is the cluster's diagnostic channel
        // and not a terminal. See `retry::report_by_default`.
        let retries = if crate::retry::report_by_default() {
            RetryPolicy::default()
        } else {
            RetryPolicy::default().quiet()
        };

        let mut transport = Self {
            agent: build_agent(timeout),
            discovery: !is_local(&base),
            anywhere: false,
            base,
            heavy: Arc::new(Mutex::new(HeavyProxy::Unasked)),
            token,
            retries,
            timeout,
            transaction: None,
            trace: None,
            tracestate: None,
            caller: Vec::new(),
        };
        transport.render_caller_headers();
        transport
    }

    pub(crate) fn set_retries(&mut self, policy: RetryPolicy) {
        self.retries = policy;
    }

    /// Turns the `/hosts` lookup on or off, forgetting anything it found.
    pub(crate) fn set_proxy_discovery(&mut self, enabled: bool) {
        self.discovery = enabled;
        self.forget_heavy();
    }

    /// Lets a discovered host be one outside the configured address's domain.
    pub(crate) fn set_heavy_proxies_anywhere(&mut self, enabled: bool) {
        self.anywhere = enabled;
        self.forget_heavy();
    }

    /// Drops what discovery resolved, because the rules it resolved under have
    /// changed.
    ///
    /// A fresh `Arc`, not a write through the shared one: these are builders on
    /// a clone of the client, and narrowing the rules here must not discard what
    /// the client this was cloned from has already resolved under the old ones.
    fn forget_heavy(&mut self) {
        self.heavy = Arc::new(Mutex::new(HeavyProxy::Unasked));
    }

    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        self.agent = build_agent(timeout);
    }

    pub(crate) fn set_transaction(&mut self, id: Option<String>) {
        self.transaction = id;
    }

    pub(crate) fn transaction(&self) -> Option<&str> {
        self.transaction.as_deref()
    }

    pub(crate) fn set_trace(&mut self, context: &crate::TraceContext) {
        self.trace = Some(context.header());
        self.tracestate = context.tracestate().map(str::to_owned);
        self.render_caller_headers();
    }

    pub(crate) fn trace(&self) -> Option<&str> {
        self.trace.as_deref()
    }

    pub(crate) fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    /// Executes a command, repeating it when the failure looks transient.
    ///
    /// `repeatable` says what the command allows: a read is simply re-sent, a
    /// light mutation is re-sent under a `mutation_id` the cluster
    /// deduplicates, and a heavy command is sent once whatever the policy says
    /// — and to the proxy the cluster named for heavy work, which is the other
    /// half of what [`Repeatable::Heavy`] declares.
    pub(crate) fn call(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        payload: Payload<'_>,
        repeatable: Repeatable,
    ) -> Result<Vec<u8>> {
        self.call_with(method, command, parameters, payload, repeatable, None)
    }

    /// As [`Transport::call`], with a caller-supplied mutation ID.
    pub(crate) fn call_with(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        payload: Payload<'_>,
        repeatable: Repeatable,
        mutation_id: Option<&MutationId>,
    ) -> Result<Vec<u8>> {
        let mutation_id = match (repeatable, mutation_id) {
            (_, Some(given)) => Some(given.clone()),
            (Repeatable::WithMutationId, None) => Some(MutationId::new()),
            _ => None,
        };

        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        let base = self.base_for(repeatable);
        let sent = crate::retry::run(self.retries, repeatable, command, |is_retry| {
            match &mutation_id {
                Some(id) => {
                    // The ID stays the same across attempts — that is what the
                    // cluster deduplicates by — and only the flag changes. A
                    // caller-supplied ID may already be marked as a replay,
                    // which is how a restarted process resumes; the cluster
                    // refuses a duplicate that does not say so.
                    let mut tagged = parameters.clone();
                    insert(&mut tagged, "mutation_id", string(id.as_str()));
                    insert(&mut tagged, "retry", boolean(is_retry || id.is_retry()));
                    self.send(&base, method, command, &tagged, &payload)
                }
                None => self.send(&base, method, command, parameters, &payload),
            }
        });

        self.after_heavy(repeatable, &base, sent)
    }

    /// Puts the client's transaction into a command's parameters.
    ///
    /// `None` when there is nothing to add, so the common case does not copy
    /// the parameters. One place rather than one per command: the cluster
    /// applies `transaction_id` to everything a transaction can contain, and a
    /// command this client forgot to stamp would silently do its work outside
    /// the transaction — the failure a transaction exists to prevent.
    ///
    /// A command that already names a transaction keeps the one it named:
    /// `commit_transaction` and its siblings mean a specific transaction, and
    /// that is exactly the one they are given.
    ///
    /// A command that has no transaction to be in is left alone — see
    /// [`NO_TRANSACTION`]. `Transaction` derefs to `Client`, so a launcher
    /// reaches `wait_for_operation` and its diagnostics through a bound client
    /// as a matter of course.
    fn in_transaction(&self, command: &str, parameters: &YsonValue) -> Option<YsonValue> {
        let id = self.transaction.as_ref()?;

        if NO_TRANSACTION.contains(&command) {
            return None;
        }

        if let ytsaurus_yson::YsonNode::Map(m) = &parameters.node
            && m.contains_key(TRANSACTION_ID.as_bytes())
        {
            return None;
        }

        let mut tagged = parameters.clone();
        insert(&mut tagged, TRANSACTION_ID, string(id));
        Some(tagged)
    }

    /// Which address one command is sent to.
    ///
    /// Everything light goes to the address the caller configured. A heavy
    /// command — [`Repeatable::Heavy`], the `isHeavy` bit of the cluster's own
    /// command registry — goes to a proxy that will accept one: an installation
    /// that separates the roles will not serve a heavy request on a *control*
    /// proxy, and the balancer a caller is usually pointed at fronts exactly
    /// those. See the module documentation for what the refusal looks like.
    ///
    /// The lookup happens **at most once per client** while it keeps working,
    /// and only when the first heavy command needs it. A cluster that names
    /// nobody this client may use — a single-node installation, any that does
    /// not split the roles, and one whose answer [`heavy_base`] refused — is
    /// remembered as answered, and every heavy command then goes where it
    /// always went. That fallback is not a nicety: it is the whole of what keeps
    /// a local cluster behaving as it did.
    ///
    /// The mutex is held **across the lookup**, deliberately: a second thread
    /// that wanted a heavy proxy at the same moment waits for this answer
    /// rather than asking the same question again. What makes that safe rather
    /// than a queue is that a lookup which *failed* also leaves an answer —
    /// [`HeavyProxy::FellBack`] — so the waiters find a decision rather than an
    /// invitation to repeat it. Before that, eight threads against a failing
    /// `/hosts` performed eight lookups, each waiting out the one in front.
    /// `fetch` does not touch this lock, so there is nothing here to deadlock
    /// against.
    fn base_for(&self, repeatable: Repeatable) -> Cow<'_, str> {
        if repeatable != Repeatable::Heavy || !self.discovery {
            return Cow::Borrowed(&self.base);
        }

        let mut resolved = lock(&self.heavy);
        match &*resolved {
            HeavyProxy::At(base) => return Cow::Owned(base.clone()),
            HeavyProxy::Configured => return Cow::Borrowed(&self.base),
            HeavyProxy::FellBack { until } if Instant::now() < *until => {
                return Cow::Borrowed(&self.base);
            }
            HeavyProxy::Unasked | HeavyProxy::FellBack { .. } => {}
        }

        match self.heavy_hosts() {
            // The first host this client is willing to use, not simply the
            // first host. `/hosts` is ordered best-first, and a name that is
            // blank, malformed or somewhere else entirely is passed over rather
            // than being allowed to stand for the whole answer.
            Ok(hosts) => match hosts
                .iter()
                .find_map(|host| heavy_base(&self.base, host, self.anywhere))
            {
                Some(base) => {
                    *resolved = HeavyProxy::At(base.clone());
                    Cow::Owned(base)
                }
                None => {
                    *resolved = HeavyProxy::Configured;
                    Cow::Borrowed(&self.base)
                }
            },
            // A failed lookup is never fatal: the command goes where it would
            // have gone before there was a lookup at all. Whether to ask again
            // is `worth_asking_again`, which is a different question from
            // whether to retry — a cluster with no `/hosts` endpoint answers 404
            // every time and must not be asked before every upload, while a
            // timeout or a restarting proxy says nothing about the roles and is
            // worth one more question in a moment.
            Err(error) => {
                *resolved = if crate::retry::worth_asking_again(&error) {
                    HeavyProxy::FellBack {
                        until: Instant::now() + HOSTS_RETRY_AFTER,
                    }
                } else {
                    HeavyProxy::Configured
                };
                Cow::Borrowed(&self.base)
            }
        }
    }

    /// The heavy proxies the cluster names, best first.
    ///
    /// `/hosts` answers with a JSON list of bare host names —
    /// `["n0008-sas.cluster-name", …]`, as the
    /// [HTTP proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)
    /// shows on the wire — "ordered by load … the very first proxy in the
    /// resulting list is the least loaded"
    /// ([reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)).
    ///
    /// Which role it lists is *not* in the documentation. It is
    /// `default_role_filter`, a coordinator config parameter that
    /// `TCoordinatorConfig::Register` defaults to `NApi::DefaultHttpProxyRole`,
    /// which
    /// [`yt/yt/client/api/public.h`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/api/public.h)
    /// spells `"data"` — the role that serves heavy commands. A compiled-in
    /// default an operator can change, then, rather than a protocol guarantee,
    /// which is why this client checks what it is given instead of trusting the
    /// role.
    pub(crate) fn heavy_hosts(&self) -> Result<Vec<String>> {
        let body = self.fetch("/hosts", "hosts")?;

        serde_json::from_str(&body).map_err(|e| ClientError::Decode {
            command: "hosts".to_owned(),
            reason: format!(
                "/hosts did not answer with a list of host names: {e}; body was {}",
                truncate(&body, 200)
            ),
        })
    }

    /// What a heavy command's failure says about the proxy it was routed to.
    ///
    /// Two things, and both only for a command that actually went somewhere
    /// discovered — `base` is the address it used, and this does nothing unless
    /// the resolved answer is still that same address. A discovery-off client
    /// never takes the lock at all, and a failure at the *configured* address
    /// says nothing about a lookup: it was the caller who chose that one.
    ///
    /// **The error names the host.** `write_table: transport error: io:
    /// Connection refused` is a report about an address the caller never typed
    /// and cannot see. It now reads `write_table at n0132-sas.example.net:9013:
    /// …`.
    ///
    /// **A proxy that could not be reached stops being used.** The command
    /// itself is *not* sent again — heavy commands are not retried, and by this
    /// point a streaming body has been consumed anyway. This is about the next
    /// one: the client falls back to the configured address and asks the
    /// cluster again after [`HOSTS_RETRY_AFTER`], rather than resolving the same
    /// dead host over and over, which is what made every upload after the first
    /// failure fail too. Only for a failure a different proxy could plausibly
    /// fix; a table that does not exist will not exist over there either.
    fn after_heavy<T>(&self, repeatable: Repeatable, base: &str, result: Result<T>) -> Result<T> {
        if repeatable != Repeatable::Heavy || !self.discovery {
            return result;
        }
        let Err(error) = result else {
            return result;
        };

        let mut resolved = lock(&self.heavy);
        if !matches!(&*resolved, HeavyProxy::At(at) if at == base) {
            return Err(error);
        }

        if crate::retry::worth_asking_again(&error) {
            *resolved = HeavyProxy::FellBack {
                until: Instant::now() + HOSTS_RETRY_AFTER,
            };
        }
        drop(resolved);

        Err(routed_to(error, base))
    }

    /// One attempt, read into memory.
    fn send(
        &self,
        base: &str,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        payload: &Payload<'_>,
    ) -> Result<Vec<u8>> {
        let mut bytes: &[u8] = match payload {
            Payload::None => &[],
            Payload::Bytes(bytes) => bytes,
        };
        let mut response =
            self.dispatch(base, method, command, parameters, bytes.as_body(), false)?;

        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            // Responses are small (a table read is the exception, and a
            // launcher reads results, not bulk data). The default cap is
            // conservative enough to truncate a modest table silently.
            .limit(512 * 1024 * 1024)
            .read_to_vec()
            // A `Transport` error, not `Decode`: a connection cut while the
            // body streams in is the same network failure as one cut a packet
            // earlier, and `Decode` is the one thing the retry policy never
            // repeats.
            .map_err(|e| ClientError::Transport {
                command: command.to_owned(),
                source: Box::new(e),
            })?;

        if !(200..300).contains(&status) {
            return Err(ClientError::Http {
                command: command.to_owned(),
                status,
                body: truncate(&String::from_utf8_lossy(&body), 400),
            });
        }

        Ok(body)
    }

    /// Sends a command and hands back the response body **unread**.
    ///
    /// For `read_table`, whose response is the data: reading it into a `Vec`
    /// first would put a whole table in memory, which is the thing this avoids.
    ///
    /// Sent once, never retried, and sent to a heavy proxy — a response that is
    /// the data is the shape of a heavy command, and [`Repeatable::Heavy`] is
    /// all three of those facts at once.
    pub(crate) fn open(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
    ) -> Result<ureq::Body> {
        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        // Through `retry::run` like every other command, with `Repeatable::Heavy`
        // doing the sending-once: it caps the loop at one attempt and never
        // reaches the retry announcement, so this needs no second seam of its
        // own to be timed and named. The span closes when the headers arrive —
        // the reader handed back is read after that, at the caller's pace.
        let base = self.base_for(Repeatable::Heavy);
        let opened = crate::retry::run(self.retries, Repeatable::Heavy, command, |_| {
            let response =
                self.dispatch(&base, method, command, parameters, SendBody::none(), true)?;
            let status = response.status().as_u16();

            if !(200..300).contains(&status) {
                let mut response = response;
                let body = response.body_mut().read_to_string().unwrap_or_default();
                return Err(ClientError::Http {
                    command: command.to_owned(),
                    status,
                    body: truncate(&body, 400),
                });
            }

            Ok(response.into_body())
        });

        self.after_heavy(Repeatable::Heavy, &base, opened)
    }

    /// Sends a command whose request body is read as it goes, and returns the
    /// answer.
    ///
    /// For `write_table` from something larger than memory. `rows` is read
    /// once, so this cannot be retried even in principle: a reader that has
    /// been consumed cannot be sent again.
    ///
    /// The response body has to be read whatever the caller wants with it (see
    /// below), so it is handed back rather than dropped: `write_table` ignores
    /// it, and a raw command has no one but the caller to interpret it.
    pub(crate) fn upload(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        rows: &mut dyn std::io::Read,
    ) -> Result<Vec<u8>> {
        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        // `Repeatable::Heavy` is the sending-once, as in `open`: one attempt,
        // no announcement, and the span comes from the seam every other command
        // already goes through. Unlike `open` it covers the whole transfer —
        // the body is read here, as it goes. It also picks the address: a
        // request whose body is a data stream is the one a control proxy
        // refuses by name.
        let base = self.base_for(Repeatable::Heavy);
        let sent = crate::retry::run(self.retries, Repeatable::Heavy, command, |_| {
            let mut response = self.dispatch(
                &base,
                method,
                command,
                parameters,
                SendBody::from_reader(&mut *rows),
                true,
            )?;
            let status = response.status().as_u16();

            // Read whichever way it went. A body left unread keeps the
            // connection out of the pool — `ureq` can only reuse one it knows
            // is finished — so an upload that ignored its answer would open a
            // fresh connection for every table write, and leave the old one in
            // TIME_WAIT. The benchmark is what noticed: 11 623 of them after a
            // few seconds of writing.
            //
            // Read as bytes rather than as a string: an upload's answer is a
            // small structured document today, but a raw command sends whatever
            // it was given, and lossily decoding a binary answer would be a
            // silent corruption rather than a refusal.
            let body = response
                .body_mut()
                .with_config()
                .limit(512 * 1024 * 1024)
                .read_to_vec()
                .unwrap_or_default();

            if !(200..300).contains(&status) {
                return Err(ClientError::Http {
                    command: command.to_owned(),
                    status,
                    body: truncate(&String::from_utf8_lossy(&body), 400),
                });
            }

            Ok(body)
        });

        self.after_heavy(Repeatable::Heavy, &base, sent)
    }

    /// Fetches a path that is not an API v4 command.
    ///
    /// `/hosts` is the only one, and it is not a command — but it wants most of
    /// what a command gets: the token, the guard that turns an `https://` proxy
    /// in a build without TLS into an explanation rather than a connection
    /// error, and the caller headers that say who is asking. Building a bare
    /// `ureq` request here instead is how it came to miss all of them.
    ///
    /// **The timeout and the retry policy are the exceptions**, and
    /// deliberately: this question has its own budget. One attempt bounded by
    /// [`HOSTS_TIMEOUT`], not five of up to two minutes with fifteen seconds of
    /// backoff — because a heavy command is *waiting* on the answer, holding
    /// the lock every other heavy command wants, and not getting one costs
    /// nothing worse than the routing this client had none of a release ago.
    /// A lookup worth repeating is repeated by the next heavy command after
    /// [`HOSTS_RETRY_AFTER`], which is the same retry spread out where it does
    /// not queue anybody.
    ///
    /// It goes to the **configured** address whatever it is asking about: the
    /// question `/hosts` answers is where the other addresses are.
    pub(crate) fn fetch(&self, path: &str, what: &str) -> Result<String> {
        if let Some(error) = tls_unavailable(&self.base) {
            return Err(error);
        }

        let url = format!("{}{path}", self.base);

        crate::retry::run(RetryPolicy::none(), Repeatable::Freely, what, |_| {
            let mut response =
                with_headers!(self.within_budget(self.agent.get(&url)), &self.caller)
                    .call()
                    .map_err(|e| ClientError::Transport {
                        command: what.to_owned(),
                        source: Box::new(e),
                    })?;

            let status = response.status().as_u16();
            let body = response
                .body_mut()
                .read_to_string()
                // As in `send`: a body cut off by the network must stay
                // retriable, and `Decode` is not.
                .map_err(|e| ClientError::Transport {
                    command: what.to_owned(),
                    source: Box::new(e),
                })?;

            if !(200..300).contains(&status) {
                return Err(ClientError::Http {
                    command: what.to_owned(),
                    status,
                    body: truncate(&body, 400),
                });
            }

            Ok(body)
        })
    }

    /// Builds and sends one request, and checks the cluster's own error header.
    ///
    /// Everything past this point differs only in how the response body is
    /// consumed.
    ///
    /// `streaming` lifts the agent's end-to-end timeout for this request: a
    /// table moves through [`Transport::open`] and [`Transport::upload`] for as
    /// long as it takes, and a deadline sized for control commands would cut
    /// the transfer off mid-table. The waits that precede the data — resolve,
    /// connect, sending the request, the response headers — each stay bounded
    /// by the same timeout, so a dead proxy still fails promptly; only the
    /// body itself is open-ended.
    ///
    /// `base` is where this one goes — the configured address for a light
    /// command, and whatever [`Transport::base_for`] resolved for a heavy one.
    fn dispatch(
        &self,
        base: &str,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        body: SendBody<'_>,
        streaming: bool,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        if let Some(error) = tls_unavailable(base) {
            return Err(error);
        }

        let url = format!("{base}/api/v4/{command}");

        let encoded = to_string(parameters, YsonFormat::Text).map_err(|e| ClientError::Decode {
            command: command.to_owned(),
            reason: format!("could not encode parameters: {e}"),
        })?;

        // What is being asked. Who is asking is `self.caller`, applied beside
        // this rather than concatenated onto it: those headers are already
        // rendered, and copying them into a fresh `Vec` per request is the
        // allocation this avoids.
        let headers: [(&str, String); 4] = [
            (HEADER_FORMAT, "<format=text>yson".to_owned()),
            (PARAMETERS, encoded),
            ("X-YT-Output-Format", "<format=text>yson".to_owned()),
            ("Content-Type", "application/octet-stream".to_owned()),
        ];

        let sent = match method {
            // A GET carries no body in `ureq`'s type system, which is also true
            // of every command this client sends as one.
            Method::Get => with_headers!(
                self.scoped(self.agent.get(&url), streaming),
                &headers,
                &self.caller
            )
            .call(),
            Method::Post => with_headers!(
                self.scoped(self.agent.post(&url), streaming),
                &headers,
                &self.caller
            )
            .send(body),
            Method::Put => with_headers!(
                self.scoped(self.agent.put(&url), streaming),
                &headers,
                &self.caller
            )
            .send(body),
        };

        let response = sent.map_err(|e| ClientError::Transport {
            command: command.to_owned(),
            source: Box::new(e),
        })?;

        // The cluster's own error, which is far more useful than the status.
        if let Some(raw) = header_value(response.headers(), ERROR) {
            return Err(ClientError::from_yt_error(
                command,
                response.status().as_u16(),
                &raw,
            ));
        }

        Ok(response)
    }

    /// The headers that say who is asking rather than what is being asked.
    ///
    /// One place for both, because they belong to every request and not to any
    /// command: `/hosts` is not a command and still wants them. Building its
    /// request separately is how it once came to carry no token at all — see
    /// [`Transport::fetch`].
    ///
    /// The trace context is sent on every attempt of a retried command, with
    /// the same span id each time. That is deliberate: the retries are the
    /// same logical call, and the cluster's spans for them belong under the
    /// one span the caller knows about.
    ///
    /// Rendered when the transport is built or its trace is set, and not once
    /// per request: every value here is fixed for the transport's lifetime, so
    /// re-`format!`ing the token and re-cloning the trace for each attempt of
    /// each command bought nothing. The row-by-row write path and the
    /// two-second `wait_for_operation` poll are the ones that noticed.
    fn render_caller_headers(&mut self) {
        let mut headers = Vec::new();
        if let Some(token) = &self.token {
            headers.push(("Authorization", format!("OAuth {token}")));
        }
        if let Some(trace) = &self.trace {
            headers.push((TRACEPARENT, trace.clone()));
        }
        // Passed on beside `traceparent` and never without it: the standard
        // pairs the two, and a `tracestate` sent alone names no trace.
        if let (Some(_), Some(state)) = (&self.trace, &self.tracestate) {
            headers.push((TRACESTATE, state.clone()));
        }
        self.caller = headers;
    }

    /// Applies the streaming timeout override to one request.
    ///
    /// The end-to-end deadline comes off; every phase before the data — DNS,
    /// connect, sending the request, waiting for the response headers — keeps
    /// the same bound individually. See [`Transport::dispatch`].
    fn scoped<Any>(
        &self,
        request: ureq::RequestBuilder<Any>,
        streaming: bool,
    ) -> ureq::RequestBuilder<Any> {
        if !streaming {
            return request;
        }
        request
            .config()
            .timeout_global(None)
            .timeout_resolve(Some(self.timeout))
            .timeout_connect(Some(self.timeout))
            .timeout_send_request(Some(self.timeout))
            .timeout_recv_response(Some(self.timeout))
            .build()
    }

    /// Puts the discovery budget on one request, whatever the client's timeout
    /// is.
    ///
    /// The other direction from [`Transport::scoped`], and for the mirrored
    /// reason: a table transfer is allowed to take as long as it takes, and the
    /// question in front of it is not. See [`HOSTS_TIMEOUT`].
    fn within_budget<Any>(&self, request: ureq::RequestBuilder<Any>) -> ureq::RequestBuilder<Any> {
        request
            .config()
            .timeout_global(Some(HOSTS_TIMEOUT.min(self.timeout)))
            .build()
    }
}

/// Takes the lock, and takes it back from a thread that panicked holding it.
///
/// What this guards is a cached address. A panic while resolving one leaves it
/// as it was — `Unasked`, or the answer from before — and none of that is worth
/// poisoning a client over.
fn lock(heavy: &Mutex<HeavyProxy>) -> MutexGuard<'_, HeavyProxy> {
    heavy
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Turns a host from `/hosts` into a base URL, or refuses it.
///
/// `/hosts` answers with **bare host names** —
/// `["n0008-sas.cluster-name", …]`, as the
/// [HTTP proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)
/// shows on the wire. Everything else about the address is this client's to
/// decide, and every one of those decisions is a place a forged or mistaken
/// `/hosts` body could send an upload — and the OAuth token with it — somewhere
/// the caller never named. So a name is checked rather than pasted:
///
/// - **the scheme comes from the configured address and only from there.** A
///   host naming its own is refused outright, which is what closes the
///   downgrade: `http://n0132` from an `https://` client used to strip TLS and
///   put the token on the wire in cleartext. A cluster reached over TLS serves
///   its heavy commands over TLS.
/// - **`/`, `@`, `://` and whitespace are refused.** `@` is the one that
///   matters: `real.example.net@evil.example.net` is a URL whose *host* is
///   `evil.example.net` and whose userinfo is the reassuring half.
/// - **the configured port carries through** when the name has none, because
///   the name usually has none — the coordinator only appends `:port` when its
///   `ShowPorts` config says to — and a client reached at `:8443` has no reason
///   to believe the heavy proxies answer on 80.
/// - **an unbracketed name with more than one colon is refused.** A bare IPv6
///   literal is not a valid URL authority; bracketed, it is.
/// - **the name must sit under the configured address's own domain**, unless
///   `anywhere` says otherwise. See below.
///
/// # Why the domain is checked
///
/// The `/hosts` body decides where every heavy command goes, and the request
/// that goes there carries the caller's OAuth token. On a plain-`http://` base,
/// forging that body is exactly as easy as forging a `Location` header — and
/// this client refuses to let a `Location` choose where the token goes. Letting
/// a JSON array choose it instead would be the same decision answered two ways.
///
/// The rule is deliberately mechanical, because no public-suffix list is worth
/// a dependency here: the name must **be** the configured host, or sit under
/// the configured host's parent domain — its own name minus the leftmost label,
/// never shortened below two labels. `cluster.example.net` therefore admits
/// `n0132-sas.example.net` and `n0132-sas.cluster.example.net`, and refuses
/// `cluster.example.net.evil.com`. An address that is a literal IP admits only
/// itself: an IP has no domain to share.
///
/// A refused name is passed over, and a cluster whose whole answer is refused
/// is treated as one that named nobody — the upload goes to the configured
/// address, which is where it went before there was a lookup at all.
/// `Client::with_heavy_proxies_anywhere` is the opt-in for an installation
/// whose `/hosts` genuinely names another domain.
fn heavy_base(configured: &str, host: &str, anywhere: bool) -> Option<String> {
    let host = host.trim();

    if host.is_empty()
        || host.contains("://")
        || host.contains('/')
        || host.contains('@')
        || host.contains(['?', '#'])
        || host.chars().any(char::is_whitespace)
    {
        return None;
    }

    // A bare IPv6 literal has to be bracketed to be an authority. Unbracketed,
    // a second colon means this is not one host and one port.
    if !host.starts_with('[') && host.matches(':').count() > 1 {
        return None;
    }

    if !anywhere && !same_domain(host_of(configured), host_of(host)) {
        return None;
    }

    let scheme = if configured.starts_with("https://") {
        "https://"
    } else {
        "http://"
    };

    match (has_port(host), port_of(configured)) {
        (false, Some(port)) => Some(format!("{scheme}{host}:{port}")),
        _ => Some(format!("{scheme}{host}")),
    }
}

/// Whether `discovered` sits under the same domain as `configured`.
///
/// The domain both must share is the configured host minus its leftmost label,
/// never shortened below two labels — so a client pointed at
/// `cluster.example.net` accepts anything under `example.net`, and one pointed
/// at the two-label `example.net` accepts only `example.net` and what is under
/// it. See [`heavy_base`] for why this is a suffix rule and not a public-suffix
/// lookup.
///
/// A literal IP address has no domain, so it admits only itself.
fn same_domain(configured: &str, discovered: &str) -> bool {
    let configured = configured.to_ascii_lowercase();
    let discovered = discovered.to_ascii_lowercase();

    if configured == discovered {
        return true;
    }
    if configured.parse::<std::net::IpAddr>().is_ok()
        || discovered.parse::<std::net::IpAddr>().is_ok()
    {
        return false;
    }

    let domain = match configured.split_once('.') {
        Some((_, parent)) if parent.contains('.') => parent,
        _ => configured.as_str(),
    };

    discovered == domain || discovered.ends_with(&format!(".{domain}"))
}

/// Whether an authority names a port of its own.
fn has_port(authority: &str) -> bool {
    match authority.split_once(']') {
        Some((_, rest)) => rest.starts_with(':'),
        None => authority.contains(':'),
    }
}

/// The port out of a base URL, if it names one.
fn port_of(base: &str) -> Option<&str> {
    let authority = authority_of(base);
    let port = match authority.split_once(']') {
        Some((_, rest)) => rest.strip_prefix(':')?,
        None => authority.split_once(':').map(|(_, port)| port)?,
    };
    (!port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())).then_some(port)
}

/// The `host:port` out of a base URL — what a failure should name.
///
/// Userinfo comes off, which matters twice: it is where a password would be,
/// and leaving it on would make [`port_of`] read `pass@host:8000` and find no
/// port at all.
fn authority_of(base: &str) -> &str {
    let authority = base
        .split_once("://")
        .map_or(base, |(_, rest)| rest)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    authority.rsplit_once('@').map_or(authority, |(_, h)| h)
}

/// Names the proxy a routed command actually went to.
///
/// `write_table: transport error: io: Connection refused` is a true report
/// about an address that appears nowhere in the caller's own code: the client
/// chose it, from a list the cluster gave it, and then said nothing about the
/// choice. The same misdirection as an error that blames a token for a host it
/// was never sent to.
///
/// Only for a command that was routed — a failure at the configured address
/// needs no explaining, because that is the address the caller typed.
fn routed_to(error: ClientError, base: &str) -> ClientError {
    let at = format!(" at {}", authority_of(base));

    match error {
        ClientError::Transport { command, source } => ClientError::Transport {
            command: command + &at,
            source,
        },
        ClientError::Cluster {
            command,
            code,
            message,
            raw,
        } => ClientError::Cluster {
            command: command + &at,
            code,
            message,
            raw,
        },
        ClientError::Http {
            command,
            status,
            body,
        } => ClientError::Http {
            command: command + &at,
            status,
            body,
        },
        ClientError::Decode { command, reason } => ClientError::Decode {
            command: command + &at,
            reason,
        },
        // Nothing else carries a command to qualify: an `Io` names a local
        // path, a `Config` names the build, and an `OperationFailed` is the
        // scheduler's verdict rather than one proxy's.
        other => other,
    }
}

/// Whether `base` names a cluster on this machine, or a tunnel to one.
///
/// Such a cluster is not asked where its heavy proxies are, and this is the
/// one place that decision is made. Two reasons, and either would do:
///
/// - a single-node installation has no separate heavy proxies, so the lookup
///   can only cost a round trip before the first upload;
/// - the address a cluster publishes for itself is its own, and from behind a
///   port mapping or an SSH tunnel it is not reachable at all. A local
///   YTsaurus in Docker is reached at `localhost:8000` and knows itself by the
///   container's address and port — following that would send every upload
///   somewhere this process cannot go.
///
/// So the default is "ask, unless the address says it cannot help", and
/// `Client::with_proxy_discovery` overrides it in both directions.
fn is_local(base: &str) -> bool {
    let host = host_of(base);

    if let Ok(address) = host.parse::<std::net::IpAddr>() {
        // `is_unspecified` covers `0.0.0.0`, which is not loopback but is
        // nobody else's address either.
        return address.is_loopback() || address.is_unspecified();
    }

    host.eq_ignore_ascii_case("localhost")
}

/// The host out of a base URL, without scheme, port or path.
///
/// `http://[::1]:8000` is why this is not a `split(':')`: an IPv6 literal is
/// bracketed and full of colons.
fn host_of(base: &str) -> &str {
    let authority = base
        .split_once("://")
        .map_or(base, |(_, rest)| rest)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);

    match authority.strip_prefix('[') {
        Some(literal) => literal.split(']').next().unwrap_or_default(),
        None => authority.split(':').next().unwrap_or_default(),
    }
}

/// The one place the agent is configured, so a timeout change rebuilds it the
/// same way it was first built.
fn build_agent(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        // Keep non-2xx as ordinary responses so the X-YT-Error header can be
        // read off them; ureq would otherwise collapse them to a status code
        // and discard the cluster's explanation.
        .http_status_as_error(false)
        .build()
        .into()
}

/// Refuses an `https://` proxy when the crate was built without TLS.
///
/// Without this the failure surfaces as a connection error from `ureq`, which
/// says nothing about the missing feature. See the `tls` feature: it is off in
/// worker builds so that a binary which both launches and runs jobs can be
/// cross-compiled to musl without a C toolchain.
#[cfg(not(feature = "tls"))]
fn tls_unavailable(base: &str) -> Option<ClientError> {
    base.starts_with("https://").then(|| {
        ClientError::Config(format!(
            "{base} needs TLS, and this build has none: the `tls` feature of \
             ytsaurus-client is off. Enable it, or use an http:// proxy."
        ))
    })
}

#[cfg(feature = "tls")]
fn tls_unavailable(_base: &str) -> Option<ClientError> {
    None
}

/// The HTTP verb a command is sent with.
///
/// Which one a command wants is not a matter of taste. The
/// [HTTP proxy reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference)
/// gives the rule outright:
///
/// > If the command has an input data stream, then PUT. If the command is
/// > mutating, then POST. Otherwise GET.
///
/// Those three properties are declared per command in the cluster's own driver
/// registry, so the answer for a command this crate does not model is a lookup
/// rather than a guess: `write_table` takes a data stream and is a PUT, `create`
/// mutates and is a POST, `get` and `get_supported_features` do neither and are
/// GETs.
///
/// Public because [`Client::raw_command`](crate::Client::raw_command) cannot
/// choose for a command it has never heard of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    /// A command that neither mutates nor takes an input stream.
    Get,
    /// A mutating command with no input stream — most of API v4.
    Post,
    /// A command with an input data stream: `write_table`, `write_file`.
    Put,
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yson_build::map;

    fn transport(transaction: Option<&str>) -> Transport {
        let mut transport = Transport::new("http://localhost:8000", None, Duration::from_secs(1));
        transport.set_transaction(transaction.map(str::to_owned));
        transport
    }

    fn rendered(value: &YsonValue) -> String {
        to_string(value, YsonFormat::Text).expect("encodes")
    }

    #[test]
    fn a_bound_client_puts_every_command_in_its_transaction() {
        let params = map([("path", string("//tmp/out"))]);
        let stamped = transport(Some("3-5d231-10001-db88"))
            .in_transaction("write_table", &params)
            .expect("stamped");

        assert_eq!(
            rendered(&stamped),
            r#"{path="//tmp/out";transaction_id="3-5d231-10001-db88"}"#
        );
    }

    #[test]
    fn an_unbound_client_leaves_the_parameters_alone() {
        // `None` rather than a copy: this is every command's hot path.
        let params = map([("path", string("//tmp/out"))]);
        assert!(transport(None).in_transaction("get", &params).is_none());
    }

    #[test]
    fn a_command_that_names_a_transaction_keeps_the_one_it_named() {
        // `Transaction::commit` sends `commit_transaction` through a client
        // bound to that same transaction. Overwriting the parameter here would
        // still work — but on a *nested* transaction it would commit the child
        // instead of the parent the caller asked for.
        let params = map([("transaction_id", string("the-one-i-meant"))]);
        assert!(
            transport(Some("some-other-one"))
                .in_transaction("commit_transaction", &params)
                .is_none()
        );
    }

    #[test]
    fn a_scheduler_command_is_not_put_in_a_transaction() {
        // `Transaction` derefs to `Client`, so `tx.wait_for_operation(&id)` is
        // ordinary usage — and it, plus the three diagnostic calls it makes on
        // a failure, go to the scheduler, which has no transaction to put them
        // in. Stamping them survives only as long as the proxy ignores
        // parameters it does not know.
        let params = map([("operation_id", string("1-2-3-4"))]);
        let bound = transport(Some("3-5d231-10001-db88"));

        for command in [
            "get_operation",
            "list_jobs",
            "get_job_stderr",
            "abort_operation",
        ] {
            assert!(
                bound.in_transaction(command, &params).is_none(),
                "{command} was stamped with a transaction id"
            );
        }
    }

    /// `heavy_base` under the default rules — a name may not leave the domain.
    fn routed(configured: &str, host: &str) -> Option<String> {
        heavy_base(configured, host, false)
    }

    #[test]
    fn a_host_from_the_cluster_keeps_the_scheme_it_was_reached_by() {
        // `/hosts` answers with names, not URLs. A cluster reached over TLS
        // serves heavy commands over TLS; one reached over plain HTTP — a
        // local install, a tunnel — would refuse the handshake.
        assert_eq!(
            routed("https://cluster.example.net", "n0132-sas.example.net"),
            Some("https://n0132-sas.example.net".to_owned())
        );
        assert_eq!(
            routed("http://cluster.example.net", "n0132-sas.example.net"),
            Some("http://n0132-sas.example.net".to_owned())
        );
        // A port of its own travels with the name.
        assert_eq!(
            routed(
                "http://cluster.example.net:8000",
                "n0132-sas.example.net:9013"
            ),
            Some("http://n0132-sas.example.net:9013".to_owned())
        );
        // And the configured one carries through when the name has none, which
        // is the usual case: the coordinator lists bare host names unless its
        // `ShowPorts` config says otherwise, and a cluster reached at :8000 has
        // no reason to think its heavy proxies answer on 80.
        assert_eq!(
            routed("http://cluster.example.net:8000", "n0132-sas.example.net"),
            Some("http://n0132-sas.example.net:8000".to_owned())
        );
        assert_eq!(
            routed("https://cluster.example.net:8443", "n0132-sas.example.net"),
            Some("https://n0132-sas.example.net:8443".to_owned())
        );
    }

    #[test]
    fn a_hosts_answer_cannot_send_the_token_somewhere_else() {
        // The four rows of the table in #30, each measured against the client
        // before this check existed. The `/hosts` body decides where every
        // heavy command goes, and a heavy command carries the caller's OAuth
        // token — so on a plain-http base, forging this body is exactly as easy
        // as forging a `Location` header, which this client already refuses to
        // follow.

        // 1. The scheme downgrade. `http://n0132` from an `https://` client
        //    used to strip TLS and put the token on the wire in cleartext.
        assert_eq!(routed("https://cluster.example.net", "http://n0132"), None);
        assert_eq!(
            routed("https://cluster.example.net", "https://n0132.example.net"),
            None,
            "a name that spells its own scheme is not a name"
        );

        // 2. The userinfo trick. `real@evil` is a URL whose *host* is `evil`
        //    and whose reassuring half is thrown away by every parser.
        assert_eq!(
            routed(
                "https://cluster.example.net",
                "real.example.net@evil.example.net"
            ),
            None
        );

        // 3. A path, a query or a fragment: none of them belongs in a host
        //    name, and each is a way to make one read as another.
        for shape in [
            "n0132.example.net/../../evil",
            "n0132.example.net/api",
            "n0132.example.net?x=1",
            "n0132.example.net#f",
            "n0132 .example.net",
            "n0132.example.net\tn0133.example.net",
            "",
            "   ",
        ] {
            assert_eq!(
                routed("https://cluster.example.net", shape),
                None,
                "{shape:?} was accepted as a host name"
            );
        }

        // Padding around the name is normalised rather than refused, which is
        // what the blank-name filter used to do on its own — and what makes the
        // empty entries above empty.
        assert_eq!(
            routed("https://cluster.example.net", " \tn0132.example.net\n"),
            Some("https://n0132.example.net".to_owned())
        );

        // 4. Somewhere else entirely. The name has to sit under the domain of
        //    the address the caller chose.
        for elsewhere in [
            "n0132-sas.somewhere-else.net",
            "cluster.example.net.evil.com",
            "evil.com",
            "notexample.net",
        ] {
            assert_eq!(
                routed("https://cluster.example.net", elsewhere),
                None,
                "{elsewhere} was followed"
            );
        }
    }

    #[test]
    fn the_domain_a_discovered_host_has_to_share() {
        // The configured host itself, and anything under its parent domain.
        assert!(same_domain("cluster.example.net", "cluster.example.net"));
        assert!(same_domain("cluster.example.net", "n0132-sas.example.net"));
        assert!(same_domain(
            "cluster.example.net",
            "n0132-sas.cluster.example.net"
        ));
        assert!(same_domain("cluster.example.net", "example.net"));
        // Case is not part of a host name.
        assert!(same_domain("Cluster.Example.NET", "n0132-sas.example.net"));
        // The doc's own example shape: a one-label cluster name, with the
        // proxies under it.
        assert!(same_domain("cluster-name", "n0008-sas.cluster-name"));

        // Never below two labels, or a client pointed at `example.net` would
        // follow anything at all under `.net`.
        assert!(!same_domain("example.net", "n0132-sas.other.net"));
        assert!(same_domain("example.net", "n0132-sas.example.net"));

        // A literal address has no domain to share, so it admits only itself.
        assert!(same_domain("10.0.0.7", "10.0.0.7"));
        assert!(!same_domain("10.0.0.7", "10.0.0.8"));
        assert!(!same_domain("10.0.0.7", "n0132-sas.example.net"));
        assert!(!same_domain("cluster.example.net", "10.0.0.7"));

        // Suffix, not substring: the trap this rule exists to avoid.
        assert!(!same_domain("cluster.example.net", "evil-example.net"));
        assert!(!same_domain("cluster.example.net", "example.net.evil.com"));
    }

    #[test]
    fn an_installation_that_really_does_answer_elsewhere_can_say_so() {
        // The opt-in, for a cluster fronted by a vanity address or one whose
        // data proxies live under a separate zone. It relaxes the domain and
        // nothing else: the scheme still comes from the configured address, and
        // a name carrying furniture is still not a name.
        assert_eq!(
            heavy_base(
                "https://cluster.example.net",
                "n0132-sas.somewhere-else.net",
                true
            ),
            Some("https://n0132-sas.somewhere-else.net".to_owned())
        );
        assert_eq!(
            heavy_base("https://cluster.example.net", "http://n0132", true),
            None,
            "the escape hatch is about the domain, not about the scheme"
        );
        assert_eq!(
            heavy_base(
                "https://cluster.example.net",
                "real.example.net@evil.example.net",
                true
            ),
            None
        );
        // Nor about blank entries. With the domain rule relaxed, this is the
        // only thing standing between an empty name and the base URL
        // `https://:8000`.
        for blank in ["", "   ", "\t\n"] {
            assert_eq!(
                heavy_base("https://cluster.example.net:8000", blank, true),
                None,
                "{blank:?} was accepted as a host name"
            );
        }
    }

    #[test]
    fn a_bracketed_address_keeps_its_brackets_and_a_bare_one_is_refused() {
        // A bare IPv6 literal is not a valid URL authority — bracketed, it is —
        // and an unbracketed second colon means this is not one host and one
        // port.
        assert_eq!(
            heavy_base("http://[2a02:6b8::1]:8000", "[2a02:6b8::2]:9013", true),
            Some("http://[2a02:6b8::2]:9013".to_owned())
        );
        assert_eq!(
            heavy_base("http://[2a02:6b8::1]:8000", "[2a02:6b8::2]", true),
            Some("http://[2a02:6b8::2]:8000".to_owned()),
            "the configured port carries through a bracketed name too"
        );
        assert_eq!(
            heavy_base("http://cluster.example.net", "2a02:6b8::2", true),
            None
        );
        assert_eq!(
            heavy_base("http://cluster.example.net", "n0132:9013:9014", true),
            None
        );
    }

    #[test]
    fn a_routed_failure_names_the_host_it_went_to() {
        // The report a caller gets otherwise is about an address that appears
        // nowhere in their own code: the client chose it, from a list the
        // cluster gave it, and then said nothing about the choice.
        let failed = routed_to(
            ClientError::Http {
                command: "write_table".to_owned(),
                status: 502,
                body: String::new(),
            },
            "https://n0132-sas.example.net:9013",
        );

        assert!(
            failed
                .to_string()
                .starts_with("write_table at n0132-sas.example.net:9013:"),
            "{failed}"
        );

        // Every shape that carries a command gets the same treatment; the ones
        // that do not are left exactly as they were.
        let local = routed_to(
            ClientError::Config("no proxy".to_owned()),
            "https://n0132-sas.example.net:9013",
        );
        assert_eq!(local.to_string(), "no proxy");
    }

    #[test]
    fn a_cluster_on_loopback_is_not_asked_where_its_heavy_proxies_are() {
        // The address a proxy publishes for itself is its own. Behind a port
        // mapping or an SSH tunnel — which is what reaching a cluster at
        // `localhost` means — that address is not reachable from here, so
        // following it would send every upload nowhere.
        for local in [
            "http://localhost:8000",
            "http://LOCALHOST",
            "http://127.0.0.1:8000",
            "http://127.99.1.4",
            "https://[::1]:443",
            "http://0.0.0.0:8000",
        ] {
            assert!(is_local(local), "{local}");
        }

        for remote in [
            "https://cluster.example.net",
            "http://cluster.example.net:8000",
            "https://10.0.0.7",
            "https://[2a02:6b8::1]:443",
            // The one that matters most: a host merely *named* after the
            // local one is somebody else's machine.
            "https://localhost.example.net",
        ] {
            assert!(!is_local(remote), "{remote}");
        }
    }

    #[test]
    fn the_host_is_read_out_of_the_address_without_its_furniture() {
        assert_eq!(
            host_of("https://cluster.example.net/"),
            "cluster.example.net"
        );
        assert_eq!(
            host_of("http://cluster.example.net:8000"),
            "cluster.example.net"
        );
        assert_eq!(host_of("cluster.example.net:8000"), "cluster.example.net");
        // An IPv6 literal is bracketed and full of colons, which is why the
        // port is not simply everything after the first one.
        assert_eq!(host_of("http://[2a02:6b8::1]:8000"), "2a02:6b8::1");
        assert_eq!(
            host_of("http://user:pass@cluster.example.net"),
            "cluster.example.net"
        );
    }

    #[test]
    fn only_a_heavy_command_asks_where_to_go() {
        // A transport pointed at a host it cannot reach: if a light command
        // consulted `/hosts`, this would try to and fail rather than answer
        // instantly with the configured address.
        let transport = Transport::new(
            "http://cluster.invalid:8000",
            None,
            Duration::from_millis(50),
        );

        for light in [
            Repeatable::Freely,
            Repeatable::WithMutationId,
            Repeatable::Never,
        ] {
            assert_eq!(
                transport.base_for(light),
                "http://cluster.invalid:8000",
                "{light:?} went looking for a heavy proxy"
            );
        }
    }

    #[test]
    fn starting_an_operation_still_joins_the_transaction() {
        // The exception that makes the list a list rather than "anything to do
        // with operations": an operation can run inside a transaction, and that
        // is what keeps its output invisible until the launcher commits.
        let params = map([("operation_type", string("map"))]);
        assert!(
            transport(Some("3-5d231-10001-db88"))
                .in_transaction("start_operation", &params)
                .is_some()
        );
    }
}
