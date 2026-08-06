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

use std::time::Duration;

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

/// A PEM file of root certificates to verify the cluster against, instead of
/// the Mozilla bundle `ureq` compiles in. See [`root_certs`].
///
/// Behind the feature like everything else it leads to: a build with no TLS in
/// it has no handshake to configure, and reads the variable no more than it
/// opens a socket for `https://`.
#[cfg(feature = "tls")]
const CA_BUNDLE: &str = "YT_CA_BUNDLE";

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

/// A configured connection to one cluster.
#[derive(Clone)]
pub(crate) struct Transport {
    agent: ureq::Agent,
    base: String,
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
    /// Why the TLS configuration this build was asked for could not be
    /// assembled — a `YT_CA_BUNDLE` that names nothing readable, or nothing
    /// that parsed. Carried rather than reported, because an agent is built
    /// before there is a request to fail; see [`Transport::unusable`].
    ///
    /// A `String` and not a [`ClientError`] because a `Transport` is `Clone`
    /// and an error holding an `io::Error` is not.
    tls_refused: Option<String>,
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

        let (agent, tls_refused) = build_agent(timeout);

        let mut transport = Self {
            agent,
            base,
            token,
            retries,
            timeout,
            transaction: None,
            trace: None,
            tracestate: None,
            caller: Vec::new(),
            tls_refused,
        };
        transport.render_caller_headers();
        transport
    }

    pub(crate) fn set_retries(&mut self, policy: RetryPolicy) {
        self.retries = policy;
    }

    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        let (agent, tls_refused) = build_agent(timeout);
        self.agent = agent;
        self.tls_refused = tls_refused;
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
    /// deduplicates, and a heavy command is sent once whatever the policy says.
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

        crate::retry::run(self.retries, repeatable, command, |is_retry| {
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
                    self.send(method, command, &tagged, &payload)
                }
                None => self.send(method, command, parameters, &payload),
            }
        })
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

    /// One attempt, read into memory.
    fn send(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        payload: &Payload<'_>,
    ) -> Result<Vec<u8>> {
        let mut bytes: &[u8] = match payload {
            Payload::None => &[],
            Payload::Bytes(bytes) => bytes,
        };
        let mut response = self.dispatch(method, command, parameters, bytes.as_body(), false)?;

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
    /// Sent once, never retried — this is a heavy command, and the retry rules
    /// say so.
    pub(crate) fn open(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
    ) -> Result<ureq::Body> {
        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        // Through `retry::run` like every other command, with `Repeatable::Never`
        // doing the sending-once: it caps the loop at one attempt and never
        // reaches the retry announcement, so this needs no second seam of its
        // own to be timed and named. The span closes when the headers arrive —
        // the reader handed back is read after that, at the caller's pace.
        crate::retry::run(self.retries, Repeatable::Never, command, |_| {
            let response = self.dispatch(method, command, parameters, SendBody::none(), true)?;
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
        })
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

        // `Repeatable::Never` is the sending-once, as in `open`: one attempt,
        // no announcement, and the span comes from the seam every other command
        // already goes through. Unlike `open` it covers the whole transfer —
        // the body is read here, as it goes.
        crate::retry::run(self.retries, Repeatable::Never, command, |_| {
            let mut response = self.dispatch(
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
        })
    }

    /// Fetches a path that is not an API v4 command.
    ///
    /// `/hosts` is the only one, and it is not a command — but it wants
    /// everything a command gets: the token, the global timeout, the retry
    /// policy, and the guard that turns a proxy this build cannot reach over
    /// TLS into an explanation rather than a connection error. Building a bare
    /// `ureq` request here instead is how it came to miss all four.
    pub(crate) fn fetch(&self, path: &str, what: &str) -> Result<String> {
        if let Some(error) = self.unusable() {
            return Err(error);
        }

        let url = format!("{}{path}", self.base);

        crate::retry::run(self.retries, Repeatable::Freely, what, |_| {
            let mut response = with_headers!(self.agent.get(&url), &self.caller)
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
    fn dispatch(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        body: SendBody<'_>,
        streaming: bool,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        if let Some(error) = self.unusable() {
            return Err(error);
        }

        let url = format!("{}/api/v4/{command}", self.base);

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

    /// Why no request can be sent at all, if that was settled before any was.
    ///
    /// Two reasons, and both are about TLS rather than about the network: the
    /// crate was built without the `tls` feature and the proxy is `https://`,
    /// or [`CA_BUNDLE`] named something that could not be turned into root
    /// certificates. Reported here so the caller reads a sentence naming the
    /// cause instead of a handshake failure that explains nothing.
    ///
    /// A refused bundle only bites an `https://` proxy: over plain HTTP there
    /// is no handshake for it to have configured, and a stale variable left in
    /// an environment whose cluster is local costs nothing.
    fn unusable(&self) -> Option<ClientError> {
        if let Some(error) = tls_unavailable(&self.base) {
            return Some(error);
        }

        match &self.tls_refused {
            Some(why) if self.base.starts_with("https://") => {
                Some(ClientError::Config(why.clone()))
            }
            _ => None,
        }
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
}

/// The one place the agent is configured, so a timeout change rebuilds it the
/// same way it was first built.
///
/// Hands back whatever it could not honour instead of failing: this runs while
/// a client is being constructed, where there is no request to fail and no
/// `Result` to fail into. See [`Transport::unusable`], which is where the
/// refusal is finally spoken.
fn build_agent(timeout: Duration) -> (ureq::Agent, Option<String>) {
    #[allow(unused_mut)]
    let mut builder = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        // Keep non-2xx as ordinary responses so the X-YT-Error header can be
        // read off them; ureq would otherwise collapse them to a status code
        // and discard the cluster's explanation.
        .http_status_as_error(false);

    #[allow(unused_mut)]
    let mut refused = None;

    #[cfg(feature = "tls")]
    match root_certs() {
        Ok(Some(tls)) => builder = builder.tls_config(tls),
        Ok(None) => {}
        Err(why) => refused = Some(why),
    }

    (builder.build().into(), refused)
}

/// Which roots the cluster's certificate is verified against.
///
/// `None` leaves `ureq`'s own default, the Mozilla bundle compiled in through
/// `webpki-roots`. That is what a cluster with a publicly trusted certificate
/// wants, and it stays the default here: a client may well run outside the
/// network it is talking to, where the machine's own trust store is the less
/// trustworthy of the two.
///
/// An on-premises installation behind a corporate CA is the case that needs
/// changing, and there are two ways to do it — the same two the `yt` CLI and
/// the Go SDK offer:
///
/// - **[`CA_BUNDLE`]** names a PEM file. No dependency at all, and nothing to
///   rebuild.
/// - the **`platform-verifier`** feature trusts whatever the operating system
///   trusts, so a machine where `curl` already reaches the cluster needs
///   nothing set.
///
/// The bundle wins when both are there. It is the more specific answer, and
/// the one the caller went out of their way to give.
#[cfg(feature = "tls")]
fn root_certs() -> Result<Option<ureq::tls::TlsConfig>, String> {
    roots_for(std::env::var(CA_BUNDLE).ok().as_deref())
}

/// The choice itself, split from the lookup so it can be tested without writing
/// the process environment — which is global, and in edition 2024 unsafe to
/// write.
#[cfg(feature = "tls")]
fn roots_for(named: Option<&str>) -> Result<Option<ureq::tls::TlsConfig>, String> {
    match named {
        // An empty variable is not a bundle: `YT_CA_BUNDLE=` in a shell profile
        // means "I turned that off", not "trust a file called nothing".
        Some(path) if !path.trim().is_empty() => bundle(path).map(Some),
        _ => Ok(platform_roots()),
    }
}

/// What to trust when nothing named a bundle.
///
/// `None` is `ureq`'s own default and this crate's: the Mozilla roots.
#[cfg(feature = "tls")]
fn platform_roots() -> Option<ureq::tls::TlsConfig> {
    #[cfg(feature = "platform-verifier")]
    {
        return Some(
            ureq::tls::TlsConfig::builder()
                .root_certs(ureq::tls::RootCerts::PlatformVerifier)
                .build(),
        );
    }

    #[allow(unreachable_code)]
    None
}

/// Reads a PEM file into the roots to trust, or says why it could not.
///
/// Split from [`roots_for`] so the reading and the refusal can be tested
/// against a file of their own.
///
/// **A bundle that yields no certificates is refused, not ignored.** Falling
/// back to the compiled-in roots would answer a deliberate request with the
/// very handshake failure this variable exists to end — and it would do it
/// silently, naming neither the file nor the reason. The same goes for a file
/// that cannot be read: `YT_CA_BUNDLE` pointing at a typo is a mistake worth
/// hearing about at the first request rather than at the first `UnknownIssuer`.
#[cfg(feature = "tls")]
fn bundle(path: &str) -> Result<ureq::tls::TlsConfig, String> {
    use ureq::tls::{Certificate, PemItem, RootCerts, TlsConfig, parse_pem};

    let pem = std::fs::read(path)
        .map_err(|e| format!("{CA_BUNDLE} names {path}, which could not be read: {e}"))?;

    let certs: Vec<Certificate<'static>> = parse_pem(&pem)
        .filter_map(|item| match item {
            Ok(PemItem::Certificate(cert)) => Some(cert),
            _ => None,
        })
        .collect();

    if certs.is_empty() {
        return Err(format!(
            "{CA_BUNDLE} names {path}, which holds no PEM certificates: expected at least one \
             -----BEGIN CERTIFICATE----- block"
        ));
    }

    Ok(TlsConfig::builder()
        .root_certs(RootCerts::new_with_certs(&certs))
        .build())
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

    /// A real self-signed CA, generated for these tests with `openssl req
    /// -x509`. A made-up base64 blob would parse just as well — the PEM reader
    /// only splits sections — but then the fixture would prove nothing about
    /// the shape of the thing an installation would actually hand us.
    #[cfg(feature = "tls")]
    const CA_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIDHTCCAgWgAwIBAgIUf6mwbBS7JGIyvPDkCpiBRHp914cwDQYJKoZIhvcNAQEL
BQAwHjEcMBoGA1UEAwwTeXRzYXVydXMtcnMgdGVzdCBDQTAeFw0yNjA4MDYyMDM4
MTJaFw00NjA4MDEyMDM4MTJaMB4xHDAaBgNVBAMME3l0c2F1cnVzLXJzIHRlc3Qg
Q0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDqPTrcPPGiHlv4aV8v
AdrNtzvlhHciQbd7Pz0tLCmn8OGCjwt3Q/V22h6HSWijIleHPqn6bTSMYfPGAxRe
mAiqSsMLpM+GYWZAg8Kz7VSsK4f0s4dW6i82QYFVk/+04N/0RUJ3A9RTloxSl8+a
HT5MF2x4LGr1eBgpz4UEsC5cJtkzA8OCM2a2TtNiuo/PtKzZx2TuvEk+Ub5Gn/lt
tZn8m9z6o8n51D3vEIfHfXPyFre2+cz+Ao680kc0KP8PWlG89mhvMZ2VYGJG2T/Z
6Ddpj7aXM+jKCCjBTLMkLYaIuNO9//72kmBYsVgaBAMNYMBaBqQX1TOjwxbiBbv5
fbJnAgMBAAGjUzBRMB0GA1UdDgQWBBSniLAZD6er7hHpwg12hIX57PHb2TAfBgNV
HSMEGDAWgBSniLAZD6er7hHpwg12hIX57PHb2TAPBgNVHRMBAf8EBTADAQH/MA0G
CSqGSIb3DQEBCwUAA4IBAQBsR5VKflwEwRTNY1dobAWKS6kLTszpRFlQN2qBMTv+
NhS0i7mrNUzKadZkmlQuOMIhZl6gR4mB0XVPgkJKJ+ch8SfuaBW3Po4dTdrKfB6K
CgCTM54UB3QQAlAjpVhLCS7aCT8hgKEX1+1OD1SmBNQ/Jj9OOoKxVkq9prjSzILW
pXeT/OKKRqZ7tjG2jh55XPgE+GWLCfo3VsPqcleAoxQEWATryTF4fwKI9tuAgJ8p
pN1M6UxJFatwx23InC/jVPR6wBu5h1SyCjIxuW/j8pgriTm8wR3XaTly49j6VQDH
8KGhyM+0UsZEWeI05Uq9c/Vs5TlJAcnvwJwxJqREhlHY
-----END CERTIFICATE-----
";

    /// The key half of a pair. A file holding only this is the mistake the
    /// empty-parse refusal is for: it is PEM, it is a well-formed section, and
    /// it contains no root to trust.
    #[cfg(feature = "tls")]
    const KEY_PEM: &str = "\
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgt4eMMaSBwIKAgwrT
zzKo64LyF0YMvm3I61+EK3DDRDmhRANCAAS3XrEb3d5QdjQGGuAny4phX9xstUpp
B7b7J0xB2R7nPBn3+4PRz/35FJrHFmNkKD47D6ZMldYk7ykxNLNBGzIU
-----END PRIVATE KEY-----
";

    /// A file in the temp directory, removed when the test is done with it.
    ///
    /// `YT_CA_BUNDLE` names a path, so the thing under test reads one; there is
    /// nothing to inject. The name carries a
    /// [`unique::word`](crate::unique::word) because the test binary runs its
    /// tests in threads, and two of these writing one path would be two tests
    /// reading each other's bundle.
    #[cfg(feature = "tls")]
    struct TempPem(std::path::PathBuf);

    #[cfg(feature = "tls")]
    impl TempPem {
        fn new(contents: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("ytsaurus-rs-ca-{:x}.pem", crate::unique::word(0)));
            std::fs::write(&path, contents).expect("writes the bundle");
            Self(path)
        }

        fn path(&self) -> &str {
            self.0.to_str().expect("a utf-8 temp path")
        }
    }

    #[cfg(feature = "tls")]
    impl Drop for TempPem {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).ok();
        }
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_bundle_becomes_the_roots_and_its_private_key_is_left_alone() {
        // Two certificates and a key in one file: the shape of
        // `/etc/ssl/certs/ca-certificates.crt` next to a deployment that keeps
        // everything in one PEM. Only the certificates are roots.
        let file = TempPem::new(&format!("{CA_PEM}{KEY_PEM}{CA_PEM}"));
        let config = bundle(file.path()).expect("a bundle with certificates in it");

        match config.root_certs() {
            ureq::tls::RootCerts::Specific(certs) => assert_eq!(certs.len(), 2),
            other => panic!("the bundle did not become the roots: {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_bundle_that_parses_to_nothing_is_refused() {
        // Not "and then we quietly used Mozilla's roots": that answers a
        // deliberate request with `UnknownIssuer`, which is the failure the
        // variable exists to end, and names neither the file nor the reason.
        for (what, contents) in [
            ("a key and no certificate", KEY_PEM),
            ("an empty file", ""),
            ("the cluster's HTML login page", "<html>Sign in</html>\n"),
        ] {
            let file = TempPem::new(contents);
            let refusal = bundle(file.path()).expect_err(what);

            assert!(refusal.contains(CA_BUNDLE), "{what}: {refusal}");
            assert!(refusal.contains(file.path()), "{what}: {refusal}");
            assert!(refusal.contains("no PEM certificates"), "{what}: {refusal}");
        }
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_bundle_beats_whatever_the_build_would_have_trusted() {
        // The precedence, and the whole reason the feature is not simply
        // "trust the OS": a bundle is the more specific answer and the one the
        // caller went out of their way to give. With `platform-verifier` off
        // this says the bundle beats the Mozilla roots; with it on, that it
        // beats the platform verifier too, which is the case worth pinning.
        let file = TempPem::new(CA_PEM);
        let chosen = roots_for(Some(file.path()))
            .expect("a readable bundle")
            .expect("some roots");

        assert!(
            matches!(chosen.root_certs(), ureq::tls::RootCerts::Specific(_)),
            "{:?}",
            chosen.root_certs()
        );
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_variable_that_names_nothing_is_not_a_bundle() {
        // `export YT_CA_BUNDLE=` is how a shell profile turns one off. Read as
        // a path it would be a refusal on every request.
        for named in [None, Some(""), Some("   ")] {
            let chosen = roots_for(named).expect("no bundle was named");
            let roots = chosen.as_ref().map(ureq::tls::TlsConfig::root_certs);

            // With `platform-verifier` on, an unset variable is what asks for
            // the operating system's own trust store.
            #[cfg(feature = "platform-verifier")]
            assert!(
                matches!(roots, Some(ureq::tls::RootCerts::PlatformVerifier)),
                "{roots:?}"
            );

            // Without it, nothing is configured at all and `ureq` keeps the
            // Mozilla bundle it compiles in.
            #[cfg(not(feature = "platform-verifier"))]
            assert!(roots.is_none(), "{roots:?}");
        }
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_bundle_that_cannot_be_read_is_refused_rather_than_ignored() {
        let missing = std::env::temp_dir().join("ytsaurus-rs-no-such-bundle.pem");
        let refusal =
            bundle(missing.to_str().expect("a utf-8 temp path")).expect_err("nothing to read");

        assert!(refusal.contains(CA_BUNDLE), "{refusal}");
        assert!(refusal.contains("could not be read"), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_refused_bundle_is_reported_instead_of_the_first_request() {
        // The refusal is discovered while the agent is being built, where
        // there is nothing to fail; it waits here for something that is.
        let mut transport =
            Transport::new("https://cluster.example.net", None, Duration::from_secs(1));
        transport.tls_refused = Some("YT_CA_BUNDLE names /etc/no-such-file".to_owned());

        let error = transport.unusable().expect("a refusal");
        assert!(matches!(error, ClientError::Config(_)), "{error}");
        assert!(error.to_string().contains("YT_CA_BUNDLE"), "{error}");
    }

    #[test]
    fn a_refused_bundle_does_not_stop_a_cluster_reached_over_plain_http() {
        // No handshake, so nothing the bundle would have configured. A stale
        // variable in a shell profile is not a reason to refuse a local
        // cluster.
        let mut transport = transport(None);
        transport.tls_refused = Some("YT_CA_BUNDLE names /etc/no-such-file".to_owned());

        assert!(transport.unusable().is_none());
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
