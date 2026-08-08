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

use std::path::Path;
use std::time::{Duration, Instant};

use ureq::SendBody;
use ureq::http::HeaderMap;
use ytsaurus_yson::{YsonFormat, YsonValue, to_string};

use crate::error::{ClientError, RedirectRefusal, Result, truncate};
use crate::retry::{MutationId, Repeatable, RetryPolicy};
use crate::yson_build::{boolean, insert, string};

const HEADER_FORMAT: &str = "X-YT-Header-Format";
const PARAMETERS: &str = "X-YT-Parameters";
const ERROR: &str = "X-YT-Error";
/// Where a redirect points. Read by this client rather than by `ureq` — see
/// [`Transport::redirect`].
const LOCATION: &str = "Location";
/// How many redirects one request may follow before the chain is called a loop.
///
/// `ureq`'s own default, kept so that turning the following over to this client
/// changed the policy and not the numbers.
const MAX_REDIRECTS: usize = 10;

/// The commands that carry a data stream, and so belong on a heavy proxy.
///
/// The
/// [command reference](https://ytsaurus.tech/docs/en/api/commands) draws the
/// line for us — *"light commands only transmit command parameters within a
/// query, but heavy commands write or read the data stream"* — and marks each
/// of `read_table`, `write_table`, `read_file`, `write_file` and
/// `read_blob_table` **Heavy**. `get_job_input` and `get_job_stderr` are here
/// on the same definition rather than on rows of their own: their answer *is*
/// the data stream, which is why this crate reads the first through
/// [`Transport::open`] and why `get_job_stderr` hands back bytes rather than
/// text.
///
/// The list is what the **cluster** declares heavy, not what this crate
/// happens to model: `read_file` and `read_blob_table` have no method here and
/// are reachable through
/// [`Client::raw_command_streaming`](crate::Client::raw_command_streaming) —
/// which is how the documentation on that method reads a file — so leaving
/// them out would take the advice away from exactly the caller who went to the
/// trouble of streaming.
///
/// Used for one thing only — whether a refused redirect is told to go to a
/// heavy proxy. A command sent through
/// [`Client::raw_command`](crate::Client::raw_command) that is heavy and not
/// listed here loses the advice, not the refusal.
///
/// **Merge marker.** `Repeatable` grows a `Heavy` variant on
/// `feature/heavy-proxy-routing` (#38), which encodes the cluster's `isHeavy`
/// bit for routing. The two lists say the same thing about the same commands
/// and are written down twice, so **whoever merges that branch must check this
/// list against every `Repeatable::Heavy` call site** and reconcile the two —
/// a command routed to a heavy proxy but missing here is refused a redirect
/// with `heavy: false`, and told nothing it can act on. There is no test to
/// catch it from this side: `Repeatable::Heavy` does not exist on this branch.
const HEAVY: &[&str] = &[
    "read_table",
    "write_table",
    "read_file",
    "write_file",
    "read_blob_table",
    "get_job_input",
    "get_job_stderr",
];
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

/// The most a root bundle may weigh.
///
/// Mozilla's own — `/etc/ssl/certs/ca-certificates.crt`, the largest thing
/// anyone is likely to name here — is about 200 KB, so this is three orders of
/// magnitude of headroom. It exists because the read has no other bound: the
/// client's global timeout covers requests, not files, and
/// [`Client::new`](crate::Client::new) is infallible, so a `YT_CA_BUNDLE`
/// pointing at something enormous by accident would be paid for in memory
/// before anyone could be told. Measured on a 512 MB file: 18.7 s and 1.27 GB
/// of resident memory, for a bundle that was never going to parse.
#[cfg(feature = "tls")]
const MAX_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;

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

/// The request body, in a form one request can send more than once.
///
/// `ureq`'s [`SendBody`] is one-shot by construction — it may be a reader that
/// has already been drained — so following a redirect needs the body kept as
/// something that can produce a fresh `SendBody` per hop.
///
/// Two questions are asked of it when a `3xx` arrives, and they are not the
/// same question. **Can this request be sent again?** — no, if it is a reader
/// ([`Outgoing::replayable`], [`RedirectRefusal::Body`]). **Would sending it
/// again hand someone data?** — yes, if there are bytes in it
/// ([`Outgoing::carries_data`], [`RedirectRefusal::Payload`]). A body of length
/// zero answers no to the second and yes to the first, which is why an empty
/// slice is not the same thing as a table full of rows.
enum Outgoing<'a> {
    /// No body at all — neither `Content-Length` nor `Transfer-Encoding`.
    ///
    /// [`Transport::open`]'s request, which is a `GET` for everything this
    /// crate models and reaches `ureq`'s body-carrying builder only through
    /// [`Client::raw_command_streaming`](crate::Client::raw_command_streaming).
    /// Distinct from `Bytes(&[])`, which is a body of length zero: what goes
    /// on the wire differs, and this is the one that always sent nothing.
    Empty,
    /// Bytes held in memory, and so sent again to wherever a redirect points.
    ///
    /// An empty slice belongs here rather than in [`Outgoing::Empty`]: most of
    /// API v4 carries its parameters in `X-YT-Parameters` and its payload
    /// nowhere, and such a command has always gone out as `Content-Length: 0`.
    /// A body of length zero is still a body a `GET` could not have carried —
    /// and still nothing a redirect can lose or give away.
    Bytes(&'a [u8]),
    /// A body read as it is sent — [`Client::write_table_rows`](crate::Client::write_table_rows)
    /// and every [`Client::raw_command_upload`](crate::Client::raw_command_upload).
    ///
    /// A reader cannot be rewound, and by the time a `3xx` arrives some of it
    /// has already gone out, so a redirect on one of these is refused.
    Stream(&'a mut dyn std::io::Read),
}

impl Outgoing<'_> {
    /// Whether a redirect on this request could send the same request again.
    fn replayable(&self) -> bool {
        !matches!(self, Outgoing::Stream(_))
    }

    /// Whether there are bytes here that a redirect would be giving away.
    ///
    /// A body of length zero is not data. `Content-Length: 0` is what a `POST
    /// create` sends and what a `GET` does not send at all; neither has
    /// anything in it that a caller would mind another host receiving, so
    /// neither is a reason to refuse a hop the credentials rule allows.
    fn carries_data(&self) -> bool {
        match self {
            Outgoing::Empty => false,
            Outgoing::Bytes(bytes) => !bytes.is_empty(),
            Outgoing::Stream(_) => true,
        }
    }
}

/// A configured connection to one cluster.
#[derive(Clone)]
pub(crate) struct Transport {
    agent: ureq::Agent,
    base: String,
    token: Option<String>,
    retries: RetryPolicy,
    /// End-to-end limit for buffered commands — one budget per attempt, shared
    /// out between the redirect hops that attempt makes. Per-phase limit for
    /// streaming ones. See [`Transport::dispatch`].
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

        let (agent, tls_refused) = build_agent(timeout, configured_bundle());

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
        // Through `build_agent` rather than by editing the config in place:
        // this is the one place the agent is built twice, and so the one place
        // the redirect policy could be dropped by a caller doing nothing more
        // suspicious than `with_timeout`.
        let (agent, tls_refused) = build_agent(timeout, configured_bundle());
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
        // Held as bytes rather than as a `SendBody`, so a redirect can send the
        // same request again — see [`Outgoing`]. `None` is an empty slice and
        // not `Outgoing::Empty`, which is what it has always been on the wire:
        // `Content-Length: 0`.
        let body = match payload {
            Payload::None => Outgoing::Bytes(&[]),
            Payload::Bytes(bytes) => Outgoing::Bytes(bytes),
        };
        let mut response = self.dispatch(method, command, parameters, body, false)?;

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
            let response = self.dispatch(method, command, parameters, Outgoing::Empty, true)?;
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
                Outgoing::Stream(&mut *rows),
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

        let first = format!("{}{path}", self.base);

        crate::retry::run(self.retries, Repeatable::Freely, what, |_| {
            let mut url = first.clone();
            let mut hops = 0;
            // One budget for the lookup, shared out between its hops — as in
            // `dispatch`, and for the same reason.
            let deadline = self.deadline(false);

            // `/hosts` is where a redirect would be most tempting to follow —
            // it is a lookup, not a command — and it carries the token like
            // everything else does. It goes through the same rules as a
            // command, and for the same reasons. The loop ends because
            // `redirect` refuses past [`MAX_REDIRECTS`], or sooner because the
            // deadline runs out.
            let mut response = loop {
                let left = remaining(deadline, what)?;
                let response =
                    with_headers!(self.scoped(self.agent.get(&url), false, left), &self.caller)
                        .call()
                        .map_err(|e| ClientError::Transport {
                            command: what.to_owned(),
                            source: Box::new(e),
                        })?;

                match self.redirect(what, &response, &url, &Outgoing::Empty, hops)? {
                    Some(next) => {
                        if let Some(error) = tls_unavailable(&next) {
                            return Err(error);
                        }
                        url = next;
                        hops += 1;
                    }
                    None => break response,
                }
            };

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
    /// **A buffered command's timeout is end to end across the redirects too.**
    /// The deadline is taken once, here, and every hop is given what is left of
    /// it rather than a fresh copy — which is what `ureq` did while it was the
    /// one following them, `Timeout::Global` covering the whole chain. Handing
    /// each hop the full timeout instead would make the real limit
    /// `(MAX_REDIRECTS + 1)` times the one the caller asked for: eleven times
    /// two minutes for a balancer that points at itself, on a call that
    /// promised two.
    fn dispatch(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        mut body: Outgoing<'_>,
        streaming: bool,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        if let Some(error) = self.unusable() {
            return Err(error);
        }

        let mut url = format!("{}/api/v4/{command}", self.base);

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

        // Taken once for the attempt, not once per hop. See the note above.
        let deadline = self.deadline(streaming);
        // The loop ends because `redirect` refuses past [`MAX_REDIRECTS`], and
        // sooner than that because the deadline runs out.
        let mut hops = 0;

        loop {
            let left = remaining(deadline, command)?;

            // The method survives the hop, whatever the digit: `307` and `308`
            // require it, and an API v4 command's verb belongs to the command
            // — the reference derives it from whether the command mutates and
            // whether it has an input stream, neither of which a `Location`
            // changes. So does the body, when there is one that can be sent
            // again; `redirect` refuses the hop when there is not.
            let sent = match method {
                // A GET carries no body in `ureq`'s type system, which is also
                // true of every command this client sends as one.
                Method::Get => with_headers!(
                    self.scoped(self.agent.get(&url), streaming, left),
                    &headers,
                    &self.caller
                )
                .call(),
                // `post` and `put` build the same request type, so the body is
                // chosen once for both. A fresh `SendBody` per hop rather than
                // one taken out of an `Option`: that is what lets the same
                // request go out twice, and `SendBody` cannot be reused.
                Method::Post | Method::Put => {
                    let request = with_headers!(
                        self.scoped(
                            match method {
                                Method::Put => self.agent.put(&url),
                                _ => self.agent.post(&url),
                            },
                            streaming,
                            left
                        ),
                        &headers,
                        &self.caller
                    );

                    match &mut body {
                        Outgoing::Empty => request.send(SendBody::none()),
                        Outgoing::Bytes(bytes) => request.send(*bytes),
                        Outgoing::Stream(reader) => {
                            request.send(SendBody::from_reader(&mut **reader))
                        }
                    }
                }
            };

            let response = sent.map_err(|e| ClientError::Transport {
                command: command.to_owned(),
                source: Box::new(e),
            })?;

            // Before the cluster's own error, because a redirect is not the
            // cluster reporting a failure — it is this client deciding where a
            // request goes, which is a fact no `X-YT-Error` could carry.
            if let Some(next) = self.redirect(command, &response, &url, &body, hops)? {
                // The same guard the first address got: a same-origin redirect
                // cannot change the scheme, but nothing here assumes that.
                if let Some(error) = tls_unavailable(&next) {
                    return Err(error);
                }
                url = next;
                hops += 1;
                continue;
            }

            // The cluster's own error, which is far more useful than the status.
            if let Some(raw) = header_value(response.headers(), ERROR) {
                return Err(ClientError::from_yt_error(
                    command,
                    response.status().as_u16(),
                    &raw,
                ));
            }

            return Ok(response);
        }
    }

    /// What becomes of a `3xx`: `Ok(Some(url))` to go there, `Ok(None)` to
    /// treat the response as an ordinary one, `Err` to refuse.
    ///
    /// A control proxy does not refuse a heavy *read*. It answers `307
    /// Temporary Redirect` naming a data proxy on a **different host** — the
    /// [HTTP proxy reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#return_codes)
    /// lists that code as *"Redirecting heavy queries from light to heavy
    /// proxies"*:
    ///
    /// ```text
    /// HTTP/1.1 307 Temporary Redirect
    /// Location: http://data-proxy-01.example.net:80/api/v4/read_table?path=…
    /// ```
    ///
    /// `ureq` would follow that by default and, also by default
    /// (`RedirectAuthHeaders::Never`), drop the `Authorization` header on the
    /// way. The second request therefore arrives unauthenticated and the
    /// cluster answers `Client is missing credentials` — about a token that is
    /// perfectly valid. The user then checks the token, the token file and
    /// their permissions, none of which is at fault.
    ///
    /// **`redirect_auth_headers(RedirectAuthHeaders::SameHost)` is not the
    /// answer**, though it is the first thing that suggests itself and reads
    /// like the setting this was missing. It re-attaches the header only when
    /// the redirect stays on the same host and under https; this redirect is
    /// deliberately cross-host, control proxy to data proxy, so the header
    /// would be dropped exactly as before — and the next reader would conclude
    /// the problem lay somewhere else entirely.
    ///
    /// So the rules are here instead, and there are four of them. Three say
    /// what a redirect must not take with it across an origin, and the fourth
    /// says when a route stops being one.
    ///
    /// **A redirect that leaves the origin is refused when the request carries
    /// credentials.** That leaves the honest choice — re-attach for the host
    /// the *proxy* named, or go nowhere — settled at "go nowhere". A
    /// `Location` arrives mid-flight, on a request addressed somewhere else;
    /// asking `/hosts` and addressing the answer is a question this client put
    /// deliberately, before the request was built. Same origin, and it is
    /// followed: nothing new learns the token by it, and a balancer
    /// canonicalising its own host would otherwise break every command.
    ///
    /// **A redirect on a body this client cannot send again is refused.** Not
    /// on a body: on an *unrepeatable* one, and wherever it points. Following a
    /// redirect here means sending the same request to the address it named —
    /// same method, same payload — which is what `307` and `308` require and
    /// what an API v4 command needs whatever the digit, since a command's verb
    /// is a property of the command. A payload held as bytes goes out again and
    /// nothing is lost. A payload that is a *reader* — [`Transport::upload`],
    /// so `write_table` from an iterator and every `raw_command_upload` — has
    /// already begun to drain into the first request by the time the `3xx`
    /// arrives, and cannot be rewound. That one is refused, with or without a
    /// token: dropping the rows and reporting the answer to an empty request
    /// is how a write that wrote nothing comes back looking like one that
    /// worked.
    ///
    /// **A redirect that leaves the origin is refused when the request carries
    /// data**, whether or not there is a token. This is the credentials rule
    /// again, about the other thing a caller chooses a host for: a token is not
    /// the only thing worth not handing to a host nobody named, and a table's
    /// rows are the caller's own. Sending them on would answer a header that
    /// arrived mid-flight with the contents of the request. A body of length
    /// zero is not data — `Content-Length: 0` gives nothing away — so a
    /// bodiless `POST` still goes wherever the credentials rule lets it.
    ///
    /// **A chain that does not end is refused.** [`MAX_REDIRECTS`] hops, then
    /// it is a loop rather than a route.
    ///
    /// The order is the order of what a caller most needs told. Credentials
    /// first, because a leaked token is the worst outcome and a refused one is
    /// the confusing one. Then the unrepeatable body, because that is refused
    /// at any address and so is the more general fact about the request. Then
    /// the data crossing an origin, which is the one a same-origin balancer
    /// never triggers.
    ///
    /// The deliberate way to reach a data proxy is to ask the cluster for one
    /// — `/hosts`, [`Client::heavy_proxy`](crate::Client::heavy_proxy) — and
    /// address it on purpose. Routing heavy commands there is what removes the
    /// redirect altogether; this is the half that holds when something is
    /// redirected anyway.
    ///
    /// A `3xx` that names no `Location`, or one this client cannot resolve
    /// into an address, is not a redirect that was refused — it is a proxy
    /// answering something odd, and it stays an ordinary
    /// [`ClientError::Http`].
    fn redirect(
        &self,
        command: &str,
        response: &ureq::http::Response<ureq::Body>,
        request_url: &str,
        body: &Outgoing<'_>,
        hops: usize,
    ) -> Result<Option<String>> {
        let status = response.status();
        if !status.is_redirection() {
            return Ok(None);
        }

        let Some(location) = header_value(response.headers(), LOCATION) else {
            return Ok(None);
        };
        // Resolved before anything is decided about it, so the origin
        // comparison has an origin to work with and the message names a host
        // even when the proxy sent `Location: /api/v4/…`.
        let Some(target) = resolve(request_url, &location) else {
            return Ok(None);
        };

        let refused = |refusal| {
            Err(ClientError::Redirected {
                command: command.to_owned(),
                status: status.as_u16(),
                location: target.clone(),
                refusal,
                heavy: HEAVY.contains(&command),
            })
        };

        // Computed once: both origin rules ask the same question, and it is
        // the expensive one here.
        let elsewhere = !same_origin(request_url, &target);

        // Credentials first: it is the one a caller most needs the reason for,
        // and the one a heavy `write_table` would otherwise be told the wrong
        // thing about.
        if self.token.is_some() && elsewhere {
            return refused(RedirectRefusal::Credentials);
        }
        if !body.replayable() {
            return refused(RedirectRefusal::Body);
        }
        // A token is not the only thing a caller picks a host for. Without
        // this, a tokenless `write_table` answered `302` sent its rows to
        // whichever host the header named — which is not the silent nothing
        // the rule above prevents, but it is still the request's contents
        // going somewhere nobody asked for.
        if elsewhere && body.carries_data() {
            return refused(RedirectRefusal::Payload);
        }
        if hops >= MAX_REDIRECTS {
            return refused(RedirectRefusal::TooMany);
        }

        Ok(Some(target))
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

    /// When one attempt of a command must be finished by.
    ///
    /// `None` for a streaming transfer, which is bounded per phase instead —
    /// and for a timeout so large that no `Instant` can express its deadline,
    /// where the agent's own `timeout_global` is left to do the bounding.
    fn deadline(&self, streaming: bool) -> Option<Instant> {
        if streaming {
            return None;
        }
        Instant::now().checked_add(self.timeout)
    }

    /// Bounds one request: what is left of the command's deadline, or the
    /// per-phase limits a streaming transfer gets instead.
    ///
    /// For a streaming request the end-to-end deadline comes off and every
    /// phase before the data — DNS, connect, sending the request, waiting for
    /// the response headers — keeps the same bound individually. For a
    /// buffered one `left` is the remainder of the deadline taken in
    /// [`Transport::dispatch`], so a redirect chain spends one budget between
    /// its hops rather than one apiece.
    fn scoped<Any>(
        &self,
        request: ureq::RequestBuilder<Any>,
        streaming: bool,
        left: Option<Duration>,
    ) -> ureq::RequestBuilder<Any> {
        if !streaming {
            return match left {
                Some(left) => request.config().timeout_global(Some(left)).build(),
                // No deadline to share out — the agent's own global timeout
                // still applies.
                None => request,
            };
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

/// What is left of `deadline` for the next request.
///
/// `Ok(None)` when there is no deadline to share out, and `Err` when the
/// command has already spent it — reported the way `ureq` reports the same
/// exhaustion from inside a request, so that a caller sees one answer whether
/// the budget ran out mid-request or between two hops of a redirect chain.
fn remaining(deadline: Option<Instant>, command: &str) -> Result<Option<Duration>> {
    let Some(deadline) = deadline else {
        return Ok(None);
    };

    match deadline.checked_duration_since(Instant::now()) {
        Some(left) if !left.is_zero() => Ok(Some(left)),
        _ => Err(ClientError::Transport {
            command: command.to_owned(),
            source: Box::new(ureq::Error::Timeout(ureq::Timeout::Global)),
        }),
    }
}

/// The one place the agent is configured, so a timeout change rebuilds it the
/// same way it was first built.
///
/// **`ureq` follows nothing.** Not because this client refuses redirects — it
/// follows plenty — but because the answer depends on three things at once:
/// the credentials the request carries, whether the redirect leaves the origin
/// the request was addressed to, and whether there is a body a redirect would
/// drop. No combination of `max_redirects` and `redirect_auth_headers`
/// expresses that, so the following is done in [`Transport::redirect`], where
/// all three are in hand. `max_redirects(0)` does not mean "fail on a
/// redirect": it hands the `3xx` back as an ordinary response, which is what
/// gives that decision something to read.
///
/// A note for whoever arrives here meaning to reach for
/// `redirect_auth_headers(RedirectAuthHeaders::SameHost)`: **it does not
/// help.** The redirect this exists for is a control proxy pointing at a data
/// proxy on **another** host, which is precisely the case `SameHost` does not
/// cover — it would drop the header and go anyway.
///
/// `named` is the bundle to trust — [`configured_bundle`] in production, and
/// whatever a test wants to hand it. A parameter rather than a second reading
/// of the environment, so the whole chain from a named file to an agent that
/// carries its roots can be exercised without writing a process-global
/// variable.
///
/// Hands back whatever it could not honour instead of failing: this runs while
/// a client is being constructed, where there is no request to fail and no
/// `Result` to fail into. See [`Transport::unusable`], which is where the
/// refusal is finally spoken.
///
/// A build without TLS has no handshake for a bundle to configure, so it takes
/// `named` and ignores it — one signature is better than two of them behind a
/// `cfg`.
#[cfg_attr(not(feature = "tls"), allow(unused_variables))]
fn build_agent(timeout: Duration, named: Option<&Path>) -> (ureq::Agent, Option<String>) {
    #[allow(unused_mut)]
    let mut builder = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        // Keep non-2xx as ordinary responses so the X-YT-Error header can be
        // read off them; ureq would otherwise collapse them to a status code
        // and discard the cluster's explanation.
        .http_status_as_error(false)
        // `ureq` follows nothing; the following is done in `Transport::redirect`,
        // where the credentials, the origin and the body are all in hand (see
        // this function's own note). `max_redirects(0)` hands the `3xx` back as
        // an ordinary response rather than erroring.
        .max_redirects(0);

    #[allow(unused_mut)]
    let mut refused = None;

    #[cfg(feature = "tls")]
    match root_certs(named) {
        Ok(Some(tls)) => builder = builder.tls_config(tls),
        Ok(None) => {}
        Err(why) => refused = Some(why),
    }

    (builder.build().into(), refused)
}

/// The bundle this process was pointed at, read from the environment once.
///
/// [`std::env::var_os`] rather than `var`: a path is not text, and a
/// `YT_CA_BUNDLE` that is not UTF-8 would be swallowed as "unset" by the
/// stricter one — the same silent fall-through the variable exists to end.
///
/// A build without TLS names nothing, which is the honest answer: there is no
/// handshake to configure, so the variable is read no more than a socket is
/// opened for `https://`.
#[cfg(feature = "tls")]
fn configured_bundle() -> Option<&'static Path> {
    static NAMED: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();

    NAMED
        .get_or_init(|| std::env::var_os(CA_BUNDLE).map(std::path::PathBuf::from))
        .as_deref()
}

#[cfg(not(feature = "tls"))]
fn configured_bundle() -> Option<&'static Path> {
    None
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
///
/// **The configured bundle is read and parsed once per process.** An agent is
/// rebuilt more often than it looks: [`Transport::set_timeout`] makes a new
/// one, and `Transaction::start` and its `Drop` each build a client, so an
/// uncached read cost three parses per transaction of a file whose answer
/// cannot have changed meaning in between. Anything *other* than the
/// configured bundle is parsed on the spot — only a test ever asks for one,
/// and a memo keyed on nothing would hand it the first test's answer.
///
/// **Only success is remembered.** Memoising the failure too would pin a
/// passing condition for the life of the process: a first `Client::new` that
/// lands while config management is rewriting the file in place, or before the
/// mount carrying it is ready, would leave every later client in that process
/// refusing to send anything — with no way back short of a restart. That is
/// the same "make a bad afternoon permanent" mistake this module argues
/// against in [`crate::retry`]'s certificate classification, and it would be
/// odd to commit it here. A failed read is simply tried again next time; the
/// cost is bounded by the size cap, and a bundle that is genuinely broken pays
/// it only on the construction path it was already failing.
#[cfg(feature = "tls")]
fn root_certs(named: Option<&Path>) -> Result<Option<ureq::tls::TlsConfig>, String> {
    static CONFIGURED: std::sync::OnceLock<Option<ureq::tls::TlsConfig>> =
        std::sync::OnceLock::new();

    if named == configured_bundle() {
        if let Some(roots) = CONFIGURED.get() {
            return Ok(roots.clone());
        }

        let roots = roots_for(named)?;
        // A race here is harmless: two threads that both parsed the same file
        // agree about it, and the loser drops its copy.
        let _ = CONFIGURED.set(roots.clone());
        return Ok(roots);
    }

    roots_for(named)
}

/// The choice itself, split from the lookup so it can be tested without writing
/// the process environment — which is global, and in edition 2024 unsafe to
/// write.
#[cfg(feature = "tls")]
fn roots_for(named: Option<&Path>) -> Result<Option<ureq::tls::TlsConfig>, String> {
    match named {
        // An empty variable is not a bundle: `YT_CA_BUNDLE=` in a shell profile
        // means "I turned that off", not "trust a file called nothing".
        Some(path) if !names_nothing(path) => bundle(path).map(Some),
        _ => Ok(platform_roots()),
    }
}

/// Whether a variable that is set nevertheless names no file.
///
/// `YT_CA_BUNDLE=` and `YT_CA_BUNDLE="   "` are both how a shell profile turns
/// one off; read as paths they would be a refusal on every request. A path that
/// is not UTF-8 is *not* nothing — it is a path this crate cannot spell, which
/// is exactly the case [`configured_bundle`] reads as `OsString` to keep.
#[cfg(feature = "tls")]
fn names_nothing(path: &Path) -> bool {
    path.to_str().is_some_and(|text| text.trim().is_empty())
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
///
/// **And for a block that is labelled a certificate and is not one.** PEM is an
/// envelope: `parse_pem` splits the sections and base64-decodes them, and
/// checks nothing about what comes out — `Certificate::from_der`'s own
/// documentation says the validation "is the responsibility of the TLS
/// provider". That provider is `rustls`, whose `add_parsable_certificates`
/// *drops* what it cannot parse and reports the count to nobody. So a `.p7b`
/// re-armoured under a `BEGIN CERTIFICATE` label — the usual way a Windows-born
/// bundle arrives — was accepted here, produced an empty root store, and failed
/// every request with the same `UnknownIssuer` that named neither the file nor
/// the variable. [`is_x509`] is the check that closes it, and **one bad block
/// refuses the whole file** rather than trusting a silently shorter set of
/// roots than the caller wrote down.
#[cfg(feature = "tls")]
fn bundle(path: &Path) -> Result<ureq::tls::TlsConfig, String> {
    use std::io::Read;

    use ureq::tls::{Certificate, PemItem, RootCerts, TlsConfig, parse_pem};

    let shown = path.display();

    // `stat` before `open`, and not only for the size: opening a FIFO for
    // reading blocks until someone writes to it, and there is nothing above
    // this to time it out — `Client::new` is infallible and the client's
    // global timeout covers requests, not files. A named pipe left in a
    // variable would hang the constructor for ever.
    let found = std::fs::metadata(path)
        .map_err(|e| format!("{CA_BUNDLE} names {shown}, which could not be read: {e}"))?;

    if !found.is_file() {
        return Err(format!(
            "{CA_BUNDLE} names {shown}, which is not a regular file: a root bundle is read whole, \
             and a directory or a pipe has no end to read to"
        ));
    }

    if found.len() > MAX_BUNDLE_BYTES {
        return Err(format!(
            "{CA_BUNDLE} names {shown}, which is {} bytes: a root bundle is a few hundred \
             kilobytes and this reader stops at {MAX_BUNDLE_BYTES}",
            found.len()
        ));
    }

    let mut pem = Vec::new();
    std::fs::File::open(path)
        // The cap again on the read itself, since a file can grow between the
        // two calls. One byte over is enough to notice.
        .and_then(|file| file.take(MAX_BUNDLE_BYTES + 1).read_to_end(&mut pem))
        .map_err(|e| format!("{CA_BUNDLE} names {shown}, which could not be read: {e}"))?;

    if pem.len() as u64 > MAX_BUNDLE_BYTES {
        return Err(format!(
            "{CA_BUNDLE} names {shown}, which grew past {MAX_BUNDLE_BYTES} bytes while it was \
             being read"
        ));
    }

    let mut certs: Vec<Certificate<'static>> = Vec::new();
    let mut unparsable = 0usize;
    let mut damaged: Option<String> = None;

    for item in parse_pem(&pem) {
        match item {
            Ok(PemItem::Certificate(cert)) if is_x509(cert.der()) => certs.push(cert),
            Ok(PemItem::Certificate(_)) => unparsable += 1,
            // A private key, or a section this `ureq` does not recognise. Not a
            // root, and not a mistake either: a deployment that keeps its key
            // and its CA in one file is ordinary.
            Ok(_) => {}
            // A section that did not survive the envelope: corrupt base64, or a
            // file that stops mid-block. Counted rather than skipped, because
            // skipping it is the silent truncation this whole function exists
            // to end — the roots would simply be fewer than the file says, and
            // the first request would fail `UnknownIssuer` naming neither.
            // Ordinary bundles do not reach here: leading comments and labels
            // between blocks parse cleanly, so this is damage, not decoration.
            Err(why) => {
                damaged.get_or_insert_with(|| why.to_string());
            }
        }
    }

    if let Some(why) = damaged {
        return Err(format!(
            "{CA_BUNDLE} names {shown}, which holds a section that could not be read: {why}. A \
             truncated download or a mangled copy-paste is the usual cause; the roots that did \
             parse are deliberately not used, because a bundle that is quietly shorter than the \
             file names is worse than one that is refused"
        ));
    }

    if unparsable > 0 {
        return Err(format!(
            "{CA_BUNDLE} names {shown}, where {unparsable} of {} -----BEGIN CERTIFICATE----- \
             blocks hold something that is not an X.509 certificate. A PKCS#7 `.p7b` re-armoured \
             under that label is the usual cause; `openssl pkcs7 -print_certs` converts one",
            certs.len() + unparsable
        ));
    }

    if certs.is_empty() {
        return Err(format!(
            "{CA_BUNDLE} names {shown}, which holds no PEM certificates: expected at least one \
             -----BEGIN CERTIFICATE----- block"
        ));
    }

    Ok(TlsConfig::builder()
        .root_certs(RootCerts::new_with_certs(&certs))
        .build())
}

/// DER tags, as far as a certificate's skeleton uses them.
#[cfg(feature = "tls")]
mod der {
    pub(super) const INTEGER: u8 = 0x02;
    pub(super) const BIT_STRING: u8 = 0x03;
    pub(super) const SEQUENCE: u8 = 0x30;
    /// `[0] EXPLICIT`, which is where a certificate's version lives — and where
    /// it is absent on a v1 one.
    pub(super) const VERSION: u8 = 0xa0;
}

/// Whether these bytes really are an X.509 certificate.
///
/// Not a verification and not a full parse: the question is only whether
/// `rustls` will find a certificate here, because what it does with something
/// else is discard it in silence. Checking the shape is what turns that into a
/// sentence naming the file. See [`bundle`].
///
/// ```text
/// Certificate ::= SEQUENCE {
///     tbsCertificate       TBSCertificate,
///     signatureAlgorithm   AlgorithmIdentifier,
///     signatureValue       BIT STRING }
/// ```
///
/// A PKCS#7 `ContentInfo` — the `.p7b` this exists for — is also a `SEQUENCE`,
/// but its first member is an OBJECT IDENTIFIER rather than the
/// `tbsCertificate` sequence, so it parts company on the second field and needs
/// nothing deeper to tell apart. The `tbsCertificate` check goes deeper anyway:
/// a shape that agrees this far and disagrees inside is not something anyone
/// would call a certificate.
#[cfg(feature = "tls")]
fn is_x509(der: &[u8]) -> bool {
    let Some((body, after)) = expect(der, der::SEQUENCE) else {
        return false;
    };
    if !after.is_empty() {
        return false;
    }

    let Some((tbs, rest)) = expect(body, der::SEQUENCE) else {
        return false;
    };
    let Some((_, rest)) = expect(rest, der::SEQUENCE) else {
        return false;
    };
    let Some((_, rest)) = expect(rest, der::BIT_STRING) else {
        return false;
    };

    rest.is_empty() && is_tbs_certificate(tbs)
}

/// The fixed head of a `TBSCertificate`: an optional version, a serial number,
/// and five `SEQUENCE`s — signature, issuer, validity, subject and the public
/// key. What may follow those is optional and version-dependent, and proves
/// nothing more than they already have.
#[cfg(feature = "tls")]
fn is_tbs_certificate(tbs: &[u8]) -> bool {
    let after_version = match tlv(tbs) {
        Some((tag, _, rest)) if tag == der::VERSION => rest,
        // Absent on a v1 certificate, where the serial number comes first.
        _ => tbs,
    };

    let Some((_, mut rest)) = expect(after_version, der::INTEGER) else {
        return false;
    };

    for _ in 0..5 {
        let Some((_, next)) = expect(rest, der::SEQUENCE) else {
            return false;
        };
        rest = next;
    }

    true
}

/// One DER value of the tag asked for: its contents, and what follows it.
#[cfg(feature = "tls")]
fn expect(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    match tlv(input) {
        Some((found, contents, rest)) if found == tag => Some((contents, rest)),
        _ => None,
    }
}

/// Splits one DER tag-length-value off the front of `input`.
///
/// Only what a certificate's skeleton uses: single-byte tags and definite,
/// minimally encoded lengths. The indefinite form is BER and not DER, a
/// non-minimal length is not DER either, and neither belongs in a file anyone
/// should be trusting a cluster's identity to.
#[cfg(feature = "tls")]
fn tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;

    // The high-tag-number form, which nothing in a certificate's skeleton uses.
    if tag & 0x1f == 0x1f {
        return None;
    }

    let (&first, rest) = rest.split_first()?;
    let (length, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let count = usize::from(first & 0x7f);
        // 0x80 is the indefinite form. Four bytes is 4 GB, which is more than
        // any bundle this reads and more than `MAX_BUNDLE_BYTES` allows.
        if count == 0 || count > 4 {
            return None;
        }
        let (bytes, rest) = rest.split_at_checked(count)?;
        // A leading zero, or a value the short form would have held, is a
        // length DER does not spell that way.
        if bytes[0] == 0 || (count == 1 && bytes[0] < 0x80) {
            return None;
        }
        let length = bytes
            .iter()
            .fold(0usize, |whole, byte| (whole << 8) | usize::from(*byte));
        (length, rest)
    };

    let (contents, rest) = rest.split_at_checked(length)?;
    Some((tag, contents, rest))
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

/// Resolves a `Location` against the address the request went to.
///
/// `Location` was required to be absolute until RFC 7231 relaxed it, and
/// balancers took the permission: `Location: /api/v4/exists?path=…` is an
/// ordinary answer. Reporting that back as "redirected to /api/v4/exists"
/// names no host, and comparing it against one decides nothing — so it is made
/// absolute first, and everything downstream sees an address.
///
/// The four forms of [RFC 3986 §4.2](https://www.rfc-editor.org/rfc/rfc3986#section-4.2),
/// in the order they are tried: an absolute URI keeps its own scheme and
/// authority; a network-path reference (`//host/path`) keeps the scheme; an
/// absolute-path reference (`/path`) keeps scheme and authority; a relative
/// reference keeps everything down to the directory the request's path is in.
///
/// The last of those has two forms with **no path of their own**, and
/// [§5.3](https://www.rfc-editor.org/rfc/rfc3986#section-5.3) is explicit that
/// they keep the base's: `Location: ?path=//other` against
/// `/api/v4/exists?path=//tmp` is `/api/v4/exists?path=//other`, not
/// `/api/v4/?path=//other`, and `Location: #frag` keeps the query as well.
/// Getting that wrong costs a `404` rather than a credential — the origin is
/// the same either way — but it is a `404` for a request the proxy meant to
/// answer.
///
/// `None` for a `Location` this cannot place — an empty one, or a request
/// address with no `scheme://`. The caller treats that as "not a redirect this
/// client acts on" rather than inventing a host for it.
fn resolve(request: &str, location: &str) -> Option<String> {
    let location = location.trim();
    if location.is_empty() {
        return None;
    }
    if has_scheme(location) {
        return Some(location.to_owned());
    }

    let (scheme, rest) = request.split_once("://")?;
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, target) = rest.split_at(end);
    if authority.is_empty() {
        return None;
    }

    if let Some(elsewhere) = location.strip_prefix("//") {
        return Some(format!("{scheme}://{elsewhere}"));
    }
    if location.starts_with('/') {
        return Some(format!("{scheme}://{authority}{location}"));
    }

    // The base's path and query, without the fragment: a fragment is never
    // part of what a reference is resolved against.
    let base = target.split('#').next().unwrap_or("");
    let path = base.split('?').next().unwrap_or("");

    // A reference with no path of its own keeps the base's — and a bare
    // fragment keeps the base's query too, where a query of its own replaces
    // it.
    if location.starts_with('#') {
        return Some(format!("{scheme}://{authority}{base}{location}"));
    }
    if location.starts_with('?') {
        return Some(format!("{scheme}://{authority}{path}{location}"));
    }

    // A relative path is merged with the directory the base's path is in, and
    // takes the query with it: that one belonged to the old path.
    let directory = path.rsplit_once('/').map_or("", |(head, _)| head);
    Some(format!("{scheme}://{authority}{directory}/{location}"))
}

/// Whether a string begins with a URI scheme — `ALPHA *( ALPHA / DIGIT / "+" /
/// "-" / "." ) ":"`, and the colon must come before any path, query or
/// fragment. `//host/x` and `/x:y` are not absolute; `HTTPS://h` is.
fn has_scheme(url: &str) -> bool {
    let Some(colon) = url.find(':') else {
        return false;
    };
    let scheme = &url[..colon];
    !scheme.is_empty()
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// Whether two absolute URLs share an origin: scheme, host and port.
///
/// The comparison a credential-carrying redirect turns on, so it is made to be
/// unfooled rather than to be brief. Userinfo is not part of an origin, and
/// dropping it is what stops `http://real.example.net@evil.example.net/` from
/// reading as `real.example.net`. A missing port is the scheme's default, so
/// `https://h` and `https://h:443` are one origin and `http://h` is not.
///
/// Fails closed: a URL either side cannot be split into an origin is not the
/// same origin as anything, including itself.
fn same_origin(one: &str, other: &str) -> bool {
    match (origin(one), origin(other)) {
        (Some(one), Some(other)) => one == other,
        _ => false,
    }
}

fn origin(url: &str) -> Option<(String, String, u16)> {
    let (scheme, rest) = url.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    let port = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        // An origin needs a port, and a scheme this client does not speak has
        // no default to supply one.
        _ => return None,
    };

    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);

    // `[::1]:8080` splits at the last colon; `[::1]` has colons and no port.
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, given)) if !given.is_empty() && given.bytes().all(|b| b.is_ascii_digit()) => {
            (host, given.parse().ok()?)
        }
        _ => (host_port, port),
    };
    if host.is_empty() {
        return None;
    }

    Some((scheme, host.to_ascii_lowercase(), port))
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

    fn authenticated() -> Transport {
        Transport::new(
            "http://localhost:8000",
            Some("secret-token".to_owned()),
            Duration::from_secs(1),
        )
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

    /// The same self-signed CA as [`CA_PEM`], turned into a PKCS#7 `.p7b` with
    /// `openssl crl2pkcs7 -nocrl -certfile ca.pem -outform DER` and then
    /// base64-armoured under a `CERTIFICATE` label — which is what a Windows
    /// export converted by hand actually looks like.
    ///
    /// Genuine, not hand-waved: it decodes, it is well-formed DER, and it is a
    /// `ContentInfo` rather than a `Certificate`. `parse_pem` takes it, `rustls`
    /// drops it without a word, and the root store that comes out is empty.
    /// That is the whole defect, in one constant.
    #[cfg(feature = "tls")]
    const REARMOURED_P7B: &str = "\
-----BEGIN CERTIFICATE-----
MIIDTAYJKoZIhvcNAQcCoIIDPTCCAzkCAQExADALBgkqhkiG9w0BBwGgggMhMIID
HTCCAgWgAwIBAgIUf6mwbBS7JGIyvPDkCpiBRHp914cwDQYJKoZIhvcNAQELBQAw
HjEcMBoGA1UEAwwTeXRzYXVydXMtcnMgdGVzdCBDQTAeFw0yNjA4MDYyMDM4MTJa
Fw00NjA4MDEyMDM4MTJaMB4xHDAaBgNVBAMME3l0c2F1cnVzLXJzIHRlc3QgQ0Ew
ggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDqPTrcPPGiHlv4aV8vAdrN
tzvlhHciQbd7Pz0tLCmn8OGCjwt3Q/V22h6HSWijIleHPqn6bTSMYfPGAxRemAiq
SsMLpM+GYWZAg8Kz7VSsK4f0s4dW6i82QYFVk/+04N/0RUJ3A9RTloxSl8+aHT5M
F2x4LGr1eBgpz4UEsC5cJtkzA8OCM2a2TtNiuo/PtKzZx2TuvEk+Ub5Gn/lttZn8
m9z6o8n51D3vEIfHfXPyFre2+cz+Ao680kc0KP8PWlG89mhvMZ2VYGJG2T/Z6Ddp
j7aXM+jKCCjBTLMkLYaIuNO9//72kmBYsVgaBAMNYMBaBqQX1TOjwxbiBbv5fbJn
AgMBAAGjUzBRMB0GA1UdDgQWBBSniLAZD6er7hHpwg12hIX57PHb2TAfBgNVHSME
GDAWgBSniLAZD6er7hHpwg12hIX57PHb2TAPBgNVHRMBAf8EBTADAQH/MA0GCSqG
SIb3DQEBCwUAA4IBAQBsR5VKflwEwRTNY1dobAWKS6kLTszpRFlQN2qBMTv+NhS0
i7mrNUzKadZkmlQuOMIhZl6gR4mB0XVPgkJKJ+ch8SfuaBW3Po4dTdrKfB6KCgCT
M54UB3QQAlAjpVhLCS7aCT8hgKEX1+1OD1SmBNQ/Jj9OOoKxVkq9prjSzILWpXeT
/OKKRqZ7tjG2jh55XPgE+GWLCfo3VsPqcleAoxQEWATryTF4fwKI9tuAgJ8ppN1M
6UxJFatwx23InC/jVPR6wBu5h1SyCjIxuW/j8pgriTm8wR3XaTly49j6VQDH8KGh
yM+0UsZEWeI05Uq9c/Vs5TlJAcnvwJwxJqREhlHYMQA=
-----END CERTIFICATE-----
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

        fn path(&self) -> &Path {
            &self.0
        }

        /// The path as the refusals spell it, for asserting they name it.
        fn shown(&self) -> String {
            self.0.display().to_string()
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
            assert!(refusal.contains(&file.shown()), "{what}: {refusal}");
            assert!(refusal.contains("no PEM certificates"), "{what}: {refusal}");
        }
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_pkcs7_bundle_wearing_a_certificate_label_is_refused() {
        // The headline defect. PEM is an envelope: `parse_pem` splits and
        // base64-decodes and checks nothing, and `rustls` then discards what it
        // cannot parse *in silence* — so this was accepted, the root store came
        // out empty, and every request failed `UnknownIssuer` naming neither
        // the file nor the variable. Which is precisely the outcome
        // `YT_CA_BUNDLE` exists to end, arrived at through `YT_CA_BUNDLE`.
        let file = TempPem::new(REARMOURED_P7B);
        let refusal = bundle(file.path()).expect_err("a PKCS#7 blob is not a certificate");

        assert!(refusal.contains(CA_BUNDLE), "{refusal}");
        assert!(refusal.contains(&file.shown()), "{refusal}");
        assert!(refusal.contains("not an X.509 certificate"), "{refusal}");
        assert!(refusal.contains("PKCS#7"), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn one_good_certificate_does_not_excuse_the_rest_of_the_file() {
        // The truncation case: a real root beside two blocks that are not
        // certificates. Accepting it would silently trust one third of what the
        // caller wrote down, and the request that then failed would blame the
        // cluster.
        let file = TempPem::new(&format!("{CA_PEM}{REARMOURED_P7B}{REARMOURED_P7B}"));
        let refusal = bundle(file.path()).expect_err("two blocks are not certificates");

        assert!(refusal.contains("2 of 3"), "{refusal}");
        assert!(refusal.contains(&file.shown()), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_block_that_did_not_survive_the_envelope_refuses_the_file_too() {
        // The other half of the same truncation: a section that never decodes
        // at all. `parse_pem` yields `Err` for it and the roots that did parse
        // are still perfectly good — which is exactly the trap, because a store
        // that is quietly shorter than the file fails later, as `UnknownIssuer`
        // against a cluster that is not at fault.
        //
        // Ordinary bundles do not land here: a leading comment or a label
        // between blocks parses without complaint. Only damage does.
        for (what, body) in [
            (
                "corrupt base64",
                format!(
                    "{CA_PEM}-----BEGIN CERTIFICATE-----\n!!!! not base64 !!!!\n\
                     -----END CERTIFICATE-----\n{CA_PEM}"
                ),
            ),
            (
                "a file that stops mid-block",
                format!("{CA_PEM}-----BEGIN CERTIFICATE-----\nMIIB"),
            ),
        ] {
            let file = TempPem::new(&body);
            let refusal = bundle(file.path()).err().unwrap_or_else(|| {
                panic!("{what} should refuse the file rather than shorten the store")
            });

            assert!(refusal.contains(&file.shown()), "{what}: {refusal}");
            assert!(refusal.contains("could not be read"), "{what}: {refusal}");
        }
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_bundle_larger_than_any_bundle_is_refused_rather_than_held() {
        // Sized, not written: the cap is read off the file's metadata, so the
        // bytes are never touched — which is the whole point. A 512 MB file
        // cost 18.7 s and 1.27 GB of resident memory before this, for something
        // that was never going to parse.
        let file = TempPem::new("");
        std::fs::OpenOptions::new()
            .write(true)
            .open(file.path())
            .and_then(|f| f.set_len(MAX_BUNDLE_BYTES + 1))
            .expect("sizes the file");

        let refusal = bundle(file.path()).expect_err("larger than any root bundle");

        assert!(refusal.contains(CA_BUNDLE), "{refusal}");
        assert!(refusal.contains(&file.shown()), "{refusal}");
        assert!(refusal.contains("a few hundred kilobytes"), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_bundle_that_is_not_a_regular_file_is_refused_rather_than_read() {
        // A directory, and by the same check a FIFO — which is the one that
        // matters: opening a named pipe for reading blocks until someone writes
        // to it, `Client::new` is infallible, and the client's global timeout
        // covers requests rather than files. Nothing above this would ever have
        // ended the wait.
        let refusal = bundle(&std::env::temp_dir()).expect_err("a directory is not a bundle");

        assert!(refusal.contains(CA_BUNDLE), "{refusal}");
        assert!(refusal.contains("not a regular file"), "{refusal}");
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
    fn a_named_bundle_that_will_not_parse_refuses_the_choice_itself() {
        // `roots_for` is where the fall-through would hide: turning
        // `bundle(path).map(Some)` into `Ok(bundle(path).ok())` makes an
        // unreadable bundle mean "nothing was named", which is Mozilla's roots
        // and the silent `UnknownIssuer` all over again. It is also, verbatim,
        // what the patch proposed in the issue did.
        let file = TempPem::new(KEY_PEM);
        let refusal = roots_for(Some(file.path())).expect_err("a key is not a root");

        assert!(refusal.contains(CA_BUNDLE), "{refusal}");
        assert!(refusal.contains(&file.shown()), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_variable_that_names_nothing_is_not_a_bundle() {
        // `export YT_CA_BUNDLE=` is how a shell profile turns one off. Read as
        // a path it would be a refusal on every request.
        for named in [None, Some(Path::new("")), Some(Path::new("   "))] {
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
        let refusal = bundle(&missing).expect_err("nothing to read");

        assert!(refusal.contains(CA_BUNDLE), "{refusal}");
        assert!(refusal.contains("could not be read"), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn the_variable_is_spelled_the_way_the_documentation_spells_it() {
        // The one assertion that is about the name rather than about what the
        // name does. Everything else here compares against the constant, so
        // renaming its *value* would leave the suite green and the crate
        // reading a variable nobody sets — the README, the crate docs, the
        // CHANGELOG and the `yt` CLI all say `YT_CA_BUNDLE`.
        assert_eq!(CA_BUNDLE, "YT_CA_BUNDLE");

        let missing = std::env::temp_dir().join("ytsaurus-rs-no-such-bundle.pem");
        let refusal = bundle(&missing).expect_err("nothing to read");
        assert!(refusal.contains("YT_CA_BUNDLE"), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_named_bundle_reaches_the_agent_that_is_built_from_it() {
        // The other half of the chain: `roots_for` choosing correctly is worth
        // nothing if `build_agent` drops the answer on the floor. Nothing else
        // reads the agent's own configuration back.
        let file = TempPem::new(CA_PEM);
        let (agent, refused) = build_agent(Duration::from_secs(1), Some(file.path()));

        assert!(refused.is_none(), "{refused:?}");
        assert!(
            matches!(
                agent.config().tls_config().root_certs(),
                ureq::tls::RootCerts::Specific(_)
            ),
            "{:?}",
            agent.config().tls_config().root_certs()
        );
    }

    #[test]
    #[cfg(feature = "tls")]
    fn a_bundle_the_agent_could_not_honour_is_carried_out_of_the_constructor() {
        // `build_agent` has no `Result` to fail into, so the one thing it must
        // do with a refusal is hand it back. Swallowing it — `Err(_) => {}` —
        // leaves a client that looks built, trusts Mozilla's roots, and never
        // mentions the file it was told to use.
        let file = TempPem::new(KEY_PEM);
        let (_, refused) = build_agent(Duration::from_secs(1), Some(file.path()));

        let refusal = refused.expect("the refusal reaches the transport");
        assert!(refusal.contains(CA_BUNDLE), "{refusal}");
        assert!(refusal.contains(&file.shown()), "{refusal}");
    }

    #[test]
    #[cfg(feature = "tls")]
    fn the_der_check_takes_certificates_and_leaves_everything_else() {
        use ureq::tls::{Certificate, PemItem, parse_pem};

        let der = |pem: &str| {
            parse_pem(pem.as_bytes())
                .find_map(|item| match item {
                    Ok(PemItem::Certificate(cert)) => Some(Certificate::to_owned(&cert)),
                    _ => None,
                })
                .expect("one CERTIFICATE block")
        };

        assert!(is_x509(der(CA_PEM).der()));
        assert!(!is_x509(der(REARMOURED_P7B).der()));

        // Nothing, a truncated certificate, and one with a byte glued on the
        // end — the three ways a length can lie.
        let good = der(CA_PEM);
        assert!(!is_x509(&[]));
        assert!(!is_x509(&good.der()[..good.der().len() - 1]));
        assert!(!is_x509(&[good.der(), b"\x00"].concat()));
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

    /// A transport that must refuse every request before it opens a socket,
    /// in **either** feature configuration.
    ///
    /// With `tls` on that is a `YT_CA_BUNDLE` that could not be honoured; with
    /// it off, an `https://` proxy in a build that has no handshake at all.
    /// Both are [`Transport::unusable`], which is the thing the two tests below
    /// pin — and the base is a closed port on the loopback so that a
    /// `Transport` which *did* reach the network fails fast and loudly rather
    /// than resolving a name that might exist.
    fn cannot_send() -> Transport {
        let mut transport = Transport::new("https://127.0.0.1:1", None, Duration::from_millis(250));
        transport.set_retries(RetryPolicy::none().quiet());
        #[cfg(feature = "tls")]
        {
            transport.tls_refused = Some(format!("{CA_BUNDLE} names /etc/no-such-file"));
        }
        transport
    }

    #[test]
    fn a_command_is_refused_before_a_socket_is_opened() {
        // `dispatch` is the seam every command goes through — `send`, `open`
        // and `upload` all reach it — so its guard is the one that decides
        // whether an unusable transport explains itself or fails at the
        // handshake with a sentence about the network. Removing it leaves the
        // suite green today; this is what says otherwise.
        let error = cannot_send()
            .dispatch(
                Method::Get,
                "get_supported_features",
                &map::<&str>([]),
                Outgoing::Empty,
                false,
            )
            .expect_err("a transport that cannot be used");

        assert!(matches!(error, ClientError::Config(_)), "{error}");
    }

    #[test]
    fn the_hosts_lookup_is_refused_before_a_socket_is_opened() {
        // `/hosts` is not a command and gets its request built by hand, which
        // is how it once came to carry no token; the guard is one of the four
        // things `fetch` exists to stop it missing again.
        let error = cannot_send()
            .fetch("/hosts", "hosts")
            .expect_err("a transport that cannot be used");

        assert!(matches!(error, ClientError::Config(_)), "{error}");
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

    #[test]
    fn ureq_follows_no_redirect_for_any_transport() {
        // Not "this client refuses redirects" — it follows same-origin ones.
        // It is that the answer depends on the credentials, the origin and the
        // body all at once, which no `ureq` setting combines, so the 3xx has to
        // come back unfollowed for `Transport::redirect` to read.
        assert_eq!(authenticated().agent.config().max_redirects(), 0);
        assert_eq!(transport(None).agent.config().max_redirects(), 0);
    }

    #[test]
    fn changing_the_timeout_keeps_the_redirect_policy() {
        // `set_timeout` rebuilds the agent, which makes it the one place the
        // policy can be lost — to a caller doing nothing more suspicious than
        // `Client::with_timeout`.
        let mut transport = authenticated();
        transport.set_timeout(Duration::from_secs(30));

        assert_eq!(transport.agent.config().max_redirects(), 0);
        assert_eq!(
            transport.agent.config().timeouts().global,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn a_location_is_resolved_against_the_address_it_came_from() {
        let request = "http://proxy.example.net:8000/api/v4/exists?path=//tmp";

        // Absolute: taken as it stands.
        assert_eq!(
            resolve(request, "https://data.example.net/api/v4/read_table").as_deref(),
            Some("https://data.example.net/api/v4/read_table")
        );
        // Network-path reference: the scheme survives, the host does not.
        assert_eq!(
            resolve(request, "//data.example.net/api/v4").as_deref(),
            Some("http://data.example.net/api/v4")
        );
        // Absolute path: the balancer's canonical form of the same request.
        assert_eq!(
            resolve(request, "/api/v4/exists?path=//tmp").as_deref(),
            Some("http://proxy.example.net:8000/api/v4/exists?path=//tmp")
        );
        // Relative path: against the directory, and the old query goes.
        assert_eq!(
            resolve(request, "read_table").as_deref(),
            Some("http://proxy.example.net:8000/api/v4/read_table")
        );
        // A reference with no path of its own keeps the request's — RFC 3986
        // §5.3. Dropping it back to the directory turns a rewritten command
        // into a `404` on `/api/v4/`.
        assert_eq!(
            resolve(request, "?path=//other").as_deref(),
            Some("http://proxy.example.net:8000/api/v4/exists?path=//other")
        );
        // A bare fragment keeps the query too.
        assert_eq!(
            resolve(request, "#frag").as_deref(),
            Some("http://proxy.example.net:8000/api/v4/exists?path=//tmp#frag")
        );
        // The base's own fragment is never part of what is resolved against.
        assert_eq!(
            resolve("http://h/api/v4/exists?path=//tmp#old", "?path=//other").as_deref(),
            Some("http://h/api/v4/exists?path=//other")
        );
        // Nothing to be relative to but the root.
        assert_eq!(
            resolve("http://h", "?path=//tmp").as_deref(),
            Some("http://h?path=//tmp")
        );
        assert_eq!(
            resolve("http://h", "read_table").as_deref(),
            Some("http://h/read_table")
        );
        // Whitespace is header padding, not part of the address.
        assert_eq!(
            resolve(request, "  /hosts  ").as_deref(),
            Some("http://proxy.example.net:8000/hosts")
        );
        // Nothing to place.
        assert_eq!(resolve(request, ""), None);
        assert_eq!(resolve("proxy.example.net", "/hosts"), None);
    }

    #[test]
    fn a_scheme_is_told_from_a_path() {
        assert!(has_scheme("https://h/x"));
        assert!(has_scheme("HTTP://h/x"));
        // A colon inside a path is not a scheme, and neither is one after it.
        assert!(!has_scheme("/api/v4/read:table"));
        assert!(!has_scheme("//h/x"));
        assert!(!has_scheme("read_table"));
        assert!(!has_scheme("://h"));
        // A scheme cannot start with a digit.
        assert!(!has_scheme("8000:80"));
    }

    #[test]
    fn an_origin_is_scheme_host_and_port() {
        assert!(same_origin(
            "http://proxy.example.net/api/v4/exists",
            "http://proxy.example.net/api/v4/read_table?path=//tmp"
        ));
        // A default port is the port.
        assert!(same_origin("https://h/x", "https://h:443/x"));
        assert!(same_origin(
            "http://H.example.net/x",
            "http://h.example.net/x"
        ));
        // Everything an origin is made of, one at a time.
        assert!(!same_origin("http://h/x", "https://h/x"));
        assert!(!same_origin("http://h/x", "http://other/x"));
        assert!(!same_origin("http://h/x", "http://h:8000/x"));
        // The one that reads as `real.example.net` and connects to the other.
        assert!(!same_origin(
            "http://real.example.net/x",
            "http://real.example.net@evil.example.net/x"
        ));
        // Fails closed rather than calling two unparseable things equal.
        assert!(!same_origin("not a url", "not a url"));
        assert!(!same_origin("ftp://h/x", "ftp://h/x"));
    }

    #[test]
    fn the_heavy_commands_are_the_ones_that_carry_a_stream() {
        // The advice a refused redirect ends with is "go to a heavy proxy",
        // which only a heavy command can act on.
        //
        // Every command this crate itself sends heavily is here. `get_job_stderr`
        // was the one that was not, and it is the one a launcher reaches for
        // while it is already diagnosing a failure — the worst moment to be
        // handed a refusal with no advice in it. See the merge marker on
        // [`HEAVY`]: #38 writes the same fact down a second time.
        for command in [
            "read_table",
            "write_table",
            "write_file",
            "get_job_input",
            "get_job_stderr",
        ] {
            assert!(HEAVY.contains(&command), "{command}");
        }
        // And the ones reachable only through the raw door, which is the point
        // of listing what the cluster calls heavy rather than what this crate
        // models: the documentation on `raw_command_streaming` reads a file.
        for command in ["read_file", "read_blob_table"] {
            assert!(HEAVY.contains(&command), "{command}");
        }
        for command in ["create", "exists", "start_operation", "get_job", "hosts"] {
            assert!(!HEAVY.contains(&command), "{command}");
        }
    }

    #[test]
    fn a_deadline_is_shared_out_and_then_refused() {
        let command = "exists";
        // No deadline: nothing to share out, and nothing to refuse.
        assert!(remaining(None, command).expect("no deadline").is_none());

        let ahead = Instant::now() + Duration::from_secs(30);
        let left = remaining(Some(ahead), command)
            .expect("still time")
            .expect("a bound");
        assert!(left <= Duration::from_secs(30) && left > Duration::from_secs(29));

        // Spent. Reported as the timeout it is, and as a `Transport` error, so
        // the retry policy treats it exactly as it treats one that happened
        // inside a request.
        let error = remaining(Some(Instant::now() - Duration::from_millis(1)), command)
            .expect_err("the budget is gone");
        assert!(matches!(error, ClientError::Transport { .. }), "{error:?}");
        assert!(error.to_string().contains("timeout"), "{error}");
        assert!(crate::retry::is_retriable(&error), "{error:?}");
    }
}
