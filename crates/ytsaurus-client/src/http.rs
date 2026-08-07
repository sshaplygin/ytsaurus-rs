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
/// something that can produce a fresh `SendBody` per hop. That is the whole
/// difference between the two loaded variants, and the whole of what
/// [`RedirectRefusal::Body`] now means.
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
    /// and still nothing a redirect can lose.
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
            base,
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

    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        // Through `build_agent` rather than by editing the config in place:
        // this is the one place the agent is built twice, and so the one place
        // the redirect policy could be dropped by a caller doing nothing more
        // suspicious than `with_timeout`.
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
    /// policy, and the guard that turns an `https://` proxy in a build without
    /// TLS into an explanation rather than a connection error. Building a bare
    /// `ureq` request here instead is how it came to miss all four.
    pub(crate) fn fetch(&self, path: &str, what: &str) -> Result<String> {
        if let Some(error) = tls_unavailable(&self.base) {
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

                match self.redirect(what, &response, &url, true, hops)? {
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
        if let Some(error) = tls_unavailable(&self.base) {
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
            if let Some(next) = self.redirect(command, &response, &url, body.replayable(), hops)? {
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
    /// So the rules are here instead, and there are three of them.
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
    /// on a body: on an *unrepeatable* one. Following a redirect here means
    /// sending the same request to the address it named — same method, same
    /// payload — which is what `307` and `308` require and what an API v4
    /// command needs whatever the digit, since a command's verb is a property
    /// of the command. A payload held as bytes goes out again and nothing is
    /// lost. A payload that is a *reader* — [`Transport::upload`], so
    /// `write_table` from an iterator and every `raw_command_upload` — has
    /// already begun to drain into the first request by the time the `3xx`
    /// arrives, and cannot be rewound. That one is refused, with or without a
    /// token: dropping the rows and reporting the answer to an empty request
    /// is how a write that wrote nothing comes back looking like one that
    /// worked.
    ///
    /// **A chain that does not end is refused.** [`MAX_REDIRECTS`] hops, then
    /// it is a loop rather than a route.
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
        replayable: bool,
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

        // Credentials first: it is the one a caller most needs the reason for,
        // and the one a heavy `write_table` would otherwise be told the wrong
        // thing about.
        if self.token.is_some() && !same_origin(request_url, &target) {
            return refused(RedirectRefusal::Credentials);
        }
        if !replayable {
            return refused(RedirectRefusal::Body);
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
fn build_agent(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        // Keep non-2xx as ordinary responses so the X-YT-Error header can be
        // read off them; ureq would otherwise collapse them to a status code
        // and discard the cluster's explanation.
        .http_status_as_error(false)
        .max_redirects(0)
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
