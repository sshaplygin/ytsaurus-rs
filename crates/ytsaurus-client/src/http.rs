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
    ($request:expr, $headers:expr) => {{
        let mut request = $request;
        for (name, value) in $headers {
            request = request.header(*name, value.as_str());
        }
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
    /// Stamped onto every command, when the client is bound to a transaction.
    transaction: Option<String>,
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

        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            // Keep non-2xx as ordinary responses so the X-YT-Error header can
            // be read off them; ureq would otherwise collapse them to a status
            // code and discard the cluster's explanation.
            .http_status_as_error(false)
            .build()
            .into();

        // Quiet inside a job, where stderr is the cluster's diagnostic channel
        // and not a terminal. See `retry::report_by_default`.
        let retries = if crate::retry::report_by_default() {
            RetryPolicy::default()
        } else {
            RetryPolicy::default().quiet()
        };

        Self {
            agent,
            base,
            token,
            retries,
            transaction: None,
        }
    }

    pub(crate) fn set_retries(&mut self, policy: RetryPolicy) {
        self.retries = policy;
    }

    pub(crate) fn set_transaction(&mut self, id: Option<String>) {
        self.transaction = id;
    }

    pub(crate) fn transaction(&self) -> Option<&str> {
        self.transaction.as_deref()
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
        let mut response = self.dispatch(method, command, parameters, bytes.as_body())?;

        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            // Responses are small (a table read is the exception, and a
            // launcher reads results, not bulk data). The default cap is
            // conservative enough to truncate a modest table silently.
            .limit(512 * 1024 * 1024)
            .read_to_vec()
            .map_err(|e| ClientError::Decode {
                command: command.to_owned(),
                reason: format!("could not read the response body: {e}"),
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

        let response = self.dispatch(method, command, parameters, SendBody::none())?;
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
    }

    /// Sends a command whose request body is read as it goes.
    ///
    /// For `write_table` from something larger than memory. `rows` is read
    /// once, so this cannot be retried even in principle: a reader that has
    /// been consumed cannot be sent again.
    pub(crate) fn upload(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        rows: &mut dyn std::io::Read,
    ) -> Result<()> {
        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        let mut response =
            self.dispatch(method, command, parameters, SendBody::from_reader(rows))?;
        let status = response.status().as_u16();

        // Read whichever way it went. A body left unread keeps the connection
        // out of the pool — `ureq` can only reuse one it knows is finished — so
        // an upload that ignored its answer would open a fresh connection for
        // every table write, and leave the old one in TIME_WAIT. The benchmark
        // is what noticed: 11 623 of them after a few seconds of writing.
        let body = response.body_mut().read_to_string().unwrap_or_default();

        if !(200..300).contains(&status) {
            return Err(ClientError::Http {
                command: command.to_owned(),
                status,
                body: truncate(&body, 400),
            });
        }

        Ok(())
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

        let url = format!("{}{path}", self.base);
        let mut headers: Vec<(&str, String)> = Vec::new();
        if let Some(token) = &self.token {
            headers.push(("Authorization", format!("OAuth {token}")));
        }

        crate::retry::run(self.retries, Repeatable::Freely, what, |_| {
            let mut response = with_headers!(self.agent.get(&url), &headers)
                .call()
                .map_err(|e| ClientError::Transport {
                    command: what.to_owned(),
                    source: Box::new(e),
                })?;

            let status = response.status().as_u16();
            let body = response
                .body_mut()
                .read_to_string()
                .map_err(|e| ClientError::Decode {
                    command: what.to_owned(),
                    reason: format!("could not read the response body: {e}"),
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
    fn dispatch(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        body: SendBody<'_>,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        if let Some(error) = tls_unavailable(&self.base) {
            return Err(error);
        }

        let url = format!("{}/api/v4/{command}", self.base);

        let encoded = to_string(parameters, YsonFormat::Text).map_err(|e| ClientError::Decode {
            command: command.to_owned(),
            reason: format!("could not encode parameters: {e}"),
        })?;

        let mut headers: Vec<(&str, String)> = vec![
            (HEADER_FORMAT, "<format=text>yson".to_owned()),
            (PARAMETERS, encoded),
            ("X-YT-Output-Format", "<format=text>yson".to_owned()),
            ("Content-Type", "application/octet-stream".to_owned()),
        ];
        if let Some(token) = &self.token {
            headers.push(("Authorization", format!("OAuth {token}")));
        }

        let sent = match method {
            // A GET carries no body in `ureq`'s type system, which is also true
            // of every command this client sends as one.
            Method::Get => with_headers!(self.agent.get(&url), &headers).call(),
            Method::Post => with_headers!(self.agent.post(&url), &headers).send(body),
            Method::Put => with_headers!(self.agent.put(&url), &headers).send(body),
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

#[derive(Clone, Copy)]
pub(crate) enum Method {
    Get,
    Post,
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
