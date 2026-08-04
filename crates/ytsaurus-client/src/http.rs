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
//! Rather than pretend the gap does not exist, the client checks what it can:
//! a truncated data stream is caught by validating that the response is a
//! complete YSON list fragment (see `Client::read_table`). A mid-stream failure
//! that still produces well-formed output would go unnoticed — in practice that
//! means a partial read reported as success. For a launcher driving modest
//! amounts of data this is acceptable; for bulk export it is not, and the `yt`
//! CLI remains the right tool there.

use std::time::Duration;

use ureq::http::HeaderMap;
use ytsaurus_yson::{YsonFormat, YsonValue, to_string};

use crate::error::{ClientError, Result, truncate};

const HEADER_FORMAT: &str = "X-YT-Header-Format";
const PARAMETERS: &str = "X-YT-Parameters";
const ERROR: &str = "X-YT-Error";

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

        Self { agent, base, token }
    }

    pub(crate) fn base(&self) -> &str {
        &self.base
    }

    /// Executes a command, returning the raw response body.
    pub(crate) fn call(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        payload: Payload<'_>,
    ) -> Result<Vec<u8>> {
        let url = format!("{}/api/v4/{command}", self.base);

        let encoded = to_string(parameters, YsonFormat::Text).map_err(|e| ClientError::Decode {
            command: command.to_owned(),
            reason: format!("could not encode parameters: {e}"),
        })?;

        let mut headers: Vec<(&str, String)> = vec![
            (HEADER_FORMAT, "<format=text>yson".to_owned()),
            (PARAMETERS, encoded),
            ("X-YT-Output-Format", "<format=text>yson".to_owned()),
        ];
        if let Some(token) = &self.token {
            headers.push(("Authorization", format!("OAuth {token}")));
        }

        let sent = match (method, payload) {
            (Method::Get, _) => with_headers!(self.agent.get(&url), &headers).call(),
            (Method::Post, Payload::None) => {
                with_headers!(self.agent.post(&url), &headers).send_empty()
            }
            (Method::Post, Payload::Bytes(bytes)) => with_headers!(self.agent.post(&url), &headers)
                .header("Content-Type", "application/octet-stream")
                .send(bytes),
            (Method::Put, Payload::None) => {
                with_headers!(self.agent.put(&url), &headers).send_empty()
            }
            (Method::Put, Payload::Bytes(bytes)) => with_headers!(self.agent.put(&url), &headers)
                .header("Content-Type", "application/octet-stream")
                .send(bytes),
        };

        let mut response = sent.map_err(|e| ClientError::Transport {
            command: command.to_owned(),
            source: Box::new(e),
        })?;

        let status = response.status().as_u16();

        // The cluster's own error, which is far more useful than the status.
        if let Some(raw) = header_value(response.headers(), ERROR) {
            return Err(ClientError::from_yt_error(command, status, &raw));
        }

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
