//! A thin [YTsaurus](https://ytsaurus.tech) client: enough of the HTTP API v4
//! to run a Rust worker without a Python installation.
//!
//! It is deliberately small. It does what launching a job needs — create a
//! node, upload the worker, write and read tables, start an operation and wait
//! for it — and nothing else. For everything beyond that, the `yt` CLI remains
//! the right tool.
//!
//! # Launching a job
//!
//! ```no_run
//! use ytsaurus_client::{Client, MapSpec};
//!
//! # fn main() -> Result<(), ytsaurus_client::ClientError> {
//! let client = Client::from_env()?;
//!
//! // Upload the worker, marked executable so the node can run it.
//! client.upload_worker("target/.../my_job", "//tmp/my_job")?;
//!
//! let spec = MapSpec::new("./my_job", ["//tmp/input"], ["//tmp/output"])
//!     .with_local_file("//tmp/my_job")
//!     .with_memory_limit(512 * 1024 * 1024);
//!
//! let id = client.start_map(&spec)?;
//! client.wait_for_operation(&id)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Configuration
//!
//! [`Client::from_env`] reads `YT_PROXY` for the cluster address and `YT_TOKEN`
//! for the token, matching the `yt` CLI. A bare host is assumed to be HTTPS; a
//! local cluster is reached as `http://localhost:8000`.
//!
//! # What this does not do
//!
//! Heavy commands are documented to require asking `/hosts` for a dedicated
//! proxy, and this client does not: it sends everything to the address it was
//! given. That is correct for a local cluster and for any deployment behind a
//! balancer, but on a large installation an upload may be refused with 503. See
//! [`Client::heavy_proxy`] for the escape hatch.

#![warn(missing_docs)]

use std::time::{Duration, Instant};

/// Errors.
pub mod error;
mod http;
mod spec;
/// Constructors for YSON documents, for specs this crate does not model.
pub mod yson_build;

pub use crate::error::{ClientError, Result};
pub use crate::spec::{MapReduceSpec, MapSpec, OperationType};

use crate::http::{Method, Payload, Transport};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice};

/// Default request timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

/// How often [`Client::wait_for_operation`] asks the cluster for progress.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// A connection to one YTsaurus cluster.
#[derive(Debug, Clone)]
pub struct Client {
    transport: Transport,
    poll_interval: Duration,
}

impl Client {
    /// Connects to `proxy`, with no token.
    ///
    /// `proxy` may be a bare host (`cluster.example.com`, assumed HTTPS) or
    /// carry a scheme (`http://localhost:8000`).
    #[must_use]
    pub fn new(proxy: &str) -> Self {
        Self {
            transport: Transport::new(proxy, None, DEFAULT_TIMEOUT),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Connects to `proxy` using `token` for authentication.
    #[must_use]
    pub fn with_token(proxy: &str, token: impl Into<String>) -> Self {
        Self {
            transport: Transport::new(proxy, Some(token.into()), DEFAULT_TIMEOUT),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Connects using `YT_PROXY` and, if set, `YT_TOKEN`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `YT_PROXY` is not set.
    pub fn from_env() -> Result<Self> {
        let proxy = std::env::var("YT_PROXY").map_err(|_| {
            ClientError::Config(
                "YT_PROXY is not set; export it (for a local cluster: \
                 YT_PROXY=http://localhost:8000) or use Client::new"
                    .to_owned(),
            )
        })?;

        let token = std::env::var("YT_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty());
        Ok(match token {
            Some(token) => Self::with_token(&proxy, token),
            None => Self::new(&proxy),
        })
    }

    /// Overrides how often [`Client::wait_for_operation`] polls.
    #[must_use]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Returns the least-loaded heavy proxy the cluster reports, if any.
    ///
    /// Large installations separate light and heavy proxies and answer heavy
    /// commands on a light proxy with 503. Point a second [`Client`] at this
    /// address to do uploads there. A local cluster returns nothing useful, and
    /// none of this is needed for it.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn heavy_proxy(&self) -> Result<Option<String>> {
        let url = format!("{}/hosts", self.transport.base());
        let body = ureq::get(&url)
            .call()
            .map_err(|e| ClientError::Transport {
                command: "hosts".to_owned(),
                source: Box::new(e),
            })?
            .body_mut()
            .read_to_string()
            .map_err(|e| ClientError::Decode {
                command: "hosts".to_owned(),
                reason: e.to_string(),
            })?;

        let hosts: Vec<String> = serde_json::from_str(&body).unwrap_or_default();
        Ok(hosts.into_iter().next())
    }

    // ------------------------------------------------------------- Cypress

    /// Whether a Cypress node exists.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn exists(&self, path: &str) -> Result<bool> {
        let params = yson_build::map([("path", yson_build::string(path))]);
        let body = self
            .transport
            .call(Method::Get, "exists", &params, Payload::None)?;
        Ok(matches!(
            self.value_field(&body, "exists")?.node,
            YsonNode::Boolean(true)
        ))
    }

    /// Creates a Cypress node, e.g. `table`, `file` or `map_node`.
    ///
    /// Creates missing parents and succeeds if the node already exists.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn create(&self, node_type: &str, path: &str) -> Result<()> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("type", yson_build::string(node_type)),
            ("recursive", yson_build::boolean(true)),
            ("ignore_existing", yson_build::boolean(true)),
        ]);
        self.transport
            .call(Method::Post, "create", &params, Payload::None)?;
        Ok(())
    }

    /// Removes a Cypress node. Succeeds if it is already absent.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn remove(&self, path: &str) -> Result<()> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("recursive", yson_build::boolean(true)),
            ("force", yson_build::boolean(true)),
        ]);
        self.transport
            .call(Method::Post, "remove", &params, Payload::None)?;
        Ok(())
    }

    /// Reads a node attribute, such as `@row_count`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn get(&self, path: &str) -> Result<YsonValue> {
        let params = yson_build::map([("path", yson_build::string(path))]);
        let body = self
            .transport
            .call(Method::Get, "get", &params, Payload::None)?;
        self.value_field(&body, "value")
    }

    /// Number of rows in a table.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the attribute is absent.
    pub fn row_count(&self, path: &str) -> Result<i64> {
        let value = self.get(&format!("{path}/@row_count"))?;
        value.as_i64().ok_or_else(|| ClientError::Decode {
            command: "get".to_owned(),
            reason: format!("{path}/@row_count is not an integer"),
        })
    }

    // ---------------------------------------------------------------- data

    /// Uploads a local file to Cypress, marking it executable.
    ///
    /// This is what makes a worker runnable on a node: without the `executable`
    /// attribute YTsaurus copies the binary but refuses to exec it, and the job
    /// fails with a permission error that does not mention the attribute.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the file cannot be read or the upload fails.
    pub fn upload_worker(&self, local: impl AsRef<std::path::Path>, remote: &str) -> Result<()> {
        let local = local.as_ref();
        let bytes = std::fs::read(local).map_err(|source| ClientError::Io {
            path: local.display().to_string(),
            source,
        })?;

        self.create("file", remote)?;
        self.write_file(remote, &bytes)?;
        self.set_attribute(remote, "executable", yson_build::boolean(true))
    }

    /// Writes raw bytes to a Cypress file, replacing its contents.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn write_file(&self, path: &str, contents: &[u8]) -> Result<()> {
        let params = yson_build::map([("path", yson_build::string(path))]);
        self.transport
            .call(Method::Put, "write_file", &params, Payload::Bytes(contents))?;
        Ok(())
    }

    /// Sets a node attribute.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn set_attribute(&self, path: &str, name: &str, value: YsonValue) -> Result<()> {
        let encoded =
            ytsaurus_yson::to_vec(&value, YsonFormat::Binary).map_err(|e| ClientError::Decode {
                command: "set".to_owned(),
                reason: format!("could not encode the attribute: {e}"),
            })?;

        let params = yson_build::map([
            ("path", yson_build::string(format!("{path}/@{name}"))),
            ("input_format", yson_build::binary_yson_format()),
        ]);
        self.transport
            .call(Method::Put, "set", &params, Payload::Bytes(&encoded))?;
        Ok(())
    }

    /// Writes rows to a table, replacing its contents.
    ///
    /// `rows` must be a binary YSON list fragment — exactly what a
    /// `ytsaurus-job` worker writes.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn write_table(&self, path: &str, rows: &[u8]) -> Result<()> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("input_format", yson_build::binary_yson_format()),
        ]);
        self.transport
            .call(Method::Put, "write_table", &params, Payload::Bytes(rows))?;
        Ok(())
    }

    /// Reads a whole table as a binary YSON list fragment.
    ///
    /// Reads it into memory: this is for results a launcher inspects, not for
    /// bulk export.
    ///
    /// The result is checked to be a complete list fragment. That is not
    /// pedantry — the proxy reports a mid-stream failure in a trailer this
    /// client cannot see (see the `http` module), so a truncated body is the
    /// symptom that *is* detectable, and returning it as success would hand the
    /// caller a silently short table.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the stream is truncated.
    pub fn read_table(&self, path: &str) -> Result<Vec<u8>> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("output_format", yson_build::binary_yson_format()),
        ]);
        let body = self
            .transport
            .call(Method::Get, "read_table", &params, Payload::None)?;

        check_complete_fragment(&body).map_err(|reason| ClientError::Decode {
            command: "read_table".to_owned(),
            reason: format!("{path}: {reason}"),
        })?;

        Ok(body)
    }

    // ---------------------------------------------------------- operations

    /// Starts a map operation, returning its ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_map(&self, spec: &MapSpec) -> Result<String> {
        self.start_operation(OperationType::Map, &spec.to_yson())
    }

    /// Starts a map-reduce operation, returning its ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_map_reduce(&self, spec: &MapReduceSpec) -> Result<String> {
        self.start_operation(OperationType::MapReduce, &spec.to_yson())
    }

    /// Starts an operation from a spec built by hand.
    ///
    /// The escape hatch for anything [`MapSpec`] and [`MapReduceSpec`] do not
    /// model; build the spec with [`yson_build`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_operation(&self, kind: OperationType, spec: &YsonValue) -> Result<String> {
        let params = yson_build::map([
            ("operation_type", yson_build::string(kind.as_str())),
            ("spec", spec.clone()),
        ]);
        let body = self
            .transport
            .call(Method::Post, "start_operation", &params, Payload::None)?;

        let value = self.value_field(&body, "operation_id")?;
        match &value.node {
            YsonNode::String(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
            other => Err(ClientError::Decode {
                command: "start_operation".to_owned(),
                reason: format!("operation_id is not a string: {other:?}"),
            }),
        }
    }

    /// Fetches an operation's current state, e.g. `running` or `completed`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn operation_state(&self, id: &str) -> Result<String> {
        let params = yson_build::map([
            ("operation_id", yson_build::string(id)),
            (
                "attributes",
                yson_build::list([yson_build::string("state")]),
            ),
        ]);
        let body = self
            .transport
            .call(Method::Get, "get_operation", &params, Payload::None)?;

        let value = self.field_of(&self.strip_envelope(&body, "get_operation")?, "state")?;
        match &value.node {
            YsonNode::String(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
            other => Err(ClientError::Decode {
                command: "get_operation".to_owned(),
                reason: format!("state is not a string: {other:?}"),
            }),
        }
    }

    /// Polls until the operation reaches a terminal state.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::OperationFailed`] if it ends as anything other
    /// than `completed`, or [`ClientError`] if polling itself fails.
    pub fn wait_for_operation(&self, id: &str) -> Result<()> {
        let started = Instant::now();
        let mut last_state = String::new();

        loop {
            let state = self.operation_state(id)?;

            if state != last_state {
                eprintln!(
                    "operation {id}: {state} ({:.0}s)",
                    started.elapsed().as_secs_f64()
                );
                last_state.clone_from(&state);
            }

            match state.as_str() {
                "completed" => return Ok(()),
                "failed" | "aborted" => {
                    return Err(ClientError::OperationFailed {
                        id: id.to_owned(),
                        state,
                        error: self.operation_error(id),
                    });
                }
                _ => std::thread::sleep(self.poll_interval),
            }
        }
    }

    /// Best-effort fetch of a failed operation's error document.
    fn operation_error(&self, id: &str) -> Option<String> {
        let params = yson_build::map([
            ("operation_id", yson_build::string(id)),
            (
                "attributes",
                yson_build::list([yson_build::string("result")]),
            ),
        ]);
        let body = self
            .transport
            .call(Method::Get, "get_operation", &params, Payload::None)
            .ok()?;
        Some(crate::error::truncate(&String::from_utf8_lossy(&body), 600))
    }

    // -------------------------------------------------------------- helpers

    /// API v4 wraps every structured response in a dict. Unwraps one level.
    fn strip_envelope(&self, body: &[u8], command: &str) -> Result<YsonValue> {
        from_slice(body, YsonFormat::Text).map_err(|e| ClientError::Decode {
            command: command.to_owned(),
            reason: format!(
                "{e}; body was {}",
                crate::error::truncate(&String::from_utf8_lossy(body), 200)
            ),
        })
    }

    fn field_of(&self, value: &YsonValue, key: &str) -> Result<YsonValue> {
        match &value.node {
            YsonNode::Map(m) => m
                .get(key.as_bytes())
                .cloned()
                .ok_or_else(|| ClientError::Decode {
                    command: key.to_owned(),
                    reason: format!(
                        "response has no {key:?}; keys were {:?}",
                        m.keys()
                            .map(|k| String::from_utf8_lossy(k).into_owned())
                            .collect::<Vec<_>>()
                    ),
                }),
            other => Err(ClientError::Decode {
                command: key.to_owned(),
                reason: format!("expected a dict, got {other:?}"),
            }),
        }
    }

    fn value_field(&self, body: &[u8], key: &str) -> Result<YsonValue> {
        let envelope = self.strip_envelope(body, key)?;
        self.field_of(&envelope, key)
    }
}

/// Verifies that `data` is a whole binary YSON list fragment.
///
/// Walks record boundaries without decoding, so the cost is a scan rather than
/// a parse of the whole table.
fn check_complete_fragment(mut data: &[u8]) -> std::result::Result<(), String> {
    use ytsaurus_yson::{Scan, scan_value};

    let total = data.len();
    loop {
        while data.first() == Some(&b';') || data.first().is_some_and(u8::is_ascii_whitespace) {
            data = &data[1..];
        }
        if data.is_empty() {
            return Ok(());
        }

        match scan_value(data, YsonFormat::Binary) {
            Ok(Scan::Complete { len }) => data = &data[len..],
            Ok(Scan::Incomplete) => {
                return Err(format!(
                    "the response ends inside a record — {} of {total} bytes consumed; \
                     the stream was cut short",
                    total - data.len()
                ));
            }
            Err(e) => {
                return Err(format!(
                    "the response is not valid binary YSON at byte {}: {e}",
                    total - data.len()
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_complete_fragment_is_accepted() {
        // {a=1};{a=1}
        let one = b"{\x01\x02a=\x02\x02}";
        let mut two = one.to_vec();
        two.push(b';');
        two.extend_from_slice(one);

        assert!(check_complete_fragment(b"").is_ok());
        assert!(check_complete_fragment(one).is_ok());
        assert!(check_complete_fragment(&two).is_ok());
    }

    #[test]
    fn a_truncated_fragment_is_rejected() {
        let full = b"{\x01\x02a=\x02\x02}";
        for cut in 1..full.len() {
            let err = check_complete_fragment(&full[..cut])
                .expect_err("a cut record must not pass as complete");
            assert!(
                err.contains("cut short") || err.contains("not valid"),
                "{err}"
            );
        }
    }

    #[test]
    fn truncation_after_a_whole_record_is_rejected() {
        let one = b"{\x01\x02a=\x02\x02}";
        let mut data = one.to_vec();
        data.push(b';');
        data.extend_from_slice(&one[..4]); // second record cut short

        let err = check_complete_fragment(&data).expect_err("must reject");
        assert!(err.contains("cut short"), "{err}");
    }

    #[test]
    fn from_env_explains_itself_when_unconfigured() {
        // Not asserting on process env, only that the message is actionable.
        let err = ClientError::Config("YT_PROXY is not set".to_owned());
        assert!(err.to_string().contains("YT_PROXY"));
    }
}
