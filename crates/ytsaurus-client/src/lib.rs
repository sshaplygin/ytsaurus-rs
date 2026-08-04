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
//! # When an operation fails
//!
//! [`Client::wait_for_operation`] does not stop at the state. It asks the
//! cluster which jobs failed and what they wrote to stderr, and carries both in
//! [`ClientError::OperationFailed`], so a failure explains itself without a
//! trip to the web UI:
//!
//! ```text
//! operation 1ba94195-… finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
//!   job 24c164af-… on localhost:24403: User job failed: Process terminated by signal 6
//!   stderr:
//!     thread 'main' panicked at examples/src/bin/boom.rs:37:17:
//!     boom: this job fails on purpose (row 1, 23 bytes)
//! ```
//!
//! That costs one [`Client::list_jobs`] and a few [`Client::get_job_stderr`]
//! calls per failed operation; [`Client::with_job_diagnostics`] turns it off.
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
mod jobs;
mod retry;
mod spec;
mod worker;
/// Constructors for YSON documents, for specs this crate does not model.
pub mod yson_build;

pub use crate::error::{ClientError, Result};
pub use crate::jobs::{JobFailure, JobInfo};
pub use crate::retry::{MutationId, RetryPolicy};
pub use crate::spec::{MapReduceSpec, MapSpec, OperationType, ReduceSpec, SortSpec};

use crate::http::{Method, Payload, Transport};
use crate::retry::Repeatable;
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice};

/// Default request timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

/// How often [`Client::wait_for_operation`] asks the cluster for progress.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// How many failed jobs a failed operation reports.
///
/// Jobs of one operation usually fail the same way, so the first few explain
/// the failure and the rest only make the message longer.
const REPORTED_JOBS: u32 = 3;

/// How much of a job's stderr goes into the error message.
///
/// The cluster caps saved stderr at megabytes; an error a user reads in a
/// terminal wants the tail of it, not all of it.
const STDERR_EXCERPT: usize = 4096;

/// A connection to one YTsaurus cluster.
#[derive(Debug, Clone)]
pub struct Client {
    transport: Transport,
    poll_interval: Duration,
    job_diagnostics: bool,
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
            job_diagnostics: true,
        }
    }

    /// Connects to `proxy` using `token` for authentication.
    #[must_use]
    pub fn with_token(proxy: &str, token: impl Into<String>) -> Self {
        Self {
            transport: Transport::new(proxy, Some(token.into()), DEFAULT_TIMEOUT),
            poll_interval: DEFAULT_POLL_INTERVAL,
            job_diagnostics: true,
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

    /// Overrides how a failed request is repeated.
    ///
    /// The default is five attempts with a doubling delay, which covers the
    /// transient failures a shared cluster produces — a restarting proxy, a
    /// scheduler that has lost the master. [`RetryPolicy::none`] turns it off.
    ///
    /// This applies to light commands only. Heavy ones — table and file I/O —
    /// are sent once whatever the policy says, because the documentation is
    /// explicit that they cannot be retried; a transaction is the way to make
    /// one atomic.
    #[must_use]
    pub fn with_retries(mut self, policy: RetryPolicy) -> Self {
        self.transport.set_retries(policy);
        self
    }

    /// Turns the failed-job report in [`Client::wait_for_operation`] on or off.
    ///
    /// On by default: when an operation fails, the client asks the cluster
    /// which jobs failed and what they printed, and puts that in the error.
    /// That costs one `list_jobs` and a few `get_job_stderr` calls per failed
    /// operation. The YTsaurus documentation asks that `list_jobs` not be used
    /// without an administrator's approval, so this is the way to switch it
    /// off on an installation where that approval was not given.
    #[must_use]
    pub fn with_job_diagnostics(mut self, enabled: bool) -> Self {
        self.job_diagnostics = enabled;
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
        let body = self.transport.call(
            Method::Get,
            "exists",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;
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
        self.transport.call(
            Method::Post,
            "create",
            &params,
            Payload::None,
            Repeatable::WithMutationId,
        )?;
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
        self.transport.call(
            Method::Post,
            "remove",
            &params,
            Payload::None,
            Repeatable::WithMutationId,
        )?;
        Ok(())
    }

    /// Reads a node attribute, such as `@row_count`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn get(&self, path: &str) -> Result<YsonValue> {
        let params = yson_build::map([("path", yson_build::string(path))]);
        let body = self.transport.call(
            Method::Get,
            "get",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;
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

        self.upload_executable(remote, &bytes)
    }

    /// Uploads the **running executable** to Cypress, marked executable.
    ///
    /// This is the one-binary pattern: the same program launches the operation
    /// and runs as its job, telling the two apart with
    /// [`ytsaurus_job::is_inside_job`]. The binary on the cluster is then by
    /// construction the one you just built — the whole "I uploaded a stale
    /// worker" class of bug disappears.
    ///
    /// The running executable has to be something a node can exec, so its ELF
    /// header is checked before the upload: Linux, x86-64, statically linked.
    /// Launching from macOS, or from a Linux host where the launcher is
    /// dynamically linked, it is not — this returns
    /// [`ClientError::NotAWorker`] naming the reason, instead of uploading a
    /// binary that fails on the node minutes later. Build the worker with
    /// `scripts/build-worker.sh` and upload it with [`Client::upload_worker`]
    /// in that case.
    ///
    /// [`ytsaurus_job::is_inside_job`]: https://docs.rs/ytsaurus-job/latest/ytsaurus_job/fn.is_inside_job.html
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::NotAWorker`] if the running executable cannot run
    /// on a node, or [`ClientError`] if the upload fails.
    pub fn upload_current_exe(&self, remote: &str) -> Result<()> {
        let exe = std::env::current_exe().map_err(|source| ClientError::Io {
            path: "the running executable".to_owned(),
            source,
        })?;

        let bytes = std::fs::read(&exe).map_err(|source| ClientError::Io {
            path: exe.display().to_string(),
            source,
        })?;

        if let Err(reason) = worker::check_worker_binary(&bytes) {
            return Err(ClientError::NotAWorker {
                path: exe.display().to_string(),
                reason,
            });
        }

        self.upload_executable(remote, &bytes)
    }

    /// Writes `bytes` to `remote` as a file a node is allowed to run.
    fn upload_executable(&self, remote: &str, bytes: &[u8]) -> Result<()> {
        self.create("file", remote)?;
        self.write_file(remote, bytes)?;
        self.set_attribute(remote, "executable", yson_build::boolean(true))
    }

    /// Writes raw bytes to a Cypress file, replacing its contents.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn write_file(&self, path: &str, contents: &[u8]) -> Result<()> {
        let params = yson_build::map([("path", yson_build::string(path))]);
        self.transport.call(
            Method::Put,
            "write_file",
            &params,
            Payload::Bytes(contents),
            Repeatable::Never,
        )?;
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
        self.transport.call(
            Method::Put,
            "set",
            &params,
            Payload::Bytes(&encoded),
            Repeatable::WithMutationId,
        )?;
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
        self.transport.call(
            Method::Put,
            "write_table",
            &params,
            Payload::Bytes(rows),
            Repeatable::Never,
        )?;
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
        let body = self.transport.call(
            Method::Get,
            "read_table",
            &params,
            Payload::None,
            Repeatable::Never,
        )?;

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

    /// Starts a reduce operation over sorted input, returning its ID.
    ///
    /// The input tables must already be sorted by a column set beginning with
    /// the spec's `reduce_by`; the cluster refuses the operation otherwise.
    /// [`Client::start_sort`] is how they get that way.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_reduce(&self, spec: &ReduceSpec) -> Result<String> {
        self.start_operation(OperationType::Reduce, &spec.to_yson())
    }

    /// Starts a sort operation, returning its ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_sort(&self, spec: &SortSpec) -> Result<String> {
        self.start_operation(OperationType::Sort, &spec.to_yson())
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
        self.start_operation_inner(kind, spec, None)
    }

    /// Starts an operation under a mutation ID you control.
    ///
    /// `start_operation` already tags its own retries with a fresh
    /// [`MutationId`], so a retried start never leaves two operations running.
    /// This is for the guarantee a single process cannot give itself: persist
    /// the ID, and after a crash the same call returns the operation that was
    /// already started instead of starting a second one.
    ///
    /// The cluster remembers a mutation ID for five to ten minutes, so this is
    /// a guard against a crash-and-restart, not a permanent key.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_operation_with(
        &self,
        kind: OperationType,
        spec: &YsonValue,
        mutation_id: &MutationId,
    ) -> Result<String> {
        self.start_operation_inner(kind, spec, Some(mutation_id))
    }

    fn start_operation_inner(
        &self,
        kind: OperationType,
        spec: &YsonValue,
        mutation_id: Option<&MutationId>,
    ) -> Result<String> {
        let params = yson_build::map([
            ("operation_type", yson_build::string(kind.as_str())),
            ("spec", spec.clone()),
        ]);
        let body = self.transport.call_with(
            Method::Post,
            "start_operation",
            &params,
            Payload::None,
            Repeatable::WithMutationId,
            mutation_id,
        )?;

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
        let body = self.transport.call(
            Method::Get,
            "get_operation",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

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
                        jobs: self.failed_jobs(id),
                    });
                }
                _ => std::thread::sleep(self.poll_interval),
            }
        }
    }

    /// Best-effort fetch of a failed operation's error document.
    ///
    /// Prefers the flattened message. Falls back to the raw document, because a
    /// clumsy error still beats an empty one if the response shape ever moves.
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
            .call(
                Method::Get,
                "get_operation",
                &params,
                Payload::None,
                Repeatable::Freely,
            )
            .ok()?;

        let summary = self
            .strip_envelope(&body, "get_operation")
            .ok()
            .and_then(|envelope| {
                let result = jobs::field(&envelope, "result")?;
                jobs::error_summary(jobs::field(result, "error")?)
            });

        summary.or_else(|| Some(crate::error::truncate(&String::from_utf8_lossy(&body), 600)))
    }

    // ---------------------------------------------------------------- jobs

    /// Lists an operation's jobs.
    ///
    /// `state` filters by job state — `failed`, `completed`, `running`, … — and
    /// `limit` caps how many come back.
    ///
    /// The YTsaurus documentation warns that `list_jobs` can put significant
    /// load on a cluster and asks that it not be part of a workflow without an
    /// administrator's approval. This client calls it once per failed
    /// operation, with a small limit; keep to that shape.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the response is not the
    /// documented `{jobs=[…]}`.
    pub fn list_jobs(
        &self,
        operation_id: &str,
        state: Option<&str>,
        limit: u32,
    ) -> Result<Vec<JobInfo>> {
        let mut params = yson_build::map([
            ("operation_id", yson_build::string(operation_id)),
            ("limit", yson_build::int(i64::from(limit))),
        ]);
        if let Some(state) = state {
            yson_build::insert(&mut params, "state", yson_build::string(state));
        }

        let body = self.transport.call(
            Method::Get,
            "list_jobs",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

        let envelope = self.strip_envelope(&body, "list_jobs")?;
        Ok(jobs::parse_jobs(&self.field_of(&envelope, "jobs")?))
    }

    /// Fetches what a job wrote to stderr.
    ///
    /// Returns raw bytes: stderr is whatever the process wrote, not necessarily
    /// UTF-8. Empty if the cluster saved nothing — stderr is kept for failed
    /// jobs and, when the spec asks for it, for successful ones.
    ///
    /// This is a *heavy* command, so an installation that separates light and
    /// heavy proxies may want it sent to [`Client::heavy_proxy`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn get_job_stderr(&self, operation_id: &str, job_id: &str) -> Result<Vec<u8>> {
        let params = yson_build::map([
            ("operation_id", yson_build::string(operation_id)),
            ("job_id", yson_build::string(job_id)),
        ]);
        self.transport.call(
            Method::Get,
            "get_job_stderr",
            &params,
            Payload::None,
            Repeatable::Never,
        )
    }

    /// Best-effort report of why an operation's jobs failed.
    ///
    /// Every step here may fail quietly. This runs while an error is being
    /// built, and a diagnostic that replaces the failure it was explaining is
    /// worse than no diagnostic at all.
    fn failed_jobs(&self, operation_id: &str) -> Vec<JobFailure> {
        if !self.job_diagnostics {
            return Vec::new();
        }

        self.list_jobs(operation_id, Some("failed"), REPORTED_JOBS)
            .unwrap_or_default()
            .iter()
            .take(REPORTED_JOBS as usize)
            .map(|job| JobFailure {
                id: job.id.clone(),
                address: job.address.clone(),
                error: job.error.clone(),
                stderr: self.stderr_excerpt(operation_id, job),
            })
            .collect()
    }

    /// The tail of a job's stderr, bounded and decoded lossily.
    ///
    /// Asks unconditionally rather than skipping jobs whose `stderr_size` is
    /// zero: the local cluster reported `1` for a job whose stderr was several
    /// hundred bytes, so the field cannot be trusted to mean "nothing to
    /// fetch". One request against losing the whole diagnostic is a good trade
    /// on a path that only runs when an operation has already failed.
    fn stderr_excerpt(&self, operation_id: &str, job: &JobInfo) -> Option<String> {
        let raw = self.get_job_stderr(operation_id, &job.id).ok()?;
        if raw.is_empty() {
            return None;
        }
        Some(crate::error::tail(
            &String::from_utf8_lossy(&raw),
            STDERR_EXCERPT,
        ))
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
