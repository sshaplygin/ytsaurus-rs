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
//! # All at once, or not at all
//!
//! Each step above can fail halfway and leave something behind — an empty
//! table, a stale worker, an output table holding neither the old result nor
//! the new one. [`Client::start_transaction`] makes the whole sequence one
//! event: nothing it does is visible until [`Transaction::commit`], and
//! dropping the handle aborts it, so a `?` on any line leaves the cluster as it
//! was.
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
/// Cypress locks.
pub mod lock;
mod retry;
/// Table schemas.
pub mod schema;
mod spec;
mod transaction;
mod worker;
/// Constructors for YSON documents, for specs this crate does not model.
pub mod yson_build;

pub use crate::error::{ClientError, Result};
pub use crate::jobs::{JobFailure, JobInfo};
pub use crate::lock::{Lock, LockMode};
pub use crate::retry::{MutationId, RetryPolicy};
pub use crate::schema::{Column, ColumnType, SortOrder, TableRow, TableSchema};
// The derive and the trait share a name, as `serde::Serialize` does: they live
// in different namespaces, and a user wants both under one import.
pub use crate::spec::{
    MapReduceSpec, MapSpec, OperationType, ReduceSpec, SortSpec, VanillaSpec, VanillaTask,
};
pub use crate::transaction::Transaction;
#[cfg(feature = "derive")]
pub use ytsaurus_helpers::TableRow;

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

/// Where the cluster's file cache lives.
///
/// The path the Python wrapper uses, so a cache an installation already
/// maintains — and already expires entries from — is the one this client uses
/// too.
const DEFAULT_FILE_CACHE: &str = "//tmp/yt_wrapper/file_storage/new_cache";

/// A worker binary on the cluster, as [`Client::upload_worker_cached`] left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedFile {
    /// Cypress path to reference from a spec.
    pub path: String,
    /// The name to give it in the job's sandbox.
    ///
    /// The cached node is named after the file's hash, so a command like
    /// `./my_job` needs this passed to
    /// [`MapSpec::with_local_file_named`].
    pub name: String,
    /// Whether this call had to upload it. `false` is a cache hit.
    pub uploaded: bool,
}

/// A connection to one YTsaurus cluster.
#[derive(Debug, Clone)]
pub struct Client {
    transport: Transport,
    poll_interval: Duration,
    job_diagnostics: bool,
    file_cache: String,
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
            file_cache: DEFAULT_FILE_CACHE.to_owned(),
        }
    }

    /// Connects to `proxy` using `token` for authentication.
    #[must_use]
    pub fn with_token(proxy: &str, token: impl Into<String>) -> Self {
        Self {
            transport: Transport::new(proxy, Some(token.into()), DEFAULT_TIMEOUT),
            poll_interval: DEFAULT_POLL_INTERVAL,
            job_diagnostics: true,
            file_cache: DEFAULT_FILE_CACHE.to_owned(),
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

    /// Overrides where [`Client::upload_worker_cached`] keeps its files.
    ///
    /// Defaults to the path the Python wrapper uses, so the cache is shared
    /// with whatever else the installation runs — and whatever expiry its
    /// administrators have set applies here too.
    #[must_use]
    pub fn with_file_cache(mut self, path: impl Into<String>) -> Self {
        self.file_cache = path.into();
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

    /// Binds this client to an existing transaction.
    ///
    /// Every command it then sends happens inside that transaction. This is the
    /// low-level door: [`Client::start_transaction`] is the one that starts a
    /// transaction, keeps it alive and aborts it if the work does not finish.
    /// Use this to rejoin a transaction whose ID came from somewhere else — a
    /// parent process, or a previous run.
    ///
    /// Nothing pings the transaction on this path, so it expires on the
    /// cluster's schedule unless its owner is pinging it.
    #[must_use]
    pub fn with_transaction(mut self, id: impl Into<String>) -> Self {
        self.transport.set_transaction(Some(id.into()));
        self
    }

    /// The transaction this client is bound to, if any.
    #[must_use]
    pub fn transaction_id(&self) -> Option<&str> {
        self.transport.transaction()
    }

    /// Starts a transaction, and keeps it alive while the handle lives.
    ///
    /// Everything sent through the returned [`Transaction`] is invisible to
    /// everything else until it commits, and is discarded if it does not:
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// # let rows: Vec<u8> = Vec::new();
    /// let tx = client.start_transaction()?;
    ///
    /// tx.create("table", "//tmp/out")?;   // no one else can see it yet
    /// tx.write_table("//tmp/out", &rows)?;
    ///
    /// tx.commit()?;                       // and now everyone can
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The transaction lasts 30 seconds without a ping — the cluster's own
    /// default — and the handle pings it every ten, so an operation that runs
    /// for an hour is fine. [`Client::start_transaction_with`] changes the
    /// timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the transaction cannot be started.
    pub fn start_transaction(&self) -> Result<Transaction> {
        Transaction::start(self, transaction::DEFAULT_TRANSACTION_TIMEOUT)
    }

    /// Starts a transaction that expires `timeout` after its last ping.
    ///
    /// The handle pings three times per timeout, so this is about what happens
    /// when the handle is *gone*: how long the transaction holds its locks
    /// after the process holding it dies without aborting. Shorter frees them
    /// sooner; longer survives a longer pause.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the transaction cannot be started.
    pub fn start_transaction_with(&self, timeout: Duration) -> Result<Transaction> {
        Transaction::start(self, timeout)
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
        // `{"value"=%false;}` — the envelope key is `value`, as it is for
        // `get`, not the command's own name. Asking for `exists` here failed
        // every call with a decode error, and nothing in the crate called this
        // until transactions needed to ask whether a node had survived one.
        Ok(matches!(
            self.value_field(&body, "value")?.node,
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

    /// Creates a table with a schema.
    ///
    /// A schematised table is checked on every write, stores its columns in
    /// their own types, and can be sorted and merged; an unschematised one
    /// takes anything and finds out later.
    ///
    /// ```no_run
    /// # use ytsaurus_client::{Client, Column, ColumnType, TableSchema};
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// let schema = TableSchema::new([
    ///     Column::new("host", ColumnType::Utf8).required().key(),
    ///     Column::new("size", ColumnType::Int64).required(),
    /// ]);
    /// client.create_table("//tmp/visits", &schema)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Unlike [`Client::create`], this **fails if the path already exists**.
    /// That is deliberate: the cluster ignores the attributes of a create it
    /// skips, so an `ignore_existing` version of this would quietly leave the
    /// old table with the old schema and report success. Changing the schema of
    /// a table that exists is `alter_table`'s job.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the schema is one the cluster would
    /// refuse, or [`ClientError`] if the request fails.
    pub fn create_table(&self, path: &str, schema: &TableSchema) -> Result<()> {
        // Locally first: the same rules, but as one sentence naming the column
        // rather than a nested error document from the cluster.
        schema
            .validate()
            .map_err(|reason| ClientError::Config(format!("{path}: {reason}")))?;

        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("type", yson_build::string("table")),
            ("recursive", yson_build::boolean(true)),
            // The schema goes *inside* `attributes`. A top-level `schema` here
            // is accepted, answered with 200 and a node id, and silently
            // ignored — the table comes back with an empty weak schema. This
            // is the single worst mistake available in this command.
            (
                "attributes",
                yson_build::map([("schema", schema.to_yson())]),
            ),
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

    /// The schema of a table, as the cluster stores it.
    ///
    /// Returns the raw YSON: the cluster answers with more than it was given —
    /// every column carries `required`, `type` *and* `type_v3` whichever was
    /// written, and the keys come back in alphabetical order.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn table_schema(&self, path: &str) -> Result<YsonValue> {
        self.get(&format!("{path}/@schema"))
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

    /// The names of a node's children.
    ///
    /// **Not sorted.** The order is the cluster's own and has no meaning; a
    /// listing of three dated tables came back as the second, the third and
    /// then the first. Sort it if the order matters.
    ///
    /// A path that is not a map node is an error rather than an empty list —
    /// `"List" method is not supported` — and so is a path that does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, or if the cluster marks
    /// the answer `incomplete`: a listing that is silently short is worse than
    /// no listing.
    pub fn list(&self, path: &str) -> Result<Vec<String>> {
        let params = yson_build::map([("path", yson_build::string(path))]);
        let body = self.transport.call(
            Method::Get,
            "list",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

        child_names(&self.value_field(&body, "value")?, path)
    }

    /// Copies a node, creating missing parents.
    ///
    /// Fails if `destination` exists; [`Client::copy_replacing`] is the one that
    /// overwrites.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn copy(&self, source: &str, destination: &str) -> Result<()> {
        self.transfer("copy", source, destination, false)
    }

    /// Copies a node over whatever is at `destination`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn copy_replacing(&self, source: &str, destination: &str) -> Result<()> {
        self.transfer("copy", source, destination, true)
    }

    /// Moves a node, creating missing parents.
    ///
    /// Fails if `destination` exists; [`Client::move_replacing`] is the one that
    /// overwrites, and the pair is how a result is published: write a staging
    /// table, then move it over the live one.
    ///
    /// Named `move_node` because `move` is a Rust keyword, and `client.r#move`
    /// at every call site would be a worse tax than the four extra characters.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn move_node(&self, source: &str, destination: &str) -> Result<()> {
        self.transfer("move", source, destination, false)
    }

    /// Moves a node over whatever is at `destination`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn move_replacing(&self, source: &str, destination: &str) -> Result<()> {
        self.transfer("move", source, destination, true)
    }

    fn transfer(&self, command: &str, source: &str, destination: &str, force: bool) -> Result<()> {
        let params = yson_build::map([
            ("source_path", yson_build::string(source)),
            ("destination_path", yson_build::string(destination)),
            ("recursive", yson_build::boolean(true)),
            ("force", yson_build::boolean(force)),
        ]);
        self.transport.call(
            Method::Post,
            command,
            &params,
            Payload::None,
            Repeatable::WithMutationId,
        )?;
        Ok(())
    }

    /// Creates a link at `link_path` pointing at `target`.
    ///
    /// A link resolves to its target, so `//tmp/latest/@row_count` reads the
    /// target's row count. To ask about the link itself, put `&` after its path:
    /// `//tmp/latest&/@target_path`. Without the `&` the question goes through
    /// to the target and is answered as if the link were not there.
    ///
    /// Fails if `link_path` exists; [`Client::link_replacing`] is what points an
    /// existing link somewhere else.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn link(&self, target: &str, link_path: &str) -> Result<()> {
        self.link_inner(target, link_path, false)
    }

    /// Points a link at `target`, replacing whatever is at `link_path`.
    ///
    /// The `//tmp/thing/latest` pattern: publish under a dated name, then move
    /// the link. Readers that follow the link see the old version until this
    /// call and the new one after it, and never a half-written table.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn link_replacing(&self, target: &str, link_path: &str) -> Result<()> {
        self.link_inner(target, link_path, true)
    }

    fn link_inner(&self, target: &str, link_path: &str, force: bool) -> Result<()> {
        let params = yson_build::map([
            ("target_path", yson_build::string(target)),
            ("link_path", yson_build::string(link_path)),
            ("recursive", yson_build::boolean(true)),
            ("force", yson_build::boolean(force)),
        ]);
        self.transport.call(
            Method::Post,
            "link",
            &params,
            Payload::None,
            Repeatable::WithMutationId,
        )?;
        Ok(())
    }

    /// Takes a lock, or fails because somebody else holds one.
    ///
    /// Only inside a transaction: a lock lives as long as the transaction that
    /// took it, and there is nothing else for it to belong to. A client that is
    /// not in one is told so here rather than by the cluster.
    ///
    /// The failure is worth reading — it names the transaction that won:
    ///
    /// ```text
    /// Cannot take "exclusive" lock for node //tmp/live since "exclusive" lock
    /// is taken by concurrent transaction 4-dac2-10001-eb1b
    /// ```
    ///
    /// [`Client::lock_waiting`] queues for it instead of failing.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if this client is not in a transaction,
    /// or [`ClientError`] if the lock is refused.
    pub fn lock(&self, path: &str, mode: LockMode) -> Result<Lock> {
        self.lock_inner(path, mode, false)
    }

    /// Queues for a lock, and waits until it is held.
    ///
    /// A waitable lock is **granted later, or never** — the cluster answers
    /// immediately with a lock that is `pending`, and it becomes `acquired` when
    /// the transactions ahead of it end. Returning that lock as though it were
    /// held is the mistake this command exists to make impossible: this polls
    /// until the cluster says `acquired`, and gives up after `wait_for`.
    ///
    /// The deadline is not a nicety. A request can queue for something that will
    /// never happen and the cluster will not say so: a transaction that already
    /// holds a snapshot lock on the node is refused an exclusive one outright,
    /// but the *waitable* version of the same request is queued behind a lock
    /// only that transaction's own end will release.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if this client is not in a transaction or
    /// the wait ran out, or [`ClientError`] if a request fails. A lock that is
    /// still queued when the wait runs out stays queued until the transaction
    /// ends.
    pub fn lock_waiting(&self, path: &str, mode: LockMode, wait_for: Duration) -> Result<Lock> {
        let lock = self.lock_inner(path, mode, true)?;
        let deadline = Instant::now() + wait_for;

        loop {
            let state = self.get(&format!("#{}/@state", lock.id))?;
            if state.as_str() == Some("acquired") {
                return Ok(lock);
            }

            if Instant::now() >= deadline {
                return Err(ClientError::Config(format!(
                    "lock on {path}: still {} after {:.0}s — the locks ahead of it are \
                     still held, which can include a snapshot lock this same \
                     transaction took. It stays queued until this transaction ends.",
                    state.as_str().unwrap_or("queued"),
                    wait_for.as_secs_f64()
                )));
            }
            std::thread::sleep(self.poll_interval);
        }
    }

    fn lock_inner(&self, path: &str, mode: LockMode, waitable: bool) -> Result<Lock> {
        if self.transaction_id().is_none() {
            return Err(ClientError::Config(format!(
                "lock {path}: a lock belongs to a transaction, and this client is not in \
                 one — take it through a Client::start_transaction handle. The cluster \
                 answers this with `A valid master transaction is required`."
            )));
        }

        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("mode", yson_build::string(mode.as_str())),
            ("waitable", yson_build::boolean(waitable)),
        ]);
        let body = self.transport.call(
            Method::Post,
            "lock",
            &params,
            Payload::None,
            Repeatable::WithMutationId,
        )?;

        let envelope = self.strip_envelope(&body, "lock")?;
        let text = |key: &str| -> Result<String> {
            match &self.field_of(&envelope, key)?.node {
                YsonNode::String(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
                other => Err(ClientError::Decode {
                    command: "lock".to_owned(),
                    reason: format!("{key} is not a string: {other:?}"),
                }),
            }
        };

        Ok(Lock {
            id: text("lock_id")?,
            node_id: text("node_id")?,
        })
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

    /// Uploads a worker, or finds it already on the cluster.
    ///
    /// Keyed by the file's MD5, so an unchanged binary is uploaded once and
    /// every later launch reuses it. That is the difference between a dev loop
    /// that re-sends tens of megabytes on every run and one that does not.
    ///
    /// The cached node is named after the hash, so the returned
    /// [`CachedFile::name`] is the name to give it in the sandbox — see
    /// [`MapSpec::with_local_file_named`]:
    ///
    /// ```no_run
    /// # use ytsaurus_client::{Client, MapSpec};
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// let worker = client.upload_worker_cached("target/.../my_job")?;
    /// let spec = MapSpec::new("./my_job", ["//tmp/in"], ["//tmp/out"])
    ///     .with_local_file_named(&worker.path, &worker.name);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The cache is shared: [`Client::with_file_cache`] defaults to the path
    /// the Python wrapper uses, so an installation that already expires old
    /// entries there expires these too.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the file cannot be read or the upload fails.
    pub fn upload_worker_cached(&self, local: impl AsRef<std::path::Path>) -> Result<CachedFile> {
        let local = local.as_ref();
        let bytes = std::fs::read(local).map_err(|source| ClientError::Io {
            path: local.display().to_string(),
            source,
        })?;

        let name = local
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "worker".to_owned());
        let digest = format!("{:x}", md5::compute(&bytes));

        if let Some(path) = self.file_from_cache(&digest)? {
            return Ok(CachedFile {
                path,
                name,
                uploaded: false,
            });
        }

        // Staged inside the cache node, so a cluster that expires the cache
        // expires an interrupted upload with it.
        let staging = format!("{}/staged_{digest}", self.file_cache);
        self.create("file", &staging)?;
        self.write_file_computing_md5(&staging, &bytes)?;
        self.set_attribute(&staging, "executable", yson_build::boolean(true))?;

        let path = self.put_file_to_cache(&staging, &digest)?;
        // The cache may keep the node itself rather than a copy of it, so this
        // is `force`-removing something that may already be gone. `remove`
        // tolerates that.
        self.remove(&staging)?;
        // Set on the cached path too: whether the attribute survives the move
        // decides whether the job can exec at all, and it is cheap to be sure.
        self.set_attribute(&path, "executable", yson_build::boolean(true))?;

        Ok(CachedFile {
            path,
            name,
            uploaded: true,
        })
    }

    /// Looks up a file in the cluster's file cache by its MD5.
    ///
    /// `None` means nothing is cached under that hash.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn file_from_cache(&self, md5: &str) -> Result<Option<String>> {
        self.create("map_node", &self.file_cache)?;

        let params = yson_build::map([
            ("md5", yson_build::string(md5)),
            ("cache_path", yson_build::string(&self.file_cache)),
        ]);
        let body = self.transport.call(
            Method::Get,
            "get_file_from_cache",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

        self.cached_path(&body, "get_file_from_cache")
    }

    /// Hands a file already written to Cypress to the file cache.
    ///
    /// The cluster verifies that the node's MD5 is the one given, which is why
    /// it must have been written with `compute_md5`. Returns the path the file
    /// now lives at.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn put_file_to_cache(&self, path: &str, md5: &str) -> Result<String> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("md5", yson_build::string(md5)),
            ("cache_path", yson_build::string(&self.file_cache)),
        ]);
        let body = self.transport.call(
            Method::Post,
            "put_file_to_cache",
            &params,
            Payload::None,
            Repeatable::WithMutationId,
        )?;

        self.cached_path(&body, "put_file_to_cache")?
            .ok_or_else(|| ClientError::Decode {
                command: "put_file_to_cache".to_owned(),
                reason: "the cluster returned no path for the cached file".to_owned(),
            })
    }

    /// Reads the path out of a file-cache response.
    ///
    /// These two commands answer with a **bare string**, not the `{path=…}`
    /// envelope the rest of API v4 uses, and a cache miss is an *empty* string
    /// rather than an error or an entity. Both shapes are accepted so that a
    /// cluster that grows an envelope later does not break this.
    fn cached_path(&self, body: &[u8], command: &str) -> Result<Option<String>> {
        let value = self.strip_envelope(body, command)?;
        let value = match &value.node {
            YsonNode::Map(_) => self.field_of(&value, "path")?,
            _ => value,
        };

        match &value.node {
            YsonNode::String(bytes) if !bytes.is_empty() => {
                Ok(Some(String::from_utf8_lossy(bytes).into_owned()))
            }
            YsonNode::String(_) | YsonNode::Entity => Ok(None),
            other => Err(ClientError::Decode {
                command: command.to_owned(),
                reason: format!("the cached path is not a string: {other:?}"),
            }),
        }
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
        self.write_file_inner(path, contents, false)
    }

    /// As `write_file`, asking the cluster to record the file's MD5 — which is
    /// what `put_file_to_cache` then checks against.
    fn write_file_computing_md5(&self, path: &str, contents: &[u8]) -> Result<()> {
        self.write_file_inner(path, contents, true)
    }

    fn write_file_inner(&self, path: &str, contents: &[u8], compute_md5: bool) -> Result<()> {
        let mut params = yson_build::map([("path", yson_build::string(path))]);
        if compute_md5 {
            yson_build::insert(&mut params, "compute_md5", yson_build::boolean(true));
        }

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

    /// Starts a vanilla operation, returning its ID.
    ///
    /// Jobs with no input tables: a distributed process, a side-car
    /// computation, anything that is not a transformation of a table.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_vanilla(&self, spec: &VanillaSpec) -> Result<String> {
        self.start_operation(OperationType::Vanilla, &spec.to_yson())
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

    /// The custom statistics an operation's jobs reported.
    ///
    /// Returns the `custom` subtree of the operation's job statistics, keyed by
    /// the names the jobs used. Each leaf is an aggregate — `sum`, `count`,
    /// `min`, `max` — over the jobs that reported it, so a per-row counter
    /// comes back as one number for the whole operation.
    /// [`Client::statistic_sum`] pulls a single total out of it.
    ///
    /// Empty if no job reported anything.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn custom_statistics(&self, operation_id: &str) -> Result<YsonValue> {
        let params = yson_build::map([
            ("operation_id", yson_build::string(operation_id)),
            (
                "attributes",
                yson_build::list([yson_build::string("progress")]),
            ),
        ]);
        let body = self.transport.call(
            Method::Get,
            "get_operation",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

        let envelope = self.strip_envelope(&body, "get_operation")?;
        let custom = jobs::field(&envelope, "progress")
            .and_then(|p| jobs::field(p, "job_statistics"))
            .and_then(|s| jobs::field(s, "custom"))
            .cloned();

        Ok(custom.unwrap_or(YsonValue {
            attributes: None,
            node: YsonNode::Map(std::collections::BTreeMap::new()),
        }))
    }

    /// The total of one custom statistic over an operation's completed jobs.
    ///
    /// `name` is exactly what the job called it, slashes included: the cluster
    /// keeps `rows/rejected` as one key rather than nesting it.
    ///
    /// Only `completed` jobs are counted. An aborted job's work is done again
    /// by its replacement, so including it would count the same rows twice.
    /// Job *types* are summed together, so a map-reduce reporting one name from
    /// both phases gives the operation's total.
    ///
    /// `None` means no job reported that name — which is not the same as zero.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn statistic_sum(&self, operation_id: &str, name: &str) -> Result<Option<i64>> {
        let statistics = self.custom_statistics(operation_id)?;
        Ok(jobs::field(&statistics, name).and_then(completed_total))
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

/// Reads the child names out of a `list` answer.
///
/// A truncated answer is an error rather than a short list. The cluster says so
/// with `<incomplete=%true>` — an *attribute* on the list, not an error — and a
/// caller who does not look gets a listing that is quietly missing entries.
fn child_names(value: &YsonValue, path: &str) -> Result<Vec<String>> {
    if matches!(
        value.attr("incomplete").map(|v| &v.node),
        Some(YsonNode::Boolean(true))
    ) {
        return Err(ClientError::Decode {
            command: "list".to_owned(),
            reason: format!(
                "{path} has more children than the cluster would list at once, so the \
                 answer it gave is not all of them"
            ),
        });
    }

    let YsonNode::List(items) = &value.node else {
        return Err(ClientError::Decode {
            command: "list".to_owned(),
            reason: format!("{path}: the answer is not a list: {:?}", value.node),
        });
    };

    items
        .iter()
        .map(|item| match &item.node {
            YsonNode::String(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
            other => Err(ClientError::Decode {
                command: "list".to_owned(),
                reason: format!("{path}: a child name is not a string: {other:?}"),
            }),
        })
        .collect()
}

/// Totals one custom statistic over the jobs that completed.
///
/// The cluster files a statistic as `$` → job state → job type → the
/// aggregate, so the number a user means by "how many rows did we reject" is
/// the `sum` of the `completed` jobs, added across job types. Captured from a
/// local cluster:
///
/// ```text
/// {"rows/rejected"={"$"={completed={map={count=1;max=3;min=3;sum=3}}}}}
/// ```
///
/// A flatter shape is accepted too, so a cluster that reports a bare aggregate
/// still yields a number rather than nothing.
fn completed_total(statistic: &YsonValue) -> Option<i64> {
    let Some(by_state) = jobs::field(statistic, "$") else {
        return jobs::field(statistic, "sum").and_then(YsonValue::as_i64);
    };

    let completed = jobs::field(by_state, "completed")?;
    let YsonNode::Map(by_type) = &completed.node else {
        return None;
    };

    let mut total: Option<i64> = None;
    for per_type in by_type.values() {
        if let Some(sum) = jobs::field(per_type, "sum").and_then(YsonValue::as_i64) {
            total = Some(total.unwrap_or(0) + sum);
        }
    }
    total
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
    fn a_listing_is_the_names_in_the_order_given() {
        let value = from_slice(br#"["t1";"t2";]"#, YsonFormat::Text).expect("valid YSON");
        assert_eq!(child_names(&value, "//tmp/x").unwrap(), ["t1", "t2"]);
    }

    #[test]
    fn a_truncated_listing_is_an_error_rather_than_a_short_list() {
        // What `max_size` produces, and what a node with too many children
        // produces on its own. The marker is an attribute on the list, so a
        // caller who does not look gets a listing quietly missing entries.
        let value =
            from_slice(br#"<"incomplete"=%true;>["t1";]"#, YsonFormat::Text).expect("valid YSON");

        let err = child_names(&value, "//tmp/x").expect_err("must not pass as a listing");
        assert!(err.to_string().contains("not all of them"), "{err}");
    }

    /// What a local cluster answers `exists` with, captured verbatim.
    const EXISTS_RESPONSE: &[u8] = br#"{"value"=%false;}"#;

    #[test]
    fn an_exists_answer_is_read_out_of_the_value_key() {
        let client = Client::new("http://localhost:8000");

        let value = client
            .value_field(EXISTS_RESPONSE, "value")
            .expect("the answer is an envelope around `value`");
        assert!(matches!(value.node, YsonNode::Boolean(false)));

        // The command's own name is not a key in its answer. Looking for it
        // there failed every call to `exists` with a decode error, for as long
        // as nothing in the crate called `exists`.
        assert!(client.value_field(EXISTS_RESPONSE, "exists").is_err());
    }

    /// The exact document a local cluster returned for a job that reported
    /// three statistics.
    const CUSTOM_STATISTICS: &str = r#"{
        "bytes/read" = {"$" = {completed = {map = {count=1;max=147;min=147;sum=147}}}};
        "rows/read" = {"$" = {completed = {map = {count=1;max=7;min=7;sum=7}}}};
        "rows/rejected" = {"$" = {completed = {map = {count=1;max=3;min=3;sum=3}}}};
    }"#;

    fn statistics() -> YsonValue {
        from_slice(CUSTOM_STATISTICS.as_bytes(), YsonFormat::Text).expect("valid YSON")
    }

    #[test]
    fn a_statistic_totals_over_completed_jobs() {
        let all = statistics();

        // The name keeps its slash: the cluster stores it as one key rather
        // than nesting it, which a path-walking lookup would miss entirely.
        assert_eq!(
            jobs::field(&all, "rows/rejected").and_then(completed_total),
            Some(3)
        );
        assert_eq!(
            jobs::field(&all, "bytes/read").and_then(completed_total),
            Some(147)
        );
        assert_eq!(jobs::field(&all, "rows").and_then(completed_total), None);
    }

    #[test]
    fn job_types_are_summed_and_other_states_are_not() {
        // A map-reduce reports one name from both phases; an aborted job's
        // work is redone by its replacement, so counting it would double.
        let value = from_slice(
            br#"{"$" = {
                    completed = {map = {sum=10}; partition_reduce = {sum=5}};
                    aborted   = {map = {sum=99}};
                }}"#,
            YsonFormat::Text,
        )
        .expect("valid YSON");

        assert_eq!(completed_total(&value), Some(15));
    }

    #[test]
    fn a_flat_aggregate_still_yields_a_number() {
        let value =
            from_slice(b"{count=1;max=7;min=7;sum=7}", YsonFormat::Text).expect("valid YSON");
        assert_eq!(completed_total(&value), Some(7));
    }

    #[test]
    fn an_operation_whose_jobs_all_failed_totals_nothing() {
        let value = from_slice(br#"{"$" = {failed = {map = {sum=4}}}}"#, YsonFormat::Text)
            .expect("valid YSON");
        assert_eq!(completed_total(&value), None);
    }

    #[test]
    fn from_env_explains_itself_when_unconfigured() {
        // Not asserting on process env, only that the message is actionable.
        let err = ClientError::Config("YT_PROXY is not set".to_owned());
        assert!(err.to_string().contains("YT_PROXY"));
    }
}
