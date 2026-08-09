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
//! [`Client::from_env`] reads `YT_PROXY` for the cluster address, and finds a
//! token the way the `yt` CLI does: `YT_TOKEN`, then the file named by
//! `YT_TOKEN_PATH`, then `~/.yt/token`. A machine where the CLI already works
//! needs nothing else. A bare host is assumed to be HTTPS; a local cluster is
//! reached as `http://localhost:8000`.
//!
//! `YT_CA_BUNDLE` names a PEM file of root certificates, for an installation
//! whose certificate chains to a CA the Mozilla bundle has never heard of. It
//! is read by any build with the `tls` feature — which is the default, and the
//! only kind that has a handshake to configure — and the `platform-verifier`
//! feature is the same answer without a variable to set. Every block in the
//! file must be an X.509 certificate: one that is not, a `.p7b` re-armoured
//! under a `BEGIN CERTIFICATE` label being the usual case, refuses the whole
//! file rather than becoming a root store quietly shorter than the caller
//! wrote down.
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
//! # After it has started
//!
//! An operation can be paused, given more of its pool, finished early, found by
//! the alias its spec gave it, and — the one that matters for a pipeline that
//! restarts — picked up again by a process that did not start it:
//!
//! ```no_run
//! # use ytsaurus_client::{Client, OperationParameters};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = Client::from_env()?;
//! let op = client.attach_operation(std::fs::read_to_string("run.id")?);
//!
//! op.suspend(false)?;
//! op.update_parameters(&OperationParameters::new().with_weight(2.0))?;
//! op.resume()?;
//! op.wait()?;
//! # Ok(())
//! # }
//! ```
//!
//! Everything on [`Operation`] is also on [`Client`], taking the id. See the
//! [`operation`] module for what the cluster does and does not promise about
//! each of those commands — some of it is surprising, and all of it was
//! measured.
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
//! A transaction can also outlive its handle: [`Transaction::detach`] stops
//! the keep-alive and leaves it running, [`Client::attach_transaction`] turns
//! the id back into a handle elsewhere, and [`Client::ping_transaction`],
//! [`Client::commit_transaction`] and [`Client::abort_transaction`] finish one
//! from a process that holds nothing but the id.
//!
//! # Seeing what it did
//!
//! The cluster traces itself, so joining its trace costs a header and no
//! dependency: [`Client::with_trace_context`] puts every request into the
//! trace a [`TraceContext`] names, and the proxy's own span for that request
//! is placed inside it rather than starting an orphan.
//!
//! This process's own side is the `tracing` feature, off by default: with it,
//! each attempt runs in a span carrying the command, the attempt number and
//! the elapsed time, and the message a retry prints on stderr becomes a `WARN`
//! event instead. It is off because this crate is linked into worker binaries
//! cross-compiled to musl — the same reason `tls` is.
//!
//! # Heavy commands go where the cluster says
//!
//! Table and file data — [`Client::write_table`], [`Client::read_table`],
//! [`Client::write_file`], [`Client::upload_worker`] and the streaming forms of
//! each — is what YTsaurus calls a *heavy* command, and a large installation
//! serves those on a separate set of proxies. This client asks `/hosts` the
//! first time it sends a heavy command, keeps the whole answer as a **pool**,
//! and sends each heavy command to a member **picked at random** — the way
//! both official SDKs pick, because `/hosts` is ordered by load and a client
//! that keeps one pick for its lifetime never rebalances: a draining host
//! keeps every client that ever picked it. The answer is **refreshed** when
//! it outlives [`Client::with_host_list_refresh_interval`] — a minute by
//! default, the documentation's own advice — lazily, by the heavy command
//! that finds it stale; there is no background thread, and a client that
//! stops uploading stops asking. Light commands stay on the address it was
//! configured with.
//!
//! **A proxy that fails is dropped from the pool, not committed to.** A heavy
//! command that fails for a reason attributable to the host it went to — a
//! refused connection, a 503, a certificate that does not match that host's
//! own name — takes that host out of the pool, and the next command picks
//! from what remains; a later refresh that still names the host puts it back.
//! Only a pool with nobody left in it sends the client back to the configured
//! address — and then only until it asks the cluster again, a few seconds
//! later ([`Client::with_hosts_retry_after`]). That order matters: on a
//! deployment with separate proxy roles the configured address is a *control*
//! proxy, and going back there on the first hiccup is the failure this
//! feature exists to prevent.
//!
//! **A cluster that names no heavy proxy is answered by using the configured
//! address**, which is what leaves a single-node installation working exactly
//! as it did — asked about again one refresh interval later, so a first
//! lookup that landed during a rolling restart is not a verdict for life.
//! Nor is such a cluster asked in the first place when its address
//! is on loopback: `localhost` is this machine's own cluster or a tunnel to
//! one, and the address a far-side proxy publishes for itself is not reachable
//! from either. [`Client::with_proxy_discovery`] overrides that in both
//! directions, and [`Client::heavy_proxy`] answers the question directly.
//!
//! **A discovered host is used only if it shares the configured address's own
//! domain**, and the scheme and port come from that address rather than from
//! the answer. That rule is a guard against a typo in a configuration and
//! against an obviously foreign name — not a promise about where a token can
//! end up. Steering it with a `/hosts` body means controlling that body, which
//! over `https://` means owning the proxy (which has the token already) and
//! over `http://` means being a man-in-the-middle (who reads it out of every
//! light command anyway). Where the rule does bite is a proxy registering
//! itself in the cluster's coordinator under an unintended name, and even there
//! it is coarse: sharing a parent domain on a hosting platform means sharing it
//! with every other tenant of that platform.
//! [`Client::with_heavy_proxies_in`] is the version that is a boundary — a list
//! written out on purpose — and [`Client::with_heavy_proxies_anywhere`] is the
//! opt-out for an installation whose `/hosts` genuinely names another domain.
//! When a whole answer is declined the client says so once, naming what it
//! refused and why, rather than leaving it to be deduced from a cluster error
//! later on.
//!
//! Getting this wrong does not look like a routing problem, which is why it is
//! worth spelling out what it does look like. The refusal arrives as a
//! structured YTsaurus error — `cluster error 1: Control proxy may not serve
//! heavy requests with input data` — and this crate's own error rendering does
//! not print the status beside it, which is how the status came to be recorded
//! here as 200. The cluster's own rule, from
//! `TContext::TryRedirectHeavyRequests`, turns on whether the request carries
//! input data: a heavy **write** gets **503** with `Retry-After: 60`, and a
//! heavy **read** gets a **307** to a data proxy. And a deployment **behind a
//! balancer is the case that breaks**, not the case that works: the balancer
//! fronts the control proxies, so every upload arrives at one.

#![warn(missing_docs)]

use std::time::{Duration, Instant};

/// Errors.
pub mod error;
mod http;
mod jobs;
/// Cypress locks.
pub mod lock;
mod observe;
/// The operation handle, and what its commands take and answer.
pub mod operation;
/// Table paths that carry attributes.
pub mod path;
mod retry;
/// Table schemas.
pub mod schema;
mod spec;
/// Streaming table I/O.
pub mod stream;
/// The trace a request belongs to.
pub mod trace;
mod transaction;
mod unique;
mod worker;
/// Constructors for YSON documents, for specs this crate does not model.
pub mod yson_build;

pub use crate::error::{ClientError, RedirectRefusal, Result};
pub use crate::http::Method;
pub use crate::jobs::{JobFailure, JobInfo};
pub use crate::lock::{Lock, LockMode};
pub use crate::operation::{
    Operation, OperationEvent, OperationFilter, OperationInfo, OperationList, OperationParameters,
    OperationStatus,
};
pub use crate::path::TablePath;
pub use crate::retry::{MutationId, Repeatable, RetryPolicy};
pub use crate::schema::{Column, ColumnType, SortOrder, TableRow, TableSchema};
// The derive and the trait share a name, as `serde::Serialize` does: they live
// in different namespaces, and a user wants both under one import.
pub use crate::spec::{
    EraseSpec, MapReduceSpec, MapSpec, MergeMode, MergeSpec, OperationType, ReduceSpec,
    RemoteCopySpec, SortSpec, VanillaSpec, VanillaTask,
};
pub use crate::stream::{ResponseReader, TableReader};
pub use crate::trace::TraceContext;
pub use crate::transaction::Transaction;
pub use ytsaurus_format::DataFormat;
#[cfg(feature = "derive")]
pub use ytsaurus_helpers::TableRow;
pub use ytsaurus_skiff::{
    Format as SkiffFormat, Schema as SkiffSchema, SchemaRef as SkiffSchemaRef,
    WireType as SkiffWireType,
};

use crate::http::{Payload, Transport};
use ytsaurus_skiff::Decoder as SkiffDecoder;
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

/// Where a worker goes when the file cache will not have it.
///
/// `//tmp` because it is the scratch directory an installation gives its users
/// — the cache itself lives under it — so a caller refused the cache can still
/// be expected to have this. There is nowhere further to fall: a cluster that
/// refuses this too is reported rather than worked around.
const UNCACHED_UPLOAD_DIR: &str = "//tmp";

/// `Access denied` — the cluster's code for a request no matching ACE allows.
///
/// What an installation-managed file cache answers a write with, and the whole
/// of what [`Client::upload_worker_cached`] treats as "no cache for you".
const ACCESS_DENIED: i64 = 901;

/// The `{value=…}` API v4 wraps a structured answer in.
///
/// Deserialised rather than walked, so [`Client::get_as`] reads the response
/// once. Keys the type does not mention are ignored, which is what lets the
/// envelope grow a field without breaking this.
#[derive(serde::Deserialize)]
struct Envelope<T> {
    value: T,
}

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
    /// Whether [`CachedFile::path`] is inside the shared file cache.
    ///
    /// `true` for a cache hit and for an upload the cache accepted; `false`
    /// only when the cache refused this caller and the worker went up under
    /// `//tmp` instead — see [`Client::upload_worker_cached`].
    ///
    /// **This is the field to branch on before removing anything.** The two
    /// are not the same question and neither answers the other: `uploaded`
    /// alone says the bytes were sent, which is true of both destinations, so
    /// a caller that tidies up after itself on that signal deletes the *shared
    /// cache entry* on an ordinary cluster and evicts the binary for everyone
    /// else. A caller that never tidies up leaks a node per launch on the
    /// cluster where this is `false`, since nothing expires `//tmp` uploads —
    /// which is the other half of why the fallback warns.
    pub cached: bool,
}

/// What an upload through the file cache came to.
enum Cached {
    /// It is in the cache, at this path.
    At(String),
    /// The cache refused this caller, in the cluster's own words. Carried back
    /// rather than returned as an error: see [`Client::upload_worker_cached`],
    /// which uploads outside the cache instead and says so.
    Refused(ClientError),
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

    /// Connects using `YT_PROXY`, and whatever token the environment offers.
    ///
    /// The token is looked for the way the `yt` CLI looks for it, and stops at
    /// the first that has one:
    ///
    /// 1. `YT_TOKEN`;
    /// 2. the file named by `YT_TOKEN_PATH`;
    /// 3. `~/.yt/token`.
    ///
    /// So a machine where the CLI already works needs no extra setup. A token
    /// read from a file is **trimmed**: one written with `echo` ends in a
    /// newline, and sending that produces an authentication failure that says
    /// nothing about a newline. An unreadable file is treated as no token
    /// rather than as an error, because that is what it means on a cluster that
    /// wants none.
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

        Ok(match token_from_environment() {
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

    /// Overrides the request timeout, which defaults to two minutes.
    ///
    /// For a buffered command the limit is end to end, **redirects included**:
    /// an attempt takes its deadline once and the hops it makes share what is
    /// left of it, so a proxy that redirects cannot multiply the limit by the
    /// length of the chain. A retry is a fresh attempt and gets a fresh budget,
    /// which is what [`Client::with_retries`] bounds.
    ///
    /// A streaming transfer — [`Client::read_table_streaming`],
    /// [`Client::write_table_rows`] and their kin — is not cut off mid-table:
    /// there the timeout bounds each wait *around* the data (connecting,
    /// sending the request, the response headers), and the data itself moves
    /// for as long as it takes.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.transport.set_timeout(timeout);
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

    /// Overrides whether heavy commands ask the cluster where to go.
    ///
    /// They do by default, which is what makes an upload work on an
    /// installation that separates proxy roles — unless the address this client
    /// was given is on loopback, where the lookup can only cost a round trip or
    /// name a host this process cannot reach. See the module documentation.
    ///
    /// Both overrides have a use:
    ///
    /// - `true` for a cluster reached at `localhost` that really does have
    ///   heavy proxies this process can reach — a port-forward into a real
    ///   installation, where the discovered addresses resolve;
    /// - `false` to pin every command to the address given, which is what a
    ///   balancer that already routes by role wants, and what to reach for if
    ///   the lookup itself is the thing misbehaving.
    ///
    /// This does not disturb what a client it was cloned from has already
    /// resolved.
    #[must_use]
    pub fn with_proxy_discovery(mut self, enabled: bool) -> Self {
        self.transport.set_proxy_discovery(enabled);
        self
    }

    /// Lets `/hosts` name a heavy proxy outside the configured address's own
    /// domain.
    ///
    /// **Off by default.** A discovered name is used only if it is the
    /// configured host itself or sits under that host's parent domain —
    /// `https://cluster.example.net` will follow `n0132-sas.example.net` and
    /// will not follow `n0132-sas.somewhere-else.net`. A configured name with
    /// no dots in it, which is how `YT_PROXY` is usually written, is matched as
    /// a label instead: `hume` follows `n0008-sas.hume.yt.yandex.net`. A name
    /// that is refused is passed over; a `/hosts` answer that is refused
    /// entirely leaves the upload going to the configured address, which is
    /// where it went before this client routed anything, and the client says so
    /// once rather than leaving it to be deduced.
    ///
    /// **What that rule is worth**, since it was once written down here as more
    /// than it is: it guards against a typo in a configuration and against an
    /// obviously foreign name. It is not what keeps a token where you put it.
    /// Steering a heavy command with a `/hosts` body means controlling that
    /// body — over `https://` that is owning the proxy, which has the token
    /// already, and over `http://` that is being a man-in-the-middle, who reads
    /// the token out of every light command without coming near this. Where the
    /// rule does bite is a proxy registering itself in the coordinator under an
    /// unintended name, and even there a shared parent domain on a hosting
    /// platform is shared with every tenant of it. Use
    /// [`Client::with_heavy_proxies_in`] where a real boundary is wanted.
    ///
    /// Turn it on for an installation whose `/hosts` genuinely names another
    /// domain — a cluster fronted by a vanity address, or one whose data proxies
    /// live under a separate zone. Nothing else in the client changes; the
    /// scheme still comes from the configured address, a name carrying `://`,
    /// `/`, `@` or whitespace is still refused, and the configured port still
    /// carries through.
    ///
    /// The symptom of needing it is an upload that reaches the *configured*
    /// address and is refused there — `Control proxy may not serve heavy
    /// requests with input data` — while [`Client::heavy_proxy`] shows a
    /// perfectly good address the client declined to use. The client says so
    /// itself, once, when it declines a whole `/hosts` answer, and the refusal
    /// it then collects carries the same sentence.
    ///
    /// ```
    /// use ytsaurus_client::Client;
    ///
    /// let client = Client::new("https://cluster.example.net")
    ///     .with_heavy_proxies_anywhere(true);
    /// ```
    ///
    /// **This is all or nothing**, which is why
    /// [`Client::with_heavy_proxies_in`] exists beside it: a domain rule that
    /// misses by one label should not have to be answered by removing the rule.
    /// The last of the two called is the one that decides.
    ///
    /// This does not disturb what a client it was cloned from has already
    /// resolved.
    #[must_use]
    pub fn with_heavy_proxies_anywhere(mut self, enabled: bool) -> Self {
        self.transport.set_heavy_proxies_anywhere(enabled);
        self
    }

    /// Restricts heavy commands to a list of proxies written out by hand.
    ///
    /// The third answer to "which of the names `/hosts` gives may this client
    /// send a token to", and the only one that is a boundary rather than a
    /// heuristic. The domain rule is a guard against a typo and against an
    /// obviously foreign name — it cannot be more than that without a
    /// public-suffix list, and on a shared platform a shared parent domain
    /// means very little: `yt-1234.us-east-1.elb.amazonaws.com` and every other
    /// load balancer in that region share one. A list somebody wrote on purpose
    /// does not have that problem.
    ///
    /// Names are compared **without their ports and without case**; the port a
    /// command is sent to still comes from the configured address, or from the
    /// `/hosts` entry when it carries one. Everything else in the client is
    /// unchanged: the scheme comes from the configured address, and a name
    /// carrying `://`, `/`, `@` or whitespace is still not a name.
    ///
    /// ```
    /// use ytsaurus_client::Client;
    ///
    /// let client = Client::new("https://cluster.example.net")
    ///     .with_heavy_proxies_in(["n0132-sas.example.net", "n0133-sas.example.net"]);
    /// ```
    ///
    /// An empty list admits nothing, so every heavy command stays on the
    /// configured address — [`Client::with_proxy_discovery`] is the plainer way
    /// to say that. The last of this and
    /// [`Client::with_heavy_proxies_anywhere`] to be called is the one that
    /// decides, and neither disturbs what a client this was cloned from has
    /// already resolved.
    #[must_use]
    pub fn with_heavy_proxies_in<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.transport
            .set_heavy_proxies_in(names.into_iter().map(Into::into).collect());
        self
    }

    /// Overrides the budget for the `/hosts` lookup, which defaults to 800 ms.
    ///
    /// The lookup sits in front of the first heavy command and gets its own
    /// budget rather than the client's, because not getting an answer costs
    /// nothing worse than the routing this crate had none of a release ago —
    /// see [`Client::with_timeout`] for the one that bounds a command.
    ///
    /// **Raising it is the point.** The budget used to be the smaller of 800 ms
    /// and the client's own timeout, so it could only ever be lowered: a
    /// cluster that answers `/hosts` in 900 ms could not be routed to by any
    /// configuration at all. And 800 ms is not always generous — the first
    /// heavy command is often a client's first request, which puts DNS, TCP and
    /// a TLS handshake inside the same budget.
    ///
    /// ```
    /// use std::time::Duration;
    /// use ytsaurus_client::Client;
    ///
    /// let client = Client::new("https://cluster.example.net")
    ///     .with_hosts_timeout(Duration::from_secs(3));
    /// ```
    #[must_use]
    pub fn with_hosts_timeout(mut self, timeout: Duration) -> Self {
        self.transport.set_hosts_timeout(timeout);
        self
    }

    /// Overrides how long routing stays off after it falls back, which defaults
    /// to ten seconds.
    ///
    /// Two things end up here: a `/hosts` lookup that failed for a reason that
    /// might pass, and a pool whose every host has been dropped. Both mean
    /// "use the address the caller gave, and ask the cluster again in a
    /// moment"; this is the moment. A lookup that *settled* — no such endpoint,
    /// an answer that is not a list of names, a cluster that names no heavy
    /// proxy — runs on the other clock instead: it is asked about again one
    /// [`Client::with_host_list_refresh_interval`] later, like any other
    /// answer that has grown old. So does a failed *refresh*, deliberately —
    /// a pool in hand still routes, so nothing there is urgent enough for
    /// this window.
    ///
    /// Shorter brings routing back sooner after a cluster recovers, and costs a
    /// lookup more often while it is broken. Longer is the other trade.
    #[must_use]
    pub fn with_hosts_retry_after(mut self, after: Duration) -> Self {
        self.transport.set_hosts_retry_after(after);
        self
    }

    /// Overrides how old a `/hosts` answer may grow before a heavy command
    /// re-asks, which defaults to one minute.
    ///
    /// The default is the documentation's own advice — "a good strategy is to
    /// re-query the `/hosts` list every minute or every few queries" — and
    /// the refresh is lazy, the way the C++ SDK does it: the heavy command
    /// that finds the list stale asks first, and a client that stops
    /// uploading stops asking. There is no background thread. A refresh that
    /// fails keeps the previous answer in use rather than dropping routing on
    /// the floor, and waits out another interval before asking again.
    ///
    /// The refresh is also what restores a proxy the client dropped: a heavy
    /// command that fails for a reason attributable to the host it went to —
    /// a refused connection, a 503, a certificate that does not match that
    /// host's name — takes that host out of the pool, and the next fresh
    /// answer that still names it puts it back.
    ///
    /// ```
    /// use std::time::Duration;
    /// use ytsaurus_client::Client;
    ///
    /// let client = Client::new("https://cluster.example.net")
    ///     .with_host_list_refresh_interval(Duration::from_secs(300));
    /// ```
    ///
    /// Shorter follows the cluster's load-ordering more closely and costs a
    /// lookup more often — `Duration::ZERO` re-asks before every heavy
    /// command. `Duration::MAX` disables the refresh: the first answer is
    /// then kept as long as it keeps working, though a failed host is still
    /// dropped and an emptied pool still falls back and re-asks.
    #[must_use]
    pub fn with_host_list_refresh_interval(mut self, interval: Duration) -> Self {
        self.transport.set_host_list_refresh_interval(interval);
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
    /// transaction, keeps it alive and aborts it if the work does not finish,
    /// and [`Client::attach_transaction`] is the one that turns an id from
    /// elsewhere into such a handle — pinging, able to commit and abort.
    ///
    /// This binding does neither: nothing pings the transaction on this path,
    /// so it expires on the cluster's schedule unless its owner — or
    /// [`Client::ping_transaction`] — is pinging it, and finishing it takes
    /// [`Client::commit_transaction`] or [`Client::abort_transaction`] with
    /// the id. What it buys over `attach_transaction` is costlessness: no
    /// round trip, no thread.
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

    /// Puts every request this client sends into `context`'s trace.
    ///
    /// The cluster traces itself: the proxy opens a span for each request, and
    /// a request that names a trace has its span put inside that one instead of
    /// starting an orphan. So this is the cheap half of making a launch
    /// visible — nothing is emitted from this process, and the work the cluster
    /// does on its behalf turns up under the caller's own trace.
    ///
    /// ```
    /// use ytsaurus_client::{Client, TraceContext};
    ///
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// // A service passing on the trace it was called in.
    /// let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    /// let client = Client::new("http://localhost:8000")
    ///     .with_trace_context(&TraceContext::parse(incoming)?);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`TraceContext::new`] starts a trace for a program that was not called
    /// by anything, and [`TraceContext::yt_trace_id`] spells its id the way the
    /// cluster's own logs and UI do.
    ///
    /// A [`Transaction`] started from this client inherits the context, pings
    /// included — the transaction is part of the same piece of work, and a
    /// commit that hung is one of the things a trace is for.
    #[must_use]
    pub fn with_trace_context(mut self, context: &TraceContext) -> Self {
        self.transport.set_trace(context);
        self
    }

    /// The `traceparent` header this client sends, if it was given one.
    #[must_use]
    pub fn traceparent(&self) -> Option<&str> {
        self.transport.trace()
    }

    /// The `tracestate` header this client sends, if the context it joined
    /// carried one. See [`TraceContext::with_tracestate`].
    #[must_use]
    pub fn tracestate(&self) -> Option<&str> {
        self.transport.tracestate()
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

    /// Attaches to a transaction something else started, and keeps it alive.
    ///
    /// The receiving half of [`Transaction::detach`]: one process starts a
    /// transaction and detaches, hands the id over, and this turns the id back
    /// into a real [`Transaction`] — a bound client, a pinging thread, and
    /// `commit`/`abort`/`ping` that work. Two things differ from a handle the
    /// same process started, and both follow from not being the owner:
    ///
    /// - **Dropping it detaches rather than aborts** — the pings stop and
    ///   nothing is sent. The C++ client's destructor draws the same line, and
    ///   for the same reason: an attacher's `?` must not destroy work the
    ///   process that started the transaction is still counting on. An
    ///   explicit [`Transaction::abort`] still aborts; only the drop differs.
    /// - **The ping interval is read, not chosen.** Pinging needs the
    ///   transaction's timeout and the id alone does not carry it, so this
    ///   asks the cluster for `#<id>/@timeout` — one round trip, which is also
    ///   what makes attaching to a transaction that is gone fail *here*,
    ///   rather than on the first command sent through the handle.
    ///
    /// **It pings before it returns**, one more round trip. `@timeout` is the
    /// *configured* lifetime and says nothing about how much of it is left:
    /// the id carries no hint of when its last holder pinged, so a handoff
    /// that took longer than two thirds of the timeout would otherwise hand
    /// back a handle whose first ping is already too late. That ping restarts
    /// the cluster's clock at the attach, and doubles as the liveness probe
    /// this call reports on.
    ///
    /// **Nothing stops two attaches to the same id.** Each is a real handle
    /// with a thread of its own, and they simply ping the same transaction
    /// twice as often; whichever commits or aborts first decides it, and the
    /// other's next command fails with `No such transaction`. There is no
    /// registry, on purpose — a second process attaching is the whole point,
    /// and this process is not in a position to know about it.
    ///
    /// The handle always pings. One that did not would be
    /// [`Client::with_transaction`] — the plain binding, which already exists —
    /// plus [`Client::ping_transaction`], [`Client::commit_transaction`] and
    /// [`Client::abort_transaction`], which take the bare id; reach for those
    /// where a thread per transaction is not wanted. (The Go SDK spells that
    /// choice `AttachTx(id, &AttachTxOptions{AutoPingable: false})`.)
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// # let id_from_elsewhere = String::new();
    /// let tx = client.attach_transaction(&id_from_elsewhere)?;
    ///
    /// tx.create("table", "//tmp/out")?;   // inside the shared transaction
    /// tx.commit()?;                       // and now published, by this process
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the transaction does not exist or the
    /// timeout cannot be read. The error names the id and the operation
    /// itself, because the cluster's own answer does not always do either.
    /// Both spellings were observed on a local cluster: an expired id earns
    /// `Error resolving path #<id>/@timeout` around `No such object <id>` —
    /// object, not transaction, since the id is addressed as one — while an
    /// id that never named anything is refused as `Unknown cell tag 0`, with
    /// no id in it at all. A transaction that expires between the two round
    /// trips fails the same way, on the ping: `No such transaction`.
    pub fn attach_transaction(&self, id: &str) -> Result<Transaction> {
        Transaction::attach(self, id.to_owned())
    }

    /// Tells the cluster a transaction is still wanted, by bare id.
    ///
    /// A held [`Transaction`] does this on its own thread; this is for a
    /// process that has nothing but the id — between a [`Transaction::detach`]
    /// in one process and the commit in another, *somebody* must say the
    /// transaction is still wanted, or it expires its timeout after its last
    /// ping (30 seconds by default; verified on a local cluster with a
    /// two-second timeout left alone for four). A ping is also the cheapest
    /// liveness probe: the cluster answers one for a transaction that is gone
    /// with `No such transaction`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the transaction has expired, was aborted, or
    /// never existed.
    pub fn ping_transaction(&self, id: &str) -> Result<()> {
        transaction::ping(self, id)
    }

    /// Publishes everything done in a transaction, by bare id.
    ///
    /// What lets a process finish a transaction it did not start — the other
    /// end of a [`Transaction::detach`], without the round trip and the ping
    /// thread of [`Client::attach_transaction`].
    ///
    /// Sent under a mutation ID, because **a commit is not idempotent**: the
    /// second commit of the same transaction is refused with `No such
    /// transaction`, which reads like the first one failed. The mutation ID
    /// makes a retried commit the same commit rather than a second one.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the commit fails — including `No such
    /// transaction` for one that expired, was aborted, or was already
    /// committed.
    pub fn commit_transaction(&self, id: &str) -> Result<()> {
        transaction::commit_by_id(self, id)
    }

    /// Discards everything done in a transaction, by bare id.
    ///
    /// **Forgiving, unlike [`Client::abort_operation`]**: aborting a
    /// transaction that already committed, aborted or expired — or one that
    /// never existed — answers `{}`, verified on a local cluster. So this is
    /// safe to send on any cleanup path, and it is retried freely on the same
    /// grounds.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails. The transaction expires
    /// on its own either way, once nothing is pinging it.
    pub fn abort_transaction(&self, id: &str) -> Result<()> {
        transaction::abort_by_id(self, id)
    }

    /// Asks the cluster for the least-loaded heavy proxy, if it has one.
    ///
    /// **The client already does this for itself.** Heavy commands — table and
    /// file data, in either direction — resolve a heavy proxy on their own and
    /// go there; see the module documentation for when, and for how long the
    /// answer is kept. So this is no longer the way to make an upload work: it
    /// is the way to *see* the address, or to hand it to something that is not
    /// this client — a second [`Client`], another process, a `curl`.
    ///
    /// It asks every time and shares nothing with what the client resolved for
    /// itself, so calling it neither costs nor changes anything the next
    /// command does. It also reports the name **as the cluster gave it**,
    /// before the checks automatic routing puts it through — which is what
    /// makes it the way to see why a host was declined. A name here that the
    /// uploads are not using is the symptom
    /// [`Client::with_heavy_proxies_anywhere`] exists for.
    ///
    /// It shares the lookup's budget, though: one attempt bounded by
    /// [`Client::with_hosts_timeout`] — 800 ms unless that says otherwise —
    /// rather than the client's retry policy and request timeout. The budget
    /// belongs to the question, not to whoever asked it.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, or if `/hosts` does not
    /// answer with the documented list of host names. `Ok(None)` means the
    /// cluster answered and named no heavy proxy — which a failure must not be
    /// allowed to look like, since the caller's next move is to stop looking.
    pub fn heavy_proxy(&self) -> Result<Option<String>> {
        // Through the transport, so this carries the token and the TLS guard
        // like every other request, and so that the automatic routing and this
        // read the same answer with the same parser. Not the timeout and not
        // the retry policy: `Transport::fetch` gives this question its own
        // budget, which is the whole point of it having one.
        Ok(self.transport.heavy_hosts()?.into_iter().next())
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

    /// Changes the schema of a table that already exists.
    ///
    /// The other half of [`Client::create_table`]: a table outlives the program
    /// that made it, and the rows it holds gain columns.
    ///
    /// ```no_run
    /// # use ytsaurus_client::{Client, Column, ColumnType, TableSchema};
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// let wider = TableSchema::new([
    ///     Column::new("host", ColumnType::Utf8).required().key(),
    ///     Column::new("size", ColumnType::Int64).required(),
    ///     Column::new("referrer", ColumnType::Utf8), // new, and optional
    /// ]);
    /// client.alter_table("//tmp/visits", &wider)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// **A table with rows in it accepts only changes that ask less of the
    /// rows already written.** Watched on a cluster, on a table holding two
    /// rows — and each refusal says which column and why:
    ///
    /// | Change | |
    /// | --- | --- |
    /// | add an **optional** column, anywhere in the order | allowed |
    /// | make a required column optional | allowed |
    /// | `strict` → non-strict | allowed |
    /// | add a **required** column | `Cannot insert a new required column "must" into a non-empty table` |
    /// | remove a column | `Cannot remove column "size" from a strict schema` |
    /// | change a column's type | `Type … is modified in non backward compatible manner` |
    /// | rename a column | read as a removal, and refused as one |
    /// | make the table sorted | `Cannot change schema from unsorted to sorted` |
    /// | non-strict → `strict` | `Changing "strict" from "false" to "true" is not allowed` |
    ///
    /// Two consequences worth knowing before either becomes permanent:
    ///
    /// - **An empty table accepts all of it** — dropping columns, changing types,
    ///   becoming sorted. So a schema change tried out on an empty table proves
    ///   nothing about the same change on a full one.
    /// - **A non-strict schema can never gain a named column**:
    ///   `Cannot insert a new column "note" into non-strict schema`. Relaxing
    ///   `strict` is a one-way door out of schema evolution.
    ///
    /// Unlike `create`, the schema here is a **top-level parameter** rather than
    /// an attribute — the two commands are exact opposites on this, and `create`
    /// silently ignores the spelling `alter_table` requires.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the schema is one the cluster would
    /// refuse outright, or [`ClientError`] if the change is rejected as
    /// incompatible.
    pub fn alter_table(&self, path: &str, schema: &TableSchema) -> Result<()> {
        schema
            .validate()
            .map_err(|reason| ClientError::Config(format!("{path}: {reason}")))?;

        let params = yson_build::map([
            ("path", yson_build::string(path)),
            // Top-level, where `create` wants it inside `attributes`. Getting
            // this the wrong way round fails loudly here and silently there.
            ("schema", schema.to_yson()),
        ]);
        self.transport.call(
            Method::Post,
            "alter_table",
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

    /// Removes a Cypress node.
    ///
    /// The node must exist, and a map node must be empty — the cluster's own
    /// defaults, and the safe ones: a mistyped path fails instead of deleting
    /// whatever it happened to name. [`Client::remove_tree`] is the deliberate
    /// spelling for a subtree.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the node does not exist, is a non-empty map
    /// node, or the request fails.
    pub fn remove(&self, path: &str) -> Result<()> {
        self.remove_with(path, false, false)
    }

    /// Removes a Cypress node and everything under it. Succeeds if it is
    /// already absent.
    ///
    /// This is `recursive` plus `force`: the spelling for "make this path not
    /// exist", whatever is there now — which is also why it deserves a moment
    /// of care with the argument.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn remove_tree(&self, path: &str) -> Result<()> {
        self.remove_with(path, true, true)
    }

    fn remove_with(&self, path: &str, recursive: bool, force: bool) -> Result<()> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("recursive", yson_build::boolean(recursive)),
            ("force", yson_build::boolean(force)),
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
    /// # A cache you may not write to
    ///
    /// On an installation where that shared path is maintained by its
    /// operators, an ordinary user may read it and nothing more — and the
    /// cluster answers a write with `Access denied`. That is a **degraded
    /// cache, not a failed upload**: the worker goes up outside the cache
    /// instead, to a path of its own under `//tmp`, and the launch proceeds.
    ///
    /// It is warned about rather than passed over, on stderr — as a `WARN`
    /// event where the `tracing` feature is on — because the state is
    /// permanent until someone acts on it and invisible otherwise: every launch
    /// re-sends the whole binary, and every launch leaves a node behind that no
    /// cache expiry will collect. The warning names
    /// [`Client::with_file_cache`], which is the one line that puts a cache
    /// back.
    ///
    /// Only the cluster's refusal of *the cache* is treated this way — creating
    /// the cache directory, creating the staging node inside it, and the
    /// handover to `put_file_to_cache`. Any other failure, including an
    /// `Access denied` on anything else, is returned.
    ///
    /// [`CachedFile::cached`] is which of the two happened, and it is the field
    /// to read before doing anything to [`CachedFile::path`]: on the fallback
    /// path that node is this launch's own and nobody else's, while on the
    /// ordinary path it is the installation's shared cache entry.
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
                cached: true,
            });
        }

        let (path, cached) = match self.upload_into_cache(&bytes, &digest)? {
            Cached::At(path) => {
                // Set on the cached path too: whether the attribute survives
                // the move decides whether the job can exec at all, and it is
                // cheap to be sure.
                self.set_attribute(&path, "executable", yson_build::boolean(true))?;
                (path, true)
            }
            Cached::Refused(denial) => {
                observe::cache_refused(&self.file_cache, &denial);
                (self.upload_uncached(&digest, &bytes)?, false)
            }
        };

        Ok(CachedFile {
            path,
            name,
            uploaded: true,
            cached,
        })
    }

    /// Everything in [`Client::upload_worker_cached`] that touches the cache.
    ///
    /// Three of the calls here can be refused by an installation that keeps the
    /// cache to itself, and all three mean the same thing — this caller has no
    /// cache at this path — so all three come back as [`Cached::Refused`] for
    /// the caller to fall back on: creating the cache directory, creating the
    /// staging node **inside** it, and the handover, `put_file_to_cache`. The
    /// two creates ask for the same permission on the same directory, so which
    /// of them a given cluster refuses first is its own business.
    ///
    /// Nothing else is caught, deliberately. Between those calls the client is
    /// writing to a node it has just created: a refusal there is about that
    /// node rather than about the cache, and the same bytes sent to another
    /// path would earn the same answer, so falling back would upload twice and
    /// still fail. And a create refused for some *other* reason — a path that
    /// resolves to something else, a lock held elsewhere — is not a permission
    /// problem at all. Both are returned as they always were.
    fn upload_into_cache(&self, bytes: &[u8], digest: &str) -> Result<Cached> {
        // Created here rather than in the lookup: a cache the installation
        // maintains is one a user may only be able to read, and a lookup that
        // mutated it would fail on exactly the clusters where the cache is
        // worth the most. Being refused *here* costs a slower upload, which is
        // what makes that trade worth making.
        if let Err(denial) = self.create("map_node", &self.file_cache) {
            return refused_or_reported(denial);
        }

        // Staged inside the cache node, so a cluster that expires the cache
        // expires an interrupted upload with it.
        //
        // The name carries a nonce as well as the hash. Keyed by the hash alone
        // it names the same node for every process uploading the same binary,
        // and two CI jobs launching together would write to one node and then
        // remove it from under each other.
        let staging = format!("{}/staged_{digest}_{}", self.file_cache, MutationId::new());
        if let Err(denial) = self.create("file", &staging) {
            return refused_or_reported(denial);
        }

        let cached = self
            .write_file_computing_md5(&staging, bytes)
            .and_then(|()| self.set_attribute(&staging, "executable", yson_build::boolean(true)))
            .and_then(|()| self.put_file_to_cache(&staging, digest));

        // Removed whichever way that went. On success the cache may have kept
        // the node itself rather than a copy, so this is `force`-removing
        // something that may already be gone, which `remove_tree` tolerates.
        // On failure it is what stops a rejected upload from leaving tens of
        // megabytes behind for good: cache expiry walks the entries the cache
        // itself created, not the staging nodes beside them.
        let removed = self.remove_tree(&staging);

        match cached {
            Ok(path) => {
                // The upload's own failure is the one worth reporting; a
                // cleanup that also failed only matters when there was nothing
                // else wrong.
                removed?;
                Ok(Cached::At(path))
            }
            // Refused at the handover, with the bytes already on the cluster —
            // they are about to be sent again, which is the price of a launch
            // that runs at all. A removal that failed too is dropped here
            // rather than reported: a cache that refuses the handover may well
            // refuse the cleanup, and failing the launch over a staging node is
            // exactly what this is not doing.
            Err(denial) if denied(&denial, "put_file_to_cache") => Ok(Cached::Refused(denial)),
            Err(failed) => Err(failed),
        }
    }

    /// Uploads the worker outside the cache, for a cluster whose cache this
    /// caller may not write to.
    ///
    /// A path of its own every time, nonce and all, for the reason the staging
    /// node has one: a name derived from the hash alone is the same node for
    /// every process uploading the same binary, and two launchers starting
    /// together would take an exclusive lock on it in turn. The cost is a node
    /// per launch that no cache expiry will collect, which is the second reason
    /// the warning names [`Client::with_file_cache`].
    ///
    /// # What this node is not
    ///
    /// It is an ordinary `//tmp` node: it inherits whatever ACL `//tmp` carries
    /// on the installation, it is given no expiry, and its name is unguessable
    /// only as far as [`MutationId`] is — and the entropy it draws on says of
    /// itself that its callers need an id to be *unique, not unpredictable*,
    /// because what it was built for is deduplicating a retry rather than
    /// withholding a name. On a cluster where
    /// `//tmp` is shared scratch space, a co-tenant who can list it can also
    /// **rewrite the worker's bytes between this upload and the job that execs
    /// them**.
    ///
    /// That is the ordinary exposure of anything left in `//tmp`, and it is the
    /// same exposure the shared file cache has — but the cache is at least a
    /// path an installation curates, and this is the path taken *because* the
    /// curated one was refused. A caller who cannot accept it should point
    /// [`Client::with_file_cache`] at a directory of its own, which removes
    /// both this node and the refusal that produced it.
    fn upload_uncached(&self, digest: &str, bytes: &[u8]) -> Result<String> {
        let remote = format!(
            "{UNCACHED_UPLOAD_DIR}/ytsaurus_rs_worker_{digest}_{}",
            MutationId::new()
        );
        self.upload_executable(&remote, bytes)?;
        Ok(remote)
    }

    /// Looks up a file in the cluster's file cache by its MD5.
    ///
    /// `None` means nothing is cached under that hash — including when the
    /// cache directory does not exist yet, which is what
    /// [`Client::upload_worker_cached`] creates on its way past, on a cluster
    /// that lets it.
    ///
    /// A lookup and nothing more: it sends no mutation, so it works against a
    /// cache the caller may only read.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn file_from_cache(&self, md5: &str) -> Result<Option<String>> {
        let params = yson_build::map([
            ("md5", yson_build::string(md5)),
            ("cache_path", yson_build::string(&self.file_cache)),
        ]);
        // A `cache_path` that does not exist needs no special case: the cluster
        // answers 200 with the same empty string it uses for any other miss,
        // rather than the resolve error a missing path usually earns. Checked
        // against a local cluster with no `//tmp/yt_wrapper` at all, which is
        // the state a first upload starts from.
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
            Repeatable::Heavy,
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
    pub fn write_table(&self, path: impl Into<TablePath>, rows: &[u8]) -> Result<()> {
        self.write_table_with_format(path, rows, &DataFormat::binary_yson())
    }

    /// Writes rows to a table using a shared [`DataFormat`], replacing its
    /// contents.
    ///
    /// YSON data is a list fragment in the selected representation. Skiff data
    /// is a complete schema-described stream; direct table I/O requires exactly
    /// one schema with named non-system fields.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the format is unsupported, the data is not a
    /// complete Skiff stream, or the request fails.
    pub fn write_table_with_format(
        &self,
        path: impl Into<TablePath>,
        rows: &[u8],
        format: &DataFormat,
    ) -> Result<()> {
        let path = path.into();
        match format {
            DataFormat::Yson(format) => self.write_yson_table(&path, rows, *format),
            DataFormat::Skiff(format) => self.write_skiff_table_impl(&path, rows, format),
            _ => Err(unsupported_data_format()),
        }
    }

    fn write_yson_table(&self, path: &TablePath, rows: &[u8], format: YsonFormat) -> Result<()> {
        let params = yson_build::map([
            ("path", path.to_yson()),
            ("input_format", DataFormat::yson(format).to_yson()),
        ]);
        self.transport.call(
            Method::Put,
            "write_table",
            &params,
            Payload::Bytes(rows),
            Repeatable::Heavy,
        )?;
        Ok(())
    }

    /// Writes a complete Skiff stream to one table, replacing its contents.
    ///
    /// `format` must have exactly one table schema. Its named fields are sent
    /// as the rich-path `columns` projection, matching the Go SDK; this is how
    /// the proxy maps the positional Skiff tuple to table columns. `rows` is
    /// checked against that schema before the request is made.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the format is not a direct-table format, the
    /// stream is incomplete, or the request fails.
    pub fn write_skiff_table(
        &self,
        path: impl Into<TablePath>,
        rows: &[u8],
        format: &SkiffFormat,
    ) -> Result<()> {
        self.write_table_with_format(path, rows, &DataFormat::skiff(format.clone()))
    }

    fn write_skiff_table_impl(
        &self,
        path: &TablePath,
        rows: &[u8],
        format: &SkiffFormat,
    ) -> Result<()> {
        // The path first: it is what rejects a format that is not single-table
        // direct I/O. Checking the stream first would answer a multi-table
        // format with a decode error about a tag mismatch, which describes a
        // consequence rather than the mistake.
        let path_value = skiff_table_path(path, format)?;
        check_complete_skiff_stream(rows, format).map_err(|reason| ClientError::Decode {
            command: "write_table".to_owned(),
            reason: format!("{}: {reason}", path.as_str()),
        })?;

        let params = yson_build::map([("path", path_value), ("input_format", format.to_yson())]);
        self.transport.call(
            Method::Put,
            "write_table",
            &params,
            Payload::Bytes(rows),
            Repeatable::Heavy,
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
        self.read_table_with_format(path, &DataFormat::binary_yson())
    }

    /// Reads a whole table using a shared [`DataFormat`].
    ///
    /// The returned bytes are a YSON list fragment or a complete Skiff stream,
    /// according to `format`. The response is checked for truncated records
    /// before it is returned.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the format is unsupported, the response is
    /// incomplete, or the request fails.
    pub fn read_table_with_format(&self, path: &str, format: &DataFormat) -> Result<Vec<u8>> {
        match format {
            DataFormat::Yson(format) => self.read_yson_table(path, *format),
            DataFormat::Skiff(format) => self.read_skiff_table_impl(path, format),
            _ => Err(unsupported_data_format()),
        }
    }

    fn read_yson_table(&self, path: &str, format: YsonFormat) -> Result<Vec<u8>> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("output_format", DataFormat::yson(format).to_yson()),
        ]);
        let body = self.transport.call(
            Method::Get,
            "read_table",
            &params,
            Payload::None,
            Repeatable::Heavy,
        )?;

        check_complete_yson_fragment(&body, format).map_err(|reason| ClientError::Decode {
            command: "read_table".to_owned(),
            reason: format!("{path}: {reason}"),
        })?;

        Ok(body)
    }

    /// Reads one table as a complete Skiff stream.
    ///
    /// `format` must have exactly one table schema. Its named fields select
    /// the table columns and determine the bytes returned. The response is
    /// decoded to its end before being returned so a truncated Skiff stream is
    /// never reported as a successful table read.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the format is not a direct-table format, the
    /// response is incomplete, or the request fails.
    pub fn read_skiff_table(&self, path: &str, format: &SkiffFormat) -> Result<Vec<u8>> {
        self.read_table_with_format(path, &DataFormat::skiff(format.clone()))
    }

    fn read_skiff_table_impl(&self, path: &str, format: &SkiffFormat) -> Result<Vec<u8>> {
        let params = yson_build::map([
            ("path", skiff_table_path(&TablePath::from(path), format)?),
            ("output_format", format.to_yson()),
        ]);
        let body = self.transport.call(
            Method::Get,
            "read_table",
            &params,
            Payload::None,
            Repeatable::Heavy,
        )?;

        check_complete_skiff_stream(&body, format).map_err(|reason| ClientError::Decode {
            command: "read_table".to_owned(),
            reason: format!("{path}: {reason}"),
        })?;

        Ok(body)
    }

    /// Writes rows to a table from anything that yields them.
    ///
    /// The rows are Rust values; the encoding is this crate's problem, which is
    /// the difference between this and [`Client::write_table`]:
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// #[derive(serde::Serialize)]
    /// struct Contact<'a> {
    ///     name: &'a str,
    ///     email: &'a str,
    ///     age: i64,
    /// }
    ///
    /// client.write_table_rows("//tmp/contacts", (0..100).map(|n| Contact {
    ///     name: "Gordon Freeman",
    ///     email: "gordon@black-mesa.example",
    ///     age: 27 + n,
    /// }))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// It takes an iterator rather than a slice because the encoder sits
    /// *inside* the request body: rows are serialised a bufferful at a time as
    /// the connection asks for bytes, so a million rows cost one buffer rather
    /// than a million rows' worth of memory, and the caller never has to
    /// materialise them either.
    ///
    /// Replaces the table's contents, as [`Client::write_table`] does.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Decode`] naming the row if one cannot be
    /// serialised — the write fails rather than sending the rows before it —
    /// or [`ClientError`] if the request fails.
    pub fn write_table_rows<T, I>(&self, path: impl Into<TablePath>, rows: I) -> Result<()>
    where
        T: serde::Serialize,
        I: IntoIterator<Item = T>,
    {
        let path = path.into();
        let params = yson_build::map([
            ("path", path.to_yson()),
            ("input_format", yson_build::binary_yson_format()),
        ]);

        let mut stream = stream::RowStream::new(rows.into_iter());
        let sent = self
            .transport
            .upload(Method::Put, "write_table", &params, &mut stream);

        // Checked first: a body that failed to encode fails the request too,
        // and the transport's account of that is "the body ended early".
        if let Some(reason) = stream.failed {
            return Err(ClientError::Decode {
                command: "write_table".to_owned(),
                reason: format!("{path}: {reason}"),
            });
        }
        sent.map(|_| ())
    }

    /// Reads a whole table as typed rows.
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// #[derive(serde::Deserialize)]
    /// struct Contact {
    ///     name: String,
    ///     age: i64,
    /// }
    ///
    /// for contact in client.read_table_rows::<Contact>("//tmp/contacts")? {
    ///     println!("{} is {}", contact.name, contact.age);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Rows are **owned**, and the whole table is read before any of it is
    /// returned — this is [`Client::read_table`] with the decoding done, and it
    /// inherits the same purpose: results a launcher inspects. For a table that
    /// does not fit, or for rows borrowed from the buffer they arrived in,
    /// [`Client::read_table_streaming`] feeds `ytsaurus_job::JobReader`.
    ///
    /// Columns the type does not mention are ignored, so a struct naming two
    /// columns of a twenty-column table is a projection rather than an error.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, the stream is truncated,
    /// or a row does not match `T`.
    pub fn read_table_rows<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<Vec<T>> {
        decode_rows(&self.read_table(path)?, path)
    }

    /// Reads a node, or an attribute, into a Rust type.
    ///
    /// [`Client::get`] hands back a [`YsonValue`] to walk; this hands back the
    /// shape you were going to walk it into:
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// #[derive(serde::Deserialize)]
    /// struct Cluster {
    ///     #[serde(rename = "type")]
    ///     node_type: String,
    ///     creation_time: String,
    ///     account: String,
    /// }
    ///
    /// let root: Cluster = client.get_as("//@")?;
    /// println!("the cluster was created at {}", root.creation_time);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Attributes the type does not mention are ignored, which is what makes
    /// `//@` — a node with dozens of them — worth asking about at all.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the answer does not fit
    /// `T`.
    pub fn get_as<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let params = yson_build::map([("path", yson_build::string(path))]);
        let body = self.transport.call(
            Method::Get,
            "get",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

        // Decoded straight out of the response, envelope and all. Going through
        // `get` would build a whole `YsonValue` tree, encode it back to bytes
        // and decode those into `T` — three passes over the document and two
        // copies of it in memory, where one pass does the same job. Invisible
        // for `//@`; not for a large attribute or a subtree.
        let envelope: Envelope<T> =
            from_slice(&body, YsonFormat::Text).map_err(|e| ClientError::Decode {
                command: "get".to_owned(),
                reason: format!(
                    "{path}: the answer does not fit the type asked for: {e}; body was {}",
                    crate::error::truncate(&String::from_utf8_lossy(&body), 200)
                ),
            })?;

        Ok(envelope.value)
    }

    /// Reads a table as a stream, without holding it.
    ///
    /// The same bytes [`Client::read_table`] returns — a binary YSON list
    /// fragment — arriving as they come off the connection, so the table's size
    /// stops being the program's memory ceiling.
    ///
    /// What comes out is what a job reads on fd 0, so the same decoder handles
    /// both:
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::from_env()?;
    /// let mut reader = ytsaurus_job::JobReader::binary(client.read_table_streaming("//tmp/big")?);
    ///
    /// let mut rows = 0_u64;
    /// while let Some(event) = reader.next_event()? {
    ///     if matches!(event, ytsaurus_job::Event::Row(_)) {
    ///         rows += 1;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Client::read_table`] checks that what came back is a complete
    /// fragment; this cannot, because it never has the whole thing. A fragment
    /// cut short instead leaves a record that does not parse, and the decoder
    /// fails on it — see [`TableReader`] for why that is the same protection
    /// rather than none.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails. Failures *during* the read
    /// arrive from the reader, not from here.
    pub fn read_table_streaming(&self, path: &str) -> Result<TableReader> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("output_format", yson_build::binary_yson_format()),
        ]);
        let body = self.transport.open(Method::Get, "read_table", &params)?;
        Ok(TableReader::new(body))
    }

    /// Writes a table from a stream, without holding it.
    ///
    /// `rows` is read to its end and sent as it is read, so the rows can come
    /// from a file, a pipe, or something that generates them — anything that is
    /// a `Read`. The bytes are a binary YSON list fragment, exactly as
    /// [`Client::write_table`] expects them.
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::from_env()?;
    /// client.create("table", "//tmp/big")?;
    /// client.write_table_streaming("//tmp/big", std::fs::File::open("rows.yson")?)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This is one attempt and can never be more: a reader that has been
    /// consumed cannot be sent again. That agrees with the retry rules — heavy
    /// commands are not repeated — and a transaction is what makes such a write
    /// safe to fail.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, including when `rows`
    /// itself fails to read.
    pub fn write_table_streaming(
        &self,
        path: impl Into<TablePath>,
        mut rows: impl std::io::Read,
    ) -> Result<()> {
        let params = yson_build::map([
            ("path", path.into().to_yson()),
            ("input_format", yson_build::binary_yson_format()),
        ]);
        self.transport
            .upload(Method::Put, "write_table", &params, &mut rows)?;
        Ok(())
    }

    // ---------------------------------------------------------- operations

    /// Starts a map operation, returning its ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_map(&self, spec: &MapSpec) -> Result<String> {
        refuse_skiff_table_mismatch(spec.skiff_table_mismatch())?;
        self.start_operation(OperationType::Map, &spec.to_yson())
    }

    /// Starts a map-reduce operation, returning its ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_map_reduce(&self, spec: &MapReduceSpec) -> Result<String> {
        refuse_skiff_table_mismatch(spec.skiff_table_mismatch())?;
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
        refuse_skiff_table_mismatch(spec.skiff_table_mismatch())?;
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
    /// Returns [`ClientError::Config`] if two tasks share a name, and
    /// [`ClientError`] if the request fails.
    pub fn start_vanilla(&self, spec: &VanillaSpec) -> Result<String> {
        // Refused here rather than sent: the spec keys tasks by name, so the
        // cluster would take two tasks called the same thing as one, run half
        // the jobs, and complete. A silent half-run is worse than a rejected
        // launch.
        if let Some(name) = spec.duplicate_task() {
            return Err(ClientError::Config(format!(
                "two vanilla tasks are both called {name:?}; a spec keys its tasks \
                 by name, so the second would replace the first and its jobs would \
                 never run"
            )));
        }

        refuse_skiff_table_mismatch(spec.skiff_table_mismatch())?;
        self.start_operation(OperationType::Vanilla, &spec.to_yson())
    }

    /// Starts a merge operation, returning its ID.
    ///
    /// A [`MergeMode::Sorted`] merge does **not** need
    /// [`MergeSpec::with_merge_by`]: measured against a cluster, one sent
    /// without it is accepted and the key is taken from the sort columns the
    /// inputs already carry, with the output coming back sorted by them.
    /// Naming the columns is how to merge by fewer of them than the inputs are
    /// sorted by, or to state the assumption where a reader can see it.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails — including when a sorted
    /// merge's inputs are not sorted, which only the cluster can tell.
    pub fn start_merge(&self, spec: &MergeSpec) -> Result<String> {
        self.start_operation(OperationType::Merge, &spec.to_yson())
    }

    /// Starts an erase operation, returning its ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_erase(&self, spec: &EraseSpec) -> Result<String> {
        self.start_operation(OperationType::Erase, &spec.to_yson())
    }

    /// Starts a remote-copy operation, returning its ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn start_remote_copy(&self, spec: &RemoteCopySpec) -> Result<String> {
        self.start_operation(OperationType::RemoteCopy, &spec.to_yson())
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

    /// Stops an operation that is still running.
    ///
    /// The counterpart to starting one, and the reason it is worth having: a
    /// launcher that gives up — an interrupted `wait_for_operation`, a failed
    /// step further down the script — otherwise leaves the operation running on
    /// the cluster, spending quota on a result nobody will read.
    ///
    /// `reason` is put in the operation's error document, under the cluster's
    /// own `Operation aborted by user request`, so whoever finds the aborted
    /// operation later is told who stopped it and why. Pass `None` to say
    /// nothing.
    ///
    /// By the time this returns the operation is already `aborted`: the call
    /// takes a few hundred milliseconds, and the state has changed within it.
    /// The `aborting` state exists but no caller of this can observe it.
    ///
    /// **This is not idempotent, unlike [`Transaction::abort`].** Once the
    /// scheduler has let go of an operation it answers `No such operation`, and
    /// it lets go as soon as the first abort is accepted — so a second abort is
    /// an error rather than a shrug, even for an operation that was still
    /// running a moment ago. An operation that finished *by itself* can still
    /// be aborted for the short while the scheduler keeps it, so this is not a
    /// reliable way to ask whether one has finished either.
    ///
    /// **Sent once, and never retried**, which is the other side of the same
    /// coin. `abort_operation` is a scheduler command and the master's mutation
    /// cache does not cover it: a retry after a lost answer would be told `No
    /// such operation` and would report a successful abort as a failed one.
    /// A transport error here means the request may or may not have arrived,
    /// and the honest thing is to say so rather than to guess.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, including when the
    /// scheduler no longer has the operation.
    pub fn abort_operation(&self, id: &str, reason: Option<&str>) -> Result<()> {
        let mut params = yson_build::map([("operation_id", yson_build::string(id))]);
        if let Some(reason) = reason {
            yson_build::insert(&mut params, "abort_message", yson_build::string(reason));
        }

        self.transport.call(
            Method::Post,
            "abort_operation",
            &params,
            Payload::None,
            // Not `WithMutationId`, though this is a mutating command: that
            // deduplication lives in the master and this request goes to the
            // scheduler. Verified — a second send of the same mutation ID,
            // flagged as a retry, is answered `No such operation` rather than
            // with the first response. A retry would turn an abort that worked
            // into an error the caller believes.
            Repeatable::Never,
        )?;
        Ok(())
    }

    /// Pauses a running operation.
    ///
    /// Its jobs stop being scheduled; what is already running keeps running
    /// unless `abort_running_jobs` says otherwise, in which case the work those
    /// jobs had done is lost and will be done again after
    /// [`Client::resume_operation`].
    ///
    /// **Suspension is not a state.** A suspended operation still answers
    /// `running` to [`Client::operation_state`] — the cluster reports it in a
    /// separate `suspended` attribute, which is what
    /// [`Client::operation_suspended`] reads. Verified on a local cluster, and
    /// it is the sort of thing a poll loop gets wrong forever.
    ///
    /// **Unlike its counterpart, this one is idempotent**: suspending a
    /// suspended operation answers `{}`, so it is retried like a read. That
    /// holds only while the scheduler still has the operation — once it has let
    /// go, this answers `No such operation` like every other command here.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, including when the
    /// scheduler no longer has the operation.
    pub fn suspend_operation(&self, id: &str, abort_running_jobs: bool) -> Result<()> {
        let params = yson_build::map([
            ("operation_id", yson_build::string(id)),
            (
                "abort_running_jobs",
                yson_build::boolean(abort_running_jobs),
            ),
        ]);
        self.transport.call(
            Method::Post,
            "suspend_operation",
            &params,
            Payload::None,
            // Mutating, and repeated anyway: a second suspend of a suspended
            // operation is accepted, so a retry after a lost answer says the
            // same thing twice rather than turning a success into an error.
            // That is exactly what `abort_operation` cannot do — an abort makes
            // the scheduler let go, so its retry is guaranteed to fail.
            Repeatable::Freely,
        )?;
        Ok(())
    }

    /// Lets a suspended operation run again.
    ///
    /// **Sent once, and never retried.** Where [`Client::suspend_operation`] is
    /// idempotent, this is not: an operation that is not suspended answers code
    /// 201, `Operation is in "running" state`. A retry after a lost answer would
    /// therefore report a resume that worked as a failure — the same trap
    /// [`Client::abort_operation`] describes.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, including when the
    /// operation was not suspended.
    pub fn resume_operation(&self, id: &str) -> Result<()> {
        let params = yson_build::map([("operation_id", yson_build::string(id))]);
        self.transport.call(
            Method::Post,
            "resume_operation",
            &params,
            Payload::None,
            Repeatable::Never,
        )?;
        Ok(())
    }

    /// Finishes an operation early, keeping what it has produced.
    ///
    /// The difference from [`Client::abort_operation`]: an aborted operation's
    /// output tables are discarded, a completed one's are published. This is how
    /// a long-running vanilla operation is stopped *successfully* — it ends as
    /// `completed`, and [`Client::wait_for_operation`] returns `Ok`.
    ///
    /// **Sent once, and never retried**, for the reason
    /// [`Client::abort_operation`] gives: the second one is answered `No such
    /// operation`, so a retry turns a completion that worked into an error.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, including when the
    /// scheduler no longer has the operation.
    pub fn complete_operation(&self, id: &str) -> Result<()> {
        let params = yson_build::map([("operation_id", yson_build::string(id))]);
        self.transport.call(
            Method::Post,
            "complete_operation",
            &params,
            Payload::None,
            Repeatable::Never,
        )?;
        Ok(())
    }

    /// Changes a running operation's scheduling parameters.
    ///
    /// The pool it competes in and the share it gets, while it runs — the one
    /// thing about a started operation that is not fixed. See
    /// [`OperationParameters`].
    ///
    /// ```no_run
    /// # use ytsaurus_client::{Client, OperationParameters};
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// # let id = String::new();
    /// client.update_operation_parameters(
    ///     &id,
    ///     &OperationParameters::new().with_pool("interactive").with_weight(2.0),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The parameters go in the request's parameters, not its body: the
    /// cluster's registry declares this command's input as `null`, whatever the
    /// command reference says. It answers with an empty body rather than the
    /// `{}` its neighbours send.
    ///
    /// Repeated freely, because it assigns rather than increments: sending the
    /// same update twice leaves the operation where the first one put it. As
    /// with [`Client::suspend_operation`], that holds only while the scheduler
    /// still has the operation — if the answer to the first send is lost and
    /// the operation ends during the backoff, the retry is answered `No such
    /// operation` and this returns an error for an update that was applied.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `parameters` would change nothing —
    /// the cluster accepts an empty update and does nothing, which hides the
    /// mistake where it was made — and [`ClientError`] if the request fails.
    pub fn update_operation_parameters(
        &self,
        id: &str,
        parameters: &OperationParameters,
    ) -> Result<()> {
        if parameters.is_empty() {
            return Err(ClientError::Config(
                "update_operation_parameters was given nothing to change; the \
                 cluster answers 200 and does nothing, so this is refused here \
                 instead"
                    .to_owned(),
            ));
        }

        let params = yson_build::map([
            ("operation_id", yson_build::string(id)),
            ("parameters", parameters.to_yson()),
        ]);
        self.transport.call(
            Method::Post,
            "update_operation_parameters",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;
        Ok(())
    }

    /// Lists operations the cluster knows about.
    ///
    /// ```no_run
    /// # use ytsaurus_client::{Client, OperationFilter};
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// let mine = client.list_operations(
    ///     &OperationFilter::new().with_user("robot-loader").with_state("running"),
    /// )?;
    ///
    /// for operation in &mine.operations {
    ///     println!("{} {} {}", operation.id, operation.kind, operation.state);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The scheduler only holds operations it has not let go of. Anything older
    /// lives in the operations archive, which
    /// [`OperationFilter::with_archive`] asks for — and which a local cluster
    /// does not have.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the response cannot be
    /// decoded.
    pub fn list_operations(&self, filter: &OperationFilter) -> Result<OperationList> {
        let body = self.transport.call(
            Method::Get,
            "list_operations",
            &filter.to_yson(),
            Payload::None,
            Repeatable::Freely,
        )?;

        // No `{value=…}` envelope, and no one-key envelope either: the answer
        // is a dict of `operations` plus counters, which is why this reads the
        // document rather than unwrapping it.
        operation::parse_operations(&self.strip_envelope(&body, "list_operations")?)
    }

    /// An operation's event log.
    ///
    /// **Empty on a cluster with no operations archive.** The command is
    /// registered everywhere and answers with an empty list there, rather than
    /// with an error — verified on a local cluster, where it is always empty.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the response cannot be
    /// decoded.
    pub fn list_operation_events(&self, id: &str) -> Result<Vec<OperationEvent>> {
        let params = yson_build::map([("operation_id", yson_build::string(id))]);
        let body = self.transport.call(
            Method::Get,
            "list_operation_events",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

        // A bare list, with none of the one-key envelope the rest of API v4
        // uses — the same surprise the file-cache commands hold. An envelope
        // is read too; see `operation::parse_events` for why that is not
        // over-caution.
        operation::parse_events(&self.strip_envelope(&body, "list_operation_events")?)
    }

    /// A handle on an operation that is already running.
    ///
    /// The reattach door — C++'s `AttachOperation`, Go's `Track(id)`. Nothing is
    /// sent: an id and a client is all an [`Operation`] is, so this cannot fail
    /// and does not check that the operation exists. The first command through
    /// the handle finds that out.
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::from_env()?;
    /// // A supervisor restarts and picks up where it left off.
    /// let op = client.attach_operation(std::fs::read_to_string("run.id")?);
    /// op.wait()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// **The id is trimmed**, for the reason the token file is: the documented
    /// way to get one here is out of a file, `echo $ID > run.id` writes a
    /// newline, and an id carrying one is answered `No such operation` by an
    /// error that never mentions whitespace.
    #[must_use]
    pub fn attach_operation(&self, id: impl Into<String>) -> Operation {
        let mut id = id.into();
        if id.trim().len() != id.len() {
            id = id.trim().to_owned();
        }
        Operation::new(self.clone(), id)
    }

    /// The whole document the cluster keeps about an operation.
    ///
    /// `attributes` names what to fetch — `state`, `progress`, `result`,
    /// `runtime_parameters`, `spec`. **An empty slice asks for everything**,
    /// which is rarely what anyone wants: the full document for a trivial
    /// vanilla operation measured 119 KB on a local cluster, most of it the
    /// resolved spec and the progress tree. Naming attributes is the normal
    /// case, and the narrow readers — [`Client::operation_state`],
    /// [`Client::job_statistics`], [`Client::operation_result_error`] — are each
    /// one attribute of this.
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// # let id = String::new();
    /// let doc = client.get_operation(&id, &["state", "start_time", "suspended"])?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the answer cannot be
    /// decoded.
    pub fn get_operation(&self, id: &str, attributes: &[&str]) -> Result<YsonValue> {
        self.get_operation_inner(
            yson_build::map([("operation_id", yson_build::string(id))]),
            attributes,
        )
    }

    /// The same, for an operation found by the alias its spec gave it.
    ///
    /// An alias is a name a launcher chooses — `*nightly-load` — set in the
    /// spec's `alias` field, and the leading `*` is the cluster's requirement,
    /// not this crate's. Without it, an alias set at launch could never be
    /// looked up again.
    ///
    /// The request carries `include_runtime`, because the cluster refuses the
    /// lookup without it: *"Operation alias cannot be resolved without using
    /// runtime information"*. That also bounds what this can find — an alias is
    /// resolved from what the scheduler still holds, falling back to the
    /// operations archive, so an alias whose operation finished long ago is
    /// found only on an installation that has an archive.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails — including when no
    /// operation has that alias — or if the answer cannot be decoded.
    pub fn get_operation_by_alias(&self, alias: &str, attributes: &[&str]) -> Result<YsonValue> {
        self.get_operation_inner(
            yson_build::map([
                ("operation_alias", yson_build::string(alias)),
                ("include_runtime", yson_build::boolean(true)),
            ]),
            attributes,
        )
    }

    fn get_operation_inner(&self, params: YsonValue, attributes: &[&str]) -> Result<YsonValue> {
        let body = self.get_operation_body(params, attributes)?;
        self.strip_envelope(&body, "get_operation")
    }

    /// The bytes of a `get_operation` answer, before they are parsed.
    ///
    /// Split out for [`Client::operation_error`], which reports the raw body
    /// when it cannot be parsed — the one caller for which a decode failure is
    /// not the end of the story.
    fn get_operation_body(&self, mut params: YsonValue, attributes: &[&str]) -> Result<Vec<u8>> {
        // Omitted rather than sent empty: `attributes=[]` is a request for no
        // attributes at all, and the cluster answers `{}` to it. Leaving the
        // parameter out is how the whole document is asked for.
        if !attributes.is_empty() {
            yson_build::insert(
                &mut params,
                "attributes",
                yson_build::list(attributes.iter().map(yson_build::string)),
            );
        }

        self.transport.call(
            Method::Get,
            "get_operation",
            &params,
            Payload::None,
            Repeatable::Freely,
        )
    }

    /// Fetches an operation's current state, e.g. `running` or `completed`.
    ///
    /// **A suspended operation still reports `running`.** See
    /// [`Client::operation_suspended`], or [`Client::operation_status`] for
    /// both in one request.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn operation_state(&self, id: &str) -> Result<String> {
        operation::state_of(&self.get_operation(id, &["state"])?)
    }

    /// Whether an operation is paused.
    ///
    /// The question [`Client::operation_state`] does not answer: the cluster
    /// keeps suspension in its own attribute and leaves the state at `running`,
    /// so a loop that watches the state alone will wait out a paused operation
    /// without ever saying why.
    ///
    /// **An operation whose document does not carry the attribute is not
    /// suspended**, rather than an error: the scheduler reports it for what it
    /// still holds, and one resolved out of the operations archive may not
    /// carry it at all.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, or if the attribute is
    /// there and is not a boolean.
    pub fn operation_suspended(&self, id: &str) -> Result<bool> {
        operation::suspended_of(&self.get_operation(id, &["suspended"])?)
    }

    /// An operation's state and whether it is paused, in one request.
    ///
    /// The pair a poll loop actually needs. Asking them separately is two
    /// round trips for two attributes of one document, and a loop that asks
    /// only for the state cannot tell a running operation from a paused one —
    /// they both say `running`.
    ///
    /// ```no_run
    /// # use ytsaurus_client::Client;
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// # let client = Client::from_env()?;
    /// # let id = String::new();
    /// let status = client.operation_status(&id)?;
    /// if status.suspended {
    ///     println!("paused — it will sit at {} until it is resumed", status.state);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails or the answer cannot be
    /// decoded.
    pub fn operation_status(&self, id: &str) -> Result<OperationStatus> {
        let document = self.get_operation(id, &["state", "suspended"])?;
        Ok(OperationStatus {
            state: operation::state_of(&document)?,
            suspended: operation::suspended_of(&document)?,
        })
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
        let all = self.job_statistics(operation_id)?;
        Ok(jobs::field(&all, "custom").cloned().unwrap_or(YsonValue {
            attributes: None,
            node: YsonNode::Map(std::collections::BTreeMap::new()),
        }))
    }

    /// Everything the scheduler recorded about an operation's jobs.
    ///
    /// The whole `job_statistics` tree, custom and built-in alike.
    /// [`Client::job_statistic_sum`] is the way to read one number out of it;
    /// this is for looking around, which is how anyone finds out what a cluster
    /// actually reports.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn job_statistics(&self, operation_id: &str) -> Result<YsonValue> {
        Ok(operation::statistics_of(
            &self.get_operation(operation_id, &["progress"])?,
        ))
    }

    /// The total of one **built-in** job statistic, e.g. `time/exec`.
    ///
    /// The cluster's own statistics **nest** by path component, where a custom
    /// name keeps its slash as one key — the two are stored differently, which
    /// is why they are read differently:
    ///
    /// ```text
    /// custom:    {"rows/rejected" = {"$"  = {completed = {map = {sum=3}}}}}
    /// built-in:  {time = {exec    = {"$$" = {completed = {map = {sum=744}}}}}}
    /// ```
    ///
    /// Note the separator differs too — `$$` rather than `$`. Both are
    /// accepted here, because that difference is not something a caller should
    /// have to know.
    ///
    /// Totalled over `completed` jobs across job types, as
    /// [`Client::statistic_sum`] does, and `None` when the cluster reports
    /// nothing under that path — which is not the same as zero. A local cluster
    /// reports nothing under `user_job/cpu`, for instance.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails.
    pub fn job_statistic_sum(&self, operation_id: &str, path: &str) -> Result<Option<i64>> {
        let statistics = self.job_statistics(operation_id)?;

        let mut node = &statistics;
        for component in path.split('/') {
            match jobs::field(node, component) {
                Some(next) => node = next,
                None => return Ok(None),
            }
        }
        Ok(completed_total(node))
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
    /// **A suspended operation never reaches one**, and this says so rather
    /// than sitting there: suspension is not a state, so a paused operation
    /// goes on answering `running` for as long as it is paused. The progress
    /// line reports it, which is the difference between a wait that looks hung
    /// and one that names what it is waiting for. Resuming it — from another
    /// process, or from the one that paused it — is what ends the wait.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::OperationFailed`] if it ends as anything other
    /// than `completed`, or [`ClientError`] if polling itself fails.
    pub fn wait_for_operation(&self, id: &str) -> Result<()> {
        let started = Instant::now();
        let mut last_reported = String::new();

        loop {
            // Both attributes, in one request: a loop that watched the state
            // alone could not tell a paused operation from a running one, and
            // waiting for a resume that nobody knows is needed is the failure
            // this whole pair of readers exists to prevent.
            let OperationStatus { state, suspended } = self.operation_status(id)?;

            let reported = if suspended {
                format!("{state}, suspended")
            } else {
                state.clone()
            };
            if reported != last_reported {
                eprintln!(
                    "operation {id}: {reported} ({:.0}s)",
                    started.elapsed().as_secs_f64()
                );
                last_reported = reported;
            }

            match state.as_str() {
                "completed" => return Ok(()),
                "failed" | "aborted" => {
                    // The diagnostics go through a client that does not retry.
                    // Up to four more requests are about to be sent to explain
                    // a failure the caller already knows about, and an
                    // unhealthy cluster is exactly when they fail: under the
                    // default policy `list_jobs` alone can spend ten minutes on
                    // backoff before giving up, and every step here is
                    // best-effort, so the wait buys nothing but a program that
                    // looks hung after the operation has already ended.
                    let quick = self.without_retries();
                    return Err(ClientError::OperationFailed {
                        id: id.to_owned(),
                        state,
                        error: quick.operation_error(id),
                        jobs: quick.failed_jobs(id),
                    });
                }
                _ => std::thread::sleep(self.poll_interval),
            }
        }
    }

    /// Why an operation ended as it did, in the cluster's words.
    ///
    /// `None` for one that succeeded, and for one that has not finished. This
    /// is what [`ClientError::OperationFailed`] carries, and what reads back
    /// the `reason` given to [`Client::abort_operation`]: the reason is folded
    /// into the operation's error document rather than kept beside it, so this
    /// is how to find out who stopped an operation and why.
    ///
    /// Flattened to the outer message plus the innermost one, because the outer
    /// message of a YTsaurus error is a category and the cause is at the bottom.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the operation cannot be looked up, or if its
    /// answer cannot be decoded.
    pub fn operation_result_error(&self, id: &str) -> Result<Option<String>> {
        // Asked for through `get_operation`, not through Cypress: an operation
        // is not a node under //sys/operations on every cluster, and a local
        // one answers `has no child with key` for an id that certainly exists.
        Ok(operation::result_error_of(
            &self.get_operation(id, &["result"])?,
        ))
    }

    /// Best-effort fetch of a failed operation's error document.
    ///
    /// Prefers the flattened message. Falls back to the raw document, because a
    /// clumsy error still beats an empty one if the response shape ever moves.
    ///
    /// Used while building [`ClientError::OperationFailed`], where a failure to
    /// fetch must never replace the failure being reported — which is why this
    /// swallows errors and [`Client::operation_result_error`], which has a
    /// caller to answer to, does not.
    fn operation_error(&self, id: &str) -> Option<String> {
        // The raw body, not the parsed document: the fallback below is for the
        // case where the shape moved, and a body that does not parse at all —
        // an HTML page from an intermediary, a truncated stream — is the
        // farthest it can move. Parsing first would throw away the only
        // evidence in exactly the case the fallback exists for.
        let body = self
            .get_operation_body(
                yson_build::map([("operation_id", yson_build::string(id))]),
                &["result"],
            )
            .ok()?;

        let summary = self
            .strip_envelope(&body, "get_operation")
            .ok()
            .and_then(|document| {
                jobs::field(&document, "result")
                    .and_then(|result| jobs::error_summary(jobs::field(result, "error")?))
            });

        // Whatever the cluster said, rather than nothing: a clumsy error beats
        // an empty one if the response shape ever moves.
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

    /// Fetches one job of an operation.
    ///
    /// What [`Client::list_jobs`] reports for a job it lists, asked for by id —
    /// and the way to look at a job whose id came from somewhere else, a log
    /// line or the web interface, without listing every job of the operation.
    ///
    /// The cluster answers with the job document **unwrapped**, and calls the id
    /// `job_id` where `list_jobs` calls it `id`; both are read here, so the
    /// [`JobInfo`] that comes back is the same shape either way.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails, or if the answer names no
    /// job — which is what an unknown job id looks like.
    pub fn get_job(&self, operation_id: &str, job_id: &str) -> Result<JobInfo> {
        let params = yson_build::map([
            ("operation_id", yson_build::string(operation_id)),
            ("job_id", yson_build::string(job_id)),
        ]);
        let body = self.transport.call(
            Method::Get,
            "get_job",
            &params,
            Payload::None,
            Repeatable::Freely,
        )?;

        let document = self.strip_envelope(&body, "get_job")?;
        jobs::parse_job(&document).ok_or_else(|| ClientError::Decode {
            command: "get_job".to_owned(),
            reason: "the answer names no job".to_owned(),
        })
    }

    /// Streams the input a job was given.
    ///
    /// The rows the cluster fed to that one job, in the format its spec asked
    /// for — which is how a job that failed on one row is reproduced on a
    /// desk rather than on the cluster.
    ///
    /// This is a *heavy* command whose answer is the data, so it streams:
    /// nothing here holds the job's input, and on an installation that
    /// separates light and heavy proxies it is sent to the heavy one.
    ///
    /// **A job with no input never answers.** Measured against a local cluster:
    /// the request for a vanilla job's input sat for 30 seconds without a byte.
    /// A vanilla operation has no input tables, so there is nothing for the
    /// cluster to send and it does not say so; ask this only of a job that reads
    /// something.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails. Failures *during* the read
    /// arrive from the reader, for the reason [`ResponseReader`] describes.
    pub fn get_job_input(&self, operation_id: &str, job_id: &str) -> Result<ResponseReader> {
        let params = yson_build::map([
            ("operation_id", yson_build::string(operation_id)),
            ("job_id", yson_build::string(job_id)),
        ]);
        let body = self.transport.open(Method::Get, "get_job_input", &params)?;
        Ok(ResponseReader::new(body))
    }

    /// Fetches what a job wrote to stderr.
    ///
    /// Returns raw bytes: stderr is whatever the process wrote, not necessarily
    /// UTF-8. Empty if the cluster saved nothing — stderr is kept for failed
    /// jobs and, when the spec asks for it, for successful ones.
    ///
    /// This is a *heavy* command, so on an installation that separates light
    /// and heavy proxies it goes to the heavy one, like a table read.
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
            Repeatable::Heavy,
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

    // ------------------------------------------------------------------ raw

    /// Sends a command this crate does not model, and hands back the answer.
    ///
    /// Every other method here is a command the crate has an opinion about:
    /// parameters built for you, the response decoded into a type. This is the
    /// door to the rest of API v4 — the commands this crate has not grown yet,
    /// and the ones it never will. It is the same door
    /// [`Client::start_operation`] opens for a hand-built spec, widened from
    /// one command to all of them, and it means the answer to "can I do X
    /// against my cluster?" stops being "fork the crate".
    ///
    /// `params` is the `X-YT-Parameters` dict — build it with [`yson_build`].
    /// `payload` is the request body, for a command that takes one. What comes
    /// back is the response body, exactly as the proxy sent it; API v4 wraps a
    /// structured answer in a one-key dict, so most commands answer
    /// `{key=…}` in text YSON.
    ///
    /// ```no_run
    /// # use ytsaurus_client::{Client, Method, yson_build};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::from_env()?;
    ///
    /// // `get_supported_features` is not modelled here and takes no
    /// // parameters. It answers with what this cluster's build can do —
    /// // codecs, compression, primitive types — which is exactly the question
    /// // a crate that models a quarter of the API cannot answer for you.
    /// let body = client.raw_command(
    ///     Method::Get,
    ///     "get_supported_features",
    ///     &yson_build::empty_map(),
    ///     None,
    /// )?;
    ///
    /// println!("{}", String::from_utf8_lossy(&body));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # What this still does for you
    ///
    /// Everything that is not about the command's meaning: the token, the
    /// timeout, TLS, the header encoding, the `X-YT-Error` check that turns a
    /// cluster failure into a [`ClientError::Cluster`] with the innermost
    /// message — and the client's transaction. A raw command is stamped with
    /// `transaction_id` like every other, so a command sent through
    /// [`Transaction`] is *in* that transaction rather than quietly outside it.
    /// The exceptions are the same: a command that names its own transaction
    /// keeps it, and the scheduler commands are not stamped at all.
    ///
    /// # What it does not
    ///
    /// **It is sent once, and to the configured address.** A command this crate
    /// does not model cannot be assumed non-mutating, and a retry that applied
    /// an unknown mutation twice would be a far worse failure than one lost to
    /// a flaky proxy — so the default is [`Repeatable::Never`] and the retry
    /// policy is ignored here, whatever it says.
    ///
    /// `Never` is the safe answer for *repeating*, and it is the wrong answer
    /// for *routing*: it sends the command to the address the client was
    /// configured with, which on an installation that separates proxy roles is
    /// a control proxy that will not serve a heavy one. A raw `write_file` sent
    /// this way is refused with `Control proxy may not serve heavy requests
    /// with input data`, and a raw `read_file` is answered with a 307 to a data
    /// proxy. [`Client::raw_command_with`] is where a caller who knows the
    /// command is heavy says [`Repeatable::Heavy`] and gets both halves of that
    /// answer at once.
    ///
    /// The streaming doors need no such care:
    /// [`Client::raw_command_streaming`] and [`Client::raw_command_upload`] are
    /// heavy by construction, because streaming *is* the heavy shape.
    ///
    /// Nor does it know the verb: see [`Method`] for the cluster's own rule for
    /// picking one.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `command` is not a bare command name,
    /// if `params` is not a YSON dict — every command's parameters are one, and
    /// the client adds to them — or if a body is passed with [`Method::Get`],
    /// which carries none, so it would be dropped in silence. Otherwise
    /// [`ClientError`] as any command fails.
    pub fn raw_command(
        &self,
        method: Method,
        command: &str,
        params: &YsonValue,
        payload: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        self.raw_command_with(method, command, params, payload, Repeatable::Never, None)
    }

    /// As [`Client::raw_command`], saying how the command may be repeated.
    ///
    /// The judgement this needs is the cluster's, not a guess: a command
    /// declares whether it mutates and whether it is heavy, and [`Repeatable`]
    /// is how that reaches the retry policy. [`Repeatable::Freely`] for a read,
    /// [`Repeatable::WithMutationId`] for a light mutation the master's
    /// mutation cache covers, [`Repeatable::Heavy`] for one that moves table or
    /// file data — which also sends it to a proxy that will accept one —
    /// [`Repeatable::Never`] otherwise.
    ///
    /// "Light and mutating" is not by itself enough for a mutation ID: the
    /// cache lives in the master, and a command that goes to the **scheduler**
    /// is not covered by it. Verified for `abort_operation` — a second send of
    /// the same ID, flagged as a retry, is answered `No such operation` rather
    /// than with the first response, so the retry turns an abort that worked
    /// into an error the caller believes. Whether every scheduler command
    /// behaves that way was not checked; treat it as the working assumption
    /// and prefer `Never` when in doubt.
    ///
    /// `mutation_id` is for the guarantee a single process cannot give itself:
    /// persist it, and after a crash the same call is deduplicated against the
    /// one that already ran instead of applying twice. See [`MutationId`].
    ///
    /// An ID given here is stamped on the request **whatever `repeatable`
    /// says**, including under [`Repeatable::Never`] — the two answer different
    /// questions. `repeatable` decides whether *this* call may be sent twice;
    /// a mutation ID decides whether a *later* call, from a process that has
    /// since restarted, is recognised as the same mutation. A command that must
    /// not be retried in-process can still be worth making replayable across
    /// one, and this is how.
    ///
    /// # Errors
    ///
    /// As [`Client::raw_command`].
    pub fn raw_command_with(
        &self,
        method: Method,
        command: &str,
        params: &YsonValue,
        payload: Option<&[u8]>,
        repeatable: Repeatable,
        mutation_id: Option<&MutationId>,
    ) -> Result<Vec<u8>> {
        check_command_name(command)?;
        refuse_non_dict_parameters(command, params)?;
        refuse_body_on_get(method, command, payload.is_some())?;

        let payload = match payload {
            Some(bytes) => Payload::Bytes(bytes),
            None => Payload::None,
        };

        self.transport
            .call_with(method, command, params, payload, repeatable, mutation_id)
    }

    /// Sends a command this crate does not model and hands back its response
    /// **unread**.
    ///
    /// For a command whose answer is the data — `read_file`, `read_blob_table`,
    /// anything the cluster declares heavy on the way out. [`Client::raw_command`]
    /// would put all of it in memory first, which for those is the thing worth
    /// avoiding.
    ///
    /// ```no_run
    /// # use ytsaurus_client::{Client, Method, yson_build};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::from_env()?;
    /// // `read_file` is not modelled here: files can be written and not read
    /// // back. Until that changes, this is how one is read — and it never
    /// // holds more of the file than a buffer.
    /// let mut file = client.raw_command_streaming(
    ///     Method::Get,
    ///     "read_file",
    ///     &yson_build::map([("path", yson_build::string("//tmp/worker"))]),
    /// )?;
    ///
    /// std::io::copy(&mut file, &mut std::fs::File::create("worker")?)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Sent once, and never retried: this is the shape a heavy command takes,
    /// and the documentation is explicit that heavy commands are not repeated.
    /// It is also sent **to a heavy proxy**, for the same reason and without
    /// asking — a response that is the data is [`Repeatable::Heavy`] whatever
    /// the command turns out to be called. The request carries no body —
    /// [`Client::raw_command_upload`] is the other direction.
    ///
    /// The streaming timeout applies, so the transfer itself is not on the
    /// request clock; see [`Client::with_timeout`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `command` is not a bare command name,
    /// and [`ClientError`] if the request fails. Failures *during* the read
    /// arrive from the reader, not from here — and a body cut short by a
    /// mid-stream failure ends quietly, for the reason [`ResponseReader`]
    /// describes.
    pub fn raw_command_streaming(
        &self,
        method: Method,
        command: &str,
        params: &YsonValue,
    ) -> Result<ResponseReader> {
        check_command_name(command)?;
        refuse_non_dict_parameters(command, params)?;
        let body = self.transport.open(method, command, params)?;
        Ok(ResponseReader::new(body))
    }

    /// Sends a command this crate does not model, streaming its request body.
    ///
    /// The counterpart of [`Client::raw_command_streaming`], for a command that
    /// takes an input data stream — the PUT commands, in the cluster's own
    /// rule. `body` is read to its end and sent as it is read, so what is
    /// uploaded never has to fit in memory.
    ///
    /// This is one attempt and can never be more: a reader that has been
    /// consumed cannot be sent again. A transaction is what makes such a write
    /// safe to fail. And it goes to a heavy proxy, as
    /// [`Client::raw_command_streaming`] does and for the same reason.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `command` is not a bare command name,
    /// or if the verb is [`Method::Get`], which carries no body. Otherwise
    /// [`ClientError`] if the request fails, including when `body` itself fails
    /// to read.
    pub fn raw_command_upload(
        &self,
        method: Method,
        command: &str,
        params: &YsonValue,
        mut body: impl std::io::Read,
    ) -> Result<Vec<u8>> {
        check_command_name(command)?;
        refuse_non_dict_parameters(command, params)?;
        refuse_body_on_get(method, command, true)?;
        self.transport.upload(method, command, params, &mut body)
    }

    // -------------------------------------------------------------- helpers

    /// A copy of this client that sends each request once.
    ///
    /// For best-effort work — the diagnostics on a failed operation — where
    /// waiting out a backoff cannot improve the answer, and where the delay
    /// lands after the caller's real result is already decided.
    fn without_retries(&self) -> Self {
        self.clone().with_retries(RetryPolicy::none())
    }

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

/// A `create` inside the cache that was refused, or a `create` that failed.
///
/// Only ever called with the failure of one of the two creates
/// [`Client::upload_into_cache`] makes, both of which write into the cache
/// directory — which is what makes "denied" mean "no cache here" rather than
/// "denied something".
fn refused_or_reported(error: ClientError) -> Result<Cached> {
    if denied(&error, "create") {
        return Ok(Cached::Refused(error));
    }
    Err(error)
}

/// Whether `error` is the cluster refusing `command` on ACL grounds.
///
/// Both halves matter, and dropping either is how this would come to swallow
/// something it should report. The code alone catches every `Access denied` a
/// launch can earn, including ones no fallback addresses; the command alone
/// catches a create that failed because the path is a table, or because
/// somebody else holds a lock — failures a second attempt elsewhere would not
/// fix and a caller needs to hear about.
///
/// The code is looked for **anywhere in the document**, as
/// [`retry::is_retriable`] and `transaction_is_gone` look for theirs: an outer
/// code is often a category — `Error resolving path`, `Request retries failed`
/// — with the reason nested under it. Every transcript of this failure seen so
/// far is flat, so the walk changes nothing that has been observed; it is here
/// because the flat reading is the one that silently stops working the day a
/// proxy wraps the answer, and a fallback that stopped firing would show up as
/// a launch that used to work.
fn denied(error: &ClientError, command: &str) -> bool {
    matches!(
        error,
        ClientError::Cluster {
            command: failed,
            code,
            raw,
            ..
        } if failed == command
            && (*code == ACCESS_DENIED || retry::raw_contains_code(raw, &[ACCESS_DENIED]))
    )
}

/// Refuses a command name that would address something other than a command.
///
/// A name goes straight into `/api/v4/{command}`, and every modelled command
/// puts a literal there. The raw door takes one from a caller, so a name
/// carrying `/`, `?`, `#` or whitespace could reach a different path, append a
/// query string, or truncate the URL — none of which the caller would see,
/// because what came back would still be a plausible answer from *something*.
///
/// Command names in the driver's registry are lowercase words joined by
/// underscores, so this accepts a superset of them and nothing that changes the
/// shape of the URL. A name this refuses that a future cluster accepts is a
/// one-line change here; the reverse is a bug nobody can see.
fn check_command_name(command: &str) -> Result<()> {
    if command.is_empty() {
        return Err(ClientError::Config(
            "a raw command needs a command name, e.g. \"get_supported_features\"".to_owned(),
        ));
    }

    if let Some(bad) = command
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '_')
    {
        return Err(ClientError::Config(format!(
            "{command:?} is not a command name: it contains {bad:?}, and the name \
             goes into the request path as it is. A command is a bare name like \
             \"get_supported_features\" — the path it acts on is a parameter."
        )));
    }

    Ok(())
}

/// Refuses parameters that are not a dict.
///
/// `X-YT-Parameters` is a dict on every command, including the ones that take
/// none — [`yson_build::empty_map`] is the spelling for those. The client also
/// *adds* to what it is given: a transaction id, a mutation id and its retry
/// flag are all inserted into the caller's parameters on the way out, and
/// inserting into a value that is not a dict panics. A caller who passes a list
/// or a string here has made a mistake the cluster would report in its own
/// words at best, and which would otherwise abort their process.
fn refuse_non_dict_parameters(command: &str, params: &YsonValue) -> Result<()> {
    if !matches!(params.node, YsonNode::Map(_)) {
        return Err(ClientError::Config(format!(
            "{command}: command parameters are a YSON dict, and this is a \
             {:?}. A command that takes no parameters sends `yson_build::empty_map()`.",
            params.node
        )));
    }
    Ok(())
}

/// Refuses a request body on a verb that does not carry one.
///
/// `Transport::dispatch` sends a GET through `ureq`'s bodiless builder, which
/// is right — every GET command has an empty input stream by definition. A
/// caller who passes a payload anyway has picked the wrong verb, and the body
/// would otherwise be dropped without a word. See [`Method`] for the rule that
/// decides which verb a command wants.
fn refuse_body_on_get(method: Method, command: &str, has_body: bool) -> Result<()> {
    if has_body && matches!(method, Method::Get) {
        return Err(ClientError::Config(format!(
            "{command}: a GET carries no request body, so the payload would be \
             dropped in silence. A command with an input data stream is a PUT."
        )));
    }
    Ok(())
}

/// Finds a token the way the `yt` CLI finds one.
///
/// `YT_TOKEN`, then `YT_TOKEN_PATH`, then `~/.yt/token` — first one that has
/// something in it wins. Nothing here fails: a cluster that wants no token is
/// ordinary, and so is a home directory with no `.yt` in it.
fn token_from_environment() -> Option<String> {
    if let Some(token) = std::env::var("YT_TOKEN").ok().and_then(clean_token) {
        return Some(token);
    }

    if let Ok(path) = std::env::var("YT_TOKEN_PATH")
        && let Some(token) = read_token_file(std::path::Path::new(&path))
    {
        return Some(token);
    }

    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()?;
    read_token_file(&std::path::Path::new(&home).join(".yt").join("token"))
}

/// Reads a token out of a file, if there is one to read.
fn read_token_file(path: &std::path::Path) -> Option<String> {
    std::fs::read_to_string(path).ok().and_then(clean_token)
}

/// A token with the whitespace taken off, or nothing if that leaves nothing.
///
/// The trailing newline is the point: `echo token > ~/.yt/token` writes one,
/// and a header carrying it fails authentication with an error that never
/// mentions the newline.
fn clean_token(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// Decodes a binary YSON list fragment into typed rows.
///
/// Shared by [`Client::read_table_rows`] and the tests that check what the row
/// encoder produced, so the two halves of the round trip are the same code.
fn decode_rows<T: serde::de::DeserializeOwned>(bytes: &[u8], path: &str) -> Result<Vec<T>> {
    let mut rows = Vec::new();
    let mut stream = ytsaurus_yson::StreamDeserializer::<T>::new(bytes, true);

    loop {
        match stream.next_item() {
            Ok(Some(row)) => rows.push(row),
            Ok(None) => return Ok(rows),
            Err(e) => {
                return Err(ClientError::Decode {
                    command: "read_table".to_owned(),
                    reason: format!("{path}: row {}: {e}", rows.len()),
                });
            }
        }
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
    // `$` under a custom statistic, `$$` under a built-in one. The cluster
    // spells the same idea two ways depending on which tree you are in.
    let by_state = jobs::field(statistic, "$").or_else(|| jobs::field(statistic, "$$"));
    let Some(by_state) = by_state else {
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
#[cfg(test)]
fn check_complete_fragment(data: &[u8]) -> std::result::Result<(), String> {
    check_complete_yson_fragment(data, YsonFormat::Binary)
}

/// Verifies that `data` is a whole YSON list fragment in `format`.
fn check_complete_yson_fragment(
    mut data: &[u8],
    format: YsonFormat,
) -> std::result::Result<(), String> {
    use ytsaurus_yson::{Scan, scan_value};

    let total = data.len();
    loop {
        while data.first() == Some(&b';') || data.first().is_some_and(u8::is_ascii_whitespace) {
            data = &data[1..];
        }
        if data.is_empty() {
            return Ok(());
        }

        match scan_value(data, format) {
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
                    "the response is not valid {format:?} YSON at byte {}: {e}",
                    total - data.len()
                ));
            }
        }
    }
}

fn unsupported_data_format() -> ClientError {
    ClientError::Config(
        "this ytsaurus-client version does not support the selected data format".to_owned(),
    )
}

/// Builds the rich table path a direct Skiff table read/write requires.
///
/// The Go SDK derives this `columns` projection from the single table schema;
/// without it the positional tuple has no explicit column selection. Job I/O
/// differs: its format may have several schemas and uses the Variant16 table
/// prefix, so it is deliberately configured through operation specs instead.
///
/// The path's own attributes are kept: a Skiff write to an appending
/// [`TablePath`] has to append, exactly as the YSON one does.
/// Refuses a spec whose Skiff format does not describe the tables it will meet.
///
/// Refused here rather than sent, for the reason the duplicate-task check
/// above is: the cluster's answer to this is a rejected operation at best, and
/// at worst a job that reads a table its format does not describe and fails
/// part-way through, having already written output that now has to be cleaned
/// up.
fn refuse_skiff_table_mismatch(mismatch: Option<String>) -> Result<()> {
    match mismatch {
        Some(reason) => Err(ClientError::Config(reason)),
        None => Ok(()),
    }
}

fn skiff_table_path(path: &TablePath, format: &SkiffFormat) -> Result<YsonValue> {
    if format.table_schemas().len() != 1 {
        return Err(ClientError::Config(format!(
            "Skiff table I/O requires exactly one table schema, got {}",
            format.table_schemas().len()
        )));
    }
    let schema = format.table_schema(0).map_err(|error| {
        ClientError::Config(format!(
            "Skiff table I/O has an invalid table schema: {error}"
        ))
    })?;
    let columns = schema
        .children
        .iter()
        .map(|column| {
            let name = column.name.as_deref().ok_or_else(|| {
                ClientError::Config("Skiff table I/O schema has an unnamed column".to_owned())
            })?;
            if matches!(name, "$key_switch" | "$row_index" | "$range_index") {
                return Err(ClientError::Config(format!(
                    "Skiff table I/O schema contains job-only system column {name}"
                )));
            }
            Ok(yson_build::string(name))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut attributes = vec![("columns", yson_build::list(columns))];
    if path.is_append() {
        attributes.push(("append", yson_build::boolean(true)));
    }

    Ok(yson_build::with_attributes(
        yson_build::string(path.as_str()),
        attributes,
    ))
}

/// Checks that a returned or submitted Skiff stream is a whole number of rows.
///
/// Walks the rows without building them: `skip_row` applies the same framing,
/// schema and limit checks the decoder does — including the per-blob bound —
/// and allocates nothing. Decoding instead would build a `Value` tree for
/// every row of the caller's whole table only to drop it, which on the write
/// path is a second copy of the table in memory before the request is even
/// made. The YSON counterpart walks record boundaries the same way.
fn check_complete_skiff_stream(
    data: &[u8],
    format: &SkiffFormat,
) -> std::result::Result<(), String> {
    let mut decoder = SkiffDecoder::new(data, format.clone());
    while decoder
        .skip_row()
        .map_err(|error| format!("not a complete Skiff stream: {error}"))?
        .is_some()
    {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    use super::*;
    use ytsaurus_skiff::{Encoder as SkiffEncoder, Schema, SchemaRef, Value, WireType};

    /// A real `get_operation` answer, captured from the local cluster for an
    /// operation that was completed early.
    const GET_OPERATION: &str = include_str!("../tests/fixtures/get_operation.yson");

    /// The narrow readers are each one attribute of `get_operation`, and each
    /// assumes where that attribute sits. A response shape is a guess until
    /// something runs against a real answer, so this calls the readers
    /// themselves — the ones `operation_state`, `operation_suspended`,
    /// `operation_status` and `operation_result_error` are — on a document a
    /// cluster sent. Re-implementing the field access here instead would pass
    /// just as happily after a reader started looking somewhere else.
    ///
    /// Three of the four attributes: the capture does not include `progress`,
    /// so `job_statistics` is pinned separately below against a shape that is
    /// stated to be a guess rather than pretending otherwise.
    #[test]
    fn the_narrow_readers_agree_with_a_document_a_cluster_sent() {
        let document = from_slice(GET_OPERATION.as_bytes(), YsonFormat::Text).expect("valid YSON");

        assert_eq!(
            operation::state_of(&document).expect("the capture carries a state"),
            "completed"
        );
        assert!(
            !operation::suspended_of(&document).expect("and a boolean beside it"),
            "suspension is read from its own attribute, not from the state"
        );

        // The case `operation_result_error` exists to get right: an operation
        // that succeeded still has an error document, code 0 with an empty
        // message. Reporting that as `Some("")` would fire on every success.
        assert_eq!(
            operation::result_error_of(&document),
            None,
            "a completed operation's code-0 error document is not a failure"
        );
    }

    /// The deepest of the four guesses — `progress` → `job_statistics` — and
    /// the one the captured document cannot pin, because it was fetched
    /// without `progress`. Written out here so the assumption is at least
    /// visible and breaks a test when the reader stops matching it.
    #[test]
    fn job_statistics_are_read_from_under_progress() {
        let document = from_slice(
            br#"{"progress"={"job_statistics"={"time"={"exec"={"$$"={"completed"={"map"={"sum"=744}}}}}}}}"#,
            YsonFormat::Text,
        )
        .expect("valid YSON");

        let statistics = operation::statistics_of(&document);
        assert!(
            jobs::field(&statistics, "time").is_some(),
            "the subtree, not the progress node that holds it: {statistics:?}"
        );

        // And the empty answer, which is what an operation that has not run a
        // job yet gives — distinct from a failure to find the attribute.
        let empty = from_slice(br#"{"progress"={}}"#, YsonFormat::Text).expect("valid YSON");
        assert!(matches!(
            operation::statistics_of(&empty).node,
            YsonNode::Map(ref m) if m.is_empty()
        ));
    }

    /// The client inserts a transaction id, a mutation id and a retry flag
    /// into the parameters it is handed, and inserting into anything that is
    /// not a dict panics. A caller's mistake must be an error rather than the
    /// end of their process.
    #[test]
    fn raw_parameters_that_are_not_a_dict_are_refused() {
        let client = Client::new("http://localhost:8000").with_retries(RetryPolicy::none());
        let not_a_dict = yson_build::list([yson_build::string("get_supported_features")]);

        let refused = client.raw_command(Method::Get, "get_supported_features", &not_a_dict, None);
        assert!(
            matches!(refused, Err(ClientError::Config(_))),
            "a list of parameters is a mistake to report, not to panic on"
        );
        assert!(refuse_non_dict_parameters("c", &yson_build::empty_map()).is_ok());
    }

    /// An id that came out of a file the way the documentation shows keeps its
    /// newline, and the cluster answers a whitespace-carrying id with an error
    /// that never mentions whitespace.
    #[test]
    fn an_attached_id_is_trimmed() {
        let client = Client::new("http://localhost:8000");
        assert_eq!(client.attach_operation("1-2-3-4\n").id(), "1-2-3-4");
        assert_eq!(client.attach_operation("  1-2-3-4  ").id(), "1-2-3-4");
        assert_eq!(client.attach_operation("1-2-3-4").id(), "1-2-3-4");
    }

    #[test]
    fn a_get_answer_decodes_straight_into_the_type_asked_for() {
        // What `get_as` does with the response body, without a cluster to ask.
        // The point of the envelope struct: one pass over the document, and
        // attributes the type does not mention are skipped rather than
        // collected — which is what makes `//@`, with dozens of them, worth
        // asking about at all.
        #[derive(serde::Deserialize)]
        struct Node {
            account: String,
            #[serde(rename = "type")]
            node_type: String,
        }

        let body = br#"{"value"={"account"="tmp";"type"="table";"chunk_count"=3}}"#;
        let envelope: Envelope<Node> = from_slice(body, YsonFormat::Text).expect("decodes");

        assert_eq!(envelope.value.account, "tmp");
        assert_eq!(envelope.value.node_type, "table");
    }

    #[test]
    fn an_answer_that_does_not_fit_the_type_is_an_error_rather_than_a_default() {
        #[derive(serde::Deserialize)]
        struct Node {
            #[allow(dead_code)]
            account: String,
        }

        // No `account` at all: silently defaulting it would hand the caller a
        // node that does not exist.
        let body = br#"{"value"={"type"="table"}}"#;
        assert!(from_slice::<Envelope<Node>>(body, YsonFormat::Text).is_err());
    }

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

    fn skiff_format() -> SkiffFormat {
        SkiffFormat::new(vec![SchemaRef::Inline(Schema::tuple([
            Schema::named("found", WireType::Uint64),
            Schema::named("rcl", WireType::String32),
        ]))])
        .expect("a named tuple is a direct-table format")
    }

    #[test]
    fn skiff_table_path_selects_schema_columns() {
        let value = skiff_table_path(&TablePath::from("//tmp/table"), &skiff_format()).unwrap();
        let rendered = ytsaurus_yson::to_string(&value, YsonFormat::Text).unwrap();
        assert_eq!(rendered, r#"<columns=[found;rcl]>"//tmp/table""#);
    }

    #[test]
    fn skiff_stream_completeness_uses_the_declared_schema() {
        let schema = skiff_format().table_schema(0).unwrap().clone();
        let mut encoder = SkiffEncoder::new(Vec::new(), schema).unwrap();
        encoder
            .write(&Value::Tuple(vec![
                Value::Uint64(7),
                Value::Bytes(b"ok".to_vec()),
            ]))
            .unwrap();
        let complete = encoder.into_inner().unwrap();

        assert!(check_complete_skiff_stream(&complete, &skiff_format()).is_ok());
        for cut in 1..complete.len() {
            assert!(
                check_complete_skiff_stream(&complete[..cut], &skiff_format()).is_err(),
                "cut at {cut} must not pass"
            );
        }
    }

    #[test]
    fn direct_skiff_table_format_rejects_multi_table_and_job_controls() {
        let multiple = SkiffFormat::new(vec![
            SchemaRef::Inline(Schema::tuple([Schema::named("a", WireType::Uint64)])),
            SchemaRef::Inline(Schema::tuple([Schema::named("b", WireType::Uint64)])),
        ])
        .unwrap();
        assert!(matches!(
            skiff_table_path(&TablePath::from("//tmp/table"), &multiple),
            Err(ClientError::Config(_))
        ));

        let job_control =
            SkiffFormat::new(vec![SchemaRef::Inline(Schema::tuple([Schema::named(
                "$key_switch",
                WireType::Boolean,
            )]))])
            .unwrap();
        assert!(matches!(
            skiff_table_path(&TablePath::from("//tmp/table"), &job_control),
            Err(ClientError::Config(_))
        ));
    }

    #[test]
    fn skiff_table_calls_use_schema_format_columns_and_raw_streams() {
        let schema = skiff_format().table_schema(0).unwrap().clone();
        let mut encoder = SkiffEncoder::new(Vec::new(), schema).unwrap();
        encoder
            .write(&Value::Tuple(vec![
                Value::Uint64(7),
                Value::Bytes(b"ok".to_vec()),
            ]))
            .unwrap();
        let stream = encoder.into_inner().unwrap();

        let (proxy, write_request) = one_request_proxy(Vec::new());
        Client::new(&proxy)
            .write_table_with_format("//tmp/write", &stream, &DataFormat::skiff(skiff_format()))
            .unwrap();
        let write_request = write_request.join().unwrap();
        assert!(write_request.starts_with(b"PUT /api/v4/write_table HTTP/1.1\r\n"));
        let write_headers = String::from_utf8_lossy(&write_request);
        assert!(
            write_headers.contains("input_format=<table_skiff_schemas="),
            "{write_headers}"
        );
        assert!(
            write_headers.contains(r#"path=<columns=[found;rcl]>"//tmp/write""#),
            "{write_headers}"
        );
        assert!(write_request.ends_with(&stream));

        let (proxy, read_request) = one_request_proxy(stream.clone());
        let received = Client::new(&proxy)
            .read_table_with_format("//tmp/read", &DataFormat::skiff(skiff_format()))
            .unwrap();
        let read_request = read_request.join().unwrap();
        assert!(read_request.starts_with(b"GET /api/v4/read_table HTTP/1.1\r\n"));
        let read_headers = String::from_utf8_lossy(&read_request);
        assert!(
            read_headers.contains("output_format=<table_skiff_schemas="),
            "{read_headers}"
        );
        assert!(
            read_headers.contains(r#"path=<columns=[found;rcl]>"//tmp/read""#),
            "{read_headers}"
        );
        assert_eq!(received, stream);
    }

    #[test]
    fn shared_yson_table_format_uses_the_requested_yson_encoding() {
        let (proxy, request) = one_request_proxy(Vec::new());
        Client::new(&proxy)
            .write_table_with_format("//tmp/write", b"{value=one};", &DataFormat::text_yson())
            .unwrap();

        let request = request.join().unwrap();
        let request = String::from_utf8_lossy(&request);
        assert!(
            request.contains("input_format=<format=text>yson"),
            "{request}"
        );
    }

    #[test]
    fn a_raw_command_goes_where_it_says_with_the_parameters_it_was_given() {
        let (proxy, request) = one_request_proxy(br#"{"value"={};}"#.to_vec());
        let body = Client::new(&proxy)
            .raw_command(
                Method::Get,
                "get_supported_features",
                &yson_build::empty_map(),
                None,
            )
            .expect("sends");

        let request = request.join().unwrap();
        assert!(
            request.starts_with(b"GET /api/v4/get_supported_features HTTP/1.1\r\n"),
            "{}",
            String::from_utf8_lossy(&request)
        );

        let headers = String::from_utf8_lossy(&request);
        assert!(headers.contains("x-yt-parameters: {}"), "{headers}");
        // Handed back as it arrived. A raw command has no idea what the answer
        // means, and decoding it would be this crate guessing.
        assert_eq!(body, br#"{"value"={};}"#);
    }

    #[test]
    fn a_raw_command_carries_its_payload_and_its_transaction() {
        let (proxy, request) = one_request_proxy(Vec::new());
        Client::new(&proxy)
            .with_transaction("3-5d231-10001-db88")
            .raw_command(
                Method::Put,
                "write_file",
                &yson_build::map([("path", yson_build::string("//tmp/f"))]),
                Some(b"payload"),
            )
            .expect("sends");

        let request = request.join().unwrap();
        let headers = String::from_utf8_lossy(&request);

        assert!(
            request.starts_with(b"PUT /api/v4/write_file HTTP/1.1\r\n"),
            "{headers}"
        );
        assert!(request.ends_with(b"payload"), "{headers}");
        // The whole point of routing this through `Transport` rather than
        // handing out a bare `ureq` agent: a raw command inside a transaction
        // is *in* it, not quietly beside it.
        assert!(
            headers.contains(r#"transaction_id="3-5d231-10001-db88""#),
            "{headers}"
        );
    }

    #[test]
    fn a_raw_command_is_sent_once_unless_the_caller_says_otherwise() {
        // A command this crate does not model cannot be assumed idempotent, so
        // the default ignores the retry policy. Proved by serving one request
        // from a listener that would accept a second: a retried request would
        // hang here rather than fail.
        let (proxy, request) = one_request_proxy(Vec::new());
        let client = Client::new(&proxy).with_retries(RetryPolicy::none());
        client
            .raw_command(Method::Post, "concatenate", &yson_build::empty_map(), None)
            .expect("sends");
        request.join().unwrap();
    }

    #[test]
    fn a_mutation_id_is_sent_even_when_the_command_is_not_retried() {
        // The two answer different questions: `Repeatable` decides whether
        // *this* call may go twice, a mutation ID whether a *later* call from a
        // restarted process is recognised as the same mutation. A command too
        // dangerous to retry in-process can still be worth making replayable
        // across one, so the ID must not be dropped along with the retries.
        let id = MutationId::new().as_retry();
        let (proxy, request) = one_request_proxy(Vec::new());
        Client::new(&proxy)
            .raw_command_with(
                Method::Post,
                "concatenate",
                &yson_build::empty_map(),
                None,
                Repeatable::Never,
                Some(&id),
            )
            .expect("sends");

        let request = request.join().unwrap();
        let sent = sent_parameters(&request);

        assert_eq!(
            parameter(&sent, "mutation_id").and_then(YsonValue::as_str),
            Some(id.as_str()),
            "{}",
            String::from_utf8_lossy(&request)
        );
        // And it admits to being a replay, which is what the cluster refuses a
        // duplicate for not doing.
        assert_eq!(
            parameter(&sent, "retry").map(|v| &v.node),
            Some(&YsonNode::Boolean(true)),
            "{}",
            String::from_utf8_lossy(&request)
        );
    }

    /// The `X-YT-Parameters` document of a captured request, decoded.
    ///
    /// Reading the value rather than its spelling, because the spelling of a
    /// *generated* value is not stable. The text YSON writer leaves a string
    /// unquoted when it looks like an identifier — first byte a letter or `_`,
    /// the rest alphanumeric or `_-.`, see `ser::is_safe_unquoted` — and a
    /// mutation ID is a hex GUID printed with no leading zeros. So
    /// `ebd6e011-…` goes on the wire bare and `3f2a1b-…` goes on it quoted,
    /// decided by the first hex digit: **measured at 39.8 % unquoted over
    /// 100 000 IDs**, which is what an assertion on either spelling would have
    /// cost in flakes. Both spell the same string and the cluster takes both —
    /// the `idempotent` example deduplicated a replay whose ID went unquoted.
    fn sent_parameters(request: &[u8]) -> YsonValue {
        let head = String::from_utf8_lossy(request);
        let line = head
            .lines()
            .find(|line| {
                line.split_once(':')
                    .is_some_and(|(name, _)| name.eq_ignore_ascii_case("x-yt-parameters"))
            })
            .unwrap_or_else(|| panic!("no X-YT-Parameters header in:\n{head}"));

        let value = line
            .split_once(':')
            .expect("the header has a value")
            .1
            .trim();
        from_slice(value.as_bytes(), YsonFormat::Text)
            .unwrap_or_else(|e| panic!("parameters are not text YSON ({e}): {value}"))
    }

    /// One entry of a decoded parameter document.
    ///
    /// `YsonValue` indexes with a panicking `Index`, and a panic here would
    /// throw away the request the assertion wants to print.
    fn parameter<'a>(params: &'a YsonValue, key: &str) -> Option<&'a YsonValue> {
        match &params.node {
            YsonNode::Map(m) => m.get(key.as_bytes()),
            _ => None,
        }
    }

    #[test]
    fn a_command_name_that_would_change_the_url_is_refused() {
        // The name goes into `/api/v4/{command}` as it is. A caller that got
        // one from configuration must not be able to address `//sys` or append
        // a query string, because the answer would still look like an answer.
        let client = Client::new("http://localhost:8000");
        for bad in [
            "",
            "get/../../hosts",
            "get?x=1",
            "get#frag",
            "get value",
            "get%2f",
        ] {
            let error = client
                .raw_command(Method::Get, bad, &yson_build::empty_map(), None)
                .expect_err(&format!("{bad:?} was accepted as a command name"));
            assert!(matches!(error, ClientError::Config(_)), "{bad:?}: {error}");
        }

        assert!(check_command_name("get_supported_features").is_ok());
        assert!(check_command_name("start_tx").is_ok());
        // A digit is fine: `v3`-era names carry them and a future command may.
        assert!(check_command_name("read_table_partition2").is_ok());
    }

    #[test]
    fn a_payload_on_a_get_is_refused_rather_than_dropped() {
        // `dispatch` sends a GET through ureq's bodiless builder, so the bytes
        // would go nowhere and the request would succeed. Silent is the one
        // thing it must not be.
        let error = Client::new("http://localhost:8000")
            .raw_command(
                Method::Get,
                "read_table",
                &yson_build::empty_map(),
                Some(b"x"),
            )
            .expect_err("a GET with a body is a mistake");
        assert!(matches!(error, ClientError::Config(_)), "{error}");

        assert!(refuse_body_on_get(Method::Get, "get", false).is_ok());
        assert!(refuse_body_on_get(Method::Put, "write_file", true).is_ok());
        assert!(refuse_body_on_get(Method::Post, "create", true).is_ok());
    }

    fn one_request_proxy(body: Vec<u8>) -> (String, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request = read_http_request(&mut stream);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(&body).unwrap();
            request
        });
        (format!("http://{address}"), task)
    }

    fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        let expected = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read != 0, "client closed before sending a complete request");
            request.extend_from_slice(&buffer[..read]);
            let Some(headers_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..headers_end + 4]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then_some(value.trim())
                })
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            break headers_end + 4 + content_length;
        };
        while request.len() < expected {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read != 0, "client closed before sending its request body");
            request.extend_from_slice(&buffer[..read]);
        }
        request
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
    fn a_token_file_written_with_echo_still_works() {
        // `echo token > ~/.yt/token` is how these files get written, and the
        // newline it leaves would fail authentication with an error that never
        // mentions a newline.
        let path = std::env::temp_dir().join(format!(
            "ytsaurus-rs-token-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, "  secret-token\n").expect("writes");

        assert_eq!(read_token_file(&path).as_deref(), Some("secret-token"));

        std::fs::write(&path, "\n \n").expect("writes");
        assert_eq!(read_token_file(&path), None, "whitespace is not a token");

        std::fs::remove_file(&path).ok();
        assert_eq!(
            read_token_file(&path),
            None,
            "a missing file is no token, not an error"
        );
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
