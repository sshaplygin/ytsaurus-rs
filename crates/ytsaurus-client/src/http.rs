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
//! A failure the proxy finds after it has begun streaming a 200 is reported in
//! an `X-YT-Error` trailer, and `ureq` 3.3 exposes no trailers. A truncated
//! stream is caught instead by checking for a complete YSON list fragment
//! (`Client::read_table`), or on the streaming path by the decoder failing on
//! the cut record. A mid-stream failure that still produces well-formed output
//! goes unnoticed.
//!
//! A control proxy will not serve a heavy command, so heavy commands go where
//! `/hosts` says: [`Transport::base_for`] decides and [`HeavyProxy`] remembers.
//! What a control proxy answers instead is in [where a heavy command
//! goes](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#where-a-heavy-command-goes).

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
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
/// Where a redirect points. Read by this client, not `ureq`: see
/// [`Transport::redirect`].
const LOCATION: &str = "Location";
/// How many redirects one request may follow before the chain is called a loop:
/// `ureq`'s own default.
const MAX_REDIRECTS: usize = 10;

/// How much of a response a buffered command will hold in memory: 512 MiB,
/// counted after decompression.
///
/// Large enough not to truncate a modest table, which `ureq`'s default does
/// silently. [`Client::read_table`](crate::Client::read_table) and
/// [`Client::read_file`](crate::Client::read_file) each have a streaming half
/// that holds nothing, and the error names it ([`body_failure`]).
///
/// `ureq`'s `limit()` counts wire bytes beneath the gzip decoder, so
/// [`CapReader`] counts above it. The cap is what is held, not a process
/// budget: the growing `Vec` copies, so the peak resident set exceeds it. See
/// [response size
/// limits](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#response-size-limits).
///
/// It covers [`Transport::send`] and [`Transport::upload`], which read through
/// [`read_capped`]. The non-2xx branch of [`Transport::open`] and the `/hosts`
/// lookup in [`Transport::fetch`] take `ureq`'s wire-only limit and are not
/// bounded in memory; both read an answer the client is about to fail on.
const RESPONSE_LIMIT: u64 = 512 * 1024 * 1024;

/// The commands the cluster declares heavy: they carry a data stream.
///
/// The [command reference](https://ytsaurus.tech/docs/en/api/commands) marks
/// `read_table`, `write_table`, `read_file`, `write_file` and `read_blob_table`
/// heavy; the answers of `get_job_input` and `get_job_stderr` are data streams
/// too. `read_blob_table` has no method here and is reachable through
/// [`Client::raw_command_streaming`](crate::Client::raw_command_streaming).
///
/// Used only to decide whether a refused redirect is told to go to a heavy
/// proxy: a heavy raw command missing here loses the advice, not the refusal.
/// [`Repeatable::Heavy`] encodes the same `isHeavy` bit for routing, so a new
/// `Repeatable::Heavy` call site belongs on this list too.
const HEAVY: &[&str] = &[
    "read_table",
    "write_table",
    "read_file",
    "write_file",
    "read_blob_table",
    "get_job_input",
    "get_job_stderr",
];

/// Whether `command` is one the cluster declares heavy.
///
/// Read by the redirect advice, and by
/// [`BatchRequest::raw`](crate::BatchRequest::raw) for this crate's own policy
/// that bulk data does not travel inline in a batch. Which commands the cluster
/// accepts as batch parts depends on their data types, in
/// `batch::NOT_A_BATCH_PART`.
pub(crate) fn is_heavy(command: &str) -> bool {
    HEAVY.contains(&command)
}

/// The W3C trace context, in the spelling the proxy parses. See
/// [`TraceContext`](crate::TraceContext).
const TRACEPARENT: &str = "traceparent";
/// The vendor state the standard pairs with `traceparent`, forwarded with it.
const TRACESTATE: &str = "tracestate";

/// The parameter that puts a command inside a transaction.
const TRANSACTION_ID: &str = "transaction_id";

/// A PEM file of root certificates to verify the cluster against, instead of
/// the Mozilla bundle `ureq` compiles in. See [`root_certs`].
#[cfg(feature = "tls")]
const CA_BUNDLE: &str = "YT_CA_BUNDLE";

/// The most a root bundle may weigh. Mozilla's
/// `/etc/ssl/certs/ca-certificates.crt` is about 200 KB. Nothing else bounds
/// the read, and [`Client::new`](crate::Client::new) is infallible, so a huge
/// file named by accident would be paid for in memory before anyone was told.
#[cfg(feature = "tls")]
const MAX_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;

/// The cluster's words when a control proxy refuses a heavy command, `Control
/// proxy may not serve heavy requests with input data`
/// (`TContext::TryRedirectHeavyRequests`). Read by
/// [`crate::retry::worth_asking_again`], and by [`refusal_hint`] to explain the
/// refusal at the configured address.
pub(crate) const CONTROL_REFUSAL: &str = "may not serve heavy requests";

/// Commands that have no transaction to be in.
///
/// These go to the scheduler and the controller agents, not the master, and
/// take no `TTransactionalOptions`; stamping them works only while the proxy
/// drops parameters it does not recognise. `start_operation` is not here: an
/// operation can run inside a transaction, which keeps its output tables
/// invisible until the launcher commits.
///
/// `execute_batch`'s options (`TExecuteBatchOptions : TMutatingOptions`) have
/// no transactional half, and the cluster ignores an envelope `transaction_id`,
/// so `Client::execute_batch` stamps each part instead. See [batched
/// commands](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#batched-commands).
const NO_TRANSACTION: &[&str] = &[
    "execute_batch",
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

/// Whether `command` is one the blanket transaction stamp skips. Also read by
/// `Client::execute_batch` for each part, so that the list exists once.
pub(crate) fn takes_no_transaction(command: &str) -> bool {
    NO_TRANSACTION.contains(&command)
}

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
/// `ureq`'s [`SendBody`] is one-shot, so following a redirect needs a body that
/// can produce a fresh one per hop. A redirect asks two things of it: can it be
/// sent again ([`Outgoing::replayable`]; if not, [`RedirectRefusal::Body`]),
/// and would sending it hand someone data ([`Outgoing::carries_data`]; see
/// [`RedirectRefusal::Payload`]). An empty slice can be sent again and carries
/// no data.
enum Outgoing<'a> {
    /// No body: neither `Content-Length` nor `Transfer-Encoding`.
    /// [`Transport::open`]'s request. Unlike `Bytes(&[])`, a body of length
    /// zero, it puts nothing on the wire.
    Empty,
    /// Bytes held in memory, and so sent again to wherever a redirect points.
    /// An empty slice, which most API v4 commands send, goes out as
    /// `Content-Length: 0`.
    Bytes(&'a [u8]),
    /// A body read as it is sent:
    /// [`Client::write_table_rows`](crate::Client::write_table_rows) and
    /// [`Client::raw_command_upload`](crate::Client::raw_command_upload). A
    /// reader cannot be rewound, so a redirect on one is refused.
    Stream(&'a mut dyn std::io::Read),
}

impl Outgoing<'_> {
    /// Whether a redirect on this request could send the same request again.
    fn replayable(&self) -> bool {
        !matches!(self, Outgoing::Stream(_))
    }

    /// Whether there are bytes here that a redirect would be giving away. A
    /// body of length zero, such as a `POST create`'s, is not data.
    fn carries_data(&self) -> bool {
        match self {
            Outgoing::Empty => false,
            Outgoing::Bytes(bytes) => !bytes.is_empty(),
            Outgoing::Stream(_) => true,
        }
    }
}

/// How long the whole `/hosts` lookup may take, in one attempt: not the
/// client's request timeout or retry policy.
///
/// The lookup sits in front of the first heavy command, under the mutex, and
/// failing it is not fatal: the command goes to the configured address. The
/// client's policy would spend fifteen seconds on a 503 and ten minutes on a
/// hang. A failed lookup is asked again after [`HOSTS_RETRY_AFTER`].
const HOSTS_TIMEOUT: Duration = Duration::from_millis(800);

/// How long the configured address serves heavy commands after a lookup that
/// did not settle, before the cluster is asked again.
///
/// A failed heavy command does not come here: it drops its host from the pool
/// ([`Transport::after_heavy`]), and only an empty pool falls back. Falling
/// back at once would send the next uploads to a control proxy that refuses
/// them. A failed refresh does not come here either: the pool still routes, and
/// the question waits another [`HOST_LIST_REFRESH_INTERVAL`]
/// ([`Transport::base_for`]).
///
/// Short, because it is how quickly routing returns; long enough that a broken
/// `/hosts` costs [`HOSTS_TIMEOUT`] a few times a minute rather than per
/// upload. Settable through [`Transport::set_hosts_retry_after`] so that a test
/// can outlive a window.
const HOSTS_RETRY_AFTER: Duration = Duration::from_secs(10);

/// How old a `/hosts` answer may grow before a heavy command re-asks.
///
/// The [proxy
/// guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload) asks for
/// a re-query "every minute or every few queries". Done lazily, by the heavy
/// command that finds the list stale, as the C++ client's `THostManager` does,
/// so no background thread. Settable through
/// [`Transport::set_host_list_refresh_interval`] so that a test can observe it.
const HOST_LIST_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Where the cluster wants heavy commands sent.
///
/// Resolved on the first heavy command and maintained after that: refreshed
/// when old, a failed host dropped ([`Transport::base_for`]). Shared by every
/// clone (`Client::with_transaction`, `Operation`, the diagnostics client), so
/// that there is one lookup rather than one per command.
#[derive(Debug)]
enum HeavyProxy {
    /// The cluster has not been asked yet.
    Unasked,
    /// The answer `/hosts` gave, kept whole and picked from at random.
    Pool(HeavyPool),
    /// The cluster was asked and named none this client may use, so the
    /// configured address serves heavy commands too: a single-node cluster, an
    /// installation without separate roles, or a `/hosts` answer refused by
    /// [`heavy_base`].
    ///
    /// Unlike [`HeavyProxy::FellBack`], re-asked on the lazy
    /// [`HOST_LIST_REFRESH_INTERVAL`] rather than after [`HOSTS_RETRY_AFTER`].
    /// It still expires, so that an empty answer caught during a rolling
    /// restart is not kept for the client's life.
    Configured {
        /// When the cluster gave this answer.
        asked: Instant,
    },
    /// The question did not settle, or the whole pool has now been dropped.
    ///
    /// The configured address serves heavy commands until `until`, and then the
    /// cluster is asked once more. A waiting thread finds this rather than
    /// repeating the failing lookup, and so does a heavy command once every
    /// proxy in the pool has been dropped.
    FellBack {
        /// When to ask again. See [`HOSTS_RETRY_AFTER`]. `None` when no
        /// `Instant` can express the end:
        /// `with_hosts_retry_after(Duration::MAX)` is a fallback that never
        /// ends, not a panic.
        until: Option<Instant>,
    },
}

/// The heavy proxies this client is currently willing to use.
///
/// As in the official clients (C++ `THostManager`, Go `ProxySet`): the whole
/// `/hosts` answer, picked from at random per command. A host a command failed
/// at is dropped, and the next refresh, after [`HOST_LIST_REFRESH_INTERVAL`],
/// rebuilds the pool, restoring any host the cluster still lists. A
/// persistently bad host therefore costs one failed command per interval; Go
/// keeps a five-minute ban list instead.
#[derive(Debug)]
struct HeavyPool {
    /// Usable base URLs from the last `/hosts` answer, minus any dropped since.
    /// Never empty: [`HeavyPool::drop_host`] reports an emptied pool, and the
    /// caller turns it into [`HeavyProxy::FellBack`].
    hosts: Vec<String>,
    /// When the answer these came from arrived. Age is judged against each
    /// transport's own interval when it asks, since clones share this state,
    /// and an interval of `Duration::MAX` never elapses rather than panicking.
    fetched: Instant,
}

impl HeavyPool {
    /// One of the pool's hosts, picked at random.
    ///
    /// Random per command, as both official clients pick. This needs load
    /// spreading, not unpredictability, so `unique::word` is enough; the modulo
    /// bias is negligible.
    fn pick(&self) -> &str {
        let drawn = crate::unique::word(0) % self.hosts.len() as u64;
        &self.hosts[drawn as usize]
    }

    /// Takes a failed host out of the pool until a refresh restores it, and
    /// says whether the pool survived.
    ///
    /// By value, not by position: two commands in flight may both have gone to
    /// the failed host, and the second drop must not evict a neighbour. The
    /// caller must handle `false`, or [`HeavyPool::pick`] would divide by zero.
    #[must_use]
    fn drop_host(&mut self, base: &str) -> bool {
        self.hosts.retain(|host| host != base);
        !self.hosts.is_empty()
    }
}

/// Where one command was sent: an address this client chose out of `/hosts`, or
/// the one the caller configured.
///
/// Carried from [`Transport::base_for`] to [`Transport::after_heavy`] rather
/// than inferred from the address: `/hosts` may list the configured host
/// itself, and [`heavy_base`] then builds a byte-identical URL.
enum Destination<'a> {
    /// An address picked from the `/hosts` pool, owned because the pool may be
    /// gone by the time the failure is judged.
    Discovered(String),
    /// The address the caller gave, borrowed from the transport.
    Configured(&'a str),
}

impl Destination<'_> {
    /// The base URL to dial, whichever way it was arrived at.
    fn address(&self) -> &str {
        match self {
            Self::Discovered(base) => base,
            Self::Configured(base) => base,
        }
    }
}

/// Which of the names `/hosts` gives back this client is willing to use. See
/// [`heavy_base`] for the rule and what it is worth.
#[derive(Clone, Debug)]
enum HeavyHosts {
    /// The configured address's own domain, the default. See [`same_domain`].
    SameDomain,
    /// That domain and the ones named here: a cluster at `cluster.example.net`
    /// whose `/hosts` answers `n0132-sas.rack7.proxy-zone.net` needs
    /// `proxy-zone.net` added, not the rule removed. See [`under_domain`].
    Under {
        /// The domains as [`Transport::set_heavy_proxies_under`] normalised
        /// them: lowercased, without wildcard, scheme, port or stray dots, and
        /// without duplicates.
        domains: Vec<String>,
        /// Entries that could not be used: one with no dot left would admit a
        /// whole top-level domain. Kept so that [`Declined::because`] can name
        /// them, since the setter has no failure path and a dropped entry would
        /// otherwise look like an ignored variable.
        ignored: Vec<String>,
    },
    /// Wherever `/hosts` says, checked for being a host name and nothing else.
    Anywhere,
    /// Exactly these names, compared without case, and a port only where both
    /// sides name one.
    ///
    /// An empty list admits nothing; [`Transport::set_proxy_discovery`] says
    /// "route nowhere" plainly.
    Only(Vec<String>),
}

impl HeavyHosts {
    /// Whether a discovered host is one this client may send a token to.
    ///
    /// `configured` is the base URL the caller gave; `discovered` is one entry
    /// of the `/hosts` answer, trimmed and already known to be an authority.
    fn admits(&self, configured: &str, discovered: &str) -> bool {
        match self {
            Self::SameDomain => same_domain(host_of(configured), host_of(discovered)),
            Self::Under { domains, .. } => {
                same_domain(host_of(configured), host_of(discovered))
                    || domains
                        .iter()
                        .any(|domain| under_domain(domain, host_of(discovered)))
            }
            Self::Anywhere => true,
            Self::Only(names) => names.iter().any(|name| same_name(name, discovered)),
        }
    }
}

/// Whether a name a caller wrote out means the same proxy as a discovered one.
///
/// The host without case, and the port only where both name one: `/hosts`
/// answers bare host names unless the coordinator's `ShowPorts` says otherwise.
fn same_name(listed: &str, discovered: &str) -> bool {
    let listed = listed.trim();

    if !host_of(listed).eq_ignore_ascii_case(host_of(discovered)) {
        return false;
    }
    match (port_of(listed), port_of(discovered)) {
        (Some(listed), Some(discovered)) => listed == discovered,
        _ => true,
    }
}

/// Why a name from `/hosts` was passed over.
///
/// Kept so the client can say which: an unreadable name is a broken cluster or
/// a forged answer, and a declined one is a configuration the operator can
/// change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Declined {
    /// Not a host name: blank, or carrying a scheme, a path, userinfo,
    /// whitespace, a bad port, or brackets around something that is not an
    /// IPv6 literal.
    Malformed,
    /// A perfectly good name somewhere this client was not pointed.
    Elsewhere,
}

impl Declined {
    /// The half-sentence an operator needs, which depends on what was allowed.
    fn because(self, allowed: &HeavyHosts, configured: &str) -> String {
        match (self, allowed) {
            (Self::Malformed, _) => "is not a host name".to_owned(),
            (Self::Elsewhere, HeavyHosts::Only(_)) => {
                "is not one of the names with_heavy_proxies_in was given".to_owned()
            }
            // Name the domains given and the entries ignored, so the operator
            // can see the configuration was read.
            (Self::Elsewhere, HeavyHosts::Under { domains, ignored })
                if !domains.is_empty() || !ignored.is_empty() =>
            {
                let mut why = format!("is not under the domain of {}", host_of(configured));
                if !domains.is_empty() {
                    why.push_str(&format!(" or under {}", domains.join(", ")));
                }
                if !ignored.is_empty() {
                    why.push_str(&format!(" (ignored, not a domain: {})", ignored.join(", ")));
                }
                why
            }
            (Self::Elsewhere, _) => {
                format!("is not under the domain of {}", host_of(configured))
            }
        }
    }
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
    /// Which discovered hosts may be used. The configured address's own domain
    /// by default — see [`heavy_base`].
    hosts: HeavyHosts,
    /// The whole budget for one `/hosts` lookup, [`HOSTS_TIMEOUT`] by default.
    /// Its own field, not a minimum with `timeout`, so a cluster answering in
    /// 900 ms can be routed to.
    hosts_timeout: Duration,
    /// How long a fallback lasts before the cluster is asked again. See
    /// [`HOSTS_RETRY_AFTER`].
    hosts_retry_after: Duration,
    /// How old a `/hosts` answer may grow before a heavy command re-asks. See
    /// [`HOST_LIST_REFRESH_INTERVAL`].
    host_list_refresh: Duration,
    token: Option<String>,
    retries: RetryPolicy,
    /// End-to-end limit for a buffered command's attempt, shared by its
    /// redirect hops; per-phase limit for streaming commands. See
    /// [`Transport::dispatch`].
    timeout: Duration,
    /// How much of a buffered response this client will hold in memory:
    /// [`RESPONSE_LIMIT`], after decompression. A field so that tests can reach
    /// the cap through `Transport::set_response_limit`, which exists only under
    /// `cfg(test)`.
    response_limit: u64,
    /// Stamped onto every command, when the client is bound to a transaction.
    transaction: Option<String>,
    /// The `traceparent` header, when the client was given a trace to belong
    /// to.
    trace: Option<String>,
    /// The companion `tracestate`, carried unmodified when the context that
    /// was joined had one. See [`crate::TraceContext::tracestate`].
    tracestate: Option<String>,
    /// The headers that say who is asking, rendered once by
    /// [`Transport::render_caller_headers`]: none changes between requests.
    caller: Vec<(&'static str, String)>,
    /// Why the requested TLS configuration could not be assembled: a
    /// `YT_CA_BUNDLE` that names nothing readable, or nothing that parsed.
    /// Carried until a request can fail with it ([`Transport::unusable`]). A
    /// `String` because a `Transport` is `Clone` and an `io::Error` is not.
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
            // A bare host means TLS. Only an explicit `http://` opts out, which
            // is how a local cluster is addressed.
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
            discovery: !is_local(&base),
            hosts: HeavyHosts::SameDomain,
            hosts_timeout: HOSTS_TIMEOUT,
            hosts_retry_after: HOSTS_RETRY_AFTER,
            host_list_refresh: HOST_LIST_REFRESH_INTERVAL,
            base,
            heavy: Arc::new(Mutex::new(HeavyProxy::Unasked)),
            token,
            retries,
            timeout,
            response_limit: RESPONSE_LIMIT,
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

    /// Turns the `/hosts` lookup on or off, forgetting anything it found.
    pub(crate) fn set_proxy_discovery(&mut self, enabled: bool) {
        self.discovery = enabled;
        self.forget_heavy();
    }

    /// Lets a discovered host be one outside the configured address's domain.
    pub(crate) fn set_heavy_proxies_anywhere(&mut self, enabled: bool) {
        self.hosts = if enabled {
            HeavyHosts::Anywhere
        } else {
            HeavyHosts::SameDomain
        };
        self.forget_heavy();
    }

    /// Narrows discovered hosts to a list the caller wrote out.
    pub(crate) fn set_heavy_proxies_in(&mut self, names: Vec<String>) {
        self.hosts = HeavyHosts::Only(names);
        self.forget_heavy();
    }

    /// Widens the domain rule by the domains named, keeping the configured
    /// address's own.
    ///
    /// Normalised once, here: `*.Proxy-Zone.NET. `, `https://proxy-zone.net`
    /// and `proxy-zone.net:443` all become `proxy-zone.net`, since
    /// [`same_name`] tolerates scheme and port for
    /// [`crate::Client::with_heavy_proxies_in`] too. Duplicates are dropped,
    /// first mention winning.
    ///
    /// An entry with no dot left, such as `net`, is not used: it would admit
    /// every host in a top-level domain, as [`same_domain`] never shortens
    /// below two labels for the same reason. It is kept in `ignored` and named
    /// in the refusal ([`Declined::because`]), since this builder has no
    /// failure path. An empty entry is skipped without a report.
    pub(crate) fn set_heavy_proxies_under(&mut self, domains: Vec<String>) {
        let mut kept: Vec<String> = Vec::with_capacity(domains.len());
        let mut ignored: Vec<String> = Vec::new();

        for domain in &domains {
            // `host_of` first, then the dots: it reads a URL, so trimming first
            // would leave the dot of `https://proxy-zone.net./` in place.
            let normalised = host_of(domain.trim())
                .trim_start_matches('*')
                .trim_matches('.')
                .to_ascii_lowercase();

            // An empty entry is a list artefact, such as a trailing comma in
            // `YT_HEAVY_PROXY_DOMAINS`, and not worth reporting.
            if normalised.is_empty() {
                continue;
            }

            let (into, value) = if normalised.contains('.') {
                (&mut kept, normalised)
            } else {
                (&mut ignored, domain.trim().to_owned())
            };
            if !into.contains(&value) {
                into.push(value);
            }
        }

        self.hosts = HeavyHosts::Under {
            domains: kept,
            ignored,
        };
        self.forget_heavy();
    }

    /// Overrides the budget for one `/hosts` lookup.
    pub(crate) fn set_hosts_timeout(&mut self, timeout: Duration) {
        self.hosts_timeout = timeout;
    }

    /// Overrides how long a fallback lasts before the cluster is asked again.
    pub(crate) fn set_hosts_retry_after(&mut self, after: Duration) {
        self.hosts_retry_after = after;
    }

    /// Overrides how old a `/hosts` answer may grow before it is refreshed.
    pub(crate) fn set_host_list_refresh_interval(&mut self, interval: Duration) {
        self.host_list_refresh = interval;
    }

    /// The address the caller configured, for a test that has to see where a
    /// client was pointed without sending anything to it.
    #[cfg(test)]
    pub(crate) fn configured_address(&self) -> &str {
        &self.base
    }

    /// Which discovered hosts this transport would use, rendered as a string so
    /// that [`HeavyHosts`] stays private to this module.
    #[cfg(test)]
    pub(crate) fn heavy_hosts_debug(&self) -> String {
        format!("{:?}", self.hosts)
    }

    /// Lowers the buffered-response cap, so a test can reach it. Test-only: the
    /// cap is not a knob.
    #[cfg(test)]
    pub(crate) fn set_response_limit(&mut self, limit: u64) {
        self.response_limit = limit;
    }

    /// Drops what discovery resolved, because the rules it resolved under have
    /// changed.
    ///
    /// A fresh `Arc`, not a write through the shared one: the client this was
    /// cloned from keeps what it resolved under the old rules.
    fn forget_heavy(&mut self) {
        self.heavy = Arc::new(Mutex::new(HeavyProxy::Unasked));
    }

    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        // Through `build_agent`, so the agent keeps `max_redirects(0)`. The
        // bundle is re-read too, so a variable fixed since the client was built
        // is picked up.
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
    /// `repeatable` says what the command allows: a read is re-sent, a light
    /// mutation is re-sent under a `mutation_id` the cluster deduplicates, and
    /// a heavy command is sent once, to the proxy the cluster named for heavy
    /// work.
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
                    // The ID stays the same across attempts and only the flag
                    // changes. A caller-supplied ID may already be marked as a
                    // replay, which is how a restarted process resumes.
                    let mut tagged = parameters.clone();
                    insert(&mut tagged, "mutation_id", string(id.as_str()));
                    insert(&mut tagged, "retry", boolean(is_retry || id.is_retry()));
                    self.send(base.address(), method, command, &tagged, &payload)
                }
                None => self.send(base.address(), method, command, parameters, &payload),
            }
        });

        self.after_heavy(repeatable, &base, sent)
    }

    /// Puts the client's transaction into a command's parameters.
    ///
    /// `None` when there is nothing to add, so the common case does not copy
    /// the parameters. Done in one place so that no command can silently run
    /// outside the transaction.
    ///
    /// A command that already names a transaction keeps it
    /// (`commit_transaction` and its siblings). A command with no transaction
    /// to be in ([`NO_TRANSACTION`]) is left alone.
    fn in_transaction(&self, command: &str, parameters: &YsonValue) -> Option<YsonValue> {
        let id = self.transaction.as_ref()?;

        if takes_no_transaction(command) {
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
    /// Everything light goes to the configured address. A heavy command
    /// ([`Repeatable::Heavy`]) goes to a proxy that will accept one: a control
    /// proxy refuses it, and a balancer usually fronts control proxies.
    ///
    /// The lookup happens on the first heavy command and again, lazily, when
    /// the answer is older than the refresh interval. The whole answer is a
    /// pool; each heavy command picks a member at random, as `THostManager`
    /// (C++) and `ProxySet` (Go) do, and a failed host is dropped until a
    /// refresh ([`Transport::after_heavy`]). A refresh with no usable list
    /// keeps the pool for another interval rather than retrying on
    /// [`HOSTS_RETRY_AFTER`].
    ///
    /// A cluster naming nobody usable is [`HeavyProxy::Configured`] until an
    /// interval passes, which keeps a local cluster working; a refused answer
    /// is said once ([`crate::observe::declined`]).
    ///
    /// The mutex is held across the lookup, refresh included, so concurrent
    /// heavy commands wait for one answer. Every outcome sets a clock, so the
    /// stall is at most one [`Transport::hosts_timeout`] per interval. `fetch`
    /// does not take this lock.
    fn base_for(&self, repeatable: Repeatable) -> Destination<'_> {
        if repeatable != Repeatable::Heavy || !self.discovery {
            return Destination::Configured(&self.base);
        }

        let mut resolved = lock(&self.heavy);
        match &mut *resolved {
            HeavyProxy::Pool(pool) => {
                // Refreshed before picking, against this transport's own
                // interval.
                if pool.fetched.elapsed() >= self.host_list_refresh {
                    match self.usable_hosts() {
                        Ok(hosts) if !hosts.is_empty() => {
                            *pool = HeavyPool {
                                hosts,
                                fetched: Instant::now(),
                            };
                        }
                        // Nothing usable, such as an empty answer mid-rotation:
                        // keep the pool, wait another interval.
                        _ => pool.fetched = Instant::now(),
                    }
                }
                return Destination::Discovered(pool.pick().to_owned());
            }
            HeavyProxy::Configured { asked } if asked.elapsed() < self.host_list_refresh => {
                return Destination::Configured(&self.base);
            }
            HeavyProxy::FellBack { until } if until.is_none_or(|until| Instant::now() < until) => {
                return Destination::Configured(&self.base);
            }
            HeavyProxy::Unasked | HeavyProxy::Configured { .. } | HeavyProxy::FellBack { .. } => {}
        }

        // Only the first settle is worth a sentence: the re-ask after an
        // interval declining the same names would repeat it once a minute.
        let first_asking = matches!(&*resolved, HeavyProxy::Unasked);

        match self.heavy_hosts() {
            // The usable hosts become the pool. If none is usable, routing is
            // off, so the refusals are said.
            Ok(hosts) => {
                let (usable, refused) = self.admitted(&hosts);

                if usable.is_empty() {
                    *resolved = HeavyProxy::Configured {
                        asked: Instant::now(),
                    };
                    drop(resolved);
                    if first_asking && !refused.is_empty() && self.retries.reports() {
                        crate::observe::declined(&self.base, &refused);
                    }
                    return Destination::Configured(&self.base);
                }

                let pool = HeavyPool {
                    hosts: usable,
                    fetched: Instant::now(),
                };
                let picked = pool.pick().to_owned();
                *resolved = HeavyProxy::Pool(pool);
                Destination::Discovered(picked)
            }
            // A failed lookup is never fatal: the command goes to the
            // configured address. `worth_asking_again` decides whether to ask
            // soon: a cluster with no `/hosts` endpoint answers 404 every time,
            // while a timeout is worth another try in a moment. Either verdict
            // is re-examined later.
            Err(error) => {
                *resolved = if crate::retry::worth_asking_again(&error) {
                    HeavyProxy::FellBack {
                        until: Instant::now().checked_add(self.hosts_retry_after),
                    }
                } else {
                    HeavyProxy::Configured {
                        asked: Instant::now(),
                    }
                };
                Destination::Configured(&self.base)
            }
        }
    }

    /// One `/hosts` answer, split into the base URLs this client will use and
    /// the reasons for the names it will not — usable first, refusals second.
    fn admitted(&self, hosts: &[String]) -> (Vec<String>, Vec<String>) {
        let mut usable = Vec::new();
        let mut refused = Vec::new();
        for host in hosts {
            match heavy_base(&self.base, host, &self.hosts) {
                Ok(base) => usable.push(base),
                Err(why) => {
                    refused.push(format!("{host:?} {}", why.because(&self.hosts, &self.base)));
                }
            }
        }
        (usable, refused)
    }

    /// A fresh `/hosts` answer reduced to the base URLs this client will use.
    ///
    /// The refresh path, which neither collects nor says refusals: they were
    /// said on the first resolve.
    fn usable_hosts(&self) -> Result<Vec<String>> {
        Ok(self
            .heavy_hosts()?
            .iter()
            .filter_map(|host| heavy_base(&self.base, host, &self.hosts).ok())
            .collect())
    }

    /// The heavy proxies the cluster names, best first.
    ///
    /// `/hosts` answers a JSON list of bare host names, ordered by load ([HTTP
    /// proxy
    /// guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload),
    /// [reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)).
    /// That it lists the `data` role is an operator-changeable coordinator
    /// default, not documented, so the client checks what it is given. See
    /// [`/hosts`](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#hosts).
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
    /// Only for a command that went to a discovered address, as [`Destination`]
    /// says; a failure at the configured address is the caller's choice and
    /// says nothing about a lookup.
    ///
    /// - The error names the host: `write_table at n0132-sas.example.net:9013:
    ///   …`.
    /// - A failure [`crate::retry::attributable_to_the_host`] drops the host
    ///   from the pool, and the next command picks from what remains; the
    ///   command itself is not resent. Only an emptied pool falls back to the
    ///   configured address, which is usually a balancer in front of control
    ///   proxies. A request's own fault, such as a resolve error, keeps the
    ///   pool.
    ///
    /// [`Transport::open`] returns the body unread, so a host that dies
    /// mid-stream fails in the caller's reader and stays in the pool until a
    /// request fails at the head.
    fn after_heavy<T>(
        &self,
        repeatable: Repeatable,
        destination: &Destination<'_>,
        result: Result<T>,
    ) -> Result<T> {
        if repeatable != Repeatable::Heavy {
            return result;
        }
        let Err(error) = result else {
            return result;
        };
        if !self.discovery {
            return Err(refusal_hint(
                error,
                "this client does not route heavy commands: \
                 Client::with_proxy_discovery(true) turns the /hosts lookup on",
            ));
        }

        let base = match destination {
            // The configured address is the caller's choice, so explain only a
            // proxy that refuses this command outright.
            Destination::Configured(_) => {
                let resolved = lock(&self.heavy);
                let why = declined_routing(&resolved);
                return Err(refusal_hint(error, why));
            }
            Destination::Discovered(base) => base,
        };

        if crate::retry::attributable_to_the_host(&error) {
            let mut resolved = lock(&self.heavy);
            if let HeavyProxy::Pool(pool) = &mut *resolved
                && !pool.drop_host(base)
            {
                *resolved = HeavyProxy::FellBack {
                    until: Instant::now().checked_add(self.hosts_retry_after),
                };
            }
        }

        Err(routed_to(error, base))
    }

    /// One attempt, read into memory.
    ///
    /// The cap on what is held is [`Transport::response_limit`].
    fn send(
        &self,
        base: &str,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        payload: &Payload<'_>,
    ) -> Result<Vec<u8>> {
        // Bytes, not a `SendBody`, so a redirect can resend them
        // ([`Outgoing`]). `None` is an empty slice, `Content-Length: 0` on the
        // wire, not `Outgoing::Empty`.
        let body = match payload {
            Payload::None => Outgoing::Bytes(&[]),
            Payload::Bytes(bytes) => Outgoing::Bytes(bytes),
        };
        let mut response = self.dispatch(base, method, command, parameters, body, false)?;

        let status = response.status().as_u16();
        let body = read_capped(command, response.body_mut(), self.response_limit)?;

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
    /// Sent once, never retried, and to a heavy proxy: [`Repeatable::Heavy`].
    pub(crate) fn open(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
    ) -> Result<ureq::Body> {
        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        // Through `retry::run` for the span; `Repeatable::Heavy` caps it at one
        // attempt. The span closes when the headers arrive, and the caller
        // reads the body later.
        let base = self.base_for(Repeatable::Heavy);
        let opened = crate::retry::run(self.retries, Repeatable::Heavy, command, |_| {
            let response = self.dispatch(
                base.address(),
                method,
                command,
                parameters,
                Outgoing::Empty,
                true,
            )?;
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
    /// once, so this cannot be retried. The response body is always read, for
    /// the connection's sake, and handed back for a raw command to interpret.
    pub(crate) fn upload(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        rows: &mut dyn std::io::Read,
    ) -> Result<Vec<u8>> {
        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        // One attempt, as in `open`, but the span covers the whole transfer. A
        // data-stream body is what a control proxy refuses, so this goes to a
        // heavy proxy.
        let base = self.base_for(Repeatable::Heavy);
        let sent = crate::retry::run(self.retries, Repeatable::Heavy, command, |_| {
            let mut response = self.dispatch(
                base.address(),
                method,
                command,
                parameters,
                Outgoing::Stream(&mut *rows),
                true,
            )?;
            let status = response.status().as_u16();

            // Read whichever way it went: `ureq` pools only a connection whose
            // body was read
            // ([connections](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#connections)).
            // Read as bytes, since a raw command's answer may be binary.
            let body = match read_capped(command, response.body_mut(), self.response_limit) {
                Ok(body) => body,
                // Too large is worth failing the write over:
                // `raw_command_upload` returns this `Vec` as the answer, and an
                // empty one would misreport it.
                Err(error @ ClientError::ResponseTooLarge { .. }) => return Err(error),
                // A cut answer to a write whose status already said it was
                // done: failing would fail a write that succeeded.
                Err(_) => Vec::new(),
            };

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
    /// `/hosts` is the only one. It gets what a command gets (the token, the
    /// no-TLS guard, the caller headers) except the timeout and retry policy:
    /// one attempt bounded by [`HOSTS_TIMEOUT`], because a heavy command waits
    /// on it holding the lock. A failed lookup is retried after
    /// [`HOSTS_RETRY_AFTER`] by a later heavy command.
    ///
    /// It goes to the configured address. A same-origin redirect is followed; a
    /// cross-origin one is refused with [`ClientError::Redirected`], which the
    /// router treats as worth asking again.
    pub(crate) fn fetch(&self, path: &str, what: &str) -> Result<String> {
        if let Some(error) = self.unusable(&self.base) {
            return Err(error);
        }

        let first = format!("{}{path}", self.base);

        // One attempt, under the lookup's own budget, shared by the redirect
        // hops.
        crate::retry::run(RetryPolicy::none(), Repeatable::Freely, what, |_| {
            let mut url = first.clone();
            let mut hops = 0;
            let deadline = Instant::now().checked_add(self.hosts_timeout);

            // The loop ends at MAX_REDIRECTS, in `redirect`, or when the budget
            // runs out.
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
                        if let Some(error) = self.unusable(&next) {
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
    /// Callers differ only in how they consume the response body.
    ///
    /// `streaming` lifts the end-to-end timeout, so that [`Transport::open`]
    /// and [`Transport::upload`] can move a table for as long as it takes.
    /// Resolve, connect, sending the request and the response headers each stay
    /// bounded by the timeout, so a dead proxy still fails promptly.
    ///
    /// A buffered command's deadline is taken once and shared by every redirect
    /// hop. A fresh timeout per hop would allow `(MAX_REDIRECTS + 1)` times the
    /// requested limit
    /// ([redirects](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#redirects)).
    ///
    /// `base` is the configured address for a light command, or what
    /// [`Transport::base_for`] resolved for a heavy one.
    fn dispatch(
        &self,
        base: &str,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        mut body: Outgoing<'_>,
        streaming: bool,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        // Judged against `base`, the address actually dialled, so that a no-TLS
        // build refuses an `https://` heavy proxy too.
        if let Some(error) = self.unusable(base) {
            return Err(error);
        }

        // `mut` because a same-origin redirect reassigns it below.
        let mut url = format!("{base}/api/v4/{command}");

        let encoded = to_string(parameters, YsonFormat::Text).map_err(|e| ClientError::Decode {
            command: command.to_owned(),
            reason: format!("could not encode parameters: {e}"),
        })?;

        // What is being asked. Who is asking is `self.caller`, applied beside
        // this rather than copied into it per request.
        let headers: [(&str, String); 4] = [
            (HEADER_FORMAT, "<format=text>yson".to_owned()),
            (PARAMETERS, encoded),
            ("X-YT-Output-Format", "<format=text>yson".to_owned()),
            ("Content-Type", "application/octet-stream".to_owned()),
        ];

        // Taken once for the attempt, not once per hop.
        let deadline = self.deadline(streaming);
        // The loop ends because `redirect` refuses past [`MAX_REDIRECTS`], and
        // sooner than that because the deadline runs out.
        let mut hops = 0;

        loop {
            let left = remaining(deadline, command)?;

            // Method and body survive the hop, whatever the digit: `307` and
            // `308` require it, and an API v4 command's verb belongs to the
            // command. `redirect` refuses the hop when the body cannot be sent
            // again.
            let sent = match method {
                // A GET carries no body in `ureq`'s type system, nor in any
                // command this client sends as one.
                Method::Get => with_headers!(
                    self.scoped(self.agent.get(&url), streaming, left),
                    &headers,
                    &self.caller
                )
                .call(),
                // A fresh `SendBody` per hop, since one cannot be reused.
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

            // Before the cluster's error: a redirect is this client deciding
            // where the request goes, not a failure.
            if let Some(next) = self.redirect(command, &response, &url, &body, hops)? {
                // The guard the first address got, though a same-origin
                // redirect cannot change the scheme.
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
    /// A control proxy answers a heavy read with a cross-host `307` to a data
    /// proxy. `ureq` drops `Authorization` when it follows one, even with
    /// `RedirectAuthHeaders::SameHost`, and the cluster then blames a valid
    /// token. So this client decides, in this order
    /// ([redirects](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#redirects)):
    ///
    /// - leaving the origin with credentials: [`RedirectRefusal::Credentials`];
    ///   a same-origin redirect is followed, token and all;
    /// - a body that cannot be sent again, anywhere: [`RedirectRefusal::Body`],
    ///   since following would write no rows and report success;
    /// - leaving the origin with data, token or not:
    ///   [`RedirectRefusal::Payload`]; a body of length zero is not data;
    /// - more than [`MAX_REDIRECTS`] hops: [`RedirectRefusal::TooMany`].
    ///
    /// A `3xx` with no `Location`, or one that cannot be resolved, stays an
    /// ordinary [`ClientError::Http`].
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
        // Resolved first, so the origin comparison has an origin and the
        // message names a host even for `Location: /api/v4/…`.
        let Some(target) = resolve(request_url, &location) else {
            return Ok(None);
        };

        let refused = |refusal| {
            Err(ClientError::Redirected {
                command: command.to_owned(),
                status: status.as_u16(),
                location: target.clone(),
                refusal,
                heavy: is_heavy(command),
            })
        };

        // Both origin rules ask this.
        let elsewhere = !same_origin(request_url, &target);

        // Credentials first: the reason a caller most needs.
        if self.token.is_some() && elsewhere {
            return refused(RedirectRefusal::Credentials);
        }
        if !body.replayable() {
            return refused(RedirectRefusal::Body);
        }
        // Without a token, the rows must still not go to a host nobody named.
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
    /// They belong to every request, `/hosts` included, not to a command. The
    /// trace context goes on every attempt with the same span id: retries are
    /// one logical call. Rendered when the transport is built or its trace is
    /// set, since nothing here changes per request.
    fn render_caller_headers(&mut self) {
        let mut headers = Vec::new();
        if let Some(token) = &self.token {
            headers.push(("Authorization", format!("OAuth {token}")));
        }
        if let Some(trace) = &self.trace {
            headers.push((TRACEPARENT, trace.clone()));
        }
        // Only beside `traceparent`: a `tracestate` alone names no trace.
        if let (Some(_), Some(state)) = (&self.trace, &self.tracestate) {
            headers.push((TRACESTATE, state.clone()));
        }
        self.caller = headers;
    }

    /// Why no request can be sent at all, if that was settled before any was.
    ///
    /// Either the crate was built without the `tls` feature and the address is
    /// `https://`, or [`CA_BUNDLE`] named something that is not root
    /// certificates. Reported as a sentence naming the cause instead of a
    /// handshake failure. A refused bundle only affects `https://`.
    ///
    /// `base` is the address about to be dialled, which for a heavy command is
    /// the one `/hosts` named, so a discovered `https://` proxy is refused too.
    fn unusable(&self, base: &str) -> Option<ClientError> {
        if let Some(error) = tls_unavailable(base) {
            return Some(error);
        }

        match &self.tls_refused {
            Some(why) if base.starts_with("https://") => Some(ClientError::Config(why.clone())),
            _ => None,
        }
    }

    /// When one attempt of a command must be finished by.
    ///
    /// `None` for a streaming transfer, which is bounded per phase instead, and
    /// for a timeout no `Instant` can express, where the agent's
    /// `timeout_global` bounds it.
    fn deadline(&self, streaming: bool) -> Option<Instant> {
        if streaming {
            return None;
        }
        Instant::now().checked_add(self.timeout)
    }

    /// Bounds one request: what is left of the command's deadline, or the
    /// per-phase limits a streaming transfer gets instead.
    ///
    /// A streaming request loses the end-to-end deadline and keeps the same
    /// bound on each phase before the data: DNS, connect, sending, response
    /// headers. A buffered one gets `left`, the remainder of
    /// [`Transport::dispatch`]'s deadline.
    fn scoped<Any>(
        &self,
        request: ureq::RequestBuilder<Any>,
        streaming: bool,
        left: Option<Duration>,
    ) -> ureq::RequestBuilder<Any> {
        if !streaming {
            return match left {
                Some(left) => request.config().timeout_global(Some(left)).build(),
                // No deadline to share out: the agent's global timeout applies.
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

/// Takes the lock, and takes it back from a thread that panicked holding it.
///
/// What this guards is a cached address, which a panic leaves as it was; not
/// worth poisoning a client over.
fn lock(heavy: &Mutex<HeavyProxy>) -> MutexGuard<'_, HeavyProxy> {
    heavy
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Turns a host from `/hosts` into a base URL, or refuses it.
///
/// `/hosts` answers bare host names, and everything else about the address is
/// this client's to decide, since a forged or mistaken answer could send an
/// upload and the token elsewhere:
///
/// - the scheme comes only from the configured address; a name carrying its own
///   is refused, so `http://` cannot downgrade an `https://` client;
/// - `/`, `@`, `://`, `?`, `#` and whitespace are refused
///   (`real.example.net@evil.example.net` has the host `evil.example.net`);
/// - the configured port carries through when the name has none, as it usually
///   has none (the coordinator's `ShowPorts`);
/// - the name must be one host and at most one port, with brackets only around
///   an IPv6 literal: `ureq` 3.3 passes `[n0132.example.com]` to the resolver;
/// - the name must sit where `allowed` says: by default the configured host or
///   its parent domain ([`same_domain`]).
///
/// The domain rule is a typo guard, not a credential boundary: whoever can
/// steer `/hosts` already sees the token, and a suffix rule admits every tenant
/// of a hosting platform. `HeavyHosts::Only` is the boundary ([which names are
/// used](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#which-names-from-hosts-are-used)).
/// A refused name is passed over; a wholly refused answer means the configured
/// address, said once by [`crate::observe::declined`].
fn heavy_base(
    configured: &str,
    host: &str,
    allowed: &HeavyHosts,
) -> std::result::Result<String, Declined> {
    let host = host.trim();

    if host.is_empty()
        || host.contains("://")
        || host.contains('/')
        || host.contains('@')
        || host.contains(['?', '#'])
        || host.chars().any(char::is_whitespace)
        || !is_authority(host)
    {
        return Err(Declined::Malformed);
    }

    if !allowed.admits(configured, host) {
        return Err(Declined::Elsewhere);
    }

    let scheme = if configured.starts_with("https://") {
        "https://"
    } else {
        "http://"
    };

    Ok(match (has_port(host), port_of(configured)) {
        (false, Some(port)) => format!("{scheme}{host}:{port}"),
        _ => format!("{scheme}{host}"),
    })
}

/// Whether a name from `/hosts` is one host and at most one port.
///
/// Bracketed, it must hold an IPv6 literal: `ureq` 3.3 does not strip brackets
/// from anything else, so `[n0132.example.com]evil` would never resolve.
/// Unbracketed, a colon introduces a port of digits, which also refuses a bare
/// IPv6 literal such as `2a02:6b8::2`.
fn is_authority(host: &str) -> bool {
    match host.strip_prefix('[') {
        Some(rest) => match rest.split_once(']') {
            Some((literal, tail)) => {
                literal.parse::<std::net::Ipv6Addr>().is_ok()
                    && (tail.is_empty() || tail.strip_prefix(':').is_some_and(is_port))
            }
            None => false,
        },
        None => match host.split_once(':') {
            Some((name, port)) => !name.is_empty() && is_port(port),
            None => true,
        },
    }
}

/// Whether what follows a colon is a port and nothing else.
fn is_port(port: &str) -> bool {
    !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
}

/// Whether `discovered` sits under the same domain as `configured`.
///
/// The shared domain is the configured host minus its leftmost label, never
/// below two labels: `cluster.example.net` admits anything under `example.net`,
/// and `example.net` only itself and what is under it. See [`heavy_base`] for
/// what the rule is worth.
///
/// A configured name with no dot, such as `YT_PROXY=hume` or a short Kubernetes
/// service name, has no parent domain, so it is matched as a label of the
/// discovered name other than the leftmost: `hume` admits
/// `n0008-sas.hume.yt.example.net` and `n0008-sas.hume`, and refuses
/// `hume.evil.com`. Without this a real installation's whole `/hosts` answer
/// would be refused.
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
        // Its parent domain, never shortened below two labels.
        Some((_, parent)) if parent.contains('.') => parent,
        Some(_) => configured.as_str(),
        // A bare cluster name: a label of the discovered name, and not the
        // leftmost one, which is where the proxy's own name goes.
        None => {
            return discovered
                .split('.')
                .skip(1)
                .any(|label| label == configured);
        }
    };

    discovered == domain || discovered.ends_with(&format!(".{domain}"))
}

/// Whether `discovered` sits under a domain the caller added by hand.
///
/// The plain suffix rule, not [`same_domain`]'s: the caller wrote a domain, not
/// a host, so `proxy-zone.net` admits `n0132-sas.rack7.proxy-zone.net` and
/// itself, nothing else. `domain` is already normalised and non-empty
/// ([`Transport::set_heavy_proxies_under`]). The suffix caveat in
/// [`heavy_base`] still applies.
fn under_domain(domain: &str, discovered: &str) -> bool {
    let discovered = discovered.to_ascii_lowercase();

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
/// Userinfo comes off: it may hold a password, and [`port_of`] would find no
/// port in `pass@host:8000`.
fn authority_of(base: &str) -> &str {
    let authority = base
        .split_once("://")
        .map_or(base, |(_, rest)| rest)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    authority.rsplit_once('@').map_or(authority, |(_, h)| h)
}

/// Why a heavy command was served at the configured address after all.
///
/// One clause per state for [`refusal_hint`], naming the setting that changes
/// the answer, since the cluster's refusal does not say that this client
/// declined what `/hosts` named.
fn declined_routing(state: &HeavyProxy) -> &'static str {
    match state {
        HeavyProxy::Configured { .. } => {
            "/hosts named no heavy proxy this client would use — \
             Client::with_heavy_proxies_under([…]) or YT_HEAVY_PROXY_DOMAINS \
             names the domain they are in, Client::with_heavy_proxies_in([…]) \
             names the proxies themselves, and \
             Client::with_heavy_proxies_anywhere(true) or \
             YT_HEAVY_PROXIES_ANYWHERE=1 allows any name it refused"
        }
        HeavyProxy::FellBack { .. } => {
            "the heavy proxies /hosts named have all just failed, \
             so this went to the configured address for a moment"
        }
        HeavyProxy::Unasked | HeavyProxy::Pool(_) => "this client did not route this command",
    }
}

/// Adds the sentence a control proxy's refusal does not carry.
///
/// Only for [`CONTROL_REFUSAL`], the one failure about which proxy was asked.
/// Appended to the message rather than a new variant, so the cluster's words
/// stay matchable.
fn refusal_hint(error: ClientError, why: &str) -> ClientError {
    match error {
        ClientError::Cluster {
            command,
            code,
            message,
            raw,
        } if message.contains(CONTROL_REFUSAL) => ClientError::Cluster {
            command,
            code,
            message: format!("{message} ({why})"),
            raw,
        },
        other => other,
    }
}

/// The response cap, applied where the bytes actually accumulate.
///
/// `ureq`'s `limit()` counts what arrives on the wire; this counts what comes
/// out of the decoder, which is what is held ([response size
/// limits](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#response-size-limits)).
///
/// It reads one byte past what is left, so an overrun is seen without being
/// kept, and fails with `ureq::Error::BodyExceedsLimit` as the wire limit does,
/// so [`body_failure`] treats both alike. A body of exactly `limit` decoded
/// bytes passes; the wire limit must leave room for that body compressed
/// ([`wire_budget`]).
struct CapReader<R> {
    reader: R,
    /// The cap, kept for the error; `left` is what is spent against it.
    limit: u64,
    left: u64,
}

impl<R> CapReader<R> {
    fn new(reader: R, limit: u64) -> Self {
        CapReader {
            reader,
            limit,
            left: limit,
        }
    }
}

impl<R: std::io::Read> std::io::Read for CapReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // One byte more than may be kept tells "ended at the cap" from "ran
        // past it"; that byte is never returned.
        let room = self.left.saturating_add(1).min(buf.len() as u64) as usize;
        let read = self.reader.read(&mut buf[..room])?;

        if read as u64 > self.left {
            return Err(ureq::Error::BodyExceedsLimit(self.limit).into_io());
        }

        self.left -= read as u64;
        Ok(read)
    }
}

/// How many bytes `ureq` may transfer for a memory cap of `limit`.
///
/// The backstop beneath [`CapReader`], which counts decoded bytes: a chunked
/// stream of empty deflate stored blocks (`00 00 00 ff ff`) decodes to nothing,
/// and `flate2` loops inside one `read` that never returns without a wire
/// limit. `an_endless_body_that_decodes_to_nothing_is_still_bounded` checks it.
///
/// It cannot be `limit`: deflate expands incompressible input, so the largest
/// permitted body crosses the wire larger than the cap. The budget is zlib's
/// `deflateBound` (`n + n/8 + n/64 + 5`) with 64 bytes for the gzip header and
/// trailer (18) and the rounding of shifts. At [`RESPONSE_LIMIT`] that is 612
/// 368 448 wire bytes. An encoder that expands past `deflateBound` is refused,
/// the safe direction.
fn wire_budget(limit: u64) -> u64 {
    limit
        .saturating_add(limit >> 3)
        .saturating_add(limit >> 6)
        .saturating_add(64)
}

/// Reads a buffered response body, capped at [`RESPONSE_LIMIT`]'s worth of
/// *decoded* bytes.
///
/// [`CapReader`] is the cap the caller is promised, above the decoder; the
/// limit `ureq` is given is the backstop beneath it ([`wire_budget`]).
fn read_capped(command: &str, body: &mut ureq::Body, limit: u64) -> Result<Vec<u8>> {
    use std::io::Read;

    let transferred = wire_budget(limit);
    let mut reader = CapReader::new(body.with_config().limit(transferred).reader(), limit);

    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|e| body_failure(command, limit, e.into()))?;

    Ok(bytes)
}

/// Which error a buffered response body failed with, and whose fault it is.
///
/// A connection cut while the body streams in stays a
/// [`ClientError::Transport`]: retriable where the command allows, and for a
/// heavy command a reason to drop the host.
///
/// A body past the cap arrives as `ureq::Error::BodyExceedsLimit`, which is not
/// an `Io` error, so the `retry` predicates would read it as retriable and
/// blame the host: an over-cap [`Client::read_file`](crate::Client::read_file)
/// would empty the pool of healthy proxies. It becomes
/// [`ClientError::ResponseTooLarge`] instead, never retried and never blamed on
/// a host, carrying `limit`, the memory cap, not `ureq`'s wire budget.
fn body_failure(command: &str, limit: u64, error: ureq::Error) -> ClientError {
    if matches!(error, ureq::Error::BodyExceedsLimit(_)) {
        return ClientError::ResponseTooLarge {
            command: command.to_owned(),
            limit,
        };
    }

    ClientError::Transport {
        command: command.to_owned(),
        source: Box::new(error),
    }
}

/// Names the proxy a routed command actually went to.
///
/// `write_table: transport error: io: Connection refused` would name no address
/// the caller wrote, since the client chose it from `/hosts`. Only for a routed
/// command; the configured address needs no naming.
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
        // `ResponseTooLarge` is not qualified: `error::streaming_advice`
        // matches the command name exactly to offer the streaming half, and the
        // size does not depend on the host.
        // `a_response_too_large_keeps_the_way_past_it` checks this arm.
        error @ ClientError::ResponseTooLarge { .. } => error,
        // Nothing else carries a command at all: an `Io` names a local path, a
        // `Config` names the build, and an `OperationFailed` is the
        // scheduler's verdict rather than one proxy's.
        other => other,
    }
}

/// Whether `base` names a cluster on this machine, or a tunnel to one.
///
/// Such a cluster is not asked for heavy proxies: a single-node installation
/// has none, and the address a cluster publishes for itself is unreachable from
/// behind a port mapping or tunnel (a Docker cluster at `localhost:8000` knows
/// itself by the container's address). `Client::with_proxy_discovery` overrides
/// this either way.
fn is_local(base: &str) -> bool {
    let host = host_of(base);

    if let Ok(address) = host.parse::<std::net::IpAddr>() {
        // `0.0.0.0` is not loopback, but is nobody else's address either.
        return address.is_loopback() || address.is_unspecified();
    }

    host.eq_ignore_ascii_case("localhost")
}

/// The host out of a base URL, without scheme, port or path.
///
/// Not a `split(':')`, because of bracketed IPv6 literals such as
/// `http://[::1]:8000`.
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

/// What is left of `deadline` for the next request.
///
/// `Ok(None)` when there is no deadline, and `Err` once it is spent, reported
/// as `ureq` reports a global timeout, so the caller sees one answer whether it
/// ran out mid-request or between redirect hops.
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
/// `ureq` follows no redirects (`max_redirects(0)` hands the `3xx` back as an
/// ordinary response): whether to follow depends on the credentials, the origin
/// and the body together, which no `ureq` setting expresses, so
/// [`Transport::redirect`] decides. `RedirectAuthHeaders::SameHost` does not
/// help, since the redirect from a control proxy to a data proxy is cross-host.
///
/// `named` is the bundle to trust, [`configured_bundle`] in production, a
/// parameter so that tests need not write the environment. What cannot be
/// honoured is handed back rather than failed, since a client is being
/// constructed; [`Transport::unusable`] reports it. A build without TLS ignores
/// `named`.
#[cfg_attr(not(feature = "tls"), allow(unused_variables))]
fn build_agent(timeout: Duration, named: Option<&Path>) -> (ureq::Agent, Option<String>) {
    #[allow(unused_mut)]
    let mut builder = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        // Keep non-2xx as ordinary responses so the X-YT-Error header can be
        // read off them.
        .http_status_as_error(false)
        // This client follows redirects itself, in `Transport::redirect`.
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
/// [`std::env::var_os`] rather than `var`, so a non-UTF-8 path is not read as
/// unset. A build without TLS names nothing.
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
/// `None` leaves `ureq`'s default, the Mozilla bundle compiled in through
/// `webpki-roots`, which stays the default. Behind a corporate CA there are two
/// ways, as in the `yt` CLI and the Go SDK: [`CA_BUNDLE`] names a PEM file, and
/// the `platform-verifier` feature trusts the operating system's store. The
/// bundle wins when both are set.
///
/// The configured bundle is parsed once per process, since agents are rebuilt
/// often ([`Transport::set_timeout`], each transaction). Only success is
/// cached: a failure, such as a file being rewritten during the first
/// `Client::new`, is retried on the next construction rather than kept for the
/// process's life. Any other path, which only tests pass, is parsed on the
/// spot.
#[cfg(feature = "tls")]
fn root_certs(named: Option<&Path>) -> Result<Option<ureq::tls::TlsConfig>, String> {
    static CONFIGURED: std::sync::OnceLock<Option<ureq::tls::TlsConfig>> =
        std::sync::OnceLock::new();

    if named == configured_bundle() {
        if let Some(roots) = CONFIGURED.get() {
            return Ok(roots.clone());
        }

        let roots = roots_for(named)?;
        // A race is harmless: both threads parsed the same file.
        let _ = CONFIGURED.set(roots.clone());
        return Ok(roots);
    }

    roots_for(named)
}

/// The choice itself, split out so a test need not write the process
/// environment.
#[cfg(feature = "tls")]
fn roots_for(named: Option<&Path>) -> Result<Option<ureq::tls::TlsConfig>, String> {
    match named {
        // `YT_CA_BUNDLE=` means "turned off", not a file called nothing.
        Some(path) if !names_nothing(path) => bundle(path).map(Some),
        _ => Ok(platform_roots()),
    }
}

/// Whether a variable that is set nevertheless names no file.
///
/// `YT_CA_BUNDLE=` and `YT_CA_BUNDLE="   "` turn it off. A non-UTF-8 path is a
/// path, not nothing.
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
/// Refused rather than ignored, naming the file: an unreadable file, one with
/// no certificates, and one with any `BEGIN CERTIFICATE` block that is not
/// X.509 ([`is_x509`]). Falling back to the compiled-in roots, or keeping the
/// blocks that parsed, would end in an `UnknownIssuer` that names neither file
/// nor variable. `parse_pem` only splits and base64-decodes, and `rustls`
/// silently drops what it cannot parse, so a `.p7b` under a certificate label
/// would give an empty root store. See
/// [TLS](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#tls).
#[cfg(feature = "tls")]
fn bundle(path: &Path) -> Result<ureq::tls::TlsConfig, String> {
    use std::io::Read;

    use ureq::tls::{Certificate, PemItem, RootCerts, TlsConfig, parse_pem};

    let shown = path.display();

    // `stat` before `open`: opening a FIFO blocks until someone writes, and
    // nothing above this can time it out.
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
        // The cap again, since the file can grow between the two calls.
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
            // A private key or an unknown section: not a root, and a key and CA
            // in one file is ordinary.
            Ok(_) => {}
            // Corrupt base64 or a file cut mid-block. Counted, not skipped, so
            // the roots are never fewer than the file says. Comments and labels
            // between blocks parse cleanly and do not reach here.
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
/// Not a verification or a full parse: only whether `rustls` will find a
/// certificate here rather than discard it silently ([`bundle`]).
///
/// ```text
/// Certificate ::= SEQUENCE {
///     tbsCertificate       TBSCertificate,
///     signatureAlgorithm   AlgorithmIdentifier,
///     signatureValue       BIT STRING }
/// ```
///
/// A PKCS#7 `ContentInfo` (a `.p7b`) is a `SEQUENCE` too, but its first member
/// is an OBJECT IDENTIFIER, not the `tbsCertificate` sequence. The
/// `tbsCertificate` head is checked as well.
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
/// minimally encoded lengths. Indefinite and non-minimal lengths are BER, not
/// DER.
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
        // 0x80 is the indefinite form. Four length bytes exceed
        // `MAX_BUNDLE_BYTES` already.
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
/// Otherwise `ureq` reports a connection error that does not name the missing
/// feature. The `tls` feature is off in worker builds, so that they
/// cross-compile to musl without a C toolchain.
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
/// The [HTTP proxy
/// reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference)
/// gives the rule:
///
/// > If the command has an input data stream, then PUT. If the command is
/// > mutating, then POST. Otherwise GET.
///
/// Both properties are declared per command in the cluster's driver registry:
/// `write_table` takes a data stream and is a PUT, `create` mutates and is a
/// POST, `get` and `get_supported_features` do neither and are GETs. Public
/// because [`Client::raw_command`](crate::Client::raw_command) cannot choose
/// for a command it does not know.
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
/// Balancers send relative ones (`Location: /api/v4/exists?path=…`), so it is
/// made absolute before the origin is compared or reported. The forms of [RFC
/// 3986 §4.2](https://www.rfc-editor.org/rfc/rfc3986#section-4.2), in order: an
/// absolute URI as is; `//host/path` keeps the scheme; `/path` keeps scheme and
/// authority; a relative path keeps the request path's directory. A reference
/// with no path keeps the base's
/// ([§5.3](https://www.rfc-editor.org/rfc/rfc3986#section-5.3)):
/// `?path=//other` against `/api/v4/exists?path=//tmp` is
/// `/api/v4/exists?path=//other`, and `#frag` keeps the query too.
///
/// `None` for an empty `Location` or a request address with no `scheme://`: not
/// a redirect this client acts on.
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

    // The base's path and query; a fragment is never resolved against.
    let base = target.split('#').next().unwrap_or("");
    let path = base.split('?').next().unwrap_or("");

    // A bare fragment keeps the base's query; a query of its own replaces it.
    if location.starts_with('#') {
        return Some(format!("{scheme}://{authority}{base}{location}"));
    }
    if location.starts_with('?') {
        return Some(format!("{scheme}://{authority}{path}{location}"));
    }

    // Merged with the base path's directory, dropping the old query.
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
/// What a credential-carrying redirect turns on. Userinfo is not part of an
/// origin, so `http://real.example.net@evil.example.net/` is
/// `evil.example.net`. A missing port is the scheme's default: `https://h` and
/// `https://h:443` are one origin. Fails closed: a URL that cannot be split is
/// not the same origin as anything.
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
        // No default port for a scheme this client does not speak.
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

    /// `heavy_base` under the default rules — a name may not leave the domain.
    fn routed(configured: &str, host: &str) -> Option<String> {
        heavy_base(configured, host, &HeavyHosts::SameDomain).ok()
    }

    /// `heavy_base` with the domain rule relaxed.
    fn routed_anywhere(configured: &str, host: &str) -> Option<String> {
        heavy_base(configured, host, &HeavyHosts::Anywhere).ok()
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
    fn a_hosts_answer_cannot_send_the_token_somewhere_else() {
        // The `/hosts` body decides where every
        // heavy command goes, and a heavy command carries the caller's OAuth
        // token — so on a plain-http base, forging this body is exactly as easy
        // as forging a `Location` header, which this client already refuses to
        // follow.

        // 1. The scheme downgrade. `http://n0132` from an `https://` client
        //    would strip TLS and put the token on the wire in cleartext.
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
        // what makes the empty entries above empty.
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
    fn a_bare_cluster_name_is_matched_as_a_label_and_not_as_a_domain() {
        // `YT_PROXY=hume` — a cluster name with no dots — is the ordinary
        // spelling, and `Transport::new` supports it on purpose. It has no
        // leftmost label to take off, so the parent-domain rule degenerated to
        // "the name itself" and refused the real answer of a real installation:
        // `["n0008-sas.hume.yt.example.net"]` was declined in full, the state
        // settled as "this cluster has no heavy proxies", and it is never asked
        // again, leaving the operator with a control proxy's refusal and
        // nothing to connect it to.
        assert!(same_domain("hume", "n0008-sas.hume.yt.example.net"));
        // The documentation's own example shape, which is the same rule.
        assert!(same_domain("cluster-name", "n0008-sas.cluster-name"));
        // And Kubernetes, where a service addressed by its short name answers
        // with the fully qualified one.
        assert!(same_domain(
            "yt-http-proxy",
            "yt-http-proxy-0.yt-http-proxy.yt.svc.cluster.local"
        ));

        // Not the leftmost label, which is where the *proxy's* own name goes:
        // a name that puts the cluster's name there is claiming to be the
        // cluster, in somebody else's zone.
        assert!(!same_domain("hume", "hume.evil.com"));
        // A whole label, not a prefix of one.
        assert!(!same_domain("hume", "n0008-sas.humeier.yt.example.net"));
        assert!(!same_domain("hume", "evil.com"));
        // The configured name itself is still the configured name.
        assert!(same_domain("hume", "hume"));

        // And through `heavy_base`, which is where the base URL
        // `Transport::new` builds for a bare name meets the rule: `Client::new
        // ("hume")` is `https://hume`, and the answer above is what a real
        // installation returns for it.
        assert_eq!(
            routed("https://hume", "n0008-sas.hume.yt.example.net"),
            Some("https://n0008-sas.hume.yt.example.net".to_owned())
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

        let error = transport.unusable(&transport.base).expect("a refusal");
        assert!(matches!(error, ClientError::Config(_)), "{error}");
        // And against the address a heavy command would actually be dialled
        // at: a discovered https:// heavy proxy is refused by the same
        // bundle, a plain-http one is not.
        assert!(
            transport
                .unusable("https://n0132-sas.example.net")
                .is_some()
        );
        assert!(transport.unusable("http://n0132-sas.example.net").is_none());
        assert!(error.to_string().contains("YT_CA_BUNDLE"), "{error}");
    }

    #[test]
    fn a_refused_bundle_does_not_stop_a_cluster_reached_over_plain_http() {
        // No handshake, so nothing the bundle would have configured. A stale
        // variable in a shell profile is not a reason to refuse a local
        // cluster.
        let mut transport = transport(None);
        transport.tls_refused = Some("YT_CA_BUNDLE names /etc/no-such-file".to_owned());

        assert!(transport.unusable(&transport.base).is_none());
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
        let transport = cannot_send();
        let error = transport
            .dispatch(
                &transport.base,
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
    fn an_installation_that_really_does_answer_elsewhere_can_say_so() {
        // The opt-in, for a cluster fronted by a vanity address or one whose
        // data proxies live under a separate zone. It relaxes the domain and
        // nothing else: the scheme still comes from the configured address, and
        // a name carrying furniture is still not a name.
        assert_eq!(
            routed_anywhere(
                "https://cluster.example.net",
                "n0132-sas.somewhere-else.net"
            ),
            Some("https://n0132-sas.somewhere-else.net".to_owned())
        );
        assert_eq!(
            routed_anywhere("https://cluster.example.net", "http://n0132"),
            None,
            "the escape hatch is about the domain, not about the scheme"
        );
        assert_eq!(
            routed_anywhere(
                "https://cluster.example.net",
                "real.example.net@evil.example.net"
            ),
            None
        );
        // Nor about blank entries. With the domain rule relaxed, this is the
        // only thing standing between an empty name and the base URL
        // `https://:8000`.
        for blank in ["", "   ", "\t\n"] {
            assert_eq!(
                routed_anywhere("https://cluster.example.net:8000", blank),
                None,
                "{blank:?} was accepted as a host name"
            );
        }
    }

    #[test]
    fn a_list_written_out_by_hand_is_the_third_answer() {
        // The domain rule is a typo guard, not a boundary: on a shared platform
        // a parent domain is shared with every other tenant. A list somebody
        // wrote on purpose is the version that is a boundary — and the only
        // cure for a domain rule that misses by one label that is not "take the
        // rule away entirely".
        let only = HeavyHosts::Only(vec![
            "n0132-sas.somewhere-else.net".to_owned(),
            "n0133-sas.somewhere-else.net:9013".to_owned(),
        ]);

        assert_eq!(
            heavy_base(
                "https://cluster.example.net:8443",
                "n0132-sas.somewhere-else.net",
                &only
            ),
            Ok("https://n0132-sas.somewhere-else.net:8443".to_owned()),
            "a listed name outside the domain is still allowed"
        );
        // Case is not part of a host name, and a port is compared only where
        // both sides name one — `/hosts` usually names none.
        assert_eq!(
            heavy_base(
                "https://cluster.example.net:8443",
                "N0133-SAS.somewhere-else.net:9013",
                &only
            ),
            Ok("https://N0133-SAS.somewhere-else.net:9013".to_owned()),
        );
        assert_eq!(
            heavy_base(
                "https://cluster.example.net:8443",
                "n0133-sas.somewhere-else.net",
                &only
            ),
            Ok("https://n0133-sas.somewhere-else.net:8443".to_owned()),
            "a listed port must not be a requirement on an answer that has none"
        );
        assert_eq!(
            heavy_base(
                "https://cluster.example.net:8443",
                "n0133-sas.somewhere-else.net:9014",
                &only
            ),
            Err(Declined::Elsewhere),
            "a port both sides name has to be the same port"
        );
        // Everything else is refused, including a name the domain rule would
        // have allowed: this narrows, it does not widen.
        assert_eq!(
            heavy_base(
                "https://cluster.example.net",
                "n0134-sas.example.net",
                &only
            ),
            Err(Declined::Elsewhere)
        );
        assert_eq!(
            heavy_base("https://cluster.example.net", "http://n0132", &only),
            Err(Declined::Malformed),
            "a list is about which names, not about what a name may look like"
        );
        // An empty list admits nothing, which is a way of turning routing off.
        assert_eq!(
            heavy_base(
                "https://cluster.example.net",
                "n0132-sas.example.net",
                &HeavyHosts::Only(Vec::new())
            ),
            Err(Declined::Elsewhere)
        );
    }

    #[test]
    fn a_named_domain_widens_the_rule_without_removing_it() {
        // The shape a large installation has: the cluster is addressed as
        // `cluster.example.net` and
        // `/hosts` answers seventy-nine names under `proxy-zone.net`. The two
        // settings that existed were writing all seventy-nine down — stale the
        // moment one rotates — and taking the rule away.
        let under = HeavyHosts::Under {
            domains: vec!["proxy-zone.net".to_owned()],
            ignored: Vec::new(),
        };
        let configured = "https://cluster.example.net";

        assert_eq!(
            heavy_base(configured, "n0132-sas.rack7.proxy-zone.net", &under),
            Ok("https://n0132-sas.rack7.proxy-zone.net".to_owned())
        );
        // Case is not part of a host name here either.
        assert_eq!(
            heavy_base(configured, "N0133-SAS.rack7.PROXY-ZONE.net", &under),
            Ok("https://N0133-SAS.rack7.PROXY-ZONE.net".to_owned())
        );
        // The domain itself, not only what is under it.
        assert_eq!(
            heavy_base(configured, "proxy-zone.net", &under),
            Ok("https://proxy-zone.net".to_owned())
        );
        // It widens rather than replaces: the configured address's own domain
        // still admits its own proxies.
        assert_eq!(
            heavy_base(configured, "n0008-sas.example.net", &under),
            Ok("https://n0008-sas.example.net".to_owned())
        );
        // And it is still a rule. A neighbour that only looks like the domain
        // is not under it, and everything else is where it was.
        for elsewhere in [
            "proxy-zone.net.evil.com",
            "evil-proxy-zone.net",
            "n0132-sas.somewhere-else.net",
        ] {
            assert_eq!(
                heavy_base(configured, elsewhere, &under),
                Err(Declined::Elsewhere),
                "{elsewhere}"
            );
        }
        // A name that is not a name is refused before any of this: naming a
        // domain says which hosts, not what a host may look like.
        assert_eq!(
            heavy_base(configured, "http://n0132-sas.rack7.proxy-zone.net", &under),
            Err(Declined::Malformed)
        );
        // An empty list is exactly the default, so a variable set to nothing
        // cannot quietly widen anything.
        assert_eq!(
            heavy_base(
                configured,
                "n0132-sas.rack7.proxy-zone.net",
                &HeavyHosts::Under {
                    domains: Vec::new(),
                    ignored: Vec::new(),
                }
            ),
            Err(Declined::Elsewhere)
        );
    }

    #[test]
    fn a_refusal_names_the_domains_that_were_added() {
        // The refusal is the whole of what an operator has to work from, and
        // one that named only the configured address would read as though the
        // list had been ignored.
        let under = HeavyHosts::Under {
            domains: vec!["proxy-zone.net".to_owned()],
            ignored: Vec::new(),
        };
        let because = Declined::Elsewhere.because(&under, "https://cluster.example.net");

        assert!(because.contains("cluster.example.net"), "{because}");
        assert!(because.contains("proxy-zone.net"), "{because}");

        // With nothing added there is nothing extra to name, and the sentence
        // is the one the default rule has always given.
        assert_eq!(
            Declined::Elsewhere.because(
                &HeavyHosts::Under {
                    domains: Vec::new(),
                    ignored: Vec::new(),
                },
                "https://cluster.example.net"
            ),
            Declined::Elsewhere.because(&HeavyHosts::SameDomain, "https://cluster.example.net")
        );
    }

    #[test]
    fn a_written_domain_is_normalised_the_way_it_gets_written() {
        // These arrive from `YT_HEAVY_PROXY_DOMAINS` and from configuration
        // files as often as from a literal, so every spelling a person uses for
        // one domain has to reach the same rule. The wildcard is the one that
        // matters most: `*.proxy-zone.net` is how a zone is described in prose
        // and in a certificate, and kept verbatim it would test
        // `ends_with(".*.proxy-zone.net")` and match nothing at all — the
        // feature a silent no-op, and the heavy commands still failing.
        //
        // And they are one domain, not six: a refusal that read `not under
        // cluster.example.net or under proxy-zone.net, proxy-zone.net,
        // proxy-zone.net` looks like a bug in the client to the one person it
        // is written for.
        let mut transport = Transport::new("https://cluster.example.net", None, HOSTS_TIMEOUT);
        transport.set_heavy_proxies_under(vec![
            "  .Proxy-Zone.net. ".to_owned(),
            "*.proxy-zone.net".to_owned(),
            "https://proxy-zone.net".to_owned(),
            "proxy-zone.net:443".to_owned(),
            "https://proxy-zone.net./".to_owned(),
            "proxy-zone.net.:443".to_owned(),
        ]);

        assert_eq!(
            transport.heavy_hosts_debug(),
            r#"Under { domains: ["proxy-zone.net"], ignored: [] }"#
        );
        assert_eq!(
            heavy_base(
                &transport.base,
                "n0132-sas.rack7.proxy-zone.net",
                &transport.hosts
            ),
            Ok("https://n0132-sas.rack7.proxy-zone.net".to_owned()),
        );
    }

    #[test]
    fn an_entry_that_is_not_a_domain_is_dropped_rather_than_believed() {
        // A single label is the dangerous one: `net` is a plausible typo for a
        // real domain, and honoured as a suffix it would admit every `.net`
        // host `/hosts` could name — `with_heavy_proxies_anywhere` by accident.
        // It is kept aside rather than forgotten, because a setting that
        // changes nothing and says nothing is indistinguishable from one this
        // client never read. An entry that is nothing at all is a trailing
        // comma, and nobody needs to hear about it.
        let mut transport = Transport::new("https://cluster.example.net", None, HOSTS_TIMEOUT);
        transport.set_heavy_proxies_under(vec![
            "net".to_owned(),
            "   ".to_owned(),
            String::new(),
            ".".to_owned(),
            "*".to_owned(),
        ]);

        assert_eq!(
            transport.heavy_hosts_debug(),
            r#"Under { domains: [], ignored: ["net"] }"#
        );
        // And it is in the refusal, which is the only place an operator looks.
        let because = Declined::Elsewhere.because(&transport.hosts, &transport.base);
        assert!(because.contains("ignored, not a domain: net"), "{because}");
        // And with everything dropped the rule is exactly the default: the
        // configured address's own domain admits its own proxies, and nothing
        // else is admitted at all.
        assert_eq!(
            heavy_base(&transport.base, "n0132-sas.example.net", &transport.hosts),
            Ok("https://n0132-sas.example.net".to_owned())
        );
        assert_eq!(
            heavy_base(
                &transport.base,
                "n0132-sas.rack7.proxy-zone.net",
                &transport.hosts
            ),
            Err(Declined::Elsewhere)
        );
    }

    #[test]
    fn an_added_domain_carries_the_configured_port_and_keeps_a_named_one() {
        // Nothing about naming a domain changes where the port comes from: the
        // configured address's, unless `/hosts` named one itself.
        let under = HeavyHosts::Under {
            domains: vec!["proxy-zone.net".to_owned()],
            ignored: Vec::new(),
        };

        assert_eq!(
            heavy_base(
                "https://cluster.example.net:8443",
                "n0132-sas.rack7.proxy-zone.net",
                &under
            ),
            Ok("https://n0132-sas.rack7.proxy-zone.net:8443".to_owned())
        );
        assert_eq!(
            heavy_base(
                "https://cluster.example.net:8443",
                "n0132-sas.rack7.proxy-zone.net:9013",
                &under
            ),
            Ok("https://n0132-sas.rack7.proxy-zone.net:9013".to_owned())
        );
    }

    #[test]
    fn a_dotless_configured_name_keeps_its_label_rule_when_a_domain_is_added() {
        // `YT_PROXY=hume` is matched as a *label* of the discovered name rather
        // than as a domain — see `same_domain` — and adding a domain must not
        // cost that: `Under` is the label rule plus the domains, not instead of
        // them. Reachable without a resolver search list now that
        // `YT_PROXY_SUFFIX` exists, which is what makes this worth pinning.
        let under = HeavyHosts::Under {
            domains: vec!["proxy-zone.net".to_owned()],
            ignored: Vec::new(),
        };

        assert_eq!(
            heavy_base("https://hume", "n0008-sas.hume.yt.example.net", &under),
            Ok("https://n0008-sas.hume.yt.example.net".to_owned()),
            "the label rule still applies"
        );
        assert_eq!(
            heavy_base("https://hume", "n0132-sas.rack7.proxy-zone.net", &under),
            Ok("https://n0132-sas.rack7.proxy-zone.net".to_owned()),
            "and the added domain applies beside it"
        );
        assert_eq!(
            heavy_base("https://hume", "hume.evil.com", &under),
            Err(Declined::Elsewhere),
            "and neither admits the cluster's name in a host's position"
        );
    }

    #[test]
    fn a_bracketed_address_has_to_hold_an_ipv6_literal() {
        // A bare IPv6 literal is not a valid URL authority — bracketed, it is —
        // and an unbracketed second colon means this is not one host and one
        // port.
        assert_eq!(
            routed_anywhere("http://[2a02:6b8::1]:8000", "[2a02:6b8::2]:9013"),
            Some("http://[2a02:6b8::2]:9013".to_owned())
        );
        assert_eq!(
            routed_anywhere("http://[2a02:6b8::1]:8000", "[2a02:6b8::2]"),
            Some("http://[2a02:6b8::2]:8000".to_owned()),
            "the configured port carries through a bracketed name too"
        );
        assert_eq!(
            routed_anywhere("http://cluster.example.net", "2a02:6b8::2"),
            None
        );
        assert_eq!(
            routed_anywhere("http://cluster.example.net", "n0132:9013:9014"),
            None
        );

        // The shape that made the brackets worth checking rather than merely
        // counting colons. Probed against `ureq` 3.3: this parses with the host
        // `[n0132.example.com]` — brackets are only stripped for something that
        // is an IPv6 literal — so no DNS will ever answer it. The token stays
        // put, which is the reason the second-colon rule waved it through, and
        // the cost is worse than a leak of nothing: the address is remembered,
        // every heavy command fails resolving it, and the failures repeat for
        // as long as the client lives.
        for shape in [
            "[n0132.example.com]evil.attacker.com",
            "[n0132.example.com]",
            "[n0132.example.com]:9013",
            "[2a02:6b8::2]junk",
            "[2a02:6b8::2]:junk",
            "[2a02:6b8::2]:",
            "[2a02:6b8::2",
            "[]",
            "[]:9013",
            // A port is digits, on either shape of name.
            "n0132.example.net:",
            "n0132.example.net:90a3",
            ":9013",
        ] {
            assert_eq!(
                routed_anywhere("http://cluster.example.net", shape),
                None,
                "{shape:?} was accepted as a host name"
            );
        }
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
            let destination = transport.base_for(light);
            assert!(
                matches!(destination, Destination::Configured(_)),
                "{light:?} went looking for a heavy proxy"
            );
            assert_eq!(destination.address(), "http://cluster.invalid:8000");
        }
    }

    /// A pool of exactly these hosts, seeded as if `/hosts` had just answered.
    fn pooled(transport: &Transport, hosts: &[&str]) {
        *lock(&transport.heavy) = HeavyProxy::Pool(HeavyPool {
            hosts: hosts.iter().map(|host| (*host).to_owned()).collect(),
            fetched: Instant::now(),
        });
    }

    /// The destination a failed command reports having been routed to.
    fn discovered(base: &str) -> Destination<'static> {
        Destination::Discovered(base.to_owned())
    }

    /// The hosts a seeded pool still holds, or `None` once it stopped being
    /// a pool at all.
    fn pool_of(transport: &Transport) -> Option<Vec<String>> {
        match &*lock(&transport.heavy) {
            HeavyProxy::Pool(pool) => Some(pool.hosts.clone()),
            _ => None,
        }
    }

    #[test]
    fn a_rejected_certificate_drops_the_host_and_a_wrong_command_does_not() {
        // Without a TLS listener presenting a bad certificate. A cert rejected
        // `NotValidForName` is a per-host verdict, but it is not retriable and
        // not `worth_asking_again`, so a drop gated on either predicate would
        // pin the client to the one bad host until the window elapsed. Dropping
        // must not need the lookup's predicate to agree.
        let mut transport =
            Transport::new("https://cluster.example.net", None, Duration::from_secs(1));
        transport.set_proxy_discovery(true);
        pooled(
            &transport,
            &[
                "https://n0132-sas.example.net",
                "https://n0133-sas.example.net",
            ],
        );

        let rejected: Result<()> = Err(ClientError::Transport {
            command: "write_table".to_owned(),
            source: Box::new(ureq::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid peer certificate: certificate not valid for name \
                 \"n0132-sas.example.net\"; certificate is only valid for [\"cluster.example.net\"]",
            ))),
        });
        let reported = transport.after_heavy(
            Repeatable::Heavy,
            &discovered("https://n0132-sas.example.net"),
            rejected,
        );

        assert!(reported.is_err());
        assert_eq!(
            pool_of(&transport).as_deref(),
            Some(&["https://n0133-sas.example.net".to_owned()][..]),
            "the host whose certificate was rejected stayed in the pool"
        );

        // The other half of the predicate: a failure about the *request* —
        // a table that does not exist — will be exactly as wrong next door,
        // so it costs the pool nothing.
        let wrong_command: Result<()> = Err(ClientError::Http {
            command: "write_table".to_owned(),
            status: 404,
            body: String::new(),
        });
        let reported = transport.after_heavy(
            Repeatable::Heavy,
            &discovered("https://n0133-sas.example.net"),
            wrong_command,
        );

        assert!(reported.is_err());
        assert_eq!(
            pool_of(&transport).as_deref(),
            Some(&["https://n0133-sas.example.net".to_owned()][..]),
            "a mistaken command evicted a perfectly good host"
        );
    }

    #[test]
    fn a_response_too_large_leaves_the_host_that_served_it_in_the_pool() {
        // The predicate assertions in `the_cap_counts_…` say this one layer
        // up; this is the consequence they stand for, watched happening. A
        // pool of two, a `read_file` past the cap, and the question of whose
        // fault it was.
        let mut transport =
            Transport::new("https://cluster.example.net", None, Duration::from_secs(1));
        transport.set_proxy_discovery(true);
        let both = [
            "https://n0132-sas.example.net".to_owned(),
            "https://n0133-sas.example.net".to_owned(),
        ];
        let seed = |transport: &Transport| {
            pooled(
                transport,
                &[
                    "https://n0132-sas.example.net",
                    "https://n0133-sas.example.net",
                ],
            );
        };

        // What this error was before it was classified: `ureq`'s
        // `BodyExceedsLimit` inside a `Transport`. It is not an `Io` error, so
        // `rejected_the_certificate` cannot narrow it, and the predicate says
        // yes to a host that did nothing wrong.
        seed(&transport);
        let as_it_was: Result<()> = Err(ClientError::Transport {
            command: "read_file".to_owned(),
            source: Box::new(ureq::Error::BodyExceedsLimit(RESPONSE_LIMIT)),
        });
        let _ = transport.after_heavy(
            Repeatable::Heavy,
            &discovered("https://n0132-sas.example.net"),
            as_it_was,
        );
        assert_eq!(
            pool_of(&transport).as_deref(),
            Some(&["https://n0133-sas.example.net".to_owned()][..]),
            "the old shape was supposed to evict the host — if it no longer \
             does, the half of this test that follows has stopped proving \
             anything"
        );

        // And what it is now. The host served the request perfectly, and the
        // response will be exactly as large at the next proxy along.
        seed(&transport);
        let now: Result<()> = Err(body_failure(
            "read_file",
            RESPONSE_LIMIT,
            ureq::Error::BodyExceedsLimit(RESPONSE_LIMIT),
        ));
        let reported = transport.after_heavy(
            Repeatable::Heavy,
            &discovered("https://n0132-sas.example.net"),
            now,
        );

        assert!(reported.is_err());
        assert_eq!(
            pool_of(&transport).as_deref(),
            Some(&both[..]),
            "a response too large to hold cost the pool a healthy data proxy"
        );
    }

    #[test]
    fn a_response_too_large_keeps_the_way_past_it() {
        // `routed_to` names the proxy a routed command actually went to, by
        // appending " at <host>" to the command. `ResponseTooLarge` is the one
        // command-carrying error it leaves alone, and the coupling is easy to
        // miss: the message offers the streaming half of the same command, and
        // `error::streaming_advice` finds that half by matching the command
        // name exactly — so decorating the name deletes the advice.
        //
        // Adding a `ResponseTooLarge` arm beside the others is what fails
        // here, which is the point: an edit that decorates it uniformly should
        // have to read this, rather than have a caller discover it after being
        // told a file was too large and not told what to do instead.
        let reported = routed_to(
            body_failure(
                "read_file",
                RESPONSE_LIMIT,
                ureq::Error::BodyExceedsLimit(0),
            ),
            "https://n0132-sas.example.net",
        );

        let message = reported.to_string();
        assert!(message.contains("read_file_streaming"), "{message}");
        assert!(!message.contains(" at n0132-sas"), "{message}");

        // The neighbours it sits between are still decorated, so this is a
        // deliberate exception rather than a `routed_to` that stopped working.
        let neighbour = routed_to(
            ClientError::Decode {
                command: "read_file".to_owned(),
                reason: "cut short".to_owned(),
            },
            "https://n0132-sas.example.net",
        );
        assert!(
            neighbour.to_string().contains(" at n0132-sas"),
            "{neighbour}"
        );
    }

    #[test]
    fn a_pool_with_nobody_left_falls_back() {
        // The last host dropped is not a pool of zero to divide by — it is
        // the fallback state. That the fallback *ends* — the next heavy
        // command after the window asks the cluster again — is pinned where a
        // listener can watch it happen:
        // `an_emptied_pool_asks_the_cluster_again_after_the_window` in
        // tests/request_shape.rs.
        let mut transport =
            Transport::new("https://cluster.example.net", None, Duration::from_secs(1));
        transport.set_proxy_discovery(true);
        pooled(&transport, &["https://n0132-sas.example.net"]);

        let refused: Result<()> = Err(ClientError::Http {
            command: "write_table".to_owned(),
            status: 503,
            body: String::new(),
        });
        let _ = transport.after_heavy(
            Repeatable::Heavy,
            &discovered("https://n0132-sas.example.net"),
            refused,
        );

        assert!(
            matches!(&*lock(&transport.heavy), HeavyProxy::FellBack { .. }),
            "an emptied pool did not fall back"
        );
    }

    #[test]
    fn a_discovered_host_that_spells_the_configured_address_is_still_dropped() {
        // `/hosts` may name the configured host itself — a caller pointed
        // straight at a data proxy the coordinator also lists — and
        // `heavy_base` then builds a base URL byte-identical to the
        // configured one. Judging "was this command routed?" by comparing
        // addresses reads that failure as the caller's own choice: the
        // draining host stays in the pool, is picked again and again for as
        // long as it drains, and the error grows a sentence about routing
        // being off that is simply false. Which is why `Destination` carries
        // the fact instead of the address being trusted to imply it.
        let mut transport = Transport::new(
            "https://n0132-sas.example.net",
            None,
            Duration::from_secs(1),
        );
        transport.set_proxy_discovery(true);
        pooled(
            &transport,
            &[
                "https://n0132-sas.example.net",
                "https://n0133-sas.example.net",
            ],
        );

        let drained: Result<()> = Err(ClientError::Http {
            command: "write_table".to_owned(),
            status: 503,
            body: String::new(),
        });
        let reported = transport.after_heavy(
            Repeatable::Heavy,
            &discovered("https://n0132-sas.example.net"),
            drained,
        );

        assert!(
            reported
                .expect_err("a 503 is a failure")
                .to_string()
                .starts_with("write_table at n0132-sas.example.net:"),
            "a routed failure at the configured host's own name went unattributed"
        );
        assert_eq!(
            pool_of(&transport).as_deref(),
            Some(&["https://n0133-sas.example.net".to_owned()][..]),
            "the host was spared the drop for spelling the configured address"
        );
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
        // handed a refusal with no advice in it. See [`HEAVY`]:
        // `Repeatable::Heavy` writes the same fact down a second time, and the
        // two have to agree.
        for command in [
            "read_table",
            "write_table",
            "read_file",
            "write_file",
            "get_job_input",
            "get_job_stderr",
        ] {
            assert!(HEAVY.contains(&command), "{command}");
        }
        // And the one reachable only through the raw door, which is the point
        // of listing what the cluster calls heavy rather than what this crate
        // models.
        assert!(HEAVY.contains(&"read_blob_table"));
        for command in ["create", "exists", "start_operation", "get_job", "hosts"] {
            assert!(!HEAVY.contains(&command), "{command}");
        }
    }

    /// Serves one response carrying `payload`, and hands back the address to
    /// send to.
    ///
    /// `gzip` chooses whether the bytes go out compressed, which is the whole
    /// question these tests are about: compressed, the wire and the `Vec` are
    /// different quantities, and it matters which one the cap counts.
    ///
    /// The listener is on its own thread and is dropped with it; nothing here
    /// retries, so one accepted connection is the whole of its life.
    fn serving(payload: &[u8], gzip: bool) -> String {
        use std::io::Write;

        let (encoding, body) = if gzip {
            use flate2::{Compression, write::GzEncoder};
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(payload).expect("compresses");
            (
                "Content-Encoding: gzip\r\n",
                encoder.finish().expect("finishes"),
            )
        } else {
            ("", payload.to_vec())
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");

        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accepts");
            let mut reader = std::io::BufReader::new(stream.try_clone().expect("clones"));
            drain_request(&mut reader);

            let mut reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                 {encoding}Content-Length: {}\r\n\r\n",
                body.len()
            )
            .into_bytes();
            reply.extend_from_slice(&body);
            stream.write_all(&reply).ok();
            stream.flush().ok();
        });

        format!("http://{address}")
    }

    /// Serves a gzip body that never ends and decodes to nothing.
    ///
    /// The case the wire backstop is the only guard against, and the reason
    /// [`wire_budget`] is not simply dropped now that the cap counts decoded
    /// bytes. An empty deflate *stored* block is five bytes — `00 00 00 ff ff`
    /// — and a stream of them makes `flate2` loop **inside a single `read`**,
    /// consuming input and producing no output: [`CapReader`] is never
    /// re-entered, so `left` never moves and a cap on decoded bytes is never
    /// spent.
    ///
    /// Chunked, so there is no length to disagree with, and the thread writes
    /// until the client stops listening.
    fn serving_endless_empty_deflate() -> String {
        use std::io::Write;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");

        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accepts");
            let mut reader = std::io::BufReader::new(stream.try_clone().expect("clones"));
            drain_request(&mut reader);

            if stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                      Content-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n",
                )
                .is_err()
            {
                return;
            }

            // A well-formed gzip header, and then a member that is all framing
            // and no content, for as long as anyone is reading.
            let mut empty_blocks = Vec::new();
            for _ in 0..256 {
                empty_blocks.extend_from_slice(&[0x00, 0x00, 0x00, 0xff, 0xff]);
            }
            let mut payload = vec![0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];
            payload.extend_from_slice(&empty_blocks);

            loop {
                let framed = format!("{:x}\r\n", payload.len());
                if stream.write_all(framed.as_bytes()).is_err()
                    || stream.write_all(&payload).is_err()
                    || stream.write_all(b"\r\n").is_err()
                    || stream.flush().is_err()
                {
                    return;
                }
                payload.clone_from(&empty_blocks);
            }
        });

        format!("http://{address}")
    }

    /// Reads one whole request off `reader` — the head, and the body if it has
    /// one.
    ///
    /// Not a parser, just enough of one to know when a request has ended.
    /// `upload` is why the body half exists: its request *is* a stream, `ureq`
    /// sends it chunked, and a listener that answered before reading it would
    /// leave the client writing into a socket nobody is draining.
    fn drain_request(reader: &mut impl std::io::BufRead) {
        let mut head = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) if line == "\r\n" => break,
                Ok(_) => head.push_str(&line),
            }
        }

        let header = |name: &str| {
            head.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name)
                    .then(|| value.trim().to_owned())
            })
        };

        if header("transfer-encoding").is_some_and(|value| value.eq_ignore_ascii_case("chunked")) {
            // `<hex length>\r\n`, the bytes, `\r\n`; a length of zero ends it.
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    return;
                }
                let Ok(size) = usize::from_str_radix(line.trim(), 16) else {
                    return;
                };
                let mut chunk = vec![0; size + 2];
                if reader.read_exact(&mut chunk).is_err() || size == 0 {
                    return;
                }
            }
        }

        if let Some(length) = header("content-length").and_then(|value| value.parse().ok()) {
            let mut body = vec![0_u8; length];
            let _ = reader.read_exact(&mut body);
        }
    }

    /// `n` bytes gzip cannot shrink, the same `n` bytes every run.
    ///
    /// A xorshift rather than a constant, and that is the whole point:
    /// `vec![7; 4096]` compresses to nothing, so a boundary test written on it
    /// cannot see the case where the *wire* is larger than the `Vec` — which is
    /// the case the wire backstop has to make room for.
    fn incompressible(n: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        (0..n)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 33) as u8
            })
            .collect()
    }

    /// A transport that will hold `limit` decoded bytes and no more.
    fn capped(base: &str, limit: u64) -> Transport {
        let mut transport = Transport::new(base, None, Duration::from_secs(10));
        transport.set_response_limit(limit);
        transport
    }

    /// A `read_file` through the real `send`, capped at `limit` decoded bytes.
    fn read_file_capped(base: &str, limit: u64) -> Result<Vec<u8>> {
        capped(base, limit).send(
            base,
            Method::Get,
            "read_file",
            &map([("path", string("//tmp/f"))]),
            &Payload::None,
        )
    }

    /// A `write_table` through the real `upload`, capped the same way.
    fn upload_capped(base: &str, limit: u64) -> Result<Vec<u8>> {
        capped(base, limit).upload(
            Method::Put,
            "write_table",
            &map([("path", string("//tmp/t"))]),
            &mut std::io::empty(),
        )
    }

    #[test]
    fn the_cap_counts_the_bytes_held_and_not_the_bytes_transferred() {
        // The claim the documentation makes, and the one it could not keep
        // while `ureq`'s own `limit()` was the whole of the guard: that sits
        // *under* the gzip decoder, so it bounds the wire and not the `Vec`.
        // This body is 40 000 bytes of zeros — comfortably past a 4 096-byte
        // cap, and small enough compressed that a cap on the wire would never
        // notice it. Measured on a cluster the same way: a 5 000 000-byte file
        // of zeros arrives in 4 892 bytes, and `ureq` asked to stop at 100 000
        // hands back all five million.
        let error = read_file_capped(&serving(&vec![0_u8; 40_000], true), 4_096)
            .expect_err("the cap is reached");

        assert!(
            matches!(error, ClientError::ResponseTooLarge { limit: 4_096, .. }),
            "{error:?}"
        );

        // Two mutations this fails. Reverting the call site to an inline
        // `Transport { .. }` — the shape that was here — is the first, and it
        // is not only a worse message: `BodyExceedsLimit` is not an `Io`
        // error, so all three predicates that narrow a `Transport` by looking
        // inside it wave it through. The read would be *retried*, and a heavy
        // one would drop the host from the pool for serving the request
        // perfectly. Enough of those empty the pool and the fallback window
        // answers unrelated writes with the control proxy's refusal, which is
        // what a caller who only asked for a large file would get.
        assert!(!crate::retry::is_retriable(&error), "{error}");
        assert!(!crate::retry::worth_asking_again(&error), "{error}");
        assert!(!crate::retry::attributable_to_the_host(&error), "{error}");

        // And it says both things the caller needs: how big is too big, and
        // what to call instead. `transport error: the response body is larger
        // than request limit: 536870912` said neither.
        let message = error.to_string();
        assert!(message.contains("4096"), "{message}");
        assert!(message.contains("read_file_streaming"), "{message}");
    }

    #[test]
    fn a_body_of_exactly_the_cap_is_not_over_it() {
        // `ureq`'s `LimitReader` errors on the next `read` once its budget
        // reaches zero, and `read_to_end` always makes that read to find the
        // end — so the cap it enforces is one byte tighter than the error it
        // raises says. A body of exactly the limit is not larger than it.
        //
        // This is `CapReader`'s half of the boundary and only its half: the
        // payload compresses to nothing, so the wire guard is nowhere near
        // deciding anything, and tightening `read` to `read as u64 >=
        // self.left` is what fails here. The wire guard's own half — a body
        // *larger* on the wire than in the `Vec` — is
        // `the_wire_backstop_leaves_room_for_a_body_it_must_not_refuse`, two
        // tests rather than one so that deleting either cannot quietly unpin
        // both boundaries at once.
        let held = read_file_capped(&serving(&vec![7_u8; 4_096], true), 4_096)
            .unwrap_or_else(|e| panic!("fits exactly, but {e}"));
        assert_eq!(held, vec![7_u8; 4_096]);

        // And one byte more does not — either encoding, because either guard
        // may be the one that notices.
        for gzip in [true, false] {
            let error = read_file_capped(&serving(&vec![7_u8; 4_097], gzip), 4_096)
                .expect_err("one byte past the cap");
            assert!(
                matches!(error, ClientError::ResponseTooLarge { limit: 4_096, .. }),
                "gzip={gzip}: {error:?}"
            );
        }
    }

    #[test]
    fn the_wire_backstop_leaves_room_for_a_body_it_must_not_refuse() {
        // The cap counts decoded bytes, so the backstop beneath the decoder
        // has to admit whatever the largest permitted body weighs
        // *compressed* — and compressed is not always smaller. Deflate expands
        // what it cannot shrink, so a body of exactly the cap can cross the
        // wire larger than the cap. Measured here with `flate2`: 4 096
        // incompressible bytes gzip to 4 119, which a budget of `limit + 1`
        // refuses — a response inside the documented ceiling turned away by
        // the guard for responses outside it. See `wire_budget`.
        let awkward = incompressible(4_096);
        let compressed = {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&awkward).expect("compresses");
            encoder.finish().expect("finishes").len()
        };
        assert!(
            compressed > 4_096,
            "this needs a body gzip makes bigger, and {compressed} is not one"
        );

        let held = read_file_capped(&serving(&awkward, true), 4_096)
            .unwrap_or_else(|e| panic!("{compressed} wire bytes for 4096 held, and {e}"));
        assert_eq!(held, awkward);

        // And the plainer case the slack was first there for: with no encoding
        // the two guards count the same bytes, and `ureq`'s errors on the read
        // that finds the end. A budget of `limit` fails both halves of this.
        let held = read_file_capped(&serving(&vec![7_u8; 4_096], false), 4_096)
            .unwrap_or_else(|e| panic!("uncompressed and exactly the cap, but {e}"));
        assert_eq!(held, vec![7_u8; 4_096]);
    }

    #[test]
    fn an_endless_body_that_decodes_to_nothing_is_still_bounded() {
        // Why the wire backstop stays now that the cap counts decoded bytes. A
        // chunked stream of empty deflate stored blocks decodes to nothing at
        // all, so `CapReader` never spends a byte of its budget — and `flate2`
        // loops *inside* one `read`, so it is not even re-entered to notice.
        // Only the limit under the decoder ends this.
        //
        // On its own thread with a deadline, because what this pins is not a
        // wrong answer but no answer: without the backstop the read does not
        // return, and a test that hangs is a test that says nothing.
        let (done, answer) = std::sync::mpsc::channel();
        let base = serving_endless_empty_deflate();
        std::thread::spawn(move || {
            let _ = done.send(read_file_capped(&base, 4_096));
        });

        let outcome = answer
            .recv_timeout(Duration::from_secs(20))
            .expect("a body that never ends must still be refused, and was not");
        let error = outcome.expect_err("nothing decoded, so there is nothing to hand back");
        assert!(
            matches!(error, ClientError::ResponseTooLarge { limit: 4_096, .. }),
            "{error:?}"
        );
    }

    #[test]
    fn the_cap_a_transport_is_built_with_is_the_documented_one() {
        // `set_response_limit` is what lets every test around this one cost
        // 4 KiB instead of half a gigabyte — and it is also what would let the
        // cap quietly become something else, because a default of `u64::MAX`
        // disables the guard crate-wide and leaves all of them passing. This
        // is the one assertion that reads the number itself.
        let transport = Transport::new("https://cluster.example.net", None, Duration::from_secs(1));
        assert_eq!(transport.response_limit, RESPONSE_LIMIT);
        assert_eq!(RESPONSE_LIMIT, 512 * 1024 * 1024);
    }

    #[test]
    fn a_response_this_client_will_not_hold_fails_the_upload_that_got_it() {
        // An upload reads its answer whatever the caller wants with it — a
        // body left unread keeps the connection out of the pool — and every
        // other way that read can fail is swallowed on purpose: the status
        // line already said the write was done, a heavy command is sent once,
        // and failing there would fail a write that succeeded.
        //
        // This one is not that. `raw_command_upload` hands the `Vec` back as
        // *the answer*, so an answer too large to hold, swallowed, becomes a
        // command that returned nothing — the silent corruption the cap exists
        // to turn into a refusal. Making the arm `=> Vec::new()` is what fails
        // here, and nothing else in the suite notices it.
        let error = upload_capped(&serving(&vec![0_u8; 40_000], true), 4_096)
            .expect_err("the answer is past the cap");

        assert!(
            matches!(error, ClientError::ResponseTooLarge { limit: 4_096, .. }),
            "{error:?}"
        );

        // And an answer that fits is still handed back, so the refusal above
        // is about the size of it and not about uploads.
        let body = upload_capped(&serving(b"{\"value\"={}}", true), 4_096).expect("fits");
        assert_eq!(body, b"{\"value\"={}}");
    }

    #[test]
    fn a_body_over_the_cap_blames_the_request_and_not_the_proxy_that_served_it() {
        // The message, per command, without a socket. Each buffered read
        // points at its own streaming half, and a command that has none
        // promises nothing.
        let file = body_failure(
            "read_file",
            RESPONSE_LIMIT,
            ureq::Error::BodyExceedsLimit(RESPONSE_LIMIT),
        )
        .to_string();
        assert!(file.contains("536870912"), "{file}");
        assert!(file.contains("read_file_streaming"), "{file}");

        let table = body_failure(
            "read_table",
            RESPONSE_LIMIT,
            ureq::Error::BodyExceedsLimit(RESPONSE_LIMIT),
        )
        .to_string();
        assert!(table.contains("read_table_streaming"), "{table}");

        let get = body_failure(
            "get",
            RESPONSE_LIMIT,
            ureq::Error::BodyExceedsLimit(RESPONSE_LIMIT),
        )
        .to_string();
        assert!(!get.contains("streaming"), "{get}");

        // The cap the caller is told is the one they can plan around, not the
        // one `ureq` was handed — `read_capped` gives it a byte more so that a
        // body of exactly the cap survives the wire guard too.
        let quoted = body_failure(
            "read_file",
            RESPONSE_LIMIT,
            ureq::Error::BodyExceedsLimit(RESPONSE_LIMIT + 1),
        )
        .to_string();
        assert!(quoted.contains("536870912"), "{quoted}");
    }

    #[test]
    fn a_body_cut_short_is_still_the_network_failure_it_always_was() {
        // The other half, and the reason the split is a `match` rather than a
        // blanket reclassification: a connection cut while the body streams in
        // is the same failure as one cut a packet earlier, worth waiting for
        // and — for a heavy command — worth trying another host for.
        let error = body_failure(
            "read_file",
            RESPONSE_LIMIT,
            ureq::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            )),
        );

        assert!(matches!(error, ClientError::Transport { .. }), "{error:?}");
        assert!(crate::retry::is_retriable(&error), "{error}");
        assert!(crate::retry::attributable_to_the_host(&error), "{error}");
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
