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
//!
//! # Where a command is sent
//!
//! Not every command goes to the address the client was configured with. A
//! large installation gives its proxies roles, and a *control* proxy will not
//! serve a heavy request — so the heavy ones ask `/hosts` where they should
//! go. [`Transport::base_for`] is that decision and [`HeavyProxy`] is what it
//! remembers.
//!
//! What a control proxy does with a heavy request depends on whether the
//! request carries **input data**, and this client's own error rendering hides
//! the difference — the status is not in the message, only the cluster's error
//! document is. From `TContext::TryRedirectHeavyRequests` in
//! [`yt/yt/server/http_proxy/context.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/server/http_proxy/context.cpp):
//!
//! - a heavy command **with** an input stream — `write_table`, `write_file` —
//!   is refused with **503** and `Retry-After: 60`, carrying the error
//!   `Control proxy may not serve heavy requests with input data`;
//! - a heavy command **without** one — `read_table`, `read_file`,
//!   `get_job_input`, `get_job_stderr` — is answered with a **307** to a data
//!   proxy, or with 503 and `There are no data proxies available` if there is
//!   none.
//!
//! The documentation gives both halves and neither whole: the
//! [`/hosts` section](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)
//! says "light proxies return code 503", and the return-code table on the same
//! page lists "307 — Redirecting heavy queries from light to heavy proxies".
//! The input-data test above is what decides which.
//!
//! Discovery is what keeps either from happening. The lookup gets its **own**
//! budget rather than the client's — see [`HOSTS_TIMEOUT`] — because it sits in
//! front of the first heavy command and a proxy that cannot answer it in that
//! time has not earned the wait.

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
/// Where a redirect points. Read by this client rather than by `ureq` — see
/// [`Transport::redirect`].
const LOCATION: &str = "Location";
/// How many redirects one request may follow before the chain is called a loop.
///
/// `ureq`'s own default, kept so that turning the following over to this client
/// changed the policy and not the numbers.
const MAX_REDIRECTS: usize = 10;

/// How much of a response a buffered command will hold in memory — 512 MiB,
/// counted **after** decompression.
///
/// Buffered responses are small (a table or file read is the exception, and a
/// launcher reads results, not bulk data), and `ureq`'s own default is
/// conservative enough to truncate a modest table silently, which is the
/// failure this number exists to avoid. It is not a promise that half a
/// gigabyte in a `Vec` is a good idea: the two commands that reach it in
/// practice — [`Client::read_table`](crate::Client::read_table) and
/// [`Client::read_file`](crate::Client::read_file) — each have a streaming
/// half that holds nothing, and the error names it (see [`body_failure`]).
///
/// **`ureq`'s own `limit()` cannot enforce this**, which is why [`CapReader`]
/// exists. `BodyWithConfig::do_build` wraps the raw body source in a
/// `LimitReader` and then builds the gzip decoder *on top of it*, so the
/// number it is given bounds what arrives **on the wire** — not what lands in
/// the `Vec`. This client always asks for compression (`ureq`'s `gzip` feature
/// is on, and `tests/request_shape.rs` pins the header), so those are not the
/// same quantity and are not close to it. Measured against a local cluster: a
/// `read_file` of a 5 000 000-byte file of zeros answers `Content-Encoding:
/// gzip` in **4 892 wire bytes**, and `ureq` 3.3 asked for `.limit(100_000)`
/// on that same read hands back all 5 000 000 without an error — fifty times
/// its limit. At that ratio a 512 MiB *wire* cap would admit hundreds of
/// gigabytes into memory, which is the OOM the documentation used to promise
/// could not happen.
///
/// So the cap is applied where the bytes accumulate: [`CapReader`] sits above
/// the decoder and counts what comes out of it.
///
/// **It bounds what is held, not what a process needs.** The bytes land in a
/// `Vec` that grows by doubling and copies as it grows, so the old buffer and
/// the new one are both resident for the length of a copy — about 1.5× the cap
/// where the allocator cannot extend in place. Measured here, in a release
/// build, against a listener serving gzipped zeros: a read of 536 870 911 bytes
/// peaks at **544 178 176** of resident set for the 512 MiB it hands back, and
/// a 600 MiB read *refused* by this cap peaks at **611 385 344** — 1.14× the
/// number the error quotes. So `512 MiB` is what this client will hold, not
/// what to size a container for.
///
/// **It covers the two buffered reads and not the crate.** [`Transport::send`]
/// and [`Transport::upload`] read through [`read_capped`]; two other places
/// still take `ureq`'s wire-only default — the non-2xx branch of
/// [`Transport::open`] and the `/hosts` lookup in [`Transport::fetch`] — and a
/// gzipped body there is bounded on the wire, which is the ratio above all over
/// again. Both read an answer this client is about to fail on, so neither is
/// reached in the ordinary case; neither is bounded in memory either.
const RESPONSE_LIMIT: u64 = 512 * 1024 * 1024;

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
/// happens to model: `read_blob_table` has no method here and is reachable
/// through
/// [`Client::raw_command_streaming`](crate::Client::raw_command_streaming), so
/// leaving it out would take the advice away from exactly the caller who went
/// to the trouble of streaming. (`read_file` sat beside it until it grew
/// [`Client::read_file`](crate::Client::read_file); its entry below predates
/// the method and is unchanged by it.)
///
/// Used for one thing only — whether a refused redirect is told to go to a
/// heavy proxy. A command sent through
/// [`Client::raw_command`](crate::Client::raw_command) that is heavy and not
/// listed here loses the advice, not the refusal.
///
/// **The same fact is written down twice.** [`Repeatable::Heavy`] encodes the
/// cluster's `isHeavy` bit for *routing* (#38, since merged), and this list
/// encodes it for the redirect advice; they say the same thing about the same
/// commands. A command routed to a heavy proxy but missing here is refused a
/// redirect with `heavy: false` and told nothing it can act on, so a new
/// `Repeatable::Heavy` call site must be checked against this list. The one
/// entry that has no call site to check against is `read_blob_table`, and
/// that is the point of the paragraph above.
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
/// [`BatchRequest::raw`](crate::BatchRequest::raw) for a **narrower** job than
/// it once had. `isHeavy` is not the cluster's rule for what may be a batch
/// part — that rule is the command's data types, and lives in
/// `batch::NOT_A_BATCH_PART`; measured, `get_job_spec` is heavy and is taken as
/// a part, while `write_table` is heavy and is taken as a part *and applies*.
/// What this list still decides for a batch is this crate's own policy: bulk
/// data does not travel inline in a batch body to a light proxy, whatever the
/// cluster would tolerate.
pub(crate) fn is_heavy(command: &str) -> bool {
    HEAVY.contains(&command)
}

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

/// The cluster's own words when a proxy refuses a command because of the role
/// it has.
///
/// `Control proxy may not serve heavy requests with input data`, from
/// `TContext::TryRedirectHeavyRequests`. It is the only failure here that names
/// the *addressee* rather than the request, which is why two places read it: a
/// proxy that says it is worth asking the cluster for another one
/// ([`crate::retry::worth_asking_again`]), and a caller who got it at the
/// address they configured is owed the sentence that says why nothing routed it
/// away ([`refusal_hint`]).
pub(crate) const CONTROL_REFUSAL: &str = "may not serve heavy requests";

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
///
/// `execute_batch` is here for a different reason than its neighbours: it is
/// served by the proxy's own driver, but its options are `TExecuteBatchOptions
/// : TMutatingOptions` — no transactional half — so an outer `transaction_id`
/// means nothing. **Measured on a local cluster**: a batch stamped with one
/// created its node *outside* the transaction, visible at once and untouched
/// by the abort. `Client::execute_batch` stamps the transaction into each
/// part's parameters instead, which the same measurement shows the cluster
/// honours; the entry here keeps the blanket stamp from dressing the envelope
/// up in a parameter the cluster is known to drop.
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

/// Whether `command` is one the blanket transaction stamp skips.
///
/// Read by `Client::execute_batch` as well as by
/// [`Transport::in_transaction`], because a batch *part* is a command too: a
/// `get_operation` that takes no `transaction_id` outside a batch takes none
/// inside one, and two copies of the list would drift.
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

/// How long the whole `/hosts` lookup may take.
///
/// **Not the client's request timeout, and not its retry policy.** This
/// question sits in front of the first heavy command, its answer is a few
/// hundred bytes from a proxy the client is already talking to, and failing to
/// get one is not fatal — the command goes where it would have gone before
/// there was a lookup at all. Under the client's own policy it was five
/// attempts of up to two minutes with fifteen seconds of backoff between them,
/// so a `/hosts` that answered 503 cost a heavy command **fifteen seconds** and
/// one that hung cost it **ten minutes**, all of it under the mutex.
///
/// One attempt, then. The retry is [`HOSTS_RETRY_AFTER`] rather than a second
/// attempt inside the lock: spreading it out is what keeps a client whose
/// `/hosts` is down from paying for the answer over and over.
const HOSTS_TIMEOUT: Duration = Duration::from_millis(800);

/// How long the configured address serves heavy commands after a **lookup**
/// that did not settle, before the cluster is asked again.
///
/// A lookup that failed for a reason that might pass means "use the address the
/// caller gave, and ask again in a moment" rather than "ask again now", which
/// is what turned eight threads into eight lookups.
///
/// **A failed heavy *command* is not this.** Its answer is dropping the host
/// it used from the pool — see [`Transport::after_heavy`] — and only a pool
/// with nobody left in it comes back here. Falling back on the first failure
/// is what made a single transient 503 route the next ten seconds of uploads
/// to a control proxy that refuses every one of them, which is the symptom
/// this whole feature exists to prevent. **A failed *refresh* is not this
/// either**: the pool in hand still routes, so the question is simply put off
/// for another [`HOST_LIST_REFRESH_INTERVAL`] — see [`Transport::base_for`].
///
/// Short, because it is also how quickly routing comes back once the cluster
/// does. Long enough that a client uploading in a loop against a broken
/// `/hosts` pays [`HOSTS_TIMEOUT`] a handful of times a minute rather than
/// once per upload. Settable — [`Transport::set_hosts_retry_after`] — because
/// a constant nothing can move is a constant no test can tell from any other:
/// with this fixed at ten seconds, nothing in the suite outlived one window, so
/// [`HeavyProxy::Configured`] and [`HeavyProxy::FellBack`] were observationally
/// identical and either could be swapped for the other with every test green.
const HOSTS_RETRY_AFTER: Duration = Duration::from_secs(10);

/// How old a `/hosts` answer may grow before a heavy command re-asks.
///
/// The [proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)
/// asks for exactly this: "A good strategy is to re-query the `/hosts` list
/// every minute or every few queries and change the current proxy to which
/// queries are made." A minute, then — and lazily, on the heavy command that
/// finds the list stale, the way the C++ client's `THostManager` does it,
/// rather than from a background thread this crate would otherwise not need.
///
/// Settable — [`Transport::set_host_list_refresh_interval`] — for the same
/// reason [`HOSTS_RETRY_AFTER`] is: a constant nothing can move is a constant
/// no test can tell from any other, and "refreshed after the interval and not
/// before" is one of the properties the tests pin.
const HOST_LIST_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Where the cluster wants heavy commands sent.
///
/// Resolved on the first heavy command and *maintained* after that — refreshed
/// when it grows old, a failed host dropped — rather than asked once and kept
/// for the client's lifetime; see [`Transport::base_for`]. Shared by every
/// clone, because `Client::with_transaction`, `Operation` and the diagnostics
/// client are all clones of one client, and a lookup each would be a lookup
/// per command.
#[derive(Debug)]
enum HeavyProxy {
    /// The cluster has not been asked yet.
    Unasked,
    /// The answer `/hosts` gave, kept whole and picked from at random.
    Pool(HeavyPool),
    /// The cluster was asked and named none this client may use, so the
    /// configured address serves heavy commands too. A single-node cluster, any
    /// installation that does not separate the roles, and a `/hosts` whose
    /// answer was refused — see [`heavy_base`].
    ///
    /// **A settled answer, not a pause — but settled for one refresh
    /// interval, not for ever.** The difference from [`HeavyProxy::FellBack`]
    /// is the clock it runs on: a failure that might pass is re-asked after
    /// the short [`HOSTS_RETRY_AFTER`], where this is re-asked on the same
    /// lazy [`HOST_LIST_REFRESH_INTERVAL`] as a pool. It used to be permanent,
    /// and a permanent answer from one lookup is a pin: a launcher whose first
    /// upload landed in the few seconds of a rolling restart when `/hosts`
    /// answers `[]` would send every heavy command to the control proxy for
    /// the rest of its life.
    Configured {
        /// When the cluster gave this answer.
        asked: Instant,
    },
    /// The question did not settle, or the whole pool has now been dropped.
    ///
    /// The configured address serves heavy commands until `until`, and then the
    /// cluster is asked once more. This is what a *waiting* thread finds rather
    /// than an invitation to perform the same failing lookup itself, and what a
    /// heavy command finds once every proxy in the answer has been tried.
    FellBack {
        /// When to ask again. See [`HOSTS_RETRY_AFTER`]. `None` for a window
        /// so long no `Instant` can express its end —
        /// `with_hosts_retry_after(Duration::MAX)` means a fallback that does
        /// not end, and must not be a panic in `Instant` arithmetic instead.
        until: Option<Instant>,
    },
}

/// The heavy proxies this client is currently willing to use.
///
/// What the official clients keep and this crate did not: the C++ client's
/// `THostManager` holds the whole `/hosts` answer and picks a random member
/// per request, refreshing the list lazily when it has outlived its interval;
/// the Go client's `ProxySet` does the same with a ban list beside it. This
/// crate pinned the first name for the client's lifetime instead, and that
/// divergence produced two real failures (#40): a per-host condition — a
/// certificate valid for every proxy but one — pinned every upload to the one
/// bad host for as long as the client lived, and a fleet of clients never
/// rebalanced, each keeping whichever host its one lookup happened to name
/// however the load moved afterwards.
///
/// So: never commit to one host. A pool, picked from at random per command; a
/// host a command failed at is dropped and the next command picks from what
/// remains; a refresh — the next heavy command after
/// [`HOST_LIST_REFRESH_INTERVAL`] — rebuilds the pool from a fresh answer,
/// which is also what restores a dropped host the cluster still vouches for.
/// The restoration is deliberate and has a price: a *persistently* bad host —
/// the misissued certificate that motivated #40 — is re-learned at one failed
/// command per interval until somebody fixes it, which is the trade this
/// crate makes against keeping a ban list with its own clock (Go's five
/// minutes) for a condition that is always an operator's bug.
#[derive(Debug)]
struct HeavyPool {
    /// Usable base URLs from the last `/hosts` answer, minus any dropped
    /// since. Never empty: a pool with nothing left to pick from becomes
    /// [`HeavyProxy::FellBack`] instead, which is a state that ends —
    /// [`HeavyPool::drop_host`] says whether the pool survived, so the
    /// emptying and the transition live at one call site.
    hosts: Vec<String>,
    /// When the answer these came from arrived. Age is judged against the
    /// transport's own interval at the moment of asking, so clones of one
    /// client — which share this state but may configure different intervals
    /// — each honour their own, and an interval of `Duration::MAX` simply
    /// never elapses rather than panicking in `Instant` arithmetic.
    fetched: Instant,
}

impl HeavyPool {
    /// One of the pool's hosts, picked at random.
    ///
    /// Random per command, as both official clients pick — the property is
    /// load-spreading, not unpredictability, so the id source this crate
    /// already has is entropy enough (its contract is *unique, not
    /// unpredictable*, and `unique::word` records that this caller also
    /// leans on its uniformity). The modulo bias against a 64-bit word is
    /// beneath measuring for any real fleet.
    fn pick(&self) -> &str {
        let drawn = crate::unique::word(0) % self.hosts.len() as u64;
        &self.hosts[drawn as usize]
    }

    /// Takes a failed host out of the pool until a refresh restores it, and
    /// says whether the pool survived.
    ///
    /// By value, not by position: two commands in flight may both have gone to
    /// the host that just failed, and the second drop must not evict an
    /// innocent neighbour — or anything at all, once the first has already
    /// done it. The return value is what keeps the "never empty" invariant at
    /// the call site that could break it: a caller that drops must deal with
    /// `false` or leave a pool [`HeavyPool::pick`] would divide by zero on.
    #[must_use]
    fn drop_host(&mut self, base: &str) -> bool {
        self.hosts.retain(|host| host != base);
        !self.hosts.is_empty()
    }
}

/// Where one command was sent: an address this client chose out of `/hosts`,
/// or the one the caller configured.
///
/// A fact carried from [`Transport::base_for`] to [`Transport::after_heavy`]
/// rather than re-derived there, because the address alone cannot answer it:
/// `/hosts` may perfectly well name the configured host — a caller pointed
/// straight at a data proxy the coordinator also lists — and [`heavy_base`]
/// then builds a base URL byte-identical to the configured one. Inferring
/// "routed" by comparing strings read that case as "the caller's own choice",
/// so the draining host was never dropped and the failure was explained with
/// a sentence about routing being off. Which address was *chosen* is
/// something only the chooser knows, so the chooser says so.
enum Destination<'a> {
    /// An address picked from the `/hosts` pool, owned because the pool the
    /// pick came from may be gone by the time the failure is judged.
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

/// Which of the names `/hosts` gives back this client is willing to use.
///
/// Four answers, because the two the crate shipped with were "everything the
/// domain rule allows" and "everything at all" — and the only cure for a domain
/// rule that misses by one label was to give up the control entirely. See
/// [`heavy_base`] for what the rule is and, more to the point, what it is worth.
#[derive(Clone, Debug)]
enum HeavyHosts {
    /// The configured address's own domain, the default. See [`same_domain`].
    SameDomain,
    /// That domain **and** the ones named here, which is what an installation
    /// publishing its heavy proxies in a second zone actually has: a cluster at
    /// `cluster.example.net` whose `/hosts` answers
    /// `n0132-sas.rack7.proxy-zone.net` needs `proxy-zone.net`
    /// added, not the rule removed. See [`under_domain`].
    Under {
        /// The domains as [`Transport::set_heavy_proxies_under`] normalised
        /// them: lowercased, without wildcard, scheme, port or stray dots, and
        /// without duplicates.
        domains: Vec<String>,
        /// What was handed in and could not be used — an entry with no dot left
        /// in it, which would admit a whole top-level domain if honoured.
        ///
        /// **Kept in order to be reported.** The setter has no failure path, so
        /// an entry it drops would otherwise vanish: the rule stays where it
        /// was, every refusal reads exactly as if the caller had configured
        /// nothing, and `YT_HEAVY_PROXY_DOMAINS=net` looks from the outside like
        /// a variable that was ignored — which is the shape of the very problem
        /// this mode exists to end. [`Declined::because`] names these.
        ignored: Vec<String>,
    },
    /// Wherever `/hosts` says, checked for being a host name and nothing else.
    Anywhere,
    /// Exactly these names, compared without case — and a port only where both
    /// sides name one, since `/hosts` usually names none and the port then
    /// comes from the configured address.
    ///
    /// An empty list admits nothing, which is a way of saying "route nowhere"
    /// — [`Transport::set_proxy_discovery`] is the way of saying it plainly.
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
/// The host without case, and the port **only where both name one**: `/hosts`
/// answers with bare host names unless the coordinator's `ShowPorts` says
/// otherwise, so a list that had to spell the port out would be a list that
/// usually matched nothing.
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
/// Kept apart from the refusal itself so that the client can say which of the
/// two happened: a name it could not read is a broken cluster or a forged
/// answer, and a name it could read and declined is a configuration this
/// operator can change in one line.
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
            // The domains are named because the whole point of this mode is
            // that one more was needed: an operator reading the refusal has to
            // see the list they wrote, or the next guess is that it was ignored.
            // And what was dropped is named for the same reason, the other way
            // round: an entry that is not a domain would otherwise change
            // nothing and say nothing, which reads exactly like a variable this
            // client never looked at.
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
    /// The whole budget for one `/hosts` lookup. [`HOSTS_TIMEOUT`] by default,
    /// and its own field rather than a minimum with `timeout` so that a cluster
    /// answering in 900 ms can be routed to at all.
    hosts_timeout: Duration,
    /// How long a fallback lasts before the cluster is asked again. See
    /// [`HOSTS_RETRY_AFTER`].
    hosts_retry_after: Duration,
    /// How old a `/hosts` answer may grow before a heavy command re-asks. See
    /// [`HOST_LIST_REFRESH_INTERVAL`].
    host_list_refresh: Duration,
    token: Option<String>,
    retries: RetryPolicy,
    /// End-to-end limit for buffered commands — one budget per attempt, shared
    /// out between the redirect hops that attempt makes. Per-phase limit for
    /// streaming ones. See [`Transport::dispatch`].
    timeout: Duration,
    /// How much of a buffered response this client will hold in memory:
    /// [`RESPONSE_LIMIT`], counted after decompression.
    ///
    /// A field rather than the constant read at each of the places that need
    /// it — [`Transport::send`] and [`Transport::upload`] — because a cap only
    /// reachable by producing half a gigabyte is a cap no test reaches, and a
    /// guard no test reaches can be deleted at any of them without anything
    /// going red. It has one production value; the only thing that changes it
    /// is `Transport::set_response_limit`, which does not exist outside
    /// `cfg(test)` — and so does not exist in a rendered doc either.
    response_limit: u64,
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
    /// Normalised here rather than at each comparison: a domain arrives from a
    /// configuration file or an environment variable as often as from a
    /// literal, and every way a person writes one has to mean the same thing.
    /// `*.Proxy-Zone.NET. `, `https://proxy-zone.net` and `proxy-zone.net:443`
    /// all normalise to `proxy-zone.net` — the wildcard because that is how a
    /// zone gets described in prose and in a certificate, the scheme and port
    /// because [`same_name`] tolerates both for
    /// [`crate::Client::with_heavy_proxies_in`] and a caller has no reason to
    /// expect these two to differ.
    ///
    /// **An entry with no dot left in it is not used.** `net` is a plausible
    /// typo for a real domain and would admit every `.net` host `/hosts` could
    /// name, which is [`crate::Client::with_heavy_proxies_anywhere`] with extra
    /// steps. [`same_domain`] never shortens below two labels for the same
    /// reason; that floor is not the public-suffix argument, and dropping it
    /// because a human typed the value rather than deriving it would be
    /// backwards. `""` goes the same way — it would make the suffix test
    /// `ends_with(".")`, which is no test at all.
    ///
    /// Such an entry is **kept in `ignored` and named in the refusal**, not
    /// forgotten. This is a builder with no failure path, so dropping one in
    /// silence would leave the rule where it was and every message reading
    /// exactly as though the caller had configured nothing — an operator who
    /// sets `YT_HEAVY_PROXY_DOMAINS` and sees no change learns nothing about
    /// why, which is the shape of the problem this mode exists to end. See
    /// [`Declined::because`].
    ///
    /// Duplicates go too, and the four spellings above are why: they all
    /// normalise to one string, and a refusal listing `proxy-zone.net,
    /// proxy-zone.net` reads as a bug in the client to the one person it is
    /// written for. Order is the caller's, first mention winning.
    pub(crate) fn set_heavy_proxies_under(&mut self, domains: Vec<String>) {
        let mut kept: Vec<String> = Vec::with_capacity(domains.len());
        let mut ignored: Vec<String> = Vec::new();

        for domain in &domains {
            // `host_of` first, then the dots: it reads a URL, so a trailing dot
            // trimmed off `https://proxy-zone.net./` before it would leave the
            // path behind and the dot in place.
            let normalised = host_of(domain.trim())
                .trim_start_matches('*')
                .trim_matches('.')
                .to_ascii_lowercase();

            // An entry that is nothing at all is a list artefact — a trailing
            // comma in `YT_HEAVY_PROXY_DOMAINS`, a blank line in a config — and
            // reporting it would put "(ignored, not a domain: )" in front of
            // somebody who did not write anything to be told about.
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

    /// Which discovered hosts this transport would use, rendered.
    ///
    /// `#[cfg(test)]`, and a string rather than the enum: [`HeavyHosts`] is
    /// private to this module and worth keeping that way — the rule is chosen
    /// through `Client`, not inspected.
    #[cfg(test)]
    pub(crate) fn heavy_hosts_debug(&self) -> String {
        format!("{:?}", self.hosts)
    }

    /// Lowers the buffered-response cap, so a test can reach it.
    ///
    /// [`RESPONSE_LIMIT`] is half a gigabyte: a test that had to produce one to
    /// watch the guard work is a test that would not be written, and the guard
    /// would go unpinned at every site that applies it — which is how
    /// [`Transport::upload`]'s came to swallow the failure unnoticed.
    ///
    /// `#[cfg(test)]` rather than `pub(crate)`: the cap is not a knob, and
    /// nothing outside a test may widen it. See [`RESPONSE_LIMIT`] for why the
    /// number is the number.
    #[cfg(test)]
    pub(crate) fn set_response_limit(&mut self, limit: u64) {
        self.response_limit = limit;
    }

    /// Drops what discovery resolved, because the rules it resolved under have
    /// changed.
    ///
    /// A fresh `Arc`, not a write through the shared one: these are builders on
    /// a clone of the client, and narrowing the rules here must not discard what
    /// the client this was cloned from has already resolved under the old ones.
    fn forget_heavy(&mut self) {
        self.heavy = Arc::new(Mutex::new(HeavyProxy::Unasked));
    }

    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        // Through `build_agent` rather than by editing the config in place:
        // this is the one place the agent is built twice, and so the one place
        // the redirect policy (max_redirects(0), from #36) could be dropped by
        // a caller doing nothing more suspicious than `with_timeout`. The TLS
        // refusal is rediscovered here too — the bundle is re-read, so a
        // variable fixed since the client was built is picked up.
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
    /// deduplicates, and a heavy command is sent once whatever the policy says
    /// — and to the proxy the cluster named for heavy work, which is the other
    /// half of what [`Repeatable::Heavy`] declares.
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
                    // The ID stays the same across attempts — that is what the
                    // cluster deduplicates by — and only the flag changes. A
                    // caller-supplied ID may already be marked as a replay,
                    // which is how a restarted process resumes; the cluster
                    // refuses a duplicate that does not say so.
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
    /// Everything light goes to the address the caller configured. A heavy
    /// command — [`Repeatable::Heavy`], the `isHeavy` bit of the cluster's own
    /// command registry — goes to a proxy that will accept one: an installation
    /// that separates the roles will not serve a heavy request on a *control*
    /// proxy, and the balancer a caller is usually pointed at fronts exactly
    /// those. See the module documentation for what the refusal looks like.
    ///
    /// The lookup happens when the first heavy command needs it, and then
    /// again only when the answer has outlived the transport's refresh
    /// interval ([`HOST_LIST_REFRESH_INTERVAL`] unless overridden) — lazily,
    /// on the heavy command that finds it stale, never from a background
    /// thread. The **whole** answer is kept as a pool and every heavy command
    /// picks a member **at random**: `/hosts` is ordered by load, and a
    /// client that keeps its one pick for life never rebalances — a draining
    /// host keeps every client that ever picked it. A host a command failed
    /// at is dropped from the pool until a refresh restores it; see
    /// [`Transport::after_heavy`]. Both official clients do exactly this
    /// (`THostManager` in C++, `ProxySet` in Go), and the property they agree
    /// on is the one this preserves: **never commit to one host.**
    ///
    /// A refresh that does not produce a usable list — a failed lookup, an
    /// empty answer, an answer refused in full — keeps the pool it was
    /// refreshing and puts the question off for **another whole interval**.
    /// The hosts in hand are from an answer the cluster did give, so dropping
    /// them over a lookup hiccup would route uploads to a control proxy; and
    /// unlike the *initial* lookup, nothing here is waiting on the answer, so
    /// there is no urgency to justify the short [`HOSTS_RETRY_AFTER`] — a
    /// refresh retried on that clock against a down `/hosts` would put a
    /// lookup's stall in front of heavy traffic several times a minute,
    /// for an answer the pool makes unnecessary.
    ///
    /// A cluster that names nobody this client may use — a single-node
    /// installation, any that does not split the roles, and one whose answer
    /// [`heavy_base`] refused — is remembered as answered, and every heavy
    /// command then goes where it always went, until one refresh interval
    /// passes and the question is put once more. That fallback is not a
    /// nicety: it is the whole of what keeps a local cluster behaving as it
    /// did. A refused answer is also *said*, the first time it settles — see
    /// [`crate::observe::declined`] — because it is the one branch here that
    /// looks exactly like the bug this feature fixes; the re-asks that follow
    /// stay quiet rather than repeating the sentence once a minute.
    ///
    /// The mutex is held **across the lookup — the refresh included**,
    /// deliberately: a second thread that wanted a heavy proxy at the same
    /// moment waits for this answer rather than asking the same question
    /// again. What makes that safe rather than a queue is that every outcome
    /// leaves an answer — a pool, [`HeavyProxy::Configured`], or
    /// [`HeavyProxy::FellBack`] — with a clock on it, so the waiters find a
    /// decision and the stall is bounded: at most one lookup of at most
    /// [`Transport::hosts_timeout`] per interval, however many threads are
    /// uploading. Before that, eight threads against a failing `/hosts`
    /// performed eight lookups, each waiting out the one in front. `fetch`
    /// does not touch this lock, so there is nothing here to deadlock
    /// against.
    fn base_for(&self, repeatable: Repeatable) -> Destination<'_> {
        if repeatable != Repeatable::Heavy || !self.discovery {
            return Destination::Configured(&self.base);
        }

        let mut resolved = lock(&self.heavy);
        match &mut *resolved {
            HeavyProxy::Pool(pool) => {
                // A stale pool is refreshed before it is picked from — the
                // documentation's own strategy, and lazily like the C++
                // client, so the client that stopped uploading also stopped
                // asking. Age is judged against this transport's interval, so
                // clones sharing the state each honour their own.
                if pool.fetched.elapsed() >= self.host_list_refresh {
                    match self.usable_hosts() {
                        Ok(hosts) if !hosts.is_empty() => {
                            *pool = HeavyPool {
                                hosts,
                                fetched: Instant::now(),
                            };
                        }
                        // Nothing usable — a failed lookup, or an answer with
                        // nobody in it, which a fleet mid-rotation can
                        // briefly give. The pool in hand keeps routing and
                        // the question waits out another interval; see the
                        // doc above for why not the short retry window.
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
            // Every host this client is willing to use becomes the pool a
            // heavy command picks from. A name that is blank, malformed or
            // somewhere else entirely is passed over rather than being
            // allowed to stand for the whole answer — and, since a whole
            // answer passed over is the one failure that leaves routing
            // silently off, the reasons are said out loud once.
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
            // A failed lookup is never fatal: the command goes where it would
            // have gone before there was a lookup at all. Whether to ask again
            // *soon* is `worth_asking_again`, which is a different question
            // from whether to retry — a cluster with no `/hosts` endpoint
            // answers 404 every time and must not be asked before every
            // upload, while a timeout or a restarting proxy says nothing
            // about the roles and is worth one more question in a moment.
            // The settled verdict is re-examined an interval later either
            // way; permanence was the bug, not the memory.
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
    /// The refresh path: the refusals are neither collected nor said here —
    /// a refresh that declines what the first resolve declined would render
    /// the same sentences once a minute for the client's whole life, only to
    /// throw them away.
    fn usable_hosts(&self) -> Result<Vec<String>> {
        Ok(self
            .heavy_hosts()?
            .iter()
            .filter_map(|host| heavy_base(&self.base, host, &self.hosts).ok())
            .collect())
    }

    /// The heavy proxies the cluster names, best first.
    ///
    /// `/hosts` answers with a JSON list of bare host names —
    /// `["n0008-sas.cluster-name", …]`, as the
    /// [HTTP proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)
    /// shows on the wire — "ordered by load … the very first proxy in the
    /// resulting list is the least loaded"
    /// ([reference](https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts)).
    ///
    /// Which role it lists is *not* in the documentation. It is
    /// `default_role_filter`, a coordinator config parameter that
    /// `TCoordinatorConfig::Register` defaults to `NApi::DefaultHttpProxyRole`,
    /// which
    /// [`yt/yt/client/api/public.h`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/api/public.h)
    /// spells `"data"` — the role that serves heavy commands. A compiled-in
    /// default an operator can change, then, rather than a protocol guarantee,
    /// which is why this client checks what it is given instead of trusting the
    /// role.
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
    /// Two things, and both only for a command that actually went somewhere
    /// discovered — which is [`Destination`]'s to say, not something the
    /// address can be trusted to: `/hosts` may name the configured host
    /// itself, and a string comparison then read "routed there and failed" as
    /// "the caller's own choice", leaving a draining proxy in the pool for as
    /// long as it drained. A discovery-off client never takes the lock at
    /// all, and a failure at the *configured* address says nothing about a
    /// lookup: it was the caller who chose that one.
    ///
    /// **The error names the host.** `write_table: transport error: io:
    /// Connection refused` is a report about an address the caller never typed
    /// and cannot see. It now reads `write_table at n0132-sas.example.net:9013:
    /// …`.
    ///
    /// **A proxy a command failed at is dropped from the pool, and the next
    /// command picks from what remains.** The command itself is *not* sent
    /// again — heavy commands are not retried, and by this point a streaming
    /// body has been consumed anyway. This is about the next one. (A narrower
    /// gap survives on the streaming read path: [`Transport::open`] hands the
    /// body back unread, so a host that dies *mid-stream* fails in the
    /// caller's reader, past this seam, and stays in the pool until a request
    /// it answers at the head fails too.)
    ///
    /// It used to go back to the configured address for
    /// [`HOSTS_RETRY_AFTER`], and that is exactly the wrong address to go back
    /// to. On the deployment this feature was written for the configured
    /// address is a balancer in front of the *control* proxies, so one
    /// transient 503 from a draining data proxy — or one refused connection
    /// during a restart — turned into ten seconds of `Control proxy may not
    /// serve heavy requests with input data`, which is [#30] itself,
    /// reproducible on demand. `/hosts` had already named the alternatives.
    /// Now the failed host is dropped and the survivors carry the load, and
    /// only a pool with nobody left in it falls back — where falling back is
    /// at least a state that ends.
    ///
    /// Only for a failure **attributable to the host** it went to. A table
    /// that does not exist will not exist over there either, so a resolve
    /// error keeps the pool exactly as it was. But the predicate is
    /// [`crate::retry::attributable_to_the_host`], deliberately *not*
    /// [`crate::retry::worth_asking_again`]: the two agree except about a
    /// rejected certificate, and that disagreement was a real failure (#40).
    /// A certificate valid for every proxy but one — `NotValidForName` is a
    /// verdict about *this* host's name — answered "not worth asking the
    /// coordinator again", so the bad host was neither stepped past nor
    /// re-resolved, and every heavy command failed against it until the
    /// window elapsed and the same ordered-first host came back. Dropping a
    /// host must not require the lookup's own predicate to agree.
    ///
    /// [#30]: https://github.com/sshaplygin/ytsaurus-rs/issues/30
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
            // The command went to the configured address, which is the
            // caller's own choice and needs no explaining — unless what came
            // back is a proxy saying it will not serve this at all, which is
            // the failure routing exists to prevent and which says nothing
            // about routing being what was missing.
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
    /// The cap on what is held is [`Transport::response_limit`] — the whole of
    /// why that is a field and not the constant read here.
    fn send(
        &self,
        base: &str,
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
    /// Sent once, never retried, and sent to a heavy proxy — a response that is
    /// the data is the shape of a heavy command, and [`Repeatable::Heavy`] is
    /// all three of those facts at once.
    pub(crate) fn open(
        &self,
        method: Method,
        command: &str,
        parameters: &YsonValue,
    ) -> Result<ureq::Body> {
        let stamped = self.in_transaction(command, parameters);
        let parameters = stamped.as_ref().unwrap_or(parameters);

        // Through `retry::run` like every other command, with `Repeatable::Heavy`
        // doing the sending-once: it caps the loop at one attempt and never
        // reaches the retry announcement, so this needs no second seam of its
        // own to be timed and named. The span closes when the headers arrive —
        // the reader handed back is read after that, at the caller's pace.
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

        // `Repeatable::Heavy` is the sending-once, as in `open`: one attempt,
        // no announcement, and the span comes from the seam every other command
        // already goes through. Unlike `open` it covers the whole transfer —
        // the body is read here, as it goes. It also picks the address: a
        // request whose body is a data stream is the one a control proxy
        // refuses by name.
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
            let body = match read_capped(command, response.body_mut(), self.response_limit) {
                Ok(body) => body,
                // An answer this client will not hold is the one failure worth
                // failing the write over. `raw_command_upload` hands this
                // `Vec` back as *the answer*, so an empty one would be the
                // same silent corruption reading it as a string would: a
                // command that returned half a gigabyte reported as one that
                // returned nothing.
                Err(error @ ClientError::ResponseTooLarge { .. }) => return Err(error),
                // Anything else is a cut or unreadable answer to a write whose
                // status line already said it was done. A heavy command is
                // sent once, so failing here fails a write that succeeded; the
                // body is read for the connection's sake, and what it said is
                // not worth that.
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
    /// `/hosts` is the only one, and it is not a command — but it wants most of
    /// what a command gets: the token, the guard that turns an `https://` proxy
    /// in a build without TLS into an explanation rather than a connection
    /// error, and the caller headers that say who is asking. Building a bare
    /// `ureq` request here instead is how it came to miss all of them.
    ///
    /// **The timeout and the retry policy are the exceptions**, and
    /// deliberately: this question has its own budget. One attempt bounded by
    /// [`HOSTS_TIMEOUT`], not five of up to two minutes with fifteen seconds of
    /// backoff — because a heavy command is *waiting* on the answer, holding
    /// the lock every other heavy command wants, and not getting one costs
    /// nothing worse than the routing this client had none of a release ago.
    /// A lookup worth repeating is repeated by the next heavy command after
    /// [`HOSTS_RETRY_AFTER`], which is the same retry spread out where it does
    /// not queue anybody.
    ///
    /// It goes to the **configured** address whatever it is asking about: the
    /// question `/hosts` answers is where the other addresses are.
    ///
    /// It follows a **same-origin** redirect like any command (#36) — a
    /// balancer canonicalising its own `/hosts` URL — but a cross-origin one is
    /// refused with [`ClientError::Redirected`], which the router treats as
    /// worth asking again rather than a permanent verdict.
    pub(crate) fn fetch(&self, path: &str, what: &str) -> Result<String> {
        if let Some(error) = self.unusable(&self.base) {
            return Err(error);
        }

        let first = format!("{}{path}", self.base);

        // One attempt (#38): `/hosts` is not retried by the retry loop — the
        // retry is HOSTS_RETRY_AFTER, spread across later heavy commands. The
        // budget is the lookup's own (#38, HOSTS_TIMEOUT), shared across the
        // redirect hops this may follow (#36) rather than handed to each.
        crate::retry::run(RetryPolicy::none(), Repeatable::Freely, what, |_| {
            let mut url = first.clone();
            let mut hops = 0;
            let deadline = Instant::now().checked_add(self.hosts_timeout);

            // A same-origin redirect is followed — a balancer canonicalising
            // its own `/hosts` URL — and a cross-origin one is refused with
            // `ClientError::Redirected`, which the router treats as worth
            // asking again. The loop ends because `redirect` refuses past
            // MAX_REDIRECTS, or sooner because the budget runs out.
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
    ///
    /// `base` is where this one goes — the configured address for a light
    /// command, and whatever [`Transport::base_for`] resolved for a heavy one.
    fn dispatch(
        &self,
        base: &str,
        method: Method,
        command: &str,
        parameters: &YsonValue,
        mut body: Outgoing<'_>,
        streaming: bool,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        // Judged against `base` — the address this request is actually dialled
        // at, which for a heavy command is the one `/hosts` named (#38) — not
        // `self.base`. A no-TLS build must refuse an `https://` heavy proxy as
        // surely as an `https://` configured one, and a refused CA bundle bites
        // it too. `unusable` takes the address for that reason.
        if let Some(error) = self.unusable(base) {
            return Err(error);
        }

        // `mut` because a same-origin redirect (#36) reassigns it below.
        let mut url = format!("{base}/api/v4/{command}");

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
                heavy: is_heavy(command),
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
    ///
    /// `base` is the address the request is about to be dialled at, not
    /// necessarily [`Transport::base`]: a heavy command goes wherever `/hosts`
    /// named (#38), and both refusals are properties of *that* address rather
    /// than of the configured one. [`heavy_base`] derives the scheme from the
    /// configured address, so the two usually agree — but the parameter is what
    /// makes a discovered `https://` heavy proxy refused by a no-TLS build or a
    /// broken bundle, which a `self.base` check would wave through.
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

/// Takes the lock, and takes it back from a thread that panicked holding it.
///
/// What this guards is a cached address. A panic while resolving one leaves it
/// as it was — `Unasked`, or the answer from before — and none of that is worth
/// poisoning a client over.
fn lock(heavy: &Mutex<HeavyProxy>) -> MutexGuard<'_, HeavyProxy> {
    heavy
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Turns a host from `/hosts` into a base URL, or refuses it.
///
/// `/hosts` answers with **bare host names** —
/// `["n0008-sas.cluster-name", …]`, as the
/// [HTTP proxy guide](https://ytsaurus.tech/docs/en/user-guide/proxy/http#upload)
/// shows on the wire. Everything else about the address is this client's to
/// decide, and every one of those decisions is a place a forged or mistaken
/// `/hosts` body could send an upload — and the OAuth token with it — somewhere
/// the caller never named. So a name is checked rather than pasted:
///
/// - **the scheme comes from the configured address and only from there.** A
///   host naming its own is refused outright, which is what closes the
///   downgrade: `http://n0132` from an `https://` client used to strip TLS and
///   put the token on the wire in cleartext. A cluster reached over TLS serves
///   its heavy commands over TLS.
/// - **`/`, `@`, `://` and whitespace are refused.** `@` is the one that
///   matters: `real.example.net@evil.example.net` is a URL whose *host* is
///   `evil.example.net` and whose userinfo is the reassuring half.
/// - **the configured port carries through** when the name has none, because
///   the name usually has none — the coordinator only appends `:port` when its
///   `ShowPorts` config says to — and a client reached at `:8443` has no reason
///   to believe the heavy proxies answer on 80.
/// - **the name has to be one host and at most one port.** A bare IPv6 literal
///   is not a valid URL authority; bracketed, it is — and brackets around
///   anything that is *not* an IPv6 literal are worse than a refusal, because
///   `ureq` 3.3 hands them to the resolver unchanged. Probed:
///   `https://[n0132.example.com]evil.attacker.com` parses with the host
///   `[n0132.example.com]`, which no DNS will ever answer, so the entry cost
///   nothing but a permanently failing address.
/// - **the name must sit where `allowed` says**, which is the configured
///   address's own domain by default. See below.
///
/// # What the domain rule is worth, and what it is not
///
/// It was written down here as "the token cannot go somewhere you did not
/// name", and that is more than it does. To steer the token with a `/hosts`
/// body you must control that body: over `https://` that means owning the
/// proxy, which already has the token, and over `http://` it means being a
/// man-in-the-middle, who reads the token out of every light command without
/// touching this code path at all. The one threat model where the rule bites is
/// a proxy **registering itself** in the cluster's coordinator under a name the
/// operators did not intend.
///
/// And there it is a coarse instrument, because it is a suffix rule and cannot
/// be anything else without a public-suffix list — a dependency deliberately
/// not taken. `yt-prod.westeurope.cloudapp.azure.com` admits every Azure VM in
/// the region; `yt-1234.us-east-1.elb.amazonaws.com` admits every ELB in it.
/// So read this as **a guard against a typo in a configuration and against an
/// obviously foreign domain**, not as a boundary that holds a credential.
/// `HeavyHosts::Only` — `Client::with_heavy_proxies_in` — is the boundary,
/// because it is a list somebody wrote on purpose.
///
/// The rule itself: the name must **be** the configured host, or sit under the
/// configured host's parent domain — its own name minus the leftmost label,
/// never shortened below two labels. `cluster.example.net` therefore admits
/// `n0132-sas.example.net` and `n0132-sas.cluster.example.net`, and refuses
/// `cluster.example.net.evil.com`. A **bare cluster name** — `YT_PROXY=hume`,
/// which is the commonest spelling there is — has no parent domain, and is
/// matched as a label instead; see [`same_domain`]. An address that is a
/// literal IP admits only itself: an IP has no domain to share.
///
/// A refused name is passed over, and a cluster whose whole answer is refused
/// is treated as one that named nobody — the upload goes to the configured
/// address, which is where it went before there was a lookup at all, and
/// [`crate::observe::declined`] says so once rather than leaving it to be
/// deduced from a cluster error much later.
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
/// Bracketed, it has to hold an **IPv6 literal**: the brackets are what make a
/// literal an authority, and they are not decoration a name may wear. `ureq`
/// 3.3 does not strip them from anything else, so `[n0132.example.com]evil` was
/// accepted here and then failed to resolve for as long as the client lived —
/// every heavy command failing on DNS, the address kept, and the failures
/// repeating without end.
///
/// Unbracketed, a colon introduces a port and a port is digits. That also
/// refuses the bare IPv6 literal, which is the same rule seen from the other
/// side: `2a02:6b8::2` is not a host and a port.
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
/// The domain both must share is the configured host minus its leftmost label,
/// never shortened below two labels — so a client pointed at
/// `cluster.example.net` accepts anything under `example.net`, and one pointed
/// at the two-label `example.net` accepts only `example.net` and what is under
/// it. See [`heavy_base`] for what this rule is worth, which is less than it
/// was once written down as.
///
/// # A bare cluster name has no parent domain
///
/// `YT_PROXY=hume` is not an edge case, it is the ordinary spelling: a cluster
/// name with no dots in it, which `Transport::new` supports on purpose by
/// putting `https://` in front. Such a name has nothing to take a leftmost
/// label off, so the parent-domain rule degenerates to "the name itself" —
/// and then a `/hosts` answering `["n0008-sas.hume.yt.example.net"]`, which is
/// the real shape of a real installation, is refused **entirely and
/// permanently**: the state settles as `Configured`, the lookup is never
/// repeated, and every upload goes back to being refused by a control proxy
/// with nothing anywhere to say why. The same break waits in Kubernetes for
/// anyone who addresses the service by its short name.
///
/// So a configured name with no dot is matched as a **label** of the discovered
/// name, and not as its leftmost one: `hume` admits
/// `n0008-sas.hume.yt.example.net` and `n0008-sas.hume`, and refuses
/// `hume.evil.com` — where the cluster's name has been put in the position a
/// *host* name occupies rather than the position a domain does.
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
/// The plain suffix rule, and deliberately not [`same_domain`]'s: there the
/// domain has to be *derived* from a host name, and the leftmost-label and
/// bare-label cases exist because `YT_PROXY` is a host and not a domain. Here
/// the caller wrote a domain down, so it is used as one — `proxy-zone.net`
/// admits `n0132-sas.rack7.proxy-zone.net` and itself, and nothing
/// else.
///
/// `domain` is already trimmed, lowercased and stripped of stray dots by
/// [`Transport::set_heavy_proxies_under`], and is never empty.
///
/// This does not make the rule a boundary — the suffix caveat in [`heavy_base`]
/// applies to a domain somebody typed exactly as it applies to one that was
/// derived, and `HeavyHosts::Only` is still the version that is a boundary. What
/// it does is stop "the rule missed by one label" from having to be answered by
/// removing the rule.
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
/// Userinfo comes off, which matters twice: it is where a password would be,
/// and leaving it on would make [`port_of`] read `pass@host:8000` and find no
/// port at all.
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
/// One short clause per state, for [`refusal_hint`] to hang on the cluster's
/// own refusal. Each names the builder that changes the answer, because the
/// refusal itself names nothing: an operator reading `Control proxy may not
/// serve heavy requests with input data` has no way to learn from it that this
/// client asked `/hosts`, got a perfectly good name and declined it.
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
/// Only for that one refusal, which is the only failure here that is about
/// *which proxy was asked* — see [`CONTROL_REFUSAL`]. Everything else is about
/// the request, and a hint about routing beside it would be noise.
///
/// Appended to the message rather than wrapped in a new variant: the caller
/// wants the cluster's own words *and* the one fact the cluster cannot know,
/// and a second error type would make the first harder to match on for the sake
/// of the second.
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
/// `ureq`'s own `limit()` counts what arrives on the wire; this counts what
/// comes out of the decoder, which is what is held. [`RESPONSE_LIMIT`] has the
/// measurement that makes the difference a factor of a thousand rather than a
/// technicality.
///
/// It reads one byte past what is left, so an overrun is *visible* without
/// being kept, and fails with the same `ureq::Error::BodyExceedsLimit` the
/// wire limit raises — so both arrive at [`body_failure`] as one case with one
/// message.
///
/// **A body of exactly `limit` decoded bytes passes**, which is the other half
/// of what this fixes. `ureq`'s `LimitReader` errors on the next `read` once
/// its budget reaches zero, and `read_to_end` always makes that read to find
/// the end, so the cap it enforced was "at least the limit" while the error
/// said "larger than" it. The wire backstop has to leave room for that same
/// body compressed, which is not the same number — see [`wire_budget`].
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
        // One byte more than may be kept: enough to tell "ended exactly at the
        // cap" from "ran past it" without a probe read of its own, and the
        // byte itself is never returned to the caller.
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
/// The backstop, and it is not optional: [`CapReader`] counts what comes *out*
/// of the decoder, so a stream that decodes to nothing never spends against it.
/// An endless chunked body of empty deflate stored blocks — `00 00 00 ff ff`,
/// repeated — makes `flate2` loop inside a single `read` producing no output,
/// so `CapReader::read` is never re-entered and `left` never moves. With the
/// wire limit removed, that read does not come back;
/// `an_endless_body_that_decodes_to_nothing_is_still_bounded` is it with a
/// deadline on it. `ureq`'s own limit sits underneath the decoder and counts
/// transferred bytes, which is exactly the quantity such a stream does spend.
///
/// It cannot be `limit`, or even `limit + 1`. Deflate **expands** what it
/// cannot compress: a body of exactly `limit` decoded bytes is the largest this
/// client is documented to hand back, and gzipped it crosses the wire *larger*
/// than that. Measured with `flate2` at 4 096 incompressible bytes: **4 119**
/// on the wire, which a budget of 4 097 refuses — a response inside the
/// documented ceiling turned away by a guard that exists to catch responses
/// outside it.
///
/// So the budget is zlib's own `deflateBound` — `n + n/8 + n/64 + 5`, its bound
/// for a deflate stream that compresses nothing — with 64 bytes covering the
/// gzip wrapper's header and trailer (18) and the two bytes of rounding this
/// gives up by shifting rather than dividing. At [`RESPONSE_LIMIT`] that is
/// 612 368 448 wire bytes for 536 870 912 held, so the backstop still bounds
/// the pathological stream at about 584 MiB of transfer.
///
/// It bounds a *conformant* encoder. One that expands past `deflateBound` — a
/// dynamic Huffman block per byte, which nothing writes by accident — is
/// refused, and refusing is the safe direction to be wrong in.
fn wire_budget(limit: u64) -> u64 {
    limit
        .saturating_add(limit >> 3)
        .saturating_add(limit >> 6)
        .saturating_add(64)
}

/// Reads a buffered response body, capped at [`RESPONSE_LIMIT`]'s worth of
/// *decoded* bytes.
///
/// Two guards, counting two different things. [`CapReader`] is the cap the
/// caller is promised, and it sits above the decoder; the limit `ureq` is given
/// is the backstop underneath it, and [`wire_budget`] is why the second is not
/// simply the first.
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
/// Two failures arrive down the same road and mean opposite things.
///
/// A connection cut while the body streams in is the same network failure as
/// one cut a packet earlier, so it stays a [`ClientError::Transport`]: worth
/// waiting and repeating where the command allows it, and — for a heavy
/// command — a fair reason to drop the host it went to.
///
/// A body that ran past the cap is neither, and left as a `Transport` it would
/// be read as both. `ureq` reports it as `Error::BodyExceedsLimit`, which is
/// not an `Io` error, so every predicate that narrows `Transport` by looking
/// inside — [`crate::retry::is_retriable`], and through it
/// [`crate::retry::worth_asking_again`] and
/// [`crate::retry::attributable_to_the_host`] — answers `true` for it. The
/// consequences are not this caller's alone: an over-cap
/// [`Client::read_file`](crate::Client::read_file) would fail and take a
/// **healthy** data proxy out of the pool for it; enough of them empty the
/// pool, and the fallback window then answers unrelated *writes* with the
/// control-proxy refusal [#30] exists to prevent. Nothing about that is the
/// host's doing — it served the request perfectly — and no amount of waiting
/// shrinks the file.
///
/// So it becomes a [`ClientError::ResponseTooLarge`]: settled, and about the
/// request rather than the addressee. Never retried, never blamed on a host.
///
/// `limit` rather than the number `ureq` carries in `BodyExceedsLimit`,
/// because the two are not the same: what `ureq` is asked to enforce is a
/// transferred-byte backstop above the memory cap (see [`wire_budget`]), and
/// the cap the caller needs told is this one.
///
/// [#30]: https://github.com/sshaplygin/ytsaurus-rs/issues/30
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
/// `write_table: transport error: io: Connection refused` is a true report
/// about an address that appears nowhere in the caller's own code: the client
/// chose it, from a list the cluster gave it, and then said nothing about the
/// choice. The same misdirection as an error that blames a token for a host it
/// was never sent to.
///
/// Only for a command that was routed — a failure at the configured address
/// needs no explaining, because that is the address the caller typed.
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
        // `ResponseTooLarge` carries a command too, and is deliberately *not*
        // qualified — which is why it is written out here rather than left to
        // fall through. Its message offers the streaming half of the same
        // command, and `error::streaming_advice` finds that half by matching
        // the command name **exactly**: `read_file at n0132-sas.example.net`
        // matches nothing, and the sentence saying what to do instead
        // disappears. The host is not worth naming in any case — this failure
        // is about the size of the answer, and the answer is exactly as large
        // at the next proxy along. `a_response_too_large_keeps_the_way_past_it`
        // fails if this arm is deleted.
        error @ ClientError::ResponseTooLarge { .. } => error,
        // Nothing else carries a command at all: an `Io` names a local path, a
        // `Config` names the build, and an `OperationFailed` is the
        // scheduler's verdict rather than one proxy's.
        other => other,
    }
}

/// Whether `base` names a cluster on this machine, or a tunnel to one.
///
/// Such a cluster is not asked where its heavy proxies are, and this is the
/// one place that decision is made. Two reasons, and either would do:
///
/// - a single-node installation has no separate heavy proxies, so the lookup
///   can only cost a round trip before the first upload;
/// - the address a cluster publishes for itself is its own, and from behind a
///   port mapping or an SSH tunnel it is not reachable at all. A local
///   YTsaurus in Docker is reached at `localhost:8000` and knows itself by the
///   container's address and port — following that would send every upload
///   somewhere this process cannot go.
///
/// So the default is "ask, unless the address says it cannot help", and
/// `Client::with_proxy_discovery` overrides it in both directions.
fn is_local(base: &str) -> bool {
    let host = host_of(base);

    if let Ok(address) = host.parse::<std::net::IpAddr>() {
        // `is_unspecified` covers `0.0.0.0`, which is not loopback but is
        // nobody else's address either.
        return address.is_loopback() || address.is_unspecified();
    }

    host.eq_ignore_ascii_case("localhost")
}

/// The host out of a base URL, without scheme, port or path.
///
/// `http://[::1]:8000` is why this is not a `split(':')`: an IPv6 literal is
/// bracketed and full of colons.
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
        // From #36: this client follows redirects itself, in `Transport::redirect`,
        // so `ureq` must hand the 3xx back rather than chase it. Not "fail on a
        // redirect" — the 3xx becomes an ordinary response for that code to read.
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
        // The four rows of the table in #30, each measured against the client
        // before this check existed. The `/hosts` body decides where every
        // heavy command goes, and a heavy command carries the caller's OAuth
        // token — so on a plain-http base, forging this body is exactly as easy
        // as forging a `Location` header, which this client already refuses to
        // follow.

        // 1. The scheme downgrade. `http://n0132` from an `https://` client
        //    used to strip TLS and put the token on the wire in cleartext.
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
        // what the blank-name filter used to do on its own — and what makes the
        // empty entries above empty.
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
        // again — leaving the operator with the cluster error from #30 and
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
        // And against the address a heavy command would actually be dialled at
        // (#38): a discovered https:// heavy proxy is refused by the same
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
        // The regression #40 is about, in the one place it can be pinned
        // without a TLS listener presenting a bad certificate. A cert rejected
        // `NotValidForName` is a per-host verdict — the cluster's other
        // proxies present names that match — but it is deliberately not
        // retriable and not `worth_asking_again`, so a drop gated on either
        // predicate (as the first routing release gated it) leaves the client
        // pinned to the one bad host: not stepped past, not re-resolved,
        // failing every heavy command until the window elapses and the same
        // ordered-first host comes back. Dropping must not need the lookup's
        // predicate to agree.
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
        // #30 arriving from a caller who only asked for a large file.
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
