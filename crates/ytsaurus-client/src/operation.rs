//! The operation object, and the filters and parameters its commands take.
//!
//! An operation used to be a `String` here, and every command took one. That is
//! still true — [`Client`] carries the whole lifecycle over an id — but a string
//! is a poor thing to hand to a function, and it is nothing to *reattach* to.
//! [`Operation`] is the handle: a client and an id, with the same commands on
//! it, obtained either from an id you just started or from one you persisted
//! before the process died.
//!
//! ```no_run
//! # use ytsaurus_client::{Client, VanillaSpec, VanillaTask};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = Client::from_env()?;
//! # let spec = VanillaSpec::new(VanillaTask::new("t", "sleep 60", 1));
//! let id = client.start_vanilla(&spec)?;
//! std::fs::write("run.id", &id)?;          // survive a restart
//!
//! // …later, in a process that did not start it:
//! let op = client.attach_operation(std::fs::read_to_string("run.id")?);
//! op.suspend(false)?;
//! op.resume()?;
//! op.wait()?;
//! # Ok(())
//! # }
//! ```
//!
//! # What the cluster says about the lifecycle
//!
//! Measured on a local cluster, because none of it is obvious:
//!
//! - **Suspension is not a state.** A suspended operation still reports
//!   `running`; `suspended` is a separate attribute, which is what
//!   [`Operation::suspended`] reads — and [`Operation::status`] reads beside
//!   the state, in one request, because a poll loop needs both. Polling the
//!   state alone will never tell you an operation is paused.
//! - **Suspend is idempotent, resume is not.** Suspending a suspended operation
//!   answers `{}`; resuming one that is not suspended fails with code 201,
//!   `Operation is in "running" state`.
//! - **Complete is not idempotent**, and behaves like
//!   [`Client::abort_operation`]: the second one is answered `No such
//!   operation`.
//! - Once the scheduler has let the operation go, *every* one of these answers
//!   `No such operation` — the rule is "the scheduler still has it", not "it has
//!   not finished".

use ytsaurus_yson::{YsonNode, YsonValue};

use crate::error::{ClientError, Result};
use crate::jobs::{JobInfo, field, text};
use crate::stream::ResponseReader;
use crate::{Client, yson_build};

/// A running — or finished — operation, and the client that can ask about it.
///
/// Obtained from [`Client::attach_operation`]. Every method is the [`Client`]
/// method of the same name with the id filled in, so nothing here can be done
/// only through the handle; the handle exists so that an operation can be
/// *passed around* as one thing, and so that reattaching to one has an obvious
/// spelling.
///
/// **Dropping it does nothing**, which is the opposite of
/// [`Transaction`](crate::Transaction). A transaction that loses its handle is
/// aborted, because a transaction is a scope. An operation is meant to outlive
/// the process that started it — that is what makes reattaching worth having —
/// so this handle is a name and not a lease.
#[derive(Debug, Clone)]
pub struct Operation {
    client: Client,
    id: String,
}

impl Operation {
    pub(crate) fn new(client: Client, id: String) -> Self {
        Self { client, id }
    }

    /// The operation's ID, as the cluster named it.
    ///
    /// The thing to persist: it is what the web interface shows, and what
    /// [`Client::attach_operation`] needs to build this handle again.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The client this handle sends its commands through.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The whole operation document. See [`Client::get_operation`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn get(&self, attributes: &[&str]) -> Result<YsonValue> {
        self.client.get_operation(&self.id, attributes)
    }

    /// The current state, e.g. `running` or `completed`.
    ///
    /// Note that a **suspended operation still reports `running`**; ask
    /// [`Operation::suspended`] about that, or [`Operation::status`] about
    /// both at once.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn state(&self) -> Result<String> {
        self.client.operation_state(&self.id)
    }

    /// Whether the operation is suspended. See [`Client::operation_suspended`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn suspended(&self) -> Result<bool> {
        self.client.operation_suspended(&self.id)
    }

    /// The state and the suspension together. See [`Client::operation_status`].
    ///
    /// What a poll loop wants: the two are useless apart, and this asks for
    /// them in one request.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn status(&self) -> Result<OperationStatus> {
        self.client.operation_status(&self.id)
    }

    /// Polls until the operation finishes. See [`Client::wait_for_operation`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::OperationFailed`](crate::ClientError::OperationFailed)
    /// if it ends as anything other than `completed`.
    pub fn wait(&self) -> Result<()> {
        self.client.wait_for_operation(&self.id)
    }

    /// Stops the operation. See [`Client::abort_operation`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn abort(&self, reason: Option<&str>) -> Result<()> {
        self.client.abort_operation(&self.id, reason)
    }

    /// Pauses the operation. See [`Client::suspend_operation`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn suspend(&self, abort_running_jobs: bool) -> Result<()> {
        self.client.suspend_operation(&self.id, abort_running_jobs)
    }

    /// Lets a suspended operation run again. See [`Client::resume_operation`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails,
    /// including when the operation was not suspended.
    pub fn resume(&self) -> Result<()> {
        self.client.resume_operation(&self.id)
    }

    /// Finishes the operation with what it has. See
    /// [`Client::complete_operation`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn complete(&self) -> Result<()> {
        self.client.complete_operation(&self.id)
    }

    /// Changes the operation's scheduling parameters while it runs. See
    /// [`Client::update_operation_parameters`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails, or
    /// [`ClientError::Config`](crate::ClientError::Config) if `parameters` is
    /// empty.
    pub fn update_parameters(&self, parameters: &OperationParameters) -> Result<()> {
        self.client
            .update_operation_parameters(&self.id, parameters)
    }

    /// Why the operation ended as it did. See
    /// [`Client::operation_result_error`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn error(&self) -> Result<Option<String>> {
        self.client.operation_result_error(&self.id)
    }

    /// The operation's jobs. See [`Client::list_jobs`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn jobs(&self, state: Option<&str>, limit: u32) -> Result<Vec<JobInfo>> {
        self.client.list_jobs(&self.id, state, limit)
    }

    /// One job of the operation. See [`Client::get_job`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn job(&self, job_id: &str) -> Result<JobInfo> {
        self.client.get_job(&self.id, job_id)
    }

    /// What a job read. See [`Client::get_job_input`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn job_input(&self, job_id: &str) -> Result<ResponseReader> {
        self.client.get_job_input(&self.id, job_id)
    }

    /// What a job wrote to stderr. See [`Client::get_job_stderr`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn job_stderr(&self, job_id: &str) -> Result<Vec<u8>> {
        self.client.get_job_stderr(&self.id, job_id)
    }

    /// The operation's event log. See [`Client::list_operation_events`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn events(&self) -> Result<Vec<OperationEvent>> {
        self.client.list_operation_events(&self.id)
    }

    /// Everything the scheduler recorded about the jobs. See
    /// [`Client::job_statistics`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn statistics(&self) -> Result<YsonValue> {
        self.client.job_statistics(&self.id)
    }

    /// The statistics the jobs reported themselves. See
    /// [`Client::custom_statistics`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn custom_statistics(&self) -> Result<YsonValue> {
        self.client.custom_statistics(&self.id)
    }

    /// The total of one custom statistic. See [`Client::statistic_sum`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn statistic_sum(&self, name: &str) -> Result<Option<i64>> {
        self.client.statistic_sum(&self.id, name)
    }

    /// The total of one built-in statistic. See
    /// [`Client::job_statistic_sum`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`](crate::ClientError) if the request fails.
    pub fn job_statistic_sum(&self, path: &str) -> Result<Option<i64>> {
        self.client.job_statistic_sum(&self.id, path)
    }
}

/// One operation, as [`Client::list_operations`] reports it.
///
/// A subset of the cluster's `TOperation`, in the spirit of [`JobInfo`]: enough
/// to recognise an operation and decide what to do about it. The document has a
/// great deal more in it — `brief_spec`, `runtime_parameters`, the whole
/// progress tree — and [`Client::get_operation`] is how to read that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationInfo {
    /// Operation ID, in the form every other command here expects.
    pub id: String,
    /// The operation type — `map`, `vanilla`, `sort`, … — under the cluster's
    /// own key `type`, which is a keyword in Rust.
    pub kind: String,
    /// `running`, `completed`, `failed`, `aborted`, `pending`, …
    pub state: String,
    /// Who started it.
    pub user: Option<String>,
    /// When it started, in the cluster's ISO 8601 spelling.
    pub start_time: Option<String>,
    /// When it finished; `None` while it has not.
    pub finish_time: Option<String>,
    /// Whether it is paused.
    ///
    /// Worth having beside `state`, because a suspended operation still reports
    /// `running` there.
    pub suspended: bool,
}

/// What an operation is doing, as [`Client::operation_status`] reports it.
///
/// The pair rather than either alone: suspension is not a state, so `state`
/// says `running` for an operation that is paused and `suspended` is the only
/// thing that says otherwise. Reading them together also costs one request
/// instead of two, which a poll loop notices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationStatus {
    /// `running`, `completed`, `failed`, `aborted`, `pending`, …
    pub state: String,
    /// Whether it is paused. Never visible in `state`.
    pub suspended: bool,
}

/// The answer to [`Client::list_operations`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationList {
    /// The operations the filter matched.
    pub operations: Vec<OperationInfo>,
    /// Whether the cluster had more to say than the limit allowed.
    ///
    /// The cluster's own `incomplete`, and not an error: the way to page
    /// through a long list is to move the filter's time window, so a caller
    /// that ignores this silently sees only the first page.
    pub incomplete: bool,
}

/// One entry of an operation's event log, as `list_operation_events` reports it.
///
/// **A cluster with no operations archive has none of these.** The command is
/// registered and answers with an empty list, which is what a local cluster
/// does; the archive is what actually keeps the events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationEvent {
    /// What happened, e.g. `started_running` or `incarnation_started`.
    pub event_type: String,
    /// When, in the cluster's ISO 8601 spelling.
    pub timestamp: Option<String>,
    /// The incarnation this event belongs to, for an operation that has been
    /// restarted by the controller agent.
    pub incarnation: Option<String>,
}

/// Which operations [`Client::list_operations`] should return.
///
/// Every filter is optional and they combine; the default asks for everything
/// the cluster is willing to answer with, which is the most recent operations up
/// to its own limit.
///
/// ```
/// use ytsaurus_client::OperationFilter;
///
/// let mine = OperationFilter::new()
///     .with_user("robot-loader")
///     .with_state("running")
///     .with_limit(20);
/// ```
#[derive(Debug, Clone)]
pub struct OperationFilter {
    params: YsonValue,
}

impl Default for OperationFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationFilter {
    /// No filter at all.
    #[must_use]
    pub fn new() -> Self {
        Self {
            params: yson_build::empty_map(),
        }
    }

    fn set(mut self, key: &str, value: YsonValue) -> Self {
        yson_build::insert(&mut self.params, key, value);
        self
    }

    /// Only operations started by this user.
    #[must_use]
    pub fn with_user(self, user: impl AsRef<str>) -> Self {
        self.set("user", yson_build::string(user.as_ref()))
    }

    /// Only operations in this state — `running`, `completed`, `failed`, …
    #[must_use]
    pub fn with_state(self, state: impl AsRef<str>) -> Self {
        self.set("state", yson_build::string(state.as_ref()))
    }

    /// Only operations of this type.
    #[must_use]
    pub fn with_kind(self, kind: crate::OperationType) -> Self {
        self.set("type", yson_build::string(kind.as_str()))
    }

    /// Only operations in this pool.
    #[must_use]
    pub fn with_pool(self, pool: impl AsRef<str>) -> Self {
        self.set("pool", yson_build::string(pool.as_ref()))
    }

    /// Only operations in this pool tree.
    #[must_use]
    pub fn with_pool_tree(self, tree: impl AsRef<str>) -> Self {
        self.set("pool_tree", yson_build::string(tree.as_ref()))
    }

    /// Only operations whose id, alias, user or spec contains this text.
    ///
    /// The cluster calls it `filter`; this is the free-text search the web
    /// interface's search box sends.
    #[must_use]
    pub fn with_substring(self, text: impl AsRef<str>) -> Self {
        self.set("filter", yson_build::string(text.as_ref()))
    }

    /// Only operations that started at or after this time.
    ///
    /// An ISO 8601 timestamp as the cluster writes them —
    /// `2026-08-06T09:21:23.534387Z`. This crate has no date type and does not
    /// want a dependency on one, so the timestamps go across as text, exactly as
    /// they come back in [`OperationInfo::start_time`].
    #[must_use]
    pub fn with_from_time(self, time: impl AsRef<str>) -> Self {
        self.set("from_time", yson_build::string(time.as_ref()))
    }

    /// Only operations that started at or before this time.
    ///
    /// See [`OperationFilter::with_from_time`] for the spelling.
    #[must_use]
    pub fn with_to_time(self, time: impl AsRef<str>) -> Self {
        self.set("to_time", yson_build::string(time.as_ref()))
    }

    /// Only operations that have failed jobs.
    #[must_use]
    pub fn with_failed_jobs(self, with_failed_jobs: bool) -> Self {
        self.set("with_failed_jobs", yson_build::boolean(with_failed_jobs))
    }

    /// Also look in the operations archive, not only at what the scheduler
    /// still holds.
    ///
    /// This is how an operation that finished a while ago is found at all — and
    /// it needs an archive, which a local cluster does not have.
    #[must_use]
    pub fn with_archive(self, include: bool) -> Self {
        self.set("include_archive", yson_build::boolean(include))
    }

    /// At most this many operations.
    #[must_use]
    pub fn with_limit(self, limit: u32) -> Self {
        self.set("limit", yson_build::int(i64::from(limit)))
    }

    /// Sets any filter this builder does not model — `cursor_time`,
    /// `cursor_direction`, `include_counters`.
    #[must_use]
    pub fn with_raw(self, key: impl AsRef<str>, value: YsonValue) -> Self {
        self.set(key.as_ref(), value)
    }

    /// The filter as `list_operations` wants it.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        self.params.clone()
    }
}

/// What [`Client::update_operation_parameters`] should change.
///
/// The parameters a **running** operation will accept: which pool it competes
/// in, and how much of that pool it gets. Everything else about an operation is
/// fixed when it starts.
///
/// ```
/// use ytsaurus_client::OperationParameters;
///
/// // Move a job that turned out to matter into the pool that gets served
/// // first, and give it twice the share.
/// let urgent = OperationParameters::new().with_pool("interactive").with_weight(2.0);
/// ```
#[derive(Debug, Clone)]
pub struct OperationParameters {
    params: YsonValue,
}

impl Default for OperationParameters {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationParameters {
    /// Changes nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            params: yson_build::empty_map(),
        }
    }

    fn set(mut self, key: &str, value: YsonValue) -> Self {
        yson_build::insert(&mut self.params, key, value);
        self
    }

    /// Moves the operation into another pool.
    ///
    /// Applies to every pool tree the operation runs in. Verified on a local
    /// cluster: a top-level key here lands under
    /// `runtime_parameters/scheduling_options_per_pool_tree/<tree>`, once per
    /// tree. [`OperationParameters::with_pool_in_tree`] names one instead.
    #[must_use]
    pub fn with_pool(self, pool: impl AsRef<str>) -> Self {
        self.set("pool", yson_build::string(pool.as_ref()))
    }

    /// Changes the operation's share of its pool.
    ///
    /// A double, and the cluster means it: `1.0` is the default share, `2.0` is
    /// twice as much of whatever the pool gets.
    #[must_use]
    pub fn with_weight(self, weight: f64) -> Self {
        self.set("weight", yson_build::double(weight))
    }

    /// Moves the operation into another pool **of one tree**.
    ///
    /// For an installation with more than one pool tree, where the operation
    /// should move in one of them and stay where it is in the others.
    ///
    /// Adds to what the tree's entry already holds rather than replacing it:
    /// `update_operation_parameters` assigns, so an entry that arrived here
    /// with a `weight` in it and left with only a `pool` would reset that
    /// weight on the cluster and report success.
    #[must_use]
    pub fn with_pool_in_tree(mut self, tree: impl AsRef<str>, pool: impl AsRef<str>) -> Self {
        let mut trees = map_or_empty(tree_options(&self.params));
        let mut options = map_or_empty(field(&trees, tree.as_ref()));

        yson_build::insert(&mut options, "pool", yson_build::string(pool.as_ref()));
        yson_build::insert(&mut trees, tree.as_ref(), options);
        yson_build::insert(&mut self.params, "scheduling_options_per_pool_tree", trees);
        self
    }

    /// Sets any parameter this builder does not model — `acl`, `annotations`,
    /// `scheduling_tag_filter`.
    #[must_use]
    pub fn with_raw(self, key: impl AsRef<str>, value: YsonValue) -> Self {
        self.set(key.as_ref(), value)
    }

    /// Whether this would ask the cluster to change nothing.
    ///
    /// [`Client::update_operation_parameters`] refuses an empty update: the
    /// cluster answers 200 and does nothing, so the mistake is invisible where
    /// it is made.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match &self.params.node {
            YsonNode::Map(m) => m.is_empty(),
            _ => true,
        }
    }

    /// The parameters as `update_operation_parameters` wants them.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        self.params.clone()
    }
}

/// The `scheduling_options_per_pool_tree` already in a parameters document.
fn tree_options(params: &YsonValue) -> Option<&YsonValue> {
    field(params, "scheduling_options_per_pool_tree")
}

/// A value if it is a dict, and a fresh empty dict otherwise.
///
/// What lets a builder read back and add to what is already under a key without
/// trusting its shape: [`yson_build::insert`] panics on anything that is not a
/// dict, and `with_raw` puts whatever the caller passes wherever they name.
fn map_or_empty(value: Option<&YsonValue>) -> YsonValue {
    match value {
        Some(existing) if matches!(existing.node, YsonNode::Map(_)) => existing.clone(),
        _ => yson_build::empty_map(),
    }
}

/// Reads the `operations` list of a `list_operations` response.
///
/// An operation with no id is dropped, as a job with no id is: there is nothing
/// to ask the cluster about it afterwards. A response with no `operations` list
/// at all is an error rather than an empty list — "the cluster has no operations
/// running" is a conclusion a supervisor acts on, and a shape it does not
/// recognise must not be able to say that.
pub(crate) fn parse_operations(response: &YsonValue) -> Result<OperationList> {
    let Some(YsonNode::List(items)) = field(response, "operations").map(|ops| &ops.node) else {
        return Err(ClientError::Decode {
            command: "list_operations".to_owned(),
            reason: format!(
                "the answer carries no `operations` list: {}",
                // Truncated: a listing is large, and a wrong-shaped one is no
                // smaller for being wrong.
                crate::error::truncate(&format!("{:?}", response.node), 300)
            ),
        });
    };

    Ok(OperationList {
        operations: items.iter().filter_map(parse_operation).collect(),
        incomplete: flag(field(response, "incomplete")).unwrap_or(false),
    })
}

fn parse_operation(operation: &YsonValue) -> Option<OperationInfo> {
    let id = text(field(operation, "id")?)?;

    Some(OperationInfo {
        id,
        // `type` is the documented key; `operation_type` is the same value
        // under the name API v4 also answers with.
        kind: field(operation, "type")
            .and_then(text)
            // Decoded before falling back, not merely present: a `type` that is
            // there but unreadable must still let `operation_type` answer.
            .or_else(|| field(operation, "operation_type").and_then(text))
            .unwrap_or_default(),
        state: field(operation, "state").and_then(text).unwrap_or_default(),
        user: field(operation, "authenticated_user").and_then(text),
        start_time: field(operation, "start_time").and_then(text),
        finish_time: field(operation, "finish_time").and_then(text),
        suspended: flag(field(operation, "suspended")).unwrap_or(false),
    })
}

/// Reads a `list_operation_events` response.
///
/// The answer is a **bare list**, with none of the one-key envelope the rest of
/// API v4 wraps a structured response in — verified against a cluster, which is
/// the only way anyone would know. That cluster has no operations archive and
/// so answers `[]` every time, which means only the *empty* bare list was ever
/// seen; the `{events=[…]}` envelope is accepted too, rather than betting the
/// whole command on which of the two an installation with an archive sends.
///
/// Anything else is an error. An empty list is a legitimate answer here — it is
/// what a cluster with no archive gives — so a shape this does not recognise
/// must not be able to spell itself "no events".
pub(crate) fn parse_events(response: &YsonValue) -> Result<Vec<OperationEvent>> {
    let items = match &response.node {
        YsonNode::List(items) => items,
        _ => match field(response, "events").map(|events| &events.node) {
            Some(YsonNode::List(items)) => items,
            _ => {
                return Err(ClientError::Decode {
                    command: "list_operation_events".to_owned(),
                    reason: format!(
                        "expected a list of events, or a dict holding one under \
                         `events`: {}",
                        crate::error::truncate(&format!("{:?}", response.node), 300)
                    ),
                });
            }
        },
    };
    Ok(items.iter().filter_map(parse_event).collect())
}

fn parse_event(event: &YsonValue) -> Option<OperationEvent> {
    Some(OperationEvent {
        event_type: text(field(event, "event_type")?)?,
        timestamp: field(event, "timestamp").and_then(text),
        incarnation: field(event, "incarnation").and_then(text),
    })
}

/// A boolean field, absent-or-not-a-boolean being `None`.
pub(crate) fn flag(value: Option<&YsonValue>) -> Option<bool> {
    match value?.node {
        YsonNode::Boolean(b) => Some(b),
        _ => None,
    }
}

// The narrow readers of a `get_operation` document, as functions of the
// document rather than methods on the client. Each one is a guess about where
// an attribute sits, and a guess a test cannot reach is a guess nobody checks:
// these are what `Client::operation_state` and its kin are, and what the
// fixture test in `lib.rs` runs against a document a cluster actually sent.

/// The `state` of a `get_operation` answer.
pub(crate) fn state_of(document: &YsonValue) -> Result<String> {
    match field(document, "state").map(|state| &state.node) {
        Some(YsonNode::String(bytes)) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        other => Err(ClientError::Decode {
            command: "get_operation".to_owned(),
            reason: format!("state is missing or not a string: {other:?}"),
        }),
    }
}

/// Whether a `get_operation` answer says the operation is paused.
///
/// **Absent is `false`**, which is the honest answer rather than a decode
/// failure: the scheduler reports `suspended` for an operation it still holds,
/// and one resolved out of the operations archive may simply not carry it.
/// [`parse_operation`] reads the same attribute the same way, and the two must
/// not disagree about what absence means. A `suspended` that is *there* and not
/// a boolean is a shape that moved, and does fail.
pub(crate) fn suspended_of(document: &YsonValue) -> Result<bool> {
    match field(document, "suspended") {
        None => Ok(false),
        Some(value) => flag(Some(value)).ok_or_else(|| ClientError::Decode {
            command: "get_operation".to_owned(),
            reason: format!("suspended is not a boolean: {:?}", value.node),
        }),
    }
}

/// Why an operation ended as it did, out of a document holding its `result`.
///
/// `None` for one that succeeded, and for one that has not finished.
pub(crate) fn result_error_of(document: &YsonValue) -> Option<String> {
    let error = field(document, "result").and_then(|result| field(result, "error"))?;

    // An operation that succeeded still has an error document — code 0 with an
    // empty message. Reporting that as `Some("")` would make `if let Some(why)`
    // fire on every success and print nothing, which is the shape of bug that
    // survives review because it looks like it works.
    if field(error, "code").and_then(YsonValue::as_i64) == Some(0) {
        return None;
    }

    crate::jobs::error_summary(error)
}

/// The `job_statistics` subtree of a document holding an operation's `progress`.
///
/// An empty dict when the cluster reports none, which is what an operation that
/// has not run a job yet looks like.
pub(crate) fn statistics_of(document: &YsonValue) -> YsonValue {
    field(document, "progress")
        .and_then(|progress| field(progress, "job_statistics"))
        .cloned()
        .unwrap_or_else(yson_build::empty_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::{YsonFormat, from_slice, to_string};

    fn parse(text: &str) -> YsonValue {
        from_slice(text.as_bytes(), YsonFormat::Text).expect("valid YSON")
    }

    fn rendered(value: &YsonValue) -> String {
        to_string(value, YsonFormat::Text).expect("encodes")
    }

    /// A real `list_operations` response, captured from the local cluster with
    /// one operation running and one already completed.
    const LIST_OPERATIONS: &str = include_str!("../tests/fixtures/list_operations.yson");

    fn operations(text: &str) -> OperationList {
        parse_operations(&parse(text)).expect("a well-formed listing")
    }

    #[test]
    fn reads_a_list_captured_from_a_cluster() {
        let list = operations(LIST_OPERATIONS);

        assert_eq!(list.operations.len(), 2);
        assert!(!list.incomplete);

        let running = &list.operations[0];
        assert_eq!(running.id, "4f5a087b-aac92287-103e8-a74d2331");
        assert_eq!(running.kind, "vanilla");
        assert_eq!(running.state, "running");
        assert_eq!(running.user.as_deref(), Some("root"));
        assert!(running.start_time.is_some());
        assert_eq!(
            running.finish_time, None,
            "an operation that has not finished has no finish time, and that \
             must stay distinguishable from a time of zero"
        );

        let finished = &list.operations[1];
        assert_eq!(finished.state, "completed");
        assert!(finished.finish_time.is_some());
    }

    /// The cluster reports suspension beside the state, not in it — an
    /// operation that is paused still says `running`.
    #[test]
    fn suspension_is_read_from_its_own_field() {
        let list =
            operations(r#"{"operations"=[{"id"="a-b-c-d";"state"="running";"suspended"=%true}]}"#);
        assert_eq!(list.operations[0].state, "running");
        assert!(list.operations[0].suspended);
    }

    #[test]
    fn an_operation_without_an_id_is_dropped() {
        let list = operations(
            r#"{"operations"=[{"state"="running"};{"id"="a-b-c-d"}];"incomplete"=%true}"#,
        );
        assert_eq!(list.operations.len(), 1);
        assert_eq!(list.operations[0].id, "a-b-c-d");
        assert!(list.incomplete, "a truncated listing must say so");
    }

    /// The listing a supervisor reads to decide whether its own operation is
    /// still alive. An answer this cannot read must say so, rather than come
    /// back as "the cluster is idle" and have a duplicate started on the
    /// strength of it.
    #[test]
    fn a_response_without_an_operation_list_is_an_error() {
        assert!(parse_operations(&parse(r#"{"operations"=#}"#)).is_err());
        assert!(parse_operations(&parse(r#""not a dict""#)).is_err());
        assert!(
            parse_operations(&parse(r#"{"operations"=[]}"#)).is_ok(),
            "an empty list is a cluster with nothing running, and stays Ok"
        );
    }

    #[test]
    fn the_type_falls_back_when_it_is_present_but_unreadable() {
        // `or_else` on the field would short-circuit on presence and never
        // reach the fallback, leaving the kind empty for an operation the
        // cluster named perfectly well under its other key.
        let list =
            operations(r#"{"operations"=[{"id"="a-b-c-d";"type"=#;"operation_type"="map"}]}"#);
        assert_eq!(list.operations[0].kind, "map");
    }

    /// The documented `TOperationEvent`: a timestamp, an event type, and the
    /// incarnation fields an operation restarted by its controller agent gets.
    #[test]
    fn reads_the_documented_event_list() {
        let events = parse_events(&parse(
            r#"[
                {"timestamp"="2026-08-06T09:21:23.534387Z";"event_type"="started_running"};
                {"timestamp"="2026-08-06T09:22:00.000000Z";"event_type"="incarnation_started";
                 "incarnation"="8fd0b4a1-…"};
            ]"#,
        ))
        .expect("a bare list is the shape the cluster sent");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "started_running");
        assert_eq!(events[0].incarnation, None);
        assert_eq!(events[1].incarnation.as_deref(), Some("8fd0b4a1-…"));
    }

    /// The bare list is the shape the local cluster answers with, and the only
    /// one anyone here has seen — it has no operations archive, so it is always
    /// empty. An installation that has one may well wrap it, so both are read.
    #[test]
    fn an_enveloped_event_list_is_read_rather_than_dropped() {
        let events = parse_events(&parse(r#"{"events"=[{"event_type"="started_running"}]}"#))
            .expect("the enveloped shape is accepted too");
        assert_eq!(events.len(), 1, "an envelope must not read as no events");
    }

    /// A cluster with no operations archive answers with an empty list rather
    /// than an error, which is what the local one does.
    #[test]
    fn an_empty_event_list_is_not_a_failure() {
        assert!(
            parse_events(&parse("[]"))
                .expect("empty is fine")
                .is_empty()
        );
    }

    /// The one command here whose non-empty shape could not be checked against
    /// a cluster. An answer that is neither shape must be an error: "no events"
    /// is the normal answer, so a wrong shape that reads as one would be
    /// indistinguishable from it forever.
    #[test]
    fn an_event_answer_of_neither_shape_is_an_error() {
        assert!(parse_events(&parse(r#"{"event_list"=[]}"#)).is_err());
        assert!(parse_events(&parse(r#""not a list""#)).is_err());
    }

    /// Compared whole rather than by `contains`: these values are fixed
    /// literals, so their rendering is stable — and the text writer drops the
    /// quotes around a string that looks like an identifier, which is exactly
    /// the sort of thing a `contains` check would let past.
    #[test]
    fn a_filter_renders_the_keys_the_command_expects() {
        let filter = OperationFilter::new()
            .with_user("robot")
            .with_state("running")
            .with_kind(crate::OperationType::Merge)
            .with_limit(7);

        assert_eq!(
            rendered(&filter.to_yson()),
            "{limit=7;state=running;type=merge;user=robot}"
        );
    }

    #[test]
    fn setting_a_filter_twice_replaces_it() {
        let out = rendered(&OperationFilter::new().with_limit(1).with_limit(2).to_yson());
        assert_eq!(out, "{limit=2}");
    }

    #[test]
    fn parameters_render_pool_and_weight() {
        let out = rendered(
            &OperationParameters::new()
                .with_pool("fast")
                .with_weight(2.5)
                .to_yson(),
        );
        assert_eq!(
            out, "{pool=fast;weight=2.5}",
            "a weight is a double, and 2.5 must not arrive as an int: {out}"
        );
    }

    #[test]
    fn a_pool_can_be_set_for_one_tree_at_a_time() {
        let out = rendered(
            &OperationParameters::new()
                .with_pool_in_tree("default", "fast")
                .with_pool_in_tree("gpu", "research")
                .to_yson(),
        );
        assert_eq!(
            out, "{scheduling_options_per_pool_tree={default={pool=fast};gpu={pool=research}}}",
            "the second tree must not replace the first: {out}"
        );
    }

    /// `update_operation_parameters` assigns rather than merges, so an entry
    /// that loses a field here loses it on the cluster too — and is answered
    /// 200. The builder must add to the tree's options rather than replace
    /// them, however they got there.
    #[test]
    fn a_pool_is_added_to_what_the_tree_already_carries() {
        let out = rendered(
            &OperationParameters::new()
                .with_raw(
                    "scheduling_options_per_pool_tree",
                    yson_build::map([(
                        "default",
                        yson_build::map([("weight", yson_build::double(3.0))]),
                    )]),
                )
                .with_pool_in_tree("default", "fast")
                .to_yson(),
        );
        assert_eq!(
            out, "{scheduling_options_per_pool_tree={default={pool=fast;weight=3.0}}}",
            "the weight the caller set must survive: {out}"
        );
    }

    /// `with_raw` is the documented escape hatch and takes any value, so the
    /// builder cannot assume the shape of what it finds under a key. This used
    /// to abort the caller's process inside `yson_build::insert`.
    #[test]
    fn a_tree_option_that_is_not_a_dict_is_replaced_rather_than_panicked_on() {
        let out = rendered(
            &OperationParameters::new()
                .with_raw(
                    "scheduling_options_per_pool_tree",
                    yson_build::string("oops"),
                )
                .with_pool_in_tree("default", "fast")
                .to_yson(),
        );
        assert_eq!(
            out,
            "{scheduling_options_per_pool_tree={default={pool=fast}}}"
        );
    }

    #[test]
    fn an_empty_update_is_recognisable() {
        assert!(OperationParameters::new().is_empty());
        assert!(!OperationParameters::new().with_weight(1.0).is_empty());
    }
}
