//! Several commands in one round trip.
//!
//! The cluster command is `execute_batch`, from the
//! [command reference](https://ytsaurus.tech/docs/en/api/commands#execute_batch):
//! *"Use a single query to execute the set of commands passed in the
//! parameters."* Both official clients batch — C++
//! `IClientBase::CreateBatchRequest()`, Go `Client.NewBatchRequest()` — and a
//! launcher that creates a dozen tables without it makes a dozen round trips.
//!
//! [`BatchRequest`] is the request half; [`Client::execute_batch`] sends one
//! and is where the answer's shape — a `Result` **per part** — is explained.
//!
//! [`Client::execute_batch`]: crate::Client::execute_batch

use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, to_string};

use crate::error::{ClientError, Result};
use crate::retry::Repeatable;
use crate::schema::TableSchema;
use crate::yson_build;

/// The `concurrency` the cluster assumes when none is sent.
///
/// `Default(50)` in the command's own registration —
/// `TExecuteBatchCommand::Register` in
/// [`yt/yt/client/driver/etc_commands.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/etc_commands.cpp)
/// — and the same 50 the C++ SDK falls back to
/// (`options.Concurrency_.GetOrElse(50)` in
/// `yt/cpp/mapreduce/http_client/raw_batch_request.cpp`). Written here because
/// the default **part size** is derived from it, so the number matters even to
/// a caller who never sets either option.
const DEFAULT_CONCURRENCY: i64 = 50;

/// How many parts one HTTP request carries when the caller does not say.
///
/// The C++ SDK's rule, from `TExecuteBatchOptions` in
/// `yt/cpp/mapreduce/interface/client_method_options.h`: *"If not specified it
/// is set to `Concurrency * 5`"* — 250 at the default concurrency. See
/// [`BatchRequest::with_max_part_size`].
const PARTS_PER_CONCURRENCY: usize = 5;

/// Commands the cluster refuses to take as a batch part at all.
///
/// **The rule is the part's data types, not `isHeavy`.** The driver checks the
/// command's registered input and output types and throws
/// `Command %Qv cannot be part of a batch since it has inappropriate output
/// type %Qlv` before any part runs — so one such name fails the *whole*
/// request and costs every other part its answer. Measured against the
/// registry the cluster serves at `GET /api/v4` (190 commands on the local
/// cluster) and confirmed name by name through a real batch: a part is refused
/// when its **output type** is `tabular` or `binary`, or its **input type** is
/// `binary`. That is the list below, and it is 21 names where `isHeavy` is 7.
///
/// `isHeavy` is not merely a smaller list, it is a different one, in both
/// directions. `get_job_spec` is `is_heavy: true` and was **accepted** as a
/// part (it came back as an ordinary per-part error), while `alter_query` and
/// `push_queue_producer` are `is_heavy: false` and are refused. The harm the
/// check exists to prevent is the measured one: `[create x1, select_rows]` was
/// answered HTTP 400 `inappropriate output type "tabular"` — and `x1` was
/// created anyway, so the round trip cost the create its answer and nothing
/// else.
///
/// **A snapshot, and it can only be a snapshot.** These are the names one
/// cluster refused in one measurement; a cluster of another version registers
/// other commands, and one not listed here can still be refused on the wire.
/// [`BatchRequest::raw_with`] says so rather than promising the list is
/// complete. Two nearby refusals are the cluster's too but are *not* here,
/// because they depend on the call and not on the name: a part whose command
/// takes input and is given none fails the whole batch with
/// `Command %Qv requires input` (measured for `insert_rows`, `write_table` and
/// seven more), and an unknown name fails it with `Unknown command %Qv`.
const NOT_A_BATCH_PART: &[&str] = &[
    "alter_query",
    "get_job_fail_context",
    "get_job_input",
    "get_job_stderr",
    "get_job_trace",
    "lookup_rows",
    "pull_consumer",
    "pull_queue",
    "pull_queue_consumer",
    "pull_rows",
    "read_blob_table",
    "read_file",
    "read_journal",
    "read_query_result",
    "read_shuffle_data",
    "read_table",
    "read_table_partition",
    "run_job_shell_command",
    "select_rows",
    "write_file",
    "write_file_fragment",
];

/// How one part may be repeated, which decides how the whole batch may be.
///
/// The whole batch is one HTTP request, so it retries as one — and the safe
/// answer for the envelope is the most cautious answer among its parts. See
/// [`BatchRequest::repeatable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartKind {
    /// `exists`, `get`, `list` — non-mutating, so re-running one is harmless.
    Read,
    /// `create`, `remove`, `set` — mutating commands the **master's** mutation
    /// cache covers, which is what makes a replay of the batch safe: the
    /// driver hands each volatile part a mutation id derived from the batch's
    /// own, and the master answers a marked replay with the first response.
    /// See [`Client::execute_batch`](crate::Client::execute_batch).
    MasterMutation,
    /// A command nobody has classified. It may be mutating somewhere no
    /// mutation cache covers — the scheduler, say — so a batch carrying one is
    /// sent once, exactly as [`Client::raw_command`](crate::Client::raw_command)
    /// is and for the same reason.
    ///
    /// Where a [`BatchRequest::raw`] part lands by default. A caller who knows
    /// the command's registry bits says so with [`BatchRequest::raw_with`],
    /// and the part is then one of the two above — the *retry* class is the
    /// cluster's fact about the command, not a property of how this crate
    /// happened to spell the call.
    Raw,
}

/// What a part's success is keyed by, which is what its answer can be held to.
///
/// **Not the registry's output-type bit.** That bit was the first thing tried
/// here, and under the API version this crate speaks it does not separate
/// anything: the cluster serves its own v4 registry at `GET /api/v4`, and
/// there `remove` and `set` are `output_type: structured` exactly as `create`
/// and `get` are — it is *v3* that registers them `null`
/// (`REGISTER(TRemoveCommand, "remove", Null, Null, …, ApiVersion3)` beside
/// `REGISTER(TRemoveCommand, "remove", Null, Structured, …, ApiVersion4)` in
/// [`driver.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/driver.cpp)).
/// Measured on a local v4 cluster, one part apiece: `create` →
/// `{output={node_id=…}}`, `get` → `{output={value=…}}`, `exists` →
/// `{output={value=%false}}`, `set` → `{output={}}`, `remove` →
/// `{output={}}`. **No modelled command answers a bare `{}` on v4**, and the
/// output-type bit calls `set` and `create` the same thing.
///
/// So the useful fact is finer than the registry's, and it is the one every
/// [`BatchRequest`] method already documents: *which key the success carries*.
/// That is what makes the check bite where it was meant to — a `create` whose
/// answer has no `node_id` is refused, whether it arrived as `{}` or as the
/// `{output={}}` that a v4 cluster really can emit. See [`part_result`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Output {
    /// The success is a value under this key — `node_id` for `create`,
    /// `value` for `exists`, `get` and `list`. An answer without it is a shape
    /// this client refuses rather than reads as a success with nothing in it.
    Keyed(&'static str),
    /// Nothing this crate can hold the answer to, for one of two reasons:
    /// `set` and `remove`, whose v4 success is measurably an empty `output`
    /// and so has no key to check; and a [`BatchRequest::raw`] part, whose
    /// answer only its caller knows the shape of. Both take what comes.
    Unchecked,
}

/// One command inside a batch, in the shape the cluster takes it.
///
/// `{command=…; parameters={…}}` with an optional `input=…`, verified three
/// ways: the [command reference](https://ytsaurus.tech/docs/en/api/commands#execute_batch)
/// spells out all three fields, `TExecuteBatchCommandRequest::Register` in
/// [`yt/yt/client/driver/etc_commands.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/etc_commands.cpp)
/// registers exactly `command`, `parameters` and `input` (the last
/// `.Default()`), and a local cluster took every part this module builds.
/// `input` is where a structured-input command's value goes — `set` is the one
/// modelled here — and the driver encodes it and sets the part's
/// `input_format` itself.
#[derive(Debug, Clone)]
pub(crate) struct BatchPart {
    pub(crate) command: String,
    parameters: YsonValue,
    input: Option<YsonValue>,
    kind: PartKind,
    output: Output,
}

/// Commands batched to be sent in one round trip.
///
/// A launcher that creates a dozen tables one call at a time pays a dozen
/// round trips; batched, it pays one, and gets a dozen answers:
///
/// ```no_run
/// use ytsaurus_client::{BatchRequest, Client};
///
/// # fn main() -> Result<(), ytsaurus_client::ClientError> {
/// # let client = Client::from_env()?;
/// let mut batch = BatchRequest::new();
/// for name in ["clicks", "visits", "errors"] {
///     batch.create("table", &format!("//tmp/pipeline/{name}"));
/// }
///
/// for (name, made) in ["clicks", "visits", "errors"]
///     .iter()
///     .zip(client.execute_batch(&batch)?)
/// {
///     match made {
///         Ok(_) => {}
///         Err(error) => eprintln!("{name}: {error}"),
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// # The building shape, and why it is a builder
///
/// A batch could equally have been a slice of prepared commands. It is a
/// builder with a typed method per modelled command because the parts are not
/// free-form: the cluster refuses a command whose output type is a data stream
/// (the [command reference](https://ytsaurus.tech/docs/en/api/commands#execute_batch)
/// puts it as *light, with `null` or `structured` input and output*, which
/// measurably over-states it — `get_job_spec` is heavy and is taken, and
/// `write_table` has tabular input and is taken; see `NOT_A_BATCH_PART`),
/// and — the half a slice cannot answer — the
/// **retry** of the whole batch turns on what the parts are. A typed method
/// knows its command is a master-side Cypress command, so the batch stays
/// retriable under a mutation id; [`BatchRequest::raw`] cannot know, so it
/// makes the batch send-once. A slice of prepared commands would have had to
/// assume one answer for everything, and the safe assumption would have taken
/// the retry away from the common case. Each typed method sends **exactly**
/// the parameters its [`Client`](crate::Client) namesake sends, so a call
/// moved into a batch does not change meaning.
///
/// # Parts run in parallel
///
/// From the same reference: *"The command can (and will be) executed in
/// parallel. It means that if a set includes both writing to and reading from
/// the node, the reading result can either be the older value or the updated
/// one."* Watched happening on a local cluster: a batch that created
/// `//tmp/impl-batch-a` and asked `exists` about it in the same breath was
/// answered `%false` — both parts succeeded, in order, and the read simply ran
/// first. Do not put a part and its consequence in one batch.
///
/// # Options
///
/// [`BatchRequest::with_concurrency`] is the server-side parallelism, and
/// [`BatchRequest::with_max_part_size`] is a client-side split into several
/// requests — the same pair the C++ client exposes as
/// `TExecuteBatchOptions{Concurrency, BatchPartMaxSize}`.
///
/// **The two option setters take `self`, and the part adders take `&mut self`.**
/// They are different jobs and the shapes say so: the options are the request's
/// settings, chosen once and up front, so they chain off the constructor and
/// are gone by the time the batch has a name; the adders are the contents,
/// added in a loop, so they hand the borrow straight back. Set the options
/// first and the mix never shows:
///
/// ```
/// # use ytsaurus_client::BatchRequest;
/// let mut batch = BatchRequest::new().with_concurrency(8).with_max_part_size(64);
/// for index in 0..3 {
///     batch.create("table", &format!("//tmp/pipeline/t{index}"));
/// }
/// assert_eq!(batch.len(), 3);
/// ```
///
/// # Executing one twice sends everything twice
///
/// [`Client::execute_batch`](crate::Client::execute_batch) borrows the batch,
/// so it is still there afterwards and can be sent again — and doing so is
/// **new work, not a replay**. The parts are unchanged, but each execution
/// mints its own mutation ids, so the cluster has nothing to deduplicate
/// against and runs every part a second time. What that looks like is the
/// part's own business, and measured: a batch of [`BatchRequest::create_table`]
/// answers `501 already exists` throughout the second run, a batch of
/// [`BatchRequest::remove`] answers `500`, and a batch of
/// [`BatchRequest::create`] answers **with the same node ids as the first run**
/// — because `create` sends `ignore_existing`, not because anything was
/// deduplicated. Do not read that last one as a replay: an unchanged answer
/// from a second execution is the least informative signal here, which is why
/// a real replay wants [`Client::execute_batch_with`](crate::Client::execute_batch_with)
/// and a part that has no `ignore_existing` in it.
/// The reuse worth having is a batch of reads, or one
/// rebuilt from [`BatchRequest::new`] for the second pass. A *replay* — the
/// same mutation deduplicated against the first send — is
/// [`Client::execute_batch_with`](crate::Client::execute_batch_with) with the
/// id you kept.
#[derive(Debug, Clone, Default)]
pub struct BatchRequest {
    parts: Vec<BatchPart>,
    concurrency: Option<i64>,
    max_part_size: Option<usize>,
}

impl BatchRequest {
    /// An empty batch. Add parts with the typed methods, then hand it to
    /// [`Client::execute_batch`](crate::Client::execute_batch).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Caps how many parts the cluster works on at once.
    ///
    /// A parameter of the command itself — `concurrency`, default 50, refused
    /// unless positive (`TExecuteBatchCommand::Register` in the cluster's
    /// [driver](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/etc_commands.cpp);
    /// the [reference](https://ytsaurus.tech/docs/en/api/commands#execute_batch)
    /// documents both). The documentation's reason to lower it: *"Use this
    /// parameter to avoid exhausting your request rate limit."* Left unset,
    /// nothing is sent and the cluster's own default applies.
    ///
    /// Zero is clamped to one, as [`RetryPolicy::new`](crate::RetryPolicy::new)
    /// clamps attempts: the cluster refuses `concurrency=0` outright, and a
    /// builder that quietly built a refused request would fail at the wrong
    /// end.
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = Some(i64::from(concurrency.max(1)));
        self
    }

    /// Caps how many parts travel in one HTTP request.
    ///
    /// A bigger batch is split **client-side** into several `execute_batch`
    /// requests, sent one after another with the results stitched back in
    /// order. This is the C++ client's `BatchPartMaxSize`, defaults included:
    /// unset, it is `concurrency × 5` — 250 when concurrency is unset too
    /// (`yt/cpp/mapreduce/interface/client_method_options.h`: *"If not
    /// specified it is set to `Concurrency * 5`"*).
    ///
    /// The trade is the ordinary one. One request is one round trip and one
    /// retryable unit; a split spends a round trip per piece, and a piece that
    /// fails wholesale fails [`Client::execute_batch`](crate::Client::execute_batch)
    /// wholesale with the earlier pieces already run — which that method's
    /// documentation spells out. Zero is clamped to one, because a part size
    /// of nothing sends nothing forever.
    #[must_use]
    pub fn with_max_part_size(mut self, parts: usize) -> Self {
        self.max_part_size = Some(parts.max(1));
        self
    }

    /// Adds a `create` — the same request [`Client::create`](crate::Client::create)
    /// sends: parents are created and an existing node is accepted.
    ///
    /// The part's answer is `{node_id=…}`. With `ignore_existing` in it, a
    /// node that already existed answers with the **old** node's id and any
    /// attributes are silently ignored — the same trap
    /// [`Client::create_table`](crate::Client::create_table) documents, and
    /// the reason [`BatchRequest::create_table`] exists beside this.
    pub fn create(&mut self, node_type: &str, path: &str) -> &mut Self {
        self.push(
            "create",
            yson_build::map([
                ("path", yson_build::string(path)),
                ("type", yson_build::string(node_type)),
                ("recursive", yson_build::boolean(true)),
                ("ignore_existing", yson_build::boolean(true)),
            ]),
            None,
            PartKind::MasterMutation,
            Output::Keyed("node_id"),
        )
    }

    /// Adds a table creation with a schema — the same request
    /// [`Client::create_table`](crate::Client::create_table) sends, refusals
    /// included.
    ///
    /// The schema goes **inside `attributes`**, where `create` reads it; a
    /// top-level `schema` would be accepted and silently ignored. And unlike
    /// [`BatchRequest::create`] this part **fails on a path that already
    /// exists**, deliberately: the cluster ignores the attributes of a create
    /// it skips, so an `ignore_existing` spelling would leave the old table
    /// with the old schema under a per-part `Ok`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the schema is one the cluster would
    /// refuse — checked here, when the part is built, so the mistake is
    /// reported once rather than as a per-part error after a round trip.
    pub fn create_table(&mut self, path: &str, schema: &TableSchema) -> Result<&mut Self> {
        schema
            .validate()
            .map_err(|reason| ClientError::Config(format!("{path}: {reason}")))?;

        Ok(self.push(
            "create",
            yson_build::map([
                ("path", yson_build::string(path)),
                ("type", yson_build::string("table")),
                ("recursive", yson_build::boolean(true)),
                (
                    "attributes",
                    yson_build::map([("schema", schema.to_yson())]),
                ),
            ]),
            None,
            PartKind::MasterMutation,
            Output::Keyed("node_id"),
        ))
    }

    /// Adds an `exists` — as [`Client::exists`](crate::Client::exists).
    ///
    /// The part's answer is `{value=%true}` or `{value=%false}` — the key is
    /// `value`, not the command's name, exactly as it is outside a batch.
    pub fn exists(&mut self, path: &str) -> &mut Self {
        self.push(
            "exists",
            yson_build::map([("path", yson_build::string(path))]),
            None,
            PartKind::Read,
            Output::Keyed("value"),
        )
    }

    /// Adds a `get` — as [`Client::get`](crate::Client::get). The part's
    /// answer is `{value=…}`.
    pub fn get(&mut self, path: &str) -> &mut Self {
        self.push(
            "get",
            yson_build::map([("path", yson_build::string(path))]),
            None,
            PartKind::Read,
            Output::Keyed("value"),
        )
    }

    /// Adds a `list` — as [`Client::list`](crate::Client::list). The part's
    /// answer is `{value=[…]}`, unsorted and — unlike
    /// [`Client::list`](crate::Client::list) — **not checked for the
    /// `incomplete` marker**: a batch hands back what each part answered, and
    /// reading the attribute is the caller's to do if the node may be large.
    pub fn list(&mut self, path: &str) -> &mut Self {
        self.push(
            "list",
            yson_build::map([("path", yson_build::string(path))]),
            None,
            PartKind::Read,
            Output::Keyed("value"),
        )
    }

    /// Adds a `remove` — as [`Client::remove`](crate::Client::remove): the
    /// node must exist, and a map node must be empty.
    pub fn remove(&mut self, path: &str) -> &mut Self {
        self.push(
            "remove",
            yson_build::map([
                ("path", yson_build::string(path)),
                ("recursive", yson_build::boolean(false)),
                ("force", yson_build::boolean(false)),
            ]),
            None,
            PartKind::MasterMutation,
            Output::Unchecked,
        )
    }

    /// Adds a `remove` of a whole subtree, absent included — as
    /// [`Client::remove_tree`](crate::Client::remove_tree).
    pub fn remove_tree(&mut self, path: &str) -> &mut Self {
        self.push(
            "remove",
            yson_build::map([
                ("path", yson_build::string(path)),
                ("recursive", yson_build::boolean(true)),
                ("force", yson_build::boolean(true)),
            ]),
            None,
            PartKind::MasterMutation,
            Output::Unchecked,
        )
    }

    /// Adds a `set` of one attribute — what
    /// [`Client::set_attribute`](crate::Client::set_attribute) does.
    ///
    /// `set` takes structured input, and inside a batch that input is the
    /// part's own `input` field rather than a request body — the
    /// [reference](https://ytsaurus.tech/docs/en/api/commands#execute_batch)'s
    /// own example is a `set` carried this way, and the driver encodes the
    /// value and sets the part's `input_format` itself
    /// (`TExecuteBatchCommand::TRequestExecutor::Run`). Verified on a local
    /// cluster; the part answers `{output={}}`.
    pub fn set_attribute(&mut self, path: &str, name: &str, value: YsonValue) -> &mut Self {
        self.push(
            "set",
            yson_build::map([("path", yson_build::string(format!("{path}/@{name}")))]),
            Some(value),
            PartKind::MasterMutation,
            Output::Unchecked,
        )
    }

    /// Adds a command this crate does not model.
    ///
    /// The escape hatch, as [`Client::raw_command`](crate::Client::raw_command)
    /// is outside a batch — and with the same default and the same
    /// consequence: **a batch carrying a raw part is sent once**, whatever the
    /// retry policy says, because a command this crate cannot classify may be
    /// mutating somewhere no mutation cache covers, and a replayed batch would
    /// apply it twice. [`BatchRequest::raw_with`] is where a caller who knows
    /// the command's registry bits says otherwise, exactly as
    /// [`Client::raw_command_with`](crate::Client::raw_command_with) is
    /// outside a batch; [`Client::execute_batch`](crate::Client::execute_batch)
    /// documents the retry rule this feeds.
    ///
    /// `input` is for a structured-input command (the rule
    /// [`BatchRequest::set_attribute`] describes); commands with no input
    /// stream pass `None`. Only light commands with `null` or `structured`
    /// input and output can be parts at all — and know that a part naming a
    /// command the cluster has never heard of fails the **whole batch**, not
    /// the part: watched on a local cluster, where `{command=frobnicate}` was
    /// answered HTTP 400 and `Unknown command "frobnicate"` with no per-part
    /// results at all. (The driver decides per-part errors only after it has
    /// resolved the command's descriptor — `TRequestExecutor::Run` throws
    /// before that on an unknown name.)
    ///
    /// **A refused batch is not a partly-run batch. Every part runs.** The
    /// failure destroys the *answers*, not the work: the driver collects the
    /// sub-requests into callbacks, runs them all through
    /// `CancelableRunWithBoundedConcurrency`, and only then calls
    /// `.ValueOrThrow()` on the collected list — which discards every result
    /// together the moment one of them is the unknown-name throw. Dispatch is
    /// never aborted. Measured five ways on a local cluster, and it is not a
    /// race: `[create, frobnicate]` created its node; so did
    /// `[frobnicate, create]` with the bad part **first**;
    /// `[create, frobnicate, create]` created **both**; and at
    /// `concurrency=1`, where a reader would most expect the damage to stop
    /// early, `[frobnicate, create, create]` still created both and eight
    /// creates followed by a `frobnicate` created **all eight**. Putting the
    /// bad part first does not help, and lowering the concurrency does not
    /// help. A name worth typing here is one you have checked.
    ///
    /// The one distinction that does bound the damage is **when** the request
    /// fails. A batch refused while its *parameters are being read* never runs
    /// anything: `concurrency=0` was answered `Validation failed at
    /// /concurrency`, a part missing its `command` field and a part whose
    /// `parameters` were not a dict were both answered `Error loading parameter
    /// /requests`, and in every one of those a `create` sitting in the same
    /// request left **no node behind**. A batch that gets as far as *executing*
    /// applies all of it. Parse-time failures are total; execution-time
    /// failures are total the other way.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `command` is not a bare command
    /// name, or if `params` is not a YSON dict — the same refusals, for the
    /// same reasons, as [`Client::raw_command`](crate::Client::raw_command) —
    /// or if `command` is one the cluster will not take as a part, by the
    /// data-type rule [`BatchRequest::raw_with`] describes.
    pub fn raw(
        &mut self,
        command: &str,
        params: YsonValue,
        input: Option<YsonValue>,
    ) -> Result<&mut Self> {
        self.raw_with(command, params, input, Repeatable::Never)
    }

    /// As [`BatchRequest::raw`], saying how the part may be repeated.
    ///
    /// The asymmetry this removes: `raw` hard-codes [`Repeatable::Never`], and
    /// because the batch retries as the most cautious of its parts, **one**
    /// raw part demotes an otherwise all-read batch to send-once. A raw read —
    /// `check_permission`, `get_supported_features`, `parse_ypath` — is
    /// [`Repeatable::Freely`], and saying so leaves the batch as retriable as
    /// it was. The judgement is the cluster's, from the same `REGISTER_ALL`
    /// row [`Client::raw_command_with`](crate::Client::raw_command_with) reads
    /// it from, and the same caution applies: *light and mutating* is not
    /// enough for [`Repeatable::WithMutationId`], because the mutation cache
    /// is the **master's** and a scheduler command is not in it. Prefer
    /// [`Repeatable::Never`] when in doubt — that is why it is what `raw`
    /// gives you.
    ///
    /// A part's class is combined with the others, never applied alone: the
    /// batch is one HTTP request, so it goes out as the most cautious answer
    /// among its parts.
    ///
    /// # Errors
    ///
    /// As [`BatchRequest::raw`], and additionally [`ClientError::Config`] for
    /// [`Repeatable::Heavy`], which is not a class a part can have: it asks for
    /// a heavy proxy, and a batch does not go to one.
    ///
    /// A name is also refused when the cluster would refuse it as a part. **The
    /// cluster's rule is the command's data types, not `isHeavy`**: a part is
    /// refused when its registered output type is `tabular` or `binary`, or its
    /// input type is `binary`, and the driver throws before any part runs, so
    /// the **whole** batch fails and every other part loses its answer. That is
    /// the check `NOT_A_BATCH_PART` makes, whichever class is claimed for the
    /// name — `select_rows` and `lookup_rows` are on it, and are the ones a
    /// caller is likeliest to try.
    ///
    /// **The list is a snapshot of one cluster's registry, not a promise.** A
    /// cluster of another version registers other commands, and a name this
    /// crate has never heard of can still be refused on the wire — as can a
    /// part whose command takes input and is given none
    /// (`Command %Qv requires input`), which no list can catch because it
    /// depends on the call. What the check buys is the common mistake caught
    /// before the socket, not a guarantee that the batch will be taken.
    ///
    /// Separately, this crate refuses the bulk-data commands it lists as heavy
    /// even where the cluster would take them — `write_table` was measured
    /// being accepted as a part and applying its rows — because a part's input
    /// travels inline in the batch body to a light proxy, which is not where
    /// this crate sends table or file data.
    pub fn raw_with(
        &mut self,
        command: &str,
        params: YsonValue,
        input: Option<YsonValue>,
        repeatable: Repeatable,
    ) -> Result<&mut Self> {
        crate::check_command_name(command)?;
        crate::refuse_non_dict_parameters(command, &params)?;

        if NOT_A_BATCH_PART.contains(&command) {
            return Err(ClientError::Config(format!(
                "the cluster refuses {command} as a batch part: its registered \
                 input or output type is a data stream, and the driver throws \
                 \"cannot be part of a batch since it has inappropriate output \
                 type\" before any part runs — so the whole request fails and \
                 every other part loses its answer, while the parts that were \
                 going to apply still apply. Send it with \
                 Client::raw_command_streaming or Client::raw_command_upload, \
                 outside the batch."
            )));
        }
        if crate::http::is_heavy(command) {
            return Err(ClientError::Config(format!(
                "{command} moves bulk data, and a batch part carries its input \
                 inline in the batch body to a light proxy — which is not where \
                 this crate sends table or file data. The refusal is this \
                 crate's, not the cluster's: a {command} part was measured \
                 being accepted and applied. Send it with \
                 Client::raw_command_streaming or Client::raw_command_upload, \
                 outside the batch."
            )));
        }
        if repeatable == Repeatable::Heavy {
            return Err(ClientError::Config(format!(
                "{command} was declared Repeatable::Heavy, which is not a class \
                 a batch part can have: a heavy command is refused as a part, \
                 and Repeatable::Heavy also asks for a heavy proxy, which is \
                 not where a batch goes. Send it outside the batch."
            )));
        }

        let kind = match repeatable {
            Repeatable::Freely => PartKind::Read,
            Repeatable::WithMutationId => PartKind::MasterMutation,
            // `Never`, and any class a later release names: the batch is sent
            // once, which is the answer that is safe for all of them.
            _ => PartKind::Raw,
        };
        Ok(self.push(command, params, input, kind, Output::Unchecked))
    }

    /// How many parts the batch holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.parts.len()
    }

    /// Whether the batch holds no parts. An empty batch is refused by
    /// [`Client::execute_batch`](crate::Client::execute_batch) rather than
    /// sent.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    fn push(
        &mut self,
        command: &str,
        parameters: YsonValue,
        input: Option<YsonValue>,
        kind: PartKind,
        output: Output,
    ) -> &mut Self {
        self.parts.push(BatchPart {
            command: command.to_owned(),
            parameters,
            input,
            kind,
            output,
        });
        self
    }

    /// The parts, for [`Client::execute_batch`](crate::Client::execute_batch)
    /// to chunk and send.
    pub(crate) fn parts(&self) -> &[BatchPart] {
        &self.parts
    }

    /// The `concurrency` to send, when the caller set one.
    pub(crate) fn concurrency(&self) -> Option<i64> {
        self.concurrency
    }

    /// How many parts one HTTP request may carry — the caller's cap, or the
    /// C++ client's `concurrency × 5` when there is none.
    pub(crate) fn max_part_size(&self) -> usize {
        self.max_part_size.unwrap_or_else(|| {
            usize::try_from(self.concurrency.unwrap_or(DEFAULT_CONCURRENCY))
                .unwrap_or(usize::MAX)
                .saturating_mul(PARTS_PER_CONCURRENCY)
                .max(1)
        })
    }

    /// How the whole batch may be repeated: the most cautious of its parts.
    ///
    /// All reads — repeat freely; the batch mutates nothing, and the
    /// [reference](https://ytsaurus.tech/docs/en/api/commands#execute_batch)
    /// says as much: *"Mutating if the set includes mutating commands."* Any
    /// modelled mutation — under a mutation id, which the driver spreads over
    /// the volatile parts (see
    /// [`Client::execute_batch`](crate::Client::execute_batch)). Any raw part
    /// — sent once, because nothing can vouch for what a replay would do.
    pub(crate) fn repeatable(&self) -> Repeatable {
        if self.parts.iter().any(|part| part.kind == PartKind::Raw) {
            return Repeatable::Never;
        }
        if self
            .parts
            .iter()
            .any(|part| part.kind == PartKind::MasterMutation)
        {
            return Repeatable::WithMutationId;
        }
        Repeatable::Freely
    }
}

/// Renders one chunk of parts as the parameters `execute_batch` takes.
///
/// `transaction` is the client's bound transaction, stamped into **each
/// part**: the outer command has no transaction to be in — its options are
/// `TExecuteBatchOptions : TMutatingOptions`, with no transactional half — and
/// a local cluster proved the point by dropping an outer `transaction_id` in
/// silence: the part's create landed *outside* the transaction and survived
/// its abort. Stamping the parts is the only spelling the cluster honours,
/// and it follows the transport's own rules: a part that already names a
/// transaction keeps it, and a command on the no-transaction list is left
/// alone.
pub(crate) fn render_chunk(
    parts: &[BatchPart],
    concurrency: Option<i64>,
    transaction: Option<&str>,
) -> Result<Vec<u8>> {
    let requests = parts.iter().map(|part| {
        let mut parameters = part.parameters.clone();
        if let Some(id) = transaction
            && !crate::http::takes_no_transaction(&part.command)
            && !names_transaction(&parameters)
        {
            yson_build::insert(&mut parameters, "transaction_id", yson_build::string(id));
        }

        let mut request = yson_build::map([
            ("command", yson_build::string(&part.command)),
            ("parameters", parameters),
        ]);
        if let Some(input) = &part.input {
            yson_build::insert(&mut request, "input", input.clone());
        }
        request
    });

    let mut rendered = yson_build::map([("requests", yson_build::list(requests))]);
    if let Some(concurrency) = concurrency {
        yson_build::insert(&mut rendered, "concurrency", yson_build::int(concurrency));
    }

    to_string(&rendered, YsonFormat::Text)
        .map(String::into_bytes)
        .map_err(|e| ClientError::Decode {
            command: "execute_batch".to_owned(),
            reason: format!("could not encode the batch: {e}"),
        })
}

/// Whether a part's parameters already name a transaction of their own.
fn names_transaction(parameters: &YsonValue) -> bool {
    matches!(
        &parameters.node,
        YsonNode::Map(m) if m.contains_key(b"transaction_id".as_slice())
    )
}

/// Reads one chunk's response into per-part `Result`s.
///
/// The envelope is `{results=[…]}` — `ProduceSingleOutput(context, "results",
/// …)` in the driver, the ordinary v4 wrapping — with **one item per part, in
/// the order the parts were sent**. Each item is what
/// `TRequestExecutor::OnResponse` builds and what a local cluster actually
/// answered:
///
/// - `{error={…}}` — the part failed, and the value is a YTsaurus error
///   document in YSON: `code`, `message`, `attributes`, nested
///   `inner_errors`;
/// - `{output={…}}` — the part succeeded, and the value is the part's own
///   v4 answer, keyed by what that command returns: `{node_id=…}` for
///   `create`, `{value=…}` for `exists`, `get` and `list`, and `{}` — an
///   empty `output`, not an absent one — for `set` and `remove`;
/// - `{}` — the part succeeded and the driver wrote no `output` key at all,
///   which is what the reference's own example shows for a `set`. **No
///   modelled command answers this way on API v4**, the version this crate
///   speaks: measured one part apiece, `set` and `remove` both answer
///   `{output={}}`. The arm is kept for a [`BatchRequest::raw`] part, whose
///   command may be registered `null`-output, and is refused for any part
///   whose success this crate knows a key for. See [`part_result`].
///
/// Anything else is refused as [`ClientError::Decode`] rather than read as
/// one of the three: this crate's envelope rules were learned from `exists`
/// answering under `value` and the file cache answering with a bare string,
/// and a shape this parser does not recognise is likelier to be a new answer
/// than an empty one. A response with the wrong number of items is refused
/// whole for the same reason — pairing what answers there are against the
/// wrong parts would hand every caller after the gap somebody else's result.
pub(crate) fn parse_results(body: &[u8], parts: &[BatchPart]) -> Result<Vec<Result<YsonValue>>> {
    let envelope: YsonValue =
        ytsaurus_yson::from_slice(body, YsonFormat::Text).map_err(|e| ClientError::Decode {
            command: "execute_batch".to_owned(),
            reason: format!(
                "{e}; body was {}",
                crate::error::truncate(&String::from_utf8_lossy(body), 200)
            ),
        })?;

    let results = match &envelope.node {
        YsonNode::Map(m) => m.get(b"results".as_slice()).ok_or_else(|| {
            refused(format!(
                "the answer has no \"results\"; keys were {:?}",
                m.keys()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .collect::<Vec<_>>()
            ))
        }),
        other => Err(refused(format!("expected a dict, got {other:?}"))),
    }?;

    let YsonNode::List(items) = &results.node else {
        return Err(refused(format!(
            "\"results\" is not a list: {:?}",
            results.node
        )));
    };

    if items.len() != parts.len() {
        return Err(refused(format!(
            "{} parts were sent and {} results came back; pairing them up \
             would hand callers each other's answers",
            parts.len(),
            items.len()
        )));
    }

    items.iter().zip(parts).map(part_result).collect()
}

/// One item of the `results` list, read by the rules above.
///
/// **The check is on the key, not on the wrapper.** The scenario it exists for
/// is a `create` whose answer has no `node_id` in it: the access this crate
/// teaches for a create is `answer["node_id"]`, [`YsonValue`]'s `Index` panics
/// on a missing key, and a parser that waved the answer through would have
/// turned a strange response into a panic in caller code one frame away. A
/// guard on the *wrapper* alone — refusing only a bare `{}` — misses that
/// scenario entirely on API v4, because the shape a v4 cluster would actually
/// produce is `{output={}}`, and `set` and `remove` measurably emit exactly
/// that as their success. So [`Output::Keyed`] is held to its key wherever the
/// answer arrives, and only [`Output::Unchecked`] — `set`, `remove`, and a
/// [`BatchRequest::raw`] part whose shape only its caller knows — takes what
/// comes.
fn part_result((item, part): (&YsonValue, &BatchPart)) -> Result<Result<YsonValue>> {
    let command = &part.command;

    let YsonNode::Map(fields) = &item.node else {
        return Err(refused(format!(
            "{command}: a part's result is not a dict: {:?}",
            item.node
        )));
    };

    let error = fields.get(b"error".as_slice());
    let output = fields.get(b"output".as_slice());

    match (error, output, fields.len()) {
        (Some(error), None, 1) => Ok(Err(part_error(command, error))),
        (None, Some(output), 1) => match part.output {
            Output::Keyed(key) if field(output, key.as_bytes()).is_none() => Err(refused(format!(
                "{command}: a part succeeded with {}, which has no \"{key}\" \
                     in it — and a {command} answers under \"{key}\". Handing \
                     that back would panic one frame away, where this crate \
                     teaches answer[\"{key}\"].",
                to_string(output, YsonFormat::Text).unwrap_or_else(|_| "?".to_owned())
            ))),
            _ => Ok(Ok(output.clone())),
        },
        // Success with no `output` key at all. No modelled command answers
        // this way on v4; a raw part's command may be registered null-output.
        (None, None, 0) if part.output == Output::Unchecked => Ok(Ok(yson_build::empty_map())),
        (None, None, 0) => Err(refused(format!(
            "{command}: a part answered with an empty result, which means \"no \
             output\" — but a {command} answers with a value in it, so this is \
             a shape from nowhere. Reading it as an empty success would hand \
             back a map with no node_id or value in it, and indexing that panics."
        ))),
        _ => Err(refused(format!(
            "{command}: a part's result carries keys this client does not \
             recognise: {:?}",
            fields
                .keys()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect::<Vec<_>>()
        ))),
    }
}

/// A response shape this parser refuses to guess about.
fn refused(reason: String) -> ClientError {
    ClientError::Decode {
        command: "execute_batch".to_owned(),
        reason,
    }
}

/// Builds a part's failure from its error document.
///
/// The same flattening as everywhere else in the crate — the outer message is
/// often a category (`Error resolving path …`) with the cause at the bottom of
/// `inner_errors`, so both are carried. The document arrives as YSON here
/// rather than as the JSON of an `X-YT-Error` header, which is why this walk
/// exists beside [`ClientError::from_yt_error`]; `raw` keeps the whole
/// document in the shape it arrived, YSON text.
fn part_error(command: &str, document: &YsonValue) -> ClientError {
    let code = field(document, b"code")
        .and_then(YsonValue::as_i64)
        .unwrap_or(-1);
    let outer = field(document, b"message")
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "(no message)".to_owned());

    let message = match innermost_message(document) {
        Some(inner) if inner != outer => format!("{outer}: {inner}"),
        _ => outer,
    };

    ClientError::Cluster {
        command: command.to_owned(),
        code,
        message,
        raw: to_string(document, YsonFormat::Text).unwrap_or_default(),
    }
}

/// One field of a YSON dict, or nothing where it is not a dict.
fn field<'a>(value: &'a YsonValue, key: &[u8]) -> Option<&'a YsonValue> {
    match &value.node {
        YsonNode::Map(m) => m.get(key),
        _ => None,
    }
}

/// Walks `inner_errors` to the deepest message — the YSON twin of the JSON
/// walk in `error.rs`, kept in step with it.
fn innermost_message(document: &YsonValue) -> Option<String> {
    let inner = field(document, b"inner_errors")?;
    let YsonNode::List(errors) = &inner.node else {
        return None;
    };
    let first = errors.first()?;
    innermost_message(first).or_else(|| {
        field(first, b"message").and_then(|message| message.as_str().map(str::to_owned))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, ColumnType};

    /// Captured from a local cluster: one batch, four parts, two of them
    /// failed — a `create` over an existing node, a `set` with input, a
    /// `get`, and a `remove` of nothing.
    const ONE_FAILS_REST_SUCCEED: &[u8] = br#"{"results"=[{"error"={"code"=501;"message"="Node //tmp/impl-batch-a already exists";"attributes"={"host"="localhost";};};};{"output"={};};{"output"={"value"="table";};};{"error"={"code"=500;"message"="Node //tmp has no child with key \"impl-batch-nothing-here\"";"attributes"={"host"="localhost";};};};];}"#;

    fn four_parts() -> BatchRequest {
        let mut batch = BatchRequest::new();
        batch
            .create("table", "//tmp/impl-batch-a")
            .set_attribute("//tmp/impl-batch-b", "note", yson_build::string("hello"))
            .get("//tmp/impl-batch-b/@type")
            .remove("//tmp/impl-batch-nothing-here");
        batch
    }

    #[test]
    fn per_part_results_keep_their_order_and_their_sides() {
        let results = parse_results(ONE_FAILS_REST_SUCCEED, four_parts().parts()).expect("parses");

        assert_eq!(results.len(), 4);
        assert!(results[0].is_err() && results[3].is_err());
        assert!(results[1].is_ok() && results[2].is_ok());

        // The success carries the part's own envelope, keyed by what that
        // command returns.
        assert_eq!(
            results[2].as_ref().expect("a get succeeded")["value"].as_str(),
            Some("table")
        );
        // A `set` succeeds with an empty envelope, not with an absent one.
        assert_eq!(
            results[1].as_ref().expect("a set succeeded"),
            &yson_build::empty_map()
        );
    }

    #[test]
    fn a_part_error_flattens_like_every_other_cluster_error() {
        // Captured from a local cluster: a `get` on a missing path, where the
        // outer message is a category and the cause is one level down.
        let document = ytsaurus_yson::from_slice(
            br#"{"code"=500;"message"="Error resolving path //tmp/impl-batch-nothing/@x";"inner_errors"=[{"code"=500;"message"="Node //tmp has no child with key \"impl-batch-nothing\"";};];}"#,
            YsonFormat::Text,
        )
        .expect("valid YSON");

        let error = part_error("get", &document);
        let ClientError::Cluster {
            command,
            code,
            message,
            raw,
        } = &error
        else {
            panic!("a part failure is a cluster error: {error:?}");
        };

        assert_eq!(command, "get");
        assert_eq!(*code, 500);
        assert_eq!(
            message,
            "Error resolving path //tmp/impl-batch-nothing/@x: \
             Node //tmp has no child with key \"impl-batch-nothing\""
        );
        // The whole document survives, in the shape it arrived.
        assert!(raw.contains("inner_errors"), "{raw}");
    }

    #[test]
    fn a_result_shape_from_nowhere_is_refused_rather_than_guessed() {
        let mut one_get = BatchRequest::new();
        one_get.get("//tmp/t");

        for (body, why) in [
            (br#"{"results"=[]}"#.to_vec(), "a missing answer"),
            (br#"[]"#.to_vec(), "no envelope at all"),
            (br#"{"value"=[{}]}"#.to_vec(), "the wrong envelope key"),
            (br#"{"results"={}}"#.to_vec(), "results that are not a list"),
            (
                br#"{"results"=[{"outcome"={}}]}"#.to_vec(),
                "a key this client has never seen",
            ),
            (
                br#"{"results"=[{"output"={};"error"={}}]}"#.to_vec(),
                "both sides at once",
            ),
            (
                br#"{"results"=["ok"]}"#.to_vec(),
                "an item that is not a dict",
            ),
            (
                br#"{"results"=[{};{}]}"#.to_vec(),
                "more answers than parts",
            ),
        ] {
            let error = parse_results(&body, one_get.parts())
                .expect_err(&format!("{why} must not pass as a result"));
            assert!(
                matches!(error, ClientError::Decode { .. }),
                "{why}: {error:?}"
            );
        }
    }

    #[test]
    fn an_empty_item_is_a_success_with_nothing_to_say() {
        // The documented shape for a null-output part — the reference's own
        // example answers a `set` with `{ }`. No modelled command answers
        // that way on v4 (`set` and `remove` both answer `{output={}}`), so
        // the arm stands for a raw part whose command may be registered
        // null-output on some version — and for `set`/`remove`, which have no
        // key to be held to either way.
        let mut batch = BatchRequest::new();
        batch.set_attribute("//tmp/t", "note", yson_build::string("x"));

        let results = parse_results(br#"{"results"=[{}]}"#, batch.parts()).expect("parses");
        assert_eq!(
            results[0].as_ref().expect("a success"),
            &yson_build::empty_map()
        );
    }

    #[test]
    fn a_keyed_part_is_held_to_its_key_however_the_answer_is_wrapped() {
        // The scenario the check exists for, in the shape a v4 cluster can
        // really produce. `{output={}}` is a legitimate success for `set` and
        // `remove` on v4 — measured — so a guard that only refused a bare
        // `{}` would wave this through and panic one frame away at
        // `answer["node_id"]`.
        for (build, key) in [
            (
                (|batch: &mut BatchRequest| {
                    batch.create("table", "//tmp/t");
                }) as fn(&mut BatchRequest),
                "node_id",
            ),
            (
                |batch| {
                    batch.exists("//tmp/t");
                },
                "value",
            ),
            (
                |batch| {
                    batch.get("//tmp/t");
                },
                "value",
            ),
            (
                |batch| {
                    batch.list("//tmp/t");
                },
                "value",
            ),
        ] {
            let mut batch = BatchRequest::new();
            build(&mut batch);
            let command = batch.parts()[0].command.clone();

            for body in [
                br#"{"results"=[{"output"={}}]}"#.to_vec(),
                br#"{"results"=[{"output"={"something_else"=1}}]}"#.to_vec(),
                br#"{"results"=[{"output"="a string"}]}"#.to_vec(),
            ] {
                let error = parse_results(&body, batch.parts()).expect_err(&format!(
                    "{command} must not succeed without its {key}: {}",
                    String::from_utf8_lossy(&body)
                ));
                assert!(matches!(error, ClientError::Decode { .. }), "{error:?}");
                assert!(error.to_string().contains(key), "{error}");
            }

            // And the real answer still passes.
            let good = format!(r#"{{"results"=[{{"output"={{"{key}"="x"}}}}]}}"#);
            let results = parse_results(good.as_bytes(), batch.parts()).expect("parses");
            assert!(results[0].is_ok(), "{results:?}");
        }

        // `set` and `remove` have no key to be held to: their v4 success is
        // an empty `output`, so anything is taken as it comes.
        let mut nulls = BatchRequest::new();
        nulls
            .set_attribute("//tmp/t", "note", yson_build::string("x"))
            .remove("//tmp/t");
        let results = parse_results(
            br#"{"results"=[{"output"={}};{"output"={}}]}"#,
            nulls.parts(),
        )
        .expect("both parse");
        assert!(results.iter().all(Result::is_ok), "{results:?}");
    }

    #[test]
    fn an_empty_result_is_refused_for_a_part_whose_success_has_a_value() {
        // `{}` means the driver wrote no `output` key, which it does only for
        // a command whose output type is Null. A `create` answering that way
        // is a shape from nowhere, and reading it as an empty success hands
        // the caller a map with no `node_id` in it — which the access this
        // crate teaches, `answer["node_id"]`, then panics on.
        for build in [
            (|batch: &mut BatchRequest| {
                batch.create("table", "//tmp/t");
            }) as fn(&mut BatchRequest),
            |batch| {
                batch.exists("//tmp/t");
            },
            |batch| {
                batch.get("//tmp/t");
            },
            |batch| {
                batch.list("//tmp/t");
            },
        ] {
            let mut batch = BatchRequest::new();
            build(&mut batch);
            let command = batch.parts()[0].command.clone();

            let error = parse_results(br#"{"results"=[{}]}"#, batch.parts())
                .expect_err(&format!("{command} does not succeed with nothing to say"));
            assert!(matches!(error, ClientError::Decode { .. }), "{error:?}");
            assert!(error.to_string().contains("shape from nowhere"), "{error}");
        }

        // The parts with no key to be held to still answer bare: `set` and
        // `remove`, whose v4 success is measurably an empty `output`, and a
        // raw part, whose shape only its caller knows.
        let mut nulls = BatchRequest::new();
        nulls
            .set_attribute("//tmp/t", "note", yson_build::string("x"))
            .remove("//tmp/t");
        nulls
            .raw(
                "parse_ypath",
                yson_build::map([("path", yson_build::string("//tmp"))]),
                None,
            )
            .expect("a fine command name");

        let results =
            parse_results(br#"{"results"=[{};{};{}]}"#, nulls.parts()).expect("all three parse");
        assert!(results.iter().all(Result::is_ok), "{results:?}");
    }

    #[test]
    fn a_heavy_command_cannot_be_a_part_however_it_is_classified() {
        // The cluster fails the *whole* batch over a command whose data types
        // it will not take as a part — so the other parts would lose their
        // answers to a mistake this list can catch before the socket. The
        // rule is the data types and not `isHeavy`: `select_rows` and
        // `lookup_rows` are the ones a caller would plausibly try to batch,
        // and both were measured being refused with `inappropriate output
        // type "tabular"` while a `create` beside them applied anyway.
        for refused in [
            "write_table",
            "read_table",
            "write_file",
            "get_job_input",
            "select_rows",
            "lookup_rows",
            "get_job_trace",
            "pull_queue",
            "alter_query",
            "read_journal",
            "write_file_fragment",
        ] {
            let mut batch = BatchRequest::new();
            let error = batch
                .raw(refused, yson_build::empty_map(), None)
                .expect_err(&format!("{refused} cannot be a part"));
            assert!(
                matches!(error, ClientError::Config(_)),
                "{refused}: {error}"
            );
            assert!(batch.is_empty(), "a refused part must not be half-added");

            // And claiming a class for it does not make it acceptable.
            assert!(
                batch
                    .raw_with(refused, yson_build::empty_map(), None, Repeatable::Freely)
                    .is_err(),
                "{refused} was accepted once it claimed to be a read"
            );
        }

        // A command the cluster *does* take as a part is not refused for
        // being registered heavy: `get_job_spec` is `is_heavy: true` and was
        // measured coming back as an ordinary per-part error, not a
        // whole-batch failure.
        let mut fine = BatchRequest::new();
        fine.raw(
            "get_job_spec",
            yson_build::map([("job_id", yson_build::string("1-2-3-4"))]),
            None,
        )
        .expect("a heavy command the cluster takes as a part");
        assert_eq!(fine.len(), 1);

        // `Heavy` is not a class a part can have at all, whatever it names.
        let mut batch = BatchRequest::new();
        let error = batch
            .raw_with(
                "check_permission",
                yson_build::empty_map(),
                None,
                Repeatable::Heavy,
            )
            .expect_err("a part is never heavy");
        assert!(matches!(error, ClientError::Config(_)), "{error}");
        assert!(batch.is_empty());
    }

    #[test]
    fn the_retry_class_is_the_most_cautious_part() {
        let mut reads = BatchRequest::new();
        reads.exists("//tmp/a").get("//tmp/b").list("//tmp/c");
        assert_eq!(reads.repeatable(), Repeatable::Freely);

        let mut mutating = BatchRequest::new();
        mutating.exists("//tmp/a").create("table", "//tmp/b");
        assert_eq!(mutating.repeatable(), Repeatable::WithMutationId);

        let mut raw = BatchRequest::new();
        raw.create("table", "//tmp/b");
        raw.raw(
            "parse_ypath",
            yson_build::map([("path", yson_build::string("//tmp"))]),
            None,
        )
        .expect("a fine command name");
        assert_eq!(raw.repeatable(), Repeatable::Never);

        // A caller who knows the command's registry bits says so, and one raw
        // *read* no longer costs an all-read batch its retry.
        let mut vouched = BatchRequest::new();
        vouched.exists("//tmp/a");
        vouched
            .raw_with(
                "check_permission",
                yson_build::map([("path", yson_build::string("//tmp"))]),
                None,
                Repeatable::Freely,
            )
            .expect("a fine command name");
        assert_eq!(vouched.repeatable(), Repeatable::Freely);

        // And a raw light mutation the master's cache covers keeps the batch
        // replayable rather than demoting it to send-once.
        vouched
            .raw_with(
                "concatenate",
                yson_build::map([("destination_path", yson_build::string("//tmp/c"))]),
                None,
                Repeatable::WithMutationId,
            )
            .expect("a fine command name");
        assert_eq!(vouched.repeatable(), Repeatable::WithMutationId);
    }

    #[test]
    fn a_raw_part_is_checked_like_a_raw_command() {
        let mut batch = BatchRequest::new();

        for bad in ["", "get?x=1", "get value", "get/../hosts"] {
            let error = batch
                .raw(bad, yson_build::empty_map(), None)
                .expect_err(&format!("{bad:?} was accepted as a command name"));
            assert!(matches!(error, ClientError::Config(_)), "{bad:?}: {error}");
        }

        let error = batch
            .raw("get", yson_build::string("//tmp"), None)
            .expect_err("parameters must be a dict");
        assert!(matches!(error, ClientError::Config(_)), "{error}");
        assert!(batch.is_empty(), "a refused part must not be half-added");
    }

    #[test]
    fn a_batch_schema_is_validated_where_the_client_validates_one() {
        let mut batch = BatchRequest::new();
        let unsound = TableSchema::new([Column::new("", ColumnType::Int64)]);

        let error = batch
            .create_table("//tmp/t", &unsound)
            .expect_err("an empty column name never reaches the cluster");
        assert!(matches!(error, ClientError::Config(_)), "{error}");
        assert!(batch.is_empty());
    }

    #[test]
    fn the_part_size_default_is_the_cpp_clients_rule() {
        let batch = BatchRequest::new();
        assert_eq!(batch.max_part_size(), 250, "concurrency 50 × 5");

        assert_eq!(
            BatchRequest::new().with_concurrency(8).max_part_size(),
            40,
            "the default part size follows the concurrency"
        );
        assert_eq!(
            BatchRequest::new()
                .with_concurrency(8)
                .with_max_part_size(3)
                .max_part_size(),
            3,
            "an explicit part size wins"
        );
        // Zero would loop forever; it is clamped as RetryPolicy clamps.
        assert_eq!(BatchRequest::new().with_max_part_size(0).max_part_size(), 1);
        assert_eq!(
            BatchRequest::new().with_concurrency(0).concurrency(),
            Some(1)
        );
    }

    #[test]
    fn a_bound_transaction_reaches_the_parts_that_can_take_one() {
        let mut batch = BatchRequest::new();
        batch.create("table", "//tmp/a");
        batch
            .raw(
                "get_operation",
                yson_build::map([("operation_id", yson_build::string("1-2-3-4"))]),
                None,
            )
            .expect("a fine command name");
        batch
            .raw(
                "create",
                yson_build::map([
                    ("path", yson_build::string("//tmp/b")),
                    ("type", yson_build::string("table")),
                    ("transaction_id", yson_build::string("3-aaa-bbb-ccc")),
                ]),
                None,
            )
            .expect("a fine command name");

        let body = render_chunk(batch.parts(), None, Some("3-5d231-10001-db88")).expect("renders");
        let rendered: YsonValue =
            ytsaurus_yson::from_slice(&body, YsonFormat::Text).expect("valid YSON");
        let YsonNode::List(requests) = &rendered["requests"].node else {
            panic!("requests is a list");
        };

        // The create is stamped with the client's transaction.
        assert_eq!(
            requests[0]["parameters"]["transaction_id"].as_str(),
            Some("3-5d231-10001-db88")
        );
        // A command with no transaction to be in is left alone.
        assert!(
            field(&requests[1]["parameters"], b"transaction_id").is_none(),
            "get_operation takes no transaction"
        );
        // A part that names its own transaction keeps it.
        assert_eq!(
            requests[2]["parameters"]["transaction_id"].as_str(),
            Some("3-aaa-bbb-ccc")
        );
    }
}
