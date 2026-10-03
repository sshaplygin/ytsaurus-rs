//! Several commands in one round trip: the cluster's
//! [`execute_batch`](https://ytsaurus.tech/docs/en/api/commands#execute_batch),
//! which both official clients also expose.
//!
//! [`BatchRequest`] builds a batch; [`Client::execute_batch`] sends it and
//! answers with a `Result` per part.
//!
//! [`Client::execute_batch`]: crate::Client::execute_batch

use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, to_string};

use crate::error::{ClientError, Result};
use crate::retry::Repeatable;
use crate::schema::TableSchema;
use crate::yson_build;

/// The `concurrency` the cluster assumes when none is sent: `Default(50)` in
/// `TExecuteBatchCommand::Register`
/// ([`etc_commands.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/etc_commands.cpp)),
/// and the C++ SDK's fallback. The default part size is derived from it.
const DEFAULT_CONCURRENCY: i64 = 50;

/// Parts per HTTP request per unit of concurrency when the caller does not
/// say: the C++ SDK's `Concurrency * 5` (`TExecuteBatchOptions` in
/// `yt/cpp/mapreduce/interface/client_method_options.h`).
const PARTS_PER_CONCURRENCY: usize = 5;

/// Commands the cluster refuses as a batch part. One such part fails the whole
/// request before any part answers, while the other parts still run.
///
/// The rule is the registered data types, not `isHeavy`: output `tabular` or
/// `binary`, or input `binary`. This is what one cluster's `GET /api/v4`
/// registry gave, so a cluster of another version may refuse names not listed.
/// A part that needs input and has none, and an unknown name, also fail the
/// whole batch; they depend on the call, so they are not here. See
/// [Batched commands](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#batched-commands).
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

/// How one part may be repeated. The batch is one HTTP request, so it retries
/// as its most cautious part; see [`BatchRequest::repeatable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartKind {
    /// `exists`, `get`, `list`: re-running one is harmless.
    Read,
    /// `create`, `remove`, `set`: the master's mutation cache covers them, and
    /// the driver gives each volatile part an id derived from the batch's, so
    /// a marked replay is answered with the first response.
    MasterMutation,
    /// Unclassified: it may mutate where no mutation cache covers it, so the
    /// batch is sent once, as [`Client::raw_command`](crate::Client::raw_command)
    /// is. Where a [`BatchRequest::raw`] part lands unless
    /// [`BatchRequest::raw_with`] names the command's retry class.
    Raw,
}

/// What a part's success is keyed by, which [`part_result`] holds its answer
/// to.
///
/// The registry's output-type bit cannot do this: on API v4 `set` and `remove`
/// are `structured`, as `create` is, and answer `{output={}}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Output {
    /// The success carries this key: `node_id` for `create`, `value` for
    /// `exists`, `get` and `list`. An answer without it is refused.
    Keyed(&'static str),
    /// Anything goes: `set` and `remove`, whose v4 success is an empty
    /// `output`, and a [`BatchRequest::raw`] part, whose shape only its caller
    /// knows.
    Unchecked,
}

/// One command inside a batch: `{command=…; parameters={…}}` with an optional
/// `input=…`, as `TExecuteBatchCommandRequest::Register` in
/// [`etc_commands.cpp`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/driver/etc_commands.cpp)
/// registers it. `input` carries a structured-input command's value, such as
/// `set`'s; the driver encodes it and sets the part's `input_format`.
#[derive(Debug, Clone)]
pub(crate) struct BatchPart {
    pub(crate) command: String,
    parameters: YsonValue,
    input: Option<YsonValue>,
    kind: PartKind,
    output: Output,
}

/// Commands sent in one round trip, answered with one `Result` per part.
///
/// Each typed method sends exactly the parameters its [`Client`](crate::Client)
/// namesake sends. Parts run in parallel, so a part and its consequence belong
/// in two batches ([Batched commands](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#batched-commands)).
/// Executing a batch again runs every part again under new mutation ids; a
/// replay is [`Client::execute_batch_with`](crate::Client::execute_batch_with)
/// with the id you kept.
///
/// ```no_run
/// use ytsaurus_client::{BatchRequest, Client};
/// # let client = Client::from_env()?;
/// let names = ["clicks", "visits", "errors"];
/// let mut batch = BatchRequest::new();
/// for name in names {
///     batch.create("table", &format!("//tmp/pipeline/{name}"));
/// }
/// for (name, made) in names.iter().zip(client.execute_batch(&batch)?) {
///     if let Err(error) = made {
///         eprintln!("{name}: {error}");
///     }
/// }
/// # Ok::<(), ytsaurus_client::ClientError>(())
/// ```
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

    /// Caps how many parts the cluster runs at once: the command's
    /// [`concurrency`](https://ytsaurus.tech/docs/en/api/commands#execute_batch),
    /// lowered to avoid exhausting a request rate limit. Unset, nothing is sent
    /// and the cluster's default of 50 applies. Zero is clamped to one, since
    /// the cluster refuses `concurrency=0`.
    ///
    /// The option setters take `self` and chain off the constructor; the part
    /// adders take `&mut self`, for use in a loop:
    ///
    /// ```
    /// # use ytsaurus_client::BatchRequest;
    /// let mut batch = BatchRequest::new().with_concurrency(8).with_max_part_size(64);
    /// for index in 0..3 {
    ///     batch.create("table", &format!("//tmp/pipeline/t{index}"));
    /// }
    /// assert_eq!(batch.len(), 3);
    /// ```
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = Some(i64::from(concurrency.max(1)));
        self
    }

    /// Caps how many parts travel in one HTTP request. A bigger batch is split
    /// client-side into requests sent one after another, with the results
    /// stitched back in order.
    ///
    /// Unset, it is `concurrency × 5`, 250 at the default concurrency: the C++
    /// client's `BatchPartMaxSize`. A piece that fails wholesale fails
    /// [`Client::execute_batch`](crate::Client::execute_batch) with the earlier
    /// pieces already run. Zero is clamped to one.
    #[must_use]
    pub fn with_max_part_size(mut self, parts: usize) -> Self {
        self.max_part_size = Some(parts.max(1));
        self
    }

    /// Adds a `create`, as [`Client::create`](crate::Client::create) sends it:
    /// parents are created and an existing node is accepted.
    ///
    /// The part answers `{node_id=…}`. Because it sends `ignore_existing`, an
    /// existing node answers with the old node's id; a schema needs
    /// [`BatchRequest::create_table`].
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

    /// Adds a table creation with a schema, as
    /// [`Client::create_table`](crate::Client::create_table) sends it: the
    /// schema inside `attributes`, and no `ignore_existing`, so the part fails
    /// on an existing path instead of keeping the old schema under an `Ok`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the schema is one the cluster would
    /// refuse, checked when the part is built rather than after a round trip.
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

    /// Adds an `exists`, as [`Client::exists`](crate::Client::exists). The part
    /// answers `{value=%true}` or `{value=%false}`.
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

    /// Adds a `list`, as [`Client::list`](crate::Client::list). The part
    /// answers `{value=[…]}`, unsorted and not checked for the `incomplete`
    /// marker that [`Client::list`](crate::Client::list) refuses.
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

    /// Adds a `set` of one attribute, as
    /// [`Client::set_attribute`](crate::Client::set_attribute) does. The value
    /// travels as the part's `input`, and the part answers `{output={}}`.
    pub fn set_attribute(&mut self, path: &str, name: &str, value: YsonValue) -> &mut Self {
        self.push(
            "set",
            yson_build::map([("path", yson_build::string(format!("{path}/@{name}")))]),
            Some(value),
            PartKind::MasterMutation,
            Output::Unchecked,
        )
    }

    /// Adds a command this crate does not model, as
    /// [`Client::raw_command`](crate::Client::raw_command) sends one outside a
    /// batch.
    ///
    /// A batch with a raw part is sent once, whatever the retry policy: the
    /// command may mutate where no mutation cache covers it.
    /// [`BatchRequest::raw_with`] declares otherwise. `input` is the value of a
    /// structured-input command, as in [`BatchRequest::set_attribute`]; other
    /// commands pass `None`.
    ///
    /// A name the cluster does not know fails the whole batch with HTTP 400 and
    /// no per-part results, and the other parts still run. A batch refused while
    /// its parameters are parsed runs nothing. See
    /// [Batched commands](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#batched-commands).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `command` is not a bare command name,
    /// if `params` is not a YSON dict, or if the cluster will not take `command`
    /// as a part (the rule is on [`BatchRequest::raw_with`]).
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
    /// The batch retries as its most cautious part, so one `raw` part
    /// ([`Repeatable::Never`]) makes the whole batch send-once. A raw read such
    /// as `check_permission` is [`Repeatable::Freely`].
    /// [`Repeatable::WithMutationId`] needs a command the master's mutation
    /// cache covers, which a scheduler command is not; prefer
    /// [`Repeatable::Never`] when in doubt.
    ///
    /// # Errors
    ///
    /// As [`BatchRequest::raw`], and [`ClientError::Config`] when:
    ///
    /// - `repeatable` is [`Repeatable::Heavy`]: it asks for a heavy proxy, and a
    ///   batch does not go to one;
    /// - the cluster refuses `command` as a part: its registered output type is
    ///   `tabular` or `binary`, or its input type `binary`, as for `select_rows`
    ///   and `lookup_rows`. The list is one cluster's registry, so the cluster
    ///   may still refuse a name it lacks;
    /// - `command` moves bulk data, such as `write_table`: the cluster takes it,
    ///   but its input would travel inline in the batch body to a light proxy.
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

    /// How the whole batch may be repeated: as its most cautious part. Reads
    /// repeat freely; a modelled mutation repeats under a mutation id the driver
    /// spreads over the parts; a raw part is sent once.
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

/// Renders one chunk of parts as `execute_batch`'s parameters.
///
/// The bound `transaction` is stamped into each part, because the cluster
/// silently drops an envelope `transaction_id` (`TExecuteBatchOptions` has no
/// transactional half). A part naming its own transaction, or a command on the
/// no-transaction list, is left alone.
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

/// Reads one chunk's `{results=[…]}` into per-part `Result`s: one item per
/// part, in send order (`TRequestExecutor::OnResponse` in the driver).
///
/// - `{error={…}}`: the part failed, with a YTsaurus error document;
/// - `{output={…}}`: the part's own v4 answer, `{}` for `set` and `remove`;
/// - `{}`: success with no `output`, which no modelled command sends on v4;
///   accepted only for an [`Output::Unchecked`] part.
///
/// Any other shape, or a count that differs from the parts sent, is refused as
/// [`ClientError::Decode`]: pairing answers with the wrong parts would hand
/// callers each other's results.
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

/// One item of `results`. A [`Output::Keyed`] success is held to its key in
/// whatever wrapper it arrives: a `create` without `node_id` would make the
/// `answer["node_id"]` this crate teaches panic in the caller.
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

/// Builds a part's failure from its YSON error document, outer plus innermost
/// message, as [`ClientError::from_yt_error`] does for the JSON header. `raw`
/// keeps the whole document as YSON text.
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
