//! Operation specifications.
//!
//! Specs are YSON dicts with a great many optional fields. These builders cover
//! what launching a `ytsaurus-job` worker needs and expose an escape hatch —
//! [`MapSpec::with_raw`] — for the rest, rather than pretending to model the
//! whole surface.
//!
//! Reference:
//! <https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options>

use ytsaurus_format::DataFormat;
use ytsaurus_skiff::Format as SkiffFormat;
use ytsaurus_yson::YsonValue;

use crate::yson_build::{boolean, insert, int, list, map, string, with_attributes};

/// A `file_paths` entry that lands in the sandbox under `name`.
fn named_file(path: impl Into<String>, name: impl AsRef<str>) -> YsonValue {
    with_attributes(string(path.into()), [("file_name", string(name.as_ref()))])
}

/// Describes a Skiff format applied to a different number of tables than it
/// has schemas, or `None` when the format is YSON or the counts agree.
///
/// A Skiff format is positional: schema `k` describes table `k`, which is why
/// YTsaurus needs one per table and a YSON selection needs none. `tables` is
/// how many tables the format will actually meet, and `kind` names them for the
/// message.
fn skiff_table_mismatch(
    what: &str,
    format: &DataFormat,
    tables: usize,
    kind: &str,
) -> Option<String> {
    let schemas = format.as_skiff()?.table_schemas().len();
    if schemas == tables {
        return None;
    }
    Some(format!(
        "{what} declares {}, but this operation has {}",
        plural(schemas, "Skiff table schema"),
        plural(tables, kind)
    ))
}

fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// The kind of operation to start.
///
/// All nine the cluster registers. Five have a spec builder here; `merge`,
/// `erase` and `remote_copy` gained one with this enum, and `join_reduce` did
/// not — see its variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// A map operation.
    Map,
    /// A map-reduce operation.
    MapReduce,
    /// A reduce operation over sorted input.
    Reduce,
    /// A sort operation.
    Sort,
    /// An operation with no input tables.
    Vanilla,
    /// A merge of several tables into one. See [`MergeSpec`].
    Merge,
    /// Deletion of rows from one table. See [`EraseSpec`].
    Erase,
    /// A copy of a table from another cluster. See [`RemoteCopySpec`].
    RemoteCopy,
    /// A reduce that joins foreign tables — **the older spelling**.
    ///
    /// There is no `JoinReduceSpec`, and that is deliberate. The cluster still
    /// registers the type, but the current documentation no longer lists it
    /// among `start_operation`'s `operation_type` values, and describes the same
    /// work as a [reduce with foreign
    /// tables](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce):
    /// a `reduce` whose spec carries `join_by` and `enable_key_guarantee=%false`.
    /// Build that with [`ReduceSpec::with_raw`]:
    ///
    /// ```
    /// use ytsaurus_client::{ReduceSpec, yson_build};
    ///
    /// let spec = ReduceSpec::new("./j", ["//tmp/primary"], ["//tmp/out"], ["host"])
    ///     .with_raw("join_by", yson_build::list([yson_build::string("host")]))
    ///     .with_raw("enable_key_guarantee", yson_build::boolean(false));
    /// ```
    ///
    /// The variant exists so a caller who *does* want the older type can name it
    /// through [`Client::start_operation`](crate::Client::start_operation),
    /// which is what the enum is for.
    JoinReduce,
}

impl OperationType {
    /// The wire name, as `start_operation` expects it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            OperationType::Map => "map",
            OperationType::MapReduce => "map_reduce",
            OperationType::Reduce => "reduce",
            OperationType::Sort => "sort",
            OperationType::Vanilla => "vanilla",
            OperationType::Merge => "merge",
            OperationType::Erase => "erase",
            OperationType::RemoteCopy => "remote_copy",
            OperationType::JoinReduce => "join_reduce",
        }
    }
}

/// The parts of a user-job spec shared by mappers and reducers.
#[derive(Debug, Clone)]
struct UserJob {
    command: String,
    /// Rendered `file_paths` entries. A YSON value rather than a string
    /// because a path may carry attributes — `<file_name="cat">//tmp/…` is how
    /// a file whose Cypress name is an MD5 hash appears in the sandbox under a
    /// name the command can actually run.
    files: Vec<YsonValue>,
    memory_limit: Option<i64>,
    environment: Vec<(String, String)>,
    input_format: DataFormat,
    output_format: DataFormat,
}

impl UserJob {
    fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            files: Vec::new(),
            memory_limit: None,
            environment: Vec::new(),
            input_format: DataFormat::binary_yson(),
            output_format: DataFormat::binary_yson(),
        }
    }

    fn with_formats(&mut self, input: DataFormat, output: DataFormat) {
        self.input_format = input;
        self.output_format = output;
    }

    fn to_yson(&self) -> YsonValue {
        let mut job = map([
            ("command", string(&self.command)),
            // Both directions default to binary YSON. `with_formats` replaces
            // them together, so worker and operation cannot drift.
            ("input_format", self.input_format.to_yson()),
            ("output_format", self.output_format.to_yson()),
        ]);

        if !self.files.is_empty() {
            insert(&mut job, "file_paths", list(self.files.iter().cloned()));
        }
        if let Some(limit) = self.memory_limit {
            insert(&mut job, "memory_limit", int(limit));
        }
        if !self.environment.is_empty() {
            insert(
                &mut job,
                "environment",
                map(self
                    .environment
                    .iter()
                    .map(|(k, v)| (k.as_str(), string(v)))),
            );
        }
        job
    }
}

/// A map operation.
///
/// ```
/// use ytsaurus_client::MapSpec;
///
/// let spec = MapSpec::new("./cat", ["//tmp/in"], ["//tmp/out"])
///     .with_local_file("//tmp/cat")
///     .with_memory_limit(512 * 1024 * 1024);
/// ```
#[derive(Debug, Clone)]
pub struct MapSpec {
    mapper: UserJob,
    inputs: Vec<String>,
    outputs: Vec<String>,
    job_count: Option<i64>,
    input_table_index: bool,
    extra: Vec<(String, YsonValue)>,
}

impl MapSpec {
    /// A map running `command` over `inputs`, writing `outputs`.
    #[must_use]
    pub fn new<I, O>(command: impl Into<String>, inputs: I, outputs: O) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
        O: IntoIterator,
        O::Item: Into<String>,
    {
        Self {
            mapper: UserJob::new(command),
            inputs: inputs.into_iter().map(Into::into).collect(),
            outputs: outputs.into_iter().map(Into::into).collect(),
            job_count: None,
            input_table_index: false,
            extra: Vec::new(),
        }
    }

    /// Adds a Cypress file the job needs — normally the worker binary.
    #[must_use]
    pub fn with_local_file(mut self, path: impl Into<String>) -> Self {
        self.mapper.files.push(string(path.into()));
        self
    }

    /// Adds a Cypress file under a different name in the job's sandbox.
    ///
    /// The name matters because the job runs a *command*: a file cached under
    /// its MD5 hash arrives as `4c8f…`, and `./my_job` would not find it.
    #[must_use]
    pub fn with_local_file_named(mut self, path: impl Into<String>, name: impl AsRef<str>) -> Self {
        self.mapper.files.push(named_file(path, name));
        self
    }

    /// Sets the mapper's memory limit, in bytes.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: i64) -> Self {
        self.mapper.memory_limit = Some(bytes);
        self
    }

    /// Selects the mapper's input and output data formats.
    ///
    /// YSON selections apply to every table. A Skiff selection must contain one
    /// table schema per corresponding input or output table, in the same order.
    /// The default remains binary YSON.
    #[must_use]
    pub fn with_formats(mut self, input: DataFormat, output: DataFormat) -> Self {
        self.mapper.with_formats(input, output);
        self
    }

    /// Uses validated Skiff formats for the mapper's input and output streams.
    ///
    /// This compatibility convenience delegates to [`Self::with_formats`].
    #[must_use]
    pub fn with_skiff_formats(self, input: SkiffFormat, output: SkiffFormat) -> Self {
        self.with_formats(DataFormat::skiff(input), DataFormat::skiff(output))
    }

    /// Sets an environment variable for the job, e.g. `RUST_BACKTRACE`.
    #[must_use]
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.mapper.environment.push((key.into(), value.into()));
        self
    }

    /// Asks for the input table index to be delivered with each row.
    ///
    /// Without this, `Row::table_index` is always 0.
    #[must_use]
    pub fn with_input_table_index(mut self) -> Self {
        self.input_table_index = true;
        self
    }

    /// Requests a specific job count.
    #[must_use]
    pub fn with_job_count(mut self, count: i64) -> Self {
        self.job_count = Some(count);
        self
    }

    /// Describes a Skiff format that does not match this spec's table lists.
    ///
    /// A Skiff format needs one table schema per table, in order. Get the count
    /// wrong and the operation is submitted anyway: the cluster may refuse it,
    /// or the job may read a table the format does not describe and fail
    /// mid-stream, after it has already written output.
    /// [`Client::start_map`](crate::Client::start_map) checks this before
    /// sending the spec; check it here if you render the spec yourself with
    /// [`MapSpec::to_yson`].
    #[must_use]
    pub fn skiff_table_mismatch(&self) -> Option<String> {
        skiff_table_mismatch(
            "the mapper's input_format",
            &self.mapper.input_format,
            self.inputs.len(),
            "input table",
        )
        .or_else(|| {
            skiff_table_mismatch(
                "the mapper's output_format",
                &self.mapper.output_format,
                self.outputs.len(),
                "output table",
            )
        })
    }

    /// Sets any spec field this builder does not model.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut mapper = self.mapper.to_yson();
        if self.input_table_index {
            insert(&mut mapper, "enable_input_table_index", boolean(true));
        }

        let mut spec = map([
            ("mapper", mapper),
            ("input_table_paths", list(self.inputs.iter().map(string))),
            ("output_table_paths", list(self.outputs.iter().map(string))),
        ]);

        if let Some(count) = self.job_count {
            insert(&mut spec, "job_count", int(count));
        }
        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

/// A map-reduce operation.
///
/// The mapper is optional: without one, the input is fed straight to the
/// reducer, which is how YTsaurus models a plain shuffle-and-reduce.
#[derive(Debug, Clone)]
pub struct MapReduceSpec {
    mapper: Option<UserJob>,
    /// The mapper's formats, for the same reason and by the same route as the
    /// files below: the mapper may not exist yet when they are chosen. Keeping
    /// them here and applying them at render time is what makes
    /// `with_mapper_formats` before `with_mapper` mean what it says, without a
    /// second copy on the phase that both methods would have to keep in step.
    mapper_formats: Option<(DataFormat, DataFormat)>,
    reducer: UserJob,
    /// Files and the memory limit are promised "to both phases", so they live
    /// on the spec and reach each phase at render time. Holding them on the
    /// phases instead would make `with_local_file` before `with_mapper` a
    /// silently different program from the same calls the other way round.
    files: Vec<YsonValue>,
    memory_limit: Option<i64>,
    inputs: Vec<String>,
    outputs: Vec<String>,
    reduce_by: Vec<String>,
    sort_by: Vec<String>,
    key_switch: bool,
    extra: Vec<(String, YsonValue)>,
}

impl MapReduceSpec {
    /// A map-reduce running `reducer` over `inputs`, grouped by `reduce_by`.
    #[must_use]
    pub fn new<I, O, K>(reducer: impl Into<String>, inputs: I, outputs: O, reduce_by: K) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
        O: IntoIterator,
        O::Item: Into<String>,
        K: IntoIterator,
        K::Item: Into<String>,
    {
        Self {
            mapper: None,
            mapper_formats: None,
            reducer: UserJob::new(reducer),
            files: Vec::new(),
            memory_limit: None,
            inputs: inputs.into_iter().map(Into::into).collect(),
            outputs: outputs.into_iter().map(Into::into).collect(),
            reduce_by: reduce_by.into_iter().map(Into::into).collect(),
            sort_by: Vec::new(),
            // On by default: a reducer built on `JobReader::groups` is wrong
            // without it, and silently so — every key collapses into one group.
            key_switch: true,
            extra: Vec::new(),
        }
    }

    /// Adds a mapper phase.
    #[must_use]
    pub fn with_mapper(mut self, command: impl Into<String>) -> Self {
        self.mapper = Some(UserJob::new(command));
        self
    }

    /// Selects the mapper phase's input and output data formats.
    ///
    /// A Skiff format must contain one schema per corresponding table. It may
    /// be called before or after [`Self::with_mapper`].
    #[must_use]
    pub fn with_mapper_formats(mut self, input: DataFormat, output: DataFormat) -> Self {
        self.mapper_formats = Some((input, output));
        self
    }

    /// Uses validated Skiff formats for the mapper phase.
    ///
    /// This compatibility convenience delegates to [`Self::with_mapper_formats`].
    #[must_use]
    pub fn with_mapper_skiff_formats(self, input: SkiffFormat, output: SkiffFormat) -> Self {
        self.with_mapper_formats(DataFormat::skiff(input), DataFormat::skiff(output))
    }

    /// Selects the reducer phase's input and output data formats.
    ///
    /// A Skiff format must contain one schema per corresponding table, in the
    /// order YTsaurus uses for that phase.
    #[must_use]
    pub fn with_reducer_formats(mut self, input: DataFormat, output: DataFormat) -> Self {
        self.reducer.with_formats(input, output);
        self
    }

    /// Uses validated Skiff formats for the reducer phase.
    ///
    /// This compatibility convenience delegates to [`Self::with_reducer_formats`].
    #[must_use]
    pub fn with_reducer_skiff_formats(self, input: SkiffFormat, output: SkiffFormat) -> Self {
        self.with_reducer_formats(DataFormat::skiff(input), DataFormat::skiff(output))
    }

    /// Adds a Cypress file to both phases.
    ///
    /// One binary usually serves both, dispatching on `argv[1]`, so attaching
    /// it to each phase separately would only be a way to forget one. Order
    /// relative to [`MapReduceSpec::with_mapper`] does not matter: files are
    /// handed to the phases when the spec is rendered.
    #[must_use]
    pub fn with_local_file(self, path: impl Into<String>) -> Self {
        self.attach(string(path.into()))
    }

    /// Adds a Cypress file to both phases under a different sandbox name.
    ///
    /// See [`MapSpec::with_local_file_named`] for why the name matters.
    #[must_use]
    pub fn with_local_file_named(self, path: impl Into<String>, name: impl AsRef<str>) -> Self {
        self.attach(named_file(path, name))
    }

    fn attach(mut self, file: YsonValue) -> Self {
        self.files.push(file);
        self
    }

    /// Sets the memory limit for both phases, in bytes.
    ///
    /// As with the files, order relative to [`MapReduceSpec::with_mapper`]
    /// does not matter.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: i64) -> Self {
        self.memory_limit = Some(bytes);
        self
    }

    /// A phase's job spec, with the spec-level settings applied.
    fn phase(&self, job: &UserJob, formats: Option<&(DataFormat, DataFormat)>) -> YsonValue {
        let mut job = job.clone();
        if let Some((input, output)) = formats {
            job.with_formats(input.clone(), output.clone());
        }
        job.files.extend(self.files.iter().cloned());
        if job.memory_limit.is_none() {
            job.memory_limit = self.memory_limit;
        }
        job.to_yson()
    }

    /// Sets the sort columns, when they differ from the reduce columns.
    #[must_use]
    pub fn with_sort_by<K>(mut self, columns: K) -> Self
    where
        K: IntoIterator,
        K::Item: Into<String>,
    {
        self.sort_by = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Turns off `key_switch` delivery to the reducer.
    ///
    /// Only useful for a reducer that does not group — with it off,
    /// `JobReader::groups` sees the whole input as one group.
    #[must_use]
    pub fn without_key_switch(mut self) -> Self {
        self.key_switch = false;
        self
    }

    /// Describes a Skiff format that does not match this spec's table lists.
    ///
    /// Only the counts this builder can know. What the mapper writes and what
    /// the reducer reads are shuffle streams, and how the output tables are
    /// split between the phases depends on `mapper_output_table_count`, which
    /// this builder does not model — a spec that sets it through
    /// [`Self::with_raw`] therefore has its output side left to the cluster
    /// rather than guessed at. The Go SDK declines to check its reduce phase
    /// for the same reason.
    ///
    /// See [`MapSpec::skiff_table_mismatch`] for what an unchecked mismatch
    /// costs. [`Client::start_map_reduce`](crate::Client::start_map_reduce)
    /// checks this before sending the spec.
    #[must_use]
    pub fn skiff_table_mismatch(&self) -> Option<String> {
        let split_outputs = self
            .extra
            .iter()
            .any(|(key, _)| key == "mapper_output_table_count");

        self.mapper
            .as_ref()
            .and(self.mapper_formats.as_ref())
            .and_then(|(input, _)| {
                skiff_table_mismatch(
                    "the mapper's input_format",
                    input,
                    self.inputs.len(),
                    "input table",
                )
            })
            .or_else(|| {
                if split_outputs {
                    return None;
                }
                skiff_table_mismatch(
                    "the reducer's output_format",
                    &self.reducer.output_format,
                    self.outputs.len(),
                    "output table",
                )
            })
    }

    /// Sets any spec field this builder does not model.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut spec = map([
            ("reducer", self.phase(&self.reducer, None)),
            ("input_table_paths", list(self.inputs.iter().map(string))),
            ("output_table_paths", list(self.outputs.iter().map(string))),
            ("reduce_by", list(self.reduce_by.iter().map(string))),
        ]);

        if let Some(mapper) = &self.mapper {
            insert(
                &mut spec,
                "mapper",
                self.phase(mapper, self.mapper_formats.as_ref()),
            );
        }

        let sort_by = if self.sort_by.is_empty() {
            &self.reduce_by
        } else {
            &self.sort_by
        };
        insert(&mut spec, "sort_by", list(sort_by.iter().map(string)));

        if self.key_switch {
            // An operation with several job types gives each type its own I/O
            // section, so this is `reduce_job_io` and NOT `job_io`. Using
            // `job_io` here is accepted and silently ignored, and the reducer
            // then sees no key switches at all.
            insert(
                &mut spec,
                "reduce_job_io",
                map([(
                    "control_attributes",
                    map([("enable_key_switch", boolean(true))]),
                )]),
            );
        }

        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

/// A reduce operation over already-sorted input.
///
/// Every input table must already be sorted by a column set that *starts with*
/// `reduce_by` — [`SortSpec`] is how a table gets that way. When it is,
/// this is the operation to reach for: a map-reduce over the same data would
/// pay for a shuffle that has already been done.
///
/// ```
/// use ytsaurus_client::ReduceSpec;
///
/// let spec = ReduceSpec::new("./wordcount reduce", ["//tmp/sorted"], ["//tmp/counts"], ["word"])
///     .with_local_file("//tmp/wordcount");
/// ```
#[derive(Debug, Clone)]
pub struct ReduceSpec {
    reducer: UserJob,
    inputs: Vec<String>,
    outputs: Vec<String>,
    reduce_by: Vec<String>,
    sort_by: Vec<String>,
    job_count: Option<i64>,
    key_switch: bool,
    input_table_index: bool,
    extra: Vec<(String, YsonValue)>,
}

impl ReduceSpec {
    /// A reduce running `command` over `inputs`, grouped by `reduce_by`.
    #[must_use]
    pub fn new<I, O, K>(command: impl Into<String>, inputs: I, outputs: O, reduce_by: K) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
        O: IntoIterator,
        O::Item: Into<String>,
        K: IntoIterator,
        K::Item: Into<String>,
    {
        Self {
            reducer: UserJob::new(command),
            inputs: inputs.into_iter().map(Into::into).collect(),
            outputs: outputs.into_iter().map(Into::into).collect(),
            reduce_by: reduce_by.into_iter().map(Into::into).collect(),
            sort_by: Vec::new(),
            job_count: None,
            // As for map-reduce: a reducer built on `JobReader::groups` is
            // wrong without it, and silently so.
            key_switch: true,
            input_table_index: false,
            extra: Vec::new(),
        }
    }

    /// Adds a Cypress file the job needs — normally the worker binary.
    #[must_use]
    pub fn with_local_file(mut self, path: impl Into<String>) -> Self {
        self.reducer.files.push(string(path.into()));
        self
    }

    /// Adds a Cypress file under a different name in the job's sandbox.
    ///
    /// See [`MapSpec::with_local_file_named`] for why the name matters.
    #[must_use]
    pub fn with_local_file_named(mut self, path: impl Into<String>, name: impl AsRef<str>) -> Self {
        self.reducer.files.push(named_file(path, name));
        self
    }

    /// Sets the reducer's memory limit, in bytes.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: i64) -> Self {
        self.reducer.memory_limit = Some(bytes);
        self
    }

    /// Selects the reducer's input and output data formats.
    ///
    /// YSON selections apply to every table. A Skiff selection must contain one
    /// table schema per corresponding input or output table, in the same order.
    /// The default remains binary YSON.
    ///
    /// A Skiff reducer receives its key switch as a `$key_switch` boolean
    /// column rather than as a YSON control record, so the input schema has to
    /// declare that column for `ytsaurus-job`'s `SkiffJobReader` to report it —
    /// `enable_key_switch` asks the cluster to deliver key switches, and the
    /// format decides how they arrive. A schema without the column leaves a
    /// grouping reducer seeing one group, exactly as
    /// [`Self::without_key_switch`] would.
    #[must_use]
    pub fn with_formats(mut self, input: DataFormat, output: DataFormat) -> Self {
        self.reducer.with_formats(input, output);
        self
    }

    /// Uses validated Skiff formats for the reducer's input and output streams.
    ///
    /// This compatibility convenience delegates to [`Self::with_formats`].
    #[must_use]
    pub fn with_skiff_formats(self, input: SkiffFormat, output: SkiffFormat) -> Self {
        self.with_formats(DataFormat::skiff(input), DataFormat::skiff(output))
    }

    /// Sets an environment variable for the job, e.g. `RUST_BACKTRACE`.
    #[must_use]
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.reducer.environment.push((key.into(), value.into()));
        self
    }

    /// Sets the columns the input is sorted by, when they differ from
    /// `reduce_by`.
    ///
    /// `reduce_by` must be a prefix of them. Saying so asks the cluster to
    /// check the input really is sorted that way, and guarantees the order rows
    /// arrive in within a group.
    #[must_use]
    pub fn with_sort_by<K>(mut self, columns: K) -> Self
    where
        K: IntoIterator,
        K::Item: Into<String>,
    {
        self.sort_by = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Requests a specific job count.
    #[must_use]
    pub fn with_job_count(mut self, count: i64) -> Self {
        self.job_count = Some(count);
        self
    }

    /// Asks for the input table index to be delivered with each row.
    ///
    /// Reduce merges several sorted tables into one stream, so this is how a
    /// job tells which table a row came from.
    #[must_use]
    pub fn with_input_table_index(mut self) -> Self {
        self.input_table_index = true;
        self
    }

    /// Turns off `key_switch` delivery to the reducer.
    #[must_use]
    pub fn without_key_switch(mut self) -> Self {
        self.key_switch = false;
        self
    }

    /// Describes a Skiff format that does not match this spec's table lists.
    ///
    /// A reduce merges its input tables into one sorted stream but keeps them
    /// distinguishable, so the input format describes every input table, as the
    /// Go SDK's `setupSkiffInputFormat` also requires. See
    /// [`MapSpec::skiff_table_mismatch`] for what an unchecked mismatch costs.
    /// [`Client::start_reduce`](crate::Client::start_reduce) checks this before
    /// sending the spec.
    #[must_use]
    pub fn skiff_table_mismatch(&self) -> Option<String> {
        skiff_table_mismatch(
            "the reducer's input_format",
            &self.reducer.input_format,
            self.inputs.len(),
            "input table",
        )
        .or_else(|| {
            skiff_table_mismatch(
                "the reducer's output_format",
                &self.reducer.output_format,
                self.outputs.len(),
                "output table",
            )
        })
    }

    /// Sets any spec field this builder does not model.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut reducer = self.reducer.to_yson();
        if self.input_table_index {
            insert(&mut reducer, "enable_input_table_index", boolean(true));
        }

        let mut spec = map([
            ("reducer", reducer),
            ("input_table_paths", list(self.inputs.iter().map(string))),
            ("output_table_paths", list(self.outputs.iter().map(string))),
            ("reduce_by", list(self.reduce_by.iter().map(string))),
        ]);

        if !self.sort_by.is_empty() {
            insert(&mut spec, "sort_by", list(self.sort_by.iter().map(string)));
        }
        if let Some(count) = self.job_count {
            insert(&mut spec, "job_count", int(count));
        }

        if self.key_switch {
            // `job_io`, not `reduce_job_io`: a reduce has one job type, so it
            // has one I/O section. This is the same trap as on map-reduce, in
            // the other direction — the wrong spelling is accepted and ignored,
            // and the reducer then sees the whole input as a single group.
            insert(
                &mut spec,
                "job_io",
                map([(
                    "control_attributes",
                    map([("enable_key_switch", boolean(true))]),
                )]),
            );
        }

        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

/// A sort operation.
///
/// There is no user job: the cluster does the sorting. Its result is a sorted
/// table, which is what [`ReduceSpec`] needs.
///
/// ```
/// use ytsaurus_client::SortSpec;
///
/// let spec = SortSpec::new(["//tmp/unsorted"], "//tmp/sorted", ["word"]);
/// ```
#[derive(Debug, Clone)]
pub struct SortSpec {
    inputs: Vec<String>,
    output: String,
    sort_by: Vec<String>,
    extra: Vec<(String, YsonValue)>,
}

impl SortSpec {
    /// Sorts `inputs` into `output`, ordered by `sort_by`.
    ///
    /// Note the single output: sort writes one table, however many it reads.
    #[must_use]
    pub fn new<I, K>(inputs: I, output: impl Into<String>, sort_by: K) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
        K: IntoIterator,
        K::Item: Into<String>,
    {
        Self {
            inputs: inputs.into_iter().map(Into::into).collect(),
            output: output.into(),
            sort_by: sort_by.into_iter().map(Into::into).collect(),
            extra: Vec::new(),
        }
    }

    /// Sets any spec field this builder does not model — `partition_count`,
    /// `data_size_per_partition_job` and the rest of the sort tuning knobs.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut spec = map([
            ("input_table_paths", list(self.inputs.iter().map(string))),
            // Singular, and a string rather than a list: sort has exactly one
            // output. `output_table_paths` here is rejected by the cluster.
            ("output_table_path", string(&self.output)),
            ("sort_by", list(self.sort_by.iter().map(string))),
        ]);

        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

/// How a merge combines its inputs.
///
/// Reference:
/// <https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/merge>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// Rows in no particular order. The cluster's own default, and the cheapest.
    Unordered,
    /// Rows in the order of the input tables, each table's order preserved.
    Ordered,
    /// A sorted merge of sorted inputs, producing a sorted table.
    ///
    /// Needs `merge_by`, and the inputs must already be sorted by it.
    Sorted,
}

impl MergeMode {
    /// The wire name, as the spec's `mode` expects it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MergeMode::Unordered => "unordered",
            MergeMode::Ordered => "ordered",
            MergeMode::Sorted => "sorted",
        }
    }
}

/// A merge operation: several tables into one, with no user job.
///
/// What a sort does for order, a merge does for chunk layout — and in
/// [`MergeMode::Sorted`] it is the cheap way to combine tables that are already
/// sorted, because nothing has to be sorted again.
///
/// ```
/// use ytsaurus_client::{MergeMode, MergeSpec};
///
/// let spec = MergeSpec::new(["//tmp/monday", "//tmp/tuesday"], "//tmp/week")
///     .with_mode(MergeMode::Sorted)
///     .with_merge_by(["host"]);
/// ```
#[derive(Debug, Clone)]
pub struct MergeSpec {
    inputs: Vec<String>,
    output: String,
    mode: MergeMode,
    merge_by: Vec<String>,
    combine_chunks: Option<bool>,
    force_transform: Option<bool>,
    job_count: Option<i64>,
    extra: Vec<(String, YsonValue)>,
}

impl MergeSpec {
    /// Merges `inputs` into `output`, unordered.
    #[must_use]
    pub fn new<I>(inputs: I, output: impl Into<String>) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        Self {
            inputs: inputs.into_iter().map(Into::into).collect(),
            output: output.into(),
            mode: MergeMode::Unordered,
            merge_by: Vec::new(),
            combine_chunks: None,
            force_transform: None,
            job_count: None,
            extra: Vec::new(),
        }
    }

    /// Chooses how the inputs are combined.
    #[must_use]
    pub fn with_mode(mut self, mode: MergeMode) -> Self {
        self.mode = mode;
        self
    }

    /// The columns a [`MergeMode::Sorted`] merge merges by.
    ///
    /// The output table comes back sorted by these.
    #[must_use]
    pub fn with_merge_by<K>(mut self, columns: K) -> Self
    where
        K: IntoIterator,
        K::Item: Into<String>,
    {
        self.merge_by = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Asks the cluster to combine small chunks while it merges.
    ///
    /// This is most of why a merge is worth running on one table: a table
    /// written in many small pieces reads faster afterwards.
    #[must_use]
    pub fn with_combine_chunks(mut self, combine: bool) -> Self {
        self.combine_chunks = Some(combine);
        self
    }

    /// Runs the jobs even when the merge could be done by moving chunks.
    ///
    /// A merge that has nothing to do normally just relinks chunks. Set this
    /// when the point is the *rewrite* — a change of compression codec or
    /// erasure coding, which only happens where rows are actually copied.
    #[must_use]
    pub fn with_force_transform(mut self, force: bool) -> Self {
        self.force_transform = Some(force);
        self
    }

    /// Asks for a particular number of jobs.
    ///
    /// Takes precedence over `data_size_per_job`, which the cluster otherwise
    /// uses to decide.
    #[must_use]
    pub fn with_job_count(mut self, count: i64) -> Self {
        self.job_count = Some(count);
        self
    }

    /// Whether this spec asks for a sorted merge without saying by what.
    ///
    /// [`Client::start_merge`](crate::Client::start_merge) refuses one, for the
    /// reason [`VanillaSpec::duplicate_task`] is checked before the request goes
    /// out: the cluster's own refusal arrives later and says less.
    #[must_use]
    pub fn needs_merge_by(&self) -> bool {
        self.mode == MergeMode::Sorted && self.merge_by.is_empty()
    }

    /// Sets any spec field this builder does not model — `data_size_per_job`,
    /// `schema_inference_mode` and the rest.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut spec = map([
            ("input_table_paths", list(self.inputs.iter().map(string))),
            // Singular, as in a sort: a merge writes one table.
            ("output_table_path", string(&self.output)),
            ("mode", string(self.mode.as_str())),
        ]);

        if !self.merge_by.is_empty() {
            insert(
                &mut spec,
                "merge_by",
                list(self.merge_by.iter().map(string)),
            );
        }
        if let Some(combine) = self.combine_chunks {
            insert(&mut spec, "combine_chunks", boolean(combine));
        }
        if let Some(force) = self.force_transform {
            insert(&mut spec, "force_transform", boolean(force));
        }
        if let Some(count) = self.job_count {
            insert(&mut spec, "job_count", int(count));
        }

        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

/// An erase operation: rows out of one table, in place.
///
/// **The rows to delete are named by the path**, as a row range —
/// `//tmp/log[#10:#100]` — and a path with no range erases every row while
/// leaving the table and its schema where they are.
///
/// ```
/// use ytsaurus_client::EraseSpec;
///
/// let all = EraseSpec::new("//tmp/log");
/// let first_ten = EraseSpec::new("//tmp/log[#0:#10]");
/// ```
#[derive(Debug, Clone)]
pub struct EraseSpec {
    table: String,
    combine_chunks: Option<bool>,
    extra: Vec<(String, YsonValue)>,
}

impl EraseSpec {
    /// Erases the rows `table` names.
    ///
    /// Ranges are written into the path itself: [`TablePath`](crate::TablePath)
    /// models the write-side attributes, not read-side ranges, so this takes the
    /// path as text.
    #[must_use]
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            combine_chunks: None,
            extra: Vec::new(),
        }
    }

    /// Asks the cluster to combine what is left into larger chunks.
    #[must_use]
    pub fn with_combine_chunks(mut self, combine: bool) -> Self {
        self.combine_chunks = Some(combine);
        self
    }

    /// Sets any spec field this builder does not model.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        // `table_path`, not `input_table_paths`: erase reads and writes the same
        // table, and names it once.
        let mut spec = map([("table_path", string(&self.table))]);

        if let Some(combine) = self.combine_chunks {
            insert(&mut spec, "combine_chunks", boolean(combine));
        }
        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

/// A remote-copy operation: a table from another cluster onto this one.
///
/// The only operation whose input lives somewhere else. `cluster_name` is the
/// **source**, as this cluster's configuration names it; the operation runs
/// here, and the output path is here too.
///
/// ```
/// use ytsaurus_client::RemoteCopySpec;
///
/// let spec = RemoteCopySpec::new("hahn", ["//tmp/theirs"], "//tmp/ours")
///     .with_copy_attributes(true);
/// ```
#[derive(Debug, Clone)]
pub struct RemoteCopySpec {
    cluster_name: String,
    inputs: Vec<String>,
    output: String,
    network_name: Option<String>,
    copy_attributes: Option<bool>,
    attribute_keys: Vec<String>,
    extra: Vec<(String, YsonValue)>,
}

impl RemoteCopySpec {
    /// Copies `inputs` from the cluster `cluster_name` into `output` here.
    #[must_use]
    pub fn new<I>(cluster_name: impl Into<String>, inputs: I, output: impl Into<String>) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        Self {
            cluster_name: cluster_name.into(),
            inputs: inputs.into_iter().map(Into::into).collect(),
            output: output.into(),
            network_name: None,
            copy_attributes: None,
            attribute_keys: Vec::new(),
            extra: Vec::new(),
        }
    }

    /// Uses a named network to reach the source cluster.
    ///
    /// Installations that separate networks need this; one that does not
    /// answers fine without it.
    #[must_use]
    pub fn with_network_name(mut self, network: impl Into<String>) -> Self {
        self.network_name = Some(network.into());
        self
    }

    /// Copies the source table's attributes along with its rows.
    ///
    /// Off in the cluster's default, so a copy otherwise arrives with the rows
    /// and none of what was said about them.
    #[must_use]
    pub fn with_copy_attributes(mut self, copy: bool) -> Self {
        self.copy_attributes = Some(copy);
        self
    }

    /// Copies only these attributes, rather than all of them.
    ///
    /// Only meaningful with [`RemoteCopySpec::with_copy_attributes`].
    #[must_use]
    pub fn with_attribute_keys<K>(mut self, keys: K) -> Self
    where
        K: IntoIterator,
        K::Item: Into<String>,
    {
        self.attribute_keys = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Sets any spec field this builder does not model — `cluster_connection`,
    /// `schema_inference_mode`, `allow_unfrozen_input_tables`.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut spec = map([
            ("cluster_name", string(&self.cluster_name)),
            ("input_table_paths", list(self.inputs.iter().map(string))),
            ("output_table_path", string(&self.output)),
        ]);

        if let Some(network) = &self.network_name {
            insert(&mut spec, "network_name", string(network));
        }
        if let Some(copy) = self.copy_attributes {
            insert(&mut spec, "copy_attributes", boolean(copy));
        }
        if !self.attribute_keys.is_empty() {
            insert(
                &mut spec,
                "attribute_keys",
                list(self.attribute_keys.iter().map(string)),
            );
        }

        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

/// One task of a vanilla operation: a group of identical jobs.
///
/// Tasks are what makes a vanilla operation a distributed process rather than
/// one program: each task says how many jobs of its kind to run, and the
/// scheduler keeps that many going.
#[derive(Debug, Clone)]
pub struct VanillaTask {
    name: String,
    job: UserJob,
    job_count: i64,
    outputs: Vec<String>,
    extra: Vec<(String, YsonValue)>,
}

impl VanillaTask {
    /// `job_count` jobs running `command`, under the name `name`.
    ///
    /// The name shows up in the web interface and in the operation's progress,
    /// so `lowercase_with_underscores` and short is the convention.
    #[must_use]
    pub fn new(name: impl Into<String>, command: impl Into<String>, job_count: i64) -> Self {
        Self {
            name: name.into(),
            job: UserJob::new(command),
            job_count,
            outputs: Vec::new(),
            extra: Vec::new(),
        }
    }

    /// Adds a Cypress file the jobs need — normally the worker binary.
    #[must_use]
    pub fn with_local_file(mut self, path: impl Into<String>) -> Self {
        self.job.files.push(string(path.into()));
        self
    }

    /// Adds a Cypress file under a different name in the sandbox.
    ///
    /// See [`MapSpec::with_local_file_named`].
    #[must_use]
    pub fn with_local_file_named(mut self, path: impl Into<String>, name: impl AsRef<str>) -> Self {
        self.job.files.push(named_file(path, name));
        self
    }

    /// Sets the tables these jobs write.
    ///
    /// A vanilla task has no input, but it may have output: table `k` arrives
    /// on the same `3k + 1` descriptor as anywhere else.
    #[must_use]
    pub fn with_outputs<O>(mut self, paths: O) -> Self
    where
        O: IntoIterator,
        O::Item: Into<String>,
    {
        self.outputs = paths.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the memory limit for these jobs, in bytes.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: i64) -> Self {
        self.job.memory_limit = Some(bytes);
        self
    }

    /// Selects the data format these jobs write.
    ///
    /// Only the output direction, because a vanilla task has no input: there is
    /// no input table for an input format to describe, and the one this spec
    /// sends stays at the binary YSON every vanilla operation here has run
    /// with. A Skiff selection must contain one table schema per output table
    /// set by [`Self::with_outputs`], in the same order.
    #[must_use]
    pub fn with_output_format(mut self, output: DataFormat) -> Self {
        self.job.output_format = output;
        self
    }

    /// Uses a validated Skiff format for these jobs' output streams.
    ///
    /// This compatibility convenience delegates to [`Self::with_output_format`].
    #[must_use]
    pub fn with_skiff_output_format(self, output: SkiffFormat) -> Self {
        self.with_output_format(DataFormat::skiff(output))
    }

    /// Sets an environment variable for these jobs.
    #[must_use]
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.job.environment.push((key.into(), value.into()));
        self
    }

    /// Sets any task field this builder does not model — `gang_options` for a
    /// coordinated distributed process, for instance.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    fn to_yson(&self) -> YsonValue {
        let mut task = self.job.to_yson();
        insert(&mut task, "job_count", int(self.job_count));
        // Always sent, even when empty: the field is how the task says it has
        // no output tables, and a task with none is perfectly ordinary.
        insert(
            &mut task,
            "output_table_paths",
            list(self.outputs.iter().map(string)),
        );

        for (key, value) in &self.extra {
            insert(&mut task, key, value.clone());
        }
        task
    }
}

/// A vanilla operation: jobs with no input tables.
///
/// This is the shape for work that is not a transformation of a table — a
/// side-car computation, a distributed process, a job that fetches its own
/// input. Coordination between the jobs is the user's problem; the cluster's
/// side of the bargain is keeping `job_count` of them running.
///
/// ```
/// use ytsaurus_client::{VanillaSpec, VanillaTask};
///
/// let spec = VanillaSpec::new(
///     VanillaTask::new("worker", "./my_job", 4)
///         .with_local_file("//tmp/my_job")
///         .with_outputs(["//tmp/results"]),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct VanillaSpec {
    tasks: Vec<VanillaTask>,
    extra: Vec<(String, YsonValue)>,
}

impl VanillaSpec {
    /// An operation with one task.
    #[must_use]
    pub fn new(task: VanillaTask) -> Self {
        Self {
            tasks: vec![task],
            extra: Vec::new(),
        }
    }

    /// Adds another task, of a different kind.
    ///
    /// It needs a different *name* too — see [`VanillaSpec::duplicate_task`].
    #[must_use]
    pub fn with_task(mut self, task: VanillaTask) -> Self {
        self.tasks.push(task);
        self
    }

    /// The name two tasks share, if any.
    ///
    /// The spec keys its tasks by name, so two tasks called the same thing are
    /// one task: the later one replaces the earlier, and the operation quietly
    /// runs half the work it was handed — it completes, so nothing anywhere
    /// reports a problem. [`Client::start_vanilla`](crate::Client::start_vanilla)
    /// checks this before sending the spec; check it here if you render the
    /// spec yourself with [`VanillaSpec::to_yson`].
    #[must_use]
    pub fn duplicate_task(&self) -> Option<&str> {
        let mut seen = std::collections::HashSet::new();
        self.tasks
            .iter()
            .find(|task| !seen.insert(task.name.as_str()))
            .map(|task| task.name.as_str())
    }

    /// Describes a task whose Skiff output format does not match its outputs.
    ///
    /// See [`MapSpec::skiff_table_mismatch`] for what an unchecked mismatch
    /// costs. [`Client::start_vanilla`](crate::Client::start_vanilla) checks
    /// this before sending the spec.
    #[must_use]
    pub fn skiff_table_mismatch(&self) -> Option<String> {
        self.tasks.iter().find_map(|task| {
            skiff_table_mismatch(
                &format!("task {:?}'s output_format", task.name),
                &task.job.output_format,
                task.outputs.len(),
                "output table",
            )
        })
    }

    /// Sets any spec field this builder does not model.
    #[must_use]
    pub fn with_raw(mut self, key: impl Into<String>, value: YsonValue) -> Self {
        self.extra.push((key.into(), value));
        self
    }

    /// Renders the spec.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut spec = map([(
            "tasks",
            map(self
                .tasks
                .iter()
                .map(|task| (task.name.as_str(), task.to_yson()))),
        )]);

        for (key, value) in &self.extra {
            insert(&mut spec, key, value.clone());
        }
        spec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_skiff::{Schema, SchemaRef, WireType};
    use ytsaurus_yson::{YsonFormat, to_string};

    fn render(v: &YsonValue) -> String {
        to_string(v, YsonFormat::Text).expect("encodes")
    }

    fn skiff_format(column: &str) -> SkiffFormat {
        SkiffFormat::new(vec![SchemaRef::Inline(Schema::tuple([Schema::named(
            column,
            WireType::Uint64,
        )]))])
        .expect("a named tuple is a table schema")
    }

    fn skiff_tables(columns: &[&str]) -> SkiffFormat {
        SkiffFormat::new(
            columns
                .iter()
                .map(|column| {
                    SchemaRef::Inline(Schema::tuple([Schema::named(*column, WireType::Uint64)]))
                })
                .collect(),
        )
        .expect("named tuples are table schemas")
    }

    #[test]
    fn a_map_skiff_format_needs_one_schema_per_table() {
        let two_in_one_out = MapSpec::new("./w", ["//a", "//b"], ["//out"]);

        let short_input = two_in_one_out
            .clone()
            .with_skiff_formats(skiff_tables(&["source"]), skiff_tables(&["result"]));
        let reason = short_input
            .skiff_table_mismatch()
            .expect("one schema cannot describe two input tables");
        assert!(reason.contains("input_format"), "{reason}");
        assert!(reason.contains("1 Skiff table schema,"), "{reason}");
        assert!(reason.contains("2 input tables"), "{reason}");

        let long_output = two_in_one_out
            .clone()
            .with_skiff_formats(skiff_tables(&["a", "b"]), skiff_tables(&["x", "y"]));
        let reason = long_output
            .skiff_table_mismatch()
            .expect("two schemas cannot describe one output table");
        assert!(reason.contains("output_format"), "{reason}");

        assert!(
            two_in_one_out
                .clone()
                .with_skiff_formats(skiff_tables(&["a", "b"]), skiff_tables(&["x"]))
                .skiff_table_mismatch()
                .is_none()
        );
        // A YSON selection applies to every table, so it has nothing to count.
        assert!(two_in_one_out.skiff_table_mismatch().is_none());
    }

    #[test]
    fn a_reduce_skiff_format_needs_one_schema_per_table() {
        let spec = ReduceSpec::new("./w", ["//a", "//b"], ["//out"], ["key"]);

        let reason = spec
            .clone()
            .with_skiff_formats(skiff_tables(&["source"]), skiff_tables(&["result"]))
            .skiff_table_mismatch()
            .expect("a reduce input format describes every input table");
        assert!(reason.contains("input_format"), "{reason}");

        assert!(
            spec.with_skiff_formats(skiff_tables(&["a", "b"]), skiff_tables(&["x"]))
                .skiff_table_mismatch()
                .is_none()
        );
    }

    #[test]
    fn map_reduce_checks_the_counts_it_knows_and_leaves_the_shuffle_alone() {
        let spec = MapReduceSpec::new("./r", ["//a", "//b"], ["//out"], ["key"])
            .with_mapper("./m")
            .with_mapper_skiff_formats(skiff_tables(&["one"]), skiff_tables(&["shuffle"]));
        let reason = spec
            .skiff_table_mismatch()
            .expect("the mapper still reads the operation's input tables");
        assert!(reason.contains("input_format"), "{reason}");

        // Two schemas for what this builder renders as one shuffle stream is
        // exactly the count it refuses to guess at.
        let shuffle = MapReduceSpec::new("./r", ["//a"], ["//out"], ["key"])
            .with_mapper("./m")
            .with_mapper_skiff_formats(skiff_tables(&["one"]), skiff_tables(&["x", "y"]))
            .with_reducer_skiff_formats(skiff_tables(&["x", "y"]), skiff_tables(&["out"]));
        assert!(shuffle.skiff_table_mismatch().is_none());
    }

    #[test]
    fn a_vanilla_task_skiff_output_needs_one_schema_per_output() {
        let spec = VanillaSpec::new(
            VanillaTask::new("worker", "./w", 1)
                .with_outputs(["//one"])
                .with_skiff_output_format(skiff_tables(&["a", "b"])),
        );
        let reason = spec
            .skiff_table_mismatch()
            .expect("two schemas cannot describe one output table");
        assert!(reason.contains(r#"task "worker""#), "{reason}");

        assert!(
            VanillaSpec::new(
                VanillaTask::new("worker", "./w", 1)
                    .with_outputs(["//one"])
                    .with_skiff_output_format(skiff_tables(&["a"])),
            )
            .skiff_table_mismatch()
            .is_none()
        );
    }

    #[test]
    fn a_map_spec_carries_what_the_operation_needs() {
        let spec = MapSpec::new("./cat", ["//tmp/in"], ["//tmp/out"])
            .with_local_file("//tmp/cat")
            .with_memory_limit(1024);
        let out = render(&spec.to_yson());

        assert!(out.contains(r#"command="./cat""#), "{out}");
        assert!(out.contains(r#"file_paths=["//tmp/cat"]"#), "{out}");
        assert!(out.contains("memory_limit=1024"), "{out}");
        assert!(out.contains(r#"input_table_paths=["//tmp/in"]"#), "{out}");
        assert!(out.contains(r#"output_table_paths=["//tmp/out"]"#), "{out}");
        assert!(out.contains("input_format=<format=binary>yson"), "{out}");
    }

    #[test]
    fn multiple_outputs_are_preserved_in_order() {
        let spec = MapSpec::new("./cat", ["//tmp/a", "//tmp/b"], ["//tmp/x", "//tmp/y"]);
        let out = render(&spec.to_yson());
        assert!(
            out.contains(r#"input_table_paths=["//tmp/a";"//tmp/b"]"#),
            "{out}"
        );
        assert!(
            out.contains(r#"output_table_paths=["//tmp/x";"//tmp/y"]"#),
            "{out}"
        );
    }

    #[test]
    fn map_can_select_schema_checked_skiff_for_both_directions() {
        let out = render(
            &MapSpec::new("./worker", ["//in"], ["//out"])
                .with_skiff_formats(skiff_format("source"), skiff_format("result"))
                .to_yson(),
        );

        assert!(out.contains("input_format=<table_skiff_schemas="), "{out}");
        assert!(out.contains("output_format=<table_skiff_schemas="), "{out}");
        assert!(out.contains("name=source"), "{out}");
        assert!(out.contains("name=result"), "{out}");
        assert!(!out.contains("format=binary"), "{out}");
    }

    #[test]
    fn map_can_select_yson_and_skiff_through_the_shared_format_enum() {
        let out = render(
            &MapSpec::new("./worker", ["//in"], ["//out"])
                .with_formats(
                    DataFormat::text_yson(),
                    DataFormat::skiff(skiff_format("result")),
                )
                .to_yson(),
        );

        assert!(out.contains("input_format=<format=text>yson"), "{out}");
        assert!(out.contains("output_format=<table_skiff_schemas="), "{out}");
        assert!(out.contains("name=result"), "{out}");
    }

    #[test]
    fn table_index_is_off_unless_asked_for() {
        let plain = render(&MapSpec::new("./c", ["//i"], ["//o"]).to_yson());
        assert!(!plain.contains("enable_input_table_index"), "{plain}");

        let asked = render(
            &MapSpec::new("./c", ["//i"], ["//o"])
                .with_input_table_index()
                .to_yson(),
        );
        assert!(asked.contains("enable_input_table_index=%true"), "{asked}");
    }

    /// The mistake that cost real debugging time: on a map-reduce the reducer's
    /// section is `reduce_job_io`, and `job_io` is silently ignored.
    #[test]
    fn map_reduce_puts_key_switch_under_reduce_job_io() {
        let spec = MapReduceSpec::new("./wc reduce", ["//in"], ["//out"], ["word"])
            .with_mapper("./wc map");
        let out = render(&spec.to_yson());

        assert!(
            out.contains("reduce_job_io={control_attributes={enable_key_switch=%true}}"),
            "{out}"
        );
        // `reduce_job_io` ends with `job_io`, so a naive substring check would
        // always pass. Anchor on the key boundary instead.
        assert!(
            !out.contains(";job_io=") && !out.contains("{job_io="),
            "must not use the plain job_io section: {out}"
        );
    }

    #[test]
    fn map_reduce_can_select_skiff_per_job_phase() {
        let out = render(
            &MapReduceSpec::new("./worker reduce", ["//in"], ["//out"], ["key"])
                .with_mapper("./worker map")
                .with_mapper_skiff_formats(skiff_format("map_input"), skiff_format("map_output"))
                .with_reducer_skiff_formats(
                    skiff_format("reduce_input"),
                    skiff_format("reduce_output"),
                )
                .to_yson(),
        );

        for column in ["map_input", "map_output", "reduce_input", "reduce_output"] {
            assert!(out.contains(&format!("name={column}")), "{out}");
        }
        assert_eq!(
            out.matches("input_format=<table_skiff_schemas=").count(),
            2,
            "{out}"
        );
        assert_eq!(
            out.matches("output_format=<table_skiff_schemas=").count(),
            2,
            "{out}"
        );
    }

    /// Formats are chosen for a phase that may not exist yet, so the two call
    /// orders have to render the same spec.
    #[test]
    fn a_mapper_added_last_still_gets_its_formats() {
        let before = render(
            &MapReduceSpec::new("./r", ["//in"], ["//out"], ["k"])
                .with_mapper_skiff_formats(skiff_format("map_input"), skiff_format("map_output"))
                .with_mapper("./m")
                .to_yson(),
        );
        let after = render(
            &MapReduceSpec::new("./r", ["//in"], ["//out"], ["k"])
                .with_mapper("./m")
                .with_mapper_skiff_formats(skiff_format("map_input"), skiff_format("map_output"))
                .to_yson(),
        );

        assert_eq!(before, after);
        assert!(before.contains("name=map_input"), "{before}");
        assert!(before.contains("name=map_output"), "{before}");
    }

    #[test]
    fn key_switch_can_be_turned_off() {
        let out = render(
            &MapReduceSpec::new("./r", ["//in"], ["//out"], ["k"])
                .without_key_switch()
                .to_yson(),
        );
        assert!(!out.contains("enable_key_switch"), "{out}");
    }

    #[test]
    fn sort_by_defaults_to_reduce_by() {
        let out = render(&MapReduceSpec::new("./r", ["//in"], ["//out"], ["k"]).to_yson());
        assert!(out.contains("sort_by=[k]"), "{out}");

        let out = render(
            &MapReduceSpec::new("./r", ["//in"], ["//out"], ["k"])
                .with_sort_by(["k", "ts"])
                .to_yson(),
        );
        assert!(out.contains("sort_by=[k;ts]"), "{out}");
    }

    #[test]
    fn one_file_reaches_both_phases() {
        let out = render(
            &MapReduceSpec::new("./w reduce", ["//in"], ["//out"], ["k"])
                .with_mapper("./w map")
                .with_local_file("//tmp/w")
                .to_yson(),
        );
        assert_eq!(
            out.matches(r#"file_paths=["//tmp/w"]"#).count(),
            2,
            "the binary must be attached to both phases: {out}"
        );
    }

    /// Builder order must not change the program: a file or memory limit added
    /// before `with_mapper` reaches the mapper all the same. They used to be
    /// copied onto the phases as the calls arrived, so this exact sequence
    /// produced a mapper with no files and no limit — silently.
    #[test]
    fn a_mapper_added_last_still_gets_the_files_and_the_limit() {
        let out = render(
            &MapReduceSpec::new("./w reduce", ["//in"], ["//out"], ["k"])
                .with_local_file("//tmp/w")
                .with_memory_limit(512 * 1024 * 1024)
                .with_mapper("./w map")
                .to_yson(),
        );
        assert_eq!(
            out.matches(r#"file_paths=["//tmp/w"]"#).count(),
            2,
            "the binary must reach both phases whatever the call order: {out}"
        );
        assert_eq!(
            out.matches("memory_limit=536870912").count(),
            2,
            "the limit must reach both phases whatever the call order: {out}"
        );
    }

    /// A cached file is named after its hash, so the sandbox name has to come
    /// from an attribute or the job's command finds nothing to run.
    #[test]
    fn a_named_file_carries_its_sandbox_name() {
        let cached = "//tmp/yt_wrapper/file_storage/new_cache/da/2c76e46b90e8b9d5ec25397e14c043da";
        let out = render(
            &MapSpec::new("./cat", ["//i"], ["//o"])
                .with_local_file_named(cached, "cat")
                .to_yson(),
        );

        assert!(out.contains("file_name=cat"), "{out}");
        assert!(out.contains(cached), "{out}");
    }

    #[test]
    fn a_named_file_reaches_both_map_reduce_phases() {
        let out = render(
            &MapReduceSpec::new("./w reduce", ["//in"], ["//out"], ["k"])
                .with_mapper("./w map")
                .with_local_file_named("//tmp/cache/ab/cd", "w")
                .to_yson(),
        );
        assert_eq!(
            out.matches("file_name=w").count(),
            2,
            "the binary must be attached to both phases: {out}"
        );
    }

    #[test]
    fn a_plain_file_gets_no_attributes() {
        let out = render(
            &MapSpec::new("./cat", ["//i"], ["//o"])
                .with_local_file("//tmp/cat")
                .to_yson(),
        );
        assert!(out.contains(r#"file_paths=["//tmp/cat"]"#), "{out}");
    }

    #[test]
    fn raw_fields_land_in_the_spec() {
        let out = render(
            &MapSpec::new("./c", ["//i"], ["//o"])
                .with_raw("max_failed_job_count", int(3))
                .to_yson(),
        );
        assert!(out.contains("max_failed_job_count=3"), "{out}");
    }

    /// All nine the cluster registers. The four at the bottom were unreachable
    /// until this enum could name them — not even through a hand-built spec,
    /// because the type is a parameter of the command and not part of the spec.
    #[test]
    fn operation_type_wire_names() {
        assert_eq!(OperationType::Map.as_str(), "map");
        assert_eq!(OperationType::MapReduce.as_str(), "map_reduce");
        assert_eq!(OperationType::Reduce.as_str(), "reduce");
        assert_eq!(OperationType::Sort.as_str(), "sort");
        assert_eq!(OperationType::Vanilla.as_str(), "vanilla");
        assert_eq!(OperationType::Merge.as_str(), "merge");
        assert_eq!(OperationType::Erase.as_str(), "erase");
        assert_eq!(OperationType::RemoteCopy.as_str(), "remote_copy");
        assert_eq!(OperationType::JoinReduce.as_str(), "join_reduce");
    }

    #[test]
    fn merge_mode_wire_names() {
        assert_eq!(MergeMode::Unordered.as_str(), "unordered");
        assert_eq!(MergeMode::Ordered.as_str(), "ordered");
        assert_eq!(MergeMode::Sorted.as_str(), "sorted");
    }

    /// A merge writes **one** table and names it `output_table_path`, the
    /// singular spelling a sort uses — the plural is rejected.
    #[test]
    fn a_merge_spec_names_one_output() {
        let out = render(&MergeSpec::new(["//tmp/a", "//tmp/b"], "//tmp/all").to_yson());

        assert!(
            out.contains(r#"input_table_paths=["//tmp/a";"//tmp/b"]"#),
            "{out}"
        );
        assert!(out.contains(r#"output_table_path="//tmp/all""#), "{out}");
        assert!(
            out.contains("mode=unordered"),
            "the cheapest mode is the default, and it is sent rather than \
             assumed: {out}"
        );
        assert!(!out.contains("merge_by"), "{out}");
    }

    #[test]
    fn a_sorted_merge_carries_its_key() {
        let spec = MergeSpec::new(["//tmp/a"], "//tmp/all")
            .with_mode(MergeMode::Sorted)
            .with_merge_by(["host", "day"])
            .with_combine_chunks(true)
            .with_job_count(4);
        let out = render(&spec.to_yson());

        assert!(out.contains("mode=sorted"), "{out}");
        // Unquoted: the text writer drops the quotes around a string that
        // looks like an identifier, and both spellings are valid YSON.
        assert!(out.contains("merge_by=[host;day]"), "{out}");
        assert!(out.contains("combine_chunks=%true"), "{out}");
        assert!(out.contains("job_count=4"), "{out}");
        assert!(!spec.needs_merge_by());
    }

    /// The cluster refuses this too, later and in its own words.
    /// `Client::start_merge` reads this and says which key is missing.
    #[test]
    fn a_sorted_merge_without_a_key_is_recognisable() {
        assert!(
            MergeSpec::new(["//tmp/a"], "//tmp/all")
                .with_mode(MergeMode::Sorted)
                .needs_merge_by()
        );
        assert!(
            !MergeSpec::new(["//tmp/a"], "//tmp/all").needs_merge_by(),
            "an unordered merge has nothing to merge by"
        );
    }

    /// Erase names one table with `table_path` — it reads and writes the same
    /// one — and the rows to delete are a range **on the path**.
    #[test]
    fn an_erase_spec_names_the_table_once() {
        let out = render(&EraseSpec::new("//tmp/log[#0:#10]").to_yson());

        assert_eq!(out, r#"{table_path="//tmp/log[#0:#10]"}"#);
    }

    #[test]
    fn an_erase_spec_can_ask_for_compaction() {
        let out = render(
            &EraseSpec::new("//tmp/log")
                .with_combine_chunks(true)
                .to_yson(),
        );
        assert!(out.contains("combine_chunks=%true"), "{out}");
    }

    #[test]
    fn a_remote_copy_spec_names_the_source_cluster() {
        let spec = RemoteCopySpec::new("hahn", ["//tmp/theirs"], "//tmp/ours")
            .with_network_name("fastbone")
            .with_copy_attributes(true)
            .with_attribute_keys(["expiration_time"]);
        let out = render(&spec.to_yson());

        assert!(out.contains("cluster_name=hahn"), "{out}");
        assert!(
            out.contains(r#"input_table_paths=["//tmp/theirs"]"#),
            "{out}"
        );
        assert!(out.contains(r#"output_table_path="//tmp/ours""#), "{out}");
        assert!(out.contains("network_name=fastbone"), "{out}");
        assert!(out.contains("copy_attributes=%true"), "{out}");
        assert!(out.contains("attribute_keys=[expiration_time]"), "{out}");
    }

    #[test]
    fn the_new_specs_take_raw_fields_too() {
        let merge = render(
            &MergeSpec::new(["//i"], "//o")
                .with_raw("schema_inference_mode", string("from_output"))
                .to_yson(),
        );
        assert!(
            merge.contains("schema_inference_mode=from_output"),
            "{merge}"
        );

        let erase = render(
            &EraseSpec::new("//t")
                .with_raw("schema_inference_mode", string("auto"))
                .to_yson(),
        );
        assert!(erase.contains("schema_inference_mode=auto"), "{erase}");

        let copy = render(
            &RemoteCopySpec::new("c", ["//i"], "//o")
                .with_raw("allow_unfrozen_input_tables", boolean(true))
                .to_yson(),
        );
        assert!(copy.contains("allow_unfrozen_input_tables=%true"), "{copy}");
    }

    /// The mirror of the map-reduce trap: a reduce has one job type, so its
    /// section is the plain `job_io`.
    #[test]
    fn reduce_puts_key_switch_under_job_io() {
        let out =
            render(&ReduceSpec::new("./wc reduce", ["//sorted"], ["//out"], ["word"]).to_yson());

        assert!(
            out.contains("job_io={control_attributes={enable_key_switch=%true}}"),
            "{out}"
        );
        assert!(
            !out.contains("reduce_job_io"),
            "reduce_job_io belongs to map-reduce, not to reduce: {out}"
        );
    }

    #[test]
    fn a_reduce_spec_carries_what_the_operation_needs() {
        let spec = ReduceSpec::new("./wc reduce", ["//tmp/sorted"], ["//tmp/counts"], ["word"])
            .with_local_file("//tmp/wc")
            .with_memory_limit(1024)
            .with_job_count(2);
        let out = render(&spec.to_yson());

        assert!(out.contains(r#"command="./wc reduce""#), "{out}");
        assert!(out.contains(r#"file_paths=["//tmp/wc"]"#), "{out}");
        assert!(out.contains("memory_limit=1024"), "{out}");
        assert!(out.contains("reduce_by=[word]"), "{out}");
        assert!(out.contains("job_count=2"), "{out}");
        assert!(out.contains("input_format=<format=binary>yson"), "{out}");
    }

    /// Unlike map-reduce, `sort_by` is omitted when it was not asked for: the
    /// cluster defaults it to `reduce_by`, and stating it turns on a
    /// sortedness check the caller did not request.
    #[test]
    fn reduce_sort_by_is_only_sent_when_set() {
        let plain = render(&ReduceSpec::new("./r", ["//in"], ["//out"], ["k"]).to_yson());
        assert!(!plain.contains("sort_by"), "{plain}");

        let asked = render(
            &ReduceSpec::new("./r", ["//in"], ["//out"], ["k"])
                .with_sort_by(["k", "ts"])
                .to_yson(),
        );
        assert!(asked.contains("sort_by=[k;ts]"), "{asked}");
    }

    #[test]
    fn reduce_table_index_is_off_unless_asked_for() {
        let plain = render(&ReduceSpec::new("./r", ["//a", "//b"], ["//o"], ["k"]).to_yson());
        assert!(!plain.contains("enable_input_table_index"), "{plain}");

        let asked = render(
            &ReduceSpec::new("./r", ["//a", "//b"], ["//o"], ["k"])
                .with_input_table_index()
                .to_yson(),
        );
        assert!(asked.contains("enable_input_table_index=%true"), "{asked}");
    }

    /// Sort's output is one table and the field is singular. Spelling it like
    /// every other operation is the obvious mistake.
    #[test]
    fn sort_writes_one_table_through_a_singular_field() {
        let out = render(&SortSpec::new(["//a", "//b"], "//sorted", ["key", "sub"]).to_yson());

        assert!(out.contains(r#"output_table_path="//sorted""#), "{out}");
        assert!(!out.contains("output_table_paths"), "{out}");
        assert!(out.contains(r#"input_table_paths=["//a";"//b"]"#), "{out}");
        assert!(out.contains("sort_by=[key;sub]"), "{out}");
    }

    #[test]
    fn a_sort_spec_has_no_user_job() {
        let out = render(&SortSpec::new(["//a"], "//sorted", ["key"]).to_yson());
        assert!(
            !out.contains("command"),
            "the cluster sorts, not a job: {out}"
        );
        assert!(!out.contains("input_format"), "{out}");
    }

    #[test]
    fn a_vanilla_spec_describes_its_tasks() {
        let out = render(
            &VanillaSpec::new(
                VanillaTask::new("worker", "./my_job", 4)
                    .with_local_file("//tmp/my_job")
                    .with_outputs(["//tmp/results"])
                    .with_memory_limit(1024),
            )
            .with_task(VanillaTask::new("master", "./my_job master", 1))
            .with_raw("max_failed_job_count", int(1))
            .to_yson(),
        );

        assert!(out.contains("tasks={"), "{out}");
        assert!(out.contains("worker={"), "{out}");
        assert!(out.contains("master={"), "{out}");
        assert!(out.contains("job_count=4"), "{out}");
        assert!(out.contains("job_count=1"), "{out}");
        assert!(
            out.contains(r#"output_table_paths=["//tmp/results"]"#),
            "{out}"
        );
        assert!(out.contains("max_failed_job_count=1"), "{out}");
        // No input: that is what makes it vanilla.
        assert!(!out.contains("input_table_paths"), "{out}");
    }

    #[test]
    fn reduce_can_select_skiff_for_both_directions() {
        let out = render(
            &ReduceSpec::new("./worker", ["//in"], ["//out"], ["key"])
                .with_skiff_formats(skiff_format("reduce_input"), skiff_format("reduce_output"))
                .to_yson(),
        );

        assert!(out.contains("input_format=<table_skiff_schemas="), "{out}");
        assert!(out.contains("output_format=<table_skiff_schemas="), "{out}");
        assert!(out.contains("name=reduce_input"), "{out}");
        assert!(out.contains("name=reduce_output"), "{out}");
        assert!(!out.contains("format=binary"), "{out}");
        // The control attribute is the request; the format is the delivery.
        // Both belong in a Skiff reduce spec, as they do in the Go SDK.
        assert!(
            out.contains("control_attributes={enable_key_switch=%true}"),
            "{out}"
        );
    }

    #[test]
    fn a_vanilla_task_can_select_skiff_output_only() {
        let out = render(
            &VanillaSpec::new(
                VanillaTask::new("worker", "./my_job", 1)
                    .with_outputs(["//tmp/results"])
                    .with_skiff_output_format(skiff_format("result")),
            )
            .to_yson(),
        );

        assert!(out.contains("output_format=<table_skiff_schemas="), "{out}");
        assert!(out.contains("name=result"), "{out}");
        // No input table, so the input format stays where every vanilla
        // operation here has left it.
        assert!(out.contains("input_format=<format=binary>yson"), "{out}");
    }

    #[test]
    fn two_tasks_with_one_name_are_caught_before_the_cluster_sees_them() {
        // Rendered, they collapse into a single `worker={…}` — four jobs
        // instead of eight, the first command never run, and an operation that
        // completes. `Client::start_vanilla` refuses this rather than send it.
        let spec = VanillaSpec::new(VanillaTask::new("worker", "./j shard-a", 4))
            .with_task(VanillaTask::new("worker", "./j shard-b", 4));

        assert_eq!(spec.duplicate_task(), Some("worker"));

        let out = render(&spec.to_yson());
        assert!(!out.contains("shard-a"), "the first task is gone: {out}");
    }

    #[test]
    fn tasks_with_distinct_names_are_fine() {
        let spec = VanillaSpec::new(VanillaTask::new("worker", "./j", 4))
            .with_task(VanillaTask::new("master", "./j master", 1));
        assert_eq!(spec.duplicate_task(), None);
    }

    /// A task with no output tables still sends the field. Leaving it out is a
    /// different statement from "there are none".
    #[test]
    fn a_task_without_outputs_says_so() {
        let out = render(&VanillaSpec::new(VanillaTask::new("t", "./j", 1)).to_yson());
        assert!(out.contains("output_table_paths=[]"), "{out}");
    }

    #[test]
    fn gang_options_go_through_raw() {
        let out = render(
            &VanillaSpec::new(
                VanillaTask::new("worker", "./j", 3).with_raw("gang_options", map::<&str>([])),
            )
            .to_yson(),
        );
        assert!(out.contains("gang_options={}"), "{out}");
    }

    #[test]
    fn sort_tuning_goes_through_raw() {
        let out = render(
            &SortSpec::new(["//a"], "//sorted", ["key"])
                .with_raw("partition_count", int(4))
                .to_yson(),
        );
        assert!(out.contains("partition_count=4"), "{out}");
    }
}
