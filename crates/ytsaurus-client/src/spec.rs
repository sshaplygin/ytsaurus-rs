//! Operation specifications.
//!
//! Specs are YSON dicts with a great many optional fields. These builders cover
//! what launching a `ytsaurus-job` worker needs and expose an escape hatch —
//! [`MapSpec::with_raw`] — for the rest, rather than pretending to model the
//! whole surface.
//!
//! Reference:
//! <https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options>

use ytsaurus_yson::YsonValue;

use crate::yson_build::{binary_yson_format, boolean, insert, int, list, map, string};

/// The kind of operation to start.
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
        }
    }
}

/// The parts of a user-job spec shared by mappers and reducers.
#[derive(Debug, Clone)]
struct UserJob {
    command: String,
    files: Vec<String>,
    memory_limit: Option<i64>,
    environment: Vec<(String, String)>,
}

impl UserJob {
    fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            files: Vec::new(),
            memory_limit: None,
            environment: Vec::new(),
        }
    }

    fn to_yson(&self) -> YsonValue {
        let mut job = map([
            ("command", string(&self.command)),
            // Both directions are binary YSON, which is what `JobReader` and
            // `JobWriter` expect by default.
            ("input_format", binary_yson_format()),
            ("output_format", binary_yson_format()),
        ]);

        if !self.files.is_empty() {
            insert(&mut job, "file_paths", list(self.files.iter().map(string)));
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
        self.mapper.files.push(path.into());
        self
    }

    /// Sets the mapper's memory limit, in bytes.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: i64) -> Self {
        self.mapper.memory_limit = Some(bytes);
        self
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
    reducer: UserJob,
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
            reducer: UserJob::new(reducer),
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

    /// Adds a Cypress file to both phases.
    ///
    /// One binary usually serves both, dispatching on `argv[1]`, so attaching
    /// it to each phase separately would only be a way to forget one.
    #[must_use]
    pub fn with_local_file(mut self, path: impl Into<String>) -> Self {
        let path = path.into();
        if let Some(mapper) = &mut self.mapper {
            mapper.files.push(path.clone());
        }
        self.reducer.files.push(path);
        self
    }

    /// Sets the memory limit for both phases, in bytes.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: i64) -> Self {
        if let Some(mapper) = &mut self.mapper {
            mapper.memory_limit = Some(bytes);
        }
        self.reducer.memory_limit = Some(bytes);
        self
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
            ("reducer", self.reducer.to_yson()),
            ("input_table_paths", list(self.inputs.iter().map(string))),
            ("output_table_paths", list(self.outputs.iter().map(string))),
            ("reduce_by", list(self.reduce_by.iter().map(string))),
        ]);

        if let Some(mapper) = &self.mapper {
            insert(&mut spec, "mapper", mapper.to_yson());
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

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::{YsonFormat, to_string};

    fn render(v: &YsonValue) -> String {
        to_string(v, YsonFormat::Text).expect("encodes")
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

    #[test]
    fn raw_fields_land_in_the_spec() {
        let out = render(
            &MapSpec::new("./c", ["//i"], ["//o"])
                .with_raw("max_failed_job_count", int(3))
                .to_yson(),
        );
        assert!(out.contains("max_failed_job_count=3"), "{out}");
    }

    #[test]
    fn operation_type_wire_names() {
        assert_eq!(OperationType::Map.as_str(), "map");
        assert_eq!(OperationType::MapReduce.as_str(), "map_reduce");
        assert_eq!(OperationType::Reduce.as_str(), "reduce");
    }
}
