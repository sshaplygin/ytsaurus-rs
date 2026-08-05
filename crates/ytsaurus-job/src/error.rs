//! Errors a job can fail with.

use thiserror::Error;
use ytsaurus_skiff::CodecError;
use ytsaurus_yson::YsonError;

/// Shorthand for a job result.
pub type Result<T, E = JobError> = std::result::Result<T, E>;

/// Something went wrong reading input or writing output.
///
/// Every variant is fatal to the job. YTsaurus judges a job by its exit code,
/// so the right response is to report the error on stderr — where the operation
/// UI shows it — and exit non-zero. [`crate::run`] does that for you.
#[derive(Debug, Error)]
pub enum JobError {
    /// Reading the input stream failed.
    #[error("reading job input: {0}")]
    Read(#[source] std::io::Error),

    /// Writing to an output table failed.
    ///
    /// Treated as fatal: a partial write means the output table would be
    /// missing rows, and silently producing a truncated table is worse than
    /// failing the job.
    #[error("writing to output table {table}: {source}")]
    Write {
        /// Index of the output table that failed.
        table: usize,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// The input was not valid YSON.
    #[error("invalid YSON at byte {offset} of the input stream: {source}")]
    Yson {
        /// Offset of the failing record from the start of the stream.
        offset: u64,
        /// The underlying parse error.
        #[source]
        source: YsonError,
    },

    /// The stream ended part-way through a record.
    #[error(
        "input stream ended {buffered} bytes into an incomplete record at byte {offset}; \
         the job was most likely killed or the upstream writer failed"
    )]
    TruncatedRecord {
        /// Offset of the incomplete record from the start of the stream.
        offset: u64,
        /// How many bytes of it had arrived.
        buffered: usize,
    },

    /// A single record was larger than the reader is willing to buffer.
    ///
    /// Because a record must be contiguous in memory to be parsed, an
    /// implausibly large length prefix in corrupt input would otherwise be an
    /// out-of-memory abort. See [`crate::JobReader::with_max_record_bytes`].
    #[error(
        "record at byte {offset} needs more than the {limit} byte buffer limit; \
         raise it with JobReader::with_max_record_bytes if the data is genuinely this wide"
    )]
    RecordTooLarge {
        /// Offset of the oversized record from the start of the stream.
        offset: u64,
        /// The configured limit, in bytes.
        limit: usize,
    },

    /// A control record carried an attribute value of the wrong type.
    #[error("malformed control record at byte {offset}: {reason}")]
    BadControlRecord {
        /// Offset of the control record from the start of the stream.
        offset: u64,
        /// What was wrong with it.
        reason: String,
    },

    /// The Skiff stream could not be framed or decoded.
    #[error("invalid Skiff job stream: {0}")]
    Skiff(#[source] CodecError),

    /// The operation's Skiff schema put system fields in an invalid layout.
    #[error("invalid Skiff system schema for table {table}: {reason}")]
    BadSkiffSchema {
        /// The input-table schema that is malformed.
        table: usize,
        /// What violates the job-format rules.
        reason: String,
    },

    /// A Skiff system field carried a value that does not have its prescribed shape.
    #[error("malformed Skiff {column} field for table {table}: {reason}")]
    BadSkiffControl {
        /// The input table that supplied the bad row.
        table: usize,
        /// The system field name.
        column: &'static str,
        /// What is malformed about its value.
        reason: String,
    },

    /// The number of Skiff output descriptors did not match the format's table schemas.
    #[error("Skiff output has {sinks} descriptor(s), but its format has {schemas} table schema(s)")]
    SkiffOutputSchemaCount {
        /// Number of output descriptors supplied by the caller.
        sinks: usize,
        /// Number of schemas supplied by the output format.
        schemas: usize,
    },

    /// Writing or flushing a Skiff output table failed.
    #[error("writing Skiff output table {table}: {source}")]
    SkiffWrite {
        /// The output table that failed.
        table: usize,
        /// The framing, validation, or I/O failure.
        #[source]
        source: CodecError,
    },

    /// This version of the worker runtime does not know a future
    /// [`ytsaurus_format::DataFormat`] variant yet.
    #[error("this ytsaurus-job version does not support the selected data format")]
    UnsupportedDataFormat,

    /// A row's representation did not match the writer's selected format.
    #[error("cannot write a {row} row through a {writer} output")]
    WorkerRowFormatMismatch {
        /// Format selected by the writer.
        writer: &'static str,
        /// Representation supplied by the caller.
        row: &'static str,
    },

    /// A row was written to an output table the job does not have.
    #[error(
        "output table {index} does not exist; this job has {count} output table(s){}",
        known_tables(.names)
    )]
    UnknownTable {
        /// The index that was asked for.
        index: usize,
        /// How many output tables the job actually has.
        count: usize,
        /// Declared table names, when the writer was built with
        /// [`crate::JobWriter::named`]. Turns a bare index into something the
        /// reader of the error can act on.
        names: Vec<String>,
    },

    /// Serializing a row failed.
    #[error("serializing a row for output table {table}: {source}")]
    Serialize {
        /// Index of the destination output table.
        table: usize,
        /// The underlying serialization error.
        #[source]
        source: YsonError,
    },

    /// More custom statistics than a job is allowed to report.
    ///
    /// The limit is on distinct names, not on writes: adding to one already
    /// recorded is always fine.
    #[error(
        "this job already reports {limit} custom statistics, which is the limit; \
         {name:?} would be one too many"
    )]
    TooManyStatistics {
        /// The cluster's limit.
        limit: usize,
        /// The name that did not fit.
        name: String,
    },

    /// Sending custom statistics failed.
    ///
    /// Separate from [`JobError::Write`] because descriptor 5 is not an output
    /// table, and reporting it as "output table 5" would send the reader
    /// looking for a table that does not exist.
    #[error("sending custom job statistics: {reason}")]
    Statistics {
        /// What went wrong.
        reason: String,
    },

    /// A row was written after [`crate::JobWriter::finish`].
    ///
    /// `finish` is the writer's end: a row accepted after it would sit in the
    /// buffer and vanish when the job exits — a short table under exit code
    /// zero, the exact outcome `finish` exists to rule out. Refusing the row
    /// makes the bug the caller's to see instead of the table's to carry.
    #[error(
        "row for output table {table} written after finish(); \
         finish() must be the last thing a job does with its writer"
    )]
    WriteAfterFinish {
        /// Index of the output table the late row was meant for.
        table: usize,
    },
}

impl JobError {
    /// A short, stable name for what went wrong.
    ///
    /// Formatting a `JobError` allocates and produces a message that may change
    /// between versions. A job that quarantines bad rows wants neither: it wants
    /// a cheap, stable value to put in a `reason` column so the rejects table
    /// can be grouped and counted.
    ///
    /// ```
    /// # use ytsaurus_job::JobError;
    /// # fn demo(e: &JobError) {
    /// // Cheap and stable — safe to write into an output table.
    /// let reason: &'static str = e.kind();
    /// # }
    /// ```
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            JobError::Read(_) => "read_failed",
            JobError::Write { .. } => "write_failed",
            JobError::Yson { .. } => "invalid_yson",
            JobError::TruncatedRecord { .. } => "truncated_record",
            JobError::RecordTooLarge { .. } => "record_too_large",
            JobError::BadControlRecord { .. } => "bad_control_record",
            JobError::Skiff(_) => "invalid_skiff",
            JobError::BadSkiffSchema { .. } => "bad_skiff_schema",
            JobError::BadSkiffControl { .. } => "bad_skiff_control",
            JobError::SkiffOutputSchemaCount { .. } => "skiff_output_schema_count",
            JobError::SkiffWrite { .. } => "skiff_write_failed",
            JobError::UnsupportedDataFormat => "unsupported_data_format",
            JobError::WorkerRowFormatMismatch { .. } => "worker_row_format_mismatch",
            JobError::UnknownTable { .. } => "unknown_table",
            JobError::Serialize { .. } => "serialize_failed",
            JobError::TooManyStatistics { .. } => "too_many_statistics",
            JobError::Statistics { .. } => "statistics_failed",
            JobError::WriteAfterFinish { .. } => "write_after_finish",
        }
    }

    /// Whether this error is about one bad row rather than the stream itself.
    ///
    /// A job that quarantines bad rows should keep going for these and stop for
    /// the rest: a truncated stream or a failed write means every subsequent row
    /// is suspect, and carrying on would quietly produce a short output table.
    ///
    /// ```
    /// # use ytsaurus_job::JobError;
    /// # fn demo(e: JobError) -> Result<(), JobError> {
    /// if e.is_row_local() {
    ///     // quarantine the row and continue
    /// } else {
    ///     return Err(e);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn is_row_local(&self) -> bool {
        match self {
            JobError::Yson { .. } | JobError::Serialize { .. } => true,
            JobError::Read(_)
            | JobError::Write { .. }
            | JobError::TruncatedRecord { .. }
            | JobError::RecordTooLarge { .. }
            | JobError::BadControlRecord { .. }
            | JobError::Skiff(_)
            | JobError::BadSkiffSchema { .. }
            | JobError::BadSkiffControl { .. }
            | JobError::SkiffOutputSchemaCount { .. }
            | JobError::SkiffWrite { .. }
            | JobError::UnsupportedDataFormat
            | JobError::WorkerRowFormatMismatch { .. }
            | JobError::UnknownTable { .. }
            // Neither is about a row: one says the job asked for more
            // statistics than it may have, the other that reporting them
            // failed. Quarantining a row would not help either.
            | JobError::TooManyStatistics { .. }
            | JobError::Statistics { .. }
            // A program-order bug, not a data problem: every later row
            // would be refused the same way.
            | JobError::WriteAfterFinish { .. } => false,
        }
    }
}

/// Renders declared table names for [`JobError::UnknownTable`].
fn known_tables(names: &[String]) -> String {
    if names.is_empty() {
        String::new()
    } else {
        format!(": {}", names.join(", "))
    }
}
