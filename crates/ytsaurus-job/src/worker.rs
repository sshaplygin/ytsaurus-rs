//! One worker API selected by [`ytsaurus_format::DataFormat`].
//!
//! [`WorkerReader`] and [`WorkerWriter`] choose YSON or Skiff at the process
//! boundary. They intentionally preserve each format's row representation:
//! a YSON row is byte-exact and can be forwarded without decoding, while a
//! Skiff row is a validated dynamic [`ytsaurus_skiff::Value`]. Treating those
//! as one invented row type would discard the most useful property of both.

use std::io::{Read, Write};

use ytsaurus_format::DataFormat;
use ytsaurus_skiff::Value;

use crate::{
    Event, JobError, JobReader, JobWriter, Result, SkiffJobReader, SkiffJobWriter, SkiffRow,
    TableId,
};

/// A reader selected by the operation's input [`DataFormat`].
#[derive(Debug)]
pub enum WorkerReader<R> {
    /// A YSON job stream.
    Yson(JobReader<R>),
    /// A schema-described Skiff job stream.
    Skiff(SkiffJobReader<R>),
}

/// An input event returned by [`WorkerReader::next_event`].
#[derive(Debug)]
pub enum WorkerEvent<'input> {
    /// A YSON data row or control event.
    Yson(Event<'input>),
    /// A Skiff data row, including its current control values.
    Skiff(SkiffRow),
}

impl WorkerReader<std::io::Stdin> {
    /// Reads stdin using the operation's selected input format.
    ///
    /// # Errors
    ///
    /// Returns an error if the format is unknown to this runtime or a Skiff
    /// schema is not suitable for job input.
    pub fn from_stdin(format: DataFormat) -> Result<Self> {
        Self::new(std::io::stdin(), format)
    }
}

impl<R: Read> WorkerReader<R> {
    /// Creates a worker input reader selected by `format`.
    ///
    /// # Errors
    ///
    /// Returns an error if the format is unknown to this runtime or a Skiff
    /// schema is not suitable for job input.
    pub fn new(input: R, format: DataFormat) -> Result<Self> {
        match format {
            DataFormat::Yson(format) => Ok(Self::Yson(JobReader::with_format(input, format))),
            DataFormat::Skiff(format) => Ok(Self::Skiff(SkiffJobReader::new(input, format)?)),
            _ => Err(JobError::UnsupportedDataFormat),
        }
    }

    /// Returns the next row or control event, or `None` at clean end of input.
    ///
    /// A returned YSON event borrows the reader's buffer; process it before
    /// calling this method again. Skiff rows are owned.
    pub fn next_event(&mut self) -> Result<Option<WorkerEvent<'_>>> {
        match self {
            Self::Yson(reader) => reader
                .next_event()
                .map(|event| event.map(WorkerEvent::Yson)),
            Self::Skiff(reader) => reader.next_row().map(|row| row.map(WorkerEvent::Skiff)),
        }
    }
}

/// A row accepted by [`WorkerWriter::write`].
pub enum WorkerRow<'row> {
    /// One complete, already-encoded YSON value. The writer adds its `;`
    /// record separator, just as [`JobWriter::write_raw`] does.
    YsonRaw(&'row [u8]),
    /// One value matching the selected Skiff table schema.
    Skiff(&'row Value),
}

/// A writer selected by the operation's output [`DataFormat`].
///
/// Existing [`JobWriter`] and [`SkiffJobWriter`] APIs remain available for
/// callers that want serde-based YSON output or direct format-specific access.
pub enum WorkerWriter {
    /// A YSON output stream.
    Yson(JobWriter),
    /// Schema-described Skiff output streams.
    Skiff(SkiffJobWriter),
}

impl std::fmt::Debug for WorkerWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Yson(writer) => formatter
                .debug_tuple("WorkerWriter::Yson")
                .field(writer)
                .finish(),
            Self::Skiff(writer) => formatter
                .debug_tuple("WorkerWriter::Skiff")
                .field(writer)
                .finish(),
        }
    }
}

impl WorkerWriter {
    /// Opens YTsaurus output descriptors for `format`.
    ///
    /// For Skiff, `table_count` must equal the number of table schemas in the
    /// format. One physical descriptor is opened per output table.
    ///
    /// # Errors
    ///
    /// Returns an error if the format is unknown, its Skiff schemas are not
    /// valid job-output schemas, or its schema count differs from `table_count`.
    #[cfg(unix)]
    pub fn descriptors(format: DataFormat, table_count: usize) -> Result<Self> {
        match format {
            DataFormat::Yson(format) => {
                JobWriter::descriptors_with_format(table_count, format).map(Self::Yson)
            }
            DataFormat::Skiff(format) => {
                let schemas = format.table_schemas().len();
                if schemas != table_count {
                    return Err(JobError::SkiffOutputSchemaCount {
                        sinks: table_count,
                        schemas,
                    });
                }
                SkiffJobWriter::descriptors(format).map(Self::Skiff)
            }
            _ => Err(JobError::UnsupportedDataFormat),
        }
    }

    /// Builds a writer over arbitrary sinks, primarily for offline tests.
    ///
    /// For Skiff, the number of supplied sinks must equal the format's table
    /// schemas. For YSON it determines the number of output tables.
    ///
    /// # Errors
    ///
    /// Returns an error if the format is unknown or its Skiff output schemas
    /// are unsuitable.
    pub fn from_writers(tables: Vec<Box<dyn Write>>, format: DataFormat) -> Result<Self> {
        match format {
            DataFormat::Yson(format) => Ok(Self::Yson(JobWriter::from_writers(tables, format))),
            DataFormat::Skiff(format) => {
                SkiffJobWriter::from_writers(tables, format).map(Self::Skiff)
            }
            _ => Err(JobError::UnsupportedDataFormat),
        }
    }

    /// Number of output tables this writer can address.
    #[must_use]
    pub fn table_count(&self) -> usize {
        match self {
            Self::Yson(writer) => writer.table_count(),
            Self::Skiff(writer) => writer.table_count(),
        }
    }

    /// Writes one row to an output table.
    ///
    /// `WorkerRow` must use the representation selected by this writer's
    /// [`DataFormat`].
    ///
    /// # Errors
    ///
    /// Returns an error for a format mismatch, invalid row, unknown table, or
    /// failed output write.
    pub fn write(&mut self, table: impl Into<TableId>, row: WorkerRow<'_>) -> Result<()> {
        match (self, row) {
            (Self::Yson(writer), WorkerRow::YsonRaw(row)) => writer.write_raw(table, row),
            (Self::Skiff(writer), WorkerRow::Skiff(row)) => writer.write(table, row),
            (Self::Yson(_), WorkerRow::Skiff(_)) => Err(JobError::WorkerRowFormatMismatch {
                writer: "YSON",
                row: "Skiff",
            }),
            (Self::Skiff(_), WorkerRow::YsonRaw(_)) => Err(JobError::WorkerRowFormatMismatch {
                writer: "Skiff",
                row: "YSON",
            }),
        }
    }

    /// Flushes every output table.
    pub fn flush(&mut self) -> Result<()> {
        match self {
            Self::Yson(writer) => writer.flush(),
            Self::Skiff(writer) => writer.flush(),
        }
    }

    /// Flushes every output table and marks the writer complete.
    pub fn finish(&mut self) -> Result<()> {
        match self {
            Self::Yson(writer) => writer.finish(),
            Self::Skiff(writer) => writer.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use ytsaurus_format::SkiffFormat;
    use ytsaurus_skiff::{Encoder, Schema, SchemaRef, Value, WireType};

    use super::*;

    fn skiff_format() -> SkiffFormat {
        SkiffFormat::new(vec![SchemaRef::Inline(Schema::tuple([Schema::named(
            "value",
            WireType::String32,
        )]))])
        .unwrap()
    }

    #[test]
    fn reader_selects_yson_and_skiff_from_the_same_enum() {
        let mut yson =
            WorkerReader::new(Cursor::new(b"{value=one};"), DataFormat::text_yson()).unwrap();
        assert!(matches!(
            yson.next_event().unwrap(),
            Some(WorkerEvent::Yson(_))
        ));

        let schema = skiff_format().table_schema(0).unwrap().clone();
        let mut encoder = Encoder::new(Vec::new(), schema).unwrap();
        encoder
            .write(&Value::Tuple(vec![Value::Bytes(b"one".to_vec())]))
            .unwrap();
        let stream = encoder.into_inner().unwrap();
        let mut skiff =
            WorkerReader::new(Cursor::new(stream), DataFormat::skiff(skiff_format())).unwrap();
        assert!(matches!(
            skiff.next_event().unwrap(),
            Some(WorkerEvent::Skiff(_))
        ));
    }

    #[test]
    fn writer_rejects_a_row_from_the_other_format() {
        let mut writer =
            WorkerWriter::from_writers(vec![Box::new(Vec::new())], DataFormat::binary_yson())
                .unwrap();
        let error = writer
            .write(0, WorkerRow::Skiff(&Value::Tuple(Vec::new())))
            .unwrap_err();
        assert_eq!(error.kind(), "worker_row_format_mismatch");
    }
}
