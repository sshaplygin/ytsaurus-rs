//! Writing schema-described Skiff output tables.
//!
//! As with [`crate::JobWriter`], each table has its own descriptor: table zero
//! is fd 1, table one is fd 4, and so on. Each descriptor is a separate Skiff
//! stream, therefore every row starts with the single-table `Variant16` tag
//! zero. The job runtime never multiplexes Skiff output streams.

use std::io::Write;

use ytsaurus_skiff::{Encoder, Format, Value};

use crate::writer::{TABLE_BUFFER_BYTES, output_descriptor};
use crate::{JobError, Result, TableId, table_descriptor};

/// Writes Skiff rows to a job's output descriptors.
///
/// The output format supplies one table schema per descriptor. System fields
/// such as `$key_switch` are input-only controls and are rejected here until
/// their output semantics have been verified against a cluster.
pub struct SkiffJobWriter {
    tables: Vec<Encoder<Box<dyn Write>>>,
    finished: bool,
}

impl std::fmt::Debug for SkiffJobWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SkiffJobWriter")
            .field("tables", &self.tables.len())
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl SkiffJobWriter {
    /// Opens one Skiff output stream per YTsaurus output descriptor.
    #[cfg(unix)]
    pub fn descriptors(format: Format) -> Result<Self> {
        let tables = (0..format.table_schemas().len())
            .map(|index| -> Box<dyn Write> {
                Box::new(std::io::BufWriter::with_capacity(
                    TABLE_BUFFER_BYTES,
                    output_descriptor(table_descriptor(index)),
                ))
            })
            .collect();
        Self::from_writers(tables, format)
    }

    /// Builds a writer over arbitrary sinks, primarily for offline tests.
    pub fn from_writers(tables: Vec<Box<dyn Write>>, format: Format) -> Result<Self> {
        let schemas = format.table_schemas().len();
        if tables.len() != schemas {
            return Err(JobError::SkiffOutputSchemaCount {
                sinks: tables.len(),
                schemas,
            });
        }

        let mut encoders = Vec::with_capacity(schemas);
        for (index, table) in tables.into_iter().enumerate() {
            let schema = format
                .table_schema(index)
                .expect("Format validates every table-schema reference");
            if let Some(name) = schema
                .children
                .iter()
                .filter_map(|child| child.name.as_deref())
                .find(|name| matches!(*name, "$key_switch" | "$row_index" | "$range_index"))
            {
                return Err(JobError::BadSkiffSchema {
                    table: index,
                    reason: format!("{name} is an input-only system field"),
                });
            }
            encoders.push(Encoder::new(table, schema.clone()).map_err(|source| {
                JobError::SkiffWrite {
                    table: index,
                    source,
                }
            })?);
        }

        Ok(Self {
            tables: encoders,
            finished: false,
        })
    }

    /// Number of output tables this writer can address.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Writes one dynamic Skiff row to an output table.
    pub fn write(&mut self, table: impl Into<TableId>, row: &Value) -> Result<()> {
        let table = table.into().index();
        let count = self.tables.len();
        let encoder = self
            .tables
            .get_mut(table)
            .ok_or_else(|| JobError::UnknownTable {
                index: table,
                count,
                names: Vec::new(),
            })?;
        encoder
            .write(row)
            .map_err(|source| JobError::SkiffWrite { table, source })
    }

    /// Flushes every output table.
    pub fn flush(&mut self) -> Result<()> {
        for (table, encoder) in self.tables.iter_mut().enumerate() {
            encoder
                .flush()
                .map_err(|source| JobError::SkiffWrite { table, source })?;
        }
        Ok(())
    }

    /// Flushes every output and marks this writer complete.
    pub fn finish(&mut self) -> Result<()> {
        self.flush()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for SkiffJobWriter {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Err(error) = self.flush() {
            eprintln!("ytsaurus-job: Skiff output was not flushed cleanly: {error}");
        }
    }
}
