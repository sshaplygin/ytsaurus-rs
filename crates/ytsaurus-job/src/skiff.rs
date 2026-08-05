//! Streaming Skiff job input.
//!
//! A Skiff input row starts with its `Variant16` table index. Unlike YSON,
//! control information is represented by named tuple fields at the beginning
//! of the table schema: `$key_switch`, `$row_index`, and `$range_index`.
//! [`SkiffJobReader`] removes those fields from the returned row and exposes
//! their current values directly, mirroring the Go SDK decoder.

use std::io::Read;

use ytsaurus_skiff::{Decoder, Format, Schema, Value, WireType};

use crate::{JobError, Result};

/// One Skiff input row with its job-control values.
#[derive(Debug, Clone, PartialEq)]
pub struct SkiffRow {
    /// Index of the input table that supplied this row.
    pub table_index: usize,
    /// Index of this row in its table, as tracked by `$row_index`.
    pub row_index: i64,
    /// Index of the requested input range, as tracked by `$range_index`.
    pub range_index: i64,
    /// Whether this row starts a new reduce key group.
    pub key_switch: bool,
    value: Value,
}

impl SkiffRow {
    /// The data tuple after Skiff system columns have been removed.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Takes ownership of the data tuple after system columns are removed.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// Reads a schema-described Skiff input stream from a YTsaurus job.
#[derive(Debug)]
pub struct SkiffJobReader<R> {
    decoder: Decoder<R>,
    layouts: Vec<TableLayout>,
    state: Vec<ControlState>,
}

impl SkiffJobReader<std::io::Stdin> {
    /// Reads Skiff input from fd 0 using the operation's input format.
    pub fn from_stdin(format: Format) -> Result<Self> {
        Self::new(std::io::stdin(), format)
    }
}

impl<R: Read> SkiffJobReader<R> {
    /// Creates a reader for the supplied Skiff input format.
    ///
    /// The format must have one named-field tuple schema per input table. Job
    /// system fields may be present only as a contiguous prefix.
    pub fn new(input: R, format: Format) -> Result<Self> {
        let mut layouts = Vec::with_capacity(format.table_schemas().len());
        for index in 0..format.table_schemas().len() {
            let schema = format
                .table_schema(index)
                .expect("Format validates every table-schema reference");
            layouts.push(TableLayout::from_schema(index, schema)?);
        }
        let state = vec![ControlState::default(); layouts.len()];
        Ok(Self {
            decoder: Decoder::new(input, format),
            layouts,
            state,
        })
    }

    /// Changes the maximum `string32` or `yson32` payload accepted per field.
    #[must_use]
    pub fn with_max_blob_bytes(mut self, bytes: usize) -> Self {
        self.decoder = self.decoder.with_max_blob_bytes(bytes);
        self
    }

    /// Changes the maximum decoded footprint accepted per row.
    ///
    /// This is the Skiff counterpart of [`crate::JobReader::with_max_record_bytes`]:
    /// a field limit alone does not stop a repeated variant from decoding a
    /// small stream into a very large row.
    #[must_use]
    pub fn with_max_row_bytes(mut self, bytes: usize) -> Self {
        self.decoder = self.decoder.with_max_row_bytes(bytes);
        self
    }

    /// Returns the next input row, or `None` at a clean end of stream.
    pub fn next_row(&mut self) -> Result<Option<SkiffRow>> {
        let Some((table_index, row)) = self.decoder.next_row().map_err(JobError::Skiff)? else {
            return Ok(None);
        };
        let Value::Tuple(mut values) = row else {
            return Err(JobError::BadSkiffSchema {
                table: table_index,
                reason: "the table root did not decode as a tuple".to_owned(),
            });
        };
        let layout = &self.layouts[table_index];
        if values.len() < layout.system_columns.len() {
            return Err(JobError::BadSkiffSchema {
                table: table_index,
                reason: "decoded row is shorter than its system-column prefix".to_owned(),
            });
        }

        let mut key_switch = false;
        for (column, value) in layout
            .system_columns
            .iter()
            .copied()
            .zip(values.drain(..layout.system_columns.len()))
        {
            match column {
                SystemColumn::KeySwitch => {
                    key_switch = boolean_control(table_index, "$key_switch", value)?;
                }
                SystemColumn::RowIndex => {
                    apply_row_index(&mut self.state[table_index], table_index, value)?;
                }
                SystemColumn::RangeIndex => {
                    apply_range_index(&mut self.state[table_index], table_index, value)?;
                }
            }
        }

        // Go's decoder increments its per-table counter only when the schema
        // has no system columns at all. With `$key_switch` but no
        // `$row_index`, the reported index intentionally remains unchanged.
        if layout.system_columns.is_empty() {
            self.state[table_index].row_index = self.state[table_index].row_index.saturating_add(1);
        }

        let state = self.state[table_index];
        Ok(Some(SkiffRow {
            table_index,
            row_index: state.row_index,
            range_index: state.range_index,
            key_switch,
            value: Value::Tuple(values),
        }))
    }

    /// Returns the wrapped input reader.
    #[must_use]
    pub fn into_inner(self) -> R {
        self.decoder.into_inner()
    }
}

#[derive(Debug, Clone)]
struct TableLayout {
    system_columns: Vec<SystemColumn>,
}

impl TableLayout {
    fn from_schema(table: usize, schema: &Schema) -> Result<Self> {
        let mut system_columns = Vec::new();
        let mut data_started = false;
        let mut seen = [false; 3];

        for child in &schema.children {
            let Some(system) = SystemColumn::from_schema(table, child)? else {
                data_started = true;
                continue;
            };
            if data_started {
                return Err(JobError::BadSkiffSchema {
                    table,
                    reason: format!("{} appears after a data column", system.name()),
                });
            }
            let slot = system.slot();
            if std::mem::replace(&mut seen[slot], true) {
                return Err(JobError::BadSkiffSchema {
                    table,
                    reason: format!("{} appears more than once", system.name()),
                });
            }
            system_columns.push(system);
        }

        Ok(Self { system_columns })
    }
}

#[derive(Debug, Clone, Copy)]
enum SystemColumn {
    KeySwitch,
    RowIndex,
    RangeIndex,
}

impl SystemColumn {
    fn from_schema(table: usize, schema: &Schema) -> Result<Option<Self>> {
        match schema.name.as_deref() {
            Some("$key_switch") => {
                if schema.wire_type != WireType::Boolean || !schema.children.is_empty() {
                    return Err(JobError::BadSkiffSchema {
                        table,
                        reason: "$key_switch must be boolean".to_owned(),
                    });
                }
                Ok(Some(Self::KeySwitch))
            }
            Some("$row_index") => {
                if !optional_int64(schema) {
                    return Err(JobError::BadSkiffSchema {
                        table,
                        reason: "$row_index must be variant8<nothing;int64>".to_owned(),
                    });
                }
                Ok(Some(Self::RowIndex))
            }
            Some("$range_index") => {
                if !optional_int64(schema) {
                    return Err(JobError::BadSkiffSchema {
                        table,
                        reason: "$range_index must be variant8<nothing;int64>".to_owned(),
                    });
                }
                Ok(Some(Self::RangeIndex))
            }
            _ => Ok(None),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::KeySwitch => "$key_switch",
            Self::RowIndex => "$row_index",
            Self::RangeIndex => "$range_index",
        }
    }

    const fn slot(self) -> usize {
        match self {
            Self::KeySwitch => 0,
            Self::RowIndex => 1,
            Self::RangeIndex => 2,
        }
    }
}

fn optional_int64(schema: &Schema) -> bool {
    matches!(
        schema,
        Schema {
            wire_type: WireType::Variant8,
            children,
            ..
        } if matches!(children.as_slice(), [nothing, value]
            if nothing.wire_type == WireType::Nothing
                && value.wire_type == WireType::Int64)
    )
}

#[derive(Debug, Clone, Copy, Default)]
struct ControlState {
    row_index: i64,
    range_index: i64,
}

fn boolean_control(table: usize, column: &'static str, value: Value) -> Result<bool> {
    let Value::Boolean(value) = value else {
        return Err(bad_control(table, column, "expected boolean"));
    };
    Ok(value)
}

fn apply_row_index(state: &mut ControlState, table: usize, value: Value) -> Result<()> {
    match optional_index_value(table, "$row_index", value)? {
        Some(index) => state.row_index = index,
        None => state.row_index = state.row_index.saturating_add(1),
    }
    Ok(())
}

fn apply_range_index(state: &mut ControlState, table: usize, value: Value) -> Result<()> {
    if let Some(index) = optional_index_value(table, "$range_index", value)? {
        state.range_index = index;
    }
    Ok(())
}

fn optional_index_value(table: usize, column: &'static str, value: Value) -> Result<Option<i64>> {
    match value {
        Value::Variant {
            tag: 0,
            value: inner,
        } if matches!(*inner, Value::Nothing) => Ok(None),
        Value::Variant {
            tag: 1,
            value: inner,
        } => match *inner {
            Value::Int64(index) => Ok(Some(index)),
            _ => Err(bad_control(table, column, "tag 1 must carry int64")),
        },
        Value::Variant { tag, .. } => Err(bad_control(
            table,
            column,
            &format!("unexpected variant tag {tag}"),
        )),
        _ => Err(bad_control(
            table,
            column,
            "expected variant8<nothing;int64>",
        )),
    }
}

fn bad_control(table: usize, column: &'static str, reason: &str) -> JobError {
    JobError::BadSkiffControl {
        table,
        column,
        reason: reason.to_owned(),
    }
}
