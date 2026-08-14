//! Between the neutral row model and the wire format.
//!
//! The interface names columns; the wire numbers them and resolves the numbers
//! through a name table. Everything awkward about this module comes from that
//! one difference, and it is worth keeping in one place rather than spreading
//! through the client.

use bytes::Bytes;
use ytsaurus_api::{Error as ApiError, Result as ApiResult, Row, Value};

use crate::wire::{self, UnversionedValue};

/// Turns a named row into a wire row, numbering its values by `columns`.
///
/// A column the name table does not mention is an error rather than a silent
/// omission: the row would reach the cluster missing a field the caller
/// believed they had written.
pub fn row_to_wire(row: &Row, columns: &[String]) -> ApiResult<wire::Row> {
    let mut values = Vec::with_capacity(row.len());
    for (name, value) in row.columns() {
        let id = columns
            .iter()
            .position(|column| column == name)
            .ok_or_else(|| {
                ApiError::Conversion(format!(
                    "column {name:?} is not in the name table {columns:?}"
                ))
            })?;
        let id = u16::try_from(id).map_err(|_| {
            ApiError::Conversion(format!("a row cannot have more than {} columns", u16::MAX))
        })?;
        values.push(UnversionedValue::new(id, to_wire_value(value)));
    }
    Ok(values)
}

/// Turns a wire row back into a named one, resolving ids through `columns`.
///
/// An id the name table does not cover is reported rather than dropped —
/// silently losing a column would be indistinguishable from the cluster not
/// having sent it.
pub fn row_from_wire(row: &wire::Row, columns: &[String]) -> ApiResult<Row> {
    let mut named = Row::new();
    for value in row {
        let name = columns.get(value.id as usize).ok_or_else(|| {
            ApiError::Conversion(format!(
                "the reply used column id {} but named only {} columns",
                value.id,
                columns.len()
            ))
        })?;
        named.set(name.clone(), from_wire_value(&value.value));
    }
    Ok(named)
}

fn to_wire_value(value: &Value) -> wire::Value {
    match value {
        Value::Null => wire::Value::Null,
        Value::Int64(number) => wire::Value::Int64(*number),
        Value::Uint64(number) => wire::Value::Uint64(*number),
        Value::Double(number) => wire::Value::Double(*number),
        Value::Boolean(flag) => wire::Value::Boolean(*flag),
        Value::String(bytes) => wire::Value::String(Bytes::from(bytes.clone())),
        Value::Any(bytes) => wire::Value::Any(Bytes::from(bytes.clone())),
    }
}

fn from_wire_value(value: &wire::Value) -> Value {
    match value {
        wire::Value::Null => Value::Null,
        wire::Value::Int64(number) => Value::Int64(*number),
        wire::Value::Uint64(number) => Value::Uint64(*number),
        wire::Value::Double(number) => Value::Double(*number),
        wire::Value::Boolean(flag) => Value::Boolean(*flag),
        wire::Value::String(bytes) => Value::String(bytes.to_vec()),
        // Composite has no neutral counterpart — the interface says so — and
        // its payload is YSON either way, so it arrives as `Any` rather than
        // being dropped.
        wire::Value::Any(bytes) | wire::Value::Composite(bytes) => Value::Any(bytes.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> Vec<String> {
        vec!["key".to_owned(), "value".to_owned(), "extra".to_owned()]
    }

    #[test]
    fn a_row_round_trips_through_the_wire_model() {
        let row = Row::new()
            .with("key", 42i64)
            .with("value", "hello")
            .with("extra", None::<i64>);

        let wire_row = row_to_wire(&row, &columns()).unwrap();
        assert_eq!(wire_row[0].id, 0, "columns are numbered by the name table");
        assert_eq!(wire_row[1].id, 1);
        assert_eq!(wire_row[2].id, 2);

        assert_eq!(row_from_wire(&wire_row, &columns()).unwrap(), row);
    }

    #[test]
    fn every_value_type_survives() {
        let row = Row::new()
            .with("a", 1i64)
            .with("b", 2u64)
            .with("c", 1.5f64)
            .with("d", true)
            .with("e", vec![0xff, 0x00])
            .with("f", None::<i64>);
        let names: Vec<String> = ["a", "b", "c", "d", "e", "f"]
            .iter()
            .map(|name| (*name).to_owned())
            .collect();

        let wire_row = row_to_wire(&row, &names).unwrap();
        assert_eq!(row_from_wire(&wire_row, &names).unwrap(), row);
    }

    #[test]
    fn a_column_outside_the_name_table_is_an_error_not_a_silent_drop() {
        let row = Row::new().with("key", 1i64).with("unknown", 2i64);
        let error = row_to_wire(&row, &columns()).unwrap_err();
        assert!(
            error.to_string().contains("unknown"),
            "the error must name the column: {error}"
        );
    }

    #[test]
    fn an_id_beyond_the_name_table_is_reported() {
        let wire_row = vec![UnversionedValue::new(99, wire::Value::Int64(1))];
        let error = row_from_wire(&wire_row, &columns()).unwrap_err();
        assert!(error.to_string().contains("column id 99"), "{error}");
    }

    /// `Composite` has no neutral counterpart, and its payload is YSON either
    /// way. Arriving as `Any` keeps the bytes; dropping it would lose a column
    /// the cluster did send.
    #[test]
    fn a_composite_value_arrives_as_any_rather_than_vanishing() {
        let wire_row = vec![UnversionedValue::new(
            0,
            wire::Value::Composite(Bytes::from_static(b"[1;2;3]")),
        )];
        let row = row_from_wire(&wire_row, &columns()).unwrap();
        assert_eq!(row.get("key"), Some(&Value::Any(b"[1;2;3]".to_vec())));
    }

    #[test]
    fn doubles_keep_their_bits() {
        let names = vec!["d".to_owned()];
        for number in [0.0f64, -0.0, 1.5, f64::MIN, f64::MAX] {
            let row = Row::new().with("d", number);
            let wire_row = row_to_wire(&row, &names).unwrap();
            let back = row_from_wire(&wire_row, &names).unwrap();
            assert_eq!(
                back.get("d").unwrap().as_f64().unwrap().to_bits(),
                number.to_bits()
            );
        }
    }
}
