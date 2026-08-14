//! The row model both transports speak.
//!
//! HTTP carries rows as YSON and RPC carries them in the wire protocol, and
//! neither type belongs in an interface the other has to implement. This is the
//! meeting point: a column name, and a value from the set both formats can
//! represent.
//!
//! Column **names**, not ids. The RPC wire format numbers its values and
//! resolves them through a name table; HTTP names them directly. A caller
//! should not have to know which, so the id is an implementation detail of the
//! RPC side.

use std::collections::BTreeMap;
use std::fmt;

/// One column value.
///
/// The variants are the YTsaurus value types that survive both transports
/// unchanged. `Composite` is deliberately absent: HTTP renders it as YSON and
/// RPC as a distinct wire type, and unifying them would mean claiming a
/// round-trip this crate cannot guarantee. Use [`Value::Any`] and read the YSON.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Uint64(u64),
    Double(f64),
    Boolean(bool),
    /// A byte string. YTsaurus strings are bytes, not text, so this is not
    /// `String`: a column may legitimately hold something that is not UTF-8.
    String(Vec<u8>),
    /// A YSON-encoded value of any shape.
    Any(Vec<u8>),
}

impl Value {
    /// The value as a byte string, if it is one.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::String(bytes) | Self::Any(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// The value as text, if it is a byte string that happens to be UTF-8.
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(self.as_bytes()?).ok()
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Uint64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Double(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(*value),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Uint64(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.as_bytes().to_vec())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value.into_bytes())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::String(value)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self::Null,
        }
    }
}

/// One row: named columns, in insertion order.
///
/// Ordered rather than a map because a key row's column order is the table's
/// key order, and a lookup that reordered the key columns would ask for a
/// different row. `BTreeMap` would silently sort them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    columns: Vec<(String, Value)>,
}

impl Row {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a column, keeping the order it was added in.
    #[must_use]
    pub fn with(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.columns.push((name.into(), value.into()));
        self
    }

    /// Adds a column in place.
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.columns.push((name.into(), value.into()));
    }

    /// The value of a column, if the row has one.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .find(|(column, _)| column == name)
            .map(|(_, value)| value)
    }

    /// The columns, in order.
    pub fn columns(&self) -> &[(String, Value)] {
        &self.columns
    }

    /// The column names, in order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|(name, _)| name.as_str())
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// The row as a map, for callers that would rather look columns up than
    /// walk them. Loses the ordering, which is why it is not the representation.
    pub fn to_map(&self) -> BTreeMap<&str, &Value> {
        self.columns
            .iter()
            .map(|(name, value)| (name.as_str(), value))
            .collect()
    }
}

impl FromIterator<(String, Value)> for Row {
    fn from_iter<I: IntoIterator<Item = (String, Value)>>(iterator: I) -> Self {
        Self {
            columns: iterator.into_iter().collect(),
        }
    }
}

impl fmt::Display for Row {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("{")?;
        for (index, (name, value)) in self.columns.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            match value {
                Value::String(bytes) | Value::Any(bytes) => match std::str::from_utf8(bytes) {
                    Ok(text) => write!(formatter, "{name}={text:?}")?,
                    Err(_) => write!(formatter, "{name}=<{} bytes>", bytes.len())?,
                },
                other => write!(formatter, "{name}={other:?}")?,
            }
        }
        formatter.write_str("}")
    }
}

/// A row that may be absent.
///
/// A lookup returns one of these per key asked for, in order, and `None` means
/// the key had no row. Shortening the list instead would silently misalign
/// every answer after the missing one.
pub type MaybeRow = Option<Row>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_keeps_the_order_its_columns_were_added_in() {
        // Key order is table order; sorting would ask for a different row.
        let row = Row::new().with("b", 2i64).with("a", 1i64).with("c", 3i64);
        assert_eq!(row.names().collect::<Vec<_>>(), ["b", "a", "c"]);
    }

    #[test]
    fn columns_are_read_by_name() {
        let row = Row::new().with("key", 42i64).with("value", "hello");
        assert_eq!(row.get("key"), Some(&Value::Int64(42)));
        assert_eq!(row.get("value").and_then(Value::as_str), Some("hello"));
        assert_eq!(row.get("absent"), None);
    }

    #[test]
    fn strings_are_bytes_and_need_not_be_utf8() {
        let row = Row::new().with("raw", vec![0xff, 0xfe]);
        assert_eq!(row.get("raw").unwrap().as_bytes(), Some(&[0xff, 0xfe][..]));
        assert_eq!(
            row.get("raw").unwrap().as_str(),
            None,
            "not UTF-8, and that is allowed"
        );
    }

    #[test]
    fn an_option_becomes_null() {
        let row = Row::new()
            .with("present", Some(1i64))
            .with("absent", None::<i64>);
        assert_eq!(row.get("present"), Some(&Value::Int64(1)));
        assert!(row.get("absent").unwrap().is_null());
    }

    #[test]
    fn display_is_readable_and_does_not_choke_on_binary() {
        let row = Row::new().with("key", 1i64).with("blob", vec![0xff, 0x00]);
        assert_eq!(row.to_string(), r#"{key=Int64(1), blob=<2 bytes>}"#);
    }

    #[test]
    fn accessors_report_the_wrong_type_as_absent() {
        let value = Value::Int64(1);
        assert_eq!(value.as_i64(), Some(1));
        assert_eq!(value.as_u64(), None);
        assert_eq!(value.as_str(), None);
        assert!(!value.is_null());
    }
}
