//! # ytsaurus-yson
//!
//! A Rust library for serializing and deserializing the YSON format.

#![warn(missing_docs)]

// Internal modules hidden from the public API to reduce clutter
pub(crate) mod access;
/// Tools for working with YSON attributes and metadata.
pub mod attributes;
/// Deserialization logic and types.
pub mod de;
/// Error types and handling.
pub mod error;
pub(crate) mod lexer;
/// Abstract Syntax Tree (AST) representation of YSON values.
pub mod node;
/// Locating record boundaries in a partially-read buffer.
pub mod scan;
/// Serialization logic and types.
pub mod ser;
pub(crate) mod varint;

// Public re-exports
pub use crate::attributes::WithAttributes;
pub use crate::de::StreamDeserializer;
pub use crate::error::YsonError;
pub use crate::node::{YsonNode, YsonValue};
pub use crate::scan::{Scan, scan_value};
pub use crate::ser::YsonFormat;

use crate::de::Deserializer;
use crate::ser::Serializer;
use serde::{Deserialize, Serialize};

/// Helper to determine if a format is binary.
fn is_binary(format: YsonFormat) -> bool {
    match format {
        YsonFormat::Binary => true,
        YsonFormat::Text => false,
    }
}

/// Deserializes an instance of type `T` from a byte slice in the specified YSON format.
///
/// # Examples
///
/// ```
/// use ytsaurus_yson::{from_slice, YsonFormat};
/// use std::collections::HashMap;
///
/// let data = b"{key=\"42\"; status=\"active\"}";
/// let map: HashMap<String, String> = from_slice(data, YsonFormat::Text).unwrap();
///
/// assert_eq!(map.get("key").unwrap(), "42");
/// ```
///
/// # Errors
///
/// Returns [`YsonError`] if:
/// - The input data has invalid YSON syntax.
/// - The input contains invalid UTF-8 sequences (when in text mode).
/// - The data structure does not match the requirements of the target type `T`.
pub fn from_slice<'a, T>(bytes: &'a [u8], format: YsonFormat) -> Result<T, YsonError>
where
    T: Deserialize<'a>,
{
    let mut de = Deserializer::from_bytes(bytes, is_binary(format));
    T::deserialize(&mut de)
}

/// Serializes the given value into a byte vector using the specified YSON format.
///
/// # Examples
///
/// ```
/// use ytsaurus_yson::{to_vec, YsonFormat};
///
/// let data = vec![1, 2, 3];
/// let bytes = to_vec(&data, YsonFormat::Binary).unwrap();
/// assert!(!bytes.is_empty());
/// ```
///
/// # Errors
///
/// Returns [`YsonError`] if serialization fails, which can occur due to:
/// - Recursion depth limits being exceeded.
/// - Custom serialization errors defined by the type `T`.
pub fn to_vec<T: Serialize>(value: &T, format: YsonFormat) -> Result<Vec<u8>, YsonError> {
    let mut ser = Serializer::new(is_binary(format));
    value.serialize(&mut ser)?;
    Ok(ser.output)
}

/// Serializes the given value into a YSON-formatted string.
///
/// # Examples
///
/// ```
/// use ytsaurus_yson::{to_string, YsonFormat};
///
/// let val = ("answer", 42);
/// let res = to_string(&val, YsonFormat::Text).unwrap();
/// assert_eq!(res, "[answer;42]");
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The format is [`YsonFormat::Binary`] (binary YSON cannot be represented as a UTF-8 string).
/// - The serialization output contains invalid UTF-8 sequences.
/// - Serialization fails due to internal structural constraints.
pub fn to_string<T: Serialize>(value: &T, format: YsonFormat) -> Result<String, YsonError> {
    if matches!(format, YsonFormat::Binary) {
        return Err(YsonError::Custom(
            "Cannot use to_string for binary format".into(),
        ));
    }
    let bytes = to_vec(value, format)?;
    String::from_utf8(bytes).map_err(|_| YsonError::Custom("Invalid UTF-8 output".into()))
}
