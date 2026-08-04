use std::fmt::Display;

use serde::de;
use thiserror::Error;

/// Errors that can occur during YSON serialization or deserialization.
#[derive(Error, Clone, Debug, PartialEq)]
pub enum YsonError {
    /// Reached the end of the input stream gracefully.
    #[error("End of input")]
    Eof,

    /// Reached the end of the input unexpectedly (e.g., in the middle of a string).
    /// Contains the byte position where the EOF was encountered.
    #[error("Unexpected end of input at position {0}")]
    UnexpectedEof(usize),

    /// Encountered a byte that is not a valid YSON marker.
    /// Contains the invalid byte and its position.
    #[error("Invalid binary marker 0x{0:x} at position {1}")]
    InvalidMarker(u8, usize),

    /// Failed to parse a variable-length integer (varint).
    /// Contains the starting position of the malformed varint.
    #[error("Malformed varint at position {0}")]
    MalformedVarint(usize),

    /// Encountered a string that is not valid UTF-8.
    /// Contains the starting position of the invalid string.
    #[error("Invalid UTF-8 string at position {0}")]
    InvalidUtf8(usize),

    /// Found a token that does not match the expected YSON structure.
    #[error("Expected {expected}, found {found} at position {pos}")]
    UnexpectedToken {
        /// A description of what the parser was looking for.
        expected: &'static str,
        /// A string representation of the token that was actually found.
        found: String,
        /// The byte position of the unexpected token.
        pos: usize,
    },

    /// A catch-all for custom errors produced by `serde` or the user's data types.
    #[error("Custom error from serde: {0}")]
    Custom(String),
}

impl de::Error for YsonError {
    fn custom<T: Display>(msg: T) -> Self {
        YsonError::Custom(msg.to_string())
    }
}

impl serde::ser::Error for YsonError {
    fn custom<T: Display>(msg: T) -> Self {
        YsonError::Custom(msg.to_string())
    }
}
