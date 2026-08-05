//! YTsaurus [Skiff] schema and format support.
//!
//! Skiff is schema-driven: a byte stream cannot be decoded without its table
//! schemas. This crate validates that contract and provides a bounded dynamic
//! codec; typed rows and job-runtime integration build on those layers.
//!
//! [Skiff]: https://ytsaurus.tech/docs/en/user-guide/storage/skiff

#![warn(missing_docs)]

/// Skiff schema and format types.
pub mod schema;
/// Bounded Skiff stream encoding and decoding.
pub mod wire;

pub use crate::schema::{Format, Schema, SchemaError, SchemaRef, WireType};
pub use crate::wire::{
    CodecError, DEFAULT_MAX_BLOB_BYTES, DEFAULT_MAX_ROW_BYTES, Decoder, Encoder, Value, Variant,
};
