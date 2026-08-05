//! Bounded encoding and decoding of schema-described Skiff values.
//!
//! [`Encoder`] deliberately follows the Go SDK's `NewEncoder`: it writes a
//! `Variant16` tag of zero followed by the one table schema it was given.
//! [`Decoder`] is the complementary job-input form: it receives a [`Format`]
//! and uses the leading `Variant16` tag to select an input table schema.
//!
//! The dynamic [`Value`] form is the codec's compatibility layer. Typed
//! `SkiffRow` support will build on it without making the framing or limit
//! checks depend on Serde internals.

use std::io::{ErrorKind, Read, Write};

use thiserror::Error;

use crate::{Format, Schema, WireType};

/// The largest `string32` or `yson32` payload accepted by default.
///
/// This is YTsaurus's documented maximum row size and the same bound used by
/// the Go SDK reader. It protects a decoder from turning an untrusted `u32`
/// length prefix into an unbounded allocation.
pub const DEFAULT_MAX_BLOB_BYTES: usize = 128 * 1024 * 1024;

/// The largest decoded footprint accepted for one row by default.
///
/// Bounding blobs does not bound a row. Skiff is compact and positional: one
/// `repeated_variant8` item can cost a single tag byte on the wire and tens of
/// bytes of decoded [`Value`], and the item loop runs until the end tag or the
/// end of the stream. Without this limit a few hundred megabytes of hostile or
/// corrupt stream decode into a multi-gigabyte value and the process is
/// OOM-killed rather than given an error.
///
/// This is the same ceiling `ytsaurus-job`'s YSON `JobReader` puts on one
/// record.
pub const DEFAULT_MAX_ROW_BYTES: usize = 256 * 1024 * 1024;

/// A dynamically decoded Skiff value.
///
/// Every variant maps directly to one [`WireType`]. Compound values retain
/// tags and wire order so they can be re-encoded without a lossy conversion.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `nothing`, an empty payload.
    Nothing,
    /// `boolean`.
    Boolean(bool),
    /// `int8`.
    Int8(i8),
    /// `int16`.
    Int16(i16),
    /// `int32`.
    Int32(i32),
    /// `int64`.
    Int64(i64),
    /// `int128`.
    Int128(i128),
    /// `int256`, kept as its little-endian two's-complement bytes.
    Int256([u8; 32]),
    /// `uint8`.
    Uint8(u8),
    /// `uint16`.
    Uint16(u16),
    /// `uint32`.
    Uint32(u32),
    /// `uint64`.
    Uint64(u64),
    /// `double`.
    Double(f64),
    /// `string32`, which may contain non-UTF-8 bytes.
    Bytes(Vec<u8>),
    /// `yson32`, kept as one binary-YSON payload.
    Yson(Vec<u8>),
    /// A `variant8` or `variant16` child selected by `tag`.
    Variant {
        /// The selected child index.
        tag: u16,
        /// The selected child's value.
        value: Box<Value>,
    },
    /// A `repeated_variant8` or `repeated_variant16` sequence.
    RepeatedVariants(Vec<Variant>),
    /// A tuple, in schema-child order.
    Tuple(Vec<Value>),
}

/// One non-terminal element of [`Value::RepeatedVariants`].
#[derive(Debug, Clone, PartialEq)]
pub struct Variant {
    /// The selected child index.
    pub tag: u16,
    /// The selected child's value.
    pub value: Value,
}

impl Value {
    /// A stable description used when a value does not fit its schema node.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Nothing => "nothing",
            Self::Boolean(_) => "boolean",
            Self::Int8(_) => "int8",
            Self::Int16(_) => "int16",
            Self::Int32(_) => "int32",
            Self::Int64(_) => "int64",
            Self::Int128(_) => "int128",
            Self::Int256(_) => "int256",
            Self::Uint8(_) => "uint8",
            Self::Uint16(_) => "uint16",
            Self::Uint32(_) => "uint32",
            Self::Uint64(_) => "uint64",
            Self::Double(_) => "double",
            Self::Bytes(_) => "string32",
            Self::Yson(_) => "yson32",
            Self::Variant { .. } => "variant",
            Self::RepeatedVariants(_) => "repeated variant",
            Self::Tuple(_) => "tuple",
        }
    }
}

/// An encoder for one Skiff table stream.
///
/// Each row is prefixed with `0_u16`, exactly as Go's `skiff.NewEncoder`
/// does. A job with several output descriptors creates one encoder per output
/// table; it does not multiplex table indexes into one descriptor.
#[derive(Debug)]
pub struct Encoder<W> {
    output: W,
    schema: Schema,
    max_blob_bytes: usize,
}

impl<W: Write> Encoder<W> {
    /// Creates an encoder for a single table schema.
    ///
    /// The schema must satisfy the YTsaurus table-format contract in full: a
    /// tuple root whose every child is named. These are the rules
    /// [`Format::new`] applies, and they are applied here so that a schema
    /// which can be encoded is always one that can also be declared to the
    /// cluster and read back by the matching [`Decoder`].
    pub fn new(output: W, schema: Schema) -> Result<Self, CodecError> {
        schema.validate().map_err(CodecError::InvalidSchema)?;
        if schema.wire_type != WireType::Tuple {
            return Err(CodecError::TableSchemaMustBeTuple {
                found: schema.wire_type,
            });
        }
        crate::schema::validate_table_schema(&schema).map_err(CodecError::InvalidSchema)?;
        Ok(Self {
            output,
            schema,
            max_blob_bytes: DEFAULT_MAX_BLOB_BYTES,
        })
    }

    /// Changes the maximum `string32` or `yson32` payload this encoder accepts.
    #[must_use]
    pub fn with_max_blob_bytes(mut self, bytes: usize) -> Self {
        self.max_blob_bytes = bytes;
        self
    }

    /// Writes one row with the single-table `Variant16` tag.
    pub fn write(&mut self, row: &Value) -> Result<(), CodecError> {
        write_all(&mut self.output, &0_u16.to_le_bytes())?;
        encode_value(&mut self.output, &self.schema, row, self.max_blob_bytes)
    }

    /// Flushes bytes held by the caller's writer.
    pub fn flush(&mut self) -> Result<(), CodecError> {
        self.output.flush().map_err(CodecError::Write)
    }

    /// Returns the output writer after flushing it.
    pub fn into_inner(mut self) -> Result<W, CodecError> {
        self.flush()?;
        Ok(self.output)
    }
}

/// A decoder for a multiplexed YTsaurus Skiff input stream.
#[derive(Debug)]
pub struct Decoder<R> {
    input: R,
    format: Format,
    max_blob_bytes: usize,
    max_row_bytes: usize,
}

impl<R: Read> Decoder<R> {
    /// Creates a decoder for rows described by `format`.
    #[must_use]
    pub fn new(input: R, format: Format) -> Self {
        Self {
            input,
            format,
            max_blob_bytes: DEFAULT_MAX_BLOB_BYTES,
            max_row_bytes: DEFAULT_MAX_ROW_BYTES,
        }
    }

    /// Changes the maximum `string32` or `yson32` payload this decoder accepts.
    #[must_use]
    pub fn with_max_blob_bytes(mut self, bytes: usize) -> Self {
        self.max_blob_bytes = bytes;
        self
    }

    /// Changes the maximum decoded footprint this decoder accepts per row.
    ///
    /// See [`DEFAULT_MAX_ROW_BYTES`] for why a blob limit alone is not enough.
    #[must_use]
    pub fn with_max_row_bytes(mut self, bytes: usize) -> Self {
        self.max_row_bytes = bytes;
        self
    }

    /// Decodes the next table-indexed row, or `None` at a clean end of stream.
    ///
    /// An end of stream after any part of a row is [`CodecError::Truncated`],
    /// never a successful short read.
    pub fn next_row(&mut self) -> Result<Option<(usize, Value)>, CodecError> {
        let Some(first) = read_first_byte(&mut self.input)? else {
            return Ok(None);
        };
        let mut table_tag = [first, 0];
        read_exact(&mut self.input, &mut table_tag[1..], "table Variant16 tag")?;
        let index = usize::from(u16::from_le_bytes(table_tag));
        let schema = self
            .format
            .table_schema(index)
            .map_err(CodecError::InvalidSchema)?;
        let mut budget = RowBudget::new(self.max_blob_bytes, self.max_row_bytes);
        let row = decode_value(&mut self.input, schema, &mut budget)?;
        Ok(Some((index, row)))
    }

    /// Advances past the next row without building it, returning its table
    /// index, or `None` at a clean end of stream.
    ///
    /// Framing, schema and limit checks are the ones [`Self::next_row`]
    /// applies, so a stream this accepts is exactly a stream that decodes.
    /// What it does not do is allocate: no `Vec` per blob, no `Box` per
    /// variant, no `Value` tree to throw away. That is the difference between
    /// asking whether a stream is a whole number of rows and decoding it to
    /// find out — the first question is the one a caller validating a table
    /// write is asking.
    pub fn skip_row(&mut self) -> Result<Option<usize>, CodecError> {
        let Some(first) = read_first_byte(&mut self.input)? else {
            return Ok(None);
        };
        let mut table_tag = [first, 0];
        read_exact(&mut self.input, &mut table_tag[1..], "table Variant16 tag")?;
        let index = usize::from(u16::from_le_bytes(table_tag));
        let schema = self
            .format
            .table_schema(index)
            .map_err(CodecError::InvalidSchema)?;
        let mut budget = RowBudget::new(self.max_blob_bytes, self.max_row_bytes);
        skip_value(&mut self.input, schema, &mut budget)?;
        Ok(Some(index))
    }

    /// Returns the wrapped input reader.
    #[must_use]
    pub fn into_inner(self) -> R {
        self.input
    }
}

/// A wire or I/O failure while encoding or decoding Skiff.
#[derive(Debug, Error)]
pub enum CodecError {
    /// A schema was invalid before a stream was touched.
    #[error("invalid Skiff schema: {0}")]
    InvalidSchema(#[source] crate::SchemaError),
    /// A table format schema did not have the required tuple root.
    #[error("Skiff table schema root must be tuple, got {found}")]
    TableSchemaMustBeTuple {
        /// The root wire type that was supplied.
        found: WireType,
    },
    /// The stream ended after a row had started.
    #[error("Skiff stream ended while reading {context}")]
    Truncated {
        /// The incomplete portion of the stream.
        context: &'static str,
    },
    /// One row would decode into more memory than the configured limit.
    #[error("Skiff row does not fit in the {limit}-byte decode limit")]
    RowTooLarge {
        /// The configured maximum decoded footprint of one row.
        limit: usize,
    },
    /// A declared blob size would exceed the configured resource limit.
    #[error("Skiff {wire_type} payload is {length} bytes, exceeding the {limit}-byte limit")]
    BlobTooLarge {
        /// The schema type that carried the length.
        wire_type: WireType,
        /// The decoded or supplied payload size.
        length: usize,
        /// The configured maximum.
        limit: usize,
    },
    /// An encoded value did not match its schema node.
    #[error("Skiff {expected} node cannot encode {actual}")]
    ValueDoesNotMatchSchema {
        /// The schema's requested wire type.
        expected: WireType,
        /// The supplied value's stable kind.
        actual: &'static str,
    },
    /// A tuple carried a different number of values than its schema children.
    #[error("Skiff tuple has {actual} values, but its schema has {expected}")]
    TupleLength {
        /// The schema child count.
        expected: usize,
        /// The supplied value count.
        actual: usize,
    },
    /// A variant tag did not select a child in its schema.
    #[error("Skiff {wire_type} tag {tag} has no matching child in a {children}-child schema")]
    InvalidVariantTag {
        /// The variant wire type.
        wire_type: WireType,
        /// The invalid tag.
        tag: u16,
        /// The schema child count.
        children: usize,
    },
    /// A repeated-variant value carried a tag wider than the variant supports.
    #[error("Skiff {wire_type} tag {tag} cannot fit in its tag width")]
    VariantTagTooWide {
        /// The repeated-variant wire type.
        wire_type: WireType,
        /// The tag that did not fit.
        tag: u16,
    },
    /// A caller supplied a payload that cannot fit into its `u32` length prefix.
    #[error("Skiff {wire_type} payload is {length} bytes, which cannot fit in u32")]
    BlobLengthOverflowsU32 {
        /// The affected wire type.
        wire_type: WireType,
        /// The supplied payload length.
        length: usize,
    },
    /// The underlying writer failed.
    #[error("writing Skiff stream: {0}")]
    Write(#[source] std::io::Error),
    /// The underlying reader failed for a reason other than end of stream.
    #[error("reading Skiff stream: {0}")]
    Read(#[source] std::io::Error),
}

fn encode_value<W: Write>(
    output: &mut W,
    schema: &Schema,
    value: &Value,
    max_blob_bytes: usize,
) -> Result<(), CodecError> {
    match (schema.wire_type, value) {
        (WireType::Nothing, Value::Nothing) => Ok(()),
        (WireType::Boolean, Value::Boolean(value)) => write_all(output, &[u8::from(*value)]),
        (WireType::Int8, Value::Int8(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Int16, Value::Int16(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Int32, Value::Int32(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Int64, Value::Int64(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Int128, Value::Int128(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Int256, Value::Int256(value)) => write_all(output, value),
        (WireType::Uint8, Value::Uint8(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Uint16, Value::Uint16(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Uint32, Value::Uint32(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Uint64, Value::Uint64(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::Double, Value::Double(value)) => write_all(output, &value.to_le_bytes()),
        (WireType::String32, Value::Bytes(value)) | (WireType::Yson32, Value::Yson(value)) => {
            write_blob(output, schema.wire_type, value, max_blob_bytes)
        }
        (WireType::Variant8 | WireType::Variant16, Value::Variant { tag, value }) => {
            let child = variant_child(schema, *tag)?;
            write_variant_tag(output, schema.wire_type, *tag)?;
            encode_value(output, child, value, max_blob_bytes)
        }
        (
            WireType::RepeatedVariant8 | WireType::RepeatedVariant16,
            Value::RepeatedVariants(items),
        ) => {
            for item in items {
                let child = variant_child(schema, item.tag)?;
                write_variant_tag(output, schema.wire_type, item.tag)?;
                encode_value(output, child, &item.value, max_blob_bytes)?;
            }
            write_repeated_variant_end(output, schema.wire_type)
        }
        (WireType::Tuple, Value::Tuple(values)) => {
            if values.len() != schema.children.len() {
                return Err(CodecError::TupleLength {
                    expected: schema.children.len(),
                    actual: values.len(),
                });
            }
            for (child, value) in schema.children.iter().zip(values) {
                encode_value(output, child, value, max_blob_bytes)?;
            }
            Ok(())
        }
        (expected, value) => Err(CodecError::ValueDoesNotMatchSchema {
            expected,
            actual: value.kind(),
        }),
    }
}

/// How much decoded row one call to [`Decoder::next_row`] may still produce.
///
/// The charge is an estimate of the decoded footprint — one [`Value`] per
/// decoded node, plus each blob payload — rather than an exact allocation
/// count. That is deliberate: the bound has to be proportional to what a row
/// costs in memory, and for repeated variants that differs from what it costs
/// on the wire by more than an order of magnitude. The blob limit rides along
/// because both bounds are consumed at the same points.
struct RowBudget {
    max_blob_bytes: usize,
    limit: usize,
    remaining: usize,
}

impl RowBudget {
    fn new(max_blob_bytes: usize, max_row_bytes: usize) -> Self {
        Self {
            max_blob_bytes,
            limit: max_row_bytes,
            remaining: max_row_bytes,
        }
    }

    /// Charges `bytes` against the row, before the memory is committed.
    fn charge(&mut self, bytes: usize) -> Result<(), CodecError> {
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or(CodecError::RowTooLarge { limit: self.limit })?;
        Ok(())
    }
}

fn decode_value<R: Read>(
    input: &mut R,
    schema: &Schema,
    budget: &mut RowBudget,
) -> Result<Value, CodecError> {
    // Every decoded node lands in a Box, a Vec or the row itself, so it costs
    // at least one Value. Charging here also bounds the repeated-variant loop
    // below: each item costs a Value, so the item count cannot outrun the
    // budget however few wire bytes an item takes.
    budget.charge(size_of::<Value>())?;
    match schema.wire_type {
        WireType::Nothing => Ok(Value::Nothing),
        WireType::Boolean => Ok(Value::Boolean(read_byte(input, "boolean")? != 0)),
        WireType::Int8 => Ok(Value::Int8(i8::from_le_bytes(read_array(input, "int8")?))),
        WireType::Int16 => Ok(Value::Int16(i16::from_le_bytes(read_array(
            input, "int16",
        )?))),
        WireType::Int32 => Ok(Value::Int32(i32::from_le_bytes(read_array(
            input, "int32",
        )?))),
        WireType::Int64 => Ok(Value::Int64(i64::from_le_bytes(read_array(
            input, "int64",
        )?))),
        WireType::Int128 => Ok(Value::Int128(i128::from_le_bytes(read_array(
            input, "int128",
        )?))),
        WireType::Int256 => Ok(Value::Int256(read_array(input, "int256")?)),
        WireType::Uint8 => Ok(Value::Uint8(read_byte(input, "uint8")?)),
        WireType::Uint16 => Ok(Value::Uint16(u16::from_le_bytes(read_array(
            input, "uint16",
        )?))),
        WireType::Uint32 => Ok(Value::Uint32(u32::from_le_bytes(read_array(
            input, "uint32",
        )?))),
        WireType::Uint64 => Ok(Value::Uint64(u64::from_le_bytes(read_array(
            input, "uint64",
        )?))),
        WireType::Double => Ok(Value::Double(f64::from_le_bytes(read_array(
            input, "double",
        )?))),
        WireType::String32 => Ok(Value::Bytes(read_blob(input, WireType::String32, budget)?)),
        WireType::Yson32 => Ok(Value::Yson(read_blob(input, WireType::Yson32, budget)?)),
        WireType::Variant8 | WireType::Variant16 => {
            let tag = read_variant_tag(input, schema.wire_type)?;
            let child = variant_child(schema, tag)?;
            let value = decode_value(input, child, budget)?;
            Ok(Value::Variant {
                tag,
                value: Box::new(value),
            })
        }
        WireType::RepeatedVariant8 | WireType::RepeatedVariant16 => {
            let mut items = Vec::new();
            loop {
                let tag = read_variant_tag(input, schema.wire_type)?;
                if is_repeated_variant_end(schema.wire_type, tag) {
                    break;
                }
                let child = variant_child(schema, tag)?;
                items.push(Variant {
                    tag,
                    value: decode_value(input, child, budget)?,
                });
            }
            Ok(Value::RepeatedVariants(items))
        }
        WireType::Tuple => {
            let values = schema
                .children
                .iter()
                .map(|child| decode_value(input, child, budget))
                .collect::<Result<_, _>>()?;
            Ok(Value::Tuple(values))
        }
    }
}

/// The wire width and truncation context of a fixed-size type.
///
/// The contexts match [`decode_value`]'s, so a truncated stream reports the
/// same field whichever way it was read.
const fn fixed_width(wire_type: WireType) -> Option<(u64, &'static str)> {
    match wire_type {
        WireType::Boolean => Some((1, "boolean")),
        WireType::Int8 => Some((1, "int8")),
        WireType::Int16 => Some((2, "int16")),
        WireType::Int32 => Some((4, "int32")),
        WireType::Int64 => Some((8, "int64")),
        WireType::Int128 => Some((16, "int128")),
        WireType::Int256 => Some((32, "int256")),
        WireType::Uint8 => Some((1, "uint8")),
        WireType::Uint16 => Some((2, "uint16")),
        WireType::Uint32 => Some((4, "uint32")),
        WireType::Uint64 => Some((8, "uint64")),
        WireType::Double => Some((8, "double")),
        _ => None,
    }
}

fn skip_value<R: Read>(
    input: &mut R,
    schema: &Schema,
    budget: &mut RowBudget,
) -> Result<(), CodecError> {
    // Charged as decode_value charges, so the two accept the same rows. A
    // stream that skips is a stream the decoder can read, which is the whole
    // value of asking the cheap question.
    budget.charge(size_of::<Value>())?;
    if let Some((width, context)) = fixed_width(schema.wire_type) {
        return skip_exact(input, width, context);
    }
    match schema.wire_type {
        WireType::Nothing => Ok(()),
        WireType::String32 | WireType::Yson32 => {
            skip_blob(input, schema.wire_type, budget)?;
            Ok(())
        }
        WireType::Variant8 | WireType::Variant16 => {
            let tag = read_variant_tag(input, schema.wire_type)?;
            skip_value(input, variant_child(schema, tag)?, budget)
        }
        WireType::RepeatedVariant8 | WireType::RepeatedVariant16 => loop {
            let tag = read_variant_tag(input, schema.wire_type)?;
            if is_repeated_variant_end(schema.wire_type, tag) {
                return Ok(());
            }
            skip_value(input, variant_child(schema, tag)?, budget)?;
        },
        WireType::Tuple => {
            for child in &schema.children {
                skip_value(input, child, budget)?;
            }
            Ok(())
        }
        // Every fixed-width type is answered above.
        _ => unreachable!("fixed_width covers the remaining wire types"),
    }
}

fn skip_blob<R: Read>(
    input: &mut R,
    wire_type: WireType,
    budget: &mut RowBudget,
) -> Result<(), CodecError> {
    let length = usize::try_from(u32::from_le_bytes(read_array(input, "blob length")?))
        .expect("u32 always fits usize on supported Rust targets");
    check_blob_length(wire_type, length, budget.max_blob_bytes)?;
    budget.charge(length)?;
    skip_exact(input, length as u64, "blob payload")
}

fn skip_exact<R: Read>(input: &mut R, count: u64, context: &'static str) -> Result<(), CodecError> {
    let skipped = std::io::copy(&mut input.by_ref().take(count), &mut std::io::sink())
        .map_err(CodecError::Read)?;
    if skipped != count {
        return Err(CodecError::Truncated { context });
    }
    Ok(())
}

fn variant_child(schema: &Schema, tag: u16) -> Result<&Schema, CodecError> {
    schema
        .children
        .get(usize::from(tag))
        .ok_or(CodecError::InvalidVariantTag {
            wire_type: schema.wire_type,
            tag,
            children: schema.children.len(),
        })
}

fn write_variant_tag<W: Write>(
    output: &mut W,
    wire_type: WireType,
    tag: u16,
) -> Result<(), CodecError> {
    match wire_type {
        WireType::Variant8 | WireType::RepeatedVariant8 => {
            let tag =
                u8::try_from(tag).map_err(|_| CodecError::VariantTagTooWide { wire_type, tag })?;
            write_all(output, &[tag])
        }
        WireType::Variant16 | WireType::RepeatedVariant16 => write_all(output, &tag.to_le_bytes()),
        _ => unreachable!("only variant schema nodes request a variant tag"),
    }
}

fn read_variant_tag<R: Read>(input: &mut R, wire_type: WireType) -> Result<u16, CodecError> {
    match wire_type {
        WireType::Variant8 | WireType::RepeatedVariant8 => {
            Ok(u16::from(read_byte(input, "variant8 tag")?))
        }
        WireType::Variant16 | WireType::RepeatedVariant16 => {
            Ok(u16::from_le_bytes(read_array(input, "variant16 tag")?))
        }
        _ => unreachable!("only variant schema nodes request a variant tag"),
    }
}

fn write_repeated_variant_end<W: Write>(
    output: &mut W,
    wire_type: WireType,
) -> Result<(), CodecError> {
    match wire_type {
        WireType::RepeatedVariant8 => write_all(output, &[u8::MAX]),
        WireType::RepeatedVariant16 => write_all(output, &u16::MAX.to_le_bytes()),
        _ => unreachable!("only repeated-variant schema nodes have an end tag"),
    }
}

fn is_repeated_variant_end(wire_type: WireType, tag: u16) -> bool {
    match wire_type {
        WireType::RepeatedVariant8 => tag == u16::from(u8::MAX),
        WireType::RepeatedVariant16 => tag == u16::MAX,
        _ => unreachable!("only repeated-variant schema nodes have an end tag"),
    }
}

fn write_blob<W: Write>(
    output: &mut W,
    wire_type: WireType,
    value: &[u8],
    max_blob_bytes: usize,
) -> Result<(), CodecError> {
    check_blob_length(wire_type, value.len(), max_blob_bytes)?;
    let length = u32::try_from(value.len()).map_err(|_| CodecError::BlobLengthOverflowsU32 {
        wire_type,
        length: value.len(),
    })?;
    write_all(output, &length.to_le_bytes())?;
    write_all(output, value)
}

fn read_blob<R: Read>(
    input: &mut R,
    wire_type: WireType,
    budget: &mut RowBudget,
) -> Result<Vec<u8>, CodecError> {
    let length = usize::try_from(u32::from_le_bytes(read_array(input, "blob length")?))
        .expect("u32 always fits usize on supported Rust targets");
    check_blob_length(wire_type, length, budget.max_blob_bytes)?;
    budget.charge(length)?;
    let mut value = vec![0; length];
    read_exact(input, &mut value, "blob payload")?;
    Ok(value)
}

fn check_blob_length(
    wire_type: WireType,
    length: usize,
    max_blob_bytes: usize,
) -> Result<(), CodecError> {
    if length > max_blob_bytes {
        return Err(CodecError::BlobTooLarge {
            wire_type,
            length,
            limit: max_blob_bytes,
        });
    }
    Ok(())
}

fn write_all<W: Write>(output: &mut W, bytes: &[u8]) -> Result<(), CodecError> {
    output.write_all(bytes).map_err(CodecError::Write)
}

fn read_first_byte<R: Read>(input: &mut R) -> Result<Option<u8>, CodecError> {
    let mut byte = [0; 1];
    loop {
        match input.read(&mut byte) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(Some(byte[0])),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(CodecError::Read(error)),
        }
    }
}

fn read_byte<R: Read>(input: &mut R, context: &'static str) -> Result<u8, CodecError> {
    let mut byte = [0; 1];
    read_exact(input, &mut byte, context)?;
    Ok(byte[0])
}

fn read_array<R: Read, const N: usize>(
    input: &mut R,
    context: &'static str,
) -> Result<[u8; N], CodecError> {
    let mut bytes = [0; N];
    read_exact(input, &mut bytes, context)?;
    Ok(bytes)
}

fn read_exact<R: Read>(
    input: &mut R,
    bytes: &mut [u8],
    context: &'static str,
) -> Result<(), CodecError> {
    input.read_exact(bytes).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            CodecError::Truncated { context }
        } else {
            CodecError::Read(error)
        }
    })
}
