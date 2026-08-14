//! Layer 4: the row wire format.
//!
//! Rows do not travel as protobuf fields. `api_service.proto` says so outright
//! — "actual data is passed via attachments in the wire protocol" — and the
//! request message carries only a `TRowsetDescriptor` naming the columns. The
//! bytes themselves are this format, which is neither YSON nor Skiff.
//!
//! The layout, from `yt/yt/client/table_client/wire_protocol.cpp` and its
//! second implementation in `yt/go/wire`:
//!
//! ```text
//!   rowset:  u64 row count, then that many rows
//!   row:     u64 value count, or 0xffff_ffff_ffff_ffff for a null row
//!   value:   u64 header, then the payload the header's type calls for
//!
//!   value header (8 bytes):
//!     0..2  u16  column id, an index into the rowset descriptor's name table
//!     2..3  u8   EValueType
//!     3..4  u8   aggregate flag
//!     4..8  u32  payload length, for the string-like types only
//! ```
//!
//! Everything is little-endian and every element is padded to an 8-byte
//! boundary — `SerializationAlignment == sizeof(i64)`. A null value has no
//! payload at all; a scalar has exactly 8 bytes; a string-like value has its
//! bytes followed by zero to seven padding bytes.
//!
//! Sans-io and allocation-conscious: decoding borrows out of the caller's
//! buffer through [`Bytes`], so a rowset is not copied to be read.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// `SerializationAlignment` — `yt/yt/client/table_client/wire_protocol.h`.
pub const ALIGNMENT: usize = 8;

/// The value-count word that marks a null row, distinct from a row with no
/// values. `yt/go/wire/writer.go` calls it `nullRowMarker`.
pub const NULL_ROW_MARKER: u64 = u64::MAX;

/// `MaxRowsPerRowset` — `yt/yt/client/table_client/public.h`.
pub const MAX_ROWS_PER_ROWSET: u64 = 5 * 1024 * 1024;

/// `MaxValuesPerRow` — `yt/yt/client/table_client/public.h`.
pub const MAX_VALUES_PER_ROW: u64 = 1024;

/// `MaxStringValueLength` and `MaxAnyValueLength` — both 16 MB.
pub const MAX_VALUE_LENGTH: u32 = 16 * 1024 * 1024;

/// `EValueType` — `yt/yt/client/table_client/row_base.h`.
///
/// The numbering is not dense: the scalar types are 0x02..0x06 and the
/// string-like ones start again at 0x10.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ValueType {
    Null = 0x02,
    Int64 = 0x03,
    Uint64 = 0x04,
    Double = 0x05,
    Boolean = 0x06,
    String = 0x10,
    Any = 0x11,
    Composite = 0x12,
}

impl ValueType {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            0x02 => Some(Self::Null),
            0x03 => Some(Self::Int64),
            0x04 => Some(Self::Uint64),
            0x05 => Some(Self::Double),
            0x06 => Some(Self::Boolean),
            0x10 => Some(Self::String),
            0x11 => Some(Self::Any),
            0x12 => Some(Self::Composite),
            _ => None,
        }
    }

    /// Whether values of this type carry their payload as a length-prefixed
    /// blob rather than in an 8-byte word.
    ///
    /// `Composite` belongs here. The Go SDK's *reader* agrees, but its writer
    /// omits `Composite` from the branch that writes the blob, so a composite
    /// value it encodes loses its payload; this crate follows the C++, where
    /// `IsStringLikeType` covers `String`, `Any` and `Composite` alike. See
    /// `docs/rpc-compatibility.md`.
    pub fn is_string_like(self) -> bool {
        matches!(self, Self::String | Self::Any | Self::Composite)
    }
}

/// One value of a row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Uint64(u64),
    Double(f64),
    Boolean(bool),
    String(Bytes),
    /// A YSON-encoded value of any shape.
    Any(Bytes),
    /// A YSON-encoded value of a composite column type.
    Composite(Bytes),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Int64(_) => ValueType::Int64,
            Self::Uint64(_) => ValueType::Uint64,
            Self::Double(_) => ValueType::Double,
            Self::Boolean(_) => ValueType::Boolean,
            Self::String(_) => ValueType::String,
            Self::Any(_) => ValueType::Any,
            Self::Composite(_) => ValueType::Composite,
        }
    }

    fn blob(&self) -> Option<&Bytes> {
        match self {
            Self::String(bytes) | Self::Any(bytes) | Self::Composite(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// The 8-byte payload word for the scalar types.
    fn scalar(&self) -> Option<u64> {
        match self {
            Self::Int64(value) => Some(*value as u64),
            Self::Uint64(value) => Some(*value),
            Self::Double(value) => Some(value.to_bits()),
            Self::Boolean(value) => Some(u64::from(*value)),
            _ => None,
        }
    }

    /// The number of bytes this value occupies on the wire, header included.
    pub fn wire_size(&self) -> usize {
        let payload = match self {
            Self::Null => 0,
            Self::String(bytes) | Self::Any(bytes) | Self::Composite(bytes) => {
                bytes.len() + padding_for(bytes.len())
            }
            _ => ALIGNMENT,
        };
        ALIGNMENT + payload
    }
}

/// A value together with the column it belongs to.
#[derive(Debug, Clone, PartialEq)]
pub struct UnversionedValue {
    /// An index into the rowset descriptor's name table, not a column name.
    pub id: u16,
    /// Set on values of aggregate columns; a plain write leaves it false.
    pub aggregate: bool,
    pub value: Value,
}

impl UnversionedValue {
    pub fn new(id: u16, value: Value) -> Self {
        Self {
            id,
            aggregate: false,
            value,
        }
    }

    pub fn wire_size(&self) -> usize {
        self.value.wire_size()
    }
}

/// One row: its values, in the order they were written.
pub type Row = Vec<UnversionedValue>;

/// A row that may be absent. `None` is the null row, which is not the same as
/// a row with no values — a lookup that finds no row for a key reports it as a
/// null row, so collapsing the two loses the answer.
pub type MaybeRow = Option<Row>;

/// What went wrong reading or writing a rowset.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("rowset is truncated: need {needed} more bytes at offset {offset}")]
    Truncated { offset: usize, needed: usize },
    #[error("rowset declares {count} rows, more than the {MAX_ROWS_PER_ROWSET} allowed")]
    TooManyRows { count: u64 },
    #[error("row {row} declares {count} values, more than the {MAX_VALUES_PER_ROW} allowed")]
    TooManyValues { row: usize, count: u64 },
    #[error("value is {length} bytes, more than the {MAX_VALUE_LENGTH} allowed")]
    ValueTooLong { length: u32 },
    #[error("unknown value type {0:#04x}")]
    UnknownValueType(u8),
    #[error("{0} bytes are left over after the last row")]
    TrailingBytes(usize),
}

const fn padding_for(length: usize) -> usize {
    (ALIGNMENT - (length % ALIGNMENT)) % ALIGNMENT
}

/// The number of bytes [`encode_rowset`] will produce.
pub fn encoded_size(rows: &[MaybeRow]) -> usize {
    let mut size = ALIGNMENT;
    for row in rows {
        size += ALIGNMENT;
        if let Some(row) = row {
            size += row.iter().map(UnversionedValue::wire_size).sum::<usize>();
        }
    }
    size
}

/// Encodes a rowset into the wire format.
///
/// Fails rather than emitting a rowset the server will reject or, worse,
/// misread: the limits are checked here as well as on decode, because the
/// length word is a `u32` and a blob larger than that would wrap silently.
/// The Go writer validates the same three limits before producing a byte.
pub fn encode_rowset(rows: &[MaybeRow]) -> Result<Bytes, WireError> {
    let mut buffer = BytesMut::with_capacity(encoded_size(rows));
    encode_rowset_into(rows, &mut buffer)?;
    Ok(buffer.freeze())
}

/// Encodes a rowset, appending to an existing buffer.
///
/// On failure `out` may hold a partial rowset; callers that reuse a buffer
/// should truncate it back themselves.
pub fn encode_rowset_into(rows: &[MaybeRow], out: &mut BytesMut) -> Result<(), WireError> {
    if rows.len() as u64 > MAX_ROWS_PER_ROWSET {
        return Err(WireError::TooManyRows {
            count: rows.len() as u64,
        });
    }

    out.put_u64_le(rows.len() as u64);
    for (index, row) in rows.iter().enumerate() {
        let Some(row) = row else {
            out.put_u64_le(NULL_ROW_MARKER);
            continue;
        };
        if row.len() as u64 > MAX_VALUES_PER_ROW {
            return Err(WireError::TooManyValues {
                row: index,
                count: row.len() as u64,
            });
        }
        out.put_u64_le(row.len() as u64);
        for value in row {
            encode_value(value, out)?;
        }
    }
    Ok(())
}

fn encode_value(value: &UnversionedValue, out: &mut BytesMut) -> Result<(), WireError> {
    let blob = value.value.blob();
    if let Some(blob) = blob
        && blob.len() as u64 > u64::from(MAX_VALUE_LENGTH)
    {
        // Checked against the protocol limit rather than against `u32::MAX`:
        // a blob between the two would fit the length word and still be
        // refused by the server, and one beyond `u32::MAX` would wrap the word
        // and turn the rest of the payload into garbage value headers.
        return Err(WireError::ValueTooLong {
            length: blob.len().min(u32::MAX as usize) as u32,
        });
    }

    out.put_u16_le(value.id);
    out.put_u8(value.value.value_type() as u8);
    out.put_u8(u8::from(value.aggregate));
    // The length word is meaningful only for the string-like types; the C++
    // and Go writers both leave it zero otherwise.
    out.put_u32_le(blob.map_or(0, |bytes| bytes.len() as u32));

    if let Some(scalar) = value.value.scalar() {
        out.put_u64_le(scalar);
    } else if let Some(blob) = blob {
        out.put_slice(blob);
        // Pad with zeroes. The padding bytes are never read back, but writing
        // whatever happened to be in the buffer would make the encoding
        // non-deterministic and the golden vectors meaningless.
        out.put_bytes(0, padding_for(blob.len()));
    }
    Ok(())
}

/// Decodes a rowset from the wire format.
///
/// The returned values borrow `input` rather than copying it.
pub fn decode_rowset(input: &Bytes) -> Result<Vec<MaybeRow>, WireError> {
    let mut reader = Reader { input, offset: 0 };

    let row_count = reader.read_u64()?;
    if row_count > MAX_ROWS_PER_ROWSET {
        return Err(WireError::TooManyRows { count: row_count });
    }

    // `row_count` is bounded, but 5M is still a large reservation to make on a
    // peer's say-so, so grow into it instead of trusting it up front.
    let mut rows = Vec::new();
    for index in 0..row_count as usize {
        let value_count = reader.read_u64()?;
        if value_count == NULL_ROW_MARKER {
            rows.push(None);
            continue;
        }
        if value_count > MAX_VALUES_PER_ROW {
            return Err(WireError::TooManyValues {
                row: index,
                count: value_count,
            });
        }
        let mut row = Row::with_capacity(value_count as usize);
        for _ in 0..value_count {
            row.push(reader.read_value()?);
        }
        rows.push(Some(row));
    }

    if reader.offset != input.len() {
        return Err(WireError::TrailingBytes(input.len() - reader.offset));
    }

    Ok(rows)
}

struct Reader<'a> {
    input: &'a Bytes,
    offset: usize,
}

impl Reader<'_> {
    fn need(&self, count: usize) -> Result<(), WireError> {
        if self.input.len() - self.offset < count {
            return Err(WireError::Truncated {
                offset: self.offset,
                needed: count - (self.input.len() - self.offset),
            });
        }
        Ok(())
    }

    fn read_u64(&mut self) -> Result<u64, WireError> {
        self.need(8)?;
        let mut slice = &self.input[self.offset..self.offset + 8];
        self.offset += 8;
        Ok(slice.get_u64_le())
    }

    fn read_value(&mut self) -> Result<UnversionedValue, WireError> {
        self.need(8)?;
        let header = &self.input[self.offset..self.offset + 8];
        let id = u16::from_le_bytes(header[0..2].try_into().unwrap());
        let raw_type = header[2];
        let aggregate = header[3] != 0;
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap());
        self.offset += 8;

        let value_type =
            ValueType::from_wire(raw_type).ok_or(WireError::UnknownValueType(raw_type))?;

        let value = match value_type {
            ValueType::Null => Value::Null,
            ValueType::Int64 | ValueType::Uint64 | ValueType::Double | ValueType::Boolean => {
                let word = self.read_u64()?;
                match value_type {
                    ValueType::Int64 => Value::Int64(word as i64),
                    ValueType::Uint64 => Value::Uint64(word),
                    ValueType::Double => Value::Double(f64::from_bits(word)),
                    // Any non-zero word is true; the C++ writes 1.
                    _ => Value::Boolean(word != 0),
                }
            }
            ValueType::String | ValueType::Any | ValueType::Composite => {
                if length > MAX_VALUE_LENGTH {
                    return Err(WireError::ValueTooLong { length });
                }
                let length = length as usize;
                let padded = length + padding_for(length);
                self.need(padded)?;
                let blob = self.input.slice(self.offset..self.offset + length);
                self.offset += padded;
                match value_type {
                    ValueType::String => Value::String(blob),
                    ValueType::Any => Value::Any(blob),
                    _ => Value::Composite(blob),
                }
            }
        };

        Ok(UnversionedValue {
            id,
            aggregate,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encodes a rowset that is known to be valid.
    fn unwrap_encode(rows: &[MaybeRow]) -> Bytes {
        encode_rowset(rows).expect("this rowset is within every limit")
    }

    fn round_trip(rows: &[MaybeRow]) -> Vec<MaybeRow> {
        let encoded = unwrap_encode(rows);
        assert_eq!(
            encoded.len(),
            encoded_size(rows),
            "encoded_size disagrees with what encode_rowset wrote"
        );
        assert_eq!(
            encoded.len() % ALIGNMENT,
            0,
            "a rowset is 8-byte aligned throughout"
        );
        decode_rowset(&encoded).expect("what this encoder wrote must decode")
    }

    fn sample_row() -> Row {
        // The same row as `testRow` in `yt/go/wire/row_test.go`, so a reader
        // comparing the two implementations is comparing like with like.
        vec![
            UnversionedValue::new(1, Value::Null),
            UnversionedValue::new(2, Value::Boolean(true)),
            UnversionedValue::new(3, Value::Boolean(false)),
            UnversionedValue::new(4, Value::Int64(-42)),
            UnversionedValue::new(5, Value::Uint64(42)),
            UnversionedValue::new(6, Value::Double(1.25)),
            UnversionedValue::new(7, Value::String(Bytes::from_static(b"foobar"))),
            UnversionedValue::new(8, Value::Any(Bytes::from_static(b"[1;2;3]"))),
            UnversionedValue::new(9, Value::String(Bytes::new())),
        ]
    }

    #[test]
    fn value_types_carry_the_documented_numbers() {
        assert_eq!(ValueType::Null as u8, 0x02);
        assert_eq!(ValueType::Int64 as u8, 0x03);
        assert_eq!(ValueType::Uint64 as u8, 0x04);
        assert_eq!(ValueType::Double as u8, 0x05);
        assert_eq!(ValueType::Boolean as u8, 0x06);
        assert_eq!(ValueType::String as u8, 0x10);
        assert_eq!(ValueType::Any as u8, 0x11);
        assert_eq!(ValueType::Composite as u8, 0x12);
    }

    #[test]
    fn the_rowset_and_row_headers_are_single_words() {
        let encoded = unwrap_encode(&[Some(vec![UnversionedValue::new(0, Value::Int64(7))])]);
        assert_eq!(&encoded[0..8], &1u64.to_le_bytes(), "row count");
        assert_eq!(&encoded[8..16], &1u64.to_le_bytes(), "value count");
        assert_eq!(encoded.len(), 8 + 8 + 8 + 8);
    }

    #[test]
    fn a_value_header_is_id_type_aggregate_length() {
        let value = UnversionedValue {
            id: 0x1234,
            aggregate: true,
            value: Value::String(Bytes::from_static(b"abc")),
        };
        let encoded = unwrap_encode(&[Some(vec![value])]);
        let header = &encoded[16..24];
        assert_eq!(&header[0..2], &0x1234u16.to_le_bytes(), "id");
        assert_eq!(header[2], ValueType::String as u8, "type");
        assert_eq!(header[3], 1, "aggregate");
        assert_eq!(&header[4..8], &3u32.to_le_bytes(), "length");
        assert_eq!(&encoded[24..27], b"abc");
        assert_eq!(
            &encoded[27..32],
            &[0, 0, 0, 0, 0],
            "padded to eight with zeroes"
        );
    }

    #[test]
    fn a_null_value_has_no_payload_at_all() {
        let encoded = unwrap_encode(&[Some(vec![UnversionedValue::new(1, Value::Null)])]);
        // rowset header + row header + one 8-byte value header, and nothing else.
        assert_eq!(encoded.len(), 24);
        assert_eq!(
            &encoded[20..24],
            &0u32.to_le_bytes(),
            "length word stays zero"
        );
    }

    #[test]
    fn scalars_occupy_exactly_one_word() {
        for value in [
            Value::Int64(-1),
            Value::Uint64(u64::MAX),
            Value::Double(-0.0),
            Value::Boolean(true),
        ] {
            let encoded = unwrap_encode(&[Some(vec![UnversionedValue::new(0, value.clone())])]);
            assert_eq!(
                encoded.len(),
                32,
                "{value:?} should be header plus one word"
            );
            assert_eq!(
                &encoded[20..24],
                &0u32.to_le_bytes(),
                "{value:?} must leave the length word zero"
            );
        }
    }

    #[test]
    fn everything_round_trips() {
        let rows = vec![None, Some(Vec::new()), Some(sample_row())];
        assert_eq!(round_trip(&rows), rows);
    }

    #[test]
    fn a_null_row_is_not_an_empty_row() {
        let encoded_null = unwrap_encode(&[None]);
        let encoded_empty = unwrap_encode(&[Some(Vec::new())]);
        assert_ne!(encoded_null, encoded_empty);
        assert_eq!(&encoded_null[8..16], &NULL_ROW_MARKER.to_le_bytes());
        assert_eq!(&encoded_empty[8..16], &0u64.to_le_bytes());

        assert_eq!(decode_rowset(&encoded_null).unwrap(), vec![None]);
        assert_eq!(
            decode_rowset(&encoded_empty).unwrap(),
            vec![Some(Vec::new())]
        );
    }

    #[test]
    fn strings_of_every_length_modulo_eight_round_trip() {
        // The padding rule is where an implementation silently goes wrong, so
        // walk every residue plus the boundary cases.
        for length in 0..24usize {
            let blob = Bytes::from(vec![b'x'; length]);
            let rows = vec![Some(vec![UnversionedValue::new(
                0,
                Value::String(blob.clone()),
            )])];
            let encoded = unwrap_encode(&rows);
            assert_eq!(
                encoded.len() % ALIGNMENT,
                0,
                "length {length} left the stream unaligned"
            );
            assert_eq!(
                round_trip(&rows),
                rows,
                "length {length} did not round-trip"
            );
        }
    }

    /// The Go SDK's writer omits `Composite` from the branch that writes the
    /// blob, so a composite value it encodes arrives empty. The C++ treats
    /// `Composite` as string-like everywhere, and so does this crate.
    #[test]
    fn composite_values_keep_their_payload() {
        let rows = vec![Some(vec![UnversionedValue::new(
            3,
            Value::Composite(Bytes::from_static(b"[1;2;3]")),
        )])];
        let decoded = round_trip(&rows);
        assert_eq!(decoded, rows);
        match &decoded[0].as_ref().unwrap()[0].value {
            Value::Composite(blob) => assert_eq!(blob, &Bytes::from_static(b"[1;2;3]")),
            other => panic!("expected a composite value, got {other:?}"),
        }
    }

    #[test]
    fn doubles_survive_bit_for_bit() {
        for value in [
            0.0,
            -0.0,
            1.25,
            f64::MIN,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            let rows = vec![Some(vec![UnversionedValue::new(0, Value::Double(value))])];
            let decoded = round_trip(&rows);
            match decoded[0].as_ref().unwrap()[0].value {
                Value::Double(read) => assert_eq!(read.to_bits(), value.to_bits()),
                ref other => panic!("expected a double, got {other:?}"),
            }
        }
        // NaN separately: it is never equal to itself, so compare the bits.
        let rows = vec![Some(vec![UnversionedValue::new(
            0,
            Value::Double(f64::NAN),
        )])];
        let encoded = unwrap_encode(&rows);
        match decode_rowset(&encoded).unwrap()[0].as_ref().unwrap()[0].value {
            Value::Double(read) => assert!(read.is_nan()),
            ref other => panic!("expected a double, got {other:?}"),
        }
    }

    #[test]
    fn negative_integers_use_two_s_complement_in_the_word() {
        let rows = vec![Some(vec![UnversionedValue::new(0, Value::Int64(-42))])];
        let encoded = unwrap_encode(&rows);
        assert_eq!(&encoded[24..32], &(-42i64 as u64).to_le_bytes());
        assert_eq!(round_trip(&rows), rows);
    }

    #[test]
    fn the_aggregate_flag_survives() {
        let rows = vec![Some(vec![UnversionedValue {
            id: 5,
            aggregate: true,
            value: Value::Int64(1),
        }])];
        assert_eq!(round_trip(&rows), rows);
    }

    /// Every prefix of a valid rowset must be rejected, and rejected as a
    /// *truncation* rather than as some incidental error — a decoder that
    /// mistook a short buffer for a different fault would be reporting the
    /// wrong thing to the caller.
    #[test]
    fn truncated_input_is_an_error_not_a_panic() {
        let rows = vec![Some(sample_row())];
        let whole = unwrap_encode(&rows);
        for length in 0..whole.len() {
            let truncated = whole.slice(0..length);
            match decode_rowset(&truncated) {
                Err(WireError::Truncated { offset, needed }) => {
                    assert!(
                        needed > 0,
                        "a truncation that needs no more bytes is not one"
                    );
                    assert!(
                        offset <= length,
                        "reported offset {offset} is past the {length} bytes given"
                    );
                }
                Err(other) => panic!("cut to {length} bytes gave {other:?}, not a truncation"),
                Ok(rows) => panic!("a rowset cut to {length} bytes decoded to {rows:?}"),
            }
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut encoded = BytesMut::from(&unwrap_encode(&[Some(sample_row())])[..]);
        encoded.put_u64_le(0);
        assert_eq!(
            decode_rowset(&encoded.freeze()),
            Err(WireError::TrailingBytes(8))
        );
    }

    #[test]
    fn an_unknown_value_type_is_rejected() {
        let mut encoded = BytesMut::from(
            &unwrap_encode(&[Some(vec![UnversionedValue::new(0, Value::Int64(1))])])[..],
        );
        encoded[18] = 0x7f;
        assert_eq!(
            decode_rowset(&encoded.freeze()),
            Err(WireError::UnknownValueType(0x7f))
        );
    }

    #[test]
    fn an_absurd_row_count_is_rejected_before_anything_is_reserved() {
        let mut buffer = BytesMut::new();
        buffer.put_u64_le(MAX_ROWS_PER_ROWSET + 1);
        assert_eq!(
            decode_rowset(&buffer.freeze()),
            Err(WireError::TooManyRows {
                count: MAX_ROWS_PER_ROWSET + 1
            })
        );
    }

    #[test]
    fn an_absurd_value_count_is_rejected() {
        let mut buffer = BytesMut::new();
        buffer.put_u64_le(1);
        buffer.put_u64_le(MAX_VALUES_PER_ROW + 1);
        assert_eq!(
            decode_rowset(&buffer.freeze()),
            Err(WireError::TooManyValues {
                row: 0,
                count: MAX_VALUES_PER_ROW + 1
            })
        );
    }

    #[test]
    fn an_absurd_value_length_is_rejected_before_the_bytes_are_read() {
        let mut buffer = BytesMut::new();
        buffer.put_u64_le(1);
        buffer.put_u64_le(1);
        buffer.put_u16_le(0);
        buffer.put_u8(ValueType::String as u8);
        buffer.put_u8(0);
        buffer.put_u32_le(MAX_VALUE_LENGTH + 1);
        assert_eq!(
            decode_rowset(&buffer.freeze()),
            Err(WireError::ValueTooLong {
                length: MAX_VALUE_LENGTH + 1
            })
        );
    }

    /// A row count that is plausible but unbacked must not make the decoder
    /// reserve for it. This would take 5M rows' worth of allocation if the
    /// count were trusted, and only 16 bytes of input exist.
    #[test]
    fn a_large_but_legal_row_count_with_no_rows_behind_it_fails_cheaply() {
        let mut buffer = BytesMut::new();
        buffer.put_u64_le(MAX_ROWS_PER_ROWSET);
        assert!(matches!(
            decode_rowset(&buffer.freeze()),
            Err(WireError::Truncated { .. })
        ));
    }

    /// The decoder refuses these; so must the encoder. A rowset this crate
    /// emits and then cannot read back would be a bug the golden vectors would
    /// never catch, because they only cover valid input.
    #[test]
    fn the_encoder_refuses_what_the_decoder_would() {
        let too_many_values = vec![Some(
            (0..MAX_VALUES_PER_ROW as u16 + 1)
                .map(|id| UnversionedValue::new(id, Value::Int64(0)))
                .collect::<Row>(),
        )];
        assert_eq!(
            encode_rowset(&too_many_values),
            Err(WireError::TooManyValues {
                row: 0,
                count: MAX_VALUES_PER_ROW + 1
            })
        );

        let too_long = vec![Some(vec![UnversionedValue::new(
            0,
            Value::String(Bytes::from(vec![0u8; MAX_VALUE_LENGTH as usize + 1])),
        )])];
        assert_eq!(
            encode_rowset(&too_long),
            Err(WireError::ValueTooLong {
                length: MAX_VALUE_LENGTH + 1
            })
        );
    }

    #[test]
    fn a_big_rowset_round_trips() {
        let rows: Vec<MaybeRow> = (0..1000)
            .map(|index| {
                Some(vec![
                    UnversionedValue::new(0, Value::Int64(index)),
                    UnversionedValue::new(1, Value::String(Bytes::from(format!("row {index}")))),
                ])
            })
            .collect();
        assert_eq!(round_trip(&rows), rows);
    }
}
