use crate::error::YsonError;
use serde::{Serialize, ser};

/// Supported format for YSON data representation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum YsonFormat {
    /// Binary format
    Binary,
    /// Text format (human readable)
    Text,
}

/// A structure for serializing Rust types into YSON byte sequences.
pub struct Serializer {
    /// The buffer where the serialized YSON bytes are stored.
    pub output: Vec<u8>,
    pub(crate) is_binary: bool,
    pub(crate) is_writing_attributes: bool,
}

impl Serializer {
    /// Creates a new `Serializer` instance with a pre-allocated buffer.
    ///
    /// # Arguments
    ///
    /// * `is_binary` - Set to `true` for binary output, or `false` for text output.
    ///
    /// # Examples
    ///
    /// ```
    /// use ytsaurus_yson::ser::Serializer;
    /// use serde::Serialize;
    ///
    /// let mut ser = Serializer::new(false);
    /// 42i64.serialize(&mut ser).unwrap();
    ///
    /// assert_eq!(ser.output, b"42");
    /// ```
    #[must_use]
    pub fn new(is_binary: bool) -> Self {
        Self::with_buffer(Vec::with_capacity(8192), is_binary)
    }

    /// Creates a serializer that appends into an existing buffer.
    ///
    /// Lets a caller that serializes many values in a row — a job writing one
    /// row per record, say — reuse a single allocation instead of paying for a
    /// fresh `Vec` each time. The buffer is **not** cleared; clear it yourself
    /// between values if you want them separately.
    #[must_use]
    pub fn with_buffer(buffer: Vec<u8>, is_binary: bool) -> Self {
        Self {
            output: buffer,
            is_binary,
            is_writing_attributes: false,
        }
    }

    /// Consumes the serializer and returns the buffer, ready to be reused.
    #[must_use]
    pub fn into_output(self) -> Vec<u8> {
        self.output
    }

    #[inline]
    fn write_entity(&mut self) {
        self.output.push(0x23);
    }

    fn write_bool(&mut self, v: bool) {
        if self.is_binary {
            self.output.push(if v { 0x05 } else { 0x04 });
        } else {
            self.output
                .extend_from_slice(if v { b"%true" } else { b"%false" });
        }
    }

    fn write_i64(&mut self, v: i64) {
        if self.is_binary {
            self.output.push(0x02);
            crate::varint::write_varint(v, &mut self.output);
        } else {
            self.output
                .extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
    }

    fn write_u64(&mut self, v: u64) {
        if self.is_binary {
            self.output.push(0x06);
            crate::varint::write_uvarint(v, &mut self.output);
        } else {
            self.output
                .extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
            self.output.push(b'u');
        }
    }

    fn write_f64(&mut self, v: f64) {
        if self.is_binary {
            self.output.push(0x03);
            self.output.extend_from_slice(&v.to_le_bytes());
        } else if v.is_nan() {
            self.output.extend_from_slice(b"%nan");
        } else if v.is_infinite() {
            self.output.extend_from_slice(if v.is_sign_negative() {
                b"%-inf"
            } else {
                b"%inf"
            });
        } else {
            let s = ryu::Buffer::new().format(v).to_owned();
            self.output.extend_from_slice(s.as_bytes());
            if !s.contains(&['.', 'e', 'E'][..]) {
                self.output.extend_from_slice(b".0");
            }
        }
    }

    fn write_string(&mut self, v: &str) {
        if self.is_binary {
            self.output.push(0x01);
            crate::varint::write_varint(v.len() as i64, &mut self.output);
            self.output.extend_from_slice(v.as_bytes());
        } else if is_safe_unquoted(v.as_bytes()) {
            self.output.extend_from_slice(v.as_bytes());
        } else {
            self.output.push(b'"');
            for &b in v.as_bytes() {
                match b {
                    b'"' => self.output.extend_from_slice(b"\\\""),
                    b'\\' => self.output.extend_from_slice(b"\\\\"),
                    b'\n' => self.output.extend_from_slice(b"\\n"),
                    b'\r' => self.output.extend_from_slice(b"\\r"),
                    b'\t' => self.output.extend_from_slice(b"\\t"),
                    0x00..=0x1F => {
                        const HEX: &[u8] = b"0123456789abcdef";
                        self.output.extend_from_slice(&[
                            b'\\',
                            b'x',
                            HEX[(b >> 4) as usize],
                            HEX[(b & 0x0F) as usize],
                        ]);
                    }
                    _ => self.output.push(b),
                }
            }
            self.output.push(b'"');
        }
    }
}

macro_rules! impl_serialize {
    // Numbers
    ($($name:ident($ty:ty) => $method:ident as $cast:ty),*) => {
        $(fn $name(self, v: $ty) -> Result<(), Self::Error> { self.$method(v as $cast); Ok(()) })*
    };
    // None, Unit
    (@empty $($name:ident $(($($arg:ident: $ty:ty),*))?),*) => {
        $(fn $name(self $(, $($arg: $ty),*)?) -> Result<(), Self::Error> { self.write_entity(); Ok(()) })*
    };
}

impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = YsonError;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    impl_serialize! {
        serialize_i8(i8) => write_i64 as i64, serialize_i16(i16) => write_i64 as i64,
        serialize_i32(i32) => write_i64 as i64, serialize_i64(i64) => write_i64 as i64,
        serialize_u8(u8) => write_u64 as u64, serialize_u16(u16) => write_u64 as u64,
        serialize_u32(u32) => write_u64 as u64, serialize_u64(u64) => write_u64 as u64,
        serialize_f32(f32) => write_f64 as f64, serialize_f64(f64) => write_f64 as f64
    }

    impl_serialize!(@empty serialize_none, serialize_unit, serialize_unit_struct(_n: &'static str));

    fn serialize_bool(self, v: bool) -> Result<(), Self::Error> {
        self.write_bool(v);
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<(), Self::Error> {
        self.write_string(&v.to_string());
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<(), Self::Error> {
        self.write_string(v);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), Self::Error> {
        if self.is_binary {
            self.output.push(0x01);
            crate::varint::write_varint(v.len() as i64, &mut self.output);
            self.output.extend_from_slice(v);
        } else {
            self.output.push(b'"');
            for &b in v {
                match b {
                    b'"' => self.output.extend_from_slice(b"\\\""),
                    b'\\' => self.output.extend_from_slice(b"\\\\"),
                    b'\n' => self.output.extend_from_slice(b"\\n"),
                    b'\r' => self.output.extend_from_slice(b"\\r"),
                    b'\t' => self.output.extend_from_slice(b"\\t"),
                    0x20..=0x7E => self.output.push(b),
                    _ => {
                        const HEX: &[u8] = b"0123456789abcdef";
                        self.output.extend_from_slice(&[
                            b'\\',
                            b'x',
                            HEX[(b >> 4) as usize],
                            HEX[(b & 0x0F) as usize],
                        ]);
                    }
                }
            }
            self.output.push(b'"');
        }
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, v: &T) -> Result<(), Self::Error> {
        v.serialize(self)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<(), Self::Error> {
        v.serialize(self)
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<(), Self::Error> {
        self.write_string(variant);
        Ok(())
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        var: &'static str,
        val: &T,
    ) -> Result<(), Self::Error> {
        self.output.push(b'{');
        self.write_string(var);
        self.output.push(b'=');
        val.serialize(&mut *self)?;
        self.output.push(b'}');
        Ok(())
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.output.push(b'[');
        Ok(Compound {
            ser: self,
            first: true,
            mode: CompoundMode::Seq,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        var: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.output.push(b'{');
        self.write_string(var);
        self.output.extend_from_slice(b"=[");
        Ok(Compound {
            ser: self,
            first: true,
            mode: CompoundMode::VariantSeq,
        })
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let (open, mode) = if self.is_writing_attributes {
            (b'<', CompoundMode::Attr)
        } else {
            (b'{', CompoundMode::Map)
        };
        self.output.push(open);
        self.is_writing_attributes = false;
        Ok(Compound {
            ser: self,
            first: true,
            mode,
        })
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let mode = if name == "$__yson_attributes" {
            CompoundMode::AttrWrapper
        } else if self.is_writing_attributes {
            self.output.push(b'<');
            self.is_writing_attributes = false;
            CompoundMode::Attr
        } else {
            CompoundMode::Struct {
                attr_open: false,
                body_open: false,
                value_written: false,
            }
        };
        Ok(Compound {
            ser: self,
            first: true,
            mode,
        })
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        var: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.output.push(b'{');
        self.write_string(var);
        self.output.extend_from_slice(b"={");
        Ok(Compound {
            ser: self,
            first: true,
            mode: CompoundMode::VariantMap,
        })
    }
}

#[derive(Clone, Copy)]
enum CompoundMode {
    Seq,
    Map,
    Attr,
    AttrWrapper,
    VariantSeq,
    VariantMap,
    Struct {
        attr_open: bool,
        body_open: bool,
        /// A `$value` field has been written: the struct *is* that value, and
        /// no further field can follow it.
        value_written: bool,
    },
}

/// A helper for serializing compound YSON types such as lists, maps, and structs.
pub struct Compound<'a> {
    ser: &'a mut Serializer,
    first: bool,
    mode: CompoundMode,
}

impl Compound<'_> {
    #[inline]
    fn check_first(&mut self) {
        if !self.first {
            self.ser.output.push(b';');
        }
        self.first = false;
    }
}

macro_rules! delegate_seq {
    ($($trait:ident),*) => {
        $(impl<'a> ser::$trait for Compound<'a> {
            type Ok = (); type Error = YsonError;
            fn serialize_element<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Self::Error> {
                self.check_first(); v.serialize(&mut *self.ser)
            }
            fn end(self) -> Result<(), Self::Error> { self.ser.output.push(b']'); Ok(()) }
        })*
    };
}
delegate_seq!(SerializeSeq, SerializeTuple);

impl ser::SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = YsonError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Self::Error> {
        self.check_first();
        v.serialize(&mut *self.ser)
    }
    fn end(self) -> Result<(), Self::Error> {
        self.ser.output.push(b']');
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = YsonError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Self::Error> {
        self.check_first();
        v.serialize(&mut *self.ser)
    }
    fn end(self) -> Result<(), Self::Error> {
        self.ser.output.extend_from_slice(b"]}");
        Ok(())
    }
}

impl ser::SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = YsonError;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.check_first();
        key.serialize(&mut *self.ser)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.ser.output.push(b'=');
        value.serialize(&mut *self.ser)
    }
    fn end(self) -> Result<(), Self::Error> {
        self.ser
            .output
            .push(if matches!(self.mode, CompoundMode::Attr) {
                b'>'
            } else {
                b'}'
            });
        Ok(())
    }
}

impl ser::SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = YsonError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        match self.mode {
            CompoundMode::AttrWrapper => {
                if key == "$attributes" {
                    self.ser.is_writing_attributes = true;
                    value.serialize(&mut *self.ser)?;
                } else if key == "$value" {
                    value.serialize(&mut *self.ser)?;
                }
            }
            CompoundMode::Struct {
                mut attr_open,
                mut body_open,
                mut value_written,
            } => {
                if let Some(attr_name) = key.strip_prefix('@') {
                    // YSON attributes stand strictly before the value they
                    // decorate. Opening `<` here would land it inside the map
                    // body, and the output would not parse.
                    if body_open || value_written {
                        return Err(YsonError::Custom(format!(
                            "attribute field \"@{attr_name}\" is declared after a value \
                             field; attribute fields must come first in the struct"
                        )));
                    }
                    if !attr_open {
                        self.ser.output.push(b'<');
                        attr_open = true;
                        self.first = true;
                    }
                    self.check_first();
                    self.ser.write_string(attr_name);
                    self.ser.output.push(b'=');
                } else {
                    if attr_open {
                        self.ser.output.push(b'>');
                        attr_open = false;
                    }
                    if key == "$value" {
                        // The struct *is* this value; it cannot also have map
                        // entries, before or after.
                        if body_open || value_written {
                            return Err(YsonError::Custom(
                                "\"$value\" cannot share a struct with plain fields: \
                                 one value cannot have two bodies"
                                    .into(),
                            ));
                        }
                        value_written = true;
                    } else {
                        if value_written {
                            return Err(YsonError::Custom(format!(
                                "field \"{key}\" is declared after \"$value\"; a struct \
                                 with a \"$value\" field can carry only attributes beside it"
                            )));
                        }
                        if !body_open {
                            self.ser.output.push(b'{');
                            body_open = true;
                            self.first = true;
                        }
                        self.check_first();
                        self.ser.write_string(key);
                        self.ser.output.push(b'=');
                    }
                }

                self.mode = CompoundMode::Struct {
                    attr_open,
                    body_open,
                    value_written,
                };
                value.serialize(&mut *self.ser)?;
            }
            _ => {
                self.check_first();
                self.ser.write_string(key);
                self.ser.output.push(b'=');
                value.serialize(&mut *self.ser)?;
            }
        }
        Ok(())
    }

    fn end(self) -> Result<(), Self::Error> {
        match self.mode {
            CompoundMode::Attr => self.ser.output.push(b'>'),
            CompoundMode::Seq | CompoundMode::VariantSeq => self.ser.output.push(b']'),
            CompoundMode::Struct {
                attr_open,
                body_open,
                value_written,
            } => {
                if attr_open {
                    // Every field was an attribute. An attribute block cannot
                    // stand alone, and the only value an all-attribute struct
                    // can mean is the entity: `<a=1>#`.
                    self.ser.output.extend_from_slice(b">#");
                } else if body_open {
                    self.ser.output.push(b'}');
                } else if !value_written {
                    // No fields at all. Zero bytes is not a YSON value; an
                    // empty struct is an empty map.
                    self.ser.output.extend_from_slice(b"{}");
                }
            }
            CompoundMode::AttrWrapper => {}
            _ => self.ser.output.push(b'}'),
        }
        Ok(())
    }
}

impl ser::SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = YsonError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        k: &'static str,
        v: &T,
    ) -> Result<(), Self::Error> {
        ser::SerializeStruct::serialize_field(self, k, v)
    }
    fn end(self) -> Result<(), Self::Error> {
        self.ser.output.extend_from_slice(b"}}");
        Ok(())
    }
}

fn is_safe_unquoted(b: &[u8]) -> bool {
    matches!(b.first(), Some(f) if f.is_ascii_alphabetic() || *f == b'_')
        && b.iter()
            .all(|&c| c.is_ascii_alphanumeric() || b"_-.".contains(&c))
}
