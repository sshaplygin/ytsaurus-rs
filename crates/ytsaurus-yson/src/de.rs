use crate::access::{AttributesWrapperAccess, CommaSeparated, EmptyMapAccess, EnumAccess};
use crate::lexer::YsonIterator;
use crate::node::{Token, YsonNode, YsonValue};
use crate::{access::FlatStructAccess, error::YsonError};
use serde::Deserialize;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// A structure for deserializing YSON data into Rust types.
pub struct Deserializer<'de> {
    pub(crate) lexer: YsonIterator<'de>,
    pub(crate) is_reading_attributes: bool,
    /// Whether the container just visited consumed its own terminator.
    ///
    /// Written by `CommaSeparated`'s `Drop`, read by `end_container` the moment
    /// the visit returns — so it always describes the container that has just
    /// closed, never an earlier one.
    pub(crate) container_ended: bool,
    depth: usize,
    max_depth: usize,
}

impl<'de> Deserializer<'de> {
    /// Creates a new YSON deserializer from the given byte slice.
    ///
    /// # Arguments
    ///
    /// * `input` - The raw byte slice containing YSON data.
    /// * `is_binary` - Set to `true` if the input is in YSON binary format,
    ///   or `false` if it is in YSON text format.
    ///
    /// # Examples
    ///
    /// ```
    /// use ytsaurus_yson::de::Deserializer;
    /// use serde::Deserialize;
    ///
    /// let input = b"42";
    /// let mut de = Deserializer::from_bytes(input, false);
    /// let value = i64::deserialize(&mut de).unwrap();
    ///
    /// assert_eq!(value, 42);
    /// ```
    #[must_use]
    pub fn from_bytes(input: &'de [u8], is_binary: bool) -> Self {
        Deserializer {
            lexer: YsonIterator::new(input, is_binary),
            is_reading_attributes: false,
            container_ended: false,
            depth: 0,
            max_depth: 128,
        }
    }

    /// Closes a container whose visitor stopped reading before the end of it.
    ///
    /// A `Vec` asks for one element more than there are, and the `None` that
    /// answers it is what consumes the `]`. A **fixed-length** visitor — a
    /// tuple, a tuple struct, an array — asks for exactly its length and stops,
    /// so nothing ever reads the terminator. Left there it is read as the
    /// *enclosing* container's: `[[1;2];3]` into `(Vec<i32>, i32)` would end the
    /// outer list at the inner `]` and lose the `3`.
    ///
    /// So whoever opened the container closes it, if the visitor did not.
    ///
    /// # Errors
    ///
    /// Returns [`YsonError`] if what follows is not the terminator — which is
    /// how a list longer than the tuple it is being read into is refused,
    /// rather than silently truncated.
    fn end_container(&mut self, end_byte: u8) -> Result<(), YsonError> {
        if self.container_ended {
            return Ok(());
        }

        // A trailing `;` is allowed before the terminator, exactly as it is
        // between elements: `[10;20;]` is a two-element list.
        if self.lexer.peek_byte()? == b';' {
            self.lexer.next_token()?;
        }

        let byte = self.lexer.peek_byte()?;
        if byte != end_byte {
            return Err(YsonError::Custom(format!(
                "expected {:?} to close the value, found byte {byte:#04x} at offset {}",
                end_byte as char,
                self.lexer.pos()
            )));
        }
        self.lexer.next_token()?;
        Ok(())
    }

    /// Verifies the input is exhausted, insignificant whitespace aside.
    ///
    /// [`crate::from_slice`] calls this after the value: trailing bytes mean a
    /// corrupt or concatenated document, and reading just the front of one as
    /// a healthy value is how corruption goes unnoticed.
    ///
    /// # Errors
    ///
    /// Returns [`YsonError`] naming the offset of the first trailing byte.
    pub fn end(&mut self) -> Result<(), YsonError> {
        match self.lexer.peek_byte() {
            Err(YsonError::Eof) => Ok(()),
            Ok(byte) => Err(YsonError::Custom(format!(
                "trailing data after the value: byte {byte:#04x} at offset {}",
                self.lexer.pos()
            ))),
            Err(other) => Err(other),
        }
    }

    pub(crate) fn enter_recursion(&mut self) -> Result<(), YsonError> {
        self.depth += 1;
        if self.depth > self.max_depth {
            return Err(YsonError::Custom("Recursion limit exceeded".into()));
        }
        Ok(())
    }

    pub(crate) fn leave_recursion(&mut self) {
        self.depth -= 1;
    }

    fn skip_attributes(&mut self) -> Result<(), YsonError> {
        if self.lexer.peek_byte()? == b'<' {
            self.enter_recursion()?;
            self.lexer.next_token()?;
            let mut attr_depth = 1;
            while attr_depth > 0 {
                match self.lexer.next_token()? {
                    Token::BeginAttributes => attr_depth += 1,
                    Token::EndAttributes => attr_depth -= 1,
                    _ => {}
                }
                if attr_depth > self.max_depth {
                    return Err(YsonError::Custom("Attributes nesting too deep".into()));
                }
            }
            self.leave_recursion();
        }
        Ok(())
    }
}

macro_rules! delegate_skip_attributes {
    ( $($method:ident),* $(,)? ) => {
        $(
            fn $method<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                if !self.is_reading_attributes {
                    self.skip_attributes()?;
                }
                self.deserialize_any(visitor)
            }
        )*
    };
}

impl<'de> de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = YsonError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let was_reading_attributes = self.is_reading_attributes;
        self.is_reading_attributes = false;

        if was_reading_attributes {
            if self.lexer.peek_byte()? != b'<' {
                return visitor.visit_map(EmptyMapAccess);
            }
            self.lexer.next_token()?;
            return visitor.visit_map(CommaSeparated::new(self, b'>')?);
        }

        if self.lexer.peek_byte()? == b'<' {
            return visitor.visit_map(FlatStructAccess::new(self)?);
        }

        match self.lexer.next_token()? {
            Token::Entity => visitor.visit_unit(),
            Token::Boolean(b) => visitor.visit_bool(b),
            Token::Int64(i) => visitor.visit_i64(i),
            Token::Uint64(u) => visitor.visit_u64(u),
            Token::Double(d) => visitor.visit_f64(d),
            Token::String(s) => match s {
                Cow::Borrowed(b) => {
                    if let Ok(utf8) = std::str::from_utf8(b) {
                        visitor.visit_borrowed_str(utf8)
                    } else {
                        visitor.visit_borrowed_bytes(b)
                    }
                }
                Cow::Owned(vec) => match String::from_utf8(vec) {
                    Ok(utf8) => visitor.visit_string(utf8),
                    Err(e) => visitor.visit_byte_buf(e.into_bytes()),
                },
            },
            Token::BeginList => {
                let value = visitor.visit_seq(CommaSeparated::new(&mut *self, b']')?)?;
                self.end_container(b']')?;
                Ok(value)
            }
            Token::BeginMap => {
                let value = visitor.visit_map(CommaSeparated::new(&mut *self, b'}')?)?;
                self.end_container(b'}')?;
                Ok(value)
            }
            Token::BeginAttributes => {
                let value = visitor.visit_map(CommaSeparated::new(&mut *self, b'>')?)?;
                self.end_container(b'>')?;
                Ok(value)
            }
            t => Err(YsonError::Custom(format!("Unexpected token: {t:?}"))),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let was_reading_attributes = self.is_reading_attributes;
        self.is_reading_attributes = false;

        if was_reading_attributes {
            if self.lexer.peek_byte()? == b'<' {
                self.is_reading_attributes = true;
                let res = visitor.visit_some(&mut *self);
                self.is_reading_attributes = false;
                res
            } else {
                visitor.visit_none()
            }
        } else {
            self.skip_attributes()?;
            if self.lexer.peek_byte()? == b'#' {
                self.lexer.next_token()?;
                visitor.visit_none()
            } else {
                visitor.visit_some(self)
            }
        }
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if name == "$__yson_attributes" {
            return visitor.visit_seq(AttributesWrapperAccess::new(self)?);
        }
        if fields.iter().any(|f| f.starts_with('@')) {
            return visitor.visit_map(FlatStructAccess::new(self)?);
        }

        if !self.is_reading_attributes {
            self.skip_attributes()?;
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if !self.is_reading_attributes {
            self.skip_attributes()?;
        }

        let peeked = self.lexer.peek_byte()?;
        if peeked == b'{' {
            self.lexer.next_token()?;
            let val = visitor.visit_enum(EnumAccess::new(self, true))?;

            loop {
                match self.lexer.peek_byte() {
                    Ok(b';' | b'}') => break,
                    Ok(_) => {
                        self.lexer.next_token()?;
                    }
                    Err(_) => break,
                }
            }

            if let Ok(b';') = self.lexer.peek_byte() {
                self.lexer.next_token()?;
            }

            match self.lexer.next_token()? {
                Token::EndMap => Ok(val),
                t => Err(YsonError::Custom(format!(
                    "Expected '}}' after variant, got {t:?}"
                ))),
            }
        } else {
            visitor.visit_enum(EnumAccess::new(self, false))
        }
    }

    delegate_skip_attributes! {
        deserialize_bool, deserialize_i8, deserialize_i16, deserialize_i32,
        deserialize_i64, deserialize_i128, deserialize_u8, deserialize_u16,
        deserialize_u32, deserialize_u64, deserialize_u128, deserialize_f32,
        deserialize_f64, deserialize_char, deserialize_str, deserialize_string,
        deserialize_bytes, deserialize_byte_buf, deserialize_unit,
        deserialize_seq, deserialize_map, deserialize_identifier,
        deserialize_ignored_any
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if !self.is_reading_attributes {
            self.skip_attributes()?;
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if !self.is_reading_attributes {
            self.skip_attributes()?;
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if !self.is_reading_attributes {
            self.skip_attributes()?;
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if !self.is_reading_attributes {
            self.skip_attributes()?;
        }
        self.deserialize_any(visitor)
    }
}

/// A streaming deserializer that reads a sequence of YSON values from an input buffer.
///
/// In many YSON use cases, data is provided as a sequence of top-level values
/// optionally separated by semicolons (e.g., `1; 2; 3;`). `StreamDeserializer`
/// allows you to lazily iterate through these values without having to wrap
/// them in a list `[...]`.
///
/// # Examples
///
/// ```
/// use ytsaurus_yson::de::StreamDeserializer;
///
/// let input = b"1; 2; 3";
/// let mut stream = StreamDeserializer::<i32>::new(input, false);
///
/// assert_eq!(stream.next_item().unwrap(), Some(1));
/// assert_eq!(stream.next_item().unwrap(), Some(2));
/// assert_eq!(stream.next_item().unwrap(), Some(3));
/// assert_eq!(stream.next_item().unwrap(), None); // End of stream
/// ```
pub struct StreamDeserializer<'de, T> {
    de: Deserializer<'de>,
    first: bool,
    _marker: std::marker::PhantomData<T>,
}

impl<'de, T> StreamDeserializer<'de, T>
where
    T: de::Deserialize<'de>,
{
    /// Creates a new `StreamDeserializer` from the given byte slice.
    ///
    /// # Arguments
    ///
    /// * `input` - The raw byte slice containing a sequence of YSON values.
    /// * `is_binary` - `true` for binary format, `false` for text format.
    #[must_use]
    pub fn new(input: &'de [u8], is_binary: bool) -> Self {
        Self {
            de: Deserializer::from_bytes(input, is_binary),
            first: true,
            _marker: std::marker::PhantomData,
        }
    }

    /// Deserializes the next item in the stream.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(T))` if a value was successfully deserialized.
    /// - `Ok(None)` if the end of the input was reached.
    /// - `Err(YsonError)` if a parsing error occurred or if the data doesn't match type `T`.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The YSON syntax is malformed.
    /// - An item separator (semicolon) is missing where one is expected.
    /// - The input ends prematurely after a separator.
    pub fn next_item(&mut self) -> Result<Option<T>, YsonError> {
        let peek_res = self.de.lexer.peek_byte();

        if matches!(peek_res, Err(YsonError::Eof)) {
            return Ok(None);
        }

        let next_byte = peek_res?;

        if self.first {
            self.first = false;
        } else if next_byte == b';' {
            self.de.lexer.next_token()?;
            if matches!(self.de.lexer.peek_byte(), Err(YsonError::Eof)) {
                return Ok(None);
            }
        }

        let item = T::deserialize(&mut self.de)?;
        Ok(Some(item))
    }
}

/// A YSON map key or attribute name.
///
/// YSON keys are byte strings, not UTF-8, and [`YsonNode::Map`] stores them as
/// `Vec<u8>` — so decoding a key through `String` would reject perfectly legal
/// documents. This accepts both the UTF-8 and the raw-bytes visitor calls and
/// keeps the bytes intact either way.
struct MapKey(Vec<u8>);

impl<'de> Deserialize<'de> for MapKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MapKeyVisitor;

        impl Visitor<'_> for MapKeyVisitor {
            type Value = MapKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a YSON map key (byte string)")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(MapKey(v.as_bytes().to_vec()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(MapKey(v.into_bytes()))
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(MapKey(v.to_vec()))
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(MapKey(v))
            }
        }

        deserializer.deserialize_any(MapKeyVisitor)
    }
}

macro_rules! impl_visit_primitives {
    ( $( $method:ident ( $v_type:ty ) => $node_variant:ident ),* ) => {
        $(
            fn $method<E>(self, v: $v_type) -> Result<Self::Value, E> {
                Ok(YsonValue {
                    attributes: None,
                    node: YsonNode::$node_variant(v),
                })
            }
        )*
    };
}

impl<'de> Deserialize<'de> for YsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct YsonValueVisitor;

        impl<'de> Visitor<'de> for YsonValueVisitor {
            type Value = YsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("any YSON value")
            }

            impl_visit_primitives! {
                visit_bool(bool) => Boolean,
                visit_i64(i64) => Int64,
                visit_u64(u64) => Uint64,
                visit_f64(f64) => Double
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(YsonValue {
                    attributes: None,
                    node: YsonNode::String(v.as_bytes().to_vec()),
                })
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(YsonValue {
                    attributes: None,
                    node: YsonNode::String(v.to_vec()),
                })
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(YsonValue {
                    attributes: None,
                    node: YsonNode::String(v),
                })
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(YsonValue {
                    attributes: None,
                    node: YsonNode::Entity,
                })
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(elem) = seq.next_element()? {
                    vec.push(elem);
                }
                Ok(YsonValue {
                    attributes: None,
                    node: YsonNode::List(vec),
                })
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut attributes = BTreeMap::new();
                let mut plain_map = BTreeMap::new();
                let mut body_node = None;
                let mut is_attributed = false;

                while let Some(MapKey(key)) = map.next_key::<MapKey>()? {
                    if let Some(attr_name) = key.strip_prefix(b"@") {
                        is_attributed = true;
                        attributes.insert(attr_name.to_vec(), map.next_value()?);
                    } else if key == b"$value" {
                        is_attributed = true;
                        let val: YsonValue = map.next_value()?;
                        body_node = Some(val.node);
                        if let Some(inner_attrs) = val.attributes {
                            attributes.extend(inner_attrs);
                        }
                    } else {
                        plain_map.insert(key, map.next_value()?);
                    }
                }

                if is_attributed {
                    // This is the flat presentation of an attributed value:
                    // "@" keys are the attributes, and the body is either a
                    // "$value" entry (non-map bodies) or the remaining plain
                    // keys (a map body, whose entries arrive at this level).
                    // The plain keys used to be discarded outright, so an
                    // attributed *map* decoded to an attributed entity — the
                    // whole body silently lost.
                    if !plain_map.is_empty() {
                        if body_node.is_some() {
                            let (key, _) = plain_map.iter().next().expect("checked non-empty");
                            return Err(serde::de::Error::custom(format!(
                                "map carries \"$value\" beside a plain key {:?}; \
                                 one value cannot have two bodies",
                                String::from_utf8_lossy(key)
                            )));
                        }
                        body_node = Some(YsonNode::Map(plain_map));
                    }
                    Ok(YsonValue {
                        attributes: if attributes.is_empty() {
                            None
                        } else {
                            Some(attributes)
                        },
                        node: body_node.unwrap_or(YsonNode::Entity),
                    })
                } else {
                    Ok(YsonValue {
                        attributes: None,
                        node: YsonNode::Map(plain_map),
                    })
                }
            }
        }

        deserializer.deserialize_any(YsonValueVisitor)
    }
}
