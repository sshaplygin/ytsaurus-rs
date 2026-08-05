use serde::de::{
    self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor,
    value::StringDeserializer,
};

use crate::{de::Deserializer, error::YsonError, node::Token};

/// Deserializer for a key that is an arbitrary byte string.
///
/// YSON attribute names, like map keys, are byte strings. Forcing them through
/// `&str` would either fail or silently drop bytes, so the key is handed to the
/// visitor via `visit_bytes`. Both `String` and the field-identifier visitors
/// that `#[derive(Deserialize)]` generates accept that call, so `@`-prefixed
/// struct fields keep working exactly as before.
struct ByteKeyDeserializer(Vec<u8>);

impl<'de> de::Deserializer<'de> for ByteKeyDeserializer {
    type Error = YsonError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_bytes(&self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

#[derive(PartialEq)]
enum FlatState {
    Attributes,
    Between,
    Body,
    ValueOnly,
    Done,
}

pub(crate) struct FlatStructAccess<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    state: FlatState,
    is_value_only: bool,
}

impl<'a, 'de> FlatStructAccess<'a, 'de> {
    pub(crate) fn new(de: &'a mut Deserializer<'de>) -> Result<Self, YsonError> {
        de.enter_recursion()?;

        let state = match de.lexer.peek_byte()? {
            b'<' => {
                de.lexer.next_token()?;
                FlatState::Attributes
            }
            b'{' => {
                de.lexer.next_token()?;
                FlatState::Body
            }
            b'#' => {
                de.lexer.next_token()?;
                FlatState::Done
            }
            _ => FlatState::ValueOnly,
        };

        Ok(FlatStructAccess {
            de,
            state,
            is_value_only: false,
        })
    }
}

impl Drop for FlatStructAccess<'_, '_> {
    fn drop(&mut self) {
        self.de.leave_recursion();
    }
}

impl<'de> MapAccess<'de> for FlatStructAccess<'_, 'de> {
    type Error = YsonError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        loop {
            match self.state {
                FlatState::Attributes => {
                    let peeked = self.de.lexer.peek_byte()?;
                    if peeked == b'>' {
                        self.de.lexer.next_token()?;
                        self.state = FlatState::Between;
                        continue;
                    }
                    if peeked == b';' {
                        self.de.lexer.next_token()?;
                        continue;
                    }

                    let token = self.de.lexer.next_token()?;
                    if let Token::String(s) = token {
                        // `@` marks this key as an attribute rather than a body
                        // field; the rest of the key is passed through verbatim
                        // so that non-UTF-8 names survive.
                        let mut prefixed = Vec::with_capacity(s.len() + 1);
                        prefixed.push(b'@');
                        prefixed.extend_from_slice(&s);
                        self.is_value_only = false;
                        return seed.deserialize(ByteKeyDeserializer(prefixed)).map(Some);
                    }
                    return Err(YsonError::Custom(
                        "Expected string key in attributes".into(),
                    ));
                }
                FlatState::Between => {
                    let peeked = self.de.lexer.peek_byte()?;
                    if peeked == b'{' {
                        self.de.lexer.next_token()?;
                        self.state = FlatState::Body;
                        continue;
                    } else if peeked == b'#' {
                        self.de.lexer.next_token()?;
                        self.state = FlatState::Done;
                        return Ok(None);
                    }
                    self.state = FlatState::ValueOnly;
                    continue;
                }
                FlatState::Body => {
                    let peeked = self.de.lexer.peek_byte()?;
                    if peeked == b'}' {
                        self.de.lexer.next_token()?;
                        self.state = FlatState::Done;
                        return Ok(None);
                    }
                    if peeked == b';' {
                        self.de.lexer.next_token()?;
                        continue;
                    }

                    self.is_value_only = false;
                    return seed.deserialize(&mut *self.de).map(Some);
                }
                FlatState::ValueOnly => {
                    self.state = FlatState::Done;
                    self.is_value_only = true;
                    let deserializer: StringDeserializer<YsonError> =
                        "$value".to_string().into_deserializer();
                    return seed.deserialize(deserializer).map(Some);
                }
                FlatState::Done => return Ok(None),
            }
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        if self.is_value_only {
            return seed.deserialize(&mut *self.de);
        }

        let token = self.de.lexer.next_token()?;
        if token != Token::KeyValueSeparator {
            return Err(YsonError::Custom(format!("Expected '=', got {token:?}")));
        }
        seed.deserialize(&mut *self.de)
    }
}

pub(crate) struct EnumAccess<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    is_map_wrapped: bool,
}

impl<'a, 'de> EnumAccess<'a, 'de> {
    pub(crate) fn new(de: &'a mut Deserializer<'de>, is_map_wrapped: bool) -> Self {
        EnumAccess { de, is_map_wrapped }
    }
}

impl<'de> de::EnumAccess<'de> for EnumAccess<'_, 'de> {
    type Error = YsonError;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self.de)?;
        Ok((val, self))
    }
}

impl<'de> de::VariantAccess<'de> for EnumAccess<'_, 'de> {
    type Error = YsonError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        if self.is_map_wrapped {
            let token = self.de.lexer.next_token()?;
            if token != Token::KeyValueSeparator {
                return Err(YsonError::Custom("Expected '='".into()));
            }
            let val_token = self.de.lexer.next_token()?;
            if val_token != Token::Entity {
                return Err(YsonError::Custom(
                    "Expected '#' for unit variant in map".into(),
                ));
            }
        }
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        let token = self.de.lexer.next_token()?;
        if token != Token::KeyValueSeparator {
            return Err(YsonError::Custom("Expected '='".into()));
        }
        seed.deserialize(self.de)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let token = self.de.lexer.next_token()?;
        if token != Token::KeyValueSeparator {
            return Err(YsonError::Custom("Expected '='".into()));
        }
        de::Deserializer::deserialize_seq(self.de, visitor)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let token = self.de.lexer.next_token()?;
        if token != Token::KeyValueSeparator {
            return Err(YsonError::Custom("Expected '='".into()));
        }
        de::Deserializer::deserialize_map(self.de, visitor)
    }
}

pub(crate) struct EmptyMapAccess;
impl<'de> MapAccess<'de> for EmptyMapAccess {
    type Error = YsonError;
    fn next_key_seed<K>(&mut self, _seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        Ok(None)
    }
    fn next_value_seed<V>(&mut self, _seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        unreachable!()
    }
}

pub(crate) struct AttributesWrapperAccess<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    state: u8,
}

impl<'a, 'de> AttributesWrapperAccess<'a, 'de> {
    pub(crate) fn new(de: &'a mut Deserializer<'de>) -> Result<Self, YsonError> {
        de.enter_recursion()?;
        Ok(AttributesWrapperAccess { de, state: 0 })
    }
}

impl Drop for AttributesWrapperAccess<'_, '_> {
    fn drop(&mut self) {
        self.de.leave_recursion();
    }
}

impl<'de> SeqAccess<'de> for AttributesWrapperAccess<'_, 'de> {
    type Error = YsonError;
    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.state {
            0 => {
                self.state = 1;
                self.de.is_reading_attributes = true;
                let val = seed.deserialize(&mut *self.de)?;
                self.de.is_reading_attributes = false;
                Ok(Some(val))
            }
            1 => {
                self.state = 2;
                let val = seed.deserialize(&mut *self.de)?;
                Ok(Some(val))
            }
            _ => Ok(None),
        }
    }
}

pub(crate) struct CommaSeparated<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    end_byte: u8,
    /// Whether the terminator has been read.
    ///
    /// A visitor that asks for elements until it is told there are none — a
    /// `Vec`, a map — reads it here. A fixed-length one never asks the last
    /// time, so it does not, and the deserializer that opened the container has
    /// to close it. This is what tells the two apart.
    ended: bool,
}

impl<'a, 'de> CommaSeparated<'a, 'de> {
    pub(crate) fn new(de: &'a mut Deserializer<'de>, end_byte: u8) -> Result<Self, YsonError> {
        de.enter_recursion()?;
        Ok(CommaSeparated {
            de,
            end_byte,
            ended: false,
        })
    }

    /// Consumes the terminator and remembers having done so.
    fn take_end(&mut self) -> Result<(), YsonError> {
        self.de.lexer.next_token()?;
        self.ended = true;
        Ok(())
    }
}

impl Drop for CommaSeparated<'_, '_> {
    fn drop(&mut self) {
        // Reported to the deserializer, which reads it the moment the visit
        // returns. Nested containers finish first, so the last write before
        // that read is always this container's own.
        self.de.container_ended = self.ended;
        self.de.leave_recursion();
    }
}

impl<'de> MapAccess<'de> for CommaSeparated<'_, 'de> {
    type Error = YsonError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let peeked = self.de.lexer.peek_byte()?;
        if peeked == self.end_byte {
            self.take_end()?;
            return Ok(None);
        }

        if peeked == b';' {
            self.de.lexer.next_token()?;

            if self.de.lexer.peek_byte()? == self.end_byte {
                self.take_end()?;
                return Ok(None);
            }
        }

        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let token = self.de.lexer.next_token()?;
        if token != Token::KeyValueSeparator {
            return Err(YsonError::Custom(format!("Expected '=', got {token:?}")));
        }

        seed.deserialize(&mut *self.de)
    }
}

impl<'de> SeqAccess<'de> for CommaSeparated<'_, 'de> {
    type Error = YsonError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let peeked = self.de.lexer.peek_byte()?;
        if peeked == self.end_byte {
            self.take_end()?;
            return Ok(None);
        }

        if peeked == b';' {
            self.de.lexer.next_token()?;

            if self.de.lexer.peek_byte()? == self.end_byte {
                self.take_end()?;
                return Ok(None);
            }
        }

        seed.deserialize(&mut *self.de).map(Some)
    }
}
