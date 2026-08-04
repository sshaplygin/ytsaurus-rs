use serde::ser::{Serialize, SerializeMap, SerializeSeq, SerializeStruct, Serializer};
use std::{borrow::Cow, collections::BTreeMap};

/// Represents a complete YSON value, including its optional attributes and data node.
#[derive(Debug, Clone, PartialEq)]
pub struct YsonValue {
    /// Optional attributes associated with this value.
    /// In YSON, attributes are stored as a map of byte strings to other YSON values.
    pub attributes: Option<BTreeMap<Vec<u8>, YsonValue>>,
    /// The data content of this YSON node.
    pub node: YsonNode,
}

impl YsonValue {
    /// Attempts to interpret the node as a UTF-8 string.
    /// Returns `None` if the node is not a string or if the bytes are not valid UTF-8.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        if let YsonNode::String(bytes) = &self.node {
            std::str::from_utf8(bytes).ok()
        } else {
            None
        }
    }

    /// Attempts to interpret the node as a 64-bit signed integer.
    /// Returns `None` if the node is not an `Int64`.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        if let YsonNode::Int64(v) = self.node {
            Some(v)
        } else {
            None
        }
    }

    /// Retrieves an attribute by its string key.
    /// Returns `None` if attributes are missing or the key is not found.
    #[must_use]
    pub fn attr(&self, key: &str) -> Option<&YsonValue> {
        self.attributes.as_ref()?.get(key.as_bytes())
    }
}

impl<'a> std::ops::Index<&'a str> for YsonValue {
    type Output = YsonValue;

    /// Provides convenient access to map elements or attributes using index notation.
    ///
    /// # Panics
    /// Panics if the key is not found or if the value is not a map.
    ///
    /// # Examples
    /// ```
    /// use ytsaurus_yson::{YsonValue, from_slice, YsonFormat};
    ///
    /// let input = b"<status=\"ok\">{id=1u}";
    /// let value: YsonValue = from_slice(input, YsonFormat::Text).unwrap();
    ///
    /// // Access an attribute with '@' prefix
    /// assert_eq!(value["@status"].as_str(), Some("ok"));
    ///
    /// // Access a map field directly
    /// // Note: value["id"] would work if it were a map
    /// ```
    fn index(&self, key: &'a str) -> &Self::Output {
        if let Some(attr_name) = key.strip_prefix('@') {
            return self
                .attributes
                .as_ref()
                .and_then(|a| a.get(attr_name.as_bytes()))
                .expect("Attribute not found");
        }
        if let YsonNode::Map(m) = &self.node {
            return m.get(key.as_bytes()).expect("Key not found in map");
        }
        panic!("Value is not a map");
    }
}

/// Represents the data variants available in the YSON data model.
#[derive(Debug, Clone, PartialEq)]
pub enum YsonNode {
    /// An empty value, represented by `#` in text format.
    Entity,
    /// A boolean value (`%true` or `%false`).
    Boolean(bool),
    /// A signed 64-bit integer.
    Int64(i64),
    /// An unsigned 64-bit integer, followed by `u` in text format (e.g., `42u`).
    Uint64(u64),
    /// A double-precision floating point number.
    Double(f64),
    /// A byte string.
    String(Vec<u8>),
    /// A list of YSON values, enclosed in `[...]`.
    List(Vec<YsonValue>),
    /// A map of byte strings to YSON values, enclosed in `{...}`.
    Map(BTreeMap<Vec<u8>, YsonValue>),
}

/// Serializes a YSON string key/value, which is an arbitrary byte string.
///
/// Valid UTF-8 goes through `serialize_str` so that text output can use the
/// unquoted-identifier form where possible; anything else goes through
/// `serialize_bytes`, which never loses non-UTF-8 bytes. In binary format both
/// paths emit exactly `0x01 + zigzag(len) + raw bytes`.
struct ByteString<'a>(&'a [u8]);

impl Serialize for ByteString<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match std::str::from_utf8(self.0) {
            Ok(s) => serializer.serialize_str(s),
            Err(_) => serializer.serialize_bytes(self.0),
        }
    }
}

/// Serializes a `BTreeMap<Vec<u8>, YsonValue>` preserving non-UTF-8 keys.
struct ByteKeyedMap<'a>(&'a BTreeMap<Vec<u8>, YsonValue>);

impl Serialize for ByteKeyedMap<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(&ByteString(key), value)?;
        }
        map.end()
    }
}

impl Serialize for YsonNode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            YsonNode::Entity => serializer.serialize_unit(),
            YsonNode::Boolean(v) => serializer.serialize_bool(*v),
            YsonNode::Int64(v) => serializer.serialize_i64(*v),
            YsonNode::Uint64(v) => serializer.serialize_u64(*v),
            YsonNode::Double(v) => serializer.serialize_f64(*v),
            YsonNode::String(bytes) => ByteString(bytes).serialize(serializer),
            YsonNode::List(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            YsonNode::Map(entries) => ByteKeyedMap(entries).serialize(serializer),
        }
    }
}

impl Serialize for YsonValue {
    /// Round-trips through [`crate::to_vec`]/[`crate::from_slice`].
    ///
    /// Note that maps are stored in a `BTreeMap`, so keys come back out in
    /// sorted order rather than in the order they appeared in the input. The
    /// round-trip therefore preserves the *value*, not the exact byte layout.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.attributes {
            Some(attributes) if !attributes.is_empty() => {
                // `$__yson_attributes` is the marker the serializer looks for to
                // emit `<attrs>value` rather than a plain map; see `ser.rs`.
                let mut state = serializer.serialize_struct("$__yson_attributes", 2)?;
                state.serialize_field("$attributes", &ByteKeyedMap(attributes))?;
                state.serialize_field("$value", &self.node)?;
                state.end()
            }
            _ => self.node.serialize(serializer),
        }
    }
}

/// Represents individual lexical units (tokens) produced by the YSON lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token<'a> {
    /// Opening bracket for attributes: `<`.
    BeginAttributes,
    /// Closing bracket for attributes: `>`.
    EndAttributes,
    /// Opening bracket for a list: `[`.
    BeginList,
    /// Closing bracket for a list: `]`.
    EndList,
    /// Opening bracket for a map: `{`.
    BeginMap,
    /// Closing bracket for a map: `}`.
    EndMap,

    /// A string literal, either quoted or unquoted. Uses `Cow` for zero-copy borrowing.
    String(Cow<'a, [u8]>),
    /// A signed 64-bit integer literal.
    Int64(i64),
    /// An unsigned 64-bit integer literal.
    Uint64(u64),
    /// A floating point literal.
    Double(f64),
    /// A boolean literal.
    Boolean(bool),
    /// The entity literal: `#`.
    Entity,

    /// Key-value separator: `=`.
    KeyValueSeparator,
    /// Item separator: `;`.
    ItemSeparator,
}
