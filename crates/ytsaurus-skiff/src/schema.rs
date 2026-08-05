//! Schema and format values exchanged with YTsaurus.
//!
//! Reference: <https://ytsaurus.tech/docs/en/user-guide/storage/skiff>.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;
use ytsaurus_yson::{YsonNode, YsonValue};

/// A Skiff encoding used by one schema node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WireType {
    /// Empty payload; valid only as a variant child.
    Nothing,
    /// One byte, zero or one.
    Boolean,
    /// One signed byte.
    Int8,
    /// Two little-endian signed bytes.
    Int16,
    /// Four little-endian signed bytes.
    Int32,
    /// Eight little-endian signed bytes.
    Int64,
    /// Sixteen little-endian signed bytes.
    Int128,
    /// Thirty-two little-endian signed bytes.
    Int256,
    /// One unsigned byte.
    Uint8,
    /// Two little-endian unsigned bytes.
    Uint16,
    /// Four little-endian unsigned bytes.
    Uint32,
    /// Eight little-endian unsigned bytes.
    Uint64,
    /// Eight-byte IEEE 754 floating-point number.
    Double,
    /// A little-endian `u32` length followed by arbitrary bytes.
    String32,
    /// A little-endian `u32` length followed by binary YSON bytes.
    Yson32,
    /// An eight-bit child tag followed by that child's value.
    Variant8,
    /// A sixteen-bit child tag followed by that child's value.
    Variant16,
    /// A sequence of `variant8` values ending in tag `0xff`.
    RepeatedVariant8,
    /// A sequence of `variant16` values ending in tag `0xffff`.
    RepeatedVariant16,
    /// The concatenation of its children's values.
    Tuple,
}

impl WireType {
    /// The protocol spelling used in a Skiff schema's `wire_type` field.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nothing => "nothing",
            Self::Boolean => "boolean",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Int128 => "int128",
            Self::Int256 => "int256",
            Self::Uint8 => "uint8",
            Self::Uint16 => "uint16",
            Self::Uint32 => "uint32",
            Self::Uint64 => "uint64",
            Self::Double => "double",
            Self::String32 => "string32",
            Self::Yson32 => "yson32",
            Self::Variant8 => "variant8",
            Self::Variant16 => "variant16",
            Self::RepeatedVariant8 => "repeated_variant8",
            Self::RepeatedVariant16 => "repeated_variant16",
            Self::Tuple => "tuple",
        }
    }

    /// Parses the protocol spelling used in a Skiff schema.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "nothing" => Self::Nothing,
            "boolean" => Self::Boolean,
            "int8" => Self::Int8,
            "int16" => Self::Int16,
            "int32" => Self::Int32,
            "int64" => Self::Int64,
            "int128" => Self::Int128,
            "int256" => Self::Int256,
            "uint8" => Self::Uint8,
            "uint16" => Self::Uint16,
            "uint32" => Self::Uint32,
            "uint64" => Self::Uint64,
            "double" => Self::Double,
            "string32" => Self::String32,
            "yson32" => Self::Yson32,
            "variant8" => Self::Variant8,
            "variant16" => Self::Variant16,
            "repeated_variant8" => Self::RepeatedVariant8,
            "repeated_variant16" => Self::RepeatedVariant16,
            "tuple" => Self::Tuple,
            _ => return None,
        })
    }

    /// Whether this type is a simple payload rather than a schema container.
    #[must_use]
    pub const fn is_simple(self) -> bool {
        matches!(
            self,
            Self::Boolean
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Int128
                | Self::Int256
                | Self::Uint8
                | Self::Uint16
                | Self::Uint32
                | Self::Uint64
                | Self::Double
                | Self::String32
                | Self::Yson32
        )
    }

    /// The fixed payload width, or `None` for variable-width/container types.
    #[must_use]
    pub const fn fixed_width(self) -> Option<usize> {
        match self {
            Self::Nothing => Some(0),
            Self::Boolean | Self::Int8 | Self::Uint8 => Some(1),
            Self::Int16 | Self::Uint16 => Some(2),
            Self::Int32 | Self::Uint32 => Some(4),
            Self::Int64 | Self::Uint64 | Self::Double => Some(8),
            Self::Int128 => Some(16),
            Self::Int256 => Some(32),
            Self::String32
            | Self::Yson32
            | Self::Variant8
            | Self::Variant16
            | Self::RepeatedVariant8
            | Self::RepeatedVariant16
            | Self::Tuple => None,
        }
    }
}

impl std::fmt::Display for WireType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One node in a Skiff schema tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// How this node is encoded.
    pub wire_type: WireType,
    /// Optional name used to map a table field to a table column.
    pub name: Option<String>,
    /// Child schema nodes, in wire order.
    pub children: Vec<Schema>,
}

impl Schema {
    /// Creates an unnamed schema node with no children.
    #[must_use]
    pub const fn leaf(wire_type: WireType) -> Self {
        Self {
            wire_type,
            name: None,
            children: Vec::new(),
        }
    }

    /// Creates a named schema node with no children.
    #[must_use]
    pub fn named(name: impl Into<String>, wire_type: WireType) -> Self {
        Self {
            wire_type,
            name: Some(name.into()),
            children: Vec::new(),
        }
    }

    /// Creates a tuple schema from children in their wire order.
    #[must_use]
    pub fn tuple(children: impl IntoIterator<Item = Schema>) -> Self {
        Self {
            wire_type: WireType::Tuple,
            name: None,
            children: children.into_iter().collect(),
        }
    }

    /// Wraps this schema as `variant8<nothing; self>`, the table encoding for
    /// an optional simple column.
    #[must_use]
    pub fn optional(self) -> Self {
        Self {
            wire_type: WireType::Variant8,
            name: self.name.clone(),
            children: vec![Schema::leaf(WireType::Nothing), Self { name: None, ..self }],
        }
    }

    /// Validates this schema independently of a table or format.
    ///
    /// This deliberately checks structural safety only. Table-specific rules
    /// such as dense/sparse columns are validated by the job/client layer that
    /// knows which format direction it is configuring.
    pub fn validate(&self) -> Result<(), SchemaError> {
        let count = self.children.len();
        if self.wire_type == WireType::Nothing || self.wire_type.is_simple() {
            if count != 0 {
                return Err(SchemaError::UnexpectedChildren {
                    wire_type: self.wire_type,
                    count,
                });
            }
        } else {
            match self.wire_type {
                WireType::Variant8 if count > 256 => {
                    return Err(SchemaError::TooManyChildren {
                        wire_type: self.wire_type,
                        count,
                        maximum: 256,
                    });
                }
                WireType::Variant16 if count > 65_536 => {
                    return Err(SchemaError::TooManyChildren {
                        wire_type: self.wire_type,
                        count,
                        maximum: 65_536,
                    });
                }
                WireType::RepeatedVariant8 if count > 255 => {
                    return Err(SchemaError::TooManyChildren {
                        wire_type: self.wire_type,
                        count,
                        maximum: 255,
                    });
                }
                WireType::RepeatedVariant16 if count > 65_535 => {
                    return Err(SchemaError::TooManyChildren {
                        wire_type: self.wire_type,
                        count,
                        maximum: 65_535,
                    });
                }
                WireType::Variant8
                | WireType::Variant16
                | WireType::RepeatedVariant8
                | WireType::RepeatedVariant16
                | WireType::Tuple => {}
                // The condition above has already handled every leaf.
                _ => unreachable!("Skiff leaves were handled before compound validation"),
            }
        }

        for child in &self.children {
            child.validate()?;
        }
        Ok(())
    }

    /// Renders this schema as the YSON map used in `table_skiff_schemas`.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut fields = BTreeMap::new();
        fields.insert(b"wire_type".to_vec(), string(self.wire_type.as_str()));
        if let Some(name) = &self.name {
            fields.insert(b"name".to_vec(), string(name));
        }
        if !self.children.is_empty() {
            fields.insert(
                b"children".to_vec(),
                list(self.children.iter().map(Self::to_yson)),
            );
        }
        value(YsonNode::Map(fields))
    }

    /// Parses and structurally validates a schema YSON map.
    pub fn from_yson(input: &YsonValue) -> Result<Self, SchemaError> {
        reject_attributes(input, "schema")?;
        let fields = map_fields(input, "schema")?;
        reject_unknown(fields, &[b"wire_type", b"name", b"children"], "schema")?;

        let wire_type = required_string(fields, b"wire_type", "schema")?;
        let wire_type = WireType::parse(wire_type)
            .ok_or_else(|| SchemaError::UnknownWireType(wire_type.to_owned()))?;
        let name = optional_string(fields, b"name", "schema")?.map(str::to_owned);
        let children = match fields.get(b"children".as_slice()) {
            None => Vec::new(),
            Some(child_values) => list_items(child_values, "schema.children")?
                .iter()
                .map(Self::from_yson)
                .collect::<Result<_, _>>()?,
        };

        let schema = Self {
            wire_type,
            name,
            children,
        };
        schema.validate()?;
        Ok(schema)
    }
}

/// A table schema placed inline in a format or referenced through its registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRef {
    /// A schema value in `table_skiff_schemas`.
    Inline(Schema),
    /// A `$name` lookup in `skiff_schema_registry`.
    Registry(String),
}

/// A YTsaurus `<...>skiff` format declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Format {
    table_schemas: Vec<SchemaRef>,
    schema_registry: BTreeMap<String, Schema>,
}

impl Format {
    /// Builds a Skiff format with inline table schemas only.
    ///
    /// Use [`Format::from_parts`] when a table schema refers to the registry.
    pub fn new(table_schemas: Vec<SchemaRef>) -> Result<Self, SchemaError> {
        Self::from_parts(table_schemas, BTreeMap::new())
    }

    /// Builds a Skiff format and validates its table schemas and registry.
    pub fn from_parts(
        table_schemas: Vec<SchemaRef>,
        schema_registry: BTreeMap<String, Schema>,
    ) -> Result<Self, SchemaError> {
        let format = Self {
            table_schemas,
            schema_registry,
        };
        format.validate()?;
        Ok(format)
    }

    /// Schemas in table-index order.
    #[must_use]
    pub fn table_schemas(&self) -> &[SchemaRef] {
        &self.table_schemas
    }

    /// Schemas shared by one or more [`SchemaRef::Registry`] values.
    #[must_use]
    pub fn schema_registry(&self) -> &BTreeMap<String, Schema> {
        &self.schema_registry
    }

    /// Resolves the schema for table `index`.
    pub fn table_schema(&self, index: usize) -> Result<&Schema, SchemaError> {
        let reference = self
            .table_schemas
            .get(index)
            .ok_or(SchemaError::MissingTableSchema { index })?;
        match reference {
            SchemaRef::Inline(schema) => Ok(schema),
            SchemaRef::Registry(name) => self
                .schema_registry
                .get(name)
                .ok_or_else(|| SchemaError::UnknownRegistryReference(name.clone())),
        }
    }

    /// Validates all format references and schema trees.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.table_schemas.is_empty() {
            return Err(SchemaError::EmptyTableSchemas);
        }
        for schema in self.schema_registry.values() {
            schema.validate()?;
        }
        for (index, reference) in self.table_schemas.iter().enumerate() {
            match reference {
                SchemaRef::Inline(schema) => schema.validate()?,
                SchemaRef::Registry(name) if self.schema_registry.contains_key(name) => {}
                SchemaRef::Registry(name) => {
                    return Err(SchemaError::UnknownRegistryReference(name.clone()));
                }
            }
            // Resolving here pins the error to a table position if a future
            // reference representation adds more ways to fail.
            validate_table_schema(self.table_schema(index)?)?;
        }
        Ok(())
    }

    /// Renders `<table_skiff_schemas=[...];...>skiff` for YTsaurus requests.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        let mut attributes = BTreeMap::new();
        attributes.insert(
            b"table_skiff_schemas".to_vec(),
            list(self.table_schemas.iter().map(SchemaRef::to_yson)),
        );
        if !self.schema_registry.is_empty() {
            let mut registry = BTreeMap::new();
            for (name, schema) in &self.schema_registry {
                registry.insert(name.as_bytes().to_vec(), schema.to_yson());
            }
            attributes.insert(
                b"skiff_schema_registry".to_vec(),
                value(YsonNode::Map(registry)),
            );
        }
        YsonValue {
            attributes: Some(attributes),
            node: YsonNode::String(b"skiff".to_vec()),
        }
    }

    /// Parses and validates a YTsaurus Skiff format declaration.
    pub fn from_yson(input: &YsonValue) -> Result<Self, SchemaError> {
        let YsonNode::String(name) = &input.node else {
            return Err(SchemaError::FormatMustBeSkiff);
        };
        if name.as_slice() != b"skiff" {
            return Err(SchemaError::FormatMustBeSkiff);
        }
        let attributes = input
            .attributes
            .as_ref()
            .ok_or(SchemaError::MissingFormatAttribute("table_skiff_schemas"))?;
        reject_unknown(
            attributes,
            &[b"table_skiff_schemas", b"skiff_schema_registry"],
            "format attributes",
        )?;

        let table_values = attributes
            .get(b"table_skiff_schemas".as_slice())
            .ok_or(SchemaError::MissingFormatAttribute("table_skiff_schemas"))?;
        let table_schemas = list_items(table_values, "format.table_skiff_schemas")?
            .iter()
            .map(SchemaRef::from_yson)
            .collect::<Result<_, _>>()?;

        let schema_registry = match attributes.get(b"skiff_schema_registry".as_slice()) {
            None => BTreeMap::new(),
            Some(value) => {
                reject_attributes(value, "format.skiff_schema_registry")?;
                let entries = map_fields(value, "format.skiff_schema_registry")?;
                let mut registry = BTreeMap::new();
                for (name, schema) in entries {
                    let name = std::str::from_utf8(name).map_err(|_| SchemaError::InvalidUtf8 {
                        field: "format.skiff_schema_registry key",
                    })?;
                    registry.insert(name.to_owned(), Schema::from_yson(schema)?);
                }
                registry
            }
        };

        Self::from_parts(table_schemas, schema_registry)
    }
}

impl SchemaRef {
    fn to_yson(&self) -> YsonValue {
        match self {
            Self::Inline(schema) => schema.to_yson(),
            Self::Registry(name) => string(format!("${name}")),
        }
    }

    fn from_yson(input: &YsonValue) -> Result<Self, SchemaError> {
        reject_attributes(input, "table schema reference")?;
        match &input.node {
            YsonNode::String(name) if name.first() == Some(&b'$') && name.len() > 1 => {
                let name =
                    std::str::from_utf8(&name[1..]).map_err(|_| SchemaError::InvalidUtf8 {
                        field: "table schema registry reference",
                    })?;
                Ok(Self::Registry(name.to_owned()))
            }
            YsonNode::String(_) => Err(SchemaError::InvalidRegistryReference),
            YsonNode::Map(_) => Ok(Self::Inline(Schema::from_yson(input)?)),
            _ => Err(SchemaError::InvalidSchemaReference),
        }
    }
}

/// A schema or format declaration the codec refuses before reading a stream.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// A `wire_type` value is not part of the Skiff protocol.
    #[error("unknown Skiff wire type {0:?}")]
    UnknownWireType(String),
    /// A leaf type declared child schemas.
    #[error("Skiff {wire_type} cannot have {count} child schema node(s)")]
    UnexpectedChildren {
        /// The leaf type that was given children.
        wire_type: WireType,
        /// The supplied child count.
        count: usize,
    },
    /// A variant has more children than its tag can name.
    #[error("Skiff {wire_type} has {count} children, exceeding its {maximum} child limit")]
    TooManyChildren {
        /// The variant type.
        wire_type: WireType,
        /// The supplied child count.
        count: usize,
        /// The tag's largest allowed child count.
        maximum: usize,
    },
    /// A format omitted the required table-schema list.
    #[error("Skiff format requires at least one table schema")]
    EmptyTableSchemas,
    /// A table schema lookup named no registered schema.
    #[error("Skiff schema registry has no entry named {0:?}")]
    UnknownRegistryReference(String),
    /// A table index was not included in a format.
    #[error("Skiff format has no schema for table index {index}")]
    MissingTableSchema {
        /// The requested table index.
        index: usize,
    },
    /// The format value was not the literal string `skiff`.
    #[error("Skiff format must be the attributed string \"skiff\"")]
    FormatMustBeSkiff,
    /// A required format attribute was absent.
    #[error("Skiff format is missing required attribute {0:?}")]
    MissingFormatAttribute(&'static str),
    /// A registry reference did not start with a non-empty `$` name.
    #[error("Skiff registry reference must be a non-empty string beginning with '$'")]
    InvalidRegistryReference,
    /// A table schema entry was neither an inline schema map nor a registry reference.
    #[error("Skiff table schema reference must be a schema map or '$' registry reference")]
    InvalidSchemaReference,
    /// A YSON value had attributes where the schema grammar forbids them.
    #[error("Skiff {context} must not carry YSON attributes")]
    UnexpectedAttributes {
        /// The grammar item that had attributes.
        context: &'static str,
    },
    /// A YSON value did not have the expected map shape.
    #[error("Skiff {context} must be a YSON map")]
    ExpectedMap {
        /// The grammar item that was not a map.
        context: &'static str,
    },
    /// A YSON value did not have the expected list shape.
    #[error("Skiff {context} must be a YSON list")]
    ExpectedList {
        /// The grammar item that was not a list.
        context: &'static str,
    },
    /// A required map field was absent.
    #[error("Skiff {context} is missing required field {field:?}")]
    MissingField {
        /// The enclosing schema object.
        context: &'static str,
        /// The absent field name.
        field: &'static str,
    },
    /// A map field did not contain a YSON string.
    #[error("Skiff {context}.{field} must be a YSON string")]
    ExpectedString {
        /// The enclosing schema object.
        context: &'static str,
        /// The malformed field name.
        field: &'static str,
    },
    /// A field that must be text contained invalid UTF-8 bytes.
    #[error("Skiff {field} must be valid UTF-8")]
    InvalidUtf8 {
        /// The malformed field.
        field: &'static str,
    },
    /// A schema map carried an unsupported field.
    #[error("Skiff {context} has unsupported field {field:?}")]
    UnknownField {
        /// The enclosing schema object.
        context: &'static str,
        /// The unknown field, rendered losslessly for diagnostics.
        field: String,
    },
    /// A table schema's root node was not a tuple.
    #[error("Skiff table schema root must be tuple, got {found}")]
    TableSchemaRootMustBeTuple {
        /// The supplied root type.
        found: WireType,
    },
    /// A direct child of the table-root tuple had no name.
    #[error("Skiff table schema child {index} must have a non-empty name")]
    TableSchemaChildMissingName {
        /// The child position in the root tuple.
        index: usize,
    },
}

pub(crate) fn validate_table_schema(schema: &Schema) -> Result<(), SchemaError> {
    if schema.wire_type != WireType::Tuple {
        return Err(SchemaError::TableSchemaRootMustBeTuple {
            found: schema.wire_type,
        });
    }
    for (index, child) in schema.children.iter().enumerate() {
        if child.name.as_deref().is_none_or(str::is_empty) {
            return Err(SchemaError::TableSchemaChildMissingName { index });
        }
    }
    Ok(())
}

fn string(bytes: impl AsRef<[u8]>) -> YsonValue {
    value(YsonNode::String(bytes.as_ref().to_vec()))
}

fn list(values: impl IntoIterator<Item = YsonValue>) -> YsonValue {
    value(YsonNode::List(values.into_iter().collect()))
}

fn value(node: YsonNode) -> YsonValue {
    YsonValue {
        attributes: None,
        node,
    }
}

fn reject_attributes(value: &YsonValue, context: &'static str) -> Result<(), SchemaError> {
    if value.attributes.is_some() {
        return Err(SchemaError::UnexpectedAttributes { context });
    }
    Ok(())
}

fn map_fields<'a>(
    value: &'a YsonValue,
    context: &'static str,
) -> Result<&'a BTreeMap<Vec<u8>, YsonValue>, SchemaError> {
    match &value.node {
        YsonNode::Map(fields) => Ok(fields),
        _ => Err(SchemaError::ExpectedMap { context }),
    }
}

fn list_items<'a>(
    value: &'a YsonValue,
    context: &'static str,
) -> Result<&'a [YsonValue], SchemaError> {
    match &value.node {
        YsonNode::List(items) => Ok(items),
        _ => Err(SchemaError::ExpectedList { context }),
    }
}

fn required_string<'a>(
    fields: &'a BTreeMap<Vec<u8>, YsonValue>,
    field: &'static [u8],
    context: &'static str,
) -> Result<&'a str, SchemaError> {
    let value = fields.get(field).ok_or(SchemaError::MissingField {
        context,
        field: std::str::from_utf8(field).expect("literal field name"),
    })?;
    as_utf8_string(
        value,
        context,
        std::str::from_utf8(field).expect("literal field name"),
    )
}

fn optional_string<'a>(
    fields: &'a BTreeMap<Vec<u8>, YsonValue>,
    field: &'static [u8],
    context: &'static str,
) -> Result<Option<&'a str>, SchemaError> {
    let field_name = std::str::from_utf8(field).expect("literal field name");
    fields
        .get(field)
        .map(|value| as_utf8_string(value, context, field_name))
        .transpose()
}

fn as_utf8_string<'a>(
    value: &'a YsonValue,
    context: &'static str,
    field: &'static str,
) -> Result<&'a str, SchemaError> {
    reject_attributes(value, context)?;
    let YsonNode::String(bytes) = &value.node else {
        return Err(SchemaError::ExpectedString { context, field });
    };
    std::str::from_utf8(bytes).map_err(|_| SchemaError::InvalidUtf8 { field })
}

fn reject_unknown(
    fields: &BTreeMap<Vec<u8>, YsonValue>,
    allowed: &[&[u8]],
    context: &'static str,
) -> Result<(), SchemaError> {
    let allowed = allowed.iter().copied().collect::<BTreeSet<_>>();
    for field in fields.keys() {
        if !allowed.contains(field.as_slice()) {
            return Err(SchemaError::UnknownField {
                context,
                field: String::from_utf8_lossy(field).into_owned(),
            });
        }
    }
    Ok(())
}
