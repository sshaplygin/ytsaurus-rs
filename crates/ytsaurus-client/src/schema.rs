//! Table schemas: what a table promises about its columns, which the cluster
//! then checks on every write. The wire form is a YSON list of column dicts,
//! with attributes on the list:
//! `<strict=%true;unique_keys=%false>[{name="key";type="string";required=%true};…]`.
//! Build one with [`TableSchema::new`], or derive it with [`TableRow`]. See the
//! [reference](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
//! and [Table schemas](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#table-schemas).

use ytsaurus_yson::YsonValue;

use crate::yson_build::{boolean, list, map, string, with_attributes};

/// A column's type, in the `type` spelling. Composite types are out of scope;
/// describe such a column as [`ColumnType::Any`]. The `type_v3` spelling differs
/// in two names, `bool` for `boolean` and `yson` for `any`, and a `type`
/// field refuses them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// 8-bit signed integer.
    Int8,
    /// 16-bit signed integer.
    Int16,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 8-bit unsigned integer.
    Uint8,
    /// 16-bit unsigned integer.
    Uint16,
    /// 32-bit unsigned integer.
    Uint32,
    /// 64-bit unsigned integer.
    Uint64,
    /// Single-precision float.
    Float,
    /// Double-precision float.
    Double,
    /// Boolean.
    Boolean,
    /// A byte string. YTsaurus strings are arbitrary bytes, not text.
    String,
    /// A string the cluster checks is valid UTF-8.
    Utf8,
    /// Any YSON value, stored as-is. Never required.
    Any,

    // Temporal and tagged types: nothing maps to them automatically.
    /// Days since the Unix epoch, unsigned.
    Date,
    /// Seconds since the Unix epoch, unsigned.
    Datetime,
    /// Microseconds since the Unix epoch, unsigned.
    Timestamp,
    /// A signed count of microseconds.
    Interval,
    /// Signed days since the Unix epoch.
    Date32,
    /// Signed seconds since the Unix epoch.
    Datetime64,
    /// Signed microseconds since the Unix epoch.
    Timestamp64,
    /// A signed count of microseconds, over the wider range.
    Interval64,
    /// UTF-8 text the cluster checks is valid JSON.
    Json,
    /// A 16-byte UUID.
    Uuid,
    /// A column that holds nothing; reads back `required=%false` with no `optional` wrapper.
    Void,
    /// The type with no values at all.
    Null,
}

impl ColumnType {
    /// The wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnType::Int8 => "int8",
            ColumnType::Int16 => "int16",
            ColumnType::Int32 => "int32",
            ColumnType::Int64 => "int64",
            ColumnType::Uint8 => "uint8",
            ColumnType::Uint16 => "uint16",
            ColumnType::Uint32 => "uint32",
            ColumnType::Uint64 => "uint64",
            ColumnType::Float => "float",
            ColumnType::Double => "double",
            ColumnType::Boolean => "boolean",
            ColumnType::String => "string",
            ColumnType::Utf8 => "utf8",
            ColumnType::Any => "any",
            ColumnType::Date => "date",
            ColumnType::Datetime => "datetime",
            ColumnType::Timestamp => "timestamp",
            ColumnType::Interval => "interval",
            ColumnType::Date32 => "date32",
            ColumnType::Datetime64 => "datetime64",
            ColumnType::Timestamp64 => "timestamp64",
            ColumnType::Interval64 => "interval64",
            ColumnType::Json => "json",
            ColumnType::Uuid => "uuid",
            ColumnType::Void => "void",
            ColumnType::Null => "null",
        }
    }

    /// Whether a column of this type may be required: all but `any`, `null` and `void`.
    #[must_use]
    pub fn can_be_required(self) -> bool {
        !matches!(self, ColumnType::Any | ColumnType::Null | ColumnType::Void)
    }

    /// Parses a wire name in either spelling (`bool` and `yson` included) into
    /// the `type` spelling, for `#[yt(column_type = "…")]` and configuration.
    /// Not `FromStr`: callers want the `Option`.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "int8" => ColumnType::Int8,
            "int16" => ColumnType::Int16,
            "int32" => ColumnType::Int32,
            "int64" => ColumnType::Int64,
            "uint8" => ColumnType::Uint8,
            "uint16" => ColumnType::Uint16,
            "uint32" => ColumnType::Uint32,
            "uint64" => ColumnType::Uint64,
            "float" => ColumnType::Float,
            "double" => ColumnType::Double,
            "boolean" | "bool" => ColumnType::Boolean,
            "string" => ColumnType::String,
            "utf8" => ColumnType::Utf8,
            "any" | "yson" => ColumnType::Any,
            "date" => ColumnType::Date,
            "datetime" => ColumnType::Datetime,
            "timestamp" => ColumnType::Timestamp,
            "interval" => ColumnType::Interval,
            "date32" => ColumnType::Date32,
            "datetime64" => ColumnType::Datetime64,
            "timestamp64" => ColumnType::Timestamp64,
            "interval64" => ColumnType::Interval64,
            "json" => ColumnType::Json,
            "uuid" => ColumnType::Uuid,
            "void" => ColumnType::Void,
            "null" => ColumnType::Null,
            _ => return None,
        })
    }
}

/// Which way a key column is sorted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Smallest first. The only order a cluster accepts today.
    Ascending,
    /// Largest first. **A cluster is likely to refuse it**: `Descending sort order
    /// is not available in this context yet`, unless
    /// `//sys/@config/enable_descending_sort_order` is on.
    Descending,
}

impl SortOrder {
    /// The wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SortOrder::Ascending => "ascending",
            SortOrder::Descending => "descending",
        }
    }
}

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    name: String,
    column_type: ColumnType,
    required: bool,
    sort_order: Option<SortOrder>,
}

impl Column {
    /// A column that may be missing or `#`.
    #[must_use]
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            required: false,
            sort_order: None,
        }
    }

    /// Marks the column as one every row must have, as `i64` is to
    /// `Option<i64>`; the cluster rejects a row that leaves it out.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Makes this an ascending key column. Key columns must come first, or the
    /// cluster answers `Key columns must form a prefix of schema`.
    #[must_use]
    pub fn key(self) -> Self {
        self.sorted(SortOrder::Ascending)
    }

    /// Makes this a key column sorted the given way; see [`SortOrder::Descending`].
    #[must_use]
    pub fn sorted(mut self, order: SortOrder) -> Self {
        self.sort_order = Some(order);
        self
    }

    /// The column's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The column's type.
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }

    /// Whether every row must carry it.
    #[must_use]
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Its sort order, if it is a key column.
    #[must_use]
    pub fn sort_order(&self) -> Option<SortOrder> {
        self.sort_order
    }

    fn to_yson(&self) -> YsonValue {
        let mut column = map([
            ("name", string(&self.name)),
            ("type", string(self.column_type.as_str())),
            ("required", boolean(self.required)),
        ]);
        if let Some(order) = self.sort_order {
            crate::yson_build::insert(&mut column, "sort_order", string(order.as_str()));
        }
        column
    }
}

/// What a table promises about its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    columns: Vec<Column>,
    strict: bool,
    unique_keys: bool,
}

impl TableSchema {
    /// A strict schema: the listed columns and nothing else, so a misspelled
    /// column is refused rather than stored.
    #[must_use]
    pub fn new(columns: impl IntoIterator<Item = Column>) -> Self {
        Self {
            columns: columns.into_iter().collect(),
            strict: true,
            unique_keys: false,
        }
    }

    /// Allows rows to carry columns the schema does not mention.
    #[must_use]
    pub fn non_strict(mut self) -> Self {
        self.strict = false;
        self
    }

    /// Promises that no two rows share a key. Needs key columns; the cluster
    /// enforces it on write.
    #[must_use]
    pub fn with_unique_keys(mut self, unique: bool) -> Self {
        self.unique_keys = unique;
        self
    }

    /// The columns, in order.
    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Checks locally, naming the column, what a create would refuse with error
    /// 314: column count and names, required types, key prefix and unique keys.
    ///
    /// # Errors
    ///
    /// Returns the reason the schema is invalid.
    pub fn validate(&self) -> std::result::Result<(), String> {
        /// The cluster's own ceiling.
        const MAX_COLUMNS: usize = 32_000;
        /// Longest column name a cluster accepts.
        const MAX_NAME: usize = 256;

        if self.columns.len() > MAX_COLUMNS {
            return Err(format!(
                "a table may have at most {MAX_COLUMNS} columns; this schema has {}",
                self.columns.len()
            ));
        }

        let mut seen = std::collections::BTreeSet::new();
        for column in &self.columns {
            let name = column.name();

            if name.is_empty() {
                return Err("a column name cannot be empty".to_owned());
            }
            if name.len() > MAX_NAME {
                return Err(format!(
                    "column {name:?} is {} bytes long; the limit is {MAX_NAME}",
                    name.len()
                ));
            }
            if name.starts_with('@') {
                return Err(format!(
                    "column {name:?} starts with '@', which YTsaurus reserves for attributes"
                ));
            }
            if !seen.insert(name) {
                return Err(format!("column {name:?} appears twice"));
            }

            if column.is_required() && !column.column_type().can_be_required() {
                return Err(format!(
                    "column {name:?} is of type {}, which cannot be required",
                    column.column_type().as_str()
                ));
            }
        }

        // Key columns must be a prefix: the first non-key column ends the key,
        // and nothing after it may be sorted.
        let keys = self
            .columns
            .iter()
            .take_while(|c| c.sort_order().is_some())
            .count();
        if let Some(stray) = self.columns[keys..]
            .iter()
            .find(|c| c.sort_order().is_some())
        {
            return Err(format!(
                "key columns must be the first columns of the schema, and {:?} is not; \
                 move it before {:?}",
                stray.name(),
                self.columns[keys].name()
            ));
        }

        if self.unique_keys && keys == 0 {
            return Err(
                "unique_keys promises no two rows share a key, but this schema has no key columns"
                    .to_owned(),
            );
        }

        Ok(())
    }

    /// Renders the schema as the cluster expects it.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        with_attributes(
            list(self.columns.iter().map(Column::to_yson)),
            [
                ("strict", boolean(self.strict)),
                ("unique_keys", boolean(self.unique_keys)),
            ],
        )
    }
}

/// A Rust type that describes a table's rows. Implement it, or derive it from
/// the struct's fields:
///
/// ```ignore
/// use ytsaurus_client::TableRow;
/// #[derive(TableRow)]
/// struct Visit<'a> {
///     #[yt(key)]
///     host: &'a str,
///     size: i64,
///     referrer: Option<&'a str>, // optional, because the Rust type says so
/// }
///
/// client.create_table("//tmp/visits", &Visit::table_schema())?;
/// ```
pub trait TableRow {
    /// The schema of a table holding these rows.
    fn table_schema() -> TableSchema;
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::{YsonFormat, to_string};

    fn render(schema: &TableSchema) -> String {
        to_string(&schema.to_yson(), YsonFormat::Text).expect("encodes")
    }

    #[test]
    fn a_schema_renders_as_an_attributed_list_of_columns() {
        let schema = TableSchema::new([
            Column::new("key", ColumnType::String).required(),
            Column::new("count", ColumnType::Int64),
        ]);

        assert_eq!(
            render(&schema),
            r#"<strict=%true;unique_keys=%false>[{name=key;required=%true;type=string};{name=count;required=%false;type=int64}]"#
        );
    }

    #[test]
    fn a_key_column_carries_its_sort_order() {
        let schema = TableSchema::new([Column::new("k", ColumnType::String)
            .required()
            .sorted(SortOrder::Ascending)])
        .with_unique_keys(true);

        let out = render(&schema);
        assert!(out.contains("sort_order=ascending"), "{out}");
        assert!(out.contains("unique_keys=%true"), "{out}");
    }

    #[test]
    fn strictness_is_on_unless_turned_off() {
        assert!(render(&TableSchema::new([])).contains("strict=%true"));
        assert!(
            render(&TableSchema::new([]).non_strict()).contains("strict=%false"),
            "a non-strict table accepts columns the schema never mentioned"
        );
    }

    #[test]
    fn every_type_has_a_wire_name_and_parses_back() {
        for ty in [
            ColumnType::Date,
            ColumnType::Datetime,
            ColumnType::Timestamp,
            ColumnType::Interval,
            ColumnType::Date32,
            ColumnType::Datetime64,
            ColumnType::Timestamp64,
            ColumnType::Interval64,
            ColumnType::Json,
            ColumnType::Uuid,
            ColumnType::Void,
            ColumnType::Null,
            ColumnType::Int8,
            ColumnType::Int16,
            ColumnType::Int32,
            ColumnType::Int64,
            ColumnType::Uint8,
            ColumnType::Uint16,
            ColumnType::Uint32,
            ColumnType::Uint64,
            ColumnType::Float,
            ColumnType::Double,
            ColumnType::Boolean,
            ColumnType::String,
            ColumnType::Utf8,
            ColumnType::Any,
        ] {
            assert_eq!(ColumnType::parse(ty.as_str()), Some(ty), "{ty:?}");
        }

        assert_eq!(ColumnType::parse("bool"), Some(ColumnType::Boolean));
        assert_eq!(ColumnType::parse("int128"), None);
    }
}
