//! Table schemas: what a table promises about its columns.
//!
//! A schematised table is worth the trouble because the cluster then checks
//! every write against it, stores columns in their own type rather than as
//! YSON, and can sort and merge them. An unschematised one accepts anything and
//! finds out later.
//!
//! The wire form is a YSON list of column dicts, carrying attributes:
//!
//! ```text
//! <strict=%true;unique_keys=%false>[{name="key";type="string";required=%true};…]
//! ```
//!
//! Build one by hand with [`TableSchema::new`], or derive it from the struct
//! the rows already have — see [`TableRow`].
//!
//! Reference:
//! <https://ytsaurus.tech/docs/en/user-guide/storage/static-schema>

use ytsaurus_yson::YsonValue;

use crate::yson_build::{boolean, list, map, string, with_attributes};

/// A column's type, in the `type` spelling.
///
/// The primitives a job's row can hold. Composite types — lists, structs,
/// tuples — are out of scope here; a column holding one is described as
/// [`ColumnType::Any`], which is what YTsaurus stores an arbitrary YSON value
/// as.
///
/// These are the **`type`** names. YTsaurus has a second, newer spelling,
/// `type_v3`, and exactly two names differ between them: `boolean` is `bool`
/// there, and `any` is `yson`. Sending a `type_v3` name in a `type` field is
/// refused — `Error parsing ESimpleLogicalValueType value "bool"` — so the two
/// vocabularies must not be mixed. Every other name is the same string in both.
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
    /// Any YSON value, stored as-is.
    ///
    /// Never required: the cluster answers `Column of type "any" cannot be
    /// "required"`.
    Any,

    // The temporal and tagged types. Nothing maps to them automatically — a
    // Rust `i64` is an `int64`, and turning it into an `interval` because it
    // looks like one is how a schema comes to lie about the data. Ask for them
    // by name.
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
    /// A column that holds nothing.
    ///
    /// Reads back as `required=%false` without an `optional` wrapper, unlike
    /// every other type.
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

    /// Whether a column of this type may be declared required.
    ///
    /// Three types may not, and the cluster says so in as many words —
    /// `Column of type "any" cannot be "required"`, `Null type cannot be
    /// required`, and the same for `void`. Each of them already means "there
    /// may be nothing here", so promising a value would contradict the type.
    #[must_use]
    pub fn can_be_required(self) -> bool {
        !matches!(self, ColumnType::Any | ColumnType::Null | ColumnType::Void)
    }

    /// Parses a wire name, for the derive's `#[yt(column_type = "…")]` escape
    /// hatch and for anyone building a schema from configuration.
    ///
    /// Accepts either vocabulary — `bool` and `yson` are understood as the
    /// `type_v3` spellings of `boolean` and `any` — but what comes back is
    /// always the `type` spelling, which is the one this crate sends.
    ///
    /// Not `FromStr`: an unknown type name is not an error worth a type of its
    /// own, and every caller here wants the `Option`.
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
            // "bool" is the type_v3 spelling of the same type.
            "boolean" | "bool" => ColumnType::Boolean,
            "string" => ColumnType::String,
            "utf8" => ColumnType::Utf8,
            // "yson" is the type_v3 spelling of the same type.
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
    /// Largest first.
    ///
    /// **A cluster is likely to refuse this.** The order exists in the
    /// protocol, but creating a table with it was answered with
    /// `Descending sort order is not available in this context yet`; it is
    /// gated behind `//sys/@config/enable_descending_sort_order`, off by
    /// default. It is here because the protocol has it, not because it works.
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

    /// Marks the column as one every row must have.
    ///
    /// A required column is what `i64` means and an optional one is what
    /// `Option<i64>` means: the cluster rejects a row that leaves a required
    /// column out.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Makes this an ascending key column.
    ///
    /// Key columns must be the *first* columns of the schema, in order: the
    /// cluster refuses a schema whose keys are not a prefix with
    /// `Key columns must form a prefix of schema`.
    #[must_use]
    pub fn key(self) -> Self {
        self.sorted(SortOrder::Ascending)
    }

    /// Makes this a key column, sorted the given way.
    ///
    /// See [`SortOrder::Descending`] before reaching for anything but
    /// ascending.
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
    /// A strict schema: the listed columns and nothing else.
    ///
    /// Strict is the default because it is the one that catches mistakes — a
    /// non-strict table quietly accepts a misspelled column name and stores it
    /// as an unschematised extra.
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

    /// Promises that no two rows share a key.
    ///
    /// Only meaningful when the schema has key columns; the cluster enforces it
    /// on write.
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

    /// Checks what the cluster would otherwise reject with error 314.
    ///
    /// Every rule here was watched being enforced by a cluster; catching them
    /// locally turns a round trip and a nested error document into one
    /// sentence naming the column.
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

/// A Rust type that describes a table's rows.
///
/// Implement it by hand, or derive it — the derive reads the struct's fields
/// and their types, which is the same information a schema carries:
///
/// ```ignore
/// use ytsaurus_client::TableRow;
///
/// #[derive(TableRow)]
/// struct Visit<'a> {
///     #[yt(key)]
///     host: &'a str,
///     size: i64,
///     referrer: Option<&'a str>,   // optional, because the Rust type says so
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
