//! Table paths that carry attributes.
//!
//! A YTsaurus path is not only a string: it is a YSON value, and attributes on
//! it change what a command does with it. `<append=%true>//tmp/log` and
//! `//tmp/log` name the same table and mean opposite things — one adds rows,
//! the other replaces them. `<columns=[host];ranges=[{lower_limit={row_index=0};
//! upper_limit={row_index=100}}]>//tmp/log` names a hundred rows of one column
//! of it, which is the difference between a read worth doing over a laptop
//! link and one that is not.
//!
//! This crate sent bare strings until now, so every write replaced the table
//! and append was unreachable. [`TablePath`] is the type that makes the
//! attributes expressible, and `From<&str>` is what keeps `client.write_table
//! ("//tmp/out", …)` reading exactly as it did.
//!
//! The attribute spellings are the
//! [rich YPath reference](https://ytsaurus.tech/docs/en/user-guide/storage/ypath):
//! `columns` is a list of names, `ranges` a list of maps with `lower_limit`,
//! `upper_limit` and `exact`, and inside a limit sit `row_index`, `key` and
//! `key_bound`. The Go SDK's `ypath.Rich` renders the same shapes
//! (`yt/go/ypath/rich.go`: `Ranges []Range \`yson:"ranges,attr"\``,
//! `ReadLimit{Key []any; RowIndex *int64}`).

use std::ops::{Bound, RangeBounds};

use ytsaurus_yson::{YsonFormat, YsonValue};

use crate::yson_build;

/// A table to read from or write to, and which part of it.
///
/// Built from a `&str` wherever a plain path will do:
///
/// ```
/// # use ytsaurus_client::TablePath;
/// let replace = TablePath::from("//tmp/log");
/// let add = TablePath::new("//tmp/log").append();
/// let head = TablePath::new("//tmp/log").columns(["host", "status"]).range(0..100);
/// ```
///
/// Append is a write-side attribute; columns and ranges are read-side ones,
/// the same split the C++ `TRichYPath` and the Go `ypath.Rich` carry. The
/// write methods **refuse** a path with a read selection rather than sending
/// it — the cluster ignores a selection on a write and replaces the whole
/// table with a 200, which is silent data loss. Measured on a local cluster,
/// in both spellings: `write_table_rows("//tmp/t[#0:#2]", rows)` replaced
/// everything and reported success, and a `write_table` whose path carried
/// `<ranges=[{lower_limit={row_index=0};upper_limit={row_index=2}}]>` as an
/// attribute did exactly the same — 200, three rows replaced by one.
///
/// The path *string* is never parsed. Rich YPath syntax spelled into it —
/// `<append=%true>//tmp/t`, `//tmp/t[#0:#2]`, `//tmp/t{a,b}` — goes to the
/// cluster verbatim on a read, where the cluster honours it, and is refused on
/// a write, where the cluster would not: the attribute form of a selection is
/// ignored there, and this type exists so that cannot happen by accident.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TablePath {
    path: String,
    append: bool,
    columns: Option<Vec<String>>,
    ranges: Vec<RowRange>,
}

impl TablePath {
    /// A path that names the whole table: a write **replaces** its contents,
    /// a read returns every row of every column — the defaults everywhere in
    /// YTsaurus.
    #[must_use]
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            append: false,
            columns: None,
            ranges: Vec::new(),
        }
    }

    /// Adds rows to the table instead of replacing it.
    ///
    /// The table has to exist: appending to a path that does not is refused
    /// with `Error getting basic attributes of user objects`, which is the
    /// cluster's way of saying there was nothing to append to.
    ///
    /// **A sorted table stays sorted, and the cluster checks.** Rows appended
    /// after a larger key are refused — `Sort order violation: [0#9] > [0#1]`
    /// — so an append to a sorted table is a continuation of it rather than an
    /// addition to it.
    #[must_use]
    pub fn append(mut self) -> Self {
        self.append = true;
        self
    }

    /// Reads only the named columns.
    ///
    /// Three columns out of a forty-column table cost three columns' worth of
    /// wire and decode, which is what makes a laptop-side read of a wide table
    /// reasonable. The names travel as the `columns` attribute on the path,
    /// which the
    /// [rich YPath reference](https://ytsaurus.tech/docs/en/user-guide/storage/ypath)
    /// says is *"recognized by the table data read command (`read_table`)"* —
    /// and by read commands **only**, which is why the write methods refuse a
    /// path carrying one rather than letting the cluster ignore it.
    ///
    /// **A column the table does not have is not an error** — measured on a
    /// local cluster: `columns(["a", "nosuch"])` against a table with no
    /// `nosuch` answered 200, every row carrying only `a`. Rows simply come
    /// back without the key, exactly as they do for a row with no value in a
    /// named column, so a typo here reads clean and decodes short. A struct
    /// decoded from such a read fails loudly on the missing field, which is
    /// where the typo surfaces; a map decodes to fewer keys and does not.
    ///
    /// Calling this again replaces the selection rather than adding to it.
    #[must_use]
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Reads only the rows a [`RowRange`] selects. May be called several
    /// times: the ranges are read one after another, in the order given —
    /// the documented meaning of the `ranges` attribute.
    ///
    /// Plain Rust ranges convert, so row windows read as they would on a
    /// slice:
    ///
    /// ```
    /// # use ytsaurus_client::{Key, RowRange, TablePath};
    /// let first_two = TablePath::new("//tmp/t").range(0..2);
    /// let from_key = TablePath::new("//tmp/t")
    ///     .range(RowRange::keys(Key::from("alice")..Key::from("bob")));
    /// ```
    #[must_use]
    pub fn range(mut self, range: impl Into<RowRange>) -> Self {
        self.ranges.push(range.into());
        self
    }

    /// Whether [`TablePath::append`] was called on this path.
    ///
    /// Not "whether the cluster will append": the cluster parses attributes out
    /// of the path *string* too, so a path built from the text
    /// `<append=%true>//tmp/t` appends while this answers `false`. Spelling the
    /// attribute into the string is not a supported way to ask for it, and a
    /// *write* to such a string is refused outright — see [`TablePath`].
    #[must_use]
    pub fn is_append(&self) -> bool {
        self.append
    }

    /// The columns [`TablePath::columns`] selected, if any.
    #[must_use]
    pub fn selected_columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }

    /// The ranges [`TablePath::range`] added, in the order they will be read.
    #[must_use]
    pub fn selected_ranges(&self) -> &[RowRange] {
        &self.ranges
    }

    /// The path itself, without the attributes.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.path
    }

    /// The path as the command parameter wants it.
    ///
    /// A bare string when there is nothing to say, because that is what every
    /// version of this crate has sent and there is no reason for the common
    /// case to start looking different on the wire.
    pub(crate) fn to_yson(&self) -> YsonValue {
        let path = yson_build::string(&self.path);
        let mut attributes: Vec<(&str, YsonValue)> = Vec::new();
        if self.append {
            attributes.push(("append", yson_build::boolean(true)));
        }
        if let Some(columns) = &self.columns {
            attributes.push((
                "columns",
                yson_build::list(columns.iter().map(yson_build::string)),
            ));
        }
        if !self.ranges.is_empty() {
            attributes.push((
                "ranges",
                yson_build::list(self.ranges.iter().map(RowRange::to_yson)),
            ));
        }
        if attributes.is_empty() {
            path
        } else {
            yson_build::with_attributes(path, attributes)
        }
    }

    /// Why a write must not send this path, if it must not.
    ///
    /// Two families of refusal, both protecting against the same measured
    /// failure — a selection on a write is **ignored with a 200** and the
    /// whole table is replaced:
    ///
    /// - a typed selection ([`TablePath::columns`] / [`TablePath::range`]),
    ///   which only read commands recognise;
    /// - rich YPath syntax spelled into the path string — a leading `<…>`
    ///   attribute block, or an unescaped `[` / `{` — which this client never
    ///   parses and a write-side cluster silently strips.
    pub(crate) fn write_refusal(&self) -> Option<String> {
        if self.columns.is_some() {
            return Some(format!(
                "{self}: a write cannot select columns — the cluster ignores the \
                 `columns` attribute on a write and writes whole rows, reporting \
                 success; column selection belongs on reads"
            ));
        }
        if !self.ranges.is_empty() {
            return Some(format!(
                "{self}: a write cannot take a row range — the cluster ignores the \
                 `ranges` attribute on a write and replaces the whole table with a \
                 200, which is silent data loss; ranges belong on reads"
            ));
        }
        if self.path.starts_with('<') {
            return Some(format!(
                "{}: attributes spelled into the path string are not parsed by this \
                 client, and a write with them is refused — a range or column \
                 selection in there would be silently ignored by the cluster; use \
                 TablePath::append() to append, or Client::raw_command for any \
                 other write attribute",
                self.path
            ));
        }
        if let Some(selector) = first_unescaped_selector(&self.path) {
            return Some(format!(
                "{}: `{selector}` in the path string is rich YPath selection syntax, \
                 which the cluster silently ignores on a write — \
                 write_table(\"//tmp/t[#0:#2]\", …) replaced the whole table and \
                 answered 200 — so a write takes a bare path; select rows and \
                 columns on reads, with TablePath::range and TablePath::columns \
                 (a literal `[` or `{{` in a node name is escaped as `\\[` / `\\{{`)",
                self.path
            ));
        }
        None
    }

    /// Why a read must not send this path, if it must not.
    ///
    /// A read passes the string through verbatim, selection syntax and all —
    /// the cluster honours it there, and code that read `//tmp/t[#0:#2]`
    /// before this type existed keeps working. The one shape refused is
    /// string-spelled syntax **combined with** a typed selection: that would
    /// put two spellings of a selection on one path, and whichever the
    /// cluster preferred, the caller would silently read the wrong rows.
    pub(crate) fn read_refusal(&self) -> Option<String> {
        if self.columns.is_none() && self.ranges.is_empty() {
            return None;
        }
        if self.path.starts_with('<') || first_unescaped_selector(&self.path).is_some() {
            return Some(format!(
                "{}: the path string already spells a selection (`<…>`, `[…]` or \
                 `{{…}}`), and this client does not parse it; adding \
                 TablePath::columns or TablePath::range on top would send two \
                 spellings of a selection and read something other than what was \
                 asked — put the whole selection in the typed API, on a bare path",
                self.path
            ));
        }
        None
    }
}

/// The first unescaped `[` or `{` in a path string, if any.
///
/// Rich YPath escapes a literal bracket in a node name as `\[` / `\{`
/// (and a literal backslash as `\\`), so an unescaped one is selection
/// syntax, not a name.
fn first_unescaped_selector(path: &str) -> Option<char> {
    let mut bytes = path.bytes();
    while let Some(byte) = bytes.next() {
        match byte {
            b'\\' => {
                bytes.next();
            }
            b'[' => return Some('['),
            b'{' => return Some('{'),
            _ => {}
        }
    }
    None
}

/// One entry of a path's `ranges` attribute: which rows to read.
///
/// Built three ways, one per selector the
/// [rich YPath reference](https://ytsaurus.tech/docs/en/user-guide/storage/ypath)
/// defines:
///
/// - [`RowRange::rows`] — by row index; plain Rust ranges convert via `Into`,
///   so `path.range(0..100)` reads as it would on a slice;
/// - [`RowRange::keys`] — by key, on a sorted table;
/// - [`RowRange::exact_key`] — exactly the rows whose key starts with a tuple.
///
/// A range never mixes `exact` with a lower or upper limit, because no
/// constructor can express that — the reference defines them as alternatives.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowRange {
    lower: Option<Limit>,
    upper: Option<Limit>,
    exact: Option<Limit>,
}

impl RowRange {
    /// Rows by index: `rows(0..100)`, `rows(100..)`, `rows(..)`.
    ///
    /// Rust range semantics and the cluster's are the same — `lower_limit` is
    /// inclusive and `upper_limit` exclusive for a `row_index` (the reference:
    /// *"All limit types except for `key_bound` are inclusive in the
    /// `lower_limit` attribute and exclusive in the `upper_limit`
    /// attribute"*) — so `0..2` means rows 0 and 1 on both sides of the wire,
    /// and `..=2` rows 0 through 2.
    #[must_use]
    pub fn rows(rows: impl RangeBounds<i64>) -> Self {
        // The two saturations at i64's edge are exact, not approximate: a
        // table's row count fits in an i64, so no row has index i64::MAX —
        // an exclusive lower bound *at* i64::MAX excludes every possible row
        // either way, and an inclusive upper bound there includes them all.
        let lower = match rows.start_bound() {
            Bound::Included(&index) => Some(Limit::RowIndex(index)),
            Bound::Excluded(&index) => Some(Limit::RowIndex(index.saturating_add(1))),
            Bound::Unbounded => None,
        };
        let upper = match rows.end_bound() {
            Bound::Included(&index) => index.checked_add(1).map(Limit::RowIndex),
            Bound::Excluded(&index) => Some(Limit::RowIndex(index)),
            Bound::Unbounded => None,
        };
        Self {
            lower,
            upper,
            exact: None,
        }
    }

    /// Rows by key, on a sorted table.
    ///
    /// Takes a Rust range of [`Key`]s, and the inclusivity travels with it:
    ///
    /// - `keys(a..b)` — from `a` inclusive to `b` exclusive. These are the
    ///   spellings the `key` selector has natively (inclusive in
    ///   `lower_limit`, exclusive in `upper_limit`), so they are sent as
    ///   `{key=[…]}`, exactly what the Go SDK's `ypath.Key` sends.
    /// - `keys(a..=b)` — inclusive upper bound. The `key` selector cannot say
    ///   that, so it is sent as the cluster's `key_bound` form,
    ///   `{key_bound=["<="; […]]}`; an exclusive *lower* bound
    ///   (`(Bound::Excluded(a), …)`) likewise becomes `{key_bound=[">"; […]]}`.
    ///   The reference defines `key_bound` as `[relation; prefix]` with `>`
    ///   `>=` allowed only in `lower_limit` and `<` `<=` only in
    ///   `upper_limit`, and this constructor is what makes the wrong pairing
    ///   unwritable.
    ///
    /// A key shorter than the table's key columns is a prefix bound: tuples
    /// are compared component-wise, the shorter being smaller when equal so
    /// far, which the reference spells out and which is what makes
    /// `keys(Key::from("a")..Key::from("b"))` on a two-column key mean "every
    /// row whose first component is in `[a, b)`".
    #[must_use]
    pub fn keys(keys: impl RangeBounds<Key>) -> Self {
        let lower = match keys.start_bound() {
            Bound::Included(key) => Some(Limit::Key(key.clone())),
            Bound::Excluded(key) => Some(Limit::KeyBound {
                relation: ">",
                key: key.clone(),
            }),
            Bound::Unbounded => None,
        };
        let upper = match keys.end_bound() {
            Bound::Included(key) => Some(Limit::KeyBound {
                relation: "<=",
                key: key.clone(),
            }),
            Bound::Excluded(key) => Some(Limit::Key(key.clone())),
            Bound::Unbounded => None,
        };
        Self {
            lower,
            upper,
            exact: None,
        }
    }

    /// Exactly the rows whose full key starts with `key`.
    ///
    /// The `exact` selector of the reference: *"only returns those rows where
    /// the full key contains the `key` tuple as its prefix"*. On a table keyed
    /// by `(host, path)`, `exact_key(Key::from("example.com"))` is every row
    /// of that host — the same rows `keys(k..=k)` would select, said in the
    /// cluster's own word for it.
    #[must_use]
    pub fn exact_key(key: impl Into<Key>) -> Self {
        Self {
            lower: None,
            upper: None,
            exact: Some(Limit::Key(key.into())),
        }
    }

    /// The range as one entry of the `ranges` attribute.
    pub(crate) fn to_yson(&self) -> YsonValue {
        let mut entries: Vec<(&str, YsonValue)> = Vec::new();
        if let Some(exact) = &self.exact {
            entries.push(("exact", exact.to_yson()));
        }
        if let Some(lower) = &self.lower {
            entries.push(("lower_limit", lower.to_yson()));
        }
        if let Some(upper) = &self.upper {
            entries.push(("upper_limit", upper.to_yson()));
        }
        yson_build::map(entries)
    }
}

impl From<std::ops::Range<i64>> for RowRange {
    fn from(rows: std::ops::Range<i64>) -> Self {
        Self::rows(rows)
    }
}

impl From<std::ops::RangeFrom<i64>> for RowRange {
    fn from(rows: std::ops::RangeFrom<i64>) -> Self {
        Self::rows(rows)
    }
}

impl From<std::ops::RangeTo<i64>> for RowRange {
    fn from(rows: std::ops::RangeTo<i64>) -> Self {
        Self::rows(rows)
    }
}

impl From<std::ops::RangeInclusive<i64>> for RowRange {
    fn from(rows: std::ops::RangeInclusive<i64>) -> Self {
        Self::rows(rows)
    }
}

impl From<std::ops::RangeToInclusive<i64>> for RowRange {
    fn from(rows: std::ops::RangeToInclusive<i64>) -> Self {
        Self::rows(rows)
    }
}

impl From<std::ops::RangeFull> for RowRange {
    fn from(rows: std::ops::RangeFull) -> Self {
        Self::rows(rows)
    }
}

/// One limit of a [`RowRange`], in the cluster's own representation.
#[derive(Debug, Clone, PartialEq)]
enum Limit {
    /// `{row_index=N}`.
    RowIndex(i64),
    /// `{key=[…]}` — inclusive as a lower limit, exclusive as an upper one.
    Key(Key),
    /// `{key_bound=[relation; […]]}` — the two inclusivities `key` cannot say.
    KeyBound { relation: &'static str, key: Key },
}

impl Limit {
    fn to_yson(&self) -> YsonValue {
        match self {
            Limit::RowIndex(index) => yson_build::map([("row_index", yson_build::int(*index))]),
            Limit::Key(key) => yson_build::map([("key", key.to_yson())]),
            Limit::KeyBound { relation, key } => yson_build::map([(
                "key_bound",
                yson_build::list([yson_build::string(relation), key.to_yson()]),
            )]),
        }
    }
}

/// A key tuple: the value of one row's key columns, or a prefix of them.
///
/// A key is a **list** of YSON values, compared component-wise — the same
/// `[]any` the Go SDK's `ypath.Key(values …any)` takes. Single-component keys
/// convert from the value itself; a composite or mixed-type key is spelled
/// with [`yson_build`](crate::yson_build):
///
/// ```
/// # use ytsaurus_client::{Key, yson_build};
/// let host = Key::from("example.com");
/// let host_and_code = Key::new([yson_build::string("example.com"), yson_build::int(404)]);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Key(Vec<YsonValue>);

impl Key {
    /// A key from its component values, in key-column order.
    #[must_use]
    pub fn new(parts: impl IntoIterator<Item = YsonValue>) -> Self {
        Self(parts.into_iter().collect())
    }

    fn to_yson(&self) -> YsonValue {
        yson_build::list(self.0.iter().cloned())
    }
}

impl From<&str> for Key {
    fn from(part: &str) -> Self {
        Self(vec![yson_build::string(part)])
    }
}

impl From<String> for Key {
    fn from(part: String) -> Self {
        Self(vec![yson_build::string(part)])
    }
}

impl From<i64> for Key {
    fn from(part: i64) -> Self {
        Self(vec![yson_build::int(part)])
    }
}

impl From<Vec<YsonValue>> for Key {
    fn from(parts: Vec<YsonValue>) -> Self {
        Self(parts)
    }
}

impl From<&str> for TablePath {
    fn from(path: &str) -> Self {
        Self::new(path)
    }
}

impl From<String> for TablePath {
    fn from(path: String) -> Self {
        Self::new(path)
    }
}

impl From<&String> for TablePath {
    fn from(path: &String) -> Self {
        Self::new(path.as_str())
    }
}

impl From<&TablePath> for TablePath {
    fn from(path: &TablePath) -> Self {
        path.clone()
    }
}

// The shapes `&str` used to absorb by deref coercion and `Into` does not. A
// `&&str` is what `for path in &paths` hands you, and a `Cow<str>` is what a
// function that sometimes rewrites a path returns; neither is exotic, and
// leaving them out would break code that compiled before this type existed.
impl From<&&str> for TablePath {
    fn from(path: &&str) -> Self {
        Self::new(*path)
    }
}

impl From<std::borrow::Cow<'_, str>> for TablePath {
    fn from(path: std::borrow::Cow<'_, str>) -> Self {
        Self::new(path.into_owned())
    }
}

impl std::fmt::Display for TablePath {
    /// Prints the path the way the cluster spells it —
    /// `<append=%true;columns=[a]>//tmp/out` — so an error naming the path
    /// says which rows and columns were in play, not only which table.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self.to_yson();
        if let Some(attributes) = &value.attributes {
            f.write_str("<")?;
            for (i, (name, attribute)) in attributes.iter().enumerate() {
                if i > 0 {
                    f.write_str(";")?;
                }
                // Encoding a value this type built cannot fail — every leaf is
                // a string, an int or a boolean — so the fallback is belt and
                // braces rather than a reachable path.
                let rendered = ytsaurus_yson::to_string(attribute, YsonFormat::Text)
                    .unwrap_or_else(|_| "?".to_owned());
                write!(f, "{}={rendered}", String::from_utf8_lossy(name))?;
            }
            f.write_str(">")?;
        }
        f.write_str(&self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::to_string;

    fn rendered(path: &TablePath) -> String {
        to_string(&path.to_yson(), YsonFormat::Text).expect("encodes")
    }

    #[test]
    fn a_plain_path_is_a_plain_string() {
        // What every version of this crate has sent. A path that started
        // carrying `<append=%false>` would be a change in the request for no
        // change in the meaning.
        assert_eq!(rendered(&TablePath::from("//tmp/out")), r#""//tmp/out""#);
    }

    #[test]
    fn an_appending_path_carries_the_attribute() {
        assert_eq!(
            rendered(&TablePath::new("//tmp/out").append()),
            r#"<append=%true>"//tmp/out""#
        );
    }

    #[test]
    fn a_column_selection_is_a_list_on_the_path() {
        // The doc's spelling: `columns` is an attribute on the path holding a
        // list of names. A sibling parameter would be ignored, exactly as a
        // sibling `append` is.
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").columns(["host", "status"])),
            r#"<columns=[host;status]>"//tmp/t""#
        );
    }

    #[test]
    fn naming_columns_again_replaces_the_selection() {
        // Two calls are one decision revised, not a union — a caller looping
        // over candidate selections must get the last one, not the sum.
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").columns(["a"]).columns(["b"])),
            r#"<columns=[b]>"//tmp/t""#
        );
    }

    #[test]
    fn a_row_range_renders_the_documented_limits() {
        // `0..2` is rows 0 and 1 in Rust and on the cluster: lower_limit
        // inclusive, upper_limit exclusive, both spelled row_index.
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(0..2)),
            r#"<ranges=[{lower_limit={row_index=0};upper_limit={row_index=2}}]>"//tmp/t""#
        );
    }

    #[test]
    fn half_open_row_ranges_leave_the_absent_limit_out() {
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(10..)),
            r#"<ranges=[{lower_limit={row_index=10}}]>"//tmp/t""#
        );
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(..5)),
            r#"<ranges=[{upper_limit={row_index=5}}]>"//tmp/t""#
        );
        // `..` is a range with nothing to say, and an empty map is how the
        // ranges list says "everything".
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(..)),
            r#"<ranges=[{}]>"//tmp/t""#
        );
    }

    #[test]
    fn inclusive_row_bounds_become_the_exclusive_wire_form() {
        // `..=2` includes row 2; the wire only has an exclusive upper
        // row_index, so it travels as 3.
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(0..=2)),
            r#"<ranges=[{lower_limit={row_index=0};upper_limit={row_index=2}}]>"#
                .replace("row_index=2}}", "row_index=3}}")
                + r#""//tmp/t""#
        );
        // An exclusive lower bound has no wire form either; it travels as the
        // next index.
        assert_eq!(
            rendered(
                &TablePath::new("//tmp/t")
                    .range(RowRange::rows((Bound::Excluded(4_i64), Bound::Unbounded)))
            ),
            r#"<ranges=[{lower_limit={row_index=5}}]>"//tmp/t""#
        );
    }

    #[test]
    fn an_inclusive_bound_at_the_top_of_i64_means_unbounded() {
        // `..=i64::MAX` has no exclusive spelling one greater, and needs
        // none: no row index exceeds i64::MAX, so the honest translation is
        // "no upper limit" — not a saturated bound that would silently drop
        // the last representable row.
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(0..=i64::MAX)),
            r#"<ranges=[{lower_limit={row_index=0}}]>"//tmp/t""#
        );
    }

    #[test]
    fn key_ranges_use_the_key_selector_where_it_says_the_right_thing() {
        // The `key` selector is inclusive below and exclusive above by
        // definition, which is exactly what a Rust `a..b` means — so those two
        // bounds are sent in the doc's plain form, the one the Go SDK sends.
        assert_eq!(
            rendered(
                &TablePath::new("//tmp/t")
                    .range(RowRange::keys(Key::from("alice")..Key::from("bob")))
            ),
            r#"<ranges=[{lower_limit={key=[alice]};upper_limit={key=[bob]}}]>"//tmp/t""#
        );
    }

    #[test]
    fn the_other_two_inclusivities_use_key_bound() {
        // `key` cannot say "strictly above" or "up to and including"; the
        // documented `key_bound` form — `[relation; prefix]` — can, and the
        // relation the constructor picks is the only one the docs allow on
        // that side (`>` `>=` below, `<` `<=` above).
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(RowRange::keys((
                Bound::Excluded(Key::from("alice")),
                Bound::Included(Key::from("bob"))
            )))),
            r#"<ranges=[{lower_limit={key_bound=[">";[alice]]};upper_limit={key_bound=["<=";[bob]]}}]>"//tmp/t""#
        );
        // `a..=b` is the common way to ask for an inclusive top.
        assert_eq!(
            rendered(
                &TablePath::new("//tmp/t")
                    .range(RowRange::keys(Key::from("alice")..=Key::from("bob")))
            ),
            r#"<ranges=[{lower_limit={key=[alice]};upper_limit={key_bound=["<=";[bob]]}}]>"//tmp/t""#
        );
    }

    #[test]
    fn an_exact_key_is_the_exact_selector() {
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(RowRange::exact_key(Key::from("alice")))),
            r#"<ranges=[{exact={key=[alice]}}]>"//tmp/t""#
        );
    }

    #[test]
    fn a_composite_key_keeps_its_components_in_order() {
        // A key is a tuple compared component-wise; the order given is the
        // order sent, because reordering it would compare different columns.
        //
        // `example.com` renders unquoted — the text writer's identifier rule
        // (`ser::is_safe_unquoted`) allows `.` in the tail. Both spellings are
        // the same YSON string, and this literal is *fixed*, so pinning the
        // rendering is safe where pinning a generated value's would not be.
        let key = Key::new([yson_build::string("example.com"), yson_build::int(404)]);
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(RowRange::exact_key(key))),
            r#"<ranges=[{exact={key=[example.com;404]}}]>"//tmp/t""#
        );
    }

    #[test]
    fn several_ranges_are_read_in_the_order_given() {
        // "The specified ranges will be read sequentially, in the order in
        // which they are specified" — so the Vec must not be sorted or
        // deduplicated on the way out.
        assert_eq!(
            rendered(&TablePath::new("//tmp/t").range(5..6).range(0..1)),
            r#"<ranges=[{lower_limit={row_index=5};upper_limit={row_index=6}};{lower_limit={row_index=0};upper_limit={row_index=1}}]>"//tmp/t""#
        );
    }

    #[test]
    fn everything_a_path_can_say_fits_on_one_path() {
        // Append beside a read selection renders fine — the write methods are
        // what refuse the combination, not the renderer, because a read of an
        // append-marked path is harmless and refusing it here would make
        // TablePath order-sensitive.
        assert_eq!(
            rendered(
                &TablePath::new("//tmp/t")
                    .append()
                    .columns(["a"])
                    .range(0..1)
            ),
            r#"<append=%true;columns=[a];ranges=[{lower_limit={row_index=0};upper_limit={row_index=1}}]>"//tmp/t""#
        );
    }

    #[test]
    fn it_is_built_from_every_shape_of_string_a_call_site_has() {
        // Deref coercion used to absorb all of these when the parameter was a
        // `&str`, and `Into` does not: each one that is missing is a call site
        // that stops compiling when this type arrives. `&&str` is what
        // `for path in &paths` gives you, and `Cow` is what a function that
        // sometimes rewrites a path returns.
        let owned = String::from("//tmp/out");
        let borrowed: &str = "//tmp/out";
        let paths = vec!["//tmp/out"];

        assert_eq!(TablePath::from("//tmp/out").as_str(), "//tmp/out");
        assert_eq!(TablePath::from(owned.clone()).as_str(), "//tmp/out");
        assert_eq!(TablePath::from(&owned).as_str(), "//tmp/out");
        assert_eq!(TablePath::from(&borrowed).as_str(), "//tmp/out");
        assert_eq!(
            TablePath::from(std::borrow::Cow::Borrowed("//tmp/out")).as_str(),
            "//tmp/out"
        );
        for path in &paths {
            assert_eq!(TablePath::from(path).as_str(), "//tmp/out");
        }
    }

    #[test]
    fn it_prints_the_way_the_cluster_spells_it() {
        // So that an error message naming the path says which of the two it
        // was. "wrote to //tmp/out" and "appended to //tmp/out" are different
        // events and the difference is the whole feature.
        assert_eq!(TablePath::from("//tmp/out").to_string(), "//tmp/out");
        assert_eq!(
            TablePath::new("//tmp/out").append().to_string(),
            "<append=%true>//tmp/out"
        );
        // A selection prints too: an error about a partial read should say
        // which part was being read.
        assert_eq!(
            TablePath::new("//tmp/t")
                .columns(["a"])
                .range(0..2)
                .to_string(),
            "<columns=[a];ranges=[{lower_limit={row_index=0};upper_limit={row_index=2}}]>//tmp/t"
        );
    }

    #[test]
    fn append_is_a_property_of_the_path_and_not_of_the_string() {
        let path = TablePath::new("//tmp/out");
        assert!(!path.is_append());
        assert!(path.clone().append().is_append());
        // The original is unchanged: the builder returns a new value, so a path
        // held for reuse cannot be turned into an appending one behind the
        // caller's back.
        assert!(!path.is_append());
    }

    #[test]
    fn a_write_refuses_a_typed_read_selection() {
        // The design driver of this module: on a write the cluster ignores
        // `columns` and `ranges` and replaces the whole table with a 200.
        // Sending them anyway would be that silent loss with nicer syntax.
        let columns = TablePath::new("//tmp/t").columns(["a"]);
        let reason = columns.write_refusal().expect("refused");
        assert!(reason.contains("columns"), "{reason}");

        let ranged = TablePath::new("//tmp/t").range(0..2);
        let reason = ranged.write_refusal().expect("refused");
        assert!(reason.contains("range"), "{reason}");

        assert!(TablePath::new("//tmp/t").write_refusal().is_none());
        assert!(TablePath::new("//tmp/t").append().write_refusal().is_none());
    }

    #[test]
    fn a_write_refuses_selection_syntax_spelled_into_the_string() {
        // The measured trap, verbatim from the local cluster:
        // write_table_rows("//tmp/t[#0:#2]", rows) replaced the whole table
        // and returned success. The string is not parsed; it is refused.
        for path in ["//tmp/t[#0:#2]", "//tmp/t{a,b}", "<append=%true>//tmp/t"] {
            let refusal = TablePath::new(path).write_refusal();
            assert!(refusal.is_some(), "{path} was not refused");
        }
        // An escaped bracket is a node name, not syntax, and stays writable.
        assert!(TablePath::new(r"//tmp/t\[x\]").write_refusal().is_none());
        assert!(TablePath::new(r"//tmp/t\{x\}").write_refusal().is_none());
    }

    #[test]
    fn a_read_takes_the_string_verbatim_unless_a_typed_selection_joins_it() {
        // Reads honoured string-spelled ranges before this type existed, and
        // still do — the cluster reads them correctly there.
        assert!(TablePath::new("//tmp/t[#0:#2]").read_refusal().is_none());
        assert!(
            TablePath::new("//tmp/t")
                .columns(["a"])
                .read_refusal()
                .is_none()
        );
        // Both spellings on one path is the shape with no right answer.
        assert!(
            TablePath::new("//tmp/t[#0:#2]")
                .range(0..2)
                .read_refusal()
                .is_some()
        );
        assert!(
            TablePath::new("<columns=[a]>//tmp/t")
                .columns(["b"])
                .read_refusal()
                .is_some()
        );
    }
}
