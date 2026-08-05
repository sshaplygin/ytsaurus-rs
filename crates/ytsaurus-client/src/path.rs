//! Table paths that carry attributes.
//!
//! A YTsaurus path is not only a string: it is a YSON value, and attributes on
//! it change what a command does with it. `<append=%true>//tmp/log` and
//! `//tmp/log` name the same table and mean opposite things — one adds rows,
//! the other replaces them.
//!
//! This crate sent bare strings until now, so every write replaced the table
//! and append was unreachable. [`TablePath`] is the type that makes the
//! attribute expressible, and `From<&str>` is what keeps `client.write_table
//! ("//tmp/out", …)` reading exactly as it did.

use ytsaurus_yson::YsonValue;

use crate::yson_build;

/// A table to write to, and how.
///
/// Built from a `&str` wherever a plain path will do:
///
/// ```
/// # use ytsaurus_client::TablePath;
/// let replace = TablePath::from("//tmp/log");
/// let add = TablePath::new("//tmp/log").append();
/// ```
///
/// The Go SDK's `ypath.Rich` carries column filters and row ranges here too.
/// Those are read-side concerns and are not modelled yet; this type exists so
/// that adding them does not mean adding a second write method for each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePath {
    path: String,
    append: bool,
}

impl TablePath {
    /// A path that **replaces** the table's contents, which is the default
    /// everywhere in YTsaurus.
    #[must_use]
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            append: false,
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

    /// Whether [`TablePath::append`] was called on this path.
    ///
    /// Not "whether the cluster will append": the cluster parses attributes out
    /// of the path *string* too, so a path built from the text
    /// `<append=%true>//tmp/t` appends while this answers `false`. Spelling the
    /// attribute into the string is not a supported way to ask for it.
    #[must_use]
    pub fn is_append(&self) -> bool {
        self.append
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
        if self.append {
            yson_build::with_attributes(path, [("append", yson_build::boolean(true))])
        } else {
            path
        }
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.append {
            write!(f, "<append=%true>{}", self.path)
        } else {
            f.write_str(&self.path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::{YsonFormat, to_string};

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
}
