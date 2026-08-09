//! Small constructors for the YSON documents the API expects.
//!
//! Command parameters and operation specs are YSON dicts. `YsonValue` can model
//! all of them, but building one by hand is verbose — its map keys are `Vec<u8>`
//! and every leaf needs wrapping. These helpers keep the call sites readable.
//!
//! The client encodes parameters with this project's own codec rather than
//! reaching for JSON, which keeps the dependency list short and exercises
//! `ytsaurus-yson` against the real cluster on every request.

use std::collections::BTreeMap;

use ytsaurus_yson::{YsonNode, YsonValue};

/// A YSON string.
#[must_use]
pub fn string(value: impl AsRef<[u8]>) -> YsonValue {
    YsonValue {
        attributes: None,
        node: YsonNode::String(value.as_ref().to_vec()),
    }
}

/// A YSON int64.
#[must_use]
pub fn int(value: i64) -> YsonValue {
    YsonValue {
        attributes: None,
        node: YsonNode::Int64(value),
    }
}

/// A YSON uint64.
///
/// A different YSON type from [`int`], but on a `uint64` key column the
/// cluster does not hold that against a value it can read: measured,
/// `{exact={key=[42]}}` and `{exact={key=[42u]}}` returned the same row, so
/// [`Key::from`](crate::Key)`(42_i64)` finds it. The difference that bites is
/// *range*. `Key::from(i64)` tops out at `i64::MAX`, so every key above that
/// is one it cannot spell, and such a row is reachable only through this
/// helper — `Key::new([yson_build::uint(u64::MAX)])` returned the row keyed
/// `18446744073709551615u`.
#[must_use]
pub fn uint(value: u64) -> YsonValue {
    YsonValue {
        attributes: None,
        node: YsonNode::Uint64(value),
    }
}

/// A YSON double.
///
/// The type a scheduler weight has: `update_operation_parameters` takes
/// `weight=2.5`, and an int64 in its place is a different YSON value.
#[must_use]
pub fn double(value: f64) -> YsonValue {
    YsonValue {
        attributes: None,
        node: YsonNode::Double(value),
    }
}

/// A YSON boolean.
#[must_use]
pub fn boolean(value: bool) -> YsonValue {
    YsonValue {
        attributes: None,
        node: YsonNode::Boolean(value),
    }
}

/// A YSON list.
#[must_use]
pub fn list(items: impl IntoIterator<Item = YsonValue>) -> YsonValue {
    YsonValue {
        attributes: None,
        node: YsonNode::List(items.into_iter().collect()),
    }
}

/// A YSON dict.
#[must_use]
pub fn map<K: AsRef<[u8]>>(entries: impl IntoIterator<Item = (K, YsonValue)>) -> YsonValue {
    let mut out = BTreeMap::new();
    for (key, value) in entries {
        out.insert(key.as_ref().to_vec(), value);
    }
    YsonValue {
        attributes: None,
        node: YsonNode::Map(out),
    }
}

/// An empty YSON dict — the parameters of a command that takes none.
///
/// `map([])` cannot express this: the key type has nothing to be inferred from,
/// and `map` takes its entries as an `impl Trait` argument, so a turbofish is
/// not allowed either. A command like `get_supported_features` still has to
/// send `{}` in `X-YT-Parameters`, so the shorthand exists.
#[must_use]
pub fn empty_map() -> YsonValue {
    YsonValue {
        attributes: None,
        node: YsonNode::Map(BTreeMap::new()),
    }
}

/// Attaches attributes to a value, as in `<format=binary>yson`.
#[must_use]
pub fn with_attributes<K: AsRef<[u8]>>(
    value: YsonValue,
    attributes: impl IntoIterator<Item = (K, YsonValue)>,
) -> YsonValue {
    let mut attrs = BTreeMap::new();
    for (key, v) in attributes {
        attrs.insert(key.as_ref().to_vec(), v);
    }
    YsonValue {
        attributes: if attrs.is_empty() { None } else { Some(attrs) },
        node: value.node,
    }
}

/// `<format=binary>yson` — the format a `ytsaurus-job` worker expects.
#[must_use]
pub fn binary_yson_format() -> YsonValue {
    with_attributes(string("yson"), [("format", string("binary"))])
}

/// Inserts into a value that is known to be a dict; panics otherwise.
///
/// A `Result` here would be noise at every call site, so the invariant is kept
/// by the callers instead: a builder reading back a value a caller supplied
/// through `with_raw` passes it through `map_or_empty` first, and the raw
/// command doors refuse parameters that are not a dict before anything is
/// stamped onto them. Reach for one of those rather than widening this.
pub(crate) fn insert(target: &mut YsonValue, key: impl AsRef<[u8]>, value: YsonValue) {
    match &mut target.node {
        YsonNode::Map(m) => {
            m.insert(key.as_ref().to_vec(), value);
        }
        other => panic!("expected a dict, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ytsaurus_yson::{YsonFormat, to_string};

    #[test]
    fn no_parameters_still_encodes_as_a_dict() {
        // `X-YT-Parameters` is a YSON dict on every command, including the ones
        // that take nothing. An empty *list* or a missing header would be a
        // different statement to the proxy.
        assert_eq!(
            to_string(&empty_map(), YsonFormat::Text).expect("encodes"),
            "{}"
        );
    }

    #[test]
    fn builds_the_documents_the_api_expects() {
        let spec = map([
            ("input_table_paths", list([string("//tmp/in")])),
            ("output_table_paths", list([string("//tmp/out")])),
            (
                "mapper",
                map([
                    ("command", string("./worker")),
                    ("memory_limit", int(536_870_912)),
                ]),
            ),
        ]);

        let encoded = to_string(&spec, YsonFormat::Text).expect("encodes");
        assert!(
            encoded.contains("input_table_paths=[\"//tmp/in\"]"),
            "{encoded}"
        );
        assert!(encoded.contains("memory_limit=536870912"), "{encoded}");
    }

    #[test]
    fn format_attributes_render_as_yson_expects() {
        let encoded = to_string(&binary_yson_format(), YsonFormat::Text).expect("encodes");
        assert_eq!(encoded, "<format=binary>yson");
    }

    #[test]
    fn booleans_use_the_yson_spelling() {
        let encoded = to_string(&map([("enable", boolean(true))]), YsonFormat::Text).unwrap();
        assert_eq!(encoded, "{enable=%true}");
    }

    #[test]
    fn an_unsigned_integer_is_not_the_same_value_as_a_signed_one() {
        // YSON writes a uint64 with a `u` suffix, and these are two distinct
        // values in this crate's own model.
        let encoded = to_string(&map([("n", uint(42))]), YsonFormat::Text).unwrap();
        assert_eq!(encoded, "{n=42u}");
        assert_ne!(uint(42), int(42));
        // The cluster, though, does not make the caller pick: measured on a
        // `uint64`-keyed table, `{exact={key=[42]}}` and `{exact={key=[42u]}}`
        // both returned the row. What only this helper can do is name a key
        // above `i64::MAX` at all — that half of `u64` is unreachable from
        // `Key::from(i64)`, and the row keyed `18446744073709551615u` came
        // back for exactly this spelling.
        let encoded = to_string(&map([("n", uint(u64::MAX))]), YsonFormat::Text).unwrap();
        assert_eq!(encoded, "{n=18446744073709551615u}");
        assert!(i64::try_from(u64::MAX).is_err());
    }
}
