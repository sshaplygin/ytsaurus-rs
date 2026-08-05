//! Shared helpers for the workspace's runnable worker examples.

use ytsaurus_skiff::{Format, Schema, SchemaRef, WireType};

/// The one-column dynamic Skiff table format used by `skiff_cat`.
///
/// `value` is deliberately `string32`, so the offline and cluster examples
/// prove that arbitrary (not merely UTF-8) bytes survive the worker path.
#[must_use]
pub fn skiff_passthrough_format() -> Format {
    Format::new(vec![SchemaRef::Inline(Schema::tuple([Schema::named(
        "value",
        WireType::String32,
    )]))])
    .expect("the fixed skiff_cat table schema is valid")
}
