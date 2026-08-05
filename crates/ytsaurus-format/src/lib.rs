//! The data encoding used at a YTsaurus table or worker boundary.
//!
//! [`DataFormat`] is shared by `ytsaurus-client` operation/table APIs and by
//! `ytsaurus-job` worker APIs.  Keeping that choice in one small crate means a
//! launcher and its worker use the same value rather than two look-alike sets
//! of format options that can drift apart.
//!
//! The enum is non-exhaustive: adding a YTsaurus format is a semver-compatible
//! extension, while callers that need to distinguish formats are prompted to
//! handle future variants.

#![warn(missing_docs)]

use std::collections::BTreeMap;

pub use ytsaurus_skiff::Format as SkiffFormat;
pub use ytsaurus_yson::YsonFormat;

use ytsaurus_yson::{YsonNode, YsonValue};

/// A supported YTsaurus data format.
///
/// This enum selects framing and the wire-format declaration sent to the
/// cluster. The payload representation deliberately remains format-specific:
/// YSON callers use bytes (or the existing serde-based convenience APIs), and
/// Skiff callers use the dynamic schema/value APIs. A common enum must not
/// pretend those two row models are interchangeable.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DataFormat {
    /// Text or binary YSON.
    Yson(YsonFormat),
    /// Schema-described Skiff.
    Skiff(SkiffFormat),
}

impl DataFormat {
    /// The normal format for YTsaurus jobs: binary YSON.
    #[must_use]
    pub const fn binary_yson() -> Self {
        Self::Yson(YsonFormat::Binary)
    }

    /// Human-readable text YSON.
    #[must_use]
    pub const fn text_yson() -> Self {
        Self::Yson(YsonFormat::Text)
    }

    /// Selects either YSON encoding explicitly.
    #[must_use]
    pub const fn yson(format: YsonFormat) -> Self {
        Self::Yson(format)
    }

    /// Selects a validated Skiff format.
    #[must_use]
    pub fn skiff(format: SkiffFormat) -> Self {
        Self::Skiff(format)
    }

    /// Returns the selected YSON encoding, if this is a YSON format.
    #[must_use]
    pub const fn as_yson(&self) -> Option<YsonFormat> {
        match self {
            Self::Yson(format) => Some(*format),
            Self::Skiff(_) => None,
        }
    }

    /// Returns the selected Skiff declaration, if this is Skiff.
    #[must_use]
    pub const fn as_skiff(&self) -> Option<&SkiffFormat> {
        match self {
            Self::Yson(_) => None,
            Self::Skiff(format) => Some(format),
        }
    }

    /// Renders the YSON `input_format` or `output_format` declaration accepted
    /// by YTsaurus commands and operation specs.
    #[must_use]
    pub fn to_yson(&self) -> YsonValue {
        match self {
            Self::Yson(format) => yson_format(*format),
            Self::Skiff(format) => format.to_yson(),
        }
    }
}

fn yson_format(format: YsonFormat) -> YsonValue {
    let spelling = match format {
        YsonFormat::Binary => b"binary".as_slice(),
        YsonFormat::Text => b"text".as_slice(),
    };
    let mut attributes = BTreeMap::new();
    attributes.insert(
        b"format".to_vec(),
        YsonValue {
            attributes: None,
            node: YsonNode::String(spelling.to_vec()),
        },
    );
    YsonValue {
        attributes: Some(attributes),
        node: YsonNode::String(b"yson".to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use ytsaurus_skiff::{Schema, SchemaRef, WireType};

    use super::*;

    #[test]
    fn yson_variants_render_the_cluster_declarations() {
        assert_eq!(
            ytsaurus_yson::to_string(&DataFormat::binary_yson().to_yson(), YsonFormat::Text)
                .unwrap(),
            "<format=binary>yson"
        );
        assert_eq!(
            ytsaurus_yson::to_string(&DataFormat::text_yson().to_yson(), YsonFormat::Text).unwrap(),
            "<format=text>yson"
        );
    }

    #[test]
    fn skiff_variant_delegates_to_the_validated_format() {
        let skiff = SkiffFormat::new(vec![SchemaRef::Inline(Schema::tuple([Schema::named(
            "value",
            WireType::String32,
        )]))])
        .unwrap();
        let format = DataFormat::skiff(skiff.clone());

        assert_eq!(format.as_yson(), None);
        assert_eq!(format.as_skiff(), Some(&skiff));
        assert_eq!(format.to_yson(), skiff.to_yson());
    }
}
