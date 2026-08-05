use serde::{Deserialize, Serialize};
use ytsaurus_yson::{attributes::WithAttributes, de::Deserializer, ser::Serializer};
mod common;

fn roundtrip<T>(value: &T, is_binary: bool) -> T
where
    T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
{
    let mut serializer = Serializer::new(is_binary);
    value
        .serialize(&mut serializer)
        .expect("Serialization failed");

    let mut deserializer = Deserializer::from_bytes(&serializer.output, is_binary);
    T::deserialize(&mut deserializer).expect("Deserialization failed")
}

#[cfg(test)]
mod unit_tests {
    use ytsaurus_yson::{YsonFormat, from_slice};

    use crate::common::*;

    use super::*;

    #[test]
    fn test_serialize_with_attributes_text() {
        let data = WithAttributes {
            attributes: Meta {
                active: true,
                role: "admin".to_string(),
            },
            value: User {
                name: "Alice".to_string(),
                age: 30,
            },
        };

        let mut serializer = Serializer::new(false);
        data.serialize(&mut serializer).unwrap();
        let result = String::from_utf8(serializer.output).unwrap();

        assert!(result.starts_with('<'));
        assert!(result.contains("active=%true"));
        assert!(result.contains("role=admin"));
        assert!(result.contains('>'));
        assert!(result.contains("name=Alice"));
        assert!(result.contains("age=30u"));
    }

    #[test]
    fn test_deserialize_with_attributes_text() {
        let input = b"<\"active\"=%true; \"role\"=\"admin\">{\"name\"=\"Alice\"; \"age\"=30u}";

        let mut deserializer = Deserializer::from_bytes(input, false);
        let result: WithAttributes<User, Meta> =
            WithAttributes::deserialize(&mut deserializer).expect("Failed to deserialize");

        assert!(result.attributes.active);
        assert_eq!(result.attributes.role, "admin");
        assert_eq!(result.value.name, "Alice");
        assert_eq!(result.value.age, 30);
    }

    #[test]
    fn test_deserialize_fallback_without_attributes() {
        let input = b"{name=Bob; age=25u}";

        let mut deserializer = Deserializer::from_bytes(input, false);
        let result: WithAttributes<User, Option<Meta>> =
            WithAttributes::deserialize(&mut deserializer).unwrap();

        assert!(result.attributes.is_none());
        assert_eq!(result.value.name, "Bob");
        assert_eq!(result.value.age, 25);
    }

    #[test]
    fn test_serialize_deserialize_binary_mode() {
        let data = WithAttributes {
            attributes: Meta {
                active: true,
                role: "superuser".to_string(),
            },
            value: User {
                name: "Dave".to_string(),
                age: 40,
            },
        };

        let result = roundtrip(&data, true);
        assert_eq!(data, result);
    }

    #[test]
    fn test_nested_with_attributes() {
        let nested = WithAttributes {
            attributes: Meta {
                active: true,
                role: "outer".to_string(),
            },
            value: WithAttributes {
                attributes: Meta {
                    active: false,
                    role: "inner".to_string(),
                },
                value: 42i64,
            },
        };

        assert_eq!(nested, roundtrip(&nested, false));
        assert_eq!(nested, roundtrip(&nested, true));
    }

    #[test]
    fn test_attribute_skipping() {
        let yson = b"<system_attr=123>42";

        let val: i64 = from_slice(yson, YsonFormat::Text)
            .expect("Parser should skip attributes for primitives");
        assert_eq!(val, 42);
    }

    /// An attributed *map* reaches `YsonValue`'s visitor flattened: `@`-keys
    /// carry the attributes and the body's entries arrive as plain keys at the
    /// same level. Those plain keys used to be discarded, so `<a=b>{x=10}`
    /// decoded to an attributed entity — the whole body silently lost.
    #[test]
    fn an_attributed_map_keeps_its_body() {
        use ytsaurus_yson::{YsonNode, YsonValue};

        let val: YsonValue =
            from_slice(b"<a=b; c=d> {x=10}", YsonFormat::Text).expect("attributed map");
        assert!(val.attributes.is_some());
        assert_eq!(
            val["x"].as_i64(),
            Some(10),
            "the body must survive the attributes: {val:?}"
        );

        // The literal spelling of the flat form means the same thing.
        let val: YsonValue =
            from_slice(b"{\"@x\"=1; other=2}", YsonFormat::Text).expect("flat form");
        assert_eq!(val.attr("x").and_then(|a| a.as_i64()), Some(1));
        assert_eq!(val["other"].as_i64(), Some(2));

        // A non-map body still travels as "$value".
        let scalar: YsonValue =
            from_slice(b"{\"@x\"=1; \"$value\"=2}", YsonFormat::Text).expect("scalar body");
        assert_eq!(scalar.as_i64(), Some(2));

        // "$value" beside a plain key would be two bodies for one value.
        let err = from_slice::<YsonValue>(b"{\"$value\"=1; other=2}", YsonFormat::Text)
            .expect_err("two bodies must not decode");
        assert!(err.to_string().contains("other"), "{err}");

        // And a round trip through the serializer loses nothing.
        let source: YsonValue =
            from_slice(b"<a=b> {x=10}", YsonFormat::Text).expect("attributed map");
        let encoded = ytsaurus_yson::to_vec(&source, YsonFormat::Text).expect("encodes");
        let back: YsonValue = from_slice(&encoded, YsonFormat::Text).expect("decodes back");
        assert!(back.attr("a").is_some());
        assert!(matches!(back.node, YsonNode::Map(_)), "{back:?}");
    }

    /// The serializer used to emit structural garbage for three struct shapes
    /// instead of either valid YSON or an error: an `@`-field declared after a
    /// plain field produced `{a=1<x=2>}`, an empty struct produced zero bytes,
    /// and an all-attribute struct produced `<x=1>` with no value node. The
    /// first is now an error (YSON attributes stand strictly before their
    /// value); the other two produce `{}` and `<…>#`.
    #[test]
    fn struct_shapes_serialize_to_valid_yson_or_error() {
        use ytsaurus_yson::{YsonFormat, YsonValue, to_vec};

        #[derive(Serialize)]
        struct AttrAfterValue {
            a: i64,
            #[serde(rename = "@x")]
            x: i64,
        }
        let err = to_vec(&AttrAfterValue { a: 1, x: 2 }, YsonFormat::Text)
            .expect_err("an attribute after a value field cannot serialize");
        assert!(err.to_string().contains("@x"), "{err}");

        #[derive(Serialize)]
        struct ValueBesidePlain {
            a: i64,
            #[serde(rename = "$value")]
            v: i64,
        }
        let err = to_vec(&ValueBesidePlain { a: 1, v: 2 }, YsonFormat::Text)
            .expect_err("$value beside a plain field cannot serialize");
        assert!(err.to_string().contains("$value"), "{err}");

        #[derive(Serialize)]
        struct Empty {}
        let bytes = to_vec(&Empty {}, YsonFormat::Text).expect("an empty struct serializes");
        assert_eq!(bytes, b"{}");
        let _: YsonValue = from_slice(&bytes, YsonFormat::Text).expect("and parses back");

        #[derive(Serialize)]
        struct OnlyAttributes {
            #[serde(rename = "@x")]
            x: i64,
        }
        let bytes =
            to_vec(&OnlyAttributes { x: 1 }, YsonFormat::Text).expect("attributes serialize");
        assert_eq!(bytes, b"<x=1>#");
        let back: YsonValue = from_slice(&bytes, YsonFormat::Text).expect("and parse back");
        assert_eq!(back.attr("x").and_then(|a| a.as_i64()), Some(1));
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    #[serde(untagged)]
    enum Untagged {
        Number(i64),
        Text(String),
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    #[serde(tag = "type", content = "payload")]
    enum AdjacentlyTagged {
        Message { text: String },
    }

    #[test]
    fn test_advanced_enums() {
        let num = Untagged::Number(42);
        assert_eq!(num, roundtrip(&num, false));

        let msg = AdjacentlyTagged::Message {
            text: "Hello".into(),
        };
        assert_eq!(msg, roundtrip(&msg, false));
    }
}
