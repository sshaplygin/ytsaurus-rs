use serde::{
    Deserialize, Serialize,
    de::{self, SeqAccess, Visitor},
};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

/// Wrapper that pairs a value of type `T` with its associated YSON attributes of type `A`
///
/// Any value can have optional map of attributes
///
/// # Examples
///
/// ```
/// use ytsaurus_yson::{WithAttributes, from_slice, YsonFormat};
/// use std::collections::BTreeMap;
///
/// // YSON: <author="Alice">"Hello"
/// let input = b"<author=\"Alice\">\"Hello\"";
///
/// // Define a value where attributes are a BTreeMap and the content is a String
/// type MyNode = WithAttributes<String, BTreeMap<String, String>>;
///
/// let node: MyNode = from_slice(input, YsonFormat::Text).unwrap();
///
/// // Access attributes
/// assert_eq!(node.attributes.get("author").unwrap(), "Alice");
///
/// // Access inner value directly via Deref or .value
/// assert_eq!(node.value, "Hello");
/// assert_eq!(*node, "Hello");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WithAttributes<T, A> {
    /// The attributes associated with the value.
    pub attributes: A,
    /// Data content of the YSON node.
    pub value: T,
}

impl<T: Serialize, A: Serialize> Serialize for WithAttributes<T, A> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("$__yson_attributes", 2)?;
        state.serialize_field("$attributes", &self.attributes)?;
        state.serialize_field("$value", &self.value)?;
        state.end()
    }
}

impl<'de, T, A> Deserialize<'de> for WithAttributes<T, A>
where
    T: Deserialize<'de>,
    A: Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct WAVisitor<T, A>(PhantomData<(T, A)>);

        impl<'de, T, A> Visitor<'de> for WAVisitor<T, A>
        where
            T: Deserialize<'de>,
            A: Deserialize<'de>,
        {
            type Value = WithAttributes<T, A>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("YSON node with optional attributes")
            }

            fn visit_seq<V: SeqAccess<'de>>(self, mut seq: V) -> Result<Self::Value, V::Error> {
                let attributes = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::custom("Missing attributes element"))?;

                let value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::custom("Missing value element"))?;

                Ok(WithAttributes { attributes, value })
            }
        }

        deserializer.deserialize_struct(
            "$__yson_attributes",
            &["$attributes", "$value"],
            WAVisitor(PhantomData),
        )
    }
}

impl<V, A> Deref for WithAttributes<V, A> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<V, A> DerefMut for WithAttributes<V, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
