use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use ytsaurus_yson::{attributes::WithAttributes, de::Deserializer, ser::Serializer};

use crate::common::*;

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

prop_compose! {
    fn user_strategy()(name in "[a-zA-Z0-9 ]*", age in any::<u32>()) -> User {
        User { name, age }
    }
}

prop_compose! {
    fn meta_strategy()(active in any::<bool>(), role in "[a-zA-Z0-9 ]*") -> Meta {
        Meta { active, role }
    }
}

fn status_strategy() -> impl Strategy<Value = UserStatus> {
    prop_oneof![
        Just(UserStatus::Pending),
        Just(UserStatus::Active),
        "[a-zA-Z0-9 ]*".prop_map(UserStatus::Banned),
        (any::<u32>(), "[a-zA-Z0-9 ]*")
            .prop_map(|(code, reason)| UserStatus::Custom { code, reason })
    ]
}

prop_compose! {
    fn complex_entity_strategy()(
        id in any::<u64>(),
        user_val in user_strategy(),
        user_meta in meta_strategy(),
        tags in proptest::collection::vec("[a-zA-Z0-9 ]*", 0..5),
        status in proptest::option::of(status_strategy())
    ) -> ComplexEntity {
        ComplexEntity {
            id,
            user: WithAttributes { attributes: user_meta, value: user_val },
            tags,
            status,
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_roundtrip_all_primitives(
        b in any::<bool>(),
        u in any::<u64>(),
        i in any::<i64>(),
        s in "[a-zA-Z0-9_]*",
    ) {
        assert_eq!(b, roundtrip(&b, true));
        assert_eq!(u, roundtrip(&u, true));
        assert_eq!(i, roundtrip(&i, true));
        assert_eq!(s, roundtrip(&s, true));

        assert_eq!(b, roundtrip(&b, false));
        assert_eq!(u, roundtrip(&u, false));
        assert_eq!(i, roundtrip(&i, false));
        assert_eq!(s, roundtrip(&s, false));
    }

    #[test]
    fn prop_roundtrip_f64(v in any::<f64>()) {
        if !v.is_nan() {
            assert_eq!(v, roundtrip(&v, true));
            assert_eq!(v, roundtrip(&v, false));
        }
    }

    #[test]
    fn prop_roundtrip_complex_map(
        v in proptest::collection::hash_map(
            "[a-z]+",
            any::<i32>(),
            0..10
        )
    ) {
        assert_eq!(v, roundtrip(&v, true));
        assert_eq!(v, roundtrip(&v, false));
    }

    #[test]
    fn prop_roundtrip_complex_entity(v in complex_entity_strategy()) {
        assert_eq!(v, roundtrip(&v, true));
        assert_eq!(v, roundtrip(&v, false));
    }
}
