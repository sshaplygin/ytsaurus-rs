#[cfg(test)]
mod coverage_tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};
    use ytsaurus_yson::{
        StreamDeserializer, WithAttributes, YsonError, YsonFormat, YsonNode, YsonValue, from_slice,
        to_string, to_vec,
    };

    #[test]
    fn test_malformed_varint() {
        let data = vec![
            0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ];
        let res: Result<i64, _> = from_slice(&data, YsonFormat::Binary);
        assert!(res.is_err());
    }

    #[test]
    fn test_invalid_utf8() {
        let data = b"\"\xFF\"";
        let res: Result<String, _> = from_slice(data, YsonFormat::Text);
        assert!(res.is_err());
    }

    #[test]
    fn test_with_attributes() {
        let data = b"<a=b; c=d> {x=10}";
        let val: YsonValue = from_slice(data, YsonFormat::Text).unwrap();
        assert!(val.attributes.is_some());
    }

    #[test]
    fn test_lexer_comments() {
        let comment = "10 // This is comment \n 20";
        let val: i64 = from_slice(comment.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(val, 10);

        let data2 = "10 /* Multiline \n comment */ 20";
        let val2: i64 = from_slice(data2.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(val2, 10);
    }

    #[test]
    fn test_serialize_edge_cases() {
        let _ = to_string(&'a', YsonFormat::Text).unwrap();
        let s = to_string(&Option::<i32>::None, YsonFormat::Text).unwrap();
        assert_eq!(s, "#");
        let _ = to_string(&serde_bytes::Bytes::new(b"\x00\x01"), YsonFormat::Text).unwrap();
    }

    #[test]
    fn test_recursion_limit() {
        let mut deeply_nested = String::new();
        for _ in 0..150 {
            deeply_nested.push('[');
        }
        for _ in 0..150 {
            deeply_nested.push(']');
        }

        let res: Result<YsonValue, _> = from_slice(deeply_nested.as_bytes(), YsonFormat::Text);
        assert!(matches!(res, Err(YsonError::Custom(_))));
    }

    #[test]
    fn test_special_floats_and_errors() {
        let nan: f64 = from_slice(b"%nan", YsonFormat::Text).unwrap();
        assert!(nan.is_nan());
        let inf: f64 = from_slice(b"%inf", YsonFormat::Text).unwrap();
        assert!(inf.is_infinite() && inf.is_sign_positive());
        let neg_inf: f64 = from_slice(b"%-inf", YsonFormat::Text).unwrap();
        assert!(neg_inf.is_infinite() && neg_inf.is_sign_negative());
        let invalid: Result<f64, _> = from_slice(b"%invalid", YsonFormat::Text);
        assert!(invalid.is_err());
    }

    #[test]
    fn test_string_escapes_and_bytes() {
        let s: String = from_slice(b"\"line1\\nline2\\t\\x41\\102\"", YsonFormat::Text).unwrap();
        assert_eq!(s, "line1\nline2\tAB");

        let bytes = serde_bytes::Bytes::new(b"\x00\x01\n\r\t\"\\");
        let ser_text = to_string(&bytes, YsonFormat::Text).unwrap();
        assert!(ser_text.contains("\\x00"));
        assert!(ser_text.contains("\\n"));
    }

    #[test]
    fn test_lexer_eof_and_invalid_markers() {
        assert!(from_slice::<String>(b"\"unclosed string", YsonFormat::Text).is_err());
        assert!(from_slice::<Vec<i32>>(b"[1; 2;", YsonFormat::Text).is_err());
        assert!(from_slice::<i32>(&[0x99, 0x00], YsonFormat::Binary).is_err());
    }

    #[test]
    fn test_varint_overflow() {
        let bad_varint = [0xFF; 12];
        let res: Result<u64, _> = from_slice(&bad_varint, YsonFormat::Binary);
        assert!(res.is_err());
    }

    #[test]
    fn test_yson_value_api() {
        let input = b"{\"@attr\"=10; \"@name\"=\"foo\"; \"$value\"=42}";
        let val: YsonValue = from_slice(input, YsonFormat::Text).unwrap();

        assert_eq!(val.as_i64(), Some(42));
        assert_eq!(val["@attr"].as_i64(), Some(10));
        assert_eq!(val.attr("name").unwrap().as_str(), Some("foo"));

        let res = std::panic::catch_unwind(|| {
            let _ = val["nonexistent"];
        });
        assert!(res.is_err());
    }

    #[test]
    fn test_with_attributes_deref() {
        let mut obj = WithAttributes {
            attributes: 1,
            value: 100,
        };
        assert_eq!(*obj, 100);
        *obj = 200;
        assert_eq!(obj.value, 200);
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct FlatTest {
        #[serde(rename = "@id")]
        id: String,
        #[serde(rename = "$value")]
        value: i32,
    }

    #[test]
    fn test_flat_struct() {
        let flat = FlatTest {
            id: "a1".into(),
            value: 99,
        };

        let s = to_string(&flat, YsonFormat::Text).unwrap();
        assert!(s.contains("id=a1"));

        let input = b"<id=\"a1\"> 99";
        let res: FlatTest = from_slice(input, YsonFormat::Text).unwrap();
        assert_eq!(flat, res);
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum ComplexEnum {
        Unit,
        NewType(i32),
        Struct { x: i32 },
    }

    #[test]
    fn test_enum_serialization_variants() {
        let t = (
            1,
            "two",
            ComplexEnum::NewType(3),
            ComplexEnum::Struct { x: 5 },
        );

        let ser = to_string(&t, YsonFormat::Text).unwrap();
        let de: (i32, String, ComplexEnum, ComplexEnum) =
            from_slice(ser.as_bytes(), YsonFormat::Text).unwrap();

        assert_eq!(t.2, de.2);
        assert_eq!(t.3, de.3);
    }

    #[test]
    fn test_binary_to_string_error() {
        let res = to_string(&42, YsonFormat::Binary);
        assert!(res.is_err());

        use serde::ser::Error;
        let err = YsonError::custom("custom_ser_error");
        assert_eq!(err, YsonError::Custom("custom_ser_error".into()));
    }

    #[test]
    fn test_stream_deserializer() {
        let input = b"1; 2; 3";
        let mut stream = StreamDeserializer::<i32>::new(input, false);

        assert_eq!(stream.next_item().unwrap(), Some(1));
        assert_eq!(stream.next_item().unwrap(), Some(2));
        assert_eq!(stream.next_item().unwrap(), Some(3));
        assert_eq!(stream.next_item().unwrap(), None);
    }

    #[test]
    fn test_complex_string_escapes() {
        let input = b"\"octal:\\142; hex:\\x41; slash:\\\\; quote:\\\"; returns:\\r\"";
        let s: String = from_slice(input, YsonFormat::Text).unwrap();
        assert_eq!(s, "octal:b; hex:A; slash:\\; quote:\"; returns:\r");
    }

    #[test]
    fn test_lexer_invalid_values() {
        assert!(from_slice::<YsonValue>(b" = 1", YsonFormat::Text).is_err());
        assert!(from_slice::<bool>(b"%maybe", YsonFormat::Text).is_err());
        assert!(from_slice::<String>(b"\"\\x4\"", YsonFormat::Text).is_err());
    }

    #[test]
    fn test_comments_full_coverage() {
        let input = "/* multiline \n comment */ {key=//single line\nvalue}";
        let val: BTreeMap<String, String> = from_slice(input.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(val.get("key").unwrap(), "value");
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum TupleEnum {
        Point(i32, i32),
    }

    #[test]
    fn test_tuple_and_variants() {
        let pair = (10, 20);
        let ser_pair = to_string(&pair, YsonFormat::Text).unwrap();
        assert_eq!(ser_pair, "[10;20]");
        let de_pair: (i32, i32) = from_slice(ser_pair.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(pair, de_pair);

        let p = TupleEnum::Point(1, 2);
        let ser_p = to_vec(&p, YsonFormat::Text).unwrap();

        assert!(!ser_p.is_empty());

        let s = String::from_utf8_lossy(&ser_p);
        assert!(s.contains("Point") && s.contains("[1;2]"));

        let de_p: TupleEnum = from_slice(&ser_p, YsonFormat::Text).unwrap();
        assert_eq!(p, de_p);
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct AttrBody {
        #[serde(rename = "@id")]
        id: i32,
        #[serde(rename = "$value")]
        body: Option<String>,
    }

    #[test]
    fn test_flat_struct_states() {
        let input = b"<id=1> #";
        let res: AttrBody = from_slice(input, YsonFormat::Text).unwrap();
        assert_eq!(res.id, 1);
        assert!(res.body.is_none());

        let input_map = b"<id=2> {unused=42}";
        let res_map: AttrBody = from_slice(input_map, YsonFormat::Text).unwrap();
        assert_eq!(res_map.id, 2);
    }

    #[test]
    fn test_trailing_semicolons() {
        let list: Vec<i32> = from_slice(b"[1; 2; 3; ]", YsonFormat::Text).unwrap();
        assert_eq!(list, vec![1, 2, 3]);

        let map: BTreeMap<String, i32> = from_slice(b"{a=1; b=2; }", YsonFormat::Text).unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_yson_value_edge_cases() {
        let val = YsonValue {
            attributes: None,
            node: ytsaurus_yson::YsonNode::Entity,
        };
        assert_eq!(val.as_str(), None);
        assert_eq!(val.as_i64(), None);

        let res = std::panic::catch_unwind(|| {
            let _ = val["any"];
        });
        assert!(res.is_err());
    }

    #[test]
    fn test_serialize_bytes_binary() {
        let data = serde_bytes::ByteBuf::from(vec![1, 2, 3]);
        let bin = to_vec(&data, YsonFormat::Binary).unwrap();
        assert_eq!(bin[0], 0x01);
        let decoded: serde_bytes::ByteBuf = from_slice(&bin, YsonFormat::Binary).unwrap();
        assert_eq!(decoded.into_vec(), vec![1, 2, 3]);
    }

    #[test]
    fn test_serialize_special_floats_text() {
        let n = f64::NAN;
        let s = to_string(&n, YsonFormat::Text).unwrap();
        assert_eq!(s, "%nan");

        let inf = f64::INFINITY;
        assert_eq!(to_string(&inf, YsonFormat::Text).unwrap(), "%inf");

        let ninf = f64::NEG_INFINITY;
        assert_eq!(to_string(&ninf, YsonFormat::Text).unwrap(), "%-inf");
    }

    #[test]
    fn test_complex_string_escapes_and_bytes() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct StringEscapes {
            s: String,
            #[serde(with = "serde_bytes")]
            b: Vec<u8>,
        }

        let val = StringEscapes {
            s: "\"\\ \n \r \t \x01".to_string(),
            b: vec![0, 255, b'A'],
        };

        let res = to_string(&val, YsonFormat::Text).unwrap();
        assert!(res.contains("\\\""));
        assert!(res.contains("\\x01"));

        let de: StringEscapes = from_slice(res.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(val, de);

        let octal_yson = "{s=\"\\101\\102\"; b=\"\\x41\"}";
        let de_octal: StringEscapes = from_slice(octal_yson.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(de_octal.s, "AB");
        assert_eq!(de_octal.b, b"A");
    }

    #[test]
    fn test_flat_struct_advanced_states() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct FlatEntity {
            #[serde(rename = "@attr")]
            attr: i32,
            #[serde(rename = "$value")]
            value: Option<i32>,
        }
        let de1: FlatEntity = from_slice(b"<attr=1>#", YsonFormat::Text).unwrap();
        assert_eq!(de1.attr, 1);
        assert_eq!(de1.value, None);

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct FlatBody {
            #[serde(rename = "@attr")]
            attr: i32,
            field: i32,
        }
        let yson2 = "<attr=1>{field=42;}";
        let de2: FlatBody = from_slice(yson2.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(de2.attr, 1);
        assert_eq!(de2.field, 42);
    }

    #[test]
    fn test_enum_error_paths_and_edge_cases() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum TestEnum {
            Unit,
            Newtype(i32),
            Tuple(i32, i32),
            Struct { a: i32 },
        }

        let yson_enum = "{Newtype=1;}";
        let de: TestEnum = from_slice(yson_enum.as_bytes(), YsonFormat::Text).unwrap();
        assert!(matches!(de, TestEnum::Newtype(1)));

        let bad_yson = "{Unit #}";
        let res: Result<TestEnum, _> = from_slice(bad_yson.as_bytes(), YsonFormat::Text);
        assert!(res.is_err());

        let unit_map = "{Unit=#}";
        let de_unit: TestEnum = from_slice(unit_map.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(de_unit, TestEnum::Unit);
    }

    #[test]
    fn test_yson_value_visitor_full() {
        let val_entity: YsonValue = from_slice(b"#", YsonFormat::Text).unwrap();
        assert_eq!(val_entity.node, YsonNode::Entity);

        let yson_nested = "<a=1><b=2>content";
        let val: YsonValue = from_slice(yson_nested.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(val.attr("a").unwrap().as_i64(), Some(1));
        assert_eq!(val.attr("b").unwrap().as_i64(), Some(2));
        assert_eq!(val.as_str(), Some("content"));

        assert_eq!(val["@a"].as_i64(), Some(1));
    }

    #[test]
    fn test_lexer_comments_and_errors() {
        let yson = "/* comment1 \n comment2 */ {key=/* nested */1}";
        let de: BTreeMap<String, i32> = from_slice(yson.as_bytes(), YsonFormat::Text).unwrap();
        assert_eq!(de["key"], 1);

        let bad_yson = "< =1>#";
        let res: Result<YsonValue, _> = from_slice(bad_yson.as_bytes(), YsonFormat::Text);
        assert!(res.is_err());
    }

    #[test]
    fn test_stream_deserializer_semicolon_eof() {
        let data = "1;2;";
        let mut iter = StreamDeserializer::<i32>::new(data.as_bytes(), false);
        assert_eq!(iter.next_item().unwrap(), Some(1));
        assert_eq!(iter.next_item().unwrap(), Some(2));
        assert_eq!(iter.next_item().unwrap(), None);
    }

    #[test]
    fn test_serialize_bytes_text_non_ascii() {
        let bytes = vec![0, 1, 2, 128, 255];
        let mut ser = ytsaurus_yson::ser::Serializer::new(false);
        serde::Serializer::serialize_bytes(&mut ser, &bytes).unwrap();
        let res = String::from_utf8(ser.output).unwrap();
        assert!(res.contains("\\x00"));
        assert!(res.contains("\\xff"));
    }

    #[test]
    fn test_empty_map_access_trigger() {
        let yson = "content";
        let de: Result<WithAttributes<String, BTreeMap<String, String>>, _> =
            from_slice(yson.as_bytes(), YsonFormat::Text);
        assert!(de.is_ok());
        assert!(de.unwrap().attributes.is_empty());
    }

    #[test]
    fn test_wa_expecting_coverage() {
        use serde::de::IntoDeserializer;
        let deserializer = 42.into_deserializer();
        let res: Result<WithAttributes<String, i32>, serde::de::value::Error> =
            Deserialize::deserialize(deserializer);

        assert!(res.is_err());
        assert!(
            res.err()
                .unwrap()
                .to_string()
                .contains("YSON node with optional attributes")
        );
    }

    #[test]
    fn test_lexer_invalid_marker_text_error() {
        let yson = "!";
        let res: Result<YsonValue, _> = from_slice(yson.as_bytes(), YsonFormat::Text);

        assert!(matches!(res, Err(YsonError::InvalidMarker(b'!', 0))));
    }

    #[test]
    fn test_binary_string_negative_length_error() {
        let data = vec![0x01, 0x01];
        let res: Result<String, _> = from_slice(&data, YsonFormat::Binary);

        assert!(res.is_err());
        if let Err(YsonError::Custom(msg)) = res {
            assert_eq!(msg, "String length cannot be negative");
        }
    }
}
