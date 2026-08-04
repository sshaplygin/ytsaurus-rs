#![no_main]

use libfuzzer_sys::fuzz_target;
use serde::Deserialize;
use ytsaurus_yson::{de::Deserializer, node::YsonValue};

fuzz_target!(|data: &[u8]| {
    let mut de = Deserializer::from_bytes(data, true);

    let _ = YsonValue::deserialize(&mut de);
});
