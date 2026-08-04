//! A deterministic stand-in for the libFuzzer targets in `fuzz/`.
//!
//! `cargo fuzz` needs nightly and a separate install, so it cannot gate every
//! CI run. These tests drive the same entry points (`YsonValue` from arbitrary
//! bytes, in both formats) with a fixed pseudorandom corpus, asserting only the
//! invariant that matters: **the parser never panics, it returns `Err`**.
//!
//! The generator is seeded, so a failure here reproduces exactly. To run the
//! real coverage-guided fuzzer:
//!
//! ```sh
//! cargo install cargo-fuzz
//! cargo +nightly fuzz run fuzz_target_1 -- -max_total_time=60   # binary
//! cargo +nightly fuzz run fuzz_target_2 -- -max_total_time=60   # text
//! ```
//! (run from `crates/ytsaurus-yson/`).

use ytsaurus_yson::{StreamDeserializer, YsonFormat, YsonValue, from_slice};

/// xorshift64*, so the corpus is identical on every platform and every run.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn byte(&mut self) -> u8 {
        (self.next_u64() >> 33) as u8
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() >> 33) as usize % n
    }
}

/// Bytes that carry meaning in YSON, so the corpus lands on real parser paths
/// far more often than uniform noise would.
const INTERESTING: &[u8] = &[
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x7F, 0x80, 0xFF, b'#', b'<', b'>', b'[', b']',
    b'{', b'}', b'=', b';', b'"', b'\\', b'%', b'u', b'n', b't', b'a', b'0', b'9', b'-', b'+',
    b'.', b'e', b' ', b'\n', b'/', b'*',
];

fn corpus(rng: &mut Rng, len: usize) -> Vec<u8> {
    (0..len)
        .map(|_| {
            if rng.below(4) == 0 {
                rng.byte()
            } else {
                INTERESTING[rng.below(INTERESTING.len())]
            }
        })
        .collect()
}

#[test]
fn random_input_never_panics() {
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);

    for _ in 0..20_000 {
        let len = rng.below(64) + 1;
        let data = corpus(&mut rng, len);

        for format in [YsonFormat::Binary, YsonFormat::Text] {
            // Only the absence of a panic is asserted; Err is a fine outcome,
            // and so is Ok when the bytes happen to form a valid document.
            let _: Result<YsonValue, _> = from_slice(&data, format);
        }
    }
}

/// The streaming path has its own separator bookkeeping, so it gets its own run.
#[test]
fn random_input_never_panics_in_the_streaming_parser() {
    let mut rng = Rng(0x0FED_CBA9_8765_4321);

    for _ in 0..10_000 {
        let len = rng.below(128) + 1;
        let data = corpus(&mut rng, len);

        for binary in [true, false] {
            let mut stream = StreamDeserializer::<YsonValue>::new(&data, binary);
            // Bounded so a parser that fails to advance cannot hang the test.
            for _ in 0..64 {
                match stream.next_item() {
                    Ok(Some(_)) => continue,
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
}

/// Truncating a valid document at every possible offset must never panic.
/// This is the realistic failure mode for a job: a stream cut short.
#[test]
fn truncated_valid_documents_never_panic() {
    let sources: [&[u8]; 4] = [
        include_bytes!("fixtures/go_to_rust_binary.bin"),
        include_bytes!("fixtures/rust_to_go_binary.bin"),
        include_bytes!("fixtures/go_to_rust_text.txt"),
        include_bytes!("fixtures/rust_to_go_text.txt"),
    ];

    for (i, source) in sources.iter().enumerate() {
        let format = if i < 2 {
            YsonFormat::Binary
        } else {
            YsonFormat::Text
        };
        for cut in 0..source.len() {
            let _: Result<YsonValue, _> = from_slice(&source[..cut], format);
        }
    }
}

/// Flipping a single byte of a valid document must never panic either.
#[test]
fn single_byte_corruption_never_panics() {
    let source: &[u8] = include_bytes!("fixtures/go_to_rust_binary.bin");
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);

    for _ in 0..5_000 {
        let mut data = source.to_vec();
        let pos = rng.below(data.len());
        data[pos] ^= 1 << rng.below(8);
        let _: Result<YsonValue, _> = from_slice(&data, YsonFormat::Binary);
    }
}
