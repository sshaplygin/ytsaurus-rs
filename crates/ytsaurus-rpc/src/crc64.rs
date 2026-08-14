//! YTsaurus's CRC64, the checksum every bus packet header carries.
//!
//! This is **not** one of the named CRC-64 variants. YTsaurus computes it in
//! `yt/yt/core/misc/checksum.cpp`, which dispatches to ISA-L; the Go SDK
//! reimplements the same function in `yt/go/crc64/crc64.go` by embedding a
//! 256-entry table and byte-swapping the result, so that Go's LSB-first
//! `hash/crc64.Update` can be reused.
//!
//! Rather than copy 256 magic constants out of the Go source, the table here is
//! derived at compile time from the single polynomial that generates it. That
//! polynomial was recovered from the Go table and checked two ways: the
//! generated table is identical to Go's entry for entry (`table_matches_the_go_sdk`
//! pins four spread-out entries), and the checksums match all twelve canonical
//! vectors in `yt/go/crc64/crc64_test.go` (`canonical_vectors`).
//!
//! The shape is a normal (MSB-first) CRC-64 over `POLYNOMIAL`, with a zero
//! initial value and no final xor, whose register is byte-swapped on the way
//! out. Storing the table byte-swapped lets the hot loop stay LSB-first.

/// The generator polynomial in normal (MSB-first) form.
///
/// Recovered from `yt/go/crc64/crc64.go`: for an MSB-first table, entry 1 is
/// the polynomial itself, and that entry byte-swapped is `0xe543279765927881`.
const POLYNOMIAL: u64 = 0xe543_2797_6592_7881;

/// MSB-first table, each entry byte-swapped so the update loop can be LSB-first.
const TABLE: [u64; 256] = build_table();

const fn build_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut index = 0usize;
    while index < 256 {
        let mut register = (index as u64) << 56;
        let mut bit = 0;
        while bit < 8 {
            register = if register & (1 << 63) != 0 {
                (register << 1) ^ POLYNOMIAL
            } else {
                register << 1
            };
            bit += 1;
        }
        table[index] = register.swap_bytes();
        index += 1;
    }
    table
}

/// The checksum of a single contiguous buffer.
pub fn checksum(bytes: &[u8]) -> u64 {
    Crc64::new().chain(bytes).finish()
}

/// An incremental checksum, for the parts of a message that are not contiguous.
#[derive(Debug, Clone, Copy, Default)]
pub struct Crc64 {
    register: u64,
}

impl Crc64 {
    pub const fn new() -> Self {
        Self { register: 0 }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        let mut register = self.register;
        for &byte in bytes {
            register = TABLE[((register ^ byte as u64) & 0xff) as usize] ^ (register >> 8);
        }
        self.register = register;
    }

    #[must_use]
    pub fn chain(mut self, bytes: &[u8]) -> Self {
        self.update(bytes);
        self
    }

    /// The checksum as it is written on the wire.
    pub const fn finish(&self) -> u64 {
        self.register.swap_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every vector from `yt/go/crc64/crc64_test.go`, verbatim. Captured from
    /// the reference implementation rather than computed here, which is the
    /// whole point of them.
    const CANONICAL: &[(u64, &str)] = &[
        (0x0000000000000000, ""),
        (0x74b42565ce6232d5, "a"),
        (0x5f02be5e81cf7b1c, "ab"),
        (0xaadaac6d7d340c20, "abc"),
        (0xd35b54234f7f70a0, "abcd"),
        (0xe729d85f050fa861, "abcde"),
        (0x4852bb31b666ae4f, "abcdef"),
        (0xab31ee2e0fe39abb, "abcdefg"),
        (0x3dc543531acca62b, "abcdefgh"),
        (0x43c501e26fc35778, "abcdefghi"),
        (0x4cc4843d59c1373e, "abcdefghij"),
        (
            0x481ac76eee0d3ebd,
            "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977",
        ),
    ];

    #[test]
    fn canonical_vectors() {
        for &(expected, input) in CANONICAL {
            assert_eq!(
                checksum(input.as_bytes()),
                expected,
                "checksum of {input:?} disagrees with the Go SDK"
            );
        }
    }

    /// The derived table must be Go's table. Four entries spread across it are
    /// enough to catch a wrong polynomial or a lost byte swap, and they are the
    /// only numbers in this file copied from the reference.
    #[test]
    fn table_matches_the_go_sdk() {
        assert_eq!(TABLE[0], 0x0000000000000000);
        assert_eq!(TABLE[1], 0x81789265972743e5);
        assert_eq!(TABLE[128], 0xceb75cdca3c3c984);
        assert_eq!(TABLE[255], 0x0b0d194bb09f4fa4);
    }

    /// Feeding the same bytes in several pieces must not change the answer —
    /// the bus checksums a message part by part.
    #[test]
    fn incremental_matches_one_shot() {
        let data: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        for split in [0, 1, 7, 8, 9, 255, 256, 999, 1000] {
            let (head, tail) = data.split_at(split);
            assert_eq!(
                Crc64::new().chain(head).chain(tail).finish(),
                checksum(&data),
                "split at {split} disagrees with the one-shot checksum"
            );
        }
    }

    #[test]
    fn empty_input_is_zero() {
        assert_eq!(checksum(b""), 0);
        assert_eq!(Crc64::new().finish(), 0);
    }
}
