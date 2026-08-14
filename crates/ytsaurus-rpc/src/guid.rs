//! YTsaurus GUIDs: 16 bytes, four little-endian words, printed backwards.
//!
//! The text form is YTsaurus's own and not RFC 4122's — `yt/go/guid/guid.go`
//! says so out loud ("Unfortunately YT uses non standard text representation").
//! It is `%x-%x-%x-%x` over the four words in *descending* order, with each
//! word printed without leading zeroes.

use std::fmt;

/// A YTsaurus GUID.
///
/// Stored as the 16 wire bytes, so conversion to and from the protobuf halves
/// and the packet header words is a matter of slicing, never of guessing an
/// order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Guid(pub [u8; 16]);

impl Guid {
    /// The all-zero GUID, which several protocol fields use to mean "none".
    pub const NULL: Self = Self([0; 16]);

    /// A fresh random GUID (version 4 layout, as the Go SDK's `guid.New` uses).
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        rand::fill(&mut bytes);
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Self(bytes)
    }

    /// The four little-endian 32-bit words, in wire order.
    ///
    /// This is the order they appear in a bus packet header — see
    /// `yt/go/bus/bus.go`, which writes `Parts()` into bytes 8..24.
    pub fn parts(&self) -> [u32; 4] {
        let mut parts = [0u32; 4];
        for (index, part) in parts.iter_mut().enumerate() {
            let start = index * 4;
            *part = u32::from_le_bytes(self.0[start..start + 4].try_into().unwrap());
        }
        parts
    }

    /// Builds a GUID from the four little-endian words, in wire order.
    pub fn from_parts(parts: [u32; 4]) -> Self {
        let mut bytes = [0u8; 16];
        for (index, part) in parts.iter().enumerate() {
            let start = index * 4;
            bytes[start..start + 4].copy_from_slice(&part.to_le_bytes());
        }
        Self(bytes)
    }

    /// The two little-endian 64-bit halves, which is how `TGuid` carries a GUID
    /// through protobuf (`first` = bytes 0..8, `second` = bytes 8..16).
    pub fn halves(&self) -> (u64, u64) {
        (
            u64::from_le_bytes(self.0[0..8].try_into().unwrap()),
            u64::from_le_bytes(self.0[8..16].try_into().unwrap()),
        )
    }

    /// Builds a GUID from the two `TGuid` halves.
    pub fn from_halves(first: u64, second: u64) -> Self {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&first.to_le_bytes());
        bytes[8..16].copy_from_slice(&second.to_le_bytes());
        Self(bytes)
    }

    /// The protobuf form. Both halves are `required` in `guid.proto`, so they
    /// are plain values rather than options.
    pub fn to_proto(self) -> crate::proto::misc::TGuid {
        let (first, second) = self.halves();
        crate::proto::misc::TGuid { first, second }
    }

    /// Reads the protobuf form.
    pub fn from_proto(proto: &crate::proto::misc::TGuid) -> Self {
        Self::from_halves(proto.first, proto.second)
    }

    /// True for the all-zero GUID.
    pub fn is_null(&self) -> bool {
        self.0 == [0; 16]
    }
}

impl fmt::Display for Guid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [a, b, c, d] = self.parts();
        write!(formatter, "{d:x}-{c:x}-{b:x}-{a:x}")
    }
}

impl std::str::FromStr for Guid {
    type Err = ParseGuidError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut words = [0u32; 4];
        let mut seen = 0;
        for (index, field) in text.split('-').enumerate() {
            if index >= 4 {
                return Err(ParseGuidError);
            }
            if field.is_empty() || field.len() > 8 {
                return Err(ParseGuidError);
            }
            // Printed most-significant word first, so fill backwards.
            words[3 - index] = u32::from_str_radix(field, 16).map_err(|_| ParseGuidError)?;
            seen += 1;
        }
        if seen != 4 {
            return Err(ParseGuidError);
        }
        Ok(Self::from_parts(words))
    }
}

/// A GUID that is not four hexadecimal words separated by dashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseGuidError;

impl fmt::Display for ParseGuidError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expected a YTsaurus GUID of the form a-b-c-d, in hexadecimal")
    }
}

impl std::error::Error for ParseGuidError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parts_are_little_endian_words_in_wire_order() {
        let guid = Guid::from_parts([1, 0, 0, 0]);
        // The handshake packet id, and the one place the exact bytes are
        // pinned by the protocol: `yt/go/bus/bus.go` sends guid.FromParts(1,0,0,0).
        assert_eq!(
            guid.0,
            [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "word 0 must land in bytes 0..4, little-endian"
        );
        assert_eq!(guid.parts(), [1, 0, 0, 0]);
    }

    #[test]
    fn halves_match_the_protobuf_split() {
        let guid = Guid::from_parts([0x11111111, 0x22222222, 0x33333333, 0x44444444]);
        let (first, second) = guid.halves();
        assert_eq!(first, 0x2222222211111111);
        assert_eq!(second, 0x4444444433333333);
        assert_eq!(Guid::from_halves(first, second), guid);
    }

    #[test]
    fn text_form_prints_the_words_backwards() {
        // Matches Go's `fmt.Sprintf("%x-%x-%x-%x", d, c, b, a)`.
        let guid = Guid::from_parts([0xa, 0xb, 0xc, 0xd]);
        assert_eq!(guid.to_string(), "d-c-b-a");
    }

    #[test]
    fn text_form_round_trips() {
        for guid in [
            Guid::NULL,
            Guid::from_parts([1, 0, 0, 0]),
            Guid::from_parts([0xdeadbeef, 0x12345678, 1, 0xffffffff]),
            Guid::random(),
        ] {
            let text = guid.to_string();
            assert_eq!(
                text.parse::<Guid>().unwrap(),
                guid,
                "{text} did not round-trip"
            );
        }
    }

    #[test]
    fn malformed_text_is_rejected() {
        for bad in [
            "",
            "1-2-3",
            "1-2-3-4-5",
            "1-2-3-zz",
            "-1-2-3",
            "111111111-2-3-4",
        ] {
            assert!(bad.parse::<Guid>().is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn proto_round_trips() {
        let guid = Guid::random();
        assert_eq!(Guid::from_proto(&guid.to_proto()), guid);
    }

    #[test]
    fn random_guids_differ() {
        assert_ne!(Guid::random(), Guid::random());
        assert!(!Guid::random().is_null());
        assert!(Guid::NULL.is_null());
    }
}
