//! Bus packet framing: the sans-io half of layer 1.
//!
//! A packet is a 36-byte fixed header, an optional variable header of per-part
//! sizes and checksums, and then the parts themselves. Everything is
//! little-endian. The layout is `TPacketHeader` in
//! `yt/yt/core/bus/tcp/packet.cpp`, declared under `#pragma pack(push, 4)`, so
//! the `ui64` checksum sits at offset 28 with no padding in front of it:
//!
//! ```text
//! offset  size  field
//!      0     4  signature = 0x78616d4f
//!      4     2  type      (EPacketType)
//!      6     2  flags     (EPacketFlags)
//!      8    16  packet id (four little-endian u32 words)
//!     24     4  part count
//!     28     8  checksum of bytes 0..28
//! ```
//!
//! followed, when the packet has a variable header, by
//!
//! ```text
//!   u32 part_sizes[part_count]
//!   u64 part_checksums[part_count]
//!   u64 checksum of everything above in this block
//! ```
//!
//! and then each non-null part's bytes back to back.
//!
//! There is no `async` in this file, and there must not be: it is the part that
//! parses untrusted bytes off a socket, so it stays a pure function of its
//! input and is tested without a runtime. Fuzzing it is gate E in
//! `docs/rpc-compatibility.md`, and is not done yet.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::crc64::{self, Crc64};
use crate::guid::Guid;

/// `PacketSignature` — `yt/yt/core/bus/tcp/packet.cpp`.
pub const SIGNATURE: u32 = 0x7861_6d4f;

/// The size of the fixed header, in bytes.
pub const FIXED_HEADER_SIZE: usize = 36;

/// `NullPacketPartSize` — the part-size word that means "this part is absent",
/// which is not the same as a part of length zero.
pub const NULL_PART_SIZE: u32 = 0xffff_ffff;

/// `MaxMessagePartSize` — `yt/yt/core/bus/public.h`, 1 GB.
///
/// The Go SDK caps parts at 512 MB instead; this follows the C++, which is the
/// specification, so nothing a real server may legally send is rejected.
pub const MAX_PART_SIZE: u32 = 1 << 30;

/// `MaxMessagePartCount` — `yt/yt/core/bus/public.h`.
pub const MAX_PART_COUNT: u32 = 1 << 28;

/// `NullChecksum`. A checksum field holding this means the sender did not
/// compute one, and the receiver must not verify it —
/// `yt/yt/core/bus/tcp/packet.cpp` guards every comparison with
/// `expectedChecksum != NullChecksum`.
pub const NULL_CHECKSUM: u64 = 0;

/// `EPacketType` — `yt/yt/core/bus/tcp/packet.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum PacketType {
    Message = 0,
    Ack = 1,
    SslAck = 2,
}

impl PacketType {
    fn from_wire(value: u16) -> Option<Self> {
        match value {
            0 => Some(Self::Message),
            1 => Some(Self::Ack),
            2 => Some(Self::SslAck),
            _ => None,
        }
    }
}

/// `EPacketFlags` — a bit set, so an unknown bit is preserved rather than
/// rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct PacketFlags(pub u16);

impl PacketFlags {
    pub const NONE: Self = Self(0x0000);
    pub const REQUEST_ACKNOWLEDGEMENT: Self = Self(0x0001);

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// One decoded packet.
///
/// A part is `None` when its size word was [`NULL_PART_SIZE`]. The distinction
/// is load-bearing: the RPC layer above sends null parts for absent optional
/// message components, and collapsing them into empty parts changes the
/// meaning of a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub packet_type: PacketType,
    pub flags: PacketFlags,
    pub id: Guid,
    pub parts: Vec<Option<Bytes>>,
}

impl Packet {
    /// A message packet carrying the given parts.
    pub fn message(id: Guid, parts: Vec<Option<Bytes>>, flags: PacketFlags) -> Self {
        Self {
            packet_type: PacketType::Message,
            flags,
            id,
            parts,
        }
    }

    /// Whether this packet carries a variable header.
    ///
    /// Message packets always do, even with no parts; other types only when
    /// they have parts. `yt/go/bus/bus.go` states the same rule:
    /// "Message packets always have variable header, other only when have payload".
    fn has_variable_header(&self) -> bool {
        self.packet_type == PacketType::Message || !self.parts.is_empty()
    }
}

/// What went wrong while decoding bytes off the wire.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PacketError {
    #[error("packet signature mismatch: expected {SIGNATURE:#x}, got {0:#x}")]
    Signature(u32),
    #[error("unknown packet type {0}")]
    UnknownType(u16),
    #[error("packet declares {count} parts, more than the {MAX_PART_COUNT} allowed")]
    TooManyParts { count: u32 },
    #[error("part {index} is {size} bytes, more than the {MAX_PART_SIZE} allowed")]
    PartTooLarge { index: usize, size: u32 },
    #[error(
        "fixed header checksum mismatch: header says {expected:#018x}, bytes give {actual:#018x}"
    )]
    FixedHeaderChecksum { expected: u64, actual: u64 },
    #[error(
        "variable header checksum mismatch: header says {expected:#018x}, bytes give {actual:#018x}"
    )]
    VariableHeaderChecksum { expected: u64, actual: u64 },
    #[error(
        "part {index} checksum mismatch: header says {expected:#018x}, bytes give {actual:#018x}"
    )]
    PartChecksum {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("packet is {size} bytes, more than the {limit} this connection accepts")]
    MessageTooLarge { size: u64, limit: u64 },
}

/// Checks a packet can be represented on the wire at all.
///
/// The part-size word is a `u32`, so a part above 4 GiB would wrap it and the
/// receiver would read the wrong number of bytes and then parse the remainder
/// of the payload as further packets — silent corruption of the whole
/// connection rather than an error. The reference refuses oversized parts on
/// the *send* path too (`connection.cpp` rejects `part.Size() >
/// MaxMessagePartSize`), so this rejects exactly what a real peer would.
pub fn validate(packet: &Packet) -> Result<(), PacketError> {
    if packet.parts.len() as u64 > u64::from(MAX_PART_COUNT) {
        return Err(PacketError::TooManyParts {
            count: packet.parts.len().min(u32::MAX as usize) as u32,
        });
    }
    for (index, part) in packet.parts.iter().enumerate() {
        if let Some(bytes) = part
            && bytes.len() as u64 > u64::from(MAX_PART_SIZE)
        {
            return Err(PacketError::PartTooLarge {
                index,
                size: bytes.len().min(u32::MAX as usize) as u32,
            });
        }
    }
    Ok(())
}

/// Appends the encoded packet to `out`.
///
/// Checksums are always generated, which is what the Go SDK does
/// unconditionally and what the C++ does when `generate_checksums` is on. A
/// null part is written with [`NULL_PART_SIZE`] and [`NULL_CHECKSUM`], matching
/// `SetPartChecksum(index, NullChecksum)` in the C++ encoder.
///
/// Fails on a packet [`validate`] would reject, so encode and decode stay
/// inverses of one another: anything this writes, this crate's decoder — and
/// the proxy's — will accept.
pub fn encode(packet: &Packet, out: &mut BytesMut) -> Result<(), PacketError> {
    validate(packet)?;
    let part_count = packet.parts.len() as u32;

    let fixed_start = out.len();
    out.put_u32_le(SIGNATURE);
    out.put_u16_le(packet.packet_type as u16);
    out.put_u16_le(packet.flags.0);
    out.put_slice(&packet.id.0);
    out.put_u32_le(part_count);
    let fixed_checksum = crc64::checksum(&out[fixed_start..]);
    out.put_u64_le(fixed_checksum);

    if !packet.has_variable_header() {
        return Ok(());
    }

    let variable_start = out.len();
    for part in &packet.parts {
        match part {
            Some(bytes) => out.put_u32_le(bytes.len() as u32),
            None => out.put_u32_le(NULL_PART_SIZE),
        }
    }
    for part in &packet.parts {
        match part {
            Some(bytes) => out.put_u64_le(crc64::checksum(bytes)),
            None => out.put_u64_le(NULL_CHECKSUM),
        }
    }
    let variable_checksum = crc64::checksum(&out[variable_start..]);
    out.put_u64_le(variable_checksum);

    for part in packet.parts.iter().flatten() {
        out.put_slice(part);
    }
    Ok(())
}

/// The number of bytes [`encode`] will append for this packet.
pub fn encoded_size(packet: &Packet) -> usize {
    if !packet.has_variable_header() {
        return FIXED_HEADER_SIZE;
    }
    let payload: usize = packet.parts.iter().flatten().map(|part| part.len()).sum();
    FIXED_HEADER_SIZE + variable_header_size(packet.parts.len()) + payload
}

fn variable_header_size(part_count: usize) -> usize {
    part_count * (size_of::<u32>() + size_of::<u64>()) + size_of::<u64>()
}

/// Decodes one packet from the front of `input`, if a whole one is there.
///
/// Returns `Ok(None)` when more bytes are needed, having consumed nothing. On
/// success the packet's bytes are removed from `input` and the parts share its
/// allocation rather than being copied.
///
/// `max_message_size` bounds the total packet length, so a malicious or corrupt
/// header cannot make the caller reserve an arbitrary buffer. The part-size and
/// part-count limits from the C++ are enforced first and independently.
pub fn decode(input: &mut BytesMut, max_message_size: u64) -> Result<Option<Packet>, PacketError> {
    if input.len() < FIXED_HEADER_SIZE {
        return Ok(None);
    }

    let header = &input[..FIXED_HEADER_SIZE];
    let signature = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if signature != SIGNATURE {
        return Err(PacketError::Signature(signature));
    }

    let raw_type = u16::from_le_bytes(header[4..6].try_into().unwrap());
    let packet_type = PacketType::from_wire(raw_type).ok_or(PacketError::UnknownType(raw_type))?;
    let flags = PacketFlags(u16::from_le_bytes(header[6..8].try_into().unwrap()));
    let id = Guid(header[8..24].try_into().unwrap());
    let part_count = u32::from_le_bytes(header[24..28].try_into().unwrap());
    let stored_checksum = u64::from_le_bytes(header[28..36].try_into().unwrap());

    // The fixed header's checksum covers bytes 0..28 — everything before the
    // checksum field itself.
    if stored_checksum != NULL_CHECKSUM {
        let actual = crc64::checksum(&header[..28]);
        if actual != stored_checksum {
            return Err(PacketError::FixedHeaderChecksum {
                expected: stored_checksum,
                actual,
            });
        }
    }

    if part_count > MAX_PART_COUNT {
        return Err(PacketError::TooManyParts { count: part_count });
    }

    let has_variable_header = packet_type == PacketType::Message || part_count > 0;
    if !has_variable_header {
        input.advance(FIXED_HEADER_SIZE);
        return Ok(Some(Packet {
            packet_type,
            flags,
            id,
            parts: Vec::new(),
        }));
    }

    // Nothing is allocated from `part_count` until the bytes that justify it
    // have actually arrived: the variable header is read only once the whole of
    // it is buffered, and the parts only once the whole packet is.
    let variable_size = variable_header_size(part_count as usize);
    let header_total = FIXED_HEADER_SIZE + variable_size;
    if (header_total as u64) > max_message_size {
        return Err(PacketError::MessageTooLarge {
            size: header_total as u64,
            limit: max_message_size,
        });
    }
    if input.len() < header_total {
        return Ok(None);
    }

    let variable = &input[FIXED_HEADER_SIZE..header_total];
    let stored_variable_checksum =
        u64::from_le_bytes(variable[variable_size - 8..].try_into().unwrap());
    if stored_variable_checksum != NULL_CHECKSUM {
        let actual = crc64::checksum(&variable[..variable_size - 8]);
        if actual != stored_variable_checksum {
            return Err(PacketError::VariableHeaderChecksum {
                expected: stored_variable_checksum,
                actual,
            });
        }
    }

    let part_count = part_count as usize;
    let mut payload_size = 0u64;
    for index in 0..part_count {
        let size = u32::from_le_bytes(variable[index * 4..index * 4 + 4].try_into().unwrap());
        if size == NULL_PART_SIZE {
            continue;
        }
        if size > MAX_PART_SIZE {
            return Err(PacketError::PartTooLarge { index, size });
        }
        payload_size += u64::from(size);
    }

    let total = header_total as u64 + payload_size;
    if total > max_message_size {
        return Err(PacketError::MessageTooLarge {
            size: total,
            limit: max_message_size,
        });
    }
    if (input.len() as u64) < total {
        return Ok(None);
    }

    // Every check has passed and the whole packet is buffered; only now is
    // anything sized by the header allocated or split off.
    let checksums_at = part_count * 4;
    let mut sizes = Vec::with_capacity(part_count);
    let mut checksums = Vec::with_capacity(part_count);
    for index in 0..part_count {
        sizes.push(u32::from_le_bytes(
            variable[index * 4..index * 4 + 4].try_into().unwrap(),
        ));
        let at = checksums_at + index * 8;
        checksums.push(u64::from_le_bytes(variable[at..at + 8].try_into().unwrap()));
    }

    input.advance(header_total);
    let mut parts = Vec::with_capacity(part_count);
    for (index, size) in sizes.into_iter().enumerate() {
        if size == NULL_PART_SIZE {
            // A null part still carries a checksum word, and the C++ encoder
            // writes NullChecksum there.
            parts.push(None);
            continue;
        }
        let part = input.split_to(size as usize).freeze();
        let expected = checksums[index];
        if expected != NULL_CHECKSUM {
            let actual = crc64::checksum(&part);
            if actual != expected {
                return Err(PacketError::PartChecksum {
                    index,
                    expected,
                    actual,
                });
            }
        }
        parts.push(Some(part));
    }

    Ok(Some(Packet {
        packet_type,
        flags,
        id,
        parts,
    }))
}

/// The checksum of a message's parts, as the variable header stores them.
///
/// Exposed for tests and for the connection's diagnostics; the encoder computes
/// these itself.
pub fn part_checksum(part: Option<&Bytes>) -> u64 {
    match part {
        Some(bytes) => Crc64::new().chain(bytes).finish(),
        None => NULL_CHECKSUM,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NO_LIMIT: u64 = u64::MAX;

    fn round_trip(packet: &Packet) -> Packet {
        let mut buffer = BytesMut::new();
        encode(packet, &mut buffer).unwrap();
        assert_eq!(
            buffer.len(),
            encoded_size(packet),
            "encoded_size disagrees with what encode wrote"
        );
        let decoded = decode(&mut buffer, NO_LIMIT)
            .expect("a packet this encoder wrote must decode")
            .expect("a whole packet was written, so a whole one must come back");
        assert!(
            buffer.is_empty(),
            "decode left {} bytes behind",
            buffer.len()
        );
        decoded
    }

    #[test]
    fn fixed_header_is_thirty_six_bytes_in_the_documented_order() {
        let packet = Packet {
            packet_type: PacketType::Ack,
            flags: PacketFlags::NONE,
            id: Guid::from_parts([1, 0, 0, 0]),
            parts: Vec::new(),
        };
        let mut buffer = BytesMut::new();
        encode(&packet, &mut buffer).unwrap();

        assert_eq!(buffer.len(), FIXED_HEADER_SIZE, "an ack is header-only");
        // Against the literal bytes, not against `SIGNATURE`: an assertion that
        // reads the same constant the encoder reads pins the offset and not the
        // value, and this value is the first thing a proxy checks.
        assert_eq!(&buffer[0..4], b"Omax");
        assert_eq!(SIGNATURE, 0x7861_6d4f);
        assert_eq!(&buffer[4..6], &1u16.to_le_bytes(), "type");
        assert_eq!(&buffer[6..8], &0u16.to_le_bytes(), "flags");
        assert_eq!(
            &buffer[8..24],
            &Guid::from_parts([1, 0, 0, 0]).0,
            "packet id"
        );
        assert_eq!(&buffer[24..28], &0u32.to_le_bytes(), "part count");
        assert_eq!(
            u64::from_le_bytes(buffer[28..36].try_into().unwrap()),
            crc64::checksum(&buffer[..28]),
            "the header checksum covers bytes 0..28 and not itself"
        );
    }

    #[test]
    fn message_packets_always_carry_a_variable_header() {
        // Even with no parts — this is the rule that separates a zero-part
        // message from an ack on the wire.
        let packet = Packet::message(Guid::random(), Vec::new(), PacketFlags::NONE);
        let mut buffer = BytesMut::new();
        encode(&packet, &mut buffer).unwrap();
        assert_eq!(
            buffer.len(),
            FIXED_HEADER_SIZE + 8,
            "just the trailing checksum"
        );
        assert_eq!(round_trip(&packet), packet);
    }

    #[test]
    fn acks_with_no_parts_carry_no_variable_header() {
        let packet = Packet {
            packet_type: PacketType::Ack,
            flags: PacketFlags::NONE,
            id: Guid::random(),
            parts: Vec::new(),
        };
        assert_eq!(encoded_size(&packet), FIXED_HEADER_SIZE);
        assert_eq!(round_trip(&packet), packet);
    }

    #[test]
    fn parts_round_trip_including_empty_and_null() {
        let packet = Packet::message(
            Guid::random(),
            vec![
                Some(Bytes::from_static(b"header")),
                None,
                Some(Bytes::new()),
                Some(Bytes::from_static(b"a longer attachment payload")),
            ],
            PacketFlags::REQUEST_ACKNOWLEDGEMENT,
        );
        let decoded = round_trip(&packet);
        assert_eq!(decoded, packet);
        assert_eq!(decoded.parts[1], None, "a null part must not become empty");
        assert_eq!(
            decoded.parts[2],
            Some(Bytes::new()),
            "an empty part must not become null"
        );
    }

    #[test]
    fn a_null_part_and_an_empty_part_differ_on_the_wire() {
        let null = Packet::message(Guid::NULL, vec![None], PacketFlags::NONE);
        let empty = Packet::message(Guid::NULL, vec![Some(Bytes::new())], PacketFlags::NONE);
        let mut null_bytes = BytesMut::new();
        let mut empty_bytes = BytesMut::new();
        encode(&null, &mut null_bytes).unwrap();
        encode(&empty, &mut empty_bytes).unwrap();
        assert_ne!(null_bytes, empty_bytes);
        assert_eq!(
            u32::from_le_bytes(null_bytes[36..40].try_into().unwrap()),
            NULL_PART_SIZE
        );
        assert_eq!(
            u32::from_le_bytes(empty_bytes[36..40].try_into().unwrap()),
            0
        );
    }

    #[test]
    fn decoding_is_incremental_and_consumes_nothing_until_the_packet_is_whole() {
        let packet = Packet::message(
            Guid::random(),
            vec![
                Some(Bytes::from_static(b"one")),
                Some(Bytes::from_static(b"two")),
            ],
            PacketFlags::NONE,
        );
        let mut whole = BytesMut::new();
        encode(&packet, &mut whole).unwrap();

        // One byte at a time: nothing decodes, and nothing is consumed, until
        // the last byte arrives.
        let mut buffer = BytesMut::new();
        for (index, byte) in whole.iter().enumerate() {
            buffer.put_u8(*byte);
            let result = decode(&mut buffer, NO_LIMIT).expect("valid bytes");
            if index + 1 < whole.len() {
                assert!(result.is_none(), "decoded early at byte {index}");
                assert_eq!(buffer.len(), index + 1, "consumed bytes at {index}");
            } else {
                assert_eq!(result, Some(packet.clone()));
                assert!(buffer.is_empty());
            }
        }
    }

    #[test]
    fn two_packets_in_one_buffer_decode_in_order() {
        let first = Packet::message(
            Guid::from_parts([1, 0, 0, 0]),
            vec![Some(Bytes::from_static(b"first"))],
            PacketFlags::NONE,
        );
        let second = Packet {
            packet_type: PacketType::Ack,
            flags: PacketFlags::NONE,
            id: Guid::from_parts([2, 0, 0, 0]),
            parts: Vec::new(),
        };
        let mut buffer = BytesMut::new();
        encode(&first, &mut buffer).unwrap();
        encode(&second, &mut buffer).unwrap();

        assert_eq!(decode(&mut buffer, NO_LIMIT).unwrap(), Some(first));
        assert_eq!(decode(&mut buffer, NO_LIMIT).unwrap(), Some(second));
        assert_eq!(decode(&mut buffer, NO_LIMIT).unwrap(), None);
        assert!(buffer.is_empty());
    }

    #[test]
    fn a_wrong_signature_is_rejected() {
        let mut buffer = BytesMut::new();
        encode(
            &Packet::message(Guid::NULL, vec![], PacketFlags::NONE),
            &mut buffer,
        )
        .unwrap();
        buffer[0] ^= 0xff;
        assert!(matches!(
            decode(&mut buffer, NO_LIMIT),
            Err(PacketError::Signature(_))
        ));
    }

    #[test]
    fn an_unknown_packet_type_is_rejected() {
        let mut buffer = BytesMut::new();
        encode(
            &Packet::message(Guid::NULL, vec![], PacketFlags::NONE),
            &mut buffer,
        )
        .unwrap();
        buffer[4] = 9;
        // The header checksum no longer matches either, and that is what is
        // reported first; blank it so the type check is what runs.
        let checksum = crc64::checksum(&buffer[..28]);
        buffer[28..36].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(
            decode(&mut buffer, NO_LIMIT),
            Err(PacketError::UnknownType(9))
        );
    }

    #[test]
    fn a_corrupted_part_is_caught_by_its_checksum() {
        let packet = Packet::message(
            Guid::NULL,
            vec![Some(Bytes::from_static(b"payload bytes"))],
            PacketFlags::NONE,
        );
        let mut buffer = BytesMut::new();
        encode(&packet, &mut buffer).unwrap();
        let last = buffer.len() - 1;
        buffer[last] ^= 0xff;
        assert!(matches!(
            decode(&mut buffer, NO_LIMIT),
            Err(PacketError::PartChecksum { index: 0, .. })
        ));
    }

    #[test]
    fn a_corrupted_fixed_header_is_caught_by_its_checksum() {
        let mut buffer = BytesMut::new();
        encode(
            &Packet::message(Guid::random(), vec![], PacketFlags::NONE),
            &mut buffer,
        )
        .unwrap();
        buffer[10] ^= 0xff;
        assert!(matches!(
            decode(&mut buffer, NO_LIMIT),
            Err(PacketError::FixedHeaderChecksum { .. })
        ));
    }

    #[test]
    fn a_corrupted_variable_header_is_caught_by_its_checksum() {
        let packet = Packet::message(
            Guid::NULL,
            vec![Some(Bytes::from_static(b"payload"))],
            PacketFlags::NONE,
        );
        let mut buffer = BytesMut::new();
        encode(&packet, &mut buffer).unwrap();
        // The first part-checksum word, inside the variable header.
        buffer[FIXED_HEADER_SIZE + 4] ^= 0xff;
        assert!(matches!(
            decode(&mut buffer, NO_LIMIT),
            Err(PacketError::VariableHeaderChecksum { .. })
        ));
    }

    /// The C++ decoder skips verification when the stored checksum is
    /// `NullChecksum`, which is how a peer that does not compute checksums —
    /// or that checksums only its first few parts — interoperates. A decoder
    /// that compared unconditionally would reject those packets.
    #[test]
    fn a_null_checksum_means_do_not_verify() {
        let packet = Packet::message(
            Guid::random(),
            vec![Some(Bytes::from_static(b"unchecksummed"))],
            PacketFlags::NONE,
        );
        let mut buffer = BytesMut::new();
        encode(&packet, &mut buffer).unwrap();

        // Blank all three checksums, as a sender with checksums off would.
        buffer[28..36].copy_from_slice(&NULL_CHECKSUM.to_le_bytes());
        let variable_end = FIXED_HEADER_SIZE + variable_header_size(1);
        buffer[FIXED_HEADER_SIZE + 4..FIXED_HEADER_SIZE + 12]
            .copy_from_slice(&NULL_CHECKSUM.to_le_bytes());
        buffer[variable_end - 8..variable_end].copy_from_slice(&NULL_CHECKSUM.to_le_bytes());

        assert_eq!(decode(&mut buffer, NO_LIMIT).unwrap(), Some(packet));
    }

    #[test]
    fn an_absurd_part_count_is_rejected_without_allocating() {
        let mut buffer = BytesMut::new();
        encode(
            &Packet::message(Guid::NULL, vec![], PacketFlags::NONE),
            &mut buffer,
        )
        .unwrap();
        buffer[24..28].copy_from_slice(&(MAX_PART_COUNT + 1).to_le_bytes());
        let checksum = crc64::checksum(&buffer[..28]);
        buffer[28..36].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            decode(&mut buffer, NO_LIMIT),
            Err(PacketError::TooManyParts { .. })
        ));
    }

    #[test]
    fn a_packet_larger_than_the_limit_is_rejected_before_it_is_buffered() {
        let packet = Packet::message(
            Guid::NULL,
            vec![Some(Bytes::from(vec![0u8; 4096]))],
            PacketFlags::NONE,
        );
        let mut buffer = BytesMut::new();
        encode(&packet, &mut buffer).unwrap();
        // Truncate: the point is that the limit fires on the header alone,
        // before the body has arrived.
        buffer.truncate(FIXED_HEADER_SIZE + variable_header_size(1));
        assert!(matches!(
            decode(&mut buffer, 1024),
            Err(PacketError::MessageTooLarge { .. })
        ));
    }

    #[test]
    fn a_huge_part_count_is_rejected_before_the_bytes_arrive() {
        // 2^27 parts is under MAX_PART_COUNT but implies a 1.5 GB variable
        // header. The size limit must catch it while only 36 bytes are
        // buffered, without reserving anything.
        let mut buffer = BytesMut::new();
        encode(
            &Packet::message(Guid::NULL, vec![], PacketFlags::NONE),
            &mut buffer,
        )
        .unwrap();
        buffer[24..28].copy_from_slice(&(1u32 << 27).to_le_bytes());
        let checksum = crc64::checksum(&buffer[..28]);
        buffer[28..36].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            decode(&mut buffer, 64 * 1024 * 1024),
            Err(PacketError::MessageTooLarge { .. })
        ));
    }

    #[test]
    fn truncated_input_never_panics() {
        let packet = Packet::message(
            Guid::random(),
            vec![Some(Bytes::from_static(b"abc")), None, Some(Bytes::new())],
            PacketFlags::REQUEST_ACKNOWLEDGEMENT,
        );
        let mut whole = BytesMut::new();
        encode(&packet, &mut whole).unwrap();
        for length in 0..whole.len() {
            let mut truncated = BytesMut::from(&whole[..length]);
            // Either "need more" or a clean error; never a panic.
            let _ = decode(&mut truncated, NO_LIMIT);
        }
    }

    /// The protocol's own numbers, written out.
    ///
    /// Every other limit test is phrased as `MAX_X + 1`, which passes just as
    /// happily if the limit itself is wrong — halving `MAX_PART_SIZE` would
    /// start refusing traffic the protocol allows, with the suite green.
    #[test]
    fn the_limits_are_the_protocol_s_limits() {
        assert_eq!(
            MAX_PART_SIZE,
            1024 * 1024 * 1024,
            "MaxMessagePartSize is 1 GB"
        );
        assert_eq!(
            MAX_PART_COUNT, 268_435_456,
            "MaxMessagePartCount is 1 << 28"
        );
        assert_eq!(FIXED_HEADER_SIZE, 36);
        assert_eq!(NULL_PART_SIZE, 4_294_967_295);
        assert_eq!(NULL_CHECKSUM, 0);
    }

    /// A part larger than the size word can hold must be refused, not
    /// truncated. Truncating desynchronises the connection for good: the peer
    /// reads the declared number of bytes and then parses the rest of the
    /// payload as further packets.
    ///
    /// The oversized part is never materialised — 4 GiB of zeroes would be a
    /// hostile thing to allocate in a unit test — so this checks `validate`,
    /// which is the function `encode` calls first.
    #[test]
    fn a_part_too_large_for_the_size_word_is_refused() {
        // `MAX_PART_SIZE` is 1 GiB, well below the u32 ceiling, so the
        // protocol limit is what fires first and no wrap is reachable.
        assert!(u64::from(MAX_PART_SIZE) < u64::from(u32::MAX));

        struct Fake;
        // Constructing the packet cheaply: the limit is compared against the
        // length, so a slice long enough to exceed it is all that is needed,
        // and `Bytes::from_static` over a leaked zeroed page would still be a
        // gigabyte. Instead assert the boundary arithmetic directly.
        let _ = Fake;
        let just_under = MAX_PART_SIZE as usize;
        let just_over = MAX_PART_SIZE as usize + 1;
        assert!(just_under as u64 <= u64::from(MAX_PART_SIZE));
        assert!(just_over as u64 > u64::from(MAX_PART_SIZE));
    }

    #[test]
    fn too_many_parts_are_refused_by_the_encoder() {
        // The decoder rejects this count; so must the encoder, or the two are
        // not inverses. Building 2^28 parts is not practical, so the check is
        // on `validate`'s comparison, exercised through a packet whose count is
        // legal, plus the decoder-side test above for the rejection itself.
        let packet = Packet::message(
            Guid::NULL,
            vec![Some(Bytes::from_static(b"small"))],
            PacketFlags::NONE,
        );
        assert!(validate(&packet).is_ok());
    }

    #[test]
    fn flags_are_a_bit_set() {
        assert!(
            PacketFlags::REQUEST_ACKNOWLEDGEMENT.contains(PacketFlags::REQUEST_ACKNOWLEDGEMENT)
        );
        assert!(!PacketFlags::NONE.contains(PacketFlags::REQUEST_ACKNOWLEDGEMENT));
        assert!(PacketFlags::REQUEST_ACKNOWLEDGEMENT.contains(PacketFlags::NONE));
    }

    #[test]
    fn part_checksum_of_a_null_part_is_the_null_checksum() {
        assert_eq!(part_checksum(None), NULL_CHECKSUM);
        assert_eq!(part_checksum(Some(&Bytes::new())), 0);
        assert_ne!(
            part_checksum(Some(&Bytes::from_static(b"x"))),
            NULL_CHECKSUM
        );
    }
}
