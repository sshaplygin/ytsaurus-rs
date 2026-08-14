//! Layer 1: bus, the TCP transport.
//!
//! [`packet`] is the sans-io half — pure byte↔struct functions with no `async`
//! anywhere. This module is the thin I/O edge that owns a socket, performs the
//! handshake and reads and writes whole packets.

pub mod packet;

use bytes::{Bytes, BytesMut};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::error::{Error, Result};
use crate::guid::Guid;
use crate::proto;
use packet::{Packet, PacketFlags, PacketType};

/// The four bytes in front of a serialized `THandshake`, spelling "bush" on the
/// wire — `handshakeSignature` in `yt/go/bus/bus.go`.
pub const HANDSHAKE_SIGNATURE: u32 = 0x6873_7562;

/// The packet id both sides use for the handshake: the GUID whose first word is
/// 1 and whose rest is zero.
fn handshake_packet_id() -> Guid {
    Guid::from_parts([1, 0, 0, 0])
}

/// `EEncryptionMode`. Only `Disabled` is implemented — TLS is a later feature,
/// and a peer that *requires* encryption is refused rather than silently
/// downgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum EncryptionMode {
    Disabled = 0,
    Optional = 1,
    Required = 2,
}

/// How large a single packet may be before the connection rejects it.
///
/// The protocol allows 1 GB per part; this is a much lower default so a corrupt
/// or hostile length word cannot make the client reserve unbounded memory. A
/// caller reading genuinely large rowsets can raise it.
pub const DEFAULT_MAX_MESSAGE_SIZE: u64 = 512 * 1024 * 1024;

/// One TCP connection speaking bus, after a successful handshake.
///
/// Reading and writing are separate halves so the connection actor can own one
/// in each direction without a lock.
#[derive(Debug)]
pub struct Bus {
    pub reader: BusReader,
    pub writer: BusWriter,
    /// The connection id sent in our handshake, for diagnostics.
    pub connection_id: Guid,
}

#[derive(Debug)]
pub struct BusReader {
    stream: OwnedReadHalf,
    buffer: BytesMut,
    max_message_size: u64,
}

#[derive(Debug)]
pub struct BusWriter {
    stream: OwnedWriteHalf,
    buffer: BytesMut,
}

impl Bus {
    /// Connects and completes the handshake.
    pub async fn connect(address: &str) -> Result<Self> {
        Self::connect_with(address, DEFAULT_MAX_MESSAGE_SIZE).await
    }

    /// Connects with an explicit packet-size ceiling.
    pub async fn connect_with(address: &str, max_message_size: u64) -> Result<Self> {
        let stream = TcpStream::connect(address)
            .await
            .map_err(|source| Error::Connect {
                address: address.to_owned(),
                source,
            })?;
        // Bus is a request/response protocol over long-lived connections;
        // Nagle would add up to 40 ms to a small request whose whole point is
        // latency.
        stream.set_nodelay(true)?;

        let (read_half, write_half) = stream.into_split();
        let mut bus = Self {
            reader: BusReader {
                stream: read_half,
                buffer: BytesMut::with_capacity(64 * 1024),
                max_message_size,
            },
            writer: BusWriter {
                stream: write_half,
                buffer: BytesMut::with_capacity(64 * 1024),
            },
            connection_id: Guid::random(),
        };
        bus.handshake().await?;
        Ok(bus)
    }

    /// Sends our handshake and reads the peer's.
    ///
    /// The client speaks first. Both sides send a *message* packet whose single
    /// part is the handshake signature followed by a serialized `THandshake`.
    async fn handshake(&mut self) -> Result<()> {
        let handshake = proto::bus::THandshake {
            connection_id: self.connection_id.to_proto(),
            encryption_mode: Some(EncryptionMode::Disabled as i32),
            ..Default::default()
        };

        let mut part = Vec::with_capacity(4 + handshake.encoded_len());
        part.extend_from_slice(&HANDSHAKE_SIGNATURE.to_le_bytes());
        handshake
            .encode(&mut part)
            .expect("a Vec never runs out of room");

        self.writer
            .send(&Packet::message(
                handshake_packet_id(),
                vec![Some(Bytes::from(part))],
                PacketFlags::NONE,
            ))
            .await?;

        let reply = self.reader.receive().await?;
        if reply.packet_type != PacketType::Message {
            return Err(Error::Protocol(format!(
                "handshake reply is a {:?} packet, expected a message",
                reply.packet_type
            )));
        }
        if reply.id != handshake_packet_id() {
            return Err(Error::Protocol(format!(
                "handshake reply has packet id {}, expected {}",
                reply.id,
                handshake_packet_id()
            )));
        }
        let [Some(payload)] = reply.parts.as_slice() else {
            return Err(Error::Protocol(format!(
                "handshake reply has {} parts, expected exactly one",
                reply.parts.len()
            )));
        };
        if payload.len() < 4 {
            return Err(Error::Protocol(
                "handshake reply is too short to hold its signature".to_owned(),
            ));
        }
        let signature = u32::from_le_bytes(payload[0..4].try_into().unwrap());
        if signature != HANDSHAKE_SIGNATURE {
            return Err(Error::Protocol(format!(
                "handshake reply signature is {signature:#010x}, expected {HANDSHAKE_SIGNATURE:#010x}"
            )));
        }

        let peer =
            proto::bus::THandshake::decode(&payload[4..]).map_err(|source| Error::Decode {
                message: "THandshake",
                source,
            })?;
        if peer.encryption_mode == Some(EncryptionMode::Required as i32) {
            return Err(Error::Protocol(
                "the proxy requires encryption, which this crate does not implement yet".to_owned(),
            ));
        }

        Ok(())
    }
}

impl BusWriter {
    /// Writes one packet and flushes it.
    pub async fn send(&mut self, message: &Packet) -> Result<()> {
        self.buffer.clear();
        packet::encode(message, &mut self.buffer);
        self.stream.write_all(&self.buffer).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown().await?;
        Ok(())
    }
}

impl BusReader {
    /// Reads until one whole packet is available.
    pub async fn receive(&mut self) -> Result<Packet> {
        loop {
            if let Some(message) = packet::decode(&mut self.buffer, self.max_message_size)? {
                return Ok(message);
            }
            let read = self.stream.read_buf(&mut self.buffer).await?;
            if read == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the proxy closed the connection",
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A stub that speaks just enough bus to answer a handshake, so the
    /// handshake can be tested without a cluster.
    async fn handshake_stub(reply: impl Fn(Packet) -> Option<Packet> + Send + 'static) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut read_half, mut write_half) = stream.into_split();
            let mut buffer = BytesMut::new();
            loop {
                match packet::decode(&mut buffer, DEFAULT_MAX_MESSAGE_SIZE) {
                    Ok(Some(request)) => {
                        if let Some(response) = reply(request) {
                            let mut out = BytesMut::new();
                            packet::encode(&response, &mut out);
                            let _ = write_half.write_all(&out).await;
                            let _ = write_half.flush().await;
                        }
                        return;
                    }
                    Ok(None) => {}
                    Err(_) => return,
                }
                if read_half.read_buf(&mut buffer).await.unwrap_or(0) == 0 {
                    return;
                }
            }
        });
        address
    }

    fn handshake_reply(mode: EncryptionMode) -> Packet {
        let handshake = proto::bus::THandshake {
            connection_id: Guid::random().to_proto(),
            encryption_mode: Some(mode as i32),
            ..Default::default()
        };
        let mut part = Vec::new();
        part.extend_from_slice(&HANDSHAKE_SIGNATURE.to_le_bytes());
        handshake.encode(&mut part).unwrap();
        Packet::message(
            handshake_packet_id(),
            vec![Some(Bytes::from(part))],
            PacketFlags::NONE,
        )
    }

    #[tokio::test]
    async fn the_client_speaks_first_and_its_handshake_is_well_formed() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let sender = std::sync::Mutex::new(Some(sender));
        let address = handshake_stub(move |request| {
            if let Some(sender) = sender.lock().unwrap().take() {
                let _ = sender.send(request.clone());
            }
            Some(handshake_reply(EncryptionMode::Disabled))
        })
        .await;

        Bus::connect(&address)
            .await
            .expect("the handshake should succeed");

        let request = receiver.await.unwrap();
        assert_eq!(request.packet_type, PacketType::Message);
        assert_eq!(
            request.id,
            handshake_packet_id(),
            "the handshake packet id is 1-0-0-0"
        );
        assert_eq!(request.parts.len(), 1);

        let payload = request.parts[0].as_ref().unwrap();
        assert_eq!(
            u32::from_le_bytes(payload[0..4].try_into().unwrap()),
            HANDSHAKE_SIGNATURE
        );
        assert_eq!(&payload[0..4], b"bush", "the signature spells bush");
        let handshake = proto::bus::THandshake::decode(&payload[4..]).unwrap();
        assert_eq!(handshake.encryption_mode, Some(0), "encryption is disabled");
    }

    #[tokio::test]
    async fn a_peer_that_requires_encryption_is_refused_not_downgraded() {
        let address = handshake_stub(|_| Some(handshake_reply(EncryptionMode::Required))).await;
        let error = Bus::connect(&address).await.unwrap_err();
        assert!(
            error.to_string().contains("requires encryption"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_handshake_with_the_wrong_signature_is_refused() {
        let address = handshake_stub(|_| {
            Some(Packet::message(
                handshake_packet_id(),
                vec![Some(Bytes::from_static(b"junk-and-more-junk"))],
                PacketFlags::NONE,
            ))
        })
        .await;
        let error = Bus::connect(&address).await.unwrap_err();
        assert!(
            error.to_string().contains("signature"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_handshake_with_the_wrong_packet_id_is_refused() {
        let address = handshake_stub(|_| {
            let mut reply = handshake_reply(EncryptionMode::Disabled);
            reply.id = Guid::from_parts([7, 0, 0, 0]);
            Some(reply)
        })
        .await;
        let error = Bus::connect(&address).await.unwrap_err();
        assert!(
            error.to_string().contains("packet id"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_closed_connection_is_an_error_not_a_hang() {
        let address = handshake_stub(|_| None).await;
        let error = Bus::connect(&address).await.unwrap_err();
        assert!(
            error.to_string().contains("closed the connection"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn connecting_to_a_closed_port_reports_the_address() {
        // Port 1 on loopback: reserved, and nothing listens there.
        let error = Bus::connect("127.0.0.1:1").await.unwrap_err();
        assert!(
            error.to_string().contains("127.0.0.1:1"),
            "unexpected error: {error}"
        );
    }
}
