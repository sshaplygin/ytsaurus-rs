//! Scratch probes for the review. Deleted afterwards.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use ytsaurus_rpc::bus::packet::{self, Packet, PacketFlags};
use ytsaurus_rpc::bus::{DEFAULT_MAX_MESSAGE_SIZE, HANDSHAKE_SIGNATURE};
use ytsaurus_rpc::connection::Connection;
use ytsaurus_rpc::guid::Guid;
use ytsaurus_rpc::proto;

fn handshake_reply(id: Guid) -> Vec<u8> {
    let handshake = proto::bus::THandshake {
        connection_id: Guid::random().to_proto(),
        encryption_mode: Some(0),
        ..Default::default()
    };
    let mut part = Vec::new();
    part.extend_from_slice(&HANDSHAKE_SIGNATURE.to_le_bytes());
    handshake.encode(&mut part).unwrap();
    let reply = Packet::message(id, vec![Some(Bytes::from(part))], PacketFlags::NONE);
    let mut out = BytesMut::new();
    packet::encode(&reply, &mut out);
    out.to_vec()
}

/// Probe 1: a single corrupt packet kills the reader task while the writer
/// stays alive. Does a later call with no timeout hang forever?
#[tokio::test]
async fn probe_reader_death_hangs_later_calls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();
        let mut buffer = BytesMut::new();
        // Read the handshake.
        loop {
            if let Ok(Some(request)) = packet::decode(&mut buffer, DEFAULT_MAX_MESSAGE_SIZE) {
                write_half
                    .write_all(&handshake_reply(request.id))
                    .await
                    .unwrap();
                break;
            }
            if read_half.read_buf(&mut buffer).await.unwrap() == 0 {
                return;
            }
        }
        // Now send one packet with a broken signature: the client's decoder
        // errors, which ends its read loop.
        let mut junk = BytesMut::new();
        packet::encode(
            &Packet::message(Guid::random(), vec![Some(Bytes::from_static(b"x"))], PacketFlags::NONE),
            &mut junk,
        );
        junk[0] ^= 0xff;
        write_half.write_all(&junk).await.unwrap();
        // Keep the socket open and keep draining, so the writer half stays fine.
        let mut sink = vec![0u8; 4096];
        loop {
            if read_half.read(&mut sink).await.unwrap_or(0) == 0 {
                return;
            }
        }
    });

    let connection = Connection::connect(&address, None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(connection.is_closed(), "the reader task should have died");

    let request = proto::api::TReqPingTransaction {
        transaction_id: Guid::random().to_proto(),
        ..Default::default()
    };
    let call = connection.invoke::<proto::api::TRspPingTransaction>(
        "PingTransaction",
        &request,
        Vec::new(),
        None,
        "TRspPingTransaction",
    );
    let outcome = tokio::time::timeout(Duration::from_millis(1500), call).await;
    match outcome {
        Err(_) => panic!("HANG CONFIRMED: the call never returned after the reader died"),
        Ok(result) => println!("call returned: {result:?}"),
    }
}

/// Probe 2: the peer stops reading. Does a call with a 100 ms timeout still
/// return in anything like 100 ms?
#[tokio::test]
async fn probe_stalled_writer_ignores_the_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();
        let mut buffer = BytesMut::new();
        loop {
            if let Ok(Some(request)) = packet::decode(&mut buffer, DEFAULT_MAX_MESSAGE_SIZE) {
                write_half
                    .write_all(&handshake_reply(request.id))
                    .await
                    .unwrap();
                break;
            }
            if read_half.read_buf(&mut buffer).await.unwrap() == 0 {
                return;
            }
        }
        // Never read another byte, and never close.
        tokio::time::sleep(Duration::from_secs(120)).await;
        drop(read_half);
    });

    let connection = Arc::new(Connection::connect(&address, None).await.unwrap());
    let finished = Arc::new(AtomicUsize::new(0));
    let attachment = Bytes::from(vec![0u8; 1024 * 1024]);

    let mut handles = Vec::new();
    for _ in 0..200 {
        let connection = Arc::clone(&connection);
        let finished = Arc::clone(&finished);
        let attachment = attachment.clone();
        handles.push(tokio::spawn(async move {
            let _ = connection
                .invoke::<proto::api::TRspPingTransaction>(
                    "PingTransaction",
                    &proto::api::TReqPingTransaction {
                        transaction_id: Guid::random().to_proto(),
                        ..Default::default()
                    },
                    vec![attachment],
                    Some(Duration::from_millis(100)),
                    "TRspPingTransaction",
                )
                .await;
            finished.fetch_add(1, Ordering::Relaxed);
        }));
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    let done = finished.load(Ordering::Relaxed);
    println!("after 3 s, {done}/200 calls with a 100 ms timeout have returned");
    assert_eq!(
        done, 200,
        "HANG CONFIRMED: {} of 200 calls are still stuck 30x past their timeout",
        200 - done
    );
}

/// Probe 3: dropping the invoke future part-way. Is a cancellation sent?
#[tokio::test]
async fn probe_plain_drop_sends_no_cancelation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (seen_sender, mut seen) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();
        let mut buffer = BytesMut::new();
        let mut handshaken = false;
        loop {
            match packet::decode(&mut buffer, DEFAULT_MAX_MESSAGE_SIZE) {
                Ok(Some(request)) => {
                    if !handshaken {
                        handshaken = true;
                        write_half
                            .write_all(&handshake_reply(request.id))
                            .await
                            .unwrap();
                    } else if let Some(Some(part)) = request.parts.first() {
                        let _ = seen_sender.send(part[0..4].to_vec());
                    }
                    continue;
                }
                Ok(None) => {}
                Err(_) => return,
            }
            if read_half.read_buf(&mut buffer).await.unwrap_or(0) == 0 {
                return;
            }
        }
    });

    let connection = Connection::connect(&address, None).await.unwrap();
    let request = proto::api::TReqPingTransaction {
        transaction_id: Guid::random().to_proto(),
        ..Default::default()
    };
    {
        let call = connection.invoke::<proto::api::TRspPingTransaction>(
            "PingTransaction",
            &request,
            Vec::new(),
            None,
            "TRspPingTransaction",
        );
        let _ = tokio::time::timeout(Duration::from_millis(150), call).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut tags = Vec::new();
    while let Ok(tag) = seen.try_recv() {
        tags.push(String::from_utf8_lossy(&tag).into_owned());
    }
    println!("messages the proxy saw: {tags:?}");
    assert!(
        tags.iter().any(|tag| tag == "rpcc"),
        "NO CANCELLATION: dropping the future sent {tags:?}, no rpcc"
    );
}
