//! What the client actually puts on the wire, and what it does with the reply.
//!
//! The request builders are unit-tested in `client.rs`, but everything between
//! a builder and the socket was not: which attachment is sent, whether the
//! token reaches the header, what happens to the rows that come back. Mutation
//! testing put a number on it — twelve of thirteen deliberate defects in that
//! layer survived the whole suite — so these drive a real [`Client`] against a
//! stub proxy and read the bytes it receives.
//!
//! A stub rather than a cluster because these assert on the *request*, and a
//! real proxy answers without telling you what it saw. The live end-to-end
//! example covers the other direction.

use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use ytsaurus_rpc::bus::packet::{self, Packet, PacketFlags};
use ytsaurus_rpc::bus::{DEFAULT_MAX_MESSAGE_SIZE, HANDSHAKE_SIGNATURE};
use ytsaurus_rpc::client::{Client, LookupOptions, SelectOptions, TransactionType};
use ytsaurus_rpc::guid::Guid;
use ytsaurus_rpc::proto;
use ytsaurus_rpc::wire::{self, MaybeRow, UnversionedValue, Value};

/// One request as the stub saw it.
#[derive(Clone)]
struct SeenRequest {
    header: proto::rpc::TRequestHeader,
    /// The header part exactly as it arrived. Decoding into `TRequestHeader`
    /// drops the extension fields `prost` does not generate, and the
    /// credentials are one of those, so the raw bytes are the only place a
    /// token can be looked for.
    raw_header: Bytes,
    body: Bytes,
    attachments: Vec<Bytes>,
}

/// A proxy that records every request and answers with a canned reply.
struct StubProxy {
    address: String,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for StubProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl StubProxy {
    fn requests(&self) -> Vec<SeenRequest> {
        self.seen.lock().unwrap().clone()
    }

    fn method(&self, name: &str) -> SeenRequest {
        self.requests()
            .into_iter()
            .find(|request| request.header.method == name)
            .unwrap_or_else(|| panic!("the client never sent {name}"))
    }
}

/// Answers each request by method name.
async fn stub_proxy(reply: impl Fn(&str) -> (Vec<u8>, Vec<Bytes>) + Send + 'static) -> StubProxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let seen: Arc<Mutex<Vec<SeenRequest>>> = Arc::default();
    let recorded = Arc::clone(&seen);

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();
        let mut buffer = BytesMut::new();
        let mut handshaken = false;

        loop {
            match packet::decode(&mut buffer, DEFAULT_MAX_MESSAGE_SIZE) {
                Ok(Some(request)) => {
                    if !handshaken {
                        handshaken = true;
                        let handshake = proto::bus::THandshake {
                            connection_id: Guid::random().to_proto(),
                            encryption_mode: Some(0),
                            ..Default::default()
                        };
                        let mut part = Vec::new();
                        part.extend_from_slice(&HANDSHAKE_SIGNATURE.to_le_bytes());
                        handshake.encode(&mut part).unwrap();
                        let mut out = BytesMut::new();
                        packet::encode(
                            &Packet::message(
                                request.id,
                                vec![Some(Bytes::from(part))],
                                PacketFlags::NONE,
                            ),
                            &mut out,
                        )
                        .unwrap();
                        if write_half.write_all(&out).await.is_err() {
                            return;
                        }
                        continue;
                    }

                    let Some(Some(header_part)) = request.parts.first().cloned() else {
                        continue;
                    };
                    let Ok(header) = proto::rpc::TRequestHeader::decode(&header_part[4..]) else {
                        continue;
                    };
                    let method = header.method.clone();
                    let request_id = Guid::from_proto(header.request_id.as_ref().unwrap());

                    recorded.lock().unwrap().push(SeenRequest {
                        header,
                        raw_header: header_part.clone(),
                        body: request.parts.get(1).cloned().flatten().unwrap_or_default(),
                        attachments: request.parts[2..].iter().flatten().cloned().collect(),
                    });

                    let (body, attachments) = reply(&method);
                    let response_header = proto::rpc::TResponseHeader {
                        request_id: Some(request_id.to_proto()),
                        ..Default::default()
                    };
                    let mut header_bytes = Vec::new();
                    header_bytes.extend_from_slice(&0x6f63_7072u32.to_le_bytes());
                    response_header.encode(&mut header_bytes).unwrap();

                    let mut parts = vec![Some(Bytes::from(header_bytes)), Some(Bytes::from(body))];
                    parts.extend(attachments.into_iter().map(Some));

                    let mut out = BytesMut::new();
                    packet::encode(
                        &Packet::message(Guid::random(), parts, PacketFlags::NONE),
                        &mut out,
                    )
                    .unwrap();
                    if write_half.write_all(&out).await.is_err() {
                        return;
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

    StubProxy {
        address,
        seen,
        task,
    }
}

/// The rowset a lookup reply carries, and the descriptor naming its columns.
fn lookup_reply(rows: &[MaybeRow]) -> (Vec<u8>, Vec<Bytes>) {
    let response = proto::api::TRspLookupRows {
        rowset_descriptor: proto::api::TRowsetDescriptor {
            wire_format_version: Some(1),
            rowset_kind: Some(proto::api::ERowsetKind::RkUnversioned as i32),
            name_table_entries: ["key", "value"]
                .iter()
                .map(|name| proto::api::t_rowset_descriptor::TNameTableEntry {
                    name: Some((*name).to_owned()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        ..Default::default()
    };
    (
        response.encode_to_vec(),
        vec![wire::encode_rowset(rows).unwrap()],
    )
}

fn transaction_reply() -> Vec<u8> {
    proto::api::TRspStartTransaction {
        id: Guid::from_parts([7, 0, 0, 0]).to_proto(),
        start_timestamp: 1234,
        ..Default::default()
    }
    .encode_to_vec()
}

fn key(value: i64) -> Vec<UnversionedValue> {
    vec![UnversionedValue::new(0, Value::Int64(value))]
}

#[tokio::test]
async fn a_lookup_sends_its_keys_as_a_rowset_and_reads_the_answer_back() {
    let found = vec![Some(vec![
        UnversionedValue::new(0, Value::Int64(1)),
        UnversionedValue::new(1, Value::String(Bytes::from_static(b"one"))),
    ])];
    let expected = found.clone();
    let stub = stub_proxy(move |_| lookup_reply(&expected)).await;

    let client = Client::connect(&stub.address).await.unwrap();
    let rows = client
        .lookup_rows("//tmp/t", &["key"], &[key(1)], LookupOptions::default())
        .await
        .unwrap();

    // The reply's rows reached the caller.
    assert_eq!(rows, found);

    // The keys went out as a wire rowset in an attachment, not in the body.
    let request = stub.method("LookupRows");
    assert_eq!(request.attachments.len(), 1, "one attachment, the keys");
    assert_eq!(
        wire::decode_rowset(&request.attachments[0]).unwrap(),
        vec![Some(key(1))],
        "the attachment is the keys asked for"
    );

    // And the body named the table and asked for missing keys to be reported.
    let body = proto::api::TReqLookupRows::decode(request.body.clone()).unwrap();
    assert_eq!(body.path, b"//tmp/t".to_vec());
    assert_eq!(body.keep_missing_rows, Some(true));
    assert_eq!(body.timestamp, None, "no timestamp means latest, not zero");
}

#[tokio::test]
async fn a_missing_key_comes_back_as_a_null_row_in_place() {
    // Second key absent: the proxy answers with a null row, and the caller must
    // see `None` at that position rather than a shorter list.
    let answer = vec![
        Some(vec![UnversionedValue::new(0, Value::Int64(1))]),
        None,
        Some(vec![UnversionedValue::new(0, Value::Int64(3))]),
    ];
    let expected = answer.clone();
    let stub = stub_proxy(move |_| lookup_reply(&expected)).await;

    let client = Client::connect(&stub.address).await.unwrap();
    let rows = client
        .lookup_rows(
            "//tmp/t",
            &["key"],
            &[key(1), key(2), key(3)],
            LookupOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 3, "one answer per key asked for");
    assert!(rows[0].is_some());
    assert!(rows[1].is_none(), "the missing key must stay in place");
    assert!(rows[2].is_some());
}

#[tokio::test]
async fn a_write_names_its_transaction_and_one_modification_type_per_row() {
    let stub = stub_proxy(|method| match method {
        "StartTransaction" => (transaction_reply(), Vec::new()),
        _ => (Vec::new(), Vec::new()),
    })
    .await;

    let client = Client::connect(&stub.address).await.unwrap();
    let transaction = client
        .start_transaction(TransactionType::Tablet, Default::default())
        .await
        .unwrap();

    let rows = vec![
        vec![
            UnversionedValue::new(0, Value::Int64(1)),
            UnversionedValue::new(1, Value::String(Bytes::from_static(b"one"))),
        ],
        vec![
            UnversionedValue::new(0, Value::Int64(2)),
            UnversionedValue::new(1, Value::String(Bytes::from_static(b"two"))),
        ],
    ];
    transaction
        .insert_rows("//tmp/t", &["key", "value"], &rows)
        .await
        .unwrap();

    let request = stub.method("ModifyRows");
    let body = proto::api::TReqModifyRows::decode(request.body.clone()).unwrap();

    assert_eq!(
        Guid::from_proto(&body.transaction_id),
        transaction.id(),
        "the write must name the transaction it belongs to"
    );
    assert_eq!(
        body.row_modification_types,
        vec![0, 0],
        "one RMT_WRITE per row, parallel to the attachment"
    );
    assert_eq!(request.attachments.len(), 1);
    assert_eq!(
        wire::decode_rowset(&request.attachments[0]).unwrap().len(),
        2,
        "both rows travelled in the attachment"
    );
}

#[tokio::test]
async fn a_delete_is_a_write_with_a_different_modification_type() {
    let stub = stub_proxy(|method| match method {
        "StartTransaction" => (transaction_reply(), Vec::new()),
        _ => (Vec::new(), Vec::new()),
    })
    .await;

    let client = Client::connect(&stub.address).await.unwrap();
    let transaction = client
        .start_transaction(TransactionType::Tablet, Default::default())
        .await
        .unwrap();
    transaction
        .delete_rows("//tmp/t", &["key"], &[key(1)])
        .await
        .unwrap();

    let body = proto::api::TReqModifyRows::decode(stub.method("ModifyRows").body.clone()).unwrap();
    assert_eq!(
        body.row_modification_types,
        vec![1],
        "RMT_DELETE is 1, and a delete that wrote instead would be silent data loss"
    );
}

#[tokio::test]
async fn a_read_inside_a_transaction_uses_its_start_timestamp() {
    let stub = stub_proxy(|method| match method {
        "StartTransaction" => (transaction_reply(), Vec::new()),
        "LookupRows" => lookup_reply(&[]),
        _ => (
            proto::api::TRspSelectRows {
                rowset_descriptor: proto::api::TRowsetDescriptor {
                    wire_format_version: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            }
            .encode_to_vec(),
            vec![wire::encode_rowset(&[]).unwrap()],
        ),
    })
    .await;

    let client = Client::connect(&stub.address).await.unwrap();
    let transaction = client
        .start_transaction(TransactionType::Tablet, Default::default())
        .await
        .unwrap();
    assert_eq!(transaction.start_timestamp(), 1234);

    transaction
        .lookup_rows("//tmp/t", &["key"], &[key(1)], LookupOptions::default())
        .await
        .unwrap();
    transaction
        .select_rows("* from [//tmp/t]", SelectOptions::default())
        .await
        .unwrap();

    // Neither message has a transaction_id field: the transaction is expressed
    // purely as the timestamp, so getting this wrong reads the wrong data
    // rather than failing.
    let lookup =
        proto::api::TReqLookupRows::decode(stub.method("LookupRows").body.clone()).unwrap();
    assert_eq!(lookup.timestamp, Some(1234));

    let select =
        proto::api::TReqSelectRows::decode(stub.method("SelectRows").body.clone()).unwrap();
    assert_eq!(select.timestamp, Some(1234));
    assert_eq!(select.query, "* from [//tmp/t]");
}

#[tokio::test]
async fn a_select_outside_a_transaction_omits_the_timestamp() {
    // Omitted means the proto default, which is the latest committed data.
    // Sending 0 would be NullTimestamp and read something else entirely.
    let stub = stub_proxy(|_| {
        (
            proto::api::TRspSelectRows {
                rowset_descriptor: proto::api::TRowsetDescriptor {
                    wire_format_version: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            }
            .encode_to_vec(),
            vec![wire::encode_rowset(&[]).unwrap()],
        )
    })
    .await;

    let client = Client::connect(&stub.address).await.unwrap();
    client
        .select_rows("* from [//tmp/t]", SelectOptions::default())
        .await
        .unwrap();

    let select =
        proto::api::TReqSelectRows::decode(stub.method("SelectRows").body.clone()).unwrap();
    assert_eq!(
        select.timestamp, None,
        "a select with no timestamp must omit the field, not send zero"
    );
}

#[tokio::test]
async fn the_token_reaches_the_request_header() {
    let stub = stub_proxy(|_| lookup_reply(&[])).await;

    let client = Client::builder(&stub.address)
        .token("secret-token")
        .connect()
        .await
        .unwrap();
    client
        .lookup_rows("//tmp/t", &["key"], &[key(1)], LookupOptions::default())
        .await
        .unwrap();

    // The credentials ride in a proto2 extension `prost` does not generate, so
    // this searches the header bytes for field 110 and decodes it. A token that
    // never left would be rejected by every authenticated cluster with no local
    // symptom at all.
    let request = stub.method("LookupRows");
    let mut expected_key = Vec::new();
    prost::encoding::encode_key(
        110,
        prost::encoding::WireType::LengthDelimited,
        &mut expected_key,
    );

    let body = &request.raw_header[4..];
    let at = body
        .windows(expected_key.len())
        .position(|window| window == expected_key)
        .expect("the credentials extension must be in the header the proxy received");

    let mut rest = &body[at + expected_key.len()..];
    let length = prost::encoding::decode_varint(&mut rest).unwrap() as usize;
    let credentials = proto::rpc::TCredentialsExt::decode(&rest[..length]).unwrap();
    assert_eq!(credentials.token.as_deref(), Some("secret-token"));
}

#[tokio::test]
async fn no_token_means_no_credentials_on_the_wire() {
    let stub = stub_proxy(|_| lookup_reply(&[])).await;

    let client = Client::connect(&stub.address).await.unwrap();
    client
        .lookup_rows("//tmp/t", &["key"], &[key(1)], LookupOptions::default())
        .await
        .unwrap();

    let request = stub.method("LookupRows");
    let mut expected_key = Vec::new();
    prost::encoding::encode_key(
        110,
        prost::encoding::WireType::LengthDelimited,
        &mut expected_key,
    );
    assert!(
        !request.raw_header[4..]
            .windows(expected_key.len())
            .any(|window| window == expected_key),
        "a client with no token must not send a credentials extension"
    );
}
