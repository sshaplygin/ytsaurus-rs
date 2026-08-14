//! The connection actor.
//!
//! One TCP connection carries many concurrent requests, so the socket is owned
//! by background tasks rather than by the caller: a writer task drains a
//! **bounded** channel — so backpressure is real and a runaway caller cannot
//! queue unbounded memory — and a reader task matches each response to the
//! `oneshot` waiting for it, keyed by request id.
//!
//! Cancellation is protocol-level. Dropping the future returned by [`Connection::invoke`]
//! sends the protocol's cancellation message, because a client-side-only
//! timeout leaves the proxy doing work nobody will read.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use prost::Message;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::bus::packet::{Packet, PacketFlags, PacketType};
use crate::bus::{Bus, BusReader, BusWriter};
use crate::error::{Error, Result};
use crate::guid::Guid;
use crate::proto;
use crate::rpc::{self, ResponseMessage};

/// How many outbound messages may be queued before senders wait.
const OUTBOUND_QUEUE: usize = 64;

/// The callers waiting for responses, and whether the connection is still
/// usable.
///
/// The two live under one lock on purpose. A caller registers itself and the
/// reader task declares the connection dead; if those could interleave, a
/// caller could register just after the reader cleared the map and then wait
/// for a response no one will ever deliver.
#[derive(Debug, Default)]
struct Waiters {
    closed: bool,
    by_request: HashMap<Guid, oneshot::Sender<ResponseMessage>>,
}

impl Waiters {
    /// Marks the connection dead and wakes everyone waiting on it. Dropping the
    /// senders is what turns a lost connection into an error for each caller
    /// rather than a hang.
    fn close(&mut self) {
        self.closed = true;
        self.by_request.clear();
    }
}

type Pending = Arc<Mutex<Waiters>>;

/// A live connection to one RPC proxy.
#[derive(Debug)]
pub struct Connection {
    outbound: mpsc::Sender<Packet>,
    pending: Pending,
    address: String,
    token: Option<String>,
    closed: Arc<AtomicBool>,
}

impl Connection {
    /// Connects to a proxy and starts the reader and writer tasks.
    pub async fn connect(address: &str, token: Option<String>) -> Result<Self> {
        let bus = Bus::connect(address).await?;
        Ok(Self::from_bus(bus, address.to_owned(), token))
    }

    fn from_bus(bus: Bus, address: String, token: Option<String>) -> Self {
        let Bus { reader, writer, .. } = bus;
        let pending: Pending = Arc::default();
        let closed = Arc::new(AtomicBool::new(false));
        let (outbound, outbound_receiver) = mpsc::channel(OUTBOUND_QUEUE);

        tokio::spawn(write_loop(
            writer,
            outbound_receiver,
            Arc::clone(&pending),
            Arc::clone(&closed),
        ));
        tokio::spawn(read_loop(reader, Arc::clone(&pending), Arc::clone(&closed)));

        Self {
            outbound,
            pending,
            address,
            token,
            closed,
        }
    }

    /// The address this connection was opened to.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Whether the connection has failed or been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Calls one method and waits for its response.
    ///
    /// The timeout is sent to the server in the request header *and* applied
    /// locally, so the two agree: a local-only timeout would leave the proxy
    /// working, and a server-only one would leave the caller waiting if the
    /// connection stalled.
    pub async fn invoke<Response: Message + Default>(
        &self,
        method: &str,
        body: &impl Message,
        attachments: Vec<Bytes>,
        timeout: Option<std::time::Duration>,
        response_name: &'static str,
    ) -> Result<(Response, Vec<Bytes>)> {
        let response = self
            .invoke_raw(rpc::API_SERVICE, method, body, attachments, timeout, None)
            .await?;
        let decoded = response.decode_body::<Response>(response_name)?;
        Ok((decoded, response.attachments))
    }

    /// Calls one method, returning the whole response message.
    pub async fn invoke_raw(
        &self,
        service: &str,
        method: &str,
        body: &impl Message,
        attachments: Vec<Bytes>,
        timeout: Option<std::time::Duration>,
        mutation_id: Option<Guid>,
    ) -> Result<ResponseMessage> {
        let mut builder = rpc::RequestHeaderBuilder::new(service, method);
        builder.timeout = timeout;
        builder.mutation_id = mutation_id;
        let request_id = builder.request_id;
        let header = builder.build();

        // The deadline covers the whole call, not just the wait for a reply.
        // Queuing the request can block too — the outbound channel is bounded,
        // and a peer that stops reading backs the writer up until it is full —
        // so a deadline applied only to the reply would be no deadline at all
        // in exactly the case a caller most needs one.
        let deadline = timeout.map(|limit| tokio::time::Instant::now() + limit);
        let timed_out = || Error::Timeout {
            service: service.to_owned(),
            method: method.to_owned(),
            timeout: timeout.unwrap_or_default(),
        };

        let (sender, receiver) = oneshot::channel();
        {
            let mut waiters = self.pending.lock().await;
            // Checked under the same lock the reader closes with, so a
            // connection that has already died fails the call here instead of
            // parking it for ever.
            if waiters.closed {
                return Err(Error::ConnectionClosed { request_id });
            }
            waiters.by_request.insert(request_id, sender);
        }

        // Armed from here on. However this function leaves — returning, or the
        // caller dropping the future part-way — the guard removes the pending
        // entry and, once the request has actually been queued, tells the
        // server to stop working on a result nobody will read.
        let mut guard = PendingGuard {
            pending: Arc::clone(&self.pending),
            outbound: self.outbound.clone(),
            request_id,
            service: service.to_owned(),
            method: method.to_owned(),
            completed: false,
            sent: false,
        };

        let parts = rpc::encode_request(&header, self.token.as_deref(), body, attachments);
        let packet = Packet::message(Guid::random(), parts, PacketFlags::NONE);
        let queued = match deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline, self.outbound.send(packet)).await {
                    Ok(queued) => queued,
                    // Never queued, so there is nothing for the server to cancel;
                    // the guard still removes the pending entry.
                    Err(_) => return Err(timed_out()),
                }
            }
            None => self.outbound.send(packet).await,
        };
        if queued.is_err() {
            guard.complete();
            return Err(Error::ConnectionClosed { request_id });
        }
        guard.sent = true;

        let response = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, receiver).await {
                Ok(received) => received,
                // Dropping the guard sends the cancellation, so the timeout
                // path needs nothing of its own.
                Err(_) => return Err(timed_out()),
            },
            None => receiver.await,
        };

        let response = match response {
            Ok(response) => response,
            // The sender was dropped, which only happens when the reader task
            // ended — the connection is gone, and there is nothing to cancel.
            Err(_) => {
                guard.complete();
                return Err(Error::ConnectionClosed { request_id });
            }
        };
        // The answer is in hand: nothing to remove and nothing to cancel.
        guard.complete();

        if let Some(error) = response.error() {
            return Err(Error::response(service, method, error));
        }
        Ok(response)
    }
}

/// Cleans up after an in-flight request however its future ends — including
/// when the caller drops it part-way.
///
/// Two jobs. It removes the entry from the pending map, or the map grows
/// without bound on a long-lived connection. And it sends the protocol's
/// cancellation, because **cancellation is protocol-level**: a client that
/// merely stops waiting leaves the proxy computing a result nobody will read,
/// which is exactly the cost this crate exists to avoid.
///
/// Stood down once the response is in hand, since there is then nothing to
/// remove and nothing to cancel.
struct PendingGuard {
    pending: Pending,
    outbound: mpsc::Sender<Packet>,
    request_id: Guid,
    service: String,
    method: String,
    /// The call finished on its own; no cleanup is owed.
    completed: bool,
    /// The request reached the outbound queue, so the server may be working on
    /// it. A request that never got that far has nothing to cancel, and saying
    /// otherwise would send a cancellation for a request id the server has
    /// never seen.
    sent: bool,
}

impl PendingGuard {
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let pending = Arc::clone(&self.pending);
        let request_id = self.request_id;
        // `Drop` cannot await, so the removal is handed to the runtime. It is
        // idempotent, so racing with the reader task is harmless.
        tokio::spawn(async move {
            pending.lock().await.by_request.remove(&request_id);
        });

        if !self.sent {
            return;
        }

        // Best-effort and non-blocking: if the queue is full or the connection
        // is gone there is nothing useful to do, and dropping a future must not
        // block.
        let parts = rpc::encode_cancelation(request_id, &self.service, &self.method);
        let _ = self
            .outbound
            .try_send(Packet::message(Guid::random(), parts, PacketFlags::NONE));
    }
}

async fn write_loop(
    mut writer: BusWriter,
    mut outbound: mpsc::Receiver<Packet>,
    pending: Pending,
    closed: Arc<AtomicBool>,
) {
    while let Some(packet) = outbound.recv().await {
        if writer.send(&packet).await.is_err() {
            break;
        }
    }
    closed.store(true, Ordering::Relaxed);
    pending.lock().await.close();
    let _ = writer.shutdown().await;
}

async fn read_loop(mut reader: BusReader, pending: Pending, closed: Arc<AtomicBool>) {
    loop {
        let packet = match reader.receive().await {
            Ok(packet) => packet,
            Err(_) => break,
        };

        // Acks carry no payload and are only interesting when delivery
        // tracking was requested, which this client does not request.
        if packet.packet_type != PacketType::Message {
            continue;
        }

        let Ok(response) = rpc::decode_response(packet.parts) else {
            // A message that is not a response cannot be routed to anyone, and
            // the connection is still usable for the requests that are.
            continue;
        };
        let Some(request_id) = response.request_id() else {
            continue;
        };
        if let Some(sender) = pending.lock().await.by_request.remove(&request_id) {
            let _ = sender.send(response);
        }
    }

    closed.store(true, Ordering::Relaxed);
    // The reader is what delivers every response, so once it stops the
    // connection is finished: waiters are woken with an error, and later calls
    // are refused rather than parked for ever.
    pending.lock().await.close();
}

/// Asks a proxy for the current set of RPC proxies.
///
/// This is the RPC `DiscoveryService`, not the HTTP `discover_proxies`
/// command; it needs an already-connected proxy, so it refreshes a proxy list
/// rather than bootstrapping one. See `docs/rpc-compatibility.md`.
pub async fn discover_proxies(
    connection: &Connection,
    role: Option<&str>,
    timeout: Option<std::time::Duration>,
) -> Result<Vec<String>> {
    let request = proto::api::TReqDiscoverProxies {
        role: role.map(str::to_owned),
        ..Default::default()
    };
    let response = connection
        .invoke_raw(
            rpc::DISCOVERY_SERVICE,
            "DiscoverProxies",
            &request,
            Vec::new(),
            timeout,
            None,
        )
        .await?;
    let decoded = response.decode_body::<proto::api::TRspDiscoverProxies>("TRspDiscoverProxies")?;
    Ok(decoded.addresses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::packet;
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A stub proxy: completes the handshake, then answers each request through
    /// the supplied closure. Enough to test routing, cancellation and
    /// connection loss without a cluster.
    ///
    /// Dropping it really does drop the connection. The accepted socket is
    /// owned by the spawned task, not by this struct, so without the explicit
    /// abort the socket would stay open after the stub went out of scope and a
    /// test waiting for the connection to fail would wait for ever.
    struct StubProxy {
        address: String,
        seen: mpsc::UnboundedReceiver<Packet>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for StubProxy {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn stub_proxy(
        answer: impl Fn(&proto::rpc::TRequestHeader) -> Option<Vec<Option<Bytes>>> + Send + 'static,
    ) -> StubProxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (seen_sender, seen) = mpsc::unbounded_channel();

        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut read_half, mut write_half) = stream.into_split();
            let mut buffer = BytesMut::new();
            let mut handshaken = false;

            loop {
                let decoded = packet::decode(&mut buffer, crate::bus::DEFAULT_MAX_MESSAGE_SIZE);
                match decoded {
                    Ok(Some(request)) => {
                        if !handshaken {
                            handshaken = true;
                            let handshake = proto::bus::THandshake {
                                connection_id: Guid::random().to_proto(),
                                encryption_mode: Some(0),
                                ..Default::default()
                            };
                            let mut part = Vec::new();
                            part.extend_from_slice(&crate::bus::HANDSHAKE_SIGNATURE.to_le_bytes());
                            handshake.encode(&mut part).unwrap();
                            let reply = Packet::message(
                                request.id,
                                vec![Some(Bytes::from(part))],
                                PacketFlags::NONE,
                            );
                            let mut out = BytesMut::new();
                            packet::encode(&reply, &mut out);
                            if write_half.write_all(&out).await.is_err() {
                                return;
                            }
                            continue;
                        }

                        let header_part = request.parts[0].as_ref().unwrap().clone();
                        let header = proto::rpc::TRequestHeader::decode(&header_part[4..]).unwrap();
                        let _ = seen_sender.send(request.clone());

                        if let Some(parts) = answer(&header) {
                            let reply = Packet::message(Guid::random(), parts, PacketFlags::NONE);
                            let mut out = BytesMut::new();
                            packet::encode(&reply, &mut out);
                            if write_half.write_all(&out).await.is_err() {
                                return;
                            }
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

    fn success_reply(request_id: Guid, body: &impl Message) -> Vec<Option<Bytes>> {
        let header = proto::rpc::TResponseHeader {
            request_id: Some(request_id.to_proto()),
            ..Default::default()
        };
        let mut header_part = Vec::new();
        header_part.extend_from_slice(&(rpc::MessageType::Response as u32).to_le_bytes());
        header.encode(&mut header_part).unwrap();
        vec![
            Some(Bytes::from(header_part)),
            Some(Bytes::from(body.encode_to_vec())),
        ]
    }

    fn error_reply(request_id: Guid, code: i32, message: &str) -> Vec<Option<Bytes>> {
        let header = proto::rpc::TResponseHeader {
            request_id: Some(request_id.to_proto()),
            error: Some(proto::misc::TError {
                code,
                message: Some(message.to_owned()),
                attributes: None,
                inner_errors: vec![],
            }),
            ..Default::default()
        };
        let mut header_part = Vec::new();
        header_part.extend_from_slice(&(rpc::MessageType::Response as u32).to_le_bytes());
        header.encode(&mut header_part).unwrap();
        vec![Some(Bytes::from(header_part))]
    }

    #[tokio::test]
    async fn a_call_gets_its_own_response() {
        let stub = stub_proxy(|header| {
            let request_id = Guid::from_proto(header.request_id.as_ref().unwrap());
            Some(success_reply(
                request_id,
                &proto::api::TRspPingTransaction::default(),
            ))
        })
        .await;

        let connection = Connection::connect(&stub.address, None).await.unwrap();
        let (_response, attachments) = connection
            .invoke::<proto::api::TRspPingTransaction>(
                "PingTransaction",
                &proto::api::TReqPingTransaction {
                    transaction_id: Guid::random().to_proto(),
                    ..Default::default()
                },
                Vec::new(),
                None,
                "TRspPingTransaction",
            )
            .await
            .unwrap();
        assert!(attachments.is_empty());
    }

    /// The point of the actor: several requests in flight on one connection,
    /// answered out of order, each reaching its own caller.
    #[tokio::test]
    async fn concurrent_requests_are_routed_by_request_id() {
        let stub = stub_proxy(|header| {
            let request_id = Guid::from_proto(header.request_id.as_ref().unwrap());
            // Echo the method name back inside the response so each caller can
            // check it got *its* answer.
            Some(success_reply(
                request_id,
                &proto::api::TRspGetNode {
                    value: header.method.clone().into_bytes(),
                },
            ))
        })
        .await;

        let connection = Arc::new(Connection::connect(&stub.address, None).await.unwrap());
        let methods = ["GetNode", "ListNode", "ExistsNode", "SetNode"];
        let mut handles = Vec::new();
        for method in methods {
            let connection = Arc::clone(&connection);
            handles.push(tokio::spawn(async move {
                connection
                    .invoke::<proto::api::TRspGetNode>(
                        method,
                        &proto::api::TReqGetNode::default(),
                        Vec::new(),
                        None,
                        "TRspGetNode",
                    )
                    .await
                    .map(|(response, _)| String::from_utf8(response.value).unwrap())
            }));
        }

        for (method, handle) in methods.iter().zip(handles) {
            assert_eq!(&handle.await.unwrap().unwrap(), method);
        }
    }

    #[tokio::test]
    async fn a_server_error_becomes_a_rust_error_with_its_code() {
        let stub = stub_proxy(|header| {
            let request_id = Guid::from_proto(header.request_id.as_ref().unwrap());
            Some(error_reply(
                request_id,
                crate::error::codes::NO_SUCH_TRANSACTION,
                "no such transaction",
            ))
        })
        .await;

        let connection = Connection::connect(&stub.address, None).await.unwrap();
        let error = connection
            .invoke::<proto::api::TRspPingTransaction>(
                "PingTransaction",
                &proto::api::TReqPingTransaction {
                    transaction_id: Guid::random().to_proto(),
                    ..Default::default()
                },
                Vec::new(),
                None,
                "TRspPingTransaction",
            )
            .await
            .unwrap_err();

        assert!(error.has_code(crate::error::codes::NO_SUCH_TRANSACTION));
        assert!(
            error
                .to_string()
                .contains("ApiService.PingTransaction failed")
        );
    }

    #[tokio::test]
    async fn a_timeout_reports_the_method_and_cancels_the_request() {
        // Never answers, so the local timeout is what ends the call.
        let mut stub = stub_proxy(|_| None).await;
        let connection = Connection::connect(&stub.address, None).await.unwrap();

        let error = connection
            .invoke::<proto::api::TRspPingTransaction>(
                "PingTransaction",
                &proto::api::TReqPingTransaction {
                    transaction_id: Guid::random().to_proto(),
                    ..Default::default()
                },
                Vec::new(),
                Some(std::time::Duration::from_millis(50)),
                "TRspPingTransaction",
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Timeout { .. }), "got {error}");

        // The request, then the cancellation for it.
        let request = stub.seen.recv().await.unwrap();
        let header_part = request.parts[0].as_ref().unwrap();
        assert_eq!(&header_part[0..4], b"rpci");
        let header = proto::rpc::TRequestHeader::decode(&header_part[4..]).unwrap();
        let request_id = Guid::from_proto(header.request_id.as_ref().unwrap());
        // The header carries the deadline too, so the server stops on its own
        // even if the cancellation is lost.
        assert_eq!(header.timeout, Some(50_000));

        let cancelation = stub.seen.recv().await.expect("a cancellation must follow");
        let part = cancelation.parts[0].as_ref().unwrap();
        assert_eq!(&part[0..4], b"rpcc", "cancellation is an rpcc message");
        let cancel_header = proto::rpc::TRequestCancelationHeader::decode(&part[4..]).unwrap();
        assert_eq!(Guid::from_proto(&cancel_header.request_id), request_id);
    }

    #[tokio::test]
    async fn a_dropped_connection_fails_the_calls_in_flight() {
        let stub = stub_proxy(|_| None).await;
        let connection = Connection::connect(&stub.address, None).await.unwrap();

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

        // Killing the stub closes the socket, which must wake the caller with
        // an error rather than leaving it parked forever.
        drop(stub);
        let error = call.await.unwrap_err();
        assert!(
            matches!(error, Error::ConnectionClosed { .. }),
            "got {error}"
        );
    }

    /// Dropping a call must tell the server to stop, not just stop listening.
    /// A client that only stops waiting leaves the proxy computing a result
    /// nobody will read — the exact cost this crate exists to avoid — which is
    /// why `TRequestHeader` has an `uncancelable` flag at all.
    #[tokio::test]
    async fn dropping_a_call_cancels_it_on_the_wire() {
        let mut stub = stub_proxy(|_| None).await;
        let connection = Connection::connect(&stub.address, None).await.unwrap();

        let request = proto::api::TReqSelectRows {
            query: "* from [//tmp/t]".to_owned(),
            ..Default::default()
        };
        {
            let call = connection.invoke::<proto::api::TRspSelectRows>(
                "SelectRows",
                &request,
                Vec::new(),
                // No timeout: the drop is what has to do the cancelling.
                None,
                "TRspSelectRows",
            );
            let _ = tokio::time::timeout(std::time::Duration::from_millis(50), call).await;
        }

        let sent = stub.seen.recv().await.expect("the request");
        let header_part = sent.parts[0].as_ref().unwrap();
        assert_eq!(&header_part[0..4], b"rpci");
        let header = proto::rpc::TRequestHeader::decode(&header_part[4..]).unwrap();
        let request_id = Guid::from_proto(header.request_id.as_ref().unwrap());

        let cancelation = stub
            .seen
            .recv()
            .await
            .expect("dropping the future must send a cancellation");
        let part = cancelation.parts[0].as_ref().unwrap();
        assert_eq!(&part[0..4], b"rpcc");
        let cancel_header = proto::rpc::TRequestCancelationHeader::decode(&part[4..]).unwrap();
        assert_eq!(Guid::from_proto(&cancel_header.request_id), request_id);
        assert_eq!(cancel_header.method, "SelectRows");
    }

    /// A completed call must NOT be cancelled: the answer is already in hand,
    /// and a stray cancellation for a finished request is noise on the wire.
    #[tokio::test]
    async fn a_completed_call_sends_no_cancellation() {
        let mut stub = stub_proxy(|header| {
            let request_id = Guid::from_proto(header.request_id.as_ref().unwrap());
            Some(success_reply(
                request_id,
                &proto::api::TRspPingTransaction::default(),
            ))
        })
        .await;

        let connection = Connection::connect(&stub.address, None).await.unwrap();
        let request = proto::api::TReqPingTransaction {
            transaction_id: Guid::random().to_proto(),
            ..Default::default()
        };
        connection
            .invoke::<proto::api::TRspPingTransaction>(
                "PingTransaction",
                &request,
                Vec::new(),
                None,
                "TRspPingTransaction",
            )
            .await
            .unwrap();

        let _request = stub.seen.recv().await.expect("the request");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            stub.seen.try_recv().is_err(),
            "a completed call must not be followed by a cancellation"
        );
    }

    #[tokio::test]
    async fn the_pending_map_does_not_leak_when_a_call_is_dropped() {
        let stub = stub_proxy(|_| None).await;
        let connection = Connection::connect(&stub.address, None).await.unwrap();

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
            // Give it long enough to register, then abandon it.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(50), call).await;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            connection.pending.lock().await.by_request.is_empty(),
            "a dropped call left its entry in the pending map"
        );
    }
}
