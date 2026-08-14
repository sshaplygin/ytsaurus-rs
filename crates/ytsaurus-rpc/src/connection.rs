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

type Pending = Arc<Mutex<HashMap<Guid, oneshot::Sender<ResponseMessage>>>>;

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

        tokio::spawn(write_loop(writer, outbound_receiver, Arc::clone(&closed)));
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

        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(request_id, sender);

        // From here on every exit path must remove the entry, or the map grows
        // without bound on a connection that outlives many failures.
        let guard = PendingGuard {
            pending: Arc::clone(&self.pending),
            request_id,
            armed: true,
        };

        let parts = rpc::encode_request(&header, self.token.as_deref(), body, attachments);
        let packet = Packet::message(Guid::random(), parts, PacketFlags::NONE);
        if self.outbound.send(packet).await.is_err() {
            return Err(Error::ConnectionClosed { request_id });
        }

        let response = match timeout {
            Some(limit) => match tokio::time::timeout(limit, receiver).await {
                Ok(received) => received,
                Err(_) => {
                    // The server is still working on it; tell it to stop.
                    self.cancel(request_id, service, method);
                    return Err(Error::Timeout {
                        service: service.to_owned(),
                        method: method.to_owned(),
                        timeout: limit,
                    });
                }
            },
            None => receiver.await,
        };

        let response = match response {
            Ok(response) => response,
            // The sender was dropped, which only happens when the reader task
            // ended — the connection is gone.
            Err(_) => return Err(Error::ConnectionClosed { request_id }),
        };
        drop(guard);

        if let Some(error) = response.error() {
            return Err(Error::Response {
                service: service.to_owned(),
                method: method.to_owned(),
                error,
            });
        }
        Ok(response)
    }

    /// Sends a protocol-level cancellation for an in-flight request.
    ///
    /// Best-effort and non-blocking: if the outbound queue is full or the
    /// connection is gone there is nothing useful to do, and the caller is
    /// already on an error path.
    fn cancel(&self, request_id: Guid, service: &str, method: &str) {
        let parts = rpc::encode_cancelation(request_id, service, method);
        let packet = Packet::message(Guid::random(), parts, PacketFlags::NONE);
        let _ = self.outbound.try_send(packet);
    }
}

/// Removes a request from the pending map however its future ends, including
/// when it is dropped part-way.
struct PendingGuard {
    pending: Pending,
    request_id: Guid,
    armed: bool,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let pending = Arc::clone(&self.pending);
        let request_id = self.request_id;
        // `Drop` cannot await, so the removal is handed to the runtime. It is
        // idempotent, so racing with the reader task is harmless.
        tokio::spawn(async move {
            pending.lock().await.remove(&request_id);
        });
    }
}

async fn write_loop(
    mut writer: BusWriter,
    mut outbound: mpsc::Receiver<Packet>,
    closed: Arc<AtomicBool>,
) {
    while let Some(packet) = outbound.recv().await {
        if writer.send(&packet).await.is_err() {
            break;
        }
    }
    closed.store(true, Ordering::Relaxed);
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
        if let Some(sender) = pending.lock().await.remove(&request_id) {
            let _ = sender.send(response);
        }
    }

    closed.store(true, Ordering::Relaxed);
    // Waking every waiter is what turns a dropped connection into an error for
    // each caller instead of a hang: dropping the senders resolves their
    // `oneshot`s with a receive error.
    pending.lock().await.clear();
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
            connection.pending.lock().await.is_empty(),
            "a dropped call left its entry in the pending map"
        );
    }
}
