//! Layer 2: the RPC envelope that rides inside a bus message.
//!
//! A bus message is a list of parts, and the RPC layer gives them meaning:
//!
//! ```text
//!   part 0   u32 message type, then the serialized TRequestHeader
//!   part 1   the serialized request body (a TReq* message)
//!   part 2+  attachments
//! ```
//!
//! and symmetrically for a response, whose part 0 carries a `TResponseHeader`.
//! The 4-byte type word is `TFixedMessageHeader` in
//! `yt/yt/core/rpc/message.cpp`, declared under `#pragma pack(push, 1)` around
//! a single `ui32`, so it is exactly four little-endian bytes with nothing
//! after it but the protobuf.
//!
//! Sans-io, like [`crate::bus::packet`]: these are functions from parts to
//! parts, and the connection actor is the only thing that touches a socket.

use bytes::Bytes;
use prost::Message;

use crate::error::{Error, Result, YtError};
use crate::guid::Guid;
use crate::proto;

/// The RPC service this crate speaks to — `api_service_proxy.h`.
pub const API_SERVICE: &str = "ApiService";

/// The discovery service, which runs alongside the API service on a proxy.
pub const DISCOVERY_SERVICE: &str = "DiscoveryService";

/// `ProtocolVersionMajor` for `ApiService` —
/// `yt/go/yt/internal/rpcclient/rpc_proxy.go`.
pub const PROTOCOL_VERSION_MAJOR: i32 = 1;

/// The major protocol version of every other service on the proxy.
///
/// The version is **per service**, not per connection: the C++ takes it from
/// the service descriptor (`client.cpp` sets `protocol_version_major` from
/// `serviceDescriptor.ProtocolVersion.Major`), and `ApiService` is the only one
/// that declares a major version of 1. `DiscoveryService` is still at 0, and
/// announcing 1 to it earns a flat refusal from a real proxy — "Server major
/// protocol version differs from client major protocol version" — which is how
/// it was found here.
pub const DEFAULT_PROTOCOL_VERSION_MAJOR: i32 = 0;

/// The major protocol version to announce when calling `service`.
pub fn protocol_version_major(service: &str) -> i32 {
    match service {
        API_SERVICE => PROTOCOL_VERSION_MAJOR,
        _ => DEFAULT_PROTOCOL_VERSION_MAJOR,
    }
}

/// `ECodec::None` — `yt/yt/core/compression/public.h`.
///
/// The header must say which codec the body and attachments use. This crate
/// only implements the identity codec, so it always says `None`; the field is
/// still set explicitly, because leaving it out puts the request into the
/// legacy-codec path (`EnableLegacyRpcCodecs`) where the body would need a
/// serialization envelope instead.
pub const CODEC_NONE: i32 = 0;

/// `EMessageType` — `yt/yt/core/rpc/message.h`. The values spell "rpci",
/// "rpcc" and "rpco" when written little-endian.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum MessageType {
    Request = 0x6963_7072,
    RequestCancelation = 0x6363_7072,
    Response = 0x6f63_7072,
}

impl MessageType {
    fn from_wire(value: u32) -> Option<Self> {
        match value {
            0x6963_7072 => Some(Self::Request),
            0x6363_7072 => Some(Self::RequestCancelation),
            0x6f63_7072 => Some(Self::Response),
            _ => None,
        }
    }
}

/// The protobuf field number of the `TCredentialsExt` extension of
/// `TRequestHeader` — `yt/yt_proto/yt/core/rpc/proto/rpc.proto`.
///
/// `prost` does not generate proto2 extensions, so the field is appended by
/// hand. That is wire-identical: an extension is an ordinary field with a
/// reserved number, and protobuf does not care in what order fields appear.
const CREDENTIALS_EXT_FIELD: u32 = 110;

/// Builds the `TRequestHeader` for one call.
#[derive(Debug, Clone)]
pub struct RequestHeaderBuilder {
    pub request_id: Guid,
    pub service: String,
    pub method: String,
    pub timeout: Option<std::time::Duration>,
    pub mutation_id: Option<Guid>,
    pub retry: bool,
    pub user: Option<String>,
    pub token: Option<String>,
}

impl RequestHeaderBuilder {
    pub fn new(service: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            request_id: Guid::random(),
            service: service.into(),
            method: method.into(),
            timeout: None,
            mutation_id: None,
            retry: false,
            user: None,
            token: None,
        }
    }

    /// The protobuf header, without the credentials extension — that is added
    /// by [`encode_request`], which is the only place that can append it after
    /// serialization.
    pub fn build(&self) -> proto::rpc::TRequestHeader {
        proto::rpc::TRequestHeader {
            request_id: Some(self.request_id.to_proto()),
            service: self.service.clone(),
            method: self.method.clone(),
            protocol_version_major: Some(protocol_version_major(&self.service)),
            // Microseconds: `TRequestHeader.timeout` is a TDuration, and
            // YTsaurus durations are microsecond counts.
            timeout: self.timeout.map(|timeout| timeout.as_micros() as i64),
            mutation_id: self.mutation_id.map(Guid::to_proto),
            retry: Some(self.retry),
            user: self.user.clone(),
            request_codec: Some(CODEC_NONE),
            response_codec: Some(CODEC_NONE),
            ..Default::default()
        }
    }
}

/// Appends a length-delimited protobuf field by number.
///
/// Used for the extension fields `prost` will not generate.
fn append_length_delimited_field(buffer: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    prost::encoding::encode_key(
        field_number,
        prost::encoding::WireType::LengthDelimited,
        buffer,
    );
    prost::encoding::encode_varint(payload.len() as u64, buffer);
    buffer.extend_from_slice(payload);
}

/// Serializes part 0 of a message: the type word, then the header protobuf.
fn encode_header_part(
    message_type: MessageType,
    header: &impl Message,
    token: Option<&str>,
) -> Bytes {
    let mut buffer = Vec::with_capacity(4 + header.encoded_len());
    buffer.extend_from_slice(&(message_type as u32).to_le_bytes());
    header
        .encode(&mut buffer)
        .expect("a Vec never runs out of room");

    if let Some(token) = token {
        let credentials = proto::rpc::TCredentialsExt {
            token: Some(token.to_owned()),
            ..Default::default()
        };
        append_length_delimited_field(
            &mut buffer,
            CREDENTIALS_EXT_FIELD,
            &credentials.encode_to_vec(),
        );
    }

    Bytes::from(buffer)
}

/// Builds the bus parts for one request.
///
/// The body and the attachments are written as they are: with the codec set to
/// `None`, "compressing" is the identity, which is what
/// `yt/go/bus/client.go` does through `compression.NewCodec(CodecIDNone)`.
pub fn encode_request(
    header: &proto::rpc::TRequestHeader,
    token: Option<&str>,
    body: &impl Message,
    attachments: Vec<Bytes>,
) -> Vec<Option<Bytes>> {
    let mut parts = Vec::with_capacity(2 + attachments.len());
    parts.push(Some(encode_header_part(
        MessageType::Request,
        header,
        token,
    )));
    parts.push(Some(Bytes::from(body.encode_to_vec())));
    parts.extend(attachments.into_iter().map(Some));
    parts
}

/// Builds the bus parts for a cancellation.
///
/// One part, as `CreateRequestCancelationMessage` in
/// `yt/yt/core/rpc/message.cpp` builds it. Dropping a future has to send this
/// or the proxy keeps working on a result nobody will read.
pub fn encode_cancelation(request_id: Guid, service: &str, method: &str) -> Vec<Option<Bytes>> {
    let header = proto::rpc::TRequestCancelationHeader {
        request_id: request_id.to_proto(),
        service: service.to_owned(),
        method: method.to_owned(),
        realm_id: None,
    };
    vec![Some(encode_header_part(
        MessageType::RequestCancelation,
        &header,
        None,
    ))]
}

/// A response, taken apart.
#[derive(Debug, Clone)]
pub struct ResponseMessage {
    pub header: proto::rpc::TResponseHeader,
    pub body: Option<Bytes>,
    pub attachments: Vec<Bytes>,
}

impl ResponseMessage {
    /// The request this response answers.
    pub fn request_id(&self) -> Option<Guid> {
        self.header.request_id.as_ref().map(Guid::from_proto)
    }

    /// The server-reported failure, if there is one.
    ///
    /// `TResponseHeader.error` is optional and "if omitted then OK is assumed"
    /// — and an error with code 0 is also success, which is why this checks the
    /// code rather than the presence of the field. `NewErrorFromProto` in
    /// `yt/go/proto/core/misc/convert.go` makes the same test.
    pub fn error(&self) -> Option<YtError> {
        let error = self.header.error.as_ref()?;
        let converted = YtError::from_proto(error);
        (converted.code != crate::error::codes::OK).then_some(converted)
    }

    /// Decodes the body into `T`.
    pub fn decode_body<T: Message + Default>(&self, message_name: &'static str) -> Result<T> {
        let body = self.body.as_ref().ok_or_else(|| {
            Error::Protocol(format!("response to {message_name} has no body part"))
        })?;
        T::decode(body.clone()).map_err(|source| Error::Decode {
            message: message_name,
            source,
        })
    }
}

/// Takes apart the parts of a received message.
///
/// Rejects anything that is not a response, which is what the Go client does —
/// it warns and ignores. Here it is an error, because the connection actor
/// routes by request id and has nowhere to put a message it cannot classify.
pub fn decode_response(parts: Vec<Option<Bytes>>) -> Result<ResponseMessage> {
    let mut parts = parts.into_iter();
    let header_part = parts
        .next()
        .flatten()
        .ok_or_else(|| Error::Protocol("message has no header part".to_owned()))?;

    if header_part.len() < 4 {
        return Err(Error::Protocol(format!(
            "message header part is {} bytes, too short for a message type",
            header_part.len()
        )));
    }
    let raw_type = u32::from_le_bytes(header_part[0..4].try_into().unwrap());
    match MessageType::from_wire(raw_type) {
        Some(MessageType::Response) => {}
        Some(other) => {
            return Err(Error::Protocol(format!(
                "expected a response message, got {other:?}"
            )));
        }
        None => {
            return Err(Error::Protocol(format!(
                "unknown RPC message type {raw_type:#010x}"
            )));
        }
    }

    let header =
        proto::rpc::TResponseHeader::decode(&header_part[4..]).map_err(|source| Error::Decode {
            message: "TResponseHeader",
            source,
        })?;

    // An error response carries no body, so a missing part 1 is not itself a
    // fault; `decode_body` complains only when a body is actually wanted.
    let body = parts.next().flatten();
    let attachments = parts.flatten().collect();

    Ok(ResponseMessage {
        header,
        body,
        attachments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The proxy refuses a call that announces the wrong major version, and the
    /// right version depends on which service is being called — not on the
    /// connection. Getting this wrong fails every call to that service.
    /// Numbers the server matches on, written out.
    #[test]
    fn the_wire_constants_are_the_documented_ones() {
        assert_eq!(
            CREDENTIALS_EXT_FIELD, 110,
            "TCredentialsExt is extension 110"
        );
        assert_eq!(CODEC_NONE, 0, "ECodec::None is 0");
        assert_eq!(PROTOCOL_VERSION_MAJOR, 1);
        assert_eq!(DEFAULT_PROTOCOL_VERSION_MAJOR, 0);
        assert_eq!(API_SERVICE, "ApiService");
        assert_eq!(DISCOVERY_SERVICE, "DiscoveryService");
    }

    #[test]
    fn the_protocol_version_is_per_service() {
        assert_eq!(protocol_version_major(API_SERVICE), 1);
        assert_eq!(protocol_version_major(DISCOVERY_SERVICE), 0);

        let header = RequestHeaderBuilder::new(DISCOVERY_SERVICE, "DiscoverProxies").build();
        assert_eq!(header.protocol_version_major, Some(0));
        let header = RequestHeaderBuilder::new(API_SERVICE, "LookupRows").build();
        assert_eq!(header.protocol_version_major, Some(1));
    }

    #[test]
    fn message_type_words_spell_rpci_rpcc_and_rpco() {
        // The C++ comments name the spelling; this asserts the byte order that
        // makes it true, which is the part an implementation gets wrong.
        assert_eq!(&(MessageType::Request as u32).to_le_bytes(), b"rpci");
        assert_eq!(
            &(MessageType::RequestCancelation as u32).to_le_bytes(),
            b"rpcc"
        );
        assert_eq!(&(MessageType::Response as u32).to_le_bytes(), b"rpco");
    }

    #[test]
    fn a_request_has_a_header_part_a_body_part_and_then_attachments() {
        let header = RequestHeaderBuilder::new(API_SERVICE, "LookupRows").build();
        let body = proto::api::TReqLookupRows::default();
        let parts = encode_request(
            &header,
            None,
            &body,
            vec![Bytes::from_static(b"rowset"), Bytes::from_static(b"more")],
        );

        assert_eq!(parts.len(), 4);
        let header_part = parts[0].as_ref().unwrap();
        assert_eq!(&header_part[0..4], b"rpci");
        assert_eq!(parts[2].as_ref().unwrap(), &Bytes::from_static(b"rowset"));
        assert_eq!(parts[3].as_ref().unwrap(), &Bytes::from_static(b"more"));
    }

    #[test]
    fn the_header_part_parses_back_as_a_request_header() {
        let built = RequestHeaderBuilder {
            timeout: Some(std::time::Duration::from_secs(30)),
            ..RequestHeaderBuilder::new(API_SERVICE, "StartTransaction")
        };
        let header = built.build();
        let parts = encode_request(
            &header,
            None,
            &proto::api::TReqStartTransaction::default(),
            vec![],
        );
        let header_part = parts[0].as_ref().unwrap();

        let parsed = proto::rpc::TRequestHeader::decode(&header_part[4..]).unwrap();
        assert_eq!(parsed.service, API_SERVICE);
        assert_eq!(parsed.method, "StartTransaction");
        assert_eq!(parsed.protocol_version_major, Some(PROTOCOL_VERSION_MAJOR));
        assert_eq!(parsed.request_codec, Some(CODEC_NONE));
        assert_eq!(parsed.response_codec, Some(CODEC_NONE));
        // Microseconds, not milliseconds and not nanoseconds.
        assert_eq!(parsed.timeout, Some(30_000_000));
        assert_eq!(
            Guid::from_proto(&parsed.request_id.unwrap()),
            built.request_id
        );
    }

    /// The credentials extension is appended by hand because `prost` does not
    /// generate proto2 extensions. This checks the bytes really are field 110,
    /// length-delimited, holding a `TCredentialsExt` — the only way to know the
    /// hand-rolled encoding is right without a server.
    #[test]
    fn the_token_is_appended_as_extension_field_110() {
        let header = RequestHeaderBuilder::new(API_SERVICE, "LookupRows").build();
        let parts = encode_request(
            &header,
            Some("secret-token"),
            &proto::api::TReqLookupRows::default(),
            vec![],
        );
        let header_part = parts[0].as_ref().unwrap();
        let body = &header_part[4..];

        // Field 110, wire type 2 -> key varint (110 << 3) | 2 = 882.
        let mut expected_key = Vec::new();
        prost::encoding::encode_key(
            CREDENTIALS_EXT_FIELD,
            prost::encoding::WireType::LengthDelimited,
            &mut expected_key,
        );
        let key_at = body
            .windows(expected_key.len())
            .position(|window| window == expected_key)
            .expect("the credentials key must be in the header bytes");

        let payload = &body[key_at + expected_key.len()..];
        let (length, rest) = {
            let mut cursor = payload;
            let length = prost::encoding::decode_varint(&mut cursor).unwrap();
            (length as usize, cursor)
        };
        let credentials = proto::rpc::TCredentialsExt::decode(&rest[..length]).unwrap();
        assert_eq!(credentials.token.as_deref(), Some("secret-token"));
    }

    #[test]
    fn no_token_means_no_extension() {
        let header = RequestHeaderBuilder::new(API_SERVICE, "LookupRows").build();
        let with = encode_request(
            &header,
            Some("t"),
            &proto::api::TReqLookupRows::default(),
            vec![],
        );
        let without = encode_request(
            &header,
            None,
            &proto::api::TReqLookupRows::default(),
            vec![],
        );
        assert!(without[0].as_ref().unwrap().len() < with[0].as_ref().unwrap().len());
    }

    fn response_parts(
        header: proto::rpc::TResponseHeader,
        body: Option<&[u8]>,
    ) -> Vec<Option<Bytes>> {
        let mut header_part = Vec::new();
        header_part.extend_from_slice(&(MessageType::Response as u32).to_le_bytes());
        header.encode(&mut header_part).unwrap();
        let mut parts = vec![Some(Bytes::from(header_part))];
        if let Some(body) = body {
            parts.push(Some(Bytes::copy_from_slice(body)));
        }
        parts
    }

    #[test]
    fn a_successful_response_decodes_with_its_body() {
        let request_id = Guid::random();
        let body = proto::api::TRspStartTransaction::default();
        let parts = response_parts(
            proto::rpc::TResponseHeader {
                request_id: Some(request_id.to_proto()),
                ..Default::default()
            },
            Some(&body.encode_to_vec()),
        );

        let response = decode_response(parts).unwrap();
        assert_eq!(response.request_id(), Some(request_id));
        assert!(response.error().is_none());
        response
            .decode_body::<proto::api::TRspStartTransaction>("TRspStartTransaction")
            .unwrap();
    }

    #[test]
    fn an_error_response_surfaces_the_error_and_has_no_body() {
        let parts = response_parts(
            proto::rpc::TResponseHeader {
                request_id: Some(Guid::random().to_proto()),
                error: Some(proto::misc::TError {
                    code: crate::error::codes::RESOLVE_ERROR,
                    message: Some("no such table".to_owned()),
                    attributes: None,
                    inner_errors: vec![],
                }),
                ..Default::default()
            },
            None,
        );

        let response = decode_response(parts).unwrap();
        let error = response.error().expect("the header carries an error");
        assert_eq!(error.code, crate::error::codes::RESOLVE_ERROR);
        assert!(
            response
                .decode_body::<proto::api::TRspLookupRows>("TRspLookupRows")
                .is_err()
        );
    }

    /// "If omitted then OK is assumed" — and a present error with code 0 is
    /// also success. Treating any present `error` field as a failure would turn
    /// good responses into errors.
    #[test]
    fn an_error_with_code_zero_is_success() {
        let parts = response_parts(
            proto::rpc::TResponseHeader {
                request_id: Some(Guid::random().to_proto()),
                error: Some(proto::misc::TError {
                    code: 0,
                    message: Some(String::new()),
                    attributes: None,
                    inner_errors: vec![],
                }),
                ..Default::default()
            },
            Some(&[]),
        );
        assert!(decode_response(parts).unwrap().error().is_none());
    }

    #[test]
    fn a_message_that_is_not_a_response_is_rejected() {
        let mut header_part = Vec::new();
        header_part.extend_from_slice(&(MessageType::Request as u32).to_le_bytes());
        proto::rpc::TRequestHeader {
            service: API_SERVICE.to_owned(),
            method: "LookupRows".to_owned(),
            ..Default::default()
        }
        .encode(&mut header_part)
        .unwrap();

        let error = decode_response(vec![Some(Bytes::from(header_part))]).unwrap_err();
        assert!(error.to_string().contains("expected a response message"));
    }

    #[test]
    fn a_short_or_missing_header_part_is_rejected_not_panicked_on() {
        assert!(decode_response(vec![]).is_err());
        assert!(decode_response(vec![None]).is_err());
        assert!(decode_response(vec![Some(Bytes::from_static(b"rp"))]).is_err());
        assert!(decode_response(vec![Some(Bytes::from_static(b"nope"))]).is_err());
    }

    #[test]
    fn cancelation_is_one_part_naming_the_request() {
        let request_id = Guid::random();
        let parts = encode_cancelation(request_id, API_SERVICE, "SelectRows");
        assert_eq!(parts.len(), 1, "the C++ builds a single-part message");

        let part = parts[0].as_ref().unwrap();
        assert_eq!(&part[0..4], b"rpcc");
        let header = proto::rpc::TRequestCancelationHeader::decode(&part[4..]).unwrap();
        assert_eq!(Guid::from_proto(&header.request_id), request_id);
        assert_eq!(header.service, API_SERVICE);
        assert_eq!(header.method, "SelectRows");
    }

    #[test]
    fn attachments_survive_the_round_trip() {
        let parts = response_parts(proto::rpc::TResponseHeader::default(), Some(b"body"));
        let mut parts = parts;
        parts.push(Some(Bytes::from_static(b"attachment one")));
        parts.push(Some(Bytes::from_static(b"attachment two")));

        let response = decode_response(parts).unwrap();
        assert_eq!(response.body.as_deref(), Some(&b"body"[..]));
        assert_eq!(response.attachments.len(), 2);
        assert_eq!(
            response.attachments[1],
            Bytes::from_static(b"attachment two")
        );
    }
}
