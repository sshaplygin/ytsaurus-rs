//! The error model.
//!
//! A server-side failure arrives as `TError`
//! (`yt_proto/yt/core/misc/proto/error.proto`): a code, a message, an
//! attribute dictionary and — the part that matters — a list of *inner*
//! errors. YTsaurus nests errors deeply, and the innermost one is usually the
//! only one that says what actually went wrong: the outer layers say "lookup
//! failed", "tablet request failed", and the innermost says "no such table".
//! Flattening that to a string loses the diagnosis, so [`YtError`] keeps the
//! tree and [`YtError::find`] walks it.

use std::fmt;

use crate::guid::Guid;

/// Error codes worth naming. Values are from `yt/go/yterrors/error_code.go`,
/// which is generated from the same C++ headers the proxy uses.
pub mod codes {
    pub const OK: i32 = 0;
    pub const GENERIC: i32 = 1;
    pub const CANCELED: i32 = 2;
    pub const TIMEOUT: i32 = 3;
    pub const TRANSPORT_ERROR: i32 = 100;
    pub const UNAVAILABLE: i32 = 105;
    pub const REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED: i32 = 108;
    pub const RPC_AUTHENTICATION_ERROR: i32 = 109;
    pub const RESOLVE_ERROR: i32 = 500;
    pub const AUTHENTICATION_ERROR: i32 = 900;
    pub const NO_SUCH_TRANSACTION: i32 = 11000;
}

/// An error reported by the server, with its nesting preserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YtError {
    pub code: i32,
    pub message: String,
    /// Attribute values are YSON, exactly as the wire carries them; they are
    /// kept as bytes so nothing is lost to a decoder this crate does not need.
    pub attributes: Vec<(String, Vec<u8>)>,
    pub inner_errors: Vec<YtError>,
}

impl YtError {
    /// Reads the protobuf form, keeping the whole tree.
    pub fn from_proto(proto: &crate::proto::misc::TError) -> Self {
        Self {
            code: proto.code,
            message: proto.message.clone().unwrap_or_default(),
            attributes: proto
                .attributes
                .as_ref()
                .map(|dictionary| {
                    dictionary
                        .attributes
                        .iter()
                        .map(|attribute| (attribute.key.clone(), attribute.value.clone()))
                        .collect()
                })
                .unwrap_or_default(),
            inner_errors: proto.inner_errors.iter().map(Self::from_proto).collect(),
        }
    }

    /// The first error in the tree with this code, outermost first.
    ///
    /// The useful question is almost never "what is the outer code" but "is
    /// this failure a `NoSuchTransaction` anywhere inside", because retry and
    /// reporting decisions hang on the inner code.
    pub fn find(&self, code: i32) -> Option<&YtError> {
        if self.code == code {
            return Some(self);
        }
        self.inner_errors.iter().find_map(|inner| inner.find(code))
    }

    /// Whether this error or any error nested inside it has this code.
    pub fn has_code(&self, code: i32) -> bool {
        self.find(code).is_some()
    }

    /// The innermost error along the first chain of inner errors — usually the
    /// one that names the real cause.
    pub fn innermost(&self) -> &YtError {
        let mut current = self;
        while let Some(first) = current.inner_errors.first() {
            current = first;
        }
        current
    }

    /// The value of an attribute, as raw YSON bytes.
    pub fn attribute(&self, key: &str) -> Option<&[u8]> {
        self.attributes
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_slice())
    }
}

impl fmt::Display for YtError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} (code {})", self.message, self.code)?;
        // Nesting is printed as an indented tree; a one-line rendering of a
        // four-deep YTsaurus error is unreadable, and the depth is the
        // diagnosis.
        for inner in &self.inner_errors {
            let rendered = inner.to_string();
            for line in rendered.lines() {
                write!(formatter, "\n    {line}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for YtError {}

/// Anything that can go wrong in this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server answered, and the answer was a failure.
    #[error("{service}.{method} failed: {error}")]
    Response {
        service: String,
        method: String,
        #[source]
        error: YtError,
    },

    /// The bytes on the connection were not a well-formed packet.
    #[error("bus protocol error: {0}")]
    Packet(#[from] crate::bus::packet::PacketError),

    /// The connection failed, or was never established.
    #[error("connection to {address} failed: {source}")]
    Connect {
        address: String,
        #[source]
        source: std::io::Error,
    },

    /// I/O on an established connection failed.
    #[error("connection lost: {0}")]
    Io(#[from] std::io::Error),

    /// The peer sent something structurally valid but not what the protocol
    /// allows here.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// A protobuf message did not parse.
    #[error("could not decode {message}: {source}")]
    Decode {
        message: &'static str,
        #[source]
        source: prost::DecodeError,
    },

    /// The request did not complete inside its deadline.
    #[error("{service}.{method} timed out after {timeout:?}")]
    Timeout {
        service: String,
        method: String,
        timeout: std::time::Duration,
    },

    /// The connection was closed while this request was in flight.
    #[error("connection closed with request {request_id} in flight")]
    ConnectionClosed { request_id: Guid },

    /// A rowset could not be decoded.
    #[error("row wire format: {0}")]
    Wire(#[from] crate::wire::WireError),
}

impl Error {
    /// The server-reported error, if this failure came from the server.
    pub fn yt_error(&self) -> Option<&YtError> {
        match self {
            Self::Response { error, .. } => Some(error),
            _ => None,
        }
    }

    /// Whether this failure carries the given YTsaurus error code anywhere in
    /// its nesting.
    pub fn has_code(&self, code: i32) -> bool {
        self.yt_error().is_some_and(|error| error.has_code(code))
    }
}

/// The crate's result type.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto;

    fn proto_error(
        code: i32,
        message: &str,
        inner: Vec<proto::misc::TError>,
    ) -> proto::misc::TError {
        proto::misc::TError {
            code,
            message: Some(message.to_owned()),
            attributes: None,
            inner_errors: inner,
        }
    }

    #[test]
    fn nesting_survives_the_conversion() {
        let wire = proto_error(
            1,
            "lookup failed",
            vec![proto_error(
                1,
                "tablet request failed",
                vec![proto_error(codes::RESOLVE_ERROR, "no such table", vec![])],
            )],
        );

        let error = YtError::from_proto(&wire);
        assert_eq!(error.message, "lookup failed");
        assert_eq!(error.inner_errors.len(), 1);
        assert_eq!(error.innermost().message, "no such table");
        assert_eq!(error.innermost().code, codes::RESOLVE_ERROR);
    }

    #[test]
    fn find_reaches_an_inner_code() {
        let wire = proto_error(
            1,
            "outer",
            vec![proto_error(
                codes::NO_SUCH_TRANSACTION,
                "no such transaction",
                vec![],
            )],
        );
        let error = YtError::from_proto(&wire);

        assert!(error.has_code(codes::NO_SUCH_TRANSACTION));
        assert_eq!(
            error.find(codes::NO_SUCH_TRANSACTION).unwrap().message,
            "no such transaction"
        );
        assert!(!error.has_code(codes::TIMEOUT));
        assert!(error.find(codes::TIMEOUT).is_none());
    }

    #[test]
    fn display_indents_the_tree() {
        let wire = proto_error(1, "outer", vec![proto_error(500, "inner", vec![])]);
        let rendered = YtError::from_proto(&wire).to_string();
        assert_eq!(rendered, "outer (code 1)\n    inner (code 500)");
    }

    #[test]
    fn attributes_are_kept_as_yson_bytes() {
        let wire = proto::misc::TError {
            code: 500,
            message: Some("no such node".to_owned()),
            attributes: Some(proto::ytree::TAttributeDictionary {
                attributes: vec![proto::ytree::TAttribute {
                    key: "path".to_owned(),
                    value: b"\x01\x0c//tmp/nope".to_vec(),
                }],
            }),
            inner_errors: vec![],
        };
        let error = YtError::from_proto(&wire);
        assert_eq!(error.attribute("path"), Some(&b"\x01\x0c//tmp/nope"[..]));
        assert_eq!(error.attribute("missing"), None);
    }

    #[test]
    fn a_missing_message_reads_as_empty() {
        let wire = proto::misc::TError {
            code: codes::GENERIC,
            message: None,
            attributes: None,
            inner_errors: vec![],
        };
        let error = YtError::from_proto(&wire);
        assert_eq!(error.code, codes::GENERIC);
        assert_eq!(error.message, "");
    }

    #[test]
    fn has_code_reaches_through_the_crate_error() {
        let error = Error::Response {
            service: "ApiService".to_owned(),
            method: "LookupRows".to_owned(),
            error: YtError::from_proto(&proto_error(
                1,
                "outer",
                vec![proto_error(codes::NO_SUCH_TRANSACTION, "gone", vec![])],
            )),
        };
        assert!(error.has_code(codes::NO_SUCH_TRANSACTION));
        assert!(!error.has_code(codes::TIMEOUT));
        assert!(error.yt_error().is_some());
        assert!(error.to_string().contains("ApiService.LookupRows failed"));
    }
}
