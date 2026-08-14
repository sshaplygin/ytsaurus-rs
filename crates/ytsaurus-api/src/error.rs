//! One error type for both transports.
//!
//! The implementations have their own — `ytsaurus_client::Error` carries HTTP
//! status codes, `ytsaurus_rpc::Error` carries a nested `TError` — and neither
//! belongs in an interface the other implements. This keeps what a caller can
//! actually act on and carries the original underneath, so nothing is lost.

/// A failure from either transport.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The cluster refused the request.
    ///
    /// `code` is the YTsaurus error code where one was reported. Both
    /// transports produce them from the same table, so a caller can match on
    /// them without knowing which wire it is on.
    #[error("{operation} failed: {message}")]
    Cluster {
        operation: String,
        message: String,
        code: Option<i32>,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// The cluster could not be reached, or the connection failed mid-request.
    #[error("{operation}: {message}")]
    Transport {
        operation: String,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// The request did not complete inside its deadline.
    #[error("{operation} timed out")]
    Timeout { operation: String },

    /// A value or row could not be converted between this interface's model and
    /// the transport's.
    #[error("{0}")]
    Conversion(String),

    /// The transport does not implement this, and says so rather than pretending.
    #[error("{transport} does not support {what}")]
    Unsupported {
        transport: crate::Transport,
        what: &'static str,
    },
}

impl Error {
    /// The YTsaurus error code, if the cluster reported one.
    pub fn code(&self) -> Option<i32> {
        match self {
            Self::Cluster { code, .. } => *code,
            _ => None,
        }
    }

    /// Whether this is worth trying again.
    ///
    /// Deliberately coarse: a transport failure or a timeout may succeed on a
    /// second attempt, and a refusal from the cluster generally will not. A
    /// caller that needs the real retry rules should use the transport's own
    /// client, which has them.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transport { .. } | Self::Timeout { .. })
    }

    /// Builds a cluster refusal.
    pub fn cluster(
        operation: impl Into<String>,
        message: impl Into<String>,
        code: Option<i32>,
    ) -> Self {
        Self::Cluster {
            operation: operation.into(),
            message: message.into(),
            code,
            source: None,
        }
    }

    /// Builds a cluster refusal that keeps the transport's own error.
    pub fn cluster_from(
        operation: impl Into<String>,
        code: Option<i32>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Cluster {
            operation: operation.into(),
            message: source.to_string(),
            code,
            source: Some(Box::new(source)),
        }
    }

    /// Builds a transport failure that keeps the transport's own error.
    pub fn transport_from(
        operation: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Transport {
            operation: operation.into(),
            message: source.to_string(),
            source: Some(Box::new(source)),
        }
    }
}

/// The interface's result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Error codes both transports report, for callers that match on them.
///
/// The values are YTsaurus's own, from the same table the C++ and Go clients
/// use; they do not depend on the transport.
pub mod codes {
    pub const OK: i32 = 0;
    pub const GENERIC: i32 = 1;
    pub const TIMEOUT: i32 = 3;
    pub const RESOLVE_ERROR: i32 = 500;
    pub const AUTHENTICATION_ERROR: i32 = 900;
    pub const NO_SUCH_TRANSACTION: i32 = 11000;
    /// The table is not mounted, which is the first thing to check when a
    /// dynamic-table call fails on a cluster that was just set up.
    pub const TABLET_NOT_MOUNTED: i32 = 1702;
}

/// Renders a chain of sources, which is where the transport's own detail lives.
pub fn describe(error: &dyn std::error::Error) -> String {
    let mut description = error.to_string();
    let mut current = error.source();
    while let Some(cause) = current {
        description.push_str("\n  caused by: ");
        description.push_str(&cause.to_string());
        current = cause.source();
    }
    description
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("the underlying thing broke")]
    struct Underlying;

    #[test]
    fn a_cluster_refusal_keeps_its_code() {
        let error = Error::cluster("lookup_rows", "no such table", Some(codes::RESOLVE_ERROR));
        assert_eq!(error.code(), Some(codes::RESOLVE_ERROR));
        assert!(!error.is_retryable(), "a refusal will refuse again");
        assert!(error.to_string().contains("lookup_rows failed"));
    }

    #[test]
    fn a_transport_failure_is_worth_retrying() {
        let error = Error::transport_from("select_rows", Underlying);
        assert!(error.is_retryable());
        assert_eq!(error.code(), None);
    }

    #[test]
    fn the_original_error_survives_underneath() {
        let error = Error::cluster_from("insert_rows", Some(1), Underlying);
        let described = describe(&error);
        assert!(
            described.contains("the underlying thing broke"),
            "the transport's own error must not be thrown away: {described}"
        );
    }

    #[test]
    fn an_unsupported_operation_names_the_transport() {
        let error = Error::Unsupported {
            transport: crate::Transport::Rpc,
            what: "operations",
        };
        assert_eq!(error.to_string(), "RPC does not support operations");
    }
}
