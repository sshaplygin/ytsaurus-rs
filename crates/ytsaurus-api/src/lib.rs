//! The transport-independent YTsaurus client interface.
//!
//! YTsaurus reaches its dynamic tables two ways — HTTP API v4 and the RPC proxy
//! — and the C++ client does not make callers choose an API to go with the
//! transport. It has one interface and two constructors:
//!
//! ```cpp
//! IClientPtr CreateClient   (const TString& serverName, ...);  // HTTP
//! IClientPtr CreateRpcClient(const TString& serverName, ...);  // RPC proxy
//! ```
//!
//! This crate is the Rust equivalent of the interface those return, and the
//! layering mirrors the C++ exactly:
//!
//! | C++ | here |
//! | --- | --- |
//! | `yt/yt/client/api` — the interface | this crate |
//! | `yt/yt/client/api/rpc_proxy` — one implementation | `ytsaurus-rpc` |
//! | `yt/cpp/mapreduce` — the wrapper with both constructors | `ytsaurus-client` |
//!
//! So the two constructors live in `ytsaurus-client`, which depends on both,
//! and switching transport is one line:
//!
//! ```ignore
//! let client = ytsaurus_client::create_client("localhost:8000")?;      // HTTP
//! let client = ytsaurus_client::create_rpc_client("localhost:8011")?;  // RPC
//! // everything below is identical
//! let rows = client.lookup_rows("//tmp/t", &[key], &LookupOptions::default())?;
//! ```
//!
//! # This interface is synchronous
//!
//! Deliberately, and it is the one decision here worth arguing about. The C++
//! wrapper is blocking, every other crate in this workspace is synchronous, and
//! a MapReduce job is a synchronous, single-purpose process. So the shared
//! interface blocks.
//!
//! `ytsaurus-rpc`'s own API stays `async`, and callers who want multiplexed
//! in-flight requests — the entire reason the RPC proxy exists — should use it
//! directly rather than through this. What this buys is portability between
//! transports, not concurrency.
//!
//! # What it covers
//!
//! The dynamic-table surface both transports implement: reads, writes and the
//! transactions they run in. Cypress, operations and file I/O stay on
//! `ytsaurus-client`, because the RPC crate deliberately does not implement
//! them and an interface with half its methods unavailable on one transport
//! would be worse than two honest APIs.

pub mod error;
pub mod value;

pub use error::{Error, Result};
pub use value::{MaybeRow, Row, Value};

/// A read timestamp.
pub type Timestamp = u64;

/// Options for [`TableClient::lookup_rows`].
#[derive(Debug, Clone, Default)]
pub struct LookupOptions {
    /// The columns to return. Empty means all of them.
    pub columns: Vec<String>,
    /// The timestamp to read at. `None` reads the latest committed data.
    ///
    /// Inside a transaction this is filled in for you; setting it by hand there
    /// would read at a different point than the transaction sees.
    pub timestamp: Option<Timestamp>,
}

/// Options for [`TableClient::select_rows`].
#[derive(Debug, Clone, Default)]
pub struct SelectOptions {
    /// The timestamp to read at. `None` reads the latest committed data.
    pub timestamp: Option<Timestamp>,
    /// Stop after this many rows, if set.
    pub limit: Option<u64>,
}

/// What a dynamic table can be asked to do, whatever the transport.
///
/// Implemented by the HTTP client and by the RPC client's blocking facade. The
/// two are wire-level different and behaviourally the same, which is the whole
/// point.
pub trait TableClient {
    /// Which transport this client speaks, for diagnostics and for tests that
    /// want to run the same checks against both.
    fn transport(&self) -> Transport;

    /// Looks rows up by key.
    ///
    /// `keys` holds one row per key, carrying only the key columns. The result
    /// has **one entry per key, in the order asked**, and a key with no row
    /// comes back as `None`.
    fn lookup_rows(
        &self,
        path: &str,
        keys: &[Row],
        options: &LookupOptions,
    ) -> Result<Vec<MaybeRow>>;

    /// Runs a query and returns its rows.
    fn select_rows(&self, query: &str, options: &SelectOptions) -> Result<Vec<Row>>;

    /// Writes rows outside a transaction.
    ///
    /// The transports differ underneath — HTTP has a standalone `insert_rows`
    /// command, RPC has none and needs a transaction — and this hides that.
    fn insert_rows(&self, path: &str, rows: &[Row]) -> Result<()>;

    /// Deletes rows by key, outside a transaction.
    fn delete_rows(&self, path: &str, keys: &[Row]) -> Result<()>;

    /// Starts a tablet transaction.
    ///
    /// Boxed because the transaction types differ per transport and a caller
    /// holding a `dyn TableClient` cannot name either.
    fn start_transaction(&self) -> Result<Box<dyn TableTransaction + '_>>;
}

/// A transaction over a dynamic table.
///
/// Dropping one does **not** abort it: neither transport can abort reliably
/// from `Drop`, and a silent best-effort attempt would be a lie. An unfinished
/// transaction expires on the server after its timeout.
pub trait TableTransaction {
    /// The transaction id, as the cluster shows it.
    fn id(&self) -> String;

    /// Looks rows up as of this transaction.
    fn lookup_rows(
        &self,
        path: &str,
        keys: &[Row],
        options: &LookupOptions,
    ) -> Result<Vec<MaybeRow>>;

    /// Runs a query as of this transaction.
    fn select_rows(&self, query: &str, options: &SelectOptions) -> Result<Vec<Row>>;

    /// Writes rows in this transaction.
    fn insert_rows(&self, path: &str, rows: &[Row]) -> Result<()>;

    /// Deletes rows by key in this transaction.
    fn delete_rows(&self, path: &str, keys: &[Row]) -> Result<()>;

    /// Tells the server the transaction is still wanted.
    fn ping(&self) -> Result<()>;

    /// Commits. Takes the transaction by box because it consumes it.
    fn commit(self: Box<Self>) -> Result<()>;

    /// Aborts.
    fn abort(self: Box<Self>) -> Result<()>;
}

/// Which wire a client speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// HTTP API v4.
    Http,
    /// The RPC proxy, over bus.
    Rpc,
}

impl std::fmt::Display for Transport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Http => "HTTP",
            Self::Rpc => "RPC",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The interface has to be usable behind a `dyn`, or the two constructors
    /// cannot return the same thing — which is the entire point of it.
    #[test]
    fn the_interface_is_object_safe() {
        struct Nothing;

        impl TableClient for Nothing {
            fn transport(&self) -> Transport {
                Transport::Http
            }
            fn lookup_rows(&self, _: &str, _: &[Row], _: &LookupOptions) -> Result<Vec<MaybeRow>> {
                Ok(Vec::new())
            }
            fn select_rows(&self, _: &str, _: &SelectOptions) -> Result<Vec<Row>> {
                Ok(Vec::new())
            }
            fn insert_rows(&self, _: &str, _: &[Row]) -> Result<()> {
                Ok(())
            }
            fn delete_rows(&self, _: &str, _: &[Row]) -> Result<()> {
                Ok(())
            }
            fn start_transaction(&self) -> Result<Box<dyn TableTransaction + '_>> {
                Err(Error::Unsupported {
                    transport: Transport::Http,
                    what: "transactions in this stub",
                })
            }
        }

        let client: Box<dyn TableClient> = Box::new(Nothing);
        assert_eq!(client.transport(), Transport::Http);
        assert!(
            client
                .lookup_rows("//tmp/t", &[], &LookupOptions::default())
                .is_ok()
        );
    }

    #[test]
    fn transport_names_itself() {
        assert_eq!(Transport::Http.to_string(), "HTTP");
        assert_eq!(Transport::Rpc.to_string(), "RPC");
    }
}
