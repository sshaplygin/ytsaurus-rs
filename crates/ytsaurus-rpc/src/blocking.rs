//! A blocking facade, in the shape `reqwest::blocking` uses.
//!
//! The RPC client is `async` because multiplexed in-flight requests are the
//! entire reason to speak this protocol. But a MapReduce job is a synchronous,
//! single-purpose process, and enriching rows from a dynamic table mid-map is
//! the obvious use — so there has to be a way in that does not ask the caller
//! for a runtime.
//!
//! This owns a current-thread runtime and drives each call to completion on it.
//! It is what implements [`ytsaurus_api::TableClient`], so a caller can hold one
//! interface and choose the transport at construction, as the C++ client does.
//!
//! **It gives up the concurrency.** One call at a time, and the multiplexing
//! the connection is capable of goes unused. Anything that wants it should use
//! [`crate::Client`] directly and bring its own runtime.

use std::sync::Arc;

use tokio::runtime::Runtime;
use ytsaurus_api::{
    Error as ApiError, LookupOptions, MaybeRow, Result as ApiResult, Row, SelectOptions,
    TableClient, TableTransaction, Transport,
};

use crate::client::{Client as AsyncClient, StartTransactionOptions, Transaction, TransactionType};
use crate::wire;

mod convert;

pub use convert::{row_from_wire, row_to_wire};

/// A synchronous RPC-proxy client.
///
/// Cheap to clone in the sense that matters — the runtime and connection are
/// shared — but it is not `Clone`, because two handles driving one
/// current-thread runtime from two threads would serialise on it in a way that
/// looks like a deadlock rather than like contention.
pub struct Client {
    runtime: Arc<Runtime>,
    inner: Arc<AsyncClient>,
}

impl Client {
    /// Connects to an RPC proxy at `host:port`.
    pub fn connect(address: &str) -> ApiResult<Self> {
        Self::builder(address).connect()
    }

    /// Starts configuring a client.
    pub fn builder(address: &str) -> ClientBuilder {
        ClientBuilder {
            address: address.to_owned(),
            token: None,
            timeout: None,
        }
    }

    /// The asynchronous client underneath, for a caller that wants one call to
    /// use the concurrency this facade gives up.
    pub fn inner(&self) -> &Arc<AsyncClient> {
        &self.inner
    }

    /// Runs a future to completion on this client's runtime.
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

/// Configuration for [`Client::connect`].
pub struct ClientBuilder {
    address: String,
    token: Option<String>,
    timeout: Option<std::time::Duration>,
}

impl ClientBuilder {
    /// The token sent with every request.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// The per-request deadline.
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn connect(self) -> ApiResult<Client> {
        // Current-thread: this facade drives one call at a time, so a
        // multi-thread runtime would cost threads for concurrency that is not
        // there. `enable_all` because the connection needs the timer and the
        // network driver.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| ApiError::transport_from("starting a runtime", error))?;

        let mut builder = AsyncClient::builder(&self.address);
        if let Some(token) = self.token {
            builder = builder.token(token);
        }
        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
        }

        let inner = runtime
            .block_on(builder.connect())
            .map_err(|error| ApiError::transport_from("connecting", error))?;

        Ok(Client {
            runtime: Arc::new(runtime),
            inner: Arc::new(inner),
        })
    }
}

/// Maps this crate's error onto the interface's.
fn map_error(operation: &str, error: crate::Error) -> ApiError {
    match &error {
        crate::Error::Response {
            error: reported, ..
        } => {
            let code = Some(reported.code);
            ApiError::cluster_from(operation, code, error)
        }
        crate::Error::Timeout { .. } => ApiError::Timeout {
            operation: operation.to_owned(),
        },
        _ => ApiError::transport_from(operation, error),
    }
}

/// The columns a set of rows mentions, in first-seen order.
///
/// The wire format numbers values and resolves them through a name table, so
/// every row in one request has to agree on that table. Rows that name their
/// columns individually are folded into one here.
fn column_names(rows: &[Row]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for row in rows {
        for name in row.names() {
            if !names.iter().any(|known| known == name) {
                names.push(name.to_owned());
            }
        }
    }
    names
}

fn to_wire_rows(rows: &[Row], columns: &[String]) -> ApiResult<Vec<wire::Row>> {
    rows.iter().map(|row| row_to_wire(row, columns)).collect()
}

fn from_wire_rows(rows: Vec<MaybeRowWire>, columns: &[String]) -> ApiResult<Vec<MaybeRow>> {
    rows.into_iter()
        .map(|row| match row {
            Some(row) => row_from_wire(&row, columns).map(Some),
            None => Ok(None),
        })
        .collect()
}

type MaybeRowWire = Option<wire::Row>;

impl TableClient for Client {
    fn transport(&self) -> Transport {
        Transport::Rpc
    }

    fn lookup_rows(
        &self,
        path: &str,
        keys: &[Row],
        options: &LookupOptions,
    ) -> ApiResult<Vec<MaybeRow>> {
        let key_columns = column_names(keys);
        let wire_keys = to_wire_rows(keys, &key_columns)?;
        let borrowed: Vec<&str> = key_columns.iter().map(String::as_str).collect();
        let filter: Vec<&str> = options.columns.iter().map(String::as_str).collect();

        let (rows, columns) = self
            .runtime
            .block_on(self.inner.lookup_rows_with_columns(
                path,
                &borrowed,
                &wire_keys,
                crate::client::LookupOptions {
                    timestamp: options.timestamp,
                    column_filter: filter,
                },
            ))
            .map_err(|error| map_error("lookup_rows", error))?;
        from_wire_rows(rows, &columns)
    }

    fn select_rows(&self, query: &str, options: &SelectOptions) -> ApiResult<Vec<Row>> {
        let query = match options.limit {
            Some(limit) => format!("{query} limit {limit}"),
            None => query.to_owned(),
        };
        let (rows, columns) = self
            .runtime
            .block_on(self.inner.select_rows_with_columns(
                &query,
                crate::client::SelectOptions {
                    timestamp: options.timestamp,
                },
            ))
            .map_err(|error| map_error("select_rows", error))?;

        rows.into_iter()
            .flatten()
            .map(|row| row_from_wire(&row, &columns))
            .collect()
    }

    fn insert_rows(&self, path: &str, rows: &[Row]) -> ApiResult<()> {
        // The RPC proxy has no standalone insert: writes belong to a tablet
        // transaction. HTTP's `insert_rows` opens one implicitly, so this does
        // the same and the two behave alike.
        let transaction = self.start_transaction()?;
        transaction.insert_rows(path, rows)?;
        transaction.commit()
    }

    fn delete_rows(&self, path: &str, keys: &[Row]) -> ApiResult<()> {
        let transaction = self.start_transaction()?;
        transaction.delete_rows(path, keys)?;
        transaction.commit()
    }

    fn start_transaction(&self) -> ApiResult<Box<dyn TableTransaction + '_>> {
        let transaction = self
            .runtime
            .block_on(
                self.inner
                    .start_transaction(TransactionType::Tablet, StartTransactionOptions::default()),
            )
            .map_err(|error| map_error("start_transaction", error))?;

        Ok(Box::new(BlockingTransaction {
            runtime: Arc::clone(&self.runtime),
            transaction,
        }))
    }
}

/// A tablet transaction, driven synchronously.
struct BlockingTransaction<'a> {
    runtime: Arc<Runtime>,
    transaction: Transaction<'a>,
}

impl TableTransaction for BlockingTransaction<'_> {
    fn id(&self) -> String {
        self.transaction.id().to_string()
    }

    fn lookup_rows(
        &self,
        path: &str,
        keys: &[Row],
        options: &LookupOptions,
    ) -> ApiResult<Vec<MaybeRow>> {
        let key_columns = column_names(keys);
        let wire_keys = to_wire_rows(keys, &key_columns)?;
        let borrowed: Vec<&str> = key_columns.iter().map(String::as_str).collect();
        let filter: Vec<&str> = options.columns.iter().map(String::as_str).collect();

        let (rows, columns) = self
            .runtime
            .block_on(self.transaction.lookup_rows_with_columns(
                path,
                &borrowed,
                &wire_keys,
                crate::client::LookupOptions {
                    timestamp: None,
                    column_filter: filter,
                },
            ))
            .map_err(|error| map_error("lookup_rows", error))?;
        from_wire_rows(rows, &columns)
    }

    fn select_rows(&self, query: &str, options: &SelectOptions) -> ApiResult<Vec<Row>> {
        let query = match options.limit {
            Some(limit) => format!("{query} limit {limit}"),
            None => query.to_owned(),
        };
        let (rows, columns) =
            self.runtime
                .block_on(self.transaction.select_rows_with_columns(
                    &query,
                    crate::client::SelectOptions { timestamp: None },
                ))
                .map_err(|error| map_error("select_rows", error))?;

        rows.into_iter()
            .flatten()
            .map(|row| row_from_wire(&row, &columns))
            .collect()
    }

    fn insert_rows(&self, path: &str, rows: &[Row]) -> ApiResult<()> {
        let columns = column_names(rows);
        let wire_rows = to_wire_rows(rows, &columns)?;
        let borrowed: Vec<&str> = columns.iter().map(String::as_str).collect();
        self.runtime
            .block_on(self.transaction.insert_rows(path, &borrowed, &wire_rows))
            .map_err(|error| map_error("insert_rows", error))
    }

    fn delete_rows(&self, path: &str, keys: &[Row]) -> ApiResult<()> {
        let columns = column_names(keys);
        let wire_keys = to_wire_rows(keys, &columns)?;
        let borrowed: Vec<&str> = columns.iter().map(String::as_str).collect();
        self.runtime
            .block_on(self.transaction.delete_rows(path, &borrowed, &wire_keys))
            .map_err(|error| map_error("delete_rows", error))
    }

    fn ping(&self) -> ApiResult<()> {
        self.runtime
            .block_on(self.transaction.ping())
            .map_err(|error| map_error("ping_transaction", error))
    }

    fn commit(self: Box<Self>) -> ApiResult<()> {
        let this = *self;
        this.runtime
            .block_on(this.transaction.commit())
            .map_err(|error| map_error("commit_transaction", error))
    }

    fn abort(self: Box<Self>) -> ApiResult<()> {
        let this = *self;
        this.runtime
            .block_on(this.transaction.abort())
            .map_err(|error| map_error("abort_transaction", error))
    }
}
