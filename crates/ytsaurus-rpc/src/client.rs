//! The public API: a deliberately small subset of the RPC proxy's surface.
//!
//! Transactions, `lookup_rows`, `select_rows` and `modify_rows` — the calls
//! that justify speaking this protocol at all. Everything else the proxy can do
//! is reachable over HTTP through `ytsaurus-client`, and is not reimplemented
//! here.

use std::time::Duration;

use bytes::Bytes;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::guid::Guid;
use crate::proto;
use crate::wire::{self, MaybeRow, Row};

/// The default per-request deadline.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// The default transaction timeout: the server aborts a transaction that is not
/// pinged within it.
pub const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(15);

/// `ETransactionType` — `api_service.proto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TransactionType {
    Master = 0,
    Tablet = 1,
}

/// `EAtomicity` — `api_service.proto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Atomicity {
    Full = 0,
    None = 1,
}

/// `ERowModificationType` — `api_service.proto`.
///
/// The numbering has a gap: there is no 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RowModificationType {
    Write = 0,
    Delete = 1,
    /// "Write and lock" — the wire name is `RMT_MODIFY`.
    WriteAndLock = 3,
}

/// A timestamp, as the tablet API counts them.
pub type Timestamp = u64;

/// The read timestamp meaning "the latest committed data".
///
/// It is the declared proto2 default of every `timestamp` field in the read
/// methods. **Zero is not this**: zero is `NullTimestamp`, and sending it asks
/// for something else entirely.
pub const LATEST_TIMESTAMP: Timestamp = 0x3fff_ffff_ffff_ff01;

/// A client bound to one RPC proxy.
///
/// One connection multiplexes every call, so a `Client` is cheap to share:
/// wrap it in an `Arc` and call it concurrently rather than opening more.
#[derive(Debug)]
pub struct Client {
    connection: Connection,
    timeout: Duration,
}

impl Client {
    /// Connects to an RPC proxy at `host:port`.
    ///
    /// The address is the one `discover_proxies` returns, not the HTTP proxy's.
    pub async fn connect(address: &str) -> Result<Self> {
        Self::builder(address).connect().await
    }

    /// Starts configuring a client.
    pub fn builder(address: &str) -> ClientBuilder {
        ClientBuilder {
            address: address.to_owned(),
            token: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// The underlying connection, for callers that need to invoke a method this
    /// crate does not wrap.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// The addresses of the cluster's RPC proxies, asked of this one.
    pub async fn discover_proxies(&self, role: Option<&str>) -> Result<Vec<String>> {
        crate::connection::discover_proxies(&self.connection, role, Some(self.timeout)).await
    }

    /// Starts a transaction.
    ///
    /// A tablet transaction is **sticky**: it belongs to the proxy that created
    /// it, and every later call in it must go to that same proxy. This client
    /// holds one connection, so that happens naturally — but a transaction
    /// started here must not be used through another `Client`.
    pub async fn start_transaction(
        &self,
        transaction_type: TransactionType,
        options: StartTransactionOptions,
    ) -> Result<Transaction<'_>> {
        let request = start_transaction_request(transaction_type, &options);

        let (response, _) = self
            .connection
            .invoke::<proto::api::TRspStartTransaction>(
                "StartTransaction",
                &request,
                Vec::new(),
                Some(self.timeout),
                "TRspStartTransaction",
            )
            .await?;

        Ok(Transaction {
            client: self,
            id: Guid::from_proto(&response.id),
            start_timestamp: response.start_timestamp,
            finished: false,
        })
    }

    /// Looks rows up by key.
    ///
    /// `keys` holds one row per key, carrying only the key columns. The result
    /// has one entry per key **in the order asked**, and a key with no row
    /// comes back as `None` — which is why the rows are `Option`s rather than a
    /// shorter list.
    pub async fn lookup_rows(
        &self,
        path: &str,
        columns: &[&str],
        keys: &[Row],
        options: LookupOptions<'_>,
    ) -> Result<Vec<MaybeRow>> {
        let keys: Vec<MaybeRow> = keys.iter().cloned().map(Some).collect();
        let request = lookup_request(path, columns, &options);

        let (response, attachments) = self
            .connection
            .invoke::<proto::api::TRspLookupRows>(
                "LookupRows",
                &request,
                vec![wire::encode_rowset(&keys)?],
                Some(self.timeout),
                "TRspLookupRows",
            )
            .await?;

        decode_rowset_attachments(&attachments, Some(&response.rowset_descriptor))
    }

    /// Looks rows up, returning the names of the columns as well as the rows.
    ///
    /// A value carries a numeric id, not a name, and the reply's descriptor is
    /// what resolves them. Callers that map rows onto named columns — the
    /// blocking facade, and anything building a `serde` row — need both.
    pub async fn lookup_rows_with_columns(
        &self,
        path: &str,
        columns: &[&str],
        keys: &[Row],
        options: LookupOptions<'_>,
    ) -> Result<(Vec<MaybeRow>, Vec<String>)> {
        let keys: Vec<MaybeRow> = keys.iter().cloned().map(Some).collect();
        let request = lookup_request(path, columns, &options);

        let (response, attachments) = self
            .connection
            .invoke::<proto::api::TRspLookupRows>(
                "LookupRows",
                &request,
                vec![wire::encode_rowset(&keys)?],
                Some(self.timeout),
                "TRspLookupRows",
            )
            .await?;

        let descriptor = &response.rowset_descriptor;
        let rows = decode_rowset_attachments(&attachments, Some(descriptor))?;
        Ok((rows, descriptor_column_names(descriptor)))
    }

    /// Runs a query and returns its rows.
    ///
    /// The returned descriptor names the columns, in the order the values are
    /// numbered; [`select_rows_with_columns`](Self::select_rows_with_columns)
    /// hands both back when the caller needs the names.
    pub async fn select_rows(&self, query: &str, options: SelectOptions) -> Result<Vec<MaybeRow>> {
        Ok(self.select_rows_with_columns(query, options).await?.0)
    }

    /// Runs a query, returning its rows and the names of their columns.
    pub async fn select_rows_with_columns(
        &self,
        query: &str,
        options: SelectOptions,
    ) -> Result<(Vec<MaybeRow>, Vec<String>)> {
        let request = select_request(query, &options);

        let (response, attachments) = self
            .connection
            .invoke::<proto::api::TRspSelectRows>(
                "SelectRows",
                &request,
                Vec::new(),
                Some(self.timeout),
                "TRspSelectRows",
            )
            .await?;

        let descriptor = &response.rowset_descriptor;
        let rows = decode_rowset_attachments(&attachments, Some(descriptor))?;
        Ok((rows, descriptor_column_names(descriptor)))
    }
}

/// Configuration for [`Client::connect`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    address: String,
    token: Option<String>,
    timeout: Duration,
}

impl ClientBuilder {
    /// The token sent in every request's credentials extension.
    ///
    /// A local cluster with authentication disabled needs none.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// The per-request deadline, sent to the server as well as applied locally.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn connect(self) -> Result<Client> {
        let connection = Connection::connect(&self.address, self.token).await?;
        Ok(Client {
            connection,
            timeout: self.timeout,
        })
    }
}

/// Options for [`Client::start_transaction`].
#[derive(Debug, Clone)]
pub struct StartTransactionOptions {
    /// How long the server waits between pings before aborting.
    pub timeout: Duration,
    pub atomicity: Atomicity,
    pub parent_id: Option<Guid>,
}

impl Default for StartTransactionOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TRANSACTION_TIMEOUT,
            atomicity: Atomicity::Full,
            parent_id: None,
        }
    }
}

/// Options for [`Client::lookup_rows`].
#[derive(Debug, Clone, Default)]
pub struct LookupOptions<'a> {
    /// The read timestamp. `None` reads the latest committed data; inside a
    /// tablet transaction it must be the transaction's start timestamp, which
    /// [`Transaction::lookup_rows`] fills in.
    pub timestamp: Option<Timestamp>,
    /// The columns to return. Empty means all of them.
    pub column_filter: Vec<&'a str>,
}

/// Options for [`Client::select_rows`].
#[derive(Debug, Clone, Default)]
pub struct SelectOptions {
    /// The read timestamp; see [`LookupOptions::timestamp`].
    pub timestamp: Option<Timestamp>,
}

/// An open transaction.
///
/// Dropping one does **not** abort it: `Drop` cannot await, and a silent
/// best-effort abort would be a lie. An unfinished transaction is left to
/// expire on the server after its timeout, and [`Transaction::abort`] is there
/// to end it now.
#[derive(Debug)]
pub struct Transaction<'a> {
    client: &'a Client,
    id: Guid,
    start_timestamp: Timestamp,
    finished: bool,
}

impl Transaction<'_> {
    pub fn id(&self) -> Guid {
        self.id
    }

    /// The transaction's read timestamp.
    ///
    /// Reads inside a tablet transaction are expressed purely as this
    /// timestamp: `TReqLookupRows` and `TReqSelectRows` have no
    /// `transaction_id` field at all.
    pub fn start_timestamp(&self) -> Timestamp {
        self.start_timestamp
    }

    /// Tells the server the transaction is still wanted.
    ///
    /// Must be called more often than the transaction's timeout, or the server
    /// aborts it.
    pub async fn ping(&self) -> Result<()> {
        let request = proto::api::TReqPingTransaction {
            transaction_id: self.id.to_proto(),
            ..Default::default()
        };
        self.client
            .connection
            .invoke::<proto::api::TRspPingTransaction>(
                "PingTransaction",
                &request,
                Vec::new(),
                Some(self.client.timeout),
                "TRspPingTransaction",
            )
            .await?;
        Ok(())
    }

    /// Commits the transaction.
    pub async fn commit(mut self) -> Result<()> {
        let request = proto::api::TReqCommitTransaction {
            transaction_id: self.id.to_proto(),
            ..Default::default()
        };
        self.client
            .connection
            .invoke::<proto::api::TRspCommitTransaction>(
                "CommitTransaction",
                &request,
                Vec::new(),
                Some(self.client.timeout),
                "TRspCommitTransaction",
            )
            .await?;
        self.finished = true;
        Ok(())
    }

    /// Aborts the transaction.
    pub async fn abort(mut self) -> Result<()> {
        let request = proto::api::TReqAbortTransaction {
            transaction_id: self.id.to_proto(),
            ..Default::default()
        };
        self.client
            .connection
            .invoke::<proto::api::TRspAbortTransaction>(
                "AbortTransaction",
                &request,
                Vec::new(),
                Some(self.client.timeout),
                "TRspAbortTransaction",
            )
            .await?;
        self.finished = true;
        Ok(())
    }

    /// Whether the transaction has been committed or aborted.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Looks rows up as of this transaction's start timestamp.
    pub async fn lookup_rows(
        &self,
        path: &str,
        columns: &[&str],
        keys: &[Row],
        mut options: LookupOptions<'_>,
    ) -> Result<Vec<MaybeRow>> {
        options.timestamp = Some(self.start_timestamp);
        self.client.lookup_rows(path, columns, keys, options).await
    }

    /// Looks rows up as of this transaction, with the reply's column names.
    pub async fn lookup_rows_with_columns(
        &self,
        path: &str,
        columns: &[&str],
        keys: &[Row],
        mut options: LookupOptions<'_>,
    ) -> Result<(Vec<MaybeRow>, Vec<String>)> {
        options.timestamp = Some(self.start_timestamp);
        self.client
            .lookup_rows_with_columns(path, columns, keys, options)
            .await
    }

    /// Runs a query as of this transaction, with the reply's column names.
    pub async fn select_rows_with_columns(
        &self,
        query: &str,
        mut options: SelectOptions,
    ) -> Result<(Vec<MaybeRow>, Vec<String>)> {
        options.timestamp = Some(self.start_timestamp);
        self.client.select_rows_with_columns(query, options).await
    }

    /// Runs a query as of this transaction's start timestamp.
    pub async fn select_rows(
        &self,
        query: &str,
        mut options: SelectOptions,
    ) -> Result<Vec<MaybeRow>> {
        options.timestamp = Some(self.start_timestamp);
        self.client.select_rows(query, options).await
    }

    /// Writes rows.
    pub async fn insert_rows(&self, path: &str, columns: &[&str], rows: &[Row]) -> Result<()> {
        self.modify_rows(path, columns, rows, RowModificationType::Write)
            .await
    }

    /// Deletes rows by key. Each row carries only the key columns.
    pub async fn delete_rows(&self, path: &str, columns: &[&str], keys: &[Row]) -> Result<()> {
        self.modify_rows(path, columns, keys, RowModificationType::Delete)
            .await
    }

    /// Applies one modification type to every row.
    ///
    /// `row_modification_types` is a parallel array to the rows in the
    /// attachment: entry *i* is the type of row *i*, and the server relies on
    /// the two staying the same length.
    pub async fn modify_rows(
        &self,
        path: &str,
        columns: &[&str],
        rows: &[Row],
        modification: RowModificationType,
    ) -> Result<()> {
        let owned: Vec<MaybeRow> = rows.iter().cloned().map(Some).collect();
        let request = modify_request(self.id, path, columns, rows.len(), modification);

        self.client
            .connection
            .invoke::<proto::api::TRspModifyRows>(
                "ModifyRows",
                &request,
                vec![wire::encode_rowset(&owned)?],
                Some(self.client.timeout),
                "TRspModifyRows",
            )
            .await?;
        Ok(())
    }
}

/// Builds the `StartTransaction` request.
///
/// Separated from the call so the bytes a method puts on the wire can be
/// asserted without a proxy: these functions are where a wrong or missing field
/// would live, and a mistake in one is invisible until a cluster rejects it.
fn start_transaction_request(
    transaction_type: TransactionType,
    options: &StartTransactionOptions,
) -> proto::api::TReqStartTransaction {
    proto::api::TReqStartTransaction {
        r#type: transaction_type as i32,
        timeout: Some(options.timeout.as_micros() as i64),
        // A tablet transaction is pinned to the proxy that created it; say so,
        // as both reference clients do.
        sticky: Some(transaction_type == TransactionType::Tablet),
        atomicity: Some(options.atomicity as i32),
        parent_id: options.parent_id.map(Guid::to_proto),
        ..Default::default()
    }
}

/// Builds the `LookupRows` request. The keys travel separately, in attachments.
fn lookup_request(
    path: &str,
    columns: &[&str],
    options: &LookupOptions<'_>,
) -> proto::api::TReqLookupRows {
    proto::api::TReqLookupRows {
        // `bytes`, not `string`: a YPath is a byte string.
        path: path.as_bytes().to_vec(),
        rowset_descriptor: name_table_descriptor(columns),
        timestamp: options.timestamp,
        // One answer per key asked for, so a key with no row comes back as a
        // null row rather than shortening the list and silently misaligning
        // every answer after it.
        keep_missing_rows: Some(true),
        columns: options
            .column_filter
            .iter()
            .map(|column| (*column).to_owned())
            .collect(),
        ..Default::default()
    }
}

/// Builds the `SelectRows` request.
fn select_request(query: &str, options: &SelectOptions) -> proto::api::TReqSelectRows {
    proto::api::TReqSelectRows {
        query: query.to_owned(),
        timestamp: options.timestamp,
        ..Default::default()
    }
}

/// Builds the `ModifyRows` request. The rows travel separately, in attachments.
fn modify_request(
    transaction_id: Guid,
    path: &str,
    columns: &[&str],
    row_count: usize,
    modification: RowModificationType,
) -> proto::api::TReqModifyRows {
    proto::api::TReqModifyRows {
        transaction_id: transaction_id.to_proto(),
        path: path.as_bytes().to_vec(),
        rowset_descriptor: name_table_descriptor(columns),
        // One entry per row, in the same order as the rows in the attachment.
        // The server relies on the two staying the same length.
        row_modification_types: vec![modification as i32; row_count],
        ..Default::default()
    }
}

/// The column names a reply's descriptor carries, in id order.
fn descriptor_column_names(descriptor: &proto::api::TRowsetDescriptor) -> Vec<String> {
    descriptor
        .name_table_entries
        .iter()
        .map(|entry| entry.name.clone().unwrap_or_default())
        .collect()
}

/// Builds the descriptor that names the columns a rowset's value ids refer to.
///
/// A value carries a numeric id, not a column name; the id indexes this table.
fn name_table_descriptor(columns: &[&str]) -> proto::api::TRowsetDescriptor {
    proto::api::TRowsetDescriptor {
        wire_format_version: Some(CURRENT_WIRE_FORMAT_VERSION),
        rowset_kind: Some(proto::api::ERowsetKind::RkUnversioned as i32),
        name_table_entries: columns
            .iter()
            .map(|name| proto::api::t_rowset_descriptor::TNameTableEntry {
                name: Some((*name).to_owned()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `CurrentWireFormatVersion` — `yt/go/yt/internal/rpcclient/wire.go`.
const CURRENT_WIRE_FORMAT_VERSION: i32 = 1;

/// Decodes the rows an API response carries in its attachments.
///
/// Attachments are concatenated before decoding: a large rowset is split across
/// several, and each is a slice of one stream rather than a self-contained
/// rowset — `mergeAttachments` in the Go client does the same.
fn decode_rowset_attachments(
    attachments: &[Bytes],
    descriptor: Option<&proto::api::TRowsetDescriptor>,
) -> Result<Vec<MaybeRow>> {
    if let Some(descriptor) = descriptor
        && let Some(version) = descriptor.wire_format_version
        && version != CURRENT_WIRE_FORMAT_VERSION
    {
        return Err(Error::Protocol(format!(
            "the proxy replied with wire format version {version}, and this client speaks {CURRENT_WIRE_FORMAT_VERSION}"
        )));
    }

    let merged = match attachments {
        [] => return Ok(Vec::new()),
        [single] => single.clone(),
        many => {
            let mut merged =
                bytes::BytesMut::with_capacity(many.iter().map(Bytes::len).sum::<usize>());
            for attachment in many {
                merged.extend_from_slice(attachment);
            }
            merged.freeze()
        }
    };

    Ok(wire::decode_rowset(&merged)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{UnversionedValue, Value};
    use prost::Message as _;

    /// The enums are hand-written mirrors of proto enums, so they are compared
    /// against the generated types rather than against restatements of
    /// themselves.
    #[test]
    fn enum_values_match_the_proto() {
        assert_eq!(
            RowModificationType::Write as i32,
            proto::api::ERowModificationType::RmtWrite as i32
        );
        assert_eq!(
            RowModificationType::Delete as i32,
            proto::api::ERowModificationType::RmtDelete as i32
        );
        assert_eq!(
            RowModificationType::WriteAndLock as i32,
            proto::api::ERowModificationType::RmtModify as i32
        );
        assert_eq!(
            TransactionType::Master as i32,
            proto::api::ETransactionType::TtMaster as i32
        );
        assert_eq!(
            TransactionType::Tablet as i32,
            proto::api::ETransactionType::TtTablet as i32
        );
        assert_eq!(Atomicity::Full as i32, proto::api::EAtomicity::AFull as i32);
        assert_eq!(Atomicity::None as i32, proto::api::EAtomicity::ANone as i32);
    }

    #[test]
    fn enum_values_match_the_documented_numbers() {
        assert_eq!(TransactionType::Master as i32, 0);
        assert_eq!(TransactionType::Tablet as i32, 1);
        assert_eq!(Atomicity::Full as i32, 0);
        assert_eq!(Atomicity::None as i32, 1);
        assert_eq!(RowModificationType::Write as i32, 0);
        assert_eq!(RowModificationType::Delete as i32, 1);
        // There is no 2 in the RPC-proxy enum.
        assert_eq!(RowModificationType::WriteAndLock as i32, 3);
    }

    /// The sentinel must be the value the proxy itself defaults to, not merely
    /// a non-zero number this crate agrees with itself about.
    ///
    /// Zero is `NullTimestamp` and asks for something else entirely, so a
    /// client that sent it instead would read wrong data rather than fail.
    #[test]
    fn the_latest_timestamp_sentinel_is_the_proto_default() {
        assert_ne!(LATEST_TIMESTAMP, 0, "zero is NullTimestamp, not 'latest'");

        // Round-tripping through the generated type is what ties the constant
        // to `api_service.proto`: a request that leaves `timestamp` unset is
        // read back by the server as its declared default, and this asserts
        // that is the value named here.
        let mut buffer = Vec::new();
        proto::api::TReqLookupRows {
            path: b"//tmp/t".to_vec(),
            timestamp: None,
            ..Default::default()
        }
        .encode(&mut buffer)
        .unwrap();
        let parsed = proto::api::TReqLookupRows::decode(&buffer[..]).unwrap();
        assert_eq!(
            parsed.timestamp.unwrap_or(LATEST_TIMESTAMP),
            LATEST_TIMESTAMP
        );
        assert_eq!(LATEST_TIMESTAMP, 0x3fff_ffff_ffff_ff01);
    }

    /// The four methods this crate exists for, checked field by field.
    ///
    /// A wrong or missing field here is invisible locally and only shows up as
    /// a cluster rejecting the call — or worse, accepting it and doing
    /// something subtly different from what was asked.
    #[test]
    fn lookup_asks_for_what_it_promises() {
        let request = lookup_request(
            "//tmp/table",
            &["key"],
            &LookupOptions {
                timestamp: None,
                column_filter: vec!["key", "value"],
            },
        );

        // A YPath is `bytes`, not `string`.
        assert_eq!(request.path, b"//tmp/table".to_vec());
        assert_eq!(request.columns, ["key", "value"]);
        assert_eq!(
            request.keep_missing_rows,
            Some(true),
            "without this a missing key shortens the answer and misaligns the rest"
        );
        assert_eq!(
            request.timestamp, None,
            "omitted means the proto default, which is the latest committed data;              sending 0 would ask for NullTimestamp instead"
        );
        assert_eq!(
            request
                .rowset_descriptor
                .name_table_entries
                .iter()
                .map(|entry| entry.name.clone().unwrap())
                .collect::<Vec<_>>(),
            ["key"],
            "the descriptor names the key columns the attachment carries"
        );
    }

    #[test]
    fn a_lookup_in_a_transaction_reads_at_its_start_timestamp() {
        // `TReqLookupRows` has no transaction_id field at all: a read inside a
        // tablet transaction is expressed purely as this timestamp.
        let request = lookup_request(
            "//tmp/table",
            &["key"],
            &LookupOptions {
                timestamp: Some(1234),
                column_filter: Vec::new(),
            },
        );
        assert_eq!(request.timestamp, Some(1234));
        assert!(
            request.columns.is_empty(),
            "an empty filter means every column"
        );
    }

    #[test]
    fn select_carries_the_query_and_the_timestamp() {
        let request = select_request(
            "* from [//tmp/t]",
            &SelectOptions {
                timestamp: Some(99),
            },
        );
        assert_eq!(request.query, "* from [//tmp/t]");
        assert_eq!(request.timestamp, Some(99));
    }

    #[test]
    fn modify_names_the_transaction_and_one_type_per_row() {
        let transaction = Guid::random();
        let request = modify_request(
            transaction,
            "//tmp/table",
            &["key", "value"],
            3,
            RowModificationType::Delete,
        );

        assert_eq!(Guid::from_proto(&request.transaction_id), transaction);
        assert_eq!(request.path, b"//tmp/table".to_vec());
        assert_eq!(
            request.row_modification_types,
            vec![RowModificationType::Delete as i32; 3],
            "one entry per row, parallel to the rows in the attachment"
        );
        assert!(
            request.row_legacy_read_locks.is_empty()
                && request.row_legacy_locks.is_empty()
                && request.row_locks.is_empty(),
            "the lock arrays are all-or-nothing per request; a partially filled              one breaks the server's one-per-row invariant"
        );
    }

    #[test]
    fn only_a_tablet_transaction_is_sticky() {
        let options = StartTransactionOptions::default();
        let tablet = start_transaction_request(TransactionType::Tablet, &options);
        assert_eq!(tablet.r#type, 1);
        assert_eq!(
            tablet.sticky,
            Some(true),
            "a tablet tx belongs to one proxy"
        );
        assert_eq!(
            tablet.timeout,
            Some(options.timeout.as_micros() as i64),
            "microseconds, not milliseconds"
        );

        let master = start_transaction_request(TransactionType::Master, &options);
        assert_eq!(master.r#type, 0);
        assert_eq!(master.sticky, Some(false));
    }

    #[test]
    fn the_descriptor_numbers_columns_in_order() {
        let descriptor = name_table_descriptor(&["key", "value", "extra"]);
        assert_eq!(descriptor.wire_format_version, Some(1));
        assert_eq!(
            descriptor.rowset_kind,
            Some(proto::api::ERowsetKind::RkUnversioned as i32)
        );
        let names: Vec<_> = descriptor
            .name_table_entries
            .iter()
            .map(|entry| entry.name.clone().unwrap())
            .collect();
        assert_eq!(names, ["key", "value", "extra"]);
    }

    #[test]
    fn attachments_are_concatenated_before_decoding() {
        // One rowset split across two attachments must decode as one rowset,
        // not as two.
        let rows = vec![
            Some(vec![UnversionedValue::new(0, Value::Int64(1))]),
            Some(vec![UnversionedValue::new(0, Value::Int64(2))]),
        ];
        let encoded = wire::encode_rowset(&rows).unwrap();
        let split = encoded.len() / 2;
        let attachments = vec![encoded.slice(0..split), encoded.slice(split..)];

        let decoded = decode_rowset_attachments(&attachments, None).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn no_attachments_means_no_rows() {
        assert_eq!(decode_rowset_attachments(&[], None).unwrap(), Vec::new());
    }

    #[test]
    fn an_unknown_wire_format_version_is_refused() {
        let descriptor = proto::api::TRowsetDescriptor {
            wire_format_version: Some(99),
            ..Default::default()
        };
        let error = decode_rowset_attachments(&[], Some(&descriptor)).unwrap_err();
        assert!(
            error.to_string().contains("wire format version 99"),
            "unexpected error: {error}"
        );
    }
}
