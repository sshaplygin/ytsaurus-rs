//! A client for the YTsaurus **RPC proxy**.
//!
//! HTTP API v4 — which [`ytsaurus-client`](https://docs.rs/ytsaurus-client) speaks —
//! can already reach every dynamic-table command. This crate exists for
//! latency and throughput under concurrency, never for capability: one
//! connection multiplexes many in-flight requests, where HTTP pays its
//! per-request cost every time.
//!
//! # The protocol is four layers, and only the top one looks familiar
//!
//! | Layer | What it is | Module |
//! | --- | --- | --- |
//! | 1 | **Bus** — framed, checksummed packets over TCP | [`bus`] |
//! | 2 | **RPC envelope** — request and response headers, `TError` | [`rpc`] |
//! | 3 | **API surface** — generated protobuf | [`proto`] |
//! | 4 | **Row wire format** — rows in attachments, not protobuf fields | [`wire`] |
//!
//! Layer 4 is the one that surprises people: rows do **not** travel as
//! protobuf. `api_service.proto` says outright that "actual data is passed via
//! attachments in the wire protocol", and that format is neither YSON nor
//! Skiff — it is a third one, mandatory for every dynamic-table read and write.
//!
//! # Shape of the code
//!
//! The parsers are **sans-io**: [`crc64`], [`bus::packet`], [`rpc`] and
//! [`wire`] are pure functions from bytes to values, with no `async` anywhere,
//! so every one of them is tested and fuzzed without a runtime. `async` appears
//! only at the I/O edges — [`bus::Bus`] and [`connection::Connection`].
//!
//! # Example
//!
//! ```no_run
//! use ytsaurus_rpc::client::{Client, LookupOptions};
//! use ytsaurus_rpc::wire::{UnversionedValue, Value};
//!
//! # async fn example() -> ytsaurus_rpc::error::Result<()> {
//! let client = Client::connect("localhost:8011").await?;
//!
//! let key = vec![UnversionedValue::new(0, Value::Int64(42))];
//! let rows = client
//!     .lookup_rows("//tmp/table", &["key"], &[key], LookupOptions::default())
//!     .await?;
//!
//! // One entry per key asked for, in order; `None` where the key had no row.
//! for row in rows {
//!     println!("{row:?}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Status
//!
//! Pre-release, and not published. What is implemented, what is deliberately
//! left out and what has actually been run against a cluster are listed in
//! `docs/rpc-compatibility.md`.

pub mod bus;
pub mod client;
pub mod connection;
pub mod crc64;
pub mod error;
pub mod guid;
pub mod proto;
pub mod rpc;
pub mod wire;

pub use client::{Client, ClientBuilder, Transaction};
pub use error::{Error, Result, YtError};
pub use guid::Guid;
pub use wire::{Row, UnversionedValue, Value, ValueType};
