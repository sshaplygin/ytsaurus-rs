# Changelog

All notable changes to `ytsaurus-rpc` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**. The ship gates in
[docs/rpc-compatibility.md](../../docs/rpc-compatibility.md) are not all green
(gate D, the HTTP `discover_proxies` bootstrap, is open), and the API may change
in a patch release. The scope is deliberately narrow: transactions,
`lookup_rows`, `select_rows` and `modify_rows`, not the other 150 request types.

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.3.0 - 2026-08-16

First release, as pre-release.

### Fixed

- Rowset encoding checks every row limit and the 1 GiB RPC attachment limit
  before reserving memory, and returns an allocation failure as an error
  instead of aborting.
- Connection backpressure counts requests already sent: at most 256 calls in
  flight per connection, and a cancellation keeps its call's slot until the
  writer handles it, so later cancellations are not lost.
- The end-to-end examples have distinct Cargo target names: `rpc_e2e` for the
  RPC client, `client_e2e` for the HTTP client.

### Added

A client for the YTsaurus RPC proxy, covering all four protocol layers from the
TCP framing up.

- Bus (layer 1): packet encode and decode with CRC-64 verification, the
  handshake, and sans-io incremental decoding that consumes nothing until a
  whole packet has arrived.
- YTsaurus's CRC-64, which is none of the named variants: polynomial
  `0xE543279765927881`, table derived at compile time and checked against the
  Go SDK's table and canonical vectors.
- RPC envelope (layer 2): request and response messages, `TError` with nesting
  and attributes preserved, per-request timeouts sent to the server and applied
  locally, protocol-level cancellation, and token authentication through
  `TCredentialsExt`.
- Row wire format (layer 4): unversioned rowsets, every value type, the 8-byte
  alignment and padding rule, and the null row a lookup returns for a missing
  key.
- A connection actor multiplexing concurrent requests, with a bounded writer
  channel and a reader task routing each response to its caller.
- The API subset: `start_transaction`, `ping`, `commit`, `abort`,
  `lookup_rows`, `select_rows`, `modify_rows` and `DiscoverProxies`. Any other
  method is reachable through `Connection::invoke_raw`.
- Golden vectors from `tests/rpc-go-interop/`, a Go program pinned to yt/go
  v0.0.33, consumed by the Rust tests in both directions.
- An end-to-end example that writes, looks up, selects and deletes on a real
  cluster: `cargo run -p ytsaurus-rpc --example rpc_e2e`.

### Deliberate divergences from the reference clients

Recorded in [docs/rpc-compatibility.md](../../docs/rpc-compatibility.md).

- Composite values keep their payload, as in the C++; the Go SDK's writer
  omits it. A test in the Go harness pins the Go defect.
- A zero checksum (`NullChecksum`) means "do not verify", as in the C++; the
  Go SDK verifies every checksum.
- The major protocol version is per service: `ApiService` is at 1 and
  `DiscoveryService` at 0, and a real proxy refuses a call to the latter that
  announces 1.

### Found by independent review, before any of this shipped

- A call's deadline covers queuing the request, and a call made after the
  reader task died fails instead of waiting for ever. Both are tested in
  `connection_failure_modes.rs`.
- The rowset encoder enforces the three rowset limits the decoder checks, so an
  oversized value is refused instead of wrapping its length word into a
  corrupt stream.
- Connect and handshake have a deadline.
- Four tests that could not fail now can.
- The four core methods have request coverage: request building is separate
  from calling, and each field is asserted.

### Found by a second review, at the final state

- Dropping a `Connection` outside a runtime no longer panics, and it releases
  its reader task and socket even when the peer does not close first.
- The packet encoder refuses a part above the part-size limit instead of
  truncating its size to `u32`.
- Part counts have their own ceiling, much lower than the packet size ceiling,
  so a packet of many empty parts cannot exhaust memory.
- Cancellations have their own channel, drained by the writer first, so a full
  request queue no longer swallows them.
- `init-protos.sh` checks the working tree, not only the fetched commit, before
  reporting success.
- The async `Client` is tested outside the live-cluster example, and the
  wire-visible constants (packet signature, credentials field number) are
  asserted against fixed values.

### Added later in the same cycle

- `ytsaurus_rpc::blocking`, a blocking facade in the shape of
  `reqwest::blocking`: a private current-thread runtime, one call at a time. It
  implements `ytsaurus_api::TableClient`, so `ytsaurus_client::create_rpc_client`
  returns the same interface as `create_client`. The async `Client`, which
  multiplexes, is unchanged.
- `lookup_rows_with_columns` and the transaction's `select_rows_with_columns`,
  which also return the reply's column names.

### Not implemented

TLS, compression codecs, versioned rowsets, streaming reads and writes, retry
policies, connection pooling, and HTTP bootstrap discovery. The open gates are
listed in the compatibility document.
