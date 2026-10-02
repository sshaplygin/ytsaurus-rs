# RPC proxy compatibility

Every surface of `ytsaurus-rpc`, its state, and its ship gate.

Status: pre-release. `ytsaurus-proto` and `ytsaurus-rpc` were published at
0.3.0, by human decision (Hard rule 1), before these gates were green; the
gates still apply to the claim of compatibility. The version is 0.x, the API
may change in a patch release, and the scope is transactions, `lookup_rows`,
`select_rows` and `modify_rows`, not the other 150 request types.

## What it is pinned to

| Thing | Pin | Where |
| --- | --- | --- |
| `.proto` files | ytsaurus/ytsaurus `stable/25.4` @ `c91fcbe2cd0b9bf8a2fbae078885b9d423f22b62` | `third_party/ytsaurus` submodule |
| Reference implementation for vectors | Go SDK `yt/go` v0.0.33 | `tests/rpc-go-interop/go.mod` |
| Cluster verified against | `ghcr.io/ytsaurus/local:stable`, server `25.4.260522002` | [`rpc_e2e.rs`](../crates/ytsaurus-rpc/examples/rpc_e2e.rs) runs in the post-merge `Cluster E2E` workflow against the local cluster's RPC proxy |

The protos are a submodule, not a copy: an exact upstream commit,
byte-identical, with nothing fetched at build time.
`stable/25.4` is the branch the local cluster this was verified against runs.
The pin moves only when a human moves it.

## Layer by layer

### Layer 1: bus framing

| Surface | State | Verified by |
| --- | --- | --- |
| Fixed 36-byte packet header | Implemented | Unit tests pin every field offset |
| Variable header (sizes, checksums, trailing checksum) | Implemented | Round-trip and corruption tests |
| `EPacketType` `Message` / `Ack` / `SslAck` | Decoded; only `Message` is sent | Unit tests |
| Null part vs empty part | Implemented, kept distinct | `a_null_part_and_an_empty_part_differ_on_the_wire` |
| CRC-64 | Implemented | 12 canonical vectors + Go-produced bus-shaped vectors |
| `NullChecksum` means "do not verify" | Implemented | `a_null_checksum_means_do_not_verify` |
| Handshake | Implemented | Stub tests + live cluster |
| Delivery-tracking acks | Not implemented; never requested, so never expected | |
| TLS / `SslAck` | Not implemented; a peer that requires encryption is refused, not downgraded | `a_peer_that_requires_encryption_is_refused_not_downgraded` |
| Multiplexing bands | Not implemented; the default band is used | |

The CRC-64 is polynomial `0xE543279765927881` in normal (MSB-first) form, zero
initial value, no final xor, register byte-swapped on output: not ECMA-182,
XZ, ISO or Jones. `crates/ytsaurus-rpc/src/crc64.rs` derives its table from
that constant and checks it against the Go SDK's table and vectors.

### Layer 2: RPC envelope

| Surface | State | Verified by |
| --- | --- | --- |
| Request message layout (type word, header, body, attachments) | Implemented | Unit tests + live cluster |
| Response parsing, including error responses | Implemented | Unit tests + live cluster |
| `TError` with nesting and attributes preserved | Implemented | `nesting_survives_the_conversion` |
| Protocol-level cancellation (`rpcc`) | Implemented, sent when a call times out | `a_timeout_reports_the_method_and_cancels_the_request` |
| Timeouts, in the header and locally | Implemented, and the two agree | Same test |
| In-flight calls and cancellations | Bounded at 256 per connection; cancellation retains its slot until the writer handles it | Connection unit tests |
| Token auth via `TCredentialsExt` (field 110) | Implemented | `the_token_is_appended_as_extension_field_110` |
| Compression codecs | Not implemented; `ECodec::None` only, and the header says so | |
| Streaming payload / feedback messages | Not implemented | |
| Retries and `mutation_id` | Field is plumbed; no retry policy | |

`prost` does not generate proto2 extensions, so the credentials extension is
appended by hand as field 110, which is wire-identical; a test decodes it back
to a `TCredentialsExt`.

### Layer 3: API surface and discovery

| Surface | State |
| --- | --- |
| Generated types for the whole `api_service.proto` | Available through `ytsaurus-proto` |
| `StartTransaction` / `Ping` / `Commit` / `Abort` | Implemented |
| `LookupRows`, `SelectRows`, `ModifyRows` | Implemented |
| `DiscoverProxies` over RPC | Implemented |
| HTTP `discover_proxies` bootstrap | Not implemented; see gate D |
| Connection pool, per-proxy health, banning | Not implemented; one connection per client |
| Every other method of the 158 | Reachable via `Connection::invoke_raw`, not wrapped |

### Layer 4: row wire format

| Surface | State | Verified by |
| --- | --- | --- |
| Unversioned rowset encode and decode | Implemented | Go-produced golden vectors, both directions |
| `Null`, `Int64`, `Uint64`, `Double`, `Boolean`, `String`, `Any` | Implemented | Golden vectors |
| `Composite` | Implemented; diverges from the Go SDK, see below | Round-trip test + a Go test pinning the defect |
| Null row vs empty row | Implemented, kept distinct | `the_null_row_survives_the_reference_bytes` |
| 8-byte alignment and padding | Implemented | Every length 0..24 is walked |
| Aggregate flag | Carried through | `the_aggregate_flag_survives` |
| Aggregate rowset size | Refused above the 1 GiB RPC attachment limit before allocation | `a_rowset_larger_than_one_rpc_attachment_is_refused_before_allocation` |
| Versioned rowsets | Not implemented; only on concrete need | |
| Row-stream block envelope | Not implemented; not used by these methods, see below | |

## Deliberate divergences

- Composite values follow the C++, where `IsStringLikeType` covers `String`,
  `Any` and `Composite`. The Go writer (`yt/go/wire/writer.go`, `writeValue`)
  handles `TypeBytes` and `TypeAny` but not `TypeComposite`: it omits the
  payload while the length word counts it, and its own reader reads it back.
  `TestCompositeWriterDropsItsPayload` in `tests/rpc-go-interop/` pins the
  defect and fails if the Go SDK is fixed.
- Checksums follow the C++, which skips verification when the stored value is
  `NullChecksum` (`packet.cpp` guards all three comparisons with
  `expectedChecksum != NullChecksum`); Go verifies every one. That lets a peer
  with checksums off, or on for its first few parts only, interoperate, and
  rejects nothing the reference server accepts.
- Part size: accepts what the C++ accepts (1 GB per part; Go caps at 512 MB),
  with a much lower default ceiling on the whole packet so a corrupt length
  word cannot reserve unbounded memory.
- No row-stream envelope: the C++ `SerializeRowStreamBlockEnvelope` is for the
  streaming table reader and writer. For request/response methods both
  reference clients put the descriptor in the protobuf message and the raw
  rowset in the attachments: C++
  `DeserializeRowset(rsp->rowset_descriptor(), MergeRefsToRef(rsp->Attachments()))`,
  Go `decodeFromWire(rsp.Attachments)`. An envelope here would be wrong.

## Ship gates

All must be green before this is called anything but pre-release. None has
been relaxed.

- A, the sans-io layers are checked against a reference: green for two of
  four. Rowset vectors come from the pinned Go SDK, consumed in both
  directions; the CRC-64 matches twelve canonical vectors plus bus-shaped ones.
  Bus framing and the RPC envelope are checked only against this crate's own
  encoder and a live proxy. The Go packet encoder is unexported, so closing
  this means capturing bytes off a real proxy as fixtures.
- B, a live proxy and this crate understand each other: green.
  `cargo run -p ytsaurus-rpc --example rpc_e2e` writes, looks up, selects and
  deletes in the post-merge `Cluster E2E` workflow.
- C, a differential test against the reference driver: not started. No test
  runs the same operation through `ytsaurus-rpc-driver` and compares row for
  row; agreement with the reference is from reading the Go and C++ sources.
- D, discovery without a known proxy: not started. `DiscoverProxies` over RPC
  needs a proxy already. The HTTP `discover_proxies` route both reference
  clients use would add an HTTP stack to this crate; `ytsaurus-client` can
  answer it.
- E, the parsers are fuzzed: not started. The packet and rowset decoders read
  untrusted bytes; they have exhaustive truncation tests only.
- F2, the connection's failure modes are covered: green.
  `connection_failure_modes.rs` tests the two defects that shipped: a deadline
  not covering queuing, and a dead reader leaving later calls waiting for ever.
- F, a connection survives a proxy dying: not started. An in-flight call fails
  cleanly on a dropped connection (tested); there is no reconnection or
  per-proxy banning.
- G, the numbers that justify the project: not started. Latency and
  throughput under concurrency, the case for RPC over HTTP, are unmeasured
  (#67); until then this is a protocol implementation, not a recommendation.
- H, publishable: green. `cargo package` omits submodules, so
  `ytsaurus-proto` commits its generated bindings, with no build script; CI
  regenerates them and fails on a diff.

## Running the checks

```sh
./scripts/init-protos.sh                       # only to regenerate ytsaurus-proto
cargo test -p ytsaurus-rpc                     # unit tests + golden vectors
cd tests/rpc-go-interop && go test ./...       # regenerate the vectors
cargo run -p ytsaurus-rpc --example rpc_e2e    # against a live RPC proxy
```
