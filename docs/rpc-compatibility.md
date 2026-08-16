# RPC proxy compatibility

What `ytsaurus-rpc` implements, what it deliberately does not, and what has
actually been run against a cluster. Same job as
[skiff-compatibility.md](skiff-compatibility.md) and written for the same
reason: a reverse-engineered binary protocol needs a contract naming every
surface, its state and its ship gate, or "compatible" means whatever the reader
hopes.

**Status: published from 0.3.0, and pre-release.** Both `ytsaurus-proto` and
`ytsaurus-rpc` are on crates.io, by the explicit human decision Hard rule 1
asks for, and the gates below are **still not all green** — A is green for two
of its four layers, and C, D and E are open. The publish changes nothing about
that: the version is 0.x, the API may change in a patch release, and the scope
stays deliberately narrow — transactions, `lookup_rows`, `select_rows` and
`modify_rows`, not the other 150 request types. This is the same arrangement
`ytsaurus-skiff` has had since 0.2.5, and for the same reason: `ytsaurus-client`
gained `create_rpc_client`, which returns a `ytsaurus_api::TableClient` backed by
this crate, and could not reach the registry while this one was `publish =
false`.

## What it is pinned to

| Thing | Pin | Where |
| --- | --- | --- |
| `.proto` files | ytsaurus/ytsaurus `stable/25.4` @ `c91fcbe2cd0b9bf8a2fbae078885b9d423f22b62` | `third_party/ytsaurus` submodule |
| Reference implementation for vectors | Go SDK `yt/go` v0.0.33 | `tests/rpc-go-interop/go.mod` |
| Cluster verified against | `ghcr.io/ytsaurus/local:stable`, server `25.4.260522002` | recorded here; the run is `crates/ytsaurus-rpc/examples/rpc_e2e.rs`, which is not in CI |

The protos are a **submodule, not a copy**. A submodule records an exact
upstream commit, so the definitions are pinned the way protocol work needs
while staying byte-identical to upstream's — there is no vendored copy that can
silently drift, and nothing is fetched at build time. The pin moves only when a
human moves it.

`stable/25.4` rather than `main` because it is the branch the local cluster this
was verified against actually runs.

## Layer by layer

### Layer 1 — bus framing

| Surface | State | Verified by |
| --- | --- | --- |
| Fixed 36-byte packet header | Implemented | Unit tests pin every field offset |
| Variable header (sizes, checksums, trailing checksum) | Implemented | Round-trip and corruption tests |
| `EPacketType` `Message` / `Ack` / `SslAck` | Decoded; only `Message` is sent | Unit tests |
| Null part vs empty part | Implemented, kept distinct | `a_null_part_and_an_empty_part_differ_on_the_wire` |
| CRC-64 | Implemented | 12 canonical vectors + Go-produced bus-shaped vectors |
| `NullChecksum` means "do not verify" | Implemented | `a_null_checksum_means_do_not_verify` |
| Handshake | Implemented | Stub tests + live cluster |
| Delivery-tracking acks | **Not implemented** — never requested, so never expected | — |
| TLS / `SslAck` | **Not implemented**; a peer that requires encryption is refused, not downgraded | `a_peer_that_requires_encryption_is_refused_not_downgraded` |
| Multiplexing bands | **Not implemented** — the default band is used | — |

The CRC-64 is worth stating exactly, because reaching for an off-the-shelf one
gives wrong answers: **polynomial `0xE543279765927881` in normal (MSB-first)
form, zero initial value, no final xor, and the register byte-swapped on the way
out.** It is not ECMA-182, not XZ, not ISO, not Jones. `crates/ytsaurus-rpc/src/crc64.rs`
derives its table from that one constant and checks it against the Go SDK's
table and vectors.

### Layer 2 — RPC envelope

| Surface | State | Verified by |
| --- | --- | --- |
| Request message layout (type word, header, body, attachments) | Implemented | Unit tests + live cluster |
| Response parsing, including error responses | Implemented | Unit tests + live cluster |
| `TError` with nesting and attributes preserved | Implemented | `nesting_survives_the_conversion` |
| Protocol-level cancellation (`rpcc`) | Implemented, sent when a call times out | `a_timeout_reports_the_method_and_cancels_the_request` |
| Timeouts, in the header and locally | Implemented, and the two agree | Same test |
| In-flight calls and cancellations | Bounded at 256 per connection; cancellation retains its slot until the writer handles it | Connection unit tests |
| Token auth via `TCredentialsExt` (field 110) | Implemented | `the_token_is_appended_as_extension_field_110` |
| Compression codecs | **Not implemented** — `ECodec::None` only, and the header says so | — |
| Streaming payload / feedback messages | **Not implemented** | — |
| Retries and `mutation_id` | Field is plumbed; **no retry policy** | — |

`prost` does not generate proto2 extensions, so the credentials extension is
appended by hand as field 110. That is wire-identical — an extension is an
ordinary field with a reserved number — and a test decodes the bytes back to a
`TCredentialsExt` to prove it.

### Layer 3 — API surface and discovery

| Surface | State |
| --- | --- |
| Generated types for the whole `api_service.proto` | Available through `ytsaurus-proto` |
| `StartTransaction` / `Ping` / `Commit` / `Abort` | Implemented |
| `LookupRows`, `SelectRows`, `ModifyRows` | Implemented |
| `DiscoverProxies` over RPC | Implemented |
| HTTP `discover_proxies` bootstrap | **Not implemented** — see gate D |
| Connection pool, per-proxy health, banning | **Not implemented** — one connection per client |
| Every other method of the 158 | Reachable via `Connection::invoke_raw`, not wrapped |

### Layer 4 — row wire format

| Surface | State | Verified by |
| --- | --- | --- |
| Unversioned rowset encode and decode | Implemented | Go-produced golden vectors, both directions |
| `Null`, `Int64`, `Uint64`, `Double`, `Boolean`, `String`, `Any` | Implemented | Golden vectors |
| `Composite` | Implemented — **diverges from the Go SDK, see below** | Round-trip test + a Go test pinning the defect |
| Null row vs empty row | Implemented, kept distinct | `the_null_row_survives_the_reference_bytes` |
| 8-byte alignment and padding | Implemented | Every length 0..24 is walked |
| Aggregate flag | Carried through | `the_aggregate_flag_survives` |
| Aggregate rowset size | Refused above the 1 GiB RPC attachment limit before allocation | `a_rowset_larger_than_one_rpc_attachment_is_refused_before_allocation` |
| Versioned rowsets | **Not implemented** — only on concrete need | — |
| Row-stream block envelope | **Not implemented** — not used by these methods; see below | — |

## Deliberate divergences

**Composite values.** The Go SDK's writer (`yt/go/wire/writer.go`, `writeValue`)
handles `TypeBytes` and `TypeAny` but not `TypeComposite`, so a composite
value's payload is never written — while its length word still claims the bytes
are there and its own reader will read them back. The C++ treats `Composite` as
string-like everywhere (`IsStringLikeType` covers `String`, `Any` and
`Composite`), and this crate follows the C++. The defect is pinned by
`TestCompositeWriterDropsItsPayload` in `tests/rpc-go-interop/`, so if the Go
SDK is fixed, that test fails and this note gets revisited.

**Checksum verification.** The Go SDK verifies every checksum unconditionally.
The C++ skips verification when the stored value is `NullChecksum`
(`packet.cpp` guards all three comparisons with `expectedChecksum != NullChecksum`),
which is how a peer with checksums off — or one that checksums only its first
few parts — interoperates. This crate follows the C++, which is strictly more
permissive and cannot reject traffic the reference server considers valid.

**Part size limit.** The C++ allows 1 GB per part; the Go SDK caps at 512 MB.
This crate follows the C++ for what it will *accept*, and applies a much lower
default ceiling on the whole packet so a corrupt length word cannot make it
reserve unbounded memory.

**No row-stream envelope.** `SerializeRowStreamBlockEnvelope` in the C++ wraps
rowsets in a block envelope of part counts and lengths. That is the *streaming*
path (table reader and writer). For request/response methods — the ones here —
both reference clients put the descriptor in the protobuf message and the raw
rowset bytes in the attachments, with no envelope: C++
`DeserializeRowset(rsp->rowset_descriptor(), MergeRefsToRef(rsp->Attachments()))`
and Go `decodeFromWire(rsp.Attachments)`. Implementing the envelope here would
be wrong, not merely extra.

## Ship gates

Each must be green before this is described as anything but pre-release. They
were written as gates on *publishing* as well, and publishing happened first, at
0.3.0, by a human decision recorded above — so what they now gate is the claim,
not the upload. Nothing below has been relaxed to match.

- **A — the sans-io layers are checked against a reference, not against
  themselves.** *Green for two of the four.* The row wire format has rowset
  vectors produced by the pinned Go SDK and consumed in both directions, and
  the CRC-64 matches all twelve canonical vectors plus bus-shaped ones.
  **Bus framing and the RPC envelope have no reference-produced vectors**:
  both are checked against this crate's own encoder, plus a live proxy that
  accepts what they write and whose replies they read. The Go SDK's packet
  encoder is unexported, so closing this properly means capturing bytes off a
  real proxy and keeping them as fixtures.
- **B — a live proxy accepts what this writes and this reads what it sends.**
  *Green.* `cargo run -p ytsaurus-rpc --example rpc_e2e` writes, looks up, selects
  and deletes on a real cluster. Not in CI: it needs a multi-GB image and an
  RPC-enabled cluster.
- **C — a differential test against the reference driver.** *Not started.* The
  Go and C++ sources were read closely, but no test yet performs the same
  operation through `ytsaurus-rpc-driver` and compares row for row. Until this
  is green, "agrees with the reference implementation" is a claim about reading,
  not about running.
- **D — discovery bootstraps without being told a proxy.** *Not started.*
  `DiscoverProxies` over RPC works, but it needs a proxy already. The HTTP
  `discover_proxies` route both reference clients use is not implemented,
  because it would add an HTTP stack to a crate that has none;
  `ytsaurus-client` already speaks HTTP v4 and can answer it.
- **E — the parsers are fuzzed.** *Not started.* Both the packet decoder and the
  rowset decoder consume untrusted bytes off a socket. There are exhaustive
  truncation tests, which is not the same thing.
- **F2 — the connection's failure modes are covered.** *Green.*
  `connection_failure_modes.rs` holds one test per defect that shipped: a
  deadline that did not cover queuing the request, and a dead reader that left
  later calls waiting for ever. Both were found by review rather than by use,
  which is the argument for keeping the tests.
- **F — a connection survives a proxy dying.** *Not started.* An in-flight call
  fails cleanly when the connection drops, which is tested; reconnection and
  per-proxy banning are not implemented.
- **G — the numbers that justify the project.** *Not started.* The case for RPC
  over HTTP is latency and throughput under concurrency, and it is unmeasured
  here. Until it is, this crate is a protocol implementation, not a
  recommendation.
- **H — publishable.** *Not started.* `cargo package` does not include
  submodules, so publishing `ytsaurus-proto` needs the generated bindings
  committed, or the protos vendored at package time. Hard rule 1 governs the
  release itself.

## Running the checks

```sh
./scripts/init-protos.sh                       # once, after cloning
cargo test -p ytsaurus-rpc                     # unit tests + golden vectors
cd tests/rpc-go-interop && go test ./...       # regenerate the vectors
cargo run -p ytsaurus-rpc --example rpc_e2e    # against a live RPC proxy
```
