# Changelog

All notable changes to `ytsaurus-rpc` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

First release of the crate: a client for the YTsaurus **RPC proxy**, covering
all four layers of the protocol from the TCP framing up.

- **Bus, layer 1.** Packet encode and decode with CRC-64 verification, the
  handshake, and incremental decoding that consumes nothing until a whole
  packet has arrived. The codec is sans-io, so it is tested without a runtime.
- **YTsaurus's CRC-64.** Not one of the named variants: polynomial
  `0xE543279765927881` in normal form, zero initial value, no final xor, and
  the register byte-swapped on the way out. The table is derived at compile time
  from that single constant rather than copied, and checked against the Go SDK's
  table and all twelve of its canonical vectors.
- **RPC envelope, layer 2.** Request and response messages, `TError` with its
  nesting and attributes preserved, per-request timeouts that are sent to the
  server as well as applied locally, protocol-level cancellation, and token
  authentication through the `TCredentialsExt` extension.
- **Row wire format, layer 4.** Unversioned rowsets — the format dynamic tables
  actually use, which is neither YSON nor Skiff. Every value type, the 8-byte
  alignment and padding rule, and the null row that a lookup uses to say "no
  row for this key".
- **A connection actor.** One connection multiplexes concurrent requests: a
  writer task drains a bounded channel so backpressure is real, and a reader
  task routes each response to the caller waiting on it.
- **The API subset.** `start_transaction`, `ping`, `commit`, `abort`,
  `lookup_rows`, `select_rows`, `modify_rows`, and `DiscoverProxies`. Any other
  method is reachable through `Connection::invoke_raw`.
- **Golden vectors produced by the reference implementation.**
  `tests/rpc-go-interop/` is a Go program pinned to yt/go v0.0.33 that emits
  byte vectors the Rust tests consume in both directions — the same arrangement
  `tests/skiff-go-interop/` uses for Skiff.
- **An end-to-end example** that writes, looks up, selects and deletes on a real
  cluster: `cargo run -p ytsaurus-rpc --example e2e`.

### Deliberate divergences from the reference clients

Recorded in full in [docs/rpc-compatibility.md](../../docs/rpc-compatibility.md).

- **Composite values keep their payload.** The Go SDK's writer omits
  `TypeComposite` from the branch that writes the blob, so a composite value it
  encodes arrives empty while its length word still claims the bytes are there.
  The C++ treats `Composite` as string-like everywhere and so does this crate.
  A test in the Go harness pins the defect, so it fails if the SDK is fixed.
- **A zero checksum means "do not verify".** The Go SDK compares every checksum
  unconditionally; the C++ skips the comparison when the stored value is
  `NullChecksum`, which is how a peer that does not checksum interoperates.
  This follows the C++.
- **The major protocol version is per service, not per connection.**
  `ApiService` is at 1 and `DiscoveryService` is still at 0; announcing 1 to the
  latter is refused outright by a real proxy. The C++ takes the version from
  each service's descriptor, so it is derivable from the sources — but it is
  stated nowhere as a rule, and a live proxy is what surfaced it here.

### Not implemented

TLS, compression codecs, versioned rowsets, streaming reads and writes, retry
policies, connection pooling, and HTTP bootstrap discovery. The gates that are
still open — including a differential test against the reference driver, fuzzing
and the benchmark that would justify the project — are listed in the
compatibility document.
