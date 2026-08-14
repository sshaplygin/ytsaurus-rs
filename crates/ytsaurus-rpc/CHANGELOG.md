# Changelog

All notable changes to `ytsaurus-rpc` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Rowset encoding validates before it allocates.** A row could contain many
  clones of one 16 MiB `Bytes`, requiring tens of gibibytes of output while
  holding only one backing allocation. The encoder now checks every row limit
  and the 1 GiB RPC attachment limit before reserving, and returns allocation
  failure as an error instead of aborting.
- **Connection backpressure now includes requests already sent to a proxy.**
  One connection accepts at most 256 in-flight calls. A cancellation retains
  that call's slot until the writer handles its packet, so a blocked writer
  cannot fill the cancellation queue and silently lose later cancellations.
- **End-to-end examples have distinct Cargo target names.** Use `rpc_e2e` for
  the RPC client and `client_e2e` for the HTTP client; building all examples no
  longer makes both packages emit `target/.../examples/e2e`.

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
  cluster: `cargo run -p ytsaurus-rpc --example rpc_e2e`.

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

### Found by independent review, before any of this shipped

Four reviewers read the crate against the C++ and Go sources without having
written any of it. The protocol itself came back clean — every byte offset,
enum value, alignment rule and API field checked and sound. What they found was
around it, and all of it is fixed here:

- **Two ways a call could wait for ever.** A deadline did not cover queuing the
  request, so a peer that stopped reading left calls stuck far past their
  timeout — 134 of 200 in the test that found it. And once the reader task
  died, which a single corrupt packet is enough to cause, later calls were
  still queued and then waited for a reply nobody remained to route. Both are
  regression-tested in `connection_failure_modes.rs`.
- **The encoder trusted what the decoder checks**, writing the value length with
  `as u32` and enforcing none of the three rowset limits, so an oversized blob
  wrapped the length word into a silently corrupt stream.
- **Connect and handshake had no deadline**, so a proxy that accepted a
  connection and then said nothing parked the caller indefinitely.
- **Four tests could not fail**, including two that hung rather than failing
  when no cancellation was sent — in CI, indistinguishable from a stuck runner.
- **The four methods the crate exists for had no request coverage.** Request
  building is now separate from calling, and each field is asserted.

### Found by a second review, at the final state

Four more reviewers read the crate in isolated worktrees, where they could
break it freely, and one judge re-checked every finding: 24 of 29 stood. The
protocol was clean again; the defects were around it.

- **Two ways to lose a connection's resources.** `Drop` handed its cleanup to
  `tokio::spawn`, which panics outside a runtime — and a panic while unwinding
  aborts the process. Dropping a `Connection` left its reader task and socket
  alive against any peer that did not close first: measured, 128 descriptors
  and 181 tasks still held after 60 connections were dropped.
- **The packet encoder had the defect the rowset encoder was just fixed for**,
  truncating part sizes to `u32`: a 4 GiB part declared 64 bytes and wrote
  4294967368, after which the peer parses the rest as further packets.
- **The size ceiling did not bound memory.** A part is 12 bytes on the wire and
  about 44 once decoded, so a packet inside a 512 MB ceiling could declare 44
  million empty parts — 2.31 GiB of peak RSS, measured. Part counts now have
  their own, much lower ceiling.
- **Cancellation failed exactly when it mattered**, posted with `try_send` onto
  the request queue whose fullness is what causes timeouts: 72 requests reached
  a stalled proxy and not one cancellation followed. Cancellations now have a
  channel the writer drains first.
- **`init-protos.sh` reported success on a checkout that was not one**, because
  the fetch was guarded on the commit and the commit says nothing about the
  working tree.

And the tests were measured rather than admired: 85 deliberate defects were run
through the suite. Twelve of thirteen in the async client survived, because
nothing outside the live-cluster example ever built a `Client`; four
wire-visible constants were asserted against themselves, so changing the packet
signature or the credentials field number kept the suite green. Both are closed,
and the fourteen reported survivors were re-run afterwards — all fourteen now
fail.

### Not implemented

TLS, compression codecs, versioned rowsets, streaming reads and writes, retry
policies, connection pooling, and HTTP bootstrap discovery. The gates that are
still open — including a differential test against the reference driver, fuzzing
and the benchmark that would justify the project — are listed in the
compatibility document.
