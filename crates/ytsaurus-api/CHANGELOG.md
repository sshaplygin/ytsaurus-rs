# Changelog

All notable changes to `ytsaurus-api` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**: the interface both transports implement has not
settled, the version is 0.x, and the API may change in a patch release.

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.3.0 - 2026-08-16

First release, as pre-release: the transport-independent YTsaurus client
interface. Published so that `ytsaurus-rpc` and `ytsaurus-client`'s
`create_client` / `create_rpc_client`, which return its `TableClient`, could be.

- Added `TableClient`, the interface the HTTP and RPC clients both implement,
  mirroring `yt/yt/client/api` in the C++ client.
- Added the row model the two transports share: values, rows and the
  unversioned row representation.
- Added the error type the interface returns, including `Unsupported`: a
  capability the API has and a transport does not, such as tablet transactions
  over HTTP.
