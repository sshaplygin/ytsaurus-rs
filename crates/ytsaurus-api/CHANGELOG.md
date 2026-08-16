# Changelog

All notable changes to `ytsaurus-api` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**. It is the interface both transports implement, it
has not settled, and it is the one thing in this workspace that is expensive to
change afterwards — the version is 0.x and the API may change in a patch
release.

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.
`ytsaurus-skiff` and `ytsaurus-job` shipped a test file each that could not
compile from their tarballs; this crate had no such file.

## 0.3.0 - 2026-08-16

First release of the crate: the **transport-independent** YTsaurus client
interface, and the row model both transports speak.

Published, as **pre-release**, by the human decision Hard rule 1 asks for. What
the publish buys is `ytsaurus-rpc` and `ytsaurus-client`'s `create_client` /
`create_rpc_client`, which return this crate's `TableClient` and could not reach
crates.io while this was `publish = false`.

- **Added** `TableClient`, the interface an HTTP client and an RPC client both
  implement, so choosing a transport is one line and nothing below it changes.
  This mirrors `yt/yt/client/api` in the C++ client, where the same split is
  what lets a caller be written once and run over either.
- **Added** the row model the two transports share — values, rows and the
  unversioned row representation — so a row does not have to be translated
  between them.
- **Added** the error type the interface returns, including `Unsupported`, which
  is how a transport says that a capability exists in the API and not in it.
  Tablet transactions over HTTP are the case that made it necessary: they are
  sticky to the proxy that created them, and an HTTP client routes each request
  independently.
