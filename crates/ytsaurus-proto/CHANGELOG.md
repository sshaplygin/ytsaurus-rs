# Changelog

All notable changes to `ytsaurus-proto` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**, and its contents are generated: what changes here
is which upstream `.proto` files are compiled and which submodule commit they
are taken from.

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.3.0 - 2026-08-16

First release: `prost`-generated bindings for the transitive import closure of
the YTsaurus RPC-proxy API surface, twenty `.proto` files from `guid.proto` and
`error.proto` up to `api_service.proto`, pinned to submodule commit `c91fcbe2`.

- Added the generated modules under `nyt`, nested exactly as the protobuf
  packages (`prost` writes cross-package references as `super::` paths), with
  the aliases `api`, `bus`, `misc`, `rpc` and `ytree`.
- Changed: the generated Rust is committed and there is no build script, so the
  crate builds from the registry with neither the submodule nor `protoc`, and
  depends only on `prost`. Regenerate with `cargo xtask generate-protos`; CI
  fails on a diff.
