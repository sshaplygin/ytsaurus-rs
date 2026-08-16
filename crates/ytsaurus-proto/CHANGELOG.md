# Changelog

All notable changes to `ytsaurus-proto` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**, and its contents are generated: what changes here
is which upstream `.proto` files are compiled and which submodule commit they
are taken from.

## 0.3.0 - 2026-08-16

First release of the crate: `prost`-generated bindings for the transitive import
closure of the YTsaurus RPC-proxy API surface — twenty `.proto` files, from
`guid.proto` and `error.proto` up to `api_service.proto`.

- **Added** the generated modules under `nyt`, mirroring the protobuf package
  names exactly — `prost` writes cross-package references as `super::`-relative
  paths, so the nesting is not a matter of taste — with the aliases `api`,
  `bus`, `misc`, `rpc` and `ytree` over the ones the workspace uses.

- **Changed, to make publishing possible: the generated Rust is committed and
  the build script is gone.** It used to run `prost-build` over the
  `third_party/ytsaurus` submodule at build time. `cargo package` does not walk
  into a submodule, so that produced a crate which builds from its own
  repository and fails from the registry — the definitions are not in the
  tarball, and could not be without vendoring a copy of the `.proto` files this
  project deliberately does not keep.

  So `src/generated/` is committed and regeneration is a tool,
  `cargo xtask generate-protos`. The submodule remains the **only** source of
  protobuf definitions — nothing here reads a copy — and CI regenerates and
  fails on a diff, so committed code that has drifted from the pin is a red
  build rather than a discovery made later.

  For a consumer this means the crate builds with neither the submodule nor
  `protoc`, and pulls in neither `prost-build` nor a vendored `protoc` binary:
  the only dependency is `prost`.

The definitions are pinned to submodule commit `c91fcbe2`.
