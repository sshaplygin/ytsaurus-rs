//! The generated protobuf types, re-exported.
//!
//! They live in [`ytsaurus_proto`], which builds them from the upstream
//! `.proto` files in the `third_party/ytsaurus` submodule. Codegen is a
//! separate crate so it compiles once and is cached: the API service alone
//! generates a quarter of a megabyte of Rust, and rebuilding that on every edit
//! to this crate would dominate its build.

pub use ytsaurus_proto::*;
