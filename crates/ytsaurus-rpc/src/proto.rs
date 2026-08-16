//! The generated protobuf types, re-exported.
//!
//! They live in [`ytsaurus_proto`], generated from the upstream `.proto` files
//! in the YTsaurus repository. They are a separate crate so they compile once
//! and are cached: the API service alone is a quarter of a megabyte of Rust,
//! and rebuilding that on every edit to this crate would dominate its build.
//!
//! That crate commits its generated code and has no build script, so nothing
//! here needs `protoc` or the `.proto` files themselves.

pub use ytsaurus_proto::*;
