//! Generates the protobuf bindings from the upstream `.proto` files.
//!
//! The files are read out of the `third_party/ytsaurus` submodule, not from a
//! copy in this repository. A submodule records an exact upstream commit, so
//! the definitions are pinned in the way the protocol work requires while
//! staying byte-identical to upstream's — there is no vendored copy that can
//! drift, and nothing is fetched at build time.

use std::io::Result;
use std::path::{Path, PathBuf};

/// The include root. `.proto` files import each other by paths that begin with
/// `yt_proto/`, so this is the directory those paths are relative to.
const INCLUDE_ROOT: &str = "../../third_party/ytsaurus/yt";

/// The transitive import closure of the RPC-proxy API surface.
///
/// Listed rather than globbed: this is the set the crate commits to compiling,
/// and a file appearing upstream should not silently enlarge it.
const PROTOS: &[&str] = &[
    "yt_proto/yt/core/misc/proto/guid.proto",
    "yt_proto/yt/core/misc/proto/error.proto",
    "yt_proto/yt/core/misc/proto/hyperloglog.proto",
    "yt_proto/yt/core/bus/proto/bus.proto",
    "yt_proto/yt/core/rpc/proto/rpc.proto",
    "yt_proto/yt/core/tracing/proto/tracing_ext.proto",
    "yt_proto/yt/core/ytree/proto/attributes.proto",
    "yt_proto/yt/core/ytree/proto/request_complexity_limits.proto",
    "yt_proto/yt/core/yson/proto/protobuf_interop.proto",
    "yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto",
    "yt_proto/yt/client/api/rpc_proxy/proto/discovery_service.proto",
    "yt_proto/yt/client/chaos_client/proto/replication_card.proto",
    "yt_proto/yt/client/chunk_client/proto/data_statistics.proto",
    "yt_proto/yt/client/hive/proto/timestamp_map.proto",
    "yt_proto/yt/client/node_tracker_client/proto/node.proto",
    "yt_proto/yt/client/scheduler/proto/spec_patch.proto",
    "yt_proto/yt/client/table_chunk_format/proto/chunk_meta.proto",
    "yt_proto/yt/client/table_chunk_format/proto/column_meta.proto",
    "yt_proto/yt/client/table_client/proto/versioned_io_options.proto",
    "yt_proto/yt/client/tablet_client/proto/lock_mask.proto",
];

fn main() -> Result<()> {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join(INCLUDE_ROOT);

    // An uninitialised submodule is the one build failure a contributor will
    // hit, so it says what to run rather than reporting a missing file.
    if !root.join(PROTOS[0]).exists() {
        panic!(
            "the YTsaurus protos are missing from {}\n\
             \n\
             They live in the `third_party/ytsaurus` submodule. Run:\n\
             \n    ./scripts/init-protos.sh\n",
            root.display()
        );
    }

    // A checked-out submodule's files change only when the pin moves, and the
    // pin is a tracked file, so watching it is enough to catch that.
    println!("cargo:rerun-if-changed=build.rs");
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={}", root.join(proto).display());
    }

    // A `protoc` on PATH wins, so a contributor can point at their own; the
    // vendored binary is what makes `cargo test --workspace` work with nothing
    // but the Rust toolchain installed, which is the standard this repository
    // already holds `scripts/build-worker.sh` to.
    if std::env::var_os("PROTOC").is_none()
        && let Ok(path) = protoc_bin_vendored::protoc_bin_path()
    {
        // SAFETY: single-threaded build script, before any thread is spawned.
        unsafe { std::env::set_var("PROTOC", path) };
    }

    let paths: Vec<PathBuf> = PROTOS.iter().map(|proto| root.join(proto)).collect();
    // YTsaurus protos are proto2 and lean on `required`. `prost` renders a
    // `required` field as a plain value and an `optional` one as an `Option`,
    // so the two spellings are visible in the generated types and callers have
    // to match them field by field.
    prost_build::Config::new().compile_protos(&paths, &[root.as_path() as &Path])?;
    Ok(())
}
