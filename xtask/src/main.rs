//! Repository tasks.
//!
//! ```sh
//! cargo xtask generate-protos   # or: cargo run -p xtask -- generate-protos
//! ```

use std::io::Result;
use std::path::{Path, PathBuf};

/// The include root. `.proto` files import each other by paths that begin with
/// `yt_proto/`, so this is the directory those paths are relative to.
const INCLUDE_ROOT: &str = "third_party/ytsaurus/yt";

/// Where the generated modules land. Committed, and what `ytsaurus-proto`'s
/// `lib.rs` includes.
const OUT_DIR: &str = "crates/ytsaurus-proto/src/generated";

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
    match std::env::args().nth(1).as_deref() {
        Some("generate-protos") => generate_protos(),
        other => {
            eprintln!(
                "unknown task {:?}\n\ntasks:\n    generate-protos    regenerate \
                 ytsaurus-proto/src/generated from the submodule\n",
                other.unwrap_or("<none>")
            );
            std::process::exit(2);
        }
    }
}

/// Regenerates `ytsaurus-proto`'s committed bindings from the pinned submodule.
///
/// This was a `build.rs` in `ytsaurus-proto` until that crate was published.
/// `cargo package` does not walk into a submodule, so a build script reading
/// `third_party/ytsaurus` produces a crate that builds from this repository and
/// fails from the registry — the definitions are not in the tarball and cannot
/// be, short of vendoring a copy of the `.proto` files this project
/// deliberately does not keep.
///
/// So the *generated Rust* is committed and this is the tool that writes it.
/// The submodule stays the only source of protobuf definitions — nothing reads
/// a copy — and a consumer building from crates.io needs neither the submodule
/// nor `protoc`. CI runs this and fails on a diff, so committed code that has
/// drifted from the pinned submodule is a red build rather than a surprise
/// later.
fn generate_protos() -> Result<()> {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask/ has a parent")
        .to_path_buf();
    let root = repo.join(INCLUDE_ROOT);
    let out = repo.join(OUT_DIR);

    // An uninitialised submodule is the one failure a contributor will hit, so
    // it says what to run rather than reporting a missing file.
    if !root.join(PROTOS[0]).exists() {
        panic!(
            "the YTsaurus protos are missing from {}\n\
             \n\
             They live in the `third_party/ytsaurus` submodule. Run:\n\
             \n    ./scripts/init-protos.sh\n",
            root.display()
        );
    }

    // Stale output is worse than none: a `.proto` dropped from the list above
    // would otherwise leave its module behind, still compiling and no longer
    // generated from anything.
    if out.exists() {
        std::fs::remove_dir_all(&out)?;
    }
    std::fs::create_dir_all(&out)?;

    // A `protoc` on PATH wins, so a contributor can point at their own; the
    // vendored binary is what makes regeneration work with nothing but the Rust
    // toolchain installed, which is the standard this repository already holds
    // `scripts/build-worker.sh` to.
    if std::env::var_os("PROTOC").is_none()
        && let Ok(path) = protoc_bin_vendored::protoc_bin_path()
    {
        // SAFETY: single-threaded, before any thread is spawned.
        unsafe { std::env::set_var("PROTOC", path) };
    }

    let paths: Vec<PathBuf> = PROTOS.iter().map(|proto| root.join(proto)).collect();
    // YTsaurus protos are proto2 and lean on `required`. `prost` renders a
    // `required` field as a plain value and an `optional` one as an `Option`,
    // so the two spellings are visible in the generated types and callers have
    // to match them field by field.
    prost_build::Config::new()
        .out_dir(&out)
        .compile_protos(&paths, &[root.as_path() as &Path])?;

    println!("generated into {}", out.display());
    Ok(())
}
