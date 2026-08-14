//! Generated protobuf bindings for the YTsaurus RPC proxy.
//!
//! `build.rs` runs `prost-build` over the upstream `.proto` files in the
//! `third_party/ytsaurus` submodule — not over a copy of them. The submodule
//! records an exact upstream commit, so the definitions are pinned without a
//! vendored copy that could drift, and nothing is fetched while building.
//!
//! Run `./scripts/init-protos.sh` once after cloning; it checks the submodule
//! out shallow and sparse, so it costs about 23 MB rather than the whole
//! monorepo.
//!
//! `prost` names a module after its protobuf package, and writes generated
//! cross-package references as `super::`-relative paths, so the nesting below
//! is not a matter of taste — it has to mirror the package names exactly or
//! the generated code does not resolve.
//!
//! Everything under [`nyt`] is generated. The aliases beneath it are this
//! crate's own, and are the names the rest of the workspace uses.
//!
//! ```
//! use ytsaurus_proto::rpc::TRequestHeader;
//!
//! let header = TRequestHeader {
//!     service: "ApiService".to_owned(),
//!     method: "LookupRows".to_owned(),
//!     ..Default::default()
//! };
//! assert_eq!(header.service, "ApiService");
//! ```

#![allow(clippy::doc_overindented_list_items, clippy::enum_variant_names)]

/// Generated code, in modules mirroring the protobuf package names.
pub mod nyt {
    pub mod n_proto {
        include!(concat!(env!("OUT_DIR"), "/nyt.n_proto.rs"));
    }

    pub mod n_bus {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_bus.n_proto.rs"));
        }
    }

    pub mod n_rpc {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_rpc.n_proto.rs"));
        }
    }

    pub mod n_tracing {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_tracing.n_proto.rs"));
        }
    }

    pub mod ny_tree {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.ny_tree.n_proto.rs"));
        }
    }

    pub mod n_yson {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_yson.n_proto.rs"));
        }
    }

    pub mod n_api {
        pub mod n_rpc_proxy {
            pub mod n_proto {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/nyt.n_api.n_rpc_proxy.n_proto.rs"
                ));
            }
        }
    }

    pub mod n_chaos_client {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_chaos_client.n_proto.rs"));
        }
    }

    pub mod n_chunk_client {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_chunk_client.n_proto.rs"));
        }
    }

    pub mod n_hive_client {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_hive_client.n_proto.rs"));
        }
    }

    pub mod n_node_tracker_client {
        pub mod n_proto {
            include!(concat!(
                env!("OUT_DIR"),
                "/nyt.n_node_tracker_client.n_proto.rs"
            ));
        }
    }

    pub mod n_scheduler {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_scheduler.n_proto.rs"));
        }
    }

    pub mod n_table_chunk_format {
        pub mod n_proto {
            include!(concat!(
                env!("OUT_DIR"),
                "/nyt.n_table_chunk_format.n_proto.rs"
            ));
        }
    }

    pub mod n_table_client {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_table_client.n_proto.rs"));
        }
    }

    pub mod n_tablet_client {
        pub mod n_proto {
            include!(concat!(env!("OUT_DIR"), "/nyt.n_tablet_client.n_proto.rs"));
        }
    }
}

/// `NYT.NApi.NRpcProxy.NProto` — the API service surface.
pub use nyt::n_api::n_rpc_proxy::n_proto as api;
/// `NYT.NBus.NProto` — `THandshake`.
pub use nyt::n_bus::n_proto as bus;
/// `NYT.NProto` — `TGuid`, `TError`.
pub use nyt::n_proto as misc;
/// `NYT.NRpc.NProto` — the request and response headers.
pub use nyt::n_rpc::n_proto as rpc;
/// `NYT.NYTree.NProto` — the attribute dictionary errors carry.
pub use nyt::ny_tree::n_proto as ytree;
