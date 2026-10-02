//! The README's quick start: a static table written and read back as Rust
//! values over HTTP, then one dynamic-table read through `TableClient`.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example quickstart
//! ```
//!
//! This crate does not model creating or mounting a dynamic table, so the
//! dynamic half reads one that already exists and is mounted, named by
//! `YT_DYNAMIC_TABLE`, and is skipped when that is unset. `create_client`
//! sends no token; a cluster that wants one needs `create_client_with_token`.
//! Built with `--features rpc`, the same read goes to the RPC proxy named by
//! `YT_RPC_PROXY` instead.

// README-START
use serde::{Deserialize, Serialize};
use ytsaurus_client::{Client, TableRow};

#[derive(Serialize, Deserialize, Debug, PartialEq, ytsaurus_helpers::TableRow)]
struct Contact {
    name: String,
    age: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A static table over HTTP, as Rust values.
    let client = Client::from_env()?;
    let path = "//tmp/ytsaurus_rs_quickstart";
    client.remove_tree(path)?;
    client.create_table(path, &Contact::table_schema())?;
    let rows = vec![Contact {
        name: "Ann".into(),
        age: 31,
    }];
    client.write_table_rows(path, &rows)?;
    assert_eq!(client.read_table_rows::<Contact>(path)?, rows);
    println!("wrote and read back {path}");

    // A dynamic table, over HTTP or the RPC proxy.
    if let Ok(table) = std::env::var("YT_DYNAMIC_TABLE") {
        #[cfg(not(feature = "rpc"))]
        let tables = ytsaurus_client::create_client(&std::env::var("YT_PROXY")?)?;
        #[cfg(feature = "rpc")]
        let tables = ytsaurus_client::create_rpc_client(&std::env::var("YT_RPC_PROXY")?)?;
        let found =
            tables.select_rows(&format!("* from [{table}] limit 10"), &Default::default())?;
        println!("{} rows over {}", found.len(), tables.transport());
    }
    Ok(())
}
// README-END
