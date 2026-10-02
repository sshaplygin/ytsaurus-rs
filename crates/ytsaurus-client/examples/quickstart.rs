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
//! `YT_DYNAMIC_TABLE`, and is skipped when that is unset.
//!
//! `create_client` and `create_rpc_client` send no token: the dynamic half
//! does not share the token `Client::from_env` found for the static half. A
//! cluster that wants one needs `create_client_with_token` or
//! `create_rpc_client_with_token`.
//!
//! The dynamic half goes over HTTP only. The RPC constructor is a comment in
//! it, because the README quotes this block and a `cfg(feature = "rpc")` there
//! would name the reader's crate's feature, not this one's;
//! `both_transports.rs` runs the same calls over both.

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

    // A dynamic table.
    if let Ok(table) = std::env::var("YT_DYNAMIC_TABLE") {
        let tables = ytsaurus_client::create_client(&std::env::var("YT_PROXY")?)?;
        // With the `rpc` feature: ytsaurus_client::create_rpc_client(&address)?
        let found =
            tables.select_rows(&format!("* from [{table}] limit 10"), &Default::default())?;
        println!("{} rows", found.len());
    }
    Ok(())
}
// README-END
