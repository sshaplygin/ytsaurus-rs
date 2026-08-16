//! End to end against a real RPC proxy: create a dynamic table, write to it in
//! a transaction, look the rows up, select them, delete one, and check every
//! answer.
//!
//! This is the check that the four layers actually work — everything below it
//! is verified against golden vectors and stubs, and this is where a real proxy
//! gets a vote. Start the repository's local cluster script, which enables an
//! RPC proxy on port 8011, then run:
//!
//! ```sh
//! tests/cluster-e2e/run_local_cluster.sh
//! cargo run -p ytsaurus-rpc --example rpc_e2e
//! ```
//!
//! `YT_RPC_PROXY` overrides the address, `YT_TOKEN` supplies a token for a
//! cluster with authentication on. The table is created over HTTP, because
//! creating and mounting tables is Cypress work this crate deliberately does
//! not wrap — the RPC path here is exactly the subset it claims.

use std::time::Duration;

use ytsaurus_rpc::client::{
    Client, LookupOptions, SelectOptions, StartTransactionOptions, TransactionType,
};
use ytsaurus_rpc::wire::{MaybeRow, Row, UnversionedValue, Value};

const TABLE: &str = "//tmp/ytsaurus_rpc_e2e";
const COLUMNS: &[&str] = &["key", "value"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("YT_RPC_PROXY").unwrap_or_else(|_| "localhost:8011".to_owned());
    let http = std::env::var("YT_PROXY").unwrap_or_else(|_| "localhost:8010".to_owned());
    let token = std::env::var("YT_TOKEN").ok();

    println!("== preparing {TABLE} over HTTP at {http}");
    prepare_table(&http, token.as_deref())?;

    println!("== connecting to the RPC proxy at {address}");
    let mut builder = Client::builder(&address).timeout(Duration::from_secs(30));
    if let Some(token) = &token {
        builder = builder.token(token.clone());
    }
    let client = builder.connect().await?;
    println!("   handshake done");

    let proxies = client.discover_proxies(None).await?;
    println!("== discover_proxies -> {proxies:?}");
    assert!(!proxies.is_empty(), "the cluster reported no RPC proxies");

    println!("== writing three rows in a tablet transaction");
    let transaction = client
        .start_transaction(TransactionType::Tablet, StartTransactionOptions::default())
        .await?;
    println!(
        "   transaction {} at timestamp {}",
        transaction.id(),
        transaction.start_timestamp()
    );

    let rows: Vec<Row> = (1..=3)
        .map(|key| {
            vec![
                UnversionedValue::new(0, Value::Int64(key)),
                UnversionedValue::new(1, Value::String(format!("value {key}").into())),
            ]
        })
        .collect();
    transaction.insert_rows(TABLE, COLUMNS, &rows).await?;
    transaction.commit().await?;
    println!("   committed");

    println!("== looking up keys 1, 2 and 99");
    let keys: Vec<Row> = [1, 2, 99]
        .iter()
        .map(|key| vec![UnversionedValue::new(0, Value::Int64(*key))])
        .collect();
    let found = client
        .lookup_rows(TABLE, &["key"], &keys, LookupOptions::default())
        .await?;

    for (key, row) in [1, 2, 99].iter().zip(&found) {
        println!("   {key} -> {}", render(row));
    }
    assert_eq!(found.len(), 3, "one answer per key asked for, in order");
    assert!(found[0].is_some(), "key 1 was written and must be found");
    assert!(found[1].is_some(), "key 2 was written and must be found");
    assert!(
        found[2].is_none(),
        "key 99 was never written, so it must come back as a null row"
    );
    assert_eq!(
        value_of(&found[0], 1),
        Some(Value::String("value 1".into())),
        "the row read back must hold what was written"
    );

    println!("== selecting every row");
    let (selected, columns) = client
        .select_rows_with_columns(
            &format!("* from [{TABLE}] order by key limit 10"),
            SelectOptions::default(),
        )
        .await?;
    println!("   columns {columns:?}");
    for row in &selected {
        println!("   {}", render(row));
    }
    assert_eq!(selected.len(), 3, "three rows were written");

    println!("== deleting key 2");
    let transaction = client
        .start_transaction(TransactionType::Tablet, StartTransactionOptions::default())
        .await?;
    transaction
        .delete_rows(
            TABLE,
            &["key"],
            &[vec![UnversionedValue::new(0, Value::Int64(2))]],
        )
        .await?;
    transaction.commit().await?;

    let after = client
        .lookup_rows(
            TABLE,
            &["key"],
            &[vec![UnversionedValue::new(0, Value::Int64(2))]],
            LookupOptions::default(),
        )
        .await?;
    assert!(after[0].is_none(), "key 2 was deleted and must be gone");
    println!("   key 2 is gone");

    println!("== reading inside a transaction");
    let transaction = client
        .start_transaction(TransactionType::Tablet, StartTransactionOptions::default())
        .await?;
    // A read inside a tablet transaction is expressed purely as its start
    // timestamp; there is no transaction_id field on LookupRows at all.
    let in_transaction = transaction
        .lookup_rows(
            TABLE,
            &["key"],
            &[vec![UnversionedValue::new(0, Value::Int64(1))]],
            LookupOptions::default(),
        )
        .await?;
    assert!(
        in_transaction[0].is_some(),
        "key 1 is visible in the transaction"
    );
    transaction.ping().await?;
    transaction.abort().await?;
    println!("   read, pinged and aborted");

    println!("\nAll RPC end-to-end checks passed against {address}");
    Ok(())
}

/// Creates and mounts the table over HTTP API v4 with `curl`.
///
/// Deliberately not through this crate: Cypress and tablet management are out
/// of its scope, and doing it here would blur what the RPC path was actually
/// asked to do.
fn prepare_table(http: &str, token: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let schema = r#"[
        {"name":"key","type":"int64","sort_order":"ascending"},
        {"name":"value","type":"string"}
    ]"#;

    command(
        http,
        token,
        "remove",
        &format!(r#"{{"path":"{TABLE}","force":true}}"#),
    )?;
    command(
        http,
        token,
        "create",
        &format!(
            r#"{{"path":"{TABLE}","type":"table","attributes":{{"dynamic":true,"schema":{schema}}}}}"#
        ),
    )?;
    command(
        http,
        token,
        "mount_table",
        &format!(r#"{{"path":"{TABLE}"}}"#),
    )?;

    // Mounting is asynchronous; the table must be mounted before a tablet
    // transaction can write to it.
    for _ in 0..60 {
        let state = command(
            http,
            token,
            "get",
            &format!(r#"{{"path":"{TABLE}/@tablet_state"}}"#),
        )?;
        if state.contains("mounted") {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err("the table did not reach the mounted state in 30 seconds".into())
}

fn command(
    http: &str,
    token: Option<&str>,
    name: &str,
    parameters: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut request = std::process::Command::new("curl");
    request
        .arg("-sS")
        .arg("-X")
        .arg("POST")
        .arg(format!("http://{http}/api/v4/{name}"))
        .arg("-H")
        .arg("Content-Type: application/json")
        // The header is itself parsed as JSON when the request is JSON, so the
        // format has to be a JSON string rather than the YSON spelling.
        .arg("-H")
        .arg("X-YT-Output-Format: \"json\"")
        .arg("-d")
        .arg(parameters);
    if let Some(token) = token {
        request
            .arg("-H")
            .arg(format!("Authorization: OAuth {token}"));
    }

    let output = request.output()?;
    let body = String::from_utf8_lossy(&output.stdout).into_owned();
    if body.contains("\"code\"") && body.contains("message") && !body.contains("mounted") {
        // `remove` of a missing node is expected to fail; everything else is
        // reported so a broken setup is not mistaken for a protocol bug.
        if name != "remove" {
            return Err(format!("HTTP {name} failed: {body}").into());
        }
    }
    Ok(body)
}

fn render(row: &MaybeRow) -> String {
    match row {
        None => "<missing>".to_owned(),
        Some(values) => {
            let rendered: Vec<String> = values
                .iter()
                .map(|value| format!("{}={:?}", value.id, value.value))
                .collect();
            format!("[{}]", rendered.join(", "))
        }
    }
}

fn value_of(row: &MaybeRow, id: u16) -> Option<Value> {
    row.as_ref()?
        .iter()
        .find(|value| value.id == id)
        .map(|value| value.value.clone())
}
