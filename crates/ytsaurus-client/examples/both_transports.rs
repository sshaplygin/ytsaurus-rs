//! The same code, run twice: once over HTTP and once over the RPC proxy.
//!
//! This is what the shared interface is for, and it is also the only place the
//! two transports are checked to agree. They are wire-level unrelated — YSON
//! over HTTP, the row wire protocol over bus — so "they behave the same" is a
//! claim that needs running, not asserting.
//!
//! Needs a cluster with an RPC proxy, which the stock local one does not have:
//!
//! ```sh
//! docker run -d --name yt.rpc -p 8010:80 -p 8011:8011 \
//!     ghcr.io/ytsaurus/local:stable \
//!     --proxy-config '{coordinator={public_fqdn="localhost:8010"};}' \
//!     --rpc-proxy-count 1 --rpc-proxy-port 8011 --node-count 1 --id rpcsaurus
//!
//! cargo run -p ytsaurus-client --features rpc --example both_transports
//! ```
//!
//! `YT_PROXY` and `YT_RPC_PROXY` override the addresses.

use ytsaurus_api::{LookupOptions, Row, SelectOptions, TableClient, Value};

const TABLE: &str = "//tmp/ytsaurus_both_transports";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A bare host means HTTPS; a local cluster speaks plain HTTP.
    let http = std::env::var("YT_PROXY").unwrap_or_else(|_| "http://localhost:8010".to_owned());
    let rpc = std::env::var("YT_RPC_PROXY").unwrap_or_else(|_| "localhost:8011".to_owned());

    println!("== preparing {TABLE}");
    prepare(&http)?;

    let clients: Vec<Box<dyn TableClient>> = vec![
        ytsaurus_client::create_client(&http)?,
        ytsaurus_client::create_rpc_client(&rpc)?,
    ];

    let mut answers = Vec::new();
    for client in &clients {
        println!("\n== over {}", client.transport());
        answers.push(exercise(client.as_ref())?);
    }

    // The point of the whole exercise: two transports, one answer.
    println!("\n== comparing");
    let (first, second) = (&answers[0], &answers[1]);
    assert_eq!(
        first, second,
        "the transports disagree:\n  HTTP: {first:?}\n  RPC:  {second:?}"
    );
    println!("   HTTP and RPC agree row for row");

    println!("\nBoth transports passed, through one interface.");
    Ok(())
}

/// Everything a caller can do through the shared interface, run against one
/// client and reduced to something comparable.
fn exercise(client: &dyn TableClient) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<Row> = (1..=3)
        .map(|key| {
            Row::new()
                .with("key", key as i64)
                .with("value", format!("value {key}"))
        })
        .collect();

    client.insert_rows(TABLE, &rows)?;
    println!("   wrote {} rows", rows.len());

    // One answer per key asked for, including the key that is not there.
    let keys: Vec<Row> = [1i64, 2, 99]
        .iter()
        .map(|key| Row::new().with("key", *key))
        .collect();
    let found = client.lookup_rows(TABLE, &keys, &LookupOptions::default())?;
    for (key, row) in [1, 2, 99].iter().zip(&found) {
        match row {
            Some(row) => println!("   {key} -> {row}"),
            None => println!("   {key} -> <missing>"),
        }
    }
    assert_eq!(found.len(), 3, "one answer per key, in order");
    assert!(found[2].is_none(), "key 99 was never written");

    let selected = client.select_rows(
        &format!("* from [{TABLE}] order by key limit 10"),
        &SelectOptions::default(),
    )?;
    println!("   selected {} rows", selected.len());

    // Tablet transactions are sticky to one proxy, which an HTTP client cannot
    // hold — so this is the one place the two transports genuinely differ, and
    // the interface says so rather than failing obscurely on the second call.
    match client.start_transaction() {
        Ok(transaction) => {
            println!("   transaction {}", transaction.id());
            transaction.insert_rows(
                TABLE,
                &[Row::new().with("key", 42i64).with("value", "in tx")],
            )?;
            transaction.ping()?;
            transaction.abort()?;

            let after = client.lookup_rows(
                TABLE,
                &[Row::new().with("key", 42i64)],
                &LookupOptions::default(),
            )?;
            assert!(after[0].is_none(), "an aborted write must not be visible");
            println!("   the aborted write is not visible");
        }
        Err(ytsaurus_api::Error::Unsupported { transport, what }) => {
            println!("   no transaction: {transport} does not support {what}");
        }
        Err(error) => return Err(error.into()),
    }

    client.delete_rows(TABLE, &[Row::new().with("key", 2i64)])?;
    println!("   deleted key 2");

    // Reduced to strings so two transports can be compared directly.
    let mut summary: Vec<String> = client
        .select_rows(
            &format!("* from [{TABLE}] order by key limit 10"),
            &SelectOptions::default(),
        )?
        .iter()
        .map(|row| {
            format!(
                "{}={}",
                row.get("key").and_then(Value::as_i64).unwrap_or_default(),
                row.get("value").and_then(Value::as_str).unwrap_or_default()
            )
        })
        .collect();
    summary.sort();
    println!("   final table: {summary:?}");

    // Left as the next transport found it.
    client.delete_rows(
        TABLE,
        &[1i64, 2, 3]
            .iter()
            .map(|key| Row::new().with("key", *key))
            .collect::<Vec<_>>(),
    )?;

    Ok(summary)
}

/// Creates and mounts the table. Cypress work, so it goes over HTTP whichever
/// transport is being exercised.
fn prepare(http: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = ytsaurus_client::Client::new(http);
    let _ = client.remove_tree(TABLE);

    let schema = ytsaurus_client::yson_build::with_attributes(
        ytsaurus_client::yson_build::list([
            ytsaurus_client::yson_build::map([
                ("name", ytsaurus_client::yson_build::string("key")),
                ("type", ytsaurus_client::yson_build::string("int64")),
                (
                    "sort_order",
                    ytsaurus_client::yson_build::string("ascending"),
                ),
            ]),
            ytsaurus_client::yson_build::map([
                ("name", ytsaurus_client::yson_build::string("value")),
                ("type", ytsaurus_client::yson_build::string("string")),
            ]),
        ]),
        [("unique_keys", ytsaurus_client::yson_build::boolean(true))],
    );

    client.raw_command(
        ytsaurus_client::Method::Post,
        "create",
        &ytsaurus_client::yson_build::map([
            ("path", ytsaurus_client::yson_build::string(TABLE)),
            ("type", ytsaurus_client::yson_build::string("table")),
            (
                "attributes",
                ytsaurus_client::yson_build::map([
                    ("dynamic", ytsaurus_client::yson_build::boolean(true)),
                    ("schema", schema),
                ]),
            ),
        ]),
        None,
    )?;

    client.raw_command(
        ytsaurus_client::Method::Post,
        "mount_table",
        &ytsaurus_client::yson_build::map([("path", ytsaurus_client::yson_build::string(TABLE))]),
        None,
    )?;

    // Mounting is asynchronous, and a write to an unmounted table fails.
    for _ in 0..60 {
        let state = client.get(&format!("{TABLE}/@tablet_state"))?;
        if state.as_str() == Some("mounted") {
            println!("   mounted");
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Err("the table did not mount within 30 seconds".into())
}
