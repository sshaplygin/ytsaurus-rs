//! `cluster_info`: connect, and ask the cluster about itself.
//!
//! The Go SDK's `yt/go/examples/cypress-example`: one config, one typed read of
//! `//@`, one line saying when the cluster was created. This example also
//! checks that the struct's three attributes are a projection of the dozens
//! the node carries, that `get` and `get_as` agree, that the same struct reads
//! `//tmp/@`, that a scalar attribute reads into a `String`, and that a type
//! the answer cannot fit is a `ClientError::Decode`. It writes nothing.
//!
//! - **Transport.** Go's `-use-rpc` flag builds an `ythttp` or `ytrpc` client
//!   from one config. This example reads Cypress, which only the HTTP `Client`
//!   does; the RPC proxy client (`create_rpc_client`, the `rpc` feature)
//!   serves dynamic tables through `TableClient`.
//! - **TLS.** Go's `-use-tls` has no counterpart: the `tls` feature is on by
//!   default, so an `https://` proxy needs no flag. Musl worker builds turn it
//!   off, because `ring` needs a C cross-compiler there.
//! - **Token.** Go's `ReadTokenFromFile: true`; [`Client::from_env`] tries
//!   `YT_TOKEN`, then the file named by `YT_TOKEN_PATH`, then `~/.yt/token`,
//!   where the `yt` CLI looks.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example cluster_info
//! ```

use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError};
use ytsaurus_yson::YsonNode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ncluster_info failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    // Go: yt.Config{Proxy: *flagProxy, ReadTokenFromFile: true, UseTLS: *flagUseTLS}.
    let client = Client::from_env()?;

    step("Asking the root node when the cluster was created");
    let root: NodeInfo = client.get_as("//@")?;
    // The one line the Go program prints.
    println!("   cluster was created at {}", root.creation_time);

    // Go decodes this into `yson.Time`. Here it stays the ISO 8601 instant the
    // cluster sent, checked for that shape.
    check(
        "the creation time came back as a timestamp",
        !root.creation_time.is_empty()
            && root.creation_time.contains('T')
            && root.creation_time.ends_with('Z'),
    )?;
    done(&format!("in account {}", root.account));

    step("What the struct did not ask for");
    // The same attributes untyped, only to be counted.
    let all = client.get("//@")?;
    let offered = match &all.node {
        YsonNode::Map(attributes) => attributes.len(),
        _ => 0,
    };
    check(
        &format!("the cluster offered {offered} attributes, and the struct named 3"),
        offered > 3,
    )?;

    // `get` returns a document to walk; `get_as` the shape it is walked into.
    // Both must agree on one attribute.
    let asked_directly = client.get("//@type")?;
    check(
        &format!("//@ is a {} whichever way it is read", root.node_type),
        asked_directly.as_str() == Some(root.node_type.as_str()),
    )?;

    step("The same struct, a different path");
    // Type, creation time and account are on every Cypress node.
    let tmp: NodeInfo = client.get_as("//tmp/@")?;
    check(
        &format!(
            "//tmp is a {} created at {}, in account {}",
            tmp.node_type, tmp.creation_time, tmp.account
        ),
        tmp.node_type == "map_node" && !tmp.account.is_empty(),
    )?;

    step("And one attribute on its own, which is no struct at all");
    // A path ending in a scalar attribute answers with the scalar, which
    // `get_as` reads into a `String`. A cluster need not have a name.
    if client.exists("//sys/@cluster_name")? {
        let name: String = client.get_as("//sys/@cluster_name")?;
        check(
            &format!("this cluster calls itself {name:?}"),
            !name.is_empty(),
        )?;
    } else {
        done("//sys/@cluster_name is not set here, which is allowed");
    }

    step("Asking for a type the answer cannot fit");
    // `creation_time` is a timestamp string and `Impossible` wants a number, so
    // this must be an error naming the path, not a panic and not a zero.
    match client.get_as::<Impossible>("//@") {
        Ok(_) => {
            return Err(ClientError::Config(
                "a timestamp string was accepted as a number, so the decoder is not checking"
                    .to_owned(),
            ));
        }
        Err(e) => {
            check(
                "a type that does not fit is an error, not a panic",
                matches!(e, ClientError::Decode { .. }),
            )?;
            println!("   {e}");
        }
    }

    println!("\nOne connection and one typed read, which is the whole Go example.");
    println!("Nothing was written: the one example that leaves the cluster as it found it.");
    Ok(())
}

/// The three attributes this program has a use for, from a node that has dozens.
///
/// Go uses `yson:"…"` tags and a `yt.NodeType`; here the type is the `String`
/// the cluster sends, renamed because `type` is a keyword.
#[derive(serde::Deserialize)]
struct NodeInfo {
    #[serde(rename = "type")]
    node_type: String,
    creation_time: String,
    account: String,
}

/// The same attribute, asked for as something it can never be.
///
/// Exists to be refused.
#[derive(serde::Deserialize)]
#[expect(dead_code)]
struct Impossible {
    creation_time: u64,
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn done(what: &str) {
    println!("   ok {what}");
}

fn check(what: &str, passed: bool) -> Result<(), ClientError> {
    if passed {
        done(what);
        return Ok(());
    }
    eprintln!("   FAIL {what}");
    Err(ClientError::Config(format!("check failed: {what}")))
}
