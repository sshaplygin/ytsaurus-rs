//! `cluster_info` — connect, and ask the cluster about itself.
//!
//! The Go SDK ships this as `yt/go/examples/cypress-example`, and despite the
//! name it is not a tour of Cypress: it is the smallest program that proves a
//! connection works. One config, one typed read of `//@`, one line of output
//! saying when the cluster was created. This is that program in Rust, and the
//! three places where the shapes differ are worth saying plainly.
//!
//! **The transport is not a choice here.** Go takes a `-use-rpc` flag and
//! builds either an `ythttp` or an `ytrpc` client from the same `yt.Config`.
//! There is nothing to pick between: this crate speaks HTTP API v4, and the
//! RPC proxy is a recorded non-goal — AGENTS.md, "Non-goals": *"RPC proxy
//! (custom binary protocol), protobuf row format, dynamic tables, non-Linux
//! targets, publishing to crates.io."* Go's `-use-tls` has no counterpart
//! either. TLS is the `tls` feature, on by default, so an `https://` proxy
//! needs no flag; the only thing that turns it off is a musl worker build,
//! where `rustls` reaches `ring`, which wants a C cross-compiler.
//!
//! **The token is found the same way.** Go asks for `ReadTokenFromFile: true`;
//! [`Client::from_env`] is that and the rest of the search — `YT_TOKEN`, then
//! the file named by `YT_TOKEN_PATH`, then `~/.yt/token` — which is where the
//! `yt` CLI looks, so a machine the CLI already works on needs nothing more.
//!
//! **Attributes the struct does not name are ignored.** That is the whole
//! reason to ask for `//@` with a type rather than walk what comes back: a
//! node carries dozens of attributes, this program has a use for three, and
//! the decoder drops the rest. The run below counts them, so the claim is
//! checked rather than asserted.
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
    // The Go example's whole configuration, minus the flags it has no use for:
    // yt.Config{Proxy: *flagProxy, ReadTokenFromFile: true, UseTLS: *flagUseTLS}.
    let client = Client::from_env()?;

    step("Asking the root node when the cluster was created");
    let root: NodeInfo = client.get_as("//@")?;
    // The one line the Go program prints.
    println!("   cluster was created at {}", root.creation_time);

    // Go decodes this into `yson.Time`. There is no date type in this stack and
    // no dependency worth taking for one, so it stays the ISO 8601 instant the
    // cluster sent — checked for being that, rather than for merely being text.
    check(
        "the creation time came back as a timestamp",
        !root.creation_time.is_empty()
            && root.creation_time.contains('T')
            && root.creation_time.ends_with('Z'),
    )?;
    done(&format!("in account {}", root.account));

    step("What the struct did not ask for");
    // The same attributes untyped, only to be counted. If this were not far
    // larger than three, the sentence in the module doc would be decoration.
    let all = client.get("//@")?;
    let offered = match &all.node {
        YsonNode::Map(attributes) => attributes.len(),
        _ => 0,
    };
    check(
        &format!("the cluster offered {offered} attributes, and the struct named 3"),
        offered > 3,
    )?;

    // `get` hands back a document to walk; `get_as` hands back the shape you
    // were going to walk it into. Both roads to one attribute, so a change that
    // broke either would show up as disagreement rather than as silence.
    let asked_directly = client.get("//@type")?;
    check(
        &format!("//@ is a {} whichever way it is read", root.node_type),
        asked_directly.as_str() == Some(root.node_type.as_str()),
    )?;

    step("The same struct, a different path");
    // Nothing in `NodeInfo` is particular to the root: type, creation time and
    // account are on every Cypress node, so the same type reads any of them.
    let tmp: NodeInfo = client.get_as("//tmp/@")?;
    check(
        &format!(
            "//tmp is a {} created at {}, in account {}",
            tmp.node_type, tmp.creation_time, tmp.account
        ),
        tmp.node_type == "map_node" && !tmp.account.is_empty(),
    )?;

    step("And one attribute on its own, which is no struct at all");
    // A path ending in a scalar attribute answers with the scalar, so `get_as`
    // reads it into a `String` with nothing wrapped around it. Asked about
    // first because a cluster is under no obligation to have a name.
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
    // Deliberate: the failure mode of a typed read is the thing worth showing.
    // `creation_time` is a timestamp string and `Impossible` wants a number, so
    // this must come back as an error naming the path — not as a panic, and not
    // as a zero.
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
/// Go spells the same struct with `yson:"…"` tags and a `yt.NodeType` for the
/// first field. There is no node-type enum here, and a `String` is what the
/// cluster sends; the rename is because `type` is a keyword.
#[derive(serde::Deserialize)]
struct NodeInfo {
    #[serde(rename = "type")]
    node_type: String,
    creation_time: String,
    account: String,
}

/// The same attribute, asked for as something it can never be.
///
/// Exists to be refused, so the error path is demonstrated rather than
/// described.
#[derive(serde::Deserialize)]
#[allow(dead_code)]
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
