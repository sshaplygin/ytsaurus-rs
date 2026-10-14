//! `raw`: sending commands through `Client::raw_command` and its variants,
//! with the verb, parameters and decoding chosen by the caller.
//!
//! Four commands are used here:
//!
//! - `get_supported_features`: no parameters, a small structured answer, a GET.
//! - `write_file` and a streamed `read_file`: data in both directions, neither
//!   holding more of the file than a buffer. `Client::read_file` and
//!   `Client::read_file_streaming` model the same request.
//! - `list_operations`: a read that is safe to repeat, which is what
//!   `Repeatable` is for.
//!
//! It also sends a raw `create` inside a transaction and checks two requests
//! the client refuses. Each answer is checked. The verb follows the HTTP proxy
//! reference: *"If the command has an input data stream, then PUT. If the
//! command is mutating, then POST. Otherwise GET."*
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example raw
//! ```

use std::io::Read;
use std::process::ExitCode;

use ytsaurus_client::{Client, ClientError, Method, Repeatable, yson_build};
use ytsaurus_yson::{YsonFormat, YsonNode, from_slice};

/// Where this example works, removed on the way out.
const ROOT: &str = "//tmp/ytsaurus_rs_raw";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nraw failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;

    if client.exists(ROOT)? {
        client.remove_tree(ROOT)?;
    }
    client.create("map_node", ROOT)?;

    supported_features(&client)?;
    a_file_through_the_raw_door(&client)?;
    a_raw_command_inside_a_transaction(&client)?;
    a_read_the_caller_says_is_repeatable(&client)?;
    what_it_refuses(&client)?;

    client.remove_tree(ROOT)?;

    println!("\nFour unmodelled commands, sent without forking the crate.");
    Ok(())
}

/// The simplest possible use: a GET with no parameters.
fn supported_features(client: &Client) -> Result<(), ClientError> {
    step("A command with no parameters at all");

    // `map([])` cannot infer its key type; `empty_map` is for a command with no
    // parameters.
    let body = client.raw_command(
        Method::Get,
        "get_supported_features",
        &yson_build::empty_map(),
        None,
    )?;

    // The answer is the response body as the proxy sent it; decoding it is the
    // caller's job. The envelope is keyed by what the command returns:
    // `exists` answers `{value=…}` and this one `{features=…}`.
    let answer = decode(&body, "get_supported_features")?;
    let features = field(&answer, "features").ok_or_else(|| ClientError::Decode {
        command: "get_supported_features".to_owned(),
        reason: format!("no \"features\" key in {}", String::from_utf8_lossy(&body)),
    })?;

    let named = match &features.node {
        YsonNode::Map(m) => m
            .keys()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    check(
        &format!("the cluster described: {}", named.join(", ")),
        !named.is_empty(),
    )?;

    // One of them read out in full.
    let codecs = field(&features, "compression_codecs")
        .map(|c| match &c.node {
            YsonNode::List(items) => items.len(),
            _ => 0,
        })
        .unwrap_or(0);
    check(
        &format!("this build offers {codecs} compression codecs"),
        codecs > 0,
    )
}

/// A file written and read back through raw commands, the read never holding
/// the whole file. `Client::read_file_streaming` sends the same request.
fn a_file_through_the_raw_door(client: &Client) -> Result<(), ClientError> {
    step("A file, uploaded and streamed back");

    let path = format!("{ROOT}/blob");
    client.create("file", &path)?;

    // Larger than a buffer.
    let contents: Vec<u8> = (0..4_000_000_u32).map(|n| (n % 251) as u8).collect();

    // `write_file` is a PUT: it has an input data stream. Sent from a reader
    // rather than a slice, so the bytes never have to be in memory twice.
    client.raw_command_upload(
        Method::Put,
        "write_file",
        &yson_build::map([("path", yson_build::string(&path))]),
        std::io::Cursor::new(&contents),
    )?;

    // `read_file` is a heavy GET: its answer is the data, which `raw_command`
    // would hold in memory whole.
    let mut stream = client.raw_command_streaming(
        Method::Get,
        "read_file",
        &yson_build::map([("path", yson_build::string(&path))]),
    )?;

    // Summed as it arrives rather than collected.
    let mut sum = 0_u64;
    let mut read = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let n = stream
            .read(&mut buffer)
            .map_err(|e| ClientError::Config(format!("reading {path}: {e}")))?;
        if n == 0 {
            break;
        }
        read += n as u64;
        sum += buffer[..n].iter().map(|b| u64::from(*b)).sum::<u64>();
    }

    let expected: u64 = contents.iter().map(|b| u64::from(*b)).sum();
    check(
        &format!("{read} bytes came back, byte-for-byte what went up"),
        read == contents.len() as u64 && sum == expected,
    )?;
    check(
        &format!("the reader counted the same {read} bytes"),
        stream.bytes_read() == read,
    )
}

/// A raw command sent through a transaction joins it.
fn a_raw_command_inside_a_transaction(client: &Client) -> Result<(), ClientError> {
    step("A raw command joins the transaction it was sent through");

    let path = format!("{ROOT}/staged");
    let transaction = client.start_transaction()?;

    // `create` is modelled, which gives something to check against; any
    // command sent this way is stamped with the transaction the same way.
    transaction.raw_command(
        Method::Post,
        "create",
        &yson_build::map([
            ("type", yson_build::string("map_node")),
            ("path", yson_build::string(&path)),
        ]),
        None,
    )?;

    // A different client is outside the transaction, and must not see it.
    check(
        "the node is invisible outside the transaction",
        !client.exists(&path)?,
    )?;

    transaction.commit()?;
    check(
        "and there once the transaction commits",
        client.exists(&path)?,
    )
}

/// Retries are the caller's call, because only the caller knows the command.
fn a_read_the_caller_says_is_repeatable(client: &Client) -> Result<(), ClientError> {
    step("A read the caller marks as safe to repeat");

    // `raw_command` is `Repeatable::Never`: an unknown command cannot be
    // assumed idempotent. `list_operations` is a non-mutating, light read, so
    // the caller marks it `Freely` and gets retries.
    let body = client.raw_command_with(
        Method::Get,
        "list_operations",
        &yson_build::map([("limit", yson_build::int(1))]),
        None,
        Repeatable::Freely,
        None,
    )?;

    let answer = decode(&body, "list_operations")?;
    let keys = match &field(&answer, "value").unwrap_or(answer).node {
        YsonNode::Map(m) => m.len(),
        _ => 0,
    };

    check(
        &format!("the scheduler answered with {keys} keys"),
        keys > 0,
    )?;

    // `list_operations` is on the `NO_TRANSACTION` list that modelled commands
    // use too, so it is not stamped with a transaction even from a bound
    // client.
    Ok(())
}

/// Two requests the client refuses before sending.
fn what_it_refuses(client: &Client) -> Result<(), ClientError> {
    step("What it will not send");

    // The name goes into `/api/v4/{command}` as it is, so a name that would
    // change the URL is refused.
    let error = client
        .raw_command(
            Method::Get,
            "get/../../hosts",
            &yson_build::empty_map(),
            None,
        )
        .err();
    check(
        "a command name that would change the URL is refused",
        matches!(error, Some(ClientError::Config(_))),
    )?;

    // A GET carries no body, in ureq and in the protocol: a command with an
    // input data stream is a PUT. The payload would otherwise be dropped.
    let error = client
        .raw_command(
            Method::Get,
            "read_file",
            &yson_build::empty_map(),
            Some(b"x"),
        )
        .err();
    check(
        "a payload on a GET is refused rather than dropped",
        matches!(error, Some(ClientError::Config(_))),
    )?;

    // Nothing was sent for either: both refusals happen before the socket.
    Ok(())
}

fn decode(body: &[u8], command: &str) -> Result<ytsaurus_yson::YsonValue, ClientError> {
    from_slice(body, YsonFormat::Text).map_err(|e| ClientError::Decode {
        command: command.to_owned(),
        reason: format!("{e}; body was {}", String::from_utf8_lossy(body)),
    })
}

fn field(value: &ytsaurus_yson::YsonValue, key: &str) -> Option<ytsaurus_yson::YsonValue> {
    match &value.node {
        YsonNode::Map(m) => m.get(key.as_bytes()).cloned(),
        _ => None,
    }
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn check(what: &str, passed: bool) -> Result<(), ClientError> {
    if passed {
        println!("   ok {what}");
        return Ok(());
    }
    eprintln!("   FAIL {what}");
    Err(ClientError::Config(format!("check failed: {what}")))
}
