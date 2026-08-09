//! `raw` — sending commands this crate does not model.
//!
//! Every other example here uses a modelled command: a method that knows the
//! parameters, the verb and the shape of the answer. This one uses the door for
//! everything else. It exists because the honest answer to "can I do X against
//! my cluster?" used to be "fork the crate" — `Transport::call` was
//! `pub(crate)`, so a command with no method on `Client` could not be sent at
//! all, however well the transport underneath it would have carried it.
//!
//! Four commands are used here:
//!
//! - `get_supported_features` — what this cluster's build can do. No
//!   parameters, a small structured answer, a GET.
//! - `write_file` and `read_file` streaming — `read_file` was the crate's
//!   sharpest gap when this example was written: files could be written and
//!   not read back, and this call was the whole of how one was read. It has
//!   methods now — `Client::read_file` and `Client::read_file_streaming` (#10)
//!   grew out of exactly this — and the round trip stays because it shows the
//!   door carrying data in both directions, on commands whose wire shape this
//!   example itself verified. Neither direction ever holds more of the file
//!   than a buffer.
//! - `list_operations` — a read that is safe to repeat, which is what
//!   `Repeatable` is for, and one of the commands issue #9 lists as missing.
//!
//! The verb is not a guess. The HTTP proxy reference gives the rule outright —
//! *"If the command has an input data stream, then PUT. If the command is
//! mutating, then POST. Otherwise GET."* — and the cluster's own driver
//! registry declares those two properties per command.
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

    // `map([])` cannot say this — the key type has nothing to be inferred from
    // — so `empty_map` exists for exactly the command that takes none.
    let body = client.raw_command(
        Method::Get,
        "get_supported_features",
        &yson_build::empty_map(),
        None,
    )?;

    // What comes back is the response body as the proxy sent it. Decoding it is
    // the caller's job, because the crate has no idea what it means: this is
    // the price of the door, and the whole of it.
    //
    // The envelope is keyed by what the command returns, not by a fixed name:
    // `exists` answers `{value=…}` and this one answers `{features=…}`. Reading
    // the wrong key is the mistake that failed every `exists` call for two
    // releases, so the key is named here rather than assumed.
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

    // One of them read out in full, so the answer is shown to be an answer and
    // not merely a well-formed document.
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

/// A file written and read back through the raw door — the read never holding
/// the whole file. `Client::read_file_streaming` is this call grown into a
/// method; the door sends the same request.
fn a_file_through_the_raw_door(client: &Client) -> Result<(), ClientError> {
    step("A file, uploaded and streamed back");

    let path = format!("{ROOT}/blob");
    client.create("file", &path)?;

    // Deliberately larger than a buffer, so "streamed" means something.
    let contents: Vec<u8> = (0..4_000_000_u32).map(|n| (n % 251) as u8).collect();

    // `write_file` is a PUT: it has an input data stream. Sent from a reader
    // rather than a slice, so the bytes never have to be in memory twice.
    client.raw_command_upload(
        Method::Put,
        "write_file",
        &yson_build::map([("path", yson_build::string(&path))]),
        std::io::Cursor::new(&contents),
    )?;

    // `read_file` is a GET the cluster declares *heavy*: its answer is the
    // data. `raw_command` would put all of it in memory first, which for a file
    // of unknown size is the thing worth avoiding.
    let mut stream = client.raw_command_streaming(
        Method::Get,
        "read_file",
        &yson_build::map([("path", yson_build::string(&path))]),
    )?;

    // Hashed as it arrives rather than collected, which is the point: nothing
    // here holds the file.
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

/// The reason the door is a `Client` method rather than a bare HTTP agent.
fn a_raw_command_inside_a_transaction(client: &Client) -> Result<(), ClientError> {
    step("A raw command joins the transaction it was sent through");

    let path = format!("{ROOT}/staged");
    let transaction = client.start_transaction()?;

    // `create` *is* modelled — used here because what is being demonstrated is
    // the stamping, and a modelled command gives something to check against.
    // Any unmodelled command is stamped the same way, in the one place every
    // command passes through.
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

    // `raw_command` is `Repeatable::Never`: a command the crate has never heard
    // of cannot be assumed idempotent, and applying an unknown mutation twice
    // is a worse failure than one lost to a flaky proxy. `list_operations` is a
    // read — the cluster declares it non-mutating and light — so its caller can
    // say so and get the retry policy back.
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

    // A scheduler command has no transaction to be in, and the raw door knows
    // it: `list_operations` is on the same `NO_TRANSACTION` list every modelled
    // command is checked against, so this is not stamped even from a bound
    // client. That costs nothing on a cluster that ignores parameters it does
    // not recognise, and is the difference on one that refuses them.
    Ok(())
}

/// The two mistakes the door refuses to make quietly.
fn what_it_refuses(client: &Client) -> Result<(), ClientError> {
    step("What it will not send");

    // The name goes into `/api/v4/{command}` as it is, so a name out of a
    // config file must not be able to address something else. The failure this
    // prevents is not an error — it is a plausible answer from the wrong place.
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

    // A GET carries no body in ureq's type system, and in the protocol: a
    // command with an input data stream is a PUT by the proxy's own rule. The
    // payload would otherwise be dropped without a word.
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
