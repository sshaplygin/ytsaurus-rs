//! `batch` — a dozen commands in one round trip, and the answers one by one.
//!
//! A launcher that creates its tables one call at a time pays a round trip
//! per table. `execute_batch` sends them together — and answers **per part**,
//! so one table that already exists does not cost the others their creation.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example batch
//! ```
//!
//! **The round trip itself is not something this program can see.** It checks
//! what the cluster did — every table there, every answer at its own index,
//! the transaction invisible until the commit — but "one request" is a fact
//! about the wire, and a client on a cluster has no way to count what it sent.
//! `a_dozen_creates_are_one_request` in `tests/batch.rs` is where that claim is
//! checked, against a socket in-process that counts. Measured there and once by
//! hand through a counting TCP relay: **1** request, 9.16 ms, against 140.77 ms
//! for the same twelve creates sent one at a time.

use std::process::ExitCode;

use ytsaurus_client::{
    BatchRequest, Client, ClientError, Column, ColumnType, MutationId, TableSchema,
};

/// Where the demo keeps its tree.
const BASE: &str = "//tmp/ytsaurus_rs_batch";

/// How many tables the one-round-trip step creates.
const TABLES: usize = 12;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nbatch failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;
    client.remove_tree(BASE)?;

    step("A dozen tables in one batch");
    let mut creates = BatchRequest::new();
    for index in 0..TABLES {
        creates.create("table", &table(index));
    }
    let made = client.execute_batch(&creates)?;
    check("every part answered", made.len() == TABLES)?;
    for (index, part) in made.iter().enumerate() {
        let answer = part
            .as_ref()
            .map_err(|e| ClientError::Config(format!("creating {} failed: {e}", table(index))))?;
        // The envelope is keyed by what the part's command returns: a
        // create answers `{node_id=…}`, exactly as it would alone.
        if answer["node_id"].as_str().is_none() {
            return Err(ClientError::Config(format!(
                "a create answered without a node id: {answer:?}"
            )));
        }
    }
    done(&format!("{TABLES} creates, {TABLES} node ids"));

    let mut exists = BatchRequest::new();
    for index in 0..TABLES {
        exists.exists(&table(index));
    }
    let there = client.execute_batch(&exists)?;
    check(
        "and every table is really there",
        there.iter().all(|part| {
            part.as_ref()
                .is_ok_and(|answer| answer["value"].node == ytsaurus_yson::YsonNode::Boolean(true))
        }),
    )?;

    step("One part fails, the rest succeed — in order");
    let schema = TableSchema::new([
        Column::new("host", ColumnType::Utf8).required(),
        Column::new("size", ColumnType::Int64),
    ]);
    let mut mixed = BatchRequest::new();
    mixed
        .create_table(&table(0), &schema)? // fails: the path already exists
        .set_attribute(
            &table(1),
            "note",
            ytsaurus_client::yson_build::string("kept"),
        )
        .get(&format!("{}/@type", table(2)))
        .remove(&table(3));
    let parts = client.execute_batch(&mixed)?;

    let refused = parts[0].as_ref().expect_err("the first part failed");
    check(
        &format!("the create over an existing node failed alone: {refused}"),
        matches!(refused, ClientError::Cluster { command, code: 501, .. } if command == "create"),
    )?;
    check(
        "the set beside it succeeded",
        parts[1].is_ok() && client.get(&format!("{}/@note", table(1)))?.as_str() == Some("kept"),
    )?;
    check(
        "the get answered under `value`, as it would alone",
        parts[2]
            .as_ref()
            .is_ok_and(|answer| answer["value"].as_str() == Some("table")),
    )?;
    check(
        "the remove removed",
        parts[3].is_ok() && !client.exists(&table(3))?,
    )?;

    step("Concurrency capped, and a batch bigger than one request");
    let mut throttled = BatchRequest::new()
        .with_concurrency(2)
        .with_max_part_size(5);
    for index in 0..TABLES {
        throttled.get(&format!("{}/@type", table(index)));
    }
    let answers = client.execute_batch(&throttled)?;
    check(
        "twelve parts at five per request come back as one vector, in order",
        answers.len() == TABLES
            && answers.iter().enumerate().all(|(index, part)| match part {
                Ok(answer) => answer["value"].as_str() == Some("table"),
                // Table 3 was removed a step ago, so its part fails — the
                // split must keep failures at their own index too.
                Err(_) => index == 3,
            }),
    )?;

    step("A split batch that stops says which parts already applied");
    // A part naming a command the cluster has never heard of fails the *whole*
    // request — so putting one in the second chunk of a split batch is a
    // mid-sequence failure with the first chunk already committed. There is no
    // rollback, and the error carries the prefix rather than losing it.
    let mut stopping = BatchRequest::new().with_max_part_size(2);
    stopping
        .create("table", &format!("{BASE}/applied0"))
        .create("table", &format!("{BASE}/applied1"))
        .create("table", &format!("{BASE}/never"));
    stopping
        .raw("frobnicate", ytsaurus_client::yson_build::empty_map(), None)
        .expect("a fine command name, and no command at all");

    let stopped = client
        .execute_batch(&stopping)
        .expect_err("the second request named a command the cluster refuses");
    let ClientError::BatchInterrupted {
        answered,
        parts,
        cause,
    } = &stopped
    else {
        return Err(ClientError::Config(format!(
            "a stopped split batch must carry its prefix, not just: {stopped}"
        )));
    };
    check(
        &format!(
            "the batch stopped after {} of {parts} parts: {cause}",
            answered.len()
        ),
        *parts == 4 && answered.len() == 2 && answered.iter().all(|part| part.is_ok()),
    )?;
    check(
        "the parts it did answer for are on the cluster — nothing rolled back",
        client.exists(&format!("{BASE}/applied0"))?
            && client.exists(&format!("{BASE}/applied1"))?,
    )?;
    // And the sharper half, which is why `answered` reports what came *back*
    // rather than claiming what was applied: the request that failed is not a
    // no-op either. The parts of it the driver could resolve ran before the
    // unknown name threw, so a create sitting beside `frobnicate` lands even
    // though the batch is refused with no per-part results at all. Reported,
    // not asserted: it is the cluster's behaviour today, and a version that
    // stopped doing it would be a fix rather than a regression.
    done(&format!(
        "the create beside the unknown command {} — a refused request is not a no-op",
        if client.exists(&format!("{BASE}/never"))? {
            "landed anyway"
        } else {
            "did not land on this cluster"
        }
    ));

    step("One mutation id, replayed, is answered with the first result");
    // The guarantee a single process cannot give itself: persist the id, and
    // after a crash the same batch is deduplicated rather than applied twice.
    // `create_table` deliberately omits `ignore_existing`, so a second send
    // that was *not* recognised as a replay fails — which is what makes the
    // identical node ids below mean something.
    let replayed: Vec<String> = ["replay0", "replay1"]
        .iter()
        .map(|name| format!("{BASE}/{name}"))
        .collect();
    let mut once_and_again = BatchRequest::new();
    for path in &replayed {
        once_and_again.create_table(path, &schema)?;
    }

    let id = MutationId::new();
    let first = node_ids(client.execute_batch_with(&once_and_again, Some(&id))?)?;
    let again = node_ids(client.execute_batch_with(&once_and_again, Some(&id.as_retry()))?)?;
    check(
        &format!("the replay answered with the same node ids: {first:?}"),
        first == again,
    )?;

    let fresh = client.execute_batch_with(&once_and_again, Some(&MutationId::new()))?;
    check(
        "and the same batch under a fresh id is refused, as a second create is",
        fresh
            .iter()
            .all(|part| matches!(part, Err(ClientError::Cluster { code: 501, .. }))),
    )?;

    step("A batch inside a transaction is invisible until the commit");
    let tx = client.start_transaction()?;
    let mut staged = BatchRequest::new();
    staged.create("table", &format!("{BASE}/staged"));
    let inside = tx.execute_batch(&staged)?;
    check(
        "the part succeeded inside the transaction",
        inside[0].is_ok(),
    )?;
    check(
        "and nothing shows outside before the commit",
        !client.exists(&format!("{BASE}/staged"))?,
    )?;
    tx.commit()?;
    check(
        "the commit publishes it",
        client.exists(&format!("{BASE}/staged"))?,
    )?;

    client.remove_tree(BASE)?;
    println!("\nAll checks passed.");
    Ok(())
}

/// The `index`-th table of the demo.
fn table(index: usize) -> String {
    format!("{BASE}/t{index}")
}

/// The node ids of a batch of creates, in part order — every part an `Ok`
/// answering under `node_id`, or the whole thing is a failure worth reporting.
fn node_ids(
    parts: Vec<Result<ytsaurus_yson::YsonValue, ClientError>>,
) -> Result<Vec<String>, ClientError> {
    parts
        .iter()
        .map(|part| {
            let answer = part
                .as_ref()
                .map_err(|e| ClientError::Config(format!("a create in the replay failed: {e}")))?;
            answer["node_id"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| {
                    ClientError::Config(format!("a create answered without a node id: {answer:?}"))
                })
        })
        .collect()
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
