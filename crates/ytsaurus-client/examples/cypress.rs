//! `cypress` — the rest of the tree: list, copy, move, link and lock.
//!
//! Cypress is where a pipeline keeps its results, and keeping them means
//! naming them: yesterday's run beside today's, a `latest` link that always
//! points at the newest, and a lock so two launchers do not publish over each
//! other.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example cypress
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ytsaurus_client::{Client, ClientError, LockMode};

/// Where the demo keeps its tree.
const BASE: &str = "//tmp/ytsaurus_rs_cypress";

/// The three runs the demo pretends have already happened.
const RUNS: [&str; 3] = ["2026-08-01", "2026-08-02", "2026-08-03"];

/// How long the lock demo lets the first transaction hold the node.
const HELD_FOR: Duration = Duration::from_secs(3);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ncypress failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;
    let runs = format!("{BASE}/runs");
    let latest = format!("{BASE}/latest");

    step("Preparing a tree that looks like a pipeline's output");
    client.remove(BASE)?;
    for (n, day) in RUNS.iter().enumerate() {
        let path = format!("{runs}/{day}");
        client.create("table", &path)?;
        client.write_table(&path, &rows(n + 1))?;
    }
    done(&format!("{} runs under {runs}", RUNS.len()));

    step("Listing what is there");
    let listed = client.list(&runs)?;
    println!("   as the cluster gives them: {listed:?}");
    // Sorted before comparing, because the cluster's order is its own: three
    // dated tables came back as the second, the third and then the first.
    let mut sorted = listed.clone();
    sorted.sort();
    check(
        &format!("the three runs are there: {sorted:?}"),
        sorted == RUNS,
    )?;
    match client.list(&format!("{runs}/{}", RUNS[0])) {
        Ok(names) => {
            return Err(ClientError::Config(format!(
                "listing a table answered with {names:?} instead of failing"
            )));
        }
        // Not an empty list: a table has no children, and asking is a mistake
        // rather than a question with a boring answer.
        Err(e) => done(&format!("and listing a table is an error: {e}")),
    }

    step("Copying and moving");
    let archive = format!("{BASE}/archive/{}", RUNS[0]);
    client.copy(&format!("{runs}/{}", RUNS[0]), &archive)?;
    done(&format!("copied into {archive}, parents and all"));

    match client.copy(&format!("{runs}/{}", RUNS[1]), &archive) {
        Ok(()) => {
            return Err(ClientError::Config(
                "a copy over an existing node succeeded, so nothing is safe".to_owned(),
            ));
        }
        Err(e) => done(&format!("a second copy is refused: {e}")),
    }

    client.copy_replacing(&format!("{runs}/{}", RUNS[1]), &archive)?;
    check(
        "copy_replacing overwrites it",
        client.row_count(&archive)? == 2,
    )?;

    client.move_node(&archive, &format!("{BASE}/archive/moved"))?;
    check(
        "a move leaves nothing behind",
        !client.exists(&archive)? && client.exists(&format!("{BASE}/archive/moved"))?,
    )?;

    step("A link, and the trap in reading one");
    client.link(&format!("{runs}/{}", RUNS[0]), &latest)?;
    check(
        "the link resolves to the run it points at",
        client.row_count(&latest)? == 1,
    )?;

    // `&` asks about the link; without it the question goes through to the
    // target and is answered as if the link were not there.
    let target = client.get(&format!("{latest}&/@target_path"))?;
    let followed = client.get(&format!("{latest}/@type"))?;
    check(
        &format!(
            "latest&/@target_path is {:?}, while latest/@type is {:?} — the target's",
            target.as_str().unwrap_or("?"),
            followed.as_str().unwrap_or("?")
        ),
        target.as_str() == Some(format!("{runs}/{}", RUNS[0]).as_str())
            && followed.as_str() == Some("table"),
    )?;

    client.link_replacing(&format!("{runs}/{}", RUNS[2]), &latest)?;
    check(
        "link_replacing points it at the newest run",
        client.row_count(&latest)? == 3,
    )?;

    step("Publishing by moving a staging table over the live one");
    let live = format!("{BASE}/live");
    client.create("table", &live)?;
    client.write_table(&live, &rows(1))?;

    let tx = client.start_transaction()?;
    let staging = format!("{BASE}/staging");
    tx.create("table", &staging)?;
    tx.write_table(&staging, &rows(9))?;
    tx.move_replacing(&staging, &live)?;
    check(
        "readers still see the old table while the transaction is open",
        client.row_count(&live)? == 1,
    )?;
    tx.commit()?;
    check(
        "and the new one the moment it commits",
        client.row_count(&live)? == 9,
    )?;

    step("A lock, and what it refuses");
    match client.lock(&live, LockMode::Exclusive) {
        Ok(_) => {
            return Err(ClientError::Config(
                "a lock outside a transaction succeeded, which the cluster does not allow"
                    .to_owned(),
            ));
        }
        // Refused here, without a round trip: there is nothing for a lock
        // outside a transaction to belong to.
        Err(e) => done(&format!("outside a transaction: {e}")),
    }

    let first = client.start_transaction()?;
    let held = first.lock(&live, LockMode::Exclusive)?;
    done(&format!(
        "transaction {} holds lock {}",
        first.id(),
        held.id
    ));

    let second = client.start_transaction()?;
    match second.lock(&live, LockMode::Exclusive) {
        Ok(_) => {
            return Err(ClientError::Config(
                "two transactions took the same exclusive lock".to_owned(),
            ));
        }
        Err(e) => done(&format!("the second is refused, and told who won: {e}")),
    }

    // A snapshot lock asks for something else entirely: not "nobody may write"
    // but "let me keep reading what I can see now".
    second.lock(&live, LockMode::Snapshot)?;
    done("but it can pin the version it is reading");

    step("Waiting for a lock");
    // This one can never be granted: the transaction is queueing behind its own
    // snapshot lock, which only ends when the transaction does. The cluster does
    // not say so — it queues the request and leaves it pending — so the deadline
    // is the only thing that ends the wait.
    match second.lock_waiting(&live, LockMode::Exclusive, Duration::from_secs(2)) {
        Ok(_) => {
            return Err(ClientError::Config(
                "a transaction took an exclusive lock over its own snapshot lock".to_owned(),
            ));
        }
        Err(e) => done(&format!("a wait that could never end, ended: {e}")),
    }
    drop(second);

    let releasing = std::thread::spawn(move || {
        std::thread::sleep(HELD_FOR);
        drop(first); // the abort, and with it the lock
    });

    // A waitable lock comes back `pending` and is granted later. This returns
    // when the cluster says `acquired`, not when it says it has queued.
    let third = client.start_transaction()?;
    let waited = third.lock_waiting(&live, LockMode::Exclusive, Duration::from_secs(60))?;
    releasing.join().expect("the holder thread finished");
    done(&format!(
        "granted as {} once the holder went away, {}s in",
        waited.id,
        HELD_FOR.as_secs()
    ));

    println!("\nA tree with names, links and locks. Left at {BASE}");
    Ok(())
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

/// `count` rows, so a table can be told apart by its size.
fn rows(count: usize) -> Vec<u8> {
    use serde::Serialize;
    use ytsaurus_yson::{YsonFormat, to_vec};

    #[derive(Serialize)]
    struct Row {
        n: i64,
    }

    let mut out = Vec::new();
    for n in 0..count {
        let row = Row { n: n as i64 };
        out.extend_from_slice(&to_vec(&row, YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    out
}
