//! `detach` — hand a live transaction to another client.
//!
//! One client starts a transaction, creates a table only it can see, and
//! detaches: the handle is gone and the transaction is not. A second client —
//! standing in for another process — attaches by id, keeps it alive past its
//! own timeout, and commits it; only then does the table exist for anyone
//! else. The other half of the contract is checked too: an *attached* handle
//! dropped mid-work leaves the transaction running, where a *started* one
//! dropped mid-work still aborts it.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example detach
//! ```

use std::process::ExitCode;
use std::time::{Duration, Instant};

use ytsaurus_client::{Client, ClientError};

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_detach";

/// A transaction short enough that whoever holds it must really be pinging.
const SHORT: Duration = Duration::from_secs(3);

/// How long the second client holds it — several timeouts' worth, so the
/// commit succeeding proves the attached handle's pings did the keeping alive.
const HELD: Duration = Duration::from_secs(7);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\ndetach failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let first = Client::from_env()?;
    let second = Client::from_env()?; // its own connections, like another process

    step("Preparing Cypress");
    first.remove_tree(BASE)?;
    first.create("map_node", BASE)?;
    let staging = format!("{BASE}/staging");
    done("clean slate");

    step(&format!(
        "Starting a {}s transaction and detaching",
        SHORT.as_secs()
    ));
    let started = Instant::now();
    let tx = first.start_transaction_with(SHORT)?;
    tx.create("table", &staging)?;
    check("the transaction sees its table", tx.exists(&staging)?)?;
    let id = tx.detach(); // the handle ends here; the transaction does not
    println!("   detached {id}");
    check(
        "the first client no longer sees the table",
        !first.exists(&staging)?,
    )?;

    step("Attaching from the second client");
    let attached = second.attach_transaction(&id)?;
    check(
        "the attached handle sees the table",
        attached.exists(&staging)?,
    )?;

    step(&format!(
        "Holding it for {}s — past its own timeout",
        HELD.as_secs()
    ));
    std::thread::sleep(HELD);
    // Nothing in this function pinged anything. If the attached handle's
    // thread were not doing it, the transaction would have expired seconds
    // ago and the commit below would fail with `No such transaction`.
    attached.commit()?;
    check(
        &format!(
            "committed by the second client, {:.0}s after the start",
            started.elapsed().as_secs_f64()
        ),
        first.exists(&staging)?,
    )?;

    step("An attached handle dropped mid-work leaves the transaction alive");
    let orphan = {
        let tx = first.start_transaction()?;
        tx.detach()
    };
    {
        let attached = second.attach_transaction(&orphan)?;
        attached.ping()?;
    } // dropped here — attached, so this detaches rather than aborts
    second.ping_transaction(&orphan)?;
    done("still answers a ping after the attached handle dropped");

    step("A bare id is enough to finish it");
    second.abort_transaction(&orphan)?;
    match second.ping_transaction(&orphan) {
        Ok(()) => {
            return Err(ClientError::Config(
                "a ping succeeded after the abort, which means the abort did not happen".to_owned(),
            ));
        }
        Err(e) => done(&format!("aborted by id; as expected: {e}")),
    }

    step("A started handle dropped mid-work still aborts — unchanged");
    let watched = {
        let tx = first.start_transaction()?;
        tx.id().to_owned()
    }; // dropped here — started, so this aborts
    match second.ping_transaction(&watched) {
        Ok(()) => {
            return Err(ClientError::Config(
                "a started handle's drop no longer aborts its transaction".to_owned(),
            ));
        }
        Err(e) => done(&format!("as expected: {e}")),
    }

    println!("\nA transaction outlived its handle and finished in other hands.");
    println!("Tables left at {BASE}");
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
