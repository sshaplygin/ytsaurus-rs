//! Transactions: several commands, one all-or-nothing outcome.
//!
//! A launcher that creates a table, uploads a worker and runs an operation has
//! three ways to fail halfway, and each leaves something behind: an empty
//! table, a stale binary, an output table holding the previous run's rows. A
//! transaction makes the whole sequence one event — everything appears when it
//! commits, and nothing does when it does not.
//!
//! # Two things the cluster insists on
//!
//! **A transaction expires.** The cluster gives it 30 seconds and then aborts
//! it, unless something says it is still wanted. Verified on a local cluster: a
//! transaction with a two-second timeout, left alone for four, answers a ping
//! with `Transaction … has expired or was aborted`. [`Transaction`] therefore
//! keeps a thread pinging for as long as the handle lives, which is what makes
//! it usable around an operation that runs for an hour.
//!
//! **Nothing outside the transaction can see its work.** That is the point, and
//! it is also the trap: a `read_table` from a client that is not in the
//! transaction reads the table as it was before, and a second writer blocks on
//! the lock the first one took.

use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use ytsaurus_yson::YsonNode;

use crate::error::{ClientError, Result};
use crate::http::{Method, Payload};
use crate::retry::Repeatable;
use crate::{Client, yson_build};

/// What the cluster itself defaults to, and what this crate asks for.
///
/// Sent explicitly rather than left out, because the ping interval is derived
/// from it: a client that assumed the wrong default would ping too slowly and
/// lose the transaction.
pub(crate) const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);

/// A transaction, alive for as long as this handle is.
///
/// Obtained from [`Client::start_transaction`]. It derefs to a [`Client`] bound
/// to it, so every command sent through it happens inside the transaction:
///
/// ```no_run
/// # use ytsaurus_client::Client;
/// # fn main() -> Result<(), ytsaurus_client::ClientError> {
/// # let client = Client::from_env()?;
/// # let rows: Vec<u8> = Vec::new();
/// let tx = client.start_transaction()?;
///
/// tx.create("table", "//tmp/out")?;
/// tx.write_table("//tmp/out", &rows)?;
///
/// tx.commit()?;                     // now //tmp/out exists, with its rows
/// # Ok(())
/// # }
/// ```
///
/// **Dropping it aborts it.** That is what makes the `?` on those two lines
/// safe: a failure anywhere returns from the function, the handle drops on the
/// way out, and the cluster is left as it was. Only [`Transaction::commit`]
/// publishes anything.
pub struct Transaction {
    /// A client bound to this transaction.
    client: Client,
    id: String,
    /// Set by whichever of commit/abort ran, so `Drop` does not send a second.
    done: bool,
    keep_alive: Option<KeepAlive>,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("id", &self.id)
            .field("done", &self.done)
            .finish()
    }
}

impl Transaction {
    pub(crate) fn start(client: &Client, timeout: Duration) -> Result<Self> {
        let millis = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        let params = yson_build::map([("timeout", yson_build::int(millis))]);

        let body = client.transport.call(
            Method::Post,
            "start_transaction",
            &params,
            Payload::None,
            // Under a mutation ID, so a retried start cannot leave a
            // transaction nobody holds a handle to — it would hold its locks
            // until it expired.
            Repeatable::WithMutationId,
        )?;

        let value = client.value_field(&body, "transaction_id")?;
        let YsonNode::String(bytes) = &value.node else {
            return Err(ClientError::Decode {
                command: "start_transaction".to_owned(),
                reason: format!("transaction_id is not a string: {:?}", value.node),
            });
        };
        let id = String::from_utf8_lossy(bytes).into_owned();

        let client = client.clone().with_transaction(&id);
        let keep_alive = KeepAlive::spawn(client.clone(), id.clone(), ping_interval(timeout));

        Ok(Self {
            client,
            id,
            done: false,
            keep_alive,
        })
    }

    /// The transaction's ID, as the cluster named it.
    ///
    /// Worth logging: it is what identifies the transaction in the web UI, and
    /// what [`Client::with_transaction`] needs to rejoin it from elsewhere.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The client bound to this transaction.
    ///
    /// Rarely needed — [`Transaction`] derefs to it — but a `&Client` is what a
    /// function taking one wants to be handed.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Publishes everything done in the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the commit fails, which leaves the
    /// transaction aborted and nothing published.
    pub fn commit(mut self) -> Result<()> {
        self.finish("commit_transaction")
    }

    /// Discards everything done in the transaction.
    ///
    /// The same thing dropping the handle does, for when it should read as a
    /// decision rather than as a scope ending.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails. The transaction expires on
    /// its own either way, once nothing is pinging it.
    pub fn abort(mut self) -> Result<()> {
        self.finish("abort_transaction")
    }

    /// Tells the cluster the transaction is still wanted.
    ///
    /// The handle does this on its own; this is for a process that wants to
    /// check the transaction is still there — a ping is how the cluster reports
    /// that it is not.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the transaction has expired or was aborted.
    pub fn ping(&self) -> Result<()> {
        ping(&self.client, &self.id)
    }

    fn finish(&mut self, command: &'static str) -> Result<()> {
        self.stop_pinging();

        if self.done {
            return Ok(());
        }
        self.done = true;

        let params = yson_build::map([("transaction_id", yson_build::string(&self.id))]);
        self.client.transport.call(
            Method::Post,
            command,
            &params,
            Payload::None,
            // A commit that is retried after its answer was lost must not be a
            // second commit: the cluster refuses that with `No such
            // transaction`, which reads like the commit failed when it
            // succeeded. The mutation ID makes the retry the same commit.
            Repeatable::WithMutationId,
        )?;
        Ok(())
    }

    fn stop_pinging(&mut self) {
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.stop();
        }
    }
}

impl Deref for Transaction {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.client
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.done {
            self.stop_pinging();
            return;
        }

        // Abandoning it would work too — an unpinged transaction expires — but
        // it would hold its locks until then, and a failed launcher should not
        // block the next attempt for half a minute. The error is dropped
        // because a destructor has nowhere to report one, and because the
        // cluster accepts an abort of a transaction that is already gone.
        let _ = self.finish("abort_transaction");
    }
}

/// Sends one ping.
fn ping(client: &Client, id: &str) -> Result<()> {
    let params = yson_build::map([("transaction_id", yson_build::string(id))]);
    client.transport.call(
        Method::Post,
        "ping_transaction",
        &params,
        Payload::None,
        // A ping says "still here"; sending it twice says it twice.
        Repeatable::Freely,
    )?;
    Ok(())
}

/// How often to ping a transaction with this timeout.
///
/// A third of it, so a lost ping is not a lost transaction. The floor is for a
/// caller who asks for a timeout of milliseconds: below three seconds the
/// pings stop keeping up, which is the right answer — a transaction that short
/// is one that is meant to expire.
fn ping_interval(timeout: Duration) -> Duration {
    (timeout / 3).max(Duration::from_secs(1))
}

/// The thread that keeps one transaction alive.
struct KeepAlive {
    /// Raised to ask the thread to stop; the condvar wakes it out of its wait.
    stop: Arc<(Mutex<bool>, Condvar)>,
}

impl KeepAlive {
    /// Starts pinging `id` every `interval`.
    ///
    /// `None` if the thread could not be spawned. The transaction still works;
    /// it just has to finish within its timeout, which is a better outcome than
    /// refusing to start one at all.
    fn spawn(client: Client, id: String, interval: Duration) -> Option<Self> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let signal = Arc::clone(&stop);

        std::thread::Builder::new()
            .name("yt-transaction-ping".to_owned())
            .spawn(move || {
                let (lock, wake) = &*signal;
                loop {
                    {
                        let guard = lock.lock().unwrap_or_else(PoisonError::into_inner);
                        if *guard {
                            return;
                        }
                        let (guard, _) = wake
                            .wait_timeout(guard, interval)
                            .unwrap_or_else(PoisonError::into_inner);
                        // Checked again on the way out, not only on the way in:
                        // a stop raised *during* a ping arrives while nothing is
                        // waiting on the condvar, so the notification is missed
                        // and only this test catches it.
                        if *guard {
                            return;
                        }
                    }

                    // A failed ping is not fatal on its own — the transport has
                    // already retried it, and there is time for the next one
                    // before the transaction expires. Whatever the ping would
                    // have said, the next command in the transaction says too,
                    // and that one has a caller to report it to.
                    let _ = ping(&client, &id);
                }
            })
            .ok()
            .map(|_handle| Self { stop })
    }

    /// Asks the thread to stop, without waiting for it.
    ///
    /// Not joined on purpose: the thread may be inside a ping, and a request
    /// can take as long as the client's timeout. Blocking a `Drop` for two
    /// minutes to tidy up a thread that is about to exit on its own would be a
    /// worse bargain than letting a stray ping land on a committed
    /// transaction, which the cluster answers with an error nobody reads.
    fn stop(self) {
        let (lock, wake) = &*self.stop;
        *lock.lock().unwrap_or_else(PoisonError::into_inner) = true;
        wake.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lost_ping_is_not_a_lost_transaction() {
        // The invariant, whatever else changes: three pings fit inside one
        // timeout, so one going missing costs nothing.
        for seconds in [3, 30, 60, 3600] {
            let timeout = Duration::from_secs(seconds);
            let interval = ping_interval(timeout);
            assert!(
                interval * 3 <= timeout,
                "{seconds}s timeout pinged every {interval:?}"
            );
        }
    }

    #[test]
    fn a_timeout_below_the_floor_is_the_callers_business() {
        // The floor is what keeps a caller who asks for 50 ms from turning the
        // ping thread into a load generator. A transaction that short is one
        // that is meant to expire.
        assert_eq!(
            ping_interval(Duration::from_millis(50)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn the_keep_alive_thread_stops_when_asked() {
        // The transaction it would ping does not exist, so every ping fails;
        // the thread must survive that and still exit on request. A thread that
        // died on the first failed ping would leave real transactions to
        // expire.
        let client = Client::new("http://127.0.0.1:1").with_retries(crate::RetryPolicy::none());
        let keep_alive = KeepAlive::spawn(client, "1-2-3-4".to_owned(), Duration::from_millis(1))
            .expect("the thread starts");

        let stop = Arc::clone(&keep_alive.stop);
        keep_alive.stop();

        assert!(
            *stop.0.lock().expect("not poisoned"),
            "stop() must raise the flag the thread waits on"
        );
    }
}
