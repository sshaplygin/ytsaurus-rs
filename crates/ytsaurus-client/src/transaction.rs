//! Transactions: several commands, one all-or-nothing outcome.
//!
//! Everything done in a transaction appears when it commits, and nothing does
//! when it does not, so a launcher that fails halfway leaves no empty table or
//! stale binary behind. Nothing outside the transaction sees its work: a
//! `read_table` from another client reads the table as it was, and a second
//! writer blocks on the lock the first one took.
//!
//! The cluster aborts a transaction its timeout (30 seconds by default) after
//! its last ping, so [`Transaction`] pings from a thread for as long as the
//! handle lives. [`Transaction::detach`] stops the pings and returns the id;
//! [`Client::attach_transaction`] turns an id back into a pinging handle, and
//! [`Client::ping_transaction`], [`Client::commit_transaction`] and
//! [`Client::abort_transaction`] act on a bare id.
//!
//! A started handle aborts on drop, which makes `?` safe inside a transaction.
//! An attached one detaches on drop, so an attacher cannot destroy what the
//! starter still counts on; the C++ client's destructor draws the same line.
//! See [Transactions](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#transactions).

use std::convert::Infallible;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use ytsaurus_yson::{YsonNode, YsonValue};

use crate::error::{ClientError, Result};
use crate::http::{Method, Payload};
use crate::retry::{Repeatable, RetryPolicy};
use crate::{Client, yson_build};

/// The cluster's default timeout, sent explicitly because the ping interval is
/// derived from it.
pub(crate) const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the abort sent from `Drop` may take: a destructor, perhaps in a
/// panic unwind, must not hang on an unreachable cluster, and a lost abort
/// expires anyway once nothing pings.
const DROP_ABORT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long [`Transaction::detach`] waits for the keep-alive thread, whose
/// ping budget can reach two minutes; `detach`'s documentation gives the
/// arithmetic.
const DETACH_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// A transaction, alive for as long as this handle is.
///
/// Obtained from [`Client::start_transaction`]. It derefs to a [`Client`] bound
/// to it, so every command sent through it happens inside the transaction.
/// Dropping it aborts it, so a `?` leaves the cluster as it was; only
/// [`Transaction::commit`] publishes anything.
///
/// ```no_run
/// # use ytsaurus_client::Client;
/// # let client = Client::from_env()?;
/// # let rows: Vec<u8> = Vec::new();
/// let tx = client.start_transaction()?;
/// tx.create("table", "//tmp/out")?;
/// tx.write_table("//tmp/out", &rows)?;
/// tx.commit()?; // now //tmp/out exists, with its rows
/// # Ok::<(), ytsaurus_client::ClientError>(())
/// ```
pub struct Transaction {
    /// A client bound to this transaction.
    client: Client,
    id: String,
    /// Set by whichever of commit/abort/detach ran, so `Drop` sends nothing.
    done: bool,
    keep_alive: Option<KeepAlive>,
    origin: Origin,
}

/// How a handle came to hold its transaction, which is what `Drop` turns on.
#[derive(Clone, Copy, Debug)]
enum Origin {
    /// Started by this handle: dropping it aborts.
    Started,
    /// Attached to a transaction started elsewhere: dropping it detaches.
    Attached,
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
            // Under a mutation ID, so a retried start cannot leave an orphan
            // transaction holding locks until it expires. A retried start
            // returns the first attempt's transaction, whose clock started
            // then; unlike `attach`, it is not pinged here, which would cost a
            // round trip on every start.
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

        Ok(Self::held(client, id, timeout, Origin::Started))
    }

    pub(crate) fn attach(client: &Client, id: String) -> Result<Self> {
        // Read the timeout to know the ping interval, and to fail here rather
        // than on a later command if the transaction is gone.
        let value = client
            .get(&format!("#{id}/@timeout"))
            .map_err(|error| attach_failed(&id, error))?;
        let timeout = attached_timeout(&id, &value)?;

        // Ping once before the handle exists: `@timeout` is the configured
        // lifetime, not the remaining one, and the keep-alive's first ping is
        // an interval away, so a handoff longer than two thirds of the timeout
        // would yield a handle whose transaction has already expired. Under the
        // caller's retry policy, since the caller waits on this verdict.
        ping(client, &id).map_err(|error| attach_failed(&id, error))?;

        Ok(Self::held(client, id, timeout, Origin::Attached))
    }

    /// A handle around `id`, pinging every third of `timeout`.
    fn held(client: &Client, id: String, timeout: Duration, origin: Origin) -> Self {
        let client = client.clone().with_transaction(&id);
        let interval = ping_interval(timeout);

        // One attempt, bounded under the interval: a ping on the full retry
        // pipeline could stall for minutes while the transaction expired. A
        // lost ping costs nothing; the next one is its retry.
        let mut ping_client = client.clone();
        ping_client.transport.set_retries(RetryPolicy::none());
        ping_client
            .transport
            .set_timeout(ping_request_timeout(interval));
        let keep_alive = KeepAlive::spawn(ping_client, id.clone(), interval);

        Self {
            client,
            id,
            done: false,
            keep_alive,
            origin,
        }
    }

    /// The transaction's ID: what the web UI shows, and what
    /// [`Client::with_transaction`] needs to rejoin it from elsewhere.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The client bound to this transaction, for a function that takes a
    /// `&Client`; [`Transaction`] also derefs to it.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Publishes everything done in the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the commit fails. The handle is consumed
    /// either way and `Drop` then aborts, so nothing is published and no lock
    /// is held until expiry.
    pub fn commit(mut self) -> Result<()> {
        self.finish("commit_transaction")
    }

    /// Discards everything done in the transaction, as dropping the handle
    /// does, but as an explicit decision.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the request fails. The transaction expires
    /// on its own either way, once nothing pings it.
    pub fn abort(mut self) -> Result<()> {
        self.finish("abort_transaction")
    }

    /// Tells the cluster the transaction is still wanted. The handle pings on
    /// its own; this is how a process checks that the transaction still exists.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the transaction has expired or was aborted.
    pub fn ping(&self) -> Result<()> {
        ping(&self.client, &self.id)
    }

    /// Whether the keep-alive has given up: a ping was answered "no such
    /// transaction", because the transaction expired or was aborted or
    /// committed elsewhere.
    ///
    /// False means only that no ping was answered that way yet;
    /// [`Transaction::ping`] asks. It is also false with nothing pinging, when
    /// the keep-alive thread failed to spawn or panicked, and a ping answers for
    /// the transaction, not the thread; ping or attach afresh to be sure. Once
    /// the handle is detached, [`Client::ping_transaction`] on the id is the
    /// only probe.
    #[must_use]
    pub fn is_lost(&self) -> bool {
        self.keep_alive.as_ref().is_some_and(KeepAlive::lost)
    }

    /// Stops keeping the transaction alive, leaves it running, and returns its
    /// id: C++'s `ITransaction::Detach()`.
    ///
    /// Nothing is committed or aborted. The transaction expires its timeout
    /// after its last ping, 30 seconds by default, unless another process
    /// re-holds it with [`Client::attach_transaction`] or finishes it with
    /// [`Client::commit_transaction`] or [`Client::abort_transaction`].
    ///
    /// The keep-alive is stopped and waited for, up to five seconds; one last
    /// ping may restart the clock. A ping's budget is
    /// `clamp(interval / 2, 1 s, 120 s)` with `interval = max(timeout / 3, 1 s)`:
    /// under the five-second wait for a timeout below 30 s, and equal to it at
    /// the 30 s default. Above the default, a stalled ping can land after this
    /// returns, and the transaction then lives a full timeout from there; the
    /// thread exits when that ping ends. See [Handing a transaction
    /// to another process](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#handing-a-transaction-to-another-process).
    ///
    /// `mem::forget` on a [`Transaction`] instead leaks a thread that pings,
    /// holding the transaction and its locks, for the life of the process.
    #[must_use = "the id is the only way left to reach the transaction"]
    pub fn detach(mut self) -> String {
        // `Drop` still runs when this consumes the handle; `done` is what
        // makes it send nothing.
        self.done = true;
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.stop_and_join();
        }
        self.id.clone()
    }

    fn finish(&mut self, command: &'static str) -> Result<()> {
        if self.done {
            self.stop_pinging();
            return Ok(());
        }

        let params = yson_build::map([("transaction_id", yson_build::string(&self.id))]);
        // Sent while the pings still run: a commit can outlast the
        // transaction's timeout (a two-minute request timeout plus retries,
        // against 30 s), and an expired transaction answers `No such
        // transaction`.
        let outcome = self.client.transport.call(
            Method::Post,
            command,
            &params,
            Payload::None,
            // A retried commit must be the same commit: a second one is refused
            // with `No such transaction`, which reads as if the first failed.
            Repeatable::WithMutationId,
        );

        // Only a terminal answer ends the transaction. A failed commit still
        // holds its locks, so `done` stays unset and `Drop` aborts it. A failed
        // abort is finished: repeating it in `Drop` would only spend the retry
        // budget twice.
        self.done = outcome.is_ok() || command == "abort_transaction";
        self.stop_pinging();

        outcome.map(|_| ())
    }

    /// Asks the keep-alive to stop, and drops it; idempotent. Dropping it also
    /// drops the flag [`Transaction::is_lost`] reads, so a non-terminal caller
    /// would have to carry that verdict out first.
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

        if matches!(self.origin, Origin::Attached) {
            // An attached handle does not own the transaction: detach, sending
            // nothing, so an attacher's `?` cannot destroy the starter's work.
            self.stop_pinging();
            return;
        }

        // Abort rather than abandon, so a failed launcher's locks do not block
        // the next attempt until expiry. One bounded attempt: a destructor must
        // not block for the full retry budget, and a lost abort expires anyway.
        // The error is dropped: there is nowhere to report it, and the cluster
        // accepts an abort of a transaction already gone. `abort()` keeps the
        // full retries.
        self.client.transport.set_retries(RetryPolicy::none());
        self.client.transport.set_timeout(DROP_ABORT_TIMEOUT);
        let _ = self.finish("abort_transaction");
    }
}

/// Sends one ping.
pub(crate) fn ping(client: &Client, id: &str) -> Result<()> {
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

/// Commits a transaction held as nothing but an id. [`Transaction::commit`]
/// goes through `finish`, which also stops the pings.
pub(crate) fn commit_by_id(client: &Client, id: &str) -> Result<()> {
    let params = yson_build::map([("transaction_id", yson_build::string(id))]);
    client.transport.call(
        Method::Post,
        "commit_transaction",
        &params,
        Payload::None,
        // Not idempotent: the mutation ID makes a retried commit the same one.
        Repeatable::WithMutationId,
    )?;
    Ok(())
}

/// `#<id>/@timeout`, as a duration. The cluster answers `Int64`; `Uint64` is
/// accepted too. Anything else, zero or negative included, fails the attach
/// naming the attribute: read as zero, it would floor [`ping_interval`] and
/// leave a 1 Hz pinger running.
fn attached_timeout(id: &str, value: &YsonValue) -> Result<Duration> {
    let millis = match value.node {
        YsonNode::Int64(millis) if millis > 0 => u64::try_from(millis).ok(),
        YsonNode::Uint64(millis) if millis > 0 => Some(millis),
        _ => None,
    };

    millis
        .map(Duration::from_millis)
        .ok_or_else(|| ClientError::Decode {
            command: "attach_transaction".to_owned(),
            reason: format!(
                "#{id}/@timeout is not a positive number of milliseconds: {:?}",
                value.node
            ),
        })
}

/// Rewrites a failed timeout read as a failed attach naming the id, keeping the
/// code and raw document. A garbage id such as `1-2-3-4` is refused as
/// `cluster error 1: Unknown cell tag 0`, which names neither.
fn attach_failed(id: &str, error: ClientError) -> ClientError {
    match error {
        ClientError::Cluster {
            code, message, raw, ..
        } => ClientError::Cluster {
            command: "attach_transaction".to_owned(),
            code,
            message: format!("cannot attach to transaction {id}: {message}"),
            raw,
        },
        other => other,
    }
}

/// Aborts a transaction that is held as nothing but an id.
pub(crate) fn abort_by_id(client: &Client, id: &str) -> Result<()> {
    let params = yson_build::map([("transaction_id", yson_build::string(id))]);
    client.transport.call(
        Method::Post,
        "abort_transaction",
        &params,
        Payload::None,
        // An abort of a transaction already gone answers `{}`, so a repeat
        // needs no mutation ID.
        Repeatable::Freely,
    )?;
    Ok(())
}

/// How often to ping: a third of the timeout, so one lost ping is not a lost
/// transaction, and never more often than once a second. Below a 3 s timeout
/// the pings fall behind, which suits a transaction that is meant to expire.
fn ping_interval(timeout: Duration) -> Duration {
    (timeout / 3).max(Duration::from_secs(1))
}

/// One ping request's budget: half the interval, so a stalled ping leaves the
/// next one room, between one second and the transport's two minutes.
fn ping_request_timeout(interval: Duration) -> Duration {
    (interval / 2)
        .max(Duration::from_secs(1))
        .min(crate::DEFAULT_TIMEOUT)
}

/// Whether the cluster says the transaction no longer exists: code 11000
/// (`NoSuchTransaction`) or the master's other spelling, looked for in the
/// whole document because the outer error is often a wrapper. Anything else is
/// indistinguishable from a live transaction, so the pings continue.
fn transaction_is_gone(error: &ClientError) -> bool {
    match error {
        ClientError::Cluster { code, raw, .. } => {
            *code == 11000
                || raw.contains("No such transaction")
                || raw.contains("has expired or was aborted")
        }
        _ => false,
    }
}

/// The thread that keeps one transaction alive.
struct KeepAlive {
    /// Raised to ask the thread to stop; the condvar wakes it out of its wait.
    stop: Arc<(Mutex<bool>, Condvar)>,
    /// Raised by the thread when it gave up; read by [`Transaction::is_lost`].
    lost: Arc<AtomicBool>,
    /// Disconnects when the thread's body ends, on every path; nothing is sent
    /// on it. A `recv_timeout` on it is the timed join `std` lacks.
    exited: Receiver<Infallible>,
    /// Reaped once `exited` says the body ended; joining it directly has no bound.
    thread: std::thread::JoinHandle<()>,
}

impl KeepAlive {
    /// Starts pinging `id` every `interval`. `None` if the thread could not be
    /// spawned; the transaction then has to finish within its timeout.
    fn spawn(client: Client, id: String, interval: Duration) -> Option<Self> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let signal = Arc::clone(&stop);
        let lost = Arc::new(AtomicBool::new(false));
        let give_up = Arc::clone(&lost);
        let (alive, exited) = std::sync::mpsc::channel::<Infallible>();

        std::thread::Builder::new()
            .name("yt-transaction-ping".to_owned())
            .spawn(move || {
                // Never sent on: dropping it, however the thread ends, wakes
                // `stop_and_join`.
                let _alive = alive;
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
                        // Checked after the wait too: a stop raised during a
                        // ping finds nobody on the condvar, so its wake is lost.
                        if *guard {
                            return;
                        }
                    }

                    // A failed ping is retried by the next one, but "no such
                    // transaction" is final: stop and raise `lost` rather than
                    // ping a transaction that cannot come back.
                    if let Err(error) = ping(&client, &id)
                        && transaction_is_gone(&error)
                    {
                        give_up.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            })
            .ok()
            .map(|thread| Self {
                stop,
                lost,
                exited,
                thread,
            })
    }

    /// Whether the thread gave up because the transaction is gone.
    fn lost(&self) -> bool {
        self.lost.load(Ordering::Relaxed)
    }

    /// Asks the thread to stop, without waiting: a ping in flight may take the
    /// client's whole timeout, and a stray ping on a finished transaction only
    /// earns an error nobody reads.
    fn stop(self) {
        self.raise();
    }

    /// Asks the thread to stop and waits for it, up to [`DETACH_JOIN_TIMEOUT`],
    /// so no ping lands after [`Transaction::detach`] returns. The wait is a
    /// `recv_timeout` on `exited`, since a plain `join()` would wait out the
    /// ping's whole budget.
    fn stop_and_join(self) {
        self.raise();
        if matches!(
            self.exited.recv_timeout(DETACH_JOIN_TIMEOUT),
            Err(RecvTimeoutError::Disconnected)
        ) {
            // The body has ended, so this only reaps. An `Err` is a panic in
            // the thread, which this path must not turn into its own.
            let _ = self.thread.join();
        }
    }

    fn raise(&self) {
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

    /// A handle around `1-2-3-4` on `proxy`, unfinished and not pinging.
    fn handle_at(proxy: &str, origin: Origin) -> Transaction {
        let client = Client::new(proxy).with_retries(crate::RetryPolicy::none());
        Transaction {
            client: client.with_transaction("1-2-3-4"),
            id: "1-2-3-4".to_owned(),
            done: false,
            keep_alive: None,
            origin,
        }
    }

    /// A transaction whose commit is going nowhere: nothing listens on port 1.
    fn doomed() -> Transaction {
        handle_at("http://127.0.0.1:1", Origin::Started)
    }

    /// A socket that answers nothing and counts what reaches it.
    ///
    /// The point of a *bound* listener rather than a port nothing listens on:
    /// "nothing was sent" and "something was sent to a closed port" look the
    /// same to a caller who drops the error, which is every destructor here.
    /// A connection arriving is the evidence. Nothing is written back, so the
    /// sender sees the connection close and fails — quickly, which is all
    /// these tests need of it.
    fn watched_proxy() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let proxy = format!("http://{}", listener.local_addr().expect("has an address"));
        let arrived = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let counted = Arc::clone(&arrived);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stream.is_err() {
                    return;
                }
                counted.fetch_add(1, Ordering::Relaxed);
            }
        });

        (proxy, arrived)
    }

    /// Whether `arrived` reaches `wanted` within `budget`.
    fn connections_reach(
        arrived: &Arc<std::sync::atomic::AtomicUsize>,
        wanted: usize,
        budget: Duration,
    ) -> bool {
        let deadline = std::time::Instant::now() + budget;
        while std::time::Instant::now() < deadline {
            if arrived.load(Ordering::Relaxed) >= wanted {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        arrived.load(Ordering::Relaxed) >= wanted
    }

    #[test]
    fn a_commit_that_failed_leaves_drop_an_abort_to_send() {
        // The bug this pins down: marking the transaction finished before the
        // commit was answered. `Drop` would then send nothing, and a
        // transaction that is neither committed nor aborted nor pinged sits on
        // its locks until it expires — an hour, for an hour-long timeout.
        let mut tx = doomed();

        assert!(tx.finish("commit_transaction").is_err());
        assert!(!tx.done, "a failed commit has not finished the transaction");

        // And the abort that `Drop` would send does finish it, whether or not
        // the cluster heard it: there is nothing left to undo.
        assert!(tx.finish("abort_transaction").is_err());
        assert!(tx.done);
    }

    #[test]
    fn a_transaction_is_finished_once() {
        let mut tx = doomed();
        tx.done = true;

        // No request at all — the second call would be a second commit.
        assert!(tx.finish("commit_transaction").is_ok());
    }

    #[test]
    fn only_a_definitive_answer_stops_the_pinging() {
        let gone_by_code = ClientError::Cluster {
            command: "ping_transaction".into(),
            code: 11000,
            message: "whatever spelling".into(),
            raw: "{}".into(),
        };
        assert!(transaction_is_gone(&gone_by_code));

        let gone_by_text = ClientError::Cluster {
            command: "ping_transaction".into(),
            code: 1,
            message: "Error resolving path".into(),
            raw: r#"{"inner_errors"=[{"message"="No such transaction 1-2-3-4"}]}"#.into(),
        };
        assert!(transaction_is_gone(&gone_by_text));

        // A busy master or an unreachable proxy says nothing about the
        // transaction; the thread must keep pinging.
        let transient = ClientError::Cluster {
            command: "ping_transaction".into(),
            code: 1,
            message: "master is not ready".into(),
            raw: "{}".into(),
        };
        assert!(!transaction_is_gone(&transient));
        assert!(!transaction_is_gone(&ClientError::Config("x".into())));
    }

    #[test]
    fn a_stalled_ping_leaves_room_for_the_next_one() {
        // Half the interval, floored and capped: the request must not be able
        // to consume the slot of the ping after it.
        for seconds in [3, 30, 3600, 100_000] {
            let interval = ping_interval(Duration::from_secs(seconds));
            let bound = ping_request_timeout(interval);
            assert!(bound * 2 <= interval.max(Duration::from_secs(2)));
            assert!(bound <= crate::DEFAULT_TIMEOUT);
        }
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

    #[test]
    fn stop_and_join_waits_for_a_ping_it_caught_in_flight() {
        // What `detach` buys with the join, measured: a ping already on the
        // wire is finished before this returns. The proxy accepts and holds
        // the connection, so the ping is reliably in flight when the stop is
        // raised, and `stop_and_join` must not come back before it is over.
        // Plain `stop()` returns in ~0 ms here; that difference is the assert.
        //
        // (A thread ignoring the stop would hang instead of failing. libtest
        // has no per-test timeout, so that would stall the whole run — hence
        // the bound in `stop_and_join` itself, which caps the damage at
        // `DETACH_JOIN_TIMEOUT` even then.)
        const HELD: Duration = Duration::from_millis(400);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let proxy = format!("http://{}", listener.local_addr().expect("has an address"));
        let (accepted, an_accept) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                accepted.send(()).ok();
                // Held, unanswered, then closed: the ping fails at HELD rather
                // than waiting out its own one-second request budget.
                std::thread::sleep(HELD);
                drop(stream);
            }
        });

        let client = Client::new(&proxy).with_retries(crate::RetryPolicy::none());
        let interval = Duration::from_millis(1);
        let mut ping_client = client.clone();
        ping_client
            .transport
            .set_timeout(ping_request_timeout(interval));
        let keep_alive = KeepAlive::spawn(ping_client, "1-2-3-4".to_owned(), interval)
            .expect("the thread starts");

        an_accept
            .recv_timeout(Duration::from_secs(5))
            .expect("a ping reached the proxy");

        let waited = std::time::Instant::now();
        keep_alive.stop_and_join();
        let waited = waited.elapsed();

        assert!(
            waited >= HELD / 2,
            "stop_and_join returned in {waited:?}, so it did not wait out the ping it caught"
        );
    }

    #[test]
    fn stop_and_join_gives_up_on_a_ping_that_outlasts_the_bound() {
        // The other half of the bound, and the half nothing guarded. The test
        // above asserts only that `stop_and_join` *waits*; a plain unbounded
        // `join()` passes it just as well, and then `detach` on an hour-long
        // transaction against a hung proxy holds its caller for the ping's own
        // two-minute budget. This is the upper bound.
        //
        // It is what makes the mechanism testable rather than the wait: the
        // proxy accepts and never answers or closes, and the ping's request
        // timeout is set six times [`DETACH_JOIN_TIMEOUT`], so a
        // `stop_and_join` that had degraded to a plain join — which is exactly
        // what dropping the thread's `_alive` sender produces, since the
        // channel then reports `Disconnected` at once — returns at the request
        // timeout instead, six times late.
        const PING_BUDGET: Duration = Duration::from_secs(30);
        // Two seconds of headroom on a five-second bound, and 25 s of distance
        // to the failure it looks for. Alone among the timing assertions here
        // this one is an *upper* bound, so load pushes it toward its threshold
        // rather than away — but all that is between the bound and this
        // measurement is a `recv_timeout` waking and one `Instant::elapsed`,
        // which scheduler latency moves by a constant, not proportionally.
        // Measured over the bound: 0.3–5.1 ms idle, worst 6.0 ms across five
        // runs at load average 71 on ten cores. The mutation lands at 30.0 s.
        const HEADROOM: Duration = Duration::from_secs(2);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let proxy = format!("http://{}", listener.local_addr().expect("has an address"));
        let (accepted, an_accept) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            // Held open, never answered and never closed, so the ping can only
            // end at its own request timeout.
            let mut stalled = Vec::new();
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                stalled.push(stream);
                accepted.send(()).ok();
            }
        });

        let mut ping_client = Client::new(&proxy).with_retries(crate::RetryPolicy::none());
        ping_client.transport.set_timeout(PING_BUDGET);
        let keep_alive =
            KeepAlive::spawn(ping_client, "1-2-3-4".to_owned(), Duration::from_millis(1))
                .expect("the thread starts");

        an_accept
            .recv_timeout(Duration::from_secs(5))
            .expect("a ping reached the proxy");

        let waited = std::time::Instant::now();
        keep_alive.stop_and_join();
        let waited = waited.elapsed();

        assert!(
            waited < DETACH_JOIN_TIMEOUT + HEADROOM,
            "stop_and_join waited {waited:?} on a ping with a {PING_BUDGET:?} budget: \
             the bound is gone, and detach is back to waiting the ping out"
        );
    }

    #[test]
    fn detach_hands_back_the_id_and_disarms_drop() {
        // `detach` consumes the handle, so `Drop` still runs inside it, and
        // `done` is the only thing keeping it from aborting. Asserted against
        // a socket that counts connections rather than a dead port, where an
        // abort that was sent and one that was not look identical.
        let (proxy, arrived) = watched_proxy();
        let tx = handle_at(&proxy, Origin::Started);

        assert_eq!(tx.detach(), "1-2-3-4");

        assert!(
            !connections_reach(&arrived, 1, Duration::from_millis(300)),
            "detach sent something: a detached transaction must look untouched"
        );
    }

    #[test]
    fn a_failed_attach_names_the_id_and_keeps_the_clusters_verdict() {
        // What the cluster says about `1-2-3-4` is `cluster error 1: Unknown
        // cell tag 0` — no id, no mention of a transaction. The rebranding
        // must add both without discarding what a caller can branch on.
        let from_cluster = ClientError::Cluster {
            command: "get".into(),
            code: 1,
            message: "Unknown cell tag 0".into(),
            raw: r#"{"code":1}"#.into(),
        };

        let rebranded = attach_failed("1-2-3-4", from_cluster);
        let ClientError::Cluster {
            command,
            code,
            message,
            raw,
        } = &rebranded
        else {
            panic!("the variant must survive: {rebranded:?}");
        };
        assert_eq!(command, "attach_transaction");
        assert_eq!(*code, 1, "the cluster's code is the caller's to branch on");
        assert!(message.contains("1-2-3-4"), "{message}");
        assert!(message.contains("Unknown cell tag 0"), "{message}");
        assert_eq!(raw, r#"{"code":1}"#, "the raw document is evidence");

        // A transport failure says nothing about the id and is left alone.
        let transport = attach_failed("1-2-3-4", ClientError::Config("x".into()));
        assert!(matches!(transport, ClientError::Config(_)));
    }

    #[test]
    fn only_a_started_handles_drop_reaches_for_the_cluster() {
        // The whole of `Drop`'s distinction, in one pair. Both handles are
        // unfinished; both drop; the *started* one must abort and the
        // *attached* one must send nothing at all. Asserting the second alone
        // would pass on a `Drop` that had stopped sending anything, which is
        // why the first is here beside it.
        let (started_proxy, reached_by_started) = watched_proxy();
        drop(handle_at(&started_proxy, Origin::Started));
        assert!(
            connections_reach(&reached_by_started, 1, Duration::from_secs(5)),
            "a dropped started handle sent nothing: `?` inside a transaction \
             no longer leaves the cluster as it was"
        );

        let (attached_proxy, reached_by_attached) = watched_proxy();
        drop(handle_at(&attached_proxy, Origin::Attached));
        assert!(
            !connections_reach(&reached_by_attached, 1, Duration::from_millis(300)),
            "a dropped attached handle reached for the cluster: an attacher's \
             `?` must not destroy the owner's work"
        );
    }

    #[test]
    fn a_timeout_attribute_is_read_in_either_integer() {
        // Int64 is what the local cluster answers — `{"value"=30000;}`, no `u`
        // — but a millisecond count is exactly the sort of field a master
        // could spell unsigned, and failing an attach over that would be a bad
        // way to find out.
        for node in [YsonNode::Int64(30_000), YsonNode::Uint64(30_000)] {
            let value = YsonValue {
                attributes: None,
                node,
            };
            assert_eq!(
                attached_timeout("1-2-3-4", &value).expect("reads"),
                Duration::from_secs(30)
            );
        }
    }

    #[test]
    fn a_nonsense_timeout_attribute_is_an_error_that_names_it() {
        // Read as zero, each of these would floor `ping_interval` to a second
        // and leave a 1 Hz pinger running for the handle's whole life, on a
        // transaction whose real interval nobody knows.
        for node in [
            YsonNode::Int64(-1),
            YsonNode::Int64(0),
            YsonNode::Uint64(0),
            YsonNode::String(b"30s".to_vec()),
            YsonNode::Entity,
        ] {
            let value = YsonValue {
                attributes: None,
                node: node.clone(),
            };
            let error = attached_timeout("1-2-3-4", &value)
                .expect_err(&format!("{node:?} is not a transaction timeout"));

            let ClientError::Decode { command, reason } = &error else {
                panic!("wrong variant for {node:?}: {error:?}");
            };
            assert_eq!(command, "attach_transaction");
            assert!(reason.contains("1-2-3-4/@timeout"), "{reason}");
        }
    }
}
