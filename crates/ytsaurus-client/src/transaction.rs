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
//!
//! # Handing one to another process
//!
//! [`Transaction::detach`] stops the keep-alive and leaves the transaction
//! running; what remains is the id. [`Client::attach_transaction`] turns an id
//! back into a handle — pinging again, able to commit or abort — and
//! [`Client::ping_transaction`], [`Client::commit_transaction`] and
//! [`Client::abort_transaction`] finish one from a process that holds nothing
//! but the id. Between the detach and the next ping the transaction is on the
//! cluster's clock: it expires its timeout after its last ping, 30 seconds by
//! default.
//!
//! What `Drop` does depends on where the handle came from. A **started**
//! handle aborts on drop — that is what makes `?` safe inside a transaction. An
//! **attached** one detaches on drop: the attacher walking away must not
//! destroy what the process that started the transaction is still counting on.
//! The C++ client's destructor draws the same line.

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

/// What the cluster itself defaults to, and what this crate asks for.
///
/// Sent explicitly rather than left out, because the ping interval is derived
/// from it: a client that assumed the wrong default would ping too slowly and
/// lose the transaction.
pub(crate) const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the abort sent from `Drop` may take.
///
/// A destructor — possibly running during a panic unwind — must not hang for
/// the full retry budget against an unreachable cluster. If the abort is
/// lost, the transaction expires on its own once nothing pings it.
const DROP_ABORT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long [`Transaction::detach`] waits for the keep-alive thread.
///
/// An unbounded join would be bounded in practice by the ping's own request
/// budget — [`ping_request_timeout`] — and that is up to
/// [`crate::DEFAULT_TIMEOUT`], two minutes, for a transaction whose timeout is
/// an hour. `detach` reads as instant at every call site, so the wait has its
/// own bound instead: past this, a ping that is still stalled is left to land
/// on its own, which costs one interval of extra life on a transaction the
/// caller was handing on anyway.
const DETACH_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// Set by whichever of commit/abort/detach ran, so `Drop` sends nothing.
    done: bool,
    keep_alive: Option<KeepAlive>,
    origin: Origin,
}

/// How a handle came to hold its transaction, which is what `Drop` turns on.
#[derive(Clone, Copy, Debug)]
enum Origin {
    /// Started by this handle. Dropping it aborts: a `?` inside a transaction
    /// must leave the cluster as it was.
    Started,
    /// Attached to a transaction something else started. Dropping it detaches
    /// — stops the pinging, sends nothing — because walking away from a
    /// borrowed transaction must not destroy what its owner is still counting
    /// on. The C++ client's destructor makes the same distinction.
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

        Ok(Self::held(client, id, timeout, Origin::Started))
    }

    pub(crate) fn attach(client: &Client, id: String) -> Result<Self> {
        // The transaction's own timeout, read off the object itself: pinging
        // needs the interval, and the id alone does not carry it. The read is
        // also what makes attaching to a transaction that is gone fail *here*
        // rather than later, on the first command sent through the handle.
        let value = client
            .get(&format!("#{id}/@timeout"))
            .map_err(|error| attach_failed(&id, error))?;
        let timeout = attached_timeout(&id, &value)?;

        // Then one ping, before the handle exists. `@timeout` is the
        // *configured* lifetime, not the remaining one: the id says nothing
        // about how long ago somebody last pinged, and the keep-alive thread's
        // first ping is a whole interval away. A handoff that took longer than
        // two thirds of the timeout would hand back a handle whose first ping
        // lands after the cluster has already expired the transaction — the
        // ping would then be answered `No such transaction`, the thread would
        // give up, and the loss would surface later on an unrelated command.
        // Pinging here restarts the clock at the attach and turns a
        // transaction that is already gone into this call's error, which has a
        // caller to report it to. A *started* handle needs none of this: its
        // clock starts at the reply it was born from.
        ping(client, &id).map_err(|error| attach_failed(&id, error))?;

        Ok(Self::held(client, id, timeout, Origin::Attached))
    }

    /// A handle around `id`, pinging every third of `timeout`.
    fn held(client: &Client, id: String, timeout: Duration, origin: Origin) -> Self {
        let client = client.clone().with_transaction(&id);
        let interval = ping_interval(timeout);

        // The pings go through their own transport configuration: one attempt,
        // bounded well under the interval. A ping that rode the full retry
        // pipeline could stall its thread for minutes on one hung connection —
        // five attempts, two minutes each, backoff between — while the
        // transaction it was keeping alive quietly expired. A lost ping costs
        // nothing (the next one is the retry); a late one costs everything.
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
    /// transaction aborted and nothing published: the handle is consumed either
    /// way, and a commit that did not land drops through `Drop`, which sends the
    /// abort. A failed commit that was left neither committed nor aborted would
    /// hold its locks until it expired.
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

    /// Whether the keep-alive has given up on this transaction.
    ///
    /// The pinging thread stops on its own for exactly one reason: the cluster
    /// answered a ping with "no such transaction", which is final — the
    /// transaction expired, or somebody else aborted or committed it. Without
    /// this the thread's exit is invisible, and a handle that has quietly
    /// stopped pinging looks exactly like a healthy one until the next command
    /// fails.
    ///
    /// So this is for a holder that keeps a transaction across something long:
    /// a false answer means only that no ping has been *answered* that way
    /// yet, which is the strongest thing a handle can say without asking, and
    /// [`Transaction::ping`] is how to ask. A handle whose thread never
    /// started — the spawn failed — answers false: nothing has been lost, and
    /// nothing is pinging either.
    #[must_use]
    pub fn is_lost(&self) -> bool {
        self.keep_alive.as_ref().is_some_and(KeepAlive::lost)
    }

    /// Stops keeping the transaction alive and leaves it running.
    ///
    /// The deliberate exception to what `Drop` promises: the transaction
    /// survives the handle. Nothing is committed, aborted or otherwise decided
    /// — to the cluster a detached transaction looks exactly like a held one —
    /// so from here it lives on the cluster's terms: it expires its timeout
    /// after its last ping, 30 seconds by default, unless something else keeps
    /// it alive. That something is the point: hand the returned id to another
    /// process, which re-holds it with [`Client::attach_transaction`] or
    /// finishes it outright with [`Client::commit_transaction`] or
    /// [`Client::abort_transaction`].
    ///
    /// **No ping is in flight when this returns**, which is what lets a caller
    /// kill the process the moment it does without a stray request behind it.
    /// The keep-alive thread is asked to stop and then waited for. Two things
    /// that promise is careful about:
    ///
    /// - The keep-alive may get *one last ping* away — it can be past its own
    ///   stop check and about to send when `detach` raises the flag — so the
    ///   transaction's clock may restart once more, at up to one ping after
    ///   this was called. That ping is waited for; it is not left in flight.
    /// - The wait is bounded by `DETACH_JOIN_TIMEOUT`, five seconds. It is
    ///   normally over in the time one ping takes, but a ping stalled on a
    ///   hung proxy has a budget of its own — `min(interval / 2, 120 s)`,
    ///   which is two minutes for an hour-long transaction — and `detach` will
    ///   not hold its caller's thread for that.
    ///
    /// What C++ spells `ITransaction::Detach()`. It is also the honest way to
    /// let a transaction outlive its handle: `mem::forget` on a [`Transaction`]
    /// leaks the keep-alive thread, which goes on pinging for the life of the
    /// process and holds the transaction and its locks open indefinitely.
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
        // Sent while the pings are still running. A commit can take longer than
        // the transaction's own timeout — the request timeout is two minutes,
        // the default transaction timeout thirty seconds, and the retry loop
        // adds fifteen more — and a transaction that expires mid-commit is
        // answered `No such transaction`, discarding work that would have
        // survived had something kept saying it was wanted.
        let outcome = self.client.transport.call(
            Method::Post,
            command,
            &params,
            Payload::None,
            // A commit that is retried after its answer was lost must not be a
            // second commit: the cluster refuses that with `No such
            // transaction`, which reads like the commit failed when it
            // succeeded. The mutation ID makes the retry the same commit.
            Repeatable::WithMutationId,
        );

        // Only a terminal answer ends the transaction. A commit that failed
        // published nothing and still holds its locks, so `done` stays unset
        // and `Drop` aborts it on the way out — otherwise the transaction would
        // be neither committed nor aborted nor pinged, and would sit on its
        // locks until it expired, which for an hour-long timeout blocks the
        // next launcher for an hour. An abort that failed is finished either
        // way: there is nothing left to undo, and repeating it in `Drop` would
        // only spend the retry budget twice.
        self.done = outcome.is_ok() || command == "abort_transaction";
        self.stop_pinging();

        outcome.map(|_| ())
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

        if matches!(self.origin, Origin::Attached) {
            // An attached handle borrowed the transaction; it does not own the
            // fate of it. Dropping one detaches — the pings stop, nothing is
            // sent — and the transaction is back where `detach` left it: alive,
            // and expiring on the cluster's schedule unless somebody pings it.
            // Aborting here would let any attacher's `?` destroy work the
            // process that started the transaction still holds a handle to.
            self.stop_pinging();
            return;
        }

        // Abandoning it would work too — an unpinged transaction expires — but
        // it would hold its locks until then, and a failed launcher should not
        // block the next attempt for half a minute. The error is dropped
        // because a destructor has nowhere to report one, and because the
        // cluster accepts an abort of a transaction that is already gone.
        //
        // One bounded attempt, not the retry pipeline: a destructor that can
        // block its thread for the full budget — ten minutes against an
        // unreachable cluster — is worse than a lost abort, which expiry
        // cleans up anyway. The explicit `abort()` keeps the full retries; it
        // has a caller to wait for it.
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

/// Commits a transaction that is held as nothing but an id.
///
/// The handle's own [`Transaction::commit`] goes through `finish` instead,
/// because it also has pings to stop and a `done` flag to keep honest.
pub(crate) fn commit_by_id(client: &Client, id: &str) -> Result<()> {
    let params = yson_build::map([("transaction_id", yson_build::string(id))]);
    client.transport.call(
        Method::Post,
        "commit_transaction",
        &params,
        Payload::None,
        // A commit is not idempotent: the second is refused with `No such
        // transaction`, which reads like the *first* one failed. The mutation
        // ID makes a retried commit the same commit.
        Repeatable::WithMutationId,
    )?;
    Ok(())
}

/// `#<id>/@timeout`, as a duration.
///
/// **Both integer spellings.** The local cluster answers `{"value"=30000;}` —
/// text YSON, no `u`, so `Int64` — but a duration in milliseconds is exactly
/// the kind of field a master could send as `Uint64`, and a `Decode` error on
/// an attribute the crate can plainly read would be a poor way to find that
/// out.
///
/// Anything else — a negative, a zero, a string — is the attach failing and
/// naming the attribute. Silently reading it as zero would floor
/// [`ping_interval`] to one second and leave a 1 Hz pinger running for the
/// handle's whole life.
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

/// The timeout read failing is the attach failing, and the error should say
/// so.
///
/// The caller handed over a transaction id, not a `get`, and the cluster's own
/// answer does not always name what was asked about: an id that was never a
/// transaction is refused as `cluster error 1: Unknown cell tag 0` — observed
/// on a local cluster for `1-2-3-4` — which names neither the id nor a
/// transaction. Only an id whose cell exists earns the resolve error that
/// does. So the command is rewritten to name the operation and the message to
/// name the id, and everything else — the code, the raw document — is kept, so
/// a caller can still branch on what the cluster actually said.
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
        // An abort is forgiving — aborting a transaction that is already gone
        // answers `{}`, verified on a local cluster — so a repeat is the same
        // shrug and needs no mutation ID.
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

/// How long one ping request may take: half the interval, so a stalled ping
/// still leaves the next one room inside the transaction's timeout, and never
/// more than the transport's ordinary two minutes.
fn ping_request_timeout(interval: Duration) -> Duration {
    (interval / 2)
        .max(Duration::from_secs(1))
        .min(crate::DEFAULT_TIMEOUT)
}

/// Whether the cluster's answer says the transaction no longer exists.
///
/// 11000 is `NoSuchTransaction`; the substring covers the master's other
/// spelling — `Transaction … has expired or was aborted` — and both are
/// looked for in the full document, because the outer error is often a
/// wrapper. Anything else (a transport failure, a busy master) is
/// indistinguishable from a transaction that is still there, so the pings
/// continue.
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
    /// Raised by the thread itself when a ping was answered "no such
    /// transaction" and it gave up. Read through [`Transaction::is_lost`]:
    /// otherwise the thread's exit is invisible to the handle's owner.
    lost: Arc<AtomicBool>,
    /// Disconnects when the thread's body ends, on every path out of it.
    ///
    /// Nothing is ever sent on it. It exists because [`Transaction::detach`]
    /// needs a join with a bound and `std` has no timed one — a
    /// `recv_timeout` on this is that join.
    exited: Receiver<Infallible>,
    /// The thread itself, kept only to reap it once `exited` says its body has
    /// ended. Joining it directly is what has no bound.
    thread: std::thread::JoinHandle<()>,
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
        let lost = Arc::new(AtomicBool::new(false));
        let give_up = Arc::clone(&lost);
        let (alive, exited) = std::sync::mpsc::channel::<Infallible>();

        std::thread::Builder::new()
            .name("yt-transaction-ping".to_owned())
            .spawn(move || {
                // Held for the body's whole life and never sent on: dropping it
                // — however this thread leaves — is what wakes the waiter in
                // `stop_and_join`. Bound to a name so it is captured at all.
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
                        // Checked again on the way out, not only on the way in:
                        // a stop raised *during* a ping arrives while nothing is
                        // waiting on the condvar, so the notification is missed
                        // and only this test catches it.
                        if *guard {
                            return;
                        }
                    }

                    // A failed ping is not fatal on its own — the next one is
                    // its retry, and whatever the ping would have said, the
                    // next command in the transaction says too, to a caller
                    // who can report it. But a cluster that answers "no such
                    // transaction" has said something final: pinging on would
                    // spend a request every interval, for as long as the
                    // handle lives, on a transaction that cannot come back.
                    // The flag is what keeps that exit from being silent.
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

    /// Asks the thread to stop, without waiting for it.
    ///
    /// Not joined on purpose: the thread may be inside a ping, and a request
    /// can take as long as the client's timeout. Blocking a `Drop` for two
    /// minutes to tidy up a thread that is about to exit on its own would be a
    /// worse bargain than letting a stray ping land on a committed
    /// transaction, which the cluster answers with an error nobody reads.
    fn stop(self) {
        self.raise();
    }

    /// Asks the thread to stop and waits until it has.
    ///
    /// For [`Transaction::detach`], which has a caller to wait for it — unlike
    /// the destructor above — and which promises that no ping lands after it
    /// returns: a stray ping is harmless on a committed transaction but not on
    /// a detached one, where it would quietly extend a lifetime the caller has
    /// just finished reasoning about.
    ///
    /// **Bounded by [`DETACH_JOIN_TIMEOUT`], not by the ping.** A plain
    /// `join()` would wait out the ping's own request budget, and that is
    /// `min(interval / 2, 120 s)` — two minutes for an hour-long transaction,
    /// against a proxy that has stopped answering. So the wait is a
    /// `recv_timeout` on a channel the thread's own `Sender` closes when its
    /// body ends, which is the timed join `std` does not have.
    fn stop_and_join(self) {
        self.raise();
        if matches!(
            self.exited.recv_timeout(DETACH_JOIN_TIMEOUT),
            Err(RecvTimeoutError::Disconnected)
        ) {
            // The body has already ended, so this only reaps the thread and
            // cannot block. An `Err` from it is the thread having panicked;
            // the ping loop has nothing in it that panics, and a
            // destructor-adjacent path must not turn someone else's panic into
            // its own.
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
