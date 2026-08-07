//! What the client says about itself while it works.
//!
//! Two things are worth saying — a command is being sent again, and the file
//! cache will not have this caller's worker — and two ways of saying either,
//! with the `tracing` feature deciding which is compiled in:
//!
//! - **off**, the default: both announce themselves on stderr and nothing else
//!   is said. No dependency, and a launcher that pauses for fifteen seconds
//!   still explains the pause.
//! - **on**: every attempt runs inside a span carrying the command, the attempt
//!   number and how long it took, and both messages become `WARN` events
//!   carrying the same facts as fields. The subscriber decides where any of it
//!   goes — and if there is no subscriber, the stderr line is printed after
//!   all. The feature adds a way of saying this; it does not take the old one
//!   away, because whether it is on is not entirely up to whoever built the
//!   program. Cargo unifies features across the graph.
//!
//! The retry event is *not* inside the attempt's span, and cannot be: the
//! attempt it is complaining about has already ended, and the wait it is
//! announcing happens between two of them. It names the command itself for
//! that reason.
//!
//! The feature is off by default because this crate is linked into worker
//! binaries that cross-compile to musl with nothing but the Rust toolchain —
//! the same reason `tls` is off there. Nothing here is optional at the call
//! site: [`attempt`], [`retrying`] and [`cache_refused`] exist in both builds,
//! and in the default one they compile to the call they wrap and an
//! `eprintln!`.
//!
//! What the *cluster* records is a separate question with a separate answer,
//! and it needs no dependency at all: see [`TraceContext`](crate::TraceContext).

use std::time::Duration;

use crate::error::{ClientError, Result};

/// Runs one attempt of `command`, timed.
///
/// Wraps an attempt rather than a command, because the attempt is the thing
/// with a duration: a command that was retried four times took as long as the
/// four attempts and the waits between them, and a span that hid that would
/// report the last one as the whole story.
///
/// The streaming commands are the honest exception. Their span ends when the
/// response headers arrive, because the body is handed to the caller to read at
/// its own pace — the transfer outlives the call, and there is nothing here to
/// close the span around.
#[cfg(feature = "tracing")]
pub(crate) fn attempt<T>(
    command: &str,
    attempt: u32,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let span = tracing::info_span!(
        "ytsaurus.command",
        command = %command,
        attempt,
        elapsed_ms = tracing::field::Empty,
    );
    let _entered = span.enter();

    let started = std::time::Instant::now();
    let result = action();
    span.record("elapsed_ms", started.elapsed().as_secs_f64() * 1e3);

    if let Err(error) = &result {
        // `DEBUG`, not `WARN`: a failure that will be retried is reported by
        // `retrying`, and one that will not is returned to a caller who is
        // about to say so itself. This is the record of the attempt, not the
        // complaint about it.
        tracing::debug!(error = %error, "the attempt failed");
    }

    result
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn attempt<T>(
    _command: &str,
    _attempt: u32,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    action()
}

/// Announces that `command` failed and is about to be sent again.
///
/// `attempt` is the one that just failed and `of` is how many are allowed, the
/// same counting the span's `attempt` field uses — so an event and the span
/// beside it never disagree about which try this was, and `attempt == of` means
/// what it says.
///
/// Called only when the policy says to — see [`RetryPolicy::quiet`], and the
/// muting it does inside a job. That muting covers both spellings of this: a
/// job's stderr is a bounded buffer the cluster shows in its UI, and a
/// subscriber installed in a job is likely to be writing to exactly that.
///
/// [`RetryPolicy::quiet`]: crate::RetryPolicy::quiet
#[cfg(feature = "tracing")]
pub(crate) fn retrying(command: &str, error: &ClientError, wait: Duration, attempt: u32, of: u32) {
    tracing::warn!(
        command = %command,
        attempt,
        of,
        retry_in_s = wait.as_secs_f64(),
        error = %error,
        "the command failed; retrying"
    );

    if let Some(line) = stderr_fallback(command, error, wait, attempt, of) {
        eprintln!("{line}");
    }
}

/// What to print on stderr after emitting the event, if anything.
///
/// A feature is supposed to add, and this one would otherwise take something
/// away. Cargo unifies features across the whole graph, so any crate anywhere
/// in a build can turn `tracing` on for everybody: a launcher that never asked
/// for it, and so installed no subscriber, would find the stderr message simply
/// gone and a fifteen-second pause looking like a hang — with nothing in its
/// own manifest to explain why. Falling back keeps the default build's
/// behaviour available in every build.
///
/// `None` when a subscriber is installed. The event is the message then, and
/// saying it twice would be worse than either way of saying it once.
///
/// Returns the line rather than printing it so that the decision is something a
/// test can hold: `eprintln!` writes to a file descriptor no test in this
/// process can read back, so a fallback that printed directly could be deleted
/// wholesale and every test would still pass.
#[cfg(feature = "tracing")]
fn stderr_fallback(
    command: &str,
    error: &ClientError,
    wait: Duration,
    attempt: u32,
    of: u32,
) -> Option<String> {
    unheard().then(|| retry_message(command, error, wait, attempt, of))
}

/// Whether the event just emitted went nowhere.
///
/// `NoSubscriber` is what `tracing` falls back to when none was set, so asking
/// whether the current dispatcher is that one — globally or for this thread —
/// is asking whether anybody was listening.
#[cfg(feature = "tracing")]
fn unheard() -> bool {
    tracing::dispatcher::get_default(tracing::Dispatch::is::<tracing::subscriber::NoSubscriber>)
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn retrying(command: &str, error: &ClientError, wait: Duration, attempt: u32, of: u32) {
    eprintln!("{}", retry_message(command, error, wait, attempt, of));
}

/// Announces that the file cache will not have this caller's worker, and that
/// it is going up uncached.
///
/// Said rather than passed over because the state is invisible otherwise and
/// permanent until someone acts: every launch re-sends the whole binary, and
/// the fix is one line — [`Client::with_file_cache`] pointed somewhere this
/// caller may write. A launch that is merely slower than it should be is
/// exactly the kind of thing nobody investigates without being told.
///
/// Not routed through [`RetryPolicy::quiet`], unlike [`retrying`]: this is said
/// once per upload rather than once per attempt, and it is said by a launcher —
/// a job does not upload workers.
///
/// **One body, not two `#[cfg]` arms.** This used to be written twice, and the
/// default build's copy was an `eprintln!` and nothing else — a descriptor no
/// test in this process can read back, in the half of the file the test module
/// below is not compiled for. It could be emptied to `{}` with every test in
/// the workspace green, and it is the whole of what a default build says about
/// a cache it has given up on.
///
/// Written once, the *decision* is held in both configurations by
/// [`cache_fallback`], and with `tracing` on, emptying this body also takes the
/// `WARN` event the tests assert. **What is still not held is the `eprintln!`
/// itself in the default build.** Replacing it with `let _ = cache_fallback(…)`
/// leaves the suite green and clippy quiet, and no test here can catch that:
/// stderr is a descriptor this process cannot read back, so observing it means
/// a subprocess rather than a unit test. What was won is that the line's
/// *content* and the choice to emit it are pinned; what is left unguarded is
/// one call, which is a smaller hole than the one it replaced but is not no
/// hole. Do not read the tests below as covering it.
///
/// [`Client::with_file_cache`]: crate::Client::with_file_cache
/// [`RetryPolicy::quiet`]: crate::RetryPolicy::quiet
pub(crate) fn cache_refused(cache: &str, error: &ClientError) {
    #[cfg(feature = "tracing")]
    tracing::warn!(
        cache = %cache,
        error = %error,
        "the file cache cannot be written to; uploading the worker uncached"
    );

    if let Some(line) = cache_fallback(cache, error) {
        eprintln!("{line}");
    }
}

/// What to say on stderr about the cache, if anything.
///
/// [`stderr_fallback`]'s shape and, with the feature on, its reason: a build
/// where Cargo turned `tracing` on for a launcher that never asked has no
/// subscriber to hear the event, and must still say this somewhere.
///
/// Returning the decision rather than printing it is what makes it testable at
/// all — and this one is tested in **both** feature configurations, because the
/// default build is where the whole announcement lives.
#[cfg(feature = "tracing")]
fn cache_fallback(cache: &str, error: &ClientError) -> Option<String> {
    unheard().then(|| cache_message(cache, error))
}

#[cfg(not(feature = "tracing"))]
fn cache_fallback(cache: &str, error: &ClientError) -> Option<String> {
    // There is no subscriber in this build to have heard it, so the line is
    // always owed.
    Some(cache_message(cache, error))
}

/// The fallback announcement as a line of text.
///
/// Split out for the reason [`retry_message`] is: this is the whole of what a
/// default build says, and a message no test can reach is a message that can be
/// emptied without anything noticing.
///
/// It names three things, and each earns its place: the path that was refused,
/// so the reader knows which cache; the cluster's own words, so an ACL failure
/// is not mistaken for a network one; and the setter, because a caller who has
/// just been told the default path does not work still has nothing to do about
/// it otherwise.
fn cache_message(cache: &str, error: &ClientError) -> String {
    format!(
        "ytsaurus-client: the file cache at {cache} cannot be written to \
         ({error}); uploading the worker uncached, which re-sends it on every \
         launch. Client::with_file_cache points it at a path you can write to."
    )
}

/// The retry announcement as a line of text.
///
/// Split from the `eprintln!` so that it can be asserted on. It used to be
/// inlined into the `#[cfg(not(feature = "tracing"))]` arm, where no test could
/// reach it: the tests below are compiled exactly when that arm is not, so the
/// message every default build prints was checked by nothing at all, and
/// swapping `attempt` for `of` or dropping the reason left CI green.
fn retry_message(
    command: &str,
    error: &ClientError,
    wait: Duration,
    attempt: u32,
    of: u32,
) -> String {
    format!(
        "ytsaurus-client: {command} failed ({error}); \
         retrying in {:.1}s (attempt {attempt} of {of})",
        wait.as_secs_f64()
    )
}

/// The stderr spelling, which is what a default build prints.
///
/// Not gated on the feature — that is the whole point. The module below is
/// compiled only with `tracing` on, so everything it asserts is about the half
/// of this file that most users never build.
#[cfg(test)]
mod message_tests {
    use super::*;

    fn unavailable() -> ClientError {
        ClientError::Cluster {
            command: "get".to_owned(),
            code: 105,
            message: "Master is not connected".to_owned(),
            raw: r#"{"code":105}"#.to_owned(),
        }
    }

    #[test]
    fn the_message_names_the_command_the_reason_the_wait_and_the_try() {
        let line = retry_message(
            "start_operation",
            &unavailable(),
            Duration::from_millis(1500),
            2,
            5,
        );

        assert!(line.contains("start_operation"), "{line}");
        // The reason is what makes the message worth having; a pause with no
        // explanation is the thing it exists to avoid.
        assert!(line.contains("Master is not connected"), "{line}");
        assert!(line.contains("1.5s"), "the wait is not in seconds: {line}");
        // Counted the same way the span's `attempt` field is, and in that
        // order: swapping the two reads as a retry budget four times too big.
        assert!(line.contains("attempt 2 of 5"), "{line}");
    }

    #[test]
    fn the_cache_warning_names_the_path_the_refusal_and_the_way_out() {
        let denied = ClientError::Cluster {
            command: "create".to_owned(),
            code: 901,
            message: "Access denied for user \"robot\": \"write | modify_children\" \
                      permission for node //tmp/yt_wrapper/file_storage/new_cache \
                      is not allowed by any matching ACE"
                .to_owned(),
            raw: r#"{"code":901}"#.to_owned(),
        };

        let line = cache_message("//tmp/yt_wrapper/file_storage/new_cache", &denied);

        assert!(
            line.contains("//tmp/yt_wrapper/file_storage/new_cache"),
            "{line}"
        );
        // The cluster's own words: without them an ACL refusal is
        // indistinguishable from a proxy that was down for a moment, and only
        // one of those is worth acting on.
        assert!(line.contains("Access denied"), "{line}");
        // The whole reason this is a warning rather than silence. A caller told
        // only that the cache is gone has nothing to do about it; this is the
        // one line that puts it back.
        assert!(line.contains("Client::with_file_cache"), "{line}");
        // And what it costs until then, which is what makes it worth reading.
        assert!(line.contains("every launch"), "{line}");
    }

    #[test]
    fn the_cache_warning_is_owed_to_stderr_when_nothing_else_carries_it() {
        // The default build's entire announcement, and until this existed it
        // was an `eprintln!` in a `#[cfg(not(feature = "tracing"))]` arm — a
        // descriptor no test in this process can read back, in the half of the
        // file this module is not compiled for. Emptying that body left every
        // test green while a deployment uploading uncached for ever went back
        // to being silent. Asserted here rather than in the module below
        // because *here* is where the default build's copy is compiled.
        let denied = ClientError::Cluster {
            command: "create".to_owned(),
            code: 901,
            message: "Access denied for user \"robot\"".to_owned(),
            raw: r#"{"code":901}"#.to_owned(),
        };

        let line = cache_fallback("//tmp/mine/cache", &denied)
            .expect("no subscriber is installed, so stderr is the only way to say it");

        assert!(line.contains("//tmp/mine/cache"), "{line}");
        assert!(line.contains("Access denied"), "{line}");
        assert!(line.contains("Client::with_file_cache"), "{line}");
    }

    #[test]
    fn the_wait_is_rounded_rather_than_spelled_out() {
        // A backoff is a float, and an unrounded one puts sixteen digits in a
        // line whose whole job is to be read at a glance.
        let line = retry_message(
            "get",
            &unavailable(),
            Duration::from_nanos(1_234_567_891),
            1,
            3,
        );

        assert!(line.contains("1.2s"), "{line}");
        assert!(!line.contains("1.234"), "{line}");
    }
}

/// What a subscriber is handed, checked against what the client promises.
///
/// The subscriber is written out by hand rather than pulled in: `tracing` is
/// the facade, `tracing-subscriber` is a second dependency, and this needs
/// nothing but somewhere to put the fields.
#[cfg(all(test, feature = "tracing"))]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    use crate::retry::{Repeatable, RetryPolicy};

    /// Every span and event that reached the subscriber, as flat text.
    ///
    /// The **level is part of the line**, and deliberately so: without it a
    /// test cannot tell a `WARN` from a `TRACE`, and demoting the retry event
    /// to a level nobody's filter passes would be exactly the regression this
    /// module exists to prevent — invisible, and green.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<String>>);

    impl Recorder {
        fn note(&self, kind: &str, meta: &Metadata<'_>, fields: impl FnOnce(&mut Fields<'_>)) {
            let mut line = format!("{kind} {} {}", meta.level(), meta.name());
            fields(&mut Fields(&mut line));
            self.0.lock().expect("not poisoned").push(line);
        }

        fn lines(&self) -> Vec<String> {
            self.0.lock().expect("not poisoned").clone()
        }
    }

    struct Fields<'a>(&'a mut String);

    impl Visit for Fields<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write;
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    impl Subscriber for Recorder {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, span: &Attributes<'_>) -> Id {
            self.note("span", span.metadata(), |fields| span.record(fields));
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, values: &Record<'_>) {
            let mut line = String::from("record");
            values.record(&mut Fields(&mut line));
            self.0.lock().expect("not poisoned").push(line);
        }

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            self.note("event", event.metadata(), |fields| {
                event.record(fields);
            });
        }

        fn enter(&self, _span: &Id) {}
        fn exit(&self, _span: &Id) {}
    }

    /// Runs `work` with a subscriber attached, and returns what it collected.
    fn recorded(work: impl FnOnce()) -> Vec<String> {
        let recorder = Arc::new(Recorder::default());
        tracing::subscriber::with_default(Arc::clone(&recorder), work);
        recorder.lines()
    }

    fn unavailable() -> ClientError {
        ClientError::Cluster {
            command: "get".to_owned(),
            code: 105,
            message: "Master is not connected".to_owned(),
            raw: r#"{"code":105}"#.to_owned(),
        }
    }

    /// The command spans in `lines`, in the order they were opened.
    fn spans(lines: &[String]) -> Vec<&String> {
        lines
            .iter()
            .filter(|line| line.starts_with("span INFO ytsaurus.command"))
            .collect()
    }

    #[test]
    fn an_attempt_is_a_span_naming_the_command_the_try_and_the_time() {
        // Driven through `retry::run` rather than by calling `attempt`
        // directly, so the command name and the attempt number come from the
        // real call site. Called directly, this test would pass just as
        // happily against a `run` that had stopped opening spans at all, or
        // that passed a constant where the command should be.
        let mut tries = 0;
        let lines = recorded(|| {
            crate::retry::run(
                RetryPolicy::new(3, Duration::ZERO, Duration::ZERO),
                Repeatable::Freely,
                "start_operation",
                |_| {
                    tries += 1;
                    if tries < 3 {
                        Err(unavailable())
                    } else {
                        Ok(())
                    }
                },
            )
            .expect("the third attempt succeeds");
        });

        let spans = spans(&lines);
        assert_eq!(spans.len(), 3, "one span per attempt: {lines:?}");
        for (index, span) in spans.iter().enumerate() {
            assert!(span.contains("command=start_operation"), "{span}");
            // Counted from one, and rising: an attempt number frozen at 1 is
            // exactly what a retried command must not report.
            assert!(span.contains(&format!("attempt={}", index + 1)), "{span}");
        }

        // Recorded at the end rather than declared at the start: the duration
        // is not known when the span opens.
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.contains("elapsed_ms="))
                .count(),
            3,
            "every attempt is timed: {lines:?}"
        );
    }

    #[test]
    fn the_span_is_at_info_and_the_retry_at_warn() {
        // Levels are the whole of how a subscriber filters, so they are part
        // of the contract rather than a detail of the macro that was reached
        // for. Nothing else here would notice a span demoted to `TRACE`: it
        // would still carry every field this file asserts on, and no default
        // filter would ever show it again.
        let lines = recorded(|| {
            crate::retry::run(
                RetryPolicy::new(2, Duration::ZERO, Duration::ZERO),
                Repeatable::Freely,
                "get",
                |_| Err::<(), _>(unavailable()),
            )
            .expect_err("nothing here succeeds");
        });

        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("span INFO ytsaurus.command")),
            "the command span is not at INFO: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("event WARN") && l.contains("retrying")),
            "the retry is not at WARN: {lines:?}"
        );
    }

    #[test]
    fn the_send_once_commands_get_a_span_of_their_own() {
        // `read_table` and `write_table` never reach `retry::run` — they are
        // sent once — so their spans are opened in `http.rs` instead. Deleting
        // those two wrappers is invisible to every other test in this file,
        // and they are the commands whose duration a user most wants.
        //
        // Nothing listens on port 1, so both calls fail; a span that was
        // opened is recorded whichever way the attempt went.
        let transport = crate::http::Transport::new(
            "http://127.0.0.1:1",
            None,
            std::time::Duration::from_millis(200),
        );
        let params = crate::yson_build::map([("path", crate::yson_build::string("//tmp/t"))]);

        let reading = recorded(|| {
            transport
                .open(crate::http::Method::Get, "read_table", &params)
                .expect_err("nothing is listening");
        });
        assert!(
            spans(&reading)
                .iter()
                .any(|span| span.contains("command=read_table")),
            "read_table opened no span: {reading:?}"
        );

        let writing = recorded(|| {
            let mut rows: &[u8] = b"";
            transport
                .upload(crate::http::Method::Put, "write_table", &params, &mut rows)
                .expect_err("nothing is listening");
        });
        assert!(
            spans(&writing)
                .iter()
                .any(|span| span.contains("command=write_table")),
            "write_table opened no span: {writing:?}"
        );
    }

    #[test]
    fn a_retry_says_so_through_tracing_instead_of_on_stderr() {
        // The seam the issue asks for: with the feature on, the message that
        // used to be an `eprintln!` is an event with the same facts in fields.
        let lines = recorded(|| {
            crate::retry::run(
                RetryPolicy::new(3, Duration::ZERO, Duration::ZERO),
                Repeatable::Freely,
                "get",
                |_| Err::<(), _>(unavailable()),
            )
            .expect_err("nothing here succeeds");
        });

        let retries: Vec<&String> = lines
            .iter()
            .filter(|line| line.starts_with("event") && line.contains("retrying"))
            .collect();

        assert_eq!(retries.len(), 2, "three attempts, two retries: {lines:?}");
        assert!(retries[0].contains("command=get"), "{}", retries[0]);
        // Attempts, not retries — the same counting the span uses, so that the
        // two can be read side by side. Three allowed attempts means the
        // second retry says `attempt=2 of=3`, and nothing ever says `of=2`
        // next to a span whose `attempt` reached 3.
        assert!(retries[0].contains("attempt=1"), "{}", retries[0]);
        assert!(retries[0].contains("of=3"), "{}", retries[0]);
        assert!(retries[1].contains("attempt=2"), "{}", retries[1]);
        assert!(retries[1].contains("of=3"), "{}", retries[1]);
        assert!(
            retries[0].contains("Master is not connected"),
            "the reason is what makes the message worth having: {}",
            retries[0]
        );
    }

    #[test]
    fn the_stderr_message_survives_the_feature_being_turned_on_for_us() {
        // Cargo unifies features across the graph, so `tracing` can be turned
        // on for this crate by some unrelated dependency of a launcher that
        // installed no subscriber. Replacing the `eprintln!` outright would
        // then delete that launcher's only sign of a retry, with nothing in
        // its own manifest to explain the silence — so the fallback is what
        // makes this feature additive rather than substitutive.
        let fallback = || stderr_fallback("get", &unavailable(), Duration::from_secs(2), 1, 3);

        // No subscriber: the event went nowhere, so the line is still owed.
        let unheard = fallback().expect("nothing is listening, so stderr is the fallback");
        assert!(unheard.contains("get"), "{unheard}");
        assert!(unheard.contains("attempt 1 of 3"), "{unheard}");
        assert!(unheard.contains("Master is not connected"), "{unheard}");

        // A subscriber: the event *is* the message, and saying it twice on a
        // subscriber that writes to stderr is the noise `quiet` exists to stop.
        let heard = tracing::subscriber::with_default(Arc::new(Recorder::default()), fallback);
        assert_eq!(
            heard, None,
            "a subscriber is installed and the message would be printed twice"
        );
    }

    #[test]
    fn an_unusable_file_cache_is_a_warning_here_too() {
        // The fallback is silent apart from this, and a deployment uploading
        // uncached for ever is otherwise indistinguishable from a slow one. At
        // `WARN`, beside the retry event, because both are "this worked, but
        // not the way you meant".
        let denied = ClientError::Cluster {
            command: "create".to_owned(),
            code: 901,
            message: "Access denied for user \"robot\"".to_owned(),
            raw: r#"{"code":901}"#.to_owned(),
        };

        let lines = recorded(|| cache_refused("//tmp/yt_wrapper/file_storage/new_cache", &denied));

        let warning = lines
            .iter()
            .find(|line| line.starts_with("event WARN"))
            .unwrap_or_else(|| panic!("nothing was said about the cache: {lines:?}"));
        assert!(
            warning.contains("uploading the worker uncached"),
            "{warning}"
        );
        // Which cache, and why — as fields, so a collector can group by the
        // path rather than by matching on a sentence.
        assert!(
            warning.contains("cache=//tmp/yt_wrapper/file_storage/new_cache"),
            "{warning}"
        );
        assert!(warning.contains("Access denied"), "{warning}");

        // And said once. The event carries everything the stderr line does, so
        // a subscriber that writes to stderr would otherwise print the warning
        // twice — the same rule `stderr_fallback` follows for a retry.
        assert_eq!(
            tracing::subscriber::with_default(Arc::new(Recorder::default()), || cache_fallback(
                "//tmp/yt_wrapper/file_storage/new_cache",
                &denied
            )),
            None,
            "a subscriber is installed and the warning would be printed twice"
        );
    }

    #[test]
    fn a_quiet_policy_is_quiet_in_this_spelling_too() {
        // The muting that a job depends on: its stderr is the cluster's
        // bounded diagnostic buffer, and a subscriber installed in a job is
        // very likely writing to it. Routing the message through `tracing`
        // must not route it around the mute.
        let policy = RetryPolicy::new(3, Duration::ZERO, Duration::ZERO);

        let attempt_it = |policy: RetryPolicy| {
            recorded(move || {
                crate::retry::run(policy, Repeatable::Freely, "get", |_| {
                    Err::<(), _>(unavailable())
                })
                .expect_err("nothing here succeeds");
            })
        };

        assert!(
            !attempt_it(policy.quiet())
                .iter()
                .any(|line| line.contains("retrying")),
            "a quiet policy announced its retries"
        );
        assert!(
            attempt_it(policy.loud())
                .iter()
                .any(|line| line.contains("retrying")),
            "a loud policy said nothing"
        );

        // Quiet is about the announcement, not the attempts: the spans are
        // still there, which is what a collector would want either way.
        assert!(
            attempt_it(policy.quiet())
                .iter()
                .filter(|line| line.starts_with("span INFO ytsaurus.command"))
                .count()
                == 3,
            "a quiet policy stopped opening spans"
        );
    }
}
