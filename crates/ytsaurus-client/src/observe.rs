//! What the client says about itself while it works.
//!
//! Two ways of saying it, and which one is compiled in is the `tracing`
//! feature:
//!
//! - **off**, the default: a retry announces itself on stderr and nothing else
//!   is said. No dependency, and a launcher that pauses for fifteen seconds
//!   still explains the pause.
//! - **on**: every attempt runs inside a span carrying the command, the attempt
//!   number and how long it took, and the retry message becomes a `WARN` event
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
//! site: [`attempt`] and [`retrying`] exist in both builds, and in the default
//! one they compile to the call they wrap and an `eprintln!`.
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
    // `NoSubscriber` is what `tracing` falls back to when none was set, so
    // asking whether the current dispatcher is that one — globally or for this
    // thread — is asking whether the event just emitted went anywhere.
    let listening = !tracing::dispatcher::get_default(
        tracing::Dispatch::is::<tracing::subscriber::NoSubscriber>,
    );

    (!listening).then(|| retry_message(command, error, wait, attempt, of))
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn retrying(command: &str, error: &ClientError, wait: Duration, attempt: u32, of: u32) {
    eprintln!("{}", retry_message(command, error, wait, attempt, of));
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
