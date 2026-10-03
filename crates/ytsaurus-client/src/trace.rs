//! The trace a request belongs to, carried to the cluster in a `traceparent`
//! header.
//!
//! The proxy opens a span for every request; with a `traceparent` it joins the
//! caller's trace. The header is the
//! [W3C one](https://www.w3.org/TR/trace-context/#traceparent-header):
//!
//! ```text
//! traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
//!              ^^ ^^ 32 hex: the trace  ^^ 16 hex: the caller's span  ^^ flags
//! ```
//!
//! The proxy (`TryParseTraceParent` in `yt/yt/core/http/helpers.cpp`) also
//! accepts it without the version, and reads the flags as bit 0 sampled, bit 1
//! debug. The cluster spells a trace id as a GUID, `8e9bcc43-5c2be9b4-…`, and
//! [`TraceContext::yt_trace_id`] converts to that spelling. Observed behaviour
//! is in the
//! [protocol reference](https://github.com/sshaplygin/ytsaurus-rs/blob/main/docs/protocol-reference.md#tracing).

use crate::error::{ClientError, Result};
use crate::unique::word;

/// Bit 0 of the flags: this trace is being recorded.
const SAMPLED: u8 = 0x01;

/// The trace a request belongs to.
///
/// [`TraceContext::parse`] continues a trace a caller passed on, so the
/// cluster's work appears under the request that caused it.
/// [`TraceContext::new`] starts one, for a program that is nobody's callee.
///
/// ```
/// use ytsaurus_client::{Client, TraceContext};
///
/// # fn main() -> Result<(), ytsaurus_client::ClientError> {
/// let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
/// let client = Client::new("http://localhost:8000")
///     .with_trace_context(&TraceContext::parse(incoming)?);
///
/// let trace = TraceContext::new();
/// eprintln!("trace {}", trace.yt_trace_id());
/// let client = Client::new("http://localhost:8000").with_trace_context(&trace);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// 32 lowercase hex digits.
    trace_id: String,
    /// 16 lowercase hex digits: the parent span of this client's requests.
    span_id: String,
    flags: u8,
    /// Carried unmodified; see [`TraceContext::with_tracestate`].
    tracestate: Option<String>,
}

impl TraceContext {
    /// Starts a trace, sampled.
    ///
    /// The C++ and Python wrappers also start traces sampled. An unsampled
    /// context is one that arrived that way through [`TraceContext::parse`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            trace_id: format!("{:016x}{:016x}", word(0), word(1)),
            span_id: format!("{:016x}", word(2)),
            flags: SAMPLED,
            tracestate: None,
        }
    }

    /// Continues the trace a `traceparent` header names.
    ///
    /// Accepts `00-<trace>-<span>-<flags>` and the version-less form the Go SDK
    /// sends, in either case of hex.
    ///
    /// The span id is kept as it arrived, so the cluster's spans hang under the
    /// span the caller named. The W3C rule is to substitute the forwarder's own
    /// span, but this crate emits no spans a collector knows, so an invented id
    /// would name a parent that does not exist.
    ///
    /// A `tracestate` goes through [`TraceContext::with_tracestate`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the header is not a traceparent. The
    /// proxy drops a malformed header without complaint, so it is refused here.
    pub fn parse(header: &str) -> Result<Self> {
        let header = header.trim();
        let parts: Vec<&str> = header.split('-').collect();

        let [version, trace_id, span_id, flags] = match parts[..] {
            // The version-less form, which the proxy reads as version 00.
            // Recognised by the trace id rather than the count, so a header
            // with its flags cut off falls through to the arm below.
            [trace_id, span_id, flags] if is_hex(trace_id, 32) => ["00", trace_id, span_id, flags],
            // Version, trace and span with the flags missing: say so, rather
            // than blame the version as a bad trace id.
            [version, trace_id, _] if is_hex(version, 2) && is_hex(trace_id, 32) => {
                return Err(malformed(header, "the flags are missing"));
            }
            [version, trace_id, span_id, flags] => [version, trace_id, span_id, flags],
            // A later version may add fields after the flags; the standard
            // says to read the first four and ignore the rest. Version 00
            // defines exactly four, so a fifth group there is malformed.
            [version, trace_id, span_id, flags, ..] if !version.eq_ignore_ascii_case("00") => {
                [version, trace_id, span_id, flags]
            }
            _ => {
                return Err(malformed(
                    header,
                    "expected version-traceid-spanid-flags, in four hyphenated groups",
                ));
            }
        };

        if !is_hex(version, 2) {
            return Err(malformed(header, "the version is not two hex digits"));
        }
        // The standard reserves `ff` as never a valid version.
        if version.eq_ignore_ascii_case("ff") {
            return Err(malformed(header, "ff is not a valid version"));
        }
        if !is_hex(trace_id, 32) {
            return Err(malformed(header, "the trace id is not 32 hex digits"));
        }
        if is_zero(trace_id) {
            return Err(malformed(header, "the trace id is all zeros"));
        }
        if !is_hex(span_id, 16) {
            return Err(malformed(header, "the span id is not 16 hex digits"));
        }
        if is_zero(span_id) {
            return Err(malformed(header, "the span id is all zeros"));
        }
        if !is_hex(flags, 2) {
            return Err(malformed(header, "the flags are not two hex digits"));
        }

        Ok(Self {
            trace_id: trace_id.to_ascii_lowercase(),
            span_id: span_id.to_ascii_lowercase(),
            flags: u8::from_str_radix(flags, 16).unwrap_or_default(),
            tracestate: None,
        })
    }

    /// Carries a `tracestate` header alongside the `traceparent`.
    ///
    /// The standard asks a forwarder to pass `tracestate` on unmodified; it
    /// carries vendor data for the caller's tracing backend, not the cluster's.
    /// This client has no vendor entry of its own, so it never rewrites it.
    ///
    /// ```
    /// use ytsaurus_client::{Client, TraceContext};
    ///
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    /// let context = TraceContext::parse(incoming)?.with_tracestate("vendora=t61,vendorb=x9");
    ///
    /// let client = Client::new("http://localhost:8000").with_trace_context(&context);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_tracestate(mut self, state: impl Into<String>) -> Self {
        self.tracestate = Some(state.into());
        self
    }

    /// The `tracestate` this context carries, if it was given one.
    #[must_use]
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    /// The trace, as the header spells it: 32 lowercase hex digits.
    #[must_use]
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    /// The trace, as the cluster spells it: [`TraceContext::trace_id`]'s four
    /// 32-bit groups, hyphenated, leading zeros dropped (an all-zero group keeps
    /// one digit). This is the form in the proxy log, the `X-YT-Trace-Id`
    /// response header and the cluster's UI.
    ///
    /// ```
    /// use ytsaurus_client::TraceContext;
    ///
    /// # fn main() -> Result<(), ytsaurus_client::ClientError> {
    /// let trace = TraceContext::parse("00-08e9bcc435c2be9b456f18c4e117ea31-00f067aa0ba902b7-01")?;
    ///
    /// assert_eq!(trace.trace_id(), "08e9bcc435c2be9b456f18c4e117ea31");
    /// assert_eq!(trace.yt_trace_id(), "8e9bcc4-35c2be9b-456f18c4-e117ea31");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn yt_trace_id(&self) -> String {
        // The cluster's spelling is `WriteGuidToBuffer` in
        // `library/cpp/yt/misc/guid.cpp`. Slicing cannot fail: `parse` checks
        // and `new` formats the trace id as 32 ASCII hex digits.
        let groups: Vec<&str> = (0..4)
            .map(|group| {
                let group = &self.trace_id[group * 8..group * 8 + 8];
                let trimmed = group.trim_start_matches('0');
                if trimmed.is_empty() { "0" } else { trimmed }
            })
            .collect();

        groups.join("-")
    }

    /// The span this client's requests hang under: 16 lowercase hex digits.
    #[must_use]
    pub fn span_id(&self) -> &str {
        &self.span_id
    }

    /// Whether the trace is being recorded.
    ///
    /// A context that arrived unsampled is passed on unsampled: the decision
    /// belongs to whoever started the trace.
    #[must_use]
    pub fn is_sampled(&self) -> bool {
        self.flags & SAMPLED != 0
    }

    /// The `traceparent` header value, as it goes on the wire.
    #[must_use]
    pub fn header(&self) -> String {
        format!("00-{}-{}-{:02x}", self.trace_id, self.span_id, self.flags)
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TraceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.header())
    }
}

fn malformed(header: &str, reason: &str) -> ClientError {
    ClientError::Config(format!("{header:?} is not a traceparent: {reason}"))
}

fn is_hex(text: &str, digits: usize) -> bool {
    text.len() == digits && text.bytes().all(|b| b.is_ascii_hexdigit())
}

fn is_zero(text: &str) -> bool {
    text.bytes().all(|b| b == b'0')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_is_carried_through_unchanged() {
        // The example from the W3C specification, which is also the shape the
        // proxy's own parser expects.
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let context = TraceContext::parse(header).expect("parses");

        assert_eq!(context.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(context.span_id(), "00f067aa0ba902b7");
        assert!(context.is_sampled());
        assert_eq!(context.header(), header);
    }

    #[test]
    fn the_version_less_form_the_go_sdk_sends_is_accepted() {
        // `injectTracing` formats `%s-%016x-%02x` — no version — and the
        // proxy's parser has a note saying it supports exactly that. A client
        // that refused it would refuse what an official client sends.
        let context = TraceContext::parse("4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .expect("parses");

        assert_eq!(
            context.header(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "what is sent on is the four-part form"
        );
    }

    #[test]
    fn what_is_sent_is_lowercase_whatever_arrived() {
        // The standard requires lowercase on the wire; the proxy's hex parser
        // does not care. Liberal in, strict out.
        let context =
            TraceContext::parse("00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01")
                .expect("parses");

        assert_eq!(
            context.header(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn an_unsampled_trace_stays_unsampled() {
        // The sampling decision belongs to whoever started the trace. Turning
        // it on here would record this client's half of a trace whose other
        // half was dropped.
        let context =
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
                .expect("parses");

        assert!(!context.is_sampled());
        assert!(context.header().ends_with("-00"));
    }

    #[test]
    fn the_debug_flag_survives_the_round_trip() {
        // Bit 1 is `debug` to the proxy — `spanContext.Debug = options & 2u` —
        // and this client has no opinion about it beyond passing it on.
        let context =
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03")
                .expect("parses");

        assert!(context.is_sampled());
        assert!(context.header().ends_with("-03"));
    }

    #[test]
    fn a_version_from_the_future_is_read_as_far_as_it_is_understood() {
        // The standard's versioning rule, and the only thing that keeps a
        // version-00 parser working against a later sender: read the four
        // fields version 00 defines, ignore whatever follows. Refusing the
        // whole header instead would turn "this client is older than the
        // caller" into a failed request, because the documented usage
        // `?`-propagates the refusal.
        let context =
            TraceContext::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-af00")
                .expect("a later version is read as far as it is understood");

        assert_eq!(context.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(context.span_id(), "00f067aa0ba902b7");
        assert!(context.is_sampled());
        // Sent on as the version this client actually speaks, not the one it
        // was handed: claiming 01 would promise fields it did not carry.
        assert_eq!(
            context.header(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn version_zero_has_exactly_four_fields() {
        // The other half of the rule: 00 defines four groups and no more, so a
        // fifth is a malformed header rather than a newer one.
        assert!(
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-af00")
                .is_err()
        );
    }

    #[test]
    fn a_truncated_header_says_which_field_is_missing() {
        // Three groups, and the first is a version rather than a trace id —
        // this is the four-part form cut short, not the version-less form the
        // Go SDK sends. Read as the latter it would report a 32-digit trace id
        // as "not 32 hex digits", which is the wrong field and a genuinely
        // confusing thing to be told.
        let refusal = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7")
            .expect_err("the flags are not optional");

        let reason = refusal.to_string();
        assert!(reason.contains("flags"), "{reason}");
        assert!(
            !reason.contains("trace id"),
            "the trace id in this header is perfectly well formed: {reason}"
        );
    }

    #[test]
    fn a_tracestate_is_carried_beside_the_traceparent_untouched() {
        // The standard pairs the two and asks a forwarder to pass the second
        // on unmodified: it is where a vendor keeps its sampling decision or
        // its correlation key, and this hop losing it costs the caller's own
        // backend, not the cluster's.
        let context =
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .expect("parses")
                .with_tracestate("vendora=t61rcWkgMzE,vendorb=x9");

        assert_eq!(
            context.tracestate(),
            Some("vendora=t61rcWkgMzE,vendorb=x9"),
            "not rewritten: this client has no vendor entry of its own to add"
        );
        // And it is not smuggled into the traceparent, which has no room for it.
        assert_eq!(
            context.header(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn a_context_without_a_tracestate_has_none() {
        assert_eq!(TraceContext::new().tracestate(), None);
        assert_eq!(
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .expect("parses")
                .tracestate(),
            None
        );
    }

    #[test]
    fn a_malformed_header_is_refused_rather_than_sent() {
        // Watched on a local cluster: `traceparent: not-a-traceparent` is
        // answered 200, with a trace id the proxy generated for itself. So a
        // header this client failed to notice was wrong would leave the trace
        // quietly lacking the part that mattered. Each of these says which
        // part is wrong.
        let refused = [
            "",
            "nonsense",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            // One digit short, one digit long.
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b77-01",
            // Not hex.
            "00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01",
            "zz-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0",
            // All-zero ids are invalid by the standard, and useless anyway.
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            // `ff` is reserved as never-a-version.
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ];

        for header in refused {
            assert!(
                TraceContext::parse(header).is_err(),
                "{header:?} was accepted"
            );
        }
    }

    #[test]
    fn the_cluster_spelling_is_the_same_bits_punctuated() {
        // `FormatTraceParentHeader` writes the GUID's four 32-bit groups in
        // the order the cluster prints them, zero-padded to eight digits each;
        // `WriteGuidToBuffer` drops those zeros again. So the two spellings
        // differ by punctuation and padding and nothing else.
        let context =
            TraceContext::parse("00-8e9bcc435c2be9b456f18c4e117ea314-00f067aa0ba902b7-01")
                .expect("parses");

        assert_eq!(context.yt_trace_id(), "8e9bcc43-5c2be9b4-56f18c4e-117ea314");
    }

    #[test]
    fn a_group_the_cluster_would_shorten_is_shortened_here_too() {
        // Not derived from the format string — captured. Each of these was sent
        // to a local cluster as a `traceparent` and read back out of the
        // `X-YT-Trace-Id` of the answer, which is the proxy saying which trace
        // it decided the request belonged to.
        let observed = [
            (
                "4bf92f3577b34da6a3ce929d0e0e4736",
                "4bf92f35-77b34da6-a3ce929d-e0e4736",
            ),
            ("00000001000000020000000300000004", "1-2-3-4"),
            // A group of nothing but zeros keeps one digit, never none.
            ("00000000000000010000000000000002", "0-1-0-2"),
        ];

        for (sent, echoed) in observed {
            let header = format!("00-{sent}-00f067aa0ba902b7-01");
            let context = TraceContext::parse(&header).expect("parses");
            assert_eq!(context.yt_trace_id(), echoed, "{sent}");
        }
    }

    #[test]
    fn a_fresh_context_is_well_formed_and_new_every_time() {
        let mine = TraceContext::new();

        assert!(mine.is_sampled());
        assert_eq!(
            TraceContext::parse(&mine.header()).expect("its own header parses"),
            mine
        );

        let ids: std::collections::HashSet<String> =
            (0..10_000).map(|_| TraceContext::new().trace_id).collect();
        assert_eq!(
            ids.len(),
            10_000,
            "two traces sharing an id would be one trace"
        );
    }
}
