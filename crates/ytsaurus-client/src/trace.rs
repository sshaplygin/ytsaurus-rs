//! The trace a request belongs to, carried to the cluster in a `traceparent`
//! header.
//!
//! The cluster is already instrumented: the proxy opens a span for every
//! request it serves, and each of those spans either starts a trace of its own
//! or continues one the caller named. Naming it is this whole module — a
//! request sent with a `traceparent` shows up under the caller's trace rather
//! than as an orphan, so a launch that took four minutes can be looked at
//! beside whatever asked for it.
//!
//! The header is the
//! [W3C one](https://www.w3.org/TR/trace-context/#traceparent-header):
//!
//! ```text
//! traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
//!              ^^ ^^ 32 hex: the trace  ^^ 16 hex: the caller's span  ^^ flags
//! ```
//!
//! All three official clients send exactly that: `FormatTraceParentHeader` in
//! the C++ wrapper (`yt/cpp/mapreduce/http/helpers.cpp`), `injectTracing` in
//! the Go SDK (`yt/go/yt/internal/httpclient/client.go`), and
//! `generate_traceparent` in the Python wrapper (`yt/python/yt/wrapper/`).
//! What the proxy accepts is `TryParseTraceParent` in
//! `yt/yt/core/http/helpers.cpp`, and it is slightly wider than the standard:
//! the version may be left off entirely — which is what the Go SDK does — and
//! the flags are read as a byte with **bit 0 sampled, bit 1 debug**.
//!
//! # Finding the trace afterwards
//!
//! The cluster spells a trace id as one of its own GUIDs —
//! `8e9bcc43-5c2be9b4-56f18c4e-117ea314` — and the header spells the same 128
//! bits as 32 hex digits. They are the same four 32-bit groups in the same
//! order, so the only difference is the dashes and the leading zeros the
//! cluster drops (`WriteGuidToBuffer` in `library/cpp/yt/misc/guid.cpp`, and
//! `FormatTraceParentHeader`, which pads them back). [`TraceContext::yt_trace_id`]
//! does that conversion, so the id can be pasted into the cluster's own log
//! search rather than translated by hand.
//!
//! # Watched rather than assumed
//!
//! A proxy puts the trace id it decided on into the `X-YT-Trace-Id` of the
//! response, which makes every question above answerable with one request. On a
//! local cluster, sending
//! `traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01` to
//! `/api/v4/exists` comes back with
//! `X-YT-Trace-Id: 4bf92f35-77b34da6-a3ce929d-e0e4736` — the same id, the
//! cluster's spelling, a leading zero dropped. The version-less form and
//! uppercase hex are adopted the same way; a header that does not parse is
//! answered 200 with an id the proxy invented, which is the whole reason
//! [`TraceContext::parse`] refuses one rather than passing it on.

use crate::error::{ClientError, Result};
use crate::unique::word;

/// Bit 0 of the flags: this trace is being recorded.
const SAMPLED: u8 = 0x01;

/// The trace a request belongs to.
///
/// Two ways in. [`TraceContext::parse`] continues a trace that already exists,
/// which is the usual one: a service that received a `traceparent` of its own
/// passes it on, and the cluster's work appears under the same trace as the
/// request that caused it.
///
/// ```
/// use ytsaurus_client::{Client, TraceContext};
///
/// # fn main() -> Result<(), ytsaurus_client::ClientError> {
/// let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
/// let client = Client::new("http://localhost:8000")
///     .with_trace_context(&TraceContext::parse(incoming)?);
/// # Ok(())
/// # }
/// ```
///
/// [`TraceContext::new`] starts one, for a program that is nobody's callee.
/// Print the id it made and the cluster's copy of the trace can be found by
/// it:
///
/// ```
/// use ytsaurus_client::{Client, TraceContext};
///
/// let trace = TraceContext::new();
/// eprintln!("trace {}", trace.yt_trace_id());
///
/// let client = Client::new("http://localhost:8000").with_trace_context(&trace);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// 32 lowercase hex digits.
    trace_id: String,
    /// 16 lowercase hex digits: the span this client's requests are children
    /// of.
    span_id: String,
    flags: u8,
    /// The `tracestate` that arrived beside the `traceparent`, if any, carried
    /// unmodified. See [`TraceContext::with_tracestate`].
    tracestate: Option<String>,
}

impl TraceContext {
    /// Starts a trace, sampled.
    ///
    /// Sampled because a caller who asked for a trace wants it kept: the C++
    /// wrapper's `EnableClientTracing` and the Python wrapper's
    /// `generate_traceparent` both do the same. An unsampled context is one
    /// that arrived that way — see [`TraceContext::parse`].
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
    /// Both spellings the proxy accepts are accepted here: the standard
    /// `00-<trace>-<span>-<flags>` and the version-less three-part form the Go
    /// SDK sends. Hex digits may be upper or lower case on the way in; what
    /// this client sends is always lowercase, as the standard requires.
    ///
    /// The span id is carried through **as it arrived**, so the cluster's spans
    /// hang under the span the *caller* named rather than under one belonging
    /// to this process. That is what a client with nothing of its own to point
    /// at can honestly do: the W3C wording asks a forwarder to substitute the
    /// id of its own current span, and this crate emits no spans the collector
    /// would know about — an invented id would name a parent that does not
    /// exist. The work still lands in the right trace, one level up from where
    /// a fully instrumented service would put it.
    ///
    /// A `tracestate` that arrived beside the header is not in it, and is
    /// passed on separately — see [`TraceContext::with_tracestate`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the header is not a traceparent.
    /// Refusing is the point: a malformed header is dropped by the proxy
    /// without complaint, and the trace would then be silently missing the
    /// half that mattered.
    pub fn parse(header: &str) -> Result<Self> {
        let header = header.trim();
        let parts: Vec<&str> = header.split('-').collect();

        let [version, trace_id, span_id, flags] = match parts[..] {
            // The three-part form has no version, and the proxy reads it as
            // zero. Recognised by the trace id rather than by the count, so
            // that a four-part header with its flags cut off is not silently
            // read as this one — see the arm below.
            [trace_id, span_id, flags] if is_hex(trace_id, 32) => ["00", trace_id, span_id, flags],
            // Version, trace, span, and nothing where the flags should be.
            // Destructured as the version-less form this would report the
            // *version* as a bad trace id, sending whoever is debugging a
            // truncated header to a field that is perfectly well formed.
            [version, trace_id, _] if is_hex(version, 2) && is_hex(trace_id, 32) => {
                return Err(malformed(header, "the flags are missing"));
            }
            [version, trace_id, span_id, flags] => [version, trace_id, span_id, flags],
            // A version this client does not know may define fields after the
            // flags, and the standard's versioning rule is to read the four
            // that version 00 defines and ignore the rest — that rule is the
            // only reason a version-00 parser keeps working against a
            // version-01 sender. Version 00 itself defines exactly four, so a
            // fifth group there is malformed rather than from the future.
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
        // `ff` is reserved by the standard as "no version will ever be this",
        // so a header carrying it is malformed rather than from the future.
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
    /// The standard pairs the two, and asks a participant that forwards one to
    /// forward the other unmodified: `tracestate` is where a vendor puts the
    /// sampling decision or the correlation key that its own backend reads, and
    /// dropping it on this hop loses that for everything downstream. The proxy
    /// itself has no opinion about it — this is for the caller's backend, not
    /// the cluster's.
    ///
    /// Not modified on the way through, deliberately: rewriting the list means
    /// claiming a vendor entry of one's own, and this client has none.
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

    /// The trace, as the **cluster** spells it: four hyphenated hex groups,
    /// leading zeros dropped.
    ///
    /// This is the form that appears in the proxy log, in the `X-YT-Trace-Id`
    /// header of a response, and in the cluster's UI — the same 128 bits as
    /// [`TraceContext::trace_id`], punctuated the way every other YTsaurus id
    /// is.
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
        // Sliced from the string rather than reassembled from its bytes: the
        // trace id is 32 ASCII hex digits by construction — `parse` checks it
        // and `new` formats it — so there is no decoding to fail. Going
        // through `from_utf8` needed a fallback for a case that cannot happen,
        // and the only cheap fallback was a wrong id, which is worse than an
        // error: an id that is off by one group matches nothing in the proxy
        // log and says nothing about why.
        let groups: Vec<&str> = (0..4)
            .map(|group| {
                let group = &self.trace_id[group * 8..group * 8 + 8];
                // The cluster prints one to eight digits per group, so a group
                // that is all zeros keeps a single one.
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
    /// belongs to whoever started the trace, and overriding it here would
    /// record half a trace.
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
