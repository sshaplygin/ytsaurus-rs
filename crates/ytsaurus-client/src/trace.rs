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

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{ClientError, Result};

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
        }
    }

    /// Continues the trace a `traceparent` header names.
    ///
    /// Both spellings the proxy accepts are accepted here: the standard
    /// `00-<trace>-<span>-<flags>` and the version-less three-part form the Go
    /// SDK sends. Hex digits may be upper or lower case on the way in; what
    /// this client sends is always lowercase, as the standard requires.
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

        // The three-part form has no version, and the proxy reads it as zero.
        let [version, trace_id, span_id, flags] = match parts[..] {
            [trace_id, span_id, flags] => ["00", trace_id, span_id, flags],
            [version, trace_id, span_id, flags] => [version, trace_id, span_id, flags],
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
        })
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
        let groups: Vec<&str> = self
            .trace_id
            .as_bytes()
            .chunks(8)
            .map(|group| {
                let group = std::str::from_utf8(group).unwrap_or("0");
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

/// Counts calls, so two ids made in the same nanosecond still differ.
static IDS: AtomicU64 = AtomicU64::new(0);

/// Sixty-four bits unlikely to have been produced before.
///
/// The entropy is `RandomState`'s, which the standard library seeds from the OS
/// once per process, mixed with a counter and the clock — the same source
/// [`MutationId`](crate::MutationId) draws on, and for the same reason: an id
/// has to be *unique*, not unpredictable, and that is a poor reason to add a
/// random-number crate to a dependency list this short.
fn word(salt: u64) -> u64 {
    use std::hash::{BuildHasher, Hasher, RandomState};

    let counter = IDS.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(counter);
    hasher.write_u64(nanos);
    hasher.write_u64(salt);
    hasher.finish()
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
