//! Finding record boundaries without decoding.
//!
//! A YTsaurus job reads a *list fragment* — `value; value; value;` — from a pipe,
//! and that stream can be far larger than memory. To consume it incrementally you
//! need to answer one question about a partially-filled buffer: **where does the
//! next complete value end, and is there one at all?**
//!
//! [`scan_value`] answers exactly that. It walks the token stream without
//! allocating or building any value, and reports either the length of the first
//! complete value or that more bytes are needed. Truncation is reported as
//! [`Scan::Incomplete`] rather than as an error, which is what makes it safe to
//! call on a buffer that stops in the middle of a record.

use crate::{error::YsonError, lexer::YsonIterator, node::Token, ser::YsonFormat};

/// Nesting depth beyond which scanning gives up.
///
/// Matches the deserializer's limit, so anything this accepts can also be
/// decoded, and a hostile stream cannot drive the scanner into deep recursion.
const MAX_DEPTH: usize = 128;

/// Outcome of scanning for one complete value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scan {
    /// A complete value occupies the first `len` bytes of the input.
    Complete {
        /// Length of the value in bytes, from the start of the input.
        len: usize,
    },
    /// The input ends in the middle of a value; supply more bytes and retry.
    ///
    /// An empty input is also `Incomplete`: whether that means "end of stream"
    /// or "not read enough yet" is for the caller to decide.
    Incomplete,
}

/// Returns the length of the first complete YSON value in `input`.
///
/// Leading whitespace and comments (text format only) are counted as part of the
/// value's extent, so `&input[..len]` can be handed straight to
/// [`crate::from_slice`]. A leading item separator is *not* consumed — strip it
/// before calling.
///
/// # Errors
///
/// Returns [`YsonError`] if the input is malformed in a way that more data
/// cannot fix, such as an invalid marker or a mismatched bracket. Running out of
/// bytes is never an error; it is [`Scan::Incomplete`].
///
/// # Examples
///
/// ```
/// use ytsaurus_yson::{YsonFormat, scan::{Scan, scan_value}};
///
/// // Two records; only the first is measured.
/// let input = b"{a=1};{b=2}";
/// assert_eq!(scan_value(input, YsonFormat::Text)?, Scan::Complete { len: 5 });
///
/// // A record cut short asks for more bytes instead of failing.
/// assert_eq!(scan_value(b"{a=", YsonFormat::Text)?, Scan::Incomplete);
/// # Ok::<(), ytsaurus_yson::YsonError>(())
/// ```
pub fn scan_value(input: &[u8], format: YsonFormat) -> Result<Scan, YsonError> {
    let mut lexer = YsonIterator::new(input, matches!(format, YsonFormat::Binary));

    match scan_tree(&mut lexer, 0) {
        Ok(()) => Ok(Scan::Complete { len: lexer.pos() }),
        // Running past the end of the buffer means "not enough bytes yet", not
        // "broken". The caller distinguishes the two by knowing whether the
        // underlying stream is exhausted.
        Err(YsonError::Eof | YsonError::UnexpectedEof(_)) => Ok(Scan::Incomplete),
        Err(e) => Err(e),
    }
}

/// `<tree> = [ <attributes> ], <object>`
fn scan_tree(lexer: &mut YsonIterator<'_>, depth: usize) -> Result<(), YsonError> {
    if depth > MAX_DEPTH {
        return Err(YsonError::Custom("Recursion limit exceeded".into()));
    }

    let mut token = lexer.next_token()?;

    if matches!(token, Token::BeginAttributes) {
        scan_fragment(lexer, depth + 1, Token::EndAttributes)?;
        token = lexer.next_token()?;
    }

    scan_object(lexer, depth, token)
}

fn scan_object(
    lexer: &mut YsonIterator<'_>,
    depth: usize,
    token: Token<'_>,
) -> Result<(), YsonError> {
    match token {
        Token::String(_)
        | Token::Int64(_)
        | Token::Uint64(_)
        | Token::Double(_)
        | Token::Boolean(_)
        | Token::Entity => Ok(()),

        Token::BeginList => scan_list(lexer, depth + 1),
        Token::BeginMap => scan_fragment(lexer, depth + 1, Token::EndMap),

        other => Err(YsonError::UnexpectedToken {
            expected: "a YSON value",
            found: format!("{other:?}"),
            pos: lexer.pos(),
        }),
    }
}

/// `<list-fragment> = { <list-item>, ";" }, [ <list-item> ]` up to `]`.
fn scan_list(lexer: &mut YsonIterator<'_>, depth: usize) -> Result<(), YsonError> {
    if depth > MAX_DEPTH {
        return Err(YsonError::Custom("Recursion limit exceeded".into()));
    }

    loop {
        match lexer.peek_byte()? {
            b']' => {
                lexer.next_token()?;
                return Ok(());
            }
            b';' => {
                lexer.next_token()?;
            }
            _ => scan_tree(lexer, depth)?,
        }
    }
}

/// `<map-fragment> = { <key-value-pair>, ";" }, [ <key-value-pair> ]` up to
/// `end` (`}` for a map, `>` for an attribute block).
fn scan_fragment(
    lexer: &mut YsonIterator<'_>,
    depth: usize,
    end: Token<'static>,
) -> Result<(), YsonError> {
    if depth > MAX_DEPTH {
        return Err(YsonError::Custom("Recursion limit exceeded".into()));
    }

    let end_byte = match end {
        Token::EndMap => b'}',
        Token::EndAttributes => b'>',
        _ => unreachable!("scan_fragment is only called for maps and attributes"),
    };

    loop {
        let peeked = lexer.peek_byte()?;
        if peeked == end_byte {
            lexer.next_token()?;
            return Ok(());
        }
        if peeked == b';' {
            lexer.next_token()?;
            continue;
        }

        // <key-value-pair> = <string>, "=", <tree>
        match lexer.next_token()? {
            Token::String(_) => {}
            other => {
                return Err(YsonError::UnexpectedToken {
                    expected: "a map key",
                    found: format!("{other:?}"),
                    pos: lexer.pos(),
                });
            }
        }
        match lexer.next_token()? {
            Token::KeyValueSeparator => {}
            other => {
                return Err(YsonError::UnexpectedToken {
                    expected: "'='",
                    found: format!("{other:?}"),
                    pos: lexer.pos(),
                });
            }
        }
        scan_tree(lexer, depth)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(input: &[u8], format: YsonFormat) -> usize {
        match scan_value(input, format).expect("scan must not fail") {
            Scan::Complete { len } => len,
            Scan::Incomplete => panic!("expected a complete value in {input:?}"),
        }
    }

    #[test]
    fn scans_text_scalars() {
        assert_eq!(complete(b"42", YsonFormat::Text), 2);
        assert_eq!(complete(b"42;43", YsonFormat::Text), 2);
        assert_eq!(complete(b"#", YsonFormat::Text), 1);
        assert_eq!(complete(b"%true", YsonFormat::Text), 5);
        assert_eq!(complete(br#""a;b""#, YsonFormat::Text), 5);
    }

    #[test]
    fn scans_text_composites() {
        assert_eq!(complete(b"{a=1}", YsonFormat::Text), 5);
        assert_eq!(complete(b"{a=1};{b=2}", YsonFormat::Text), 5);
        assert_eq!(complete(b"[1;2;3]", YsonFormat::Text), 7);
        assert_eq!(complete(b"{a={b=[1;2]}}", YsonFormat::Text), 13);
        assert_eq!(complete(b"{a=1;}", YsonFormat::Text), 6);
    }

    #[test]
    fn scans_attributed_values() {
        assert_eq!(complete(b"<a=1>#", YsonFormat::Text), 6);
        assert_eq!(complete(b"<a=1>#;{b=2}", YsonFormat::Text), 6);
        assert_eq!(complete(b"<a=1;b=2>[1]", YsonFormat::Text), 12);
        assert_eq!(complete(b"<a=<b=1>#>#", YsonFormat::Text), 11);
    }

    #[test]
    fn reports_truncation_as_incomplete() {
        for input in [
            b"{a=1".as_slice(),
            b"{a=",
            b"{",
            b"[1;2",
            b"<a=1>",
            b"<a=1",
            b"",
            b"\"unterminated",
        ] {
            assert_eq!(
                scan_value(input, YsonFormat::Text).expect("no error"),
                Scan::Incomplete,
                "input {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn scans_binary_values() {
        // 0x02 int64, zigzag(1) = 2
        assert_eq!(complete(&[0x02, 0x02], YsonFormat::Binary), 2);
        // 0x01 string, zigzag(3) = 6, "abc"
        assert_eq!(complete(b"\x01\x06abc", YsonFormat::Binary), 5);
        // A string whose bytes contain YSON punctuation must not confuse the scan.
        assert_eq!(complete(b"\x01\x06};]", YsonFormat::Binary), 5);
        // 0x03 double + 8 bytes
        assert_eq!(
            complete(&[0x03, 0, 0, 0, 0, 0, 0, 0, 0], YsonFormat::Binary),
            9
        );
    }

    #[test]
    fn scans_a_binary_map_and_stops_at_the_boundary() {
        // {a=1};{a=1}
        let one = b"{\x01\x02a=\x02\x02}";
        let mut two = one.to_vec();
        two.push(b';');
        two.extend_from_slice(one);

        assert_eq!(complete(one, YsonFormat::Binary), one.len());
        assert_eq!(complete(&two, YsonFormat::Binary), one.len());
    }

    #[test]
    fn binary_truncation_is_incomplete() {
        let full = b"{\x01\x02a=\x02\x02}";
        for cut in 0..full.len() {
            assert_eq!(
                scan_value(&full[..cut], YsonFormat::Binary).expect("no error"),
                Scan::Incomplete,
                "cut at {cut}"
            );
        }
        // A string header promising more bytes than are present.
        assert_eq!(
            scan_value(b"\x01\x14abc", YsonFormat::Binary).expect("no error"),
            Scan::Incomplete
        );
    }

    #[test]
    fn rejects_malformed_input() {
        // Undefined marker.
        assert!(scan_value(&[0x07], YsonFormat::Binary).is_err());
        // Map with no `=`.
        assert!(scan_value(b"{a 1}", YsonFormat::Text).is_err());
        // Closing bracket with nothing open.
        assert!(scan_value(b"]", YsonFormat::Text).is_err());
    }

    #[test]
    fn rejects_deep_nesting() {
        let deep = vec![b'['; MAX_DEPTH + 10];
        assert!(scan_value(&deep, YsonFormat::Text).is_err());
    }

    #[test]
    fn text_comments_count_toward_the_value() {
        // Leading trivia is included so the slice can be re-parsed as-is.
        assert_eq!(complete(b"/* c */42", YsonFormat::Text), 9);
        assert_eq!(complete(b"  42", YsonFormat::Text), 4);
    }
}
