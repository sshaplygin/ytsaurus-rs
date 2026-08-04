//! Table I/O that does not go through memory.
//!
//! [`Client::read_table`](crate::Client::read_table) and
//! [`Client::write_table`](crate::Client::write_table) hold a whole table at
//! once, which is right for a launcher inspecting a result and wrong for
//! anything the size of the data. The streaming pair moves the same bytes
//! without ever holding more than a buffer of them.
//!
//! Both are the raw byte stream — a YSON list fragment — because that is what
//! the other end of this project already speaks: `ytsaurus_job::JobReader`
//! reads exactly this, so a table read on a laptop and a table read inside a
//! job go through the same decoder.

use std::io::Read;

/// A table's rows, arriving as they are read.
///
/// A YSON list fragment, in whatever format the read asked for — binary by
/// default, which is what `ytsaurus_job::JobReader::binary` expects.
///
/// # The check this gives up
///
/// [`Client::read_table`](crate::Client::read_table) verifies that what came
/// back is a *complete* fragment, which is the client's only defence against a
/// mid-stream failure it cannot see (the proxy reports one in a trailer, and
/// `ureq` 3.3 exposes no trailers — rechecked against its source, not assumed).
/// Streaming cannot do that up front: the point is not to have the whole thing.
///
/// The defence moves to the decoder. A fragment cut short leaves a record that
/// does not parse, and `JobReader` fails on it rather than stopping quietly —
/// which is the same protection, applied at the point where it can still be
/// applied.
pub struct TableReader {
    inner: ureq::BodyReader<'static>,
    read: u64,
}

impl TableReader {
    pub(crate) fn new(body: ureq::Body) -> Self {
        Self {
            // A reader has no size cap, where `read_to_vec` — what the buffered
            // path uses — stops at 10 MB unless told otherwise. That asymmetry
            // is the right way round: a stream has no size a client should
            // presume, and a table read into memory very much does.
            inner: body.into_reader(),
            read: 0,
        }
    }

    /// How many bytes have come out of it so far.
    #[must_use]
    pub fn bytes_read(&self) -> u64 {
        self.read
    }
}

impl Read for TableReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        Ok(n)
    }
}

impl std::fmt::Debug for TableReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableReader")
            .field("bytes_read", &self.read)
            .finish()
    }
}

/// Turns rows into the byte stream a table write sends.
///
/// The encoder sits inside the request body rather than in front of it: rows
/// are serialised a bufferful at a time, as the transport asks for bytes, so
/// writing a million rows costs one buffer and not a million rows' worth of
/// memory. That is the difference between
/// [`Client::write_table_rows`](crate::Client::write_table_rows) and encoding a
/// `Vec<u8>` first, and it is why the encoder lives here.
pub(crate) struct RowStream<I> {
    rows: I,
    buffer: Vec<u8>,
    position: usize,
    /// How many rows have been encoded, so a failure can name which one.
    written: u64,
    /// The first row that would not serialise.
    ///
    /// `Read` can only report an `io::Error`, which the transport wraps in
    /// whatever it makes of a failed body. Keeping the real reason here lets
    /// the caller be told what actually happened: which is that row 40 000 has
    /// a map key that is not a string, not that the connection broke.
    pub(crate) failed: Option<String>,
}

/// How much to encode before handing bytes over.
const ROW_CHUNK: usize = 64 * 1024;

impl<T, I> RowStream<I>
where
    T: serde::Serialize,
    I: Iterator<Item = T>,
{
    pub(crate) fn new(rows: I) -> Self {
        Self {
            rows,
            buffer: Vec::with_capacity(ROW_CHUNK + 4096),
            position: 0,
            written: 0,
            failed: None,
        }
    }

    /// Encodes rows until the buffer is full or the rows run out.
    fn fill(&mut self) {
        self.buffer.clear();
        self.position = 0;

        while self.buffer.len() < ROW_CHUNK {
            let Some(row) = self.rows.next() else { break };

            // Serialised straight into the buffer that is about to be sent:
            // `to_vec` would allocate a `Vec` per row, which for a table write
            // is one allocation per row of the table.
            let mut serializer =
                ytsaurus_yson::ser::Serializer::with_buffer(std::mem::take(&mut self.buffer), true);
            let outcome = serde::Serialize::serialize(&row, &mut serializer);
            self.buffer = serializer.into_output();

            if let Err(e) = outcome {
                self.failed = Some(format!("row {}: {e}", self.written));
                return;
            }
            self.buffer.push(b';');
            self.written += 1;
        }
    }
}

impl<T, I> Read for RowStream<I>
where
    T: serde::Serialize,
    I: Iterator<Item = T>,
{
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.position == self.buffer.len() {
            if self.failed.is_some() {
                // Ending the body early would upload a truncated table and
                // call it success. Failing the request is the only honest
                // answer, and `failed` carries the reason out.
                return Err(std::io::Error::other("a row could not be encoded"));
            }
            self.fill();
            if let Some(reason) = &self.failed {
                return Err(std::io::Error::other(reason.clone()));
            }
            if self.buffer.is_empty() {
                return Ok(0);
            }
        }

        let n = out.len().min(self.buffer.len() - self.position);
        out[..n].copy_from_slice(&self.buffer[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reader_of(bytes: &[u8]) -> TableReader {
        TableReader::new(ureq::Body::builder().data(bytes.to_vec()))
    }

    fn encoded<T: serde::Serialize>(rows: Vec<T>) -> Vec<u8> {
        let mut stream = RowStream::new(rows.into_iter());
        let mut out = Vec::new();
        stream.read_to_end(&mut out).expect("encodes");
        out
    }

    #[test]
    fn rows_become_a_yson_list_fragment() {
        #[derive(serde::Serialize)]
        struct Row {
            n: i64,
        }

        let bytes = encoded(vec![Row { n: 1 }, Row { n: 2 }]);

        // Two records, each terminated: exactly what `write_table` and a job's
        // output both expect.
        assert_eq!(bytes.iter().filter(|b| **b == b';').count(), 2);
        assert!(bytes.ends_with(b";"));

        let decoded: Vec<std::collections::BTreeMap<String, i64>> =
            crate::decode_rows(&bytes, "test").expect("round-trips");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0]["n"], 1);
        assert_eq!(decoded[1]["n"], 2);
    }

    #[test]
    fn no_rows_is_an_empty_body_rather_than_an_error() {
        // An empty table is a legitimate result, and a write that refused to
        // send one would make callers special-case it.
        let bytes = encoded(Vec::<i64>::new());
        assert!(bytes.is_empty());
    }

    #[test]
    fn a_row_that_cannot_be_encoded_fails_the_write() {
        // The codec itself refuses almost nothing — it writes whatever the
        // visitor hands it, byte-string map keys included, which is the point
        // of the fork. So the failure that actually happens is a caller's own
        // `Serialize` refusing a value, and that is what this stands in for.
        struct Unwritable(u32);

        impl serde::Serialize for Unwritable {
            fn serialize<S: serde::Serializer>(
                &self,
                s: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                if self.0 == 2 {
                    return Err(serde::ser::Error::custom("this row refuses to be written"));
                }
                s.serialize_u32(self.0)
            }
        }

        let mut stream = RowStream::new((0..5).map(Unwritable));
        let mut out = Vec::new();

        // Sending the rows encoded so far would leave a short table reported
        // as a successful write, which is the failure worth preventing.
        assert!(stream.read_to_end(&mut out).is_err());
        let reason = stream.failed.expect("the reason must survive the error");
        assert!(reason.contains("row 2"), "{reason}");
        assert!(reason.contains("refuses to be written"), "{reason}");
    }

    #[test]
    fn a_million_rows_do_not_become_a_million_rows_of_memory() {
        #[derive(serde::Serialize)]
        struct Row {
            n: i64,
            payload: &'static str,
        }

        let rows = (0..1_000_000).map(|n| Row {
            n,
            payload: "0123456789abcdef0123456789abcdef",
        });
        let mut stream = RowStream::new(rows);

        let mut total = 0_u64;
        let mut scratch = [0_u8; 8192];
        loop {
            let n = stream.read(&mut scratch).expect("encodes");
            if n == 0 {
                break;
            }
            total += n as u64;
        }

        // Far more than the buffer, which is the point of the exercise.
        assert!(total > 40_000_000, "{total} bytes");
        assert!(
            stream.buffer.capacity() < 4 * ROW_CHUNK,
            "the buffer grew with the table: {} bytes",
            stream.buffer.capacity()
        );
    }

    #[test]
    fn it_hands_back_what_came_in_and_counts_it() {
        let mut reader = reader_of(b"{\x01\x02a=\x02\x02};");
        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("reads");

        assert_eq!(out, b"{\x01\x02a=\x02\x02};");
        assert_eq!(reader.bytes_read(), out.len() as u64);
    }

    #[test]
    fn the_count_follows_the_reading_rather_than_the_response() {
        // A caller that stops early has read what it read; the count is not the
        // table's size, and saying so is the difference between a progress
        // number and a wrong one.
        let mut reader = reader_of(&[b'x'; 100]);
        let mut first = [0_u8; 10];
        reader.read_exact(&mut first).expect("reads");

        assert_eq!(reader.bytes_read(), 10);
    }

    #[test]
    fn a_table_past_the_buffered_paths_cap_streams_whole() {
        // Twice what `read_to_vec` would take without being told otherwise. A
        // cap on this path would show up as a short table rather than an error,
        // which is the worst way for a limit to be discovered.
        let big = vec![b'y'; 20 * 1024 * 1024];
        let mut reader = reader_of(&big);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("reads");
        assert_eq!(out.len(), big.len());
    }
}
