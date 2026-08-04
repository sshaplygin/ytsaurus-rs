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

#[cfg(test)]
mod tests {
    use super::*;

    fn reader_of(bytes: &[u8]) -> TableReader {
        TableReader::new(ureq::Body::builder().data(bytes.to_vec()))
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
