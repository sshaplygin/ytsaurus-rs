//! Helpers shared by the job runtime tests.
//!
//! Binary YSON is built by hand here rather than with the serializer, so the
//! tests check the runtime against the wire format described in the YTsaurus
//! docs instead of against our own encoder.

#![allow(dead_code)]

use std::io::{self, Read};

pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn bin_string(s: &[u8], out: &mut Vec<u8>) {
    out.push(0x01);
    uvarint(zigzag(s.len() as i64), out);
    out.extend_from_slice(s);
}

pub fn bin_i64(v: i64, out: &mut Vec<u8>) {
    out.push(0x02);
    uvarint(zigzag(v), out);
}

/// `{key=value}` with a string value.
pub fn bin_row_str(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = vec![b'{'];
    bin_string(key, &mut out);
    out.push(b'=');
    bin_string(value, &mut out);
    out.push(b'}');
    out
}

/// `{key=value}` with an int64 value.
pub fn bin_row_i64(key: &[u8], value: i64) -> Vec<u8> {
    let mut out = vec![b'{'];
    bin_string(key, &mut out);
    out.push(b'=');
    bin_i64(value, &mut out);
    out.push(b'}');
    out
}

/// `<key=value>#` with an int64 attribute — a control record.
pub fn bin_control_i64(key: &[u8], value: i64) -> Vec<u8> {
    let mut out = vec![b'<'];
    bin_string(key, &mut out);
    out.push(b'=');
    bin_i64(value, &mut out);
    out.extend_from_slice(b">#");
    out
}

/// `<key=%true>#`.
pub fn bin_control_bool(key: &[u8], value: bool) -> Vec<u8> {
    let mut out = vec![b'<'];
    bin_string(key, &mut out);
    out.push(b'=');
    out.push(if value { 0x05 } else { 0x04 });
    out.extend_from_slice(b">#");
    out
}

/// Joins records into a list fragment: `a;b;c;`.
pub fn fragment(records: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for record in records {
        out.extend_from_slice(record);
        out.push(b';');
    }
    out
}

/// A reader that yields at most `chunk` bytes per call.
///
/// Real input arrives from a pipe in arbitrary-sized pieces, so the runtime must
/// cope with a record split across any number of reads. Setting `chunk` to 1
/// exercises every possible split point.
pub struct ChunkedReader {
    data: Vec<u8>,
    pos: usize,
    chunk: usize,
}

impl ChunkedReader {
    pub fn new(data: Vec<u8>, chunk: usize) -> Self {
        Self {
            data,
            pos: 0,
            chunk: chunk.max(1),
        }
    }
}

impl Read for ChunkedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.data.len() - self.pos;
        let n = remaining.min(buf.len()).min(self.chunk);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// A reader that returns `Interrupted` before every successful read.
///
/// `Read::read` is allowed to fail with `ErrorKind::Interrupted` when a signal
/// arrives, and the caller is expected to retry rather than treat it as an
/// error.
pub struct InterruptingReader {
    inner: ChunkedReader,
    interrupt_next: bool,
}

impl InterruptingReader {
    pub fn new(data: Vec<u8>, chunk: usize) -> Self {
        Self {
            inner: ChunkedReader::new(data, chunk),
            interrupt_next: true,
        }
    }
}

impl Read for InterruptingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.interrupt_next {
            self.interrupt_next = false;
            return Err(io::Error::new(io::ErrorKind::Interrupted, "signal"));
        }
        self.interrupt_next = true;
        self.inner.read(buf)
    }
}

/// A sink that records everything written to it.
#[derive(Clone, Default)]
pub struct SharedBuffer(pub std::rc::Rc<std::cell::RefCell<Vec<u8>>>);

impl SharedBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contents(&self) -> Vec<u8> {
        self.0.borrow().clone()
    }
}

impl io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Where cargo put a compiled example, so a test can run one.
///
/// The workers under `examples/` are the thing these end-to-end tests exist to
/// run: they exec the binary the way a cluster does — rows in on fd 0, tables
/// out on fds 1 and 4 — rather than calling into the library, because what the
/// cluster runs is a process and not a function.
///
/// **Derived from the test binary's own path, because cargo names no variable
/// for it.** `CARGO_BIN_EXE_<name>` exists for `[[bin]]` targets and has no
/// counterpart for examples, so the only thing to go on is that both land in
/// the same profile directory: the test at `<profile>/deps/<name>-<hash>` and
/// the example at `<profile>/examples/<name>`. That holds for `--release` and
/// for a `--target` cross-build too, since the whole tree moves together.
///
/// # Panics
///
/// If the example is not there, which means it was not built. `cargo test` and
/// `cargo test --all-targets` build examples; `cargo test --test wordcount_e2e`
/// on its own does not, and the message says so rather than leaving a reader
/// with `No such file or directory` about a path they never wrote.
pub fn example(name: &str) -> std::path::PathBuf {
    let mut path = std::env::current_exe().expect("a test knows its own path");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push("examples");
    path.push(name);

    assert!(
        path.is_file(),
        "the `{name}` example is not built at {}.\n\
         Run `cargo test -p ytsaurus-job` or `cargo build -p ytsaurus-job \
         --examples` first: cargo builds examples for a whole-package test run \
         and not for `--test <name>` on its own.",
        path.display()
    );
    path
}
