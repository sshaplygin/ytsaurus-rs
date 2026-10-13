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

/// A runnable copy of one of the `examples/` workers, built by this function.
///
/// End-to-end tests exec the worker as a cluster does (rows in on fd 0, tables
/// out on fds 1 and 4), because what the cluster runs is a process. Cargo sets
/// `CARGO_BIN_EXE_<name>` only for `[[bin]]` targets, so the path is derived:
/// the test binary is `<profile>/deps/<name>-<hash>` and the runnable example
/// `<profile>/examples/<name>`, under `--release` and `--target` alike.
///
/// No ordinary test command reliably builds the runnable example:
/// - `cargo test --all-targets` compiles every example as a libtest harness at
///   `examples/<name>-<hash>`, which answers `running 0 tests`; a hashed
///   sibling is never used.
/// - a plain `cargo test` builds runnable examples except those declared
///   `test = true` in `Cargo.toml`, which also get the harness: `counted`,
///   `sessionize`, `shards` and `wordcount`, four of the five workers exec'd
///   here.
/// - `cargo test --test cat_e2e` builds no examples at all.
///
/// So this runs `cargo build --example <name>` once per name per test binary.
/// The cargo that started the test has released the target-directory lock by
/// then, so the build waits only if something else is building.
///
/// # Panics
///
/// If the example cannot be built, with cargo's own output.
pub fn example(name: &str) -> std::path::PathBuf {
    use std::sync::{Mutex, OnceLock};

    // One build per name per process: several tests in one binary ask for the
    // same worker, and they run on their own threads.
    static BUILT: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();

    let mut dir = std::env::current_exe().expect("a test knows its own path");
    dir.pop();
    if dir.ends_with("deps") {
        dir.pop();
    }
    // `<profile>` — `debug` for the dev profile, and its own name otherwise.
    let profile = dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("debug")
        .to_owned();
    dir.push("examples");
    let path = dir.join(name);

    let mut built = BUILT
        .get_or_init(|| Mutex::new(std::collections::HashSet::new()))
        .lock()
        .expect("the build set is only ever locked here");

    // Once per name per process, and **not** only when the binary is missing.
    // An integration test does not depend on an example target, so cargo will
    // not rebuild one for it; guarded on `!path.is_file()`, every run after
    // the first would drive whatever binary is on disk, and an edited worker's
    // e2e test would report on the previous edit while passing. Cargo is a
    // no-op when nothing changed.
    if built.insert(name.to_owned()) {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let mut command = std::process::Command::new(cargo);
        command.args(["build", "-p", "ytsaurus-job", "--example", name]);
        // `--profile dev` is spelled `debug` in the directory and rejected on
        // the command line, so the one case that needs no flag is the one that
        // cannot take it.
        if profile != "debug" {
            command.args(["--profile", &profile]);
        }

        let out = command
            .output()
            .unwrap_or_else(|e| panic!("could not run cargo to build the `{name}` example: {e}"));
        assert!(
            out.status.success(),
            "building the `{name}` example failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    drop(built);

    assert!(
        path.is_file(),
        "no runnable `{name}` example at {} even after building it",
        path.display()
    );
    path
}
