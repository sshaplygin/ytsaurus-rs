//! Writing a job's output tables.
//!
//! YTsaurus gives a job one file descriptor per output table, numbered `3k + 1`:
//! table 0 is fd 1 (stdout), table 1 is fd 4, table 2 is fd 7, and so on. A job
//! can also write everything to a single descriptor and switch destinations with
//! `<table_index=N>#` records. Both are supported here; see [`JobWriter`].

use std::io::Write;

use serde::Serialize;
use ytsaurus_yson::{YsonFormat, ser::Serializer};

use crate::error::{JobError, Result};

/// Per-table output buffer. 256 KiB keeps the write syscall count low without
/// making a many-table job expensive.
pub(crate) const TABLE_BUFFER_BYTES: usize = 256 * 1024;

/// File descriptor for output table `index`, per the `3k + 1` rule.
#[must_use]
pub fn table_descriptor(index: usize) -> i32 {
    (3 * index + 1) as i32
}

/// A handle to one of a job's output tables.
///
/// Obtained from [`JobWriter::named`], which hands out exactly one per declared
/// table, so a handle is always in range. Naming them at the call site is the
/// point: `writer.write(rejects, &row)` says what it does, where
/// `writer.write(1, &row)` needs a comment and survives being transposed with
/// `write(0, …)` — a job that then runs happily and fills each table with the
/// other's rows.
///
/// A `usize` still converts, so existing code keeps working.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(usize);

impl TableId {
    /// The table's index.
    #[must_use]
    pub fn index(self) -> usize {
        self.0
    }
}

impl From<usize> for TableId {
    fn from(index: usize) -> Self {
        TableId(index)
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// How rows reach their destination table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Routing {
    /// One descriptor per table.
    Descriptors,
    /// Everything on one descriptor, with `<table_index=N>#` switch records.
    TableSwitches { current: i64 },
}

/// Writes rows to a job's output tables.
///
/// Output is buffered. [`JobWriter::finish`] must be called (or [`crate::run`]
/// used, which calls it) — dropping the writer cannot report a failed flush, and
/// a silently truncated output table is the worst possible outcome.
///
/// # Example
///
/// ```no_run
/// use serde::Serialize;
/// use ytsaurus_job::JobWriter;
///
/// #[derive(Serialize)]
/// struct Row<'a> { key: &'a str, count: u64 }
///
/// let mut writer = JobWriter::descriptors(1)?;
/// writer.write(0, &Row { key: "a", count: 1 })?;
/// writer.finish()?;
/// # Ok::<(), ytsaurus_job::JobError>(())
/// ```
pub struct JobWriter {
    /// Physical sinks. With table switches there is exactly one, regardless of
    /// how many logical tables the job addresses.
    tables: Vec<Box<dyn Write>>,
    /// How many output tables the job has, which is what `write` validates
    /// against.
    logical_tables: usize,
    format: YsonFormat,
    routing: Routing,
    /// Reused across rows so serializing does not allocate per row.
    scratch: Vec<u8>,
    finished: bool,
    /// Declared table names, when built with [`JobWriter::named`]. Empty
    /// otherwise; used to make an out-of-range write explain itself.
    names: Vec<String>,
}

impl std::fmt::Debug for JobWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobWriter")
            .field("tables", &self.tables.len())
            .field("format", &self.format)
            .field("routing", &self.routing)
            .finish_non_exhaustive()
    }
}

impl JobWriter {
    /// Writes binary YSON to one descriptor per output table (fds 1, 4, 7, …).
    ///
    /// This is the normal arrangement, and the one YTsaurus assumes unless the
    /// job says otherwise.
    ///
    /// # Errors
    ///
    /// Currently infallible, but returns `Result` so that opening descriptors
    /// can start reporting failures without breaking callers.
    #[cfg(unix)]
    pub fn descriptors(table_count: usize) -> Result<Self> {
        Self::descriptors_with_format(table_count, YsonFormat::Binary)
    }

    /// Writes YSON in `format` to one descriptor per output table (fds 1, 4,
    /// 7, …).
    ///
    /// Most workers should use [`JobWriter::descriptors`], whose binary YSON
    /// default matches YTsaurus jobs. This constructor exists for
    /// [`crate::WorkerWriter`], which exposes the shared [`ytsaurus_format::DataFormat`]
    /// selection without silently changing the requested YSON encoding.
    ///
    /// # Errors
    ///
    /// Currently infallible, but returns `Result` so that opening descriptors
    /// can start reporting failures without breaking callers.
    #[cfg(unix)]
    pub fn descriptors_with_format(table_count: usize, format: YsonFormat) -> Result<Self> {
        let tables = (0..table_count)
            .map(|i| -> Box<dyn Write> {
                Box::new(std::io::BufWriter::with_capacity(
                    TABLE_BUFFER_BYTES,
                    output_descriptor(table_descriptor(i)),
                ))
            })
            .collect();

        Ok(Self::from_writers(tables, format))
    }

    /// Declares output tables by name, returning a handle for each.
    ///
    /// The names are for the reader, not the cluster — YTsaurus knows tables by
    /// position. What they buy is a call site that says which table it means:
    ///
    /// ```no_run
    /// # use serde::Serialize;
    /// # use ytsaurus_job::JobWriter;
    /// # #[derive(Serialize)] struct Row { a: i64 }
    /// # let row = Row { a: 1 };
    /// let (mut writer, [events, rejects]) = JobWriter::named(["events", "rejects"])?;
    ///
    /// writer.write(events, &row)?;
    /// writer.write(rejects, &row)?;
    /// writer.finish()?;
    /// # Ok::<(), ytsaurus_job::JobError>(())
    /// ```
    ///
    /// Handles come out in the order declared, so table 0 is the first name.
    /// Because they are the only way to get a [`TableId`] other than converting
    /// a `usize`, an out-of-range write is impossible when using this
    /// constructor.
    ///
    /// # Errors
    ///
    /// See [`JobWriter::descriptors`].
    #[cfg(unix)]
    pub fn named<const N: usize>(names: [&str; N]) -> Result<(Self, [TableId; N])> {
        let mut writer = Self::descriptors(N)?;
        writer.names = names.iter().map(|n| (*n).to_owned()).collect();
        Ok((writer, Self::ids()))
    }

    /// Like [`JobWriter::named`], over arbitrary sinks, for tests.
    #[must_use]
    pub fn named_writers<const N: usize>(
        names: [&str; N],
        tables: Vec<Box<dyn Write>>,
        format: YsonFormat,
    ) -> (Self, [TableId; N]) {
        let mut writer = Self::from_writers(tables, format);
        writer.names = names.iter().map(|n| (*n).to_owned()).collect();
        (writer, Self::ids())
    }

    /// Handles `0..N`, in declaration order.
    fn ids<const N: usize>() -> [TableId; N] {
        let mut ids = [TableId(0); N];
        for (i, id) in ids.iter_mut().enumerate() {
            *id = TableId(i);
        }
        ids
    }

    /// The name declared for `table`, if the writer was built with
    /// [`JobWriter::named`].
    #[must_use]
    pub fn table_name(&self, table: impl Into<TableId>) -> Option<&str> {
        self.names.get(table.into().index()).map(String::as_str)
    }

    /// Writes every table to fd 1, switching with `<table_index=N>#` records.
    ///
    /// Useful when a job produces rows for many tables interleaved, since it
    /// avoids keeping a buffer per descriptor. Note that YTsaurus does not
    /// define the ordering of rows that reach one table through two descriptors,
    /// so do not mix this with [`JobWriter::descriptors`].
    ///
    /// # Errors
    ///
    /// See [`JobWriter::descriptors`].
    #[cfg(unix)]
    pub fn table_switches(table_count: usize) -> Result<Self> {
        let single: Box<dyn Write> = Box::new(std::io::BufWriter::with_capacity(
            TABLE_BUFFER_BYTES,
            output_descriptor(table_descriptor(0)),
        ));

        Ok(Self::from_writer_with_switches(
            single,
            table_count,
            YsonFormat::Binary,
        ))
    }

    /// Builds a table-switching writer over an arbitrary sink.
    ///
    /// The counterpart to [`JobWriter::from_writers`] for switch mode, so the
    /// switching logic can be tested against an in-memory buffer.
    #[must_use]
    pub fn from_writer_with_switches(
        sink: Box<dyn Write>,
        table_count: usize,
        format: YsonFormat,
    ) -> Self {
        let mut writer = Self::from_writers(vec![sink], format);
        // One physical descriptor, `table_count` logical tables.
        writer.routing = Routing::TableSwitches { current: 0 };
        writer.logical_tables = table_count;
        writer
    }

    /// Builds a writer over arbitrary sinks, one per output table.
    ///
    /// Exists so jobs can be tested against in-memory buffers instead of real
    /// file descriptors.
    #[must_use]
    pub fn from_writers(tables: Vec<Box<dyn Write>>, format: YsonFormat) -> Self {
        let logical_tables = tables.len();
        Self {
            tables,
            format,
            routing: Routing::Descriptors,
            scratch: Vec::with_capacity(8192),
            finished: false,
            logical_tables,
            names: Vec::new(),
        }
    }

    /// Number of output tables this writer can address.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.logical_tables
    }

    /// Serializes `row` and appends it to output table `table`.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::UnknownTable`] if `table` is out of range,
    /// [`JobError::Serialize`] if the row cannot be encoded, or
    /// [`JobError::Write`] if the descriptor rejects the write.
    pub fn write<T: Serialize + ?Sized>(
        &mut self,
        table: impl Into<TableId>,
        row: &T,
    ) -> Result<()> {
        let table = table.into().index();
        self.check_table(table)?;

        // Take the scratch buffer out so `write_bytes` can borrow `self`
        // mutably, then hand the allocation back regardless of the outcome.
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();

        let mut ser = Serializer::with_buffer(scratch, matches!(self.format, YsonFormat::Binary));
        let outcome = row.serialize(&mut ser);
        let encoded = ser.into_output();

        let result = match outcome {
            Ok(()) => self.write_bytes(table, &encoded),
            Err(source) => Err(JobError::Serialize { table, source }),
        };

        self.scratch = encoded;
        result
    }

    /// Appends an already-encoded row to output table `table`.
    ///
    /// The bytes must be one complete YSON value in this writer's format; the
    /// record separator is added here. Pass [`crate::Row::raw`] to forward an
    /// input row unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::UnknownTable`] or [`JobError::Write`].
    pub fn write_raw(&mut self, table: impl Into<TableId>, row: &[u8]) -> Result<()> {
        let table = table.into().index();
        self.check_table(table)?;
        self.write_bytes(table, row)
    }

    fn check_table(&self, table: usize) -> Result<()> {
        // A row accepted after finish() would sit in the buffer and vanish at
        // exit — the short-table-under-exit-zero outcome finish() rules out.
        if self.finished {
            return Err(JobError::WriteAfterFinish { table });
        }
        if table >= self.logical_tables {
            return Err(JobError::UnknownTable {
                index: table,
                count: self.logical_tables,
                names: self.names.clone(),
            });
        }
        Ok(())
    }

    fn write_bytes(&mut self, table: usize, encoded: &[u8]) -> Result<()> {
        let (sink_index, switch) = match &mut self.routing {
            Routing::Descriptors => (table, None),
            Routing::TableSwitches { current } => {
                let needed = i64::try_from(table).unwrap_or(i64::MAX);
                let switch = if *current == needed {
                    None
                } else {
                    *current = needed;
                    Some(needed)
                };
                (0, switch)
            }
        };

        let format = self.format;
        let sink = &mut self.tables[sink_index];

        if let Some(index) = switch {
            let record = encode_table_switch(index, format);
            sink.write_all(&record)
                .map_err(|source| JobError::Write { table, source })?;
        }

        sink.write_all(encoded)
            .and_then(|()| sink.write_all(b";"))
            .map_err(|source| JobError::Write { table, source })
    }

    /// Flushes every output table.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Write`] if a table cannot be flushed.
    pub fn flush(&mut self) -> Result<()> {
        for (table, sink) in self.tables.iter_mut().enumerate() {
            sink.flush()
                .map_err(|source| JobError::Write { table, source })?;
        }
        Ok(())
    }

    /// Flushes and marks the writer complete.
    ///
    /// Call this before the job exits. A buffered row that never reaches the
    /// descriptor is a row missing from the output table, and a job that exits
    /// zero with a short table is far worse than one that fails loudly.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Write`] if a table cannot be flushed.
    pub fn finish(&mut self) -> Result<()> {
        self.flush()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for JobWriter {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Last-ditch flush so a job that forgot `finish()` still produces its
        // output; the error cannot be returned from `drop`, so say so loudly.
        // The job's exit code is unaffected, which is exactly why `finish()`
        // exists and why `run()` calls it.
        if let Err(e) = self.flush() {
            eprintln!("ytsaurus-job: output was not flushed cleanly: {e}");
        }
    }
}

/// `<table_index=N>#;`
fn encode_table_switch(index: i64, format: YsonFormat) -> Vec<u8> {
    match format {
        YsonFormat::Text => format!("<table_index={index}>#;").into_bytes(),
        YsonFormat::Binary => {
            let mut out = vec![b'<', 0x01];
            write_zigzag(b"table_index".len() as i64, &mut out);
            out.extend_from_slice(b"table_index");
            out.push(b'=');
            out.push(0x02);
            write_zigzag(index, &mut out);
            out.push(b'>');
            out.push(b'#');
            out.push(b';');
            out
        }
    }
}

fn write_zigzag(value: i64, out: &mut Vec<u8>) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Wraps an inherited descriptor without taking ownership of it.
#[cfg(unix)]
pub(crate) fn output_descriptor(fd: i32) -> OutputDescriptor {
    use std::os::fd::FromRawFd;

    // SAFETY: `fd` was opened by YTsaurus for this job before it started, and
    // stays open for the job's lifetime. The `File` is wrapped in
    // `ManuallyDrop` below, so this never takes ownership of the descriptor.
    let file = unsafe { std::fs::File::from_raw_fd(fd) };
    OutputDescriptor {
        file: std::mem::ManuallyDrop::new(file),
    }
}

/// An output file descriptor the job inherited.
///
/// The descriptor is deliberately **never closed**. `File::from_raw_fd` takes
/// ownership and would close on drop — for table 0 that is fd 1, which
/// `std::io::stdout()` also refers to, so closing it would leave any later
/// `println!` writing to a closed (or worse, recycled) descriptor. The process
/// exiting is what closes these, and that is the right time.
#[cfg(unix)]
#[derive(Debug)]
pub struct OutputDescriptor {
    file: std::mem::ManuallyDrop<std::fs::File>,
}

#[cfg(unix)]
impl Write for OutputDescriptor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.file.write_all(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
