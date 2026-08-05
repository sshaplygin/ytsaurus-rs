//! Incremental reading of a job's input stream.
//!
//! YTsaurus hands a job a *list fragment* on fd 0 — `record; record; record;` —
//! that can be far larger than the job's memory limit. [`JobReader`] consumes it
//! a buffer at a time, never holding more than one record plus the read buffer,
//! and hands out rows that borrow directly from that buffer.

use std::io::{ErrorKind, Read};

use serde::Deserialize;
use ytsaurus_yson::{Scan, YsonFormat, YsonNode, YsonValue, from_slice, scan::scan_value};

use crate::error::{JobError, Result};

/// Default read buffer, and the steady-state memory cost of a reader.
const DEFAULT_BUFFER_BYTES: usize = 1024 * 1024;

/// Default ceiling on a single record, to bound the damage from corrupt input.
const DEFAULT_MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;

/// One item from the input stream.
#[derive(Debug)]
pub enum Event<'a> {
    /// A data row.
    Row(Row<'a>),
    /// A reduce key boundary: the previous row and the next row belong to
    /// different keys.
    ///
    /// Only produced when the operation sets `control_attributes.enable_key_switch`.
    /// [`JobReader::groups`] turns these into per-key iterators.
    KeySwitch,
}

/// A single input row, borrowed from the reader's buffer.
///
/// The row is not decoded until you ask for it, so a job that only forwards
/// rows never pays to parse them — see [`Row::raw`].
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    /// Index of the input table this row came from.
    ///
    /// Stays `0` unless the operation enables `control_attributes.enable_table_index`.
    pub table_index: i64,
    /// Index of this row within its input table, if `enable_row_index` is set.
    pub row_index: Option<i64>,
    /// Index of the requested range this row came from, if `enable_range_index` is set.
    pub range_index: Option<i64>,

    bytes: &'a [u8],
    format: YsonFormat,
    offset: u64,
}

impl<'a> Row<'a> {
    /// The row's raw YSON bytes, exactly as they arrived.
    ///
    /// Writing these straight to an output table reproduces the row
    /// byte-for-byte, which decoding and re-encoding does not guarantee (map
    /// keys come back sorted). This is what an identity job should use.
    #[must_use]
    pub fn raw(&self) -> &'a [u8] {
        self.bytes
    }

    /// Offset of this row from the start of the input stream, for diagnostics.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Decodes the row into `T`.
    ///
    /// `T` may borrow from the row (`&str`, `&[u8]`), which avoids copying
    /// string columns; such a `T` cannot outlive this row.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Yson`] if the row does not match `T`'s shape.
    pub fn parse<T: Deserialize<'a>>(&self) -> Result<T> {
        from_slice(self.bytes, self.format).map_err(|source| JobError::Yson {
            offset: self.offset,
            source,
        })
    }

    /// Decodes the row into the dynamic [`YsonValue`] representation.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Yson`] if the row is not valid YSON.
    pub fn value(&self) -> Result<YsonValue> {
        self.parse()
    }
}

/// What the reader has lined up but not yet handed out.
///
/// Holds only lengths, never a borrow, so it can sit in the reader across calls
/// without freezing the buffer.
#[derive(Debug, Clone, Copy)]
enum Pending {
    Row { len: usize },
    KeySwitch { len: usize },
}

/// Reads a YTsaurus job's input stream incrementally.
///
/// # Example
///
/// ```no_run
/// use ytsaurus_job::{Event, JobReader};
///
/// let mut reader = JobReader::from_stdin();
/// while let Some(event) = reader.next_event()? {
///     match event {
///         Event::Row(row) => {
///             let _bytes = row.raw();
///         }
///         Event::KeySwitch => {}
///     }
/// }
/// # Ok::<(), ytsaurus_job::JobError>(())
/// ```
#[derive(Debug)]
pub struct JobReader<R> {
    input: R,
    format: YsonFormat,

    buf: Vec<u8>,
    /// Start of the unconsumed region of `buf`.
    pos: usize,
    /// End of valid data in `buf`.
    filled: usize,
    /// Stream offset corresponding to `buf[0]`.
    base_offset: u64,
    /// The input reader has signalled end of file.
    input_done: bool,

    pending: Option<Pending>,
    max_record_bytes: usize,

    table_index: i64,
    row_index: Option<i64>,
    range_index: Option<i64>,
}

impl JobReader<std::io::Stdin> {
    /// Reads binary YSON from fd 0, which is where YTsaurus puts a job's input.
    ///
    /// Use [`JobReader::text`] instead if the operation was configured with
    /// `<format=text>yson`.
    #[must_use]
    pub fn from_stdin() -> Self {
        Self::binary(std::io::stdin())
    }
}

impl<R: Read> JobReader<R> {
    /// Reads binary YSON — the format jobs normally use.
    #[must_use]
    pub fn binary(input: R) -> Self {
        Self::with_format(input, YsonFormat::Binary)
    }

    /// Reads text YSON, which is useful for fixtures and debugging.
    #[must_use]
    pub fn text(input: R) -> Self {
        Self::with_format(input, YsonFormat::Text)
    }

    /// Reads YSON in an explicit format.
    #[must_use]
    pub fn with_format(input: R, format: YsonFormat) -> Self {
        Self {
            input,
            format,
            buf: vec![0; DEFAULT_BUFFER_BYTES],
            pos: 0,
            filled: 0,
            base_offset: 0,
            input_done: false,
            pending: None,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            table_index: 0,
            row_index: None,
            range_index: None,
        }
    }

    /// Sets the read buffer size. Records larger than this grow the buffer.
    #[must_use]
    pub fn with_buffer_size(mut self, bytes: usize) -> Self {
        self.buf = vec![0; bytes.max(64)];
        self
    }

    /// Sets the ceiling on a single record.
    ///
    /// The buffer grows on demand up to this limit; beyond it the job fails with
    /// [`JobError::RecordTooLarge`] rather than trying to allocate whatever a
    /// corrupt length prefix asked for.
    #[must_use]
    pub fn with_max_record_bytes(mut self, bytes: usize) -> Self {
        self.max_record_bytes = bytes;
        self
    }

    /// Returns the next event, or `None` at end of stream.
    ///
    /// The returned [`Event`] borrows the reader's buffer, so it must be dropped
    /// before the next call — the compiler enforces this.
    ///
    /// # Errors
    ///
    /// Returns [`JobError`] if the stream cannot be read or does not parse.
    pub fn next_event(&mut self) -> Result<Option<Event<'_>>> {
        let Some(pending) = self.ensure_pending()? else {
            return Ok(None);
        };

        match pending {
            Pending::KeySwitch { len } => {
                self.consume(len);
                Ok(Some(Event::KeySwitch))
            }
            Pending::Row { len } => {
                let start = self.pos;
                let offset = self.base_offset + start as u64;
                let row_index = self.row_index;
                self.consume(len);
                Ok(Some(Event::Row(Row {
                    table_index: self.table_index,
                    row_index,
                    range_index: self.range_index,
                    bytes: &self.buf[start..start + len],
                    format: self.format,
                    offset,
                })))
            }
        }
    }

    /// Splits the stream into reduce groups on `key_switch` boundaries.
    ///
    /// Requires `control_attributes.enable_key_switch` on the operation;
    /// without it the whole input is a single group.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ytsaurus_job::JobReader;
    /// let mut reader = JobReader::from_stdin();
    /// let mut groups = reader.groups();
    /// while let Some(mut group) = groups.next_group()? {
    ///     while let Some(row) = group.next_row()? {
    ///         let _ = row.raw();
    ///     }
    /// }
    /// # Ok::<(), ytsaurus_job::JobError>(())
    /// ```
    pub fn groups(&mut self) -> Groups<'_, R> {
        Groups {
            reader: self,
            in_group: false,
            key_columns: Vec::new(),
        }
    }

    /// Like [`JobReader::groups`], but decodes the reduce key for each group.
    ///
    /// Pass the same columns the operation was given as `reduce_by`. Each
    /// [`Group`] then answers [`Group::key`] without the caller having to parse
    /// its first row and copy the key out.
    ///
    /// YTsaurus does not transmit the key: `key_switch` carries no payload, and
    /// the key lives in the rows. So this reads it from the group's first row —
    /// the same work a job would do by hand, done once and in one place.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ytsaurus_job::JobReader;
    /// let mut reader = JobReader::from_stdin();
    /// let mut groups = reader.groups_by(["user_id"]);
    /// while let Some(mut group) = groups.next_group()? {
    ///     let user = group.key().bytes("user_id").unwrap_or_default().to_vec();
    ///     while let Some(row) = group.next_row()? {
    ///         let _ = (&user, row.raw());
    ///     }
    /// }
    /// # Ok::<(), ytsaurus_job::JobError>(())
    /// ```
    pub fn groups_by<I>(&mut self, columns: I) -> Groups<'_, R>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Groups {
            reader: self,
            in_group: false,
            key_columns: columns
                .into_iter()
                .map(|c| c.as_ref().as_bytes().to_vec())
                .collect(),
        }
    }

    /// Decodes the requested key columns out of the pending row.
    ///
    /// Called while a row is pending, so `pos..pos + len` is that row and the
    /// buffer cannot move underneath it.
    fn decode_key(&mut self, len: usize, columns: &[Vec<u8>]) -> Result<GroupKey> {
        let start = self.pos;
        let offset = self.base_offset + start as u64;
        let record = &self.buf[start..start + len];

        let value: YsonValue =
            from_slice(record, self.format).map_err(|source| JobError::Yson { offset, source })?;

        let YsonNode::Map(fields) = &value.node else {
            return Err(JobError::Yson {
                offset,
                source: ytsaurus_yson::YsonError::Custom(
                    "a reduce key can only be read from a row that is a map".to_owned(),
                ),
            });
        };

        // Missing columns are skipped rather than fatal: a reduce key column
        // may legitimately be absent from a row, and failing the whole job over
        // it would be a worse default than reporting the key we could read.
        let mut decoded = Vec::with_capacity(columns.len());
        for name in columns {
            if let Some(v) = fields.get(name) {
                decoded.push((name.clone(), v.clone()));
            }
        }

        Ok(GroupKey { columns: decoded })
    }

    /// Advances `pos` past a record that has been handed out.
    fn consume(&mut self, len: usize) {
        self.pos += len;
        // YTsaurus emits `<row_index=N>#` only at discontinuities — the start
        // of a range or chunk. Every row after it implicitly advances the
        // index, so count the row that was just consumed, whether it was
        // handed out or skipped.
        if matches!(self.pending, Some(Pending::Row { .. }))
            && let Some(index) = self.row_index.as_mut()
        {
            *index += 1;
        }
        self.pending = None;
    }

    /// Discards the pending record without handing it out.
    fn discard_pending(&mut self) {
        if let Some(Pending::Row { len } | Pending::KeySwitch { len }) = self.pending {
            self.consume(len);
        }
    }

    /// Ensures a row or key switch is buffered and ready at `self.pos`.
    ///
    /// Control records other than `key_switch` are consumed and applied here, so
    /// callers only ever see data. Idempotent: while something is pending the
    /// buffer is never refilled, which is what makes it safe to look before
    /// deciding to consume.
    fn ensure_pending(&mut self) -> Result<Option<Pending>> {
        if let Some(pending) = self.pending {
            return Ok(Some(pending));
        }

        loop {
            let Some(len) = self.next_record_len()? else {
                return Ok(None);
            };

            let offset = self.base_offset + self.pos as u64;
            let record = &self.buf[self.pos..self.pos + len];

            // Control records are attributed entities; data rows are maps. Only
            // a leading `<` can be a control record, so rows are never decoded
            // here and cost nothing to route.
            let classified = if first_significant_byte(record, self.format) == Some(b'<') {
                classify_attributed(record, self.format, offset)?
            } else {
                Classified::Row
            };

            match classified {
                Classified::Row => {
                    let pending = Pending::Row { len };
                    self.pending = Some(pending);
                    return Ok(Some(pending));
                }
                Classified::KeySwitch => {
                    let pending = Pending::KeySwitch { len };
                    self.pending = Some(pending);
                    return Ok(Some(pending));
                }
                Classified::TableIndex(i) => {
                    self.table_index = i;
                    // A new table restarts row and range numbering; drop the
                    // stale values rather than reporting them from the old
                    // table.
                    self.row_index = None;
                    self.range_index = None;
                    self.pos += len;
                }
                Classified::RowIndex(i) => {
                    self.row_index = Some(i);
                    self.pos += len;
                }
                Classified::RangeIndex(i) => {
                    self.range_index = Some(i);
                    self.pos += len;
                }
                Classified::Skip => self.pos += len,
            }
        }
    }

    /// Length of the next complete record, refilling the buffer as needed.
    ///
    /// Leaves `self.pos` at the first byte of that record.
    fn next_record_len(&mut self) -> Result<Option<usize>> {
        loop {
            self.skip_separators();

            if self.pos < self.filled {
                match scan_value(&self.buf[self.pos..self.filled], self.format) {
                    Ok(Scan::Complete { len }) => return Ok(Some(len)),
                    Ok(Scan::Incomplete) => {}
                    Err(source) => {
                        return Err(JobError::Yson {
                            offset: self.base_offset + self.pos as u64,
                            source,
                        });
                    }
                }
            }

            if self.input_done {
                return if self.pos == self.filled {
                    Ok(None)
                } else {
                    Err(JobError::TruncatedRecord {
                        offset: self.base_offset + self.pos as u64,
                        buffered: self.filled - self.pos,
                    })
                };
            }

            self.fill()?;
        }
    }

    /// Skips record separators and whitespace between records.
    fn skip_separators(&mut self) {
        while self.pos < self.filled {
            match self.buf[self.pos] {
                b';' => self.pos += 1,
                b if b.is_ascii_whitespace() => self.pos += 1,
                _ => break,
            }
        }
    }

    /// Compacts the buffer, grows it if a single record needs more room, and
    /// reads once from the input.
    fn fill(&mut self) -> Result<()> {
        if self.pos > 0 {
            self.buf.copy_within(self.pos..self.filled, 0);
            self.filled -= self.pos;
            self.base_offset += self.pos as u64;
            self.pos = 0;
        }

        if self.filled == self.buf.len() {
            // The record in flight does not fit. Double the buffer, but refuse
            // to chase an absurd length prefix into an OOM abort.
            let new_len = self.buf.len().saturating_mul(2);
            if self.buf.len() >= self.max_record_bytes {
                return Err(JobError::RecordTooLarge {
                    offset: self.base_offset,
                    limit: self.max_record_bytes,
                });
            }
            self.buf.resize(new_len.min(self.max_record_bytes), 0);
        }

        loop {
            match self.input.read(&mut self.buf[self.filled..]) {
                Ok(0) => {
                    self.input_done = true;
                    return Ok(());
                }
                Ok(n) => {
                    self.filled += n;
                    return Ok(());
                }
                // A signal interrupted the read; nothing was consumed, so retry.
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(JobError::Read(e)),
            }
        }
    }
}

/// What a record turned out to be.
enum Classified {
    /// A data row, to be handed to the job.
    Row,
    /// A control record carrying nothing this reader acts on. Consumed silently.
    Skip,
    TableIndex(i64),
    RowIndex(i64),
    RangeIndex(i64),
    KeySwitch,
}

/// First byte that is not insignificant whitespace.
fn first_significant_byte(record: &[u8], format: YsonFormat) -> Option<u8> {
    match format {
        YsonFormat::Binary => record.first().copied(),
        YsonFormat::Text => record.iter().find(|b| !b.is_ascii_whitespace()).copied(),
    }
}

/// Decides what an attributed record (`<...>...`) is.
///
/// Per the YTsaurus docs a control record is an *entity* carrying attributes,
/// while a data record is a map. An attributed entity is therefore always a
/// control record, even when the attribute is one this version does not know —
/// such a record must be skipped, never handed to the job as a row.
fn classify_attributed(record: &[u8], format: YsonFormat, offset: u64) -> Result<Classified> {
    let value: YsonValue =
        from_slice(record, format).map_err(|source| JobError::Yson { offset, source })?;

    // Attributes on something that is not an entity: a data row that happens to
    // carry attributes.
    if !matches!(value.node, YsonNode::Entity) {
        return Ok(Classified::Row);
    }
    let Some(attributes) = value.attributes.as_ref() else {
        // A bare `#` with no attributes. Not a row and not a control record;
        // there is nothing to hand over, so drop it.
        return Ok(Classified::Skip);
    };

    let as_i64 = |name: &str, v: &YsonValue| -> Result<i64> {
        v.as_i64().ok_or_else(|| JobError::BadControlRecord {
            offset,
            reason: format!("{name} must be an int64, got {:?}", v.node),
        })
    };

    for (key, v) in attributes {
        match key.as_slice() {
            b"key_switch" => {
                return match v.node {
                    YsonNode::Boolean(true) => Ok(Classified::KeySwitch),
                    // `<key_switch=%false>#` is a control record, just not a
                    // group boundary.
                    YsonNode::Boolean(false) => Ok(Classified::Skip),
                    ref other => Err(JobError::BadControlRecord {
                        offset,
                        reason: format!("key_switch must be a boolean, got {other:?}"),
                    }),
                };
            }
            b"table_index" => return Ok(Classified::TableIndex(as_i64("table_index", v)?)),
            b"row_index" => return Ok(Classified::RowIndex(as_i64("row_index", v)?)),
            b"range_index" => return Ok(Classified::RangeIndex(as_i64("range_index", v)?)),
            _ => {}
        }
    }

    // An attributed entity carrying only attributes we do not recognise. It is
    // still a control record, so skip it rather than failing: YTsaurus may add
    // control attributes later, and a job built today should survive meeting
    // one. Emitting it as a row would silently corrupt the output.
    Ok(Classified::Skip)
}

/// The reduce key of a group, decoded from its first row.
///
/// Empty unless the group came from [`JobReader::groups_by`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GroupKey {
    columns: Vec<(Vec<u8>, YsonValue)>,
}

impl GroupKey {
    /// The key column `name`, if the group has one.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&YsonValue> {
        self.columns
            .iter()
            .find(|(k, _)| k == name.as_bytes())
            .map(|(_, v)| v)
    }

    /// The key column `name` as raw bytes, if it is a string.
    ///
    /// Reduce keys are frequently byte strings rather than text, so this does
    /// not go through `str`.
    #[must_use]
    pub fn bytes(&self, name: &str) -> Option<&[u8]> {
        match &self.get(name)?.node {
            YsonNode::String(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// The key column `name` as UTF-8, if it is a string and valid UTF-8.
    #[must_use]
    pub fn str(&self, name: &str) -> Option<&str> {
        std::str::from_utf8(self.bytes(name)?).ok()
    }

    /// The key column `name` as an integer, if it is one.
    #[must_use]
    pub fn i64(&self, name: &str) -> Option<i64> {
        self.get(name)?.as_i64()
    }

    /// Every key column, in the order they were requested.
    #[must_use]
    pub fn columns(&self) -> &[(Vec<u8>, YsonValue)] {
        &self.columns
    }

    /// Whether any key column was decoded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Iterator over reduce groups. Created by [`JobReader::groups`] or
/// [`JobReader::groups_by`].
#[derive(Debug)]
pub struct Groups<'r, R> {
    reader: &'r mut JobReader<R>,
    in_group: bool,
    /// Columns forming the reduce key; empty for [`JobReader::groups`].
    key_columns: Vec<Vec<u8>>,
}

impl<R: Read> Groups<'_, R> {
    /// Advances to the next group.
    ///
    /// Rows left unread in the current group are skipped, so a caller that only
    /// needs the first row of each group does not have to drain the rest.
    ///
    /// # Errors
    ///
    /// Returns [`JobError`] if the stream cannot be read or does not parse.
    pub fn next_group(&mut self) -> Result<Option<Group<'_, R>>> {
        if self.in_group {
            // Drain what is left of the current group, stopping on its boundary.
            loop {
                match self.reader.ensure_pending()? {
                    None => {
                        self.in_group = false;
                        return Ok(None);
                    }
                    Some(Pending::KeySwitch { .. }) => {
                        self.reader.discard_pending();
                        break;
                    }
                    Some(Pending::Row { .. }) => self.reader.discard_pending(),
                }
            }
        }

        // A group exists only if at least one more record is coming.
        match self.reader.ensure_pending()? {
            None => {
                self.in_group = false;
                Ok(None)
            }
            Some(Pending::KeySwitch { .. }) => {
                // Back-to-back switches: an empty group. YTsaurus does not emit
                // these, but reporting one is more honest than dropping it.
                //
                // The group is born `done`, and `in_group` stays false: its
                // boundary was the switch just consumed, so there is nothing to
                // drain before the next group. A live group here would hand out
                // the *next* group's rows under this group's (empty) key, and
                // that group would never be seen.
                self.reader.discard_pending();
                self.in_group = false;
                Ok(Some(Group {
                    reader: self.reader,
                    done: true,
                    key: GroupKey::default(),
                }))
            }
            Some(Pending::Row { len }) => {
                // Decode the key now, while the first row is pending and the
                // buffer is guaranteed not to move. Doing it here rather than
                // in `Group::key` keeps that accessor a plain `&self`.
                let key = if self.key_columns.is_empty() {
                    GroupKey::default()
                } else {
                    self.reader.decode_key(len, &self.key_columns)?
                };

                self.in_group = true;
                Ok(Some(Group {
                    reader: self.reader,
                    done: false,
                    key,
                }))
            }
        }
    }
}

/// The rows of a single reduce group. Created by [`Groups::next_group`].
#[derive(Debug)]
pub struct Group<'g, R> {
    reader: &'g mut JobReader<R>,
    done: bool,
    key: GroupKey,
}

impl<R: Read> Group<'_, R> {
    /// The group's reduce key.
    ///
    /// Populated only for groups from [`JobReader::groups_by`]; otherwise
    /// [`GroupKey::is_empty`] is true.
    #[must_use]
    pub fn key(&self) -> &GroupKey {
        &self.key
    }

    /// Returns the next row of this group, or `None` at the group boundary.
    ///
    /// # Errors
    ///
    /// Returns [`JobError`] if the stream cannot be read or does not parse.
    pub fn next_row(&mut self) -> Result<Option<Row<'_>>> {
        if self.done {
            return Ok(None);
        }

        match self.reader.ensure_pending()? {
            None | Some(Pending::KeySwitch { .. }) => {
                // Leave the switch pending; `next_group` consumes it.
                self.done = true;
                Ok(None)
            }
            Some(Pending::Row { len }) => {
                let start = self.reader.pos;
                let offset = self.reader.base_offset + start as u64;
                let row_index = self.reader.row_index;
                self.reader.consume(len);
                Ok(Some(Row {
                    table_index: self.reader.table_index,
                    row_index,
                    range_index: self.reader.range_index,
                    bytes: &self.reader.buf[start..start + len],
                    format: self.reader.format,
                    offset,
                }))
            }
        }
    }
}
