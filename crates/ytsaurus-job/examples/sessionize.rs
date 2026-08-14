//! `sessionize` — a production-shaped pilot: access-log sessionization.
//!
//! This exists to put `ytsaurus-job` under the kind of load a real task
//! generates, rather than the tidy shape an example usually has. Specifically it
//! wants: wide rows with mixed types, byte columns that are not UTF-8, several
//! output tables, a reduce over a realistic key, and input that is partly
//! malformed.
//!
//! ## Map phase
//!
//! Validates each event and routes it:
//!
//! - table 0 `events`   — well-formed events, keyed for the reduce
//! - table 1 `rejects`  — rows that failed validation, with the reason
//!
//! A bad row must never fail the job. In a real pipeline one corrupt row in a
//! billion should not cost the whole operation, so the mapper quarantines
//! instead of panicking.
//!
//! ## Reduce phase
//!
//! Groups by `user_id`, splits each user's events into sessions on a 30-minute
//! inactivity gap, and emits:
//!
//! - table 0 `sessions` — one row per session
//! - table 1 `users`    — one row per user, aggregating their sessions
//!
//! ```sh
//! scripts/build-worker.sh sessionize
//! # see tests/e2e/run_pilot.sh for the full operation invocation
//! ```

use serde::{Deserialize, Serialize};
use ytsaurus_job::yson::{YsonNode, YsonValue};
use ytsaurus_job::{DataFormat, WorkerEvent, WorkerReader, WorkerRow, WorkerWriter};
use ytsaurus_job::{Event, JobError, JobReader, JobWriter, Row, TableId};
use ytsaurus_skiff::{Format, Schema, SchemaRef, Value, WireType};

/// Inactivity gap that starts a new session, in microseconds.
const SESSION_GAP_US: i64 = 30 * 60 * 1_000_000;

/// A raw access-log event, as it arrives.
///
/// Deliberately wide and mixed-typed. `user_id` and `user_agent` are byte
/// columns: user agents in real logs are routinely not valid UTF-8, and a job
/// that declared them `String` would fail on those rows.
#[derive(Deserialize)]
struct RawEvent<'a> {
    #[serde(with = "serde_bytes", borrow)]
    user_id: &'a [u8],
    timestamp: i64,
    #[serde(borrow)]
    url: &'a str,
    #[serde(default, borrow)]
    referer: Option<&'a str>,
    #[serde(with = "serde_bytes", borrow)]
    user_agent: &'a [u8],
    status: i64,
    bytes_sent: u64,
    is_mobile: bool,
    latency_ms: f64,
}

/// A validated event, as handed to the reducer.
#[derive(Serialize, Deserialize)]
struct CleanEvent<'a> {
    #[serde(with = "serde_bytes", borrow)]
    user_id: &'a [u8],
    timestamp: i64,
    #[serde(borrow)]
    url: &'a str,
    #[serde(with = "serde_bytes", borrow)]
    user_agent: &'a [u8],
    status: i64,
    bytes_sent: u64,
    is_mobile: bool,
    latency_ms: f64,
    /// Whether the request arrived from another site.
    is_external: bool,
}

/// A row that failed validation, kept for inspection rather than dropped.
///
/// Borrows the offending row rather than copying it. The rejects path exists to
/// be cheap when a fraction of a huge input is corrupt, and `raw: Vec<u8>` would
/// copy every bad row for no reason — the value is serialized before the borrow
/// ends.
#[derive(Serialize)]
struct Reject<'a> {
    #[serde(with = "serde_bytes")]
    raw: &'a [u8],
    reason: &'a str,
    row_index: Option<i64>,
}

#[derive(Serialize)]
struct Session {
    #[serde(with = "serde_bytes")]
    user_id: Vec<u8>,
    session_index: i64,
    started_at: i64,
    ended_at: i64,
    duration_us: i64,
    hits: i64,
    bytes_sent: u64,
    errors: i64,
    is_mobile: bool,
    mean_latency_ms: f64,
    #[serde(with = "serde_bytes")]
    entry_url: Vec<u8>,
}

#[derive(Serialize)]
struct UserSummary {
    #[serde(with = "serde_bytes")]
    user_id: Vec<u8>,
    sessions: i64,
    hits: i64,
    bytes_sent: u64,
    errors: i64,
    total_duration_us: i64,
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    match mode.as_str() {
        "map" => ytsaurus_job::run(map),
        "reduce" => ytsaurus_job::run(reduce),
        // The same mapper, stopped early. Running all three over one table and
        // comparing what the scheduler charged each is how the cost of decoding
        // is separated from the cost of the work — see `examples/profile.rs` in
        // ytsaurus-client, and docs/benchmarking.md for what the answer is
        // worth.
        "map-frames" => ytsaurus_job::run(map_frames),
        "map-parse" => ytsaurus_job::run(map_parse),
        "map-one" => ytsaurus_job::run(map_one),
        "map-parse-dynamic" => ytsaurus_job::run(map_parse_dynamic),
        "map-one-dynamic" => ytsaurus_job::run(map_one_dynamic),
        "map-parse-skiff" => ytsaurus_job::run(map_parse_skiff),
        "map-one-skiff" => ytsaurus_job::run(map_one_skiff),
        other => {
            eprintln!(
                "usage: sessionize <map|map-one|map-one-dynamic|map-one-skiff|reduce|\
                 map-frames|map-parse|map-parse-dynamic|map-parse-skiff>   (got {other:?})"
            );
            std::process::exit(2);
        }
    }
}

/// The mapper with everything after framing removed.
///
/// Reads the stream and finds record boundaries, decoding nothing. This is the
/// floor: whatever a job cannot avoid paying to be handed its input.
fn map_frames() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut rows = 0u64;

    while let Some(event) = reader.next_event()? {
        if let Event::Row(row) = event {
            // Reads the slice's length, not its contents — and that is enough,
            // because `next_event` reads stdin and cannot be elided. An earlier
            // comment here claimed this "touches the bytes"; it does not, and
            // saying so mattered: this leg is the denominator's first bucket in
            // every decode-share number the harness prints.
            rows += row.raw().len() as u64 & 1;
        }
    }

    eprintln!("sessionize map-frames: {rows}");
    Ok(())
}

/// The mapper with everything after decoding removed.
///
/// Decodes each row into the same `RawEvent` the real mapper uses, then drops
/// it. The difference from `map-frames` is the cost of YSON decoding on this
/// job's actual rows, which is the number the Skiff question turns on.
fn map_parse() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut kept = 0u64;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };
        match row.parse::<RawEvent>() {
            Err(e) if !e.is_row_local() => return Err(e),
            Err(_) => {}
            Ok(event) => kept += event.timestamp as u64 & 1,
        }
    }

    eprintln!("sessionize map-parse: {kept}");
    Ok(())
}

/// The mapper with one output instead of two.
///
/// The comparison task of `docs/format-comparison.md`: read nine mixed-type
/// columns, validate, derive one, write the survivors — and **no shuffle**, so
/// nothing about plan shape can get into the measurement. That was the lesson
/// of the wordcount comparison, where the whole of a 1.8× gap turned out to be
/// a combiner.
///
/// One output rather than two for the same reason as the reduce is absent: a
/// second output descriptor is outside the single shape Skiff is
/// cluster-verified in (`docs/skiff-compatibility.md`, required test 4), and
/// the Skiff leg has to run the same job as this one or the legs are not
/// comparable. Bad rows are counted and dropped instead of quarantined — the
/// count goes to stderr, which the operation shows.
///
/// Together with [`map_frames`] and [`map_parse`] this is the deepest of three
/// stops over one table: frames, then decode, then the work. The differences
/// are what each layer costs.
fn map_one() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(1)?;

    let mut kept = 0u64;
    let mut rejected = 0u64;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };

        let clean = match row.parse::<RawEvent>() {
            Err(e) if !e.is_row_local() => return Err(e),
            Err(_) => {
                rejected += 1;
                continue;
            }
            Ok(event) => {
                if validate(&event).is_err() {
                    rejected += 1;
                    continue;
                }
                CleanEvent {
                    is_external: event
                        .referer
                        .is_some_and(|r| !r.is_empty() && !r.starts_with('/')),
                    user_id: event.user_id,
                    timestamp: event.timestamp,
                    url: event.url,
                    user_agent: event.user_agent,
                    status: event.status,
                    bytes_sent: event.bytes_sent,
                    is_mobile: event.is_mobile,
                    latency_ms: event.latency_ms,
                }
            }
        };

        writer.write(0, &clean)?;
        kept += 1;
    }

    eprintln!("sessionize map-one: kept {kept}, dropped {rejected}");
    writer.finish()
}

// ------------------------------------------------------------ the Skiff legs
//
// The same job again, on a positional wire format. Written out by hand because
// Skiff has no schema inference yet (`docs/skiff-compatibility.md` lists it as
// planned), and positionally because that is what the format is: there are no
// field names on the wire, so a column is wherever the schema says it is and
// nowhere else. Get the order wrong and the job reads a timestamp as a status
// without complaining — which is the trade this leg exists to price.
//
// There is deliberately no `map-frames-skiff`. A Skiff stream has no
// self-describing record boundaries: finding the end of a row *is* decoding it
// against the schema, so the frames-only stop that the YSON legs subtract
// cannot exist here. That asymmetry is a fact about the formats, not a gap in
// this file, and it is why the Skiff comparison is stop-to-stop against the
// dynamic YSON legs rather than a subtraction of its own.

/// The nine columns of the input table, in the order the schema fixes them.
///
/// `referer` is `optional`, which on the wire is `variant8<nothing; string32>`
/// — one tag byte on every row, present or not.
fn input_format() -> DataFormat {
    DataFormat::skiff(
        Format::new(vec![SchemaRef::Inline(Schema::tuple([
            Schema::named("user_id", WireType::String32),
            Schema::named("timestamp", WireType::Int64),
            Schema::named("url", WireType::String32),
            Schema::named("referer", WireType::String32).optional(),
            Schema::named("user_agent", WireType::String32),
            Schema::named("status", WireType::Int64),
            Schema::named("bytes_sent", WireType::Uint64),
            Schema::named("is_mobile", WireType::Boolean),
            Schema::named("latency_ms", WireType::Double),
        ]))])
        .expect("the input schema is a valid Skiff format"),
    )
}

/// What the mapper writes: the input's nine less `referer`, plus `is_external`.
fn output_format() -> DataFormat {
    DataFormat::skiff(
        Format::new(vec![SchemaRef::Inline(Schema::tuple([
            Schema::named("user_id", WireType::String32),
            Schema::named("timestamp", WireType::Int64),
            Schema::named("url", WireType::String32),
            Schema::named("user_agent", WireType::String32),
            Schema::named("status", WireType::Int64),
            Schema::named("bytes_sent", WireType::Uint64),
            Schema::named("is_mobile", WireType::Boolean),
            Schema::named("latency_ms", WireType::Double),
            Schema::named("is_external", WireType::Boolean),
        ]))])
        .expect("the output schema is a valid Skiff format"),
    )
}

/// Field positions in [`input_format`]. Named so the job reads like the others.
mod at {
    pub const USER_ID: usize = 0;
    pub const TIMESTAMP: usize = 1;
    pub const URL: usize = 2;
    pub const REFERER: usize = 3;
    pub const STATUS: usize = 5;
    pub const LATENCY_MS: usize = 8;
    // `user_agent`, `bytes_sent` and `is_mobile` are not named because nothing
    // reads them: they are validated by the schema on the way in and moved to
    // the output without being looked at, which is what a positional format
    // buys when a column only has to survive the trip.
}

/// [`map_parse_dynamic`]'s stop, on Skiff.
///
/// Decoding is not optional here — see the note above — so this measures the
/// whole read path against the dynamic YSON leg's, which is the comparison the
/// Skiff question actually needs.
fn map_parse_skiff() -> Result<(), JobError> {
    let mut reader = WorkerReader::from_stdin(input_format())?;
    let mut kept = 0u64;

    while let Some(event) = reader.next_event()? {
        let WorkerEvent::Skiff(row) = event else {
            unreachable!("the reader was configured for Skiff");
        };
        if let Value::Tuple(fields) = row.value()
            && let Some(Value::Int64(timestamp)) = fields.get(at::TIMESTAMP)
        {
            kept += *timestamp as u64 & 1;
        }
    }

    eprintln!("sessionize map-parse-skiff: {kept}");
    Ok(())
}

/// [`map_one`] on Skiff: same five rules, same derived column, positional.
fn map_one_skiff() -> Result<(), JobError> {
    let mut reader = WorkerReader::from_stdin(input_format())?;
    let mut writer = WorkerWriter::descriptors(output_format(), 1)?;

    let mut kept = 0u64;
    let mut rejected = 0u64;

    while let Some(event) = reader.next_event()? {
        let WorkerEvent::Skiff(row) = event else {
            unreachable!("the reader was configured for Skiff");
        };
        // `into_value` rather than `value`: the row is already owned, and
        // taking it lets the output tuple be built by moving the fields
        // instead of cloning eight of them. Measured off-cluster, the clones
        // this replaces were 3 allocations a row and 12 % of the leg — a
        // self-inflicted handicap that made Skiff look worse than it is.
        let Value::Tuple(mut fields) = row.into_value() else {
            rejected += 1;
            continue;
        };

        let (
            Some(Value::Bytes(user_id)),
            Some(Value::Int64(timestamp)),
            Some(Value::Bytes(url)),
            Some(Value::Int64(status)),
            Some(Value::Double(latency_ms)),
        ) = (
            fields.get(at::USER_ID),
            fields.get(at::TIMESTAMP),
            fields.get(at::URL),
            fields.get(at::STATUS),
            fields.get(at::LATENCY_MS),
        )
        else {
            rejected += 1;
            continue;
        };

        if user_id.is_empty()
            || *timestamp <= 0
            || !(100..=599).contains(status)
            || !latency_ms.is_finite()
            || *latency_ms < 0.0
            || url.is_empty()
        {
            rejected += 1;
            continue;
        }

        // An absent optional arrives as the nothing arm of the variant.
        let is_external = match fields.get(at::REFERER) {
            Some(Value::Variant { value, .. }) => match value.as_ref() {
                Value::Bytes(referer) => !referer.is_empty() && !referer.starts_with(b"/"),
                _ => false,
            },
            _ => false,
        };

        // The output schema is the input's less `referer`, plus `is_external`
        // — which is exactly this: drop the optional, append the derived. A
        // `remove` shifts five elements and allocates nothing.
        fields.remove(at::REFERER);
        fields.push(Value::Boolean(is_external));
        let clean = Value::Tuple(fields);

        writer.write(0, WorkerRow::Skiff(&clean))?;
        kept += 1;
    }

    eprintln!("sessionize map-one-skiff: kept {kept}, dropped {rejected}");
    writer.finish()
}

// ------------------------------------------------- the dynamic control legs
//
// The same two stops again, through `YsonValue` instead of a typed struct.
//
// They exist to make a format comparison mean something. Skiff's job API has no
// typed rows yet — `docs/skiff-compatibility.md` lists them as planned — so a
// Skiff leg can only be dynamic, and putting it against the typed YSON legs
// would compare two APIs while calling it a comparison of two formats. These
// are the control: same format as the typed legs, same API level as the Skiff
// one. `docs/benchmarking.md` already warns not to compare Skiff's dynamic
// result with YSON's borrowed-serde one; this is how that warning is obeyed on
// a cluster rather than in a benchmark harness.

/// A dynamic value carrying no attributes.
///
/// There is no builder for these, which is part of what the control measures:
/// the dynamic path costs a `BTreeMap` per row and this much ceremony per
/// field, against a `#[derive(Serialize)]` on the typed side.
fn node(node: YsonNode) -> YsonValue {
    YsonValue {
        attributes: None,
        node,
    }
}

/// A field of a dynamic row, without the panic `Index` gives on a missing key.
fn field<'row>(row: &'row YsonValue, key: &str) -> Option<&'row YsonValue> {
    match &row.node {
        YsonNode::Map(fields) => fields.get(key.as_bytes()),
        _ => None,
    }
}

fn bytes_of<'row>(row: &'row YsonValue, key: &str) -> Option<&'row [u8]> {
    match &field(row, key)?.node {
        YsonNode::String(bytes) => Some(bytes),
        _ => None,
    }
}

fn int_of(row: &YsonValue, key: &str) -> Option<i64> {
    match &field(row, key)?.node {
        YsonNode::Int64(value) => Some(*value),
        _ => None,
    }
}

/// [`map_parse`], decoding into a `YsonValue` DOM instead of a struct.
fn map_parse_dynamic() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut kept = 0u64;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };
        let value: YsonValue = row.value()?;
        // Touch one field, so the DOM cannot be built and discarded unread.
        kept += int_of(&value, "timestamp").unwrap_or(0) as u64 & 1;
    }

    eprintln!("sessionize map-parse-dynamic: {kept}");
    Ok(())
}

/// [`map_one`] through the dynamic value type, start to finish.
///
/// Reads a DOM, validates by walking it, and builds another DOM to write. No
/// serde on either side — which is the point: this is what a job author writes
/// when the format has no typed rows.
fn map_one_dynamic() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let mut writer = JobWriter::descriptors(1)?;

    let mut kept = 0u64;
    let mut rejected = 0u64;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };

        let value: YsonValue = match row.value() {
            Ok(value) => value,
            Err(e) if e.is_row_local() => {
                rejected += 1;
                continue;
            }
            Err(e) => return Err(e),
        };

        let (Some(user_id), Some(timestamp), Some(url), Some(status)) = (
            bytes_of(&value, "user_id"),
            int_of(&value, "timestamp"),
            bytes_of(&value, "url"),
            int_of(&value, "status"),
        ) else {
            rejected += 1;
            continue;
        };
        let latency = match &field(&value, "latency_ms").map(|v| &v.node) {
            Some(YsonNode::Double(latency)) => *latency,
            _ => {
                rejected += 1;
                continue;
            }
        };

        // The same five rules as `validate`, spelled against the DOM.
        if user_id.is_empty()
            || timestamp <= 0
            || !(100..=599).contains(&status)
            || !latency.is_finite()
            || latency < 0.0
            || url.is_empty()
        {
            rejected += 1;
            continue;
        }

        let is_external = match bytes_of(&value, "referer") {
            Some(referer) => !referer.is_empty() && !referer.starts_with(b"/"),
            None => false,
        };

        let mut out = std::collections::BTreeMap::new();
        for key in [
            "user_id",
            "timestamp",
            "url",
            "user_agent",
            "status",
            "bytes_sent",
            "is_mobile",
            "latency_ms",
        ] {
            let Some(field) = field(&value, key) else {
                rejected += 1;
                continue;
            };
            out.insert(key.as_bytes().to_vec(), field.clone());
        }
        out.insert(
            b"is_external".to_vec(),
            node(YsonNode::Boolean(is_external)),
        );

        writer.write(0, &node(YsonNode::Map(out)))?;
        kept += 1;
    }

    eprintln!("sessionize map-one-dynamic: kept {kept}, dropped {rejected}");
    writer.finish()
}

/// Why a row is unusable. Returning a reason rather than a bool means the
/// rejects table explains itself.
fn validate(event: &RawEvent<'_>) -> Result<(), &'static str> {
    if event.user_id.is_empty() {
        return Err("empty user_id");
    }
    if event.timestamp <= 0 {
        return Err("non-positive timestamp");
    }
    if !(100..=599).contains(&event.status) {
        return Err("status outside 100..=599");
    }
    if !event.latency_ms.is_finite() || event.latency_ms < 0.0 {
        return Err("latency_ms is negative or not finite");
    }
    if event.url.is_empty() {
        return Err("empty url");
    }
    Ok(())
}

fn map() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let (mut writer, [events, rejects]) = JobWriter::named(["events", "rejects"])?;

    let mut kept = 0u64;
    let mut rejected = 0u64;

    while let Some(event) = reader.next_event()? {
        let Event::Row(row) = event else { continue };

        // Both failure modes now yield a cheap, stable `&'static str`:
        // `JobError::kind` for a decode failure, our own reason for an invalid
        // row. Nothing is formatted and nothing is allocated per bad row, and
        // the values are stable enough to group a rejects table by.
        //
        // `is_row_local` decides whether to carry on: a bad row is quarantined,
        // but a truncated stream or a failed write means every later row is
        // suspect, so the job stops.
        let outcome: Result<CleanEvent, &'static str> = match row.parse::<RawEvent>() {
            Err(e) if !e.is_row_local() => return Err(e),
            Err(e) => Err(e.kind()),
            Ok(event) => (|| {
                validate(&event)?;
                Ok(CleanEvent {
                    is_external: event
                        .referer
                        .is_some_and(|r| !r.is_empty() && !r.starts_with('/')),
                    user_id: event.user_id,
                    timestamp: event.timestamp,
                    url: event.url,
                    user_agent: event.user_agent,
                    status: event.status,
                    bytes_sent: event.bytes_sent,
                    is_mobile: event.is_mobile,
                    latency_ms: event.latency_ms,
                })
            })(),
        };

        match outcome {
            Ok(clean) => {
                writer.write(events, &clean)?;
                kept += 1;
            }
            Err(reason) => {
                quarantine(&mut writer, rejects, &row, reason)?;
                rejected += 1;
            }
        }
    }

    // stderr shows up in the operation UI, so this is the job explaining itself.
    eprintln!("sessionize map: kept {kept}, rejected {rejected}");
    writer.finish()
}

/// Writes a bad row to the rejects table, preserving the original bytes.
fn quarantine(
    writer: &mut JobWriter,
    rejects: TableId,
    row: &Row<'_>,
    reason: &str,
) -> Result<(), JobError> {
    writer.write(
        rejects,
        &Reject {
            raw: row.raw(),
            reason,
            row_index: row.row_index,
        },
    )
}

fn reduce() -> Result<(), JobError> {
    let mut reader = JobReader::from_stdin();
    let (mut writer, [sessions_table, users_table]) = JobWriter::named(["sessions", "users"])?;

    let mut groups = reader.groups_by(["user_id"]);
    let mut users = 0u64;
    let mut sessions_emitted = 0u64;

    while let Some(mut group) = groups.next_group()? {
        // The reduce key comes from the group rather than being re-derived
        // from the first row.
        let user_id = group.key().bytes("user_id").unwrap_or_default().to_vec();

        let mut current: Option<SessionAcc> = None;
        let mut finished: Vec<Session> = Vec::new();

        while let Some(row) = group.next_row()? {
            let event: CleanEvent = row.parse()?;

            // A gap longer than the threshold closes the current session.
            let starts_new_session = match &current {
                Some(acc) => event.timestamp - acc.ended_at > SESSION_GAP_US,
                None => true,
            };

            if starts_new_session {
                if let Some(acc) = current.take() {
                    finished.push(acc.finish(finished.len() as i64));
                }
                current = Some(SessionAcc::start(&event));
            } else if let Some(acc) = &mut current {
                acc.push(&event);
            }
        }

        if let Some(acc) = current {
            finished.push(acc.finish(finished.len() as i64));
        }

        let mut summary = UserSummary {
            user_id: user_id.clone(),
            sessions: finished.len() as i64,
            hits: 0,
            bytes_sent: 0,
            errors: 0,
            total_duration_us: 0,
        };

        for session in &finished {
            summary.hits += session.hits;
            summary.bytes_sent += session.bytes_sent;
            summary.errors += session.errors;
            summary.total_duration_us += session.duration_us;
            writer.write(sessions_table, session)?;
            sessions_emitted += 1;
        }

        writer.write(users_table, &summary)?;
        users += 1;
    }

    eprintln!("sessionize reduce: {users} users, {sessions_emitted} sessions");
    writer.finish()
}

/// A session being accumulated.
///
/// Owned rather than borrowed: it outlives the row it started from, which is
/// exactly the case the zero-copy reader cannot serve.
struct SessionAcc {
    user_id: Vec<u8>,
    started_at: i64,
    ended_at: i64,
    hits: i64,
    bytes_sent: u64,
    errors: i64,
    is_mobile: bool,
    latency_total: f64,
    entry_url: Vec<u8>,
}

impl SessionAcc {
    fn start(event: &CleanEvent<'_>) -> Self {
        Self {
            user_id: event.user_id.to_vec(),
            started_at: event.timestamp,
            ended_at: event.timestamp,
            hits: 1,
            bytes_sent: event.bytes_sent,
            errors: i64::from(event.status >= 400),
            is_mobile: event.is_mobile,
            latency_total: event.latency_ms,
            entry_url: event.url.as_bytes().to_vec(),
        }
    }

    fn push(&mut self, event: &CleanEvent<'_>) {
        // Events within a group are not guaranteed sorted unless the operation
        // sorts by timestamp, so track the extremes rather than assuming order.
        self.started_at = self.started_at.min(event.timestamp);
        self.ended_at = self.ended_at.max(event.timestamp);
        self.hits += 1;
        self.bytes_sent += event.bytes_sent;
        self.errors += i64::from(event.status >= 400);
        self.latency_total += event.latency_ms;
        self.is_mobile |= event.is_mobile;
    }

    fn finish(self, index: i64) -> Session {
        Session {
            user_id: self.user_id,
            session_index: index,
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_us: self.ended_at - self.started_at,
            hits: self.hits,
            bytes_sent: self.bytes_sent,
            errors: self.errors,
            is_mobile: self.is_mobile,
            mean_latency_ms: self.latency_total / self.hits as f64,
            entry_url: self.entry_url,
        }
    }
}

/// Unused today, but the shape a per-user cache would take. Kept out of the hot
/// path deliberately: see FRICTION 3 in the friction log.
#[allow(dead_code)]
#[cfg(test)]
mod tests {
    use super::*;

    fn event(ts: i64, status: i64) -> CleanEvent<'static> {
        CleanEvent {
            user_id: b"u1",
            timestamp: ts,
            url: "/a",
            user_agent: b"agent",
            status,
            bytes_sent: 10,
            is_mobile: false,
            latency_ms: 1.0,
            is_external: false,
        }
    }

    #[test]
    fn a_gap_longer_than_the_threshold_splits_the_session() {
        let mut acc = SessionAcc::start(&event(0, 200));
        acc.push(&event(SESSION_GAP_US - 1, 200));
        let s = acc.finish(0);
        assert_eq!(s.hits, 2);
        assert_eq!(s.duration_us, SESSION_GAP_US - 1);
    }

    #[test]
    fn errors_are_counted_from_status() {
        let mut acc = SessionAcc::start(&event(0, 200));
        acc.push(&event(1, 404));
        acc.push(&event(2, 500));
        acc.push(&event(3, 302));
        assert_eq!(acc.finish(0).errors, 2);
    }

    #[test]
    fn out_of_order_events_still_bound_the_session() {
        let mut acc = SessionAcc::start(&event(100, 200));
        acc.push(&event(50, 200));
        acc.push(&event(150, 200));
        let s = acc.finish(0);
        assert_eq!(s.started_at, 50);
        assert_eq!(s.ended_at, 150);
        assert_eq!(s.duration_us, 100);
    }

    #[test]
    fn mean_latency_is_averaged_over_hits() {
        let mut acc = SessionAcc::start(&event(0, 200));
        acc.push(&event(1, 200));
        acc.push(&event(2, 200));
        assert!((acc.finish(0).mean_latency_ms - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn validation_rejects_what_it_should() {
        let ok = RawEvent {
            user_id: b"u",
            timestamp: 1,
            url: "/x",
            referer: None,
            user_agent: b"a",
            status: 200,
            bytes_sent: 1,
            is_mobile: false,
            latency_ms: 1.0,
        };
        assert!(validate(&ok).is_ok());

        let cases: [(RawEvent, &str); 5] = [
            (
                RawEvent {
                    user_id: b"",
                    ..ok_like()
                },
                "empty user_id",
            ),
            (
                RawEvent {
                    timestamp: 0,
                    ..ok_like()
                },
                "non-positive timestamp",
            ),
            (
                RawEvent {
                    status: 999,
                    ..ok_like()
                },
                "status outside 100..=599",
            ),
            (
                RawEvent {
                    latency_ms: f64::NAN,
                    ..ok_like()
                },
                "latency_ms is negative or not finite",
            ),
            (
                RawEvent {
                    url: "",
                    ..ok_like()
                },
                "empty url",
            ),
        ];
        for (event, expected) in cases {
            assert_eq!(validate(&event), Err(expected));
        }
    }

    fn ok_like() -> RawEvent<'static> {
        RawEvent {
            user_id: b"u",
            timestamp: 1,
            url: "/x",
            referer: None,
            user_agent: b"a",
            status: 200,
            bytes_sent: 1,
            is_mobile: false,
            latency_ms: 1.0,
        }
    }
}
