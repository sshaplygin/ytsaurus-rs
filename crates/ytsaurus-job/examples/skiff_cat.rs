//! `skiff_cat` — a dynamic Skiff identity mapper.
//!
//! It is deliberately small: one `string32` column is read and written with
//! the same validated schema, including non-UTF-8 bytes. The accompanying
//! `ytsaurus-client` `skiff_launch` example creates the matching table,
//! selects Skiff for the map operation and checks the decoded result.

use ytsaurus_job::{DataFormat, WorkerEvent, WorkerReader, WorkerRow, WorkerWriter};
use ytsaurus_skiff::{Format, Schema, SchemaRef, WireType};

/// The one-column dynamic Skiff table format this worker reads and writes.
///
/// `value` is deliberately `string32`, so the offline and cluster examples
/// prove that arbitrary — not merely UTF-8 — bytes survive the worker path.
///
/// Written out here rather than shared with `tests/skiff_cat_e2e.rs`, which
/// declares the same schema itself. A test that took its fixture from the code
/// under test would agree with it by construction; two independent spellings
/// have to agree with each other, which is the same reason `run_e2e.sh` reads
/// the tables back with the official Python client.
fn skiff_passthrough_format() -> Format {
    Format::new(vec![SchemaRef::Inline(Schema::tuple([Schema::named(
        "value",
        WireType::String32,
    )]))])
    .expect("the fixed skiff_cat table schema is valid")
}

fn main() {
    ytsaurus_job::run(|| {
        let format = DataFormat::skiff(skiff_passthrough_format());
        let mut reader = WorkerReader::from_stdin(format.clone())?;
        let mut writer = WorkerWriter::descriptors(format, 1)?;

        while let Some(event) = reader.next_event()? {
            let WorkerEvent::Skiff(row) = event else {
                unreachable!("the reader was configured for Skiff");
            };
            writer.write(0, WorkerRow::Skiff(row.value()))?;
        }

        writer.finish()
    });
}
