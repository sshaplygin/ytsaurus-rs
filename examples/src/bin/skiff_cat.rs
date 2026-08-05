//! `skiff_cat` — a dynamic Skiff identity mapper.
//!
//! It is deliberately small: one `string32` column is read and written with
//! the same validated schema, including non-UTF-8 bytes. The accompanying
//! `ytsaurus-client` `skiff_launch` example creates the matching table,
//! selects Skiff for the map operation and checks the decoded result.

use ytsaurus_examples::skiff_passthrough_format;
use ytsaurus_job::{DataFormat, WorkerEvent, WorkerReader, WorkerRow, WorkerWriter};

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
