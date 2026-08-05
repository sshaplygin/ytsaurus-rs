//! Dynamic Skiff map launched entirely through `ytsaurus-client`.
//!
//! ```sh
//! ./scripts/build-worker.sh skiff_cat
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example skiff_launch
//! ```

use ytsaurus_client::{
    Client, DataFormat, MapSpec, SkiffFormat, SkiffSchema, SkiffSchemaRef, SkiffWireType,
};
use ytsaurus_skiff::{Decoder, Encoder, Value};

const BASE: &str = "//tmp/ytsaurus_rs_skiff_example";
const WORKER: &str = "target/x86_64-unknown-linux-musl/release-worker/skiff_cat";

fn main() -> Result<(), ytsaurus_client::ClientError> {
    let client = Client::from_env()?;
    let format = table_format();
    let rows = [
        Value::Tuple(vec![Value::Bytes(b"hello".to_vec())]),
        Value::Tuple(vec![Value::Bytes(vec![0, 0xff, b'x'])]),
    ];
    let stream = encode_rows(&format, &rows);

    // remove_tree, as every other example does: force, so the first run does
    // not fail on a path that is not there yet, and recursive, so a second run
    // does not fail on the non-empty map_node the first one left.
    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &format!("{BASE}/input"))?;
    client.create("table", &format!("{BASE}/output"))?;
    client.upload_worker(WORKER, &format!("{BASE}/skiff_cat"))?;
    let data_format = DataFormat::skiff(format.clone());
    client.write_table_with_format(format!("{BASE}/input"), &stream, &data_format)?;

    let spec = MapSpec::new(
        "./skiff_cat",
        [format!("{BASE}/input")],
        [format!("{BASE}/output")],
    )
    .with_local_file(format!("{BASE}/skiff_cat"))
    .with_memory_limit(512 * 1024 * 1024)
    .with_formats(data_format.clone(), data_format.clone());
    let operation = client.start_map(&spec)?;
    client.wait_for_operation(&operation)?;

    let output = client.read_table_with_format(&format!("{BASE}/output"), &data_format)?;
    let mut decoder = Decoder::new(output.as_slice(), format);
    for row in &rows {
        let Some((table, decoded)) =
            decoder
                .next_row()
                .map_err(|error| ytsaurus_client::ClientError::Decode {
                    command: "read_skiff_table".to_owned(),
                    reason: error.to_string(),
                })?
        else {
            return Err(ytsaurus_client::ClientError::Decode {
                command: "read_skiff_table".to_owned(),
                reason: "Skiff output ended before all rows arrived".to_owned(),
            });
        };
        if table != 0 || decoded != *row {
            return Err(ytsaurus_client::ClientError::Decode {
                command: "read_skiff_table".to_owned(),
                reason: format!("unexpected Skiff output row for table {table}: {decoded:?}"),
            });
        }
    }
    if decoder
        .next_row()
        .map_err(|error| ytsaurus_client::ClientError::Decode {
            command: "read_skiff_table".to_owned(),
            reason: error.to_string(),
        })?
        .is_some()
    {
        return Err(ytsaurus_client::ClientError::Decode {
            command: "read_skiff_table".to_owned(),
            reason: "Skiff output contains unexpected extra rows".to_owned(),
        });
    }

    println!("Skiff map succeeded: {} rows", rows.len());
    Ok(())
}

fn table_format() -> SkiffFormat {
    SkiffFormat::new(vec![SkiffSchemaRef::Inline(SkiffSchema::tuple([
        SkiffSchema::named("value", SkiffWireType::String32),
    ]))])
    .expect("the fixed example schema is valid")
}

fn encode_rows(format: &SkiffFormat, rows: &[Value]) -> Vec<u8> {
    let schema = format
        .table_schema(0)
        .expect("format has table zero")
        .clone();
    let mut encoder = Encoder::new(Vec::new(), schema).expect("schema is valid");
    for row in rows {
        encoder.write(row).expect("row matches fixed schema");
    }
    encoder.into_inner().expect("flushes in-memory stream")
}
