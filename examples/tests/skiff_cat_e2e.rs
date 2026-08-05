use std::{
    io::Write,
    process::{Command, Stdio},
};

use ytsaurus_examples::skiff_passthrough_format;
use ytsaurus_skiff::{Decoder, Encoder, Value};

#[test]
fn dynamic_skiff_worker_round_trips_non_utf8_rows() {
    let rows = [
        Value::Tuple(vec![Value::Bytes(b"plain".to_vec())]),
        Value::Tuple(vec![Value::Bytes(vec![0, 0xff, b'x'])]),
    ];
    let schema = skiff_passthrough_format().table_schema(0).unwrap().clone();
    let mut encoder = Encoder::new(Vec::new(), schema).unwrap();
    for row in &rows {
        encoder.write(row).unwrap();
    }
    let input = encoder.into_inner().unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_skiff_cat"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("starts the real worker binary");
    child
        .stdin
        .take()
        .expect("worker has stdin")
        .write_all(&input)
        .expect("writes Skiff input");
    let output = child.wait_with_output().expect("waits for the worker");

    assert!(
        output.status.success(),
        "worker stderr: {:?}",
        output.stderr
    );
    let mut decoder = Decoder::new(output.stdout.as_slice(), skiff_passthrough_format());
    for row in rows {
        assert_eq!(decoder.next_row().unwrap(), Some((0, row)));
    }
    assert_eq!(decoder.next_row().unwrap(), None);
}
