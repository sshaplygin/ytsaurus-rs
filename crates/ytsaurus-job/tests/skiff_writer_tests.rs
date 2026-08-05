use std::{cell::RefCell, io::Write, rc::Rc};

use ytsaurus_job::{JobError, SkiffJobWriter};
use ytsaurus_skiff::{Decoder, Format, Schema, SchemaRef, Value, WireType};

#[derive(Clone, Default)]
struct SharedBuffer(Rc<RefCell<Vec<u8>>>);

impl Write for SharedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn counts() -> Schema {
    Schema::tuple([Schema::named("count", WireType::Uint64)])
}

fn labels() -> Schema {
    Schema::tuple([Schema::named("label", WireType::String32)])
}

fn format(schemas: impl IntoIterator<Item = Schema>) -> Format {
    Format::new(schemas.into_iter().map(SchemaRef::Inline).collect()).unwrap()
}

#[test]
fn routes_each_output_to_its_own_single_table_skiff_stream() {
    let counts = counts();
    let labels = labels();
    let first = SharedBuffer::default();
    let second = SharedBuffer::default();
    let mut writer = SkiffJobWriter::from_writers(
        vec![Box::new(first.clone()), Box::new(second.clone())],
        format([counts.clone(), labels.clone()]),
    )
    .unwrap();

    writer
        .write(0, &Value::Tuple(vec![Value::Uint64(7)]))
        .unwrap();
    writer
        .write(1, &Value::Tuple(vec![Value::Bytes(b"ok".to_vec())]))
        .unwrap();
    writer.finish().unwrap();

    let first_bytes = first.0.borrow().clone();
    let mut first_decoder = Decoder::new(first_bytes.as_slice(), format([counts]));
    assert_eq!(
        first_decoder.next_row().unwrap(),
        Some((0, Value::Tuple(vec![Value::Uint64(7)])))
    );
    let second_bytes = second.0.borrow().clone();
    let mut second_decoder = Decoder::new(second_bytes.as_slice(), format([labels]));
    assert_eq!(
        second_decoder.next_row().unwrap(),
        Some((0, Value::Tuple(vec![Value::Bytes(b"ok".to_vec())])))
    );
}

#[test]
fn rejects_input_only_system_fields_in_an_output_format() {
    let schema = Schema::tuple([Schema::named("$key_switch", WireType::Boolean)]);

    assert!(matches!(
        SkiffJobWriter::from_writers(vec![Box::new(SharedBuffer::default())], format([schema])),
        Err(JobError::BadSkiffSchema { table: 0, .. })
    ));
}
