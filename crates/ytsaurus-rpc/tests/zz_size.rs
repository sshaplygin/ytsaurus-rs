#[test]
fn sizes() {
    println!(
        "Option<Bytes> = {}, Bytes = {}",
        std::mem::size_of::<Option<bytes::Bytes>>(),
        std::mem::size_of::<bytes::Bytes>()
    );
    let limit: u64 = 512 * 1024 * 1024;
    let part_count = (limit - 36 - 8) / 12;
    println!(
        "part_count {part_count}: sizes {} MB, checksums {} MB, parts {} MB",
        part_count * 4 / 1_048_576,
        part_count * 8 / 1_048_576,
        part_count * std::mem::size_of::<Option<bytes::Bytes>>() as u64 / 1_048_576
    );
}
