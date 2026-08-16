//! End-to-end test of the `cat` worker, minus the cluster.
//!
//! This runs the **actual compiled worker binary** the way YTsaurus runs it:
//! job input on fd 0, output table 0 on fd 1, output table 1 on fd 4.
//!
//! The fixtures are **captured from a real YTsaurus cluster** by
//! `tests/cluster-e2e/capture_fixtures.sh` — `cat_input.bin` is literally the byte
//! stream a job was handed on fd 0, and the expected outputs are what the
//! cluster returns for those tables. So this is not a test of our reading of
//! the specification against itself; the bytes came from YTsaurus.
//!
//! What it still cannot cover is the operation plumbing — scheduling, retries,
//! how a failing job is reported. `tests/cluster-e2e/run_e2e.sh` covers that against a
//! live cluster.

use std::path::{Path, PathBuf};
use std::process::Command;

mod common;

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/cluster-e2e/fixtures"
);

const INPUT: &[u8] = include_bytes!("../../../tests/cluster-e2e/fixtures/cat_input.bin");
const EXPECTED_TABLE_0: &[u8] =
    include_bytes!("../../../tests/cluster-e2e/fixtures/cat_expected_table_0.bin");
const EXPECTED_TABLE_1: &[u8] =
    include_bytes!("../../../tests/cluster-e2e/fixtures/cat_expected_table_1.bin");
const EXPECTED_SINGLE: &[u8] =
    include_bytes!("../../../tests/cluster-e2e/fixtures/cat_expected_single.bin");

/// A scratch directory that cleans up after itself.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ytsaurus-rs-e2e-{}-{tag}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs the worker with the job's descriptor layout.
///
/// `bash` does the descriptor wiring, because that is precisely what the cluster
/// does for a real job and it keeps the test honest about fd numbering: table 1
/// really does have to arrive on fd 4.
fn run_cat(input: &Path, outputs: &[PathBuf], extra_args: &str) -> std::process::Output {
    let mut redirects = String::new();
    for (i, out) in outputs.iter().enumerate() {
        let fd = 3 * i + 1;
        redirects.push_str(&format!(" {fd}>{:?}", out.display().to_string()));
    }

    let script = format!(
        "{:?} {extra_args} <{:?}{redirects}",
        common::example("cat").display(),
        input.display().to_string()
    );

    Command::new("bash")
        .arg("-c")
        .arg(&script)
        .output()
        .unwrap_or_else(|e| panic!("failed to run: {script}\n{e}"))
}

fn write_input(dir: &TempDir) -> PathBuf {
    let path = dir.join("input.bin");
    std::fs::write(&path, INPUT).expect("write input");
    path
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Reports the first differing byte, which is far more useful than a 300 KB
/// assertion dump.
fn assert_bytes_eq(actual: &[u8], expected: &[u8], what: &str) {
    if actual == expected {
        return;
    }
    let at = actual
        .iter()
        .zip(expected)
        .position(|(a, b)| a != b)
        .unwrap_or(actual.len().min(expected.len()));
    let from = at.saturating_sub(24);
    panic!(
        "{what} differs\n  first difference at byte {at}\n  \
         actual len {} expected len {}\n  actual   ..{:02x?}\n  expected ..{:02x?}",
        actual.len(),
        expected.len(),
        &actual[from..(at + 24).min(actual.len())],
        &expected[from..(at + 24).min(expected.len())],
    );
}

/// The headline check: with two output tables, every row must come back
/// byte-for-byte, routed by its input table index.
#[test]
fn cat_reproduces_its_input_across_two_tables() {
    let dir = TempDir::new("two-tables");
    let input = write_input(&dir);
    let outputs = vec![dir.join("table0.bin"), dir.join("table1.bin")];

    let output = run_cat(&input, &outputs, "--tables 2");
    assert!(
        output.status.success(),
        "cat failed: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    assert_bytes_eq(
        &read(&outputs[0]),
        EXPECTED_TABLE_0,
        "output table 0 (fd 1)",
    );
    assert_bytes_eq(
        &read(&outputs[1]),
        EXPECTED_TABLE_1,
        "output table 1 (fd 4)",
    );
}

/// With a single output table, everything lands in table 0 in stream order.
#[test]
fn cat_reproduces_its_input_on_one_table() {
    let dir = TempDir::new("one-table");
    let input = write_input(&dir);
    let outputs = vec![dir.join("table0.bin")];

    let output = run_cat(&input, &outputs, "");
    assert!(
        output.status.success(),
        "cat failed: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    assert_bytes_eq(&read(&outputs[0]), EXPECTED_SINGLE, "output table 0");
}

/// Empty input is a normal case — YTsaurus starts jobs for empty chunks — and
/// must produce an empty table rather than an error.
#[test]
fn cat_handles_empty_input() {
    let dir = TempDir::new("empty");
    let input = dir.join("empty.bin");
    std::fs::write(&input, b"").expect("write");
    let outputs = vec![dir.join("table0.bin")];

    let output = run_cat(&input, &outputs, "");
    assert!(
        output.status.success(),
        "cat failed on empty input: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(read(&outputs[0]).is_empty(), "empty input produced output");
}

/// A truncated stream must fail the job loudly. YTsaurus judges a job by its
/// exit code, so silently succeeding on a short read would publish a table
/// missing rows.
#[test]
fn cat_fails_on_truncated_input() {
    let dir = TempDir::new("truncated");
    let input = dir.join("truncated.bin");
    std::fs::write(&input, &INPUT[..INPUT.len() - 32]).expect("write");
    let outputs = vec![dir.join("table0.bin"), dir.join("table1.bin")];

    let output = run_cat(&input, &outputs, "--tables 2");

    assert!(
        !output.status.success(),
        "truncated input must fail the job, but it exited successfully"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("the job failed"),
        "stderr should explain the failure so it is visible in the operation UI, got: {stderr}"
    );
}

/// The uploaded table payloads must still match what the generator produces, so
/// they cannot drift from the script that documents them.
///
/// This checks `table_rows_*.bin` only. `cat_input.bin` is a capture from a real
/// cluster and has no generator to compare against — `capture_fixtures.sh`
/// refreshes it, and the consistency check below stands in for a diff.
#[test]
fn table_payloads_are_reproducible() {
    let script = Path::new(FIXTURES)
        .parent()
        .unwrap()
        .join("generate_fixtures.py");
    if !script.exists() {
        return;
    }

    let Ok(python) = which_python() else {
        eprintln!("python3 not available; skipping fixture reproducibility check");
        return;
    };

    let before: Vec<Vec<u8>> = (0..2)
        .map(|i| read(&Path::new(FIXTURES).join(format!("table_rows_{i}.bin"))))
        .collect();

    let output = Command::new(python)
        .arg(&script)
        .output()
        .expect("run generator");
    assert!(
        output.status.success(),
        "generator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    for (i, expected) in before.iter().enumerate() {
        assert_bytes_eq(
            &read(&Path::new(FIXTURES).join(format!("table_rows_{i}.bin"))),
            expected,
            &format!("regenerated table_rows_{i}.bin"),
        );
    }
}

/// The captured job input and the captured table contents must agree: every row
/// the cluster returns for a table has to appear verbatim in what a job was
/// handed. If that stops holding, the fixtures were captured from different
/// data and the tests above would be checking the wrong thing.
#[test]
fn captured_fixtures_are_mutually_consistent() {
    for (i, expected) in [EXPECTED_TABLE_0, EXPECTED_TABLE_1].into_iter().enumerate() {
        assert!(
            INPUT
                .windows(expected.len())
                .any(|window| window == expected),
            "table {i} rows do not appear verbatim in the captured job input"
        );
    }

    // The single-table expectation is just both tables in stream order.
    let mut both = EXPECTED_TABLE_0.to_vec();
    both.extend_from_slice(EXPECTED_TABLE_1);
    assert_bytes_eq(EXPECTED_SINGLE, &both, "cat_expected_single.bin");
}

/// The capture must actually contain the control records the job runtime is
/// built to understand — otherwise the offline test proves nothing about them.
#[test]
fn captured_input_contains_real_control_records() {
    // `<table_index=...;>#` as YTsaurus emits it: 0x01 string marker, zigzag
    // length 22 for an 11-byte name, and note the `;` *inside* the attribute
    // block, which a naive reading of the grammar would omit.
    let mut marker = vec![b'<', 0x01, 0x16];
    marker.extend_from_slice(b"table_index");
    assert!(
        INPUT.windows(marker.len()).any(|w| w == marker),
        "captured input has no table_index control record"
    );

    let mut marker = vec![b'<', 0x01, 0x12];
    marker.extend_from_slice(b"row_index");
    assert!(
        INPUT.windows(marker.len()).any(|w| w == marker),
        "captured input has no row_index control record"
    );

    let mut marker = vec![b'<', 0x01, 0x16];
    marker.extend_from_slice(b"range_index");
    assert!(
        INPUT.windows(marker.len()).any(|w| w == marker),
        "captured input has no range_index control record"
    );

    // The trailing `;` inside the attribute block is the detail the synthetic
    // fixture got wrong; pin it so a future regeneration cannot lose it.
    let mut framed = vec![b'<', 0x01, 0x16];
    framed.extend_from_slice(b"table_index");
    framed.extend_from_slice(&[b'=', 0x02, 0x00, b';', b'>', b'#']);
    assert!(
        INPUT.windows(framed.len()).any(|w| w == framed),
        "expected `<table_index=0;>#` with the trailing semicolon YTsaurus emits"
    );
}

fn which_python() -> Result<String, ()> {
    for candidate in ["python3", "python"] {
        if Command::new(candidate)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
        {
            return Ok(candidate.to_string());
        }
    }
    Err(())
}
