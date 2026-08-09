//! End-to-end test of the `counted` worker's statistics, minus the cluster.
//!
//! Runs the **actual compiled worker** with the descriptor layout YTsaurus
//! gives a job — input on fd 0, output table 0 on fd 1, and **statistics on fd
//! 5** — and checks what lands on each. The statistics channel is the point:
//! it is a real descriptor the job writes YSON into, and nothing else in the
//! test suite would notice if the format drifted.
//!
//! `YT_JOB_ID` is set because the writer refuses to touch descriptor 5 outside
//! a job. In a launcher, fd 5 is as likely to be an open socket as to be
//! nothing at all.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Serialize;
use ytsaurus_yson::{YsonFormat, to_vec};

mod common;

/// A scratch directory that cleans up after itself.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ytsaurus-rs-counted-{}-{tag}-{:?}",
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

#[derive(Serialize)]
struct Kept<'a> {
    key: &'a str,
    count: i64,
}

#[derive(Serialize)]
struct Rejected {
    count: i64,
}

/// Three rows with a `key`, two without.
fn input() -> Vec<u8> {
    let mut out = Vec::new();
    for (i, key) in ["a", "b", "c"].iter().enumerate() {
        out.extend_from_slice(
            &to_vec(
                &Kept {
                    key,
                    count: i as i64,
                },
                YsonFormat::Binary,
            )
            .unwrap(),
        );
        out.push(b';');
    }
    for count in [7, 8] {
        out.extend_from_slice(&to_vec(&Rejected { count }, YsonFormat::Binary).unwrap());
        out.push(b';');
    }
    out
}

/// Runs the worker the way the cluster does, including descriptor 5.
fn run(dir: &TempDir, input_path: &Path) -> (Vec<u8>, String) {
    let output = dir.join("table0.bin");
    let statistics = dir.join("statistics.yson");

    let script = format!(
        "{:?} <{:?} 1>{:?} 5>{:?}",
        common::example("counted").display(),
        input_path.display().to_string(),
        output.display().to_string(),
        statistics.display().to_string(),
    );

    let result = Command::new("bash")
        .arg("-c")
        .arg(&script)
        // The worker only writes statistics when it believes it is a job.
        .env("YT_JOB_ID", "0-0-0-1")
        .output()
        .unwrap_or_else(|e| panic!("failed to run: {script}\n{e}"));

    assert!(
        result.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    (
        std::fs::read(&output).expect("read output table"),
        std::fs::read_to_string(&statistics).expect("read statistics"),
    )
}

/// Counts records in a binary YSON list fragment.
///
/// By scanning, not by counting `;` bytes: a `;` inside an encoded value is
/// data, and counting those is how a test convinces itself of the wrong number.
fn count_rows(mut data: &[u8]) -> usize {
    use ytsaurus_yson::{Scan, scan::scan_value};

    let mut rows = 0;
    loop {
        while data.first() == Some(&b';') {
            data = &data[1..];
        }
        if data.is_empty() {
            return rows;
        }
        match scan_value(data, YsonFormat::Binary) {
            Ok(Scan::Complete { len }) => {
                rows += 1;
                data = &data[len..];
            }
            other => panic!("the output table is not a complete list fragment: {other:?}"),
        }
    }
}

#[test]
fn statistics_reach_descriptor_five() {
    let dir = TempDir::new("stats");
    let input_path = dir.join("input.bin");
    std::fs::write(&input_path, input()).expect("write input");

    let (table, statistics) = run(&dir, &input_path);

    // A YSON list fragment holding one map, which is the format the reference
    // implementation sends. The byte count is derived rather than written out:
    // every row of the fixture minus the five `;` separators, which belong to
    // the fragment and not to any row.
    let row_bytes = input().len() - 5;
    assert_eq!(
        statistics.trim_end(),
        format!(r#"{{"bytes/read"={row_bytes};"rows/read"=5;"rows/rejected"=2}};"#),
        "statistics on fd 5"
    );

    // And the job did its actual work: two rows dropped, three kept.
    assert_eq!(count_rows(&table), 3, "only rows with a key are written");
}

#[test]
fn a_job_with_nothing_to_report_writes_nothing() {
    let dir = TempDir::new("empty");
    let input_path = dir.join("input.bin");
    std::fs::write(&input_path, b"").expect("write input");

    let (table, statistics) = run(&dir, &input_path);

    assert!(table.is_empty());
    assert!(
        statistics.is_empty(),
        "an empty statistics set must not write an empty map: {statistics:?}"
    );
}
