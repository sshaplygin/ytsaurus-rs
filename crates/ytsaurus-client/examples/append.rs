//! `append` — adding rows to a table instead of replacing it.
//!
//! Every write this crate could make used to replace the table, because a path
//! was sent as a bare string and `<append=%true>` is an *attribute* on it. So a
//! pipeline that produced its output in pieces had one option: keep everything
//! written so far and send it again with each piece. That is quadratic, and the
//! last section of this example measures how quadratic.
//!
//! ```sh
//! export YT_PROXY=http://localhost:8000
//! cargo run --release -p ytsaurus-client --example append
//! ```
//!
//! `YT_APPEND_ROWS` and `YT_APPEND_CHUNKS` set the size of that measurement;
//! the defaults are 60 000 rows in 12 pieces.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use ytsaurus_client::{Client, ClientError, Column, ColumnType, TablePath, TableSchema};
use ytsaurus_yson::YsonFormat;

/// Where the demo keeps its tables.
const BASE: &str = "//tmp/ytsaurus_rs_append";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nappend failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ClientError> {
    let client = Client::from_env()?;
    let log = format!("{BASE}/log");

    step("Preparing Cypress");
    client.remove(BASE)?;
    client.create("map_node", BASE)?;
    client.create("table", &log)?;
    done(BASE);

    step("Three writes to the same table");
    client.write_table_rows(&log, entries(0..3))?;
    check(
        "a plain write puts 3 rows there",
        client.row_count(&log)? == 3,
    )?;

    client.write_table_rows(&log, entries(3..5))?;
    check(
        "a second plain write replaces them: 2 rows",
        client.row_count(&log)? == 2,
    )?;

    client.write_table_rows(TablePath::new(&log).append(), entries(5..9))?;
    check(
        "an appending write adds to them: 6 rows",
        client.row_count(&log)? == 6,
    )?;

    // The rows already there are the ones that were there, not a fresh copy:
    // an append that quietly rewrote would pass a row count and fail this.
    // Indexed only after the length is known: `&kept[..2]` inside the message
    // would panic on the very regression this exists to report.
    let kept: Vec<Entry> = client.read_table_rows(&log)?;
    let survived = kept.len() == 6 && kept[0].n == 3 && kept[1].n == 4 && kept[5].n == 8;
    check(
        &format!("and the rows already there are the ones that were: {survived}"),
        survived,
    )?;

    step("Appending to a table that is sorted");
    // The interesting case. A sorted table stays sorted through an append, and
    // the cluster is the one holding that: it checks the keys against the ones
    // already there rather than taking the table's word for it afterwards.
    let sorted = format!("{BASE}/sorted");
    let schema = TableSchema::new([
        Column::new("n", ColumnType::Int64).required().key(),
        Column::new("payload", ColumnType::Utf8).required(),
    ]);
    client.create_table(&sorted, &schema)?;
    client.write_table_rows(&sorted, entries(10..13))?;
    client.write_table_rows(TablePath::new(&sorted).append(), entries(13..16))?;
    check(
        "6 rows, and the table is still sorted",
        client.row_count(&sorted)? == 6 && is_sorted(&client, &sorted)?,
    )?;

    match client.write_table_rows(TablePath::new(&sorted).append(), entries(0..1)) {
        Ok(()) => {
            eprintln!("   FAIL a key smaller than the last was appended to a sorted table");
            return Err(ClientError::Config(
                "the cluster did not enforce sort order on append".to_owned(),
            ));
        }
        Err(e) => {
            check(
                "and a key smaller than the last is refused",
                e.to_string().contains("Sort order violation"),
            )?;
            println!("   {}", first_line(&e.to_string()));
        }
    }

    step("Appending to a table that does not exist");
    // Not created for you. The error says it in the cluster's own terms, which
    // are not obvious, so it is worth seeing once.
    match client.write_table_rows(
        TablePath::new(format!("{BASE}/missing")).append(),
        entries(0..1),
    ) {
        Ok(()) => {
            return Err(ClientError::Config(
                "appending created a table, which it is not supposed to do".to_owned(),
            ));
        }
        Err(e) => {
            // Checked, not merely printed: without this a transport blip, a
            // timeout or a typo in BASE would all read as the feature working.
            check(
                "refused: the table has to exist first",
                e.to_string()
                    .contains("Error getting basic attributes of user objects"),
            )?;
            println!("   {}", first_line(&e.to_string()));
        }
    }

    step("Appending to something that is not a table");
    // The path is resolved before the rows are looked at, and the cluster says
    // what it found instead of what it wanted.
    let file = format!("{BASE}/afile");
    client.create("file", &file)?;
    client.write_file(&file, b"not a table")?;
    match client.write_table_rows(TablePath::new(&file).append(), entries(0..1)) {
        Ok(()) => {
            return Err(ClientError::Config(
                "rows were appended to a file node".to_owned(),
            ));
        }
        Err(e) => {
            check(
                "a file node is refused by type, not by parse error",
                e.to_string().contains(r#"expected "table", actual "file""#),
            )?;
            println!("   {}", first_line(&e.to_string()));
        }
    }

    step("Zero rows");
    // The asymmetry that costs a table: an empty *append* is nothing happening,
    // an empty *replace* is a truncation. Both are one line of code apart.
    let empty = format!("{BASE}/empty");
    client.create("table", &empty)?;
    client.write_table_rows(&empty, entries(0..4))?;
    client.write_table_rows(TablePath::new(&empty).append(), entries(0..0))?;
    check(
        "appending no rows leaves the 4 that were there",
        client.row_count(&empty)? == 4,
    )?;
    client.write_table_rows(&empty, entries(0..0))?;
    check(
        "writing no rows empties the table: 0",
        client.row_count(&empty)? == 0,
    )?;

    step("The other two writers");
    // `write_table_rows` is the one the sections above use. The attribute has to
    // survive the other two routes into the same command, and the only way to
    // know it does is to send rows through them.
    let others = format!("{BASE}/others");
    client.create("table", &others)?;
    client.write_table_streaming(&others, std::io::Cursor::new(encoded(entries(0..2))))?;
    client.write_table_streaming(
        TablePath::new(&others).append(),
        std::io::Cursor::new(encoded(entries(2..5))),
    )?;
    check(
        "write_table_streaming appends: 2 then 3 makes 5",
        client.row_count(&others)? == 5,
    )?;
    client.write_table(TablePath::new(&others).append(), &encoded(entries(5..6)))?;
    check(
        "write_table appends too: 6",
        client.row_count(&others)? == 6,
    )?;

    step("Appending inside a transaction");
    // An append is a write like any other, so it belongs to the transaction it
    // was made in: invisible until the commit, gone if there is none.
    let staged = format!("{BASE}/staged");
    client.create("table", &staged)?;
    client.write_table_rows(&staged, entries(0..3))?;
    {
        let tx = client.start_transaction()?;
        tx.client()
            .write_table_rows(TablePath::new(&staged).append(), entries(3..6))?;
        check(
            "inside the transaction the table has 6 rows",
            tx.client().row_count(&staged)? == 6,
        )?;
        check(
            "outside it, still 3",
            client.row_count(&staged)? == 3 && client.read_table_rows::<Entry>(&staged)?.len() == 3,
        )?;
        tx.abort()?;
    }
    check(
        "and after the abort the appended rows are gone: 3",
        client.row_count(&staged)? == 3,
    )?;
    {
        let tx = client.start_transaction()?;
        tx.client()
            .write_table_rows(TablePath::new(&staged).append(), entries(3..6))?;
        tx.commit()?;
    }
    check(
        "a committed one keeps them: 6",
        client.row_count(&staged)? == 6,
    )?;

    step("Four writers at once");
    // The difference that makes append worth reaching for beyond the wire
    // saving: a replacing write takes an *exclusive* lock on the table and the
    // losers fail, while appends take a shared one and all of them land.
    let shared = format!("{BASE}/shared");
    client.create("table", &shared)?;
    check(
        "four concurrent appends of 100 rows: all 400 land",
        raced(&client, &shared, true)? == 400,
    )?;
    let exclusive = format!("{BASE}/exclusive");
    client.create("table", &exclusive)?;
    check(
        "four concurrent replaces leave one writer's 100",
        raced(&client, &exclusive, false)? == 100,
    )?;

    step("What a reader sees while an append is in flight");
    // Nothing, which is the answer that lets a reader poll `@row_count` without
    // a lock: the rows arrive with the upload transaction's commit, all at once.
    let inflight = format!("{BASE}/inflight");
    client.create("table", &inflight)?;
    client.write_table_rows(&inflight, entries(0..10))?;
    let body = encoded(entries(10..40));
    let (path, writing) = (inflight.clone(), client.clone());
    let upload = std::thread::spawn(move || {
        writing.write_table_streaming(TablePath::new(&path).append(), Trickle::new(body))
    });
    let mut seen = Vec::new();
    while !upload.is_finished() {
        seen.push(client.row_count(&inflight)?);
        std::thread::sleep(Duration::from_millis(100));
    }
    upload.join().expect("the upload thread finished")?;
    check(
        &format!(
            "{} readings during the upload, every one of them 10",
            seen.len()
        ),
        !seen.is_empty() && seen.iter().all(|&n| n == 10),
    )?;
    check("and 40 once it commits", client.row_count(&inflight)? == 40)?;

    benchmark(&client)?;

    println!("\nA path is a YSON value, not a string, and `<append=%true>` is what it says.");
    println!("Tables left at {BASE}");
    Ok(())
}

/// What the pipeline shape costs when the table has to be rewritten each time.
///
/// Both halves write the same rows in the same number of pieces and end with
/// the same table. The difference is only what goes over the wire: an append
/// sends each row once, and a rewrite sends everything written so far, every
/// time.
fn benchmark(client: &Client) -> Result<(), ClientError> {
    let rows = number("YT_APPEND_ROWS", 60_000);
    let chunks = number("YT_APPEND_CHUNKS", 12).max(1);
    // At least one row per piece, or both loops write nothing and the
    // comparison is between two empty tables.
    let per_chunk = (rows / chunks).max(1);

    step(&format!(
        "Writing {rows} rows in {chunks} pieces, both ways"
    ));

    let appended = format!("{BASE}/bench_append");
    client.create("table", &appended)?;
    let by_append = time(|| {
        for chunk in 0..chunks {
            let from = chunk * per_chunk;
            client.write_table_rows(
                TablePath::new(&appended).append(),
                entries(from..from + per_chunk),
            )?;
        }
        Ok(())
    })?;
    // What the loops actually write, which is not `rows` unless it divides:
    // `per_chunk` truncates.
    let sent_appending = per_chunk * chunks;

    let rewritten = format!("{BASE}/bench_rewrite");
    client.create("table", &rewritten)?;
    let by_rewrite = time(|| {
        for chunk in 0..chunks {
            // Everything from the beginning, every time — which is the only
            // thing a client without append can do.
            client.write_table_rows(&rewritten, entries(0..(chunk + 1) * per_chunk))?;
        }
        Ok(())
    })?;
    let sent_rewriting = per_chunk * chunks * (chunks + 1) / 2;

    check(
        "both tables ended up the same size",
        client.row_count(&appended)? == client.row_count(&rewritten)?,
    )?;

    println!(
        "   appending   {:>6.2}s   {:>9} rows sent",
        by_append.as_secs_f64(),
        sent_appending
    );
    println!(
        "   rewriting   {:>6.2}s   {:>9} rows sent   ({:.1}× the data)",
        by_rewrite.as_secs_f64(),
        sent_rewriting,
        sent_rewriting as f64 / sent_appending as f64
    );

    // The data ratio is arithmetic — a rewrite of k pieces sends (k+1)/2 times
    // the rows — so it is what gets asserted on. The clock is printed and not
    // checked: at small sizes per-request overhead dominates, and three
    // consecutive runs of a working feature disagreed about which was quicker.
    check(
        &format!("appending sent {sent_appending} rows against {sent_rewriting}"),
        sent_appending < sent_rewriting,
    )?;

    Ok(())
}

/// One row of the log.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Entry {
    n: i64,
    payload: String,
}

/// Rows `range`, in order.
fn entries(range: std::ops::Range<u64>) -> impl Iterator<Item = Entry> {
    range.map(|n| Entry {
        n: n as i64,
        payload: format!("entry {n:08} 0123456789abcdef0123456789abcdef"),
    })
}

/// Rows as the wire wants them: binary YSON values, one after another.
fn encoded(rows: impl Iterator<Item = Entry>) -> Vec<u8> {
    let mut out = Vec::new();
    for row in rows {
        out.extend_from_slice(&ytsaurus_yson::to_vec(&row, YsonFormat::Binary).expect("encodes"));
        out.push(b';');
    }
    out
}

/// Four threads writing 100 rows each to one table, and what is left afterwards.
fn raced(client: &Client, path: &str, appending: bool) -> Result<i64, ClientError> {
    let mut writers = Vec::new();
    for worker in 0..4_u64 {
        let (client, path) = (client.clone(), path.to_owned());
        writers.push(std::thread::spawn(move || {
            let rows = entries(worker * 100..worker * 100 + 100);
            if appending {
                client.write_table_rows(TablePath::new(&path).append(), rows)
            } else {
                client.write_table_rows(&path, rows)
            }
        }));
    }

    let refused = writers
        .into_iter()
        .map(|w| w.join().expect("the writer thread finished"))
        .filter(Result::is_err)
        .count();
    println!("   {refused} of the 4 were refused");

    client.row_count(path)
}

/// A reader that hands over its bytes slowly, so a write stays in flight long
/// enough to watch.
struct Trickle {
    data: Vec<u8>,
    at: usize,
}

impl Trickle {
    fn new(data: Vec<u8>) -> Self {
        Self { data, at: 0 }
    }
}

impl std::io::Read for Trickle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.at >= self.data.len() {
            return Ok(0);
        }
        std::thread::sleep(Duration::from_millis(200));
        let n = buf.len().min(64).min(self.data.len() - self.at);
        buf[..n].copy_from_slice(&self.data[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// Whether the cluster still calls the table sorted.
fn is_sorted(client: &Client, path: &str) -> Result<bool, ClientError> {
    client.get_as::<bool>(&format!("{path}/@sorted"))
}

fn time(work: impl FnOnce() -> Result<(), ClientError>) -> Result<Duration, ClientError> {
    let started = Instant::now();
    work()?;
    Ok(started.elapsed())
}

fn number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn first_line(message: &str) -> &str {
    message.lines().next().unwrap_or(message)
}

fn step(what: &str) {
    println!("\n== {what}");
}

fn done(what: &str) {
    println!("   ok {what}");
}

fn check(what: &str, passed: bool) -> Result<(), ClientError> {
    if passed {
        done(what);
        return Ok(());
    }
    eprintln!("   FAIL {what}");
    Err(ClientError::Config(format!("check failed: {what}")))
}
