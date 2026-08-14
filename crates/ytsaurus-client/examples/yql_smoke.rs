//! `yql_smoke` — phase 0 of `docs/format-comparison.md`.
//!
//! Answers four questions about a cluster, driving Query Tracker through
//! `Client::raw_command`. It is an **observation** program: it prints the
//! bodies it got rather than only what it made of them, because the next
//! decision — whether these commands deserve modelled methods on `Client` —
//! needs an observed answer rather than a guessed one.
//!
//! 1. Does this installation run YQL at all?
//! 2. Which UDF modules does the agent load? `Re2` and `String` decide how
//!    phase 1 tokenises `wordcount`. Asked first of the questions that could
//!    fail, because it needs no table and so cannot be lost to one.
//! 3. How is a table referred to — is there a `USE <cluster>` to get right?
//! 4. Does a table-to-table `INSERT` run, and **where are the IDs of the
//!    operations it spawns**?
//!
//! ```sh
//! tests/e2e/run_local_cluster.sh
//! export YT_PROXY=http://localhost:8000
//! cargo run -p ytsaurus-client --example yql_smoke
//!
//! # one query, whole answer printed — how every finding below was settled
//! YT_YQL_QUERY='SELECT 1;' cargo run -p ytsaurus-client --example yql_smoke
//! ```
//!
//! ## What it found on `ghcr.io/ytsaurus/local:stable`, 13 August 2026
//!
//! Each finding says what established it, because three earlier ones were
//! written down from runs that could not have shown them and were false. The
//! way they were false is worth keeping: every one came from a query that
//! **spawned no operations** — `SELECT 1`, which runs inside the agent, and a
//! repeat served from cache — so "there is no operation id here" was a
//! statement about a query that had none.
//!
//! - The cluster is called **`locasaurus`** and needs no `USE`: a
//!   backtick-quoted absolute path resolves on its own. `USE locasaurus;` also
//!   works; `` `locasaurus.//tmp/…` `` does not. *(The three spellings in
//!   question 3, each run.)*
//! - **`Re2`, `String` and `Unicode` are loaded.** *(Question 2, one query
//!   each.)*
//! - **The query cache makes a repeat free.** The same `INSERT` run twice
//!   without `PRAGMA yt.QueryCacheMode = "disable"` spawns two operations and
//!   then **none**; with the pragma, two and two. A benchmark without it would
//!   have measured a cache hit and called it a fast runtime. *(Two pairs of
//!   runs through `YT_YQL_QUERY`, operations counted per query.)*
//! - **YQL's default job memory is not quite enough here.** Without a pragma
//!   the `map_reduce` stage dies with `User job failed: Memory limit exceeded`.
//!   The default is `reducer.memory_limit = 545523360` — 512 MB plus overhead,
//!   the same order as the 512 MB the other examples give a worker — and the
//!   boundary is between **576M (fails) and 640M (passes)**. Hence
//!   [`PRAGMAS`]. *(A query per candidate limit, plus `get_operation` on the
//!   spec.)*
//! - **The operation IDs are in the answer**, under
//!   `progress/yql_progress/<node>/remoteId`, spelled `<proxy>/<operation
//!   id>`, and again under `progress/yql_statistics/…/_id`. They are empty for
//!   nodes that are not YT operations, which is every node of `SELECT 1`.
//! - **`list_operations` finds them server-side**: its `filter` parameter
//!   matches the title YQL gives each operation, `YQL operation (<query id> by
//!   <user>)`. So the modelled `Client::list_operations` with
//!   `OperationFilter::with_substring(query_id)` is the lookup, and no
//!   `raw_command` detour is needed for it. *(Both checked against a query
//!   whose operations existed — the mistake that made the earlier claim false.)*

use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use ytsaurus_client::{
    Client, ClientError, Column, ColumnType, Method, OperationFilter, Repeatable, TableSchema,
    error_summary, yson_build,
};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue, from_slice, to_string};

/// Where this example works, left behind for inspection.
const BASE: &str = "//tmp/ytsaurus_rs_yql";

/// Prepended to every query that touches a table.
///
/// Both are load-bearing, and the module docs say how each was measured.
/// Without the first, a repeated query is served from cache and spawns
/// nothing at all; without the second, the `map_reduce` stage dies on its own
/// memory limit. 640M is the measured boundary rather than a round number: it
/// is close to YQL's own default, so the Rust side of a comparison stays at
/// the 512 MB the other examples use rather than being handed several times
/// what it needs.
const PRAGMAS: &str = "PRAGMA yt.QueryCacheMode = \"disable\";\n\
                       PRAGMA yt.DefaultMemoryLimit = \"640M\";\n";

/// How long to wait for one query before giving up on it and aborting it.
const QUERY_TIMEOUT: Duration = Duration::from_secs(300);

/// States a query does not leave.
///
/// Observed on the way there: `pending`, `running`, `completing`, `failing`.
/// Anything unknown is treated as non-terminal and waits, which is the safe
/// direction: a query wrongly called finished is a wrong measurement, a query
/// wrongly waited on is a slow one.
const TERMINAL: [&str; 3] = ["completed", "failed", "aborted"];

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        // The gate not passing is a legitimate outcome and still a successful
        // run of this program — but the exit code has to say so.
        Ok(false) => {
            eprintln!("\nthe YQL gate did not pass; see above for which step failed");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("\nyql_smoke could not finish: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<bool, ClientError> {
    let client = Client::from_env()?;

    // One query, printed whole. The knob exists because settling a question
    // like "is an operation id anywhere in this answer" is a loop, and running
    // the whole gate for each turn of it costs a minute a time. Every finding
    // in the module docs was settled through here.
    if let Ok(query) = std::env::var("YT_YQL_QUERY") {
        step("One query, verbatim");
        // Through the same wait as the gate: a cluster that has just come up
        // fails this query with "YQL agent stage ... is not found", and the
        // knob is what a person uses most, so it is the last place that should
        // report a starting cluster as a broken one.
        wait_for_agent(&client)?;
        let outcome = run_query(&client, &query, true)?;
        return Ok(outcome.completed());
    }

    let mut answers: Vec<(&str, String)> = Vec::new();

    step("The cluster");
    let cluster = match client.get("//sys/@cluster_name") {
        Ok(value) => text_of(&value).unwrap_or_default(),
        Err(e) => {
            println!("   //sys/@cluster_name is unreadable: {e}");
            String::new()
        }
    };
    println!("   cluster_name = {cluster:?}");

    // ------------------------------------------------------------ SELECT 1

    step("1. SELECT 1 — does this installation run YQL?");
    let trivial = match wait_for_agent(&client) {
        Ok(outcome) => outcome,
        Err(e) => {
            println!("   start_query itself failed: {e}");
            println!("\n   The gate stops here: no query engine to measure.");
            return Ok(false);
        }
    };
    if !trivial.completed() {
        println!("\n   The gate stops here: the cluster accepts queries but does not run them.");
        return Ok(false);
    }
    answers.push(("YQL runs", "yes".to_owned()));

    // --------------------------------------------------------- UDF modules

    // Before anything that needs a table: this question has no table in it,
    // and phase 1's tokenisation depends on the answer, so it must not be lost
    // to a failure that belongs to a different question.
    step("2. Which UDF modules the agent loads");
    let mut loaded = Vec::new();
    for (module, query) in [
        (
            "Re2::FindAndConsume",
            "$m = Re2::FindAndConsume(\"[A-Za-z0-9']+\"); SELECT $m(\"a b-c\");",
        ),
        (
            "String::SplitToList",
            "SELECT String::SplitToList(\"a b c\", \" \");",
        ),
        (
            "Unicode::SplitToList",
            "SELECT Unicode::SplitToList(\"a b c\", \" \");",
        ),
    ] {
        match run_query(&client, query, false) {
            Ok(outcome) if outcome.completed() => {
                println!("   {module}: loaded");
                loaded.push(module);
            }
            Ok(outcome) => println!("   {module}: {}", outcome.summary()),
            Err(e) => println!("   {module}: {e}"),
        }
    }
    answers.push((
        "UDF modules loaded",
        if loaded.is_empty() {
            "none".to_owned()
        } else {
            loaded.join(", ")
        },
    ));

    // ---------------------------------------------------- table references

    step("3. How a table is referred to");
    let input = format!("{BASE}/lines");
    let output = format!("{BASE}/counts");
    if let Err(e) = prepare_input(&client, &input) {
        // Not fatal, and not `?`: the answers already collected are the
        // point of this program, and one of them is worth more than a tidy
        // exit.
        println!("   could not prepare {input}: {e}");
        report(&answers);
        return Ok(false);
    }

    let qualified = format!("`{}.{input}`", cluster_or_guess(&cluster));
    let spellings: [(&str, String, &str); 3] = [
        ("bare path", String::new(), input.as_str()),
        (
            "USE <cluster>",
            format!("USE {};\n", cluster_or_guess(&cluster)),
            input.as_str(),
        ),
        ("cluster-qualified path", String::new(), qualified.as_str()),
    ];

    // Both halves are carried forward, because they differ in different
    // places: `USE` is a prefix, a qualified name is the table expression
    // itself. Keeping only one of them would answer question 4 with a spelling
    // that had just failed.
    let mut working: Option<(String, String)> = None;
    for (name, prefix, table) in &spellings {
        let reference = if table.starts_with('`') {
            (*table).to_owned()
        } else {
            format!("`{table}`")
        };
        let query = format!("{PRAGMAS}{prefix}SELECT COUNT(*) AS n FROM {reference};");
        match run_query(&client, &query, false) {
            Ok(outcome) if outcome.completed() => {
                println!("   {name}: works");
                if working.is_none() {
                    working = Some((prefix.clone(), reference));
                }
            }
            Ok(outcome) => println!("   {name}: {}", outcome.summary()),
            Err(e) => println!("   {name}: {e}"),
        }
    }

    let Some((prefix, reference)) = working else {
        println!("\n   The gate stops here: no spelling of a table reference worked.");
        report(&answers);
        return Ok(false);
    };
    answers.push((
        "table reference",
        if prefix.is_empty() {
            format!("{reference}, no USE")
        } else {
            format!("{reference} after {}", prefix.trim_end())
        },
    ));

    // ------------------------------------------------------ table to table

    step("4. INSERT INTO … SELECT …, and the operations it spawns");
    let insert = format!(
        "{PRAGMAS}{prefix}INSERT INTO `{output}` WITH TRUNCATE\n\
         SELECT text, CAST(COUNT(*) AS Int64) AS count\n\
         FROM {reference}\n\
         GROUP BY text;"
    );
    println!("{}", indent(&insert, "     "));

    let outcome = match run_query(&client, &insert, false) {
        Ok(outcome) => outcome,
        Err(e) => {
            println!("   start_query failed: {e}");
            report(&answers);
            return Ok(false);
        }
    };

    // Read whether or not the query worked: a failed query that still spawned
    // operations answers the question either way.
    let spawned = match operations_of(&client, &outcome.id) {
        Ok(spawned) => spawned,
        Err(e) => {
            println!("   list_operations failed: {e}");
            Vec::new()
        }
    };
    if spawned.is_empty() {
        println!("   list_operations found no operation for this query");
    }
    for operation in &spawned {
        println!("   spawned {} {} {}", operation.0, operation.1, operation.2);
        // The question is not "is there a key called operation" — an earlier
        // version asked that and got the wrong answer — but "is this id in the
        // body", which is answerable by looking for the id itself.
        let mentions = paths_mentioning(&outcome.answer, &operation.0);
        if mentions.is_empty() {
            println!("     not mentioned anywhere in the get_query answer");
        }
        for (path, value) in &mentions {
            println!("     get_query {path} = {}", clip(value, 120));
        }
    }
    answers.push((
        "operations per query",
        format!(
            "{} — see the paths above for where the ids appear",
            spawned.len()
        ),
    ));

    if !outcome.completed() {
        println!("\n   The gate stops here: a table-to-table query does not run.");
        report(&answers);
        return Ok(false);
    }

    match client.row_count(&output) {
        Ok(rows) => println!("   {output} holds {rows} rows"),
        Err(e) => println!("   {output} is unreadable: {e}"),
    }

    step("Gate passed — the answers phase 1 needs");
    report(&answers);
    println!("\n   Left at {BASE}; remove with: yt remove {BASE} --recursive");
    Ok(true)
}

fn report(answers: &[(&str, String)]) {
    for (question, answer) in answers {
        println!("   {question}: {answer}");
    }
}

fn cluster_or_guess(cluster: &str) -> &str {
    if cluster.is_empty() {
        "primary"
    } else {
        cluster
    }
}

// --------------------------------------------------------------- the loop

/// What one query did.
struct Outcome {
    id: String,
    state: String,
    /// Whether the wait ran out before the query reached a terminal state.
    ///
    /// Separate from `state` rather than spliced into it: a query that
    /// completes on the very poll that notices the deadline is completed, and
    /// a state string reading `completed (timed out)` would fail every
    /// comparison against `completed`.
    timed_out: bool,
    answer: YsonValue,
    error: Option<Vec<String>>,
    /// The error document itself, for `error_summary`.
    error_tree: Option<YsonValue>,
}

impl Outcome {
    fn completed(&self) -> bool {
        self.state == "completed"
    }

    /// One line: the state, and what went wrong.
    ///
    /// The category and the innermost cause, which is what
    /// [`ytsaurus_client::error_summary`] produces — the crate has known how to
    /// flatten one of these since jobs had errors, and this example carried its
    /// own worse version until that function was made public. The chain is
    /// still collected and printed in full elsewhere here, because this is an
    /// observation program and the whole tree is sometimes the point.
    fn summary(&self) -> String {
        let mut line = if self.timed_out {
            format!(
                "{} (gave up after {}s)",
                self.state,
                QUERY_TIMEOUT.as_secs()
            )
        } else {
            self.state.clone()
        };
        if let Some(cause) = self.error_tree.as_ref().and_then(error_summary) {
            line.push_str(" — ");
            line.push_str(&clip_str(&cause, 300));
        }
        line
    }
}

/// Starts a query and polls it to a terminal state.
///
/// The verb is not a guess. The HTTP proxy reference gives the rule outright —
/// *"If the command has an input data stream, then PUT. If the command is
/// mutating, then POST. Otherwise GET."* — and the cluster's driver registry
/// declares those two properties per command. So `start_query` is a POST and
/// `get_query` a GET; `Repeatable::Never` for the first because the crate has
/// never heard of it and a retry could start a second query rather than find
/// the first, `Repeatable::Freely` for the second because it is a read.
///
/// `verbose` prints the whole final answer, which is what settles a question
/// about the shape of an API nobody here had seen.
fn run_query(client: &Client, query: &str, verbose: bool) -> Result<Outcome, ClientError> {
    let params = yson_build::map([
        ("engine", yson_build::string("yql")),
        ("query", yson_build::string(query)),
    ]);
    let body = client.raw_command(Method::Post, "start_query", &params, None)?;
    let started = decode(&body, "start_query")?;
    if verbose {
        println!("   start_query answered {}", clip(&started, 400));
    }
    // The envelope is `{query_id=…}`, observed. No fallback for a bare-string
    // body: an untested branch that fired would poll on whatever it was given.
    let id = field(&started, "query_id")
        .as_ref()
        .and_then(text_of)
        .ok_or_else(|| ClientError::Decode {
            command: "start_query".to_owned(),
            reason: format!("no query_id in {}", clip(&started, 400)),
        })?;
    println!("   query_id = {id}");

    let deadline = Instant::now() + QUERY_TIMEOUT;
    let mut last_state = String::new();
    loop {
        let body = client.raw_command_with(
            Method::Get,
            "get_query",
            &yson_build::map([("query_id", yson_build::string(&id))]),
            None,
            Repeatable::Freely,
            None,
        )?;
        let answer = decode(&body, "get_query")?;
        let state = field(&answer, "state")
            .as_ref()
            .and_then(text_of)
            .unwrap_or_else(|| "<no state>".to_owned());

        if state != last_state {
            println!("   state: {state}");
            last_state = state.clone();
        }

        // Terminal wins over the deadline: a query that finished on this very
        // poll finished, however long it took to get here.
        let terminal = TERMINAL.contains(&state.as_str());
        let timed_out = !terminal && Instant::now() >= deadline;
        if terminal || timed_out {
            let error_tree = field(&answer, "error");
            let error = error_tree
                .as_ref()
                .map(error_messages)
                .filter(|messages| !messages.is_empty());
            if state != "completed" {
                for message in error.iter().flatten() {
                    println!("   error: {}", clip_str(message, 500));
                }
            }
            if timed_out {
                println!(
                    "   giving up after {}s in state {state}",
                    QUERY_TIMEOUT.as_secs()
                );
                abort_query(client, &id);
            }
            if verbose {
                println!(
                    "   get_query answered:\n{}",
                    indent(&render(&answer), "     ")
                );
            }
            return Ok(Outcome {
                id,
                state,
                timed_out,
                answer,
                error,
                error_tree,
            });
        }

        sleep(Duration::from_millis(500));
    }
}

/// `SELECT 1`, retried while the YQL agent is still coming up.
///
/// A freshly started or resumed cluster answers `ping` in seconds, publishes
/// `//sys/@cluster_name` a little later, and registers its YQL agent later
/// still. In between, Query Tracker **accepts** a query and then fails it with
/// `YQL agent stage "production" is not found in cluster directory` — so a gate
/// run too early reports "this installation does not run YQL" about an
/// installation that does. That is a wrong answer to the one question this
/// program exists to answer, which is worth a minute of patience.
///
/// Only that one error is waited on. Anything else is the answer.
fn wait_for_agent(client: &Client) -> Result<Outcome, ClientError> {
    const ATTEMPTS: usize = 12;
    const GAP: Duration = Duration::from_secs(10);

    for attempt in 1..=ATTEMPTS {
        let outcome = run_query(client, "SELECT 1;", attempt == ATTEMPTS)?;
        let still_starting = outcome
            .error
            .iter()
            .flatten()
            .any(|message| message.contains("is not found in cluster directory"));

        if !still_starting {
            return Ok(outcome);
        }
        println!("   the YQL agent has not registered yet; waiting ({attempt}/{ATTEMPTS})");
        sleep(GAP);
    }

    run_query(client, "SELECT 1;", true)
}

/// Stops a query this program has given up on.
///
/// Best effort, and reported rather than propagated: the caller is already
/// handling a failure, and a query left running on the cluster is the thing
/// worth avoiding. POST and `Repeatable::Never`, for `start_query`'s reasons.
fn abort_query(client: &Client, id: &str) {
    let params = yson_build::map([("query_id", yson_build::string(id))]);
    match client.raw_command(Method::Post, "abort_query", &params, None) {
        Ok(_) => println!("   aborted {id}"),
        Err(e) => println!("   could not abort {id}: {e}"),
    }
}

/// The operations a query spawned: `(id, type, state)` apiece.
///
/// YQL titles every operation it starts `YQL operation (<query id> by <user>)`,
/// and the cluster's own `filter` matches on it — so this is the modelled
/// `Client::list_operations`, not a raw command and not a scan of titles this
/// program does itself. An earlier version did both, on the strength of a test
/// against a query that had spawned nothing.
///
/// Read soon rather than later: a local cluster has no operations archive, so
/// the scheduler's memory is the only copy.
fn operations_of(
    client: &Client,
    query_id: &str,
) -> Result<Vec<(String, String, String)>, ClientError> {
    let list = client.list_operations(&OperationFilter::new().with_substring(query_id))?;
    if list.incomplete {
        println!("   list_operations answered incomplete: there may be more");
    }
    Ok(list
        .operations
        .into_iter()
        .map(|operation| (operation.id, operation.kind, operation.state))
        .collect())
}

// ---------------------------------------------------------------- fixtures

/// One small schematised table. YQL needs a schema, and so will the Skiff leg.
fn prepare_input(client: &Client, path: &str) -> Result<(), ClientError> {
    #[derive(serde::Serialize)]
    struct Line {
        text: String,
    }

    client.remove_tree(BASE)?;
    client.create("map_node", BASE)?;
    client.create_table(
        path,
        &TableSchema::new([Column::new("text", ColumnType::String).required()]),
    )?;
    client.write_table_rows(
        path,
        [
            Line {
                text: "the quick brown fox".to_owned(),
            },
            Line {
                text: "the quick brown fox".to_owned(),
            },
            Line {
                text: "jumps over the lazy dog".to_owned(),
            },
        ],
    )?;
    println!("   {path}: 3 rows, strict schema");
    Ok(())
}

// ------------------------------------------------------------------ digging

/// The messages inside a cluster error tree, outermost first.
///
/// The tree is mostly attributes — pids, thread names, trace ids — and the one
/// thing a reader needs is the sentence at the bottom of it. Printing the tree
/// instead of this is how a failed query costs an hour.
fn error_messages(error: &YsonValue) -> Vec<String> {
    let mut found = Vec::new();
    collect_messages(error, &mut found);
    found
}

fn collect_messages(value: &YsonValue, found: &mut Vec<String>) {
    match &value.node {
        YsonNode::Map(entries) => {
            if let Some(message) = entries.get(b"message".as_slice()).and_then(text_of)
                && !message.is_empty()
            {
                found.push(message);
            }
            for (key, child) in entries {
                // `attributes` holds no messages, only the diagnostic noise.
                if key.as_slice() != b"attributes" {
                    collect_messages(child, found);
                }
            }
        }
        YsonNode::List(items) => {
            for child in items {
                collect_messages(child, found);
            }
        }
        _ => {}
    }
}

/// Every place in `value` whose string contains `needle`, with its path.
///
/// By value rather than by key name. The question is "is this operation id in
/// this answer", and the ids turned out to live under `remoteId` and `_id` —
/// keys a search for the word "operation" cannot find, which is exactly how an
/// earlier version of this file concluded they were absent.
fn paths_mentioning(value: &YsonValue, needle: &str) -> Vec<(String, YsonValue)> {
    let mut found = Vec::new();
    walk(value, String::new(), needle, &mut found);
    found
}

fn walk(value: &YsonValue, path: String, needle: &str, found: &mut Vec<(String, YsonValue)>) {
    match &value.node {
        YsonNode::Map(entries) => {
            for (key, child) in entries {
                let key = String::from_utf8_lossy(key);
                let child_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}/{key}")
                };
                walk(child, child_path, needle, found);
            }
        }
        YsonNode::List(items) => {
            for (index, child) in items.iter().enumerate() {
                walk(child, format!("{path}[{index}]"), needle, found);
            }
        }
        YsonNode::String(bytes) => {
            if String::from_utf8_lossy(bytes).contains(needle) {
                found.push((path, value.clone()));
            }
        }
        _ => {}
    }
}

// ------------------------------------------------------------------ plumbing

fn decode(body: &[u8], command: &str) -> Result<YsonValue, ClientError> {
    from_slice(body, YsonFormat::Text).map_err(|e| ClientError::Decode {
        command: command.to_owned(),
        reason: format!("{e}; body was {}", String::from_utf8_lossy(body)),
    })
}

fn field(value: &YsonValue, key: &str) -> Option<YsonValue> {
    match &value.node {
        YsonNode::Map(entries) => entries.get(key.as_bytes()).cloned(),
        _ => None,
    }
}

fn text_of(value: &YsonValue) -> Option<String> {
    match &value.node {
        YsonNode::String(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

fn render(value: &YsonValue) -> String {
    to_string(value, YsonFormat::Text).unwrap_or_else(|e| format!("<unrenderable: {e}>"))
}

fn clip(value: &YsonValue, limit: usize) -> String {
    clip_str(&render(value), limit)
}

fn clip_str(text: &str, limit: usize) -> String {
    if text.chars().count() > limit {
        format!("{}…", text.chars().take(limit).collect::<String>())
    } else {
        text.to_owned()
    }
}

fn indent(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn step(what: &str) {
    println!("\n== {what}");
}

#[cfg(test)]
mod tests {
    use super::{clip_str, error_messages, paths_mentioning};
    use ytsaurus_yson::{YsonFormat, from_slice};

    fn parse(text: &str) -> ytsaurus_yson::YsonValue {
        from_slice(text.as_bytes(), YsonFormat::Text).expect("the fixture parses")
    }

    /// The shape a failed query answers with: categories on the outside, the
    /// cause at the bottom, and attributes carrying a `host` that is not a
    /// message however much it looks like one.
    #[test]
    fn error_messages_keeps_the_cause_last() {
        let error = parse(
            "{code=1;message=\"Failed to run query\";\
             attributes={message=\"not a real message\";host=localhost};\
             inner_errors=[{code=1;message=\"Execution\";\
             inner_errors=[{code=1205;message=\"User job failed: Memory limit exceeded\"}]}]}",
        );

        assert_eq!(
            error_messages(&error),
            [
                "Failed to run query",
                "Execution",
                "User job failed: Memory limit exceeded",
            ]
        );
    }

    #[test]
    fn error_messages_skips_empty_ones() {
        // A successful query answers with `error={code=0;message=""}`, which
        // must not read as an error with a blank message.
        assert!(error_messages(&parse("{code=0;message=\"\";attributes={}}")).is_empty());
    }

    /// The finding this file got wrong once: the ids are under `remoteId`, a
    /// key no search for the word "operation" would reach.
    #[test]
    fn paths_mentioning_finds_an_id_under_any_key() {
        let answer = parse(
            "{progress={yql_progress={\"11\"={remoteId=\"localhost:80/op-7\"};\
             \"1\"={remoteId=\"\"}};\
             yql_statistics={yt={\"11\"={_id=\"op-7\"}}}}}",
        );

        let paths: Vec<String> = paths_mentioning(&answer, "op-7")
            .into_iter()
            .map(|(path, _)| path)
            .collect();

        assert_eq!(
            paths,
            [
                "progress/yql_progress/11/remoteId",
                "progress/yql_statistics/yt/11/_id",
            ]
        );
    }

    #[test]
    fn paths_mentioning_answers_nothing_when_the_id_is_absent() {
        assert!(paths_mentioning(&parse("{progress={yql_plan={}}}"), "op-7").is_empty());
    }

    #[test]
    fn clip_str_counts_characters_not_bytes() {
        // A byte-wise clip would split the multi-byte character and panic.
        assert_eq!(clip_str("ошибка", 3), "оши…");
        assert_eq!(clip_str("short", 10), "short");
    }
}
