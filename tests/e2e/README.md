# End-to-end tests

Two layers. Both have been run; the second needs Docker, so only the first
runs in CI.

| | Runs in CI | Needs a cluster | What it proves |
| --- | --- | --- | --- |
| [`examples/tests/cat_e2e.rs`](../../examples/tests/cat_e2e.rs) | yes | no | the compiled worker handles the real byte stream correctly |
| [`run_e2e.sh`](run_e2e.sh) | no | yes | the above, plus the scheduler, operation spec and re-encoding |

The important point: the offline test's fixtures are **captured from a real
cluster**, not written by hand. `cat_input.bin` is literally the stream a job was
handed on fd 0.

## Offline test (runs automatically)

```sh
cargo test -p ytsaurus-examples --test cat_e2e
```

Runs the real `cat` binary the way the cluster runs it — input on fd 0, output
table 0 on fd 1, output table 1 on fd 4, wired by shell redirection — and
compares its output against the captured golden bytes. Covers descriptor
numbering, control records, table routing, byte-exact pass-through of non-UTF-8
data, empty input, and that a truncated stream fails the job.

## Cluster test

```sh
pip install ytsaurus-client ytsaurus-yson   # both: binary YSON needs the bindings
tests/e2e/run_local_cluster.sh              # localhost:8000, UI on :8001
tests/e2e/run_e2e.sh
tests/e2e/run_local_cluster.sh --stop
```

`run_e2e.sh` uploads the table payloads, runs `cat` as a real map operation and
asserts the output table reads back identical to the input, repeats with two
input and two output tables to exercise table switching, and finishes with a
`wordcount` map-reduce checked against a hand-computed result.

**The same three checks run without Python**, as
[`examples/e2e.rs`](../../crates/ytsaurus-client/examples/e2e.rs):

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh cat wordcount
cargo run -p ytsaurus-client --example e2e
```

Every command the script sends has a method on `Client` — `remove --recursive
--force` is `remove_tree`, `create --recursive` is `create`, `write-table
--format '<format=binary>yson'` is `write_table` because binary YSON is the
default — and the two `--spec` fragments that carry the meaning,
`enable_input_table_index` and `enable_key_switch` under **`reduce_job_io`**,
are modelled on the spec builders. The one thing the CLI does that the client
does not is **create the destination tables**: `yt map --dst` makes them, and an
operation that made its own outputs would turn a mistyped destination into a
stray table rather than an error, so the example creates them itself.

**Both are kept, and that is deliberate.** The Rust example proves the client
can drive the cluster unaided; the shell script proves the *worker's* output is
right according to a **different implementation** — the official Python client
reading the same tables. A check that only ever agrees with itself is worth less
than one that has to agree with somebody else, so `run_e2e.sh` stays as the
independent reading, and `examples/e2e.rs` is what runs on a machine with no
Python.

### Dynamic Skiff map

The Skiff path is exercised offline by
[`examples/tests/skiff_cat_e2e.rs`](../../examples/tests/skiff_cat_e2e.rs): it
runs the real `skiff_cat` worker with non-UTF-8 `string32` data. With the local
cluster running, the equivalent client-driven cluster check is:

```sh
./scripts/build-worker.sh skiff_cat
YT_PROXY=http://localhost:8000 cargo run -p ytsaurus-client --example skiff_launch
```

It writes and reads raw Skiff streams through the HTTP client, launches a map
whose mapper format is Skiff, and verifies decoded output rows. This is not yet
a CI or captured-cluster fixture; run it against a real cluster before treating
the dynamic Skiff layer as release-ready.

The comparison is input-table read-back against output-table read-back, not
against the uploaded file. **The cluster re-encodes rows on ingest** — 309 676
bytes uploaded came back as 309 688 — so comparing against the upload would fail
for reasons that have nothing to do with the job.

### Without the `yt` CLI

Two examples drive a cluster through `ytsaurus-client` alone, with nothing
Python on `PATH`:

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh cat boom selfrun wordcount
cargo run -p ytsaurus-client --example e2e          # all of run_e2e.sh, no Python
cargo run -p ytsaurus-client --example launch       # the happy path
cargo run -p ytsaurus-client --example diagnose     # the failure path
cargo run -p ytsaurus-client --example sort_reduce  # sort, then reduce over it
cargo run -p ytsaurus-client --example idempotent   # a repeated start is one operation
cargo run -p ytsaurus-client --example cached_upload # the second upload is a cache hit
cargo run -p ytsaurus-client --example statistics   # what the job counted, read back
cargo run -p ytsaurus-client --example vanilla      # three jobs with no input table
cargo run -p ytsaurus-client --example schema       # a derived schema the cluster enforces
cargo run -p ytsaurus-client --example cluster_info # connect, and read a node into a type
cargo run -p ytsaurus-client --example table_usage  # Rust values in, Rust values out
cargo run -p ytsaurus-client --example abort        # stopping an operation, and what it costs
cargo run -p ytsaurus-client --example lifecycle    # pause, reprice, finish early, reattach; merge and erase
cargo run --release -p ytsaurus-client --example append  # adding rows, against rewriting them
cargo run -p ytsaurus-client --example transaction  # published all at once, or not at all
cargo run -p ytsaurus-client --example cypress      # list, copy, move, link and lock
cargo run -p ytsaurus-client --example raw          # commands the crate does not model
cargo run --release -p ytsaurus-client --example streaming  # a table bigger than the program
cargo run --release -p ytsaurus-client --example profile     # what the pilot spends on decoding

# One binary that is both launcher and job. On macOS the launcher cannot be the
# uploaded file, so point it at the musl build of the same source.
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-examples --bin selfrun
```

`diagnose` runs the `boom` worker, which panics on its first row, and checks
that the failed job's stderr came back in the error rather than only in the web
UI. It exits non-zero if the operation *succeeds* — that would mean it is no
longer testing anything.

### Last run

All checks passed against `ghcr.io/ytsaurus/local:stable` on 2026-08-04
(Docker on macOS/arm64, x86_64 image under emulation):

```text
== Comparing input and output byte-for-byte
   ok identical (309688 bytes)
== Two input tables, two output tables, with table switching
   ok table 0 identical
   ok table 1 identical
== Wordcount map-reduce
   ok wordcount matches the reference (9 words)
```

`diagnose` on the same cluster, same day:

```text
operation 1ba94195-3142e068-103e8-ffe93efc finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
  job 24c164af-a273b7fd-10384-1000001 on localhost:24403: User job failed: Process terminated by signal 6
  stderr:
    boom: started, reading input
    ytsaurus-job: the job panicked and will fail.
    thread 'main' panicked at examples/src/bin/boom.rs:37:17:
    boom: this job fails on purpose (row 1, 23 bytes)
   ok a failed job was reported
   ok the job's stderr came back
   ok the stderr is the job's own panic
   ok the job error explains the exit
```

`selfrun` too, from both sides. On the macOS host the launcher is Mach-O and is
refused before it can be uploaded:

```text
/…/target/debug/selfrun cannot run on a cluster node: it is not an ELF binary,
so a Linux node cannot exec it. Build the worker with scripts/build-worker.sh …
```

and the real one-binary path — the binary uploading *itself* — was verified by
running the musl build as the launcher inside Linux, which is what a Linux
developer's machine would do:

```sh
docker cp target/x86_64-unknown-linux-musl/release-worker/selfrun yt.backend:/tmp/selfrun
docker exec -e YT_PROXY=http://localhost:80 yt.backend /tmp/selfrun
```

```text
== Uploading this very binary
   ok /tmp/selfrun -> //tmp/ytsaurus_rs_selfrun/selfrun
== Waiting for it
   ok completed
== Reading the result back
   ok 3 rows, 104 bytes
```

`sort_reduce`, same cluster:

```text
== Sorting it
   ok the table is now sorted by [word]
== Reducing over the sorted table
   ok 4 rows
== Checking the totals
   ok alpha = 6
   ok beta = 6
   ok delta = 1
   ok gamma = 7
   ok no extra groups
```

Four rows rather than one is the check that matters: it means `key_switch`
reached the reducer. In the plain `job_io` section, because a reduce has one job
type — the map-reduce trap in the other direction.

`idempotent`, same cluster:

```text
== Starting the operation twice under one mutation ID
   mutation_id fcbe6ca-c0358138-dd69c16e-9e2ac0aa
   ok first  -> 5a49b501-3620e60c-103e8-8cac5f56
   ok second -> 5a49b501-3620e60c-103e8-8cac5f56
   ok both calls returned the same operation
== And a different ID really does start another one
   ok a fresh mutation ID starts a second operation
```

The first attempt at this example failed, and usefully: sending the same
`mutation_id` twice **without** the `retry` flag is refused with `Duplicate
request is not marked as "retry"`. The cluster does not infer a replay from
recognising the ID, which is why `MutationId::as_retry()` exists.

`cached_upload`, same cluster (a 491 KiB worker — the gap grows with the
binary):

```text
== First upload
   uploaded in 166 ms -> //tmp/yt_wrapper/file_storage/new_cache/da/2c76e46b...
== Second upload of the same binary
   cache hit in 32 ms -> //tmp/yt_wrapper/file_storage/new_cache/da/2c76e46b...
   ok the second call skipped the upload
   ok and found the same file
== Running the cached binary
   ok the identity map reproduced its input
```

The last check is the one that matters beyond the timing: the cached node keeps
its `executable` attribute and reaches the sandbox under the name the command
expects, so a cached binary really runs.

`statistics`, same cluster — seven rows in, three of them without a `key`
column, which the job drops:

```text
== What the job reported
   {"bytes/read"={"$"={completed={map={count=1;max=147;min=147;sum=147}}}};
    "rows/read"={"$"={completed={map={count=1;max=7;min=7;sum=7}}}};
    "rows/rejected"={"$"={completed={map={count=1;max=3;min=3;sum=3}}}}}
   ok rows/read is 7
   ok rows/rejected is 3
```

`schema`, same cluster — the schema comes from `#[derive(TableRow)]` on the
struct the rows have, and nothing is written out by hand:

```text
== Creating a table from the struct its rows have
   <strict=%true;unique_keys=%false>[{name=host;required=%true;sort_order=ascending;type=utf8};…]
   ok the cluster kept the columns as given
   ok and marked the table sorted
== Every column type the crate can name
   ok 26 column types accepted
== The schema is a promise the cluster keeps
   ok a row missing a required column is refused
   write_table: cluster error 307: Required column "size" cannot have "null" value
== Evolving the schema of a table that already has rows
   ok 2 rows written
   ok an optional column can be added to a table with rows in it
   ok dropping a column: cluster error 316: … Cannot remove column "size" from a strict schema
   ok adding a required column: … Cannot insert a new required column "must" into a non-empty table
   ok changing a column's type: … Type of "" field is modified in non backward compatible manner
   ok and an empty table accepts what a full one refuses
== And the one order this cluster will not take
   as documented: create: cluster error 314: Descending sort order is not available in this context yet
```

The last line of the evolution section is the one to remember: **an empty table
accepts every change a full one refuses**, so a migration rehearsed on an empty
table has proved nothing. The widening itself comes from a struct that gained a
field — the schema is still never written out by hand.

The last two are the ones worth keeping: a schema the cluster does not enforce
would be decoration, and the descending refusal is checked rather than asserted
so that the day a cluster enables it, the run says so instead of going stale.

`append` and `abort`, same cluster — the two gaps the Go SDK comparison found:

```text
== Three writes to the same table
   ok a plain write puts 3 rows there
   ok a second plain write replaces them: 2 rows
   ok an appending write adds to them: 6 rows
== Appending to a table that is sorted
   ok 6 rows, and the table is still sorted
   ok and a key smaller than the last is refused
   write_table: cluster error 301: Sort order violation: [0#15] > [0#0]
== Appending to a table that does not exist
   ok refused: the table has to exist first
== Writing 60000 rows in 12 pieces, both ways
   appending     0.60s       60000 rows sent
   rewriting     1.03s      390000 rows sent   (6.5× the data)

== Aborting it, with a reason
   ok the scheduler took the request in 399 ms
   ok and it was already `aborted` — 0.0s of waiting
   ok and no job is still running (0 left in the list)
== Reading back why it stopped
   Operation aborted by user request: stopped by the abort example
   ok and kept the reason given: "stopped by the abort example"
== Aborting it again
   ok is refused, because the scheduler has let go of it
   abort_operation: cluster error 200: No such operation 675da9d0-…
```

Two of those lines are the ones that would not have been guessed. Appending to a
**sorted** table is a checked operation — the cluster refuses a key that arrives
out of order, so an append there is a continuation rather than an addition. And
**aborting is not idempotent**: an operation the scheduler has finished with is
gone from it, where a transaction would have forgiven the second abort.

`table_usage` and `cluster_info`, same cluster — the two examples that mirror
the Go SDK's `table-usage` and `cypress-example`. Between them they are the
whole typed path: a schema derived from a struct, a hundred rows written as
Rust values, the row count read out of the attribute map into a one-field
struct, the same hundred read back and compared element for element, and a
node read into a type that names three of its forty-eight attributes:

```text
== Writing 100 rows, as Rust values
   ok 100 contacts written
== Asking the cluster how many rows it has
   ok row_count is 100
   ok and the attribute map agrees, read into a one-field struct
== Reading them back, as Rust values
   ok 100 rows came back
   ok and every one is the row that went in, in order
== A struct naming one column is a projection
   ok 100 names came back, the first Some("Gopher 0")
== The same projection, in the other direction
   ok a row missing the other three columns is refused
   write_table: cluster error 307: Required column "email" cannot have "null" value

   cluster was created at 2026-08-04T16:42:47.385970Z
   ok the cluster offered 48 attributes, and the struct named 3
   ok this cluster calls itself "locasaurus"
   ok a type that does not fit is an error, not a panic
   get: … invalid type: string "2026-08-04T16:42:47.385970Z", expected u64
```

The last two lines of each are the ones that matter. A projection reads and does
**not** write — the columns it leaves out were promised as required — and a type
the answer cannot fit is an error naming the path rather than a panic or a zero.

`transaction`, same cluster — the same map operation as `launch`, run so that
nothing it does exists until it commits:

```text
== A table that exists only inside a transaction
   transaction 4-29da-10001-6f45
   ok the transaction sees it
   ok and nothing outside does
== Aborting it
   ok nothing was left behind
== A launcher that fails halfway
   the launcher failed: the step after the write did not work out
   ok the half-written table is gone with it
== Publishing an operation's output atomically
   ok operation d202198d-95866cce-103e8-91b8ab44 completed
   ok outside the transaction the old result is still the result
   ok and the worker is not in Cypress at all
== Committing
   ok the output is the operation's, all at once
   ok and the upload came with it
== What a transaction that is gone looks like
   ok as expected: create: cluster error 500: Error resolving path
      //tmp/ytsaurus_rs_transaction/never: No such transaction 4-2c2e-10001-6b6e
== Holding a 2s transaction for 6s
   ok committed 6s in
```

Three of those checks are the ones worth keeping. The failing launcher contains
no cleanup code at all — the abort is the handle being dropped by the `?`. The
operation's *upload* is as invisible as its output, so the publish really is one
event rather than two. And the last section holds a transaction three timeouts
past its expiry: without the ping thread the cluster would have aborted it four
seconds earlier, so it is the one check that fails if the keep-alive stops
working.

`profile`, same cluster — the pilot's mapper run three times over one 48 MiB
table, stopped at three depths, timed by the scheduler:

```text
== What each phase cost
   being handed the rows        2225 ms    45.8%
   decoding them                 514 ms    10.6%
   validating and writing       2120 ms    43.6%
   ————————————————————————————————————————
   the pilot's map              4859 ms   100.0%
```

Decoding is ~10 % of a job that does something with its rows, against 66 % for
the microbenchmark's job that does nothing with them. That is the answer the
backlog wanted from this: the Skiff question loses urgency.
[`docs/benchmarking.md`](../../docs/benchmarking.md) records it with the three
reasons it is a reading rather than a verdict — chief among them that rounds of
the same mode scattered by a second, which is more than the quantity being
measured.

`streaming`, same cluster — the same 64 MiB table written from a generator and
then read back both ways, with peak RSS watched throughout:

```text
== Writing about 64 MiB from a generator
   ok 1242757 rows, 53.5 MiB on the cluster, peak RSS 2.9 MiB
== Reading it back as a stream
   ok 1242757 rows counted, peak RSS 3.8 MiB
   ok and their values add up to what was written
== The same table, read into memory
   ok 67.7 MiB in hand, peak RSS 74.7 MiB

Streaming the 67.7 MiB table cost 1.0 MiB of peak RSS; reading it in cost 70.9 MiB.
```

`ru_maxrss` is a high-water mark, so the streaming figures are not an average
that a spike could hide behind. The last line is the whole point of the item:
the buffered pair charges the table's size to the program, and the streaming
pair charges a buffer.

`cypress`, same cluster — a small tree of dated runs, a `latest` link over it,
and three transactions competing for one lock:

```text
== Listing what is there
   as the cluster gives them: ["2026-08-02", "2026-08-03", "2026-08-01"]
   ok the three runs are there: ["2026-08-01", "2026-08-02", "2026-08-03"]
   ok and listing a table is an error: list: cluster error 103: "List" method is not supported
== Copying and moving
   ok a second copy is refused: copy: cluster error 501: Node … already exists
   ok copy_replacing overwrites it
   ok a move leaves nothing behind
== A link, and the trap in reading one
   ok latest&/@target_path is "…/runs/2026-08-01", while latest/@type is "table" — the target's
== Publishing by moving a staging table over the live one
   ok readers still see the old table while the transaction is open
   ok and the new one the moment it commits
== A lock, and what it refuses
   ok the second is refused, and told who won: lock: cluster error 402: Cannot take
      "exclusive" lock … since "exclusive" lock is taken by concurrent transaction …
   ok but it can pin the version it is reading
== Waiting for a lock
   ok a wait that could never end, ended: still pending after 2s
   ok granted as 4-1d5ab-100c8-57bfbc50 once the holder went away, 3s in
```

The first line is the finding that matters most: **`list` is not sorted**. The
last two are the other one — a waitable lock is `pending`, not held, and it can
queue for something that will never happen, which is why `lock_waiting` has a
deadline rather than trusting the cluster to end the wait.

`vanilla`, same cluster — three jobs, no input table anywhere, and their stderr
read back after they succeeded:

```text
== Running 3 jobs with nothing to read
   ok operation b528b474-8714f38c-103e8-2ab7da1e completed
== Reading back what the jobs printed
   ok the cluster still lists all 3 jobs
   8d297f9c: shards: job 1 of 3
   76472094: shards: job 0 of 3
   50c5574a: shards: job 2 of 3
   ok all 3 succeeded and their stderr survived
== Checking what the jobs wrote
   ok 3 rows, one per job
   ok the jobs identified themselves as {0, 1, 2}
   ok their slices add up to 500500
   ok and cover every one of the 1000 numbers exactly once
```

The cookies are the check that matters: the cluster hands each job a distinct
one, which is all a vanilla job has to divide the work by.

The stderr section is the Go SDK's `vanilla-example` in full, and it settles two
things a cluster had to answer: **stderr is kept for jobs that succeeded**, with
no spec option asked for, and it must be **asked for promptly** — `list_jobs`
answers with an empty list for an operation that finished a while ago, because
the controller agent forgets its jobs and this cluster has no job archive.

That document is why the client does not walk `rows/rejected` as a path: the
name is one key, and `$` → job state → job type sits below it. The operation
*succeeded* — the only sign three rows went missing is the statistic.

### Commands the crate does not model

`cargo run -p ytsaurus-client --example raw`, on the same cluster on
2026-08-06. Four commands with no method on `Client`, sent through
`Client::raw_command` and its three siblings:

```text
== A command with no parameters at all
   ok the cluster described: compression_codecs, erasure_codecs, node_flavors,
      operation_statistics_descriptions, primitive_types,
      query_memory_limit_in_tablet_nodes, require_password_in_authentication_commands,
      structured_web_json, user_tokens_metadata
   ok this build offers 71 compression codecs
== A file, uploaded and streamed back
   ok 4000000 bytes came back, byte-for-byte what went up
   ok the reader counted the same 4000000 bytes
== A raw command joins the transaction it was sent through
   ok the node is invisible outside the transaction
   ok and there once the transaction commits
== A read the caller marks as safe to repeat
   ok the scheduler answered with 8 keys
== What it will not send
   ok a command name that would change the URL is refused
   ok a payload on a GET is refused rather than dropped
```

Three things the cluster settled that could not be settled by reading:

- **`get_supported_features` answers `{features=…}`, not `{value=…}`.** The v4
  envelope is keyed by what the command returns, which is the same trap that
  made `exists` read the wrong key for two releases. The example names the key
  rather than assuming one, and there are 71 compression codecs behind it.
- **`write_file` and `read_file` round-trip through the raw door**, 4 MB
  byte-for-byte, with neither direction holding the file: the upload came from
  a reader and the download was summed as it arrived. `read_file` is not
  modelled by this crate, so this is the whole of how a file is read back
  today.
- **A raw command is genuinely inside the transaction it was sent through.**
  The staged node is invisible to a second client until the commit, which is
  the claim that would be silently false if the raw door bypassed
  `Transport::in_transaction`.

### The same three checks, without Python

`cargo run -p ytsaurus-client --example e2e`, on `ghcr.io/ytsaurus/local:stable`
on 2026-08-06. Compare the byte counts and the word count with `run_e2e.sh`'s
above — they are the same numbers, reached without the `yt` CLI:

```text
== Preflight
   ok workers built, fixtures read (309676 and 175 bytes)
== Preparing Cypress
   ok //tmp/ytsaurus_rs_e2e, with both workers uploaded
== Running cat as a map operation
   ok operation cee8ff6b-1203ddb1-103e8-b1bf75b9 finished
== Comparing input and output byte-for-byte
   ok identical (309688 bytes)
== Two input tables, two output tables, with table switching
   ok table 0 identical (309688 bytes)
   ok table 1 identical (180 bytes)
== Wordcount map-reduce
   ok wordcount matches the reference (9 words)
```

309 676 bytes went up and 309 688 came back, which is the cluster's re-encoding
showing through in the preflight line — and the reason the comparison is
read-back against read-back rather than against the file.

**Where the bytes come from matters.** The first two checks upload
`fixtures/table_rows_*.bin` as bytes, exactly as the shell script pipes them
into `yt write-table`. Those were built by `generate_fixtures.py` straight from
the binary YSON specification and deliberately *not* by this project's encoder;
regenerating them in the example would have quietly thrown that away. The
wordcount input is written with `write_table_rows` — through this project's
encoder — because that check asserts a set of counts, not a byte sequence.

## Refreshing the golden fixtures

```sh
tests/e2e/capture_fixtures.sh
```

Runs a map operation whose *output* format is text YSON while its input stays
binary, so a shell one-liner can base64 the whole of stdin into one row. That is
the only way to get the raw job stream back out without a purpose-built binary.
It then re-runs the offline test against the fresh bytes.

`generate_fixtures.py` still builds the **table payloads** (`table_rows_*.bin`)
from the specification, without this project's encoder, and a test checks they
stay reproducible. Only the job-input framing is captured, because that framing
is the cluster's to define.

### What capturing changed

The synthetic fixture was wrong in ways only a cluster could reveal:

1. **`<table_index=0;>#`** — YTsaurus emits a trailing `;` *inside* the attribute
   block. The hand-built fixture omitted it. (The parser accepted both, so
   nothing was broken — but the fixture was not what a job actually sees.)
2. **A column value cannot carry attributes.** The fixture had one; YTsaurus
   rejects it at write time with `Table values cannot have top-level
   attributes`, so no job could ever receive it. Removed.

Both are now pinned by tests in `cat_e2e.rs`.

## Environment notes

Things that cost time here, recorded so they do not cost it again:

- **Binary YSON needs two packages**: `ytsaurus-client` alone fails with
  `YSON bindings required`; `ytsaurus-yson` supplies them.
- **`--spec` is YSON, not JSON.** `{mapper={memory_limit=536870912}}`, not
  `{"mapper":{...}}`, which fails with `Unexpected token ":"`.
- **`map-reduce` uses `--map-local-file` / `--reduce-local-file`**, not
  `--local-file`.
- **Control attributes for a reducer live under `reduce_job_io`**, not `job_io`:
  an operation with several job types gives each type its own section.
- YTsaurus publishes x86_64 images only. On Apple Silicon the cluster runs under
  emulation, which the YTsaurus docs say is not guaranteed to work — it did work
  here, but a Linux x86_64 host is the reliable option.
