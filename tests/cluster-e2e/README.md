# End-to-end tests

Three layers. The fixture check runs in every CI invocation; the Docker-backed
checks run after a push to `main` (including a merged pull request).

| | Runs in CI | Needs a cluster | What it checks |
| --- | --- | --- | --- |
| [`crates/ytsaurus-job/tests/cat_e2e.rs`](../../crates/ytsaurus-job/tests/cat_e2e.rs) | every invocation | no | the compiled worker handles the real byte stream correctly |
| [`run_e2e.sh`](run_e2e.sh) | after a push to `main` | yes | the above, plus the scheduler, operation spec and re-encoding |
| [`rpc_e2e`](../../crates/ytsaurus-rpc/examples/rpc_e2e.rs) | after a push to `main` | yes | RPC proxy transactions, dynamic-table reads and writes |

The offline test's fixtures are captured from a real cluster: `cat_input.bin` is
the stream a job was handed on fd 0.

## Offline test (runs automatically)

```sh
cargo test -p ytsaurus-job --test cat_e2e
```

Runs the real `cat` binary the way the cluster runs it (input on fd 0, output
table 0 on fd 1, output table 1 on fd 4, wired by shell redirection) and
compares its output with the captured golden bytes. Covers descriptor
numbering, control records, table routing, byte-exact pass-through of non-UTF-8
data, empty input, and that a truncated stream fails the job.

## Cluster test

```sh
pip install ytsaurus-client ytsaurus-yson   # both: binary YSON needs the bindings
tests/cluster-e2e/run_local_cluster.sh              # HTTP :8000, RPC :8011, UI :8001
tests/cluster-e2e/run_e2e.sh
cargo run -p ytsaurus-rpc --example rpc_e2e
tests/cluster-e2e/run_local_cluster.sh --stop
```

`run_e2e.sh` uploads the table payloads, runs `cat` as a map operation and
asserts the output table reads back identical to the input, repeats with two
input and two output tables to exercise table switching, and finishes with a
`wordcount` map-reduce checked against a hand-computed result. Output is
compared read-back against read-back, because the cluster re-encodes rows on
ingest.

The same three checks run without Python as
[`examples/client_e2e.rs`](../../crates/ytsaurus-client/examples/client_e2e.rs):

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh cat wordcount
cargo run -p ytsaurus-client --example client_e2e
```

Each command the script sends has a `Client` method (`remove --recursive
--force` is `remove_tree`, `create --recursive` is `create`, `write-table
--format '<format=binary>yson'` is `write_table`), and the spec builders model
`enable_input_table_index` and `enable_key_switch` under `reduce_job_io`. The
client, unlike `yt map --dst`, does not create destination tables, so a
mistyped destination is an error; the example creates them. Both are kept:
`client_e2e` runs where there is no Python, and `run_e2e.sh` checks the
worker's output with a different implementation, the official Python client.

### Dynamic Skiff map

[`crates/ytsaurus-job/tests/skiff_cat_e2e.rs`](../../crates/ytsaurus-job/tests/skiff_cat_e2e.rs)
runs the real `skiff_cat` worker offline with non-UTF-8 `string32` data. With
the local cluster running, the client-driven check is:

```sh
./scripts/build-worker.sh skiff_cat
YT_PROXY=http://localhost:8000 cargo run -p ytsaurus-client --example skiff_launch
```

It writes a Skiff stream through `write_table_with_format`, runs a map whose
mapper format is Skiff, reads the output through `read_table_with_format`,
compares both rows element by element (including the non-UTF-8
`[0, 0xff, b'x']`) and checks that no extra rows arrived. It is not in CI and
has no captured fixture. Run on a managed multi-node installation on 2026-08-09:

```text
operation f532b7b6-7f7d2064-3f403e8-66d783be: completed (15s)
Skiff map succeeded: 2 rows
```

That verifies the dynamic Skiff map path only: one table, one output
descriptor, no table, range or row indexes, no key switch. Required test 4 of
[`docs/skiff-compatibility.md`](../../docs/skiff-compatibility.md) needs all of
those, and this run covers none.

### YQL, through the escape hatch

[`examples/yql_smoke.rs`](../../crates/ytsaurus-client/examples/yql_smoke.rs) is
phase 0 of [`docs/format-comparison.md`](../../docs/format-comparison.md). It
drives Query Tracker through `Client::raw_command` (`start_query`, `get_query`,
`list_operations`) and prints the bodies, without asserting on them.

```sh
export YT_PROXY=http://localhost:8000
cargo run -p ytsaurus-client --example yql_smoke
YT_YQL_QUERY='SELECT 1;' cargo run -p ytsaurus-client --example yql_smoke  # one query, verbatim
```

Run against the local Docker cluster on 2026-08-13:

```text
YQL runs: yes
UDF modules loaded: Re2::FindAndConsume, String::SplitToList, Unicode::SplitToList
table reference: `//tmp/ytsaurus_rs_yql/lines`, no USE
operations per query: 2 — see the paths above for where the ids appear
```

A repeated query is served from cache and spawns no operations, so every timing
needs `PRAGMA yt.QueryCacheMode = "disable"`. A YQL job here does not fit
YQL's 545 MB default memory limit: 576M fails, 640M passes, hence
`PRAGMA yt.DefaultMemoryLimit = "640M"` for a `map_reduce` stage. Both are in
the example's `PRAGMAS`. It prints where each spawned operation's id appears
in the `get_query` answer (`progress/yql_progress/<node>/remoteId`,
`…/yql_statistics/…/_id`) and reads
the operations back through `Client::list_operations` with
`OperationFilter::with_substring(query_id)`, which the cluster matches against
the title YQL gives each operation.

### The format comparison

[`examples/format_compare.rs`](../../crates/ytsaurus-client/examples/format_compare.rs)
is phases 1 and 2 of the same document: one task over one input table, as up to
eight legs, timed by the cluster. It reproduces every figure in
[`docs/benchmarking.md`](../../docs/benchmarking.md) §5 and needs a worker built
first.

```sh
export YT_PROXY=http://localhost:8000

# the comparison the document is about: nine mixed-type columns, no shuffle,
# one output table, three formats and a query
scripts/build-worker.sh sessionize
YT_COMPARE_TASK=project YT_COMPARE_MIB=48 YT_COMPARE_ROUNDS=9 \
    cargo run --release -p ytsaurus-client --example format_compare

# the default task, which shuffles; its numbers are not about formats
scripts/build-worker.sh wordcount
cargo run --release -p ytsaurus-client --example format_compare
```

`yt remove //tmp/ytsaurus_rs_compare --recursive` clears its leftover tables.
Every leg that produces rows is diffed against the first before any clock is
read, so a run that reports timings is one whose legs agreed. Expect about a
minute a round per leg on the local cluster at 48 MiB, and read the *paired by
round* block, not the `vs first` columns. The `project` task needs a YQL agent,
so run [the YQL check](#yql-through-the-escape-hatch) first on a fresh cluster.

### Without the `yt` CLI

These examples drive a cluster through `ytsaurus-client` alone, with nothing
Python on `PATH`:

```sh
export YT_PROXY=http://localhost:8000
scripts/build-worker.sh cat boom selfrun wordcount
cargo run -p ytsaurus-client --example client_e2e   # all of run_e2e.sh, no Python
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
cargo run -p ytsaurus-client --example rich_path    # which columns and rows a path names
cargo run -p ytsaurus-client --example transaction  # published all at once, or not at all
cargo run -p ytsaurus-client --example detach       # a transaction handed to a second client
cargo run -p ytsaurus-client --example cypress      # list, copy, move, link and lock
cargo run -p ytsaurus-client --example batch        # a dozen commands, one round trip, answers apiece
cargo run -p ytsaurus-client --example raw          # commands the crate does not model
cargo run -p ytsaurus-client --example yql_smoke    # does this cluster run YQL, and what does it answer
cargo run --release -p ytsaurus-client --example streaming  # a table bigger than the program
cargo run --release -p ytsaurus-client --example profile     # what the pilot spends on decoding
cargo run --release -p ytsaurus-client --example format_compare  # YSON, Skiff and YQL on one task

# One source, two build outputs: `cargo run` makes a host launcher on every
# platform, so point it at the static musl worker built above.
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-job --example selfrun

# An https cluster needs the launcher to have TLS, which `examples/` leaves off
# by default:
#   selfrun: https://… needs TLS, and this build has none: the `tls` feature of
#   ytsaurus-client is off. Enable it, or use an http:// proxy.
YT_WORKER_BINARY=target/x86_64-unknown-linux-musl/release-worker/selfrun \
    cargo run -p ytsaurus-job --example selfrun --features example-tls
```

`--features example-tls` changes only the launcher; the `musl` CI job asserts
the worker's dependency graph has no TLS. The client examples need no flag:
`ytsaurus-client`'s `tls` feature is on by default and only `examples/` turns it
off.

`diagnose` runs the `boom` worker, which panics on its first row, and checks
that the failed job's stderr comes back in the error, not only in the web UI. It
exits non-zero if the operation succeeds.

## Last run

On `ghcr.io/ytsaurus/local:stable` (Docker on macOS/arm64, x86_64 image under
emulation) unless another cluster is given. `run_e2e.sh` and `diagnose` ran on
2026-08-04; other dates are given where known.

`run_e2e.sh`:

```text
== Comparing input and output byte-for-byte
   ok identical (309688 bytes)
== Two input tables, two output tables, with table switching
   ok table 0 identical
   ok table 1 identical
== Wordcount map-reduce
   ok wordcount matches the reference (9 words)
```

`diagnose`:

```text
operation 1ba94195-3142e068-103e8-ffe93efc finished as failed: Failed jobs limit exceeded: Process terminated by signal 6
  job 24c164af-a273b7fd-10384-1000001 on localhost:24403: User job failed: Process terminated by signal 6
  stderr:
    boom: started, reading input
    ytsaurus-job: the job panicked and will fail.
    thread 'main' panicked at crates/ytsaurus-job/examples/boom.rs:37:17:
    boom: this job fails on purpose (row 1, 23 bytes)
   ok a failed job was reported
   ok the job's stderr came back
   ok the stderr is the job's own panic
   ok the job error explains the exit
```

`selfrun`, from both sides. On the macOS host the launcher is Mach-O and is
refused before upload:

```text
/…/target/debug/selfrun cannot run on a cluster node: it is not an ELF binary,
so a Linux node cannot exec it. Build the worker with scripts/build-worker.sh …
```

The direct-static path, the binary uploading itself, was verified by running the
musl build as the launcher inside Linux:

```sh
docker cp target/x86_64-unknown-linux-musl/release-worker/selfrun yt.backend:/tmp/selfrun
docker exec -e YT_PROXY=http://localhost:80 yt.backend /tmp/selfrun
```

```text
== Uploading the worker
   ok /tmp/selfrun -> //tmp/ytsaurus_rs_selfrun/selfrun
== Waiting for it
   ok completed
== Reading the result back
   ok 3 rows, 104 bytes
```

`sort_reduce` sorts a table by `word`, reduces over it and checks the totals:
4 rows, `alpha = 6`, `beta = 6`, `delta = 1`, `gamma = 7`, no extra groups. Four
groups rather than one means `key_switch` reached the reducer, in the plain
`job_io` section, since a reduce has one job type.

`idempotent`: starting an operation twice under one mutation ID returns the same
operation ID; a fresh mutation ID starts a second one.
Re-sending a `mutation_id` without the `retry` flag is refused with
`Duplicate request is not marked as "retry"`; `MutationId::as_retry()` sets it.

`cached_upload`, with a 491 KiB worker (the gap grows with the binary): the
first upload took 166 ms, the second was a cache hit in 32 ms finding the same
file under `//tmp/yt_wrapper/file_storage/new_cache/`, and the identity map
reproduced its input from the cached binary, keeping its `executable` attribute
and expected name.

`statistics`: seven rows in, three of them without a `key` column, which the job
drops. The operation succeeds; only the statistic shows the dropped rows:
`rows/read` 7, `rows/rejected` 3, `bytes/read` 147, each filed as
`{"$"={completed={map={count=1;max=…;min=…;sum=…}}}}`.

`schema` creates a table from `#[derive(TableRow)]` and reads back
`<strict=%true;unique_keys=%false>[{name=host;required=%true;sort_order=ascending;type=utf8};…]`:
the columns as given, the table sorted. All 26 column types the crate can name
are accepted. A row missing a required column is refused with
`write_table: cluster error 307: Required column "size" cannot have "null" value`.
On a table with 2 rows, a struct that gained an optional field widens the
schema; dropping a column (error 316), adding a required one and changing a
type are refused, and an empty table accepts all three. `sort_order=descending`
is refused with error 314, as documented; the run checks it, so a cluster that
enables it makes the run say so. The messages are in the
[protocol reference](../../docs/protocol-reference.md#table-schemas).

`append`: a plain write put 3 rows; a second plain write replaced them with 2;
an appending write made 6. An append to a sorted table kept it sorted, and a key
smaller than the last was refused with `write_table: cluster error 301: Sort
order violation: [0#15] > [0#0]`. An append to a missing table was refused.
60 000 rows in 12 pieces:

```text
   appending     0.60s       60000 rows sent
   rewriting     1.03s      390000 rows sent   (6.5× the data)
```

`abort`: the scheduler took the request in 399 ms and the operation was already
`aborted`, with 0.0 s of waiting; its job stopped 0.0 s after it. The error
document read back `Operation aborted by user request: stopped by the abort
example`. A second abort was refused with `abort_operation: cluster error 200:
No such operation 675da9d0-…`: the scheduler had let go of the operation.

`rich_path`, on 2026-08-09, over a table keyed `(host, path)` holding `(a,/x)
(a,/y) (b,/x) (b,/y) (c,/x)`, checking which rows come back (the wire shapes are
pinned offline). `keys(a..b)` returned `a/x a/y` (2 rows); `keys(a..=b)`
added all of host `b` (4 rows), the mixed `key`/`key_bound` entry accepted.
`keys((Excluded(a), Unbounded))` dropped every row of host `a`. `exact_key(a)`
returned every row of host `a`, as `keys(a..=a)` does, and a full key selected
its single row. A write naming a row range and a write whose path string spells
one were both refused, leaving 5 rows. Why `key` and `key_bound` disagree on a
prefix: [protocol reference](../../docs/protocol-reference.md#selecting-columns-and-rows-on-a-path).

`table_usage` and `cluster_info` (Go's `table-usage` and `cypress-example`):
100 contacts written as Rust values; `row_count` 100, which
the attribute map read into a one-field struct agrees with; all 100 read back
equal, in order. A struct naming one column reads 100 names (the first
`Some("Gopher 0")`) but cannot write: the row lacks three required columns,
`write_table: cluster error 307: Required column "email" cannot have "null"
value`. The cluster, created at 2026-08-04T16:42:47.385970Z and calling itself
`"locasaurus"`, offered 48 attributes and the struct named 3. A type that does
not fit is an error naming the path, not a panic:
`get: … invalid type: string "2026-08-04T16:42:47.385970Z", expected u64`.

`transaction` runs `launch`'s map so nothing exists until commit. A table
created in transaction `4-29da-10001-6f45` is seen inside it and not outside;
aborting leaves nothing; a launcher that fails halfway loses its half-written
table with no cleanup code, `?` dropping the handle. After the operation
completes, outside the transaction the old result is still the result and the
worker is not in Cypress; the commit publishes both.
A gone transaction answers `create: cluster error 500: Error resolving path
//tmp/ytsaurus_rs_transaction/never: No such transaction 4-2c2e-10001-6b6e`. A
2 s transaction held for 6 s committed; without the ping thread the cluster
would have aborted it four seconds earlier.

`profile` measures the pilot's decoding share; its results and the Skiff-default
question are in [`docs/benchmarking.md`](../../docs/benchmarking.md). It
defaults to five rounds; at three, on a production cluster, it declined to
answer.

`streaming` wrote about 64 MiB from a generator (1 242 757 rows, 53.5 MiB on the
cluster) at 2.9 MiB peak RSS; streamed it back, rows counted and values summing
to what was written, at 3.8 MiB; and read the table into memory, 67.7 MiB in
hand, at 74.7 MiB. Streaming cost 1.0 MiB of peak RSS, reading it in 70.9 MiB.
`ru_maxrss` is a high-water mark, so no spike hides in these figures.

`cypress`: dated runs, a `latest` link, three transactions competing for one
lock. `list` returned `["2026-08-02", "2026-08-03", "2026-08-01"]`, unsorted;
listing a table failed, `list: cluster error 103: "List" method is not
supported`. A second copy was refused, `copy: cluster error 501: Node … already
exists`; `copy_replacing` overwrote it; a move left nothing behind.
`latest&/@target_path` was `…/runs/2026-08-01` while `latest/@type` was the
target's `table`. A staging table moved over the live one in a transaction was
invisible until the commit. The second exclusive lock was refused naming the
winner (`lock: cluster error 402: Cannot take "exclusive" lock … since
"exclusive" lock is taken by concurrent transaction …`), though the loser could
still pin the version it was reading. A wait that could never end was still
pending after 2 s; the waitable lock was granted (`4-1d5ab-100c8-57bfbc50`) 3 s
in, once the holder went away.

`vanilla`, the Go SDK's `vanilla-example`: operation
`b528b474-8714f38c-103e8-2ab7da1e` ran 3 jobs with no input and completed. The
cluster still listed all 3, and their stderr (`shards: job 1 of 3`, and so on)
survived success. They wrote 3 rows, one per job, identifying themselves as
`{0, 1, 2}` by cookie, which is all a vanilla job has to divide the work by;
their slices add up to 500500 and cover each of the 1000 numbers once. Stderr
must be read promptly; see the
[protocol reference](../../docs/protocol-reference.md#jobs-listed-and-read).

`raw`, on 2026-08-06: four commands with no method on `Client`, through
`Client::raw_command` and its three siblings. `get_supported_features` answered
`{features=…}` with the nine keys listed in the
[protocol reference](../../docs/protocol-reference.md#commands-and-verbs), 71
compression codecs among them. A file of 4 000 000 bytes went up and came back
byte for byte, the reader counting the same bytes, neither direction holding
the file; that is the wire shape `Client::read_file` and
`Client::read_file_streaming` are built on, re-verified through them with an
empty file, a `compression_codec=zlib_6` node and a missing path. A node staged
by a raw command was invisible to a second client until the commit, which would
fail if the raw door bypassed `Transport::in_transaction`. A read marked safe to
repeat got 8 keys from the scheduler. A command name that would change the URL,
and a payload on a GET, were refused rather than sent or dropped.

`client_e2e`, on 2026-08-06, reached `run_e2e.sh`'s numbers without the `yt`
CLI: fixtures of 309 676 and 175 bytes read, both workers uploaded to
`//tmp/ytsaurus_rs_e2e`, the `cat` map's output identical at 309 688 bytes, the
table-switching outputs identical at 309 688 and 180 bytes, and the wordcount
matching the reference (9 words). 309 676 bytes going up and 309 688 coming back
is the cluster's re-encoding. The first two checks upload
`fixtures/table_rows_*.bin` as bytes, as the shell script pipes them into
`yt write-table`; `generate_fixtures.py` builds those from the binary YSON
specification without this project's encoder. The wordcount input goes through
`write_table_rows`, this project's encoder, because that check asserts a set of
counts, not a byte sequence.

## Refreshing the golden fixtures

```sh
tests/cluster-e2e/capture_fixtures.sh
```

Runs a map whose output format is text YSON while its input stays binary, so a
shell one-liner can base64 the raw job stream into one row, then re-runs the
offline test on the fresh bytes. `generate_fixtures.py` still builds the table
payloads (`table_rows_*.bin`) from the specification, and a test checks they
stay reproducible. Only the job-input framing is captured, because it is the
cluster's to define.

Capturing corrected two errors in the earlier hand-built fixture, both now
pinned by tests in `cat_e2e.rs`: YTsaurus writes `<table_index=0;>#` with a
trailing `;` inside the attribute block, which the fixture omitted (the parser
accepts both); and the fixture had a column value with attributes, which
YTsaurus rejects at write time (`Table values cannot have top-level
attributes`).

## Environment notes

- The `yt` CLI's pitfalls (two Python packages for binary YSON, `--spec` in YSON,
  `--map-local-file` / `--reduce-local-file`, `reduce_job_io`) are in the
  [protocol reference](../../docs/protocol-reference.md#cluster-gotchas).
- YTsaurus publishes x86_64 images only. On Apple Silicon the cluster runs under
  emulation, which the YTsaurus docs say is not guaranteed to work. It worked
  here; a Linux x86_64 host is the reliable option.

### Against a cluster that is not the local one

A managed installation differs from `ghcr.io/ytsaurus/local:stable` in four ways,
and each stops the suite. On a managed multi-node installation on 2026-08-09,
all 24 examples passed with the environment below. The names are placeholders.

```sh
export YT_PROXY=cluster.example.net

# 1. A private CA. Without this the first request fails with
#    `invalid peer certificate: UnknownIssuer` (compiled-in Mozilla roots, not
#    the machine's store). The `yt` CLI reads the same variable; the Go SDK
#    takes the system store.
export YT_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

# 2. Heavy proxies in another domain. `/hosts` names 79 under *.proxy-zone.net;
#    the default rule refuses all 79, and every upload fails at the control
#    proxy with `Control proxy may not serve heavy requests with input data`.
export YT_HEAVY_PROXY_DOMAINS=proxy-zone.net

# 3. A shared file cache only its owner may write to. Only `cached_upload`
#    cares, and it brings its own at //tmp/ytsaurus_rs_cached_cache. Set this
#    only to point it elsewhere.
# export YT_FILE_CACHE=//tmp/ytsaurus_rs_cache

# 4. https: the launcher needs TLS; the musl worker still must not have it.
cargo run -p ytsaurus-job --example selfrun --features example-tls
```

From that run:

- No pool configuration was needed: the default `physical` tree and the
  ephemeral pool the scheduler gives a user carried all 24 examples, and
  `//tmp` was writable.
- `YT_PROXY=hume` becomes `https://hume`, which resolves only through the
  machine's resolver search list; `YT_PROXY_SUFFIX=.yt.example.net` completes a
  dotless name as the Go SDK does. No suffix is compiled in.
- `streaming` moved its 64 MiB in 8 s. The RSS baseline was 50.0 MiB against
  2.9 MiB locally (the release build and the host allocator, not a regression),
  and streaming cost 0.0 MiB of growth against 25.7 MiB buffered. `profile`'s
  production numbers are in [`docs/benchmarking.md`](../../docs/benchmarking.md).

`Client::from_env` reads `YT_HEAVY_PROXY_DOMAINS`, `YT_FILE_CACHE` and
`YT_PROXY_SUFFIX` for every example; in Rust they are
`Client::with_heavy_proxies_under`, `Client::with_file_cache` and a spelled-out
address. `YT_HEAVY_PROXIES_ANYWHERE=1` removes the domain rule, as the official
Go SDK does with `/hosts`.
