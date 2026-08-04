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

The comparison is input-table read-back against output-table read-back, not
against the uploaded file. **The cluster re-encodes rows on ingest** — 309 676
bytes uploaded came back as 309 688 — so comparing against the upload would fail
for reasons that have nothing to do with the job.

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
