# Parity with the Go SDK's examples

There is no official Rust SDK for YTsaurus, but there is an official
[Go one](https://pkg.go.dev/go.ytsaurus.tech/yt/go), and its `yt/go/examples`
directory is twelve programs that between them define what an SDK for this
cluster is expected to do. This document maps every one of them onto this
workspace: what is covered, what is deliberately not, and what the exercise
changed.

The reason for doing it this way round is that a feature list written by the
people who built the thing is worth more than one written by the people
reimplementing it. Three of the twelve turned out to ask for something this
client could not do.

## The map

| Go example | Here | What it is about |
| --- | --- | --- |
| `compute-email-example` | [`examples/src/bin/selfrun.rs`](../examples/src/bin/selfrun.rs) | one binary that is both launcher and job |
| `count-names-example` | [`sort_reduce.rs`](../crates/ytsaurus-client/examples/sort_reduce.rs) | sort a table, then reduce over its key groups |
| `cypress-example` | [`cluster_info.rs`](../crates/ytsaurus-client/examples/cluster_info.rs) | connect, and read a node into a Rust type |
| `schema` | [`schema.rs`](../crates/ytsaurus-client/examples/schema.rs) | infer a schema from a struct, create, read back, alter |
| `table-usage` | [`table_usage.rs`](../crates/ytsaurus-client/examples/table_usage.rs) | typed rows in, typed rows out |
| `vanilla-example` | [`vanilla.rs`](../crates/ytsaurus-client/examples/vanilla.rs) | jobs with no input table, and reading their stderr |
| `admin` | — | cluster maintenance requests — see below |
| `discovery-client` | — | discovery service, over the bus protocol |
| `dynamic-table` | — | dynamic tables |
| `ordered-dynamic-table` | — | dynamic tables, without keys |
| `query-tracker` | — | Query Tracker, over a dynamic table |
| `tracing` | — | OpenTelemetry spans around client calls |

Six covered, six not — and the six that are not are one decision each, recorded
below rather than left as an absence a reader has to interpret.

Where this repository goes further than the Go example, it says so: `schema.rs`
creates all 26 column types the crate can name and watches the cluster refuse
what it should refuse, where the Go example is twenty lines with every error
discarded; `cypress.rs` is a tour of `list`/`copy`/`move`/`link`/`lock` that has
no Go counterpart at all.

## What the Go examples asked for, and got

Three things were missing here and are not missing now. All three came from
reading what the Go programs do rather than from a wish list.

**Typed table rows** — `table-usage`. Go writes structs to a table
(`writer.Write(v)`) and reads structs back (`reader.Scan(&c)`), and the SDK does
the encoding. This client had `write_table(&[u8])` and `read_table() -> Vec<u8>`,
so **eleven of its twelve examples hand-rolled the same YSON encode loop**. That
count is the finding: when every example writes the same twelve lines, the
twelve lines belong in the library.

```rust
client.write_table_rows("//tmp/contacts", contacts.iter())?;
let back: Vec<Contact> = client.read_table_rows("//tmp/contacts")?;
```

`write_table_rows` takes an iterator rather than a slice because the encoder
sits *inside* the request body: rows are serialised a bufferful at a time as the
connection asks for bytes, so a million rows cost one buffer.

Nine examples lost their encode loop to it. Three keep one, each for a reason
that survives the change: `schema.rs` needs a row the derive **could not**
produce — one with a required column missing — in order to watch the cluster
refuse it, and `streaming.rs` and `profile.rs` generate rows as raw bytes
because what they measure is the byte stream itself.

**Typed nodes** — `cypress-example`. Go reads `//@` into a struct of the three
attributes it cares about and ignores the other few dozen. `Client::get` handed
back a `YsonValue` to walk; `Client::get_as::<T>` hands back the shape you were
going to walk it into.

**Job stderr on the success path** — `vanilla-example`. Go lists an operation's
jobs after it *succeeds* and prints what each wrote. This client had
`list_jobs` and `get_job_stderr` — and no example called either, because the
only caller was the automatic failure report inside `wait_for_operation`. Two
public functions were shipping with nothing exercising them.

Two facts about that, both established against a cluster rather than assumed:

- **Stderr is kept for successful jobs**, with no spec option needed. The
  cluster returned a completed job's stderr verbatim.
- **Ask promptly.** `list_jobs` answers with an empty list for an operation that
  finished a while ago: the controller agent forgets its jobs, and a cluster
  with no job archive — a local one — then has nothing left to say. The harvest
  belongs right after `wait_for_operation`, which is where both examples do it.

## What is deliberately absent

Each of these is a decision already recorded in
[AGENTS.md](../AGENTS.md); none is an oversight.

**Dynamic tables** — `dynamic-table`, `ordered-dynamic-table`, and the data path
of `query-tracker`. AGENTS.md, *Non-goals*: "RPC proxy (custom binary protocol),
protobuf row format, **dynamic tables**, non-Linux targets, publishing to
crates.io", and hard rule 5: "**No scope creep.** … out of scope until a human
decides otherwise." An ordered table is a dynamic table without key columns, so
both examples land inside the exclusion. `mount_table`, `insert_rows`,
`select_rows` and `lookup_rows` are absent accordingly.

**The discovery service** — `discovery-client`. Excluded by mechanism rather
than by name: it does not speak HTTP at all, but the bus protocol with protobuf
bodies, which is the "RPC proxy (custom binary protocol), protobuf row format"
non-goal twice over.

**Tracing** — `tracing`. The backlog ranked it P3 #15 with the note "only worth
doing if a user asks", and the recorded status is that nobody has. Worth
separating if that changes: emitting a `traceparent` header is a small,
dependency-free change to the transport, while an OpenTelemetry exporter is a
dependency tree.

**Cluster maintenance** — `admin`. Not named in the non-goals, so this is an
open decision rather than a closed one. It is out of the client's stated charter
— "It does what launching a job needs … and nothing else" — and it is the only
surface in the set where getting it wrong on a shared cluster is destructive
rather than merely wrong. Two commands and two enums if the charter widens.

**Query Tracker** — `query-tracker`. Undecided. The example's data path is a
dynamic table, which settles the example, but the Query Tracker itself is plain
HTTP API v4 and would fit this transport unchanged.

## What the Go examples show that this client still cannot do

Found while checking the twelve programs against the API. None of these blocks
an example, which is exactly why they had not been noticed.

1. **No `abort_operation`.** A launcher can start an operation and wait for it,
   but cannot cancel one: interrupting `wait_for_operation` leaves the operation
   running on the cluster and spending its quota. Note the asymmetry — a
   `Transaction` aborts itself when dropped, so the crate takes cleanup
   seriously for transactions and not for operations. `suspend`, `resume` and
   `list_operations` are absent with it.
2. **No append.** `write_table`, `read_table` and their streaming forms take a
   `&str` path and send it as a bare YSON string, so `<append=%true>` — an
   attributed path — cannot be expressed at all, and every write replaces the
   table. Go's `ypath.Rich` carries `Append`, `Columns` and `Ranges` as a matter
   of course. Incremental append is an ordinary pipeline shape, and the word
   appears nowhere in this repository.
3. **No escape hatch.** `Transport::call` is `pub(crate)`, so a command this
   crate does not model cannot be sent at all: the answer to "can I do X" is
   "fork it". `Client::start_operation` taking a raw spec already sets the
   precedent that a raw door is acceptable here.
4. **No web UI links.** Three Go examples print `yt.WebUIOperationURL(...)`.
   Deliberately not built: the URL is `https://<host>/<cluster>/operations/<id>`
   and the cluster name is not derivable from a proxy address, so the choice is
   between asking the caller for something they would have to look up and
   printing a link that might be wrong.

Items 1 and 2 are worth a decision. The rest is noted so that the next person
reading the Go SDK does not have to find them again.

## Running the Rust side

Every example checks itself and exits non-zero when a check fails, so this is a
test suite that happens to be readable:

```sh
export YT_PROXY=http://localhost:8000
tests/e2e/run_local_cluster.sh
scripts/build-worker.sh

cargo run -p ytsaurus-client --example cluster_info
cargo run -p ytsaurus-client --example table_usage
cargo run -p ytsaurus-client --example schema
cargo run -p ytsaurus-client --example sort_reduce
cargo run -p ytsaurus-client --example vanilla
cargo run -p ytsaurus-examples --bin selfrun     # on Linux; see the README on macOS
```

[`tests/e2e/README.md`](../tests/e2e/README.md) holds the output of each, from
the runs that were actually made.

## Interop below the examples

The other kind of interop this project keeps is at the codec:
[`crates/ytsaurus-yson/tests/interop_tests.rs`](../crates/ytsaurus-yson/tests/interop_tests.rs)
runs against fixtures **written by `go.ytsaurus.tech/yt/go/yson`** — the same
family of code the cluster runs — in both directions and both formats. Example
parity says the two SDKs can do the same things; those fixtures say they agree
on the bytes.
