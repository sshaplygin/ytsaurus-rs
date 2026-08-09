# Changelog

## 0.2.5 - 2026-08-10

First release on crates.io.

### A managed installation is configurable from the environment

Everything here comes from one run of the whole example suite against a managed
multi-node installation rather than the local Docker cluster. Four of the
differences stopped the suite outright, and none of them could be answered by
configuration: every example builds its client with `Client::from_env`, so a
policy settable only in Rust is a policy an example cannot be run under. The
run needed a source patch, and that is the bug these entries fix.

- **Added** `Client::with_heavy_proxies_under(domains)`: the configured
  address's own domain **plus** the ones named. The installation published all
  79 of its heavy proxies in a zone of its own — a domain the configured address
  does not share — so the default rule refused every one of them and no heavy
  command could be sent at all, each upload dying at the control proxy with
  `Control proxy may not serve heavy requests with input data`. The two settings
  that existed were `with_heavy_proxies_in`, which meant writing 79 names down
  and re-writing them whenever a proxy rotated, and `with_heavy_proxies_anywhere`,
  which is the rule removed. A domain is what an installation actually has.

  It is still a suffix rule and still worth what one is worth — a guard against
  a typo and an obviously foreign name, not a boundary that holds a credential;
  `with_heavy_proxies_in` remains the boundary. Entries are normalised the way
  people write them (space, a leading or trailing dot, a leading `*`, a scheme,
  a port), and one left with **no dot in it** is dropped rather than honoured:
  `net` would admit every `.net` host the cluster could name, which is
  `with_heavy_proxies_anywhere` by accident. Worth recording beside this: the
  **Go SDK filters `/hosts` not at all**, so `anywhere` is not a weakening
  relative to the official client — it is the official client's behaviour.

- **Added** four variables to `Client::from_env`. Each is inert when unset, and
  all of them — `YT_PROXY` now included — are **trimmed**, with a variable set
  to nothing read as unset. That last part changes `YT_PROXY` itself:
  `YT_PROXY=" http://localhost:8000 "` used to reach `Client::new` verbatim and
  fail as a malformed URL, and `YT_PROXY=` used to address `https://`.

  | Variable | Effect |
  | --- | --- |
  | `YT_PROXY_SUFFIX` | Completes a bare cluster name — `YT_PROXY=hume` with `YT_PROXY_SUFFIX=.yt.example.net` addresses `hume.yt.example.net` — behind the Go SDK's gate: no colon, no dot, no `localhost`. No suffix is compiled in, because this client is not one installation's. |
  | `YT_HEAVY_PROXY_DOMAINS` | Comma- or space-separated, into `with_heavy_proxies_under`. |
  | `YT_HEAVY_PROXIES_ANYWHERE` | `1`, `true` or `yes`, into `with_heavy_proxies_anywhere`. Applied after the domains, so the wider of the two wins rather than whichever was exported last. |
  | `YT_FILE_CACHE` | Into `with_file_cache`, for an installation whose shared cache is read-only. |

  The environment can **widen** the heavy-proxy rule and cannot narrow it:
  `with_heavy_proxies_in` is the mode that is a boundary, and a boundary a
  variable could set is a boundary a variable could move.

  `YT_PROXY_SUFFIX` also makes the label rule reachable without a resolver
  search list. That rule matches a dotless `YT_PROXY` as a label of the
  discovered name, and until now a dotless `YT_PROXY` could only be connected to
  if the machine's DNS configuration completed it.

- **Changed** the message a rejected root store produces. `invalid peer
  certificate: UnknownIssuer` is the whole of what a cluster behind a private CA
  says on its first request, before any YTsaurus logic runs, and it named
  neither `YT_CA_BUNDLE` nor the `platform-verifier` feature — on a machine
  where `curl` reaches the same cluster, nothing else about it suggests whose
  roots were consulted. `ClientError::Transport` now carries both, for that
  verdict only: `NotValidForName` is a certificate that does not cover the host
  asked for, which no root store mends. Classified through `retry`'s existing
  narrowing rather than by looking for the word, so
  `Other(OtherError("UnknownIssuer lookup failed"))` — a transient
  platform-verifier condition the client deliberately retries — does not collect
  advice to enable the verifier it already has.

- **Changed** both refusals an operator reads when `/hosts` is declined. They
  offered `with_heavy_proxies_in` and `with_heavy_proxies_anywhere`: write out
  79 names, or take the rule away. Both now offer all three, with the
  environment spellings beside them — the announcement made once at the first
  lookup, and the sentence appended to the cluster's own refusal on every heavy
  command after it. A domain that could not be used — one with no dot in it,
  which would admit a whole top-level domain — is named there too, rather than
  dropped in silence and leaving the setting looking unread.

- **Changed** `cached_upload` to bring its own file cache. It clears one entry
  so the first upload is a real miss, and a shared managed cache refuses that —
  `remove` is denied to an ordinary user, code 901 — which stopped the example
  in setup on an installation where the client itself was fine. `YT_FILE_CACHE`
  points it back at a shared one, and it now stops with an explanation instead
  of timing two warm uploads against each other.

- **Changed** `profile` to build its closing caption from the run — the cluster's
  own `//sys/@cluster_name`, the rounds and the size — rather than printing a
  hardcoded description of the laptop it was written on, and raised the default
  `YT_PROFILE_ROUNDS` from 3 to 5: at 3 the example could not separate the phases
  on a shared cluster and said so, which is the guard working and a poor default.

### A transaction can outlive its handle

- **Added** `Transaction::detach` (#13): stops the keep-alive thread and
  leaves the transaction running, returning the id — what C++ spells
  `ITransaction::Detach()`. Nothing is committed, aborted or otherwise
  decided, so from there the transaction lives on the cluster's terms: it
  expires its timeout after its last ping, 30 s by default, unless whoever
  received the id keeps it alive. The keep-alive thread is asked to stop and
  then waited for, **for up to five seconds** — a detach racing its own ping
  neither panics nor, inside that bound, leaves a request behind to restart
  the expiry clock after the caller has finished reasoning about it. The
  keep-alive may still get one last ping away before it sees the stop, and
  that ping is what the wait is for. The bound is deliberate, in place of the
  ping's own request budget of `min(interval / 2, 120 s)` — two minutes for an
  hour-long transaction against a proxy that has stopped answering — because
  `detach` reads as instant at every call site.

  **The bound is reachable, and then the promise stops.** Five seconds covers
  a ping's whole budget while the transaction's timeout is under 30 s and
  equals it at the 30 s default, so at or below the default the wait always
  ends in the thread's exit. Above it — every long-running launcher
  transaction — a ping stalled on a proxy that has stopped answering outlasts
  the wait, is left in flight, and can reach the master *after* `detach`
  returned, restarting the clock there: the transaction then lives a full
  timeout from wherever that ping landed rather than from the detach. Nothing
  leaks — the thread re-reads the stop flag as soon as its ping ends, so at
  most one ping is outstanding and it exits inside that same budget — but a
  caller above the default cannot treat `detach` as the transaction's last
  ping. Both bounds are on `Transaction::detach`.

- **Added** `Client::attach_transaction(id)`: the receiving half. Turns an id
  into a real `Transaction` — bound client, ping thread, working
  `commit`/`abort`/`ping` — where `with_transaction` only binds commands. The
  ping interval is read from `#<id>/@timeout`, because the id alone does not
  carry it; that round trip is also what makes attaching to a transaction that
  is gone fail immediately, with the cluster's resolve error naming the id.
  **Dropping an attached handle detaches rather than aborts**, following the
  C++ destructor's line: an attacher's `?` must not destroy work the process
  that started the transaction still holds a handle to. The handle always
  pings — Go's `AttachTx(id, {AutoPingable: false})` maps onto
  `with_transaction` plus the by-id commands below.

  It also **pings once before returning**, a second round trip, because
  `@timeout` is the *configured* lifetime and says nothing about how much of
  it a handoff has already spent: an attach at t=21 s of a 30 s transaction
  last pinged at t=0 would otherwise schedule its first ping for t=31 s, one
  second after the cluster had expired it — and the reply, `No such
  transaction`, would stop the keep-alive thread silently. Any handoff slower
  than `timeout × 2/3` lost the transaction that way. The ping restarts the
  clock at the attach and doubles as the probe, so a transaction that died in
  the handoff is this call's error rather than a later command's.

- **Added** `Transaction::is_lost`: whether the keep-alive has given up. It
  stops on its own for exactly one reason — a ping answered "no such
  transaction", which is final — and that exit used to be invisible, leaving a
  handle that pings nothing looking exactly like a healthy one. Go reports the
  same thing by pushing on `Tx.Finished()`; this is polled.

  **False is not "something is pinging"**, and the doc now says which other
  states read false: a thread that never started because the spawn failed, and
  a thread that panicked (nothing on the ping path panics as it stands). A
  ping does not expose either — it answers for the transaction, not for the
  thread. `is_lost` is also `&self` where `detach` consumes the handle, so
  after a detach the only probe left is `Client::ping_transaction` on the id.

- **Added** `Client::ping_transaction`, `Client::commit_transaction` and
  `Client::abort_transaction`, taking the bare id, so a process that holds
  nothing else can finish someone else's transaction. Commit rides under a
  mutation ID (it is not idempotent — the second commit is answered `No such
  transaction`, which reads like the first one failed); abort is retried
  freely on the cluster's own forgiveness (aborting a transaction that is
  gone answers `{}`); a ping doubles as the liveness probe.

- **Unchanged, and deliberately**: dropping a transaction this process
  *started* still aborts it. That is what makes `?` safe inside a
  transaction, and `examples/transaction.rs` still demonstrates exactly that;
  `tests/transaction_lifecycle.rs` pins all four drop/detach shapes at the
  wire level, stub-served in-process. The new `detach` example runs the
  handoff against a cluster: start, detach, drop, attach from a second
  client, hold past the transaction's own timeout, commit.

- **Note for the paranoid**: `mem::forget` on a `Transaction` is not a way to
  hand it on. It leaks the keep-alive thread, which goes on pinging for the
  life of the process and holds the transaction and its locks open
  indefinitely. `detach` is the sanctioned form, and it says so now. Nothing
  stops two attaches to the same id either: each gets a handle and a thread of
  its own, they ping the same transaction twice as often, and whichever
  finishes it first decides it — deliberate, since a second process attaching
  is the whole point, and documented on `attach_transaction`.
### A file can be read back

- **Added** `Client::read_file` and `Client::read_file_streaming` — the mirror
  `write_file` shipped without, and the sharpest asymmetry in the crate until
  now: a worker binary could be uploaded and never fetched back (#10). The
  pair takes the shape of `read_table`'s: buffered for a result a launcher
  inspects, streaming for a file that does not fit — which a file is exactly
  the thing to be. `read_file` is a heavy command without input data, so both
  go to the proxy the cluster names, as the table reads do, and an
  installation whose control proxy answers heavy reads with a cross-host 307
  is handled by the same routing rather than by following the redirect with
  the token stripped. Verified against a local cluster: 4 MB of non-UTF-8
  bytes round-trip byte-for-byte through both halves, and an empty file reads
  back empty. The buffered half holds the whole file in memory, and the cap on
  that is the bullet below.

- **Added** the completeness check the buffered read needs where a table read
  already had one. The proxy reports a mid-stream failure in a trailer `ureq`
  cannot see, and a file's bytes carry no framing — where a truncated table
  leaves a record that does not parse, a truncated file just ends, looking
  exactly like a shorter file. So `read_file` compares the body against the
  node's `@uncompressed_data_size`, one light `get` after the heavy read, and
  a body of any other length is an error naming both numbers. The attribute is
  the *logical* size — verified with `compression_codec=zlib_6`, 1 000 000
  bytes reading back whole off 4 214 on disk — and there is no `@file_size`,
  whatever the name suggests: asked, the cluster answers `Attribute
  "file_size" is not found`. An answer that is not an integer fails the read
  too, because a check that quietly stopped checking would be worse than none.
  The streaming path cannot check — the point is not to have the whole thing —
  and `FileReader` says what to compare instead: `bytes_read` against the same
  attribute.

- **Added** `FileReader`, the streaming read's return type — `ResponseReader`
  under the name the file path uses, exactly as `TableReader` is for tables.

### The 512 MiB cap on a buffered response, which was not one

- **Fixed** the cap itself, which counted the wrong bytes and so bounded
  nothing worth bounding. `ureq`'s `limit()` wraps the *raw* body source and
  builds the gzip decoder on top of it, so the number it takes is a limit on
  what arrives on the wire — and every request this crate sends carries
  `Accept-Encoding: gzip`. Measured against a cluster: a 600 MiB file of zeros
  read back through `read_file` crosses the wire in 611 522 bytes, and the code
  as it stood — `.limit(536870912).read_to_vec()` — handed back all
  629 145 600 without an error. At that ratio a 512 MiB wire cap admits
  hundreds of gigabytes into memory. The cap now sits **above** the decoder and
  counts the bytes that land in the `Vec`, so the promise the documentation
  made is the one the code keeps: the same read is now refused, and
  `read_file_streaming` still moves all 600 MiB of it. A body of *exactly* the
  cap also passes now, where the old guard failed it — `ureq`'s reader errors
  on the read that finds the end, so "larger than the limit" was a byte off,
  and deflate expands what it cannot compress, so the wire backstop underneath
  has to allow the largest permitted body room to arrive *bigger*: 4 096
  incompressible bytes gzip to 4 119. The backstop is `deflateBound`'s worth of
  slack, which still bounds the case it exists for — an endless stream of empty
  deflate blocks, which decodes to nothing and so is invisible to a cap on
  decoded bytes — at about 584 MiB of transfer.

  **Observable:** a buffered response that decodes to more than 512 MiB used
  to succeed, allocating however much it decoded to. It now fails.
  `read_table_streaming`, `read_file_streaming` and `write_table` are
  unaffected — none of them buffers.

  **The cap is on what is held, not on what a process needs.** The bytes land
  in a `Vec` that grows by doubling and copies as it grows, so peak residency
  runs above the ceiling: measured, a 600 MiB read refused by the cap peaks at
  611 385 344 bytes of resident set and one that holds 512 MiB peaks at
  544 178 176, with about 1.5× the worst case the growth implies. And it is
  the buffered commands and uploads that are capped, not the crate: two error
  paths — the non-2xx branch of a streaming open, and the `/hosts` lookup —
  still take `ureq`'s wire-only default.

- **Changed** the error class for a buffered response over the cap, from
  `ClientError::Transport` to a new `ClientError::ResponseTooLarge { command,
  limit }`. It reaches `read_table`, `read_table_with_format`,
  `read_skiff_table`, `read_table_rows`, a buffered `raw_command`, and any
  light command with an answer that large. A caller matching
  `ClientError::Transport { .. }` to back off and retry will stop matching —
  which is the point. `Transport` means *the request never got its answer*, and
  every predicate that narrows it by looking inside (`is_retriable`, and
  through it `worth_asking_again` and `attributable_to_the_host`) answered
  `true` here, because `BodyExceedsLimit` is not an `Io` error. So an over-cap
  heavy read was retried, and it **dropped a healthy data proxy from the pool**
  — enough of them empty it, and the fallback window then answers unrelated
  writes with the control-proxy refusal #30 exists to prevent. The new variant
  is retried by nothing and blamed on nobody, and its message names the cap and
  the streaming half of the same command. `ClientError` is `#[non_exhaustive]`,
  so a new variant cannot break an exhaustive match.
### The read half of a rich path

- **Added** column and row selection to `TablePath`: `columns(…)` and
  `range(…)`, with `RowRange` and `Key` behind the latter. Three columns of a
  hundred rows now cost three columns of a hundred rows on the wire, not the
  whole table. The selections travel as the `columns` and `ranges` attributes
  *on the path* — the same mechanism as `<append=%true>`, and for the same
  reason: a sibling parameter is silently dropped. Spellings are the
  [rich YPath reference](https://ytsaurus.tech/docs/en/user-guide/storage/ypath),
  and `ypath.Rich` in the Go SDK renders the identical shapes.

- **Added** row ranges as plain Rust ranges — `path.range(0..100)`,
  `range(100..)`, `range(..)` — because Rust's `..` and the cluster's
  `row_index` limits mean the same thing: inclusive below, exclusive above.
  Key ranges on sorted tables take the same shape,
  `RowRange::keys(Key::from("a")..Key::from("b"))`, and the inclusivity
  travels with the range: the two bounds the `key` selector says natively are
  sent as `key`, the other two (`..=`, `Bound::Excluded` below) as the
  cluster's `key_bound=[relation;prefix]` form, whose relation is the only one
  the reference allows on that side. `RowRange::exact_key` is the `exact`
  selector. A range never mixes `exact` with a limit because no constructor
  can write that.

  **The two selectors compare a short key by opposite rules, and the difference
  is a group of rows.** Measured on a local cluster, table keyed
  `(host, path)`, rows `(a,/x) (a,/y) (b,/x) (b,/y) (c,/x)`: `keys(a..b)`
  returned the two `a` rows, `keys(a..=b)` returned four — all of host `b` —
  and `keys((Excluded(a), Unbounded))` returned three, having dropped every row
  of host `a` rather than one row. `key` compares component-wise with the
  shorter tuple smaller; `key_bound` truncates the row's key to the bound's
  length first, so every row sharing the prefix compares equal to it. The same
  run settled the other open question: a range entry carrying `key` on one side
  and `key_bound` on the other — what `keys(a..=b)` sends — is accepted, though
  the reference documents the two selectors only separately.
  `examples/rich_path.rs` is that run, and it checks itself.

- **Added** `yson_build::uint`, without which the top half of a `uint64` key
  column had no spelling: the `From` shortcuts on `Key` give int64 for an
  integer, so every key above `i64::MAX` was unnameable. Measured, the type
  itself is not the obstacle — on a `uint64`-keyed table `{exact={key=[42]}}`
  and `{exact={key=[42u]}}` both returned the row — but the row keyed
  `18446744073709551615u` came back only for the uint spelling.

- **Changed** `read_table`, `read_table_with_format`, `read_skiff_table`,
  `read_table_rows` and `read_table_streaming` to take `impl Into<TablePath>`,
  as the write methods already did. Call sites that pass `&str`, `String`,
  `&String`, `&&str` or `Cow<str>` compile unchanged; a call that leaned on
  inference (`path.as_ref()`) may need to say `&str` once.

- **Breaking** a *write* to a path carrying a read selection is refused
  locally, as `ClientError::Config`, before anything is sent. The cluster
  ignores a selection on a write and replaces the whole table with a 200 —
  measured in both spellings: `write_table_rows("//tmp/t[#0:#2]", rows)`
  replaced everything and reported success, and a `write_table` whose path
  carried `ranges` as a typed *attribute* did exactly the same, 200 and three
  rows replaced by one — so the only honest write is no write.
  The same refusal covers selection syntax spelled into the path *string* on a
  write: a leading `<…>` block, or an unescaped `[` / `{` (a literal bracket
  in a node name is escaped, `\[`, and still writable). Reads keep taking
  string-spelled paths verbatim, because the cluster honours them there and
  always has — except where the typed API would spell **the same kind** of
  selection a second time, or where the string opens with `<…>`. Measured in
  the shape this client sends (attributes hung outside a YSON string node), a
  doubled kind is not a draw: the **attribute wins and the caller's
  string-spelled half is discarded**, at 200, with nothing said.
  `<ranges=[…0:2]>"//tmp/t[#3:#5]"` returned rows 0–1, not 3–4, and
  `<columns=[n]>"//tmp/t{k}"` returned column `n`, not `k`. Rows against
  columns *compose* and are sent — `<columns=[n]>"//tmp/t[#3:#5]"` gave rows
  3–4 carrying only `n`. A leading `<…>` is refused whatever it holds, not
  because the cluster objects — it composes there too — but because this
  client cannot parse the block to see which attribute it names, and if it
  names the one being added the caller's is discarded in silence.

  **In plain terms, one working spelling stops working:
  `write_table("<append=%true>//tmp/t", rows)` used to append and now returns
  `ClientError::Config`.** The cluster did parse and honour that string, so
  nothing was being silently ignored there — but this client does not parse a
  path string at all, and the one syntax covers both `<append=%true>`, which
  the cluster honours on a write, and `<ranges=…>`, which it drops while
  replacing the table. Telling them apart would mean parsing rich YPath;
  refusing is the only answer that is right for both. `TablePath::append()` is
  the replacement, and `Client::raw_command` takes any other write attribute.
  Nothing in this repository used the string spelling, so the break is
  external-only.

- **Breaking** `TablePath` no longer derives `Eq` (a key bound may hold a
  double, which has no `Eq`); `PartialEq` remains. Neither `TablePath` nor
  `RowRange` derives `Default`: `TablePath::default()` is the empty path,
  which names no table and no caller wanted. `read_skiff_table` refuses a path
  whose **columns** are selected twice — `TablePath::columns`, and now also
  `{…}` in the path *string*, since the Skiff format's fields become a
  `columns` attribute whether the caller named one or not. Measured, the
  synthesised attribute wins (`<columns=[n]>"//tmp/t{k}"` came back as column
  `n`), so the Skiff tuple stays aligned with its schema and nothing decodes
  wrong — what is lost is the caller's own `{…}`, discarded at 200 without a
  word, and refusing is how they hear about it. **A row range joins a Skiff
  read freely, typed or string-spelled** — `<columns=[n]>"//tmp/t[#3:#5]"`
  answered 200 with rows 3–4 carrying only `n`, since ranges pick rows and the
  schema picks columns.

- **Breaking** a row range asking for rows no table has is refused rather than
  sent, in both selectors: one that runs backwards (`rows(5..3)`,
  `keys(b..a)`) and one with a negative row index (`rows(-5..2)`). Measured,
  the two fail differently. A backwards range is answered 200 with no rows —
  `{lower_limit={row_index=5};upper_limit={row_index=3}}` and
  `{lower_limit={key=[3]};upper_limit={key=[1]}}` both came back empty. **A
  negative row index is clamped to 0 and the read succeeds**:
  `{lower_limit={row_index=-5}}` returned all five rows of a five-row table and
  `-5..2` returned rows 0 and 1, so a negative lower limit reads exactly as `0`
  would, and only a negative *upper* limit comes back empty. A bound that
  arrives only from arithmetic that went wrong, and is then silently replaced
  by one that reads from the start of the table, is worth an error. An *empty*
  range is still fine: `rows(5..5)` is legal on a slice and honestly asks for
  no rows, and `keys(a..a)` likewise.

  **`columns([])` is not in that set and is sent.** Measured, `<columns=[]>`
  answers 200 with one empty map per row and composes with a range, so it
  counts the rows of a range — or probes whether a key range holds any — with
  no column bytes on the wire, which `Client::row_count` cannot do, reading as
  it does the whole-table `@row_count` attribute.

### Heavy proxies: a pool, picked at random, refreshed — never one host for life

- **Changed** how heavy commands choose their proxy, to parity with the C++
  and Go SDKs (#40). The `/hosts` answer used to be resolved once and its
  first name pinned for the client's whole life, stepping to the next name
  only on a failure the *lookup's* predicate recognised. Both official
  clients deliberately never commit to one host, and the divergence produced
  two real failures: a certificate rejected `NotValidForName` — a per-host
  condition; the fleet's other proxies were fine — pinned every upload to
  the one bad proxy for as long as the client lived, and a fleet of pinned
  clients never rebalanced, each keeping whichever host its one lookup
  happened to name through every drain and load shift afterwards. Now the
  whole answer is a pool, each heavy command picks a member at random (the
  crate's existing id source is the entropy — *unique, not unpredictable* is
  the right bar for load-spreading, so no new dependency), and the list is
  refreshed lazily by the first heavy command that finds it older than the
  refresh interval, per the documentation's own "re-query every minute". A
  failed refresh keeps the previous answer in use and waits out another
  interval before asking again. No background thread; a client that stops
  uploading stops asking.

  Two behaviour changes a caller can observe, neither breaking an API:
  a heavy command may now pay one `/hosts` round trip mid-life — bounded by
  `with_hosts_timeout`, at most once per interval — where it could only pay
  one up front before; and "the cluster named no heavy proxy" now expires
  with the same interval instead of settling for ever, so a first lookup
  that landed during a rolling restart is no longer a verdict for the
  client's whole life. `with_proxy_discovery(false)` still pins everything
  to the configured address, `heavy_proxy()` still answers with the
  cluster's first pick, and `with_hosts_retry_after` still means what it
  meant — how long the fallback lasts.

- **Fixed** the pinning half of that divergence on its own terms: a heavy
  command's failure now drops the host it went to on any failure
  *attributable to the host* — a refused connection, a 503, a wrong-role
  refusal, and now a rejected certificate — rather than only on the failures
  `worth_asking_again` recognises, which is the predicate for the `/hosts`
  lookup and answers `false` for a certificate verdict. The dropped host
  stays out until a refresh names it again — so a host that is *persistently*
  bad costs one failed command per interval until an operator fixes it,
  rather than every command until the client is restarted; a pool with
  nobody left falls back to the configured address for
  `with_hosts_retry_after` and the cluster is then asked afresh, exactly as
  before.

- **Added** `Client::with_host_list_refresh_interval(Duration)`, defaulting
  to one minute. `Duration::ZERO` re-asks before every heavy command;
  `Duration::MAX` disables the refresh — the first answer is kept as long as
  it keeps working, though a failed host is still dropped and an emptied
  pool still falls back and re-asks.

### A dozen commands, one round trip, and the answers one by one

- **Added** `BatchRequest` and `Client::execute_batch` — the cluster's
  `execute_batch`, which both official clients have had all along
  (C++ `CreateBatchRequest`, Go `NewBatchRequest`). A launcher that creates a
  dozen tables no longer makes a dozen round trips (#11).

  **The answer is a `Vec` of per-part `Result`s**, because that is what a
  batch is: its parts fail individually, and collapsing them into one `Result`
  would lose the only thing batching costs any clarity on. Each `Ok` carries
  the part's own envelope, keyed by what that command returns — `{node_id=…}`
  for a create, `{value=…}` for an exists — and each `Err` is an ordinary
  `ClientError::Cluster` named after the part's command, flattened
  outer-plus-innermost like every other. Verified on a local cluster with a
  four-part batch answering `[error 501, ok, ok, error 500]`, in exactly the
  order the parts went in.

  **The building shape is a builder**, not a slice of prepared commands:
  typed methods (`create`, `create_table`, `exists`, `get`, `list`, `remove`,
  `remove_tree`, `set_attribute`) that send exactly what their `Client`
  namesakes send, plus a `raw` escape hatch. A builder is what lets the retry
  class be decided instead of guessed — see below — and what keeps a part
  from being a shape the cluster refuses. **The cluster's rule is the
  command's data types, not `isHeavy`**: a part is refused when its registered
  output type is `tabular` or `binary`, or its input type is `binary`, and the
  driver throws before any part runs, so one such name fails the *whole*
  request. Measured against the registry the cluster serves at `GET /api/v4`
  (190 commands) and confirmed name by name — 21 names, where the crate's
  `HEAVY` list has 7, and the two differ in both directions: `get_job_spec` is
  `is_heavy: true` and is **accepted** as a part, while `alter_query` and
  `push_queue_producer` are `is_heavy: false` and are refused. `select_rows`
  and `lookup_rows` are on it, and are what a caller would plausibly try. A
  `raw` part naming one is refused where it is written rather than costing a
  round trip and taking the other parts' answers with it. The list is a
  snapshot of one cluster's registry, not a promise of completeness, and
  `BatchRequest::raw_with` says so.

- **Added** the two options both official clients expose:
  `with_concurrency`, the command's own server-side cap (“to avoid exhausting
  your request rate limit”, default 50 in the cluster's registration), and
  `with_max_part_size`, the C++ client's `BatchPartMaxSize` — a bigger batch
  is split into consecutive requests, `concurrency × 5` per request unless
  told otherwise, results stitched back in part order. There is no rollback
  across the requests, which is what the C++ client's `ExecuteBatch` does too;
  the C++ client also re-queues a *retriable part* client-side, and this one
  deliberately does not — per-part errors are handed back for the caller to
  judge, and `docs/sdk-comparison.md` discloses the difference.

- **Added** `ClientError::BatchInterrupted`, so a split batch that stops part
  of the way through does not throw away the parts that already applied. The
  loop used to return the failure and nothing else: five creates at two per
  request, second request out of retries, and the caller got
  `Http { status: 503 }` with no word of the two tables now on the cluster —
  and no way to recover, because re-running the same `BatchRequest` mints
  fresh mutation ids and applies the first two a second time. The error now
  carries `answered` — one entry per part of every request that completed, in
  part order, with the same per-part `Ok`/`Err` split the success path hands
  back — beside `parts` and the `cause`. A batch that fits one request is
  unchanged: there is no prefix, so the underlying error is returned as it
  always was.

  `answered` is what came **back**, which is deliberately not a claim about
  what was *applied* — and the rendered one-liner says so too, because that is
  the sentence a log line and an `unwrap()` panic show. A request refused
  *while executing* runs **every one of its parts** and then throws the whole
  result list away: the driver collects the sub-requests into callbacks, runs
  them all through `CancelableRunWithBoundedConcurrency`, and discards
  everything at `.ValueOrThrow()`. Dispatch is never aborted, so this is not a
  race and no arrangement of the parts limits it — measured, a `create` beside
  a part naming an unknown command landed with the bad part first *and* last,
  two creates around one both landed, and at `concurrency=1` eight creates
  before the bad part all landed. The bound that does hold is the other one: a
  request refused while its *parameters are being read* runs nothing
  (`Validation failed at /concurrency`, `Error loading parameter /requests`,
  `Missing required parameter /requests` all left no node behind). So the parts
  before `answered.len()` are settled, and the request that failed is unknown
  territory — not because some of it might have run, but because all of it did
  and none of it said what happened. That is what a transaction is for.

- **Added** `Client::execute_batch_with`, taking a caller-supplied
  `MutationId` — the same guarantee `Client::raw_command_with` offers and the
  one a single process cannot give itself: persist the id, and a batch
  replayed after a crash is deduplicated against the send that may already
  have happened. It was the one thing the feature's own headline safety claim
  rested on and the API could not express. **Reproduced through this method**
  on a local cluster, with `create_table` parts and not `create` ones: sent
  under an explicit id and again under `id.as_retry()` it answered the same two
  node ids, where a fresh id got two `501 already exists`. The spelling
  matters — `BatchRequest::create` sends `ignore_existing`, so a second send
  answers with the *old* node's id whether or not a replay was recognised, and
  a two-`create` batch under a **fresh** id was measured returning ids
  identical to the first send's. That looks exactly like a deduplicated replay
  and is not one; `create_table` omits `ignore_existing`, which is what makes
  the identical ids mean something.

  An id covers **one** request and a split batch with one is refused with
  `ClientError::Config`: the cluster derives each part's id by incrementing
  the batch's, so a second request under anything derived from the same id
  would collide with the first request's parts and be answered with their
  results.

- **Added** `BatchRequest::raw_with`, taking the `Repeatable` its `Client`
  namesake takes. `raw` hard-codes `Repeatable::Never`, and because a batch
  retries as the most cautious of its parts, one raw **read** —
  `check_permission`, `get_supported_features`, `parse_ypath` — demoted an
  otherwise all-read batch to send-once. A caller who knows the command's
  registry bits says so and keeps the retry. `Repeatable::Heavy` is refused:
  it is not a class a part can have, since it asks for a heavy proxy and a
  batch does not go to one.

- **A mutating batch is retried under one mutation id, and that is safe
  because the cluster spreads it over the parts.** The driver hands part *k*
  the batch's id plus *k* (`GenerateNextBatchMutationId`) and stamps the
  batch's `retry` flag into every volatile part, so a replayed batch replays
  every part under its original id and the master's cache answers each with
  its first response. Measured, not assumed, with `create_table` parts —
  `create` sends `ignore_existing` and would have answered with the same ids
  either way: replayed under its id with `retry=%true` the batch answered the
  **same two node ids** both times, where the same batch under a fresh id got
  two `501 already exists`.
  A batch of nothing but reads mutates nothing and carries no id; a batch
  holding a **`raw` part is sent once**, whatever the policy says, because a
  command this crate cannot classify may mutate somewhere no mutation cache
  covers — the scheduler commands are the measured example.

- **A client bound to a transaction puts the parts in it, not the
  envelope.** `execute_batch` has no transactional options, and a local
  cluster proved what that means: an outer `transaction_id` was dropped in
  silence and the part's create landed *outside* the transaction, surviving
  its abort — the silent escape a transaction exists to prevent. Each part is
  stamped instead, with the transport's own exceptions: a part that names its
  own transaction keeps it, and a command with no transaction to be in is
  left alone.

- **The batch's parameters travel in the POST body**, where every other
  command's ride in `X-YT-Parameters`: a batch's parameters *are* the batched
  commands, and a header has a size nobody promises. The C++ client makes the
  same choice for this same command, and the proxy merges body parameters
  with the header's (`TContext::CaptureParameters`) — measured: `requests` in
  the body and `mutation_id` in the header land as one parameter set.

- **Parts run in parallel, and the documentation means it.** A batch that
  created a node and asked `exists` about it in the same breath was answered
  `%false` — both parts succeeded, and the read simply ran first. A part and
  its consequence belong in two batches. Also measured: a part naming an
  unknown command fails the **whole batch** (HTTP 400, `Unknown command
  "frobnicate"`, no per-part results), because the driver resolves the command
  before per-part error handling begins — and **every other part of that
  request still ran**, at any position and at any concurrency, so the refusal
  is not a rollback and not a race.

- The parser refuses what it does not recognise — a `results` count that
  does not match the parts, an item that is neither `{output=…}` nor
  `{error=…}` nor a bare empty map — rather than reading it as somebody's
  success, and it holds a part's success to **the key that command answers
  under**: `node_id` for a `create`, `value` for an `exists`, `get` or `list`.
  The key and not the wrapper, because a guard on the wrapper is dead on API
  v4: measured one part apiece, `create` → `{output={node_id=…}}`, `set` →
  `{output={}}`, `remove` → `{output={}}`, `exists` → `{output={value=%false}}`
  — **no modelled command answers a bare `{}`**, and the registry the cluster
  serves at `GET /api/v4` calls `remove` and `set` `structured` exactly as it
  calls `create` (it is `/api/v3` that calls them `null`). So `{output={}}` is
  a shape a v4 cluster really produces, it is a legitimate `set` success, and
  only the key tells it from a `create` that would panic the caller at
  `answer["node_id"]`. This crate's envelope rules were learned from `exists`
  answering under `value`; the paranoia is paid for.

  **And the empty map is only for the commands that can answer with one.**
  `{}` means the driver wrote no `output` key, which it does only where the
  command's registered output type is `Null` — `set` and `remove` here. It
  used to be accepted for every command, so a cluster or proxy answering `{}`
  to a `create` passed the parser as a success with nothing in it, and
  `answer["node_id"]` — the access this crate teaches, in the rustdoc, in the
  example and in the tests — then **panicked** in caller code one frame away.
  A structured-output part is now held to an `output`, and only a `raw` part,
  whose registry bits only its caller knows, is still taken as it comes.

- **A batch is the crate's first light command with a body**, so a
  cross-origin redirect on one is refused with `RedirectRefusal::Payload`
  where the same creates sent individually are bodiless POSTs the rule
  deliberately lets through. Narrow — a client with a token is refused a
  cross-origin hop anyway — but a tokenless client behind a balancer that
  canonicalises to another origin finds batching breaks what individual calls
  did. Documented on `execute_batch`, where a caller meets it.

  Verified end to end on a local cluster by
  `cargo run -p ytsaurus-client --example batch`: twelve creates in one batch
  and every node there afterwards, the one-fails-rest-succeed batch, a split
  batch with a failure kept at its own index, a split batch stopped
  mid-sequence reporting the two parts that had applied, one mutation id
  replayed for the same two node ids, and a batch inside a transaction
  invisible until the commit. The **round trip** itself is not something a
  program on a cluster can see, so the example no longer claims it: it is
  pinned by `tests/batch.rs` against an in-process socket that counts, and
  measured once through a counting TCP relay at **1** request and 9.16 ms
  against 140.77 ms for twelve individual calls. The wire shape — verb, body,
  the mutation id repeated across a retry, absent from a read-only batch and
  the caller's own when there is one — is pinned by the same file.

### A cache that refuses you, and the upload that goes anyway

- **Fixed** `Client::upload_worker_cached` dying at the first upload on an
  installation that maintains `//tmp/yt_wrapper/file_storage` itself. The
  `create` on the miss branch is answered `cluster error 901: Access denied for
  user …: "write | modify_children" … is not allowed by any matching ACE`, and
  four of the shipped examples never got past it — `vanilla`, `statistics`,
  `cached_upload` and `profile` (#32). A 901 on the cache's **own** writes —
  creating the cache directory, creating the staging node inside it, and the
  handover to `put_file_to_cache` — now means "no cache for you": the worker
  goes up under `//tmp` on a path of its own and the launch carries on.

  Precisely those three. A 901 on the bytes themselves is about the node this
  client has just created, not about the cache, and the same bytes sent
  elsewhere would earn the same answer; a create that failed for a resolve
  error or a lock held elsewhere is not a permission problem at all. Both are
  returned as they always were, because a fallback that swallowed either would
  upload twice and then report success.

  The code is looked for anywhere in the error document rather than only at the
  top, as the retry classifier and the transaction one already do: every
  transcript seen so far is flat, and an outer code is routinely a category with
  the reason nested under it.

- **Breaking** `CachedFile` gained a `cached` field. Code that matches the
  struct by name or reads its fields is unaffected; code that destructures every
  field needs `..`.

  It is there because `uploaded` was answering two questions with one bit. It is
  true both for a file the cache accepted and for one that went to `//tmp`
  because the cache would not, and `path` was the only difference — so a
  launcher that cleans up after itself was deleting the installation's **shared
  cache entry** on an ordinary cluster, evicting the binary for everyone else,
  and one that does not clean up leaks a node per launch on the cluster where
  the fallback fires, since nothing expires those. `cached` is the field to
  branch on: true for a hit and for an accepted upload, false only for the
  fallback.

- **Added** a warning when that happens, on stderr and as a `WARN` event where
  the `tracing` feature is on. The state is permanent until someone acts and
  invisible otherwise — every launch re-sends the whole binary and leaves a node
  behind. The message names the path that was refused, quotes the cluster so an
  ACL failure is not mistaken for a flaky proxy, and names
  `Client::with_file_cache`, which already existed and is the one line that puts
  a cache back.

- **Documented** what the fallback node is: an ordinary `//tmp` node with
  whatever ACL `//tmp` carries, no expiry, and a name unguessable only as far
  as a mutation ID is — the entropy behind one says of itself that its callers
  need an id to be *unique, not unpredictable*, having been built to
  deduplicate a retry rather than to withhold a name. On shared scratch space a
  co-tenant can rewrite the worker's bytes between the upload and the job that
  execs them. It is the ordinary exposure of anything left in `//tmp`, and it
  is the reason to point `with_file_cache` at a directory of your own rather
  than to accept the fallback as a settled state.

**Not verified against a cluster.** A local cluster in Docker makes the caller
`root` and can never answer `Access denied`, which is why this was found on a
real multi-node installation and not before. `tests/file_cache.rs` scripts a
cluster on a socket in-process and asserts the sequence of commands, which is
what is actually under test: which call was refused, and what the client did
next.

### A token no longer follows a redirect to a host nobody chose

- **Fixed** a credential-carrying request being redirected and arriving
  unauthenticated. A control proxy does not refuse a heavy *read*: it answers
  `307 Temporary Redirect` naming a data proxy on **another host** — the
  [HTTP proxy reference][return-codes] gives that row as *"Redirecting heavy
  queries from light to heavy proxies"* — and `ureq` drops the `Authorization`
  header when it follows one; its default is `RedirectAuthHeaders::Never`. The
  read then arrived without a token and the cluster answered `cluster error
  111: Client is missing credentials`, which sent the user to check their
  token, their token file and their permissions. None of them was at fault.

  A request whose redirect **changes origin** — scheme, host or port — now
  fails with **`ClientError::Redirected`** when it carries credentials, naming
  the status and where it pointed. The alternative, re-attaching the
  credentials and going, would follow an unsolicited instruction that arrived
  mid-flight, on a request addressed somewhere else. Asking the cluster for a
  data proxy is the deliberate route to the same place, `Client::heavy_proxy`,
  and the error says so — but only to a command that could use one. A `create`
  that met a balancer's `301` is not told to go and find a heavy proxy.

  The error stops short of telling anyone their token is good: a gateway in
  front of the cluster may answer an expired token with a redirect of its own,
  so it reports only what this client is certain of — the credentials were not
  sent to the host that answered.

  A redirect that **stays on the same origin** is followed, token and all.
  Nothing new learns the credential by it, and a balancer canonicalising its
  own host would otherwise break every command against that installation. The
  `Location` is resolved against the address the request went to
  ([RFC 3986 §4.2][rfc3986]), so `Location: /api/v4/exists` is placed on the
  host that sent it rather than reported as a path with no host in it.

  `redirect_auth_headers(RedirectAuthHeaders::SameHost)` is **not** the fix and
  the source says why where someone would reach for it: the redirect is
  deliberately cross-host, which is exactly the case that setting does not
  cover.

- **Changed** a followed redirect now sends the **same request** again: same
  method, same body. That is what `307` and `308` require by definition, and
  what an API v4 command needs whatever the digit — a command's verb is fixed
  by the command, so a `create` rewritten into a `GET` is not a `create`. A
  bodiless `POST` therefore follows a balancer's canonical-host `301` like any
  other command, and a same-origin `write_table` sends its rows on rather than
  losing them.

  Two things still do not travel, and the rule for both is the **origin**:

  - **credentials** — unchanged, and described above;
  - **data**, with or without a token
    (`RedirectRefusal::Payload`, new). The same objection as the token, about
    the other thing a caller picks a host for: a tokenless `write_table` does
    not get to send a table's rows to whichever host a `Location` header
    names. A body of length zero is not data — `Content-Length: 0` gives
    nothing away — so most of API v4 is unaffected.

  Separately, a body this client **cannot send a second time** is refused
  wherever it points: `write_table_rows` and `raw_command_upload` read their
  body as they send it, so by the time the `3xx` arrives some of it has gone
  and a reader cannot be rewound. That closes a silent data loss that predates
  all of this — a redirect that dropped the body left `write_table` returning
  `Ok(())` having written no rows — and reports it as
  `ClientError::Redirected { refusal: RedirectRefusal::Body, .. }`.

- **Fixed** the request timeout being multiplied by the length of a redirect
  chain. `Client::with_timeout` documents an end-to-end limit for a buffered
  command, and taking the following away from `ureq` — whose `Timeout::Global`
  had covered the whole chain — gave every hop a fresh copy of it instead. The
  real limit became `(hops + 1) ×` the one asked for: a client with a 400 ms
  timeout, meeting a proxy that redirects to itself with 300 ms of thought per
  hop, returned from `exists` after **3.36 s and eleven requests**. At the
  default two minutes that is twenty-two of them, on one `exists` — and the
  same on `Client::heavy_proxy`, which follows its own redirects.

  An attempt now takes its deadline once and gives each hop what is left of it,
  so the chain spends one budget. A retry is a fresh attempt and still gets a
  fresh budget, as it always did.

- **Fixed** `Location: ?path=//other` resolving against the request's
  *directory* rather than its path — `/api/v4/?path=//other` where
  [RFC 3986 §5.3][rfc3986-5.3] asks for `/api/v4/exists?path=//other`. A
  reference with no path of its own keeps the base's, and a bare `#fragment`
  keeps the base's query as well. Costs a `404` rather than a credential, since
  the origin is the same either way — but a `404` for a request the proxy meant
  to have answered.

- **Added** `RedirectRefusal`, the reason a redirect was refused:
  `Credentials`, `Body`, `Payload` or `TooMany`. It is a field on
  `ClientError::Redirected`, and it renders the clause the message carries.
  Non-exhaustive, which is what let `Payload` join it.

  `ureq` now follows **no** redirect for any transport (`max_redirects(0)`) and
  this client follows them itself, because the answer turns on the credentials,
  the origin and the body at once, and no combination of `max_redirects` and
  `redirect_auth_headers` expresses that. A chain longer than ten hops is a
  loop rather than a route, and is refused as one.

- **Fixed** `get_job_stderr` missing from the list of heavy commands, so a
  launcher whose stderr fetch met a redirect was refused with no advice
  attached — at the moment it was already diagnosing a failure. The list is the
  cluster's `isHeavy` bit rather than an inventory of what this crate models,
  which is why `read_file` and `read_blob_table` stay on it: `raw_command`
  sends those, and the documentation on `raw_command_streaming` reads a file.

- **Breaking** `ClientError` is now `#[non_exhaustive]`. A `match` over it must
  carry a `_` arm. The ways a cluster can refuse are the cluster's to add and
  not this crate's to freeze — every release so far has added one — so the
  attribute goes on while the release is source-breaking anyway rather than
  after. Naming a variant, constructing one and destructuring one are
  unaffected.

[return-codes]: https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#return_codes
[rfc3986]: https://www.rfc-editor.org/rfc/rfc3986#section-4.2
[rfc3986-5.3]: https://www.rfc-editor.org/rfc/rfc3986#section-5.3

### A cluster behind a private CA is reachable

- **Added** `YT_CA_BUNDLE`, a PEM file of root certificates to verify the
  cluster against instead of the Mozilla bundle `ureq` compiles in, and the
  **`platform-verifier`** feature, which trusts whatever the operating system
  trusts. Both are off by default and the default is unchanged: a client may be
  running outside the network it is talking to, where the machine's own trust
  store is the less trustworthy of the two.

  Until now there was no way to name a CA at all, so an on-premises
  installation whose chain ends at a corporate root was simply unreachable over
  `https://` — which is the scheme a bare host name in `YT_PROXY` selects.
  `curl` reaches the same URL, because it reads the OS trust store. There was no
  workaround in this crate's public API. Both official clients and the `yt` CLI
  let a deployment point at its own CA; this is that (#29).

  **A bundle that yields no certificates is refused**, with an error naming the
  file, rather than quietly becoming the Mozilla roots — a fallback would answer
  a deliberate request with the very `UnknownIssuer` the variable exists to end,
  and name neither the file nor the reason. So is one that cannot be read. The
  refusal is discovered while the agent is being built, where there is nothing
  to fail, so it waits for the first request that would have needed it. A
  cluster reached over plain HTTP is not refused: there is no handshake for the
  bundle to have configured.

  **And so is a `BEGIN CERTIFICATE` block that is not an X.509 certificate.**
  PEM is only an envelope: the reader splits the sections and base64-decodes
  them, and `rustls` then discards a block it cannot parse *without telling
  anyone*. So a PKCS#7 `.p7b` re-armoured under that label — how a Windows-born
  bundle usually arrives — was accepted here, produced an empty root store, and
  failed every request with exactly the `UnknownIssuer` that naming a CA is
  supposed to prevent, mentioning neither the file nor the variable. Every block
  is now checked to be a certificate, and one that is not refuses the whole
  file, naming it and saying how many blocks were wrong: a root store silently
  shorter than the one the caller wrote down is the same failure a step later.

  The bundle wins where both are set. It is the more specific answer, and the
  one the caller went out of their way to give.

  The file is read **once per process** and capped at 16 MB, and it must be a
  regular file. `Client::new` cannot fail and the client's global timeout covers
  requests rather than files, so there was nothing above the read to bound it: a
  variable naming a FIFO hung the constructor for ever, and one naming something
  enormous was paid for in memory before anyone could be told. Once per process
  because an agent is rebuilt more often than it looks — `Client::with_timeout`
  makes a new one, and a transaction's start and drop each build a client.

  **No new direct dependency, and the musl worker graph is unchanged.** `ureq`
  3.3 already offers both routes; both sit behind the `tls` feature, which
  `examples/` — what `build-worker.sh` cross-compiles — turns off. The CI guard
  now searches for `rustls-platform-verifier` alongside `tracing`, `rustls` and
  `ring`.

- **Fixed** a certificate error being retried five times. An unknown issuer is
  not a transient failure — the same roots reject the same certificate on the
  fifth attempt — but it arrived as a transport error, and every transport error
  was retriable, so a misconfigured CA took about fifteen seconds of doubling
  backoff to report. It is reported at the first attempt now.

  Deliberately narrow, and narrower than "the certificate was rejected". Only
  `UnknownIssuer` and `NotValidForName` are settled, because both are decided by
  *this client's* root store and *this client's* URL, neither of which the next
  attempt changes. Every other TLS complaint is still retried: an expired or
  revoked certificate is a property of the fleet member that answered, and a
  round-robin set mid-rotation may answer with a renewed one; a revocation list
  that could not be fetched is transient by definition; and
  `rustls-platform-verifier` — which the new `platform-verifier` feature turns
  on — reports a failed revocation lookup or a momentarily unreadable trust
  store as `Other(…)` under the same prefix, so classifying that would have made
  enabling the feature a way of turning a bad afternoon on this machine into a
  permanent failure. A reset connection, a refused one and a timeout are all
  still retried, as is every protocol-level disagreement mid-handshake.
### Heavy commands go to a heavy proxy, without being asked to

`Client::heavy_proxy` has always worked, and **nothing ever called it**. Every
heavy command — `write_table`, `write_file`, `read_table`, `upload_worker`,
`write_table_rows`, the streaming forms of each — went to whatever `YT_PROXY`
held, which on an installation that separates proxy roles is a control proxy,
and a control proxy refuses one. Run against a real multi-node cluster rather
than a local Docker one, that failed **19 of the 21 shipped examples** at their
first table write ([#30](https://github.com/sshaplygin/ytsaurus-rs/issues/30)).

- **Added** automatic routing. The first heavy command asks `/hosts`, the
  answer is kept for the client's lifetime and shared by every clone of it, and
  a heavy command that fails for a reason another proxy might not have gives up
  the host it used for the next name in that same answer. The failed command
  itself is not re-sent: heavy commands are not retried, and by then a streamed
  body is gone.

- **A failure moves to the next proxy, not back to the configured address.**
  The first cut of this feature had no ban list — Go's has one, and its absence
  was disclosed in `docs/sdk-comparison.md` rather than reconsidered — so one
  transient 503 from a draining data proxy, or one refused connection during a
  restart, sent the next ten seconds of heavy commands to the address the
  caller configured. On the deployment this whole feature was written for that
  address is a balancer in front of the **control** proxies, which refuse every
  heavy command with input data: the fallback reproduced [#30] on demand, once
  per hiccup, and heavy commands are not retried, so every command in the
  window failed. `/hosts` had already named the alternatives. Now the answer is
  kept whole, best first, the failed host is dropped, and only an answer whose
  every name has failed falls back to the configured address — for ten seconds,
  and then the cluster is asked again.

  A proxy that refuses heavy work **because of the role it has** is given up
  the same way. `/hosts` lists whatever `default_role_filter` says, which is a
  coordinator config parameter rather than a guarantee, so a control proxy can
  appear in it; its refusal is a cluster error that no retry could ever fix and
  that asking the coordinator again fixes immediately — which is exactly the
  distinction `worth_asking_again` was split out for.

- **Added** `Client::with_heavy_proxies_in`, a list of proxies written out by
  hand. The domain rule below is a guard against a typo, not a boundary; and
  `with_heavy_proxies_anywhere` was all-or-nothing, so the only cure for a rule
  that missed by one label was to remove the rule. Names are compared without
  case, and with a port only where both sides name one.

- **Added** `Client::with_hosts_timeout` and `Client::with_hosts_retry_after`.
  The lookup budget was `min(800 ms, the client's own timeout)`, so
  `with_timeout` could only ever lower it and a cluster answering `/hosts` in
  900 ms was unroutable by any configuration at all — while the first heavy
  command, which is often a client's first request, pays DNS, TCP and a TLS
  handshake out of the same 800 ms. The retry window is settable for a smaller
  reason that turned out to matter: at a fixed ten seconds no test outlived one,
  so `HeavyProxy::Configured` and `HeavyProxy::FellBack` were observationally
  identical and either could be written where the other was with the whole suite
  green. Both are now pinned by a test that sets the window to nothing.

- **A `/hosts` answer this client declines in full is announced**, once, naming
  what was refused and why — on stderr, or as a `WARN` event where the `tracing`
  feature is on, and muted inside a job like every other thing this client says.
  And when a heavy command is then refused at the configured address, the
  cluster's `Control proxy may not serve heavy requests with input data` carries
  the sentence the cluster cannot know: that this client asked, declined the
  answer, and which builder call changes that. Silence there was the whole
  failure mode — the operator gets back the error from #30 with nothing to
  connect it to.

  The refusal that prompted this is not hypothetical. `Client::new("hume")` — a
  bare cluster name, which `Transport::new` supports on purpose and which is how
  `YT_PROXY` is usually written — has no parent domain to take a leftmost label
  off, so the rule degenerated to "the name itself" and refused
  `["n0008-sas.hume.yt.example.net"]` in full, permanently and in silence. A
  configured name with no dot is now matched as a **label** of the discovered
  name, and not as its leftmost one: `hume` follows
  `n0008-sas.hume.yt.example.net` and not `hume.evil.com`. The same break was
  waiting in Kubernetes for anyone addressing the service by its short name.

- **A bracketed name has to hold an IPv6 literal.** The rule was "an unbracketed
  name with more than one colon is refused", which waved through anything that
  started with `[`. Probed against `ureq` 3.3:
  `https://[n0132.example.com]evil.attacker.com` parses with the host
  `[n0132.example.com]` — the brackets are stripped only for a literal — so the
  token went nowhere, and the cost was worse than a leak of nothing. The address
  was remembered, every heavy command failed to resolve it, and (before the ban
  list above) the failure repeated for as long as the client lived. A port is
  now digits on either shape of name, too.

[#30]: https://github.com/sshaplygin/ytsaurus-rs/issues/30

- **The classification is the one the crate already had.** `Repeatable` encodes
  the two bits the cluster's own command registry declares, `isVolatile` and
  `isHeavy`, and `Repeatable::Never` was carrying both "heavy" and "mutating
  where no mutation cache covers it". Those are now `Repeatable::Heavy` and
  `Repeatable::Never`, so the heavy commands are the ones already marked heavy
  rather than a second list beside the first, free to drift from it. The two
  streaming seams — `Transport::open` and `Transport::upload` — are heavy by
  construction, so a raw streaming command is routed too.

  **Breaking** `Repeatable` gained a variant *and* `#[non_exhaustive]`. Code
  that matches it exhaustively needs a `_` arm — and will not need another one
  the next time the registry earns a name here.

- **A discovered host is checked rather than pasted**: same domain as the
  configured address (or the configured host itself), scheme and port from the
  configured address, and no `://`, `/`, `@` or whitespace. Measured on the
  first cut of this feature: `http://n0132` from an `https://` client stripped
  TLS and put the token on the wire in cleartext, `real.example.net@evil.example.net`
  connected to `evil.example.net`, and a configured `:8443` was dropped.
  A refused name is passed over and the rest of the list tried; a `/hosts`
  answer refused entirely leaves the upload going where it went before there
  was a lookup.

  **What the domain rule is worth** was overstated when it landed, and is worth
  stating plainly instead. It is a guard against a typo in a configuration and
  against an obviously foreign name — not what keeps the token where the caller
  put it. Steering a heavy command with a `/hosts` body means controlling that
  body: over `https://` that is owning the proxy, which has the token already,
  and over `http://` it is being a man-in-the-middle, who reads the token out of
  every light command without coming near this code. The threat it does cover is
  a proxy registering itself in the coordinator under an unintended name, and
  even there it is coarse, because a suffix rule with no public-suffix list
  behind it — a dependency deliberately not taken — reads
  `yt-1234.us-east-1.elb.amazonaws.com` as sharing a domain with every other
  load balancer in the region. The scheme, the port and the `@`/`/`/`://`
  refusals are the parts that hold up on their own.

  **Added** `Client::with_heavy_proxies_anywhere`, the opt-in for an
  installation whose `/hosts` genuinely names another domain. It relaxes the
  domain rule and nothing else.

- **The lookup has its own budget**: one attempt bounded by 800 ms, not the
  client's five attempts of up to two minutes with fifteen seconds of backoff
  between them — all of which used to run while holding the lock every other
  heavy command wants. Measured: a `/hosts` answering 503 cost a heavy command
  **15.03 s**, and one that accepted the question and never answered cost it
  **615 s**. Both are now under a second (3 ms and 804 ms). A lookup that failed
  for a reason that might pass leaves the configured address in use for ten
  seconds and is then asked again, which is the same retry spread out where it
  queues nobody: **eight threads against a hanging `/hosts` took 240 s and
  performed 40 lookups, and now take 809 ms and perform one.** Eight threads
  against a healthy one still ask exactly once, which they already did.

- **A heavy proxy that cannot be reached is given up.** Before, the answer was
  thrown away, the same question asked, the same dead host resolved, and every
  upload for the rest of the client's life failed the same way — the shipped
  test asserted exactly that. The measured trigger is ordinary: a single-node
  container reached from the host is not on loopback (`172.17.0.2`), so
  discovery runs and `/hosts` answers with a container-internal name. Now the
  first upload fails, the next name in the answer takes over, and when there is
  none the second upload succeeds against the configured address with the
  cluster asked again ten seconds later.

- **A cluster that names no heavy proxy keeps serving them itself.** That is
  what leaves a single-node installation working exactly as it did, and an
  absent `/hosts` (404) is remembered as such so it is not asked again before
  every upload. So is a body that is not a list of host names, and so is an
  answer whose every name was refused.

- **A cluster on loopback is not asked at all**, which is the other half of
  leaving local alone: `localhost` is this machine's own cluster or a tunnel to
  one, and the address a proxy publishes for itself is not reachable from the
  near end of either. Following it would break every upload that works today,
  and the round trip could not have helped in the first place.
  **Added** `Client::with_proxy_discovery` to override that in both directions
  — on for a port-forward into a real installation, off to pin everything to
  the address given. With it off, a heavy failure now takes no lock and reads
  no state, rather than mutating an answer the client will never look at.

- **A routed failure names the host it went to.** `write_table: transport
  error: io: Connection refused` is a true report about an address that appears
  nowhere in the caller's own code — the client chose it, out of a list the
  cluster gave it, and then said nothing about the choice. It now reads
  `write_table at n0132-sas.example.net:9013: …`.

- **"Would waiting help?" and "would asking again help?" are two questions**,
  and `retry::is_retriable` was being asked both. The second is now
  `worth_asking_again`, used by the two places that decide whether to keep or
  discard what `/hosts` said. They agree on every failure this release can
  produce; the point of splitting them is the ones the next release can — a
  refused redirect is worth asking again and a rejected certificate is not, and
  neither answer is the retry policy's.

- **Corrected** what this crate said about the refusal, twice over. The claim
  that it is "not a 503, it is an HTTP 200" was never observed: `ClientError`
  renders a cluster error without its status, so a 503 carrying an `X-YT-Error`
  header looks exactly like a 200 carrying one. The cluster's own rule
  (`TContext::TryRedirectHeavyRequests`) splits on whether the request carries
  input data — a heavy **write** gets 503 with `Retry-After: 60` and the string
  `Control proxy may not serve heavy requests with input data`, a heavy **read**
  gets a **307** to a data proxy — and the documentation gives one half in its
  `/hosts` section and the other in its return-code table. Only the error string
  is first-hand here. "`/hosts` defaults to the `data` role" was asserted with
  no citation and now has one: it is `default_role_filter`, a coordinator config
  parameter whose compiled-in default is `data`, so an operator can change it.
  And a deployment **behind a balancer is the case that breaks**, not the case
  that works — the balancer fronts the control proxies. `heavy_proxy` remains,
  no longer as the escape hatch that makes an upload work but as the way to see
  the address or hand it to something that is not this client.

- **Not done, and written down instead:** the documentation asks for `/hosts` to
  be re-queried "every minute or every few queries", and both official clients
  do. This one asks once per client and then walks the answer it was given,
  asking again only when the answer runs out. That is a load-balancing
  regression the cluster absorbs, not a correctness one, and it is now disclosed
  in `docs/sdk-comparison.md` rather than left to be discovered.

- **Tested offline**, because none of it can be verified here: two listeners in
  `tests/request_shape.rs`, one answering `/hosts` with the other's address, and
  assertions about which one each command reached. A cluster answers a heavy
  command the same way whichever proxy was asked, so nothing about the answers
  could have caught this. Every one of the three unpinned heavy call sites —
  `write_file`, `write_skiff_table`, `read_skiff_table` — is now in that test's
  exact request list; `write_file` is the one that mattered, because
  `upload_worker`, `upload_current_exe` and `upload_worker_cached` all funnel
  through it.

### An operation is no longer a string and four commands

- **Added** the rest of the operation lifecycle — `suspend_operation`,
  `resume_operation`, `complete_operation`, `update_operation_parameters`,
  `list_operations`, `list_operation_events`, `get_operation`,
  `get_operation_by_alias`, `operation_suspended`, `operation_status`,
  `get_job` and `get_job_input` — and `Operation`, a handle over a client and
  an id, with the same commands on it.

  **Both shapes, deliberately.** The flat `Client` methods are the primitives
  and nothing was taken away from them; the handle exists because an operation
  is a thing to pass around and, more to the point, a thing to *reattach* to.
  `Client::attach_operation(id)` is that door — C++'s `AttachOperation`, Go's
  `Track(id)` — and it is what a supervised pipeline needs after a restart: the
  id is the durable name, so a process that did not start an operation can
  still pause, reprice, wait for or finish it. `start_map` and its siblings
  still return a `String`, so no existing call site changed.

  Unlike `Transaction`, **dropping an `Operation` does nothing**. A transaction
  is a scope and loses its work when the handle goes; an operation is meant to
  outlive the process that started it, which is the whole point.

  Four narrow readers — `operation_state`, `job_statistics`,
  `operation_result_error` and the private `operation_error` — were each
  building the same `get_operation` request. They are now one attribute of the
  general one, and each one's *reading* is a function of the document that a
  test runs against an answer a cluster actually sent.

  `operation_status` reads the state and the suspension **together**, because
  they are useless apart: suspension is not a state, so `state` says `running`
  for a paused operation and only `suspended` says otherwise. It is what
  `wait_for_operation` polls with, so a wait on a paused operation now reports
  `running, suspended` instead of sitting silent until someone resumes it.

- **Measured against a cluster**, because none of it is guessable from the
  command reference:

  - **suspension is not a state.** A suspended operation still reports
    `running`; `operation_suspended` reads the attribute that actually says so.
  - **suspend is idempotent and resume is not** — a second suspend is accepted,
    a resume of something that is not suspended is refused with code 201. So
    suspend is the one mutating scheduler command here that is retried, on its
    own idempotency rather than under a mutation ID the master's cache would not
    honour. An abort *causes* the scheduler to let go, so its retry always
    fails; a repeated suspend just says the same thing twice.
  - **complete is not idempotent**, exactly as abort is not.
  - `update_operation_parameters` carries its parameters in the header, not a
    body, whatever the command reference says — and answers with an empty body.
    It assigns rather than increments, so it is repeated freely; an update that
    would change nothing is refused here, because the cluster answers 200 and
    does nothing.
  - an alias needs `include_runtime=%true` or the cluster refuses to resolve it.
    An alias could be **set** through `with_raw` before this and never found
    again.
  - `get_operation` with no attributes named asks for the whole document, which
    was **119 KB** for a one-job vanilla operation. `attributes=[]` asks for
    nothing at all.

- **Added** the four operation types the enum could not name — `Merge`,
  `Erase`, `RemoteCopy` and `JoinReduce` — with `MergeSpec`, `EraseSpec` and
  `RemoteCopySpec` beside them, and `start_merge`, `start_erase` and
  `start_remote_copy`.

  **A sorted merge does not need `merge_by`**, which took a cluster to find
  out: sent without one, it is accepted and the key comes from the sort columns
  the inputs already carry, with the output arriving `sorted_by` those. An
  earlier draft of this refused such a spec locally on the assumption that the
  cluster would; it does not, and the check blocked an ordinary operation. The
  `lifecycle` example runs the case, so the claim cannot drift back.

  **`join_reduce` gets no spec builder**, and that is the answer rather than an
  omission: the current documentation no longer lists it among
  `start_operation`'s types and describes the same work as a reduce with
  `join_by` and `enable_key_guarantee=%false`, which `ReduceSpec::with_raw`
  builds today. The variant is there so the older type can still be named.

- **Added** the `lifecycle` example, which runs all of the above against a
  cluster and checks the answers: it starts a long vanilla operation under an
  alias, attaches to it, finds it by that alias, pauses and resumes it, reprices
  it, lists it back, reads one of its jobs by id, finishes it early, and then
  merges two sorted tables and erases a row range from the result.

### The client can be watched: a trace the cluster joins, and spans if you want them

Two halves of one problem, kept apart because they cost different things. A
launch that took four minutes, a command that was retried three times, a
transaction that was never committed — none of it left anything behind to look
at, and that is the first thing a production deployment needs.

- **Added** `TraceContext` and `Client::with_trace_context`, which put every
  request into a trace **without adding a dependency**. The cluster is already
  instrumented: its proxy opens a span for each request it serves, and a
  request carrying a `traceparent` has that span placed inside the caller's
  trace instead of starting an orphan. So the whole of this half is a header,
  and the operational value is most of the issue's.

  The header is the [W3C one](https://www.w3.org/TR/trace-context/), which is
  what all three official clients send — `FormatTraceParentHeader` in the C++
  wrapper, `injectTracing` in the Go SDK, `generate_traceparent` in the Python
  one — and what `TryParseTraceParent` reads on the proxy side. That parser is
  slightly wider than the standard, and both of its spellings are accepted
  here: the version may be missing entirely, which is the form the Go SDK
  sends.

  `TraceContext::parse` continues a trace that already exists, which is the
  case that matters — a service passes on the context it was called in, and the
  cluster's work turns up under the same trace as the request that caused it.
  `TraceContext::new` starts one for a program nobody called. A malformed
  header is **refused rather than sent**: the proxy drops one it cannot parse
  without saying so, and the trace would then be quietly missing the half that
  mattered. A header from a *later version* of the standard is not malformed,
  though — the standard's versioning rule is to read the four fields version 00
  defines and ignore whatever follows, which is the only thing that keeps this
  parser working against a caller that has moved on, and the documented usage
  `?`-propagates a refusal into a failed request.

  The span id is carried through as it arrived, so the cluster's spans hang
  under the span the *caller* named. A fully instrumented forwarder would
  substitute its own; this crate emits no spans a collector would know about,
  so an invented id would name a parent that does not exist. The work lands in
  the right trace, one level up from where it would otherwise sit.

- **Added** `TraceContext::with_tracestate`, which carries the `tracestate`
  header the standard pairs with `traceparent`. A participant that forwards one
  is required to forward the other unmodified: it is where a vendor keeps a
  sampling decision or a correlation key, and dropping it on this hop costs the
  caller's own backend — the proxy itself has no opinion about it. Not
  rewritten on the way through, because rewriting the list means claiming a
  vendor entry, and this client has none.

  `TraceContext::yt_trace_id` spells the id the way the cluster does —
  `8e9bcc43-5c2be9b4-56f18c4e-117ea314` rather than 32 undivided hex digits —
  because that is the spelling in the proxy log, in the `X-YT-Trace-Id`
  response header and in the UI. They are the same four 32-bit groups in the
  same order; only the dashes and the leading zeros differ.

  `/hosts` carries the header too. It is not a command and builds its own
  request, which is exactly how it once came to carry neither the token nor the
  timeout, and a heavy-proxy lookup slow enough to matter is one worth seeing.

- **Added** a `tracing` feature, **off by default**, which is the half that
  does cost a dependency. With it on, every attempt runs inside a span carrying
  the command, the attempt number and how long it took, and the retry message
  becomes a `WARN` event rather than a line on stderr — the same facts as
  fields, going wherever the subscriber sends them.

  Off by default for the reason `tls` is: this crate is linked into worker
  binaries that cross-compile to musl with nothing but the Rust toolchain, and
  a worker should carry only what it runs on. `examples/` already depends on
  the client with `default-features = false`, so the worker build never sees
  it. What it costs when it is on is three crates more to compile —
  `tracing`, its `pin-project-lite`, and `tracing-core` — plus `once_cell`,
  which a default build already has by way of `rustls` and a build without
  TLS does not. The facade is taken without its `attributes` feature:
  `#[instrument]` is a proc macro, and the one span here is opened by hand, at
  the single seam every command already passes through.

  Nothing is emitted without a subscriber, which is what makes it a facade — so
  **the stderr line is printed after all when none is installed**. Cargo
  unifies features across the whole graph, which means any crate anywhere in a
  build can turn this on for everybody: a launcher that never asked for it
  would otherwise find its only sign of a retry gone, a fifteen-second pause
  looking like a hang, and nothing in its own manifest to explain the silence.
  A feature should add a way of saying this, not take the old one away.

  The event and the span agree on their counting. `attempt` is the try that
  just failed and `of` is how many are allowed, in both — so `attempt == of`
  means the last one, and an event never reads `4 of 4` beside a span that says
  `attempt=5`.

- **Kept**: the retry reporting still mutes itself inside a job. `RetryPolicy`
  decides whether a retry is announced at all, and that decision now covers
  both spellings of the announcement — a job's stderr is the cluster's bounded
  diagnostic buffer, and a subscriber installed in a job is more often than not
  writing to that same buffer. `RetryPolicy::loud` puts the messages back
  either way.

### The cluster end-to-end test no longer needs Python

- **Added** the `e2e` example, which runs all three checks
  `tests/e2e/run_e2e.sh` runs — `cat` as an identity map compared
  byte-for-byte, two input and two output tables with table switching, and a
  `wordcount` map-reduce against a hand-computed reference — through this
  crate alone. The shell script needs the `yt` CLI, which needs a Python
  installation, which is the one thing this stack exists to avoid.

  Nothing had to be added to the client for it: every command the script sends
  already had a method, including the two `--spec` fragments that carry the
  meaning (`enable_input_table_index`, and `enable_key_switch` under
  `reduce_job_io` rather than `job_io`). The one difference is that the example
  **creates its destination tables** — `yt map --dst` makes them, and this
  crate does not, because an operation that made its own outputs would turn a
  mistyped destination into a stray table rather than an error.

  The script stays. It reads the same tables with the official Python client,
  so it checks the worker's output against an implementation we did not write;
  the example proves the client can drive a cluster unaided.

### A command this crate does not model can now be sent

- **Added** `Client::raw_command`, and with it the answer to "can I do X
  against my cluster?" stops being "fork the crate". `Transport::call` was
  `pub(crate)`, so a command with no method on `Client` could not be sent at
  all — the transport under it would have carried the request perfectly well,
  and there was no way in. `Client::start_operation` taking a hand-built spec
  already set the precedent; this generalises it to every command.

  Four entry points, because a raw command has the same three shapes a
  modelled one does:

  - `raw_command(method, command, params, payload)` — buffered, the common
    case;
  - `raw_command_with(…, repeatable, mutation_id)` — the same, with the retry
    classification the caller knows and the crate cannot;
  - `raw_command_streaming(method, command, params)` — the response handed
    back unread, for a command whose answer is the data (`read_file`,
    `read_blob_table`);
  - `raw_command_upload(method, command, params, body)` — the request body
    read as it is sent, for a command with an input data stream.

- **Added** `Method` and `Repeatable` to the public API, since a caller cannot
  choose either for a command the crate has never heard of. `Method` carries
  the proxy's own rule for picking a verb: *input data stream → PUT, mutating
  → POST, otherwise GET*.

- **Added** `ResponseReader`, the response-body reader `raw_command_streaming`
  hands back. `TableReader` is now a name for it — the same type, unchanged
  for every existing caller, because nothing about reading a body as it
  arrives was ever specific to tables.

- **Added** `yson_build::empty_map`, for a command that takes no parameters.
  `map([])` cannot express it: the key type has nothing to be inferred from,
  and `map` takes its entries as an `impl Trait` argument, so a turbofish is
  not allowed either.

  Three decisions made deliberately rather than by default:

  - **A raw command is sent once.** A command the crate does not model cannot
    be assumed idempotent, and a retry that applied an unknown mutation twice
    is a worse failure than one lost to a flaky proxy — so `raw_command`
    ignores the retry policy whatever it says. `raw_command_with` is where a
    caller who knows the command says otherwise.
  - **It is stamped with the client's transaction**, exactly as every modelled
    command is, so a raw command sent through a `Transaction` is *in* it
    rather than quietly beside it. The `NO_TRANSACTION` exceptions apply
    unchanged.
  - **The command name is checked before the URL is built.** It goes into
    `/api/v4/{command}` as it is, so a name carrying `/`, `?`, `#` or
    whitespace is refused: the failure it would otherwise produce is not an
    error but a plausible answer from the wrong place. A payload passed with
    `Method::Get` is refused for the same reason — a GET carries no body, so
    it would be dropped in silence.

### `remove` stopped being `rm -rf`

- **Changed** `Client::remove`: it sent `recursive=%true; force=%true` on
  every call, so `remove` of a map node deleted the entire subtree under it,
  and a mistyped path "succeeded" by not existing. It now sends the
  cluster's own defaults — the node must exist, a map node must be empty —
  and the old behaviour has a deliberate spelling, `Client::remove_tree`.
  **Breaking** for callers who relied on `remove` to clear subtrees or
  tolerate absence: say `remove_tree`. The examples' cleanup already does.

### The keep-alive pings can no longer lose the transaction they keep alive

- **Fixed** transaction pings riding the full retry pipeline and the
  two-minute request timeout: one hung proxy connection could stall the ping
  thread for minutes — five attempts, two minutes each, backoff between —
  while the 30-second transaction it was keeping alive quietly expired. A
  ping now gets one attempt, bounded by half the ping interval; the next
  ping is its retry.
- **Fixed** the ping thread outliving its transaction: every error was
  swallowed, so a cluster answering `No such transaction` was pinged again
  every interval for as long as the handle lived. A definitive
  "transaction is gone" answer now stops the thread; transient failures
  keep it pinging.
- **Fixed** `Drop` of an uncommitted `Transaction` sending its abort through
  the full retry pipeline — a destructor, possibly during a panic unwind,
  could block its thread for ten minutes against an unreachable cluster. The
  abort from `Drop` is now one attempt with a five-second bound; a lost one
  is cleaned up by expiry, exactly as if the process had crashed. The
  explicit `abort()` keeps the full retries, since it has a caller to wait
  for it.

### A connection cut mid-body retries like one cut mid-request

- **Fixed** a network failure while reading a response body being wrapped as
  `Decode`, which the retry policy never repeats — so a `Repeatable::Freely`
  read whose body was cut off failed permanently, while the identical reset
  one packet earlier (before the headers) retried as `Transport`. Both are now
  `Transport`.

### Builder order stopped mattering in `MapReduceSpec`

- **Fixed** `with_local_file`, `with_local_file_named` and `with_memory_limit`
  reaching the mapper only when `with_mapper` had been called *first*. They
  copied onto the phases as the calls arrived, so
  `.with_local_file("//tmp/w").with_mapper("./w map")` produced a mapper with
  no files and no memory limit — silently, since the reducer still had both.
  Files and the limit now live on the spec and reach each phase when it is
  rendered, so the same calls mean the same program in any order.

### A table transfer is no longer on a two-minute clock

- **Fixed** the 120-second request timeout applying end to end to streaming
  transfers. It was installed as `ureq`'s global timeout, which by its own
  definition runs "from DNS lookup to finishing reading the response body" —
  so `read_table_streaming`, `write_table_rows` and `write_table_streaming`,
  the APIs that exist for tables too big to buffer, were cut off mid-table
  after two minutes. A streaming request now bounds each wait *around* the
  data — resolve, connect, sending the request, the response headers — by the
  same timeout, and leaves the data itself open-ended. Buffered commands keep
  the end-to-end limit.
- **Added** `Client::with_timeout`. The two-minute default was also the only
  value: nothing let a caller on a slow link raise it, or a test against a
  dead proxy lower it.

### Stopping an operation, and adding to a table

The two gaps [`docs/go-parity.md`](../../docs/go-parity.md) found by going
through the Go SDK's examples, and the two it said were worth a decision.

- **Added** `Client::abort_operation`. A launcher can now say never mind. Until
  this, interrupting a wait left the operation running on the cluster, spending
  quota on a result nobody would read.
- **Added** `Client::operation_result_error`, promoted from a private helper:
  the `reason` an abort carries is folded into the operation's error document
  rather than kept beside it, so reading it back is what makes passing it worth
  anything.
- **Added** `TablePath`, and **changed** `write_table`, `write_table_rows` and
  `write_table_streaming` to take `impl Into<TablePath>`. Existing call sites
  pass `&str` and are unaffected; `TablePath::new(p).append()` adds rows instead
  of replacing them.

A YTsaurus path is a YSON value, not a string, and `<append=%true>` is an
**attribute on the path**. That is why the type exists rather than an
`append: bool` parameter: the attribute has to travel on the path itself, and a
client that sent it beside the path would have the cluster replace the table and
report success. A wire-level test pins the distinction.

```rust
client.write_table_rows(TablePath::new("//tmp/log").append(), entries)?;
client.abort_operation(&id, Some("the input turned out to be yesterday's"))?;
```

Six cluster facts, each from probing before writing anything:

- **Aborting is not idempotent, and never carries a mutation ID.** The scheduler
  lets go of an operation as soon as the first abort lands and then answers `No
  such operation`. That also rules out the usual retry protection: the master's
  mutation cache does not cover a scheduler command, so a resend of the same ID
  is refused rather than deduplicated, and a retry would report a successful
  abort as a failed one. Sent once, `Repeatable::Never`.
- **The operation is already aborted when the call returns**, ~350 ms later.
  There is an `aborting` state; the request outlives it.
- **Appends take a shared lock, replaces an exclusive one.** Four concurrent
  appends to one table all land; four concurrent replaces leave one winner and
  three `Cannot take "exclusive" lock` failures. Beyond the wire saving, this is
  most of why append is worth having.
- **Appending nothing is a no-op; *writing* nothing truncates the table.**
- **Appending to a sorted table is checked.** The table stays sorted and a key
  smaller than the last is refused with `Sort order violation: [0#9] > [0#1]`.
  An append to a sorted table is a continuation of it, not an addition to it.
- **Appending does not create the table.** A path that does not exist is refused
  with `Error getting basic attributes of user objects`, which is the cluster
  saying there was nothing to append to.

Measured, in [`docs/benchmarking.md`](../../docs/benchmarking.md). Against the
cluster, 60 000 rows in 12 pieces: appending takes 0.60 s and sends 60 000 rows;
rewriting the table each time takes 1.03 s and sends 390 000 — 6.5× the data,
because rewriting `k` pieces sends `(k+1)/2` times the rows. A new criterion
benchmark, `cargo bench -p ytsaurus-client`, runs the write and read paths
against a loopback socket and settles a claim the last release only asserted:
the streaming row encoder is **about 20 % faster** than encoding into a `Vec`
first, at 1 000, 10 000 and 100 000 rows alike. Bounded memory was the reason it
was written that way; being quicker as well means it costs nothing.

- **Fixed** a table write leaving its connection unusable. `ureq` returns a
  connection to its pool only once the response body has been read, and the
  upload path never read one, so **every** `write_table_rows` and
  `write_table_streaming` opened a fresh connection — a few seconds of writing
  left 11 623 sockets in `TIME_WAIT`. Reading and discarding the answer took
  **23 %** off a thousand-row write. The benchmark found this; no test could
  have, because every request still succeeded.

### Rows are Rust values

- **Added** `Client::write_table_rows` and `Client::read_table_rows`. A table is
  written from anything that yields serialisable values and read back into
  anything that deserialises, so the encoding is this crate's problem rather
  than every caller's.
- **Added** `Client::get_as`, which reads a node — or one attribute — into a
  Rust type instead of a `YsonValue` to walk.

This came out of going through the **Go SDK's twelve examples** one by one and
asking what each would need here; the answer is recorded in
[`docs/go-parity.md`](../../docs/go-parity.md). Go writes structs to a table and
scans structs back, and the SDK does the encoding. This client had bytes in and
bytes out, and the consequence was measurable: **eleven of its twelve examples
hand-rolled the same YSON encode loop**. Nine of them no longer do.

```rust
client.write_table_rows("//tmp/contacts", (0..100).map(contact))?;
let back: Vec<Contact> = client.read_table_rows("//tmp/contacts")?;
```

An iterator rather than a slice, because the encoder runs **inside** the request
body: rows are serialised a bufferful at a time as the connection asks for
bytes, so a million rows cost one buffer, and the caller never has to hold them
either. A row that will not serialise fails the write with the row's number
rather than sending the rows before it — a short table reported as a successful
write is the failure worth preventing.

Reading is the launcher-shaped direction: owned rows, whole table. A struct
naming three of twenty columns is a projection rather than an error, which is
what makes it worth asking with a type at all. For tables that do not fit,
`read_table_streaming` feeding `ytsaurus_job::JobReader` is still the answer,
and now says so.

Two cluster facts came out of the same exercise, both from the Go
`vanilla-example`, which reads its jobs' stderr **after they succeed**:

- **Stderr is kept for successful jobs**, with no spec option needed.
- **Ask promptly.** `list_jobs` answers with an empty list for an operation that
  finished a while ago — the controller agent forgets its jobs, and a cluster
  with no job archive then has nothing left to say. Both examples that harvest
  stderr do it immediately after `wait_for_operation`.

`Client::list_jobs` and `Client::get_job_stderr` had no example calling either
until now; they were reachable only through the automatic failure report.

### Reading what the scheduler recorded

- **Added** `Client::job_statistics` and `Client::job_statistic_sum`, the
  built-in counterpart to `custom_statistics` / `statistic_sum`.

The two trees are stored differently, which is why they are read differently: a
custom name keeps its slash as **one key**, while a built-in statistic **nests**
by path component. The separator differs too — `$$` rather than `$` — and both
are now accepted, since that is not something a caller should have to know.

```text
custom:    {"rows/rejected" = {"$"  = {completed = {map = {sum=3}}}}}
built-in:  {time = {exec    = {"$$" = {completed = {map = {sum=744}}}}}}
```

**A local cluster reports nothing under `user_job/cpu`**, so the CPU comparison
[`docs/benchmarking.md`](../../docs/benchmarking.md) describes cannot be run
here at all. `time/exec` is what it does report, and that is what the new
`profile` example measures with.

### Pointing it at a real installation

- **Changed** `Client::from_env` to find a token the way the `yt` CLI does:
  `YT_TOKEN`, then the file named by `YT_TOKEN_PATH`, then `~/.yt/token`. A
  machine where the CLI already works now needs nothing else. The token is
  **trimmed**: `echo token > ~/.yt/token` leaves a newline, and sending that
  fails authentication with an error that never mentions a newline. An
  unreadable file means no token rather than an error — which is what it means
  on a cluster that wants none.

**Responses were already compressed** and nothing said so. `ureq`'s `gzip`
feature is on in this crate, so every request carries `Accept-Encoding: gzip`
and every answer is decompressed on the way in; the proxy honours it, including
for a streamed table read — 67.7 MiB of table arrived as 400 KiB on the wire.
Nothing in the crate would have noticed if that feature were dropped, because a
cluster answers the same either way, just larger. A new test serves one request
from a socket in-process and reads what the client actually sent, which also
pins the token header, the absence of one when there is no token, and that
parameters travel in `X-YT-Parameters` rather than a query string.

**The proxy also accepts a gzipped request body** (`Content-Encoding: gzip`),
verified on the local cluster. Compressing uploads is not implemented: it costs
a compression dependency in a crate that is linked into worker binaries and
cross-compiled to musl, and that is a trade worth making deliberately rather
than in passing.

TLS remains the one part of this that a local cluster cannot exercise: the `tls`
feature is there, on by default, and only an `https://` installation will prove
it.

### A table bigger than the program that moves it

- **Added** `Client::read_table_streaming` and `Client::write_table_streaming`,
  with the `TableReader` the first returns. The buffered pair holds a whole
  table at once, which is right for a launcher inspecting a result and wrong
  for anything the size of the data.

Both carry the same bytes as the buffered pair — a binary YSON list fragment —
so a streamed table is exactly what a job reads on fd 0, and
`ytsaurus_job::JobReader::binary` decodes it unchanged. The client sends bytes
and the job runtime decodes them; that direction stays one-way (`ytsaurus-job`
is a dev-dependency here, so the example that says so is compiled rather than
asserted).

Measured on the local cluster with `cargo run --release -p ytsaurus-client
--example streaming`, which writes a table from a generator and reads it back
both ways:

```text
Writing about 64 MiB from a generator     1242757 rows, peak RSS 2.9 MiB
Reading it back as a stream               1242757 rows counted, peak RSS 3.8 MiB
The same table, read into memory          67.7 MiB in hand, peak RSS 74.7 MiB

Streaming the 67.7 MiB table cost 1.0 MiB of peak RSS; reading it in cost 70.9 MiB.
```

Two things this gives up, both deliberate:

- **No completeness check on the streaming read.** `read_table` verifies the
  response is a whole YSON list fragment, which is the client's only defence
  against a mid-stream failure it cannot see. Streaming cannot: the point is
  not to have the whole thing. The defence moves to the decoder, where a
  fragment cut short leaves a record that does not parse — the same protection,
  applied where it still can be.
- **No retry, ever.** A reader that has been consumed cannot be sent again, so
  a streaming write is one attempt in principle and not just by policy. That
  agrees with the documented rule for heavy commands, and a transaction is what
  makes such a write safe to fail.

The `X-YT-Error` trailer question the backlog attached to this item was
rechecked rather than assumed: **`ureq` 3.3 still exposes no trailers** — the
word does not appear in its source — so the gap documented in the `http` module
stands.

Internally the transport now builds a request in one place and differs only in
how the response is consumed: into a `Vec`, as a reader, or with a reader as
the request body.

### A schema can change after the table exists

- **Added** `Client::alter_table`, the other half of `create_table`. A table
  outlives the program that made it, and the struct its rows have gains fields.

**A table with rows accepts only changes that ask less of the rows already
written.** Watched on a cluster, on a table holding two rows:

| Change | |
| --- | --- |
| add an **optional** column, anywhere in the order | allowed |
| make a required column optional | allowed |
| `strict` → non-strict | allowed |
| add a **required** column | `Cannot insert a new required column "must" into a non-empty table` |
| remove a column | `Cannot remove column "size" from a strict schema` |
| change a column's type | `Type … is modified in non backward compatible manner` |
| rename a column | read as a removal, and refused as one |
| make the table sorted | `Cannot change schema from unsorted to sorted` |
| non-strict → `strict` | `Changing "strict" from "false" to "true" is not allowed` |

Two of those deserve to be known before either becomes permanent:

- **An empty table accepts all of it.** Dropping columns, changing types,
  becoming sorted — all fine while there is nothing to break. So a migration
  rehearsed on an empty table has proved nothing about the real one.
- **A non-strict schema can never gain a named column** —
  `Cannot insert a new column "note" into non-strict schema`. Relaxing `strict`
  is a one-way door out of schema evolution.

Here the schema is a **top-level parameter**, where `create` wants it inside
`attributes`. The two commands are exact opposites on this, and only one of them
says so: `create` ignores the top-level spelling in silence.

No local compatibility checking, deliberately: error 316 carries an inner error
naming the column and the reason, and the client's error flattening — written
for failed jobs — surfaces it as one sentence. A local rule set could only add a
way to refuse something the cluster would have allowed.

Verified on the local cluster in `cargo run -p ytsaurus-client --example
schema`, which now writes rows, widens the table by deriving the schema from a
struct that gained a field, and watches the cluster refuse each incompatible
change in turn — then make the same change on an empty table.

### The rest of the Cypress tree

- **Added** `Client::list`, `copy` / `copy_replacing`, `move_node` /
  `move_replacing`, `link` / `link_replacing`, and `lock` / `lock_waiting` with
  `LockMode` and `Lock`. Between them these are what a pipeline needs to *name*
  its results: yesterday's run beside today's, a `latest` link pointing at the
  newest, and a lock so two launchers do not publish over each other.

The `_replacing` half of each pair overwrites the destination and the plain one
refuses it, which is the cluster's own default. `move_node` carries the odd name
because `move` is a Rust keyword and `client.r#move` at every call site would
cost more than four characters do.

What the cluster taught us here:

- **`list` is not sorted.** Three dated tables came back as the second, the
  third and then the first. The order is the cluster's own and means nothing.
- **A truncated listing is an attribute, not an error.** The answer comes back
  as `<incomplete=%true>[…]`, so a caller who does not look gets a listing
  quietly missing entries. `list` refuses one instead of returning it.
- **Listing a table is an error** — `"List" method is not supported` — rather
  than an empty list.
- **A link resolves to its target, including for attributes.** `latest/@type`
  answers `table`; `latest&/@type` answers `link`. The `&` is the whole
  difference between asking about the link and asking through it.
- **A lock needs a transaction**, so `lock` refuses locally rather than sending
  a request the cluster answers with `A valid master transaction is required`.
- **A waitable lock is granted later, or never.** It comes back `pending`, and
  treating that as held is the mistake the command invites; `lock_waiting` polls
  until the cluster says `acquired`. The deadline is not a nicety: a transaction
  that already holds a *snapshot* lock on the node is refused an exclusive one
  outright, but the waitable version of that request queues behind a lock only
  that transaction's own end will release. It waits forever, silently.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
cypress`, which builds a small tree of dated runs, publishes over the live table
by moving a staging one across inside a transaction, and finishes with three
transactions competing for one lock.

### Published all at once, or not at all

- **Added** `Transaction`, `Client::start_transaction`,
  `Client::start_transaction_with` and `Client::with_transaction`. Everything
  sent through a transaction is invisible to everything else until it commits,
  and is discarded if it does not — so a launcher that dies halfway leaves no
  empty table, no stale worker and no half-replaced result.
- **Fixed** `Client::exists`, which read the answer out of an `exists` key the
  cluster does not send and so failed **every** call with a decode error. It
  reads `value`, as `get` does. Nothing in the crate called it until now, which
  is how it survived two releases; a captured response is a test now.

`Transaction` derefs to a `Client` bound to it, so `tx.write_table(…)` writes
inside the transaction and `tx.start_map(…)` runs the operation inside it. The
transaction ID is stamped onto every command in one place — the transport —
because a command that forgot it would quietly do its work outside the
transaction, which is the failure a transaction exists to prevent. A command
that names a transaction itself keeps the one it named, so committing a nested
transaction commits the one meant.

**Dropping the handle aborts it.** That is what makes `?` safe inside a
transaction: a failure returns from the function, the handle drops on the way
out, and the cluster is left as it was. Only `commit` publishes.

Two facts about the cluster, both watched rather than assumed:

- **A transaction expires 30 seconds after its last ping.** Verified: one with a
  two-second timeout, left alone for four, answers a ping with `Transaction …
  has expired or was aborted`. So the handle keeps a thread pinging three times
  per timeout for as long as it lives, which is what makes a transaction usable
  around an operation that runs for an hour. Without it the feature would work
  in an example and fail on anything real.
- **Committing twice is an error**, not a no-op: `No such transaction`, which
  reads like the commit failed when it succeeded. The commit therefore carries a
  mutation ID, so a retry after a lost answer is the same commit rather than a
  second one.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
transaction`: a table visible only inside its transaction and gone after the
abort; a launcher that fails halfway and leaves nothing, with no cleanup code in
it; a map operation whose worker upload *and* output table appear only at the
commit; a command in an aborted transaction refused with `No such transaction`;
and a two-second transaction committed six seconds in, which only the ping
thread makes possible.

### A table can be told what its rows look like

- **Added** the `schema` module — `TableSchema`, `Column`, `ColumnType`,
  `SortOrder` and the `TableRow` trait — plus `Client::create_table` and
  `Client::table_schema`.
- **Added** the `derive` feature, which re-exports `#[derive(TableRow)]` from
  the new [`ytsaurus-helpers`](../ytsaurus-helpers/) crate. Off by default: it
  is a compiler plugin, and a crate that only launches operations should not pay
  to build one.

A schematised table is checked on every write. The example run against a local
cluster ends with the cluster refusing a row that left a required column out —
`Required column "size" cannot have "null" value` — which is the whole point of
saying what the rows look like.

`TableSchema::validate` catches locally what the cluster answers with error 314
a round trip later: key columns that are not a prefix, duplicate names, names
starting with `@`, `unique_keys` without a key, and a required `any`. Each
becomes one sentence naming the column.

Four protocol facts behind this, all watched on a cluster rather than taken from
the documentation:

- **A schema passed as a top-level `schema` on `create` is silently ignored.**
  The request returns 200 and a node id, and the table comes back with an empty
  weak schema. It has to go inside `attributes`. This is the single worst
  mistake the command allows, and it is why `create_table` exists rather than a
  `schema` argument on `create`.
- `create_table` deliberately **fails if the path exists**: the cluster ignores
  the attributes of a create it skips, so an `ignore_existing` version would
  quietly leave the old schema in place and report success.
- **`boolean`/`any` are the `type` spellings; `bool`/`yson` are the `type_v3`
  ones.** Those two names are the only ones that differ between the
  vocabularies, and mixing them is refused —
  `Error parsing ESimpleLogicalValueType value "bool"`.
- **Three types can never be required** — `any`, `null` and `void`. Each already
  means "there may be nothing here".

All 26 column types the crate can name were created on a local cluster and
accepted. Descending sort order was *not*: `Descending sort order is not
available in this context yet`, so `SortOrder::Descending` says as much on
itself and the example checks it rather than asserting it, so the day a cluster
enables it the run says so.

### Operations with no input tables

- **Added** `VanillaSpec`, `VanillaTask` and `Client::start_vanilla`. A vanilla
  operation runs jobs that are not a transformation of a table — a distributed
  process, a side-car computation, a job that fetches its own input — which is a
  whole category this stack could not reach.

A task says how many jobs of its kind to run and, optionally, which tables they
write; the scheduler keeps that many going. Everything else — `gang_options` for
a coordinated process, `stderr_table_path` — goes through `with_raw`.
`output_table_paths` is always sent, even empty: not sending it is a different
statement from "there are none".

Coordination between the jobs is the user's problem, and
[`ytsaurus_job::job_cookie`](../ytsaurus-job/CHANGELOG.md) is what to divide the
work by.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
vanilla`: three jobs with nothing to read, identifying themselves as 0, 1 and 2,
whose slices of a sum add up to the whole and cover every number exactly once.

### Reading back what the jobs reported

- **Added** `Client::custom_statistics` and `Client::statistic_sum`, the other
  half of [`JobStatistics`](../ytsaurus-job/CHANGELOG.md).

The tree the cluster files them in is deeper than the name suggests, and the
shape was taken from a live cluster rather than guessed:

```text
{"rows/rejected"={"$"={completed={map={count=1;max=3;min=3;sum=3}}}}}
```

The statistic's name keeps its slash as **one key** — it does not nest, so a
path-walking lookup finds nothing. Below it sit `$`, the job state, and the job
type. `statistic_sum` totals `completed` jobs across job types: a map-reduce
reporting one name from both phases gives the operation's total, while an
aborted job's work is redone by its replacement and counting it would double.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
statistics`: a job that drops rows without a `key` column reports having read
seven and rejected three, and the operation — which succeeded, with a shorter
output table and no other sign anything was dropped — reports the same.

### The worker is uploaded once, not once per launch

- **Added** `Client::upload_worker_cached`, `Client::file_from_cache`,
  `Client::put_file_to_cache` and `Client::with_file_cache`. The cluster keeps a
  file cache keyed by MD5; an unchanged binary is now found there instead of
  being re-sent, which is the slowest part of a dev loop that changes only the
  spec.
- **Added** `with_local_file_named` to all three spec builders. A cached node is
  named after its hash, so `./my_job` would find nothing to run without a
  `file_name` attribute on the path. `file_paths` entries are YSON values now
  rather than plain strings, which is what makes such attributes expressible.
- **Added** a dependency on `md5` (0.8), chosen for having no dependencies of
  its own: this crate is linked into worker binaries that cross-compile to musl
  with nothing but the Rust toolchain.

The cache defaults to `//tmp/yt_wrapper/file_storage/new_cache`, the path the
Python wrapper uses, so an installation that already expires entries there
expires ours too.

Two things the cluster settled, both now handled: `get_file_from_cache` and
`put_file_to_cache` answer with a **bare string** rather than the usual
`{path=…}` envelope, and a cache miss is an **empty string**, not an error or an
entity.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
cached_upload`: first call uploads (166 ms), second is a hit (32 ms) on the same
path, and the cached binary runs as a job — so both the `executable` attribute
and the sandbox name survive the trip through the cache.

### A transient failure no longer kills the run

A shared cluster produces failures that pass on their own — a restarting proxy,
a scheduler that has lost the master. One of those used to end the run. Light
commands are now repeated, following the
[documented rules](https://ytsaurus.tech/docs/en/api/commands#retry).

- **Added** `RetryPolicy` and `Client::with_retries`. Five attempts by default,
  with a delay that doubles from one second to ten. `RetryPolicy::none()` turns
  it off.
- **Added** `MutationId` and `Client::start_operation_with`. Every mutating
  command the client sends now carries a `mutation_id`, so a repeated request is
  deduplicated by the cluster rather than applied twice — without it, a 503 on
  the way *back* from a successful `start_operation` would leave the retry
  starting a second operation over the same tables.
- **Heavy commands are still sent once**, whatever the policy says: the
  documentation is explicit that they cannot be retried, and a transaction is
  the way to make an upload atomic.
- Retriable failures are transport errors, HTTP 429/500/502/503/504, and
  YTsaurus codes 3, 100, 105, 108, 904 and 2100 — the same set the Python
  client retries on. A retriable code is looked for throughout the error
  document, because the outer error is often a `Request retries failed` wrapper
  with the real reason nested inside. Codes that mean the request was wrong
  (500 resolve, 501 already exists) are never retried.

**A replay must admit to being one.** The cluster refuses a repeated
`mutation_id` sent without the `retry` flag — `Duplicate request is not marked
as "retry"` — rather than deduplicating it. So the flag travels with the ID:
`MutationId::as_retry()` marks a send as a replay, which is what a
crash-and-restart needs when it reuses a persisted ID.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
idempotent`: the same ID twice returns one operation, a fresh ID starts a
second. The retry classification is unit-tested, including on the exact error
document a local cluster produced when its scheduler could not reach the master.

### Reduce and sort as operations of their own

- **Added** `ReduceSpec` / `Client::start_reduce` and `SortSpec` /
  `Client::start_sort`. Reduce over an already-sorted table is one of the most
  common operation shapes, and reaching for map-reduce instead pays for a
  shuffle that has already happened. Sort is what produces the sorted table, and
  it can then be reduced again and again.

- A reduce's `key_switch` goes under **`job_io`**, not `reduce_job_io`. That is
  the map-reduce trap in the other direction — one job type, one I/O section —
  and the wrong spelling is accepted and silently ignored, leaving the reducer
  to fold every key into one group. Both spellings are now pinned by tests.

- `sort_by` is only sent when asked for: the cluster defaults it to `reduce_by`,
  and stating it turns on a sortedness check the caller did not request.

- `SortSpec` renders **`output_table_path`** — singular, and a string rather
  than a list. Sort writes exactly one table however many it reads, and the
  plural spelling every other operation uses is rejected.

Verified on the local cluster with `cargo run -p ytsaurus-client --example
sort_reduce`: seven unsorted rows sorted (`@sorted_by` becomes `[word]`), then
reduced to four correct per-word totals. Four rows rather than one is itself the
proof that `key_switch` reached the reducer.

### The binary can upload itself

- **Added** `Client::upload_current_exe`, which uploads the running executable.
  Together with
  [`ytsaurus_job::is_inside_job`](../ytsaurus-job/CHANGELOG.md) this is the
  one-binary pattern: the same program launches the operation and runs as its
  job, so what the cluster runs is what you just built.

- **Added** `ClientError::NotAWorker`. The running executable is often not
  something a node can exec — Mach-O on macOS, dynamically linked on a
  developer's Linux — and both fail on the node minutes later with an error that
  names no cause. `upload_current_exe` reads the ELF header first (Linux,
  x86-64, no interpreter) and refuses with an error that says what to build
  instead. `upload_worker` is unchanged and stays permissive: a job command can
  legitimately be a shell script.

- **Added** the `tls` feature, on by default. Turning it off drops `rustls`,
  which drags in `ring`, which needs a C toolchain to reach musl — and a binary
  that is both launcher and job has to reach musl. With it off, an `https://`
  proxy fails with an error that names the feature rather than a confusing
  connection error. Defaults are unchanged for existing users.

  This is what lets `scripts/build-worker.sh` keep its promise of needing
  nothing but the Rust toolchain, verified by cross-compiling a worker that
  contains the whole client to static musl on macOS.

Verified end to end on the local cluster from both sides: the launcher refusing
a Mach-O binary, and the musl build of the same source uploading *itself* from
inside a Linux container and being run as the job.

### Failed operations explain themselves

`wait_for_operation` now reports *why* an operation failed. On a terminal
`failed` or `aborted` it asks the cluster which jobs failed and what each wrote
to stderr, and puts both in the error. Before this, a failed operation gave you
a state string and a trip to the web UI.

- **Added** `Client::list_jobs` and `Client::get_job_stderr`, plus the `JobInfo`
  and `JobFailure` types they return.
- **Added** `Client::with_job_diagnostics`, to turn the report off. The
  YTsaurus documentation asks that `list_jobs` not be used without an
  administrator's approval; this is the way to respect that.
- **Changed** the operation error is now the flattened message
  (`Failed jobs limit exceeded: Process terminated by signal 6`) rather than a
  truncated raw document, falling back to the raw document if the shape moves.
- **Breaking** `ClientError::OperationFailed` gained a `jobs` field. Code that
  matches the variant by name is unaffected; code that destructures every field
  needs `..`.

Collecting the report is best-effort throughout: it runs while an error is being
built, and a diagnostic that replaces the failure it was explaining would be
worse than no diagnostic.

Verified on the local cluster with the new `boom` worker, which panics on its
first row, driven by `cargo run -p ytsaurus-client --example diagnose`. The
`list_jobs` response it produced is kept as a test fixture in
`tests/fixtures/list_jobs_failed.yson`.

Two things that capture taught us, both now pinned by tests:

- `stderr_size` is a hint, not a length — the cluster reported `1` for a job
  whose stderr was several hundred bytes, so the client asks for stderr whatever
  the field says.
- The useful part of a job error is the innermost one. `User job failed` is a
  category; `Process terminated by signal 6` — a Rust panic under
  `panic = "abort"` — is the answer.

## 0.2.0

First release of this crate. Version tracks the workspace.

A thin HTTP API v4 client: enough to run a Rust worker with no Python
installation. Covers Cypress (`create`, `remove`, `exists`, `get`, `row_count`),
data (`upload_worker`, `write_file`, `write_table`, `read_table`,
`set_attribute`) and operations (`start_map`, `start_map_reduce`,
`start_operation`, `operation_state`, `wait_for_operation`), with `MapSpec` and
`MapReduceSpec` builders.

Verified against a local cluster with nothing Python on `PATH`.

Two limits are documented rather than hidden: heavy commands are not routed via
`/hosts`, and `ureq` 3.3 exposes no trailers, so a failure the proxy reports
mid-stream cannot be seen. `read_table` compensates by rejecting a response that
is not a complete YSON list fragment.
