# ytsaurus-helpers

[![CI](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/sshaplygin/ytsaurus-rs/actions/workflows/ci.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)

Derive macros for [`ytsaurus-client`](../ytsaurus-client/).

A job's rows are already described by a Rust struct. A table schema describes
the same thing to the cluster, so writing it out again by hand is a chance to
disagree with yourself:

```rust
use ytsaurus_client::TableRow;

#[derive(TableRow)]
struct Visit<'a> {
    #[yt(key)]
    host: &'a str,               // utf8, required, sorted
    size: i64,                   // int64, required
    referrer: Option<&'a str>,   // utf8, optional — the Rust type says so
}

client.create_table("//tmp/visits", &Visit::table_schema())?;
```

Reach for it through the client, which re-exports the derive:

```toml
[dependencies]
ytsaurus-client = { version = "0.2", features = ["derive"] }
```

## What the types mean

| Rust | column type |
| --- | --- |
| `i8` `i16` `i32` `i64` | `int8` … `int64` |
| `u8` `u16` `u32` `u64` | `uint8` … `uint64` |
| `f32` / `f64` | `float` / `double` |
| `bool` | `boolean` |
| `String`, `&str`, `Cow<str>` | `utf8` |
| `Vec<u8>`, `&[u8]` | `string` |
| `YsonValue` | `any` |
| `Option<T>` | `T`, not required |

Text and bytes do not collapse into one type: a Rust `String` is UTF-8 by
construction and becomes `utf8`, while `Vec<u8>` becomes `string`, which is what
YTsaurus calls a byte string. That distinction is the same one the codec is
careful about, and it is the difference between a column that rejects a
non-UTF-8 byte and one that stores it.

Anything else is a **compile error naming the field**. Guessing a column type
from an unknown Rust type is how a schema comes to disagree with the data it
describes, and the cluster enforces the schema on every write. Say what you mean
with `#[yt(column_type = "…")]` — every type the cluster accepts is available by
name, including `timestamp`, `date`, `interval`, `json` and `uuid`, which no
Rust type maps to on its own.

## Attributes

On the struct:

| | |
| --- | --- |
| `#[yt(non_strict)]` | let rows carry columns the schema does not mention |
| `#[yt(unique_keys)]` | promise no two rows share a key; needs a key column |
| `#[yt(crate_path = "…")]` | for a renamed `ytsaurus-client` dependency |

On a field:

| | |
| --- | --- |
| `#[yt(key)]` | a key column, sorted ascending |
| `#[yt(name = "…")]` | the column name, when it differs from the field's |
| `#[yt(column_type = "…")]` | the column type, by its wire name |
| `#[yt(skip)]` | not a column at all |

## What it refuses, and why

Each of these is something a cluster answers with error 314 a round trip later.
Catching them at compile time turns a nested error document into a message
under the field:

- **key columns that are not a prefix** — `Key columns must form a prefix of
  schema`. The macro names the field to move rather than silently reordering
  your struct;
- **`unique_keys` with no key column** — the promise has nothing to be about;
- **duplicate column names**, after renames;
- **`Option<Option<T>>`** — a column is present or it is not; there is no second
  layer for a schema to describe.

Three column types can never be required — `any`, `null` and `void` — because
each already means "there may be nothing here". The derive never marks them so,
whatever the Rust type says.

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](../../NOTICE).
