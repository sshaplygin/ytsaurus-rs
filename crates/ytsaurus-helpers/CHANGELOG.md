# Changelog

## 0.3.0 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.2.6

Never released. The version was bumped in the workspace and the tag was never
cut; these changes reached crates.io in 0.3.0. This crate had none of its own.

## 0.2.5 - 2026-08-10

First release, and the first version of this crate. Version tracks the
workspace. Published because `ytsaurus-client`'s `derive` feature depends on it:
an optional dependency still has to exist on crates.io.

`#[derive(TableRow)]` reads a struct's fields and produces the YTsaurus table
schema they describe — one column per field, in declaration order, with the
Rust type deciding the column type and `Option<T>` the only thing that makes a
column optional.

The crate holds the macro and nothing else, because a procedural-macro crate
can export nothing else. The types it generates code against — `TableSchema`,
`Column`, `ColumnType` — live in
[`ytsaurus-client`](../ytsaurus-client/), which re-exports the derive under its
`derive` feature. That is the shape `serde` and `serde_derive` have.

What it refuses at compile time is chosen from what a cluster refuses at run
time, so the error arrives under the field instead of as error 314 from a
create: key columns that are not a prefix, `unique_keys` with no key, duplicate
column names, `Option<Option<T>>`, and any Rust type whose column type would
have to be guessed at.

Three types are never marked required — `any`, `null` and `void` — because the
cluster refuses that: each already means "there may be nothing here".

Verified against a local cluster through
`cargo run -p ytsaurus-client --example schema`: the derived schema is accepted,
the table comes back sorted by its key column, and a row missing a required
column is rejected.
