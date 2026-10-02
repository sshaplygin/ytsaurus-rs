# Changelog

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.3.0 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.2.6

Never released: the tag was never cut, and the workspace's 0.2.6 changes reached
crates.io in 0.3.0. This crate had none of its own.

## 0.2.5 - 2026-08-10

First release. The version tracks the workspace. Published because
`ytsaurus-client`'s optional `derive` feature depends on it.

- Added `#[derive(TableRow)]`, which produces the table schema a struct
  describes: one column per field, in declaration order, the Rust type deciding
  the column type and `Option<T>` alone making a column optional. The types it
  targets (`TableSchema`, `Column`, `ColumnType`) live in
  [`ytsaurus-client`](../ytsaurus-client/), which re-exports the derive under
  its `derive` feature.
- Refused at compile time, instead of as error 314 from a create: key columns
  that are not a prefix, `unique_keys` with no key, duplicate column names,
  `Option<Option<T>>`, and a Rust type with no unambiguous column type.
- `any`, `null` and `void` columns are never marked required.
