# Changelog

All notable changes to `ytsaurus-format` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**, a status it inherits from
[`ytsaurus-skiff`](../ytsaurus-skiff/), which it re-exports.

## 0.3.0 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.2.6

Never released. The version was bumped in the workspace and the tag was never
cut; these changes reached crates.io in 0.3.0. This crate had none of its own.

## 0.2.5 - 2026-08-10

First release, and the first version of this crate. Version tracks the
workspace. Published with `ytsaurus-skiff`, and for the same reason: the
launcher and the worker both select a data format, and a selection they do not
share is a selection that can drift.

`DataFormat` is that one selection — binary YSON, text YSON or Skiff — used by
`ytsaurus-client` for operation specs and table I/O and by `ytsaurus-job` for
worker I/O. Binary YSON remains the default everywhere.
