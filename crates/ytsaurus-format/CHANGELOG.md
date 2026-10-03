# Changelog

All notable changes to `ytsaurus-format` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is **pre-release**, a status it inherits from
[`ytsaurus-skiff`](../ytsaurus-skiff/), which it re-exports.

## 0.3.1 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.3.0 - 2026-08-16

No changes to this crate beyond the version, which tracks the workspace.

## 0.2.6

Never released: the tag was never cut, and the workspace's 0.2.6 changes reached
crates.io in 0.3.0. This crate had none of its own.

## 0.2.5 - 2026-08-10

First release, published with `ytsaurus-skiff`. The version tracks the
workspace.

`DataFormat` is the one data-format selection (binary YSON, text YSON or Skiff)
shared by `ytsaurus-client`, for operation specs and table I/O, and
`ytsaurus-job`, for worker I/O, so the launcher and the worker cannot drift.
Binary YSON remains the default everywhere.
