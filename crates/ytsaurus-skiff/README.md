# ytsaurus-skiff

The in-progress YTsaurus [Skiff](https://ytsaurus.tech/docs/en/user-guide/storage/skiff)
implementation for this workspace.

The implemented slice is a validated schema/format model, a bounded dynamic
wire codec, dynamic job I/O and raw client table I/O. Typed rows and schema
inference remain deliberately unimplemented until they have Go SDK v0.0.33 and
real-cluster compatibility tests. See the workspace [compatibility
contract](../../docs/skiff-compatibility.md).

This crate is not published to crates.io.

## Acknowledgement

The initial job-level framing reference was shared by
[@AzazKamaz](https://gist.github.com/AzazKamaz/711234fde6c17cfe04c83702bced19d9).
It informs test vectors; the YTsaurus protocol and Go SDK define compatibility.
