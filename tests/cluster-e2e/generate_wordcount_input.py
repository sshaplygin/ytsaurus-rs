#!/usr/bin/env python3
"""Writes the lines of text that the wordcount map-reduce counts.

    python3 tests/cluster-e2e/generate_wordcount_input.py out.yson

The output is a binary YSON *list fragment* — `{...};{...};` — which is what
`yt write-table --format '<format=binary>yson'` expects: data rows only, never
control records.

The bytes are built straight from the binary YSON specification
(https://ytsaurus.tech/docs/en/user-guide/storage/yson) with nothing but the
standard library. That is *not* the strong claim `generate_fixtures.py` makes:
this check asserts a set of counts rather than a byte sequence, so the input's
provenance matters less here — the README says as much about the Rust example,
which writes the same rows through this project's encoder on purpose. The reason
is narrower. At this point `run_e2e.sh` is feeding `yt write-table` from a shell
pipeline, so the alternatives are building a Rust helper to write four rows, or
importing the `ytsaurus-yson` bindings — which `run_e2e.sh`'s own preflight
deliberately does not ask for (only `ytsaurus-client`; `run_pilot.sh` is the
script that needs both).

The three encoding helpers are duplicated from `generate_fixtures.py` and
`generate_pilot_input.py` rather than shared: each generator stands alone, so no
edit to a common helper can quietly change what a cluster is fed.

`LINES` is one half of a pair: `check_wordcount_output.py` holds the counts these
lines imply, written out by hand. Editing one without the other is a failure, not
a silent pass — the checker compares the whole mapping for equality, so the
end-to-end run stops on the next commit that changes only this file.

This used to be a `<<'PY'` heredoc in `run_e2e.sh`. That form was safe (the
quoted delimiter keeps the shell out of the source), unlike the `python3 -c "…"`
block that is now `check_wordcount_output.py`.
"""

import sys

LINES = [
    b"the quick brown fox",
    b"jumps over the lazy dog",
    b"the fox and the dog",
    b"quick quick fox",
]


def uvarint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def zigzag(value: int) -> bytes:
    """Protobuf sint64 wire format, which is how the YSON spec frames a length.

    Python's `>>` on a negative int is an arithmetic shift, so `value >> 63` is
    the sign mask zigzag wants. The result is masked back to 64 bits because
    Python integers are unbounded.
    """
    return uvarint(((value << 1) ^ (value >> 63)) & 0xFFFFFFFFFFFFFFFF)


def yson_string(data: bytes) -> bytes:
    """0x01 + zigzag length + raw bytes."""
    return b"\x01" + zigzag(len(data)) + data


def main() -> None:
    payload = b"".join(
        b"{" + yson_string(b"text") + b"=" + yson_string(line) + b"};" for line in LINES
    )

    with open(sys.argv[1], "wb") as f:
        f.write(payload)

    print(f"   wrote {sys.argv[1]}: {len(LINES)} lines, {len(payload)} bytes")


if __name__ == "__main__":
    main()
