#!/usr/bin/env python3
"""Generates the table payloads uploaded by the end-to-end tests.

The bytes are built directly from the binary YSON specification
(https://ytsaurus.tech/docs/en/user-guide/storage/yson), deliberately *without*
using this project's own encoder — a payload produced by the code under test
would not prove anything.

This writes only `table_rows_*.bin`, the rows that get written into YTsaurus
tables. The **job input** fixture (`cat_input.bin`) and the expected outputs are
*captured from a real cluster* by `capture_fixtures.sh`, because only the
cluster can say authoritatively what a job receives — the control-record framing
it emits is not something to guess at.

Run from the repository root:

    python3 tests/e2e/generate_fixtures.py

The output is committed, so regenerating it should produce no diff unless the
payload is being changed on purpose.
"""

import math
import pathlib
import struct

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------- encoding


def uvarint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def zigzag(value: int) -> bytes:
    """Protobuf sint64 wire format, as the YSON spec specifies for int64.

    Python's `>>` on a negative int is an arithmetic shift, so `value >> 63` is
    -1 (all ones) for negatives and 0 for non-negatives — exactly the sign mask
    zigzag needs. The result is masked back to 64 bits because Python integers
    are unbounded.
    """
    return uvarint(((value << 1) ^ (value >> 63)) & 0xFFFFFFFFFFFFFFFF)


def yson_string(data: bytes) -> bytes:
    """0x01 + zigzag length + raw bytes."""
    return b"\x01" + zigzag(len(data)) + data


def yson_int64(value: int) -> bytes:
    return b"\x02" + zigzag(value)


def yson_uint64(value: int) -> bytes:
    return b"\x06" + uvarint(value)


def yson_double(value: float) -> bytes:
    return b"\x03" + struct.pack("<d", value)


def yson_bool(value: bool) -> bytes:
    return b"\x05" if value else b"\x04"


ENTITY = b"#"


def yson_map(pairs) -> bytes:
    out = bytearray(b"{")
    for i, (key, value) in enumerate(pairs):
        if i:
            out += b";"
        out += yson_string(key) + b"=" + value
    out += b"}"
    return bytes(out)


def yson_list(items) -> bytes:
    out = bytearray(b"[")
    for i, item in enumerate(items):
        if i:
            out += b";"
        out += item
    out += b"]"
    return bytes(out)


def control(key: bytes, value: bytes) -> bytes:
    """`<key=value>#` — the shape of every control record."""
    return b"<" + yson_string(key) + b"=" + value + b">" + ENTITY


# ------------------------------------------------------------------ data


def rows_table_0():
    """Every scalar type the YSON spec defines, plus the awkward cases."""
    return [
        # Integer boundaries.
        yson_map(
            [
                (b"name", yson_string(b"integers")),
                (b"int_min", yson_int64(-(2**63))),
                (b"int_max", yson_int64(2**63 - 1)),
                (b"int_zero", yson_int64(0)),
                (b"int_neg", yson_int64(-1)),
                (b"uint_max", yson_uint64(2**64 - 1)),
                (b"uint_zero", yson_uint64(0)),
            ]
        ),
        # Floating point, including the values that do not survive text format.
        yson_map(
            [
                (b"name", yson_string(b"doubles")),
                (b"pi", yson_double(math.pi)),
                (b"zero", yson_double(0.0)),
                (b"neg_zero", yson_double(-0.0)),
                (b"inf", yson_double(math.inf)),
                (b"neg_inf", yson_double(-math.inf)),
                (b"nan", yson_double(math.nan)),
                (b"tiny", yson_double(5e-324)),
                (b"huge", yson_double(1.7976931348623157e308)),
            ]
        ),
        # Booleans and entity (null).
        yson_map(
            [
                (b"name", yson_string(b"booleans_and_entity")),
                (b"yes", yson_bool(True)),
                (b"no", yson_bool(False)),
                (b"nothing", ENTITY),
            ]
        ),
        # Strings, including bytes that are not valid UTF-8.
        yson_map(
            [
                (b"name", yson_string(b"strings")),
                (b"empty", yson_string(b"")),
                (b"ascii", yson_string(b"hello world")),
                (b"utf8", yson_string("привет мир 🎉".encode())),
                (b"not_utf8", yson_string(bytes([0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]))),
                (b"lone_surrogate", yson_string(bytes([0xED, 0xA0, 0x80]))),
                (b"embedded_nul", yson_string(b"a\x00b")),
                (b"yson_punctuation", yson_string(b"{};[]<>=#")),
                (b"newlines", yson_string(b"line1\nline2\ttab")),
            ]
        ),
        # Composite values.
        yson_map(
            [
                (b"name", yson_string(b"composites")),
                (b"empty_list", yson_list([])),
                (b"empty_map", yson_map([])),
                (b"numbers", yson_list([yson_int64(i) for i in range(1, 6)])),
                (
                    b"nested",
                    yson_map(
                        [
                            (
                                b"inner",
                                yson_list(
                                    [yson_string(b"a"), yson_map([(b"deep", yson_int64(1))])]
                                ),
                            ),
                        ]
                    ),
                ),
                (
                    b"mixed",
                    yson_list([yson_int64(1), yson_string(b"two"), yson_bool(True), ENTITY]),
                ),
            ]
        ),
        # NOTE: a column value carrying attributes is deliberately absent.
        # YTsaurus rejects it at write time with "Table values cannot have
        # top-level attributes", so a job can never receive one and a fixture
        # containing it would not be faithful. Attributes still appear in the
        # job stream — that is what control records are — and the codec's
        # handling of them is covered by the ytsaurus-yson test suite.
        #
        # A wide-ish row and a large string, to cross buffer boundaries.
        yson_map(
            [(b"name", yson_string(b"wide"))]
            + [(f"column_{i:04}".encode(), yson_int64(i)) for i in range(500)]
        ),
        yson_map(
            [
                (b"name", yson_string(b"large")),
                (b"blob", yson_string(b"x" * 300_000)),
            ]
        ),
    ]


def rows_table_1():
    return [
        yson_map(
            [
                (b"name", yson_string(b"second_table")),
                (b"index", yson_int64(i)),
            ]
        )
        for i in range(5)
    ]


def main() -> None:
    FIXTURES.mkdir(parents=True, exist_ok=True)

    for index, rows in enumerate((rows_table_0(), rows_table_1())):
        payload = b"".join(row + b";" for row in rows)
        path = FIXTURES / f"table_rows_{index}.bin"
        path.write_bytes(payload)
        print(f"{path.name}: {len(rows)} rows, {len(payload)} bytes")


if __name__ == "__main__":
    main()
