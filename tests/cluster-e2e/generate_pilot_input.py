#!/usr/bin/env python3
"""Generates a production-shaped access-log table for the pilot.

Deterministic: a fixed seed, so a failing run reproduces exactly and the
expected result can be recomputed independently by `check_pilot_output.py`.

Mixed in with the well-formed events is a fixed set of corrupt rows, so the
pilot exercises the quarantine path on a real cluster rather than only in unit
tests.

    python3 tests/cluster-e2e/generate_pilot_input.py out.yson
"""

import random
import struct
import sys

# 2026-01-01T00:00:00Z in epoch microseconds.
BASE_US = 1_767_225_600 * 1_000_000
MINUTE_US = 60 * 1_000_000

USERS = 60
SEED = 20260804


# ------------------------------------------------------------ binary YSON


def uvarint(v: int) -> bytes:
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)
    return bytes(out)


def zigzag(v: int) -> bytes:
    return uvarint(((v << 1) ^ (v >> 63)) & 0xFFFFFFFFFFFFFFFF)


def y_bytes(b: bytes) -> bytes:
    return b"\x01" + zigzag(len(b)) + b


def y_int(v: int) -> bytes:
    return b"\x02" + zigzag(v)


def y_uint(v: int) -> bytes:
    return b"\x06" + uvarint(v)


def y_double(v: float) -> bytes:
    return b"\x03" + struct.pack("<d", v)


def y_bool(v: bool) -> bytes:
    return b"\x05" if v else b"\x04"


ENTITY = b"#"


def row(pairs) -> bytes:
    out = bytearray(b"{")
    for i, (k, v) in enumerate(pairs):
        if i:
            out += b";"
        out += y_bytes(k) + b"=" + v
    out += b"}"
    return bytes(out)


# ------------------------------------------------------------------- data

URLS = [
    b"/",
    b"/search",
    b"/item/42",
    b"/cart",
    b"/checkout",
    b"/help",
    b"/api/v1/items",
    b"/static/app.js",
]

# Real user agents are frequently not valid UTF-8; one here deliberately is not.
AGENTS = [
    b"Mozilla/5.0 (X11; Linux x86_64)",
    b"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)",
    bytes([0x4D, 0x6F, 0x7A, 0xFF, 0xFE, 0x2F, 0x35]),  # invalid UTF-8
    b"curl/8.4.0",
]


def good_event(rng, user: bytes, ts: int) -> bytes:
    status = rng.choices([200, 200, 200, 301, 404, 500], k=1)[0]
    return row(
        [
            (b"user_id", y_bytes(user)),
            (b"timestamp", y_int(ts)),
            (b"url", y_bytes(rng.choice(URLS))),
            (b"referer", y_bytes(b"https://example.org/") if rng.random() < 0.6 else ENTITY),
            (b"user_agent", y_bytes(rng.choice(AGENTS))),
            (b"status", y_int(status)),
            (b"bytes_sent", y_uint(rng.randrange(200, 200_000))),
            (b"is_mobile", y_bool(rng.random() < 0.4)),
            (b"latency_ms", y_double(round(rng.uniform(1.0, 900.0), 3))),
        ]
    )


def corrupt_rows() -> list:
    """One row per validation rule, plus structural damage."""
    base = [
        (b"user_id", y_bytes(b"corrupt")),
        (b"timestamp", y_int(BASE_US)),
        (b"url", y_bytes(b"/x")),
        (b"user_agent", y_bytes(b"agent")),
        (b"status", y_int(200)),
        (b"bytes_sent", y_uint(1)),
        (b"is_mobile", y_bool(False)),
        (b"latency_ms", y_double(1.0)),
    ]

    def variant(**overrides):
        return row([(k, overrides.get(k.decode(), v)) for k, v in base])

    return [
        variant(user_id=y_bytes(b"")),  # empty user_id
        variant(user_id=ENTITY),  # null user_id
        variant(timestamp=y_int(0)),  # non-positive timestamp
        variant(timestamp=y_int(-1)),  # negative timestamp
        variant(status=y_int(9999)),  # status out of range
        variant(status=y_int(0)),  # status out of range
        variant(latency_ms=y_double(float("nan"))),  # NaN latency
        variant(latency_ms=y_double(-5.0)),  # negative latency
        variant(url=y_bytes(b"")),  # empty url
        variant(timestamp=y_bytes(b"not-a-number")),  # wrong type
        row(base[:2]),  # missing columns
    ]


def main() -> None:
    rng = random.Random(SEED)
    out = bytearray()

    for u in range(USERS):
        user = f"user-{u:04}".encode()
        # Each user gets a few sessions, separated by gaps well over 30 minutes.
        cursor = BASE_US + rng.randrange(0, 120) * MINUTE_US
        for _ in range(rng.randrange(1, 5)):
            for _ in range(rng.randrange(1, 12)):
                out += good_event(rng, user, cursor) + b";"
                cursor += rng.randrange(1, 20) * MINUTE_US // 2  # < 30 min
            cursor += rng.randrange(31, 180) * MINUTE_US  # session break

    for bad in corrupt_rows():
        out += bad + b";"

    with open(sys.argv[1], "wb") as f:
        f.write(bytes(out))

    print(
        f"   wrote {sys.argv[1]}: {len(out)} bytes, {USERS} users, "
        f"{len(corrupt_rows())} corrupt rows"
    )


if __name__ == "__main__":
    main()
