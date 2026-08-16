#!/usr/bin/env python3
"""Independently recomputes the pilot's expected result and compares.

The point of this script is that it shares no code with the Rust worker. It
reads the same input with the *official* Python YSON parser, applies the same
validation and sessionization rules, and checks the cluster's output against
that. If both agreed only because they were the same implementation, the check
would be worth nothing.

    python3 check_pilot_output.py events.yson users.json sessions.json <clean_count>
"""

import json
import math
import sys
from collections import defaultdict

from yt import yson

SESSION_GAP_US = 30 * 60 * 1_000_000

REQUIRED = (
    "user_id",
    "timestamp",
    "url",
    "user_agent",
    "status",
    "bytes_sent",
    "is_mobile",
    "latency_ms",
)


def fail(message: str) -> None:
    print(f"   \033[31mFAIL\033[0m {message}", file=sys.stderr)
    sys.exit(1)


def ok(message: str) -> None:
    print(f"   \033[32mok\033[0m {message}")


def is_boolean(v) -> bool:
    """True for a YSON boolean.

    `bool` cannot be subclassed in Python, so the YSON bindings model booleans
    with `YsonBoolean`, which derives from `int`. A plain `isinstance(v, bool)`
    is therefore False for every value this parser produces.
    """
    return isinstance(v, (bool, yson.YsonBoolean))


def is_integer(v) -> bool:
    """True for a YSON int64/uint64, excluding booleans."""
    return isinstance(v, int) and not is_boolean(v)


def is_null(v) -> bool:
    """True for a YSON entity (`#`), which models null.

    It does not compare `is None`: the parser returns a `YsonEntity` instance,
    a distinct object, so an identity check silently misses every null.
    """
    return v is None or isinstance(v, yson.YsonEntity)


def is_valid(event: dict) -> bool:
    """Mirrors `validate` in sessionize.rs, plus the parse step."""
    for column in REQUIRED:
        if column not in event or is_null(event[column]):
            return False

    if not is_integer(event["timestamp"]):
        return False
    if not is_integer(event["status"]):
        return False
    if not is_integer(event["bytes_sent"]):
        return False
    if not is_boolean(event["is_mobile"]):
        return False
    if not isinstance(event["latency_ms"], float):
        return False

    if len(event["user_id"]) == 0:
        return False
    if event["timestamp"] <= 0:
        return False
    if not (100 <= event["status"] <= 599):
        return False
    if not math.isfinite(event["latency_ms"]) or event["latency_ms"] < 0.0:
        return False
    if len(event["url"]) == 0:
        return False
    return True


def as_bytes(v) -> bytes:
    """Normalises a YSON string to bytes.

    A UTF-8 string arrives as `YsonUnicode` (a `str`), a non-UTF-8 one as a
    `YsonStringProxy` that must go through `get_bytes`.
    """
    if isinstance(v, bytes):
        return v
    if yson.is_unicode(v):
        return str(v).encode()
    try:
        return yson.get_bytes(v)
    except (TypeError, AttributeError):
        return str(v).encode()


def main() -> None:
    events_path, users_path, sessions_path, clean_count = sys.argv[1:5]

    raw = open(events_path, "rb").read()
    parsed = list(yson.loads(raw, yson_type="list_fragment"))

    valid = [e for e in parsed if is_valid(e)]
    rejected = len(parsed) - len(valid)

    if len(valid) != int(clean_count):
        fail(
            f"the job kept {clean_count} events, this script expects {len(valid)} "
            f"(of {len(parsed)} rows, {rejected} invalid)"
        )
    ok(f"{len(valid)} valid / {rejected} rejected — matches the job")

    # Sessionize independently.
    by_user = defaultdict(list)
    for e in valid:
        by_user[as_bytes(e["user_id"])].append(e)

    expected_sessions = 0
    expected_users = {}
    for user, events in by_user.items():
        events.sort(key=lambda e: e["timestamp"])

        sessions = []
        current = None
        for e in events:
            if current is not None and e["timestamp"] - current["ended_at"] > SESSION_GAP_US:
                sessions.append(current)
                current = None
            if current is None:
                current = {
                    "started_at": e["timestamp"],
                    "ended_at": e["timestamp"],
                    "hits": 0,
                    "bytes_sent": 0,
                    "errors": 0,
                }
            current["ended_at"] = max(current["ended_at"], e["timestamp"])
            current["hits"] += 1
            current["bytes_sent"] += e["bytes_sent"]
            current["errors"] += 1 if e["status"] >= 400 else 0
        if current is not None:
            sessions.append(current)

        expected_sessions += len(sessions)
        expected_users[user] = {
            "sessions": len(sessions),
            "hits": sum(s["hits"] for s in sessions),
            "bytes_sent": sum(s["bytes_sent"] for s in sessions),
            "errors": sum(s["errors"] for s in sessions),
        }

    # Compare against what the cluster produced.
    actual_users = {}
    with open(users_path) as f:
        for line in f:
            if not line.strip():
                continue
            u = json.loads(line)
            actual_users[as_bytes(u["user_id"])] = u

    actual_sessions = sum(1 for line in open(sessions_path) if line.strip())

    if len(actual_users) != len(expected_users):
        fail(f"users: job produced {len(actual_users)}, expected {len(expected_users)}")
    ok(f"{len(actual_users)} users — matches")

    if actual_sessions != expected_sessions:
        fail(f"sessions: job produced {actual_sessions}, expected {expected_sessions}")
    ok(f"{actual_sessions} sessions — matches")

    mismatches = []
    for user, expected in expected_users.items():
        actual = actual_users.get(user)
        if actual is None:
            mismatches.append(f"{user!r}: missing from the job's output")
            continue
        for field in ("sessions", "hits", "bytes_sent", "errors"):
            if actual[field] != expected[field]:
                mismatches.append(
                    f"{user!r}.{field}: job {actual[field]}, expected {expected[field]}"
                )

    if mismatches:
        for m in mismatches[:10]:
            print(f"     {m}", file=sys.stderr)
        fail(f"{len(mismatches)} per-user mismatches")
    ok("every per-user aggregate matches (sessions, hits, bytes_sent, errors)")


if __name__ == "__main__":
    main()
