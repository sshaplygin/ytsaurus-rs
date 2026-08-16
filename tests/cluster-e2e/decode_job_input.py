#!/usr/bin/env python3
"""Turns the cluster's capture row back into the raw job input fixture.

`capture_fixtures.sh` runs a map operation whose mapper base64s the whole of
its stdin into a single column. That indirection is not decoration: the job's
stdin is binary YSON, and the only way to carry arbitrary bytes back out of a
job through a *text* output format is to encode them. The cluster then hands
the row back via `yt read-table --format json`, which writes one JSON object
per line.

This script undoes both wrappers — JSON, then base64 — and writes the bytes
that a job actually received on fd 0 to `cat_input.bin`, the fixture the
offline `cat_e2e` test replays.

    python3 tests/cluster-e2e/decode_job_input.py capture.json cat_input.bin

Only the first line is read. The caller has already asserted `@row_count == 1`,
because a capture split across two jobs would silently yield a truncated
fixture; taking line one of a multi-row read-back would hide that, so the guard
belongs at the point where the row count is known, not here.

The output is committed, so re-decoding the same capture must produce no diff.
"""

import base64
import json
import pathlib
import sys


def main() -> None:
    capture_path, output_path = sys.argv[1:3]

    first_line = pathlib.Path(capture_path).read_text().strip().split("\n")[0]
    row = json.loads(first_line)
    data = base64.b64decode(row["capture"])

    pathlib.Path(output_path).write_bytes(data)
    print(f"   captured {len(data)} bytes")


if __name__ == "__main__":
    main()
