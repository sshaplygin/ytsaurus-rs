#!/usr/bin/env python3
"""Guards the one invariant that ties the captured fixtures together.

`cat_input.bin` is what a job receives on fd 0; `cat_expected_table_0.bin` and
`cat_expected_table_1.bin` are what the same tables return when read back. The
two are captured by separate cluster round-trips, so nothing forces them to
agree — but the offline `cat_e2e` test asserts that reading the job input
reproduces the expected rows. If the cluster ever reframes rows on their way
into a job (different string encoding, reordered columns, re-chunked values),
the fixtures would quietly disagree and the offline test would be checking a
premise that no longer holds.

So: each table's rows must appear *verbatim*, as a contiguous byte run, inside
the job input. Control records may surround them; they may not rewrite them.

    python3 tests/cluster-e2e/check_fixture_consistency.py tests/cluster-e2e/fixtures

How this fails: it exits non-zero with the offending table index as soon as a
table's bytes are not a substring of the job input. Flipping a single byte in
either file — `printf '\\x00' | dd of=fixtures/cat_input.bin bs=1 seek=100
conv=notrunc` on a copy — is enough to trip it, which is how it was proved to
fail rather than merely to stay silent.
"""

import pathlib
import sys

TABLES = (0, 1)


def main() -> None:
    fixtures = pathlib.Path(sys.argv[1])
    job_input = (fixtures / "cat_input.bin").read_bytes()

    for index in TABLES:
        rows = (fixtures / f"cat_expected_table_{index}.bin").read_bytes()
        if job_input.find(rows) < 0:
            sys.exit(f"table {index} rows are not a verbatim substring of the job input")

    print("   consistency check passed: table rows appear verbatim in the job input")


if __name__ == "__main__":
    main()
