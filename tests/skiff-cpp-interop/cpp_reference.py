#!/usr/bin/env python3
"""Regenerates the C++ Skiff byte vectors the Rust tests read.

The reference is the real C++ implementation: `yt_yson_bindings` is a compiled
extension over `library/cpp/skiff`, so every byte below was produced by
`TUncheckedSkiffWriter` and read back by `TCheckedSkiffParser`. It is not a
second implementation of the format, and it needs no Arcadia build — a wheel.

Two families, because the bindings offer two doors into the same C++ code and
neither reaches everything:

*Record vectors* go through `SkiffRecord`, which takes a hand-written Skiff
schema and so controls the wire shape exactly, but whose field types are
limited to int64, uint64, boolean, double, string32 and yson32
(`yt/yt/python/yson/skiff/record.cpp` aborts the process on anything else).

*Structured vectors* go through the typed-dataclass layer, which derives the
Skiff schema from Python annotations. That gives up control of the leaf types —
the inference widens every integer to int64 — but it is the only door that emits
`repeated_variant8` and nested `tuple`, the shapes YT's composite types map onto.

Running this script rewrites `*.hex` and fails if the C++ writer and the C++
parser disagree about any vector.
"""

from __future__ import annotations

import sys
from io import BytesIO
from pathlib import Path
from typing import Any, NotRequired, TypedDict

import yt.wrapper.schema as typed_schema

# The bindings resolve `yt.yson.yson_types` lazily from inside C++. Importing it
# here is load-bearing, not tidiness.
import yt.yson.yson_types  # noqa: F401
from yt.wrapper.format import StructuredSkiffFormat
from yt.wrapper.schema import yt_dataclass
from yt_yson_bindings import SkiffSchema, dump_skiff, load_skiff

HERE = Path(__file__).parent

ROW_INDEX = "$row_index"
RANGE_INDEX = "$range_index"


class SchemaNode(TypedDict):
    """One node of a Skiff schema, in the JSON-ish shape the bindings take.

    Both halves of the script produce these: the record family hand-writes them
    below, and the typed layer derives one per structured vector. `name` is
    absent on anonymous leaves and `children` on every scalar wire type, which
    is why only `wire_type` is required.
    """

    wire_type: str
    name: NotRequired[str]
    children: NotRequired[list[SchemaNode]]


def optional(wire_type: str, name: str) -> SchemaNode:
    """`variant8<nothing; wire_type>`, the Skiff spelling of a nullable column."""
    return {
        "wire_type": "variant8",
        "name": name,
        "children": [{"wire_type": "nothing"}, {"wire_type": wire_type}],
    }


# ---------------------------------------------------------------- record family

# What one record field can hold. `str` never appears on the writing side — it is
# what the C++ parser may hand back for a string32, and `normalize` is what makes
# the two comparable.
FieldValue = int | float | bool | bytes | str | None


class RecordVector(TypedDict):
    file: str
    doc: list[str]
    schemas: list[SchemaNode]
    rows: list[tuple[int, dict[str, FieldValue]]]


# Each vector: the table schemas it declares, and the rows to write through them.
# A row is `(table_index, {field: value})`. Multi-table vectors are assembled per
# row and read back multiplexed, because the C++ writer emits one table per
# stream while the reader consumes the concatenation of all of them — that
# asymmetry is the protocol, not a limitation of this script.
RECORD_VECTORS: list[RecordVector] = [
    {
        "file": "scalars.hex",
        "doc": [
            "Scalar columns: int64, uint64, boolean, double, string32.",
            "",
            "Row 1: minimums and a non-UTF-8 string32 payload.",
            "Row 2: maximums, negative zero and an empty string32.",
        ],
        "schemas": [
            {
                "wire_type": "tuple",
                "children": [
                    {"wire_type": "int64", "name": "i"},
                    {"wire_type": "uint64", "name": "u"},
                    {"wire_type": "boolean", "name": "b"},
                    {"wire_type": "double", "name": "d"},
                    {"wire_type": "string32", "name": "s"},
                ],
            }
        ],
        "rows": [
            (0, {"i": -9223372036854775808, "u": 0, "b": False, "d": -0.0, "s": b"\xff\x61"}),
            (
                0,
                {
                    "i": 9223372036854775807,
                    "u": 18446744073709551614,
                    "b": True,
                    "d": 1.5,
                    "s": b"",
                },
            ),
        ],
    },
    {
        "file": "optional.hex",
        "doc": [
            "Optional columns, both variant8 tags.",
            "",
            "Row 1: tag 0 (nothing) for both columns.",
            "Row 2: tag 1 (present) for both columns.",
        ],
        "schemas": [
            {
                "wire_type": "tuple",
                "children": [optional("int64", "oi"), optional("string32", "os")],
            }
        ],
        "rows": [
            (0, {"oi": None, "os": None}),
            (0, {"oi": -5, "os": b"\xffa"}),
        ],
    },
    {
        "file": "sparse.hex",
        "doc": [
            "A dense column followed by $sparse_columns: repeated_variant16.",
            "",
            "Row 1: no sparse field set, so only the 0xFFFF end tag.",
            "Row 2: the second sparse field only.",
            "Row 3: both sparse fields, in schema order.",
        ],
        "schemas": [
            {
                "wire_type": "tuple",
                "children": [
                    {"wire_type": "int64", "name": "dense"},
                    {
                        "wire_type": "repeated_variant16",
                        "name": "$sparse_columns",
                        "children": [
                            {"wire_type": "int64", "name": "sp1"},
                            {"wire_type": "string32", "name": "sp2"},
                        ],
                    },
                ],
            }
        ],
        "rows": [
            (0, {"dense": 1}),
            (0, {"dense": 2, "sp2": b"z"}),
            (0, {"dense": 3, "sp1": 42, "sp2": b"q"}),
        ],
    },
    {
        "file": "other_columns.hex",
        "doc": [
            "A dense column followed by $other_columns: yson32.",
            "",
            "The payload is the binary YSON map the C++ writer builds for every",
            "value the schema does not name. Rust carries those bytes opaquely,",
            "so this vector pins the framing and the C++ YSON spelling at once.",
        ],
        "schemas": [
            {
                "wire_type": "tuple",
                "children": [
                    {"wire_type": "int64", "name": "dense"},
                    {"wire_type": "yson32", "name": "$other_columns"},
                ],
            }
        ],
        "rows": [
            (0, {"dense": 7, "extra": b"hello", "num": 3}),
        ],
    },
    {
        "file": "system_columns.hex",
        "doc": [
            "Job control columns ahead of the data column:",
            "$row_index and $range_index as variant8<nothing;int64>,",
            "$key_switch as boolean.",
            "",
            "Row 1: every control field absent, key switch false.",
            "Row 2: row index 5, range index 2, key switch true.",
        ],
        "schemas": [
            {
                "wire_type": "tuple",
                "children": [
                    optional("int64", ROW_INDEX),
                    optional("int64", RANGE_INDEX),
                    {"wire_type": "boolean", "name": "$key_switch"},
                    {"wire_type": "int64", "name": "a"},
                ],
            }
        ],
        "rows": [
            (0, {ROW_INDEX: None, RANGE_INDEX: None, "$key_switch": False, "a": 7}),
            (0, {ROW_INDEX: 5, RANGE_INDEX: 2, "$key_switch": True, "a": 9}),
        ],
    },
    {
        "file": "multi_table.hex",
        "doc": [
            "Two table schemas multiplexed into one input stream.",
            "",
            "The Variant16 prefix selects the table: 0x0000 for the int64 table,",
            "0x0001 for the string32 one. Rows alternate 0, 1, 0.",
        ],
        "schemas": [
            {"wire_type": "tuple", "children": [{"wire_type": "int64", "name": "a"}]},
            {"wire_type": "tuple", "children": [{"wire_type": "string32", "name": "b"}]},
        ],
        "rows": [
            (0, {"a": 11}),
            (1, {"b": b"xy"}),
            (0, {"a": 12}),
        ],
    },
]


def skiff_schema(schema: SchemaNode) -> SkiffSchema:
    """One C++ `TSkiffSchema` for one table.

    The bindings take exactly one table schema per object; a multiplexed stream
    is described by a list of these, one per table index.
    """
    return SkiffSchema([schema], {}, RANGE_INDEX, ROW_INDEX)


def encode_record_row(schema: SchemaNode, fields: dict[str, FieldValue], table_index: int) -> bytes:
    """Writes one row through the C++ writer, under `table_index`'s tag.

    The writer always emits tag 0, because it writes one table per stream. The
    reader multiplexes, so a vector for table `i` carries tag `i`. Only the
    two-byte prefix is rewritten here; every byte after it is the C++ writer's,
    and `verify_record` reads the result back through the C++ parser, so a wrong
    tag fails the run rather than reaching the checked-in file.
    """
    compiled = skiff_schema(schema)
    record = compiled.create_record()
    for name, value in fields.items():
        record[name] = value
    stream = BytesIO()
    dump_skiff([record], [stream], schemas=[compiled])
    encoded = stream.getvalue()
    assert encoded[:2] == b"\x00\x00", "the C++ writer is expected to tag its single table 0"
    return table_index.to_bytes(2, "little") + encoded[2:]


def normalize(value: FieldValue) -> FieldValue:
    """Compares written and read-back values without minding text vs bytes."""
    if isinstance(value, str):
        return value.encode()
    return value


def verify_record(vector: RecordVector, encoded: bytes) -> None:
    """Reads the vector back through the C++ parser and compares every field."""
    decoded = list(
        load_skiff(
            BytesIO(encoded),
            schemas=[skiff_schema(schema) for schema in vector["schemas"]],
            row_index_column_name=ROW_INDEX,
            range_index_column_name=RANGE_INDEX,
            # Without this the bindings wrap every string in a YSON value type,
            # and the lazy import behind that wrapping fails for any payload that
            # is not valid UTF-8 — which the corpus deliberately contains.
            # `encoding=None` returns the payload bytes unchanged, which is also
            # what a byte-exact comparison wants.
            encoding=None,
        )
    )
    expected = vector["rows"]
    if len(decoded) != len(expected):
        raise SystemExit(
            f"{vector['file']}: the C++ parser read {len(decoded)} rows, expected {len(expected)}"
        )
    # `strict` is redundant after the length check above, and it is here anyway:
    # the check is the thing that could be edited away, and a silent truncation
    # would turn a missing row into a passing run.
    for index, (record, (_, fields)) in enumerate(zip(decoded, expected, strict=True)):
        for name, want in fields.items():
            got = record[name]
            if normalize(got) != normalize(want):
                raise SystemExit(
                    f"{vector['file']} row {index} field {name!r}: "
                    f"the C++ parser read {got!r}, the C++ writer was given {want!r}"
                )


# ------------------------------------------------------------ structured family


@yt_dataclass
class Inner:
    x: typed_schema.Int64
    y: str


@yt_dataclass
class Composite:
    """The shapes YT's composite types map onto, as C++ writes them.

    `list` becomes `repeated_variant8` with tag 0 per item and the 0xFF
    terminator; a nested dataclass becomes a `tuple`; `Optional` becomes
    `variant8<nothing; T>`. Nothing else here can produce a `repeated_variant8`
    at all — not the record layer above, and not the Go SDK, whose codec has no
    repeated-variant path in either direction.
    """

    id: typed_schema.Int64
    tags: list[str]
    nested: Inner
    maybe: typed_schema.Int64 | None


class StructuredVector(TypedDict):
    # `dataclass` and `rows` are deliberately `Any`: `@yt_dataclass` builds the
    # class at runtime, and a type checker sees only the bare annotated body, so
    # there is no visible type here to name.
    file: str
    doc: list[str]
    dataclass: type[Any]
    rows: list[Any]


STRUCTURED_VECTORS: list[StructuredVector] = [
    {
        "file": "composite.hex",
        "doc": [
            "Composite types as the C++ writer emits them, from a typed schema:",
            "list -> repeated_variant8<string32>, struct -> tuple,",
            "Optional -> variant8<nothing;int64>.",
            "",
            "Row 1: empty list, absent optional.",
            "Row 2: two list items, present optional.",
        ],
        "dataclass": Composite,
        # The `__init__` these call is generated by `@yt_dataclass` at runtime,
        # so a type checker, which does not run class decorators, sees a class
        # with no constructor arguments at all.
        #
        # The ignore sits on the **inner** `Inner(...)` only, and that is not an
        # oversight: mypy types an argument it has already reported as `Any` and
        # stops reporting the call enclosing it, so silencing `Inner` silences
        # `Composite` too. An ignore on `Composite(` as well would be unused —
        # and `warn_unused_ignores` cannot tell you so here, because the outer
        # errors only exist while the inner ones are unsuppressed. Hoisting
        # `Inner(...)` into a local would bring them back.
        "rows": [
            Composite(
                id=1,
                tags=[],
                nested=Inner(x=7, y="z"),  # type: ignore[call-arg]
                maybe=None,
            ),
            Composite(
                id=2,
                tags=["ab", ""],
                nested=Inner(x=-1, y=""),  # type: ignore[call-arg]
                maybe=-5,
            ),
        ],
    },
]


def structured_format(dataclass: type[Any], for_reading: bool) -> StructuredSkiffFormat:
    return StructuredSkiffFormat(
        [typed_schema._create_row_py_schema(dataclass)], for_reading=for_reading
    )


def encode_structured(vector: StructuredVector) -> bytes:
    stream = BytesIO()
    structured_format(vector["dataclass"], for_reading=False)._dump_rows(vector["rows"], stream)
    return stream.getvalue()


def verify_structured(vector: StructuredVector, encoded: bytes) -> None:
    decoded = list(
        structured_format(vector["dataclass"], for_reading=True).load_rows(BytesIO(encoded))
    )
    if decoded != vector["rows"]:
        raise SystemExit(
            f"{vector['file']}: the C++ parser read {decoded!r}, "
            f"the C++ writer was given {vector['rows']!r}"
        )


def inferred_schema(vector: StructuredVector) -> SchemaNode:
    """The Skiff schema the typed layer derived, recorded in the vector header.

    The Rust test hand-writes the same shape, so a change upstream to how a
    `list` or a nested dataclass is mapped should be visible in the diff rather
    than only in a failing assertion.

    The typed layer ships no type information, so `SchemaNode` is asserted of
    what it returns rather than checked; if upstream ever returns something
    else, `describe` is where it surfaces.
    """
    derived: SchemaNode = typed_schema._row_py_schema_to_skiff_schema(
        typed_schema._create_row_py_schema(vector["dataclass"])
    )
    return derived


# ------------------------------------------------------------------- rendering


def render(doc: list[str], rows: list[tuple[str, bytes]], width: int | None = None) -> str:
    """Formats a vector as the commented hex the Rust tests read.

    `width` wraps a block at that many bytes per line; without it each entry is
    one line, which is what the per-row record vectors want.
    """
    lines = [
        "# YTsaurus C++ Skiff reference vector.",
        "#",
        "# Generated by tests/skiff-cpp-interop/cpp_reference.py against the",
        "# pinned ytsaurus-yson bindings. Do not hand-edit.",
        "#",
    ]
    lines += [f"# {line}".rstrip() for line in doc]
    for label, row in rows:
        lines.append(f"# {label}")
        chunks = [row[at : at + width] for at in range(0, len(row), width)] if width else [row]
        lines += [" ".join(f"{byte:02x}" for byte in chunk) for chunk in chunks]
    return "\n".join(lines) + "\n"


def describe(schema: SchemaNode, indent: int = 0) -> list[str]:
    """One `wire_type<child; child>` line per node, for the vector header."""
    name = schema.get("name")
    label = schema["wire_type"] + (f" {name!r}" if name else "")
    lines = [" " * indent + label]
    for child in schema.get("children", []):
        lines += describe(child, indent + 2)
    return lines


def main() -> int:
    for vector in RECORD_VECTORS:
        rows = [
            encode_record_row(vector["schemas"][table_index], fields, table_index)
            for table_index, fields in vector["rows"]
        ]
        encoded = b"".join(rows)
        verify_record(vector, encoded)
        labelled = [
            (f"row {index}, table {vector['rows'][index][0]}", row)
            for index, row in enumerate(rows)
        ]
        (HERE / vector["file"]).write_text(render(vector["doc"], labelled))
        print(f"{vector['file']}: {len(rows)} rows, {len(encoded)} bytes")

    # A name of its own, rather than reusing `vector`: the two families are
    # different shapes, and one loop variable cannot be both.
    for structured in STRUCTURED_VECTORS:
        encoded = encode_structured(structured)
        verify_structured(structured, encoded)
        doc = structured["doc"] + ["", "Schema derived by the typed layer:"]
        doc += ["  " + line for line in describe(inferred_schema(structured))]
        # Emitted as one block rather than split per row. The structured writer
        # writes the whole batch in one call, and there is no way to find a row
        # boundary without parsing: rows are variable-length and 0x0000 occurs
        # inside payloads, so scanning for the next table tag mislabels them.
        block = [
            (f"{len(structured['rows'])} rows, {len(encoded)} bytes", encoded),
        ]
        (HERE / structured["file"]).write_text(render(doc, block, width=16))
        print(f"{structured['file']}: {len(structured['rows'])} rows, {len(encoded)} bytes")

    return 0


if __name__ == "__main__":
    sys.exit(main())
