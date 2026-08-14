// Package rpcinterop is a version-pinned executable reference for the two
// binary formats `ytsaurus-rpc` had to reimplement: the row wire protocol and
// YTsaurus's CRC-64.
//
// It is deliberately not a second implementation. It *produces byte vectors*
// with the Go SDK, which the Rust tests then consume in both directions — the
// same shape as tests/skiff-go-interop/, and for the same reason: a format
// checked only against our own reading of the specification is checked against
// itself.
//
// Run `go test ./...` here to regenerate the vectors; the Rust tests read the
// `.hex` files that appear.
package rpcinterop

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"go.ytsaurus.tech/yt/go/crc64"
	"go.ytsaurus.tech/yt/go/wire"
)

// writeVector records a byte vector for the Rust tests, with a comment naming
// what it encodes so the file is readable on its own.
func writeVector(t *testing.T, name, description string, data []byte) {
	t.Helper()

	var out strings.Builder
	out.WriteString("# " + description + "\n")
	out.WriteString("# Produced by the Go SDK v0.0.33; do not hand-edit.\n")
	for offset := 0; offset < len(data); offset += 16 {
		end := offset + 16
		if end > len(data) {
			end = len(data)
		}
		out.WriteString(hex.EncodeToString(data[offset:end]) + "\n")
	}

	path := name + ".hex"
	if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// rowsetCases are the rowsets whose bytes the Rust encoder must reproduce.
//
// Between them they cover every value type the Go writer handles, the padding
// residues that matter, the null row and the empty row.
func rowsetCases() []struct {
	name        string
	description string
	rowset      []wire.Row
} {
	return []struct {
		name        string
		description string
		rowset      []wire.Row
	}{
		{
			"rowset_empty",
			"a rowset with no rows at all",
			[]wire.Row{},
		},
		{
			"rowset_null_and_empty_rows",
			"a null row followed by a row with no values -- the two must not encode alike",
			[]wire.Row{nil, {}},
		},
		{
			"rowset_scalars",
			"one row holding null, both booleans, int64, uint64 and float64",
			[]wire.Row{{
				wire.NewNull(0),
				wire.NewBool(1, true),
				wire.NewBool(2, false),
				wire.NewInt64(3, -42),
				wire.NewUint64(4, 42),
				wire.NewFloat64(5, 1.25),
			}},
		},
		{
			"rowset_extremes",
			"the boundary values of each scalar type",
			[]wire.Row{{
				wire.NewInt64(0, -9223372036854775808),
				wire.NewInt64(1, 9223372036854775807),
				wire.NewUint64(2, 18446744073709551615),
				wire.NewFloat64(3, 0),
				// math.Copysign, not the literal -0: Go has no negative-zero
				// constant, so `-0` is positive zero and the case would not
				// test the sign bit at all.
				wire.NewFloat64(4, math.Copysign(0, -1)),
			}},
		},
		{
			"rowset_strings",
			"string lengths 0..9, which walks every 8-byte padding residue",
			[]wire.Row{{
				wire.NewBytes(0, []byte("")),
				wire.NewBytes(1, []byte("a")),
				wire.NewBytes(2, []byte("ab")),
				wire.NewBytes(3, []byte("abc")),
				wire.NewBytes(4, []byte("abcd")),
				wire.NewBytes(5, []byte("abcde")),
				wire.NewBytes(6, []byte("abcdef")),
				wire.NewBytes(7, []byte("abcdefg")),
				wire.NewBytes(8, []byte("abcdefgh")),
				wire.NewBytes(9, []byte("abcdefghi")),
			}},
		},
		{
			"rowset_any",
			"a YSON-encoded any value beside a string",
			[]wire.Row{{
				wire.NewBytes(0, []byte("key")),
				wire.NewAny(1, []byte("[1;2;3]")),
			}},
		},
		{
			"rowset_many_rows",
			"several rows, so the row loop is exercised rather than one row",
			[]wire.Row{
				{wire.NewInt64(0, 1), wire.NewBytes(1, []byte("one"))},
				{wire.NewInt64(0, 2), wire.NewBytes(1, []byte("two"))},
				nil,
				{wire.NewInt64(0, 3), wire.NewBytes(1, []byte("three"))},
			},
		},
		{
			"rowset_non_utf8",
			"a byte string that is not valid UTF-8 -- YT columns are byte strings",
			[]wire.Row{{
				wire.NewBytes(0, []byte{0xff, 0xfe, 0x00, 0x80}),
			}},
		},
	}
}

// TestRowsetVectors writes the reference bytes and checks the Go SDK reads its
// own output back, so a vector is never recorded from an encoder the decoder
// disagrees with.
func TestRowsetVectors(t *testing.T) {
	for _, testCase := range rowsetCases() {
		t.Run(testCase.name, func(t *testing.T) {
			encoded, err := wire.MarshalRowset(testCase.rowset)
			if err != nil {
				t.Fatal(err)
			}

			if len(encoded)%8 != 0 {
				t.Fatalf("rowset is %d bytes, which is not 8-byte aligned", len(encoded))
			}

			decoded, err := wire.UnmarshalRowset(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if len(decoded) != len(testCase.rowset) {
				t.Fatalf("round trip changed the row count: %d -> %d",
					len(testCase.rowset), len(decoded))
			}

			writeVector(t, testCase.name, testCase.description, encoded)
		})
	}
}

// TestCompositeWriterDropsItsPayload pins a defect in the Go SDK, because the
// Rust implementation deliberately diverges from it.
//
// wire.Value's writer (yt/go/wire/writer.go, writeValue) handles TypeBytes and
// TypeAny but not TypeComposite, so a composite value's blob is never written —
// though wireSize reserved room for it and the reader will happily read it
// back. The C++ treats Composite as string-like everywhere
// (IsStringLikeType covers String, Any and Composite), and so does
// `ytsaurus-rpc`. If this test ever fails, the Go SDK has been fixed and
// docs/rpc-compatibility.md should be updated.
func TestCompositeWriterDropsItsPayload(t *testing.T) {
	payload := []byte("[1;2;3]")
	encoded, err := wire.MarshalRowset([]wire.Row{{wire.NewComposite(0, payload)}})
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(encoded, payload) {
		t.Fatalf("the Go SDK now writes composite payloads; "+
			"docs/rpc-compatibility.md says it does not. Encoded: %x", encoded)
	}

	// The length word still claims the payload is there, which is what makes
	// the omission silent rather than an error.
	header := encoded[16:24]
	claimed := uint32(header[4]) | uint32(header[5])<<8 | uint32(header[6])<<16 | uint32(header[7])<<24
	if claimed != uint32(len(payload)) {
		t.Fatalf("expected the header to claim %d payload bytes, got %d", len(payload), claimed)
	}
}

// TestCRC64Vectors records checksums over inputs shaped like the ones the bus
// layer actually checksums: a 28-byte fixed packet header prefix, and variable
// headers of a few part counts.
//
// The canonical short vectors in the Go SDK's own crc64_test.go are already
// mirrored in the Rust unit tests; these add the lengths and alignments that
// only occur in real packets.
func TestCRC64Vectors(t *testing.T) {
	inputs := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"fixed_header_prefix", func() []byte {
			// signature, type=Message, flags=None, packet id 1-0-0-0, 1 part:
			// exactly the 28 bytes a handshake packet's header checksum covers.
			header := make([]byte, 28)
			copy(header[0:4], []byte{0x4f, 0x6d, 0x61, 0x78})
			header[8] = 1
			header[24] = 1
			return header
		}()},
		{"variable_header_one_part", func() []byte {
			// one part size of 42, one zero checksum: the 12 bytes a one-part
			// variable header checksums.
			block := make([]byte, 12)
			block[0] = 42
			return block
		}()},
		{"all_byte_values", func() []byte {
			data := make([]byte, 256)
			for i := range data {
				data[i] = byte(i)
			}
			return data
		}()},
		{"long_run", bytes.Repeat([]byte("ytsaurus"), 100)},
	}

	var out strings.Builder
	out.WriteString("# YTsaurus CRC-64 over bus-shaped inputs.\n")
	out.WriteString("# Produced by the Go SDK v0.0.33; do not hand-edit.\n")
	out.WriteString("# Format: <name> <input hex> <checksum hex>\n")
	for _, input := range inputs {
		checksum := crc64.Checksum(input.data)
		out.WriteString(fmt.Sprintf("%s %s %016x\n",
			input.name, hex.EncodeToString(input.data), checksum))
	}

	if err := os.WriteFile("crc64_vectors.txt", []byte(out.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
