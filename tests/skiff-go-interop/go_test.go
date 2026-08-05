package skiffinterop

import (
	"bytes"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"go.ytsaurus.tech/yt/go/skiff"
)

// This is intentionally a version-pinned executable reference, not a second
// implementation. Rust tests consume the matching schema shape; later codec
// tests will also consume this exact Go-produced byte vector in both directions.
func TestV0033ReferenceEncoderVector(t *testing.T) {
	schema := skiff.Schema{
		Type: skiff.TypeTuple,
		Children: []skiff.Schema{
			{Name: "found", Type: skiff.TypeUint64},
			{Name: "rcl", Type: skiff.TypeString32},
		},
	}
	type row struct {
		Found uint64 `yson:"found"`
		Rcl   string `yson:"rcl"`
	}

	var out bytes.Buffer
	encoder, err := skiff.NewEncoder(&out, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Write(row{Found: 7, Rcl: "abc"}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Flush(); err != nil {
		t.Fatal(err)
	}

	want := []byte{
		0x00, 0x00, // Variant16: table 0
		0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00, 'a', 'b', 'c',
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("Go SDK v0.0.33 vector = %x, want %x", out.Bytes(), want)
	}

	format := skiff.Format{Name: "skiff", TableSchemas: []any{&schema}}
	decoder, err := skiff.NewDecoder(bytes.NewReader(want), format)
	if err != nil {
		t.Fatal(err)
	}
	if !decoder.Next() {
		t.Fatalf("Go SDK v0.0.33 decoder did not read its reference vector: %v", decoder.Err())
	}
	var decoded row
	if err := decoder.Scan(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != (row{Found: 7, Rcl: "abc"}) {
		t.Fatalf("Go SDK v0.0.33 decoded %+v", decoded)
	}
	if decoder.Next() || decoder.Err() != nil {
		t.Fatalf("Go SDK v0.0.33 stream did not finish cleanly: %v", decoder.Err())
	}
}

func TestV0033SharedScalarCorpusInBothDirections(t *testing.T) {
	expected, err := readHexCorpus("scalar_row.hex")
	if err != nil {
		t.Fatal(err)
	}

	schema := skiff.Schema{
		Type: skiff.TypeTuple,
		Children: []skiff.Schema{
			{Name: "bool", Type: skiff.TypeBoolean},
			{Name: "i8", Type: skiff.TypeInt8},
			{Name: "i16", Type: skiff.TypeInt16},
			{Name: "i32", Type: skiff.TypeInt32},
			{Name: "i64", Type: skiff.TypeInt64},
			{Name: "u8", Type: skiff.TypeUint8},
			{Name: "u16", Type: skiff.TypeUint16},
			{Name: "u32", Type: skiff.TypeUint32},
			{Name: "u64", Type: skiff.TypeUint64},
			{Name: "f64", Type: skiff.TypeDouble},
			{Name: "bytes", Type: skiff.TypeString32},
		},
	}

	var out bytes.Buffer
	encoder, err := skiff.NewEncoder(&out, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteRow([]any{
		true, int8(-8), int16(-16), int32(-32), int64(-64),
		uint8(8), uint16(16), uint32(32), uint64(64), -1.5, []byte{0xff, 'a'},
	}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), expected) {
		t.Fatalf("Go SDK v0.0.33 scalar vector = %x, want shared corpus %x", out.Bytes(), expected)
	}

	type row struct {
		Bool  bool    `yson:"bool"`
		I8    int8    `yson:"i8"`
		I16   int16   `yson:"i16"`
		I32   int32   `yson:"i32"`
		I64   int64   `yson:"i64"`
		U8    uint8   `yson:"u8"`
		U16   uint16  `yson:"u16"`
		U32   uint32  `yson:"u32"`
		U64   uint64  `yson:"u64"`
		F64   float64 `yson:"f64"`
		Bytes []byte  `yson:"bytes"`
	}
	format := skiff.Format{Name: "skiff", TableSchemas: []any{&schema}}
	decoder, err := skiff.NewDecoder(bytes.NewReader(expected), format)
	if err != nil {
		t.Fatal(err)
	}
	if !decoder.Next() {
		t.Fatalf("Go SDK v0.0.33 decoder did not read scalar corpus: %v", decoder.Err())
	}
	var decoded row
	if err := decoder.Scan(&decoded); err != nil {
		t.Fatal(err)
	}
	want := row{
		Bool: true, I8: -8, I16: -16, I32: -32, I64: -64,
		U8: 8, U16: 16, U32: 32, U64: 64, F64: -1.5, Bytes: []byte{0xff, 'a'},
	}
	if !bytes.Equal(decoded.Bytes, want.Bytes) {
		t.Fatalf("Go SDK v0.0.33 decoded bytes %x, want %x", decoded.Bytes, want.Bytes)
	}
	if decoded.Bool != want.Bool || decoded.I8 != want.I8 || decoded.I16 != want.I16 ||
		decoded.I32 != want.I32 || decoded.I64 != want.I64 || decoded.U8 != want.U8 ||
		decoded.U16 != want.U16 || decoded.U32 != want.U32 || decoded.U64 != want.U64 ||
		decoded.F64 != want.F64 {
		t.Fatalf("Go SDK v0.0.33 decoded %+v, want %+v", decoded, want)
	}
	if decoder.Next() || decoder.Err() != nil {
		t.Fatalf("Go SDK v0.0.33 scalar stream did not finish cleanly: %v", decoder.Err())
	}
}

func TestV0033SharedOptionalCorpusInBothDirections(t *testing.T) {
	expected, err := readHexCorpus("optional_row.hex")
	if err != nil {
		t.Fatal(err)
	}
	optionalString := func(name string) skiff.Schema {
		return skiff.Schema{
			Name: name,
			Type: skiff.TypeVariant8,
			Children: []skiff.Schema{
				{Type: skiff.TypeNothing},
				{Type: skiff.TypeString32},
			},
		}
	}
	schema := skiff.Schema{
		Type: skiff.TypeTuple,
		Children: []skiff.Schema{
			optionalString("absent"),
			optionalString("present"),
		},
	}

	var out bytes.Buffer
	encoder, err := skiff.NewEncoder(&out, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteRow([]any{nil, []byte{0xff, 'a'}}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), expected) {
		t.Fatalf("Go SDK v0.0.33 optional vector = %x, want shared corpus %x", out.Bytes(), expected)
	}

	type row struct {
		Absent  *[]byte `yson:"absent"`
		Present *[]byte `yson:"present"`
	}
	format := skiff.Format{Name: "skiff", TableSchemas: []any{&schema}}
	decoder, err := skiff.NewDecoder(bytes.NewReader(expected), format)
	if err != nil {
		t.Fatal(err)
	}
	if !decoder.Next() {
		t.Fatalf("Go SDK v0.0.33 decoder did not read optional corpus: %v", decoder.Err())
	}
	var decoded row
	if err := decoder.Scan(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Absent != nil || decoded.Present == nil || !bytes.Equal(*decoded.Present, []byte{0xff, 'a'}) {
		t.Fatalf("Go SDK v0.0.33 decoded optional row %+v", decoded)
	}
	if decoder.Next() || decoder.Err() != nil {
		t.Fatalf("Go SDK v0.0.33 optional stream did not finish cleanly: %v", decoder.Err())
	}
}

func TestV0033SharedJobControlCorpus(t *testing.T) {
	expected, err := readHexCorpus("control_rows.hex")
	if err != nil {
		t.Fatal(err)
	}
	optionalIndex := func(name string) skiff.Schema {
		return skiff.Schema{
			Name: name,
			Type: skiff.TypeVariant8,
			Children: []skiff.Schema{
				{Type: skiff.TypeNothing},
				{Type: skiff.TypeInt64},
			},
		}
	}
	schema := skiff.Schema{
		Type: skiff.TypeTuple,
		Children: []skiff.Schema{
			{Name: "$key_switch", Type: skiff.TypeBoolean},
			optionalIndex("$row_index"),
			optionalIndex("$range_index"),
			{Name: "count", Type: skiff.TypeUint64},
		},
	}
	format := skiff.Format{Name: "skiff", TableSchemas: []any{&schema}}
	decoder, err := skiff.NewDecoder(bytes.NewReader(expected), format)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		Count uint64 `yson:"count"`
	}
	want := []struct {
		rowIndex, rangeIndex int64
		keySwitch            bool
		count                uint64
	}{
		{rowIndex: 2, rangeIndex: 0, keySwitch: false, count: 7},
		{rowIndex: 3, rangeIndex: 3, keySwitch: true, count: 11},
	}
	for index, expectedRow := range want {
		if !decoder.Next() {
			t.Fatalf("Go SDK v0.0.33 decoder missed control row %d: %v", index, decoder.Err())
		}
		if decoder.TableIndex() != 0 || decoder.RowIndex() != expectedRow.rowIndex ||
			decoder.RangeIndex() != int(expectedRow.rangeIndex) || decoder.KeySwitch() != expectedRow.keySwitch {
			t.Fatalf("Go SDK v0.0.33 control state at row %d = table=%d row=%d range=%d key=%t", index,
				decoder.TableIndex(), decoder.RowIndex(), decoder.RangeIndex(), decoder.KeySwitch())
		}
		var decoded row
		if err := decoder.Scan(&decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Count != expectedRow.count {
			t.Fatalf("Go SDK v0.0.33 decoded count at row %d = %d, want %d", index, decoded.Count, expectedRow.count)
		}
	}
	if decoder.Next() || decoder.Err() != nil {
		t.Fatalf("Go SDK v0.0.33 control stream did not finish cleanly: %v", decoder.Err())
	}
}

func TestV0033ResolvesSchemaRegistryForSharedScalarCorpus(t *testing.T) {
	expected, err := readHexCorpus("scalar_row.hex")
	if err != nil {
		t.Fatal(err)
	}
	schema := skiff.Schema{
		Type: skiff.TypeTuple,
		Children: []skiff.Schema{
			{Name: "bool", Type: skiff.TypeBoolean},
			{Name: "i8", Type: skiff.TypeInt8},
			{Name: "i16", Type: skiff.TypeInt16},
			{Name: "i32", Type: skiff.TypeInt32},
			{Name: "i64", Type: skiff.TypeInt64},
			{Name: "u8", Type: skiff.TypeUint8},
			{Name: "u16", Type: skiff.TypeUint16},
			{Name: "u32", Type: skiff.TypeUint32},
			{Name: "u64", Type: skiff.TypeUint64},
			{Name: "f64", Type: skiff.TypeDouble},
			{Name: "bytes", Type: skiff.TypeString32},
		},
	}
	format := skiff.Format{
		Name:         "skiff",
		TableSchemas: []any{"$scalar"},
		SchemaRegistry: map[string]*skiff.Schema{
			"scalar": &schema,
		},
	}
	decoder, err := skiff.NewDecoder(bytes.NewReader(expected), format)
	if err != nil {
		t.Fatal(err)
	}
	if !decoder.Next() {
		t.Fatalf("Go SDK v0.0.33 registry decoder did not read scalar corpus: %v", decoder.Err())
	}
	type row struct {
		Bool bool `yson:"bool"`
	}
	var decoded row
	if err := decoder.Scan(&decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Bool {
		t.Fatal("Go SDK v0.0.33 registry decoder lost the first scalar field")
	}
	if decoder.Next() || decoder.Err() != nil {
		t.Fatalf("Go SDK v0.0.33 registry stream did not finish cleanly: %v", decoder.Err())
	}
}

func readHexCorpus(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var digits strings.Builder
	for _, line := range strings.Split(string(data), "\n") {
		if before, _, found := strings.Cut(line, "#"); found {
			line = before
		}
		for _, character := range line {
			if character != ' ' && character != '\t' && character != '\r' {
				digits.WriteRune(character)
			}
		}
	}
	return hex.DecodeString(digits.String())
}
