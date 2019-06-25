package dataframe

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestNewSmartBuilder(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: COL0NAME, Type: arrow.PrimitiveTypes.Int32},
			{Name: COL1NAME, Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	smartBuilder := NewSmartBuilder(b, schema)

	int32Vals := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, v := range int32Vals {
		smartBuilder.Append(0, v)
	}
	smartBuilder.Append(0, nil)

	float64Vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, v := range float64Vals {
		smartBuilder.Append(1, v)
	}
	smartBuilder.Append(1, nil)

	rec1 := b.NewRecord()
	defer rec1.Release()

	cols := make([]array.Column, 0, len(rec1.Columns()))
	for i, cI := range rec1.Columns() {
		field := rec1.Schema().Field(i)
		chunk := array.NewChunked(field.Type, []array.Interface{cI})
		col := array.NewColumn(field, chunk)
		defer col.Release()
		cols = append(cols, *col)
		chunk.Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["f1-i32"]: [1 2 3 4 5 6 7 8 9 (null)]
rec[0]["f2-f64"]: [1 2 3 4 5 6 7 8 9 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func buildDf(pool *memory.CheckedAllocator, dtype arrow.DataType, vals []interface{}) (*DataFrame, error) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: fmt.Sprintf("col-%s", dtype.Name()), Type: dtype},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	smartBuilder := NewSmartBuilder(b, schema)
	for i := range schema.Fields() {
		for j := range vals {
			smartBuilder.Append(i, vals[j])
		}
		smartBuilder.Append(i, nil)
	}

	rec1 := b.NewRecord()
	defer rec1.Release()

	cols := make([]array.Column, 0, len(rec1.Columns()))
	for i, cI := range rec1.Columns() {
		field := rec1.Schema().Field(i)
		chunk := array.NewChunked(field.Type, []array.Interface{cI})
		col := array.NewColumn(field, chunk)
		defer col.Release()
		cols = append(cols, *col)
		chunk.Release()
	}

	return NewDataFrameFromColumns(pool, cols)
}

func TestNewSmartBuilderBoolean(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = (i%2 == 0)
	}
	df, err := buildDf(pool, arrow.FixedWidthTypes.Boolean, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-bool"]: [true false true false true false true false true (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt8(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int8(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int8, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int8"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt16(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int16(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int16, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int16"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt32(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int32(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int32, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int32"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt64(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int64(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int64, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int64"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderUint8(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint8(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint8, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint8"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}
func TestNewSmartBuilderUint16(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint16(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint16, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint16"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderUint32(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint32(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint32, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint32"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderUint64(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint64(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint64, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint64"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderFloat32(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = float32(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Float32, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-float32"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderFloat64(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = float64(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Float64, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-float64"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderString(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = fmt.Sprintf("%d", i)
	}
	df, err := buildDf(pool, arrow.BinaryTypes.String, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-utf8"]: ["0" "1" "2" "3" "4" "5" "6" "7" "8" (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}
