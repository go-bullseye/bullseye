package iterator_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-bullseye/bullseye/iterator"
)

func TestInt32ValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	var numRows int64
	for i := range records {
		defer records[i].Release()
		numRows += records[i].NumRows()
	}

	expectedValues := make([]int32, 0, numRows)
	expectedValuesBool := make([]bool, 0, numRows)
	for i := range records {
		ref := records[i].Column(0).(*array.Int32)
		values := ref.Int32Values()
		for j := range values {
			expectedValues = append(expectedValues, values[j])
			expectedValuesBool = append(expectedValuesBool, ref.IsNull(j))
		}
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	column := tbl.Column(0)
	cr := iterator.NewInt32ValueIterator(column)
	defer cr.Release()

	n := 0
	for cr.Next() {
		value, null := cr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		if got, want := null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}
		n++
	}
}

func TestInt32ValueIteratorPointer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	var numRows int64
	for i := range records {
		defer records[i].Release()
		numRows += records[i].NumRows()
	}

	expectedPtrs := make([]*int32, 0, numRows)
	for i := range records {
		ref := records[i].Column(0).(*array.Int32)
		values := ref.Int32Values()
		for j := range values {
			if ref.IsNull(j) {
				expectedPtrs = append(expectedPtrs, nil)
			} else {
				expectedPtrs = append(expectedPtrs, &values[j])
			}
		}
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	column := tbl.Column(0)
	cr := iterator.NewInt32ValueIterator(column)
	defer cr.Release()

	n := 0
	for cr.Next() {
		value := cr.ValuePointer()
		if got, want := value, expectedPtrs[n]; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		n++
	}
}

func TestFloat64ValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	expectedValues := []float64{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	}

	expectedValuesBool := []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, false, true, false, true, true, true, true, true, false,
		true, true, true, true, true, true, true, true, true, true,
	}

	b.Field(0).(*array.Float64Builder).AppendValues(expectedValues[0:10], nil)
	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.Float64Builder).AppendValues(expectedValues[10:20], expectedValuesBool[10:20])
	rec2 := b.NewRecord()
	defer rec2.Release()

	b.Field(0).(*array.Float64Builder).AppendValues(expectedValues[20:30], nil)
	rec3 := b.NewRecord()
	defer rec3.Release()

	records := []array.Record{rec1, rec2, rec3}
	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()
	column := tbl.Column(0)
	vr := iterator.NewFloat64ValueIterator(column)
	defer vr.Release()

	n := 0
	for vr.Next() {
		value, null := vr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%f, want=%f", got, want)
		}
		if got, want := !null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v (n=%d)", got, want, n)
		}
		n++
	}
}

func TestDate32ValueIterator(t *testing.T) {
	t.Skip("TODO: Implement.")
}

func TestDate64ValueIterator(t *testing.T) {
	t.Skip("TODO: Implement.")
}

func TestBooleanValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "c1-bools", Type: arrow.FixedWidthTypes.Boolean},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	expectedValues := []bool{
		true, true, true, true, true, true, true, true, true, true,
		false, false, false, false, false, false, false, false, false, false,
		true, true, false, true, true, true, true, true, true, true,
	}

	expectedValuesBool := []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, false, true, false, true, true, true, true, true, false,
		true, true, true, true, true, true, true, true, true, true,
	}

	b.Field(0).(*array.BooleanBuilder).AppendValues(expectedValues[0:10], nil)
	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues(expectedValues[10:20], expectedValuesBool[10:20])
	rec2 := b.NewRecord()
	defer rec2.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues(expectedValues[20:30], nil)
	rec3 := b.NewRecord()
	defer rec3.Release()

	records := []array.Record{rec1, rec2, rec3}
	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()
	column := tbl.Column(0)
	vr := iterator.NewBooleanValueIterator(column)
	defer vr.Release()

	n := 0
	for vr.Next() {
		value, null := vr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%t, want=%t", got, want)
		}
		if got, want := !null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v (n=%d)", got, want, n)
		}
		n++
	}
}

func TestStringValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "c1-strings", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	expectedValues := []string{
		"true", "aaa", "true", "true", "true", "ccc", "true", "d", "true", "e",
		"false", "false", "false", "false", "false", "false", "false", "dog", "false", "false",
		"true", "true", "bbb", "true", "true", "true", "true", "true", "cat", "true",
	}

	expectedValuesBool := []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, false, true, false, true, true, true, true, true, false,
		true, true, true, true, true, true, true, true, true, true,
	}

	b.Field(0).(*array.StringBuilder).AppendValues(expectedValues[0:10], nil)
	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.StringBuilder).AppendValues(expectedValues[10:20], expectedValuesBool[10:20])
	rec2 := b.NewRecord()
	defer rec2.Release()

	b.Field(0).(*array.StringBuilder).AppendValues(expectedValues[20:30], nil)
	rec3 := b.NewRecord()
	defer rec3.Release()

	records := []array.Record{rec1, rec2, rec3}
	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()
	column := tbl.Column(0)
	vr := iterator.NewStringValueIterator(column)
	defer vr.Release()

	n := 0
	for vr.Next() {
		value, null := vr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%s, want=%s", got, want)
		}
		if got, want := !null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v (n=%d)", got, want, n)
		}
		n++
	}
}
