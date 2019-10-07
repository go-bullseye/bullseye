package iterator_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/float16"
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

func TestValueAsJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	for _, tc := range []struct {
		name     string
		iterator iterator.ValueIterator
		result   string
		err      error
	}{
		{
			name: "int32 test",
			iterator: func() iterator.ValueIterator {
				ib := array.NewInt32Builder(mem)
				defer ib.Release()

				ib.AppendValues([]int32{123}, nil)
				i1 := ib.NewInt32Array()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.PrimitiveTypes.Int32,
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "i32", Type: arrow.PrimitiveTypes.Int32}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `123`,
			err:    nil,
		},
		{
			name: "string test",
			iterator: func() iterator.ValueIterator {
				ib := array.NewStringBuilder(mem)
				defer ib.Release()

				ib.AppendValues([]string{"foo bar"}, nil)
				i1 := ib.NewStringArray()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.BinaryTypes.String,
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "str", Type: arrow.BinaryTypes.String}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `"foo bar"`,
			err:    nil,
		},
		{
			name: "float16 test",
			iterator: func() iterator.ValueIterator {
				ib := array.NewFloat16Builder(mem)
				defer ib.Release()

				ib.AppendValues(f16sFrom([]float32{1}), nil)
				i1 := ib.NewFloat16Array()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.FixedWidthTypes.Float16,
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "f16", Type: arrow.FixedWidthTypes.Float16}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `1`,
			err:    nil,
		},
		{
			name: "list of string test",
			iterator: func() iterator.ValueIterator {
				lb := array.NewListBuilder(mem, arrow.BinaryTypes.String)
				defer lb.Release()

				vb := lb.ValueBuilder().(*array.StringBuilder)
				lb.Append(true)
				vb.Append("foo")
				vb.Append("bar")
				lb.Append(false)
				lb.Append(true)
				vb.Append("ping")

				i1 := lb.NewListArray()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.ListOf(arrow.BinaryTypes.String),
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "los", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `["foo","bar"]`,
			err:    nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.iterator.Release()
			tc.iterator.Next()

			res, err := tc.iterator.ValueAsJSON()
			if err != tc.err {
				t.Errorf("got err=%v, want err=%v for test: %s\n", err, tc.err, tc.name)
			}

			// marshal the result
			resultBytes, err := json.Marshal(res)
			if err != tc.err {
				t.Errorf("got error marshaling json for test: %s\n%v", tc.name, err)
			}

			result := string(resultBytes)
			if result != tc.result {
				t.Errorf("got result=%s, want result=%s for test: %s | %T - %T\n", result, tc.result, tc.name, result, tc.result)
			}
		})
	}
}

func f16sFrom(vs []float32) []float16.Num {
	o := make([]float16.Num, len(vs))
	for i, v := range vs {
		o[i] = float16.New(v)
	}
	return o
}
