package dataframe

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/apache/arrow/go/arrow/memory"
)

const (
	toJSONResult = `{"col1-i32":1,"col10-ts-s":1,"col2-f64":1,"col3-f16":1,"col4-date32":1,"col5-date64":1,"col6-mitvl":1,"col7-dtitvl":{"days":1,"milliseconds":1},"col8-dec128":{"lo":1,"hi":1},"col9-duration-s":1}
{"col1-i32":2,"col10-ts-s":2,"col2-f64":2,"col3-f16":2,"col4-date32":2,"col5-date64":2,"col6-mitvl":2,"col7-dtitvl":{"days":2,"milliseconds":2},"col8-dec128":{"lo":2,"hi":2},"col9-duration-s":2}
{"col1-i32":3,"col10-ts-s":3,"col2-f64":3,"col3-f16":3,"col4-date32":3,"col5-date64":3,"col6-mitvl":3,"col7-dtitvl":{"days":3,"milliseconds":3},"col8-dec128":{"lo":3,"hi":3},"col9-duration-s":3}
{"col1-i32":4,"col10-ts-s":4,"col2-f64":4,"col3-f16":4,"col4-date32":4,"col5-date64":4,"col6-mitvl":4,"col7-dtitvl":{"days":4,"milliseconds":4},"col8-dec128":{"lo":0,"hi":0},"col9-duration-s":4}
{"col1-i32":5,"col10-ts-s":5,"col2-f64":5,"col3-f16":5,"col4-date32":5,"col5-date64":5,"col6-mitvl":5,"col7-dtitvl":{"days":5,"milliseconds":5},"col8-dec128":{"lo":18446744073709551611,"hi":-1},"col9-duration-s":5}
{"col1-i32":6,"col10-ts-s":6,"col2-f64":6,"col3-f16":6,"col4-date32":6,"col5-date64":6,"col6-mitvl":6,"col7-dtitvl":{"days":6,"milliseconds":6},"col8-dec128":{"lo":18446744073709551610,"hi":-1},"col9-duration-s":6}
{"col1-i32":7,"col10-ts-s":7,"col2-f64":7,"col3-f16":7,"col4-date32":7,"col5-date64":7,"col6-mitvl":7,"col7-dtitvl":{"days":7,"milliseconds":7},"col8-dec128":{"lo":7,"hi":0},"col9-duration-s":7}
{"col1-i32":8,"col10-ts-s":8,"col2-f64":8,"col3-f16":8,"col4-date32":8,"col5-date64":8,"col6-mitvl":8,"col7-dtitvl":{"days":8,"milliseconds":8},"col8-dec128":{"lo":8,"hi":0},"col9-duration-s":8}
{"col1-i32":null,"col10-ts-s":null,"col2-f64":null,"col3-f16":null,"col4-date32":null,"col5-date64":null,"col6-mitvl":null,"col7-dtitvl":null,"col8-dec128":null,"col9-duration-s":null}
{"col1-i32":10,"col10-ts-s":10,"col2-f64":10,"col3-f16":10,"col4-date32":10,"col5-date64":10,"col6-mitvl":10,"col7-dtitvl":{"days":10,"milliseconds":10},"col8-dec128":{"lo":10,"hi":0},"col9-duration-s":10}
`
)

func f16sFrom(vs []float64) []float16.Num {
	o := make([]float16.Num, len(vs))
	for i, v := range vs {
		o[i] = float16.New(float32(v))
	}
	return o
}

func TestToJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "col2-f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "col3-f16", Type: arrow.FixedWidthTypes.Float16},
			{Name: "col4-date32", Type: arrow.PrimitiveTypes.Date32},
			{Name: "col5-date64", Type: arrow.PrimitiveTypes.Date64},
			{Name: "col6-mitvl", Type: arrow.FixedWidthTypes.MonthInterval},
			{Name: "col7-dtitvl", Type: arrow.FixedWidthTypes.DayTimeInterval},
			{Name: "col8-dec128", Type: &arrow.Decimal128Type{Precision: 10, Scale: 1}},
			{Name: "col9-duration-s", Type: arrow.FixedWidthTypes.Duration_s},
			{Name: "col10-ts-s", Type: arrow.FixedWidthTypes.Timestamp_s},
			{Name: "col11-list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		},
		nil,
	)

	// print the schema
	fmt.Println(schema.String())

	recordBuilder := array.NewRecordBuilder(pool, schema)
	defer recordBuilder.Release()

	valids := []bool{true, true, true, true, true, true, true, true, false, true}
	float16Values := f16sFrom([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	dayTimeIntervalValues := []arrow.DayTimeInterval{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}, {10, 10}}
	decimal128Values := []decimal128.Num{decimal128.New(1, 1), decimal128.New(2, 2), decimal128.New(3, 3), {}, decimal128.FromI64(-5), decimal128.FromI64(-6), decimal128.FromI64(7), decimal128.FromI64(8), decimal128.FromI64(9), decimal128.FromI64(10)}

	recordBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	recordBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})

	recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6}, nil)
	recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{7, 8, 9, 10}, []bool{true, true, false, true})

	recordBuilder.Field(2).(*array.Float16Builder).AppendValues(float16Values, valids)

	recordBuilder.Field(3).(*array.Date32Builder).AppendValues([]arrow.Date32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, valids)

	recordBuilder.Field(4).(*array.Date64Builder).AppendValues([]arrow.Date64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, valids)

	recordBuilder.Field(5).(*array.MonthIntervalBuilder).AppendValues([]arrow.MonthInterval{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, valids)

	recordBuilder.Field(6).(*array.DayTimeIntervalBuilder).AppendValues(dayTimeIntervalValues, valids)

	recordBuilder.Field(7).(*array.Decimal128Builder).AppendValues(decimal128Values, valids)

	recordBuilder.Field(8).(*array.DurationBuilder).AppendValues([]arrow.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, valids)

	recordBuilder.Field(9).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, valids)

	lb := recordBuilder.Field(10).(*array.ListBuilder)
	for _, v := range valids {
		lb.Append(v)
		lb.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	}

	rec1 := recordBuilder.NewRecord()
	defer rec1.Release()

	df, err := NewDataFrameFromRecord(pool, rec1)

	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	var b bytes.Buffer
	err = df.ToJSON(&b)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := b.String(), toJSONResult; got != want {
		t.Fatalf("got=%s, want=%s", got, want)
	}
}
