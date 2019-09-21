package dataframe

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
)

var (
	toJSONResult = `{"col1-i32":1,"col2-f64":1}
{"col1-i32":2,"col2-f64":2}
{"col1-i32":3,"col2-f64":3}
{"col1-i32":4,"col2-f64":4}
{"col1-i32":5,"col2-f64":5}
{"col1-i32":6,"col2-f64":6}
{"col1-i32":7,"col2-f64":7}
{"col1-i32":8,"col2-f64":8}
{"col1-i32":9,"col2-f64":9}
{"col1-i32":10,"col2-f64":10}
`
)

func TestToJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	// f, err := os.Create("/tmp/dataframe-test.json")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer f.Close()
	// defer func() {
	// 	err := f.Sync()
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// }()

	var b bytes.Buffer

	err = df.ToJSON(&b)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := b.String(), toJSONResult; got != want {
		t.Fatalf("got=%s, want=%s", got, want)
	}
}
