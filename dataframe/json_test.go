package dataframe

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
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

	f, err := os.Create("/tmp/dataframe-test.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	err = df.ToJSON(f)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Sync()
	if err != nil {
		t.Fatal(err)
	}
}
