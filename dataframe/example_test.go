package dataframe_test

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-bullseye/bullseye/dataframe"
)

// This example demonstrates creating a new DataFrame from memory
// using a Dict and displaying the contents of it.
func Example_newDataFrameFromMemory() {
	pool := memory.NewGoAllocator()
	df, _ := dataframe.NewDataFrameFromMem(pool, dataframe.Dict{
		"col1": []int32{1, 2, 3, 4, 5},
		"col2": []float64{1.1, 2.2, 3.3, 4.4, 5},
		"col3": []string{"foo", "bar", "ping", "", "pong"},
		"col4": []interface{}{2, 4, 6, nil, 8},
	})
	defer df.Release()
	fmt.Printf("DataFrame:\n%s\n", df.Display(0))

	// Output:
	// DataFrame:
	// rec[0]["col1"]: [1 2 3 4 5]
	// rec[0]["col2"]: [1.1 2.2 3.3 4.4 5]
	// rec[0]["col3"]: ["foo" "bar" "ping" "" "pong"]
	// rec[0]["col4"]: [2 4 6 (null) 8]
}
