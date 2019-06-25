package dataframe

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-bullseye/bullseye/internal/cast"
	"github.com/go-bullseye/bullseye/internal/constructors"
)

// NewColumnFromMem is a helper for creating a new Column from memory.
func NewColumnFromMem(mem memory.Allocator, name string, values interface{}) (*array.Column, error) {
	arr, field, err := constructors.NewInterfaceFromMem(mem, name, values, nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	// create the chunk from the data
	chunk := array.NewChunked(arr.DataType(), []array.Interface{arr})
	defer chunk.Release()

	// create the column from the schema and chunk
	col := array.NewColumn(*field, chunk)

	return col, nil
}

// NewColumnFromSparseMem is a helper for creating a new Column from sparse memory.
func NewColumnFromSparseMem(mem memory.Allocator, name string, values []interface{}, valueIndexes []int, size int) (*array.Column, error) {
	// build valid mask
	valid := make([]bool, size)
	for _, idx := range valueIndexes {
		valid[idx] = true
	}

	ifaceDense, err := cast.SparseCollectionToInterface(values, valueIndexes, size)
	if err != nil {
		return nil, err
	}

	arr, field, err := constructors.NewInterfaceFromMem(mem, name, ifaceDense, valid)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	// create the chunk from the data
	chunk := array.NewChunked(arr.DataType(), []array.Interface{arr})
	defer chunk.Release()

	// create the column from the schema and chunk
	col := array.NewColumn(*field, chunk)

	return col, nil
}
