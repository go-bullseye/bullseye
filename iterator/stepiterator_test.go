package iterator_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-bullseye/bullseye/iterator"
)

func TestNewStepIteratorForColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	for i := range records {
		defer records[i].Release()
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	cols := make([]array.Column, 0, tbl.NumCols())
	for i := 0; i < int(tbl.NumCols()); i++ {
		cols = append(cols, *tbl.Column(i))
	}

	it := iterator.NewStepIteratorForColumns(cols)
	defer it.Release()
}
