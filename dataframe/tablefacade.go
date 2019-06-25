package dataframe

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

// I don't want to force the DataFrame API to conform to the TableReader API.
// (i.e. forcing NumCols to return int64 doesn't make sense in Go).
// So this is a facade the DataFrame TableReader expects.

// TableFacade is a simple facade for a TableReader.
type TableFacade interface {
	array.Table
}

type tableReaderFacade struct {
	df *DataFrame
}

// NewTableFacade creates a new TableFacade for a DataFrame.
func NewTableFacade(df *DataFrame) TableFacade {
	return &tableReaderFacade{
		df: df,
	}
}

func (f *tableReaderFacade) Schema() *arrow.Schema {
	return f.df.Schema()
}

func (f *tableReaderFacade) NumRows() int64 {
	return f.df.NumRows()
}

func (f *tableReaderFacade) NumCols() int64 {
	return int64(f.df.NumCols())
}

// Column is an immutable column data structure consisting of
// a field (type metadata) and a chunked data array.
func (f *tableReaderFacade) Column(i int) *array.Column {
	return f.df.ColumnAt(i)
}

func (f *tableReaderFacade) Retain() {
	f.df.Retain()
}

func (f *tableReaderFacade) Release() {
	f.df.Release()
}
