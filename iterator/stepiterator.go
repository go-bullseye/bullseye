package iterator

import (
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/go-bullseye/bullseye/internal/debug"
)

// StepValue holds the value for a given step.
type StepValue struct {
	Values []interface{}
	Exists []bool
	Dtypes []arrow.DataType
}

// Value returns the value at index i and the data type for that value.
func (sv StepValue) Value(i int) (interface{}, arrow.DataType) {
	return sv.Values[i], sv.Dtypes[i]
}

// StepIterator iterates over multiple iterators in step.
type StepIterator interface {
	Values() *StepValue
	Next() bool
	Retain()
	Release()
}

// stepIterator has a max number of elements it
// can iterator over that must fit into uint64
// which I doubt anyone is going to go over.
type stepIterator struct {
	refCount  int64
	iterators []ValueIterator
	index     uint64
	stepValue *StepValue
	dtypes    []arrow.DataType
}

// NewStepIteratorForColumns creates a new StepIterator given a slice of columns.
func NewStepIteratorForColumns(cols []array.Column) StepIterator {
	itrs := make([]ValueIterator, 0, len(cols))
	dtypes := make([]arrow.DataType, 0, len(cols))
	for i := range cols {
		itrs = append(itrs, NewValueIterator(&cols[i]))
		dtypes = append(dtypes, cols[i].DataType())
	}
	// NewStepIterator will retain the value iterators refs
	// so we need to remove our ref to them.
	for i := range itrs {
		defer itrs[i].Release()
	}
	return NewStepIterator(dtypes, itrs...)
}

// NewStepIterator creates a new StepIterator given a bunch of ValueIterators.
func NewStepIterator(dtypes []arrow.DataType, iterators ...ValueIterator) StepIterator {
	for i := range iterators {
		iterators[i].Retain()
	}
	return &stepIterator{
		refCount:  1,
		iterators: iterators,
		index:     0,
		dtypes:    dtypes,
	}
}

// Values returns the values in the current step as a StepValue.
func (s *stepIterator) Values() *StepValue {
	return s.stepValue
}

// Next returns false when there are no more rows in any iterator.
func (s *stepIterator) Next() bool {
	// build the step values
	step := &StepValue{
		Values: make([]interface{}, len(s.iterators)),
		Exists: make([]bool, len(s.iterators)),
		Dtypes: s.dtypes,
	}

	next := false
	for i, iterator := range s.iterators {
		exists := iterator.Next()
		next = exists || next
		step.Exists[i] = exists

		if exists {
			step.Values[i] = iterator.ValueInterface()
		} else {
			step.Values[i] = nil
		}
	}

	s.stepValue = step
	return next
}

func (s *stepIterator) Retain() {
	atomic.AddInt64(&s.refCount, 1)
}

func (s *stepIterator) Release() {
	refs := atomic.AddInt64(&s.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		for i := range s.iterators {
			s.iterators[i].Release()
		}
		s.iterators = nil
	}
}
