// DO NOT USE THIS!
package iterator

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/go-bullseye/bullseye/internal/debug"
)

// ListValueIterator iterates over the list elements.
// For example, in a list like: [[0 1 2] (null) [3 4 5] [6 7 8] (null)]
// First [0 1 2] would be returned, then (null), then [3 4 5], etc..
type ListValueIterator struct {
	refCount      int64
	chunkIterator *ChunkIterator

	// Things we need to maintain for the iterator
	index int         // current value index
	ref   *array.List // the chunk reference
	done  bool        // there are no more elements for this iterator

	dataType arrow.DataType
}

func NewListValueIterator(col *array.Column) *ListValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewChunkIterator(col)

	return &ListValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index: 0,
		ref:   nil,

		dataType: col.DataType(),
	}
}

func (vr *ListValueIterator) ValueInterface() interface{} {
	fmt.Println("called ListValueIterator ValueInterface")
	elDt := vr.ref.DataType().(*arrow.ListType).Elem()
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	j := vr.index + vr.ref.Offset() // index + data offset
	offsets := vr.ref.Offsets()
	beg := int64(offsets[j])
	end := int64(offsets[j+1])
	arr := array.NewSlice(vr.ref.ListValues(), beg, end)
	defer arr.Release()
	return NewInterfaceValueIterator(
		arrow.Field{Name: "item", Type: elDt, Nullable: true},
		arr,
	)
}

func (vr *ListValueIterator) DataType() arrow.DataType {
	return vr.dataType
}

func (vr *ListValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.ref == nil || vr.index >= vr.ref.Len() {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *ListValueIterator) nextChunk() bool {
	// Advance the chunk until we get one with data in it or we are done
	if !vr.chunkIterator.Next() {
		// No more chunks
		return false
	}

	// There was another chunk.
	// We maintain the ref and the values because the ref is going to allow us to retain the memory.
	ref := vr.chunkIterator.Chunk()
	ref.Retain()

	if vr.ref != nil {
		vr.ref.Release()
	}

	vr.ref = ref.(*array.List)
	vr.index = 0
	return true
}

// Retain keeps a reference to the ListValueIterator
func (vr *ListValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the ListValueIterator
func (vr *ListValueIterator) Release() {
	debug.Assert(atomic.LoadInt64(&vr.refCount) > 0, "too many releases")

	if atomic.AddInt64(&vr.refCount, -1) == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
	}
}
