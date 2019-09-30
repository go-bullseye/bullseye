package iterator

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/go-bullseye/bullseye/internal/debug"
)

// ValueIterator is a generic iterator for scanning over values.
type ValueIterator interface {
	// ValueInterface returns the current value as an interface{}.
	ValueInterface() interface{}

	// Next moves the iterator to the next value. This will return false when there are no more values.
	Next() bool

	// Retain keeps a reference to the ValueIterator.
	Retain()

	// Release removes a reference to the ValueIterator.
	Release()
}

func NewInterfaceValueIterator(field arrow.Field, iface array.Interface) ValueIterator {
	chunk := array.NewChunked(iface.DataType(), []array.Interface{iface})
	defer chunk.Release()

	col := array.NewColumn(field, chunk)
	defer col.Release()

	return NewValueIterator(col)
}

// NewValueIterator creates a new generic ValueIterator.
func NewValueIterator(column *array.Column) ValueIterator {
	field := column.Field()
	switch field.Type.(type) {

	case *arrow.Int64Type:
		return NewInt64ValueIterator(column)

	case *arrow.Uint64Type:
		return NewUint64ValueIterator(column)

	case *arrow.Float64Type:
		return NewFloat64ValueIterator(column)

	case *arrow.Int32Type:
		return NewInt32ValueIterator(column)

	case *arrow.Uint32Type:
		return NewUint32ValueIterator(column)

	case *arrow.Float32Type:
		return NewFloat32ValueIterator(column)

	case *arrow.Int16Type:
		return NewInt16ValueIterator(column)

	case *arrow.Uint16Type:
		return NewUint16ValueIterator(column)

	case *arrow.Int8Type:
		return NewInt8ValueIterator(column)

	case *arrow.Uint8Type:
		return NewUint8ValueIterator(column)

	case *arrow.TimestampType:
		return NewTimestampValueIterator(column)

	case *arrow.Time32Type:
		return NewTime32ValueIterator(column)

	case *arrow.Time64Type:
		return NewTime64ValueIterator(column)

	case *arrow.Date32Type:
		return NewDate32ValueIterator(column)

	case *arrow.Date64Type:
		return NewDate64ValueIterator(column)

	case *arrow.DurationType:
		return NewDurationValueIterator(column)

	case *arrow.MonthIntervalType:
		return NewMonthIntervalValueIterator(column)

	case *arrow.Float16Type:
		return NewFloat16ValueIterator(column)

	case *arrow.Decimal128Type:
		return NewDecimal128ValueIterator(column)

	case *arrow.DayTimeIntervalType:
		return NewDayTimeIntervalValueIterator(column)

	case *arrow.BooleanType:
		return NewBooleanValueIterator(column)

	case *arrow.StringType:
		return NewStringValueIterator(column)

	case *arrow.ListType:
		return NewListValueIterator(column)

	case *arrow.StructType:
		return NewStructValueIterator(column)

	default:
		panic(fmt.Errorf("dataframe/valueiterator: unhandled field type %T", field.Type))
	}
}

// Int64ValueIterator is an iterator for reading an Arrow Column value by value.
type Int64ValueIterator struct {
	refCount      int64
	chunkIterator *Int64ChunkIterator

	// Things we need to maintain for the iterator
	index  int          // current value index
	values []int64      // current chunk values
	ref    *array.Int64 // the chunk reference
	done   bool         // there are no more elements for this iterator
}

// NewInt64ValueIterator creates a new Int64ValueIterator for reading an Arrow Column.
func NewInt64ValueIterator(col *array.Column) *Int64ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewInt64ChunkIterator(col)

	return &Int64ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Int64ValueIterator) Value() (int64, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Int64ValueIterator) ValuePointer() *int64 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Int64ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Int64ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Int64ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Int64ValueIterator.
func (vr *Int64ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Int64ValueIterator.
func (vr *Int64ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Uint64ValueIterator is an iterator for reading an Arrow Column value by value.
type Uint64ValueIterator struct {
	refCount      int64
	chunkIterator *Uint64ChunkIterator

	// Things we need to maintain for the iterator
	index  int           // current value index
	values []uint64      // current chunk values
	ref    *array.Uint64 // the chunk reference
	done   bool          // there are no more elements for this iterator
}

// NewUint64ValueIterator creates a new Uint64ValueIterator for reading an Arrow Column.
func NewUint64ValueIterator(col *array.Column) *Uint64ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewUint64ChunkIterator(col)

	return &Uint64ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Uint64ValueIterator) Value() (uint64, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Uint64ValueIterator) ValuePointer() *uint64 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Uint64ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Uint64ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Uint64ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Uint64ValueIterator.
func (vr *Uint64ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Uint64ValueIterator.
func (vr *Uint64ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Float64ValueIterator is an iterator for reading an Arrow Column value by value.
type Float64ValueIterator struct {
	refCount      int64
	chunkIterator *Float64ChunkIterator

	// Things we need to maintain for the iterator
	index  int            // current value index
	values []float64      // current chunk values
	ref    *array.Float64 // the chunk reference
	done   bool           // there are no more elements for this iterator
}

// NewFloat64ValueIterator creates a new Float64ValueIterator for reading an Arrow Column.
func NewFloat64ValueIterator(col *array.Column) *Float64ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewFloat64ChunkIterator(col)

	return &Float64ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Float64ValueIterator) Value() (float64, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Float64ValueIterator) ValuePointer() *float64 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Float64ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Float64ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Float64ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Float64ValueIterator.
func (vr *Float64ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Float64ValueIterator.
func (vr *Float64ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Int32ValueIterator is an iterator for reading an Arrow Column value by value.
type Int32ValueIterator struct {
	refCount      int64
	chunkIterator *Int32ChunkIterator

	// Things we need to maintain for the iterator
	index  int          // current value index
	values []int32      // current chunk values
	ref    *array.Int32 // the chunk reference
	done   bool         // there are no more elements for this iterator
}

// NewInt32ValueIterator creates a new Int32ValueIterator for reading an Arrow Column.
func NewInt32ValueIterator(col *array.Column) *Int32ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewInt32ChunkIterator(col)

	return &Int32ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Int32ValueIterator) Value() (int32, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Int32ValueIterator) ValuePointer() *int32 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Int32ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Int32ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Int32ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Int32ValueIterator.
func (vr *Int32ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Int32ValueIterator.
func (vr *Int32ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Uint32ValueIterator is an iterator for reading an Arrow Column value by value.
type Uint32ValueIterator struct {
	refCount      int64
	chunkIterator *Uint32ChunkIterator

	// Things we need to maintain for the iterator
	index  int           // current value index
	values []uint32      // current chunk values
	ref    *array.Uint32 // the chunk reference
	done   bool          // there are no more elements for this iterator
}

// NewUint32ValueIterator creates a new Uint32ValueIterator for reading an Arrow Column.
func NewUint32ValueIterator(col *array.Column) *Uint32ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewUint32ChunkIterator(col)

	return &Uint32ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Uint32ValueIterator) Value() (uint32, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Uint32ValueIterator) ValuePointer() *uint32 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Uint32ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Uint32ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Uint32ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Uint32ValueIterator.
func (vr *Uint32ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Uint32ValueIterator.
func (vr *Uint32ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Float32ValueIterator is an iterator for reading an Arrow Column value by value.
type Float32ValueIterator struct {
	refCount      int64
	chunkIterator *Float32ChunkIterator

	// Things we need to maintain for the iterator
	index  int            // current value index
	values []float32      // current chunk values
	ref    *array.Float32 // the chunk reference
	done   bool           // there are no more elements for this iterator
}

// NewFloat32ValueIterator creates a new Float32ValueIterator for reading an Arrow Column.
func NewFloat32ValueIterator(col *array.Column) *Float32ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewFloat32ChunkIterator(col)

	return &Float32ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Float32ValueIterator) Value() (float32, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Float32ValueIterator) ValuePointer() *float32 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Float32ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Float32ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Float32ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Float32ValueIterator.
func (vr *Float32ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Float32ValueIterator.
func (vr *Float32ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Int16ValueIterator is an iterator for reading an Arrow Column value by value.
type Int16ValueIterator struct {
	refCount      int64
	chunkIterator *Int16ChunkIterator

	// Things we need to maintain for the iterator
	index  int          // current value index
	values []int16      // current chunk values
	ref    *array.Int16 // the chunk reference
	done   bool         // there are no more elements for this iterator
}

// NewInt16ValueIterator creates a new Int16ValueIterator for reading an Arrow Column.
func NewInt16ValueIterator(col *array.Column) *Int16ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewInt16ChunkIterator(col)

	return &Int16ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Int16ValueIterator) Value() (int16, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Int16ValueIterator) ValuePointer() *int16 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Int16ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Int16ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Int16ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Int16ValueIterator.
func (vr *Int16ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Int16ValueIterator.
func (vr *Int16ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Uint16ValueIterator is an iterator for reading an Arrow Column value by value.
type Uint16ValueIterator struct {
	refCount      int64
	chunkIterator *Uint16ChunkIterator

	// Things we need to maintain for the iterator
	index  int           // current value index
	values []uint16      // current chunk values
	ref    *array.Uint16 // the chunk reference
	done   bool          // there are no more elements for this iterator
}

// NewUint16ValueIterator creates a new Uint16ValueIterator for reading an Arrow Column.
func NewUint16ValueIterator(col *array.Column) *Uint16ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewUint16ChunkIterator(col)

	return &Uint16ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Uint16ValueIterator) Value() (uint16, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Uint16ValueIterator) ValuePointer() *uint16 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Uint16ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Uint16ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Uint16ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Uint16ValueIterator.
func (vr *Uint16ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Uint16ValueIterator.
func (vr *Uint16ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Int8ValueIterator is an iterator for reading an Arrow Column value by value.
type Int8ValueIterator struct {
	refCount      int64
	chunkIterator *Int8ChunkIterator

	// Things we need to maintain for the iterator
	index  int         // current value index
	values []int8      // current chunk values
	ref    *array.Int8 // the chunk reference
	done   bool        // there are no more elements for this iterator
}

// NewInt8ValueIterator creates a new Int8ValueIterator for reading an Arrow Column.
func NewInt8ValueIterator(col *array.Column) *Int8ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewInt8ChunkIterator(col)

	return &Int8ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Int8ValueIterator) Value() (int8, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Int8ValueIterator) ValuePointer() *int8 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Int8ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Int8ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Int8ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Int8ValueIterator.
func (vr *Int8ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Int8ValueIterator.
func (vr *Int8ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Uint8ValueIterator is an iterator for reading an Arrow Column value by value.
type Uint8ValueIterator struct {
	refCount      int64
	chunkIterator *Uint8ChunkIterator

	// Things we need to maintain for the iterator
	index  int          // current value index
	values []uint8      // current chunk values
	ref    *array.Uint8 // the chunk reference
	done   bool         // there are no more elements for this iterator
}

// NewUint8ValueIterator creates a new Uint8ValueIterator for reading an Arrow Column.
func NewUint8ValueIterator(col *array.Column) *Uint8ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewUint8ChunkIterator(col)

	return &Uint8ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Uint8ValueIterator) Value() (uint8, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Uint8ValueIterator) ValuePointer() *uint8 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Uint8ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Uint8ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Uint8ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Uint8ValueIterator.
func (vr *Uint8ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Uint8ValueIterator.
func (vr *Uint8ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// TimestampValueIterator is an iterator for reading an Arrow Column value by value.
type TimestampValueIterator struct {
	refCount      int64
	chunkIterator *TimestampChunkIterator

	// Things we need to maintain for the iterator
	index  int               // current value index
	values []arrow.Timestamp // current chunk values
	ref    *array.Timestamp  // the chunk reference
	done   bool              // there are no more elements for this iterator
}

// NewTimestampValueIterator creates a new TimestampValueIterator for reading an Arrow Column.
func NewTimestampValueIterator(col *array.Column) *TimestampValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewTimestampChunkIterator(col)

	return &TimestampValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *TimestampValueIterator) Value() (arrow.Timestamp, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *TimestampValueIterator) ValuePointer() *arrow.Timestamp {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *TimestampValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *TimestampValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *TimestampValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the TimestampValueIterator.
func (vr *TimestampValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the TimestampValueIterator.
func (vr *TimestampValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Time32ValueIterator is an iterator for reading an Arrow Column value by value.
type Time32ValueIterator struct {
	refCount      int64
	chunkIterator *Time32ChunkIterator

	// Things we need to maintain for the iterator
	index  int            // current value index
	values []arrow.Time32 // current chunk values
	ref    *array.Time32  // the chunk reference
	done   bool           // there are no more elements for this iterator
}

// NewTime32ValueIterator creates a new Time32ValueIterator for reading an Arrow Column.
func NewTime32ValueIterator(col *array.Column) *Time32ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewTime32ChunkIterator(col)

	return &Time32ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Time32ValueIterator) Value() (arrow.Time32, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Time32ValueIterator) ValuePointer() *arrow.Time32 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Time32ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Time32ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Time32ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Time32ValueIterator.
func (vr *Time32ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Time32ValueIterator.
func (vr *Time32ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Time64ValueIterator is an iterator for reading an Arrow Column value by value.
type Time64ValueIterator struct {
	refCount      int64
	chunkIterator *Time64ChunkIterator

	// Things we need to maintain for the iterator
	index  int            // current value index
	values []arrow.Time64 // current chunk values
	ref    *array.Time64  // the chunk reference
	done   bool           // there are no more elements for this iterator
}

// NewTime64ValueIterator creates a new Time64ValueIterator for reading an Arrow Column.
func NewTime64ValueIterator(col *array.Column) *Time64ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewTime64ChunkIterator(col)

	return &Time64ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Time64ValueIterator) Value() (arrow.Time64, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Time64ValueIterator) ValuePointer() *arrow.Time64 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Time64ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Time64ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Time64ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Time64ValueIterator.
func (vr *Time64ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Time64ValueIterator.
func (vr *Time64ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Date32ValueIterator is an iterator for reading an Arrow Column value by value.
type Date32ValueIterator struct {
	refCount      int64
	chunkIterator *Date32ChunkIterator

	// Things we need to maintain for the iterator
	index  int            // current value index
	values []arrow.Date32 // current chunk values
	ref    *array.Date32  // the chunk reference
	done   bool           // there are no more elements for this iterator
}

// NewDate32ValueIterator creates a new Date32ValueIterator for reading an Arrow Column.
func NewDate32ValueIterator(col *array.Column) *Date32ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewDate32ChunkIterator(col)

	return &Date32ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Date32ValueIterator) Value() (arrow.Date32, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Date32ValueIterator) ValuePointer() *arrow.Date32 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Date32ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Date32ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Date32ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Date32ValueIterator.
func (vr *Date32ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Date32ValueIterator.
func (vr *Date32ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Date64ValueIterator is an iterator for reading an Arrow Column value by value.
type Date64ValueIterator struct {
	refCount      int64
	chunkIterator *Date64ChunkIterator

	// Things we need to maintain for the iterator
	index  int            // current value index
	values []arrow.Date64 // current chunk values
	ref    *array.Date64  // the chunk reference
	done   bool           // there are no more elements for this iterator
}

// NewDate64ValueIterator creates a new Date64ValueIterator for reading an Arrow Column.
func NewDate64ValueIterator(col *array.Column) *Date64ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewDate64ChunkIterator(col)

	return &Date64ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Date64ValueIterator) Value() (arrow.Date64, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Date64ValueIterator) ValuePointer() *arrow.Date64 {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Date64ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Date64ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Date64ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Date64ValueIterator.
func (vr *Date64ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Date64ValueIterator.
func (vr *Date64ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// DurationValueIterator is an iterator for reading an Arrow Column value by value.
type DurationValueIterator struct {
	refCount      int64
	chunkIterator *DurationChunkIterator

	// Things we need to maintain for the iterator
	index  int              // current value index
	values []arrow.Duration // current chunk values
	ref    *array.Duration  // the chunk reference
	done   bool             // there are no more elements for this iterator
}

// NewDurationValueIterator creates a new DurationValueIterator for reading an Arrow Column.
func NewDurationValueIterator(col *array.Column) *DurationValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewDurationChunkIterator(col)

	return &DurationValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *DurationValueIterator) Value() (arrow.Duration, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *DurationValueIterator) ValuePointer() *arrow.Duration {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *DurationValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *DurationValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *DurationValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the DurationValueIterator.
func (vr *DurationValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the DurationValueIterator.
func (vr *DurationValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// MonthIntervalValueIterator is an iterator for reading an Arrow Column value by value.
type MonthIntervalValueIterator struct {
	refCount      int64
	chunkIterator *MonthIntervalChunkIterator

	// Things we need to maintain for the iterator
	index  int                   // current value index
	values []arrow.MonthInterval // current chunk values
	ref    *array.MonthInterval  // the chunk reference
	done   bool                  // there are no more elements for this iterator
}

// NewMonthIntervalValueIterator creates a new MonthIntervalValueIterator for reading an Arrow Column.
func NewMonthIntervalValueIterator(col *array.Column) *MonthIntervalValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewMonthIntervalChunkIterator(col)

	return &MonthIntervalValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *MonthIntervalValueIterator) Value() (arrow.MonthInterval, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *MonthIntervalValueIterator) ValuePointer() *arrow.MonthInterval {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *MonthIntervalValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *MonthIntervalValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *MonthIntervalValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the MonthIntervalValueIterator.
func (vr *MonthIntervalValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the MonthIntervalValueIterator.
func (vr *MonthIntervalValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Float16ValueIterator is an iterator for reading an Arrow Column value by value.
type Float16ValueIterator struct {
	refCount      int64
	chunkIterator *Float16ChunkIterator

	// Things we need to maintain for the iterator
	index  int            // current value index
	values []float16.Num  // current chunk values
	ref    *array.Float16 // the chunk reference
	done   bool           // there are no more elements for this iterator
}

// NewFloat16ValueIterator creates a new Float16ValueIterator for reading an Arrow Column.
func NewFloat16ValueIterator(col *array.Column) *Float16ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewFloat16ChunkIterator(col)

	return &Float16ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Float16ValueIterator) Value() (float16.Num, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Float16ValueIterator) ValuePointer() *float16.Num {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Float16ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Float16ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Float16ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Float16ValueIterator.
func (vr *Float16ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Float16ValueIterator.
func (vr *Float16ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// Decimal128ValueIterator is an iterator for reading an Arrow Column value by value.
type Decimal128ValueIterator struct {
	refCount      int64
	chunkIterator *Decimal128ChunkIterator

	// Things we need to maintain for the iterator
	index  int               // current value index
	values []decimal128.Num  // current chunk values
	ref    *array.Decimal128 // the chunk reference
	done   bool              // there are no more elements for this iterator
}

// NewDecimal128ValueIterator creates a new Decimal128ValueIterator for reading an Arrow Column.
func NewDecimal128ValueIterator(col *array.Column) *Decimal128ValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewDecimal128ChunkIterator(col)

	return &Decimal128ValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *Decimal128ValueIterator) Value() (decimal128.Num, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *Decimal128ValueIterator) ValuePointer() *decimal128.Num {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *Decimal128ValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *Decimal128ValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *Decimal128ValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the Decimal128ValueIterator.
func (vr *Decimal128ValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the Decimal128ValueIterator.
func (vr *Decimal128ValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}

// DayTimeIntervalValueIterator is an iterator for reading an Arrow Column value by value.
type DayTimeIntervalValueIterator struct {
	refCount      int64
	chunkIterator *DayTimeIntervalChunkIterator

	// Things we need to maintain for the iterator
	index  int                     // current value index
	values []arrow.DayTimeInterval // current chunk values
	ref    *array.DayTimeInterval  // the chunk reference
	done   bool                    // there are no more elements for this iterator
}

// NewDayTimeIntervalValueIterator creates a new DayTimeIntervalValueIterator for reading an Arrow Column.
func NewDayTimeIntervalValueIterator(col *array.Column) *DayTimeIntervalValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewDayTimeIntervalChunkIterator(col)

	return &DayTimeIntervalValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index:  0,
		values: nil,
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *DayTimeIntervalValueIterator) Value() (arrow.DayTimeInterval, bool) {
	return vr.values[vr.index], vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *DayTimeIntervalValueIterator) ValuePointer() *arrow.DayTimeInterval {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return &vr.values[vr.index]
}

// ValueInterface returns the current value as an interface{}.
func (vr *DayTimeIntervalValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.values[vr.index]
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *DayTimeIntervalValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.values == nil || vr.index >= len(vr.values) {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *DayTimeIntervalValueIterator) nextChunk() bool {
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

	vr.ref = ref
	vr.values = vr.chunkIterator.ChunkValues()
	vr.index = 0
	return true
}

// Retain keeps a reference to the DayTimeIntervalValueIterator.
func (vr *DayTimeIntervalValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the DayTimeIntervalValueIterator.
func (vr *DayTimeIntervalValueIterator) Release() {
	refs := atomic.AddInt64(&vr.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
		vr.values = nil
	}
}
