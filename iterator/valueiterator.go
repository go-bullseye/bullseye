package iterator

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
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

// NewValueIterator creates a new generic ValueIterator.
func NewValueIterator(column *array.Column) ValueIterator {
	field := column.Field()
	switch field.Type.(type) {
	case *arrow.Int8Type:
		return NewInt8ValueIterator(column)
	case *arrow.Int16Type:
		return NewInt16ValueIterator(column)
	case *arrow.Int32Type:
		return NewInt32ValueIterator(column)
	case *arrow.Int64Type:
		return NewInt64ValueIterator(column)
	case *arrow.Uint8Type:
		return NewUint8ValueIterator(column)
	case *arrow.Uint16Type:
		return NewUint16ValueIterator(column)
	case *arrow.Uint32Type:
		return NewUint32ValueIterator(column)
	case *arrow.Uint64Type:
		return NewUint64ValueIterator(column)
	case *arrow.Float16Type:
		return NewFloat16ValueIterator(column)
	case *arrow.Float32Type:
		return NewFloat32ValueIterator(column)
	case *arrow.Float64Type:
		return NewFloat64ValueIterator(column)
	case *arrow.Date32Type:
		return NewDate32ValueIterator(column)
	case *arrow.Date64Type:
		return NewDate64ValueIterator(column)
	case *arrow.BooleanType:
		return NewBooleanValueIterator(column)
	case *arrow.StringType:
		return NewStringValueIterator(column)
	case *arrow.MonthIntervalType:
		return NewMonthIntervalValueIterator(column)
	case *arrow.DayTimeIntervalType:
		return NewDayTimeIntervalValueIterator(column)
	case *arrow.Decimal128Type:
		return NewDecimal128ValueIterator(column)
	case *arrow.DurationType:
		return NewDurationValueIterator(column)

	default:
		panic(fmt.Errorf("dataframe/valueiterator: unhandled field type %T", field.Type))
	}
}
