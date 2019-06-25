package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
)

// Element is an interface for Elements within a Column.
type Element interface {
	// Compare methods
	// Eq returns true if the left Element is equal to the right Element.
	// When both are nil Eq returns false because nil actualy signifies "unknown"
	// and you can't compare two things when you don't know what they are.
	Eq(Element) (bool, error)
	// EqStrict returns true if the left Element is equal to the right Element.
	// When both are nil EqStrict returns true.
	EqStrict(Element) (bool, error)
	// Neq returns true when Eq returns false.
	Neq(Element) (bool, error)
	// Less returns true if the left Element is less than the right Element.
	Less(Element) (bool, error)
	// LessEq returns true if the left Element is less than or equal to the right Element.
	LessEq(Element) (bool, error)
	// Greater returns true if the left Element is greter than the right Element.
	Greater(Element) (bool, error)
	// GreaterEq returns true if the left Element is greter than or equal to the right Element.
	GreaterEq(Element) (bool, error)

	// Accessor/conversion methods

	// Copy returns a copy of this Element.
	Copy() Element

	// Information methods

	// String prints the value of this element as a string.
	String() string
	// IsNil returns true when the underlying value is nil.
	IsNil() bool
}

// CastElement returns an Element type for the passed DataType and value v.
func CastElement(dtype arrow.DataType, v interface{}) Element {
	switch dtype.(type) {
	// case *arrow.NullType: // TODO: implement
	// case *arrow.BooleanType: // TODO: implement
	case *arrow.Uint8Type:
		return NewUint8Element(v)
	case *arrow.Int8Type:
		return NewInt8Element(v)
	case *arrow.Uint16Type:
		return NewUint16Element(v)
	case *arrow.Int16Type:
		return NewInt16Element(v)
	case *arrow.Uint32Type:
		return NewUint32Element(v)
	case *arrow.Int32Type:
		return NewInt32Element(v)
	case *arrow.Uint64Type:
		return NewUint64Element(v)
	case *arrow.Int64Type:
		return NewInt64Element(v)
	// case arrow.HALF_FLOAT: // TODO: implement?
	case *arrow.Float32Type:
		return NewFloat32Element(v)
	case *arrow.Float64Type:
		return NewFloat64Element(v)
	case *arrow.Date32Type:
		return NewDate32Element(v)
	case *arrow.Date64Type:
		return NewDate64Element(v)
		// case *arrow.StringType: // TODO: implement

	}
	panic(fmt.Errorf("bullseye/element: unsupported element for %T", dtype))
}
