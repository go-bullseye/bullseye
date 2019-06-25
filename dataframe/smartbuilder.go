package dataframe

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

// AppenderFunc is the function to be used to convert the data to the correct type.
type AppenderFunc func(array.Builder, interface{})

// SmartBuilder knows how to convert to the correct type when building.
type SmartBuilder struct {
	recordBuilder  *array.RecordBuilder
	schema         *arrow.Schema
	fieldAppenders []AppenderFunc
}

// NewSmartBuilder creates a SmartBuilder that knows how to convert to the correct type when building.
func NewSmartBuilder(recordBuilder *array.RecordBuilder, schema *arrow.Schema) *SmartBuilder {
	sb := &SmartBuilder{
		recordBuilder:  recordBuilder,
		schema:         schema,
		fieldAppenders: make([]AppenderFunc, 0, len(schema.Fields())),
	}

	fields := sb.schema.Fields()
	for i := range fields {
		fn := initFieldAppender(&fields[i])
		sb.fieldAppenders = append(sb.fieldAppenders, fn)
	}

	return sb
}

// Append will append the value to the builder.
func (sb *SmartBuilder) Append(fieldIndex int, v interface{}) {
	field := sb.recordBuilder.Field(fieldIndex)
	appendFunc := sb.fieldAppenders[fieldIndex]
	if appendFunc == nil {
		fmt.Fprintln(os.Stderr, "warn: appendFunc is nil")
	}
	appendFunc(field, v)
}

func initFieldAppender(field *arrow.Field) AppenderFunc {
	switch field.Type.(type) {
	case *arrow.BooleanType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.BooleanBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(bool)
				builder.Append(vT)
			}
		}
	case *arrow.Int8Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int8Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int8)
				builder.Append(vT)
			}
		}
	case *arrow.Int16Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int16Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int16)
				builder.Append(vT)
			}
		}
	case *arrow.Int32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int32)
				builder.Append(vT)
			}
		}
	case *arrow.Int64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int64)
				builder.Append(vT)
			}
		}
	case *arrow.Uint8Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint8Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint8)
				builder.Append(vT)
			}
		}
	case *arrow.Uint16Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint16Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint16)
				builder.Append(vT)
			}
		}
	case *arrow.Uint32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint32)
				builder.Append(vT)
			}
		}
	case *arrow.Uint64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint64)
				builder.Append(vT)
			}
		}
	case *arrow.Float32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Float32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(float32)
				builder.Append(vT)
			}
		}
	case *arrow.Float64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Float64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(float64)
				builder.Append(vT)
			}
		}
	case *arrow.StringType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.StringBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(string)
				builder.Append(vT)
			}
		}

	default:
		panic(fmt.Errorf("dataframe/smartbuilder: unhandled field type %T", field.Type))
	}
}
