package dataframe

import (
	"fmt"
	"os"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/pkg/errors"
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
	case *arrow.Date32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Date32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := arrow.Date32(v.(int32))
				builder.Append(vT)
			}
		}

	case *arrow.ListType:
		return func(b array.Builder, v interface{}) {
			builder := b.(*array.ListBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				sub := builder.ValueBuilder()
				fmt.Printf("list type value: [%v]\n", v)
				v := reflectValueOfNonPointer(v).Elem()
				sub.Reserve(v.Len())
				builder.Append(true)
				for i := 0; i < v.Len(); i++ {
					appendValue(sub, v.Index(i).Interface())
				}
			}
		}

	case *arrow.FixedSizeListType:
		return func(b array.Builder, v interface{}) {
			builder := b.(*array.FixedSizeListBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				sub := builder.ValueBuilder()
				v := reflect.ValueOf(v).Elem()
				sub.Reserve(v.Len())
				builder.Append(true)
				for i := 0; i < v.Len(); i++ {
					appendValue(sub, v.Index(i).Interface())
				}
			}
		}

	case *arrow.StructType:
		return func(b array.Builder, v interface{}) {
			builder := b.(*array.StructBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				builder.Append(true)
				v := reflect.ValueOf(v).Elem()
				for i := 0; i < builder.NumField(); i++ {
					f := builder.FieldBuilder(i)
					appendValue(f, v.Field(i).Interface())
				}
			}
		}

	default:
		panic(fmt.Errorf("dataframe/smartbuilder: unhandled field type %T", field.Type))
	}
}

// TODO(nickpoorman): Write test that will test all the data types.
func appendValue(bldr array.Builder, v interface{}) {
	fmt.Printf("appendValue: [%v]\n", v)
	switch b := bldr.(type) {
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	case *array.Int8Builder:
		b.Append(v.(int8))
	case *array.Int16Builder:
		b.Append(v.(int16))
	case *array.Int32Builder:
		b.Append(v.(int32))
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.Uint8Builder:
		b.Append(v.(uint8))
	case *array.Uint16Builder:
		b.Append(v.(uint16))
	case *array.Uint32Builder:
		b.Append(v.(uint32))
	case *array.Uint64Builder:
		b.Append(v.(uint64))
	case *array.Float32Builder:
		b.Append(v.(float32))
	case *array.Float64Builder:
		b.Append(v.(float64))
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.Date32Builder:
		b.Append(arrow.Date32(v.(int32)))

	case *array.ListBuilder:
		b.Append(true)
		sub := b.ValueBuilder()
		v := reflect.ValueOf(v)
		for i := 0; i < v.Len(); i++ {
			appendValue(sub, v.Index(i).Interface())
		}

	case *array.FixedSizeListBuilder:
		b.Append(true)
		sub := b.ValueBuilder()
		v := reflect.ValueOf(v)
		for i := 0; i < v.Len(); i++ {
			appendValue(sub, v.Index(i).Interface())
		}

	case *array.StructBuilder:
		v := reflect.ValueOf(v)
		for i := 0; i < b.NumField(); i++ {
			f := b.FieldBuilder(i)
			appendValue(f, v.Field(i).Interface())
		}

	default:
		panic(errors.Errorf("dataframe/smartbuilder: unhandled Arrow builder type %T", b))
	}
}

// If the type of v is a pointer return the pointer as a value,
// otherwise create a new pointer to the value.
func reflectValueOfNonPointer(v interface{}) reflect.Value {
	var ptr reflect.Value
	value := reflect.ValueOf(v)
	if value.Type().Kind() == reflect.Ptr {
		ptr = value
	} else {
		ptr = reflect.New(reflect.TypeOf(v)) // create new pointer
		temp := ptr.Elem()                   // create variable to value of pointer
		temp.Set(value)                      // set value of variable to our passed in value
	}
	return ptr
}
