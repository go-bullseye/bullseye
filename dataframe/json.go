package dataframe

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/go-bullseye/bullseye/iterator"
	"github.com/pkg/errors"
)

// ToJSON writes the DataFrame as JSON.
// TODO(nickpoorman): Take a chunk size to break across files.
func (df *DataFrame) ToJSON(w io.Writer) error {
	schema := df.Schema()

	// To be efficient we want to extract one row at a time
	it := iterator.NewStepIteratorForColumns(df.Columns())
	defer it.Release()

	enc := json.NewEncoder(w)

	for it.Next() {
		stepValue := it.Values()
		jsonObj, err := rowToJSON(schema, stepValue.Values)
		if err != nil {
			return err
		}
		err = enc.Encode(jsonObj)
		if err != nil {
			return err
		}
	}

	return nil
}

func rowToJSON(schema *arrow.Schema, values []interface{}) (map[string]interface{}, error) {
	obj := make(map[string]interface{})
	fields := schema.Fields()
	for i, field := range fields {
		value, err := rowElementToJSON(field.Type, values[i])
		if err != nil {
			return nil, err
		}
		obj[field.Name] = value
	}
	return obj, nil
}

func rowElementToJSON(dtype arrow.DataType, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch dtype.ID() {
	case arrow.NULL:
		return nil, nil
	case arrow.BOOL,
		arrow.UINT8, arrow.INT8,
		arrow.UINT16, arrow.INT16,
		arrow.UINT32, arrow.INT32,
		arrow.UINT64, arrow.INT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.DATE32, arrow.DATE64,
		arrow.TIME32, arrow.TIME64,
		arrow.TIMESTAMP,
		arrow.INTERVAL, // will be converted to int32 when MonthInterval and {days,milliseconds} struct when DayTimeInterval
		arrow.DURATION, // will be converted to int64
		arrow.STRING:
		return value, nil
	case arrow.FLOAT16:
		return value.(float16.Num).Float32(), nil
	case arrow.BINARY:
		// TODO(nickpoorman): Verify this is correct....
		return value, nil
	case arrow.FIXED_SIZE_BINARY:
		// TODO(nickpoorman): Verify this is correct....
		dt := dtype.(*arrow.FixedSizeBinaryType)
		v := []byte(strings.ToUpper(hex.EncodeToString([]byte{value.(byte)})))
		if len(v) != 2*dt.ByteWidth {
			return nil, errors.Errorf("dataframe/json: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth)
		}
		return string(v), nil // re-convert as string to prevent json.Marshal from base64-encoding it.
	case arrow.DECIMAL:
		d128, ok := value.(decimal128.Num)
		if !ok {
			break
		}
		return Signed128BitInteger{Lo: d128.LowBits(), Hi: d128.HighBits()}, nil
	case arrow.LIST:
		valueList, ok := value.(array.Interface)
		if !ok {
			return nil, errors.Errorf("dataframe/json could not convert value to list")
		}
		list, err := arrayToJSON(dtype.(*arrow.ListType).Elem(), valueList)
		if err != nil {
			return nil, err
		}
		return list, nil

		// case arrow.STRUCT:
		// 	panic("not implemented")
		// case arrow.UNION:
		// 	panic("not implemented")
		// case arrow.DICTIONARY:
		// 	panic("not implemented")
		// case arrow.MAP:
		// 	panic("not implemented")
		// case arrow.EXTENSION:
		// 	panic("not implemented")
		// case arrow.FIXED_SIZE_LIST:
		// 	panic("not implemented")
	}

	return nil, errors.Errorf("dataframe/json - type not implemented: %s", dtype.Name())
}

func arrayToJSON(elmDtype arrow.DataType, arr array.Interface) (res []interface{}, err error) {
	switch arr := arr.(type) {
	case *array.Boolean:
		res = boolsToJSON(arr)

	case *array.Int8:
		res = i8ToJSON(arr)

	case *array.Int16:
		res = i16ToJSON(arr)

	case *array.Int32:
		res = i32ToJSON(arr)

	case *array.Int64:
		res = i64ToJSON(arr)

	case *array.Uint8:
		res = u8ToJSON(arr)

	case *array.Uint16:
		res = u16ToJSON(arr)

	case *array.Uint32:
		res = u32ToJSON(arr)

	case *array.Uint64:
		res = u64ToJSON(arr)

	case *array.Float16:
		res = f16ToJSON(arr)

	case *array.Float32:
		res = f32ToJSON(arr)

	case *array.Float64:
		res = f64ToJSON(arr)

	case *array.String:
		res = strToJSON(arr)

	// case *array.Binary:
	// 	return Array{
	// 		Name:   elmDtype.Name,
	// 		Count:  arr.Len(),
	// 		Data:   bytesToJSON(arr),
	// 		Valids: validsToJSON(arr),
	// 		Offset: arr.ValueOffsets(),
	// 	}

	case *array.List:
		res, err = arrayToJSON(arr.DataType().(*arrow.ListType).Elem(), arr.ListValues())

	// case *array.FixedSizeList:

	// case *array.Struct:

	case *array.FixedSizeBinary:

	case *array.Date32:
		res = date32ToJSON(arr)

	case *array.Date64:
		res = date64ToJSON(arr)

	case *array.Time32:
		res = time32ToJSON(arr)

	case *array.Time64:
		res = time64ToJSON(arr)

	case *array.Timestamp:
		res = timestampToJSON(arr)

	case *array.MonthInterval:
		res = monthintervalToJSON(arr)

	case *array.DayTimeInterval:
		res = daytimeintervalToJSON(arr)

	case *array.Duration:
		res = durationToJSON(arr)

	default:
		err = errors.Errorf("unknown array type %T", arr)
	}

	return
}

type Signed128BitInteger struct {
	Lo uint64 `json:"lo"` // low bits
	Hi int64  `json:"hi"` // high bits
}

func boolsToJSON(arr *array.Boolean) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i8ToJSON(arr *array.Int8) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i16ToJSON(arr *array.Int16) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i32ToJSON(arr *array.Int32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i64ToJSON(arr *array.Int64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func u8ToJSON(arr *array.Uint8) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func u16ToJSON(arr *array.Uint16) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func u32ToJSON(arr *array.Uint32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func u64ToJSON(arr *array.Uint64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func f16ToJSON(arr *array.Float16) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i).Float32()
	}
	return o
}

func f32ToJSON(arr *array.Float32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func f64ToJSON(arr *array.Float64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func strToJSON(arr *array.String) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func bytesToJSON(arr *array.Binary) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = strings.ToUpper(hex.EncodeToString(arr.Value(i)))
	}
	return o
}

func date32ToJSON(arr *array.Date32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int32(arr.Value(i))
	}
	return o
}

func date64ToJSON(arr *array.Date64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int64(arr.Value(i))
	}
	return o
}

func time32ToJSON(arr *array.Time32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int32(arr.Value(i))
	}
	return o
}

func time64ToJSON(arr *array.Time64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int64(arr.Value(i))
	}
	return o
}

func timestampToJSON(arr *array.Timestamp) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int64(arr.Value(i))
	}
	return o
}

func monthintervalToJSON(arr *array.MonthInterval) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int32(arr.Value(i))
	}
	return o
}

func daytimeintervalToJSON(arr *array.DayTimeInterval) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func durationToJSON(arr *array.Duration) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}
