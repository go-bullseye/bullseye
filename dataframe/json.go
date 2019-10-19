package dataframe

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/go-bullseye/bullseye/iterator"
	"github.com/pkg/errors"
)

type Signed128BitInteger struct {
	Lo uint64 `json:"lo"` // low bits
	Hi int64  `json:"hi"` // high bits
}

// ToJSON writes the DataFrame as JSON.
func (df *DataFrame) ToJSON(w io.Writer) error {
	schema := df.Schema()
	fields := schema.Fields()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}

	// Iterate over the rows

	// Extract one row at a time
	it := iterator.NewStepIteratorForColumns(df.Columns())
	defer it.Release()

	enc := json.NewEncoder(w)

	for it.Next() {
		stepValue, err := it.ValuesJSON()
		if err != nil {
			return err
		}
		// At this point everything in stepValue is json.
		// We just have to build the object from it.
		jsonObj := make(map[string]interface{})
		for i, jsonValue := range stepValue.ValuesJSON {
			jsonObj[names[i]] = jsonValue
		}

		// jsonObj, err := rowToJSON(schema, stepValue)
		// if err != nil {
		// 	return err
		// }
		err = enc.Encode(jsonObj)
		if err != nil {
			return err
		}
	}

	return nil
}

func rowToJSON(schema *arrow.Schema, stepValue *iterator.StepValue) (map[string]interface{}, error) {
	obj := make(map[string]interface{})
	fields := schema.Fields()
	for i, field := range fields {
		var value interface{}
		var err error
		if stepValue.Exists[i] {
			value, err = rowElementToJSON(field.Type, stepValue.Values[i])
			if err != nil {
				return nil, err
			}
		}
		obj[field.Name] = value
	}
	return obj, nil
}

func rowElementToJSON(dtype arrow.DataType, value interface{}) (interface{}, error) {
	fmt.Printf("rowElementToJSON type is: %s | actual type %T\n", dtype.Name(), value)
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
		fmt.Println("matched LIST")
		valueIterator, ok := value.(iterator.ValueIterator)
		if !ok {
			return nil, errors.Errorf("dataframe/json could not convert value to interface")
		}
		defer valueIterator.Release()
		list := make([]interface{}, 0, 10)
		dt := valueIterator.DataType()
		fmt.Println("calling list next")
		for valueIterator.Next() {
			el, err := rowElementToJSON(dt, valueIterator.ValueInterface())
			if err != nil {
				return nil, err
			}
			list = append(list, el)
			fmt.Println("calling list next")
		}
		// list, err := interfaceToJSON(valueIterator)
		// if err != nil {
		// 	return nil, err
		// }
		return list, nil
	case arrow.STRUCT:
		fmt.Println("matched STRUCT")
		// TODO: this is probably wrong because it's not using offsets
		valueList, ok := value.([]iterator.ValueIterator)
		if !ok {
			return nil, errors.Errorf("dataframe/json could not convert value to interface")
		}
		dt := dtype.(*arrow.StructType)
		o := make(map[string]interface{})
		for i, field := range dt.Fields() {
			vi := valueList[i].ValueInterface()
			elVal, err := rowElementToJSON(valueList[i].DataType(), vi) // rowElementToJSON(field.Type, vi)
			if err != nil {
				return nil, err
			}
			o[field.Name] = elVal
		}
		return o, nil

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

	default:
		panic("type not implemented")
	}

	return nil, errors.Errorf("dataframe/json - type not implemented: %s", dtype.Name())
}

func interfaceToJSON(arr array.Interface) (res interface{}, err error) {
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

	case *array.Binary:
		res = bytesToJSON(arr)

	case *array.List:
		fmt.Println("converting list")
		res, err = listToJSON(arr)

	case *array.FixedSizeList:
		panic("interfaceToJSON *array.FixedSizeList not implemented")

	case *array.Struct:
		// TODO: This one might be slightly more difficult because the lists could still be bunched together...
		// TODO: We will probably have to do the sublist stuff we did for listvalueiterator

		// panic("interfaceToJSON *array.Struct not implemented")
		res, err = structToJSON(arr)

	case *array.FixedSizeBinary:
		panic("interfaceToJSON *array.FixedSizeBinary not implemented")

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

func listToJSON(arr *array.List) ([]interface{}, error) {
	res := make([]interface{}, arr.Len())
	offsets := arr.Offsets()
	for i := 0; i < arr.Len(); i++ {
		j := i + arr.Offset()
		beg := int64(offsets[j])
		end := int64(offsets[j+1])
		slArr := array.NewSlice(arr.ListValues(), beg, end) // Now we have the values for only this element
		defer slArr.Release()
		el, err := interfaceToJSON(slArr) // recurse down for this element
		if err != nil {
			return nil, err
		}
		res[i] = el
	}
	return res, nil
}

func structToJSON(arr *array.Struct) (interface{}, error) {
	fmt.Println("converting struct")
	dt := arr.DataType().(*arrow.StructType)
	o := make(map[string]interface{})
	// offsets := arr.Offset()
	for i, field := range dt.Fields() {
		f := arr.Field(i)
		// f.Offsets()

		value, err := interfaceToJSON(f)
		if err != nil {
			return nil, err
		}
		o[field.Name] = value
	}

	return o, nil
}
