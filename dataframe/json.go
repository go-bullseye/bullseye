package dataframe

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/arrow"
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
	for i := range fields {
		err := fieldToJSON(obj, fields[i], values[i])
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func fieldToJSON(obj map[string]interface{}, field arrow.Field, value interface{}) error {
	name := field.Name

	if value == nil {
		obj[name] = nil
		return nil
	}

	switch field.Type.ID() {
	case arrow.NULL:
		obj[name] = nil
		return nil
	case arrow.BOOL:
		obj[name] = value
		return nil
	case arrow.UINT8:
		obj[name] = value
		return nil
	case arrow.INT8:
		obj[name] = value
		return nil
	case arrow.UINT16:
		obj[name] = value
		return nil
	case arrow.INT16:
		obj[name] = value
		return nil
	case arrow.UINT32:
		obj[name] = value
		return nil
	case arrow.INT32:
		obj[name] = value
		return nil
	case arrow.UINT64:
		obj[name] = value
		return nil
	case arrow.INT64:
		obj[name] = value
		return nil
	case arrow.FLOAT16:
		obj[name] = value.(float16.Num).Float32()
		return nil
	case arrow.FLOAT32:
		obj[name] = value
		return nil
	case arrow.FLOAT64:
		obj[name] = value
		return nil
	case arrow.STRING:
		obj[name] = value
		return nil
	case arrow.BINARY:
		obj[name] = value
		return nil
	case arrow.FIXED_SIZE_BINARY:
		// TODO(nickpoorman): Verify this is correct....
		dt := field.Type.(*arrow.FixedSizeBinaryType)
		v := []byte(strings.ToUpper(hex.EncodeToString([]byte{value.(byte)})))
		if len(v) != 2*dt.ByteWidth {
			return errors.Errorf("arrjson: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth)
		}
		obj[name] = string(v) // re-convert as string to prevent json.Marshal from base64-encoding it.
		return nil
	case arrow.DATE32:
		obj[name] = value
		return nil // will be converted to int32
	case arrow.DATE64:
		obj[name] = value
		return nil // will be converted to int64
	case arrow.TIMESTAMP:
		obj[name] = value
		return nil // will be converted to int64
	case arrow.TIME32:
		obj[name] = value
		return nil // will be converted to int32
	case arrow.TIME64:
		obj[name] = value
		return nil // will be converted to int64
	case arrow.INTERVAL:
		obj[name] = value
		return nil // will be converted to int32 when MonthInterval and {days,milliseconds} struct when DayTimeInterval
	case arrow.DECIMAL:
		d128, ok := value.(decimal128.Num)
		if !ok {
			break
		}
		obj[name] = Signed128BitInteger{Lo: d128.LowBits(), Hi: d128.HighBits()}
		return nil
	case arrow.LIST:
		panic("not implemented")
	case arrow.STRUCT:
		panic("not implemented")
	case arrow.UNION:
		panic("not implemented")
	case arrow.DICTIONARY:
		panic("not implemented")
	case arrow.MAP:
		panic("not implemented")
	case arrow.EXTENSION:
		panic("not implemented")
	case arrow.FIXED_SIZE_LIST:
		panic("not implemented")
	case arrow.DURATION:
		obj[name] = value
		return nil // will be converted to int64
	}

	return fmt.Errorf("unknown type: %s", field.Type.Name())
}

type Signed128BitInteger struct {
	Lo uint64 `json:"lo"` // low bits
	Hi int64  `json:"hi"` // high bits
}
