package dataframe

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/go-bullseye/bullseye/iterator"
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
	switch field.Type.ID() {
	case arrow.NULL:
		obj[name] = nil
	case arrow.BOOL:
		obj[name] = value
	case arrow.UINT8:
		obj[name] = value
	case arrow.INT8:
		obj[name] = value
	case arrow.UINT16:
		obj[name] = value
	case arrow.INT16:
		obj[name] = value
	case arrow.UINT32:
		obj[name] = value
	case arrow.INT32:
		obj[name] = value
	case arrow.UINT64:
		obj[name] = value
	case arrow.INT64:
		obj[name] = value
	case arrow.FLOAT16:
		// TODO(nickpoorman): Verify we don't need to do anything to this.
		obj[name] = value
	case arrow.FLOAT32:
		obj[name] = value
	case arrow.FLOAT64:
		obj[name] = value
	case arrow.STRING:
		obj[name] = value
	case arrow.BINARY:
		obj[name] = value
	case arrow.FIXED_SIZE_BINARY:
		panic("not implemented")
		// TODO: Convert to string?
	case arrow.DATE32:
		panic("not implemented")
	case arrow.DATE64:
		panic("not implemented")
	case arrow.TIMESTAMP:
		panic("not implemented")
	case arrow.TIME32:
		panic("not implemented")
	case arrow.TIME64:
		panic("not implemented")
	case arrow.INTERVAL:
		panic("not implemented")
	case arrow.DECIMAL:
		panic("not implemented")
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
		panic("not implemented")
	default:
		panic(fmt.Sprintf("unknown type: %s", field.Type.Name()))
	}

	return nil
}
