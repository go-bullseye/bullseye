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
	case arrow.STRUCT:
		// This one we need to loop over
		panic("not implemented")
	case arrow.LIST:
		// This one we need to loop over
		panic("not implemented")
	case arrow.BOOL:
		obj[name] = value
	case arrow.INT32:
		obj[name] = value
	case arrow.FLOAT64:
		obj[name] = value
	default:
		panic(fmt.Sprintf("type not supported: %s", field.Type.Name()))
	}

	return nil
}
