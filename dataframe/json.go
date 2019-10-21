package dataframe

import (
	"encoding/json"
	"io"

	"github.com/go-bullseye/bullseye/iterator"
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

		err = enc.Encode(jsonObj)
		if err != nil {
			return err
		}
	}

	return nil
}
