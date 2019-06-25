package cast

import "github.com/pkg/errors"

const inconsistentDataTypesErrMsg = "inconsistent data types for elements, expecting %v to be of type (%T)"

// SparseCollectionToInterface casts a slice of interfaces to an interface of the correct type
// for the provided sparse collection.
// This should be used for sparse as it should be faster for larger arrays.
func SparseCollectionToInterface(elms []interface{}, indexes []int, size int) (interface{}, error) {
	if len(elms) == 0 {
		return nil, nil
	}

	first := elms[0]

	var ok bool
	switch v := first.(type) {
	case bool:
		arr := make([]bool, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(bool); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int8:
		arr := make([]int8, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int8); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int16:
		arr := make([]int16, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int16); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int32:
		arr := make([]int32, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int32); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int64:
		arr := make([]int64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int64); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint8:
		arr := make([]uint8, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint8); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint16:
		arr := make([]uint16, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint16); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint32:
		arr := make([]uint32, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint32); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint64:
		arr := make([]uint64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint64); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case float32:
		arr := make([]float32, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(float32); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case float64:
		arr := make([]float64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(float64); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case string:
		arr := make([]string, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(string); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint:
		arr := make([]uint, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int:
		arr := make([]int64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			vv, okk := e.(int)
			if !okk {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
			arr[idx] = int64(vv)
		}
		return arr, nil

	default:
		return nil, errors.Errorf("dataframe/sparse: invalid data type for %v (%T)", elms, v)
	}
}
