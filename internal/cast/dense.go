package cast

import "github.com/pkg/errors"

// DenseCollectionToInterface casts a slice of interfaces to an interface of the correct type.
func DenseCollectionToInterface(elms []interface{}) (interface{}, error) {
	if len(elms) == 0 {
		return nil, nil
	}

	// find the first one that is not nil
	var first interface{}
	for i := range elms {
		if elms[i] != nil {
			first = elms[i]
			break
		}
	}

	if first == nil {
		return nil, nil
	}

	var ok bool
	switch v := first.(type) {
	case bool:
		arr := make([]bool, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(bool); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int8:
		arr := make([]int8, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(int8); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int16:
		arr := make([]int16, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(int16); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int32:
		arr := make([]int32, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(int32); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int64:
		arr := make([]int64, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(int64); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint8:
		arr := make([]uint8, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(uint8); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint16:
		arr := make([]uint16, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(uint16); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint32:
		arr := make([]uint32, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(uint32); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint64:
		arr := make([]uint64, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(uint64); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case float32:
		arr := make([]float32, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(float32); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case float64:
		arr := make([]float64, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(float64); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case string:
		arr := make([]string, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(string); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint:
		arr := make([]uint, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			if arr[i], ok = e.(uint); !ok {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int:
		arr := make([]int64, len(elms))
		for i, e := range elms {
			if e == nil {
				continue
			}
			vv, okk := e.(int)
			if !okk {
				return nil, errors.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
			arr[i] = int64(vv)
		}
		return arr, nil

	default:
		return nil, errors.Errorf("dataframe/dense: invalid data type for %v (%T)", elms, v)
	}
}
