package json

import (
	"encoding/json"
	"fmt"
	"github.com/donnyhardyanto/dxlib/utils"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

type Number interface {
	int | int8 | int16 | int32 | int64 | float32 | float64
}

func PrettyPrint(v utils.JSON) (string, error) {
	vAsString, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Printf("Error in marshall the data %v", v)
		return "", err
	}
	return string(vAsString), nil
}

// MarshalableMerge merges the two JSON-marshalable values x1 and x2,
// preferring x1 over x2 except where x1 and x2 are
// JSON objects, in which case the keys from both objects
// are included and their values merged recursively.
//
// It returns an error if x1 or x2 cannot be JSON-marshaled.
func MarshalableMerge(x1, x2 any) (any, error) {
	data1, err := json.Marshal(x1)
	if err != nil {
		return nil, err
	}
	data2, err := json.Marshal(x2)
	if err != nil {
		return nil, err
	}
	var j1 any
	err = json.Unmarshal(data1, &j1)
	if err != nil {
		return nil, err
	}
	var j2 any
	err = json.Unmarshal(data2, &j2)
	if err != nil {
		return nil, err
	}
	return anyMerge(j1, j2), nil
}

func DeepMerge(x1, x2 utils.JSON) utils.JSON {
	return anyMerge(x1, x2).(utils.JSON)
}

func anyMerge(newerValue, olderValue any) any {
	switch x1 := newerValue.(type) {
	case utils.JSON:
		x2, ok := olderValue.(utils.JSON)
		if !ok {
			return x1
		}
		for k, v3 := range x2 {
			if v1, ok := x1[k]; ok {
				x1[k] = anyMerge(v1, v3)
			} else {
				x1[k] = v3
			}
		}
	case nil:
		// anyMerge(nil, map[string]interface{...}) -> map[string]interface{...}
		x2, ok := olderValue.(utils.JSON)
		if ok {
			return x2
		}
	}
	return newerValue
}

func GetBool(kv utils.JSON, k string) (bool, error) {
	val, exists := kv[k]
	if !exists {
		return false, fmt.Errorf("key %q not found", k)
	}

	switch v := val.(type) {
	case bool:
		return v, nil
	case int64, int32, int16, int8, int:
		return v != 0, nil // converts any non-zero number to true
	case float64, float32:
		return v != 0, nil // handle floating point numbers
	case string:
		switch strings.ToLower(v) {
		case "true", "1", "yes", "on":
			return true, nil
		case "false", "0", "no", "off":
			return false, nil
		}
	}

	return false, fmt.Errorf("cannot convert %T value %v to bool", val, val)
}

func GetString(kv utils.JSON, k string) (v string, err error) {
	var z string
	switch kv[k].(type) {
	case []uint8:
		z = string(kv[k].([]uint8))
	default:
		var ok bool
		z, ok = kv[k].(string)
		if !ok {
			return "", fmt.Errorf("can not get %s as %T from %v", k, v, kv)
		}
	}
	return z, nil
}

func GetNumber[A Number](kv utils.JSON, k string) (v A, err error) {
	var z float64
	switch kv[k].(type) {
	case A:
		return kv[k].(A), nil
	case []uint8:
		s := string(kv[k].([]uint8))
		z, err = strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, err
		}
	default:
		var ok bool
		z, ok = kv[k].(float64)
		if !ok {
			return 0, fmt.Errorf("can not get %s as %T from %v", k, v, kv)
		}
	}
	y := A(z)
	return y, nil
}

func GetNumberWithDefault[A Number](kv utils.JSON, k string, defaultValue A) (v A) {
	z, ok := kv[k].(float64)
	if !ok {
		return defaultValue
	}
	y := A(z)
	return y
}

func GetInt64(kv utils.JSON, k string) (v int64, err error) {
	var a int64
	a, err = GetNumber[int64](kv, k)
	if err != nil {
		return 0, err
	}
	return a, err
}

func MustGetInt64(kv utils.JSON, k string) (v int64) {
	a, err := GetInt64(kv, k)
	if err != nil {
		panic(err)
	}
	return a
}

func GetInt64WithDefault(kv utils.JSON, k string, defaultValue int64) (v int64, err error) {
	var a int64
	a = GetNumberWithDefault(kv, k, defaultValue)
	return a, err
}

func GetInt(kv utils.JSON, k string) (v int, err error) {
	a, err := GetNumber[int](kv, k)
	if err != nil {
		return 0, err
	}
	return a, err
}

func GetFloat32(kv utils.JSON, k string) (v float32, err error) {
	a, err := GetNumber[float32](kv, k)
	if err != nil {
		return 0, err
	}
	return a, err
}

func GetFloat64(kv utils.JSON, k string) (v float64, err error) {
	a, err := GetNumber[float64](kv, k)
	if err != nil {
		return 0, err
	}
	return a, err
}

func GetIntWithDefault(kv utils.JSON, k string, defaultValue int) (v int, err error) {
	a := GetNumberWithDefault(kv, k, defaultValue)
	return a, err
}

func GetTime(kv utils.JSON, k string) (v time.Time, err error) {
	var z time.Time
	switch kv[k].(type) {
	case time.Time:
		z = kv[k].(time.Time)
	case string:
		z, err = time.Parse(time.RFC3339, kv[k].(string))
		if err != nil {
			return time.Time{}, err
		}
	default:
		var ok bool
		z, ok = kv[k].(time.Time)
		if !ok {
			return time.Time{}, fmt.Errorf("can not get %s as %T from %v", k, v, kv)
		}
	}
	return z, nil
}

func ReplaceMergeMap(m1 utils.JSON, m2 utils.JSON) utils.JSON {
	for i, e := range m2 {
		m1[i] = e
	}
	return m1
}

func Copy(m utils.JSON) utils.JSON {
	cp := make(utils.JSON)
	for k, v := range m {
		vm, ok := v.(utils.JSON)
		if ok {
			cp[k] = Copy(vm)
		} else {
			cp[k] = v
		}
	}
	return cp
}

func MapStringStringCopy(m map[string]string) map[string]string {
	cp := make(map[string]string)
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

func ResponseBodyToJSON(r *http.Response) (utils.JSON, error) {
	v := utils.JSON{}
	bodyAll, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bodyAll, &v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func GetValueWithFieldPathString(fieldPath string, data utils.JSON) (any, error) {
	path := strings.Split(fieldPath, ".")
	d := data
	var r any
	var ok bool
	for i, v := range path {
		r, ok = d[v]
		if !ok {
			err := fmt.Errorf(`GetValueWithFieldPathString: Part %d:'%s' from %s not found `, i, v, fieldPath)
			return nil, err
		}
		switch r.(type) {
		case utils.JSON:
			d = r.(utils.JSON)
		default:
			d = utils.JSON{}
		}
	}
	return r, nil
}

func getValueByFieldPathMap(dataPath utils.JSON, data utils.JSON) (r any, err error) {
	var d any
	var ok bool
	for k, v := range dataPath {
		d, ok = data[k]
		if !ok {
			return nil, fmt.Errorf("path not valid %s", k)
		}
		switch v.(type) {
		case utils.JSON:
			switch d.(type) {
			case utils.JSON:
				r, err = getValueByFieldPathMap(v.(utils.JSON), d.(utils.JSON))

			default:
				return nil, fmt.Errorf(`type does not match v=%T with d=%T`, v, d)
			}
		default:
			return d, nil
		}
	}
	return d, nil
}
