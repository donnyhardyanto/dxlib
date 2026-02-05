package api

import (
	"net/http"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/utils"
)

func (aepr *DXAPIEndPointRequest) GetParameterValueEntry(k string) (val *DXAPIEndPointRequestParameterValue, err error) {
	var ok bool
	if val, ok = aepr.ParameterValues[k]; !ok {
		// Parameter not defined in endpoint or not sent by client
		// Return a default entry indicating it doesn't exist (not an error)
		return &DXAPIEndPointRequestParameterValue{
			Owner:    aepr,
			Value:    nil,
			RawValue: nil,
			Metadata: DXAPIEndPointParameter{
				NameId:      k,
				IsMustExist: false,
				IsNullable:  true,
			},
		}, nil
	}
	return val, nil
}

func (aepr *DXAPIEndPointRequest) AssignParameterNullableInt64(target *utils.JSON, key string) (isExist bool, v *int64, err error) {
	isExist, v, err = aepr.GetParameterValueAsNullableInt64(key)
	if err != nil {
		return isExist, v, err
	}
	if isExist {
		if v != nil {
			(*target)[key] = *v
		} else {
			(*target)[key] = nil
		}
	}
	return isExist, v, nil
}

func (aepr *DXAPIEndPointRequest) AssignParameterNullableString(target *utils.JSON, key string) (isExist bool, v *string, err error) {
	isExist, v, err = aepr.GetParameterValueAsNullableString(key)
	if err != nil {
		return isExist, v, err
	}
	if isExist {
		if v != nil {
			(*target)[key] = *v
		} else {
			(*target)[key] = nil
		}
	}
	return isExist, v, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsAny(k string) (isExist bool, val any, err error) {
	valEntry, err := aepr.GetParameterValueEntry(k)
	if err != nil {
		return false, "", err
	}
	valAsAny := valEntry.Value
	if valAsAny == nil {
		if !valEntry.Metadata.IsNullable {
			if valEntry.Metadata.IsMustExist {
				err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_FIELD_VALUE_IS_NOT_EXIST:%s", k)
				return false, nil, err
			}
		}
		return false, nil, nil
	}
	return true, valAsAny, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsNullableString(k string, defaultValue ...any) (isExist bool, val *string, err error) {
	isExist, valAsAny, err := aepr.GetParameterValueAsAny(k)
	ok := false
	if !isExist {
		if defaultValue != nil {
			if len(defaultValue) > 0 {
				if defaultValue[0] == nil {
					return false, nil, nil
				} else {
					v1, ok := defaultValue[0].(string)
					if !ok {
						err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "PARAMETER_DEFAULT_VALUE_IS_NOT_STRING:%s=%v", k, v1)
						return false, nil, err
					}
					return false, &v1, nil
				}
			}
		} else {
			return isExist, nil, nil
		}
		return isExist, val, err
	}
	v1, ok := valAsAny.(string)
	if !ok {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "REQUEST_FIELD_VALUE_IS_NOT_STRING:%s=(%v)", k, valAsAny)
		return true, nil, err
	}
	return true, &v1, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsString(k string, defaultValue ...any) (isExist bool, val string, err error) {
	isExist, valAsAny, err := aepr.GetParameterValueAsAny(k)
	ok := false
	if !isExist {
		if defaultValue != nil {
			if len(defaultValue) > 0 {
				v1, ok := defaultValue[0].(string)
				if !ok {
					err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "PARAMETER_DEFAULT_VALUE_IS_NOT_STRING:%s=%v", k, v1)
					return false, "", err
				}
			}
		} else {
			return isExist, "", nil
		}
		return isExist, val, err
	}
	v1, ok := valAsAny.(string)
	if !ok {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "REQUEST_FIELD_VALUE_IS_NOT_STRING:%s=(%v)", k, valAsAny)
		return true, "", err
	}
	return true, v1, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsNullableInt64(k string, defaultValue ...any) (isExist bool, val *int64, err error) {
	isExist, valAsAny, err := aepr.GetParameterValueAsAny(k)
	ok := false
	if !isExist {
		if defaultValue != nil {
			if len(defaultValue) > 0 {
				if defaultValue[0] == nil {
					return false, nil, nil
				} else {
					v1, ok := defaultValue[0].(int64)
					if !ok {
						err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "PARAMETER_DEFAULT_VALUE_IS_NOT_NULLABLE_INT64:%s=%v", k, v1)
						return false, nil, err
					}
					return false, &v1, nil
				}
			}
		} else {
			return isExist, nil, nil
		}
		return isExist, val, err
	}
	v1, ok := valAsAny.(int64)
	if !ok {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "REQUEST_FIELD_VALUE_IS_NOT_NULLABLE_INT64:%s=(%v)", k, valAsAny)
		return true, nil, err
	}
	return true, &v1, nil
}

func getParameterValue[A any](aepr *DXAPIEndPointRequest, k string, defaultValue ...A) (isExist bool, val A, err error) {
	isExist, valAsAny, err := aepr.GetParameterValueAsAny(k)
	if !isExist {
		if len(defaultValue) > 0 {
			return false, defaultValue[0], nil
		}
		return isExist, val, err
	}
	val, ok := valAsAny.(A)
	if !ok {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "REQUEST_FIELD_VALUE_IS_NOT_TYPE:%s!=%T (%v)", k, val, valAsAny)
		return true, val, err
	}
	return true, val, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsBool(k string, defaultValue ...bool) (isExist bool, val bool, err error) {
	return getParameterValue[bool](aepr, k, defaultValue...)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsInt64(k string) (isExist bool, val int64, err error) {
	return getParameterValue[int64](aepr, k)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsFloat64(k string) (isExist bool, val float64, err error) {
	return getParameterValue[float64](aepr, k)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsFloat32(k string) (isExist bool, val float32, err error) {
	return getParameterValue[float32](aepr, k)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsTime(k string) (isExist bool, val time.Time, err error) {
	return getParameterValue[time.Time](aepr, k)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsArrayOfAny(k string) (isExist bool, val []any, err error) {
	return getParameterValue[[]any](aepr, k)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsStrings(k string) (isExist bool, val []string, err error) {
	return getParameterValue[[]string](aepr, k)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsInt64s(k string) (isExist bool, val []int64, err error) {
	return getParameterValue[[]int64](aepr, k)
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsJSON(k string) (isExist bool, val utils.JSON, err error) {
	return getParameterValue[utils.JSON](aepr, k)
}
