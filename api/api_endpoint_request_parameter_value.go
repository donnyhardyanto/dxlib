package api

import (
	"github.com/donnyhardyanto/dxlib/utils"
	security "github.com/donnyhardyanto/dxlib/utils/security"
	"strings"
	"time"
)

type DXAPIEndPointRequestParameterValue struct {
	Owner       *DXAPIEndPointRequest
	Parent      *DXAPIEndPointRequestParameterValue
	Value       any
	RawValue    any
	Metadata    DXAPIEndPointParameter
	Children    map[string]*DXAPIEndPointRequestParameterValue
	ErrValidate error
}

func (aeprpv *DXAPIEndPointRequestParameterValue) NewChild(aepp DXAPIEndPointParameter) *DXAPIEndPointRequestParameterValue {
	child := DXAPIEndPointRequestParameterValue{Owner: aeprpv.Owner, Metadata: aepp}
	child.Parent = aeprpv
	if aeprpv.Children == nil {
		aeprpv.Children = make(map[string]*DXAPIEndPointRequestParameterValue)
	}
	aeprpv.Children[aepp.NameId] = &child
	return &child
}

func (aeprpv *DXAPIEndPointRequestParameterValue) SetRawValue(rv any, variablePath string) (err error) {
	aeprpv.RawValue = rv
	if aeprpv.Metadata.Type == "json" {
		jsonValue, ok := rv.(map[string]interface{})
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", variablePath, aeprpv.Metadata.Type, utils.TypeAsString(rv), rv)
		}
		for _, v := range aeprpv.Metadata.Children {
			{
				childValue := aeprpv.NewChild(v)
				aVariablePath := variablePath + "." + v.NameId
				jv, ok := jsonValue[v.NameId]
				if !ok {
					if v.IsMustExist {
						return aeprpv.Owner.Log.WarnAndCreateErrorf("MISSING_MANDATORY_FIELD:%s", aVariablePath)
					}
				} else {
					err = childValue.SetRawValue(jv, aVariablePath)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (aeprpv *DXAPIEndPointRequestParameterValue) Validate() bool {
	if aeprpv.Metadata.IsMustExist {
		if aeprpv.RawValue == nil {
			return false
		}
	}
	if aeprpv.RawValue == nil {
		return true
	}
	rawValueType := utils.TypeAsString(aeprpv.RawValue)
	nameIdPath := aeprpv.Metadata.GetNameIdPath()
	if aeprpv.Metadata.Type != rawValueType {
		switch aeprpv.Metadata.Type {
		case "nullable-int64":
		case "int64":
			if rawValueType == "float64" {
				if !utils.IfFloatIsInt(aeprpv.RawValue.(float64)) {
					aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
					return false
				}
			}
		case "float32":
			switch rawValueType {
			case "int64":
			case "int32":
			case "float64":
			case "float32":
			default:
				aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
				return false
			}
		case "protected-string", "protected-sql-string":
			if rawValueType != "string" {
				return false
			}
		case "json":
			if rawValueType != "map[string]interface {}" {
				return false
			}
			for _, v := range aeprpv.Children {
				if v.Validate() != true {
					childRawValueType := utils.TypeAsString(aeprpv.RawValue)
					aNameIdPath := aeprpv.Metadata.GetNameIdPath()
					aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_TYPE_MATCHING:SHOULD_[%s].(%v)_BUT_RECEIVE_(%s)=%v", aNameIdPath, v.Metadata.Type, childRawValueType, v.RawValue)
					return false
				}
			}
		case "iso8601", "date", "time":
			if rawValueType != "string" {
				return false
			}
		case "array":
			if rawValueType != "[]interface {}" {
				return false
			}
		default:
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_TYPE_MATCHING:SHOULD_[%s].(%v)_BUT_RECEIVE_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			return false
		}
	}
	switch aeprpv.Metadata.Type {
	case "nullable-int64":
		if aeprpv.RawValue == nil {
			aeprpv.Value = nil
			return true
		}
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		v := int64(t)
		aeprpv.Value = v
		return true
	case "int64":
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		v := int64(t)
		aeprpv.Value = v
		return true
	case "float64":
		v, ok := aeprpv.RawValue.(float64)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		aeprpv.Value = v
		return true
	case "float32":
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		v := float32(t)
		aeprpv.Value = v
		return true
	case "protected-string":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		if security.StringCheckPossibleSQLInjection(s) {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
			return false
		}
		aeprpv.Value = s
		return true
	case "protected-sql-string":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		if security.PartSQLStringCheckPossibleSQLInjection(s) {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
			return false
		}
		aeprpv.Value = s
		return true
	case "nullable-string":
		if aeprpv.RawValue == nil {
			aeprpv.Value = nil
			return true
		}
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		aeprpv.Value = s
		return true
	case "string":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		aeprpv.Value = s
		return true
	case "json":
		s := utils.JSON{}
		for _, v := range aeprpv.Children {
			s[v.Metadata.NameId] = v.Value
		}
		aeprpv.Value = s
		return true
	case "array":
		s, ok := aeprpv.RawValue.([]any)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		aeprpv.Value = s
		return true
	case "iso8601":
		/* RFC3339Nano format conform to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
		The golang time package documentation (https://pkg.go.dev/time#pkg-constants) has wrong information on the RFC3339/RFC3329Nano format.
		but the code is conformed to the standard. Only the documentation is incorrect.
		*/
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		if strings.Contains(s, " ") {
			s = strings.Replace(s, " ", "T", 1)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			aeprpv.Owner.Log.Warnf("INVALID_RFC3339NANO_FORMAT:%s", s)
			aeprpv.ErrValidate = err
			return false
		}
		aeprpv.Value = t
		return true
	case "date":
		/* RFC3339Nano format conform to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
		The golang time package documentation (https://pkg.go.dev/time#pkg-constants) has wrong information on the RFC3339/RFC3329Nano format.
		but the code is conformed to the standard. Only the documentation is incorrect.
		*/
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		t, err := time.Parse(time.DateOnly, s)
		if err != nil {
			aeprpv.Owner.Log.Warnf("INVALID_DATE_FROMAT:%s", s)
			aeprpv.ErrValidate = err
			return false
		}
		aeprpv.Value = t
		return true
	case "time":
		/* RFC3339Nano format conform to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
		The golang time package documentation (https://pkg.go.dev/time#pkg-constants) has wrong information on the RFC3339/RFC3329Nano format.
		but the code is conformed to the standard. Only the documentation is incorrect.
		*/
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			return false
		}
		t, err := time.Parse(time.TimeOnly, s)
		if err != nil {
			aeprpv.Owner.Log.Warnf("INVALID_TIME_FROMAT:%s", s)
			aeprpv.ErrValidate = err
			return false
		}
		aeprpv.Value = t
		return true
	default:
		aeprpv.Value = aeprpv.RawValue
		return true
	}
}
