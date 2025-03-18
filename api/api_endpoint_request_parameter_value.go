package api

import (
	"fmt"
	security "github.com/donnyhardyanto/dxlib/utils/security"
	"github.com/pkg/errors"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/utils"
	_ "time/tzdata"
)

const ErrorMessageIncompatibleTypeReceived = "INCOMPATIBLE_TYPE:%s(%v)_BUT_RECEIVED_(%s)=%v"

type DXAPIEndPointRequestParameterValue struct {
	Owner           *DXAPIEndPointRequest
	Parent          *DXAPIEndPointRequestParameterValue
	Value           any
	RawValue        any
	Metadata        DXAPIEndPointParameter
	IsArrayChildren bool
	Children        map[string]*DXAPIEndPointRequestParameterValue
	ArrayChildren   []*DXAPIEndPointRequestParameterValue
	//	ErrValidate error
}

func (aeprpv *DXAPIEndPointRequestParameterValue) GetNameIdPath() (s string) {
	if aeprpv.Parent == nil {
		return aeprpv.Metadata.NameId
	}
	return aeprpv.Parent.GetNameIdPath() + "." + aeprpv.Metadata.NameId
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

func (aeprpv *DXAPIEndPointRequestParameterValue) NewArrayChild(aepp DXAPIEndPointParameter) *DXAPIEndPointRequestParameterValue {
	aeprpv.IsArrayChildren = true
	child := DXAPIEndPointRequestParameterValue{Owner: aeprpv.Owner, Metadata: aepp}
	child.Parent = aeprpv
	aeprpv.ArrayChildren = append(aeprpv.ArrayChildren, &child)
	return &child
}
func (aeprpv *DXAPIEndPointRequestParameterValue) SetRawValue(rv any, variablePath string) (err error) {
	aeprpv.RawValue = rv
	if aeprpv.Metadata.Type == "json" {
		jsonValue, ok := rv.(map[string]interface{})
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, variablePath, aeprpv.Metadata.Type, utils.TypeAsString(rv), rv)
		}
		for _, v := range aeprpv.Metadata.Children {
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
					return errors.Wrap(err, "error occured")
				}
			}

		}
	}
	if aeprpv.Metadata.Type == "array-json-template" {
		jsonArrayValue, ok := rv.([]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, variablePath, aeprpv.Metadata.Type, utils.TypeAsString(rv), rv)
		}
		for i, j := range jsonArrayValue {
			for _, v := range aeprpv.Metadata.Children {
				childValue := aeprpv.NewArrayChild(v)
				aVariablePath := fmt.Sprint("[%d]", i) + "." + v.NameId
				jj, ok := j.(map[string]interface{})
				if !ok {
					return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, variablePath, aeprpv.Metadata.Type, utils.TypeAsString(rv), rv)
				}
				jv, ok := jj[v.NameId]
				if !ok {
					if v.IsMustExist {
						return aeprpv.Owner.Log.WarnAndCreateErrorf("MISSING_MANDATORY_FIELD:%s", aVariablePath)
					}
				} else {
					err = childValue.SetRawValue(jv, aVariablePath)
					if err != nil {
						return errors.Wrap(err, "error occured")
					}
				}
			}
		}
	}
	return nil
}

func (aeprpv *DXAPIEndPointRequestParameterValue) Validate() (err error) {
	if aeprpv.Metadata.IsMustExist {
		if aeprpv.RawValue == nil {
			return errors.New("MISSING_MANDATORY_FIELD:" + aeprpv.GetNameIdPath())
		}
	}
	if aeprpv.RawValue == nil {
		return nil
	}
	rawValueType := utils.TypeAsString(aeprpv.RawValue)
	nameIdPath := aeprpv.GetNameIdPath()
	if aeprpv.Metadata.Type != rawValueType {
		switch aeprpv.Metadata.Type {
		case "nullable-int64":
		case "int64", "int64zp", "int64p":
			if rawValueType == "float64" {
				if !utils.IfFloatIsInt(aeprpv.RawValue.(float64)) {
					return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
				}
			}
		case "float32", "float32zp", "float32p":
			switch rawValueType {
			case "int64":
			case "int32":
			case "float64":
			case "float32":
			default:
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "float64", "float64zp", "float64p":
			switch rawValueType {
			case "int64":
			case "float64":
			default:
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "protected-string", "protected-sql-string", "nullable-string":
			if rawValueType != "string" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "json":
			if rawValueType != "map[string]interface {}" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
			for _, v := range aeprpv.Children {
				err = v.Validate()
				if err != nil {
					return errors.Wrap(err, "error occured")
				}
			}
		case "json-passthrough":
			if rawValueType != "map[string]interface {}" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "iso8601", "date", "time":
			if rawValueType != "string" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "email":
			if rawValueType != "string" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "phonenumber":
			if rawValueType != "string" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "npwp":
			if rawValueType != "string" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "array":
			if rawValueType != "[]interface {}" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "array-json-template":
			if rawValueType != "[]interface {}" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
			for _, j := range aeprpv.ArrayChildren {
				for _, v := range j.Children {
					err = v.Validate()
					if err != nil {
						return errors.Wrap(err, "error occured")
					}
				}
			}
		case "array-string":
			if rawValueType != "[]interface {}" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		case "array-int64":
			if rawValueType != "[]interface {}" {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_TYPE_MATCHING:SHOULD_[%s].(%v)_BUT_RECEIVE_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
	}
	switch aeprpv.Metadata.Type {
	case "nullable-int64":
		if aeprpv.RawValue == nil {
			aeprpv.Value = nil
			return nil
		}
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := int64(t)
		aeprpv.Value = v
		return nil
	case "int64":
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := int64(t)
		aeprpv.Value = v
		return nil
	case "int64p":
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := int64(t)
		if v > 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case "float64":
		v, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = v
		return nil
	case "float64zp":
		v, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if v >= 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case "float64p":
		v, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if v > 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case "float32":
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := float32(t)
		aeprpv.Value = v
		return nil
	case "float32zp":
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := float32(t)
		if v >= 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case "float32p":
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := float32(t)
		if v > 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case "protected-string":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if security.StringCheckPossibleSQLInjection(s) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
		}
		aeprpv.Value = s
		return nil
	case "protected-sql-string":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if security.PartSQLStringCheckPossibleSQLInjection(s) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
		}
		aeprpv.Value = s
		return nil
	case "nullable-string":
		if aeprpv.RawValue == nil {
			aeprpv.Value = nil
			return nil
		}
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case "string":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case "email":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if s != "" {
			if !FormatEMailCheckValid(s) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_EMAIL_FORMAT:%s", s)
			}
		}
		aeprpv.Value = s
		return nil
	case "phonenumber":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if s != "" {
			if !FormatPhoneNumberCheckValid(s) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_PHONENUMBER_FORMAT:%s", s)
			}
		}
		aeprpv.Value = s
		return nil
	case "npwp":
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if s != "" {
			if !FormatNPWPorNIKCheckValid(s) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_NPWP_FORMAT:%s", s)
			}
		}
		aeprpv.Value = s
		return nil
	case "json":
		s := utils.JSON{}
		for _, v := range aeprpv.Children {
			s[v.Metadata.NameId] = v.Value
		}
		aeprpv.Value = s
		return nil
	case "json-passthrough":
		s, ok := aeprpv.RawValue.(map[string]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case "array":
		s, ok := aeprpv.RawValue.([]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case "array-json-template":
		rawSlice, ok := aeprpv.RawValue.([]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}

		// Convert []any to []map[string]any
		a := make([]map[string]any, len(rawSlice))
		for i, v := range rawSlice {
			str, ok := v.(map[string]any)
			if !ok {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			}

			s := map[string]any{}
			for _, mv := range aeprpv.Metadata.Children {
				s[mv.NameId] = str[mv.NameId]
			}

			a[i] = s
		}
		aeprpv.Value = a
		return nil
	case "array-string":
		rawSlice, ok := aeprpv.RawValue.([]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}

		// Convert []any to []string
		s := make([]string, len(rawSlice))
		for i, v := range rawSlice {
			str, ok := v.(string)
			if !ok {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			}
			s[i] = str
		}
		aeprpv.Value = s
		return nil
	case "array-int64":
		rawSlice, ok := aeprpv.RawValue.([]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}

		// Convert []any to []string
		s := make([]int64, len(rawSlice))
		for i, v := range rawSlice {
			aNumber, ok := v.(float64)
			if !ok {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			}
			aInt := int64(aNumber)
			s[i] = aInt
		}
		aeprpv.Value = s
		return nil
	case "iso8601":
		/* RFC3339Nano format conform to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
		   The golang time package documentation (https://pkg.go.dev/time#pkg-constants) has wrong information on the RFC3339/RFC3329Nano format.
		   but the code is conformed to the standard. Only the documentation is incorrect.
		*/
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if strings.Contains(s, " ") {
			s = strings.Replace(s, " ", "T", 1)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_RFC3339NANO_FORMAT:%s", s)
		}
		aeprpv.Value = t
		return nil
	case "date":
		/* RFC3339Nano format conform to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
		   The golang time package documentation (https://pkg.go.dev/time#pkg-constants) has wrong information on the RFC3339/RFC3329Nano format.
		   but the code is conformed to the standard. Only the documentation is incorrect.
		*/
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		t, err := time.Parse(time.DateOnly, s)
		if err != nil {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_DATE_FROMAT:%s=%s", nameIdPath, s)
		}
		aeprpv.Value = t
		return nil
	case "time":
		/* RFC3339Nano format conform to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
		   The golang time package documentation (https://pkg.go.dev/time#pkg-constants) has wrong information on the RFC3339/RFC3329Nano format.
		   but the code is conformed to the standard. Only the documentation is incorrect.
		*/
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		t, err := time.Parse(time.TimeOnly, s)
		if err != nil {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_TIME_FROMAT:%s=%s", nameIdPath, s)
		}
		aeprpv.Value = t
		return nil
	default:
		aeprpv.Value = aeprpv.RawValue
		return nil
	}
}
