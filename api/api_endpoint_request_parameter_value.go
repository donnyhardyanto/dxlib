package api

import (
	"fmt"
	"strings"
	"time"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	security "github.com/donnyhardyanto/dxlib/utils/security"
	"github.com/donnyhardyanto/dxlib/errors"

	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/utils"
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
	ArrayChildren   []DXAPIEndPointRequestParameterValue
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

func (aeprpv *DXAPIEndPointRequestParameterValue) SetRawValue(rv any, variablePath string) (err error) {
	aeprpv.RawValue = rv
	if aeprpv.RawValue == nil {
		return nil
	}
	if aeprpv.Metadata.Type == dxlibTypes.APIParameterTypeJSON {
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
					return errors.Wrap(err, "error at DXAPIEndPointRequestParameterValue.SetRawValue")
				}
			}

		}
	}
	if aeprpv.Metadata.Type == dxlibTypes.APIParameterTypeArrayJSONTemplate {
		jsonArrayValue, ok := rv.([]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, variablePath, aeprpv.Metadata.Type, utils.TypeAsString(rv), rv)
		}
		for i, j := range jsonArrayValue {
			aVariablePath := fmt.Sprintf("%s[%d]", variablePath, i)

			jj, ok := j.(map[string]interface{})
			if !ok {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, aVariablePath, aeprpv.Metadata.Type, utils.TypeAsString(j), j)
			}

			// Create a new object for each array element that will hold all children
			containerObj := DXAPIEndPointRequestParameterValue{
				Owner:    aeprpv.Owner,
				Parent:   aeprpv,
				Metadata: aeprpv.Metadata,
				RawValue: jj,
			}
			containerObj.Metadata.Type = dxlibTypes.APIParameterTypeJSON
			containerObj.Metadata.NameId = aVariablePath

			for _, v := range containerObj.Metadata.Children {
				childValue := containerObj.NewChild(v)
				aVariablePath := fmt.Sprintf("%s[%d].%s", variablePath, i, v.NameId)

				jv, ok := jj[v.NameId]
				if !ok {
					if v.IsMustExist {
						return aeprpv.Owner.Log.WarnAndCreateErrorf("MISSING_MANDATORY_FIELD:%s", aVariablePath)
					}
				} else {
					err = childValue.SetRawValue(jv, aVariablePath)
					if err != nil {
						return errors.Wrap(err, "error at DXAPIEndPointRequestParameterValue.SetRawValue")
					}
				}
			}

			aeprpv.ArrayChildren = append(aeprpv.ArrayChildren, containerObj)

		}
	}
	return nil
}

func (aeprpv *DXAPIEndPointRequestParameterValue) validateWhenNotSameWithRawValue(rawValueType, nameIdPath string) (err error) {
	switch aeprpv.Metadata.Type {
	case dxlibTypes.APIParameterTypeNullableInt64:
	case dxlibTypes.APIParameterTypeInt64, dxlibTypes.APIParameterTypeInt64ZP, dxlibTypes.APIParameterTypeInt64P:
		if rawValueType == "float64" {
			if !utils.IfFloatIsInt(aeprpv.RawValue.(float64)) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		}
	case dxlibTypes.APIParameterTypeFloat32, dxlibTypes.APIParameterTypeFloat32ZP, dxlibTypes.APIParameterTypeFloat32P:
		switch rawValueType {
		case "int64":
		case "int32":
		case "float64":
		case "float32":
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
	case dxlibTypes.APIParameterTypeFloat64, dxlibTypes.APIParameterTypeFloat64ZP, dxlibTypes.APIParameterTypeFloat64P:
		switch rawValueType {
		case "int64":
		case "float64":
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
	case dxlibTypes.APIParameterTypeProtectedString, dxlibTypes.APIParameterTypeProtectedSQLString, dxlibTypes.APIParameterTypeNullableString, dxlibTypes.APIParameterTypeNonEmptyString, dxlibTypes.APIParameterTypeISO8601, dxlibTypes.APIParameterTypeDate, dxlibTypes.APIParameterTypeTime, dxlibTypes.APIParameterTypeEmail, dxlibTypes.APIParameterTypePhoneNumber, dxlibTypes.APIParameterTypeNPWP:
		if rawValueType != "string" {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
	case dxlibTypes.APIParameterTypeJSON:
		if rawValueType != "map[string]interface {}" {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
		for _, v := range aeprpv.Children {
			err = v.Validate()
			if err != nil {
				return err
			}
		}
	case dxlibTypes.APIParameterTypeJSONPassthrough:
		if rawValueType != "map[string]interface {}" {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
	case dxlibTypes.APIParameterTypeArray, dxlibTypes.APIParameterTypeArrayString, dxlibTypes.APIParameterTypeArrayInt64:
		if rawValueType != "[]interface {}" {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
	case dxlibTypes.APIParameterTypeArrayJSONTemplate:
		if rawValueType != "[]interface {}" {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
		}
		for _, j := range aeprpv.ArrayChildren {
			err = j.Validate()
			if err != nil {
				return err
			}
		}
	default:
		return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_TYPE_MATCHING:SHOULD_[%s].(%v)_BUT_RECEIVE_(%s)=%v", nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
	}
	return nil
}

func (aeprpv *DXAPIEndPointRequestParameterValue) resolveToInt64XXX(nameIdPath string) (err error) {
	switch aeprpv.Metadata.Type {
	case dxlibTypes.APIParameterTypeNullableInt64:
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
	case dxlibTypes.APIParameterTypeInt64:
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := int64(t)
		aeprpv.Value = v
		return nil
	case dxlibTypes.APIParameterTypeInt64P:
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
	case dxlibTypes.APIParameterTypeInt64ZP:
		t, ok := aeprpv.RawValue.(float64)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		v := int64(t)
		if v >= 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	default:
	}
	return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
}

func (aeprpv *DXAPIEndPointRequestParameterValue) resolveValue(nameIdPath string) (err error) {
	switch aeprpv.Metadata.Type {
	case
		dxlibTypes.APIParameterTypeNullableInt64,
		dxlibTypes.APIParameterTypeInt64,
		dxlibTypes.APIParameterTypeInt64P,
		dxlibTypes.APIParameterTypeInt64ZP:
		return aeprpv.resolveToInt64XXX(nameIdPath)
	case dxlibTypes.APIParameterTypeFloat64:
		var v float64
		switch val := aeprpv.RawValue.(type) {
		case float64:
			v = val
		case int:
			v = float64(val)
		case int32:
			v = float64(val)
		case int64:
			v = float64(val)
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = v
		return nil
	case dxlibTypes.APIParameterTypeFloat64ZP:
		var v float64
		switch val := aeprpv.RawValue.(type) {
		case float64:
			v = val
		case int:
			v = float64(val)
		case int32:
			v = float64(val)
		case int64:
			v = float64(val)
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if v >= 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case dxlibTypes.APIParameterTypeFloat64P:
		var v float64
		switch val := aeprpv.RawValue.(type) {
		case float64:
			v = val
		case int:
			v = float64(val)
		case int32:
			v = float64(val)
		case int64:
			v = float64(val)
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if v > 0 {
			aeprpv.Value = v
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case dxlibTypes.APIParameterTypeFloat32:
		var v float64
		switch val := aeprpv.RawValue.(type) {
		case float64:
			v = val
		case int:
			v = float64(val)
		case int32:
			v = float64(val)
		case int64:
			v = float64(val)
		case float32:
			v = float64(val)
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = float32(v)
		return nil
	case dxlibTypes.APIParameterTypeFloat32ZP:
		var v float64
		switch val := aeprpv.RawValue.(type) {
		case float64:
			v = val
		case int:
			v = float64(val)
		case int32:
			v = float64(val)
		case int64:
			v = float64(val)
		case float32:
			v = float64(val)
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if v >= 0 {
			aeprpv.Value = float32(v)
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case dxlibTypes.APIParameterTypeFloat32P:
		var v float64
		switch val := aeprpv.RawValue.(type) {
		case float64:
			v = val
		case int:
			v = float64(val)
		case int32:
			v = float64(val)
		case int64:
			v = float64(val)
		case float32:
			v = float64(val)
		default:
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if v > 0 {
			aeprpv.Value = float32(v)
			return nil
		}
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	case dxlibTypes.APIParameterTypeProtectedString:
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if security.StringCheckPossibleSQLInjection(s) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeProtectedSQLString:
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		if security.PartSQLStringCheckPossibleSQLInjection(s) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeNullableString:
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
	case dxlibTypes.APIParameterTypeString:
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeNonEmptyString:
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		s = strings.Trim(s, " ")
		if len(s) == 0 {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeEmail:
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
	case dxlibTypes.APIParameterTypePhoneNumber:
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
	case dxlibTypes.APIParameterTypeNPWP:
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
	case dxlibTypes.APIParameterTypeJSON:
		s := utils.JSON{}
		for _, v := range aeprpv.Children {
			s[v.Metadata.NameId] = v.Value
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeJSONPassthrough:
		s, ok := aeprpv.RawValue.(map[string]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeArray:
		s, ok := aeprpv.RawValue.([]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeArrayJSONTemplate:
		var s []utils.JSON
		for _, v := range aeprpv.ArrayChildren {
			err = v.Validate()
			if err != nil {
				return err
			}
			s = append(s, v.Value.(utils.JSON))
		}
		aeprpv.Value = s
		return nil
	case dxlibTypes.APIParameterTypeArrayString:
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
	case dxlibTypes.APIParameterTypeArrayInt64:
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
	case dxlibTypes.APIParameterTypeISO8601:
		/* RFC3339Nano format conforms to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
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
	case dxlibTypes.APIParameterTypeDate:
		/* RFC3339Nano format conforms to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
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
	case dxlibTypes.APIParameterTypeTime:
		/* RFC3339Nano format conforms to RFC3339 RFC, not Go https://pkg.go.dev/time#pkg-constants.
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
	if string(aeprpv.Metadata.Type) != rawValueType {
		err = aeprpv.validateWhenNotSameWithRawValue(rawValueType, nameIdPath)
		if err != nil {
			return err
		}
	}
	err = aeprpv.resolveValue(nameIdPath)
	if err != nil {
		return err
	}
	return nil
}
