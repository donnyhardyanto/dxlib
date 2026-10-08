package api

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	security "github.com/donnyhardyanto/dxlib/utils/security"
	"github.com/shopspring/decimal"

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
			aVariablePath := variablePath + "." + v.NameId
			jv, ok := jsonValue[v.NameId]
			if !ok {
				if v.IsMustExist {
					return aeprpv.Owner.Log.WarnAndCreateErrorf("MISSING_MANDATORY_FIELD:%s", aVariablePath)
				}
				continue
			}
			childValue := aeprpv.NewChild(v)
			err = childValue.SetRawValue(jv, aVariablePath)
			if err != nil {
				return errors.Wrap(err, "error at DXAPIEndPointRequestParameterValue.SetRawValue")
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
				aVariablePath := fmt.Sprintf("%s[%d].%s", variablePath, i, v.NameId)
				jv, ok := jj[v.NameId]
				if !ok {
					if v.IsMustExist {
						return aeprpv.Owner.Log.WarnAndCreateErrorf("MISSING_MANDATORY_FIELD:%s", aVariablePath)
					}
					continue
				}
				childValue := containerObj.NewChild(v)
				err = childValue.SetRawValue(jv, aVariablePath)
				if err != nil {
					return errors.Wrap(err, "error at DXAPIEndPointRequestParameterValue.SetRawValue")
				}
			}

			aeprpv.ArrayChildren = append(aeprpv.ArrayChildren, containerObj)

		}
	}
	return nil
}

func (aeprpv *DXAPIEndPointRequestParameterValue) validateWhenNotSameWithRawValue(rawValueType, nameIdPath string) (err error) {
	switch aeprpv.Metadata.Type {
	case dxlibTypes.APIParameterTypeNullableInt64, dxlibTypes.APIParameterTypeNullableInt32:
	case dxlibTypes.APIParameterTypeInt64, dxlibTypes.APIParameterTypeInt64ZP, dxlibTypes.APIParameterTypeInt64P, dxlibTypes.APIParameterTypeID:
		if rawValueType == "float64" {
			if !utils.IfFloatIsInt(aeprpv.RawValue.(float64)) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		}
	case dxlibTypes.APIParameterTypeInt32, dxlibTypes.APIParameterTypeInt32ZP, dxlibTypes.APIParameterTypeInt32P:
		if rawValueType == "float64" {
			if !utils.IfFloatIsInt(aeprpv.RawValue.(float64)) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			}
		}
	case dxlibTypes.APIParameterTypeMoney:
		// Money travels as a JSON string; a JSON number has already been through
		// a binary float and may have lost digits, so it is refused here as the
		// data-model layer refuses it (validateFieldValue).
		if rawValueType != "string" {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
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
	case dxlibTypes.APIParameterTypeProtectedString, dxlibTypes.APIParameterTypeProtectedSQLString, dxlibTypes.APIParameterTypeProtectedNonEmptyString, dxlibTypes.APIParameterTypeNullableString, dxlibTypes.APIParameterTypeNonEmptyString, dxlibTypes.APIParameterTypeISO8601, dxlibTypes.APIParameterTypeDate, dxlibTypes.APIParameterTypeTime, dxlibTypes.APIParameterTypeEmail, dxlibTypes.APIParameterTypePhoneNumber, dxlibTypes.APIParameterTypeNPWP:
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
	case dxlibTypes.APIParameterTypeJSONPassthrough, dxlibTypes.APIParameterTypeMapStringString:
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

// rawValueToInt64 accepts the shapes an int64 parameter actually arrives in.
// encoding/json hands every number over as a float64, and the GET and DELETE
// branch of PreProcessRequest hands every parameter over as whatever FormValue
// returned, which is always a string -- so a decimal string is a wire form this
// package has to read rather than a concession to one client. Without it an
// int64 parameter could not be declared on a query string at all. The mobile
// clients send the same shape in a JSON body, having carried these ids as
// strings since the split.
//
// Nothing is trimmed and no other notation is taken: a JSON number cannot carry
// surrounding space either, and this should be no more permissive than the
// number path. An empty string is left to fail with the rest. IsMustExist is
// checked against a nil RawValue only, so an empty string that resolved to zero
// would let a mandatory id be satisfied by a value the caller never sent.
//
// A JSON number is refused unless it is a whole number within the int64 range,
// checked on the float before it is converted (Go leaves an out-of-range
// float-to-int conversion undefined). This is the check every int64-family
// type goes through, nullable-int64 included; the type switch in
// validateWhenNotSameWithRawValue only runs for some of them. float64 cannot
// hold MaxInt64 (it rounds to 2^63), so a client that needs the top of the
// range sends it as a string.
func (aeprpv *DXAPIEndPointRequestParameterValue) rawValueToInt64(nameIdPath string) (int64, error) {
	switch val := aeprpv.RawValue.(type) {
	case float64:
		if !utils.FloatFitsInt64(val) {
			return 0, aeprpv.Owner.Log.WarnAndCreateErrorf("INT64_OUT_OF_RANGE:%s=%v", nameIdPath, val)
		}
		return int64(val), nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case string:
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_INT64_FORMAT:%s=%q", nameIdPath, val)
		}
		return v, nil
	default:
		return 0, aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
}

func (aeprpv *DXAPIEndPointRequestParameterValue) resolveToInt64XXX(nameIdPath string) (err error) {
	if aeprpv.Metadata.Type == dxlibTypes.APIParameterTypeNullableInt64 && aeprpv.RawValue == nil {
		aeprpv.Value = nil
		return nil
	}
	v, err := aeprpv.rawValueToInt64(nameIdPath)
	if err != nil {
		return err
	}
	switch aeprpv.Metadata.Type {
	case dxlibTypes.APIParameterTypeNullableInt64, dxlibTypes.APIParameterTypeInt64, dxlibTypes.APIParameterTypeID:
		aeprpv.Value = v
		return nil
	case dxlibTypes.APIParameterTypeInt64P:
		if v > 0 {
			aeprpv.Value = v
			return nil
		}
	case dxlibTypes.APIParameterTypeInt64ZP:
		if v >= 0 {
			aeprpv.Value = v
			return nil
		}
	}
	return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
}

// rawValueToInt32 takes the same wire shapes as rawValueToInt64 (a JSON number
// arrives as float64, a query-string or X-Var value as a string) and refuses
// anything outside the int32 range. The float is range-checked before it is
// converted: Go leaves an out-of-range float-to-int conversion undefined, so
// the check cannot be done on the converted value.
func (aeprpv *DXAPIEndPointRequestParameterValue) rawValueToInt32(nameIdPath string) (int32, error) {
	var v int64
	switch val := aeprpv.RawValue.(type) {
	case float64:
		if val != math.Trunc(val) || val < math.MinInt32 || val > math.MaxInt32 {
			return 0, aeprpv.Owner.Log.WarnAndCreateErrorf("INT32_OUT_OF_RANGE:%s=%v", nameIdPath, val)
		}
		v = int64(val)
	case int:
		v = int64(val)
	case int32:
		return val, nil
	case int64:
		v = val
	case string:
		p, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return 0, aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_INT32_FORMAT:%s=%q", nameIdPath, val)
		}
		return int32(p), nil
	default:
		return 0, aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, aeprpv.Owner.Log.WarnAndCreateErrorf("INT32_OUT_OF_RANGE:%s=%v", nameIdPath, v)
	}
	return int32(v), nil
}

// resolveToInt32XXX mirrors resolveToInt64XXX. A nullable-int32 parameter may
// be left out of the request; when it is, the value stays nil and the Go type
// of a present value is still a plain int32.
func (aeprpv *DXAPIEndPointRequestParameterValue) resolveToInt32XXX(nameIdPath string) (err error) {
	if aeprpv.Metadata.Type == dxlibTypes.APIParameterTypeNullableInt32 && aeprpv.RawValue == nil {
		aeprpv.Value = nil
		return nil
	}
	v, err := aeprpv.rawValueToInt32(nameIdPath)
	if err != nil {
		return err
	}
	switch aeprpv.Metadata.Type {
	case dxlibTypes.APIParameterTypeNullableInt32, dxlibTypes.APIParameterTypeInt32:
		aeprpv.Value = v
		return nil
	case dxlibTypes.APIParameterTypeInt32P:
		if v > 0 {
			aeprpv.Value = v
			return nil
		}
	case dxlibTypes.APIParameterTypeInt32ZP:
		if v >= 0 {
			aeprpv.Value = v
			return nil
		}
	}
	return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
}

// resolveToMoney reads a money parameter from its string form into a
// decimal.Decimal. Only a plain decimal string is taken ("1250000.375",
// "-12.5", "7"); exponent notation, spaces, a currency sign or an empty string
// are refused. Precision and scale are left to the NUMERIC(23,4) column.
func (aeprpv *DXAPIEndPointRequestParameterValue) resolveToMoney(nameIdPath string) (err error) {
	s, ok := aeprpv.RawValue.(string)
	if !ok {
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
	if !isPlainDecimalString(s) {
		return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_MONEY_FORMAT:%s=%q", nameIdPath, s)
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_MONEY_FORMAT:%s=%q", nameIdPath, s)
	}
	aeprpv.Value = d
	return nil
}

// isPlainDecimalString accepts an optional sign, digits, and at most one dot
// with digits on at least one side of it.
func isPlainDecimalString(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	digits, dots := 0, 0
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] >= '0' && s[i] <= '9':
			digits++
		case s[i] == '.':
			dots++
		default:
			return false
		}
	}
	return digits > 0 && dots <= 1
}

func (aeprpv *DXAPIEndPointRequestParameterValue) rawValueToFloat64(nameIdPath string) (float64, error) {
	switch val := aeprpv.RawValue.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	default:
		return 0, aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
}

func (aeprpv *DXAPIEndPointRequestParameterValue) rawValueToFloat64WithFloat32(nameIdPath string) (float64, error) {
	switch val := aeprpv.RawValue.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case float32:
		return float64(val), nil
	default:
		return 0, aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
}

func (aeprpv *DXAPIEndPointRequestParameterValue) resolveToFloat64XXX(nameIdPath string) (err error) {
	v, err := aeprpv.rawValueToFloat64(nameIdPath)
	if err != nil {
		return err
	}
	switch aeprpv.Metadata.Type {
	case dxlibTypes.APIParameterTypeFloat64:
		aeprpv.Value = v
		return nil
	case dxlibTypes.APIParameterTypeFloat64ZP:
		if v >= 0 {
			aeprpv.Value = v
			return nil
		}
	case dxlibTypes.APIParameterTypeFloat64P:
		if v > 0 {
			aeprpv.Value = v
			return nil
		}
	default:
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
	return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
}

func (aeprpv *DXAPIEndPointRequestParameterValue) resolveToFloat32XXX(nameIdPath string) (err error) {
	v, err := aeprpv.rawValueToFloat64WithFloat32(nameIdPath)
	if err != nil {
		return err
	}
	switch aeprpv.Metadata.Type {
	case dxlibTypes.APIParameterTypeFloat32:
		aeprpv.Value = float32(v)
		return nil
	case dxlibTypes.APIParameterTypeFloat32ZP:
		if v >= 0 {
			aeprpv.Value = float32(v)
			return nil
		}
	case dxlibTypes.APIParameterTypeFloat32P:
		if v > 0 {
			aeprpv.Value = float32(v)
			return nil
		}
	default:
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
	return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
}

func (aeprpv *DXAPIEndPointRequestParameterValue) resolveToStringXXX(nameIdPath string) (err error) {
	switch aeprpv.Metadata.Type {
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
	case dxlibTypes.APIParameterTypeProtectedNonEmptyString:
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		s = strings.Trim(s, " ")
		if len(s) == 0 {
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
	default:
		return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
	}
}

func (aeprpv *DXAPIEndPointRequestParameterValue) resolveValue(nameIdPath string) (err error) {
	switch aeprpv.Metadata.Type {
	case
		dxlibTypes.APIParameterTypeNullableInt64,
		dxlibTypes.APIParameterTypeInt64,
		dxlibTypes.APIParameterTypeInt64P,
		dxlibTypes.APIParameterTypeInt64ZP,
		dxlibTypes.APIParameterTypeID:
		return aeprpv.resolveToInt64XXX(nameIdPath)
	case
		dxlibTypes.APIParameterTypeNullableInt32,
		dxlibTypes.APIParameterTypeInt32,
		dxlibTypes.APIParameterTypeInt32P,
		dxlibTypes.APIParameterTypeInt32ZP:
		return aeprpv.resolveToInt32XXX(nameIdPath)
	case dxlibTypes.APIParameterTypeMoney:
		return aeprpv.resolveToMoney(nameIdPath)
	case
		dxlibTypes.APIParameterTypeFloat64,
		dxlibTypes.APIParameterTypeFloat64P,
		dxlibTypes.APIParameterTypeFloat64ZP:
		return aeprpv.resolveToFloat64XXX(nameIdPath)
	case
		dxlibTypes.APIParameterTypeFloat32,
		dxlibTypes.APIParameterTypeFloat32P,
		dxlibTypes.APIParameterTypeFloat32ZP:
		return aeprpv.resolveToFloat32XXX(nameIdPath)
	case
		dxlibTypes.APIParameterTypeProtectedString,
		dxlibTypes.APIParameterTypeProtectedNonEmptyString,
		dxlibTypes.APIParameterTypeProtectedSQLString,
		dxlibTypes.APIParameterTypeNullableString,
		dxlibTypes.APIParameterTypeString,
		dxlibTypes.APIParameterTypeNonEmptyString,
		dxlibTypes.APIParameterTypeEmail,
		dxlibTypes.APIParameterTypePhoneNumber,
		dxlibTypes.APIParameterTypeNPWP:
		return aeprpv.resolveToStringXXX(nameIdPath)
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
	case dxlibTypes.APIParameterTypeMapStringString:
		rawMap, ok := aeprpv.RawValue.(map[string]any)
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
		}
		s := make(map[string]string, len(rawMap))
		for k, v := range rawMap {
			if str, ok := v.(string); ok {
				s[k] = str
			} else {
				s[k] = fmt.Sprintf("%v", v)
			}
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
		var s []any
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

		// Each element arrives as a float64 from encoding/json and gets the
		// same whole-number and range check as a scalar int64 parameter.
		s := make([]int64, len(rawSlice))
		for i, v := range rawSlice {
			aNumber, ok := v.(float64)
			if !ok {
				return aeprpv.Owner.Log.WarnAndCreateErrorf(ErrorMessageIncompatibleTypeReceived, nameIdPath, aeprpv.Metadata.Type, utils.TypeAsString(aeprpv.RawValue), aeprpv.RawValue)
			}
			if !utils.FloatFitsInt64(aNumber) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("INT64_OUT_OF_RANGE:%s[%d]=%v", nameIdPath, i, aNumber)
			}
			s[i] = int64(aNumber)
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
	if len(aeprpv.Metadata.Enum) > 0 {
		found := false
		for _, enumVal := range aeprpv.Metadata.Enum {
			valStr := fmt.Sprintf("%v", aeprpv.Value)
			enumStr := fmt.Sprintf("%v", enumVal)
			if strings.EqualFold(valStr, enumStr) {
				found = true
				break
			}
		}
		if !found {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_ENUM_VALUE:%s=%v, allowed=%v", nameIdPath, aeprpv.Value, aeprpv.Metadata.Enum)
		}
	}
	return nil
}
