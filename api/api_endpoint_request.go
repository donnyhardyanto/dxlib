package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"dxlib/v3/log"
	"dxlib/v3/utils"
	utilsHttp "dxlib/v3/utils/http"
	utilsJson "dxlib/v3/utils/json"
	security "dxlib/v3/utils/security"
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

type DXAPIUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type DXAPIEndPointRequest struct {
	Id              string
	Context         context.Context
	EndPoint        *DXAPIEndPoint
	ParameterValues map[string]*DXAPIEndPointRequestParameterValue
	Log             log.DXLog
	FiberContext    *fiber.Ctx

	WSConnection          *websocket.Conn
	RequestBodyAsBytes    []byte
	ResponseErrorAsString string
	ResponseStatusCode    int
	ResponseBodyAsBytes   []byte
	ErrorMessage          []string
	CurrentUser           DXAPIUser
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

func (aeprpv *DXAPIEndPointRequestParameterValue) SetRawValue(rv any) (err error) {
	aeprpv.RawValue = rv
	if aeprpv.Metadata.Type == "json" {
		jsonValue, ok := rv.(map[string]interface{})
		if !ok {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("Invalid type [%s].(%v) but receive (%s)=%v ", aeprpv.Metadata.NameId, aeprpv.Metadata.Type, utils.TypeAsString(rv), rv)
		}
		for _, v := range aeprpv.Metadata.Children {
			{
				childValue := aeprpv.NewChild(v)
				err = childValue.SetRawValue(jsonValue[v.NameId])
				if err != nil {
					return err
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
	rawValueType := utils.TypeAsString(aeprpv.RawValue)
	if aeprpv.Metadata.Type != rawValueType {
		switch aeprpv.Metadata.Type {
		case "int64":
			if rawValueType == "float64" {
				if !utils.IfFloatIsInt(aeprpv.RawValue.(float64)) {
					aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("Invalid type [%s].(%v) but receive (%s)=%v ", aeprpv.Metadata.NameId, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
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
				aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("Invalid type [%s].(%v) but receive (%s)=%v ", aeprpv.Metadata.NameId, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
				return false
			}
		case "protected-string", "protected-sql-string", "iso8601":
			if rawValueType != "string" {
				return false
			}
		case "json":
			if rawValueType != "map[string]interface {}" {
				return false
			}
			for _, v := range aeprpv.Children {
				if v.Validate() != true {
					return false
				}
			}
			/*		case "iso8601":
					if rawValueType != "string" {
						return false
					}*/
		case "array":
			if rawValueType != "[]interface {}" {
				return false
			}
		default:
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("Invalid type [%s].(%v) but receive (%s)=%v ", aeprpv.Metadata.NameId, aeprpv.Metadata.Type, rawValueType, aeprpv.RawValue)
			return false
		}
	}
	switch aeprpv.Metadata.Type {
	case "int64":
		v := int64(aeprpv.RawValue.(float64))
		aeprpv.Value = v
		return true
	case "float64":
		v := aeprpv.RawValue.(float64)
		aeprpv.Value = v
		return true
	case "float32":
		v := float32(aeprpv.RawValue.(float64))
		aeprpv.Value = v
		return true
	case "protected-string":
		s := aeprpv.RawValue.(string)
		if security.StringCheckPossibleSQLInjection(s) {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
			return false
		}
		aeprpv.Value = s
		return true
	case "protected-sql-string":
		s := aeprpv.RawValue.(string)
		if security.PartSQLStringCheckPossibleSQLInjection(s) {
			aeprpv.ErrValidate = aeprpv.Owner.Log.WarnAndCreateErrorf("Possible SQL injection found [%s]", s)
			return false
		}
		aeprpv.Value = s
		return true
	case "string":
		s := aeprpv.RawValue.(string)
		aeprpv.Value = s
		return true
	case "json":
		s := aeprpv.RawValue.(utils.JSON)
		aeprpv.Value = s
		return true
	case "array":
		s := aeprpv.RawValue.([]any)
		aeprpv.Value = s
		return true
	case "iso8601":
		s := aeprpv.RawValue.(string)
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			aeprpv.Owner.Log.Warnf("Invalid RFC3339Nano format [%s]", s)
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

func (aepr *DXAPIEndPointRequest) NewAPIEndPointRequestParameter(aepp DXAPIEndPointParameter) *DXAPIEndPointRequestParameterValue {
	aerp := DXAPIEndPointRequestParameterValue{Owner: aepr, Metadata: aepp}
	aepr.ParameterValues[aepp.NameId] = &aerp
	return &aerp
}

func (aepr *DXAPIEndPointRequest) ResponseSetStatusCodeError(statusCode int, reason, reasonMessage string, data ...any) (err error) {
	if statusCode != 200 {
		aepr.Log.Warnf("Status Code: %d %s %s %v", statusCode, reason, reasonMessage, data)
	}
	return aepr.ResponseSetStatusCodeAndBodyJSON(
		statusCode,
		utils.JSON{
			"reason":         reason,
			"reason_message": reasonMessage,
			"data":           data,
		},
	)
}

func (aepr *DXAPIEndPointRequest) ResponseSetStatusCodeAndBodyJSON(statusCode int, v utils.JSON) (err error) {
	aepr.ResponseStatusCode = statusCode
	return aepr.ResponseSetFromJSON(v)
}

func (aepr *DXAPIEndPointRequest) ResponseSetFromJSON(v utils.JSON) (err error) {
	if v == nil {
		return nil
	}
	vAsBytes, err := json.Marshal(v)
	if err != nil {
		return err
	}
	aepr.FiberContext.Response().Header.Set(`Content-Type`, `application/json; charset=utf-8`)
	aepr.ResponseBodyAsBytes = vAsBytes
	return nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueEntry(k string) (val *DXAPIEndPointRequestParameterValue, err error) {
	var ok bool
	if val, ok = aepr.ParameterValues[k]; !ok {
		err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' is not found in request`, k)
		return nil, err
	}
	return val, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsAny(k string) (isExist bool, val any, err error) {
	valEntry, err := aepr.GetParameterValueEntry(k)
	if err != nil {
		return false, "", err
	}
	valAsAny := valEntry.Value
	if valAsAny == nil {
		if valEntry.Metadata.IsMustExist {
			err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' value does not exist (%v)`, k, valAsAny)
			aepr.ResponseStatusCode = http.StatusBadRequest
			return false, "", err
		}
		return false, "", nil
	}
	return true, valAsAny, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsString(k string, defaultValue ...any) (isExist bool, val string, err error) {
	isExist, valAsAny, err := aepr.GetParameterValueAsAny(k)
	if !isExist {
		if defaultValue != nil {
			val = defaultValue[0].(string)
		}
		return isExist, val, err
	}
	val, ok := valAsAny.(string)
	if !ok {
		err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' value is not string (%v)`, k, valAsAny)
		aepr.ResponseStatusCode = http.StatusBadRequest
		return true, "", err
	}
	return true, val, nil
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
		err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' value is not %T (%v)`, k, val, valAsAny)
		aepr.ResponseStatusCode = http.StatusBadRequest
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

func (aepr *DXAPIEndPointRequest) GetParameterValueAsJSON(k string) (isExist bool, val utils.JSON, err error) {
	return getParameterValue[utils.JSON](aepr, k)
}

/*func (aepr *DXAPIEndPointRequest) GetParameterValueAsInt64(k string) (isExist bool, val int64, err error) {
  isExist, valAsAny, err := aepr.GetParameterValueAsAny(k)
  if !isExist {
    return isExist, 0, err
  }
  val, ok := valAsAny.(int64)
  if !ok {
    err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' value is not int64 (%v)`, k, valAsAny)
    aepr.ResponseStatusCode=http.StatusInternalServerError

    return true, 0, err
  }
  return true, val, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsFloat64(k string) (val float64, err error) {
  valAsAny, err := aepr.GetParameterValueEntry(k)
  if err != nil {
    return val, err
  }
  val, ok := valAsAny.(float64)
  if !ok {
    err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' is not float64 (%v)`, k, valAsAny)
    aepr.ResponseStatusCode=http.StatusInternalServerError


  }
  return val, nil
}

func (aepr *DXAPIEndPointRequest) GetParameterValueAsDateTime(k string) (val time.Time, err error) {
  valAsAny, err := aepr.GetParameterValueEntry(k)
  if err != nil {
    return val, err
  }
  val, err = time.Parse(time.RFC3339Nano, valAsAny.(string))
  if err != nil {
    err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' is not Date Time (%v)`, k, err.Error())
    aepr.ResponseStatusCode=http.StatusInternalServerError

  }
  return val, nil
}


func (aepr *DXAPIEndPointRequest) GetParameterValueAsArrayOfAny(k string) (val []any, err error) {
  valAsAny, err := aepr.GetParameterValueEntry(k)
  if err != nil {
    return val, err
  }
  val, ok := valAsAny.([]any)
  if !ok {
    err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' is not Array (%v)`, k, valAsAny)
    aepr.ResponseStatusCode=http.StatusInternalServerError


  }
  return val, nil
}


func (aepr *DXAPIEndPointRequest) GetParameterValueAsMapStringAny(k string) (val utils.JSON, err error) {
  valAsAny, err := aepr.GetParameterValueEntry(k)
  if err != nil {
    return val, err
  }
  val, ok := valAsAny.(utils.JSON)
  if !ok {
    err = aepr.Log.ErrorAndCreateErrorf(`Requested field '%s' is not JSON (%v)`, k, valAsAny)
    aepr.ResponseStatusCode=http.StatusInternalServerError


  }
  return val, nil
}
*/

func (aepr *DXAPIEndPointRequest) ProxyHTTPAPIClient(method string, url string, bodyParameterAsJSON utils.JSON, headers map[string]string) (statusCode int, r utils.JSON, err error) {
	statusCode, r, err = aepr.HTTPClient(method, url, bodyParameterAsJSON, headers)
	switch statusCode {
	case 401:
		aepr.Log.Warnf("Invalid credential")
		aepr.ResponseStatusCode = http.StatusUnauthorized
		return statusCode, r, err
	case 500:
		if err != nil {
			aepr.Log.Errorf("Internal error: (%v)", err)
			aepr.ResponseStatusCode = http.StatusInternalServerError
			return statusCode, r, err
		}
	default:
		if err != nil {
			aepr.Log.Errorf("Error: (%v)", err)
			aepr.ResponseStatusCode = http.StatusInternalServerError
			return statusCode, r, err
		}
		if r["code"] == "UNAUTHORIZED" {
			aepr.Log.Warnf("Invalid credential for")
			aepr.ResponseStatusCode = http.StatusUnauthorized
			return statusCode, r, err
		}
		if r["code"] != "OK" {
			aepr.Log.Warnf("Internal error %v", r["code"])
			aepr.ResponseStatusCode = http.StatusUnauthorized

			return statusCode, r, err
		}
	}
	return statusCode, r, err
}

func (aepr *DXAPIEndPointRequest) PreProcessRequest() (err error) {
	switch aepr.EndPoint.Method {
	case "GET", "DELETE":
	case "POST", "PUT":
		switch aepr.EndPoint.RequestContentType {
		case utilsHttp.ContentTypeRaw:
			err = aepr.preProcessRequestAsRaw()
		case utilsHttp.ContentTypeApplicationJSON:
			err = aepr.preProcessRequestAsApplicationJSON()
		default:
			err = aepr.Log.WarnAndCreateErrorf(`Request content-type is not supported yet (%v)`, aepr.EndPoint.RequestContentType)
			aepr.ResponseStatusCode = http.StatusUnprocessableEntity
		}
	default:
		err = aepr.Log.WarnAndCreateErrorf(`Request method is not supported yet (%v)`, aepr.EndPoint.Method)
		aepr.ResponseStatusCode = http.StatusUnprocessableEntity

	}
	return err
}

func (aepr *DXAPIEndPointRequest) preProcessRequestAsRaw() (err error) {
	aepr.RequestBodyAsBytes = aepr.FiberContext.Body()
	return nil
}

func (aepr *DXAPIEndPointRequest) responseSetStatusCodeAsErrorf(statusCode int, reasonMessage string, data ...any) (err error) {
	err = aepr.Log.WarnAndCreateErrorf(reasonMessage, data)
	aepr.ResponseStatusCode = statusCode
	if aepr.EndPoint.Owner.IsDebug {
		aepr.ResponseSetFromJSON(utils.JSON{
			"reason_message": err.Error(),
		})
	}
	return err
}

func (aepr *DXAPIEndPointRequest) preProcessRequestAsApplicationJSON() (err error) {

	actualContentType := aepr.FiberContext.Get("Content-Type")
	if actualContentType != "" {
		if !strings.Contains(actualContentType, "application/json") {
			err := aepr.Log.WarnAndCreateErrorf(`Request content-type is not application/json but %s`, actualContentType)
			aepr.ResponseStatusCode = http.StatusUnprocessableEntity
			return err
		}
	}
	bodyAsJSON := utils.JSON{}
	aepr.RequestBodyAsBytes = aepr.FiberContext.Body()

	err = json.Unmarshal(aepr.RequestBodyAsBytes, &bodyAsJSON)
	if err != nil {
		aepr.Log.Warnf(`Request body can not be parse as JSON (%v): %v`, err, string(aepr.RequestBodyAsBytes))
		aepr.ResponseStatusCode = http.StatusUnprocessableEntity
		return err
	}
	aepr.CurrentUser.ID = ""
	aepr.CurrentUser.Name = ""

	for _, v := range aepr.EndPoint.Parameters {
		rpv := aepr.NewAPIEndPointRequestParameter(v)
		aepr.ParameterValues[v.NameId] = rpv
		err := rpv.SetRawValue(bodyAsJSON[v.NameId])
		if err != nil {
			aepr.Log.Errorf("`Error at processing parameter %s to string (%v)", v.NameId, err)
			aepr.ResponseStatusCode = http.StatusUnprocessableEntity
			return err
		}
		if rpv.Metadata.IsMustExist {
			if rpv.RawValue == nil {
				return aepr.responseSetStatusCodeAsErrorf(http.StatusUnprocessableEntity, `Mandatory parameter '%s' is not exist`, v.NameId)
			}
		}
		if rpv.RawValue != nil {
			if !rpv.Validate() {
				return aepr.responseSetStatusCodeAsErrorf(http.StatusUnprocessableEntity, `Parameter '%s' validation fail`, v.NameId)
			}
		}
	}
	return nil
}

func (aepr *DXAPIEndPointRequest) HTTPClientDo(method, url string, parameters utils.JSON, headers map[string]string) (response *http.Response, err error) {
	var client = &http.Client{}
	var request *http.Request
	effectiveUrl := url
	parametersInUrl := ""
	if method == "GET" {
		for k, v := range parameters {
			if parametersInUrl != "" {
				parametersInUrl = parametersInUrl + "&"
			}
			parametersInUrl = parametersInUrl + fmt.Sprintf("%s=%v", k, v)
		}
		effectiveUrl = url + "?" + parametersInUrl
		request, err = http.NewRequest(method, effectiveUrl, nil)
	} else {
		var parametersAsJSONString []byte
		parametersAsJSONString, err = json.Marshal(parameters)
		if err != nil {
			aepr.Log.Errorf("`Error at marshaling parameters to string (%v)", err)
			aepr.ResponseStatusCode = http.StatusInternalServerError
			return nil, err
		}
		request, err = http.NewRequest(method, effectiveUrl, bytes.NewBuffer(parametersAsJSONString))
	}
	if err != nil {
		aepr.Log.Errorf("`Error at creating new request (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return nil, err
	}
	if parameters != nil {
		request.Header.Set(`Content-Type`, "application/json")
	}
	request.Header.Set(`Cache-Control`, `no-cache`)
	for k, v := range headers {
		request.Header[k] = []string{v}
	}

	requestDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		aepr.Log.Errorf("Error in DumpRequest (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return nil, err
	}
	aepr.Log.Debugf("Send Request to %s:\n%s\n", effectiveUrl, string(requestDump))

	response, err = client.Do(request)
	if err != nil {
		aepr.Log.Errorf("Error in make HTTP request (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return nil, err
	}

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		aepr.Log.Errorf("Error in DumpResponse (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return response, err
	}
	aepr.Log.Debugf("Response :\n%s\n", string(responseDump))
	return response, nil
}

func (aepr *DXAPIEndPointRequest) HTTPClientDoBodyAsJSONString(method, url string, parametersAsJSONString string, headers map[string]string) (response *http.Response, err error) {
	var client = &http.Client{}
	var request *http.Request
	effectiveUrl := url

	request, err = http.NewRequest(method, effectiveUrl, bytes.NewBuffer([]byte(parametersAsJSONString)))

	if err != nil {
		aepr.Log.Errorf("`Error at creating new request (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return nil, err
	}
	request.Header.Set(`Content-Type`, "application/json")
	request.Header.Set(`Cache-Control`, `no-cache`)
	for k, v := range headers {
		request.Header[k] = []string{v}
	}

	requestDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		aepr.Log.Errorf("Error in DumpRequest (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return nil, err
	}
	aepr.Log.Debugf("Request :\n%s\n", string(requestDump))

	response, err = client.Do(request)
	if err != nil {
		aepr.Log.Errorf("Error in make HTTP request (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return nil, err
	}

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		aepr.Log.Errorf("Error in DumpResponse (%v)", err)
		aepr.ResponseStatusCode = http.StatusInternalServerError
		return response, err
	}
	aepr.Log.Debugf("Response :\n%s\n", string(responseDump))
	return response, nil
}

func (aepr *DXAPIEndPointRequest) HTTPClient(method, url string, parameters utils.JSON, headers map[string]string) (responseStatusCode int, responseAsJSON utils.JSON, err error) {
	responseStatusCode = 0
	r, err := aepr.HTTPClientDo(method, url, parameters, headers)
	if r != nil {
		responseStatusCode = r.StatusCode
	}
	if err != nil {
		return responseStatusCode, nil, err
	}
	if r.StatusCode != 200 {
		err = aepr.Log.ErrorAndCreateErrorf("response status code is not 200 (%v)", r.StatusCode)
		return responseStatusCode, nil, err
	}
	responseAsJSON, err = utilsHttp.ResponseBodyToJSON(r)
	if err != nil {
		aepr.Log.Errorf("Error in make HTTP request (%v)", err)
		return responseStatusCode, nil, err
	}

	vAsString, err := utilsJson.PrettyPrint(responseAsJSON)
	if err != nil {
		aepr.Log.Errorf("Error in make HTTP request (%v)", err)
		return responseStatusCode, nil, err
	}
	aepr.Log.Debugf("Response data=%s", vAsString)

	return responseStatusCode, responseAsJSON, nil
}

func (aepr *DXAPIEndPointRequest) HTTPClient2(method, url string, parameters utils.JSON, headers map[string]string) (_responseStatusCode int, responseAsJSON utils.JSON, err error) {
	r, err := aepr.HTTPClientDo(method, url, parameters, headers)
	if err != nil {
		aepr.ResponseSetStatusCodeError(400, `DIAL_ERROR`, err.Error())
		if r != nil {
			return r.StatusCode, nil, err
		} else {
			return 0, nil, err
		}
	}
	if r == nil {
		aepr.Log.PanicAndCreateErrorf("HTTPClient2-1", "r is nil")
		return 0, nil, err
	}
	responseBodyAsBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return r.StatusCode, nil, err
	}
	responseBodyAsString := string(responseBodyAsBytes)
	if r.StatusCode != 200 {
		responseStatusCodeAsString := fmt.Sprintf("%v", r.StatusCode)
		err := aepr.ResponseSetStatusCodeError(400, `PROXY_STATUS_`+responseStatusCodeAsString, responseBodyAsString)
		if err != nil {
			return 0, nil, err
		}
		return r.StatusCode, nil, err
	}

	responseAsJSON, err = utilsJson.StringToJSON(responseBodyAsString)
	if err != nil {
		aepr.Log.Warnf("Error in make HTTP request, cannot be converted to JSON (%v)", err)
		return r.StatusCode, nil, err
	}

	vAsString, err := utilsJson.PrettyPrint(responseAsJSON)
	if err != nil {
		aepr.Log.Panic("HTTPClient2-2: json2.PrettyPrint(responseAsJSON", err)
		return r.StatusCode, nil, err
	}
	aepr.Log.Debugf("Response data=%s", vAsString)

	return r.StatusCode, responseAsJSON, nil
}
