package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/donnyhardyanto/dxlib"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

type DXAPIUser struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type DXAPIEndPointRequest struct {
	Id                     string
	Context                context.Context
	EndPoint               *DXAPIEndPoint
	ParameterValues        map[string]*DXAPIEndPointRequestParameterValue
	Log                    log.DXLog
	Request                *http.Request
	RequestBodyAsBytes     []byte
	_responseWriter        *http.ResponseWriter
	_responseErrorAsString string
	_responseStatusCode    int
	//ResponseBodyAsBytes []byte
	ErrorMessage       []string
	CurrentUser        DXAPIUser
	LocalData          map[string]any
	ResponseHeaderSent bool
	ResponseBodySent   bool
}

func (aepr *DXAPIEndPointRequest) GetResponseWriter() *http.ResponseWriter {
	return aepr._responseWriter
}

func (aepr *DXAPIEndPointRequest) WriteResponseAndNewErrorf(statusCode int, msg string, data ...any) (err error) {
	err = aepr.Log.WarnAndCreateErrorf(msg, data)
	aepr.WriteResponseAsError(statusCode, err)
	return err
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsString(statusCode int, header map[string]string, s string) {
	aepr.WriteResponseAsBytes(statusCode, header, []byte(s))
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsError(statusCode int, errToSend error) {
	if aepr.ResponseHeaderSent {
		return
	}
	if (200 <= statusCode) && (statusCode < 300) {
		statusCode = 500
	}
	var s utils.JSON

	if dxlib.IsDebug {
		s = utils.JSON{
			"reason":         errToSend.Error(),
			"reason_message": errToSend.Error(),
		}
	}

	aepr.WriteResponseAsJSON(statusCode, nil, s)
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsJSON(statusCode int, header map[string]string, bodyAsJSON utils.JSON) {
	if aepr.ResponseHeaderSent {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_HEADER_ALREADY_SENT")
		return
	}
	var jsonBytes []byte
	var err error
	if bodyAsJSON != nil {
		jsonBytes, err = json.Marshal(bodyAsJSON)
		if err != nil {
			_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:ERROR_AT_MARSHAL_JSON=%s", err.Error())
			return
		}
		if header == nil {
			header = map[string]string{}
		}
		header["Content-Type"] = "application/json"
	}

	aepr.WriteResponseAsBytes(statusCode, header, jsonBytes)
	return
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsBytes(statusCode int, header map[string]string, bodyAsBytes []byte) {
	if aepr.ResponseHeaderSent {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_HEADER_ALREADY_SENT")
		return
	}
	responseWriter := *aepr.GetResponseWriter()
	for k, v := range header {
		responseWriter.Header().Set(k, v)
	}
	responseWriter.WriteHeader(statusCode)
	aepr._responseStatusCode = statusCode

	aepr.ResponseHeaderSent = true
	if aepr.ResponseBodySent {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_BODY_ALREADY_SENT")
		return
	}
	_, err := responseWriter.Write(bodyAsBytes)
	if err != nil {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:ERROR_AT_WRITE_RESPONSE=%s", err.Error())
		return
	}
	aepr.ResponseBodySent = true
	if statusCode != http.StatusOK {
		if bodyAsBytes != nil {
			aepr._responseErrorAsString = ""
		} else {
			aepr._responseErrorAsString = string(bodyAsBytes)
		}
	}
	return
}

func (aepr *DXAPIEndPointRequest) NewAPIEndPointRequestParameter(aepp DXAPIEndPointParameter) *DXAPIEndPointRequestParameterValue {
	aerp := DXAPIEndPointRequestParameterValue{Owner: aepr, Metadata: aepp}
	aepr.ParameterValues[aepp.NameId] = &aerp
	return &aerp
}

func (aepr *DXAPIEndPointRequest) GetParameterValueEntry(k string) (val *DXAPIEndPointRequestParameterValue, err error) {
	var ok bool
	if val, ok = aepr.ParameterValues[k]; !ok {
		err = aepr.Log.ErrorAndCreateErrorf(`REQUEST_FIELD_NOT_FOUND_IN_REQUEST:%s`, k)
		return nil, err
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
				err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `REQUEST_FIELD_VALUE_IS_NOT_EXIST:%s`, k)
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
						err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `PARAMETER_DEFAULT_VALUE_IS_NOT_STRING:%s=%v`, k, v1)
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
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `REQUEST_FIELD_VALUE_IS_NOT_STRING:%s=(%v)`, k, valAsAny)
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
					err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `PARAMETER_DEFAULT_VALUE_IS_NOT_STRING:%s=%v`, k, v1)
					return false, ``, err
				}
			}
		} else {
			return isExist, ``, nil
		}
		return isExist, val, err
	}
	v1, ok := valAsAny.(string)
	if !ok {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `REQUEST_FIELD_VALUE_IS_NOT_STRING:%s=(%v)`, k, valAsAny)
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
						err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `PARAMETER_DEFAULT_VALUE_IS_NOT_NULLABLE_INT64:%s=%v`, k, v1)
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
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `REQUEST_FIELD_VALUE_IS_NOT_NULLABLE_INT64:%s=(%v)`, k, valAsAny)
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
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `REQUEST_FIELD_VALUE_IS_NOT_TYPE:%s!=%T (%v)`, k, val, valAsAny)
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

func (aepr *DXAPIEndPointRequest) ProxyHTTPAPIClient(method string, url string, bodyParameterAsJSON utils.JSON, headers map[string]string) (statusCode int, r utils.JSON, err error) {
	statusCode, r, err = aepr.HTTPClient(method, url, bodyParameterAsJSON, headers)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "PROXY_HTTP_API_CLIENT_ERROR:%v", err.Error())
		return statusCode, r, err
	}
	if (200 <= statusCode) && (statusCode < 300) {
		s := ""
		if r != nil {
			s, _ = r["code"].(string)
		}
		err = aepr.WriteResponseAndNewErrorf(statusCode, "INVALID_PROXY_RESPONSE:%d %s", statusCode, s)
	}
	return statusCode, r, err
}

func (aepr *DXAPIEndPointRequest) PreProcessRequest() (err error) {
	if aepr.Request.Method != aepr.EndPoint.Method {
		if aepr.Request.Method == "OPTIONS" {
			aepr.WriteResponseAsBytes(http.StatusOK, nil, []byte(``))
			return nil
		}
		return aepr.WriteResponseAndNewErrorf(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED:%s!=%s", aepr.Request.Method, aepr.EndPoint.Method)
	}
	xVar := aepr.Request.Header.Get("X-Var")
	var xVarJSON map[string]interface{}
	if xVar != `` {
		err := json.Unmarshal([]byte(xVar), &xVarJSON)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "ERROR_PARSING_HEADER_X-VAR_AS_JSON: %v", err.Error())
		}
		for _, v := range aepr.EndPoint.Parameters {
			rpv := aepr.NewAPIEndPointRequestParameter(v)
			aepr.ParameterValues[v.NameId] = rpv
			err := rpv.SetRawValue(xVarJSON[v.NameId])
			if err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "`ERROR_PROCESSING_PARAMETER_TO_STRING:%s=(%v)", v.NameId, err.Error())
			}
		}
	}
	switch aepr.EndPoint.Method {
	case "GET", "DELETE":
		for _, v := range aepr.EndPoint.Parameters {
			rpv := aepr.NewAPIEndPointRequestParameter(v)
			aepr.ParameterValues[v.NameId] = rpv
			err := rpv.SetRawValue(aepr.Request.FormValue(v.NameId))
			if err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "ERROR_PROCESSING_PARAMETER_TO_STRING:%s=%v", v.NameId, err.Error())
			}
			if rpv.Metadata.IsMustExist {
				if rpv.RawValue == nil {
					if !rpv.Metadata.IsNullable {
						return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "MANDATORY_PARAMETER_NOT_EXIST:%s", v.NameId)
					}
				}
			}
			if rpv.RawValue != nil {
				if !rpv.Validate() {
					return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "PARAMETER_VALIDATION_FAIL:%s", v.NameId)
				}
			}
		}
	case "POST", "PUT":
		switch aepr.EndPoint.RequestContentType {
		case utilsHttp.ContentTypeApplicationOctetStream:
			for _, v := range aepr.EndPoint.Parameters {
				rpv, ok := aepr.ParameterValues[v.NameId]
				if v.IsMustExist {
					if !ok {
						return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "MANDATORY_PARAMETER_NOT_EXIST:%s", v.NameId)
					}
				}
				if rpv.RawValue != nil {
					if !rpv.Validate() {
						return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "PARAMETER_VALIDATION_FAIL:%s", v.NameId)
					}
				}
			}
			err = aepr.preProcessRequestAsApplicationOctetStream()
		case utilsHttp.ContentTypeApplicationJSON:
			err = aepr.preProcessRequestAsApplicationJSON()
		default:
			err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `Request content-type is not supported yet (%v)`, aepr.EndPoint.RequestContentType)
		}
	default:
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `Request method is not supported yet (%v)`, aepr.EndPoint.Method)
	}
	return err
}

func (aepr *DXAPIEndPointRequest) preProcessRequestAsApplicationOctetStream() (err error) {
	switch aepr.EndPoint.EndPointType {
	case EndPointTypeHTTPUploadStream:
		return nil
	default:
		aepr.RequestBodyAsBytes, err = io.ReadAll(aepr.Request.Body)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "ERROR_READING_REQUEST_BODY: %v", err.Error())
		}
	}
	return nil
}

func (aepr *DXAPIEndPointRequest) preProcessRequestAsApplicationJSON() (err error) {
	actualContentType := aepr.Request.Header.Get("Content-Type")
	if actualContentType != "" {
		if !strings.Contains(actualContentType, "application/json") {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `REQUEST_CONTENT_TYPE_IS_NOT_APPLICATION_JSON: %s`, actualContentType)
		}
	}
	bodyAsJSON := utils.JSON{}
	aepr.RequestBodyAsBytes, err = io.ReadAll(aepr.Request.Body)
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `REQUEST_BODY_CANT_BE_READ:%v=%v`, err.Error(), aepr.RequestBodyAsBytes)
	}

	if len(aepr.RequestBodyAsBytes) > 0 {
		err = json.Unmarshal(aepr.RequestBodyAsBytes, &bodyAsJSON)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `REQUEST_BODY_CANT_BE_PARSED_AS_JSON:%v`, err.Error()+"="+string(aepr.RequestBodyAsBytes))
		}
	}
	aepr.CurrentUser.Id = ""
	aepr.CurrentUser.Name = ""

	for _, v := range aepr.EndPoint.Parameters {
		rpv := aepr.NewAPIEndPointRequestParameter(v)
		aepr.ParameterValues[v.NameId] = rpv
		err := rpv.SetRawValue(bodyAsJSON[v.NameId])
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_AT_PROCESSING_PARAMETER_TO_STRING:%s=%v`, v.NameId, err.Error())
		}
		if rpv.Metadata.IsMustExist {
			if rpv.RawValue == nil {
				if !rpv.Metadata.IsNullable {
					return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `MANDATORY_PARAMETER_IS_NOT_EXIST:%s`, v.NameId)
				}
			}
		}
		if rpv.RawValue != nil {
			if !rpv.Validate() {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `PARAMETER_VALIDATION_FAIL:%s`, v.NameId)
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
			err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `SHOULD_NOT_HAPPEN:ERROR_MARSHALLING_PARAMETER_TO_STRING:%v`, err.Error())
			return nil, err
		}
		request, err = http.NewRequest(method, effectiveUrl, bytes.NewBuffer(parametersAsJSONString))
	}
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_AT_CREATING_NEW_REQUEST:%v`, err.Error())
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
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_IN_DUMP_REQUEST:%v`, err.Error())
		return nil, err
	}
	aepr.Log.Debugf("Send Request to %s:\n%s\n", effectiveUrl, string(requestDump))

	response, err = client.Do(request)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_IN_DUMP_REQUEST:%v`, err.Error())
		return nil, err
	}

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_IN_DUMP_RESPONSE:%v`, err.Error())
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
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_AT_CREATING_NEW_REQUEST:%v`, err.Error())
		return nil, err
	}
	request.Header.Set(`Content-Type`, "application/json")
	request.Header.Set(`Cache-Control`, `no-cache`)
	for k, v := range headers {
		request.Header[k] = []string{v}
	}

	requestDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "ERROR_IN_DUMP_REQUEST:%v", err.Error())
		return nil, err
	}
	aepr.Log.Debugf("Request :\n%s\n", string(requestDump))

	response, err = client.Do(request)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_IN_MAKE_HTTP_REQUEST:%v`, err.Error())
		return nil, err
	}

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, `ERROR_IN_DUMP_RESPONSE:%v`, err.Error())
		return response, err
	}
	aepr.Log.Debugf("Response :\n%s\n", string(responseDump))
	return response, nil
}

func (aepr *DXAPIEndPointRequest) HTTPClient(method, url string, parameters utils.JSON, headers map[string]string) (responseStatusCode int, responseAsJSON utils.JSON, err error) {
	responseStatusCode = 0
	r, err := aepr.HTTPClientDo(method, url, parameters, headers)
	if err != nil {
		return responseStatusCode, nil, err
	}
	if r == nil {
		err = aepr.Log.PanicAndCreateErrorf("HTTPClient: r is nil", "")
		return responseStatusCode, nil, err
	}

	responseStatusCode = r.StatusCode

	if r.StatusCode != http.StatusOK {
		err = aepr.Log.ErrorAndCreateErrorf("response status code is not 200 (%v)", r.StatusCode)
		return responseStatusCode, nil, err
	}
	responseAsJSON, err = utilsHttp.ResponseBodyToJSON(r)
	if err != nil {
		aepr.Log.Errorf("Error in make HTTP request (%v)", err.Error())
		return responseStatusCode, nil, err
	}

	vAsString, err := utilsJson.PrettyPrint(responseAsJSON)
	if err != nil {
		aepr.Log.Errorf("Error in make HTTP request (%v)", err.Error())
		return responseStatusCode, nil, err
	}
	aepr.Log.Debugf("Response data=%s", vAsString)

	return responseStatusCode, responseAsJSON, nil
}

func (aepr *DXAPIEndPointRequest) HTTPClient2(method, url string, parameters utils.JSON, headers map[string]string) (_responseStatusCode int, responseAsJSON utils.JSON, err error) {
	r, err := aepr.HTTPClientDo(method, url, parameters, headers)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "HTTPCLIENT2-0:DIAL_ERROR:%v", err.Error())
		if r != nil {
			return r.StatusCode, nil, err
		} else {
			return 0, nil, err
		}
	}
	if r == nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "HTTPCLIENT2-1:R_IS_NIL")
		return 0, nil, err
	}
	responseBodyAsBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return r.StatusCode, nil, err
	}
	responseBodyAsString := string(responseBodyAsBytes)
	if r.StatusCode != http.StatusOK {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "HTTPCLIENT2-0:PROXY_STATUS_%d", r.StatusCode)
		return r.StatusCode, nil, err
	}

	responseAsJSON, err = utils.StringToJSON(responseBodyAsString)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "HTTPCLIENT2-0:RESPONSE_BODY_CANNOT_CONVERT_TO_JSON:%v", err.Error())
		return r.StatusCode, nil, err
	}

	vAsString, err := utilsJson.PrettyPrint(responseAsJSON)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "SHOULD_NOT_HAPPEN:HTTPCLIENT2-0:ERROR_IN_JSON_PRETTY_PRINT:%v", err.Error())
		return r.StatusCode, nil, err
	}
	aepr.Log.Debugf("Response data=%s", vAsString)

	return r.StatusCode, responseAsJSON, nil
}
