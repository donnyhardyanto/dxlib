package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	"io"
	"net/http"
	"strings"
)

type DXAPIUser struct {
	Id       string `json:"id"`
	Uid      string `json:"uid"`
	LoginId  string `json:"loginid"`
	FullName string `json:"fullname"`
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
	ResponseStatusCode     int
	//ResponseBodyAsBytes []byte
	ErrorMessage       []string
	CurrentUser        DXAPIUser
	LocalData          map[string]any
	ResponseHeaderSent bool
	ResponseBodySent   bool
	SuppressLogDump    bool
}

func (aepr *DXAPIEndPointRequest) GetParameterValues() (r utils.JSON) {
	r = utils.JSON{}
	for k, v := range aepr.ParameterValues {
		r[k] = v.Value
	}
	return r
}

func (aepr *DXAPIEndPointRequest) RequestDump() ([]byte, error) {
	var b bytes.Buffer

	// By default, print out the unmodified req.RequestURI, which
	// is always set for incoming server requests. But because we
	// previously used req.URL.RequestURI and the docs weren't
	// always so clear about when to use DumpRequest vs
	// DumpRequestOut, fall back to the old way if the caller
	// provides a non-server Request.
	req := aepr.Request
	reqURI := req.RequestURI
	if reqURI == "" {
		reqURI = req.URL.RequestURI()
	}

	_, _ = fmt.Fprintf(&b, "%s %s HTTP/%d.%d\r\n", req.Method, reqURI, req.ProtoMajor, req.ProtoMinor)

	absRequestURI := strings.HasPrefix(reqURI, "http://") || strings.HasPrefix(reqURI, "https://")
	if !absRequestURI {
		host := req.Host
		if host == "" && req.URL != nil {
			host = req.URL.Host
		}
		if host != "" {
			_, _ = fmt.Fprintf(&b, "Host: %s\r\n", host)
		}
	}

	if len(req.TransferEncoding) > 0 {
		_, _ = fmt.Fprintf(&b, "Transfer-Encoding: %s\r\n", strings.Join(req.TransferEncoding, ","))
	}

	var reqWriteExcludeHeaderDump = map[string]bool{
		"Host":              true, // not in Header map anyway
		"Transfer-Encoding": true,
		"Trailer":           true,
	}

	err := req.Header.WriteSubset(&b, reqWriteExcludeHeaderDump)
	if err != nil {
		return nil, err
	}

	_, _ = io.WriteString(&b, "\r\n")
	b.Write(aepr.RequestBodyAsBytes)
	_, _ = io.WriteString(&b, "\r\n\r\n")

	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (aepr *DXAPIEndPointRequest) GetResponseWriter() *http.ResponseWriter {
	return aepr._responseWriter
}

func (aepr *DXAPIEndPointRequest) WriteResponseAndNewErrorf(statusCode int, responseMessage string, msg string, data ...any) (err error) {
	err = aepr.Log.WarnAndCreateErrorf(msg, data...)
	if responseMessage == "" {
		responseMessage = strings.ToUpper(http.StatusText(statusCode))
	}
	aepr.WriteResponseAsErrorMessage(statusCode, responseMessage)
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

	//	if dxlib.IsDebug {
	s = utils.JSON{
		"status":         http.StatusText(statusCode),
		"status_code":    statusCode,
		"reason":         errToSend.Error(),
		"reason_message": errToSend.Error(),
	}
	//	}

	aepr.WriteResponseAsJSON(statusCode, nil, s)
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsErrorMessage(statusCode int, errorMsg string) {
	if aepr.ResponseHeaderSent {
		return
	}
	if (200 <= statusCode) && (statusCode < 300) {
		statusCode = 500
	}
	var s utils.JSON

	//	if dxlib.IsDebug {
	s = utils.JSON{
		"status":         http.StatusText(statusCode),
		"status_code":    statusCode,
		"reason":         errorMsg,
		"reason_message": "",
	}
	//	}

	aepr.WriteResponseAsJSON(statusCode, nil, s)
}

func (aepr *DXAPIEndPointRequest) WriteResponseAsJSON(statusCode int, header map[string]string, bodyAsJSON utils.JSON) {
	if aepr.ResponseHeaderSent {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:RESPONSE_HEADER_ALREADY_SENT")
		return
	}
	var jsonBytes []byte
	var err error
	if bodyAsJSON == nil {
		bodyAsJSON = utils.JSON{}
	}
	if bodyAsJSON["status"] == nil {
		bodyAsJSON["status"] = http.StatusText(statusCode)
	}
	if bodyAsJSON["status_code"] == nil {
		bodyAsJSON["status_code"] = statusCode
	}
	if bodyAsJSON["reason"] == nil {
		if statusCode == 200 {
			bodyAsJSON["reason"] = "OK"
		} else {
			bodyAsJSON["reason"] = http.StatusText(statusCode)
		}
	}
	if bodyAsJSON["reason_message"] == nil {
		if statusCode == 200 {
			bodyAsJSON["reason_message"] = "OK"
		} else {
			bodyAsJSON["reason_message"] = http.StatusText(statusCode)
		}
	}

	jsonBytes, err = json.Marshal(bodyAsJSON)
	if err != nil {
		_ = aepr.Log.WarnAndCreateErrorf("SHOULD_NOT_HAPPEN:ERROR_AT_MARSHAL_JSON=%s", err.Error())
		return
	}
	if header == nil {
		header = map[string]string{}
	}
	header["Content-Type"] = "application/json"
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
	aepr.ResponseStatusCode = statusCode

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

func (aepr *DXAPIEndPointRequest) PreProcessRequest() (err error) {
	if aepr.EndPoint.RequestMaxContentLength > 0 {
		if aepr.Request.ContentLength > aepr.EndPoint.RequestMaxContentLength {
			return aepr.WriteResponseAndNewErrorf(http.StatusRequestEntityTooLarge, "", "REQUEST_MAX_CONTENT_LENGTH_EXCEEDED:%d<%d", aepr.EndPoint.RequestMaxContentLength, aepr.Request.ContentLength)
		}
	}
	aepr.ParameterValues = map[string]*DXAPIEndPointRequestParameterValue{}
	aepr.CurrentUser = DXAPIUser{}
	aepr.LocalData = map[string]any{}
	aepr.ErrorMessage = []string{}
	aepr.ResponseHeaderSent = false
	aepr.ResponseBodySent = false
	aepr.RequestBodyAsBytes = nil
	if aepr.Request.Method != aepr.EndPoint.Method {
		if aepr.Request.Method == "OPTIONS" {
			aepr.WriteResponseAsBytes(http.StatusOK, nil, []byte(``))
			return nil
		}
		return aepr.WriteResponseAndNewErrorf(http.StatusMethodNotAllowed, "", "METHOD_NOT_ALLOWED:%s!=%s", aepr.Request.Method, aepr.EndPoint.Method)
	}
	xVar := aepr.Request.Header.Get("X-Var")
	var xVarJSON map[string]interface{}
	if xVar != `` {
		err := json.Unmarshal([]byte(xVar), &xVarJSON)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_PARSING_HEADER_X-VAR_AS_JSON: %v", err.Error())
		}
		for _, v := range aepr.EndPoint.Parameters {
			rpv := aepr.NewAPIEndPointRequestParameter(v)
			aepr.ParameterValues[v.NameId] = rpv
			variablePath := v.NameId
			err := rpv.SetRawValue(xVarJSON[v.NameId], variablePath)
			if err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", err.Error())
			}
		}
	}
	switch aepr.EndPoint.Method {
	case "GET", "DELETE":
		for _, v := range aepr.EndPoint.Parameters {
			rpv := aepr.NewAPIEndPointRequestParameter(v)
			aepr.ParameterValues[v.NameId] = rpv
			variablePath := v.NameId
			err := rpv.SetRawValue(aepr.Request.FormValue(v.NameId), variablePath)
			if err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", err.Error())
			}
			if rpv.Metadata.IsMustExist {
				if rpv.RawValue == nil {
					if !rpv.Metadata.IsNullable {
						return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "MANDATORY_PARAMETER_NOT_EXIST:%s", variablePath)
					}
				}
			}
			if rpv.RawValue != nil {
				err = rpv.Validate()
				if err != nil {
					aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
					return err
				}
			}
		}
	case "POST", "PUT":
		switch aepr.EndPoint.RequestContentType {
		case utilsHttp.ContentTypeApplicationOctetStream:
			for _, v := range aepr.EndPoint.Parameters {
				rpv, ok := aepr.ParameterValues[v.NameId]
				variablePath := v.NameId
				if v.IsMustExist {
					if !ok {
						return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "MANDATORY_PARAMETER_NOT_EXIST:%s", variablePath)
					}
				}
				if rpv.RawValue != nil {
					err = rpv.Validate()
					if err != nil {
						aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
						return err
					}
				}
			}
			err = aepr.preProcessRequestAsApplicationOctetStream()
		case utilsHttp.ContentTypeApplicationJSON:
			err = aepr.preProcessRequestAsApplicationJSON()
		default:
			err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", `Request content-type is not supported yet (%v)`, aepr.EndPoint.RequestContentType)
		}
	default:
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", `Request method is not supported yet (%v)`, aepr.EndPoint.Method)
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
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_READING_REQUEST_BODY: %v", err.Error())
		}
	}
	return nil
}

func (aepr *DXAPIEndPointRequest) preProcessRequestAsApplicationJSON() (err error) {
	actualContentType := aepr.Request.Header.Get("Content-Type")
	if actualContentType != "" {
		if !strings.Contains(actualContentType, "application/json") {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", `REQUEST_CONTENT_TYPE_IS_NOT_APPLICATION_JSON: %s`, actualContentType)
		}
	}
	bodyAsJSON := utils.JSON{}
	aepr.RequestBodyAsBytes, err = io.ReadAll(aepr.Request.Body)
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", `REQUEST_BODY_CANT_BE_READ:%v=%v`, err.Error(), aepr.RequestBodyAsBytes)
	}

	if len(aepr.RequestBodyAsBytes) > 0 {
		err = json.Unmarshal(aepr.RequestBodyAsBytes, &bodyAsJSON)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", `REQUEST_BODY_CANT_BE_PARSED_AS_JSON:%v`, err.Error()+"="+string(aepr.RequestBodyAsBytes))
		}
	}

	for _, v := range aepr.EndPoint.Parameters {
		rpv := aepr.NewAPIEndPointRequestParameter(v)
		aepr.ParameterValues[v.NameId] = rpv
		variablePath := v.NameId
		err := rpv.SetRawValue(bodyAsJSON[v.NameId], variablePath)
		if err != nil {
			return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", err.Error())
		}
		if rpv.Metadata.IsMustExist {
			if rpv.RawValue == nil {
				if !rpv.Metadata.IsNullable {
					return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", `MANDATORY_PARAMETER_IS_NOT_EXIST:%s`, variablePath)
				}
			}
		}
		if rpv.RawValue != nil {
			err = rpv.Validate()
			if err != nil {
				aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
				return err
			}
		}
	}
	return nil
}
