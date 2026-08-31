package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

func (aepr *DXAPIEndPointRequest) ProxyHTTPAPIClient(method string, url string, bodyParameterAsJSON utils.JSON, headers map[string]string) (statusCode int, r utils.JSON, err error) {
	statusCode, r, err = aepr.HTTPClient(method, url, bodyParameterAsJSON, headers)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "", "PROXY_HTTP_API_CLIENT_ERROR:%v", err.Error())
		return statusCode, r, err
	}
	if (200 <= statusCode) && (statusCode < 300) {
		s := ""
		if r != nil {
			s, _ = r["code"].(string)
		}
		err = aepr.WriteResponseAndNewErrorf(statusCode, "", "INVALID_PROXY_RESPONSE:%d %s", statusCode, s)
	}
	return statusCode, r, err
}

// HTTPClientDoTimeout bounds one outbound exchange made by HTTPClientDo and
// HTTPClientDoBodyAsJSONString: the dial, the request, the response, and the
// body read that DumpResponse performs before returning.
//
// The zero value means no timeout, which is what every release up to and
// including v1.118.2 did -- so this changes nothing for a consumer that does not
// set it. A consumer that wants its proxy calls bounded assigns this once at
// start-up, before any request is served; it is not safe to change while
// requests are in flight.
//
// Without a bound, a downstream service that accepts the connection and never
// answers holds the calling handler's goroutine and its socket open for ever.
// The server's own WriteTimeout drops the *caller's* connection but does not
// unwind the handler, so every retry against a wedged downstream stacks another
// leaked goroutine.
//
// Keep any value below the server's WriteTimeoutSec (default 300), or the
// deadline passes before the 502 above can be written and the caller gets a torn
// connection instead of an answer.
var HTTPClientDoTimeout time.Duration

func (aepr *DXAPIEndPointRequest) HTTPClientDo(method, url string, parameters utils.JSON, headers map[string]string) (response *http.Response, err error) {
	var client = &http.Client{Timeout: HTTPClientDoTimeout}
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
			err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "SHOULD_NOT_HAPPEN:ERROR_MARSHALLING_PARAMETER_TO_STRING:%v", err.Error())
			return nil, err
		}
		request, err = http.NewRequest(method, effectiveUrl, bytes.NewBuffer(parametersAsJSONString))
	}
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_AT_CREATING_NEW_REQUEST:%v", err.Error())
		return nil, err
	}
	if parameters != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Cache-Control", "no-cache")
	for k, v := range headers {
		request.Header[k] = []string{v}
	}

	requestDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_IN_DUMP_REQUEST:%v", err.Error())
		return nil, err
	}
	aepr.Log.Debugf("Send Request to %s:\n%s\n", effectiveUrl, string(requestDump))

	response, err = client.Do(request)
	if err != nil {
		// 502, not 422. This is the exchange with the downstream service failing
		// -- refused, reset, or timed out -- not a caller sending something
		// unprocessable, and 422 sent an operator looking at the request body.
		//
		// The label was copied from the DumpRequest branch above; the one the
		// author intended is in the sibling function below, which handles this
		// same line correctly.
		//
		// This also unblocks three callers that already meant 502 and could not
		// say so: WriteResponse* is first-writer-wins, so the 422 written here
		// suppressed HTTPClient2's DIAL_ERROR, ProxyHTTPAPIClient's
		// PROXY_HTTP_API_CLIENT_ERROR, and any guarded fallback a consumer wrote
		// around this call.
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "", "ERROR_IN_MAKE_HTTP_REQUEST:%v", err.Error())
		return nil, err
	}

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_IN_DUMP_RESPONSE:%v", err.Error())
		return response, err
	}
	aepr.Log.Debugf("Response :\n%s\n", string(responseDump))
	return response, nil
}

func (aepr *DXAPIEndPointRequest) HTTPClientDoBodyAsJSONString(method, url string, parametersAsJSONString string, headers map[string]string) (response *http.Response, err error) {
	var client = &http.Client{Timeout: HTTPClientDoTimeout}
	var request *http.Request
	effectiveUrl := url

	request, err = http.NewRequest(method, effectiveUrl, bytes.NewBuffer([]byte(parametersAsJSONString)))

	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_AT_CREATING_NEW_REQUEST:%v", err.Error())
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cache-Control", "no-cache")
	for k, v := range headers {
		request.Header[k] = []string{v}
	}

	requestDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_IN_DUMP_REQUEST:%v", err.Error())
		return nil, err
	}
	aepr.Log.Debugf("Request :\n%s\n", string(requestDump))

	response, err = client.Do(request)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "", "ERROR_IN_MAKE_HTTP_REQUEST:%v", err.Error())
		return nil, err
	}

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "ERROR_IN_DUMP_RESPONSE:%v", err.Error())
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
		aepr.Log.Errorf(err, "Error in make HTTP request %+v", err)
		return responseStatusCode, nil, err
	}

	vAsString, err := utilsJson.PrettyPrint(responseAsJSON)
	if err != nil {
		aepr.Log.Errorf(err, "Error in make HTTP request %+v", err)
		return responseStatusCode, nil, err
	}
	aepr.Log.Debugf("Response data=%s", vAsString)

	return responseStatusCode, responseAsJSON, nil
}

func (aepr *DXAPIEndPointRequest) HTTPClient2(method, url string, parameters utils.JSON, headers map[string]string) (_responseStatusCode int, responseAsJSON utils.JSON, err error) {
	r, err := aepr.HTTPClientDo(method, url, parameters, headers)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway,
			"DIAL_ERROR",
			"HTTPCLIENT2-0:DIAL_ERROR:ERROR=%v", err.Error())
		if r != nil {
			return r.StatusCode, nil, err
		} else {
			return 0, nil, err
		}
	}
	if r == nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "", "HTTPCLIENT2-1:R_IS_NIL")
		return 0, nil, err
	}
	responseBodyAsBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return r.StatusCode, nil, err
	}
	responseBodyAsString := string(responseBodyAsBytes)
	if r.StatusCode != http.StatusOK {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "", "HTTPCLIENT2-0:PROXY_STATUS_%d", r.StatusCode)
		return r.StatusCode, nil, err
	}

	responseAsJSON, err = utils.StringToJSON(responseBodyAsString)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "", "HTTPCLIENT2-0:RESPONSE_BODY_CANNOT_CONVERT_TO_JSON:%v", err.Error())
		return r.StatusCode, nil, err
	}

	vAsString, err := utilsJson.PrettyPrint(responseAsJSON)
	if err != nil {
		err = aepr.WriteResponseAndNewErrorf(http.StatusBadGateway, "", "SHOULD_NOT_HAPPEN:HTTPCLIENT2-0:ERROR_IN_JSON_PRETTY_PRINT:%v", err.Error())
		return r.StatusCode, nil, err
	}
	aepr.Log.Debugf("Response data=%s", vAsString)

	return r.StatusCode, responseAsJSON, nil
}
