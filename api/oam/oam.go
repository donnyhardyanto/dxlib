package oam

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

func LogRequest(r *http.Request) (map[string]interface{}, error) {
	requestBodyAsBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errors.Wrap(err, "OAM_LOG_REQUEST_READ_BODY_ERROR")
	}
	lr := map[string]interface{}{
		"remote_addr": r.RemoteAddr,
		"method":      r.Method,
		"host":        r.Host,
		"request_uri": r.RequestURI,
		"query":       r.URL.Query(),
		"header":      r.Header,
		"body":        requestBodyAsBytes,
	}
	return lr, nil
}

func Echo(r *http.Request) (map[string]interface{}, error) {
	lr, err := LogRequest(r)
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{
		"log_request": lr,
	}
	return data, nil
}

// RequestForLog is the one form of a LogRequest map that may be written to a log. The headers
// are rendered through utils.WriteHeadersForLog, so a credential header shows as
// "***REDACTED***"; the query and a body that is a JSON object go through utils.MaskForLog,
// which masks by field name in the house format; any other body is shown by its length only;
// and the request URI is cut to its path, so the query string does not ride along unmasked.
// The map it is given, the echo a caller gets back, is not changed.
func RequestForLog(lr map[string]interface{}) string {
	header, _ := lr["header"].(http.Header)
	query, _ := lr["query"].(url.Values)
	body, _ := lr["body"].([]byte)
	requestURI, _ := lr["request_uri"].(string)
	masked := utils.JSON{
		"remote_addr": lr["remote_addr"],
		"method":      lr["method"],
		"host":        lr["host"],
		"path":        strings.SplitN(requestURI, "?", 2)[0],
		"query":       utils.MaskForLog(valuesAsJSON(query)),
		"header":      utils.WriteHeadersForLog(header, nil),
		"body":        bodyForLog(body),
	}
	s, err := utils.JSONToString(masked)
	if err != nil {
		return fmt.Sprintf("REQUEST_FOR_LOG_MARSHAL_ERROR: %v", err)
	}
	return s
}

// valuesAsJSON copies url.Values into the shape MaskForLog walks: each parameter name holds
// the list of its values, so the mask rule for the name applies to every value.
func valuesAsJSON(v url.Values) utils.JSON {
	out := make(utils.JSON, len(v))
	for name, values := range v {
		list := make([]any, len(values))
		for i, value := range values {
			list[i] = value
		}
		out[name] = list
	}
	return out
}

// bodyForLog returns the body as a log may carry it: a JSON object masked field by field,
// anything else (empty, an array, not JSON) by its length only.
func bodyForLog(body []byte) any {
	var object map[string]any
	if len(body) > 0 && json.Unmarshal(body, &object) == nil && object != nil {
		return utils.MaskForLog(object)
	}
	return fmt.Sprintf("(%d bytes, not a JSON object)", len(body))
}

func Ping(aepr *api.DXAPIEndPointRequest) (err error) {
	lr, err := LogRequest(aepr.Request)
	if err != nil {
		return err
	}
	aepr.Log.Infof("Receive: %s", RequestForLog(lr))
	aepr.WriteResponseAsJSON(http.StatusOK, nil, map[string]interface{}{"log_request": lr})
	return nil
}
