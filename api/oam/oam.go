package oam

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// LogRequest reads r into a map of the request as received: the headers, the query, the request
// URI and the body as sent, nothing masked. It is the input of MaskedRequest and RequestForLog
// and must not be written to a log or answered to a caller as it is; EchoHandler does so only
// under echo-raw-request.
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

// MaskedRequest is the form of a LogRequest map that may leave the process in an answer. Each
// header name holds the list of its values, every value through utils.MaskHeaderValueForAnswer,
// so a credential header (Authorization, Cookie, an API key, a token header the host declared
// or whose name says what it carries) shows as "***REDACTED***" under its name; the query and a
// body that is a JSON object go through utils.MaskForAnswer, which masks by field name under
// the host's rules; any other body is shown by its length only; and the request URI is cut to
// its path, so the query string does not ride along unmasked. The debug override
// (dxlib.IsDebug with utils.OverrideShowPasswordOnLog) does not reach it: it is a switch for
// an operator reading their own log, and the log line RequestForLog writes is the one place it
// shows these values raw. The map it is given is not changed.
func MaskedRequest(lr map[string]interface{}) utils.JSON {
	return maskedRequest(lr, forAnswer)
}

// RequestForLog renders the request as one JSON line for a log: the shape of MaskedRequest,
// masked through utils.MaskHeaderValue and utils.MaskForLog, so the debug override shows the
// values raw here as it does in every log line.
func RequestForLog(lr map[string]interface{}) string {
	s, err := utils.JSONToString(maskedRequest(lr, forLog))
	if err != nil {
		return fmt.Sprintf("REQUEST_FOR_LOG_MARSHAL_ERROR: %v", err)
	}
	return s
}

// masker is the pair of mask functions a rendering of the request goes through: one for a
// header value, one for the query and the body.
type masker struct {
	header func(name, value string) string
	data   func(utils.JSON) utils.JSON
}

var (
	// forAnswer never consults the debug override: what a caller is answered.
	forAnswer = masker{header: utils.MaskHeaderValueForAnswer, data: utils.MaskForAnswer}
	// forLog honours the debug override, as every log line does.
	forLog = masker{header: utils.MaskHeaderValue, data: utils.MaskForLog}
)

// maskedRequest builds the masked form of lr through m.
func maskedRequest(lr map[string]interface{}, m masker) utils.JSON {
	header, _ := lr["header"].(http.Header)
	query, _ := lr["query"].(url.Values)
	body, _ := lr["body"].([]byte)
	requestURI, _ := lr["request_uri"].(string)
	return utils.JSON{
		"remote_addr": lr["remote_addr"],
		"method":      lr["method"],
		"host":        lr["host"],
		"path":        strings.SplitN(requestURI, "?", 2)[0],
		"query":       m.data(valuesAsJSON(query)),
		"header":      headersMasked(header, m),
		"body":        bodyMasked(body, m),
	}
}

// headersMasked copies h into the shape of valuesAsJSON: each header name holds the list of
// its values, each through m.header.
func headersMasked(h http.Header, m masker) utils.JSON {
	out := make(utils.JSON, len(h))
	for name, values := range h {
		list := make([]any, len(values))
		for i, value := range values {
			list[i] = m.header(name, value)
		}
		out[name] = list
	}
	return out
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

// bodyMasked returns the body as a log line or an answer may carry it: a JSON object masked
// field by field through m.data, anything else (empty, an array, not JSON) by its length only.
func bodyMasked(body []byte, m masker) any {
	var object map[string]any
	if len(body) > 0 && json.Unmarshal(body, &object) == nil && object != nil {
		return m.data(object)
	}
	return fmt.Sprintf("(%d bytes, not a JSON object)", len(body))
}

// Echo returns the request as a caller may see it, {"log_request": MaskedRequest(lr)}, masked
// whatever the debug override says. The request as received, every header value and the body
// unmasked, is answered only by EchoHandler under echo-raw-request.
func Echo(r *http.Request) (map[string]interface{}, error) {
	lr, err := LogRequest(r)
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{
		"log_request": MaskedRequest(lr),
	}
	return data, nil
}

// EchoHandler is the endpoint form of Echo: it logs the request as RequestForLog and answers
// {"log_request": MaskedRequest(lr)}. Only echo-raw-request on for its API
// (DXAPI.EchoRawRequest) makes it answer the request as received instead: every header value,
// the query, the request URI and the body (as base64 bytes), unmasked; the debug override does
// not. That is unsafe behind a proxy or gateway that adds headers on the way in (a signed
// internal token, gateway auth headers, X-Forwarded-*, routing headers), because the caller
// gets them back, and a script that can read the answer reads the Cookie header, HttpOnly or
// not. It is off by default.
func EchoHandler(aepr *api.DXAPIEndPointRequest) (err error) {
	lr, err := LogRequest(aepr.Request)
	if err != nil {
		return err
	}
	aepr.Log.Infof("Receive: %s", RequestForLog(lr))
	var echo any = MaskedRequest(lr)
	if echoRawRequest(aepr) {
		echo = lr
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"log_request": echo})
	return nil
}

// echoRawRequest answers whether the request's API echoes the request as received
// (DXAPI.EchoRawRequest).
func echoRawRequest(aepr *api.DXAPIEndPointRequest) bool {
	return aepr.EndPoint != nil && aepr.EndPoint.Owner != nil && aepr.EndPoint.Owner.EchoRawRequest
}

// Ping answers that the service is up: the envelope's status ("OK"), the service's name as
// app.Set gave it (log.Log.Prefix) and the time, UTC in RFC 3339. It logs the request as
// RequestForLog and echoes none of it: a liveness answer is read by whoever can reach the
// endpoint, and a request echo would hand that reader every header a proxy or gateway added
// on the way in, and Authorization and Cookie with them. EchoHandler is the endpoint that
// echoes.
func Ping(aepr *api.DXAPIEndPointRequest) (err error) {
	lr, err := LogRequest(aepr.Request)
	if err != nil {
		return err
	}
	aepr.Log.Infof("Receive: %s", RequestForLog(lr))
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		"service": log.Log.Prefix,
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
	return nil
}
