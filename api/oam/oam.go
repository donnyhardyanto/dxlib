package oam

import (
	"io"
	"net/http"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/errors"
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

func Ping(aepr *api.DXAPIEndPointRequest) (err error) {
	data, err := Echo(aepr.Request)
	if err != nil {
		return err
	}
	aepr.Log.Infof("Receive (%v): %v", err, data)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)
	return nil
}
