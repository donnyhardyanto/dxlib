package oam

import (
	"dxlib/v3/api"
	"io"
	"net/http"
)

func LogRequest(r *http.Request) (map[string]interface{}, error) {
	requestBodyAsBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
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
		`log_request`: lr,
	}
	return data, nil
}

func Ping(aepr *api.DXAPIEndPointRequest) (err error) {
	data, err := Echo(aepr.Request)
	aepr.Log.Infof("Receive (%v): %v", err, data)
	err = aepr.ResponseSetFromJSON(data)
	return err
}
