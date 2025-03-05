package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
)

type HTTPHeader = map[string]string

type HTTPResponse struct {
	StatusCode int
	Body       []byte
	Headers    map[string][]string
}

func (hr *HTTPResponse) BodyAsString() string {
	return string(hr.Body)
}

func (hr *HTTPResponse) BodyAsJSON() (map[string]any, error) {
	var v map[string]any
	err := json.Unmarshal(hr.Body, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func HTTPClient(method string, url string, headers map[string]string, body any) (request *http.Request, response *http.Response, err error) {
	var bodyAsBytes []byte
	contentType := ``

	switch body.(type) {
	case string:
		bodyAsBytes = []byte(body.(string))
		break
	case []byte:
		bodyAsBytes = body.([]byte)
		break
	case map[string]any:
		bodyAsBytes, err = json.Marshal(body)
		if err != nil {
			return nil, nil, err
		}
		contentType = `application/json`
		break
	default:
		err = errors.New(fmt.Sprintf(`SHOULD_NOT_HAPPEN:TYPE_CANT_BE_CONVERTED_TO_BYTES:%v`, body))
		return nil, nil, err
	}

	request, err = http.NewRequest(method, url, bytes.NewBuffer(bodyAsBytes))
	if err != nil {
		return nil, nil, err
	}

	if contentType != `` {
		request.Header.Set("Content-Type", contentType)
	}
	request.Header.Set("Content-Length", fmt.Sprint(len(bodyAsBytes)))

	// Set request headers
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	// RequestCreate an HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	return request, resp, nil
}

func HTTPClientReadAll(method string, url string, headers map[string]string, body any) (request *http.Request, response *HTTPResponse, err error) {
	request, resp, err := HTTPClient(method, url, headers, body)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err2 := resp.Body.Close()
		if err2 != nil {
			log.Println(err2)
		}
	}()

	// RequestRead the response body
	responseBodyAsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	response = &HTTPResponse{
		StatusCode: resp.StatusCode,
		Body:       responseBodyAsBytes,
		Headers:    resp.Header,
	}
	return request, response, nil
}
