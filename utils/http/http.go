package http

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/donnyhardyanto/dxlib/errors"

	"github.com/donnyhardyanto/dxlib/utils"
)

type ContentType string

const ContentTypeTextPlain ContentType = "text/plain"
const ContentTypeTextHTML ContentType = "text/html"

type RequestContentType int

const (
	RequestContentTypeNone RequestContentType = iota
	RequestContentTypeApplicationOctetStream
	RequestContentTypeTextPlain
	RequestContentTypeApplicationJSON
	RequestContentTypeApplicationXWwwFormUrlEncoded
	RequestContentTypeMultiPartFormData
)

func (t RequestContentType) String() string {
	switch t {
	case RequestContentTypeApplicationJSON:
		return "application/json"
	case RequestContentTypeApplicationXWwwFormUrlEncoded:
		return "application/x-www-form-urlencoded"
	case RequestContentTypeMultiPartFormData:
		return "multipart/form-data"
	case RequestContentTypeTextPlain:
		return "text/plain"
	case RequestContentTypeApplicationOctetStream: // Map to application/octet-stream
		return "application/octet-stream"
	case RequestContentTypeNone:
		return ""
	default:
		return ""
	}
}

func ResponseBodyToJSON(response *http.Response) (utils.JSON, error) {
	v := utils.JSON{}
	bodyAsBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return v, err
	}
	err = json.Unmarshal(bodyAsBytes, &v)
	if err != nil {
		return v, err
	}
	return v, nil
}

func GetRequestBodyStream(r *http.Request) (io.Reader, error) {
	if r.Body == nil {
		return nil, errors.New("BAD_REQUEST_BODY_NIL")
	}
	return r.Body, nil
}

func HeaderToJSON(h http.Header) (r utils.JSON) {
	r = utils.JSON{}
	for k, v := range h {
		r[k] = v
	}
	return r
}

func HeaderToMapStringString(h http.Header) (r map[string]string) {
	r = make(map[string]string, len(h))
	for key, values := range h {
		if len(values) > 0 {
			// Get only the first value
			r[key] = values[0]
		} else {
			r[key] = "" // Or skip the entry entirely
		}
	}
	return r
}
