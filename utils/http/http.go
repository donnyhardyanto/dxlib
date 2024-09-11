package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"dxlib/utils"
)

type RequestContentType int

const (
	ContentTypeNone RequestContentType = iota
	ContentTypeApplicationOctetStream
	ContentTypeTextPlain
	ContentTypeApplicationJSON
	ContentTypeApplicationXWwwFormUrlEncoded
	ContentTypeMultiPartFormData
)

func (t RequestContentType) String() string {
	switch t {
	case ContentTypeApplicationJSON:
		return "application/json"
	case ContentTypeApplicationXWwwFormUrlEncoded:
		return "application/x-www-form-urlencoded"
	case ContentTypeMultiPartFormData:
		return "multipart/form-data"
	case ContentTypeTextPlain:
		return "text/plain"
	case ContentTypeApplicationOctetStream: // Map to application/octet-stream
		return "application/octet-stream"
	case ContentTypeNone:
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
