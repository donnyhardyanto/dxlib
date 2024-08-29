package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"

	"dxlib/v3/utils"
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

func GetRequestBodyStream(c *fasthttp.RequestCtx) (io.Reader, error) {
	s := c.Request.BodyStream()
	if s == nil {
		// Fallback to using the body as an io.Reader => because it was FastHTTP, it doesn't have BodyStream because it was not design for streaming
		body := c.Request.Body()
		if body == nil {
			return nil, errors.New("BAD_REQUEST_BODY_NIL")
		}
		s = io.NopCloser(bytes.NewReader(body))
	}
	return s, nil
}
