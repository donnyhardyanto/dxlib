package http

import (
	"encoding/json"
	"io"
	"net/http"

	"dxlib/v3/utils"
)

type RequestContentType int

const (
	ContentTypeNone RequestContentType = iota
	ContentTypeRaw
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
	case ContentTypeNone:
		return ""
	case ContentTypeRaw:
		return "raw"
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
