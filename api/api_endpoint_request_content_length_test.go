package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// chunkedBody hides its length, so the request carries no Content-Length and
// only the read itself can enforce the ceiling.
type chunkedBody struct{ io.Reader }

func newChunkedRequest(contentType, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/probe", chunkedBody{strings.NewReader(body)})
	r.ContentLength = -1
	r.Header.Set("Content-Type", contentType)
	return r
}

func TestJSONBodyWithoutContentLengthIsCappedAtTheEndpointLimit(t *testing.T) {
	r := newChunkedRequest("application/json", `{"note":"`+strings.Repeat("x", 200)+`"}`)
	aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
		Method:                  http.MethodPost,
		EndPointType:            EndPointTypeHTTPJSON,
		RequestContentType:      utilsHttp.RequestContentTypeApplicationJSON,
		RequestMaxContentLength: 64,
	})

	if err := aepr.PreProcessRequest(); err == nil {
		t.Fatal("oversize body accepted")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestOctetStreamBodyWithoutContentLengthIsCappedAtTheEndpointLimit(t *testing.T) {
	r := newChunkedRequest("application/octet-stream", strings.Repeat("x", 200))
	aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
		Method:                  http.MethodPost,
		EndPointType:            EndPointTypeHTTPJSON,
		RequestContentType:      utilsHttp.RequestContentTypeApplicationOctetStream,
		RequestMaxContentLength: 64,
	})

	if err := aepr.PreProcessRequest(); err == nil {
		t.Fatal("oversize body accepted")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestJSONBodyWithinTheEndpointLimitStillPasses(t *testing.T) {
	r := newChunkedRequest("application/json", `{"note":"ok"}`)
	aepr, _ := newPreProcessContext(r, &DXAPIEndPoint{
		Method:                  http.MethodPost,
		EndPointType:            EndPointTypeHTTPJSON,
		RequestContentType:      utilsHttp.RequestContentTypeApplicationJSON,
		RequestMaxContentLength: 64,
	})

	if err := aepr.PreProcessRequest(); err != nil {
		t.Fatalf("request rejected: %v", err)
	}
}
