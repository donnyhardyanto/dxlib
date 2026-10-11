package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// A body that cannot be parsed as JSON must not be echoed back to the caller
// (nor kept in the returned error): it can carry passwords, tokens or personal
// data. Name the body by its size and content type instead.
func TestUnparsableJSONBodyIsNotEchoed(t *testing.T) {
	const secret = "password-hunter2-secret"
	r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(secret+" trailing garbage"))
	r.Header.Set("Content-Type", "application/json")
	aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
		Method:             http.MethodPost,
		EndPointType:       EndPointTypeHTTPJSON,
		RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
	})

	err := aepr.PreProcessRequest()
	if err == nil {
		t.Fatal("expected the unparsable body to be rejected")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("request body leaked into the returned error: %s", err.Error())
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatalf("request body leaked into the response: %s", rec.Body.String())
	}
}
