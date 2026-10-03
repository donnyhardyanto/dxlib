package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/log"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

func newPreProcessContext(r *http.Request, ep *DXAPIEndPoint) (*DXAPIEndPointRequest, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	var rw http.ResponseWriter = rec
	l := log.NewLog(nil, r.Context(), "preprocess-test")
	return &DXAPIEndPointRequest{
		Request:         r,
		ResponseWriter:  &rw,
		Log:             l,
		ParameterValues: map[string]*DXAPIEndPointRequestParameterValue{},
		EndPoint:        ep,
	}, rec
}

// An octet-stream endpoint with an optional parameter, called without an
// X-Var header, has no entry for that parameter; it must be skipped, not
// dereferenced.
func TestOctetStreamOptionalParameterWithoutXVar(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader("raw-bytes"))
	r.Header.Set("Content-Type", "application/octet-stream")
	aepr, _ := newPreProcessContext(r, &DXAPIEndPoint{
		Method:             http.MethodPost,
		EndPointType:       EndPointTypeHTTPJSON,
		RequestContentType: utilsHttp.RequestContentTypeApplicationOctetStream,
		Parameters: []DXAPIEndPointParameter{
			{NameId: "file_name", Type: dxlibTypes.APIParameterTypeString, IsMustExist: false},
		},
	})

	if err := aepr.PreProcessRequest(); err != nil {
		t.Fatalf("request rejected: %v", err)
	}
	if string(aepr.RequestBodyAsBytes) != "raw-bytes" {
		t.Fatalf("body = %q", aepr.RequestBodyAsBytes)
	}
}
