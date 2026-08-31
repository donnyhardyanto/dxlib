package api

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/donnyhardyanto/dxlib/log"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

func newMultipartRequest(t *testing.T, target string, fields map[string]string, withFile bool) *http.Request {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	for k, v := range fields {
		if err := w.WriteField(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if withFile {
		part, err := w.CreateFormFile("file", "receipt.png")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = part.Write([]byte("PNGDATA")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, target, body)
	r.Header.Set("Content-Type", w.FormDataContentType())
	return r
}

func newMultipartRequestContext(t *testing.T, r *http.Request, params []DXAPIEndPointParameter) *DXAPIEndPointRequest {
	t.Helper()
	rec := httptest.NewRecorder()
	var rw http.ResponseWriter = rec
	l := log.NewLog(nil, r.Context(), "multipart-test")
	return &DXAPIEndPointRequest{
		Request:         r,
		ResponseWriter:  &rw,
		Log:             l,
		ParameterValues: map[string]*DXAPIEndPointRequestParameterValue{},
		EndPoint: &DXAPIEndPoint{
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeMultiPartFormData,
			Parameters:         params,
		},
	}
}

var multipartParams = []DXAPIEndPointParameter{
	{NameId: "contactcenter_session_id", Type: dxlibTypes.APIParameterTypeInt64, IsMustExist: true},
	{NameId: "caption_text", Type: dxlibTypes.APIParameterTypeString, IsMustExist: false},
}

// A multipart POST used to fall through the content-type switch and answer 422
// before the handler ran, which left every file upload endpoint dead.
func TestMultiPartFormDataResolvesValuePartsAndLeavesTheFile(t *testing.T) {
	r := newMultipartRequest(t, "/probe", map[string]string{
		"contactcenter_session_id": "42",
		// All digits, but declared a string: it has to stay text.
		"caption_text": "12345",
		// Matches no declared parameter, so it must be dropped rather than bound.
		"junk": "ignored",
	}, true)
	aepr := newMultipartRequestContext(t, r, multipartParams)

	if err := aepr.preProcessRequestAsMultiPartFormData(); err != nil {
		t.Fatalf("multipart request rejected: %v", err)
	}

	_, sessionId, err := aepr.GetParameterValueAsInt64("contactcenter_session_id")
	if err != nil || sessionId != 42 {
		t.Fatalf("session id resolved to %v (%v), want 42", sessionId, err)
	}
	_, caption, err := aepr.GetParameterValueAsString("caption_text")
	if err != nil || caption != "12345" {
		t.Fatalf("caption resolved to %q (%v), want the string 12345", caption, err)
	}
	if _, bound := aepr.ParameterValues["junk"]; bound {
		t.Error("a part matching no declared parameter was bound")
	}

	// The handler takes the file itself, so it has to survive parameter binding.
	f, header, err := aepr.Request.FormFile("file")
	if err != nil {
		t.Fatalf("file part not reachable after preprocessing: %v", err)
	}
	defer func() { _ = f.Close() }()
	if header.Filename != "receipt.png" {
		t.Errorf("filename is %q", header.Filename)
	}
}

// A query string must not be able to supply a body parameter.
func TestMultiPartFormDataIgnoresTheQueryString(t *testing.T) {
	r := newMultipartRequest(t, "/probe?contactcenter_session_id=999", map[string]string{
		"contactcenter_session_id": "42",
	}, false)
	aepr := newMultipartRequestContext(t, r, multipartParams)

	if err := aepr.preProcessRequestAsMultiPartFormData(); err != nil {
		t.Fatal(err)
	}
	_, sessionId, err := aepr.GetParameterValueAsInt64("contactcenter_session_id")
	if err != nil || sessionId != 42 {
		t.Fatalf("session id resolved to %v (%v), want the body value 42", sessionId, err)
	}
}

// A mandatory parameter absent from the form fails the way it does over JSON.
func TestMultiPartFormDataRejectsMissingMandatoryParameter(t *testing.T) {
	r := newMultipartRequest(t, "/probe", map[string]string{"caption_text": "hello"}, false)
	aepr := newMultipartRequestContext(t, r, multipartParams)

	if err := aepr.preProcessRequestAsMultiPartFormData(); err == nil {
		t.Fatal("a form with no contactcenter_session_id was accepted")
	}
}

// Past the ceiling the request is refused while it is read, not after.
func TestMultiPartFormDataRefusesAnOversizeBody(t *testing.T) {
	r := newMultipartRequest(t, "/probe", map[string]string{
		"contactcenter_session_id": "42",
		"caption_text":             string(bytes.Repeat([]byte("x"), 4096)),
	}, true)
	aepr := newMultipartRequestContext(t, r, multipartParams)
	aepr.EndPoint.RequestMaxContentLength = 512

	if err := aepr.preProcessRequestAsMultiPartFormData(); err == nil {
		t.Fatal("a body past RequestMaxContentLength was accepted")
	}
}
