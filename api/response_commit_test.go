package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// ResponseHeaderSent is what every error path consults to decide whether it may
// still write a JSON body. A handler that streams a file has to set it at the
// moment the status goes out, not after the body finishes, because the body is
// the part that fails: marked late, a failed write leaves the flag false and the
// framework appends an error object to the tail of a half-sent file, under a 200.
// A short file is a failure a client can see. A whole-looking file with JSON
// glued to the end is not.
func TestWriteResponseAsJSONStaysSilentOnceCommitted(t *testing.T) {
	rec := httptest.NewRecorder()
	var rw http.ResponseWriter = rec
	aepr := &DXAPIEndPointRequest{
		ResponseWriter: &rw,
		EndPoint:       &DXAPIEndPoint{EndPointType: EndPointTypeHTTPJSON},
	}

	// What a download handler does: commit, then write the body.
	aepr.ResponseHeaderSent = true
	rec.WriteHeader(http.StatusOK)
	if _, err := rec.Write([]byte("PARTIAL-FILE")); err != nil {
		t.Fatal(err)
	}

	// The body write failed, and the error path tries to report it.
	aepr.WriteResponseAsJSON(500, nil, map[string]any{"success": "FAIL"})

	if got := rec.Body.String(); got != "PARTIAL-FILE" {
		t.Errorf("the response carries %q; anything past the file bytes is the corruption this guards", got)
	}
}

// The same call before anything is committed has to still work, or every
// ordinary error response would go silent.
func TestWriteResponseAsJSONStillAnswersBeforeCommit(t *testing.T) {
	rec := httptest.NewRecorder()
	var rw http.ResponseWriter = rec
	aepr := &DXAPIEndPointRequest{
		ResponseWriter: &rw,
		EndPoint:       &DXAPIEndPoint{EndPointType: EndPointTypeHTTPJSON},
	}

	aepr.WriteResponseAsJSON(500, nil, map[string]any{"success": "FAIL"})

	if rec.Body.Len() == 0 {
		t.Error("an error before the response was committed wrote nothing at all")
	}
}
