package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/language"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

const testProblemTypeBaseURI = "https://example.com/problems/"

// newProblemRequest builds a request on a JSON endpoint whose API has the
// problem document setting as given, sent to /probe with a query string that
// must not reach the answer.
func newProblemRequest(enabled bool, endPointType DXAPIEndPointType) (*DXAPIEndPointRequest, *httptest.ResponseRecorder) {
	r := httptest.NewRequest(http.MethodPost, "/probe?token=secret", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	return newPreProcessContext(r, &DXAPIEndPoint{
		Owner:              &DXAPI{ProblemDetailsEnabled: enabled, ProblemTypeBaseURI: testProblemTypeBaseURI},
		Uri:                "/probe",
		Method:             http.MethodPost,
		EndPointType:       endPointType,
		RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
	})
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) utils.JSON {
	t.Helper()
	body := utils.JSON{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, rec.Body.String())
	}
	return body
}

func wantProblem(t *testing.T, rec *httptest.ResponseRecorder, status int, wantType string) utils.JSON {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d", rec.Code, status)
	}
	if ct := rec.Header().Get("Content-Type"); ct != ContentTypeProblemJSON {
		t.Fatalf("Content-Type = %q, want %q", ct, ContentTypeProblemJSON)
	}
	body := decodeBody(t, rec)
	if body["type"] != wantType {
		t.Fatalf("type = %v, want %q", body["type"], wantType)
	}
	if body["title"] != http.StatusText(status) {
		t.Fatalf("title = %v, want %q", body["title"], http.StatusText(status))
	}
	if body["status"] != float64(status) {
		t.Fatalf("status member = %v, want %d", body["status"], status)
	}
	if body["instance"] != "/probe" {
		t.Fatalf("instance = %v, want /probe (no query string)", body["instance"])
	}
	for _, k := range []string{"status_code", "reason", "reason_message"} {
		if _, ok := body[k]; ok {
			t.Fatalf("problem document carries the legacy member %q: %v", k, body)
		}
	}
	return body
}

func TestNewProblemDetails(t *testing.T) {
	t.Run("a reason code names the type", func(t *testing.T) {
		p := NewProblemDetails(http.StatusConflict, utils.JSON{
			"status":         "Conflict",
			"status_code":    http.StatusConflict,
			"reason":         "UNIQUE_FIELD_VIOLATION",
			"reason_message": "UNIQUE_FIELD_VIOLATION",
			"fields":         []string{"email"},
		}, testProblemTypeBaseURI, "/members")
		want := utils.JSON{
			"type":     testProblemTypeBaseURI + "UNIQUE_FIELD_VIOLATION",
			"title":    "Conflict",
			"status":   http.StatusConflict,
			"detail":   "UNIQUE_FIELD_VIOLATION",
			"instance": "/members",
			"fields":   []string{"email"},
		}
		gotJSON, _ := json.Marshal(p)
		wantJSON, _ := json.Marshal(want)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("got %s\nwant %s", gotJSON, wantJSON)
		}
	})
	t.Run("a reason that is not a code is about:blank", func(t *testing.T) {
		for _, reason := range []any{"BAD REQUEST", "Bad Request", "", nil, "parent must be set", 42} {
			p := NewProblemDetails(http.StatusBadRequest, utils.JSON{"reason": reason}, testProblemTypeBaseURI, "")
			if p["type"] != "about:blank" {
				t.Fatalf("reason %#v: type = %v, want about:blank", reason, p["type"])
			}
		}
	})
	t.Run("no detail and no instance leave the members out", func(t *testing.T) {
		p := NewProblemDetails(http.StatusBadRequest, utils.JSON{
			"reason": "X", "reason_message": " ", "detail": "kept?", "instance": "kept?",
		}, "", "")
		if _, ok := p["detail"]; ok {
			t.Fatalf("detail = %v, want none", p["detail"])
		}
		if _, ok := p["instance"]; ok {
			t.Fatalf("instance = %v, want none", p["instance"])
		}
		if p["type"] != "X" {
			t.Fatalf("type = %v, want the bare code with no base", p["type"])
		}
	})
	t.Run("the problem members win over extension members of the same name", func(t *testing.T) {
		p := NewProblemDetails(http.StatusNotFound, utils.JSON{"reason": "GONE", "type": "x", "title": "x"}, testProblemTypeBaseURI, "")
		if p["type"] != testProblemTypeBaseURI+"GONE" || p["title"] != "Not Found" {
			t.Fatalf("got type=%v title=%v", p["type"], p["title"])
		}
	})
}

// With the setting off, nothing changes: the refusal keeps its own body and
// application/json.
func TestProblemDetailsOffKeepsTheRefusalBody(t *testing.T) {
	aepr, rec := newProblemRequest(false, EndPointTypeHTTPJSON)
	_ = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_FIELD_VALUE_IS_NOT_STRING:%s", "name")
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	body := decodeBody(t, rec)
	want := utils.JSON{
		"status":         "Unprocessable Entity",
		"status_code":    float64(http.StatusUnprocessableEntity),
		"reason":         "UNPROCESSABLE ENTITY",
		"reason_message": "REQUEST_FIELD_VALUE_IS_NOT_STRING:name",
	}
	gotJSON, _ := json.Marshal(body)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("got %s\nwant %s", gotJSON, wantJSON)
	}
}

func TestProblemDetailsOn(t *testing.T) {
	t.Run("a parameter refusal carries its code in detail", func(t *testing.T) {
		aepr, rec := newProblemRequest(true, EndPointTypeHTTPJSON)
		_ = aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "REQUEST_FIELD_VALUE_IS_NOT_STRING:%s", "name")
		body := wantProblem(t, rec, http.StatusUnprocessableEntity, "about:blank")
		if body["detail"] != "REQUEST_FIELD_VALUE_IS_NOT_STRING:name" {
			t.Fatalf("detail = %v", body["detail"])
		}
	})
	t.Run("a domain error keeps its fields", func(t *testing.T) {
		aepr, rec := newProblemRequest(true, EndPointTypeHTTPJSON)
		aepr.WriteResponseAsDomainError(&ErrUniqueFieldViolation{TableName: "member", Fields: []string{"email"}, Values: utils.JSON{"email": "a@example.com"}})
		body := wantProblem(t, rec, http.StatusConflict, testProblemTypeBaseURI+"UNIQUE_FIELD_VIOLATION")
		fields, ok := body["fields"].([]any)
		if !ok || len(fields) != 1 || fields[0] != "email" {
			t.Fatalf("fields = %v, want [email]", body["fields"])
		}
		if strings.Contains(rec.Body.String(), "member") || strings.Contains(rec.Body.String(), "a@example.com") {
			t.Fatalf("the answer names the table or a value: %s", rec.Body.String())
		}
	})
	t.Run("a server error keeps its error_log_ref", func(t *testing.T) {
		aepr, rec := newProblemRequest(true, EndPointTypeHTTPJSON)
		aepr.WriteResponseAsInternalServerError("EXECUTE_ERROR", errors.New("pq: relation member does not exist"))
		body := wantProblem(t, rec, http.StatusInternalServerError, testProblemTypeBaseURI+"INTERNAL_SERVER_ERROR")
		if _, ok := body["error_log_ref"].(string); !ok {
			t.Fatalf("error_log_ref = %v, want a string", body["error_log_ref"])
		}
		if strings.Contains(rec.Body.String(), "relation") {
			t.Fatalf("the answer carries the driver text: %s", rec.Body.String())
		}
	})
	t.Run("a success is left as it is", func(t *testing.T) {
		aepr, rec := newProblemRequest(true, EndPointTypeHTTPJSON)
		aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"data": utils.JSON{"id": 1}})
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", ct)
		}
		body := decodeBody(t, rec)
		if body["status_code"] != float64(http.StatusOK) || body["reason"] != "OK" {
			t.Fatalf("got %v, want the usual success body", body)
		}
		if _, ok := body["type"]; ok {
			t.Fatalf("a success carries a problem type: %v", body)
		}
	})
	t.Run("the type is not translated, title and detail are", func(t *testing.T) {
		language.Dictionaries["zz"] = map[string]string{
			"UNIQUE_FIELD_VIOLATION": "Sudah terdaftar",
			"Conflict":               "Konflik",
		}
		t.Cleanup(func() { delete(language.Dictionaries, "zz") })
		aepr, rec := newProblemRequest(true, EndPointTypeHTTPJSON)
		aepr.LocalData = utils.JSON{"language": "zz"}
		aepr.WriteResponseAsDomainError(&ErrUniqueFieldViolation{TableName: "member", Fields: []string{"email"}})
		body := decodeBody(t, rec)
		if body["type"] != testProblemTypeBaseURI+"UNIQUE_FIELD_VIOLATION" {
			t.Fatalf("type = %v", body["type"])
		}
		if body["title"] != "Konflik" || body["detail"] != "Sudah terdaftar" {
			t.Fatalf("title = %v, detail = %v, want them translated", body["title"], body["detail"])
		}
	})
}

// An encrypted endpoint whose session is gone answers in plain JSON; with the
// setting on, that answer is a problem document too.
func TestProblemDetailsEncryptedSessionFallback(t *testing.T) {
	for _, endPointType := range []DXAPIEndPointType{EndPointTypeHTTPEndToEndEncryptionV3, EndPointTypeHTTPEndToEndEncryptionV4} {
		t.Run("on", func(t *testing.T) {
			aepr, rec := newProblemRequest(true, endPointType)
			aepr.WriteResponseAsJSON(http.StatusUnauthorized, nil, utils.JSON{"reason": "SESSION_NOT_FOUND"})
			body := wantProblem(t, rec, http.StatusUnauthorized, testProblemTypeBaseURI+"REFRESH_SESSION")
			if !strings.Contains(body["detail"].(string), "bootstrap") {
				t.Fatalf("detail = %v", body["detail"])
			}
		})
		t.Run("off", func(t *testing.T) {
			aepr, rec := newProblemRequest(false, endPointType)
			aepr.WriteResponseAsJSON(http.StatusUnauthorized, nil, utils.JSON{"reason": "SESSION_NOT_FOUND"})
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}
			if body := decodeBody(t, rec); body["reason"] != "REFRESH_SESSION" {
				t.Fatalf("reason = %v, want REFRESH_SESSION", body["reason"])
			}
		})
	}
	t.Run("prekey", func(t *testing.T) {
		aepr, rec := newProblemRequest(true, EndPointTypeHTTPEndToEndEncryptionV2)
		aepr.WriteResponseAsJSON(http.StatusUnauthorized, nil, utils.JSON{"reason": "PREKEY_NOT_FOUND"})
		wantProblem(t, rec, http.StatusUnauthorized, testProblemTypeBaseURI+"REFRESH_PREKEY")
	})
}
