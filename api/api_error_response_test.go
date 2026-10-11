package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The route handler used to answer every failure with a fixed code and no
// cause (PREPROCESS_ERROR, MIDDLEWARE_ERROR, INTERNAL_SERVER_ERROR), so a
// caller could not tell what went wrong. Every error answer now carries the
// operation, the cause, the next step and the status, next to the older
// members existing clients read.

func TestErrorResponseBodyShape(t *testing.T) {
	body := ErrorResponseBody(http.StatusServiceUnavailable, "", "registering the client", "dial tcp 10.0.0.5:6379: connection refused", "check that Valkey is running")
	want := utils.JSON{
		"error":          "registering the client: dial tcp 10.0.0.5:6379: connection refused",
		"operation":      "registering the client",
		"cause":          "dial tcp 10.0.0.5:6379: connection refused",
		"next":           "check that Valkey is running",
		"status":         "Service Unavailable",
		"status_code":    http.StatusServiceUnavailable,
		"reason":         "SERVICE_UNAVAILABLE",
		"reason_message": "registering the client: dial tcp 10.0.0.5:6379: connection refused",
	}
	if fmt.Sprint(body) != fmt.Sprint(want) {
		t.Fatalf("body =\n%v\nwant\n%v", body, want)
	}

	if b := ErrorResponseBody(http.StatusConflict, "TAKEN", "", "", ""); b["error"] != "TAKEN" || b["reason"] != "TAKEN" {
		t.Fatalf("a body with only a code should name it as the error: %v", b)
	}
	if _, ok := ErrorResponseBody(http.StatusConflict, "TAKEN", "x", "", "")["next"]; ok {
		t.Fatal("an empty next should be left out")
	}
}

func TestRedactErrorText(t *testing.T) {
	cases := []struct{ in, gone, kept string }{
		{`pq: password authentication failed for user "app" (dsn host=db password=hunter2 dbname=x)`, "hunter2", "password authentication failed"},
		{`redis://default:s3cr3tpass@valkey:6379/0: connection refused`, "s3cr3tpass", "redis://default:***REDACTED***@valkey:6379/0"},
		{`upstream answered 401 for Authorization: Bearer eyJhbGciOi.payload.sig`, "eyJhbGciOi", "upstream answered 401"},
		{`GET /send?api_key=AKIA1234&chat=5 failed`, "AKIA1234", "chat=5"},
		{`config: smtp_password: "p@ss word" is rejected`, "p@ss word", "smtp_password: ***REDACTED***"},
	}
	for _, c := range cases {
		out := RedactErrorText(c.in)
		if strings.Contains(out, c.gone) {
			t.Errorf("RedactErrorText(%q) = %q, still carries %q", c.in, out, c.gone)
		}
		if !strings.Contains(out, c.kept) {
			t.Errorf("RedactErrorText(%q) = %q, lost %q", c.in, out, c.kept)
		}
	}
	// Error codes and prose that name a credential without carrying one keep
	// every word: the real reason must not be hidden.
	for _, plain := range []string{
		"dial tcp 10.0.0.5:5432: i/o timeout",
		"TOKEN_EXPIRED: session expired at 10:00",
		"TOKEN_NOT_FOUND: token header missing from the request",
		"SET_ENCRYPTION_SESSION_KEY_ERROR:app.session_key_name",
		"SESSION_KEY_INVALID: unknown session",
		"Basic authentication failed for user app",
		"the session token mismatched for device 7",
		"invalid token: signature mismatch",
		"Bearer token missing",
	} {
		if out := RedactErrorText(plain); out != plain {
			t.Errorf("text with no credential changed: %q became %q", plain, out)
		}
	}
	more := []struct{ in, gone, kept string }{
		{`Authorization: Bearer abcdefgh`, "abcdefgh", "Authorization: Bearer ***REDACTED***"},
		{`sent Basic dXNlcjpwYXNzd29yZA== upstream`, "dXNlcjpwYXNzd29yZA", "sent Basic ***REDACTED*** upstream"},
		{`login refused: password: letmein`, "letmein", "password: ***REDACTED***"},
		{`API_TOKEN=plainword rejected`, "plainword", "API_TOKEN=***REDACTED***"},
		{`refresh token: 9f8e7d6c5b4a rejected`, "9f8e7d6c5b4a", "token: ***REDACTED*** rejected"},
	}
	for _, c := range more {
		out := RedactErrorText(c.in)
		if strings.Contains(out, c.gone) || !strings.Contains(out, c.kept) {
			t.Errorf("RedactErrorText(%q) = %q, want %q gone and %q kept", c.in, out, c.gone, c.kept)
		}
	}
}

// routeTo builds an endpoint on a bare DXAPI and runs one request through the
// real route handler.
func routeTo(t *testing.T, ep *DXAPIEndPoint, body string) (*httptest.ResponseRecorder, utils.JSON) {
	t.Helper()
	a := &DXAPI{NameId: "error-answer-test"}
	ep.Owner = a
	ep.Uri = "/probe"
	ep.Method = http.MethodPost
	ep.EndPointType = EndPointTypeHTTPJSON
	ep.RequestContentType = utilsHttp.RequestContentTypeApplicationJSON
	r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.routeHandler(rec, r, ep)
	return rec, decodeBody(t, rec)
}

func wantErrorAnswer(t *testing.T, rec *httptest.ResponseRecorder, body utils.JSON, status int, reason string, causeHas string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d: %v", rec.Code, status, body)
	}
	if body["reason"] != reason {
		t.Fatalf("reason = %v, want %s", body["reason"], reason)
	}
	if body["status_code"] != float64(status) {
		t.Fatalf("status_code = %v, want %d", body["status_code"], status)
	}
	for _, k := range []string{"error", "operation", "cause", "next", "reason_message", "error_log_ref"} {
		if s, _ := body[k].(string); s == "" {
			t.Fatalf("answer has no %q: %v", k, body)
		}
	}
	if !strings.Contains(body["cause"].(string), causeHas) {
		t.Fatalf("cause = %q, want it to carry %q", body["cause"], causeHas)
	}
	if body["error"] != body["reason_message"] {
		t.Fatalf("reason_message should repeat error: %v", body)
	}
}

func TestMiddlewareErrorNamesTheCause(t *testing.T) {
	ep := &DXAPIEndPoint{
		Middlewares: []DXAPIEndPointExecuteFunc{
			func(*DXAPIEndPointRequest) error { return nil },
			func(*DXAPIEndPointRequest) error { return fmt.Errorf("SESSION_NOT_FOUND: no session for the token") },
		},
		OnExecute: func(*DXAPIEndPointRequest) error { t.Fatal("OnExecute ran after a middleware refused"); return nil },
	}
	rec, body := routeTo(t, ep, `{}`)
	wantErrorAnswer(t, rec, body, http.StatusBadRequest, "MIDDLEWARE_ERROR", "SESSION_NOT_FOUND: no session for the token")
	if op := body["operation"].(string); !strings.Contains(op, "middleware 2 of 2") || !strings.Contains(op, "POST /probe") {
		t.Fatalf("operation = %q, want it to name the middleware and the endpoint", op)
	}
}

func TestMiddlewareAPIErrorKeepsItsStatus(t *testing.T) {
	ep := &DXAPIEndPoint{
		Middlewares: []DXAPIEndPointExecuteFunc{
			func(*DXAPIEndPointRequest) error {
				return NewAPIError(http.StatusUnauthorized, "SESSION_EXPIRED", "checking the session", fmt.Errorf("session expired at 10:00"), "log in again")
			},
		},
	}
	rec, body := routeTo(t, ep, `{}`)
	wantErrorAnswer(t, rec, body, http.StatusUnauthorized, "SESSION_EXPIRED", "session expired at 10:00")
	if body["next"] != "log in again" || body["operation"] != "checking the session" {
		t.Fatalf("the API error's own operation and next should be answered: %v", body)
	}
}

func TestExecuteErrorNamesTheCause(t *testing.T) {
	ep := &DXAPIEndPoint{
		OnExecute: func(*DXAPIEndPointRequest) error {
			return fmt.Errorf("INSERT INTO client: dial tcp 10.0.0.5:5432 (dsn password=hunter2): connection refused")
		},
	}
	rec, body := routeTo(t, ep, `{}`)
	wantErrorAnswer(t, rec, body, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "connection refused")
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("a credential in the cause reached the answer: %s", rec.Body.String())
	}
}

func TestExecuteAPIErrorAnswersItsStatus(t *testing.T) {
	ep := &DXAPIEndPoint{
		OnExecute: func(*DXAPIEndPointRequest) error {
			return fmt.Errorf("handler: %w", NewAPIError(http.StatusNotFound, "CONFIG_NOT_REGISTERED", "sending the message", fmt.Errorf("no config named %q", "fcm-main"), "register it with POST /config"))
		},
	}
	rec, body := routeTo(t, ep, `{}`)
	if rec.Code != http.StatusNotFound || body["reason"] != "CONFIG_NOT_REGISTERED" || body["next"] != "register it with POST /config" {
		t.Fatalf("answer = %d %v", rec.Code, body)
	}
	if _, ok := body["error_log_ref"]; ok {
		t.Fatalf("a 4xx is logged at Warn and has no error_log_ref: %v", body)
	}
}

func TestPanicNamesThePanic(t *testing.T) {
	ep := &DXAPIEndPoint{
		OnExecute: func(*DXAPIEndPointRequest) error { panic("index out of range [3] with length 2") },
	}
	rec, body := routeTo(t, ep, `{}`)
	wantErrorAnswer(t, rec, body, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "index out of range [3] with length 2")
}

func TestPreprocessErrorNamesTheCause(t *testing.T) {
	rec, body := routeTo(t, &DXAPIEndPoint{OnExecute: func(*DXAPIEndPointRequest) error { return nil }}, `{"password":"hunter2",`)
	if rec.Code < 400 {
		t.Fatalf("a broken body was accepted: %d %v", rec.Code, body)
	}
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("the request body was echoed in the answer: %s", rec.Body.String())
	}
	if c, _ := body["cause"].(string); !strings.Contains(c, "REQUEST_BODY_CANT_BE_PARSED_AS_JSON") {
		t.Fatalf("the cause does not say the body is not JSON: %v", body)
	}
	for _, k := range []string{"error", "operation", "next"} {
		if s, _ := body[k].(string); s == "" {
			t.Fatalf("answer has no %q: %v", k, body)
		}
	}
}

func TestOmitServerErrorCauseWithholdsOnly5xx(t *testing.T) {
	newRequest := func() (*DXAPIEndPointRequest, *httptest.ResponseRecorder) {
		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{}`))
		r.Header.Set("Content-Type", "application/json")
		return newPreProcessContext(r, &DXAPIEndPoint{
			Owner:              &DXAPI{OmitServerErrorCause: true},
			Uri:                "/probe",
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
		})
	}

	aepr, rec := newRequest()
	aepr.WriteResponseAsInternalServerError("EXECUTE_ERROR", fmt.Errorf("pq: relation member does not exist"))
	body := decodeBody(t, rec)
	if strings.Contains(rec.Body.String(), "relation") {
		t.Fatalf("the 5xx answer carries the driver text: %s", rec.Body.String())
	}
	for _, k := range []string{"operation", "next", "error_log_ref"} {
		if s, _ := body[k].(string); s == "" {
			t.Fatalf("a withheld answer should still carry %q: %v", k, body)
		}
	}
	if c, _ := body["cause"].(string); !strings.Contains(c, "withheld") || !strings.Contains(c, body["error_log_ref"].(string)) {
		t.Fatalf("cause should say it was withheld and where it is logged: %v", body)
	}

	aepr, rec = newRequest()
	aepr.WriteResponseAsAPIError(http.StatusConflict, "registering the client", fmt.Errorf("nameid %q is taken", "app-1"), "choose another nameid")
	if !strings.Contains(rec.Body.String(), "is taken") {
		t.Fatalf("a 4xx answer should keep its cause: %s", rec.Body.String())
	}
}

type fakeDBError struct{ cause error }

func (e *fakeDBError) Error() string {
	return "DB_UPDATE_ERROR table=member data=set={full_name=Ana Putri} where={id=7}: " + e.cause.Error()
}
func (e *fakeDBError) Unwrap() error              { return e.cause }
func (e *fakeDBError) DBOperation() string        { return "UPDATE" }
func (e *fakeDBError) DBTableName() string        { return "member" }
func (e *fakeDBError) DBMaskedDataString() string { return "set={full_name=Ana Putri} where={id=7}" }

func TestResponseCauseLeavesRowValuesInTheLog(t *testing.T) {
	err := fmt.Errorf("saving the profile: %w", &fakeDBError{cause: fmt.Errorf("pq: deadlock detected")})
	got := responseCause(err)
	want := "saving the profile: DB_UPDATE_ERROR table=member: pq: deadlock detected"
	if got != want {
		t.Fatalf("responseCause = %q, want %q", got, want)
	}
	if got := responseCause(fmt.Errorf("DB_QUERY_ERROR data={email=a@b.c}: timeout")); strings.Contains(got, "a@b.c") {
		t.Fatalf("a data block outside a DB error reached the cause: %q", got)
	}
}

func TestAPIErrorBodyLeavesRowValuesInTheLog(t *testing.T) {
	body := NewAPIError(http.StatusConflict, "", "saving the profile", &fakeDBError{cause: fmt.Errorf("pq: deadlock detected")}, "retry").ResponseBody()
	if c := body["cause"].(string); strings.Contains(c, "Ana Putri") || !strings.Contains(c, "deadlock") {
		t.Fatalf("cause = %q", c)
	}
}

// helperRequest is a request on an endpoint of an API with the given
// omit-server-error-cause setting, for the older answer helpers.
func helperRequest(omit bool) (*DXAPIEndPointRequest, *httptest.ResponseRecorder) {
	r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	return newPreProcessContext(r, &DXAPIEndPoint{
		Owner:              &DXAPI{OmitServerErrorCause: omit},
		Uri:                "/probe",
		Method:             http.MethodPost,
		EndPointType:       EndPointTypeHTTPJSON,
		RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
	})
}

// The older helpers answer through the same path as the route handler: no
// member of the body carries a masked credential or a DB error's row values,
// and with omit-server-error-cause a 5xx carries none of the error text.
func TestHelperAnswersMaskAndWithhold(t *testing.T) {
	dbErr := fmt.Errorf("saving the profile (password=hunter2): %w", &fakeDBError{cause: fmt.Errorf("pq: deadlock detected")})
	helpers := map[string]func(*DXAPIEndPointRequest, int){
		"WriteResponseAsError": func(a *DXAPIEndPointRequest, status int) { a.WriteResponseAsError(status, dbErr) },
		"WriteResponseAndLogAsError": func(a *DXAPIEndPointRequest, status int) {
			a.WriteResponseAndLogAsError(status, "", dbErr)
		},
		"WriteResponseAndNewErrorf": func(a *DXAPIEndPointRequest, status int) {
			_ = a.WriteResponseAndNewErrorf(status, "", "%s", dbErr.Error())
		},
	}
	for name, write := range helpers {
		aepr, rec := helperRequest(false)
		write(aepr, http.StatusBadGateway)
		raw := rec.Body.String()
		for _, secret := range []string{"hunter2", "Ana Putri"} {
			if strings.Contains(raw, secret) {
				t.Errorf("%s: the answer carries %q: %s", name, secret, raw)
			}
		}
		if !strings.Contains(decodeBody(t, rec)["cause"].(string), "deadlock detected") {
			t.Errorf("%s: the cause lost the driver error: %s", name, raw)
		}

		aepr, rec = helperRequest(true)
		write(aepr, http.StatusBadGateway)
		raw = rec.Body.String()
		for _, text := range []string{"deadlock", "saving the profile", "member"} {
			if strings.Contains(raw, text) {
				t.Errorf("%s: with omit-server-error-cause the 5xx answer carries %q: %s", name, text, raw)
			}
		}
		body := decodeBody(t, rec)
		if c, _ := body["cause"].(string); !strings.Contains(c, "withheld") {
			t.Errorf("%s: cause should say it was withheld: %v", name, body)
		}

		aepr, rec = helperRequest(true)
		write(aepr, http.StatusBadRequest)
		if !strings.Contains(rec.Body.String(), "deadlock detected") {
			t.Errorf("%s: a 4xx answer should keep its cause with omit-server-error-cause: %s", name, rec.Body.String())
		}
	}
}
