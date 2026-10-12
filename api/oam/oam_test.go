package oam

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/log"
)

// The credentials a request can carry, one in every place one rides: a bearer token, a cookie,
// a token header a proxy adds on the way in, a query value and a body field.
const (
	authValue   = "Bearer bearer-secret-7f3a"
	cookieValue = "session=cookie-secret-2a9b"
	proxyToken  = "proxy-token-secret-91c2"
	queryToken  = "query-token-secret-4d0e"
	password    = "body-password-secret-5b8f"
)

var secrets = []string{authValue, cookieValue, proxyToken, queryToken, password}

// newCredentialRequest builds a request to path carrying every credential above, and returns
// it with the body it was given.
func newCredentialRequest(path string) (*http.Request, string) {
	body := fmt.Sprintf(`{"username":"adinda@dailymail.co.id","password":%q}`, password)
	req := httptest.NewRequest(http.MethodGet, path+"?token="+queryToken+"&unique_id=dev-1", strings.NewReader(body))
	req.Header.Set("Authorization", authValue)
	req.Header.Set("Cookie", cookieValue)
	req.Header.Set("X-Proxy-Token", proxyToken)
	return req, body
}

// newEndPointRequest wraps req for a handler and records its answer. owner is the API the
// endpoint belongs to; nil is an endpoint with no owner, which every handler must survive.
func newEndPointRequest(req *http.Request, owner *api.DXAPI) (*api.DXAPIEndPointRequest, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	var rw http.ResponseWriter = rec
	aepr := &api.DXAPIEndPointRequest{
		Request:        req,
		ResponseWriter: &rw,
		EndPoint:       &api.DXAPIEndPoint{EndPointType: api.EndPointTypeHTTPJSON, Owner: owner},
	}
	return aepr, rec
}

// captureLog collects what the log is given until the test ends and returns a reader of it.
func captureLog(t *testing.T) func() string {
	t.Helper()
	var logged []string
	previous := log.OnError
	log.OnError = func(_ *log.DXLog, _ error, _ log.DXLogLevel, _ string, text string, _ string) error {
		logged = append(logged, text)
		return nil
	}
	t.Cleanup(func() { log.OnError = previous })
	return func() string { return strings.Join(logged, "\n") }
}

// assertNoSecret fails when text carries any credential as sent, or as the decimal bytes a
// byte slice prints as with %v.
func assertNoSecret(t *testing.T, what, text string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(text, secret) {
			t.Errorf("%s carries %q as sent:\n%s", what, secret, text)
		}
		decimal := strings.Trim(fmt.Sprint([]byte(secret)), "[]")
		if strings.Contains(text, decimal) {
			t.Errorf("%s carries %q as decimal bytes:\n%s", what, secret, text)
		}
	}
}

// Ping writes a copy of the request to the log. That copy must carry no credential as sent:
// not a header value, not a query value, not a body field, and in no spelling (a byte slice
// printed with %v spells the body out in decimals).
func TestPingLogsNoRawCredential(t *testing.T) {
	logText := captureLog(t)
	req, _ := newCredentialRequest("/ping")
	aepr, _ := newEndPointRequest(req, nil)
	if err := Ping(aepr); err != nil {
		t.Fatal(err)
	}

	all := logText()
	if all == "" {
		t.Fatal("Ping wrote nothing to the log")
	}
	assertNoSecret(t, "the log", all)
	if !strings.Contains(all, "***REDACTED***") {
		t.Errorf("no credential was redacted in the log:\n%s", all)
	}
}

// Ping answers that the service is up and nothing of the request: it used to echo the whole
// request as received, which handed a caller every header a proxy or gateway added on the
// way in, and Authorization and Cookie with them.
func TestPingAnswersStatusOnly(t *testing.T) {
	previous := log.Log.Prefix
	log.Log.Prefix = "service-under-test"
	t.Cleanup(func() { log.Log.Prefix = previous })

	req, _ := newCredentialRequest("/ping")
	aepr, rec := newEndPointRequest(req, nil)
	if err := Ping(aepr); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", rec.Code, rec.Body.String())
	}
	answer := rec.Body.String()
	assertNoSecret(t, "the ping answer", answer)

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("the ping answer is not JSON: %v\n%s", err, answer)
	}
	if body["status"] != "OK" {
		t.Errorf("status = %v, want OK:\n%s", body["status"], answer)
	}
	if body["service"] != "service-under-test" {
		t.Errorf("service = %v, want the name the process was given:\n%s", body["service"], answer)
	}
	when, _ := body["time"].(string)
	if _, err := time.Parse(time.RFC3339, when); err != nil {
		t.Errorf("time %q is not RFC 3339: %v", when, err)
	}
	for _, echoed := range []string{"log_request", "header", "query", "body", "remote_addr", "request_uri"} {
		if _, ok := body[echoed]; ok {
			t.Errorf("the ping answer echoes %q:\n%s", echoed, answer)
		}
	}
}

// Echo returns the masked form: a credential header is named but its value redacted, the query
// and the body are masked by field name, and the path carries no query string.
func TestEchoAnswersMaskedRequest(t *testing.T) {
	req, _ := newCredentialRequest("/echo")
	data, err := Echo(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	answer := string(raw)
	assertNoSecret(t, "the echo", answer)

	lr, _ := data["log_request"].(map[string]any)
	if lr == nil {
		t.Fatalf("no log_request in the echo:\n%s", answer)
	}
	header, _ := lr["header"].(map[string]any)
	for _, name := range []string{"Authorization", "Cookie", "X-Proxy-Token"} {
		values, _ := header[name].([]any)
		if len(values) != 1 || values[0] != "***REDACTED***" {
			t.Errorf("header %s = %v, want one redacted value:\n%s", name, header[name], answer)
		}
	}
	// A credential-named query parameter is masked whole, its list of values as one value.
	query, _ := lr["query"].(map[string]any)
	if query["token"] != "***REDACTED***" {
		t.Errorf("query token = %v, want redacted:\n%s", query["token"], answer)
	}
	if values, _ := query["unique_id"].([]any); len(values) != 1 || values[0] != "dev-1" {
		t.Errorf("query unique_id = %v, want its value kept:\n%s", query["unique_id"], answer)
	}
	if lr["path"] != "/echo" {
		t.Errorf("path = %v, want /echo without its query string:\n%s", lr["path"], answer)
	}
	if _, ok := lr["request_uri"]; ok {
		t.Errorf("the echo carries request_uri, which holds the query string:\n%s", answer)
	}
	body, _ := lr["body"].(map[string]any)
	if body["password"] != "***REDACTED***" {
		t.Errorf("body password = %v, want redacted:\n%s", body["password"], answer)
	}
}

// EchoHandler answers the masked form unless its API has echo-raw-request on; only then is the
// request echoed as received. The log gets the masked form either way.
func TestEchoHandlerEchoesRawOnlyWithOptIn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner *api.DXAPI
		raw   bool
	}{
		{"no owner", nil, false},
		{"default", &api.DXAPI{}, false},
		{"echo-raw-request off", &api.DXAPI{EchoRawRequest: false}, false},
		{"echo-raw-request on", &api.DXAPI{EchoRawRequest: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logText := captureLog(t)
			req, body := newCredentialRequest("/echo")
			aepr, rec := newEndPointRequest(req, tc.owner)
			if err := EchoHandler(aepr); err != nil {
				t.Fatal(err)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d, want 200: %s", rec.Code, rec.Body.String())
			}
			answer := rec.Body.String()
			assertNoSecret(t, "the log", logText())

			if !tc.raw {
				assertNoSecret(t, "the echo", answer)
				if !strings.Contains(answer, "***REDACTED***") {
					t.Errorf("no credential was redacted in the echo:\n%s", answer)
				}
				return
			}
			// The raw echo is the request as received: the body as base64 bytes.
			for _, sent := range []string{authValue, cookieValue, proxyToken, queryToken, base64.StdEncoding.EncodeToString([]byte(body))} {
				if !strings.Contains(answer, sent) {
					t.Errorf("the raw echo does not carry %q:\n%s", sent, answer)
				}
			}
		})
	}
}
