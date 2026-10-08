package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// The raw request dump that rides every error log used to drop a fixed list of header names
// and write every other header in clear, so a credential sent under any other name, a proxy or
// session token for one, went to the log on every error. The dump now masks a credential
// header by the declared lists and the credential keywords, keeps its line, and leaves the
// plain headers and the body readable.
func TestRequestDumpMasksCredentialHeaders(t *testing.T) {
	utils.SetCredentialHeaders([]string{"X-Tenant-Access"})
	t.Cleanup(func() { utils.SetCredentialHeaders(nil) })

	req := httptest.NewRequest(http.MethodPost, "/v1/things?page=2", strings.NewReader(`{"name":"x"}`))
	req.Host = "api.example.test"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", "req-42")
	req.Header.Set("Authorization", "Bearer eyJ.top.secret")
	req.Header.Set("X-Upstream-Token", "gw-token-123")
	req.Header.Set("X-Tenant-Access", "tenant-pass-456")
	req.Header.Add("Cookie", "sid=cookie-one")
	req.Header.Add("Cookie", "csrf=cookie-two")

	aepr := &DXAPIEndPointRequest{Request: req, RequestBodyAsBytes: []byte(`{"name":"x"}`)}
	dump, err := aepr.RequestDumpAsString()
	if err != nil {
		t.Fatalf("RequestDump: %v", err)
	}

	for _, raw := range []string{"eyJ.top.secret", "gw-token-123", "tenant-pass-456", "cookie-one", "cookie-two"} {
		if strings.Contains(dump, raw) {
			t.Errorf("dump carries %q in clear:\n%s", raw, dump)
		}
	}
	for _, line := range []string{
		"POST /v1/things?page=2 HTTP/1.1\r\n",
		"Host: api.example.test\r\n",
		"Authorization: ***REDACTED***\r\n",
		"X-Upstream-Token: ***REDACTED***\r\n",
		"X-Tenant-Access: ***REDACTED***\r\n",
		"Cookie: ***REDACTED***\r\nCookie: ***REDACTED***\r\n",
		"Content-Type: application/json\r\n",
		"X-Request-Id: req-42\r\n",
		"\r\n\r\n" + `{"name":"x"}`,
	} {
		if !strings.Contains(dump, line) {
			t.Errorf("dump should carry %q:\n%s", line, dump)
		}
	}
	// Header lines come in name order, so two dumps of one request read the same.
	if strings.Index(dump, "Authorization:") > strings.Index(dump, "Content-Type:") ||
		strings.Index(dump, "Content-Type:") > strings.Index(dump, "Cookie:") ||
		strings.Index(dump, "Cookie:") > strings.Index(dump, "X-Request-Id:") {
		t.Errorf("header lines are not in name order:\n%s", dump)
	}
}

// The decrypted-header lines take the same credential lists, so a host-declared header is
// masked there too, while a plain header stays readable.
func TestDecryptedDumpMasksDeclaredCredentialHeaders(t *testing.T) {
	orig := logDecryptedBody
	t.Cleanup(func() { SetLogDecryptedBody(orig) })
	SetLogDecryptedBody(true)
	utils.SetCredentialHeaders([]string{"X-Tenant-Access"})
	t.Cleanup(func() { utils.SetCredentialHeaders(nil) })

	aepr := &DXAPIEndPointRequest{
		EffectiveRequestHeader: map[string]string{
			"X-Tenant-Access":  "tenant-pass-456",
			"X-Upstream-Token": "gw-token-123",
			"X-Request-Id":     "req-42",
		},
		DecryptedRequestBody: utils.JSON{"amount": 10},
	}
	dump := aepr.DecryptedRequestDumpAsString()
	for _, raw := range []string{"tenant-pass-456", "gw-token-123"} {
		if strings.Contains(dump, raw) {
			t.Errorf("dump carries %q in clear:\n%s", raw, dump)
		}
	}
	for _, line := range []string{"X-Tenant-Access: ***REDACTED***", "X-Upstream-Token: ***REDACTED***", "X-Request-Id: req-42"} {
		if !strings.Contains(dump, line) {
			t.Errorf("dump should carry %q:\n%s", line, dump)
		}
	}
}

// The outbound proxy call logs the request it sends and the response it gets at Debug. Those
// dumps carried the credential the host set for the upstream, and the upstream's Set-Cookie,
// in clear. They are masked now, and the response body still reaches the caller.
func TestHTTPClientDoMasksCredentialHeadersInItsDumps(t *testing.T) {
	logged := captureLog(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer out-secret-789" {
			t.Errorf("upstream got Authorization %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"amount":10}` {
			t.Errorf("upstream got body %q", body)
		}
		w.Header().Set("Set-Cookie", "upstream_session=cookie-xyz; HttpOnly")
		w.Header().Set("X-Trace-Id", "trace-7")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	aepr, _ := proxyRequest(t)
	response, err := aepr.HTTPClientDo(http.MethodPost, upstream.URL+"/things", utils.JSON{"amount": 10},
		map[string]string{"Authorization": "Bearer out-secret-789", "X-Request-Id": "req-42"})
	if err != nil {
		t.Fatalf("HTTPClientDo: %v", err)
	}
	defer response.Body.Close()
	var got map[string]any
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil || got["ok"] != true {
		t.Fatalf("response body after the dump: %v, err %v", got, err)
	}

	log := logged.String()
	for _, raw := range []string{"out-secret-789", "cookie-xyz"} {
		if strings.Contains(log, raw) {
			t.Errorf("log carries %q in clear:\n%s", raw, log)
		}
	}
	for _, line := range []string{
		// The text handler quotes the message, so the bodies are looked for by their key.
		"POST /things HTTP/1.1", "Authorization: ***REDACTED***", "X-Request-Id: req-42", "amount",
		"HTTP/1.1 200 OK", "Set-Cookie: ***REDACTED***", "X-Trace-Id: trace-7", "ok",
	} {
		if !strings.Contains(log, line) {
			t.Errorf("log should carry %q:\n%s", line, log)
		}
	}
}

// The string-body variant logs the same two dumps and masks them the same way.
func TestHTTPClientDoBodyAsJSONStringMasksCredentialHeadersInItsDumps(t *testing.T) {
	logged := captureLog(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", "upstream_session=cookie-xyz")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	aepr, _ := proxyRequest(t)
	response, err := aepr.HTTPClientDoBodyAsJSONString(http.MethodPost, upstream.URL+"/things", `{"amount":10}`,
		map[string]string{"X-Api-Key": "key-secret-1"})
	if err != nil {
		t.Fatalf("HTTPClientDoBodyAsJSONString: %v", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if string(body) != `{"ok":true}` {
		t.Errorf("response body after the dump: %q", body)
	}

	log := logged.String()
	for _, raw := range []string{"key-secret-1", "cookie-xyz"} {
		if strings.Contains(log, raw) {
			t.Errorf("log carries %q in clear:\n%s", raw, log)
		}
	}
	for _, line := range []string{"X-Api-Key: ***REDACTED***", "Set-Cookie: ***REDACTED***", "amount"} {
		if !strings.Contains(log, line) {
			t.Errorf("log should carry %q:\n%s", line, log)
		}
	}
}
