package oam

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/log"
)

// Ping echoes the request back to its caller and writes a copy to the log. The copy the log
// gets must carry no credential as sent: not a header value, not a query value, not a body
// field, and in no spelling (a byte slice printed with %v spells the body out in decimals).
// The echo is the endpoint's purpose and keeps the request as sent. BUG-SEC-451.
func TestPingLogsNoRawCredential(t *testing.T) {
	const (
		authValue  = "Bearer bearer-secret-7f3a"
		proxyToken = "proxy-token-secret-91c2"
		queryToken = "query-token-secret-4d0e"
		password   = "body-password-secret-5b8f"
	)
	body := fmt.Sprintf(`{"username":"adinda@dailymail.co.id","password":%q}`, password)
	req := httptest.NewRequest(http.MethodGet, "/ping?token="+queryToken+"&unique_id=dev-1", strings.NewReader(body))
	req.Header.Set("Authorization", authValue)
	req.Header.Set("X-Proxy-Token", proxyToken)

	var logged []string
	previous := log.OnError
	log.OnError = func(_ *log.DXLog, _ error, _ log.DXLogLevel, _ string, text string, _ string) error {
		logged = append(logged, text)
		return nil
	}
	defer func() { log.OnError = previous }()

	rec := httptest.NewRecorder()
	var rw http.ResponseWriter = rec
	aepr := &api.DXAPIEndPointRequest{
		Request:        req,
		ResponseWriter: &rw,
		EndPoint:       &api.DXAPIEndPoint{EndPointType: api.EndPointTypeHTTPJSON},
	}
	if err := Ping(aepr); err != nil {
		t.Fatal(err)
	}

	all := strings.Join(logged, "\n")
	if all == "" {
		t.Fatal("Ping wrote nothing to the log")
	}
	for _, secret := range []string{authValue, proxyToken, queryToken, password} {
		if strings.Contains(all, secret) {
			t.Errorf("the log carries %q as sent:\n%s", secret, all)
		}
		decimal := strings.Trim(fmt.Sprint([]byte(secret)), "[]")
		if strings.Contains(all, decimal) {
			t.Errorf("the log carries %q as decimal bytes:\n%s", secret, all)
		}
	}
	if !strings.Contains(all, "***REDACTED***") {
		t.Errorf("no credential was redacted in the log:\n%s", all)
	}

	// The echo is unchanged: the caller gets the request as sent (the body as base64 bytes).
	echo := rec.Body.String()
	for _, sent := range []string{authValue, proxyToken, queryToken, base64.StdEncoding.EncodeToString([]byte(body))} {
		if !strings.Contains(echo, sent) {
			t.Errorf("the echo no longer carries %q:\n%s", sent, echo)
		}
	}
}
