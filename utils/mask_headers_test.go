package utils

import (
	"net/http"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib"
)

func resetHeaderState(t *testing.T) {
	t.Helper()
	prevHost := hostCredentialHeaderNames
	prevDebug, prevOverride := dxlib.IsDebug, OverrideShowPasswordOnLog
	t.Cleanup(func() {
		hostCredentialHeaderNames = prevHost
		dxlib.IsDebug, OverrideShowPasswordOnLog = prevDebug, prevOverride
	})
	hostCredentialHeaderNames = map[string]bool{}
}

// A credential header is one the built-in list names, one the host declares, or one whose name
// holds a credential keyword. The match ignores case, and a plain header is not caught.
func TestIsCredentialHeader(t *testing.T) {
	resetHeaderState(t)
	SetCredentialHeaders([]string{"X-Tenant-Access", " x-partner-signature "})

	for _, name := range []string{
		"Authorization", "authorization", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-Api-Key",
		"X-Auth-Token", "X-Csrf-Token", "WWW-Authenticate",
		"X-Upstream-Token", "X-Session-Id", "X-Client-Secret", "X-Tenant-Key", // by keyword
		"X-Tenant-Access", "x-tenant-access", "X-Partner-Signature", // by the host's list
	} {
		if !IsCredentialHeader(name) {
			t.Errorf("%s should be a credential header", name)
		}
	}
	for _, name := range []string{"Content-Type", "Accept", "X-Request-Id", "User-Agent", "Content-Length", "X-Forwarded-For"} {
		if IsCredentialHeader(name) {
			t.Errorf("%s should not be a credential header", name)
		}
	}
}

// A credential header's value is fully masked, never partially; a plain value passes through;
// the debug override shows a credential raw, as it does for a body.
func TestMaskHeaderValue(t *testing.T) {
	resetHeaderState(t)
	SetCredentialHeaders([]string{"X-Tenant-Access"})

	if got := MaskHeaderValue("Authorization", "Bearer abc.def.ghi"); got != "***REDACTED***" {
		t.Errorf("Authorization: got %q", got)
	}
	if got := MaskHeaderValue("X-Tenant-Access", "tenant-secret"); got != "***REDACTED***" {
		t.Errorf("host-declared header: got %q", got)
	}
	if got := MaskHeaderValue("X-Request-Id", "req-1"); got != "req-1" {
		t.Errorf("plain header: got %q", got)
	}

	dxlib.IsDebug, OverrideShowPasswordOnLog = true, true
	if got := MaskHeaderValue("Authorization", "Bearer abc"); got != "Bearer abc" {
		t.Errorf("debug override: got %q", got)
	}
}

// The rendered headers come in name order, one line per value, with every credential value
// masked, excluded names left out, and a line break inside a value flattened so a value cannot
// forge a header line of its own.
func TestWriteHeadersForLog(t *testing.T) {
	resetHeaderState(t)

	h := http.Header{}
	h.Add("X-Request-Id", "req-1")
	h.Add("Cookie", "session=s3cr3t")
	h.Add("Cookie", "csrf=t0k3n")
	h.Add("Authorization", "Bearer abc")
	h.Add("Host", "example.test")
	h.Add("Content-Type", "application/json")
	h.Add("X-Note", "line one\r\nX-Forged: yes")

	got := WriteHeadersForLog(h, map[string]bool{"Host": true})
	want := "Authorization: ***REDACTED***\r\n" +
		"Content-Type: application/json\r\n" +
		"Cookie: ***REDACTED***\r\n" +
		"Cookie: ***REDACTED***\r\n" +
		"X-Note: line one X-Forged: yes\r\n" +
		"X-Request-Id: req-1\r\n"
	if got != want {
		t.Errorf("headers:\n got %q\nwant %q", got, want)
	}
	for _, raw := range []string{"s3cr3t", "t0k3n", "Bearer abc", "example.test", "\r\nX-Forged"} {
		if strings.Contains(got, raw) {
			t.Errorf("rendered headers carry %q", raw)
		}
	}
	if got := WriteHeadersForLog(nil, nil); got != "" {
		t.Errorf("nil headers: got %q", got)
	}
}
