package api

import (
	"net/http"
	"testing"
)

// BUG-SEC-118. Pins the PRECEDENCE ORDER of GetIPAddress, which is the whole
// substance of that fix: X-Proxy-Client-IP (proxy-resolved, and unspoofable on the
// proxied path because api-proxy-v4 builds upstream headers under a default-deny
// allowlist) must beat X-Forwarded-For, which is attacker-settable.
//
// Why a table test and not one case: the bug was an ORDERING bug. A test that only
// checked "the proxy header is read" would still pass if someone re-ordered the
// fallbacks and put XFF first, which is exactly the regression worth catching.
// Each case therefore sets MORE than one source and asserts which one wins.
func TestGetIPAddressPrecedence(t *testing.T) {
	cases := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		want       string
	}{
		{
			name: "proxy header WINS over XFF and X-Real-IP",
			headers: map[string]string{
				"X-Proxy-Client-IP": "203.0.113.9",
				"X-Forwarded-For":   "198.51.100.1",
				"X-Real-IP":         "198.51.100.2",
			},
			remoteAddr: "10.0.0.1:5555",
			want:       "203.0.113.9",
		},
		{
			// The regression this fix was FOR: without the proxy header the code
			// falls back to an attacker-settable value, which is correct behaviour
			// but must not be reached when the trustworthy one is present.
			name: "no proxy header -> XFF, and only the FIRST chain entry",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.7, 70.41.3.18, 150.172.238.178",
			},
			remoteAddr: "10.0.0.1:5555",
			want:       "203.0.113.7",
		},
		{
			name: "XFF chain with surrounding whitespace is trimmed",
			headers: map[string]string{
				"X-Forwarded-For": "  203.0.113.7  ,  70.41.3.18  ",
			},
			want: "203.0.113.7",
		},
		{
			name:    "no proxy header and no XFF -> X-Real-IP",
			headers: map[string]string{"X-Real-IP": "203.0.113.5"},
			want:    "203.0.113.5",
		},
		{
			name:       "no headers at all -> RemoteAddr, port stripped",
			headers:    map[string]string{},
			remoteAddr: "203.0.113.4:44321",
			want:       "203.0.113.4",
		},
		{
			// An EMPTY header must not shadow the next source. A naive
			// `if _, ok := headers[k]; ok` implementation would return "" here,
			// losing the client IP entirely for any client that sends the header
			// blank - so this pins the empty-string fallthrough specifically.
			name: "empty proxy header falls through to XFF",
			headers: map[string]string{
				"X-Proxy-Client-IP": "",
				"X-Forwarded-For":   "203.0.113.6",
			},
			want: "203.0.113.6",
		},
		{
			name: "single-entry XFF is unchanged (no comma to split on)",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.8",
			},
			want: "203.0.113.8",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "http://example.test/x", nil)
			if err != nil {
				t.Fatalf("building request: %v", err)
			}
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			r.RemoteAddr = tc.remoteAddr

			if got := GetIPAddress(r); got != tc.want {
				t.Errorf("GetIPAddress() = %q, want %q", got, tc.want)
			}
		})
	}
}

// A CONTROL for the test above: assert the pre-fix behaviour would FAIL it.
//
// A test that passes both before and after a change proves nothing, so this states
// the discriminating property explicitly. The old code read X-Forwarded-For first,
// so with both headers present it returned the XFF value. If someone reverts the
// ordering, TestGetIPAddressPrecedence's first case fails with exactly this value -
// naming it here means the failure output points straight at the cause.
func TestGetIPAddressWouldHaveFailedBeforeTheFix(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/x", nil)
	r.Header.Set("X-Proxy-Client-IP", "203.0.113.9")
	r.Header.Set("X-Forwarded-For", "198.51.100.1")

	got := GetIPAddress(r)
	if got == "198.51.100.1" {
		t.Fatalf("GetIPAddress returned the X-Forwarded-For value %q - the "+
			"pre-BUG-SEC-118 ordering is back, and audit logs are trusting an "+
			"attacker-settable header over the proxy-resolved one", got)
	}
	if got != "203.0.113.9" {
		t.Fatalf("GetIPAddress() = %q, want the proxy-resolved 203.0.113.9", got)
	}
}
