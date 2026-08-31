package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donnyhardyanto/dxlib/utils"
)

// proxyRequest builds the minimum DXAPIEndPointRequest HTTPClientDo touches.
func proxyRequest(t *testing.T) (*DXAPIEndPointRequest, *httptest.ResponseRecorder) {
	t.Helper()

	recorder := httptest.NewRecorder()
	var responseWriter http.ResponseWriter = recorder
	return &DXAPIEndPointRequest{
		Id:             "test",
		EndPoint:       &DXAPIEndPoint{Uri: "/proxied"},
		Request:        httptest.NewRequest(http.MethodPost, "/proxied", nil),
		ResponseWriter: &responseWriter,
	}, recorder
}

// A failed exchange with a downstream service is 502, not 422.
//
// 422 says the caller sent something unprocessable, which sends an operator to
// look at the request body when the fault is entirely downstream. It also had a
// reason label copied from the DumpRequest branch above it.
//
// The status matters beyond its own accuracy: WriteResponse* is
// first-writer-wins, so the 422 written here suppressed HTTPClient2's
// DIAL_ERROR and ProxyHTTPAPIClient's own 502 — both of which already meant to
// answer 502 for exactly this condition and could not.
func TestHTTPClientDoAnswers502WhenTheExchangeFails(t *testing.T) {
	// A listener that accepts and closes without answering: the connection is
	// established, so this is an exchange failure rather than a refused dial.
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hijacker, ok := w.(http.Hijacker); ok {
			conn, _, err := hijacker.Hijack()
			if err == nil {
				_ = conn.Close()
				return
			}
		}
		panic("the test server cannot hijack, so the exchange cannot be failed")
	}))
	defer downstream.Close()

	aepr, recorder := proxyRequest(t)
	if _, err := aepr.HTTPClientDo(http.MethodPost, downstream.URL, utils.JSON{"a": 1}, nil); err == nil {
		t.Fatal("HTTPClientDo reported success against a downstream that answered nothing")
	}
	if recorder.Code != http.StatusBadGateway {
		t.Errorf("answered %d, want 502: the caller is fine and the downstream is not", recorder.Code)
	}
}

// The timeout is opt-in. Its zero value is what every release up to v1.118.2
// did, so a consumer that does not set it sees no change at all.
func TestHTTPClientDoTimeoutIsOptIn(t *testing.T) {
	if HTTPClientDoTimeout != 0 {
		t.Fatalf("HTTPClientDoTimeout defaults to %s, want 0 — a non-zero default changes "+
			"behaviour for every consumer that never asked for it", HTTPClientDoTimeout)
	}
}

// And when it is set, a downstream that never answers no longer holds the
// calling goroutine open indefinitely.
func TestHTTPClientDoTimeoutBoundsAWedgedDownstream(t *testing.T) {
	original := HTTPClientDoTimeout
	t.Cleanup(func() { HTTPClientDoTimeout = original })
	HTTPClientDoTimeout = 150 * time.Millisecond

	blocked := make(chan struct{})
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked // never answers until the test lets go
	}))
	defer func() { close(blocked); downstream.Close() }()

	aepr, recorder := proxyRequest(t)
	start := time.Now()
	_, err := aepr.HTTPClientDo(http.MethodPost, downstream.URL, utils.JSON{"a": 1}, nil)
	took := time.Since(start)

	if err == nil {
		t.Fatal("HTTPClientDo reported success against a downstream that never answered")
	}
	if took > 5*time.Second {
		t.Errorf("took %s: the timeout did not bound the call", took)
	}
	if recorder.Code != http.StatusBadGateway {
		t.Errorf("answered %d, want 502", recorder.Code)
	}
}
