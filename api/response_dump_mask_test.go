package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// Every response but a 200 is dumped to the log before encryption. That dump used to be the
// marshalled body in clear, so a 201 that returns the record just created, or a relayed
// upstream error, put the caller's personal data in the log. The dump has to go through the
// masker with the host's rules at every depth, while the client still receives the real body.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func newJSONRequest(rec *httptest.ResponseRecorder) *DXAPIEndPointRequest {
	var rw http.ResponseWriter = rec
	return &DXAPIEndPointRequest{
		ResponseWriter: &rw,
		EndPoint:       &DXAPIEndPoint{EndPointType: EndPointTypeHTTPJSON},
	}
}

func TestWriteResponseAsJSONDumpsNon200MaskedAtEveryDepth(t *testing.T) {
	utils.SetMaskRules(map[string]utils.MaskRule{"national_id": {Front: 4, Back: 2}, "full_name": {Front: 1}, "customer_id": {Front: 4, Back: 2}})
	t.Cleanup(func() { utils.SetMaskRules(map[string]utils.MaskRule{}) })
	logged := captureLog(t)

	rec := httptest.NewRecorder()
	aepr := newJSONRequest(rec)
	aepr.WriteResponseAsJSON(http.StatusCreated, nil, utils.JSON{
		"data": utils.JSON{
			"applicant": utils.JSON{
				"full_name":     "Budi Santoso",
				"national_id":   "3175012345678901",
				"session_token": "tok-abc",
				"record_id":     int64(9007199254740993),
				"customer_id":   int64(3175099999999999),
			},
			"relatives": []utils.JSON{{"full_name": "Siti Aminah", "national_id": "3175019876543210"}},
		},
	})

	log := logged.String()
	if !strings.Contains(log, "RESPONSE_DUMP_BEFORE_ENCRYPT") {
		t.Fatalf("a 201 should still be dumped, log: %s", log)
	}
	for _, raw := range []string{"Budi Santoso", "3175012345678901", "tok-abc", "Siti Aminah", "3175019876543210", "3175099999999999", "e+15"} {
		if strings.Contains(log, raw) {
			t.Errorf("log carries %q in clear:\n%s", raw, log)
		}
	}
	// Numbers keep every digit: 9007199254740993 is one past what a float64 can hold, and a
	// numeric field under a PII rule is masked on its digits, not on an exponent form.
	for _, masked := range []string{"3175***01", "3175***10", "3175***99", "********", "status_code=201", "9007199254740993"} {
		if !strings.Contains(log, masked) {
			t.Errorf("log should carry %q:\n%s", masked, log)
		}
	}

	// The client still gets the real body: the masking is for the log only.
	body := rec.Body.String()
	for _, raw := range []string{"Budi Santoso", "3175012345678901", "Siti Aminah"} {
		if !strings.Contains(body, raw) {
			t.Errorf("response body lost %q: %s", raw, body)
		}
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("status: got %d, want 201", rec.Code)
	}
}

func TestWriteResponseAsJSONDoesNotDumpA200(t *testing.T) {
	logged := captureLog(t)

	rec := httptest.NewRecorder()
	aepr := newJSONRequest(rec)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"data": utils.JSON{"national_id": "3175012345678901"}})

	if log := logged.String(); strings.Contains(log, "RESPONSE_DUMP_BEFORE_ENCRYPT") || strings.Contains(log, "3175012345678901") {
		t.Errorf("a 200 must not be dumped:\n%s", log)
	}
	if !strings.Contains(rec.Body.String(), "3175012345678901") {
		t.Errorf("the 200 body changed: %s", rec.Body.String())
	}
}

// The dump never falls back to the raw bytes when they cannot be read back.
func TestMaskedResponseDumpNeverReturnsRawBytes(t *testing.T) {
	got := maskedResponseDump([]byte(`not json {"national_id": "3175012345678901"}`))
	if strings.Contains(got, "3175012345678901") || !strings.Contains(got, "not dumped") {
		t.Errorf("unreadable body should be withheld, got %q", got)
	}
}
