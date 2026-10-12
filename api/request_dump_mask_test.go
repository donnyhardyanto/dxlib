package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
)

// The raw request dump that rides every error log wrote the body and the query
// as they were sent, so a login, a token refresh or a channel config with its
// secrets went to the log on every error. The dump now masks them by field
// name and leaves out what it cannot mask.

func dumpOf(t *testing.T, method, target, contentType, body string) string {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	aepr := &DXAPIEndPointRequest{Request: req, RequestBodyAsBytes: []byte(body)}
	dump, err := aepr.RequestDumpAsString()
	if err != nil {
		t.Fatalf("RequestDump: %v", err)
	}
	return dump
}

func TestRequestDumpMasksTheBody(t *testing.T) {
	cases := []struct {
		name, contentType, body string
		gone                    []string
		kept                    []string
	}{
		{"json", "application/json",
			`{"user":"ana","password":"hunter2","channel":{"bot_token":"123:ABC","chat_id":42},"keys":[{"private_key":"-----BEGIN"}]}`,
			[]string{"hunter2", "123:ABC", "-----BEGIN"}, []string{`"user":"ana"`, `"chat_id":42`}},
		{"json array", "application/json", `[{"api_key":"AKIA9"},{"name":"x"}]`, []string{"AKIA9"}, []string{`"name":"x"`}},
		{"form", "application/x-www-form-urlencoded", "username=ana&password=hunter2", []string{"hunter2"}, []string{"username=ana"}},
		{"broken json", "application/json", `{"password":"hunter2",`, []string{"hunter2"}, []string{"not valid JSON"}},
		{"plain text", "text/plain", "token hunter2", []string{"hunter2"}, []string{"13 bytes of text/plain"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dump := dumpOf(t, http.MethodPost, "/login", c.contentType, c.body)
			for _, s := range c.gone {
				if strings.Contains(dump, s) {
					t.Errorf("dump carries %q:\n%s", s, dump)
				}
			}
			for _, s := range c.kept {
				if !strings.Contains(dump, s) {
					t.Errorf("dump should carry %q:\n%s", s, dump)
				}
			}
		})
	}
}

func TestRequestDumpMasksTheQuery(t *testing.T) {
	dump := dumpOf(t, http.MethodGet, "/v1/send?access_token=hunter2&page=2", "", "")
	if strings.Contains(dump, "hunter2") {
		t.Fatalf("dump carries the query token:\n%s", dump)
	}
	if !strings.Contains(dump, "GET /v1/send?access_token=") || !strings.Contains(dump, "page=2") {
		t.Fatalf("dump lost the query's shape:\n%s", dump)
	}
}

func TestRequestDumpCutsALongBody(t *testing.T) {
	body := `{"note":"` + strings.Repeat("a", 3000) + `"}`
	dump := dumpOf(t, http.MethodPost, "/x", "application/json", body)
	if !strings.Contains(dump, "truncated") || len(dump) > 2000 {
		t.Fatalf("a long body should be cut, dump is %d bytes", len(dump))
	}
}

// A parameter that fails its type check is named with its value in the error,
// which goes to the log and back to the caller; a credential's value is masked.
func TestParameterErrorMasksACredentialValue(t *testing.T) {
	aepr := &DXAPIEndPointRequest{Log: log.NewLog(nil, t.Context(), "param-mask-test")}
	pv := &DXAPIEndPointRequestParameterValue{
		Owner:    aepr,
		RawValue: 12345678,
		Metadata: DXAPIEndPointParameter{NameId: "pin", Type: "string"},
	}
	if got := pv.maskedForMessage(pv.RawValue); got == 12345678 {
		t.Fatalf("pin value not masked: %v", got)
	}
	pv.Metadata.NameId = "page"
	if got := pv.maskedForMessage(7); got != 7 {
		t.Fatalf("a plain value should be kept: %v", got)
	}
	child := &DXAPIEndPointRequestParameterValue{Owner: aepr, Parent: pv, RawValue: "s3cret"}
	pv.Metadata.NameId = "passwords"
	if got := child.maskedForMessage(child.RawValue); got == "s3cret" {
		t.Fatalf("an array element of a credential should take its parent's name: %v", got)
	}
}

// The type error is answered to the caller as well as logged, so the debug
// override (dxlib.IsDebug with utils.OverrideShowPasswordOnLog), which shows
// raw values in a log, must not send a password or token back in the answer.
func TestParameterErrorAnswerMasksUnderDebugOverride(t *testing.T) {
	prevDebug, prevOverride := dxlib.IsDebug, utils.OverrideShowPasswordOnLog
	t.Cleanup(func() { dxlib.IsDebug, utils.OverrideShowPasswordOnLog = prevDebug, prevOverride })
	dxlib.IsDebug, utils.OverrideShowPasswordOnLog = true, true

	aepr := &DXAPIEndPointRequest{Log: log.NewLog(nil, t.Context(), "param-mask-debug-test")}
	for _, c := range []struct{ name, secret string }{
		{"password", "hunter2-raw-password"},
		{"access_token", "tok-raw-0123456789"},
	} {
		pv := &DXAPIEndPointRequestParameterValue{
			Owner:    aepr,
			RawValue: c.secret,
			Metadata: DXAPIEndPointParameter{NameId: c.name, Type: dxlibTypes.APIParameterTypeArrayString},
		}
		err := pv.validateWhenNotSameWithRawValue("string", c.name)
		if err == nil {
			t.Fatalf("%s: a string sent for an array should fail", c.name)
		}
		if strings.Contains(err.Error(), c.secret) {
			t.Fatalf("%s: the answer carries the raw value under the debug override: %v", c.name, err)
		}
		if !strings.Contains(err.Error(), "***REDACTED***") {
			t.Fatalf("%s: the answer should show the value as redacted: %v", c.name, err)
		}
	}
}
