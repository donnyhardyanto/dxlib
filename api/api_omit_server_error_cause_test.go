package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// An API made by NewAPI answers the 5xx cause, and ApplyConfigurations keeps
// it answered unless the API's block sets omit-server-error-cause to true:
// withholding the real reason is an explicit choice of the service.
func TestOmitServerErrorCauseIsOffByDefault(t *testing.T) {
	a, _ := Manager.NewAPI("omit-default-unconfigured")
	t.Cleanup(func() { delete(Manager.APIs, "omit-default-unconfigured") })
	if a.OmitServerErrorCause {
		t.Fatalf("NewAPI: OmitServerErrorCause = true, want false")
	}

	data := utils.JSON{
		"unset":        utils.JSON{"address": "127.0.0.1:0"},
		"opt-in":       utils.JSON{"address": "127.0.0.1:0", "omit-server-error-cause": true},
		"explicit-off": utils.JSON{"address": "127.0.0.1:0", "omit-server-error-cause": false},
	}
	dxlibConfiguration.Manager.Configurations["api-omit-test"] = &dxlibConfiguration.DXConfiguration{NameId: "api-omit-test", Data: &data}
	t.Cleanup(func() { delete(dxlibConfiguration.Manager.Configurations, "api-omit-test") })

	const driverText = "pq: relation member does not exist"
	for name, want := range map[string]bool{"unset": false, "opt-in": true, "explicit-off": false} {
		a, _ := Manager.NewAPI(name)
		t.Cleanup(func() { delete(Manager.APIs, name) })
		if err := a.ApplyConfigurations("api-omit-test"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if a.OmitServerErrorCause != want {
			t.Errorf("%s: OmitServerErrorCause = %v, want %v", name, a.OmitServerErrorCause, want)
		}

		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{}`))
		r.Header.Set("Content-Type", "application/json")
		aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
			Owner:              a,
			Uri:                "/probe",
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
		})
		aepr.WriteResponseAsInternalServerError("EXECUTE_ERROR", fmt.Errorf("%s", driverText))
		body := decodeBody(t, rec)
		cause, _ := body["cause"].(string)
		if !want {
			if !strings.Contains(cause, driverText) {
				t.Errorf("%s: the 5xx answer should carry the real cause: %s", name, rec.Body.String())
			}
			continue
		}
		ref, _ := body["error_log_ref"].(string)
		if strings.Contains(rec.Body.String(), "relation") || ref == "" || !strings.Contains(cause, ref) {
			t.Errorf("%s: the 5xx answer should withhold the cause and point to error_log_ref: %s", name, rec.Body.String())
		}
	}
}
