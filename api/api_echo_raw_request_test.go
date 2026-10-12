package api

import (
	"testing"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/utils"
)

// ApplyConfigurations reads echo-raw-request from the API's block, and an API whose block
// does not name it keeps the default, off: a raw echo is only ever an explicit choice.
func TestApplyConfigurationsReadsEchoRawRequest(t *testing.T) {
	data := utils.JSON{
		"masked": utils.JSON{"address": "127.0.0.1:0"},
		"raw":    utils.JSON{"address": "127.0.0.1:0", "echo-raw-request": true},
	}
	dxlibConfiguration.Manager.Configurations["api-echo-test"] = &dxlibConfiguration.DXConfiguration{NameId: "api-echo-test", Data: &data}
	t.Cleanup(func() { delete(dxlibConfiguration.Manager.Configurations, "api-echo-test") })

	for name, want := range map[string]bool{"masked": false, "raw": true} {
		a, _ := Manager.NewAPI(name)
		t.Cleanup(func() { delete(Manager.APIs, name) })
		if err := a.ApplyConfigurations("api-echo-test"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if a.EchoRawRequest != want {
			t.Errorf("%s: EchoRawRequest = %v, want %v", name, a.EchoRawRequest, want)
		}
	}
}
