package api

import (
	"testing"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/utils"
)

// An API made by NewAPI withholds the 5xx cause before any configuration is
// applied, and ApplyConfigurations keeps it withheld unless the API's block
// sets omit-server-error-cause to false: answering the error text is an
// explicit choice.
func TestOmitServerErrorCauseIsOnByDefault(t *testing.T) {
	a, _ := Manager.NewAPI("omit-default-unconfigured")
	t.Cleanup(func() { delete(Manager.APIs, "omit-default-unconfigured") })
	if !a.OmitServerErrorCause {
		t.Fatalf("NewAPI: OmitServerErrorCause = false, want true")
	}

	data := utils.JSON{
		"unset":    utils.JSON{"address": "127.0.0.1:0"},
		"explicit": utils.JSON{"address": "127.0.0.1:0", "omit-server-error-cause": true},
		"opt-out":  utils.JSON{"address": "127.0.0.1:0", "omit-server-error-cause": false},
	}
	dxlibConfiguration.Manager.Configurations["api-omit-test"] = &dxlibConfiguration.DXConfiguration{NameId: "api-omit-test", Data: &data}
	t.Cleanup(func() { delete(dxlibConfiguration.Manager.Configurations, "api-omit-test") })

	for name, want := range map[string]bool{"unset": true, "explicit": true, "opt-out": false} {
		a, _ := Manager.NewAPI(name)
		t.Cleanup(func() { delete(Manager.APIs, name) })
		if err := a.ApplyConfigurations("api-omit-test"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if a.OmitServerErrorCause != want {
			t.Errorf("%s: OmitServerErrorCause = %v, want %v", name, a.OmitServerErrorCause, want)
		}
	}
}
