package tables

import (
	"net/http"
	"testing"

	"github.com/donnyhardyanto/dxlib/api"
)

// A credential failure is answered 401 everywhere in dxlib and dxlib_module, so
// the standard response templates must say so; they once labelled it 409.
func TestStandardResponsePossibilitiesLabelInvalidCredential401(t *testing.T) {
	for name, rp := range map[string]api.DXAPIEndPointResponsePossibilities{
		"Create":      DXAPIEndPointResponsePossibilityCreate,
		"CreateByUid": DXAPIEndPointResponsePossibilityCreateByUid,
		"Read":        DXAPIEndPointResponsePossibilityRead,
		"Update":      DXAPIEndPointResponsePossibilityUpdate,
		"Delete":      DXAPIEndPointResponsePossibilityDelete,
		"List":        DXAPIEndPointResponsePossibilityList,
	} {
		p, ok := rp["invalid_credential"]
		if !ok {
			t.Errorf("%s: no invalid_credential entry", name)
			continue
		}
		if p.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: invalid_credential = %d, want 401", name, p.StatusCode)
		}
		if p.Description != "Invalid credential - 401" {
			t.Errorf("%s: description = %q, want it to name 401", name, p.Description)
		}
		for key, q := range rp {
			if q.StatusCode == http.StatusConflict {
				t.Errorf("%s: %s is labelled 409; nothing in these templates is a conflict", name, key)
			}
		}
	}
}
