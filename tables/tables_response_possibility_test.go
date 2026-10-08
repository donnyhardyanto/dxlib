package tables

import (
	"net/http"
	"testing"

	"github.com/donnyhardyanto/dxlib/api"
)

var standardResponsePossibilities = map[string]api.DXAPIEndPointResponsePossibilities{
	"Create":      DXAPIEndPointResponsePossibilityCreate,
	"CreateByUid": DXAPIEndPointResponsePossibilityCreateByUid,
	"Read":        DXAPIEndPointResponsePossibilityRead,
	"Update":      DXAPIEndPointResponsePossibilityUpdate,
	"Delete":      DXAPIEndPointResponsePossibilityDelete,
	"List":        DXAPIEndPointResponsePossibilityList,
}

// A credential failure is answered 401 everywhere in dxlib and dxlib_module, so
// the standard response templates must say so; they once labelled it 409.
func TestStandardResponsePossibilitiesLabelInvalidCredential401(t *testing.T) {
	for name, rp := range standardResponsePossibilities {
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
	}
}

// The server answers 409 for a duplicate key on a create (DoCreate) and for a
// unique-field-group violation on a create or an update (the WithValidation
// paths, through ErrUniqueFieldViolation). The templates those endpoints are
// declared with must list it, under one key, so a spec generated from them
// names the status the server sends. A read, delete or list never sends 409.
func TestStandardResponsePossibilitiesList409WhereTheServerSendsIt(t *testing.T) {
	sends409 := map[string]bool{"Create": true, "CreateByUid": true, "Update": true}
	for name, rp := range standardResponsePossibilities {
		conflicts := []string{}
		for key, q := range rp {
			if q.StatusCode == http.StatusConflict {
				conflicts = append(conflicts, key)
			}
		}
		switch {
		case sends409[name] && len(conflicts) != 1:
			t.Errorf("%s: 409 entries = %v, want exactly one (conflict)", name, conflicts)
		case sends409[name] && conflicts[0] != "conflict":
			t.Errorf("%s: 409 is labelled %q, want conflict", name, conflicts[0])
		case !sends409[name] && len(conflicts) != 0:
			t.Errorf("%s: %v labelled 409; a %s never answers a conflict", name, conflicts, name)
		}
		if p, ok := rp["conflict"]; ok && p.Description != "Conflict - 409" {
			t.Errorf("%s: conflict description = %q, want it to name 409", name, p.Description)
		}
	}
}
