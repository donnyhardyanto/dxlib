package api

import (
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// The decrypted-body dump used to mask top-level keys only, so a body that nests everything
// under "params" printed its personal data in clear. The dump now goes through the deep walk,
// and the host's rules reach a field three levels down, in an object and in an array.
func TestDecryptedDumpMasksNestedFields(t *testing.T) {
	orig := logDecryptedBody
	t.Cleanup(func() { SetLogDecryptedBody(orig) })
	SetLogDecryptedBody(true)
	utils.SetMaskRules(map[string]utils.MaskRule{
		"national_id": {Front: 4, Back: 2},
		"email":       {Kind: utils.MaskKindEmail},
		"full_name":   {Kind: utils.MaskKindInitials},
		"location":    {Kind: utils.MaskKindLocation},
	})
	t.Cleanup(func() { utils.SetMaskRules(map[string]utils.MaskRule{}) })

	aepr := &DXAPIEndPointRequest{
		EffectiveRequestHeader: map[string]string{"Authorization": "Bearer abc"},
		DecryptedRequestBody: utils.JSON{
			"params": utils.JSON{
				"applicant": utils.JSON{
					"full_name":   "Budi Santoso",
					"national_id": "3175012345678901",
					"email":       "budi@mail.com",
					"password":    "hunter2",
					"location":    utils.JSON{"lat": -6.914744, "lng": 107.609810},
					"record_id":   int64(9007199254740993),
				},
				"relatives": []utils.JSON{{"full_name": "Siti Aminah", "national_id": "3175019876543210"}},
			},
		},
	}

	dump := aepr.DecryptedRequestDumpAsString()
	for _, raw := range []string{"Budi Santoso", "3175012345678901", "budi@mail.com", "hunter2", "-6.914744", "107.60981", "Siti Aminah", "3175019876543210", "Bearer abc"} {
		if strings.Contains(dump, raw) {
			t.Errorf("dump carries %q in clear:\n%s", raw, dump)
		}
	}
	for _, masked := range []string{"B*** S***", "3175***01", "b***@ma***.com", "********", "-6.91", "107.61", "S*** A***", "3175***10", "9007199254740993"} {
		if !strings.Contains(dump, masked) {
			t.Errorf("dump should carry %q:\n%s", masked, dump)
		}
	}
}
