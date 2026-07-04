package utils

import "testing"

// BUG-SEC-122: PII partial-masking mechanism. Credentials stay FULL; PII partial (NIK 5+2, others
// 2+2); short values full; strict mode forces full; unknown fields untouched.
func TestMaskSensitiveValue_PII(t *testing.T) {
	SetMaskRules(map[string]MaskRule{
		"nik": {5, 2}, "ktp": {5, 2},
		"nama": {2, 2}, "email": {2, 2}, "alamat": {2, 2}, "phone": {2, 2},
	})
	SetMaskStrict(false)
	t.Cleanup(func() { SetMaskRules(map[string]MaskRule{}); SetMaskStrict(false) })

	cases := []struct {
		name, field string
		val, want   interface{}
	}{
		{"nik 16-digit 5+2", "nik", "3175012345678901", "31750****01"},
		{"ktp keyword", "no_ktp", "3175012345678901", "31750****01"},
		{"nama 2+2", "nama", "Budi Santoso", "Bu****so"},
		{"email 2+2", "email", "budi@mail.com", "bu****om"},
		{"credential stays FULL (not partial)", "password", "supersecret", "********"},
		{"token stays FULL", "access_token", "abcdefങ12345", "********"},
		{"unknown field untouched", "trx_type", "LOGIN", "LOGIN"},
		{"short PII → full mask", "nama", "Bud", "********"},     // len 3 < 2+2+2
		{"short nik → full mask", "nik", "12345678", "********"}, // len 8 < 5+2+2
	}
	for _, c := range cases {
		if got := MaskSensitiveValue(c.field, c.val); got != c.want {
			t.Errorf("%s: MaskSensitiveValue(%q,%v)=%v, want %v", c.name, c.field, c.val, got, c.want)
		}
	}

	// strict mode → PII becomes full mask too (prod/compliance)
	SetMaskStrict(true)
	if got := MaskSensitiveValue("nik", "3175012345678901"); got != "********" {
		t.Errorf("strict: nik=%v, want ******** (full)", got)
	}
	if got := MaskSensitiveValue("trx_type", "LOGIN"); got != "LOGIN" {
		t.Errorf("strict must not touch non-PII: trx_type=%v", got)
	}
}
