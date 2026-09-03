package utils

import "testing"

// BUG-SEC-220: what happens to a field that matches NO rule is now the host's choice, because
// neither answer is right everywhere. ALLOW keeps a developer's debug dump readable; DENY keeps a
// field nobody remembered to name out of a log that leaves the machine.
func TestMaskDefault_AllowIsTheDefault(t *testing.T) {
	resetMaskState(t)

	// An unmatched field is logged verbatim - this is the behaviour every existing consumer has,
	// and the switch must not change it until a host asks.
	if got := MaskSensitiveValue("kode_cabang", "0231"); got != "0231" {
		t.Errorf("default should be ALLOW: got %v, want the raw value", got)
	}
	if MaskDefaultDeny() {
		t.Error("MaskDefaultDeny() should report false before any host sets it")
	}
}

func TestMaskDefault_DenyMasksUnmatchedFields(t *testing.T) {
	resetMaskState(t)
	SetMaskDefaultDeny(true)

	for _, f := range []string{"kode_cabang", "norek_tujuan", "whatever_new_field", "foto_baru"} {
		if got := MaskSensitiveValue(f, "sensitive-looking-value"); got != "********" {
			t.Errorf("under DENY, unmatched %q should be masked, got %v", f, got)
		}
	}
	if !MaskDefaultDeny() {
		t.Error("MaskDefaultDeny() should report true once set")
	}
}

func TestMaskDefault_DenyRespectsTheAllowlist(t *testing.T) {
	resetMaskState(t)
	SetMaskDefaultDeny(true)
	SetLogAllowedFields([]string{"Trx_Type", "response_code", "kode_cabang"})

	// Named fields stay readable, and the match is case-insensitive on the name.
	for _, f := range []string{"trx_type", "TRX_TYPE", "response_code", "kode_cabang"} {
		if got := MaskSensitiveValue(f, "keep-me"); got != "keep-me" {
			t.Errorf("allowlisted %q should stay readable, got %v", f, got)
		}
	}
	// Anything else still goes.
	if got := MaskSensitiveValue("not_listed", "keep-me"); got != "********" {
		t.Errorf("un-listed field should be masked under DENY, got %v", got)
	}
}

// The allowlist is matched EXACTLY, unlike the deny rules which are substring-matched on purpose.
// Allowing "id" must not un-mask "national_id" - that inversion is the whole reason for the
// asymmetry, and it is the mistake this test exists to prevent.
func TestMaskDefault_AllowlistDoesNotMatchSubstrings(t *testing.T) {
	resetMaskState(t)
	SetMaskDefaultDeny(true)
	SetLogAllowedFields([]string{"id"})

	if got := MaskSensitiveValue("id", "42"); got != "42" {
		t.Errorf("the exact allowlisted name should pass, got %v", got)
	}
	for _, f := range []string{"national_id", "id_number", "customer_id"} {
		if got := MaskSensitiveValue(f, "3201234567890001"); got != "********" {
			t.Errorf("allowing \"id\" must NOT un-mask %q, got %v", f, got)
		}
	}
}

// The switch changes only the UNMATCHED case. Credentials and named PII must behave identically
// under both defaults, or turning DENY on would quietly alter what a credential looks like.
func TestMaskDefault_DoesNotAffectCredentialsOrNamedPII(t *testing.T) {
	for _, deny := range []bool{false, true} {
		resetMaskState(t)
		SetMaskDefaultDeny(deny)
		SetMaskRules(map[string]MaskRule{"nik": {Front: 5, Back: 2}})

		if got := MaskSensitiveValue("password", "hunter2"); got != "********" {
			t.Errorf("deny=%v: credential should be fully masked, got %v", deny, got)
		}
		got := MaskSensitiveValue("nik", "3201234567890001")
		if got == "3201234567890001" || got == "********" {
			t.Errorf("deny=%v: named PII should still be PARTIALLY masked, got %v", deny, got)
		}
	}
}

// resetMaskState puts the package back to its zero configuration and restores it afterwards, so
// these tests cannot leak state into each other or into the rest of the package's tests.
func resetMaskState(t *testing.T) {
	t.Helper()
	prevRules, prevStrict := maskRules, maskStrict
	prevDeny, prevAllowed := maskDefaultDeny, logAllowedFields
	t.Cleanup(func() {
		maskRules, maskStrict = prevRules, prevStrict
		maskDefaultDeny, logAllowedFields = prevDeny, prevAllowed
	})
	maskRules, maskStrict = map[string]MaskRule{}, false
	maskDefaultDeny, logAllowedFields = false, map[string]bool{}
}
