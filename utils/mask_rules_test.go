package utils

import "testing"

func TestMaskEmail2by2(t *testing.T) {
	cases := []struct{ in, want string }{
		{"adinda@example.co.id", "ad***da@ex***.co.id"},
		{"adinda@example.com", "ad***da@ex***.com"},
		{"x@mail.example.co.id", "x***@ma***.co.id"},
		{"adinda@mail.example.go.id", "ad***da@ma***.go.id"},
		{"adinda@example.id", "ad***da@ex***.id"},
		{"adinda@example.web.id", "ad***da@ex***.web.id"},

		// A local part of 4 or fewer runes keeps only its first.
		{"x@example.com", "x***@ex***.com"},
		{"abcd@example.com", "a***@ex***.com"},
		{"abcde@example.com", "ab***de@ex***.com"},
		{"@example.com", "***@ex***.com"},

		// A domain rest of 2 or fewer runes keeps only its first.
		{"adinda@ab.co.id", "ad***da@a***.co.id"},
		{"adinda@a.com", "ad***da@a***.com"},
		{"adinda@.com", "ad***da@***.com"},

		// No known ending: the whole domain is its first 2 runes then "***".
		{"adinda@example.xyz", "ad***da@ex***"},
		{"adinda@localhost", "ad***da@lo***"},
		{"adinda@", "ad***da@***"},

		// The ending is matched without case and kept as written.
		{"Adinda@Example.CO.ID", "Ad***da@Ex***.CO.ID"},

		// No "@".
		{"adinda", "***"},
		{"", "***"},

		// The last "@" splits the address.
		{"a@b@example.com", "a***@ex***.com"},

		// Multi-byte runes are kept whole.
		{"dédéçà@dömäin.com", "dé***çà@dö***.com"},
		{"ñoño@ëxample.co.id", "ñ***@ëx***.co.id"},
	}
	for _, c := range cases {
		if got := MaskEmail2by2(c.in); got != c.want {
			t.Errorf("MaskEmail2by2(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestKnownEmailEnding_LongestMatchWins(t *testing.T) {
	cases := map[string]string{
		"example.co.id":      ".co.id",
		"mail.example.ac.id": ".ac.id",
		"example.id":         ".id",
		"example.co":         ".co",
		"example.co.uk":      ".co.uk",
		"example.xyz":        "",
	}
	for domain, want := range cases {
		if got := knownEmailEnding(domain); got != want {
			t.Errorf("knownEmailEnding(%q) = %q, want %q", domain, got, want)
		}
	}
}

func TestMaskNumberRules(t *testing.T) {
	cases := []struct {
		name string
		fn   func(string) string
		in   string
		want string
	}{
		{"account", MaskNumber2by4, "1234561234", "12***1234"},
		{"national id", MaskNumber5by4, "3201012345671234", "32010***1234"},
		{"phone", MaskNumber4by3, "081234567789", "0812***789"},
		{"card", MaskNumber6by4, "4111111111111234", "411111***1234"},

		// Spaces and dashes are dropped before masking.
		{"phone with dashes", MaskNumber4by3, "0812-3456-7789", "0812***789"},
		{"phone with country code", MaskNumber4by3, "+62 812 3456 7789", "+628***789"},
		{"card with spaces", MaskNumber6by4, "4111 1111 1111 1234", "411111***1234"},
		{"account with tab", MaskNumber2by4, "12 3456\t1234", "12***1234"},

		// A value no longer than the kept characters keeps only its first.
		{"short account", MaskNumber2by4, "123456", "1***"},
		{"short national id", MaskNumber5by4, "320101234", "3***"},
		{"short phone", MaskNumber4by3, "0812345", "0***"},
		{"short card", MaskNumber6by4, "4111111234", "4***"},
		{"short after separators", MaskNumber4by3, "08-12-345", "0***"},
		{"one more than kept", MaskNumber4by3, "08123456", "0812***456"},
		{"empty", MaskNumber2by4, "", "***"},
		{"only separators", MaskNumber2by4, " - ", "***"},
	}
	for _, c := range cases {
		if got := c.fn(c.in); got != c.want {
			t.Errorf("%s: (%q) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}

func TestMask2by2(t *testing.T) {
	cases := map[string]string{
		"Pegawai Swasta": "Pe***ta",
		"Laki-laki":      "La***ki",
		"Guru":           "G***",
		"abcde":          "ab***de",
		"Ä":              "Ä***",
		"":               "***",
		"Ünïvérsität":    "Ün***ät",
	}
	for in, want := range cases {
		if got := Mask2by2(in); got != want {
			t.Errorf("Mask2by2(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMaskFixedRules(t *testing.T) {
	if got := MaskFull("Jl. Merdeka 1"); got != "********" {
		t.Errorf("MaskFull = %q", got)
	}
	if got := MaskRedacted("hunter2"); got != "***REDACTED***" {
		t.Errorf("MaskRedacted = %q", got)
	}
	if got := MaskInitials("Donny Hardyanto"); got != "D*** H***" {
		t.Errorf("MaskInitials = %q", got)
	}
	if got := MaskLocation2Decimals(-6.917464); got != -6.92 {
		t.Errorf("MaskLocation2Decimals = %v", got)
	}
	if got := MaskLocation2Decimals("-6.917464,107.619125"); got != "-6.92,107.62" {
		t.Errorf("MaskLocation2Decimals = %v", got)
	}
}

// The named rules route MaskSensitiveValue through the same functions, so a field masks the
// same way in a body dump as in a hand-written log line.
func TestNamedRulesMatchTheFunctions(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{
		"email":      RuleMaskEmail2by2,
		"account":    RuleMaskNumber2by4,
		"nik":        RuleMaskNumber5by4,
		"phone":      RuleMaskNumber4by3,
		"card":       RuleMaskNumber6by4,
		"occupation": RuleMask2by2,
		"name":       RuleMaskInitials,
		"address":    RuleMaskFull,
		"selfie":     RuleMaskRedacted,
		"location":   RuleMaskLocation2Decimals,
	})
	cases := []struct {
		field string
		in    any
		want  any
	}{
		{"email", "adinda@example.co.id", "ad***da@ex***.co.id"},
		{"account", "12-3456-1234", "12***1234"},
		{"nik", "3201012345671234", "32010***1234"},
		{"phone", "0812-3456-7789", "0812***789"},
		{"card", "4111 1111 1111 1234", "411111***1234"},
		{"occupation", "Pegawai Swasta", "Pe***ta"},
		{"name", "Donny Hardyanto", "D*** H***"},
		{"address", "Jl. Merdeka 1", "********"},
		{"selfie", "data:image/jpeg;base64,AAAA", "***REDACTED***"},
		{"location", -6.917464, -6.92},
	}
	for _, c := range cases {
		if got := MaskSensitiveValue(c.field, c.in); got != c.want {
			t.Errorf("%s: MaskSensitiveValue(%v) = %v, want %v", c.field, c.in, got, c.want)
		}
	}

	SetMaskStrict(true)
	for _, c := range cases {
		if got := MaskSensitiveValue(c.field, c.in); got != "********" {
			t.Errorf("strict %s: got %v, want ********", c.field, got)
		}
	}
}
