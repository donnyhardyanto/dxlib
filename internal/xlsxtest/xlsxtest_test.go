package xlsxtest

import "testing"

func TestDecodeEscapes(t *testing.T) {
	for in, want := range map[string]string{
		"_x0041_":       "A",
		"_x005F_x0041_": "_x0041_",
		"_x0041_x0042_": "Ax0042_",
		"_x00e9_":       "é",
		"_x004":         "_x004",
		"plain":         "plain",
		// The edges of the four-digit range. A surrogate half is not a rune,
		// so strings.Builder writes U+FFFD for it.
		"_x0000_": "\x00",
		"_xFFFF_": "\uFFFF",
		"_xD800_": "\uFFFD",
		"_xE000_": "\uE000",
	} {
		if got := DecodeEscapes(in); got != want {
			t.Errorf("DecodeEscapes(%q) = %q, want %q", in, got, want)
		}
	}
}
