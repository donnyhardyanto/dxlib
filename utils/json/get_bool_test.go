package json

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// GetBool listed five integer types in one case clause and two float types in
// another. Go only gives the bound variable a concrete type when the clause
// names exactly one type, so `v` stayed `any` and `v != 0` compared an interface
// against the untyped constant 0 -- which defaults to int.
//
// any(int64(0)) != any(int(0)) is true, because the dynamic types differ. So
// GetBool answered true for zero on every width except plain int. That is the
// type every database driver returns for an INT column, and the type
// encoding/json returns for a JSON number, so it was reached constantly:
// enable-browser-security-headers set to 0 in configuration enabled the headers.
func TestGetBoolZeroIsFalseForEveryNumericType(t *testing.T) {
	for _, c := range []struct {
		name string
		zero any
		one  any
	}{
		{"int", int(0), int(1)},
		{"int8", int8(0), int8(1)},
		{"int16", int16(0), int16(1)},
		{"int32", int32(0), int32(1)},
		{"int64", int64(0), int64(1)},
		{"float32", float32(0), float32(1)},
		{"float64", float64(0), float64(1)},
	} {
		got, err := GetBool(utils.JSON{"k": c.zero}, "k")
		if err != nil {
			t.Errorf("%s zero: unexpected error: %v", c.name, err)
		} else if got {
			t.Errorf("%s: GetBool(%v) = true, want false", c.name, c.zero)
		}

		got, err = GetBool(utils.JSON{"k": c.one}, "k")
		if err != nil {
			t.Errorf("%s one: unexpected error: %v", c.name, err)
		} else if !got {
			t.Errorf("%s: GetBool(%v) = false, want true", c.name, c.one)
		}
	}
}

// The float64 case is the one that reached production configuration, since
// encoding/json decodes every JSON number to float64.
func TestGetBoolWithDefaultHonoursAConfiguredZero(t *testing.T) {
	if got := GetBoolWithDefault(utils.JSON{"flag": float64(0)}, "flag", true); got {
		t.Error(`GetBoolWithDefault({"flag": 0}, default true) = true; a configured 0 must win over the default`)
	}
	if got := GetBoolWithDefault(utils.JSON{}, "flag", true); !got {
		t.Error("an absent key must fall back to the default")
	}
}
