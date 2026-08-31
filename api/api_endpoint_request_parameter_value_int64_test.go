package api

import (
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
)

// The mobile clients carry their ids as strings, and the GET branch of
// PreProcessRequest hands every parameter over as one, so both shapes have to
// resolve to the same int64.
func TestResolveInt64AcceptsNumbersAndDecimalStrings(t *testing.T) {
	for _, c := range []struct {
		name  string
		kind  dxlibTypes.APIParameterType
		raw   any
		want  int64
		fails bool
	}{
		{"json number", dxlibTypes.APIParameterTypeInt64, float64(12345), 12345, false},
		{"decimal string", dxlibTypes.APIParameterTypeInt64, "12345", 12345, false},
		{"zero as string", dxlibTypes.APIParameterTypeInt64, "0", 0, false},
		{"negative string", dxlibTypes.APIParameterTypeInt64, "-7", -7, false},
		{"empty string", dxlibTypes.APIParameterTypeInt64, "", 0, true},
		{"not a number", dxlibTypes.APIParameterTypeInt64, "abc", 0, true},
		{"fractional string", dxlibTypes.APIParameterTypeInt64, "5.5", 0, true},
		{"padded string", dxlibTypes.APIParameterTypeInt64, " 5", 0, true},
		{"bool", dxlibTypes.APIParameterTypeInt64, true, 0, true},

		{"zero-positive takes zero", dxlibTypes.APIParameterTypeInt64ZP, "0", 0, false},
		{"zero-positive rejects negative", dxlibTypes.APIParameterTypeInt64ZP, "-1", 0, true},
		{"positive rejects zero", dxlibTypes.APIParameterTypeInt64P, "0", 0, true},
		{"positive takes one", dxlibTypes.APIParameterTypeInt64P, "1", 1, false},
		{"id as string", dxlibTypes.APIParameterTypeID, "99", 99, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := &DXAPIEndPointRequestParameterValue{
				Owner:    &DXAPIEndPointRequest{},
				Metadata: DXAPIEndPointParameter{NameId: "id", Type: c.kind},
				RawValue: c.raw,
			}
			err := v.resolveToInt64XXX("id")
			if c.fails {
				if err == nil {
					t.Fatalf("expected %v to be rejected, got %v", c.raw, v.Value)
				}
				return
			}
			if err != nil {
				t.Fatalf("rejected %v: %v", c.raw, err)
			}
			if got, ok := v.Value.(int64); !ok || got != c.want {
				t.Fatalf("got %#v, want int64 %d", v.Value, c.want)
			}
		})
	}
}

// A nullable id still distinguishes absent from zero.
func TestResolveNullableInt64KeepsNil(t *testing.T) {
	v := &DXAPIEndPointRequestParameterValue{
		Owner:    &DXAPIEndPointRequest{},
		Metadata: DXAPIEndPointParameter{NameId: "id", Type: dxlibTypes.APIParameterTypeNullableInt64},
		RawValue: nil,
	}
	if err := v.resolveToInt64XXX("id"); err != nil {
		t.Fatal(err)
	}
	if v.Value != nil {
		t.Fatalf("got %#v, want nil", v.Value)
	}
}
