package api

import (
	"math"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
)

// A JSON number for an int64-family parameter is taken only when it is a
// whole number the int64 can hold, in either sign. Both layers are exercised
// here (the type check in validateWhenNotSameWithRawValue and the conversion
// in rawValueToInt64), because nullable-int64 reaches only the second.
func TestResolveInt64RefusesFractionsAndOutOfRangeNumbers(t *testing.T) {
	negZero := math.Copysign(0, -1)
	for _, c := range []struct {
		name  string
		kind  dxlibTypes.APIParameterType
		raw   any
		want  int64
		fails bool
	}{
		{"fraction", dxlibTypes.APIParameterTypeInt64, 5.5, 0, true},
		{"negative fraction", dxlibTypes.APIParameterTypeInt64, -5.5, 0, true},
		{"negative whole", dxlibTypes.APIParameterTypeInt64, float64(-5), -5, false},
		{"negative zero", dxlibTypes.APIParameterTypeInt64, negZero, 0, false},
		{"2^53", dxlibTypes.APIParameterTypeInt64, float64(1 << 53), 1 << 53, false},
		{"-2^53", dxlibTypes.APIParameterTypeInt64, -float64(1 << 53), -(1 << 53), false},
		{"2^53+1 as string keeps its digits", dxlibTypes.APIParameterTypeInt64, "9007199254740993", 1<<53 + 1, false},
		{"min int64 as number", dxlibTypes.APIParameterTypeInt64, float64(math.MinInt64), math.MinInt64, false},
		{"min int64 as string", dxlibTypes.APIParameterTypeInt64, "-9223372036854775808", math.MinInt64, false},
		{"max int64 as string", dxlibTypes.APIParameterTypeInt64, "9223372036854775807", math.MaxInt64, false},
		{"max int64 as number is 2^63", dxlibTypes.APIParameterTypeInt64, float64(math.MaxInt64), 0, true},
		{"2^63", dxlibTypes.APIParameterTypeInt64, float64(1 << 63), 0, true},
		{"below min int64", dxlibTypes.APIParameterTypeInt64, -9223372036854777856.0, 0, true},
		{"max+1 as string", dxlibTypes.APIParameterTypeInt64, "9223372036854775808", 0, true},
		{"min-1 as string", dxlibTypes.APIParameterTypeInt64, "-9223372036854775809", 0, true},
		{"1e30", dxlibTypes.APIParameterTypeInt64, 1e30, 0, true},
		{"-1e30", dxlibTypes.APIParameterTypeInt64, -1e30, 0, true},
		{"NaN", dxlibTypes.APIParameterTypeInt64, math.NaN(), 0, true},
		{"+Inf", dxlibTypes.APIParameterTypeInt64, math.Inf(1), 0, true},
		{"-Inf", dxlibTypes.APIParameterTypeInt64, math.Inf(-1), 0, true},

		{"id negative fraction", dxlibTypes.APIParameterTypeID, -5.5, 0, true},
		{"id 1e30", dxlibTypes.APIParameterTypeID, 1e30, 0, true},
		{"positive negative fraction", dxlibTypes.APIParameterTypeInt64P, -5.5, 0, true},
		{"positive negative zero", dxlibTypes.APIParameterTypeInt64P, negZero, 0, true},
		{"zero-positive negative fraction", dxlibTypes.APIParameterTypeInt64ZP, -5.5, 0, true},
		{"zero-positive negative zero", dxlibTypes.APIParameterTypeInt64ZP, negZero, 0, false},
		{"zero-positive fraction above zero", dxlibTypes.APIParameterTypeInt64ZP, 0.5, 0, true},

		{"nullable takes a number", dxlibTypes.APIParameterTypeNullableInt64, float64(42), 42, false},
		{"nullable takes a negative number", dxlibTypes.APIParameterTypeNullableInt64, float64(-42), -42, false},
		{"nullable fraction", dxlibTypes.APIParameterTypeNullableInt64, 5.5, 0, true},
		{"nullable negative fraction", dxlibTypes.APIParameterTypeNullableInt64, -5.5, 0, true},
		{"nullable 1e30", dxlibTypes.APIParameterTypeNullableInt64, 1e30, 0, true},
		{"nullable -Inf", dxlibTypes.APIParameterTypeNullableInt64, math.Inf(-1), 0, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := &DXAPIEndPointRequestParameterValue{
				Owner:    &DXAPIEndPointRequest{},
				Metadata: DXAPIEndPointParameter{NameId: "n", Type: c.kind},
				RawValue: c.raw,
			}
			err := v.Validate()
			if c.fails {
				if err == nil {
					t.Fatalf("expected %v to be rejected, got %#v", c.raw, v.Value)
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

// Each element of an array-int64 parameter gets the same check as a scalar.
func TestResolveArrayInt64RefusesFractionsAndOutOfRangeElements(t *testing.T) {
	for _, c := range []struct {
		name  string
		raw   []any
		want  []int64
		fails bool
	}{
		{"whole numbers", []any{float64(1), float64(-2), float64(0)}, []int64{1, -2, 0}, false},
		{"empty", []any{}, []int64{}, false},
		{"fraction", []any{float64(1), 5.5}, nil, true},
		{"negative fraction", []any{float64(1), -5.5}, nil, true},
		{"1e30", []any{1e30}, nil, true},
		{"2^63", []any{float64(1 << 63)}, nil, true},
		{"NaN", []any{math.NaN()}, nil, true},
		{"string element", []any{"1"}, nil, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := &DXAPIEndPointRequestParameterValue{
				Owner:    &DXAPIEndPointRequest{},
				Metadata: DXAPIEndPointParameter{NameId: "ids", Type: dxlibTypes.APIParameterTypeArrayInt64},
				RawValue: c.raw,
			}
			err := v.Validate()
			if c.fails {
				if err == nil {
					t.Fatalf("expected %v to be rejected, got %#v", c.raw, v.Value)
				}
				return
			}
			if err != nil {
				t.Fatalf("rejected %v: %v", c.raw, err)
			}
			got, ok := v.Value.([]int64)
			if !ok || len(got) != len(c.want) {
				t.Fatalf("got %#v, want %v", v.Value, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("got %v, want %v", got, c.want)
				}
			}
		})
	}
}
