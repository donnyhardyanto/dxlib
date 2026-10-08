package utils

import (
	"math"
	"testing"
)

// IfFloatIsInt is a whole-number test in both signs, not a positive-remainder
// test: -5.5 is as fractional as 5.5. NaN and the infinities are not whole
// numbers either.
func TestIfFloatIsInt(t *testing.T) {
	for _, c := range []struct {
		name string
		f    float64
		want bool
	}{
		{"zero", 0, true},
		{"negative zero", math.Copysign(0, -1), true},
		{"positive whole", 5, true},
		{"negative whole", -5, true},
		{"positive fraction", 5.5, false},
		{"negative fraction", -5.5, false},
		{"just below a whole", 4.999999999, false},
		{"just above a negative whole", -4.999999999, false},
		{"2^53", 1 << 53, true},
		{"2^63", 1 << 63, true},
		{"1e30", 1e30, true},
		{"-1e30", -1e30, true},
		{"NaN", math.NaN(), false},
		{"+Inf", math.Inf(1), false},
		{"-Inf", math.Inf(-1), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := IfFloatIsInt(c.f); got != c.want {
				t.Fatalf("IfFloatIsInt(%v) = %v, want %v", c.f, got, c.want)
			}
		})
	}
}

// FloatFitsInt64 adds the int64 bounds. The upper bound is exclusive at 2^63
// because float64(MaxInt64) rounds up to exactly that; the lower bound -2^63 is
// exact and fits.
func TestFloatFitsInt64(t *testing.T) {
	for _, c := range []struct {
		name string
		f    float64
		want bool
	}{
		{"zero", 0, true},
		{"negative zero", math.Copysign(0, -1), true},
		{"positive fraction", 5.5, false},
		{"negative fraction", -5.5, false},
		{"2^53", 1 << 53, true},
		{"-2^53", -(1 << 53), true},
		{"min int64", math.MinInt64, true},
		{"2^63 (what MaxInt64 rounds to)", 1 << 63, false},
		{"float64(MaxInt64)", float64(math.MaxInt64), false},
		{"below min int64", -9223372036854777856.0, false},
		{"1e30", 1e30, false},
		{"-1e30", -1e30, false},
		{"NaN", math.NaN(), false},
		{"+Inf", math.Inf(1), false},
		{"-Inf", math.Inf(-1), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := FloatFitsInt64(c.f); got != c.want {
				t.Fatalf("FloatFitsInt64(%v) = %v, want %v", c.f, got, c.want)
			}
		})
	}
}
