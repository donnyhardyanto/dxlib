package api

import (
	"encoding/json"
	"math"
	"regexp"
	"sync"
	"unicode/utf8"

	"github.com/shopspring/decimal"
)

// validateBounds checks the JSON Schema bounds a parameter declares beyond its
// type (DXAPIEndPointParameter, OPENAPI.md section 2.4), after resolveValue
// has turned the raw value into the handler's value. A bound applies to the
// values of its own JSON type only, which is also why the array-json-template
// element containers, which reuse the array's metadata as type json, are
// not checked against the array's item counts.
//
// A refusal names the parameter's path and the bound, never the string value
// itself: a string parameter may carry a secret.
// A bound is compared with the value of its own JSON type: a number in
// decimal, a string and a boolean exactly.
func (aeprpv *DXAPIEndPointRequestParameterValue) validateBounds(nameIdPath string) error {
	m := aeprpv.Metadata
	switch openAPITypeTable[m.Type].jsonType {
	case "integer", "number":
		d, ok := boundsDecimal(aeprpv.Value)
		if !ok {
			return nil
		}
		for _, c := range []struct {
			bound   *float64
			refused func(d, b decimal.Decimal) bool
			code    string
			name    string
		}{
			{m.Minimum, decimal.Decimal.LessThan, "VALUE_BELOW_MINIMUM", "minimum"},
			{m.ExclusiveMinimum, decimal.Decimal.LessThanOrEqual, "VALUE_NOT_ABOVE_EXCLUSIVE_MINIMUM", "exclusiveMinimum"},
			{m.Maximum, decimal.Decimal.GreaterThan, "VALUE_ABOVE_MAXIMUM", "maximum"},
			{m.ExclusiveMaximum, decimal.Decimal.GreaterThanOrEqual, "VALUE_NOT_BELOW_EXCLUSIVE_MAXIMUM", "exclusiveMaximum"},
		} {
			if c.bound == nil {
				continue
			}
			b, ok := boundsDecimalOfBound(*c.bound)
			if !ok {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_BOUND_DECLARED:%s, %s=%v", nameIdPath, c.name, *c.bound)
			}
			if c.refused(d, b) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("%s:%s=%v, %s=%v", c.code, nameIdPath, aeprpv.Value, c.name, *c.bound)
			}
		}
		// In decimal, not float: 0.3 is a multiple of 0.1 here, as a reader
		// of the document expects, and not by math.Mod.
		if m.MultipleOf != nil {
			b, ok := boundsDecimalOfBound(*m.MultipleOf)
			if !ok || !b.IsPositive() {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_BOUND_DECLARED:%s, multipleOf=%v", nameIdPath, *m.MultipleOf)
			}
			if !d.Mod(b).IsZero() {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_MULTIPLE_OF:%s=%v, multipleOf=%v", nameIdPath, aeprpv.Value, *m.MultipleOf)
			}
		}
		if m.Const != nil {
			c, ok := boundsDecimal(*m.Const)
			if !ok || !d.Equal(c) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_CONST:%s=%v, const=%v", nameIdPath, aeprpv.Value, *m.Const)
			}
		}
	case "string":
		// The string as sent, as JSON Schema reads it: untrimmed for the
		// non-empty types, the text of a date or a money amount rather than
		// the time.Time or decimal it resolves to.
		s, ok := aeprpv.RawValue.(string)
		if !ok {
			return nil
		}
		n := utf8.RuneCountInString(s)
		if m.MinLength != nil && n < *m.MinLength {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("STRING_TOO_SHORT:%s, length=%d, minLength=%d", nameIdPath, n, *m.MinLength)
		}
		if m.MaxLength != nil && n > *m.MaxLength {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("STRING_TOO_LONG:%s, length=%d, maxLength=%d", nameIdPath, n, *m.MaxLength)
		}
		if m.Pattern != "" {
			re, err := boundsPattern(m.Pattern)
			if err != nil {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("INVALID_PATTERN_DECLARED:%s, pattern=%s", nameIdPath, m.Pattern)
			}
			if !re.MatchString(s) {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("STRING_DOES_NOT_MATCH_PATTERN:%s, pattern=%s", nameIdPath, m.Pattern)
			}
		}
		if m.Const != nil {
			if c, ok := (*m.Const).(string); !ok || s != c {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_CONST:%s, const=%v", nameIdPath, *m.Const)
			}
		}
	case "boolean":
		if m.Const != nil {
			v, ok := aeprpv.Value.(bool)
			if c, isBool := (*m.Const).(bool); !ok || !isBool || v != c {
				return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_CONST:%s=%v, const=%v", nameIdPath, aeprpv.Value, *m.Const)
			}
		}
	case "array":
		items, ok := aeprpv.RawValue.([]any)
		if !ok {
			return nil
		}
		if m.MinItems != nil && len(items) < *m.MinItems {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("ARRAY_TOO_FEW_ITEMS:%s, count=%d, minItems=%d", nameIdPath, len(items), *m.MinItems)
		}
		if m.MaxItems != nil && len(items) > *m.MaxItems {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("ARRAY_TOO_MANY_ITEMS:%s, count=%d, maxItems=%d", nameIdPath, len(items), *m.MaxItems)
		}
		if m.UniqueItems {
			// encoding/json writes object keys sorted, so two equal objects
			// encode alike whatever order they were sent in.
			seen := make(map[string]int, len(items))
			for i, item := range items {
				b, err := json.Marshal(item)
				if err != nil {
					return aeprpv.Owner.Log.WarnAndCreateErrorf("ARRAY_ITEM_NOT_COMPARABLE:%s[%d]", nameIdPath, i)
				}
				if first, dup := seen[string(b)]; dup {
					return aeprpv.Owner.Log.WarnAndCreateErrorf("ARRAY_ITEMS_NOT_UNIQUE:%s[%d], same as %s[%d]", nameIdPath, i, nameIdPath, first)
				}
				seen[string(b)] = i
			}
		}
	}
	return nil
}

// boundsDecimalOfBound reads a declared bound; NaN and the infinities have no
// decimal and are a broken declaration, not a client error.
func boundsDecimalOfBound(f float64) (decimal.Decimal, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return decimal.Decimal{}, false
	}
	return decimal.NewFromFloat(f), true
}

// boundsDecimal reads a resolved numeric value exactly.
func boundsDecimal(v any) (decimal.Decimal, bool) {
	switch x := v.(type) {
	case int64:
		return decimal.NewFromInt(x), true
	case int32:
		return decimal.NewFromInt32(x), true
	case float64:
		return boundsDecimalOfBound(x)
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return decimal.Decimal{}, false
		}
		return decimal.NewFromFloat32(x), true
	case int:
		return decimal.NewFromInt(int64(x)), true
	case decimal.Decimal:
		return x, true
	}
	return decimal.Decimal{}, false
}

// boundsPatterns caches compiled patterns: a parameter's pattern is fixed
// for the life of the endpoint and is checked on every request.
var boundsPatterns sync.Map

func boundsPattern(pattern string) (*regexp.Regexp, error) {
	if re, ok := boundsPatterns.Load(pattern); ok {
		return re.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	boundsPatterns.Store(pattern, re)
	return re, nil
}
