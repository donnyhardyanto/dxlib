package api

import (
	"encoding/json"
	"fmt"
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
func (aeprpv *DXAPIEndPointRequestParameterValue) validateBounds(nameIdPath string) error {
	m := aeprpv.Metadata
	switch openAPITypeTable[m.Type].jsonType {
	case "integer", "number":
		d, ok := boundsDecimal(aeprpv.Value)
		if !ok {
			return nil
		}
		if m.Minimum != nil && d.LessThan(decimal.NewFromFloat(*m.Minimum)) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_BELOW_MINIMUM:%s=%v, minimum=%v", nameIdPath, aeprpv.Value, *m.Minimum)
		}
		if m.ExclusiveMinimum != nil && d.LessThanOrEqual(decimal.NewFromFloat(*m.ExclusiveMinimum)) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_ABOVE_EXCLUSIVE_MINIMUM:%s=%v, exclusiveMinimum=%v", nameIdPath, aeprpv.Value, *m.ExclusiveMinimum)
		}
		if m.Maximum != nil && d.GreaterThan(decimal.NewFromFloat(*m.Maximum)) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_ABOVE_MAXIMUM:%s=%v, maximum=%v", nameIdPath, aeprpv.Value, *m.Maximum)
		}
		if m.ExclusiveMaximum != nil && d.GreaterThanOrEqual(decimal.NewFromFloat(*m.ExclusiveMaximum)) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_BELOW_EXCLUSIVE_MAXIMUM:%s=%v, exclusiveMaximum=%v", nameIdPath, aeprpv.Value, *m.ExclusiveMaximum)
		}
		// In decimal, not float: 0.3 is a multiple of 0.1 here, as a reader
		// of the document expects, and not by math.Mod.
		if m.MultipleOf != nil && *m.MultipleOf > 0 && !d.Mod(decimal.NewFromFloat(*m.MultipleOf)).IsZero() {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_MULTIPLE_OF:%s=%v, multipleOf=%v", nameIdPath, aeprpv.Value, *m.MultipleOf)
		}
	case "string":
		// The string the handler receives: trimmed for the non-empty types.
		// The date and time types resolve to a time.Time, so their bounds
		// apply to the string that was sent.
		s, ok := aeprpv.Value.(string)
		if !ok {
			if s, ok = aeprpv.RawValue.(string); !ok {
				return nil
			}
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
		return nil
	default:
		return nil
	}
	if m.Const != nil && aeprpv.Value != nil {
		// Compared by formatted text, as Enum members are, but exactly.
		if fmt.Sprintf("%v", aeprpv.Value) != fmt.Sprintf("%v", *m.Const) {
			return aeprpv.Owner.Log.WarnAndCreateErrorf("VALUE_NOT_CONST:%s, const=%v", nameIdPath, *m.Const)
		}
	}
	return nil
}

// boundsDecimal reads a resolved numeric value exactly.
func boundsDecimal(v any) (decimal.Decimal, bool) {
	switch x := v.(type) {
	case int64:
		return decimal.NewFromInt(x), true
	case int32:
		return decimal.NewFromInt32(x), true
	case float64:
		return decimal.NewFromFloat(x), true
	case float32:
		return decimal.NewFromFloat32(x), true
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
