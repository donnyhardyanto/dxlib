package api

import (
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
)

// The JSON Schema bounds a parameter carries beyond its type: minimum,
// exclusiveMinimum, maximum, exclusiveMaximum and multipleOf on a number;
// minLength, maxLength and pattern on a string; minItems, maxItems and
// uniqueItems on an array; const on a scalar. Each belongs to one JSON type,
// and a bound on any other type is refused in both directions, because the
// validator would not check it and the document would promise what the
// server does not do. The bound a dxlib type already implies (int64p is
// minimum 1) stays with the type: the parameter carries only what is stricter.

// openAPIBoundKind names the JSON type each bound applies to.
var openAPIBoundKind = map[string]string{
	"minimum": "number", "exclusiveMinimum": "number", "maximum": "number",
	"exclusiveMaximum": "number", "multipleOf": "number",
	"minLength": "string", "maxLength": "string", "pattern": "string",
	"minItems": "array", "maxItems": "array", "uniqueItems": "array",
	"const": "scalar",
}

// openAPIBoundApplies says whether a bound of the given kind may sit on a
// value of the given JSON type.
func openAPIBoundApplies(kind, jsonType string) bool {
	switch kind {
	case "number":
		return jsonType == "integer" || jsonType == "number"
	case "scalar":
		return jsonType == "string" || jsonType == "integer" || jsonType == "number" || jsonType == "boolean"
	}
	return kind == jsonType
}

// openAPIBoundsPresent lists the bounds that are set, in a fixed order.
func openAPIBoundsPresent(minimum, exclusiveMinimum, maximum, exclusiveMaximum, multipleOf *float64,
	minLength, maxLength *int, pattern string, minItems, maxItems *int, uniqueItems bool, hasConst bool) []string {
	var out []string
	add := func(set bool, name string) {
		if set {
			out = append(out, name)
		}
	}
	add(minimum != nil, "minimum")
	add(exclusiveMinimum != nil, "exclusiveMinimum")
	add(maximum != nil, "maximum")
	add(exclusiveMaximum != nil, "exclusiveMaximum")
	add(multipleOf != nil, "multipleOf")
	add(minLength != nil, "minLength")
	add(maxLength != nil, "maxLength")
	add(pattern != "", "pattern")
	add(minItems != nil, "minItems")
	add(maxItems != nil, "maxItems")
	add(uniqueItems, "uniqueItems")
	add(hasConst, "const")
	return out
}

func openAPISchemaBounds(s *DXOpenAPISchema) []string {
	return openAPIBoundsPresent(s.Minimum, s.ExclusiveMinimum, s.Maximum, s.ExclusiveMaximum, s.MultipleOf,
		s.MinLength, s.MaxLength, s.Pattern, s.MinItems, s.MaxItems, s.UniqueItems, s.Const != nil)
}

func openAPIParameterBounds(p *DXAPIEndPointParameter) []string {
	return openAPIBoundsPresent(p.Minimum, p.ExclusiveMinimum, p.Maximum, p.ExclusiveMaximum, p.MultipleOf,
		p.MinLength, p.MaxLength, p.Pattern, p.MinItems, p.MaxItems, p.UniqueItems, p.Const != nil)
}

// openAPISchemaBoundNames is the bounds a schema states, joined, or "" for
// none. It is for a schema that becomes no parameter of its own -- array
// items, a map's values, a body object -- where no bound can be checked.
func openAPISchemaBoundNames(s *DXOpenAPISchema) string {
	if s == nil {
		return ""
	}
	return strings.Join(openAPISchemaBounds(s), "/")
}

// openAPIBoundsFromSchema carries a schema's bounds onto the parameter read
// from it, once its dxlib type is known.
func openAPIBoundsFromSchema(s *DXOpenAPISchema, p *DXAPIEndPointParameter, r *openAPISchemaResolver, pointer string) error {
	m := openAPITypeTable[p.Type]
	for _, name := range openAPISchemaBounds(s) {
		if !openAPIBoundApplies(openAPIBoundKind[name], m.jsonType) {
			return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRAINT:%s:ON_%s:%s", name, m.jsonType, pointer)
		}
	}
	if s.Const != nil {
		if err := openAPIConstFits(*s.Const, m.jsonType); err != nil {
			return errors.Wrapf(err, "OPENAPI_AT:%s/const", pointer)
		}
	}
	// Only the properties of a json type, and of an array-json-template's
	// item object, become parameters. Everything else under this schema is
	// never validated, so a bound anywhere in it is refused.
	if s.Items != nil && p.Type != dxlibTypes.APIParameterTypeArrayJSONTemplate {
		if err := openAPINoBoundsBelow(s.Items, r, pointer+"/items"); err != nil {
			return err
		}
	}
	if s.AdditionalProperties != nil {
		if err := openAPINoBoundsBelow(s.AdditionalProperties, r, pointer+"/additionalProperties"); err != nil {
			return err
		}
	}
	if s.Properties != nil && p.Type != dxlibTypes.APIParameterTypeJSON {
		for _, name := range s.Properties.Keys() {
			child, _ := s.Properties.Get(name)
			if err := openAPINoBoundsBelow(child, r, pointer+"/properties/"+openAPIPointerEscape(name)); err != nil {
				return err
			}
		}
	}
	var err error
	if p.Minimum, err = openAPIBeyondImplied("minimum", s.Minimum, m.minimum, p, pointer); err != nil {
		return err
	}
	if p.ExclusiveMinimum, err = openAPIBeyondImplied("exclusiveMinimum", s.ExclusiveMinimum, m.exclusiveMinimum, p, pointer); err != nil {
		return err
	}
	if s.MinLength != nil {
		implied := (*float64)(nil)
		if m.minLength != nil {
			implied = openAPIFloat(float64(*m.minLength))
		}
		beyond, err := openAPIBeyondImplied("minLength", openAPIFloat(float64(*s.MinLength)), implied, p, pointer)
		if err != nil {
			return err
		}
		if beyond != nil {
			p.MinLength = openAPIInt(*s.MinLength)
		}
	}
	p.Maximum, p.ExclusiveMaximum, p.MultipleOf = s.Maximum, s.ExclusiveMaximum, s.MultipleOf
	p.MaxLength, p.Pattern = s.MaxLength, s.Pattern
	p.MinItems, p.MaxItems, p.UniqueItems = s.MinItems, s.MaxItems, s.UniqueItems
	if s.Const != nil {
		v := *s.Const
		p.Const = &v
	}
	return nil
}

// openAPINoBoundsBelow refuses a bound anywhere in a schema that becomes no
// parameter, following references.
func openAPINoBoundsBelow(s *DXOpenAPISchema, r *openAPISchemaResolver, pointer string) error {
	resolved, release, err := r.resolve(s, pointer)
	if err != nil {
		return err
	}
	defer release()
	if what := openAPISchemaBoundNames(resolved); what != "" {
		return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRAINT:%s:NOT_CHECKED_HERE:%s", what, pointer)
	}
	if resolved.Items != nil {
		if err := openAPINoBoundsBelow(resolved.Items, r, pointer+"/items"); err != nil {
			return err
		}
	}
	if resolved.AdditionalProperties != nil {
		if err := openAPINoBoundsBelow(resolved.AdditionalProperties, r, pointer+"/additionalProperties"); err != nil {
			return err
		}
	}
	if resolved.Properties != nil {
		for _, name := range resolved.Properties.Keys() {
			child, _ := resolved.Properties.Get(name)
			if err := openAPINoBoundsBelow(child, r, pointer+"/properties/"+openAPIPointerEscape(name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// openAPIConstFits refuses a const of another JSON type than the value's:
// a string const on an integer would never equal a resolved number.
func openAPIConstFits(c any, jsonType string) error {
	if !openAPIScalarFits(c, jsonType) {
		return errors.Errorf("OPENAPI_CONST_OF_ANOTHER_TYPE:%v(%T):ON_%s", c, c, jsonType)
	}
	return nil
}

// openAPIScalarFits says whether a scalar read from a document or declared in
// Go is a value of the given JSON type. An integral float is an integer, as
// JSON Schema counts it.
func openAPIScalarFits(c any, jsonType string) bool {
	ok := false
	switch jsonType {
	case "string":
		_, ok = c.(string)
	case "boolean":
		_, ok = c.(bool)
	case "integer":
		switch v := c.(type) {
		case int, int32, int64:
			ok = true
		case float32:
			ok = float64(v) == math.Trunc(float64(v))
		case float64:
			ok = v == math.Trunc(v)
		}
	case "number":
		switch c.(type) {
		case int, int32, int64, float32, float64:
			ok = true
		}
	}
	return ok
}

// openAPIBeyondImplied is the part of a lower bound the dxlib type does not
// already imply: nil when the schema states exactly the type's own bound. A
// bound looser than the type's is refused: the server would refuse values the
// document says it takes.
func openAPIBeyondImplied(name string, stated, implied *float64, p *DXAPIEndPointParameter, pointer string) (*float64, error) {
	if stated == nil {
		return nil, nil
	}
	if implied == nil {
		v := *stated
		return &v, nil
	}
	if *stated < *implied {
		return nil, errors.Errorf("OPENAPI_BOUND_LOOSER_THAN_TYPE:%s=%v:%s(implies %v):%s", name, *stated, p.Type, *implied, pointer)
	}
	if *stated == *implied {
		return nil, nil
	}
	v := *stated
	return &v, nil
}

// openAPIBoundsToSchema writes a declared parameter's bounds into its schema,
// over the bound its type implies, after checking them as the reader would.
func openAPIBoundsToSchema(p *DXAPIEndPointParameter, m openAPITypeMapping, s *DXOpenAPISchema) error {
	if err := p.checkBounds(m.jsonType); err != nil {
		return err
	}
	if p.Minimum != nil {
		if _, err := openAPIBeyondImplied("minimum", p.Minimum, m.minimum, p, p.NameId); err != nil {
			return err
		}
		s.Minimum = openAPIFloat(*p.Minimum)
	}
	if p.ExclusiveMinimum != nil {
		if _, err := openAPIBeyondImplied("exclusiveMinimum", p.ExclusiveMinimum, m.exclusiveMinimum, p, p.NameId); err != nil {
			return err
		}
		s.ExclusiveMinimum = openAPIFloat(*p.ExclusiveMinimum)
	}
	if p.MinLength != nil {
		if m.minLength != nil && *p.MinLength < *m.minLength {
			return errors.Errorf("OPENAPI_BOUND_LOOSER_THAN_TYPE:minLength=%d:%s(implies %d):%s", *p.MinLength, p.Type, *m.minLength, p.NameId)
		}
		s.MinLength = openAPIInt(*p.MinLength)
	}
	s.Maximum, s.ExclusiveMaximum, s.MultipleOf = p.Maximum, p.ExclusiveMaximum, p.MultipleOf
	s.MaxLength, s.Pattern = p.MaxLength, p.Pattern
	s.MinItems, s.MaxItems, s.UniqueItems = p.MinItems, p.MaxItems, p.UniqueItems
	if p.Const != nil {
		v := *p.Const
		s.Const = &v
	}
	return nil
}

// checkBounds refuses a declared bound the validator would not apply or
// could not read: one on the wrong JSON type, a negative count, a multipleOf
// that is not positive, a pattern Go cannot compile, a null const.
func (p *DXAPIEndPointParameter) checkBounds(jsonType string) error {
	for _, name := range openAPIParameterBounds(p) {
		if !openAPIBoundApplies(openAPIBoundKind[name], jsonType) {
			return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRAINT:%s:ON_%s:%s", name, jsonType, p.NameId)
		}
	}
	for name, v := range map[string]*int{"minLength": p.MinLength, "maxLength": p.MaxLength, "minItems": p.MinItems, "maxItems": p.MaxItems} {
		if v != nil && *v < 0 {
			return errors.Errorf("OPENAPI_NEGATIVE_BOUND:%s=%d:%s", name, *v, p.NameId)
		}
	}
	for name, v := range map[string]*float64{"minimum": p.Minimum, "exclusiveMinimum": p.ExclusiveMinimum, "maximum": p.Maximum, "exclusiveMaximum": p.ExclusiveMaximum, "multipleOf": p.MultipleOf} {
		if v != nil && (math.IsNaN(*v) || math.IsInf(*v, 0)) {
			return errors.Errorf("OPENAPI_BOUND_NOT_FINITE:%s=%v:%s", name, *v, p.NameId)
		}
	}
	if p.MultipleOf != nil && !(*p.MultipleOf > 0) {
		return errors.Errorf("OPENAPI_MULTIPLE_OF_NOT_POSITIVE:%v:%s", *p.MultipleOf, p.NameId)
	}
	if p.Pattern != "" {
		if _, err := regexp.Compile(p.Pattern); err != nil {
			return errors.Errorf("OPENAPI_PATTERN_NOT_GO_RE2:%q:%s:%v", p.Pattern, p.NameId, err)
		}
	}
	if p.Const != nil {
		switch (*p.Const).(type) {
		case string, bool, int, int32, int64, float32, float64:
		default:
			return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:const-%s:%s", fmt.Sprintf("%T", *p.Const), p.NameId)
		}
		if err := openAPIConstFits(*p.Const, jsonType); err != nil {
			return errors.Wrapf(err, "OPENAPI_PARAMETER:%s", p.NameId)
		}
	}
	return nil
}
