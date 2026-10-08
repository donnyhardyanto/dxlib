package api

import (
	"fmt"
	"strings"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

func boundsFloat(v float64) *float64 { return &v }
func boundsInt(v int) *int           { return &v }
func boundsConst(v any) *any         { return &v }

// The reader takes every bound the validator checks, and refuses a bound
// value that cannot be a bound.
func TestOpenAPIReadsBounds(t *testing.T) {
	doc := openAPIMustRead(t, `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
components:
  schemas:
    N: {type: number, minimum: 1.5, exclusiveMinimum: 1, maximum: 9, exclusiveMaximum: 10, multipleOf: 0.5}
    S: {type: string, minLength: 2, maxLength: 8, pattern: '^[a-z]+$', const: abc}
    A: {type: array, minItems: 1, maxItems: 3, uniqueItems: true}
`)
	n, _ := doc.Components.Schemas.Get("N")
	if *n.Minimum != 1.5 || *n.ExclusiveMinimum != 1 || *n.Maximum != 9 || *n.ExclusiveMaximum != 10 || *n.MultipleOf != 0.5 {
		t.Errorf("N = %+v", n)
	}
	s, _ := doc.Components.Schemas.Get("S")
	if *s.MinLength != 2 || *s.MaxLength != 8 || s.Pattern != "^[a-z]+$" || *s.Const != "abc" {
		t.Errorf("S = %+v", s)
	}
	a, _ := doc.Components.Schemas.Get("A")
	if *a.MinItems != 1 || *a.MaxItems != 3 || !a.UniqueItems {
		t.Errorf("A = %+v", a)
	}

	for name, c := range map[string]struct{ schema, want string }{
		"negative maxLength":        {`{type: string, maxLength: -1}`, "OPENAPI_NEGATIVE_MAX_LENGTH:-1"},
		"negative minItems":         {`{type: array, minItems: -2}`, "OPENAPI_NEGATIVE_MIN_ITEMS:-2"},
		"fractional maxItems":       {`{type: array, maxItems: 1.5}`, "OPENAPI_BAD_INTEGER"},
		"zero multipleOf":           {`{type: number, multipleOf: 0}`, "OPENAPI_MULTIPLE_OF_NOT_POSITIVE"},
		"boolean exclusiveMaximum":  {`{type: number, exclusiveMaximum: true}`, "OPENAPI_WRONG_TYPE"},
		"pattern Go cannot compile": {`{type: string, pattern: '(?=a)'}`, "OPENAPI_PATTERN_NOT_GO_RE2"},
		"null const":                {`{type: string, const: null}`, "OPENAPI_UNSUPPORTED_CONSTRUCT:const-null"},
		"object const":              {`{type: string, const: {a: 1}}`, "non-scalar"},
		"string uniqueItems":        {`{type: array, uniqueItems: "yes"}`, "OPENAPI_WRONG_TYPE"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadOpenAPI([]byte("openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths: {}\ncomponents:\n  schemas:\n    X: " + c.schema + "\n"))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %s", err, c.want)
			}
			if !strings.Contains(err.Error(), "/components/schemas/X") {
				t.Errorf("error does not point at the schema: %v", err)
			}
		})
	}
}

// Binding carries a bound onto the parameter only where the dxlib type does
// not already imply it, and infers the plain type for a bound no type implies.
func TestOpenAPIBindCarriesBounds(t *testing.T) {
	for name, c := range map[string]struct {
		schema string
		check  func(p DXAPIEndPointParameter) bool
	}{
		"minimum 5 is int64 with a minimum": {`{type: integer, minimum: 5}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeInt64 && p.Minimum != nil && *p.Minimum == 5
		}},
		"minimum 1 stays the p type": {`{type: integer, minimum: 1, maximum: 9}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeInt64P && p.Minimum == nil && *p.Maximum == 9
		}},
		"int64p with minimum 5": {`{type: integer, minimum: 5, x-dxlib-type: int64p}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeInt64P && *p.Minimum == 5
		}},
		"exclusiveMinimum on an integer": {`{type: integer, exclusiveMinimum: 3}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeInt64 && *p.ExclusiveMinimum == 3
		}},
		"number with both lower bounds": {`{type: number, minimum: 0, exclusiveMinimum: 0}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeFloat64 && *p.Minimum == 0 && *p.ExclusiveMinimum == 0
		}},
		"float64p with multipleOf": {`{type: number, exclusiveMinimum: 0, multipleOf: 0.25}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeFloat64P && p.ExclusiveMinimum == nil && *p.MultipleOf == 0.25
		}},
		"minLength 3 is string with a minLength": {`{type: string, minLength: 3, maxLength: 9, pattern: '^a'}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeString && *p.MinLength == 3 && *p.MaxLength == 9 && p.Pattern == "^a"
		}},
		"minLength 1 stays non-empty": {`{type: string, minLength: 1, maxLength: 4}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeNonEmptyString && p.MinLength == nil && *p.MaxLength == 4
		}},
		"email with minLength 1": {`{type: string, format: email, minLength: 1}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeEmail && *p.MinLength == 1
		}},
		"array bounds": {`{type: array, items: {type: string}, minItems: 1, maxItems: 2, uniqueItems: true}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeArrayString && *p.MinItems == 1 && *p.MaxItems == 2 && p.UniqueItems
		}},
		"const": {`{type: boolean, const: true}`, func(p DXAPIEndPointParameter) bool {
			return p.Type == dxlibTypes.APIParameterTypeBoolean && p.Const != nil && *p.Const == true
		}},
	} {
		t.Run(name, func(t *testing.T) {
			a := openAPITestAPI(t, "openapi-bind-bounds-"+name)
			doc := openAPIMustRead(t, "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    get:\n      operationId: x\n      parameters:\n        - {name: q, in: query, schema: "+c.schema+"}\n")
			a.RegisterHandler("x", noop)
			if err := a.BindOpenAPI(doc); err != nil {
				t.Fatal(err)
			}
			p := a.FindEndPointByURI("/x").Parameters[0]
			if !c.check(p) {
				t.Errorf("parameter = %+v", p)
			}
		})
	}
}

// A body object and an array-json-template's element object become no
// parameter of their own, so a bound on either could not be checked.
func TestOpenAPIBindRefusesBoundsOnObjects(t *testing.T) {
	for name, schema := range map[string]string{
		"body":          `{type: object, maxItems: 1, properties: {a: {type: string}}}`,
		"template item": `{type: object, properties: {a: {type: array, x-dxlib-type: array-json-template, items: {type: object, const: 1, properties: {b: {type: string}}}}}}`,
		"template ref":  `{type: object, properties: {a: {type: array, x-dxlib-type: array-json-template, items: {$ref: '#/components/schemas/I'}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			a := openAPITestAPI(t, "openapi-bind-bounds-object-"+name)
			doc := openAPIMustRead(t, "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    post:\n      operationId: x\n      requestBody:\n        content:\n          application/json:\n            schema: "+schema+"\ncomponents:\n  schemas:\n    I: {type: object, maxLength: 3, properties: {b: {type: string}}}\n")
			a.RegisterHandler("x", noop)
			err := a.BindOpenAPI(doc)
			if err == nil || !strings.Contains(err.Error(), "OPENAPI_UNSUPPORTED_CONSTRAINT") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

// A declaration with bounds survives emit, read, bind and emit again, and the
// bound declaration is the one that was written.
func TestOpenAPIRoundTripOverBounds(t *testing.T) {
	declared := []DXAPIEndPointParameter{
		{NameId: "age", Type: dxlibTypes.APIParameterTypeInt64P, IsMustExist: true, Minimum: boundsFloat(18), Maximum: boundsFloat(130)},
		{NameId: "code", Type: dxlibTypes.APIParameterTypeString, MinLength: boundsInt(3), MaxLength: boundsInt(8), Pattern: "^[A-Z0-9]+$"},
		{NameId: "kind", Type: dxlibTypes.APIParameterTypeString, Const: boundsConst("member")},
		{NameId: "name", Type: dxlibTypes.APIParameterTypeNonEmptyString, MinLength: boundsInt(2), MaxLength: boundsInt(64)},
		{NameId: "rate", Type: dxlibTypes.APIParameterTypeFloat64P, ExclusiveMaximum: boundsFloat(1), MultipleOf: boundsFloat(0.01)},
		{NameId: "tags", Type: dxlibTypes.APIParameterTypeArrayString, MinItems: boundsInt(1), MaxItems: boundsInt(5), UniqueItems: true},
		{NameId: "lines", Type: dxlibTypes.APIParameterTypeArrayJSONTemplate, MaxItems: boundsInt(10), Children: []DXAPIEndPointParameter{
			{NameId: "qty", Type: dxlibTypes.APIParameterTypeInt32, ExclusiveMinimum: boundsFloat(0), Maximum: boundsFloat(99)},
		}},
	}
	a := openAPITestAPI(t, "roundtrip-bounds")
	a.NewEndPoint("cmdBounds", "", "/cmdBounds", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		declared, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("cmdBoundsQuery", "", "/cmdBoundsQuery", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone,
		[]DXAPIEndPointParameter{{NameId: "page", Type: dxlibTypes.APIParameterTypeInt64ZP, Maximum: boundsFloat(1000)}}, noop, nil, nil, nil, nil, 0, "")
	original, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"minimum": 18`, `"maximum": 130`, `"pattern": "^[A-Z0-9]+$"`, `"const": "member"`, `"minLength": 2`, `"exclusiveMaximum": 1`, `"multipleOf": 0.01`, `"uniqueItems": true`, `"maxItems": 10`} {
		if !strings.Contains(string(original), want) {
			t.Errorf("emitted document lacks %s", want)
		}
	}
	doc := openAPIRoundTrip(t, "bounds", original)

	b := openAPITestAPI(t, "roundtrip-bounds-bound")
	openAPIRegisterEverything(b, doc)
	if err := b.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	got := b.FindEndPointByURI("/cmdBounds").Parameters
	if len(got) != len(declared) {
		t.Fatalf("%d parameters for %d declared", len(got), len(declared))
	}
	for i := range declared {
		w, g := declared[i], got[i]
		if openAPIBoundsText(&w) != openAPIBoundsText(&g) {
			t.Errorf("%s: bounds %s, declared %s", w.NameId, openAPIBoundsText(&g), openAPIBoundsText(&w))
		}
		for j := range w.Children {
			if openAPIBoundsText(&w.Children[j]) != openAPIBoundsText(&g.Children[j]) {
				t.Errorf("%s.%s: bounds %s, declared %s", w.NameId, w.Children[j].NameId, openAPIBoundsText(&g.Children[j]), openAPIBoundsText(&w.Children[j]))
			}
		}
	}
}

// openAPIBoundsText renders a parameter's bounds for comparison; the
// pointers differ between two equal declarations, so the values are compared.
func openAPIBoundsText(p *DXAPIEndPointParameter) string {
	var b strings.Builder
	f := func(name string, v *float64) {
		if v != nil {
			fmt.Fprintf(&b, "%s=%v ", name, *v)
		}
	}
	n := func(name string, v *int) {
		if v != nil {
			fmt.Fprintf(&b, "%s=%d ", name, *v)
		}
	}
	f("minimum", p.Minimum)
	f("exclusiveMinimum", p.ExclusiveMinimum)
	f("maximum", p.Maximum)
	f("exclusiveMaximum", p.ExclusiveMaximum)
	f("multipleOf", p.MultipleOf)
	n("minLength", p.MinLength)
	n("maxLength", p.MaxLength)
	n("minItems", p.MinItems)
	n("maxItems", p.MaxItems)
	fmt.Fprintf(&b, "pattern=%q uniqueItems=%v", p.Pattern, p.UniqueItems)
	if p.Const != nil {
		fmt.Fprintf(&b, " const=%v", *p.Const)
	}
	return b.String()
}

// The emitter refuses a declared bound the validator would not check or that
// says less than the type does, rather than writing a promise nobody keeps.
func TestOpenAPIEmitRefusesUncheckableBounds(t *testing.T) {
	for name, c := range map[string]struct {
		p    DXAPIEndPointParameter
		want string
	}{
		"maxLength on an integer": {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeInt64, MaxLength: boundsInt(3)}, "OPENAPI_UNSUPPORTED_CONSTRAINT:maxLength:ON_integer"},
		"maximum on a string":     {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeString, Maximum: boundsFloat(3)}, "OPENAPI_UNSUPPORTED_CONSTRAINT:maximum:ON_string"},
		"looser than int64p":      {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeInt64P, Minimum: boundsFloat(0)}, "OPENAPI_BOUND_LOOSER_THAN_TYPE:minimum=0"},
		"looser than non-empty":   {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeNonEmptyString, MinLength: boundsInt(0)}, "OPENAPI_BOUND_LOOSER_THAN_TYPE:minLength=0"},
		"negative maxItems":       {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeArray, MaxItems: boundsInt(-1)}, "OPENAPI_NEGATIVE_BOUND:maxItems=-1"},
		"zero multipleOf":         {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeFloat64, MultipleOf: boundsFloat(0)}, "OPENAPI_MULTIPLE_OF_NOT_POSITIVE"},
		"bad pattern":             {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeString, Pattern: "(?=a)"}, "OPENAPI_PATTERN_NOT_GO_RE2"},
		"nil const":               {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeString, Const: boundsConst(nil)}, "OPENAPI_UNSUPPORTED_CONSTRUCT:const-<nil>"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := openAPISchemaFromParameter(&c.p)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %s", err, c.want)
			}
		})
	}
}

// Validate checks each bound after the value is resolved, and the refusal
// names the parameter's path and the bound.
func TestValidateChecksBounds(t *testing.T) {
	for _, c := range []struct {
		name string
		p    DXAPIEndPointParameter
		raw  any
		want string // "" when the value is accepted
	}{
		{"minimum holds", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeInt64, Minimum: boundsFloat(5)}, float64(5), ""},
		{"below minimum", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeInt64, Minimum: boundsFloat(5)}, float64(4), "VALUE_BELOW_MINIMUM:n=4"},
		{"below minimum as query string", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeInt64, Minimum: boundsFloat(5)}, "4", "VALUE_BELOW_MINIMUM:n=4"},
		{"exclusive minimum", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeInt32, ExclusiveMinimum: boundsFloat(0)}, float64(0), "VALUE_NOT_ABOVE_EXCLUSIVE_MINIMUM:n=0"},
		{"maximum holds", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeFloat64, Maximum: boundsFloat(1.5)}, 1.5, ""},
		{"above maximum", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeFloat64, Maximum: boundsFloat(1.5)}, 1.51, "VALUE_ABOVE_MAXIMUM:n=1.51"},
		{"exclusive maximum", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeFloat32, ExclusiveMaximum: boundsFloat(1)}, float64(1), "VALUE_NOT_BELOW_EXCLUSIVE_MAXIMUM"},
		{"above maximum on id", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeID, Maximum: boundsFloat(10)}, "11", "VALUE_ABOVE_MAXIMUM:n=11"},
		{"int64 past float precision", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeInt64, Maximum: boundsFloat(9007199254740992)}, "9007199254740993", "VALUE_ABOVE_MAXIMUM"},
		{"decimal multiple", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeFloat64, MultipleOf: boundsFloat(0.1)}, 0.3, ""},
		{"float32 decimal multiple", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeFloat32, MultipleOf: boundsFloat(0.05)}, 0.15, ""},
		{"not a multiple", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeInt64, MultipleOf: boundsFloat(5)}, float64(12), "VALUE_NOT_MULTIPLE_OF:n=12"},
		{"length counts characters", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString, MaxLength: boundsInt(3)}, "äöü", ""},
		{"too long", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString, MaxLength: boundsInt(3)}, "abcd", "STRING_TOO_LONG:n, length=4, maxLength=3"},
		{"too short", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString, MinLength: boundsInt(3)}, "ab", "STRING_TOO_SHORT:n, length=2, minLength=3"},
		{"length of the trimmed value", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeNonEmptyString, MinLength: boundsInt(3)}, "  ab  ", "STRING_TOO_SHORT:n, length=2"},
		{"length of a date as sent", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeDate, MaxLength: boundsInt(9)}, "2026-10-08", "STRING_TOO_LONG:n, length=10"},
		{"pattern matches", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString, Pattern: "^[A-Z]{2}[0-9]+$"}, "AB12", ""},
		{"pattern is a search", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString, Pattern: "[0-9]"}, "ab1c", ""},
		{"pattern fails", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString, Pattern: "^[A-Z]{2}[0-9]+$"}, "secret-value", "STRING_DOES_NOT_MATCH_PATTERN:n, pattern=^[A-Z]{2}[0-9]+$"},
		{"few items", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeArrayString, MinItems: boundsInt(2)}, []any{"a"}, "ARRAY_TOO_FEW_ITEMS:n, count=1, minItems=2"},
		{"many items", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeArrayInt64, MaxItems: boundsInt(1)}, []any{float64(1), float64(2)}, "ARRAY_TOO_MANY_ITEMS:n, count=2, maxItems=1"},
		{"unique items", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeArrayString, UniqueItems: true}, []any{"a", "b"}, ""},
		{"repeated item", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeArrayString, UniqueItems: true}, []any{"a", "b", "a"}, "ARRAY_ITEMS_NOT_UNIQUE:n[2], same as n[0]"},
		{"repeated object in any key order", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeArray, UniqueItems: true},
			[]any{map[string]any{"a": 1.0, "b": "x"}, map[string]any{"b": "x", "a": 1.0}}, "ARRAY_ITEMS_NOT_UNIQUE:n[1]"},
		{"const holds", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeInt64, Const: boundsConst(int64(7))}, "7", ""},
		{"const differs", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString, Const: boundsConst("member")}, "Member", "VALUE_NOT_CONST:n, const=member"},
		{"no bounds", DXAPIEndPointParameter{Type: dxlibTypes.APIParameterTypeString}, "anything", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.p.NameId = "n"
			v := &DXAPIEndPointRequestParameterValue{Owner: &DXAPIEndPointRequest{}, Metadata: c.p, RawValue: c.raw}
			err := v.Validate()
			if c.want == "" {
				if err != nil {
					t.Fatalf("refused %v: %v", c.raw, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %s", err, c.want)
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Errorf("refusal echoes the string value: %v", err)
			}
		})
	}
}

// A child's bound is checked with the child's full path, inside an object and
// inside each element of an array-json-template; the array's own item count
// is checked on the array.
func TestValidateChecksBoundsOfChildren(t *testing.T) {
	order := DXAPIEndPointParameter{NameId: "order", Type: dxlibTypes.APIParameterTypeJSON, Children: []DXAPIEndPointParameter{
		{NameId: "note", Type: dxlibTypes.APIParameterTypeString, MaxLength: boundsInt(4)},
	}}
	lines := DXAPIEndPointParameter{NameId: "lines", Type: dxlibTypes.APIParameterTypeArrayJSONTemplate, MaxItems: boundsInt(2), Children: []DXAPIEndPointParameter{
		{NameId: "qty", Type: dxlibTypes.APIParameterTypeInt64, Maximum: boundsFloat(9)},
	}}
	for _, c := range []struct {
		name string
		p    DXAPIEndPointParameter
		raw  any
		want string
	}{
		{"object child holds", order, map[string]any{"note": "ok"}, ""},
		{"object child too long", order, map[string]any{"note": "too long"}, "STRING_TOO_LONG:order.note"},
		{"element child holds", lines, []any{map[string]any{"qty": 9.0}}, ""},
		{"element child above maximum", lines, []any{map[string]any{"qty": 1.0}, map[string]any{"qty": 10.0}}, "VALUE_ABOVE_MAXIMUM:lines.lines[1].qty=10"},
		{"too many elements", lines, []any{map[string]any{}, map[string]any{}, map[string]any{}}, "ARRAY_TOO_MANY_ITEMS:lines, count=3"},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := &DXAPIEndPointRequestParameterValue{Owner: &DXAPIEndPointRequest{}, Metadata: c.p}
			if err := v.SetRawValue(c.raw, c.p.NameId); err != nil {
				t.Fatal(err)
			}
			err := v.Validate()
			if c.want == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %s", err, c.want)
			}
		})
	}
}
