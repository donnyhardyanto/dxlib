package api

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The reader takes title, default, readOnly and writeOnly, and refuses only
// what no reader could make sense of.
func TestOpenAPIReadsAnnotations(t *testing.T) {
	doc := openAPIMustRead(t, `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
components:
  schemas:
    S: {type: string, title: Code, default: abc, readOnly: true}
    N: {type: integer, default: 10, writeOnly: true}
`)
	s, _ := doc.Components.Schemas.Get("S")
	if s.Title != "Code" || s.Default == nil || *s.Default != "abc" || !s.ReadOnly || s.WriteOnly {
		t.Errorf("S = %+v", s)
	}
	n, _ := doc.Components.Schemas.Get("N")
	if n.Default == nil || *n.Default != int64(10) || n.ReadOnly || !n.WriteOnly {
		t.Errorf("N = %+v", n)
	}

	for name, c := range map[string]struct{ schema, want string }{
		"readOnly and writeOnly": {`{type: string, readOnly: true, writeOnly: true}`, "OPENAPI_READ_ONLY_AND_WRITE_ONLY"},
		"null default":           {`{type: [string, "null"], default: null}`, "OPENAPI_UNSUPPORTED_CONSTRUCT:default-null"},
		"object default":         {`{type: object, default: {a: 1}}`, "OPENAPI_UNSUPPORTED_CONSTRUCT:non-scalar-default"},
		"array default":          {`{type: array, default: []}`, "OPENAPI_UNSUPPORTED_CONSTRUCT:non-scalar-default"},
		"string readOnly":        {`{type: string, readOnly: "yes"}`, "OPENAPI_WRONG_TYPE"},
		"numeric title":          {`{type: string, title: 7}`, "OPENAPI_WRONG_TYPE"},
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

// Binding carries the annotations onto the parameter, and refuses a default
// that is not a value of the parameter's JSON type.
func TestOpenAPIBindCarriesAnnotations(t *testing.T) {
	bind := func(t *testing.T, name, schema string) (DXAPIEndPointParameter, error) {
		a := openAPITestAPI(t, "openapi-bind-annotations-"+name)
		doc, err := ReadOpenAPI([]byte("openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    get:\n      operationId: x\n      parameters:\n        - {name: q, in: query, schema: " + schema + "}\n"))
		if err != nil {
			return DXAPIEndPointParameter{}, err
		}
		a.RegisterHandler("x", noop)
		if err := a.BindOpenAPI(doc); err != nil {
			return DXAPIEndPointParameter{}, err
		}
		return a.FindEndPointByURI("/x").Parameters[0], nil
	}

	p, err := bind(t, "all", `{type: integer, minimum: 1, title: Page, default: 1, readOnly: true}`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != dxlibTypes.APIParameterTypeInt64P || p.Title != "Page" || p.Default == nil || *p.Default != int64(1) || !p.ReadOnly || p.WriteOnly {
		t.Errorf("parameter = %+v", p)
	}
	p, err = bind(t, "integral float", `{type: integer, default: 2.0}`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Default == nil || *p.Default != float64(2) {
		t.Errorf("parameter = %+v", p)
	}

	for name, c := range map[string]struct{ schema, want string }{
		"string default on an integer":  {`{type: integer, default: "7"}`, "OPENAPI_DEFAULT_OF_ANOTHER_TYPE"},
		"fractional default on integer": {`{type: integer, default: 1.5}`, "OPENAPI_DEFAULT_OF_ANOTHER_TYPE"},
		"number default on a boolean":   {`{type: boolean, default: 0}`, "OPENAPI_DEFAULT_OF_ANOTHER_TYPE"},
		"scalar default on an object":   {`{type: object, default: "x"}`, "OPENAPI_DEFAULT_OF_ANOTHER_TYPE"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := bind(t, name, c.schema)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %s", err, c.want)
			}
			if !strings.Contains(err.Error(), "/paths/~1x/get/parameters/0/schema/default") {
				t.Errorf("error does not point at the schema: %v", err)
			}
		})
	}
}

// A declaration with every annotation set is emitted, read and bound back to
// the same annotations, and the document survives the round trip byte for
// byte.
func TestOpenAPIRoundTripOverAnnotations(t *testing.T) {
	declared := []DXAPIEndPointParameter{
		{NameId: "user_id", Type: dxlibTypes.APIParameterTypeInt64P, Title: "User", ReadOnly: true},
		{NameId: "password", Type: dxlibTypes.APIParameterTypeProtectedString, IsMustExist: true, Title: "Password", WriteOnly: true},
		{NameId: "page_size", Type: dxlibTypes.APIParameterTypeInt64P, Default: boundsConst(20), Maximum: boundsFloat(100)},
		{NameId: "ratio", Type: dxlibTypes.APIParameterTypeFloat64, Default: boundsConst(0.5)},
		{NameId: "active", Type: dxlibTypes.APIParameterTypeBoolean, Default: boundsConst(true)},
		{NameId: "lang", Type: dxlibTypes.APIParameterTypeString, Enum: []any{"en", "id"}, Default: boundsConst("en")},
		{NameId: "profile", Type: dxlibTypes.APIParameterTypeJSON, Title: "Profile", Children: []DXAPIEndPointParameter{
			{NameId: "nickname", Type: dxlibTypes.APIParameterTypeString, Title: "Nickname", Default: boundsConst("")},
		}},
	}
	a := openAPITestAPI(t, "roundtrip-annotations")
	a.NewEndPoint("cmdAnnotations", "", "/cmdAnnotations", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		declared, noop, nil, nil, nil, nil, 0, "")
	original, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"title": "User"`, `"readOnly": true`, `"writeOnly": true`, `"default": 20`, `"default": 0.5`, `"default": true`, `"default": "en"`, `"default": ""`, `"title": "Nickname"`} {
		if !strings.Contains(string(original), want) {
			t.Errorf("emitted document lacks %s", want)
		}
	}
	doc := openAPIRoundTrip(t, "annotations", original)

	b := openAPITestAPI(t, "roundtrip-annotations-bound")
	openAPIRegisterEverything(b, doc)
	if err := b.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	got := b.FindEndPointByURI("/cmdAnnotations").Parameters
	if len(got) != len(declared) {
		t.Fatalf("%d parameters for %d declared", len(got), len(declared))
	}
	for i := range declared {
		w, g := declared[i], got[i]
		if openAPIAnnotationsText(&w) != openAPIAnnotationsText(&g) {
			t.Errorf("%s: annotations %s, declared %s", w.NameId, openAPIAnnotationsText(&g), openAPIAnnotationsText(&w))
		}
		for j := range w.Children {
			if openAPIAnnotationsText(&w.Children[j]) != openAPIAnnotationsText(&g.Children[j]) {
				t.Errorf("%s.%s: annotations %s, declared %s", w.NameId, w.Children[j].NameId, openAPIAnnotationsText(&g.Children[j]), openAPIAnnotationsText(&w.Children[j]))
			}
		}
	}
}

// openAPIAnnotationsText renders a parameter's annotations for comparison. A
// default read back as int64 or float64 compares equal to the int or float
// it was declared as, because the text is the same.
func openAPIAnnotationsText(p *DXAPIEndPointParameter) string {
	s := fmt.Sprintf("title=%q readOnly=%v writeOnly=%v", p.Title, p.ReadOnly, p.WriteOnly)
	if p.Default != nil {
		s += fmt.Sprintf(" default=%q", fmt.Sprint(*p.Default))
	}
	return s
}

// The emitter refuses the annotations the reader would refuse.
func TestOpenAPIEmitRefusesBadAnnotations(t *testing.T) {
	for name, c := range map[string]struct {
		p    DXAPIEndPointParameter
		want string
	}{
		"readOnly and writeOnly":       {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeString, ReadOnly: true, WriteOnly: true}, "OPENAPI_READ_ONLY_AND_WRITE_ONLY:a"},
		"string default on an integer": {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeInt64, Default: boundsConst("7")}, "OPENAPI_DEFAULT_OF_ANOTHER_TYPE"},
		"default on an object":         {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeJSON, Default: boundsConst("{}")}, "OPENAPI_DEFAULT_OF_ANOTHER_TYPE"},
		"nil default":                  {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeString, Default: boundsConst(nil)}, "OPENAPI_UNSUPPORTED_CONSTRUCT:default-null"},
		"infinite default":             {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeFloat64, Default: boundsConst(math.Inf(1))}, "OPENAPI_DEFAULT_NOT_FINITE"},
		"slice default":                {DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeArrayString, Default: boundsConst([]string{})}, "OPENAPI_UNSUPPORTED_CONSTRUCT:default-[]string"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := openAPISchemaFromParameter(&c.p)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %s", err, c.want)
			}
		})
	}
}

// Validate refuses readOnly with writeOnly and a non-scalar default in a
// document built in code, which never passes through the reader.
func TestOpenAPIValidateChecksAnnotations(t *testing.T) {
	for name, c := range map[string]struct {
		schema *DXOpenAPISchema
		want   string
	}{
		"readOnly and writeOnly": {&DXOpenAPISchema{Type: DXOpenAPISchemaType{"string"}, ReadOnly: true, WriteOnly: true}, "OPENAPI_READ_ONLY_AND_WRITE_ONLY:/components/schemas/X"},
		"map default":            {&DXOpenAPISchema{Type: DXOpenAPISchemaType{"object"}, Default: boundsConst(map[string]any{})}, "OPENAPI_UNSUPPORTED_CONSTRUCT:default-map"},
		"nil default":            {&DXOpenAPISchema{Type: DXOpenAPISchemaType{"string"}, Default: boundsConst(nil)}, "OPENAPI_UNSUPPORTED_CONSTRUCT:default-null:/components/schemas/X/default"},
	} {
		t.Run(name, func(t *testing.T) {
			schemas := NewDXOpenAPIOrderedMap[*DXOpenAPISchema]()
			schemas.Set("X", c.schema)
			doc := &DXOpenAPIDocument{OpenAPI: OpenAPIVersion, Info: DXOpenAPIInfo{Title: "a", Version: "1"},
				Paths: NewDXOpenAPIOrderedMap[*DXOpenAPIPathItem](), Components: &DXOpenAPIComponents{Schemas: schemas}}
			err := doc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %s", err, c.want)
			}
		})
	}
}

// Below a parameter -- the body object, array items, a map's values -- the
// annotations are read and kept in the document model, and dropped when the
// endpoints are built, as description is there.
func TestOpenAPIBindDropsAnnotationsBelowParameters(t *testing.T) {
	doc := openAPIMustRead(t, `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema:
              type: object
              title: Body
              properties:
                tags: {type: array, items: {type: string, title: Tag, default: a}}
                labels: {type: object, additionalProperties: {type: string, writeOnly: true}}
`)
	mt, _ := doc.Paths.values["/x"].Post.RequestBody.Content.Get("application/json")
	if mt.Schema.Title != "Body" {
		t.Errorf("body title not read: %+v", mt.Schema)
	}
	a := openAPITestAPI(t, "openapi-annotations-below")
	a.RegisterHandler("x", noop)
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	out, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{`"title": "Body"`, `"title": "Tag"`, `"default": "a"`, `"writeOnly"`} {
		if strings.Contains(string(out), gone) {
			t.Errorf("re-emitted document still has %s", gone)
		}
	}
}

// The annotations promise no check: a parameter left out is not given its
// default, a readOnly parameter is still read, and a writeOnly one is still
// required when declared so.
func TestPreProcessRequestIgnoresAnnotations(t *testing.T) {
	params := []DXAPIEndPointParameter{
		{NameId: "id", Type: dxlibTypes.APIParameterTypeInt64, ReadOnly: true},
		{NameId: "size", Type: dxlibTypes.APIParameterTypeNullableInt64, Default: boundsConst(20)},
		{NameId: "secret", Type: dxlibTypes.APIParameterTypeString, IsMustExist: true, WriteOnly: true},
	}
	run := func(body string) (*DXAPIEndPointRequest, error) {
		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		aepr, _ := newPreProcessContext(r, &DXAPIEndPoint{
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
			Parameters:         params,
		})
		return aepr, aepr.PreProcessRequest()
	}

	aepr, err := run(`{"id": 5, "secret": "s"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, id, err := aepr.GetParameterValueAsInt64("id"); err != nil || id != 5 {
		t.Errorf("readOnly id = %d, %v; want 5 as sent", id, err)
	}
	if isExist, size, err := aepr.GetParameterValueAsNullableInt64("size"); err != nil || size != nil {
		t.Errorf("size = %v (exists %v), %v; want nil: a default is not filled in", size, isExist, err)
	}
	if _, err := run(`{"id": 5}`); err == nil {
		t.Errorf("a mandatory writeOnly parameter left out was accepted")
	}
}

// The Markdown spec prints each annotation only when it is set, so a
// parameter without them prints as before.
func TestPrintSpecShowsAnnotations(t *testing.T) {
	original := SpecFormat
	t.Cleanup(func() { SpecFormat = original })
	SpecFormat = "MarkDown"

	plain := DXAPIEndPointParameter{NameId: "a", Type: dxlibTypes.APIParameterTypeString, Description: "d"}
	if got, want := plain.PrintSpec(0), " - a (string) optional d\n"; got != want {
		t.Errorf("plain = %q, want %q", got, want)
	}
	annotated := DXAPIEndPointParameter{NameId: "p", Type: dxlibTypes.APIParameterTypeString, Title: "Password", Default: boundsConst("x"), WriteOnly: true}
	got := annotated.PrintSpec(0)
	for _, want := range []string{"   Title: Password\n", "   Default: x\n", "   Write-only\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("spec lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Read-only") {
		t.Errorf("spec shows Read-only for a parameter without it:\n%s", got)
	}
}
