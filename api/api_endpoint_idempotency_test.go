package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

const idempotencyTestUUIDPattern = `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`

func idempotencyTestKey(required bool) DXAPIEndPointParameter {
	return DXAPIEndPointParameter{
		NameId: "Idempotency-Key", Type: dxlibTypes.APIParameterTypeString, IsMustExist: required,
		Pattern: idempotencyTestUUIDPattern, Description: "a UUID the client makes per request",
	}
}

// NewEndPoint returns a copy of the endpoint it registers, so the setter has
// to reach the registered one; the emitted document is the proof it did.
func TestSetEndPointIdempotencyKeyWritesTheRegisteredEndPoint(t *testing.T) {
	a := openAPITestAPI(t, "idempotency-set")
	a.NewEndPoint("pay", "", "/pay", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		[]DXAPIEndPointParameter{{NameId: "amount", Type: dxlibTypes.APIParameterTypeInt64, IsMustExist: true}}, noop, nil, nil, nil, nil, 0, "")
	if err := a.SetEndPointIdempotencyKey("POST", "/pay", idempotencyTestKey(true)); err != nil {
		t.Fatal(err)
	}
	if a.EndPoints[0].IdempotencyKey == nil || a.EndPoints[0].IdempotencyKey.NameId != "Idempotency-Key" {
		t.Fatalf("the registered endpoint has no key: %+v", a.EndPoints[0].IdempotencyKey)
	}
	if len(a.EndPoints[0].Parameters) != 1 {
		t.Errorf("the key went into Parameters: %+v", a.EndPoints[0].Parameters)
	}

	doc, err := a.OpenAPIDocument()
	if err != nil {
		t.Fatal(err)
	}
	item, _ := doc.Paths.Get("/pay")
	op := item.Post
	if op.IdempotencyKey != "Idempotency-Key" {
		t.Errorf("x-dxlib-idempotency-key = %q", op.IdempotencyKey)
	}
	if len(op.Parameters) != 1 {
		t.Fatalf("parameters = %+v, want the one header", op.Parameters)
	}
	p := op.Parameters[0]
	if p.Name != "Idempotency-Key" || p.In != "header" || !p.Required || p.Schema.Pattern != idempotencyTestUUIDPattern || p.Description == "" {
		t.Errorf("header parameter = %+v, schema %+v", p, p.Schema)
	}
	b, err := doc.AsJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"x-dxlib-idempotency-key": "Idempotency-Key"`) {
		t.Errorf("emitted JSON lacks the extension:\n%s", b)
	}
}

func TestSetEndPointIdempotencyKeyRefusals(t *testing.T) {
	a := openAPITestAPI(t, "idempotency-refusals")
	a.NewEndPoint("pay", "", "/pay", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("put", "", "/put", "PUT", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("get", "", "/get", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	a.NewWSEndPoint("ws", "", "/ws", "GET", nil, func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) { return m, nil }, nil, nil, 0, nil, nil, "")

	withType := func(t dxlibTypes.APIParameterType) DXAPIEndPointParameter {
		k := idempotencyTestKey(true)
		k.Type = t
		return k
	}
	nullable := idempotencyTestKey(true)
	nullable.IsNullable = true
	badName := idempotencyTestKey(true)
	badName.NameId = "Idempotency Key"
	withDefault := idempotencyTestKey(true)
	withDefault.Default = boundsConst("k")
	badPattern := idempotencyTestKey(true)
	badPattern.Pattern = "(?<=x)"

	cases := []struct {
		name, method, uri string
		key               DXAPIEndPointParameter
		want              string
	}{
		{"unknown endpoint", "POST", "/nowhere", idempotencyTestKey(true), "IDEMPOTENCY_KEY_ENDPOINT_NOT_FOUND"},
		{"PUT", "PUT", "/put", idempotencyTestKey(true), "IDEMPOTENCY_KEY_ON_PUT"},
		{"GET", "GET", "/get", idempotencyTestKey(true), "IDEMPOTENCY_KEY_ON_GET"},
		{"WebSocket", "GET", "/ws", idempotencyTestKey(true), "IDEMPOTENCY_KEY_ON_A_WEBSOCKET_ENDPOINT"},
		{"bad header name", "POST", "/pay", badName, "IDEMPOTENCY_KEY_BAD_HEADER_NAME"},
		{"integer type", "POST", "/pay", withType(dxlibTypes.APIParameterTypeInt64), "IDEMPOTENCY_KEY_TYPE_NOT_A_STRING"},
		{"nullable", "POST", "/pay", nullable, "IDEMPOTENCY_KEY_NULLABLE"},
		{"default", "POST", "/pay", withDefault, "IDEMPOTENCY_KEY_WITH_ONE_VALUE"},
		{"pattern not RE2", "POST", "/pay", badPattern, "OPENAPI_PATTERN_NOT_GO_RE2"},
	}
	for _, c := range cases {
		err := a.SetEndPointIdempotencyKey(c.method, c.uri, c.key)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want %s", c.name, err, c.want)
		}
	}
	for i := range a.EndPoints {
		if a.EndPoints[i].IdempotencyKey != nil {
			t.Errorf("a refused key was set on %s %s", a.EndPoints[i].Method, a.EndPoints[i].Uri)
		}
	}
}

func TestOpenAPIReadsAndBindsTheIdempotencyKey(t *testing.T) {
	doc, err := ReadOpenAPI([]byte(`openapi: 3.1.0
info: {title: idempotency-bind, version: "1"}
paths:
  /orders/{id}/pay:
    post:
      operationId: pay
      x-dxlib-idempotency-key: Idempotency-Key
      parameters:
        - {name: id, in: path, required: true, schema: {type: integer, format: int64}}
        - {name: Idempotency-Key, in: header, required: true, schema: {type: string, maxLength: 64}}
      requestBody:
        content:
          application/json:
            schema: {type: object, properties: {amount: {type: integer, format: int64}}}
`))
	if err != nil {
		t.Fatal(err)
	}
	a := openAPITestAPI(t, "idempotency-bind")
	a.RegisterHandler("pay", noop)
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	ep := a.FindEndPoint("POST", "/orders/{id}/pay")
	if ep == nil || ep.IdempotencyKey == nil {
		t.Fatalf("bound endpoint has no key: %+v", ep)
	}
	if k := ep.IdempotencyKey; k.NameId != "Idempotency-Key" || !k.IsMustExist || k.MaxLength == nil || *k.MaxLength != 64 {
		t.Errorf("bound key = %+v", k)
	}
	for _, p := range ep.Parameters {
		if p.NameId == "Idempotency-Key" {
			t.Errorf("the key was bound into Parameters")
		}
	}
}

func TestOpenAPIRefusesAHeaderThatIsNotTheIdempotencyKey(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{"header without the extension", `
      parameters:
        - {name: Idempotency-Key, in: header, schema: {type: string}}`, "parameter-in-header"},
		{"another header beside the key", `
      x-dxlib-idempotency-key: Idempotency-Key
      parameters:
        - {name: Idempotency-Key, in: header, schema: {type: string}}
        - {name: X-Trace, in: header, schema: {type: string}}`, "parameter-in-header"},
		{"extension without the header", `
      x-dxlib-idempotency-key: Idempotency-Key`, "OPENAPI_IDEMPOTENCY_KEY_WITHOUT_HEADER_PARAMETER"},
		{"key in a cookie", `
      x-dxlib-idempotency-key: q
      parameters:
        - {name: q, in: cookie, schema: {type: string}}`, "parameter-in-cookie"},
		{"bad header name", `
      x-dxlib-idempotency-key: "Idempotency Key"
      parameters:
        - {name: "Idempotency Key", in: header, schema: {type: string}}`, "OPENAPI_IDEMPOTENCY_KEY_BAD_HEADER_NAME"},
	}
	for _, c := range cases {
		_, err := ReadOpenAPI([]byte(`openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x` + c.yaml + "\n"))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want %s", c.name, err, c.want)
		}
	}

	_, err := ReadOpenAPI([]byte(`openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    put:
      operationId: x
      x-dxlib-idempotency-key: Idempotency-Key
      parameters:
        - {name: Idempotency-Key, in: header, schema: {type: string}}
`))
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_IDEMPOTENCY_KEY_ON_PUT") {
		t.Errorf("a key on PUT: err = %v", err)
	}

	// The reader leaves the schema's type to the binder, which holds a bound
	// key to what SetEndPointIdempotencyKey accepts.
	doc, err := ReadOpenAPI([]byte(`openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      x-dxlib-idempotency-key: Idempotency-Key
      parameters:
        - {name: Idempotency-Key, in: header, schema: {type: integer, format: int64}}
`))
	if err != nil {
		t.Fatal(err)
	}
	a := openAPITestAPI(t, "idempotency-bind-refusal")
	a.RegisterHandler("x", noop)
	if err := a.BindOpenAPI(doc); err == nil || !strings.Contains(err.Error(), "IDEMPOTENCY_KEY_TYPE_NOT_A_STRING") {
		t.Errorf("an integer key was bound: err = %v", err)
	}
}

func TestPreProcessRequestChecksTheIdempotencyKey(t *testing.T) {
	run := func(key *DXAPIEndPointParameter, header string) (*DXAPIEndPointRequest, *httptest.ResponseRecorder, error) {
		r := httptest.NewRequest(http.MethodPost, "/pay", strings.NewReader(`{"amount": 5}`))
		r.Header.Set("Content-Type", "application/json")
		if header != "" {
			r.Header.Set("idempotency-key", header) // header names are compared without case
		}
		aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
			Parameters:         []DXAPIEndPointParameter{{NameId: "amount", Type: dxlibTypes.APIParameterTypeInt64, IsMustExist: true}},
			IdempotencyKey:     key,
		})
		return aepr, rec, aepr.PreProcessRequest()
	}
	required, optional := idempotencyTestKey(true), idempotencyTestKey(false)
	const good = "0b6f0c1e-4a8e-4c4b-9a77-3f1f5b0e9d21"

	aepr, _, err := run(&required, good)
	if err != nil {
		t.Fatal(err)
	}
	if aepr.IdempotencyKey != good {
		t.Errorf("IdempotencyKey = %q, want %q", aepr.IdempotencyKey, good)
	}
	if _, amount, err := aepr.GetParameterValueAsInt64("amount"); err != nil || amount != 5 {
		t.Errorf("the body was not read after the key: %d, %v", amount, err)
	}

	_, rec, err := run(&required, "")
	if err == nil || !strings.Contains(err.Error(), "IDEMPOTENCY_KEY_MISSING:Idempotency-Key") || rec.Code != http.StatusBadRequest {
		t.Errorf("missing required key: %d, %v; want 400 IDEMPOTENCY_KEY_MISSING", rec.Code, err)
	}

	_, rec, err = run(&required, "not-a-uuid")
	if err == nil || rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("malformed key: %d, %v; want 422", rec.Code, err)
	}

	aepr, _, err = run(&optional, "")
	if err != nil || aepr.IdempotencyKey != "" {
		t.Errorf("optional key left out: %q, %v", aepr.IdempotencyKey, err)
	}

	aepr, _, err = run(nil, good)
	if err != nil || aepr.IdempotencyKey != "" {
		t.Errorf("an endpoint without a key read one: %q, %v", aepr.IdempotencyKey, err)
	}
}

func TestPrintSpecShowsTheIdempotencyKey(t *testing.T) {
	original := SpecFormat
	t.Cleanup(func() { SpecFormat = original })
	SpecFormat = "MarkDown"

	key := idempotencyTestKey(true)
	ep := &DXAPIEndPoint{Title: "pay", Uri: "/pay", Method: "POST", IdempotencyKey: &key}
	s, err := ep.PrintSpec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s, "####  Idempotency Key (header):\n     - Idempotency-Key (string) mandatory") {
		t.Errorf("spec lacks the key:\n%s", s)
	}
	ep.IdempotencyKey = nil
	if s, _ := ep.PrintSpec(); strings.Contains(s, "Idempotency") {
		t.Errorf("spec shows a key the endpoint does not take:\n%s", s)
	}
}
