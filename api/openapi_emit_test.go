package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// openAPITestAPI is a DXAPI under the manager that is removed again when the
// test ends, so tests do not see each other's endpoints or handler
// registrations.
func openAPITestAPI(t *testing.T, name string) *DXAPI {
	t.Helper()
	a, err := Manager.NewAPI(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		delete(Manager.APIs, name)
		openAPIStatesMu.Lock()
		delete(openAPIStates, a)
		openAPIStatesMu.Unlock()
	})
	return a
}

// openAPIDecode parses emitted JSON into generic maps for probing.
func openAPIDecode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("emitted document is not JSON: %v\n%s", err, b)
	}
	return m
}

// openAPIProbe walks a decoded document by key path, failing on a missing
// step so the message names the step.
func openAPIProbe(t *testing.T, m any, path ...string) any {
	t.Helper()
	cur := m
	for i, key := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("at %v: not an object", path[:i])
		}
		cur, ok = obj[key]
		if !ok {
			t.Fatalf("at %v: key %q missing; have %v", path[:i], key, keysOf(obj))
		}
	}
	return cur
}

func keysOf(m map[string]any) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func noop(aepr *DXAPIEndPointRequest) error { return nil }

func TestOpenAPIOperationIdIsDerivedFromTheURI(t *testing.T) {
	for uri, want := range map[string]string{
		"/cmdContactCenterUserSessionList":    "cmdContactCenterUserSessionList",
		"/v2/cmdContactCenterUserSessionList": "v2_cmdContactCenterUserSessionList",
		"/users/{id}":                         "users_id",
		"/ws":                                 "ws",
	} {
		if got := OpenAPIOperationId(uri); got != want {
			t.Errorf("%s: got %q want %q", uri, got, want)
		}
	}
}

// Every type dxlib declares in its Types table has a schema. A new dxlib type
// with no row here would make the emitter fail on the first endpoint that
// used it, which is right, but the failure belongs in this test rather than
// at a service's startup.
func TestOpenAPITypeTableCoversEveryDeclaredType(t *testing.T) {
	for apiType := range dxlibTypes.Types {
		if _, ok := openAPITypeTable[apiType]; !ok {
			t.Errorf("dxlib type %q has no JSON Schema mapping", apiType)
		}
	}
	// Three types are declared as constants without a row in Types; they are
	// mapped too, since an endpoint may declare them.
	for _, apiType := range []dxlibTypes.APIParameterType{dxlibTypes.APIParameterTypeBlob, dxlibTypes.APIParameterTypeEncryptedBlob, dxlibTypes.APIParameterTypeMoney} {
		if _, ok := openAPITypeTable[apiType]; !ok {
			t.Errorf("dxlib type %q has no JSON Schema mapping", apiType)
		}
	}
}

func TestOpenAPIEmitsAPostWithJSONBodyAsARequestBody(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-post")
	a.NewEndPoint("cmdRoutingEdit", "contactcenter-routing-edit", "/cmdRoutingEdit", "POST",
		EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		[]DXAPIEndPointParameter{
			{NameId: "token", Type: dxlibTypes.APIParameterTypeString, IsMustExist: true},
			{NameId: "routing_id", Type: dxlibTypes.APIParameterTypeInt64P, IsMustExist: true, Description: "the routing"},
			{NameId: "note", Type: dxlibTypes.APIParameterTypeString, IsNullable: true},
			{NameId: "state", Type: dxlibTypes.APIParameterTypeString, Enum: []any{"ACTIVE", "INACTIVE"}},
			{NameId: "set_field_values", Type: dxlibTypes.APIParameterTypeJSON, IsMustExist: true, Children: []DXAPIEndPointParameter{
				{NameId: "media", Type: dxlibTypes.APIParameterTypeString},
				{NameId: "queue_timeout_seconds", Type: dxlibTypes.APIParameterTypeInt64, IsMustExist: true},
			}},
			{NameId: "tags", Type: dxlibTypes.APIParameterTypeArrayString},
			{NameId: "rows", Type: dxlibTypes.APIParameterTypeArrayJSONTemplate, Children: []DXAPIEndPointParameter{
				{NameId: "id", Type: dxlibTypes.APIParameterTypeID, IsMustExist: true},
			}},
			{NameId: "labels", Type: dxlibTypes.APIParameterTypeMapStringString},
		},
		noop, nil, nil, nil, []string{"contactcenter-routing-setup"}, 2048, "writes")

	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	doc := openAPIDecode(t, b)
	if got := openAPIProbe(t, doc, "openapi"); got != "3.1.0" {
		t.Errorf("openapi = %v", got)
	}
	op := openAPIProbe(t, doc, "paths", "/cmdRoutingEdit", "post").(map[string]any)
	if op["operationId"] != "cmdRoutingEdit" || op["summary"] != "cmdRoutingEdit" || op["description"] != "contactcenter-routing-edit" {
		t.Errorf("operation header wrong: %v", op)
	}
	if op["x-dxlib-endpoint-type"] != "EndPointTypeHTTPJSON" {
		t.Errorf("endpoint type = %v", op["x-dxlib-endpoint-type"])
	}
	if p, _ := op["x-dxlib-privileges"].([]any); len(p) != 1 || p[0] != "contactcenter-routing-setup" {
		t.Errorf("privileges = %v", op["x-dxlib-privileges"])
	}
	if op["x-dxlib-rate-limit-group"] != "writes" || op["x-dxlib-max-content-length"] != float64(2048) {
		t.Errorf("rate limit / max length wrong: %v %v", op["x-dxlib-rate-limit-group"], op["x-dxlib-max-content-length"])
	}
	if _, has := op["parameters"]; has {
		t.Errorf("a POST must not have query parameters")
	}
	if _, has := op["responses"]; has {
		t.Errorf("no responses were declared, so the key must be absent")
	}
	schema := openAPIProbe(t, op, "requestBody", "content", "application/json", "schema").(map[string]any)
	if schema["type"] != "object" {
		t.Errorf("body type = %v", schema["type"])
	}
	required, _ := schema["required"].([]any)
	if len(required) != 3 || required[0] != "token" || required[1] != "routing_id" || required[2] != "set_field_values" {
		t.Errorf("required = %v", required)
	}
	props := schema["properties"].(map[string]any)
	// Declaration order survives marshalling: the ordered map is the whole
	// reason the properties are not alphabetical.
	idx := strings.Index(string(b), `"token"`)
	idx2 := strings.Index(string(b), `"routing_id"`)
	idx3 := strings.Index(string(b), `"labels"`)
	if !(idx < idx2 && idx2 < idx3) {
		t.Errorf("properties are not in declaration order")
	}
	routing := props["routing_id"].(map[string]any)
	if routing["type"] != "integer" || routing["format"] != "int64" || routing["minimum"] != float64(1) || routing["x-dxlib-type"] != "int64p" || routing["description"] != "the routing" {
		t.Errorf("int64p schema = %v", routing)
	}
	note := props["note"].(map[string]any)
	if nt, _ := note["type"].([]any); len(nt) != 2 || nt[0] != "string" || nt[1] != "null" {
		t.Errorf("nullable type = %v", note["type"])
	}
	state := props["state"].(map[string]any)
	if e, _ := state["enum"].([]any); len(e) != 2 || e[0] != "ACTIVE" {
		t.Errorf("enum = %v", state["enum"])
	}
	set := props["set_field_values"].(map[string]any)
	if set["type"] != "object" || set["x-dxlib-type"] != "json" {
		t.Errorf("json parameter = %v", set)
	}
	if r, _ := set["required"].([]any); len(r) != 1 || r[0] != "queue_timeout_seconds" {
		t.Errorf("child required = %v", set["required"])
	}
	if openAPIProbe(t, set, "properties", "media", "type") != "string" {
		t.Errorf("child schema wrong")
	}
	if openAPIProbe(t, props, "tags", "items", "type") != "string" {
		t.Errorf("array-string items wrong")
	}
	if openAPIProbe(t, props, "rows", "items", "properties", "id", "format") != "int64" {
		t.Errorf("array-json-template items wrong")
	}
	if openAPIProbe(t, props, "labels", "additionalProperties", "type") != "string" {
		t.Errorf("map-string-string wrong")
	}
}

func TestOpenAPIEmitsAGetAsQueryParameters(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-get")
	a.NewEndPoint("cmdQueryCall", "d", "/cmdQueryCall", "GET",
		EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone,
		[]DXAPIEndPointParameter{
			{NameId: "call_id", Type: dxlibTypes.APIParameterTypeString, IsMustExist: true},
			{NameId: "verbose", Type: dxlibTypes.APIParameterTypeBoolean},
		},
		noop, nil, nil, nil, nil, 0, "")
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	op := openAPIProbe(t, openAPIDecode(t, b), "paths", "/cmdQueryCall", "get").(map[string]any)
	if _, has := op["requestBody"]; has {
		t.Errorf("a GET must not have a requestBody")
	}
	params := op["parameters"].([]any)
	if len(params) != 2 {
		t.Fatalf("parameters = %v", params)
	}
	first := params[0].(map[string]any)
	if first["name"] != "call_id" || first["in"] != "query" || first["required"] != true || openAPIProbe(t, first, "schema", "type") != "string" {
		t.Errorf("first parameter = %v", first)
	}
	second := params[1].(map[string]any)
	if _, has := second["required"]; has {
		t.Errorf("an optional parameter must not write required:false")
	}
	if openAPIProbe(t, second, "schema", "type") != "boolean" {
		t.Errorf("second parameter = %v", second)
	}
	for _, key := range []string{"x-dxlib-privileges", "x-dxlib-rate-limit-group", "x-dxlib-max-content-length", "x-dxlib-request-content-type"} {
		if _, has := op[key]; has {
			t.Errorf("%s must be omitted when zero", key)
		}
	}
}

func TestOpenAPIPlacesParametersByContentType(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-ct")
	params := []DXAPIEndPointParameter{{NameId: "token", Type: dxlibTypes.APIParameterTypeString, IsMustExist: true}}
	a.NewEndPoint("multipart", "", "/multipart", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeMultiPartFormData, params, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("upload", "", "/upload", "POST", EndPointTypeHTTPUploadStream, utilsHttp.RequestContentTypeApplicationOctetStream, params, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("text", "", "/text", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeTextPlain, nil, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("none", "", "/none", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, params, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("getjson", "", "/getjson", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("emptyjson", "", "/emptyjson", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil, nil, nil, nil, 0, "")
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	doc := openAPIDecode(t, b)

	if openAPIProbe(t, doc, "paths", "/multipart", "post", "requestBody", "content", "multipart/form-data", "schema", "properties", "token", "type") != "string" {
		t.Errorf("multipart parameters must be the body schema")
	}
	upload := openAPIProbe(t, doc, "paths", "/upload", "post").(map[string]any)
	if openAPIProbe(t, upload, "requestBody", "content", "application/octet-stream", "schema", "format") != "binary" {
		t.Errorf("octet-stream body must be binary")
	}
	if openAPIProbe(t, upload, "x-dxlib-parameters", "properties", "token", "type") != "string" {
		t.Errorf("octet-stream parameters must go to x-dxlib-parameters")
	}
	if upload["x-dxlib-endpoint-type"] != "EndPointTypeHTTPUploadStream" {
		t.Errorf("endpoint type = %v", upload["x-dxlib-endpoint-type"])
	}
	if openAPIProbe(t, doc, "paths", "/text", "post", "requestBody", "content", "text/plain", "schema", "type") != "string" {
		t.Errorf("text/plain body must be a string")
	}
	none := openAPIProbe(t, doc, "paths", "/none", "post").(map[string]any)
	if _, has := none["requestBody"]; has {
		t.Errorf("no content type means no requestBody")
	}
	if openAPIProbe(t, none, "x-dxlib-parameters", "properties", "token", "type") != "string" {
		t.Errorf("parameters without a body go to x-dxlib-parameters")
	}
	if openAPIProbe(t, doc, "paths", "/getjson", "get", "x-dxlib-request-content-type") != "application/json" {
		t.Errorf("a GET's content type is kept in the extension")
	}
	empty := openAPIProbe(t, doc, "paths", "/emptyjson", "post", "requestBody", "content", "application/json", "schema").(map[string]any)
	if empty["type"] != "object" {
		t.Errorf("an empty JSON body is still an object: %v", empty)
	}
	if _, has := empty["properties"]; has {
		t.Errorf("no parameters means no properties key")
	}
}

// A WebSocket endpoint goes to the extension, with the text explaining why,
// and the three fields PreProcessRequest never reads for a WebSocket --
// parameters, content type, content length -- are the known, asserted loss.
func TestOpenAPIEmitsWebSocketEndPointsAsAnExtension(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-ws")
	a.NewWSEndPoint("ws", "WebSocket relay", "/ws", "GET", nil,
		func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) { return nil, nil },
		nil, nil, 5*time.Second, nil, []string{"ws-connect"}, "sockets")
	// The legacy form: NewEndPoint with an OnWSLoop and parameters declared.
	a.NewEndPoint("wsloop", "Loop", "/wsloop", "GET", EndPointTypeWS, utilsHttp.RequestContentTypeApplicationJSON,
		[]DXAPIEndPointParameter{{NameId: "ignored", Type: dxlibTypes.APIParameterTypeString}},
		nil, noop, nil, nil, nil, 4096, "")
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	doc := openAPIDecode(t, b)
	if paths := openAPIProbe(t, doc, "paths").(map[string]any); len(paths) != 0 {
		t.Errorf("WebSocket endpoints must not appear under paths: %v", keysOf(paths))
	}
	ext := openAPIProbe(t, doc, "x-dxlib-websocket-endpoints").(map[string]any)
	if !strings.Contains(ext["description"].(string), "OpenAPI 3.1") || !strings.Contains(ext["description"].(string), "no construct for a WebSocket lifecycle") {
		t.Errorf("the extension must explain itself: %v", ext["description"])
	}
	list := ext["endpoints"].([]any)
	if len(list) != 2 {
		t.Fatalf("endpoints = %v", list)
	}
	ws := list[0].(map[string]any)
	if ws["operationId"] != "ws" || ws["path"] != "/ws" || ws["method"] != "GET" || ws["summary"] != "ws" || ws["description"] != "WebSocket relay" || ws["periodicInterval"] != "5s" || ws["rateLimitGroup"] != "sockets" {
		t.Errorf("ws entry = %v", ws)
	}
	if p, _ := ws["privileges"].([]any); len(p) != 1 || p[0] != "ws-connect" {
		t.Errorf("privileges = %v", ws["privileges"])
	}
	loop := list[1].(map[string]any)
	for _, lost := range []string{"parameters", "requestBody", "x-dxlib-parameters", "x-dxlib-max-content-length", "x-dxlib-request-content-type"} {
		if _, has := loop[lost]; has {
			t.Errorf("%s must not be carried for a WebSocket endpoint", lost)
		}
	}
	if _, has := loop["periodicInterval"]; has {
		t.Errorf("a zero interval is omitted")
	}
}

func TestOpenAPIEmitsResponsesKeyedByStatusCode(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-responses")
	a.NewEndPoint("cmdX", "", "/cmdX", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil,
		&DXAPIEndPointResponsePossibilities{
			"not-found": {StatusCode: 404, Description: "no such session"},
			"ok": {StatusCode: 200, Description: "the session", Headers: map[string]string{"X-Var": "request id"},
				DataTemplate: []*DXAPIEndPointParameter{{NameId: "id", Type: dxlibTypes.APIParameterTypeInt64, IsMustExist: true}}},
		}, nil, nil, 0, "")
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	responses := openAPIProbe(t, openAPIDecode(t, b), "paths", "/cmdX", "post", "responses").(map[string]any)
	ok := responses["200"].(map[string]any)
	if ok["description"] != "the session" || ok["x-dxlib-response-name"] != "ok" {
		t.Errorf("200 = %v", ok)
	}
	if openAPIProbe(t, ok, "headers", "X-Var", "description") != "request id" {
		t.Errorf("header lost")
	}
	if openAPIProbe(t, ok, "content", "application/json", "schema", "properties", "id", "format") != "int64" {
		t.Errorf("data template lost")
	}
	if responses["404"].(map[string]any)["x-dxlib-response-name"] != "not-found" {
		t.Errorf("404 = %v", responses["404"])
	}
	// Lower status codes come first regardless of map iteration order.
	if strings.Index(string(b), `"200"`) > strings.Index(string(b), `"404"`) {
		t.Errorf("responses not ordered by status code")
	}
}

func TestOpenAPIRefusesTwoResponsesOnOneStatusCode(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-responses-collide")
	a.NewEndPoint("cmdX", "", "/cmdX", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil,
		&DXAPIEndPointResponsePossibilities{
			"a": {StatusCode: 200, Description: "a"},
			"b": {StatusCode: 200, Description: "b"},
		}, nil, nil, 0, "")
	_, err := a.OpenAPIAsJSON()
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_RESPONSE_STATUS_COLLISION:/cmdX:200:a:b") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAPIRefusesAnOperationIdCollision(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-collide")
	a.NewEndPoint("one", "", "/a/b", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("two", "", "/a_b", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON, nil, noop, nil, nil, nil, nil, 0, "")
	_, err := a.OpenAPIAsJSON()
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_OPERATION_ID_COLLISION:a_b:/a/b:/a_b") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAPIPathTemplatesAreWholeSegments(t *testing.T) {
	names, err := openAPIPathTemplateNames("/users/{id}/files/{file_id}")
	if err != nil || len(names) != 2 || names[0] != "id" || names[1] != "file_id" {
		t.Fatalf("names=%v err=%v", names, err)
	}
	for _, bad := range []string{"/users/u{id}", "/files/{path...}", "/x/{$}", "/a/{}", "/a/{1x}", "/a/{id}/{id}"} {
		if _, err := openAPIPathTemplateNames(bad); err == nil {
			t.Errorf("%s: expected an error", bad)
		}
	}
	// A NewEndPoint URI with a template and no declaration gets a string
	// path parameter, which is the only honest schema for it.
	a := openAPITestAPI(t, "openapi-emit-template")
	a.NewEndPoint("user", "", "/users/{id}", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	params := openAPIProbe(t, openAPIDecode(t, b), "paths", "/users/{id}", "get", "parameters").([]any)
	p := params[0].(map[string]any)
	if p["name"] != "id" || p["in"] != "path" || p["required"] != true || openAPIProbe(t, p, "schema", "type") != "string" {
		t.Errorf("path parameter = %v", p)
	}
}

// The handler is the OpenAPI counterpart of APIHandlerPrintSpec: it writes
// the document as application/json through the ordinary response path.
func TestOpenAPIHandlerServesTheDocumentAsJSON(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-handler")
	a.NewEndPoint("cmdPing", "ping", "/cmdPing", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	spec := a.NewEndPoint("openapi", "OpenAPI document", "/openapi.json", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, a.APIHandlerOpenAPI, nil, nil, nil, nil, 0, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	aepr := spec.NewEndPointRequest(context.Background(), rec, req)
	if err := a.APIHandlerOpenAPI(aepr); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status=%d content-type=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
	doc := openAPIDecode(t, rec.Body.Bytes())
	paths := openAPIProbe(t, doc, "paths").(map[string]any)
	if _, has := paths["/cmdPing"]; !has {
		t.Errorf("document lacks /cmdPing")
	}
	if _, has := paths["/openapi.json"]; !has {
		t.Errorf("the document endpoint is itself in the document, as /spec is in the Markdown one")
	}
	if openAPIProbe(t, doc, "info", "title") != "openapi-emit-handler" {
		t.Errorf("info.title = %v", openAPIProbe(t, doc, "info", "title"))
	}
}

// Two calls with nothing registered in between are byte-identical, which is
// the property every round-trip comparison rests on.
func TestOpenAPIEmissionIsDeterministic(t *testing.T) {
	a := openAPITestAPI(t, "openapi-emit-deterministic")
	a.NewEndPoint("cmdX", "", "/cmdX", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		[]DXAPIEndPointParameter{{NameId: "z", Type: dxlibTypes.APIParameterTypeString}, {NameId: "a", Type: dxlibTypes.APIParameterTypeString}},
		noop, nil, &DXAPIEndPointResponsePossibilities{"x": {StatusCode: 200, Description: "x", Headers: map[string]string{"B": "b", "A": "a"}}}, nil, []string{"p"}, 0, "")
	first, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		again, err := a.OpenAPIAsJSON()
		if err != nil {
			t.Fatal(err)
		}
		if string(again) != string(first) {
			t.Fatalf("emission differs between calls:\n%s\n---\n%s", first, again)
		}
	}
}
