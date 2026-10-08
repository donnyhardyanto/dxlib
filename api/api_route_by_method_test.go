package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// Two endpoints on one URI with different methods are two endpoints: each
// method reaches its own handler through the real listener, a method neither
// serves is refused as before, and OPTIONS still answers 200.
func TestEndPointsShareAURIByMethod(t *testing.T) {
	answer := func(what string) DXAPIEndPointExecuteFunc {
		return func(aepr *DXAPIEndPointRequest) error {
			aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"handled": what})
			return nil
		}
	}
	a := openAPIStartAPI(t, "route-by-method", func(a *DXAPI) {
		a.NewEndPoint("list", "", "/members", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, answer("list"), nil, nil, nil, nil, 0, "")
		a.NewEndPoint("create", "", "/members", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
			[]DXAPIEndPointParameter{{NameId: "name", Type: dxlibTypes.APIParameterTypeNonEmptyString, IsMustExist: true}}, answer("create"), nil, nil, nil, nil, 0, "")
		a.NewEndPoint("ping", "", "/cmdPing", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, answer("ping"), nil, nil, nil, nil, 0, "")
	})
	base := "http://" + a.Address

	if code, body := openAPIDo(t, "GET", base+"/members", ""); code != 200 || !strings.Contains(body, `"list"`) {
		t.Errorf("GET /members: %d %s", code, body)
	}
	if code, body := openAPIDo(t, "POST", base+"/members", `{"name":"a"}`); code != 200 || !strings.Contains(body, `"create"`) {
		t.Errorf("POST /members: %d %s", code, body)
	}
	if code, _ := openAPIDo(t, "POST", base+"/members", `{}`); code != http.StatusUnprocessableEntity {
		t.Errorf("POST /members without its mandatory parameter: %d, want 422", code)
	}
	if code, body := openAPIDo(t, "DELETE", base+"/members", ""); code != http.StatusMethodNotAllowed || !strings.Contains(body, "METHOD_NOT_ALLOWED") {
		t.Errorf("DELETE /members: %d %s, want 405 METHOD_NOT_ALLOWED", code, body)
	}
	if code, _ := openAPIDo(t, "OPTIONS", base+"/members", ""); code != http.StatusOK {
		t.Errorf("OPTIONS /members: %d, want 200", code)
	}
	if code, body := openAPIDo(t, "GET", base+"/cmdPing", ""); code != 200 || !strings.Contains(body, `"ping"`) {
		t.Errorf("GET /cmdPing: %d %s", code, body)
	}
	if code, _ := openAPIDo(t, "DELETE", base+"/cmdPing", ""); code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE on a GET-only URI: %d, want 405 as before", code)
	}

	if ep := a.FindEndPoint("POST", "/members"); ep == nil || ep.Title != "create" {
		t.Errorf("FindEndPoint(POST, /members) = %+v", ep)
	}
	if ep := a.FindEndPoint("DELETE", "/members"); ep != nil {
		t.Errorf("FindEndPoint(DELETE, /members) = %+v, want nil", ep)
	}
	if ep := a.FindEndPointByURI("/members"); ep == nil || ep.Title != "list" {
		t.Errorf("FindEndPointByURI must keep answering the first registered: %+v", ep)
	}
}

// endPointForMethod falls back to the first endpoint on the URI, so a URI with
// one endpoint is served exactly as it was.
func TestEndPointForMethodFallsBackToTheFirst(t *testing.T) {
	groups := endPointsByURI([]DXAPIEndPoint{
		{Uri: "/a", Method: "GET", Title: "a-get"},
		{Uri: "/b", Method: "POST", Title: "b-post"},
		{Uri: "/a", Method: "POST", Title: "a-post"},
	})
	if len(groups) != 2 || groups[0][0].Uri != "/a" || len(groups[0]) != 2 || groups[1][0].Uri != "/b" {
		t.Fatalf("groups = %+v", groups)
	}
	for method, want := range map[string]string{"GET": "a-get", "POST": "a-post", "PUT": "a-get", "OPTIONS": "a-get"} {
		if got := endPointForMethod(groups[0], method).Title; got != want {
			t.Errorf("%s /a -> %s, want %s", method, got, want)
		}
	}
	if got := endPointForMethod(groups[1], "GET").Title; got != "b-post" {
		t.Errorf("GET /b -> %s, want b-post", got)
	}

	// A WebSocket endpoint registered first does not take a method nobody
	// serves when an HTTP endpoint shares the URI; alone, it still does.
	mixed := endPointsByURI([]DXAPIEndPoint{
		{Uri: "/live", Method: "GET", Title: "ws", EndPointType: EndPointTypeWS},
		{Uri: "/live", Method: "POST", Title: "http", EndPointType: EndPointTypeHTTPJSON},
		{Uri: "/only", Method: "GET", Title: "ws-only", EndPointType: EndPointTypeWS},
	})
	for method, want := range map[string]string{"GET": "ws", "POST": "http", "PUT": "http"} {
		if got := endPointForMethod(mixed[0], method).Title; got != want {
			t.Errorf("%s /live -> %s, want %s", method, got, want)
		}
	}
	if got := endPointForMethod(mixed[1], "PUT").Title; got != "ws-only" {
		t.Errorf("PUT /only -> %s, want ws-only", got)
	}
}

// The duplicate check at registration compares the method without case.
func TestFindEndPointFoldComparesMethodWithoutCase(t *testing.T) {
	a := openAPITestAPI(t, "route-by-method-fold")
	a.NewEndPoint("list", "", "/members", "get", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	if a.findEndPointFold("GET", "/members") == nil {
		t.Errorf("GET must find the endpoint registered as get")
	}
	if a.FindEndPoint("GET", "/members") != nil {
		t.Errorf("FindEndPoint compares exactly, as the request is routed")
	}
}

const routeByMethodSpec = `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /members:
    get:
      operationId: listMembers
      parameters:
        - {name: page, in: query, schema: {type: integer, minimum: 0}}
    post:
      operationId: createMember
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                name: {type: string, minLength: 1}
              required: [name]
  /members/{id}:
    get:
      operationId: getMember
      parameters:
        - {name: id, in: path, required: true, schema: {type: integer, minimum: 1}}
    delete:
      operationId: deleteMember
      parameters:
        - {name: id, in: path, required: true, schema: {type: string, minLength: 3}}
`

// A document with several methods on one path binds one endpoint per
// operation, each with its own parameters, path parameters included.
func TestOpenAPIBindsSeveralMethodsOnOnePath(t *testing.T) {
	reply := func(aepr *DXAPIEndPointRequest) error {
		v, _ := aepr.GetParameterValueEntry("id")
		var id any
		if v != nil {
			id = v.Value
		}
		aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"method": aepr.Request.Method, "id": id})
		return nil
	}
	a := openAPIStartAPI(t, "route-by-method-bind", func(a *DXAPI) {
		for _, id := range []string{"listMembers", "createMember", "getMember", "deleteMember"} {
			a.RegisterHandler(id, reply)
		}
		if err := a.BindOpenAPI(openAPIMustRead(t, routeByMethodSpec)); err != nil {
			t.Fatal(err)
		}
	})
	base := "http://" + a.Address

	if len(a.EndPoints) != 4 {
		t.Fatalf("%d endpoints, want 4", len(a.EndPoints))
	}
	if code, body := openAPIDo(t, "GET", base+"/members?page=2", ""); code != 200 || !strings.Contains(body, `"GET"`) {
		t.Errorf("GET /members: %d %s", code, body)
	}
	if code, body := openAPIDo(t, "POST", base+"/members", `{"name":"a"}`); code != 200 || !strings.Contains(body, `"POST"`) {
		t.Errorf("POST /members: %d %s", code, body)
	}
	// GET reads id as an integer of at least 1, DELETE as a string of at
	// least three characters: each operation keeps its own declaration.
	if code, body := openAPIDo(t, "GET", base+"/members/42", ""); code != 200 || !strings.Contains(body, `"id":42`) {
		t.Errorf("GET /members/42: %d %s", code, body)
	}
	if code, _ := openAPIDo(t, "GET", base+"/members/abc", ""); code != http.StatusUnprocessableEntity {
		t.Errorf("GET /members/abc: %d, want 422 (integer)", code)
	}
	if code, body := openAPIDo(t, "DELETE", base+"/members/abc", ""); code != 200 || !strings.Contains(body, `"id":"abc"`) {
		t.Errorf("DELETE /members/abc: %d %s", code, body)
	}
	if code, _ := openAPIDo(t, "DELETE", base+"/members/ab", ""); code != http.StatusUnprocessableEntity {
		t.Errorf("DELETE /members/ab: %d, want 422 (minLength 3)", code)
	}
	if code, _ := openAPIDo(t, "PUT", base+"/members", ""); code != http.StatusMethodNotAllowed {
		t.Errorf("PUT /members: %d, want 405", code)
	}
}

// Re-emitting a bound document keeps the operationIds it gave, and the
// document survives the three round-trip legs byte for byte.
func TestOpenAPIRoundTripOverSeveralMethodsOnOnePath(t *testing.T) {
	a := openAPITestAPI(t, "route-by-method-roundtrip")
	doc := openAPIMustRead(t, routeByMethodSpec)
	openAPIRegisterEverything(a, doc)
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	emitted, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"listMembers"`, `"createMember"`, `"getMember"`, `"deleteMember"`} {
		if !strings.Contains(string(emitted), want) {
			t.Errorf("re-emitted document lost the operationId %s", want)
		}
	}
	openAPIRoundTrip(t, "several-methods", emitted)
}

// Endpoints registered in code: a URI alone keeps the id derived from it, and
// every endpoint on a URI that carries several methods takes the method as a
// prefix.
func TestOpenAPIEmitNamesSeveralMethodsOnOneURI(t *testing.T) {
	a := openAPITestAPI(t, "route-by-method-emit")
	a.NewEndPoint("list", "", "/members", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("create", "", "/members", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		[]DXAPIEndPointParameter{{NameId: "name", Type: dxlibTypes.APIParameterTypeString}}, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("ping", "", "/v2/cmdPing", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	emitted, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(emitted, &doc); err != nil {
		t.Fatal(err)
	}
	members, _ := doc["paths"].(map[string]any)["/members"].(map[string]any)
	if len(members) != 2 {
		t.Fatalf("/members has %d operations, want 2: %v", len(members), members)
	}
	if id := members["get"].(map[string]any)["operationId"]; id != "get_members" {
		t.Errorf("GET /members operationId = %v", id)
	}
	if id := members["post"].(map[string]any)["operationId"]; id != "post_members" {
		t.Errorf("POST /members operationId = %v", id)
	}
	if id := openAPIProbe(t, doc, "paths", "/v2/cmdPing", "post", "operationId"); id != "v2_cmdPing" {
		t.Errorf("a URI alone keeps its derived id, got %v", id)
	}
	if OpenAPIOperationIdForMethod("DELETE", "/v2/members/{id}") != "delete_v2_members_id" {
		t.Errorf("OpenAPIOperationIdForMethod = %s", OpenAPIOperationIdForMethod("DELETE", "/v2/members/{id}"))
	}
	openAPIRoundTrip(t, "several-methods-in-code", emitted)
}

// The binder refuses a method already registered on a URI, and takes
// another method on it.
func TestOpenAPIBindClaimsMethodAndURI(t *testing.T) {
	a := openAPITestAPI(t, "route-by-method-claim")
	a.NewEndPoint("list", "", "/members", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	a.RegisterHandler("createMember", noop)
	doc := openAPIMustRead(t, "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /members:\n    post: {operationId: createMember}\n")
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatalf("another method on a registered URI: %v", err)
	}
	if len(a.EndPoints) != 2 || a.FindEndPoint("POST", "/members") == nil {
		t.Errorf("endpoints = %d", len(a.EndPoints))
	}

	b := openAPITestAPI(t, "route-by-method-claim-ws")
	b.RegisterHandler("x", noop)
	b.RegisterWSHandler("xws", DXOpenAPIWSHandler{OnMessage: func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) { return m, nil }})
	doc = openAPIMustRead(t, `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get: {operationId: x}
x-dxlib-websocket-endpoints:
  description: d
  endpoints:
    - {operationId: xws, path: /x, method: GET}
`)
	err := b.BindOpenAPI(doc)
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_METHOD_AND_URI_ALREADY_REGISTERED:GET:/x:/paths/~1x/get:/x-dxlib-websocket-endpoints/endpoints/0") {
		t.Fatalf("err = %v", err)
	}
	if len(b.EndPoints) != 0 {
		t.Errorf("a refused document must bind nothing")
	}
}
