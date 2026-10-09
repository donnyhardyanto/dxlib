package api

import (
	"reflect"
	"strings"
	"testing"

	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

func openAPIMiddlewareAuth(aepr *DXAPIEndPointRequest) error  { return nil }
func openAPIMiddlewareAudit(aepr *DXAPIEndPointRequest) error { return nil }

type openAPIMiddlewareHolder struct{}

func (openAPIMiddlewareHolder) Check(aepr *DXAPIEndPointRequest) error { return nil }

const (
	openAPIMiddlewareAuthName  = "github.com/donnyhardyanto/dxlib/api.openAPIMiddlewareAuth"
	openAPIMiddlewareAuditName = "github.com/donnyhardyanto/dxlib/api.openAPIMiddlewareAudit"
)

func openAPIMiddlewaresOf(t *testing.T, m map[string]any, path ...string) []string {
	t.Helper()
	parent := openAPIProbe(t, m, path[:len(path)-1]...).(map[string]any)
	raw, ok := parent[path[len(path)-1]]
	if !ok {
		return nil
	}
	var names []string
	for _, v := range raw.([]any) {
		names = append(names, v.(string))
	}
	return names
}

// Each operation names its middlewares in the order they run: a plain
// function, a method value without the compiler's -fm suffix, a nil entry
// skipped. An operation with none has no key at all, and a WebSocket endpoint
// carries the list under its own block's name.
func TestOpenAPIEmitsTheMiddlewareChainInOrder(t *testing.T) {
	a := openAPITestAPI(t, "openapi-middlewares-emit")
	a.NewEndPoint("guarded", "", "/guarded", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		nil, noop, nil, nil,
		[]DXAPIEndPointExecuteFunc{openAPIMiddlewareAuth, nil, openAPIMiddlewareHolder{}.Check, openAPIMiddlewareAudit},
		nil, 0, "")
	a.NewEndPoint("open", "", "/open", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone,
		nil, noop, nil, nil, nil, nil, 0, "")
	a.NewWSEndPoint("ws", "", "/ws", "GET", nil, nil, nil, nil, 0,
		[]DXAPIEndPointExecuteFunc{openAPIMiddlewareAuth}, nil, "")

	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	m := openAPIDecode(t, b)

	got := openAPIMiddlewaresOf(t, m, "paths", "/guarded", "post", OpenAPIExtensionMiddlewares)
	if len(got) != 3 {
		t.Fatalf("x-dxlib-middlewares = %v, want three names\n%s", got, b)
	}
	if got[0] != openAPIMiddlewareAuthName || got[2] != openAPIMiddlewareAuditName {
		t.Errorf("x-dxlib-middlewares = %v, want %s first and %s last", got, openAPIMiddlewareAuthName, openAPIMiddlewareAuditName)
	}
	if !strings.HasSuffix(got[1], "openAPIMiddlewareHolder.Check") || strings.HasSuffix(got[1], "-fm") {
		t.Errorf("method value named %q, want ...openAPIMiddlewareHolder.Check without -fm", got[1])
	}
	if names := openAPIMiddlewaresOf(t, m, "paths", "/open", "get", OpenAPIExtensionMiddlewares); names != nil {
		t.Errorf("an operation with no middlewares carries %v", names)
	}
	ws := openAPIProbe(t, m, OpenAPIExtensionWebSocketEndPoints, "endpoints").([]any)[0].(map[string]any)
	if got := openAPIMiddlewaresOf(t, ws, "middlewares"); !reflect.DeepEqual(got, []string{openAPIMiddlewareAuthName}) {
		t.Errorf("WebSocket middlewares = %v", got)
	}
}

// A bound path template gets the binder's path-parameter middleware in front
// of the registered chain. The document names only the chain the service
// registered.
func TestOpenAPIMiddlewaresLeaveOutTheBindersPathParameterStep(t *testing.T) {
	doc := openAPIMustRead(t, `openapi: 3.1.0
info: {title: t, version: "1"}
paths:
  /users/{id}:
    get:
      operationId: user
      parameters:
        - {name: id, in: path, required: true, schema: {type: integer, format: int64}}
  /users/me:
    get:
      operationId: me
`)
	a := openAPITestAPI(t, "openapi-middlewares-bound")
	a.RegisterHandler("user", noop, openAPIMiddlewareAuth)
	a.RegisterHandler("me", noop, openAPIMiddlewareAuth, openAPIMiddlewareAudit)
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	if ep := a.FindEndPoint("GET", "/users/{id}"); ep == nil || len(ep.Middlewares) != 2 {
		t.Fatalf("the bound template should run the path-parameter step and one registered middleware")
	}
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	m := openAPIDecode(t, b)
	if got := openAPIMiddlewaresOf(t, m, "paths", "/users/{id}", "get", OpenAPIExtensionMiddlewares); !reflect.DeepEqual(got, []string{openAPIMiddlewareAuthName}) {
		t.Errorf("/users/{id}: %v, want only the registered middleware", got)
	}
	if got := openAPIMiddlewaresOf(t, m, "paths", "/users/me", "get", OpenAPIExtensionMiddlewares); !reflect.DeepEqual(got, []string{openAPIMiddlewareAuthName, openAPIMiddlewareAuditName}) {
		t.Errorf("/users/me: %v", got)
	}
}

// A document carrying the lists reads back to the same bytes, and binding it
// does not take the names as instructions: the registered chain is what runs
// and what is emitted afterwards.
func TestOpenAPIMiddlewaresRoundTripAndAreNotBound(t *testing.T) {
	src := `{
  "openapi": "3.1.0",
  "info": {
    "title": "t",
    "version": "1"
  },
  "paths": {
    "/cmd": {
      "post": {
        "operationId": "cmd",
        "x-dxlib-endpoint-type": "EndPointTypeHTTPJSON",
        "x-dxlib-middlewares": [
          "example.com/svc/handler.MiddlewareTokenAuth",
          "example.com/svc/handler.MiddlewareAudit"
        ]
      }
    }
  },
  "x-dxlib-websocket-endpoints": {
    "description": "d",
    "endpoints": [
      {
        "operationId": "ws",
        "path": "/ws",
        "method": "GET",
        "middlewares": [
          "example.com/svc/handler.MiddlewareTokenAuth"
        ]
      }
    ]
  }
}
`
	doc := openAPIMustRead(t, src)
	out, err := doc.AsJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != src {
		t.Fatalf("read -> AsJSON changed the document:\n%s", out)
	}

	a := openAPITestAPI(t, "openapi-middlewares-not-bound")
	a.RegisterHandler("cmd", noop, openAPIMiddlewareAudit)
	a.RegisterWSHandler("ws", DXOpenAPIWSHandler{OnLoop: noop})
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	if ep := a.FindEndPoint("POST", "/cmd"); ep == nil || len(ep.Middlewares) != 1 {
		t.Fatalf("binding took the document's names: %+v", ep)
	}
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	m := openAPIDecode(t, b)
	if got := openAPIMiddlewaresOf(t, m, "paths", "/cmd", "post", OpenAPIExtensionMiddlewares); !reflect.DeepEqual(got, []string{openAPIMiddlewareAuditName}) {
		t.Errorf("re-emitted %v, want the registered chain", got)
	}
}

func TestOpenAPIReaderRefusesABadMiddlewareList(t *testing.T) {
	for name, list := range map[string]string{
		"empty name": `[""]`,
		"not a list": `"example.com/svc.M"`,
		"not names":  `[1]`,
	} {
		_, err := ReadOpenAPI([]byte(`{"openapi":"3.1.0","info":{"title":"t","version":"1"},"paths":{"/c":{"post":{"operationId":"c","x-dxlib-middlewares":` + list + `}}}}`))
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
