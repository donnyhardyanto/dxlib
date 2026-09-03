package api

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// openAPIStartAPI starts a plaintext DXAPI on a free port the way app.start
// does and waits for it to listen. It is deliberately its own helper, so
// these tests do not lean on another test file's fixtures.
func openAPIStartAPI(t *testing.T, name string, define func(a *DXAPI)) *DXAPI {
	t.Helper()
	a := openAPITestAPI(t, name)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a.Address = ln.Addr().String()
	_ = ln.Close()
	a.WriteTimeoutSec, a.ReadTimeoutSec = 30, 30
	define(a)
	group, _ := errgroup.WithContext(context.Background())
	if err := a.StartAndWait(group); err != nil {
		t.Fatalf("StartAndWait: %v", err)
	}
	t.Cleanup(func() {
		if a.HTTPServer != nil {
			_ = a.HTTPServer.Close()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", a.Address, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return a
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s did not start listening at %s", name, a.Address)
	return nil
}

func openAPIDo(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func openAPIMustRead(t *testing.T, src string) *DXOpenAPIDocument {
	t.Helper()
	doc, err := ReadOpenAPI([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

const openAPIBindTestSpec = `openapi: 3.1.0
info: {title: bound, version: "1"}
paths:
  /cmdEcho:
    post:
      operationId: cmdEcho
      summary: echo
      description: echoes what it was sent
      x-dxlib-privileges: [echo-privilege]
      x-dxlib-max-content-length: 1024
      x-dxlib-rate-limit-group: chatty
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                token: {type: string}
                n: {type: integer, format: int64, minimum: 1}
                tags: {type: array, items: {type: string}}
              required: [token]
  /users/{id}/files/{file_id}:
    get:
      operationId: userFile
      parameters:
        - {name: id, in: path, required: true, schema: {type: integer, format: int64}}
        - {name: file_id, in: path, required: true, schema: {type: string}}
        - {name: note, in: query, schema: {type: string}}
  /users/me:
    get:
      operationId: usersMe
x-dxlib-websocket-endpoints:
  description: d
  endpoints:
    - {operationId: ws, path: /ws, method: GET, periodicInterval: 1s, privileges: [ws-connect]}
`

// The document binds, the endpoints serve, and what they serve is what the
// document declared: mandatory parameters enforced, int64p enforced, path
// parameters typed and readable through GetParameterValueAs*, a literal
// segment winning over a wildcard, and a WebSocket frame echoed.
func TestOpenAPIBoundEndPointsServeRequests(t *testing.T) {
	a := openAPIStartAPI(t, "openapi-bind-serve", func(a *DXAPI) {
		a.NewEndPoint("cmdPing", "", "/cmdPing", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, func(aepr *DXAPIEndPointRequest) error {
			aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"pong": true})
			return nil
		}, nil, nil, nil, nil, 0, "")
		a.RegisterHandler("cmdEcho", func(aepr *DXAPIEndPointRequest) error {
			_, token, err := aepr.GetParameterValueAsString("token")
			if err != nil {
				return err
			}
			_, n, _ := aepr.GetParameterValueAsInt64("n")
			aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"token": token, "n": n})
			return nil
		}, func(aepr *DXAPIEndPointRequest) error {
			aepr.LocalData["middleware-ran"] = true
			return nil
		})
		a.RegisterHandler("userFile", func(aepr *DXAPIEndPointRequest) error {
			_, id, err := aepr.GetParameterValueAsInt64("id")
			if err != nil {
				return err
			}
			_, fileId, err := aepr.GetParameterValueAsString("file_id")
			if err != nil {
				return err
			}
			_, note, _ := aepr.GetParameterValueAsString("note")
			aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"id": id, "file_id": fileId, "note": note, "middleware-ran": aepr.LocalData["middleware-ran"]})
			return nil
		}, func(aepr *DXAPIEndPointRequest) error {
			// An endpoint's own middleware runs after the path parameters
			// are in place and can read them too.
			_, id, err := aepr.GetParameterValueAsInt64("id")
			if err != nil {
				return err
			}
			aepr.LocalData["middleware-ran"] = id
			return nil
		})
		a.RegisterHandler("usersMe", func(aepr *DXAPIEndPointRequest) error {
			aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"me": true})
			return nil
		})
		a.RegisterWSHandler("ws", DXOpenAPIWSHandler{OnMessage: func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) {
			return []byte(strings.ToUpper(string(m))), nil
		}})
		if err := a.BindOpenAPI(openAPIMustRead(t, openAPIBindTestSpec)); err != nil {
			t.Fatal(err)
		}
	})
	base := "http://" + a.Address

	// The declaration is what the document said.
	echo := a.FindEndPointByURI("/cmdEcho")
	if echo == nil {
		t.Fatal("cmdEcho not bound")
	}
	if echo.Method != "POST" || echo.RequestContentType != utilsHttp.RequestContentTypeApplicationJSON || echo.RequestMaxContentLength != 1024 || echo.RateLimitGroupNameId != "chatty" || len(echo.Privileges) != 1 || echo.Privileges[0] != "echo-privilege" || echo.Title != "echo" {
		t.Errorf("cmdEcho declaration = %+v", echo)
	}
	if len(echo.Parameters) != 3 || echo.Parameters[0].NameId != "token" || !echo.Parameters[0].IsMustExist || echo.Parameters[1].Type != dxlibTypes.APIParameterTypeInt64P || echo.Parameters[1].IsMustExist || echo.Parameters[2].Type != dxlibTypes.APIParameterTypeArrayString {
		t.Errorf("cmdEcho parameters = %+v", echo.Parameters)
	}
	if len(echo.Middlewares) != 1 {
		t.Errorf("cmdEcho has %d middlewares, want the one registered", len(echo.Middlewares))
	}
	userFile := a.FindEndPointByURI("/users/{id}/files/{file_id}")
	if len(userFile.Parameters) != 1 || userFile.Parameters[0].NameId != "note" {
		t.Errorf("path parameters must not be in Parameters: %+v", userFile.Parameters)
	}
	if len(userFile.Middlewares) != 2 {
		t.Errorf("userFile has %d middlewares, want the path-parameter one plus the registered one", len(userFile.Middlewares))
	}
	if a.FindEndPointByURI("/cmdPing") == nil {
		t.Errorf("the NewEndPoint endpoint must be untouched")
	}

	// And the endpoints behave as declared.
	if code, body := openAPIDo(t, "GET", base+"/cmdPing", ""); code != 200 || !strings.Contains(body, "pong") {
		t.Errorf("cmdPing: %d %s", code, body)
	}
	if code, body := openAPIDo(t, "POST", base+"/cmdEcho", `{"token":"t-1","n":5}`); code != 200 || !strings.Contains(body, `"t-1"`) || !strings.Contains(body, `5`) {
		t.Errorf("cmdEcho: %d %s", code, body)
	}
	if code, _ := openAPIDo(t, "POST", base+"/cmdEcho", `{"n":5}`); code != http.StatusUnprocessableEntity {
		t.Errorf("cmdEcho without the mandatory token: %d, want 422", code)
	}
	if code, _ := openAPIDo(t, "POST", base+"/cmdEcho", `{"token":"t","n":0}`); code != http.StatusUnprocessableEntity {
		t.Errorf("cmdEcho with n=0 against minimum 1: %d, want 422", code)
	}
	if code, _ := openAPIDo(t, "GET", base+"/cmdEcho", ""); code != http.StatusMethodNotAllowed {
		t.Errorf("GET on a POST endpoint: %d, want 405", code)
	}
	if code, body := openAPIDo(t, "GET", base+"/users/42/files/report.pdf?note=hi", ""); code != 200 || !strings.Contains(body, `"id":42`) || !strings.Contains(body, `"report.pdf"`) || !strings.Contains(body, `"note":"hi"`) || !strings.Contains(body, `"middleware-ran":42`) {
		t.Errorf("path parameters: %d %s", code, body)
	}
	if code, _ := openAPIDo(t, "GET", base+"/users/forty-two/files/x", ""); code != http.StatusUnprocessableEntity {
		t.Errorf("non-integer path parameter: %d, want 422", code)
	}
	if code, body := openAPIDo(t, "GET", base+"/users/me", ""); code != 200 || !strings.Contains(body, `"me":true`) {
		t.Errorf("literal segment beside a wildcard: %d %s", code, body)
	}
	if code, _ := openAPIDo(t, "GET", base+"/users/42", ""); code != http.StatusNotFound {
		t.Errorf("a partial path must not route anywhere: %d, want 404", code)
	}

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+a.Address+"/ws", nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, reply, err := conn.ReadMessage()
	if err != nil || string(reply) != "HELLO" {
		t.Errorf("websocket reply = %q err=%v", reply, err)
	}
	ws := a.FindEndPointByURI("/ws")
	if ws.EndPointType != EndPointTypeWS || ws.WSPeriodicInterval != time.Second || len(ws.Privileges) != 1 {
		t.Errorf("ws declaration = %+v", ws)
	}
}

func TestOpenAPIDriftNamesBothSides(t *testing.T) {
	a := openAPITestAPI(t, "openapi-bind-drift")
	a.RegisterHandler("userFile", noop)
	a.RegisterHandler("cmdRetired", noop)
	a.RegisterWSHandler("events", DXOpenAPIWSHandler{OnMessage: func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) { return nil, nil }})
	doc := openAPIMustRead(t, openAPIBindTestSpec)

	specWithoutHandler, handlerWithoutSpec := a.OpenAPIDrift(doc)
	if strings.Join(specWithoutHandler, ",") != "cmdEcho,usersMe,ws" {
		t.Errorf("spec without handler = %v", specWithoutHandler)
	}
	if strings.Join(handlerWithoutSpec, ",") != "cmdRetired,events" {
		t.Errorf("handler without spec = %v", handlerWithoutSpec)
	}
	err := a.BindOpenAPI(doc)
	want := "OPENAPI_DRIFT:openapi-bind-drift:SPEC_WITHOUT_HANDLER=[cmdEcho,usersMe,ws]:HANDLER_WITHOUT_SPEC=[cmdRetired,events]"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v\nwant %s", err, want)
	}
	if len(a.EndPoints) != 0 {
		t.Errorf("a failed bind must leave the API untouched")
	}
}

func TestOpenAPIBindRefusesAHandlerRegisteredTwice(t *testing.T) {
	a := openAPITestAPI(t, "openapi-bind-twice")
	doc := openAPIMustRead(t, "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    get: {operationId: x}\n")
	a.RegisterHandler("x", noop)
	a.RegisterHandler("x", noop)
	err := a.BindOpenAPI(doc)
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_HANDLER_REGISTERED_TWICE:openapi-bind-twice:x") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAPIBindRefusesAURIAlreadyRegistered(t *testing.T) {
	a := openAPITestAPI(t, "openapi-bind-uri")
	a.NewEndPoint("x", "", "/x", "GET", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone, nil, noop, nil, nil, nil, nil, 0, "")
	doc := openAPIMustRead(t, "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    get: {operationId: x}\n")
	a.RegisterHandler("x", noop)
	err := a.BindOpenAPI(doc)
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_URI_ALREADY_REGISTERED:/x:already registered on the API") {
		t.Fatalf("err = %v", err)
	}
	if len(a.EndPoints) != 1 {
		t.Errorf("the existing endpoint must be the only one")
	}
}

// Go's ServeMux panics on two patterns that can both match one request and
// neither is more specific. Registering them at StartAndWait would take the
// process down after every define step had run; here it is a load error.
func TestOpenAPIBindRefusesConflictingMuxPatterns(t *testing.T) {
	a := openAPITestAPI(t, "openapi-bind-mux")
	doc := openAPIMustRead(t, `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /a/{x}/b:
    get:
      operationId: one
      parameters: [{name: x, in: path, required: true, schema: {type: string}}]
  /a/c/{y}:
    get:
      operationId: two
      parameters: [{name: y, in: path, required: true, schema: {type: string}}]
`)
	a.RegisterHandler("one", noop)
	a.RegisterHandler("two", noop)
	err := a.BindOpenAPI(doc)
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_PATH_CONFLICT:") || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAPIBindIsAllOrNothing(t *testing.T) {
	a := openAPITestAPI(t, "openapi-bind-atomic")
	doc := openAPIMustRead(t, openAPIBindTestSpec)
	a.RegisterHandler("cmdEcho", noop)
	a.RegisterHandler("userFile", noop)
	a.RegisterHandler("usersMe", noop)
	a.RegisterHandler("ws", noop) // an HTTP handler for the WebSocket operation
	err := a.BindOpenAPI(doc)
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_WS_OPERATION_HAS_HTTP_HANDLER:ws") {
		t.Fatalf("err = %v", err)
	}
	if len(a.EndPoints) != 0 {
		t.Errorf("three good operations must not be bound when the fourth fails; got %d endpoints", len(a.EndPoints))
	}
}

func TestOpenAPIBindRefusesAWSHandlerForAnHTTPOperation(t *testing.T) {
	a := openAPITestAPI(t, "openapi-bind-wrong-kind")
	doc := openAPIMustRead(t, "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    get: {operationId: x}\n")
	a.RegisterWSHandler("x", DXOpenAPIWSHandler{OnMessage: func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) { return nil, nil }})
	err := a.BindOpenAPI(doc)
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_HTTP_OPERATION_HAS_WS_HANDLER:x") {
		t.Fatalf("err = %v", err)
	}
}

// $ref into components/schemas resolves for a body, and a cycle is named.
func TestOpenAPIBindResolvesLocalReferences(t *testing.T) {
	a := openAPITestAPI(t, "openapi-bind-ref")
	doc := openAPIMustRead(t, `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema: {$ref: '#/components/schemas/Body'}
components:
  schemas:
    Body:
      type: object
      properties:
        token: {type: string}
        child: {$ref: '#/components/schemas/Child'}
      required: [token]
    Child:
      type: object
      properties:
        n: {type: integer}
`)
	a.RegisterHandler("x", noop)
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatal(err)
	}
	ep := a.FindEndPointByURI("/x")
	if len(ep.Parameters) != 2 || ep.Parameters[0].NameId != "token" || !ep.Parameters[0].IsMustExist || ep.Parameters[1].Type != dxlibTypes.APIParameterTypeJSON || len(ep.Parameters[1].Children) != 1 || ep.Parameters[1].Children[0].Type != dxlibTypes.APIParameterTypeInt64 {
		t.Errorf("parameters = %+v", ep.Parameters)
	}

	b := openAPITestAPI(t, "openapi-bind-ref-cycle")
	cyclic := openAPIMustRead(t, `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema: {$ref: '#/components/schemas/A'}
components:
  schemas:
    A:
      type: object
      properties:
        b: {$ref: '#/components/schemas/B'}
    B:
      type: object
      properties:
        a: {$ref: '#/components/schemas/A'}
`)
	b.RegisterHandler("x", noop)
	err := b.BindOpenAPI(cyclic)
	if err == nil || !strings.Contains(err.Error(), "OPENAPI_REF_CYCLE:#/components/schemas/A") {
		t.Fatalf("err = %v", err)
	}
}

// Inference without x-dxlib-type refuses what dxlib would not enforce.
func TestOpenAPIBindRefusesConstraintsDXLibDoesNotEnforce(t *testing.T) {
	for name, schema := range map[string]string{
		"minimum 5":             `{type: integer, minimum: 5}`,
		"minLength 3":           `{type: string, minLength: 3}`,
		"array of numbers":      `{type: array, items: {type: number}}`,
		"map of integers":       `{type: object, additionalProperties: {type: integer}}`,
		"x-dxlib-type mismatch": `{type: string, x-dxlib-type: int64}`,
		"format mismatch":       `{type: integer, format: int32, x-dxlib-type: int64}`,
	} {
		t.Run(name, func(t *testing.T) {
			a := openAPITestAPI(t, "openapi-bind-infer-"+name)
			doc := openAPIMustRead(t, "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    get:\n      operationId: x\n      parameters:\n        - {name: q, in: query, schema: "+schema+"}\n")
			a.RegisterHandler("x", noop)
			err := a.BindOpenAPI(doc)
			if err == nil {
				t.Fatalf("bound a parameter dxlib cannot validate as declared: %s", schema)
			}
			if !strings.Contains(err.Error(), "/paths/~1x/get/parameters/0/schema") {
				t.Errorf("error does not point at the schema: %v", err)
			}
		})
	}
}

// LoadOpenAPIFile is the startup entry point and is fatal, the way
// ApplyConfigurations is on a bad address: the child re-runs this test with
// the flag set, and the parent checks the exit status and the message.
func TestLoadOpenAPIFileIsFatalOnDrift(t *testing.T) {
	const flag = "DXLIB_API_OPENAPI_CRASH_CHILD"
	if os.Getenv(flag) == "1" {
		path := filepath.Join(t.TempDir(), "api.openapi.yaml")
		_ = os.WriteFile(path, []byte("openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /cmdX:\n    get: {operationId: cmdX}\n"), 0o644)
		a, _ := Manager.NewAPI("crash")
		a.RegisterHandler("cmdY", noop)
		_ = a.LoadOpenAPIFile(path)
		fmt.Fprintln(os.Stderr, "LOAD_OPENAPI_FILE_RETURNED_INSTEAD_OF_EXITING")
		os.Exit(3)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestLoadOpenAPIFileIsFatalOnDrift$", "-test.v")
	cmd.Env = append(os.Environ(), flag+"=1")
	out, err := cmd.CombinedOutput()
	exitErr, isExit := err.(*exec.ExitError)
	if !isExit {
		t.Fatalf("child did not exit with a failure status (err=%v); output:\n%s", err, out)
	}
	if code := exitErr.ExitCode(); code != 1 {
		t.Errorf("child exit code %d, want 1 (the fatal-log exit); output:\n%s", code, out)
	}
	want := "OPENAPI_LOAD_ERROR:crash:OPENAPI_DRIFT:crash:SPEC_WITHOUT_HANDLER=[cmdX]:HANDLER_WITHOUT_SPEC=[cmdY]"
	if !strings.Contains(string(out), want) {
		t.Errorf("child output does not carry %q:\n%s", want, out)
	}
}
