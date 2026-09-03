package api

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The round trip is the correctness argument for the reader and the binder.
// The emitter defines the dialect; if a document it wrote comes back through
// the reader unchanged, and through the reader and the binder and the emitter
// again unchanged, then the reader and the binder are correct for every
// construct the dialect contains. The corpus in testdata/openapi is the
// output of the emitter over the real services' endpoint definitions -- 105
// operations at the time of writing -- so this is not a test over invented
// examples.
//
// Leg 1: bytes -> ReadOpenAPI -> AsJSON == bytes.
// Leg 2: ReadOpenAPI -> RegisterHandler for every id -> BindOpenAPI onto a
// fresh API -> OpenAPIAsJSON == bytes.
//
// Leg 3 reads the same document as YAML, which is how a hand-edited file
// will arrive.

// openAPIFirstDifference points at the first differing line, because two
// 120 KB documents dumped whole tell a reader nothing.
func openAPIFirstDifference(a, b []byte) string {
	al, bl := strings.Split(string(a), "\n"), strings.Split(string(b), "\n")
	for i := 0; i < len(al) && i < len(bl); i++ {
		if al[i] != bl[i] {
			return fmt.Sprintf("line %d:\n  want: %s\n  got:  %s", i+1, al[i], bl[i])
		}
	}
	return fmt.Sprintf("lengths differ: %d vs %d lines", len(al), len(bl))
}

// openAPIRegisterEverything gives every operation in the document a handler
// of the right kind, so the drift check passes and the binder runs.
func openAPIRegisterEverything(a *DXAPI, doc *DXOpenAPIDocument) {
	ws := map[string]bool{}
	if doc.WebSocketEndPoints != nil {
		for _, e := range doc.WebSocketEndPoints.EndPoints {
			ws[e.OperationId] = true
		}
	}
	for _, id := range doc.OperationIds() {
		if ws[id] {
			a.RegisterWSHandler(id, DXOpenAPIWSHandler{OnMessage: func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) { return m, nil }})
		} else {
			a.RegisterHandler(id, noop)
		}
	}
}

// openAPIJSONToYAML rewrites emitted JSON as block-style YAML, keeping the
// scalars quoted as they were so "1.0.0" cannot become a number.
func openAPIJSONToYAML(t *testing.T, b []byte) []byte {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal(b, &n); err != nil {
		t.Fatalf("yaml cannot parse the emitted JSON: %v", err)
	}
	var block func(*yaml.Node)
	block = func(n *yaml.Node) {
		if n.Kind == yaml.MappingNode || n.Kind == yaml.SequenceNode || n.Kind == yaml.DocumentNode {
			n.Style = 0
		}
		for _, c := range n.Content {
			block(c)
		}
	}
	block(&n)
	out, err := yaml.Marshal(&n)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func openAPIRoundTrip(t *testing.T, label string, original []byte) *DXOpenAPIDocument {
	t.Helper()

	// Leg 1.
	doc, err := ReadOpenAPI(original)
	if err != nil {
		t.Fatalf("%s: reader refused the emitter's own output: %v", label, err)
	}
	again, err := doc.AsJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(original) {
		t.Fatalf("%s: read then emit differs from the original at %s", label, openAPIFirstDifference(original, again))
	}

	// Leg 2. The fresh API takes the document's own title and version, since
	// the emitter writes info from the API and the comparison is exact.
	a := openAPITestAPI(t, "roundtrip:"+label)
	a.NameId, a.Version = doc.Info.Title, doc.Info.Version
	openAPIRegisterEverything(a, doc)
	if err := a.BindOpenAPI(doc); err != nil {
		t.Fatalf("%s: bind failed: %v", label, err)
	}
	if got, want := len(a.EndPoints), len(doc.OperationIds()); got != want {
		t.Fatalf("%s: bound %d endpoints for %d operations", label, got, want)
	}
	rebound, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(rebound) != string(original) {
		t.Fatalf("%s: read, bind, emit differs from the original at %s", label, openAPIFirstDifference(original, rebound))
	}

	// Leg 3.
	fromYAML, err := ReadOpenAPI(openAPIJSONToYAML(t, original))
	if err != nil {
		t.Fatalf("%s: reader refused the YAML form: %v", label, err)
	}
	viaYAML, err := fromYAML.AsJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(viaYAML) != string(original) {
		t.Fatalf("%s: the YAML form reads differently at %s", label, openAPIFirstDifference(original, viaYAML))
	}
	return doc
}

func TestOpenAPIRoundTripOverTheServiceCorpus(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "openapi", "*", "*.openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 6 {
		t.Fatalf("expected the corpus of every service under testdata/openapi, found %v", files)
	}
	operations, webSockets := 0, 0
	for _, f := range files {
		original, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		doc := openAPIRoundTrip(t, f, original)
		operations += len(doc.OperationIds())
		if doc.WebSocketEndPoints != nil {
			webSockets += len(doc.WebSocketEndPoints.EndPoints)
		}
	}
	// A stale or truncated corpus would make the loop above prove nothing,
	// so the count is pinned to what the services actually declare.
	if operations < 100 {
		t.Errorf("corpus holds %d operations; the services declare over 100, so this fixture is stale", operations)
	}
	if webSockets < 1 {
		t.Errorf("corpus holds no WebSocket endpoint; the push-notification relay's /ws is expected")
	}
	t.Logf("%d operations across %d documents, %d of them WebSocket, round-tripped byte for byte", operations, len(files), webSockets)
}

// The corpus exercises the types the services happen to use. This endpoint
// uses every type in the table, plus the constructs the corpus lacks: an
// enum, a nullable, nested children, responses with headers and a data
// template, a query-parameter GET with a content type, and a bound path
// template with a typed parameter.
func TestOpenAPIRoundTripOverEveryType(t *testing.T) {
	a := openAPITestAPI(t, "roundtrip-all-types")
	var params []DXAPIEndPointParameter
	i := 0
	for apiType := range openAPITypeTable {
		i++
		p := DXAPIEndPointParameter{NameId: "p_" + strings.ReplaceAll(string(apiType), "-", "_"), Type: apiType, IsMustExist: i%2 == 0, Description: "of type " + string(apiType)}
		switch apiType {
		case dxlibTypes.APIParameterTypeJSON, dxlibTypes.APIParameterTypeArrayJSONTemplate:
			p.Children = []DXAPIEndPointParameter{
				{NameId: "inner_id", Type: dxlibTypes.APIParameterTypeID, IsMustExist: true},
				{NameId: "inner_note", Type: dxlibTypes.APIParameterTypeNullableString, IsNullable: true},
				{NameId: "inner_json", Type: dxlibTypes.APIParameterTypeJSON, Children: []DXAPIEndPointParameter{{NameId: "deep", Type: dxlibTypes.APIParameterTypeBoolean}}},
			}
		case dxlibTypes.APIParameterTypeString:
			p.Enum = []any{"A", "B"}
		case dxlibTypes.APIParameterTypeInt32:
			p.Enum = []any{1, 2, 3}
		case dxlibTypes.APIParameterTypeNullableInt64:
			p.IsNullable = true
		}
		params = append(params, p)
	}
	// Map iteration order is random, and the emitted order must follow
	// declaration order, so the test fixes an order of its own.
	for x := 0; x < len(params); x++ {
		for y := x + 1; y < len(params); y++ {
			if params[y].NameId < params[x].NameId {
				params[x], params[y] = params[y], params[x]
			}
		}
	}
	a.NewEndPoint("cmdEverything", "every type", "/cmdEverything", "POST", EndPointTypeHTTPEndToEndEncryptionV4, utilsHttp.RequestContentTypeApplicationJSON,
		params, noop, nil, &DXAPIEndPointResponsePossibilities{
			"ok":      {StatusCode: 200, Description: "fine", Headers: map[string]string{"X-Var": "echo"}, DataTemplate: []*DXAPIEndPointParameter{{NameId: "id", Type: dxlibTypes.APIParameterTypeID, IsMustExist: true}}},
			"missing": {StatusCode: 404, Description: "gone"},
		}, nil, []string{"a", "b"}, 1<<20, "heavy")
	a.NewEndPoint("cmdQuery", "", "/cmdQuery", "DELETE", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		[]DXAPIEndPointParameter{{NameId: "id", Type: dxlibTypes.APIParameterTypeInt64ZP, IsMustExist: true}, {NameId: "why", Type: dxlibTypes.APIParameterTypeString}},
		noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("multipart", "", "/multipart", "PUT", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeMultiPartFormData,
		[]DXAPIEndPointParameter{{NameId: "caption", Type: dxlibTypes.APIParameterTypeString}}, noop, nil, nil, nil, nil, 0, "")
	a.NewEndPoint("upload", "", "/upload", "POST", EndPointTypeHTTPUploadStream, utilsHttp.RequestContentTypeApplicationOctetStream,
		[]DXAPIEndPointParameter{{NameId: "name", Type: dxlibTypes.APIParameterTypeString, IsMustExist: true}}, noop, nil, nil, nil, nil, 0, "")
	a.NewWSEndPoint("events", "event socket", "/events", "GET", nil,
		func(aepr *DXAPIEndPointRequest, m []byte) ([]byte, error) { return m, nil }, nil, nil, 7*time.Second, nil, []string{"listen"}, "sockets")

	// A typed path parameter can only come from a bound document, so bind one
	// onto the same API before emitting.
	spec, err := ReadOpenAPI([]byte(`openapi: 3.1.0
info: {title: roundtrip-all-types, version: "1.0.0"}
paths:
  /users/{id}/files/{name}:
    get:
      operationId: users_id_files_name
      parameters:
        - {name: id, in: path, required: true, schema: {type: integer, format: int64, minimum: 1}}
        - {name: name, in: path, required: true, schema: {type: string, minLength: 1}}
        - {name: verbose, in: query, schema: {type: boolean}}
`))
	if err != nil {
		t.Fatal(err)
	}
	a.RegisterHandler("users_id_files_name", noop)
	if err := a.BindOpenAPI(spec); err != nil {
		t.Fatal(err)
	}

	original, err := a.OpenAPIAsJSON()
	if err != nil {
		t.Fatal(err)
	}
	doc := openAPIRoundTrip(t, "all-types", original)

	// And the bound endpoint's declaration is what the document said, type by
	// type, which the byte comparison alone would not show if both sides had
	// lost the same thing.
	item, _ := doc.Paths.Get("/users/{id}/files/{name}")
	if got := item.Get.Parameters[0].Schema.DXLibType; got != "int64p" {
		t.Errorf("path parameter id inferred as %q, want int64p", got)
	}
	if got := item.Get.Parameters[1].Schema.DXLibType; got != "non-empty-string" {
		t.Errorf("path parameter name inferred as %q, want non-empty-string", got)
	}
	everything, _ := doc.Paths.Get("/cmdEverything")
	if everything.Post.EndPointType != "EndPointTypeHTTPEndToEndEncryptionV4" || everything.Post.MaxContentLength != 1<<20 || everything.Post.RateLimitGroup != "heavy" {
		t.Errorf("endpoint facts lost: %+v", everything.Post)
	}
	mt, _ := everything.Post.RequestBody.Content.Get("application/json")
	if mt.Schema.Properties.Len() != len(openAPITypeTable) {
		t.Errorf("%d properties for %d types", mt.Schema.Properties.Len(), len(openAPITypeTable))
	}
	for _, name := range mt.Schema.Properties.Keys() {
		s, _ := mt.Schema.Properties.Get(name)
		if s.DXLibType == "" {
			t.Errorf("%s lost its x-dxlib-type", name)
		}
	}
}
