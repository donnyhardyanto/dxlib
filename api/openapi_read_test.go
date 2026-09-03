package api

import (
	"strings"
	"testing"
)

// A small document in the dialect, used as the base the refusal cases edit.
const openAPIReadTestYAML = `openapi: 3.1.0
info:
  title: api
  version: 1.0.0
paths:
  /cmdX:
    post:
      operationId: cmdX
      summary: cmdX
      x-dxlib-endpoint-type: EndPointTypeHTTPJSON
      x-dxlib-privileges: [p1, p2]
      x-dxlib-max-content-length: 4096
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                token:
                  type: string
                  x-dxlib-type: string
                id:
                  type: integer
                  format: int64
                  minimum: 1
                child:
                  $ref: '#/components/schemas/Child'
              required: [token, id]
      responses:
        "200":
          description: ok
  /cmdY:
    get:
      operationId: cmdY
      parameters:
        - name: q
          in: query
          required: true
          schema:
            type: string
components:
  schemas:
    Child:
      type: object
      properties:
        a:
          type: [string, "null"]
x-vendor-anything: { ignored: true }
`

func TestOpenAPIReadsYAML(t *testing.T) {
	doc, err := ReadOpenAPI([]byte(openAPIReadTestYAML))
	if err != nil {
		t.Fatal(err)
	}
	if doc.OpenAPI != "3.1.0" || doc.Info.Title != "api" || doc.Paths.Len() != 2 {
		t.Fatalf("doc = %+v", doc)
	}
	item, _ := doc.Paths.Get("/cmdX")
	op := item.Post
	if op == nil || op.OperationId != "cmdX" || op.EndPointType != "EndPointTypeHTTPJSON" || len(op.Privileges) != 2 || op.MaxContentLength != 4096 {
		t.Fatalf("op = %+v", op)
	}
	mt, _ := op.RequestBody.Content.Get("application/json")
	id, _ := mt.Schema.Properties.Get("id")
	if id.Type.Primary() != "integer" || id.Format != "int64" || id.Minimum == nil || *id.Minimum != 1 {
		t.Errorf("id schema = %+v", id)
	}
	child, _ := mt.Schema.Properties.Get("child")
	if child.Ref != "#/components/schemas/Child" {
		t.Errorf("child = %+v", child)
	}
	// Keys keep document order.
	if keys := mt.Schema.Properties.Keys(); strings.Join(keys, ",") != "token,id,child" {
		t.Errorf("property order = %v", keys)
	}
	if ids := doc.OperationIds(); strings.Join(ids, ",") != "cmdX,cmdY" {
		t.Errorf("operation ids = %v", ids)
	}
	a, _ := func() (*DXOpenAPISchema, bool) {
		c, _ := doc.Components.Schemas.Get("Child")
		return c.Properties.Get("a")
	}()
	if !a.Type.Nullable() || a.Type.Primary() != "string" {
		t.Errorf("nullable type lost: %v", a.Type)
	}
}

// The JSON path is encoding/json, not YAML: "\/" is a legal JSON escape that
// yaml.v3 rejects, and a tab is legal JSON whitespace everywhere.
func TestOpenAPIReadsJSONWithEscapesYAMLWouldRefuse(t *testing.T) {
	src := "{\n\t\"openapi\": \"3.1.0\",\n\t\"info\": {\"title\": \"a\\/b\", \"version\": \"1\"},\n\t\"paths\": {\"/x\": {\"get\": {\"operationId\": \"x\"}}}\n}\n"
	doc, err := ReadOpenAPI([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Info.Title != "a/b" {
		t.Errorf("title = %q", doc.Info.Title)
	}
}

func TestOpenAPIJSONAndYAMLReadToTheSameDocument(t *testing.T) {
	fromYAML, err := ReadOpenAPI([]byte(openAPIReadTestYAML))
	if err != nil {
		t.Fatal(err)
	}
	asJSON, err := fromYAML.AsJSON()
	if err != nil {
		t.Fatal(err)
	}
	fromJSON, err := ReadOpenAPI(asJSON)
	if err != nil {
		t.Fatalf("%v\n%s", err, asJSON)
	}
	again, err := fromJSON.AsJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(asJSON) {
		t.Fatalf("JSON and YAML readings differ:\n%s\n---\n%s", asJSON, again)
	}
}

// Every construct outside the dialect is refused, with the construct named
// and the JSON pointer of where it sat. The table is the contract: a case
// here is a promise that the reader will never quietly skip that construct.
func TestOpenAPIRefusesEverythingOutsideTheDialect(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr []string // every fragment must appear in the error
	}{
		{"oneOf", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema:
              oneOf: [{type: string}, {type: integer}]
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:oneOf", "/paths/~1x/post/requestBody/content/application~1json/schema/oneOf", "SCHEMA_COMPOSITION"}},
		{"anyOf", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      parameters:
        - name: q
          in: query
          schema:
            anyOf: [{type: string}]
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:anyOf", "/paths/~1x/get/parameters/0/schema/anyOf"}},
		{"allOf in components", `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
components:
  schemas:
    A:
      allOf: [{type: object}]
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:allOf", "/components/schemas/A/allOf"}},
		{"discriminator", `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
components:
  schemas:
    A:
      type: object
      discriminator: {propertyName: kind}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:discriminator", "/components/schemas/A/discriminator"}},
		{"callbacks", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      callbacks: {}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:callbacks", "/paths/~1x/post/callbacks"}},
		{"webhooks", `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
webhooks: {}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:webhooks", ":/webhooks"}},
		{"remote $ref", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema:
              $ref: 'common.yaml#/components/schemas/Body'
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:remote-$ref", "common.yaml", "/paths/~1x/post/requestBody/content/application~1json/schema/$ref"}},
		{"$ref outside components/schemas", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/parameters/P'
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:$ref", "#/components/parameters/P"}},
		{"dangling $ref", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Missing'
`, []string{"OPENAPI_REF_NOT_FOUND:#/components/schemas/Missing", "/paths/~1x/post/requestBody/content/application~1json/schema/$ref"}},
		{"security", `openapi: 3.1.0
info: {title: a, version: "1"}
security: [{bearer: []}]
paths: {}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:security", "NOT_ENFORCED"}},
		{"securitySchemes", `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
components:
  securitySchemes: {}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:securitySchemes", "/components/securitySchemes"}},
		{"servers", `openapi: 3.1.0
info: {title: a, version: "1"}
servers: [{url: http://x}]
paths: {}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:servers", ":/servers"}},
		{"tags on an operation", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      tags: [a]
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:tags", "/paths/~1x/get/tags"}},
		{"unknown standard-looking field", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      operationid: y
`, []string{"OPENAPI_UNKNOWN_FIELD:operationid", "/paths/~1x/get/operationid"}},
		{"unknown x-dxlib extension", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      x-dxlib-privilege: [a]
`, []string{"OPENAPI_UNKNOWN_DXLIB_EXTENSION:x-dxlib-privilege", "/paths/~1x/get/x-dxlib-privilege"}},
		{"nullable keyword from 3.0", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      parameters:
        - name: q
          in: query
          schema: {type: string, nullable: true}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:nullable", "USE_type_[T,null]"}},
		{"3.0 document", `openapi: 3.0.3
info: {title: a, version: "1"}
paths: {}
`, []string{"OPENAPI_VERSION_UNSUPPORTED", "3.0.3"}},
		{"header parameter", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      parameters:
        - name: X-Var
          in: header
          schema: {type: string}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:parameter-in-header", "/paths/~1x/get/parameters/0/in"}},
		{"missing operationId", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      summary: s
`, []string{"OPENAPI_REQUIRED_FIELD_MISSING:operationId", "/paths/~1x/get"}},
		{"duplicate operationId", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get: {operationId: same}
  /y:
    get: {operationId: same}
`, []string{"OPENAPI_DUPLICATE_OPERATION_ID:same", "/paths/~1x/get", "/paths/~1y/get"}},
		{"duplicate operationId across ws", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get: {operationId: same}
x-dxlib-websocket-endpoints:
  description: d
  endpoints:
    - {operationId: same, path: /ws, method: GET}
`, []string{"OPENAPI_DUPLICATE_OPERATION_ID:same", "/x-dxlib-websocket-endpoints/endpoints/0"}},
		{"duplicate key", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      operationId: y
`, []string{"OPENAPI_DUPLICATE_KEY:operationId", "/paths/~1x/get/operationId"}},
		{"duplicate key in JSON", `{"openapi":"3.1.0","info":{"title":"a","version":"1"},"paths":{"/x":{"get":{"operationId":"x","operationId":"y"}}}}`,
			[]string{"OPENAPI_DUPLICATE_KEY:operationId", "/paths/~1x/get/operationId"}},
		{"path-level parameters", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    parameters: []
    get: {operationId: x}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:path-level-parameters", "/paths/~1x/parameters"}},
		{"two methods on one path", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get: {operationId: x}
    post: {operationId: y}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:multiple-methods-on-one-path", "/paths/~1x"}},
		{"query parameter on a POST", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      parameters:
        - {name: q, in: query, schema: {type: string}}
`, []string{"OPENAPI_QUERY_PARAMETER_ON_POST:q", "/paths/~1x/post/parameters/0"}},
		{"requestBody on a GET", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      requestBody:
        content:
          application/json:
            schema: {type: object}
`, []string{"OPENAPI_REQUEST_BODY_ON_GET", "/paths/~1x/get/requestBody"}},
		{"two media types", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json: {schema: {type: object}}
          application/xml: {schema: {type: object}}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:multiple-media-types", "/paths/~1x/post/requestBody/content"}},
		{"unknown media type", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/xml: {schema: {type: object}}
`, []string{"OPENAPI_UNSUPPORTED_MEDIA_TYPE", "application/xml", "/paths/~1x/post/requestBody/content/application~1xml"}},
		{"default response", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      responses:
        default: {description: d}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:response-code-default", "/paths/~1x/get/responses/default"}},
		{"2XX response range", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      responses:
        2XX: {description: d}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:response-code-2XX"}},
		{"unknown format", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      parameters:
        - {name: q, in: query, schema: {type: string, format: uuid}}
`, []string{"OPENAPI_UNSUPPORTED_FORMAT", "uuid", "/paths/~1x/get/parameters/0/schema/format"}},
		{"type union", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      parameters:
        - {name: q, in: query, schema: {type: [string, integer]}}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:type-union", "/paths/~1x/get/parameters/0/schema/type"}},
		{"unknown x-dxlib-type", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      parameters:
        - {name: q, in: query, schema: {type: string, x-dxlib-type: varchar}}
`, []string{"OPENAPI_UNKNOWN_X_DXLIB_TYPE", "varchar"}},
		{"unknown endpoint type", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      x-dxlib-endpoint-type: EndPointTypeGraphQL
`, []string{"OPENAPI_UNKNOWN_ENDPOINT_TYPE", "EndPointTypeGraphQL", "/paths/~1x/get/x-dxlib-endpoint-type"}},
		{"WS endpoint under paths", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /ws:
    get:
      operationId: ws
      x-dxlib-endpoint-type: EndPointTypeWS
`, []string{"OPENAPI_WS_ENDPOINT_UNDER_PATHS", "/paths/~1ws/get", "x-dxlib-websocket-endpoints"}},
		{"template without parameter", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /users/{id}:
    get:
      operationId: user
`, []string{"OPENAPI_PATH_TEMPLATE_WITHOUT_PARAMETER:{id}", "/paths/~1users~1{id}/get"}},
		{"path parameter not in template", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /users:
    get:
      operationId: user
      parameters:
        - {name: id, in: path, required: true, schema: {type: string}}
`, []string{"OPENAPI_PATH_PARAMETER_NOT_IN_TEMPLATE:id"}},
		{"optional path parameter", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /users/{id}:
    get:
      operationId: user
      parameters:
        - {name: id, in: path, schema: {type: string}}
`, []string{"OPENAPI_PATH_PARAMETER_MUST_BE_REQUIRED:id"}},
		{"template inside a segment", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /users/u{id}:
    get:
      operationId: user
      parameters:
        - {name: id, in: path, required: true, schema: {type: string}}
`, []string{"OPENAPI_PATH_TEMPLATE_NOT_A_WHOLE_SEGMENT", "u{id}"}},
		{"required names unknown property", `openapi: 3.1.0
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
              properties: {a: {type: string}}
              required: [b]
`, []string{"OPENAPI_REQUIRED_NAMES_UNKNOWN_PROPERTY:b", "/required/0"}},
		{"$ref with siblings", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    post:
      operationId: x
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/A'
              type: object
components:
  schemas:
    A: {type: object}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:$ref-with-sibling-type"}},
		{"boolean additionalProperties", `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
components:
  schemas:
    A: {type: object, additionalProperties: false}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:additionalProperties-boolean", "/components/schemas/A/additionalProperties"}},
		{"pattern constraint", `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
components:
  schemas:
    A: {type: string, pattern: '^a'}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:pattern", "CONSTRAINT_NOT_ENFORCED"}},
		{"YAML alias", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get: &op {operationId: x}
  /y:
    get: *op
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-alias", "/paths/~1y/get"}},
		{"YAML timestamp scalar", `openapi: 3.1.0
info: {title: a, version: 2024-01-01}
paths: {}
`, []string{"OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-tag-!!timestamp", "/info/version", "QUOTE_THE_VALUE"}},
		{"two YAML documents", "openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths: {}\n---\nopenapi: 3.1.0\n",
			[]string{"OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-multiple-documents"}},
		{"wrong type", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      x-dxlib-privileges: admin
`, []string{"OPENAPI_WRONG_TYPE", "/paths/~1x/get/x-dxlib-privileges", "EXPECTED_array", "GOT_string"}},
		{"negative max content length", `openapi: 3.1.0
info: {title: a, version: "1"}
paths:
  /x:
    get:
      operationId: x
      x-dxlib-max-content-length: -1
`, []string{"OPENAPI_NEGATIVE_MAX_CONTENT_LENGTH:-1"}},
		{"ws bad interval", `openapi: 3.1.0
info: {title: a, version: "1"}
paths: {}
x-dxlib-websocket-endpoints:
  description: d
  endpoints:
    - {operationId: ws, path: /ws, method: GET, periodicInterval: soon}
`, []string{"OPENAPI_WS_BAD_PERIODIC_INTERVAL", "soon", "/x-dxlib-websocket-endpoints/endpoints/0/periodicInterval"}},
		{"trailing JSON", `{"openapi":"3.1.0","info":{"title":"a","version":"1"},"paths":{}} {}`,
			[]string{"OPENAPI_JSON_SYNTAX:TRAILING_CONTENT"}},
		{"empty", "   \n", []string{"OPENAPI_EMPTY_DOCUMENT"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ReadOpenAPI([]byte(c.yaml))
			if err == nil {
				t.Fatalf("accepted a document it must refuse")
			}
			for _, want := range c.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error lacks %q:\n%v", want, err)
				}
			}
		})
	}
}

// Line numbers ride along on the pointer where the parser knows them.
func TestOpenAPIErrorsCarryTheLine(t *testing.T) {
	_, err := ReadOpenAPI([]byte("openapi: 3.1.0\ninfo: {title: a, version: '1'}\npaths:\n  /x:\n    get:\n      operationId: x\n      tags: [a]\n"))
	if err == nil || !strings.Contains(err.Error(), "line=7") {
		t.Fatalf("err = %v", err)
	}
	_, err = ReadOpenAPI([]byte("{\n\"openapi\":\"3.1.0\",\n\"info\":{\"title\":\"a\",\"version\":\"1\"},\n\"paths\":{\"/x\":{\"get\":{\"operationId\":\"x\",\n\"tags\":[]}}}}"))
	if err == nil || !strings.Contains(err.Error(), "line=5") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAPIIgnoresForeignExtensionsOnly(t *testing.T) {
	doc, err := ReadOpenAPI([]byte(`openapi: 3.1.0
info: {title: a, version: "1", x-audience: internal}
paths:
  /x:
    x-owner: team
    get:
      operationId: x
      x-codegen-skip: true
      parameters:
        - {name: q, in: query, schema: {type: string, x-faker: name}}
`))
	if err != nil {
		t.Fatal(err)
	}
	// And they are not carried: the model has no place for them, so a
	// re-emission does not contain them either.
	b, _ := doc.AsJSON()
	for _, gone := range []string{"x-audience", "x-owner", "x-codegen-skip", "x-faker"} {
		if strings.Contains(string(b), gone) {
			t.Errorf("%s should not survive; the model does not carry foreign extensions", gone)
		}
	}
}
