package api

import (
	"bytes"
	"encoding/json"

	"github.com/donnyhardyanto/dxlib/errors"
)

// The OpenAPI 3.1 document model. It is deliberately the subset dxlib emits
// and reads, not the whole specification: what an endpoint declaration
// carries, and nothing an endpoint declaration cannot carry. The emitter
// (openapi_emit.go) fills it from registered endpoints, the reader
// (openapi_read.go) fills it from a file, the binder (openapi_bind.go) turns
// it back into endpoints, and every one of them marshals through this one
// model, so two documents that describe the same endpoints are byte-identical.
//
// Field order in these structs is the key order in the emitted JSON, and every
// map is an ordered map, so a document round-trips with its keys where its
// author put them. The dxlib-only facts -- endpoint type, privileges, rate
// limit group, content length ceiling, and the exact dxlib parameter type --
// live in x-dxlib-* extensions, which is what the namespace is for. See
// OPENAPI.md for the whole dialect.

const (
	// OpenAPIVersion is the version string the emitter writes and the only
	// major.minor the reader accepts. 3.0 uses a different schema dialect
	// (nullable: true, boolean exclusiveMinimum) and is refused rather than
	// half-read.
	OpenAPIVersion = "3.1.0"

	OpenAPIExtensionEndPointType       = "x-dxlib-endpoint-type"
	OpenAPIExtensionPrivileges         = "x-dxlib-privileges"
	OpenAPIExtensionRateLimitGroup     = "x-dxlib-rate-limit-group"
	OpenAPIExtensionMaxContentLength   = "x-dxlib-max-content-length"
	OpenAPIExtensionRequestContentType = "x-dxlib-request-content-type"
	OpenAPIExtensionParameters         = "x-dxlib-parameters"
	OpenAPIExtensionType               = "x-dxlib-type"
	OpenAPIExtensionResponseName       = "x-dxlib-response-name"
	OpenAPIExtensionWebSocketEndPoints = "x-dxlib-websocket-endpoints"

	// OpenAPISecuritySchemeMutualTLS is the one security scheme this dialect
	// carries: its name under components/securitySchemes and its type, both
	// spelled as OpenAPI 3.1 spells the type. It is written exactly when the
	// listener refuses a caller without a valid client certificate; see
	// openAPIMutualTLSInForce and OPENAPI.md section 2.8.
	OpenAPISecuritySchemeMutualTLS = "mutualTLS"
)

// DXOpenAPIOrderedMap is a string-keyed map that marshals in insertion
// order. encoding/json sorts map keys, which would put every parameter of
// every endpoint in alphabetical order and separate a spec's paths from the
// grouping their author gave them; a slice of keys beside the map keeps the
// order and costs nothing else.
type DXOpenAPIOrderedMap[V any] struct {
	keys   []string
	values map[string]V
}

func NewDXOpenAPIOrderedMap[V any]() *DXOpenAPIOrderedMap[V] {
	return &DXOpenAPIOrderedMap[V]{values: map[string]V{}}
}

// Set adds or replaces a key. A replaced key keeps its original position.
func (m *DXOpenAPIOrderedMap[V]) Set(key string, value V) {
	if m.values == nil {
		m.values = map[string]V{}
	}
	if _, exists := m.values[key]; !exists {
		m.keys = append(m.keys, key)
	}
	m.values[key] = value
}

func (m *DXOpenAPIOrderedMap[V]) Get(key string) (value V, ok bool) {
	if m == nil || m.values == nil {
		return value, false
	}
	value, ok = m.values[key]
	return value, ok
}

func (m *DXOpenAPIOrderedMap[V]) Keys() []string {
	if m == nil {
		return nil
	}
	return m.keys
}

func (m *DXOpenAPIOrderedMap[V]) Len() int {
	if m == nil {
		return 0
	}
	return len(m.keys)
}

func (m *DXOpenAPIOrderedMap[V]) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range m.Keys() {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		b.Write(kb)
		b.WriteByte(':')
		vb, err := json.Marshal(m.values[k])
		if err != nil {
			return nil, err
		}
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

type DXOpenAPIDocument struct {
	OpenAPI    string                                   `json:"openapi"`
	Info       DXOpenAPIInfo                            `json:"info"`
	Paths      *DXOpenAPIOrderedMap[*DXOpenAPIPathItem] `json:"paths"`
	Components *DXOpenAPIComponents                     `json:"components,omitempty"`
	// Security is the top-level security requirement. Only one is ever emitted
	// here -- mutualTLS, and only when the API is actually served under
	// mode: mtls. See openAPISecurityForTLS.
	Security []DXOpenAPISecurityRequirement `json:"security,omitempty"`
	// WebSocketEndPoints is x-dxlib-websocket-endpoints. OpenAPI 3.1 describes
	// request/response operations and has no construct for a socket lifecycle,
	// so WebSocket endpoints are listed here rather than under paths, where a
	// reader would take them for ordinary GETs.
	WebSocketEndPoints *DXOpenAPIWebSocketEndPoints `json:"x-dxlib-websocket-endpoints,omitempty"`
}

type DXOpenAPIInfo struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type DXOpenAPIComponents struct {
	Schemas         *DXOpenAPIOrderedMap[*DXOpenAPISchema]         `json:"schemas,omitempty"`
	SecuritySchemes *DXOpenAPIOrderedMap[*DXOpenAPISecurityScheme] `json:"securitySchemes,omitempty"`
}

// DXOpenAPISecurityRequirement is one entry of a security list: a scheme name
// mapped to its scopes. Only mutualTLS is emitted or accepted, and mutualTLS
// takes no scopes, so the slice is always empty -- a non-empty one is refused
// by the reader rather than ignored.
type DXOpenAPISecurityRequirement map[string][]string

// DXOpenAPISecurityScheme is a componentsecuritySchemes entry.
//
// Only type mutualTLS is ever produced. The body token that authenticates
// these APIs is deliberately NOT modelled here: OpenAPI 3.1 offers apiKey
// (in: query, header or cookie only), http, oauth2, openIdConnect and
// mutualTLS, and none of them can express a credential carried inside the
// request body. Declaring apiKey with in: body would produce a document that
// is invalid and that every reader outside dxlib would misread. The token is a
// request-body property, which is what it actually is, and it appears in the
// requestBody schema like any other parameter. OPENAPI.md states this.
type DXOpenAPISecurityScheme struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// DXOpenAPIPathItem holds one operation per HTTP method. dxlib registers one
// endpoint per URI and checks the method itself, so in an emitted document
// exactly one of these is set; a hand-written document may set several.
type DXOpenAPIPathItem struct {
	Summary     string              `json:"summary,omitempty"`
	Description string              `json:"description,omitempty"`
	Get         *DXOpenAPIOperation `json:"get,omitempty"`
	Put         *DXOpenAPIOperation `json:"put,omitempty"`
	Post        *DXOpenAPIOperation `json:"post,omitempty"`
	Delete      *DXOpenAPIOperation `json:"delete,omitempty"`
	Options     *DXOpenAPIOperation `json:"options,omitempty"`
	Head        *DXOpenAPIOperation `json:"head,omitempty"`
	Patch       *DXOpenAPIOperation `json:"patch,omitempty"`
	Trace       *DXOpenAPIOperation `json:"trace,omitempty"`
}

// openAPIMethods is every method key a path item may carry, in the order the
// specification lists them, as lower-case key and upper-case dxlib Method.
var openAPIMethods = []struct{ key, method string }{
	{"get", "GET"}, {"put", "PUT"}, {"post", "POST"}, {"delete", "DELETE"},
	{"options", "OPTIONS"}, {"head", "HEAD"}, {"patch", "PATCH"}, {"trace", "TRACE"},
}

func (p *DXOpenAPIPathItem) operation(method string) **DXOpenAPIOperation {
	switch method {
	case "GET":
		return &p.Get
	case "PUT":
		return &p.Put
	case "POST":
		return &p.Post
	case "DELETE":
		return &p.Delete
	case "OPTIONS":
		return &p.Options
	case "HEAD":
		return &p.Head
	case "PATCH":
		return &p.Patch
	case "TRACE":
		return &p.Trace
	default:
		return nil
	}
}

// Operations returns the operations set on the item with their upper-case
// method, in specification order.
func (p *DXOpenAPIPathItem) Operations() (methods []string, operations []*DXOpenAPIOperation) {
	for _, m := range openAPIMethods {
		if op := *p.operation(m.method); op != nil {
			methods = append(methods, m.method)
			operations = append(operations, op)
		}
	}
	return methods, operations
}

type DXOpenAPIOperation struct {
	OperationId string                                   `json:"operationId"`
	Summary     string                                   `json:"summary,omitempty"`
	Description string                                   `json:"description,omitempty"`
	Parameters  []*DXOpenAPIParameter                    `json:"parameters,omitempty"`
	RequestBody *DXOpenAPIRequestBody                    `json:"requestBody,omitempty"`
	Responses   *DXOpenAPIOrderedMap[*DXOpenAPIResponse] `json:"responses,omitempty"`

	// EndPointType is DXAPIEndPointType.String(), always written so that the
	// type is read off the document and never defaulted into place.
	EndPointType string `json:"x-dxlib-endpoint-type,omitempty"`
	// Privileges, RateLimitGroup and MaxContentLength are the endpoint fields
	// of the same names. Zero values are omitted and read back as zero.
	Privileges       []string `json:"x-dxlib-privileges,omitempty"`
	RateLimitGroup   string   `json:"x-dxlib-rate-limit-group,omitempty"`
	MaxContentLength int64    `json:"x-dxlib-max-content-length,omitempty"`
	// RequestContentType is written only where the request body cannot carry
	// it: a GET or DELETE declared with a content type, which dxlib ignores at
	// request time but which the declaration still holds.
	RequestContentType string `json:"x-dxlib-request-content-type,omitempty"`
	// Parameters is the declared parameter set for a body that is not the
	// parameters: application/octet-stream and text/plain, where the values
	// arrive in the X-Var header, and a POST with no content type. It is an
	// object schema, one property per parameter.
	ParametersSchema *DXOpenAPISchema `json:"x-dxlib-parameters,omitempty"`
}

type DXOpenAPIParameter struct {
	Name        string           `json:"name"`
	In          string           `json:"in"`
	Description string           `json:"description,omitempty"`
	Required    bool             `json:"required,omitempty"`
	Schema      *DXOpenAPISchema `json:"schema"`
}

type DXOpenAPIRequestBody struct {
	Description string                                    `json:"description,omitempty"`
	Required    bool                                      `json:"required,omitempty"`
	Content     *DXOpenAPIOrderedMap[*DXOpenAPIMediaType] `json:"content"`
}

type DXOpenAPIMediaType struct {
	Schema *DXOpenAPISchema `json:"schema,omitempty"`
}

type DXOpenAPIResponse struct {
	Description string                                    `json:"description"`
	Headers     *DXOpenAPIOrderedMap[*DXOpenAPIHeader]    `json:"headers,omitempty"`
	Content     *DXOpenAPIOrderedMap[*DXOpenAPIMediaType] `json:"content,omitempty"`
	// Name is the key the endpoint gave this possibility in its
	// ResponsePossibilities map. OpenAPI keys responses by status code, so the
	// name travels beside it.
	Name string `json:"x-dxlib-response-name,omitempty"`
}

type DXOpenAPIHeader struct {
	Description string           `json:"description,omitempty"`
	Required    bool             `json:"required,omitempty"`
	Schema      *DXOpenAPISchema `json:"schema,omitempty"`
}

// DXOpenAPISchemaType is JSON Schema's type keyword: one name, or a list when
// the value may also be null. It marshals as a bare string for one name and
// as an array otherwise, which is how 2020-12 spells nullable.
type DXOpenAPISchemaType []string

func (t DXOpenAPISchemaType) MarshalJSON() ([]byte, error) {
	if len(t) == 1 {
		return json.Marshal(t[0])
	}
	return json.Marshal([]string(t))
}

// Primary is the type name that is not "null".
func (t DXOpenAPISchemaType) Primary() string {
	for _, s := range t {
		if s != "null" {
			return s
		}
	}
	return ""
}

func (t DXOpenAPISchemaType) Nullable() bool {
	for _, s := range t {
		if s == "null" {
			return true
		}
	}
	return false
}

type DXOpenAPISchema struct {
	Ref                  string                                 `json:"$ref,omitempty"`
	Type                 DXOpenAPISchemaType                    `json:"type,omitempty"`
	Format               string                                 `json:"format,omitempty"`
	Description          string                                 `json:"description,omitempty"`
	Properties           *DXOpenAPIOrderedMap[*DXOpenAPISchema] `json:"properties,omitempty"`
	Required             []string                               `json:"required,omitempty"`
	Items                *DXOpenAPISchema                       `json:"items,omitempty"`
	AdditionalProperties *DXOpenAPISchema                       `json:"additionalProperties,omitempty"`
	Enum                 []any                                  `json:"enum,omitempty"`
	Minimum              *float64                               `json:"minimum,omitempty"`
	ExclusiveMinimum     *float64                               `json:"exclusiveMinimum,omitempty"`
	MinLength            *int                                   `json:"minLength,omitempty"`
	// DXLibType is the exact dxlib parameter type. Several dxlib types share
	// one JSON type -- int64, int64p and id are all integer/int64 -- so the
	// JSON Schema alone cannot say which validator runs. With it present the
	// binder uses it exactly; without it the binder infers the closest dxlib
	// type from type and format, as OPENAPI.md tabulates.
	DXLibType string `json:"x-dxlib-type,omitempty"`
}

type DXOpenAPIWebSocketEndPoints struct {
	Description string                        `json:"description"`
	EndPoints   []*DXOpenAPIWebSocketEndPoint `json:"endpoints"`
}

type DXOpenAPIWebSocketEndPoint struct {
	OperationId    string   `json:"operationId"`
	Path           string   `json:"path"`
	Method         string   `json:"method"`
	Summary        string   `json:"summary,omitempty"`
	Description    string   `json:"description,omitempty"`
	Privileges     []string `json:"privileges,omitempty"`
	RateLimitGroup string   `json:"rateLimitGroup,omitempty"`
	// PeriodicInterval is a Go duration string ("5s"); empty means the
	// library default of thirty seconds.
	PeriodicInterval string `json:"periodicInterval,omitempty"`
}

// OpenAPIWebSocketExtensionDescription is written into every emitted
// x-dxlib-websocket-endpoints block, so that a reader who has never seen dxlib
// learns from the document itself why these are not under paths.
const OpenAPIWebSocketExtensionDescription = "WebSocket endpoints registered on this API. OpenAPI 3.1 describes request/response operations and has no construct for a WebSocket lifecycle (upgrade, open, frames in both directions, periodic push, close), so they are listed here and not under paths. The protocol inside the frames is the application's own and is not described."

// AsJSON renders the document with two-space indentation. It is the one
// serialiser: the emitter, the handler and the round-trip test all go
// through it, which is what makes two documents comparable byte for byte.
func (doc *DXOpenAPIDocument) AsJSON() ([]byte, error) {
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "OPENAPI_MARSHAL_ERROR")
	}
	return append(b, '\n'), nil
}

// OperationIds returns every operationId in the document -- the path
// operations and the WebSocket endpoints -- in document order.
func (doc *DXOpenAPIDocument) OperationIds() []string {
	var ids []string
	for _, path := range doc.Paths.Keys() {
		item, _ := doc.Paths.Get(path)
		_, ops := item.Operations()
		for _, op := range ops {
			ids = append(ids, op.OperationId)
		}
	}
	if doc.WebSocketEndPoints != nil {
		for _, ws := range doc.WebSocketEndPoints.EndPoints {
			ids = append(ids, ws.OperationId)
		}
	}
	return ids
}
