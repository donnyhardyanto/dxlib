package api

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The emitter: registered endpoints -> DXOpenAPIDocument. It sits beside
// PrintSpec rather than replacing it; the Markdown spec has readers of its own.
// Everything an endpoint declares is written somewhere in the document, and
// where OpenAPI has no place for a fact the fact goes in an x-dxlib-*
// extension. What is not written is code: OnExecute, the middleware chain and
// the WebSocket hooks, which a document cannot carry and the handler registry
// in openapi_bind.go supplies instead.

// OpenAPIOperationId is the operationId of an endpoint, derived from its URI:
// the leading slash goes, every other slash becomes an underscore, and the
// braces of a path template are dropped. /cmdX is cmdX; /v2/cmdX is v2_cmdX,
// which is what keeps the v1 and v2 forms of a command distinct. Handlers
// register against this id, so the rule is part of the contract.
func OpenAPIOperationId(uri string) string {
	id := strings.TrimPrefix(uri, "/")
	id = strings.ReplaceAll(id, "/", "_")
	id = strings.ReplaceAll(id, "{", "")
	id = strings.ReplaceAll(id, "}", "")
	return id
}

// OpenAPIDocument builds the document for every endpoint registered on the
// API so far. It is a pure function of the registration state: two calls
// with no registration in between produce identical bytes through AsJSON.
func (a *DXAPI) OpenAPIDocument() (*DXOpenAPIDocument, error) {
	doc := &DXOpenAPIDocument{
		OpenAPI: OpenAPIVersion,
		Info: DXOpenAPIInfo{
			Title:       a.NameId,
			Version:     a.Version,
			Description: "Endpoints registered on API " + a.NameId + ".",
		},
		Paths: NewDXOpenAPIOrderedMap[*DXOpenAPIPathItem](),
	}
	operationIds := map[string]string{}
	claim := func(id, uri string) error {
		if first, taken := operationIds[id]; taken {
			return errors.Errorf("OPENAPI_OPERATION_ID_COLLISION:%s:%s:%s", id, first, uri)
		}
		operationIds[id] = uri
		return nil
	}
	state := openAPIStateOf(a)

	for i := range a.EndPoints {
		ep := &a.EndPoints[i]
		if ep.EndPointType == EndPointTypeWS {
			ws := openAPIWebSocketFromEndPoint(ep)
			if err := claim(ws.OperationId, ep.Uri); err != nil {
				return nil, err
			}
			if doc.WebSocketEndPoints == nil {
				doc.WebSocketEndPoints = &DXOpenAPIWebSocketEndPoints{Description: OpenAPIWebSocketExtensionDescription}
			}
			doc.WebSocketEndPoints.EndPoints = append(doc.WebSocketEndPoints.EndPoints, ws)
			continue
		}
		var pathParameters []DXAPIEndPointParameter
		if state != nil {
			pathParameters = state.pathParameters[ep.Uri]
		}
		op, err := openAPIOperationFromEndPoint(ep, pathParameters)
		if err != nil {
			return nil, err
		}
		if err := claim(op.OperationId, ep.Uri); err != nil {
			return nil, err
		}
		item, exists := doc.Paths.Get(ep.Uri)
		if !exists {
			item = &DXOpenAPIPathItem{}
			doc.Paths.Set(ep.Uri, item)
		}
		slot := item.operation(ep.Method)
		if slot == nil {
			return nil, errors.Errorf("OPENAPI_UNSUPPORTED_METHOD:%s:%s", ep.Method, ep.Uri)
		}
		if *slot != nil {
			return nil, errors.Errorf("OPENAPI_DUPLICATE_PATH_METHOD:%s:%s", ep.Method, ep.Uri)
		}
		*slot = op
	}
	return doc, nil
}

// OpenAPIAsJSON is OpenAPIDocument rendered through DXOpenAPIDocument.AsJSON.
func (a *DXAPI) OpenAPIAsJSON() ([]byte, error) {
	doc, err := a.OpenAPIDocument()
	if err != nil {
		return nil, err
	}
	return doc.AsJSON()
}

// APIHandlerOpenAPI serves the document as application/json. It is the
// OpenAPI counterpart of APIHandlerPrintSpec and is registered the same way,
// as the OnExecute of an endpoint the service chooses the path of. That
// endpoint is itself in the document, as /spec is in the Markdown one.
func (a *DXAPI) APIHandlerOpenAPI(aepr *DXAPIEndPointRequest) error {
	b, err := a.OpenAPIAsJSON()
	if err != nil {
		return err
	}
	aepr.WriteResponseAsBytes(http.StatusOK, map[string]string{"Content-Type": "application/json"}, b)
	return nil
}

// openAPIBodyMethods are the methods for which PreProcessRequest reads a body.
// Every other method has its parameters read from the query string, and the
// document places them accordingly.
var openAPIBodyMethods = map[string]bool{"POST": true, "PUT": true}

func openAPIOperationFromEndPoint(ep *DXAPIEndPoint, pathParameters []DXAPIEndPointParameter) (*DXOpenAPIOperation, error) {
	op := &DXOpenAPIOperation{
		OperationId:      OpenAPIOperationId(ep.Uri),
		Summary:          ep.Title,
		Description:      ep.Description,
		EndPointType:     ep.EndPointType.String(),
		RateLimitGroup:   ep.RateLimitGroupNameId,
		MaxContentLength: ep.RequestMaxContentLength,
	}
	if len(ep.Privileges) > 0 {
		op.Privileges = append([]string{}, ep.Privileges...)
	}

	// Path parameters first, in the order the template names them. An endpoint
	// bound from a document has its declarations in the side table; one
	// registered through NewEndPoint with braces in its URI has none, and the
	// only honest schema for it is a string -- the handler reads PathValue and
	// does its own conversion.
	templateNames, err := openAPIPathTemplateNames(ep.Uri)
	if err != nil {
		return nil, err
	}
	declared := map[string]*DXAPIEndPointParameter{}
	for i := range pathParameters {
		declared[pathParameters[i].NameId] = &pathParameters[i]
	}
	for _, name := range templateNames {
		p, ok := declared[name]
		if !ok {
			p = &DXAPIEndPointParameter{NameId: name, Type: "string", IsMustExist: true}
		}
		s, err := openAPISchemaFromParameter(p)
		if err != nil {
			return nil, errors.Wrapf(err, "OPENAPI_PATH_PARAMETER:%s", ep.Uri)
		}
		op.Parameters = append(op.Parameters, &DXOpenAPIParameter{
			Name: name, In: "path", Description: p.Description, Required: true, Schema: s,
		})
	}
	for name := range declared {
		found := false
		for _, t := range templateNames {
			if t == name {
				found = true
			}
		}
		if !found {
			return nil, errors.Errorf("OPENAPI_PATH_PARAMETER_NOT_IN_TEMPLATE:%s:%s", name, ep.Uri)
		}
	}

	contentType, err := openAPIRequestContentTypeName(ep.RequestContentType)
	if err != nil {
		return nil, errors.Wrapf(err, "OPENAPI_ENDPOINT:%s", ep.Uri)
	}

	if openAPIBodyMethods[ep.Method] {
		if err := openAPIPlaceBodyParameters(op, ep, contentType); err != nil {
			return nil, err
		}
	} else {
		for i := range ep.Parameters {
			p := &ep.Parameters[i]
			s, err := openAPISchemaFromParameter(p)
			if err != nil {
				return nil, errors.Wrapf(err, "OPENAPI_ENDPOINT:%s", ep.Uri)
			}
			op.Parameters = append(op.Parameters, &DXOpenAPIParameter{
				Name: p.NameId, In: "query", Description: p.Description, Required: p.IsMustExist, Schema: s,
			})
		}
		// dxlib ignores a content type on a query-parameter method, but the
		// declaration holds one and it comes back out unchanged.
		op.RequestContentType = contentType
	}

	responses, err := openAPIResponsesFromEndPoint(ep)
	if err != nil {
		return nil, err
	}
	op.Responses = responses
	return op, nil
}

// openAPIPlaceBodyParameters writes a POST or PUT endpoint's parameters where
// its content type says they arrive. For the encodings whose body is the
// parameter set they become the body schema; for a raw body -- an octet
// stream, plain text -- or no content type at all the body is described as
// what it is and the parameters go to x-dxlib-parameters, since dxlib reads
// them from the X-Var header in that case.
func openAPIPlaceBodyParameters(op *DXOpenAPIOperation, ep *DXAPIEndPoint, contentType string) error {
	schemaOfParameters := func() (*DXOpenAPISchema, error) {
		s := &DXOpenAPISchema{Type: DXOpenAPISchemaType{"object"}}
		if len(ep.Parameters) > 0 {
			props, required, err := openAPIPropertiesFromParameters(ep.Parameters)
			if err != nil {
				return nil, errors.Wrapf(err, "OPENAPI_ENDPOINT:%s", ep.Uri)
			}
			s.Properties, s.Required = props, required
		}
		return s, nil
	}
	body := func(mediaType string, schema *DXOpenAPISchema) {
		op.RequestBody = &DXOpenAPIRequestBody{Content: NewDXOpenAPIOrderedMap[*DXOpenAPIMediaType]()}
		op.RequestBody.Content.Set(mediaType, &DXOpenAPIMediaType{Schema: schema})
	}
	switch ep.RequestContentType {
	case utilsHttp.RequestContentTypeNone:
		if len(ep.Parameters) > 0 {
			s, err := schemaOfParameters()
			if err != nil {
				return err
			}
			op.ParametersSchema = s
		}
	case utilsHttp.RequestContentTypeApplicationJSON,
		utilsHttp.RequestContentTypeMultiPartFormData,
		utilsHttp.RequestContentTypeApplicationXWwwFormUrlEncoded:
		s, err := schemaOfParameters()
		if err != nil {
			return err
		}
		body(contentType, s)
	case utilsHttp.RequestContentTypeApplicationOctetStream:
		body(contentType, &DXOpenAPISchema{Type: DXOpenAPISchemaType{"string"}, Format: "binary"})
		if len(ep.Parameters) > 0 {
			s, err := schemaOfParameters()
			if err != nil {
				return err
			}
			op.ParametersSchema = s
		}
	case utilsHttp.RequestContentTypeTextPlain:
		body(contentType, &DXOpenAPISchema{Type: DXOpenAPISchemaType{"string"}})
		if len(ep.Parameters) > 0 {
			s, err := schemaOfParameters()
			if err != nil {
				return err
			}
			op.ParametersSchema = s
		}
	default:
		return errors.Errorf("OPENAPI_UNMAPPED_REQUEST_CONTENT_TYPE:%d:%s", int(ep.RequestContentType), ep.Uri)
	}
	return nil
}

// openAPIResponsesFromEndPoint keys the declared possibilities by status code,
// as OpenAPI requires, and keeps each one's name in x-dxlib-response-name.
// Two possibilities on one status code have no representation and are an
// error, not a last-wins. With none declared the responses key is left out
// altogether; 3.1 permits that and it is the truthful rendering.
func openAPIResponsesFromEndPoint(ep *DXAPIEndPoint) (*DXOpenAPIOrderedMap[*DXOpenAPIResponse], error) {
	if ep.ResponsePossibilities == nil || len(*ep.ResponsePossibilities) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(*ep.ResponsePossibilities))
	for name := range *ep.ResponsePossibilities {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := (*ep.ResponsePossibilities)[names[i]], (*ep.ResponsePossibilities)[names[j]]
		if a.StatusCode != b.StatusCode {
			return a.StatusCode < b.StatusCode
		}
		return names[i] < names[j]
	})
	responses := NewDXOpenAPIOrderedMap[*DXOpenAPIResponse]()
	for _, name := range names {
		rp := (*ep.ResponsePossibilities)[name]
		code := strconv.Itoa(rp.StatusCode)
		if prior, taken := responses.Get(code); taken {
			return nil, errors.Errorf("OPENAPI_RESPONSE_STATUS_COLLISION:%s:%s:%s:%s", ep.Uri, code, prior.Name, name)
		}
		r := &DXOpenAPIResponse{Description: rp.Description, Name: name}
		if len(rp.Headers) > 0 {
			headerNames := make([]string, 0, len(rp.Headers))
			for h := range rp.Headers {
				headerNames = append(headerNames, h)
			}
			sort.Strings(headerNames)
			r.Headers = NewDXOpenAPIOrderedMap[*DXOpenAPIHeader]()
			for _, h := range headerNames {
				r.Headers.Set(h, &DXOpenAPIHeader{Description: rp.Headers[h], Schema: &DXOpenAPISchema{Type: DXOpenAPISchemaType{"string"}}})
			}
		}
		if len(rp.DataTemplate) > 0 {
			template := make([]DXAPIEndPointParameter, 0, len(rp.DataTemplate))
			for _, p := range rp.DataTemplate {
				if p != nil {
					template = append(template, *p)
				}
			}
			props, required, err := openAPIPropertiesFromParameters(template)
			if err != nil {
				return nil, errors.Wrapf(err, "OPENAPI_RESPONSE:%s:%s", ep.Uri, name)
			}
			r.Content = NewDXOpenAPIOrderedMap[*DXOpenAPIMediaType]()
			r.Content.Set("application/json", &DXOpenAPIMediaType{Schema: &DXOpenAPISchema{Type: DXOpenAPISchemaType{"object"}, Properties: props, Required: required}})
		}
		responses.Set(code, r)
	}
	return responses, nil
}

// openAPIWebSocketFromEndPoint is the extension entry for a WebSocket
// endpoint. Parameters, request content type and content length are not
// carried: PreProcessRequest never runs for a WebSocket endpoint, so none of
// the three has any effect there, and the document says what the server does.
func openAPIWebSocketFromEndPoint(ep *DXAPIEndPoint) *DXOpenAPIWebSocketEndPoint {
	ws := &DXOpenAPIWebSocketEndPoint{
		OperationId:    OpenAPIOperationId(ep.Uri),
		Path:           ep.Uri,
		Method:         ep.Method,
		Summary:        ep.Title,
		Description:    ep.Description,
		RateLimitGroup: ep.RateLimitGroupNameId,
	}
	if len(ep.Privileges) > 0 {
		ws.Privileges = append([]string{}, ep.Privileges...)
	}
	if ep.WSPeriodicInterval > 0 {
		ws.PeriodicInterval = ep.WSPeriodicInterval.String()
	}
	return ws
}

// openAPIPathTemplateNames returns the {name} segments of a URI in order.
// Each template must be a whole segment, which is both what OpenAPI shows in
// its examples and the only form Go's ServeMux accepts -- /users/u{id} makes
// the mux panic at registration. Go's own {name...} and {$} have no OpenAPI
// spelling and are refused here so they never reach a document.
func openAPIPathTemplateNames(uri string) ([]string, error) {
	var names []string
	seen := map[string]bool{}
	for _, segment := range strings.Split(uri, "/") {
		open := strings.IndexByte(segment, '{')
		closeIdx := strings.IndexByte(segment, '}')
		if open < 0 && closeIdx < 0 {
			continue
		}
		if open != 0 || closeIdx != len(segment)-1 || strings.Count(segment, "{") != 1 || strings.Count(segment, "}") != 1 {
			return nil, errors.Errorf("OPENAPI_PATH_TEMPLATE_NOT_A_WHOLE_SEGMENT:%q:%s", segment, uri)
		}
		name := segment[1 : len(segment)-1]
		if name == "$" || strings.HasSuffix(name, "...") {
			return nil, errors.Errorf("OPENAPI_PATH_TEMPLATE_NOT_REPRESENTABLE:%q:%s:GO_MUX_ONLY_SYNTAX", segment, uri)
		}
		if name == "" {
			return nil, errors.Errorf("OPENAPI_PATH_TEMPLATE_EMPTY_NAME:%s", uri)
		}
		for i, c := range name {
			isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
			isDigit := c >= '0' && c <= '9'
			if !isLetter && !(isDigit && i > 0) {
				return nil, errors.Errorf("OPENAPI_PATH_TEMPLATE_BAD_NAME:%q:%s", name, uri)
			}
		}
		if seen[name] {
			return nil, errors.Errorf("OPENAPI_PATH_TEMPLATE_DUPLICATE_NAME:%q:%s", name, uri)
		}
		seen[name] = true
		names = append(names, name)
	}
	return names, nil
}
