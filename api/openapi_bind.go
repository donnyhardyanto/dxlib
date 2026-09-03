package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The binder: DXOpenAPIDocument + registered handlers -> endpoints on a
// DXAPI. This is where the FLUID/FIXED split lands. The document is the
// fluid half -- URI, method, content type, parameters, privileges, rate limit
// group, content length ceiling -- and is edited without touching Go. The
// handlers are the fixed half and register by operationId. Loading joins the
// two, and refuses to start when they disagree: an operationId in the
// document with no handler, or a handler with no operationId in the document,
// is a fatal error naming both sides, so the two cannot drift apart quietly.
//
// Endpoints registered through NewEndPoint and NewWSEndPoint are untouched by
// any of this. A service may bind a document for some of its endpoints and
// keep the rest in code; the only interaction is that a URI can be claimed
// once.

// DXOpenAPIWSHandler is what a WebSocket operation binds to: the hooks
// NewWSEndPoint takes, or OnLoop for an endpoint that runs the whole
// lifecycle itself, plus the middleware chain run before the upgrade.
type DXOpenAPIWSHandler struct {
	OnOpen      DXAPIEndPointWSOpenFunc
	OnMessage   DXAPIEndPointWSMessageFunc
	OnClose     DXAPIEndPointWSCloseFunc
	OnPeriodic  DXAPIEndPointWSPeriodicFunc
	OnLoop      DXAPIEndPointExecuteFunc
	Middlewares []DXAPIEndPointExecuteFunc
}

// dxOpenAPIHandler is one registration, HTTP or WebSocket.
type dxOpenAPIHandler struct {
	onExecute   DXAPIEndPointExecuteFunc
	middlewares []DXAPIEndPointExecuteFunc
	ws          *DXOpenAPIWSHandler
	count       int
}

// dxOpenAPIState is the per-API state of this file. It sits in a side table
// keyed by the API rather than in a field on DXAPI, so that DXAPI's own
// definition is left alone; the table is written under one lock and an API
// object lives for the process, so the pointer key is stable.
type dxOpenAPIState struct {
	mu           sync.Mutex
	handlers     map[string]*dxOpenAPIHandler
	handlerOrder []string
	// pathParameters holds the declared path parameters of every bound URI,
	// which live outside DXAPIEndPoint.Parameters (see the middleware below)
	// and which the emitter needs to write the document back out.
	pathParameters map[string][]DXAPIEndPointParameter
}

var (
	openAPIStatesMu sync.Mutex
	openAPIStates   = map[*DXAPI]*dxOpenAPIState{}
)

func openAPIStateOf(a *DXAPI) *dxOpenAPIState {
	openAPIStatesMu.Lock()
	defer openAPIStatesMu.Unlock()
	return openAPIStates[a]
}

func openAPIStateEnsure(a *DXAPI) *dxOpenAPIState {
	openAPIStatesMu.Lock()
	defer openAPIStatesMu.Unlock()
	s, ok := openAPIStates[a]
	if !ok {
		s = &dxOpenAPIState{handlers: map[string]*dxOpenAPIHandler{}, pathParameters: map[string][]DXAPIEndPointParameter{}}
		openAPIStates[a] = s
	}
	return s
}

func (s *dxOpenAPIState) register(operationId string, h *dxOpenAPIHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.handlers[operationId]; ok {
		existing.count++
		return
	}
	h.count = 1
	s.handlers[operationId] = h
	s.handlerOrder = append(s.handlerOrder, operationId)
}

// RegisterHandler names the code that runs for one operationId in the
// document this API will load. The middlewares are the endpoint's chain, in
// order, exactly as NewEndPoint would take them. Registering the same id
// twice is reported when the document is loaded, not here, so that the
// report is one error with the whole picture rather than a fatal in the
// middle of a define function.
func (a *DXAPI) RegisterHandler(operationId string, onExecute DXAPIEndPointExecuteFunc, middlewares ...DXAPIEndPointExecuteFunc) {
	openAPIStateEnsure(a).register(operationId, &dxOpenAPIHandler{onExecute: onExecute, middlewares: middlewares})
}

// RegisterWSHandler is RegisterHandler for an entry in
// x-dxlib-websocket-endpoints.
func (a *DXAPI) RegisterWSHandler(operationId string, handler DXOpenAPIWSHandler) {
	h := handler
	openAPIStateEnsure(a).register(operationId, &dxOpenAPIHandler{ws: &h})
}

// LoadOpenAPIFile reads a document and binds it. Any failure -- a file that
// does not parse, a construct outside the dialect, a drift between document
// and handlers, a URI already taken -- is fatal, the way a bad address in
// ApplyConfigurations is: a service whose privilege declarations did not load
// as written must not start.
func (a *DXAPI) LoadOpenAPIFile(path string) error {
	doc, err := ReadOpenAPIFile(path)
	if err != nil {
		return log.Log.FatalAndCreateErrorf("OPENAPI_LOAD_ERROR:%s:%v", a.NameId, err)
	}
	return a.LoadOpenAPI(doc)
}

// LoadOpenAPI binds a document already in memory, fatally on any error.
func (a *DXAPI) LoadOpenAPI(doc *DXOpenAPIDocument) error {
	if err := a.BindOpenAPI(doc); err != nil {
		return log.Log.FatalAndCreateErrorf("OPENAPI_LOAD_ERROR:%s:%v", a.NameId, err)
	}
	ws := 0
	if doc.WebSocketEndPoints != nil {
		ws = len(doc.WebSocketEndPoints.EndPoints)
	}
	log.Log.Infof("API %s: OpenAPI document bound: %d operations, %d WebSocket endpoints", a.NameId, len(doc.OperationIds())-ws, ws)
	return nil
}

// BindOpenAPI is LoadOpenAPI without the fatal: it returns the error and
// changes nothing on failure. Everything is checked and every endpoint built
// before the first one is appended, so a document is bound whole or not at
// all.
func (a *DXAPI) BindOpenAPI(doc *DXOpenAPIDocument) error {
	if err := doc.Validate(); err != nil {
		return err
	}
	state := openAPIStateEnsure(a)

	for _, id := range state.handlerOrder {
		if state.handlers[id].count > 1 {
			return errors.Errorf("OPENAPI_HANDLER_REGISTERED_TWICE:%s:%s", a.NameId, id)
		}
	}
	specWithoutHandler, handlerWithoutSpec := a.OpenAPIDrift(doc)
	if len(specWithoutHandler) > 0 || len(handlerWithoutSpec) > 0 {
		return errors.Errorf("OPENAPI_DRIFT:%s:SPEC_WITHOUT_HANDLER=[%s]:HANDLER_WITHOUT_SPEC=[%s]",
			a.NameId, strings.Join(specWithoutHandler, ","), strings.Join(handlerWithoutSpec, ","))
	}

	// URIs: once each, across what is already registered and what the
	// document adds. NewEndPoint would find a duplicate too, with a fatal;
	// this returns it.
	claimed := map[string]string{}
	for i := range a.EndPoints {
		claimed[a.EndPoints[i].Uri] = "already registered on the API"
	}
	claim := func(uri, where string) error {
		if prior, taken := claimed[uri]; taken {
			return errors.Errorf("OPENAPI_URI_ALREADY_REGISTERED:%s:%s:%s", uri, prior, where)
		}
		claimed[uri] = where
		return nil
	}
	for _, path := range doc.Paths.Keys() {
		if err := claim(path, "/paths/"+openAPIPointerEscape(path)); err != nil {
			return err
		}
	}
	if doc.WebSocketEndPoints != nil {
		for i, ws := range doc.WebSocketEndPoints.EndPoints {
			if err := claim(ws.Path, fmt.Sprintf("/%s/endpoints/%d", OpenAPIExtensionWebSocketEndPoints, i)); err != nil {
				return err
			}
		}
	}
	if err := openAPICheckMuxPatterns(a, claimed); err != nil {
		return err
	}

	resolver := &openAPISchemaResolver{components: doc.Components}
	var built []DXAPIEndPoint
	pathParameters := map[string][]DXAPIEndPointParameter{}
	for _, path := range doc.Paths.Keys() {
		item, _ := doc.Paths.Get(path)
		methods, ops := item.Operations()
		for i, op := range ops {
			pointer := "/paths/" + openAPIPointerEscape(path) + "/" + strings.ToLower(methods[i])
			h := state.handlers[op.OperationId]
			if h.ws != nil {
				return errors.Errorf("OPENAPI_HTTP_OPERATION_HAS_WS_HANDLER:%s:%s:USE_RegisterHandler", op.OperationId, pointer)
			}
			ep, pathParams, err := openAPIEndPointFromOperation(a, path, methods[i], op, h, resolver, pointer)
			if err != nil {
				return err
			}
			built = append(built, ep)
			if len(pathParams) > 0 {
				pathParameters[path] = pathParams
			}
		}
	}
	if doc.WebSocketEndPoints != nil {
		for i, ws := range doc.WebSocketEndPoints.EndPoints {
			pointer := fmt.Sprintf("/%s/endpoints/%d", OpenAPIExtensionWebSocketEndPoints, i)
			h := state.handlers[ws.OperationId]
			if h.ws == nil {
				return errors.Errorf("OPENAPI_WS_OPERATION_HAS_HTTP_HANDLER:%s:%s:USE_RegisterWSHandler", ws.OperationId, pointer)
			}
			ep, err := openAPIEndPointFromWebSocket(a, ws, h.ws, pointer)
			if err != nil {
				return err
			}
			built = append(built, ep)
		}
	}

	a.EndPoints = append(a.EndPoints, built...)
	for uri, params := range pathParameters {
		state.pathParameters[uri] = params
	}
	return nil
}

// OpenAPIDrift is the two-sided comparison behind the startup check: the
// operationIds in the document with no registered handler, and the registered
// handlers with no operationId in the document, both sorted. Either list
// non-empty means the document and the code describe different APIs.
func (a *DXAPI) OpenAPIDrift(doc *DXOpenAPIDocument) (specWithoutHandler, handlerWithoutSpec []string) {
	state := openAPIStateOf(a)
	inSpec := map[string]bool{}
	for _, id := range doc.OperationIds() {
		inSpec[id] = true
		if state == nil || state.handlers[id] == nil {
			specWithoutHandler = append(specWithoutHandler, id)
		}
	}
	if state != nil {
		for _, id := range state.handlerOrder {
			if !inSpec[id] {
				handlerWithoutSpec = append(handlerWithoutSpec, id)
			}
		}
	}
	sort.Strings(specWithoutHandler)
	sort.Strings(handlerWithoutSpec)
	return specWithoutHandler, handlerWithoutSpec
}

// openAPICheckMuxPatterns registers every URI the API will serve into a
// throwaway ServeMux. Go's mux panics at Handle time on a malformed wildcard
// or on two patterns that conflict, and StartAndWait would meet that panic
// after every definition step had run; here it becomes a load error that
// names the pattern. Raw handlers are included because they share the mux.
func openAPICheckMuxPatterns(a *DXAPI, uris map[string]string) (err error) {
	patterns := make([]string, 0, len(uris)+len(a.RawHandlers))
	for uri := range uris {
		patterns = append(patterns, uri)
	}
	sort.Strings(patterns)
	for _, rh := range a.RawHandlers {
		patterns = append(patterns, rh.Pattern)
	}
	mux := http.NewServeMux()
	for _, pattern := range patterns {
		if err := openAPITryHandle(mux, pattern); err != nil {
			return err
		}
	}
	return nil
}

func openAPITryHandle(mux *http.ServeMux, pattern string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("OPENAPI_PATH_CONFLICT:%s:%v", pattern, r)
		}
	}()
	mux.Handle(pattern, http.NotFoundHandler())
	return nil
}

// openAPIEndPointFromOperation builds the endpoint for one operation. Path
// parameters are returned separately: they are deliberately not in
// Parameters, because PreProcessRequest would look for them in the query
// string or body and reject the request as missing them. The middleware
// returned by openAPIPathParameterMiddleware puts them in ParameterValues
// instead, first in the chain, so a handler reads them with the same
// GetParameterValueAs* calls as everything else.
func openAPIEndPointFromOperation(a *DXAPI, path, method string, op *DXOpenAPIOperation, h *dxOpenAPIHandler, r *openAPISchemaResolver, pointer string) (DXAPIEndPoint, []DXAPIEndPointParameter, error) {
	ep := DXAPIEndPoint{
		Owner:                   a,
		Title:                   op.Summary,
		Description:             op.Description,
		Uri:                     path,
		Method:                  method,
		EndPointType:            EndPointTypeHTTPJSON,
		RequestContentType:      utilsHttp.RequestContentTypeNone,
		OnExecute:               h.onExecute,
		ResponsePossibilities:   &DXAPIEndPointResponsePossibilities{},
		Privileges:              append([]string{}, op.Privileges...),
		RequestMaxContentLength: op.MaxContentLength,
		RateLimitGroupNameId:    op.RateLimitGroup,
	}
	if len(op.Privileges) == 0 {
		ep.Privileges = nil
	}
	if op.EndPointType != "" {
		t, err := openAPIEndPointTypeFromName(op.EndPointType, pointer+"/"+OpenAPIExtensionEndPointType)
		if err != nil {
			return ep, nil, err
		}
		ep.EndPointType = t
	}

	var pathParams []DXAPIEndPointParameter
	for i, p := range op.Parameters {
		pPointer := fmt.Sprintf("%s/parameters/%d/schema", pointer, i)
		param, err := openAPIParameterFromSchema(p.Name, p.Schema, p.Required, r, pPointer)
		if err != nil {
			return ep, nil, err
		}
		if p.Description != "" {
			param.Description = p.Description
		}
		switch p.In {
		case "path":
			pathParams = append(pathParams, param)
		case "query":
			ep.Parameters = append(ep.Parameters, param)
		}
	}

	if openAPIBodyMethods[method] {
		if op.RequestBody != nil {
			mediaType := op.RequestBody.Content.Keys()[0]
			mtPointer := pointer + "/requestBody/content/" + openAPIPointerEscape(mediaType)
			ct, err := openAPIRequestContentTypeFromName(mediaType, mtPointer)
			if err != nil {
				return ep, nil, err
			}
			ep.RequestContentType = ct
			mt, _ := op.RequestBody.Content.Get(mediaType)
			switch ct {
			case utilsHttp.RequestContentTypeApplicationJSON,
				utilsHttp.RequestContentTypeMultiPartFormData,
				utilsHttp.RequestContentTypeApplicationXWwwFormUrlEncoded:
				if mt.Schema == nil {
					return ep, nil, openAPIMissing(mtPointer, "schema")
				}
				params, err := openAPIParametersFromObjectSchema(mt.Schema, r, mtPointer+"/schema")
				if err != nil {
					return ep, nil, err
				}
				ep.Parameters = params
			}
		}
		if op.ParametersSchema != nil {
			switch ep.RequestContentType {
			case utilsHttp.RequestContentTypeNone, utilsHttp.RequestContentTypeApplicationOctetStream, utilsHttp.RequestContentTypeTextPlain:
			default:
				return ep, nil, errors.Errorf("OPENAPI_X_DXLIB_PARAMETERS_WITH_A_PARAMETER_BODY:%s/%s:THE_BODY_SCHEMA_IS_THE_PARAMETER_SET", pointer, OpenAPIExtensionParameters)
			}
			params, err := openAPIParametersFromObjectSchema(op.ParametersSchema, r, pointer+"/"+OpenAPIExtensionParameters)
			if err != nil {
				return ep, nil, err
			}
			ep.Parameters = params
		}
	} else if op.RequestContentType != "" {
		ct, err := openAPIRequestContentTypeFromName(op.RequestContentType, pointer+"/"+OpenAPIExtensionRequestContentType)
		if err != nil {
			return ep, nil, err
		}
		ep.RequestContentType = ct
	}

	if op.Responses != nil {
		possibilities, err := openAPIResponsePossibilities(op.Responses, r, pointer+"/responses")
		if err != nil {
			return ep, nil, err
		}
		ep.ResponsePossibilities = &possibilities
	}

	if len(pathParams) > 0 {
		ep.Middlewares = append(ep.Middlewares, openAPIPathParameterMiddleware(pathParams))
	}
	ep.Middlewares = append(ep.Middlewares, h.middlewares...)
	return ep, pathParams, nil
}

// openAPIParametersFromObjectSchema resolves a body schema and reads its
// properties as the parameter list. A body that is not an object has no
// parameter list to be.
func openAPIParametersFromObjectSchema(s *DXOpenAPISchema, r *openAPISchemaResolver, pointer string) ([]DXAPIEndPointParameter, error) {
	resolved, release, err := r.resolve(s, pointer)
	if err != nil {
		return nil, err
	}
	defer release()
	if resolved.Type.Primary() != "object" {
		return nil, errors.Errorf("OPENAPI_BODY_SCHEMA_MUST_BE_AN_OBJECT:%s:GOT_%s", pointer, resolved.Type.Primary())
	}
	if resolved.AdditionalProperties != nil {
		return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:additionalProperties-on-body:%s:DECLARE_EVERY_PARAMETER", pointer)
	}
	return openAPIParametersFromProperties(resolved, r, pointer)
}

func openAPIResponsePossibilities(responses *DXOpenAPIOrderedMap[*DXOpenAPIResponse], r *openAPISchemaResolver, pointer string) (DXAPIEndPointResponsePossibilities, error) {
	out := DXAPIEndPointResponsePossibilities{}
	for _, code := range responses.Keys() {
		resp, _ := responses.Get(code)
		rPointer := pointer + "/" + code
		statusCode, err := strconv.Atoi(code)
		if err != nil {
			return nil, errors.Errorf("OPENAPI_BAD_STATUS_CODE:%s:%s", code, rPointer)
		}
		name := resp.Name
		if name == "" {
			name = code
		}
		if _, dup := out[name]; dup {
			return nil, errors.Errorf("OPENAPI_DUPLICATE_RESPONSE_NAME:%s:%s", name, rPointer)
		}
		rp := DXAPIEndPointResponsePossibility{StatusCode: statusCode, Description: resp.Description}
		if resp.Headers != nil && resp.Headers.Len() > 0 {
			rp.Headers = map[string]string{}
			for _, h := range resp.Headers.Keys() {
				header, _ := resp.Headers.Get(h)
				rp.Headers[h] = header.Description
			}
		}
		if resp.Content != nil {
			for _, mediaType := range resp.Content.Keys() {
				mt, _ := resp.Content.Get(mediaType)
				params, err := openAPIParametersFromObjectSchema(mt.Schema, r, rPointer+"/content/"+openAPIPointerEscape(mediaType)+"/schema")
				if err != nil {
					return nil, err
				}
				for i := range params {
					rp.DataTemplate = append(rp.DataTemplate, &params[i])
				}
			}
		}
		out[name] = rp
	}
	return out, nil
}

func openAPIEndPointFromWebSocket(a *DXAPI, ws *DXOpenAPIWebSocketEndPoint, h *DXOpenAPIWSHandler, pointer string) (DXAPIEndPoint, error) {
	if h.OnLoop == nil && h.OnMessage == nil {
		return DXAPIEndPoint{}, errors.Errorf("OPENAPI_WS_HANDLER_WITHOUT_OnMessage_OR_OnLoop:%s:%s", ws.OperationId, pointer)
	}
	ep := DXAPIEndPoint{
		Owner:                a,
		Title:                ws.Summary,
		Description:          ws.Description,
		Uri:                  ws.Path,
		Method:               ws.Method,
		EndPointType:         EndPointTypeWS,
		RequestContentType:   utilsHttp.RequestContentTypeNone,
		OnWSLoop:             h.OnLoop,
		OnWSOpen:             h.OnOpen,
		OnWSMessage:          h.OnMessage,
		OnWSClose:            h.OnClose,
		OnWSPeriodic:         h.OnPeriodic,
		Middlewares:          h.Middlewares,
		RateLimitGroupNameId: ws.RateLimitGroup,
	}
	if len(ws.Privileges) > 0 {
		ep.Privileges = append([]string{}, ws.Privileges...)
	}
	if ws.PeriodicInterval != "" {
		d, err := time.ParseDuration(ws.PeriodicInterval)
		if err != nil {
			return ep, errors.Errorf("OPENAPI_WS_BAD_PERIODIC_INTERVAL:%q:%s/periodicInterval", ws.PeriodicInterval, pointer)
		}
		ep.WSPeriodicInterval = d
	}
	return ep, nil
}

// openAPIPathParameterMiddleware reads each path parameter off the request
// with PathValue -- which Go's ServeMux fills for a {name} segment -- and
// puts it through the same SetRawValue and Validate as a query or body
// parameter. It runs before the endpoint's own middlewares, so those see the
// value too. The mux only routes here when every segment is present, so a
// missing value cannot occur; a value that does not validate is the usual
// 422.
func openAPIPathParameterMiddleware(params []DXAPIEndPointParameter) DXAPIEndPointExecuteFunc {
	return func(aepr *DXAPIEndPointRequest) error {
		for i := range params {
			p := params[i]
			raw := aepr.Request.PathValue(p.NameId)
			rpv := aepr.NewAPIEndPointRequestParameter(p)
			if err := rpv.SetRawValue(raw, p.NameId); err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "INVALID_PATH_PARAMETER:%s:%v", p.NameId, err)
			}
			if err := rpv.Validate(); err != nil {
				return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", "INVALID_PATH_PARAMETER:%s:%v", p.NameId, err)
			}
		}
		return nil
	}
}
