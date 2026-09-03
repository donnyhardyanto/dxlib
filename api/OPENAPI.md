# OpenAPI for the dxlib `api` package

## Summary

An API definition changes far more often than the code behind it: a parameter
is added, a privilege renamed, a content-length ceiling raised. In dxlib every
one of those changes has meant editing a Go file, because the definition lives
in `NewEndPoint` calls. What has been added, in one sentence: the endpoints
registered on a `DXAPI` can be written out as an OpenAPI 3.1 document, a
document in that same dialect can be read back and bound to handlers that
register by `operationId`, and a document and its handlers that disagree stop
the process from starting. The definition becomes a file; the code becomes a
set of named handlers; the join between them is checked.

Nothing changes for a service that does not use it. `NewEndPoint` and
`NewWSEndPoint` work as before, `PrintSpec` and `APIHandlerPrintSpec` still
serve the Markdown spec, and a service may bind a document for some endpoints
and keep the rest in code.

The one rule everything else follows from: **the emitter defines the dialect,
and the reader accepts exactly that dialect and refuses everything else by
name.** It reads the subset an endpoint declaration can express and nothing
else. A construct outside that subset (`oneOf`, a remote `$ref`, a `security`
requirement, a `pattern` constraint, an unknown standard field) is an error
naming the construct and the JSON pointer where it sat. A definition that gates
privileges must not be half-read.

The three pieces, all in `api/openapi_*.go` and all methods on `*DXAPI`:

```go
// Serve the document of whatever is registered, beside the Markdown spec.
api.NewEndPoint("openapi", "OpenAPI document", "/openapi.json", "GET",
    dxlibAPI.EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeNone,
    nil, api.APIHandlerOpenAPI, nil, nil, nil, nil, 0, "")

// Or write it to a file once, to start editing the definition there.
b, err := api.OpenAPIAsJSON()

// Bind a document: the handlers are the FIXED half, the file the FLUID half.
api.RegisterHandler("cmdContactCenterUserSessionList", handler.SessionList, handler.MiddlewareTokenAuthAndPrivilegeCheck)
api.RegisterWSHandler("ws", dxlibAPI.DXOpenAPIWSHandler{OnOpen: ws.Open, OnMessage: ws.Message, OnClose: ws.Close})
err = api.LoadOpenAPIFile("etc/api.openapi.yaml") // fatal on any disagreement
```

The rest of this document is the detail, from the split the design serves down
to the files.

## 1. The FLUID/FIXED split, and where the join is checked

The project's standing rule is that the parts of a codebase that change often,
API definitions and configuration, live apart from the logic that does not,
so that changing a parameter never means reading a control flow. An OpenAPI
file is the fluid half of an endpoint: URI, method, content type, parameters
with their types and required-ness, privileges, rate-limit group, content
length ceiling. A handler is the fixed half. The `operationId` is the name they
share.

What stays code, deliberately: `OnExecute`, the middleware chain, and the
WebSocket hooks. A document cannot carry a function, and a scheme that named
middlewares as strings would be a second registry to keep in step with the
first. `RegisterHandler` takes the middleware chain in the order `NewEndPoint`
would, so the chain sits beside the handler it guards.

The join is checked at load, both ways, in one message:

```
OPENAPI_LOAD_ERROR:api:OPENAPI_DRIFT:api:SPEC_WITHOUT_HANDLER=[cmdNew]:HANDLER_WITHOUT_SPEC=[cmdRetired]
```

An `operationId` in the document with no registered handler is a definition
nobody implements; a registered handler with no `operationId` in the document
is code the file no longer describes. Either one is the two halves drifting,
and it is fatal through `log.Log.FatalAndCreateErrorf`, the way a missing
`address` is in `ApplyConfigurations`: a service whose privilege declarations
did not load as written must not start. Under Kubernetes the pod goes to
CrashLoopBackOff with the reason in its log and the previous pods keep serving,
which is the right blast radius for a definition that disagrees with its code.

`BindOpenAPI` is the same operation returning the error instead of exiting,
for tests and for callers who want to decide. Everything is checked and every
endpoint built before the first one is appended to the API, so a document is
bound whole or not at all.

## 2. The dialect

### 2.1 Document shape

```
openapi: "3.1.0"
info: { title, version, description }
paths:
  /uri:
    <method>: <operation>
components:
  schemas: { <name>: <schema> }          # read; never written by the emitter
x-dxlib-websocket-endpoints:
  description: <why these are not under paths>
  endpoints: [ <websocket endpoint> ]
```

`info.title` is the API's `NameId` and `info.version` its `Version`. The
emitter writes `paths` in registration order and every object's keys in
declaration order, so the properties of a body come out in the order the
parameters were declared rather than alphabetically; an ordered map with its
own `MarshalJSON` does that. The reader keeps document order the same way. Two documents
describing the same endpoints are therefore byte-identical, which is what the
round-trip comparison in section 4 rests on.

### 2.2 Operations

| document | endpoint |
|---|---|
| path key | `Uri`, verbatim |
| method key (`get`, `post`, ...) | `Method`, upper-cased |
| `operationId` | derived from the URI, see below |
| `summary` | `Title` |
| `description` | `Description` |
| `parameters`, `requestBody` | `Parameters` and `RequestContentType`, placed by method and content type, see 2.3 |
| `responses` | `ResponsePossibilities`, keyed by status code, see 2.5 |
| `x-dxlib-*` | the rest of the endpoint, see 2.6 |

**operationId** is derived from the URI: the leading slash goes, every other
slash becomes an underscore, and the braces of a path template are dropped.
`/cmdContactCenterUserSessionList` is `cmdContactCenterUserSessionList`;
`/v2/cmdContactCenterUserSessionList` is `v2_cmdContactCenterUserSessionList`,
which is what keeps a v1 and a v2 form of one command distinct when both carry
the same `Title`. Handlers register against this id, so the rule is part of the
contract; two endpoints whose URIs derive to one id are an emission error
(`OPENAPI_OPERATION_ID_COLLISION`), not a last-wins. In a document read from
disk the `operationId` field is authoritative and the rule does not apply.

dxlib registers one endpoint per URI and checks the method inside it, so a
path item carries exactly one method. A hand-written document with two methods
on one path is refused (`multiple-methods-on-one-path`), because there is no
endpoint for it to become.

### 2.3 Where the parameters go

`PreProcessRequest` reads a body for `POST` and `PUT` and the query string for
everything else, and it reads a body according to the declared content type.
The document places the parameters where dxlib will read them:

| method | content type | parameters appear as |
|---|---|---|
| GET, DELETE (and any other non-body method) | any | `parameters[in=query]`; a non-empty content type is kept in `x-dxlib-request-content-type`, since dxlib ignores it here but the declaration holds it |
| POST, PUT | `application/json`, `multipart/form-data`, `application/x-www-form-urlencoded` | `requestBody.content.<type>.schema`, an object with one property per parameter and a `required` list |
| POST, PUT | `application/octet-stream` | `requestBody` is `{type: string, format: binary}`; parameters go to `x-dxlib-parameters`, because dxlib reads them from the `X-Var` header for a raw body |
| POST, PUT | `text/plain` | `requestBody` is `{type: string}`; parameters to `x-dxlib-parameters` |
| POST, PUT | none | no `requestBody`; parameters, if any, to `x-dxlib-parameters` |

Path templates (`/users/{id}`) add `parameters[in=path]`, always `required`.
A document that declares a query parameter on a POST, or a `requestBody` on a
GET, is refused rather than bound to an endpoint that would never read it.
The multipart form for the form encoding is `application/x-www-form-urlencoded`,
the registered name; dxlib's own `RequestContentType.String()` spells it
`application/x-public-form-urlencoded`, which no client sends, so the document
does not copy it.

File parts are not declared parameters (the handler reads
`MultipartForm.File`), so the multipart schema lists the text fields only.
Every multipart value arrives as text and is converted to its declared type
before validation, so a `type: integer` form field is honest.

### 2.4 Types

Several dxlib types share one JSON type (`int64`, `int64p` and `id` are all
`integer`/`int64`), so a schema alone cannot say which validator runs. Every
property the emitter writes therefore carries the exact dxlib type in
`x-dxlib-type` beside a faithful JSON Schema rendering, and where dxlib's
validator enforces a bound the schema states it (`int64p` is `minimum: 1`,
not just `integer`), so a consumer with no knowledge of dxlib still learns
what the server will do.

| dxlib type | JSON Schema |
|---|---|
| `string`, `protected-string`, `protected-sql-string`, `phonenumber`, `npwp`, `money` | `string` |
| `non-empty-string`, `protected-non-empty-string` | `string`, `minLength: 1` |
| `nullable-string` | `string` |
| `email` | `string`, `format: email` |
| `iso8601` / `date` / `time` | `string`, `format: date-time` / `date` / `time` |
| `blob` / `encrypted-blob` | `string`, `format: binary` / `byte` |
| `int32`, `nullable-int32` / `int32p` / `int32zp` | `integer`, `format: int32` / + `minimum: 1` / + `minimum: 0` |
| `int64`, `nullable-int64`, `id` / `int64p` / `int64zp` | `integer`, `format: int64` / + `minimum: 1` / + `minimum: 0` |
| `float32` / `float32p` / `float32zp` | `number`, `format: float` / + `exclusiveMinimum: 0` / + `minimum: 0` |
| `float64` / `float64p` / `float64zp` | `number`, `format: double` / + `exclusiveMinimum: 0` / + `minimum: 0` |
| `bool` | `boolean` |
| `json` | `object` with `properties` from `Children` and `required`; no `properties` when there are no children |
| `json-passthrough` | `object` |
| `map-string-string` | `object`, `additionalProperties: {type: string}` |
| `array` | `array` |
| `array-string` / `array-int64` | `array`, `items: {type: string}` / `{type: integer, format: int64}` |
| `array-json-template` | `array`, `items` an object with `properties` from `Children` |

`IsNullable` is spelled the 2020-12 way, `type: [T, "null"]`, and only when
the flag is set; the `nullable-*` types are carried by `x-dxlib-type` alone,
because a declaration may set the type without the flag. `Enum` is `enum`,
`Description` is `description`, `IsMustExist` is membership in the enclosing
`required` list (or `required: true` on a query or path parameter).

Reading a schema back, `x-dxlib-type` is used exactly when present, after a
check that the schema's `type` (and `format`, if both name one) agree with it
-- `type: string` with `x-dxlib-type: int64` is a contradiction and is refused.
Without it the dxlib type is inferred, under the rule that every constraint
the schema states must be one dxlib enforces, so the document never promises
more than the server does:

- `string` with no format is `string` (`nullable-string` if nullable);
  `minLength: 1` is `non-empty-string`; the formats above map back; any other
  format (`uuid`, `uri`) or any other `minLength` is refused.
- `integer` is `int64` (`int32` with that format); `minimum: 0` is the `zp`
  form, `minimum: 1` the `p` form, `minimum: 5` is refused. Nullable gives the
  `nullable-*` form.
- `number` is `float64` (`float32` with that format), `minimum: 0` the `zp`
  form, `exclusiveMinimum: 0` the `p` form.
- `object` with `properties` is `json`; with `additionalProperties: {type:
  string}` is `map-string-string`; with neither is `json-passthrough`, because
  dxlib's `json` type builds its value from the declared children and would
  hand the handler an empty object for a free-form body, whereas
  `json-passthrough` keeps what was sent.
- `array` with no `items` is `array`; string items `array-string`; integer
  items `array-int64`; object items `array-json-template`; anything else is
  refused.
- `money`, the `protected-*` strings, `phonenumber`, `npwp` and `id` have no
  JSON spelling of their own and are reached only through `x-dxlib-type`.

A local `$ref` into `components/schemas` is followed wherever a schema may
appear; a chain of references is followed to its end and a cycle is refused
(`OPENAPI_REF_CYCLE`). The model keeps the `$ref` unresolved so the document
re-emits as written; resolution happens when endpoints are built.

### 2.5 Responses

`ResponsePossibilities` is keyed by name with the status code inside;
OpenAPI keys responses by status code. The emitter writes `responses` keyed
by code, sorted, with the name in `x-dxlib-response-name`, `Headers` as
`headers` (name to `description`, string-typed), and `DataTemplate` as
`content.application/json.schema`, an object. Two possibilities on one status
code have no representation and are an emission error. An endpoint that
declares no possibilities gets no `responses` key at all, which 3.1 permits
and which is the truthful rendering, and reads back as none. `default` and
range keys like `2XX` are refused, because a possibility carries an integer.

### 2.6 The `x-dxlib-*` extensions

| extension | where | carries |
|---|---|---|
| `x-dxlib-endpoint-type` | operation | `EndPointType.String()`, always written so it is read and never defaulted; `EndPointTypeWS` under `paths` is refused, see 2.7 |
| `x-dxlib-privileges` | operation | `Privileges`; omitted when empty, read back as nil |
| `x-dxlib-rate-limit-group` | operation | `RateLimitGroupNameId`; omitted when empty |
| `x-dxlib-max-content-length` | operation | `RequestMaxContentLength`; omitted when zero; a negative value is refused |
| `x-dxlib-request-content-type` | operation | the declared content type of a non-body method, see 2.3; refused beside a `requestBody` |
| `x-dxlib-parameters` | operation | an object schema of the parameters when the body is not the parameter set, see 2.3 |
| `x-dxlib-type` | schema | the exact dxlib parameter type, see 2.4 |
| `x-dxlib-response-name` | response | the `ResponsePossibilities` key, see 2.5 |
| `x-dxlib-websocket-endpoints` | document | the WebSocket endpoints, see 2.7 |

Any other `x-dxlib-*` key is an error (`OPENAPI_UNKNOWN_DXLIB_EXTENSION`): a
misspelled one of ours must not be taken for somebody else's. Any `x-*` key
outside the `x-dxlib-` prefix is skipped, since that is what the extension
namespace is for, and is not carried: the model has no place for it, so it
does not survive a re-emission.

For the end-to-end-encryption endpoint types the body on the wire is an
encrypted envelope whatever the content type says; the schema describes the
decrypted request the handler sees, and `x-dxlib-endpoint-type` is how a
reader knows to expect the envelope.

### 2.7 WebSocket endpoints

OpenAPI 3.1 describes request/response operations. It has no construct for a
socket lifecycle (upgrade, open, frames in both directions, periodic push,
close), and listing a WebSocket endpoint under `paths` would present it as an
ordinary GET to every reader. So they are listed in one top-level extension:

```json
"x-dxlib-websocket-endpoints": {
  "description": "WebSocket endpoints registered on this API. OpenAPI 3.1 describes request/response operations and has no construct for a WebSocket lifecycle (...), so they are listed here and not under paths. The protocol inside the frames is the application's own and is not described.",
  "endpoints": [
    { "operationId": "ws", "path": "/ws", "method": "GET", "summary": "ws", "description": "WebSocket relay", "periodicInterval": "5s" }
  ]
}
```

The description is written into every emitted document so a reader who has
never seen dxlib learns from the file why the list is where it is. Each entry
carries `operationId`, `path`, `method`, `summary`, `description`,
`privileges`, `rateLimitGroup` and `periodicInterval` (a Go duration; absent
means the library's thirty seconds). An entry binds through
`RegisterWSHandler` to the hooks `NewWSEndPoint` takes, or to `OnLoop` for an
endpoint that runs its own lifecycle.

Three endpoint fields are not carried for a WebSocket endpoint, and the loss
is deliberate and tested: `Parameters`, `RequestContentType` and
`RequestMaxContentLength`. `PreProcessRequest` never runs for a WebSocket
endpoint, so none of the three has any effect, and the document says what the
server does.

## 3. What the reader refuses, and why

The failure this design exists to prevent is a construct quietly skipped in a
definition that decides who may call what. A reader built on a general
library with a permissive model would accept `oneOf`, `pattern` and any
`security` scheme, carry them in a struct, and bind an endpoint that enforces
none of them. (`security` is the one of those that is now partly readable, and
only the part that is true: `mutualTLS`, which the transport really does
enforce. Section 6a.) So the reader is a strict walker: every object position has the list of
keys it understands; a key on the list is read; a key off the list is an
error. The message names the key and the JSON pointer (RFC 6901, with `/` in
a path escaped as `~1`), and the line when the parser knows it:

```
OPENAPI_UNSUPPORTED_CONSTRUCT:oneOf:/paths/~1cmdX/post/requestBody/content/application~1json/schema/oneOf:line=14:SCHEMA_COMPOSITION_NOT_SUPPORTED
OPENAPI_UNKNOWN_FIELD:operationid:/paths/~1cmdX/post/operationid:line=6
```

A key is read, refused with a reason, or reported as unknown:

- **Standard keys this dialect implements** are read. The lists are in
  `openapi_read.go`, one per position.
- **Standard keys it recognises and does not implement** are refused with a
  reason: `oneOf`, `anyOf`, `allOf`, `not`, `discriminator` (schema
  composition); `callbacks`; `webhooks`; `security` and `securitySchemes`
  (requirements dxlib does not enforce, refused rather than ignored);
  `servers`, `tags`, `externalDocs`, `deprecated`, `examples`, `links`,
  `encoding`, `style`, `explode` and the like (not carried, so not accepted);
  `pattern`, `maximum`, `maxLength`, `minItems`, `uniqueItems`, `const` and the
  other constraints dxlib's validator does not apply; `nullable` (a 3.0
  keyword; `type: [T, "null"]` is the 3.1 spelling). `$ref` is accepted only
  as `#/components/schemas/<name>`; a remote reference, a reference into any
  other section, a `$ref` on a parameter, request body, response or header,
  and a `$ref` with sibling keys are all refused by name.
- **Unknown standard-looking keys** are `OPENAPI_UNKNOWN_FIELD`. A
  misspelling is the most likely cause, and silently ignoring `operationid`
  would bind nothing and say nothing.

Beyond the walker, `Validate` checks what a single node cannot show: the
version is `3.1.x` (3.0 uses a different schema dialect and is refused rather
than half-read); every `operationId` is present and unique across `paths` and
the WebSocket list; every `{name}` in a path has an `in: path` parameter and
every path parameter has a segment; `in` is `query` or `path` (`header` and
`cookie` are not bound); a request body has exactly one media type and it is
one dxlib reads; response codes are three digits; every `required` name is a
property; every `$ref` resolves; `type` is one JSON Schema type plus an
optional `"null"`; `format` is one the emitter writes. `Validate` runs in
`ReadOpenAPI` and again in `BindOpenAPI`, so a document built in code is held
to the same rules as one read from a file.

Both syntaxes are read into one ordered tree and walked once, so they cannot
drift apart. A file whose first non-blank byte is `{` is JSON, parsed with
`encoding/json`'s token stream (yaml.v3 cannot parse all of JSON: it rejects
the `\/` escape); anything else is YAML, parsed with `yaml.v3` into a
`yaml.Node`. Duplicate keys are refused in both (neither parser does that on
its own). In YAML, aliases, merge keys, a second document in the file, and
scalars whose resolved tag has no place here (an unquoted date is
`!!timestamp`) are refused with the fix in the message: quote the value.
Numbers keep their source text until they are used, so an `int64` at the edge
of its range is not rounded through a float.

## 4. The round-trip guarantee

The correctness argument for the reader and the binder is a round trip over
real endpoints, not invented ones. `api/testdata/openapi/` holds the emitter's
output over the endpoint definitions of every service in
`digital-contact-center`: 105 operations across eight documents, one per API
of six services. `TestOpenAPIRoundTripOverTheServiceCorpus` runs three legs
over each:

1. bytes → `ReadOpenAPI` → `AsJSON` is byte-identical to the input;
2. `ReadOpenAPI` → a handler registered for every id → `BindOpenAPI` onto a
   fresh API → `OpenAPIAsJSON` is byte-identical to the input;
3. the same document rewritten as YAML → `ReadOpenAPI` → `AsJSON` is
   byte-identical to the input.

If the emitter's own output survives all three, the reader and the binder are
correct for every construct the dialect contains, because the emitter is the
dialect. The corpus exercises what the services use; a second test builds one
endpoint with every type in the table plus the constructs the corpus lacks (an
enum, a nullable, nested children, responses with headers and a data
template, a bound path template with typed parameters) and runs the same
legs. The test pins the operation count, so a stale fixture fails rather than
proving nothing.

The edges where the loop is lossy, each deliberate and each asserted in a test
rather than tolerated by a looser comparison:

- Code: `OnExecute`, `Middlewares` and the WebSocket hooks are not in the
  document; the registry supplies them (section 1).
- WebSocket `Parameters`, `RequestContentType` and `RequestMaxContentLength`
  (section 2.7).
- An enum written from Go `int` values reads back as `int64`. The bytes are
  identical and dxlib compares enum members by formatted text, so nothing
  observable changes.
- A `ResponsePossibility`'s `Owner` pointer and a parameter's `Owner` and
  `Parent` pointers are rebuilt, not carried.
- Foreign `x-*` extensions in a hand-written document are dropped on
  re-emission (section 2.6). The guarantee is over documents in this dialect.

## 5. Binding

### 5.1 The registry

```go
func (a *DXAPI) RegisterHandler(operationId string, onExecute DXAPIEndPointExecuteFunc, middlewares ...DXAPIEndPointExecuteFunc)
func (a *DXAPI) RegisterWSHandler(operationId string, handler DXOpenAPIWSHandler)
func (a *DXAPI) LoadOpenAPIFile(path string) error   // fatal on error
func (a *DXAPI) LoadOpenAPI(doc *DXOpenAPIDocument) error
func (a *DXAPI) BindOpenAPI(doc *DXOpenAPIDocument) error   // returns the error
func (a *DXAPI) OpenAPIDrift(doc *DXOpenAPIDocument) (specWithoutHandler, handlerWithoutSpec []string)
```

Registration happens in the same define step as `NewEndPoint` calls, and the
load after it, so the drift check sees every handler. One document per API:
a second `BindOpenAPI` on the same API would find its URIs taken. Registering
one id twice is reported at load (`OPENAPI_HANDLER_REGISTERED_TWICE`) rather
than at the second call, so the report is one error with the whole picture. A
plain handler registered for a WebSocket id, or the reverse, is refused by
name.

The per-API state (handlers, and the declared path parameters of every bound
URI) sits in a side table keyed by the `*DXAPI` rather than in a field on
`DXAPI`, so `DXAPI`'s own definition is untouched by this feature. The table is
written under a lock and an API object lives for the process, so the key is
stable. Folding it into a field is a small change if `api.go` is being edited
anyway.

### 5.2 What binding produces

For each operation, a `DXAPIEndPoint` with `Uri`, `Method`, `EndPointType`,
`RequestContentType`, `Parameters` (built through the mapping in 2.4),
`ResponsePossibilities`, `Privileges`, `RequestMaxContentLength` and
`RateLimitGroupNameId` from the document, `OnExecute` and `Middlewares` from
the registration, and `Owner` set. A URI already registered on the API, by
`NewEndPoint` or by an earlier path in the same document, is an error before
anything is appended; `NewEndPoint` would have found it too, with a fatal.

### 5.3 Path templates and Go's ServeMux

`/users/{id}` is OpenAPI's path template and, since Go 1.22, `ServeMux`'s
wildcard pattern, the same spelling. `StartAndWait` registers every endpoint
with `mux.Handle(p.Uri, ...)`, so a bound template becomes a wildcard route
with no other change. This was verified against the real listener, not
inferred: `TestOpenAPIBoundEndPointsServeRequests` starts a `DXAPI` through
`StartAndWait`, binds `/users/{id}/files/{file_id}` and `/users/me`, and
checks that `/users/42/files/report.pdf` routes with both values, that
`/users/me` goes to the literal route (the mux prefers the more specific
pattern), and that `/users/42` is a 404.

Path parameters are deliberately **not** in `DXAPIEndPoint.Parameters`.
`PreProcessRequest` reads `Parameters` from the query string or the body, and
would reject a request as missing them. Instead the binder puts a middleware
first in the chain that reads each one with `Request.PathValue`, which the mux
fills for a `{name}` segment, and runs it through the same `SetRawValue` and
`Validate` as any other parameter into `ParameterValues`. A handler reads them
with the usual `GetParameterValueAsInt64("id")`, and so do the endpoint's own
middlewares, which run after it. The mux only routes when every segment is
present, so a missing value cannot occur; a value that does not validate
(`/users/forty-two/...` against an integer) is the usual 422. The declarations
live in the side table so the emitter can write them back out; an endpoint
registered through `NewEndPoint` with braces in its URI and no declaration is
emitted with `{type: string}` path parameters, the only honest schema for it.

Go's mux panics at `Handle` on a malformed wildcard (`/users/u{id}`) or on two
patterns neither of which is more specific (`/a/{x}/b` and `/a/c/{y}`). In
`StartAndWait` that panic would arrive after every define step had run. The
binder registers every URI the API will serve (existing endpoints, raw
handlers, and the document's paths) into a throwaway `http.NewServeMux()`
under `recover`, and turns the panic into `OPENAPI_PATH_CONFLICT:<pattern>:<the
mux's own text>`. Template segments must be whole segments, which is both
what the mux accepts and how OpenAPI's examples spell them; Go's `{name...}`
and `{$}` have no OpenAPI spelling and are refused before they reach a
document.

### 5.4 Runtime facts a document reader should know

These are dxlib's, not OpenAPI's; the document describes them faithfully but
does not change them.

- A GET parameter of type `bool` cannot be satisfied from the query string:
  the value arrives as the text `true` and dxlib's validator accepts a string
  only for the integer types. Declare it as a string, or send the request
  with an `X-Var` header.
- The `X-Var` header carries a JSON object of parameters for any method and
  is read before the body or query string. It is a transport detail and has
  no place in the document beyond `x-dxlib-parameters` for raw bodies.
- An optional parameter accepts `null` whatever its type; `IsNullable` only
  matters for a mandatory one, where it means "present but null is fine".
  `type: [T, "null"]` is written from the flag alone.

## 6. The corpus and how it was made

The eight documents under `digital-contact-center/src/cmd/<service>/openapi/`
(`api.openapi.json`, `oam.openapi.json`, `ws_api.openapi.json` as each service
has them) were written by a small program that creates the APIs a service's
configuration would (`api`, `oam`, `ws_api`), calls the same `Define*`
functions the service's `main.go` calls, and calls `OpenAPIAsJSON` on each
non-empty API. Nothing is started and no database is touched. The copies in
`api/testdata/openapi/` are the test fixture; refresh them from the service
documents when the definitions change, and the round-trip test's operation
count with them.

It has to be one binary per service: two services' handler trees in one
binary register the `postgres` driver twice and panic at init. And
`push-notification-server` panics that way on its own, because its
`handler/oam_cmd.go` imports `github.com/lib/pq`, which
registers `postgres`, and dxlib's `databases` package registers the same name,
and both are in that service's own dependency graph. Its five endpoints were
transcribed from `module_instance/define_api_push_notification.go` and
`define_oam_system.go` with stand-in handlers, every other argument verbatim.

**The fixture is anonymized, deliberately.** The copies under
`api/testdata/openapi/` keep the shape of those registrations exactly -- 104
operations across eight documents, the same parameter counts, required flags,
content types and privilege cardinality -- but every operation path, privilege
name, field name and service title is synthetic. Round-trip fidelity is a
property of structure, not of names, so nothing is lost by it. Two reasons it
is worth doing: a general-purpose library's test data has no business being one
deployment's endpoint inventory, which is a coupling problem before it is
anything else; and dxlib is a public repository, where a real corpus would
publish an API surface and its authorization vocabulary to anyone who cloned
it. The real documents stay in the service tree, which is where the drift check
uses them.

## 6a. Authentication, authorization, and what `security` may say

Three different things, kept apart on purpose.

**The body token is not a `security` scheme, and is not modelled as one.**
Authentication on these APIs is a `token` field inside the JSON request body.
OpenAPI 3.1 offers five scheme types -- `apiKey` (`in: query`, `header` or
`cookie` only), `http`, `oauth2`, `openIdConnect`, `mutualTLS` -- and not one of
them can express a credential carried in the body. Declaring `apiKey` with
`in: body` would produce a document that is invalid and that every reader
outside dxlib would misinterpret. So the token is what it actually is: a
request-body property, appearing in the `requestBody` schema like any other
parameter. That *is* the declaration; there is no `security` entry beside it.
The reader says so when someone reaches for `apiKey`:

```
OPENAPI_UNSUPPORTED_CONSTRUCT:security-scheme-type-apiKey:/components/securitySchemes/mutualTLS:
  ONLY_mutualTLS_IS_SUPPORTED:apiKey_CANNOT_DESCRIBE_A_BODY_TOKEN_BECAUSE_OPENAPI_ALLOWS_in=query|header|cookie_ONLY:
  A_BODY_TOKEN_IS_A_requestBody_PROPERTY
```

**`mutualTLS` is emitted when, and only when, it is true.** If the API is
served with TLS in force and `mode: mtls`, the document carries

```json
"components": { "securitySchemes": { "mutualTLS": { "type": "mutualTLS" } } },
"security": [ { "mutualTLS": [] } ]
```

That is a real OpenAPI 3.1 scheme type, and under `mode: mtls` every caller
really does have to present a certificate the listener verifies. Under `https`
or `http` neither field is emitted, because there would be nothing true to say.
The scheme is read off the API's own `DXAPI.TLS` rather than passed in, so a
document cannot claim a transport requirement its listener does not enforce.
The key is fixed at `mutualTLS` and the scope array must be empty; another key
would not survive the round trip, and a scope array would be an authorization
claim dxlib enforces nothing from.

**Authorization stays in `x-dxlib-privileges`.** It is checked against a
member's ACLs *after* the token middleware has run, and it is not an
alternative to `security` -- the two answer different questions. Mapping
privileges onto `security` would have meant inventing a scheme whose "scopes"
were ACL names: a lie in the document, and one that external tooling would read
as OAuth2 scopes issued by an authorization server.

## 6b. Moving an existing API onto the document

The order matters, because each step is checkable before the next.

1. **Emit what is already registered.** Leave every `NewEndPoint` call alone
   and write the document out, either from `OpenAPIAsJSON` in a small program
   or by serving `APIHandlerOpenAPI` once and saving the response. This is a
   faithful mirror of the current registrations, so if an endpoint is
   under-privileged in Go today the document will say so in the same words --
   it is a starting point, not an audit.
2. **Commit the document** beside the service's other configuration, and read
   it. This is the first time the API's shape, and which operations carry no
   privilege at all, is answerable with `jq` instead of by grepping the
   `define_api_*.go` files.
3. **Replace the registrations with handlers.** Each `NewEndPoint` call becomes
   a `RegisterHandler(operationId, handler, middlewares...)`, which takes the
   middleware chain in the order `NewEndPoint` did. Everything else -- URI,
   method, content type, parameters, privileges, rate-limit group, content
   length ceiling -- comes from the document.
4. **Load it.** `LoadOpenAPIFile` is fatal on any disagreement; `BindOpenAPI`
   returns the error instead, for tests. Every endpoint is built before the
   first is appended, so a document binds whole or not at all.
5. **Let the drift check hold the two halves together.** From here on, an
   `operationId` with no handler, or a handler with no `operationId`, stops the
   process with both lists named.

What does not move, and cannot: `OnExecute`, the middleware chain, and the
WebSocket hooks. A document cannot carry a function, and naming middlewares as
strings would be a second registry to keep in step with the first.

**The honest size of this.** One OAM API with a handful of endpoints is an
afternoon. `service-contact-center-api` is 83 operations, and converting it is
a real project whose payoff arrives only when a team stops editing Go to change
a parameter -- so it is worth doing per API, one at a time, with the drift check
catching every mistake, and not worth doing as a single change. A service may
bind a document for some of its APIs and keep the rest in code indefinitely;
nothing in the design pushes toward a big-bang migration.

## 7. What this is not

It does not consume arbitrary third-party OpenAPI documents, and it should
not grow to. A vendor's OpenAPI file is a description of *their* server; what
a client wants from it is typed request and response structs, and that is a
build-time code generation problem (`oapi-codegen` and its kind), not a
runtime parsing one. Conflating the two is what makes an OpenAPI library
heavy (schema composition, discriminators, every format, every constraint,
remote references), and every service that imports dxlib would inherit that
weight and that review surface. This reader parses the dialect dxlib itself
writes, with `gopkg.in/yaml.v3` and `encoding/json` and no new dependency.

It does not enforce `security` requirements; dxlib's privileges and
middlewares do that, and a document that declares `security` is refused so
nobody believes otherwise. It does not validate request bodies against the
schema at request time beyond what dxlib's own parameter validation does --
the schema is a rendering of that validation, not a replacement for it. And
it does not describe the protocol inside a WebSocket frame, which is the
application's own.

## 8. Files

| file | role |
|---|---|
| `openapi_document.go` | the model: typed structs with `json` tags, the ordered map, `AsJSON`, the extension names |
| `openapi_types.go` | the type mapping both ways, the content-type and endpoint-type tables, `$ref` resolution |
| `openapi_emit.go` | endpoints → document; `OpenAPIDocument`, `OpenAPIAsJSON`, `APIHandlerOpenAPI`, `OpenAPIOperationId` |
| `openapi_read.go` | file → document; the two parsers, the strict walker, `Validate` |
| `openapi_bind.go` | document + registry → endpoints; the drift check, the mux dry run, the path-parameter middleware, the fatal entry points |
| `openapi_*_test.go` | the emitter, the refusal table, the round trip over the corpus and over every type, and the bound endpoints served through the real listener |
| `testdata/openapi/` | the corpus, section 6 |
