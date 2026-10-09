package api

import (
	"net/http"

	"github.com/donnyhardyanto/dxlib/errors"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
)

// An idempotency key is a request header that tells a repeated request from a
// new one, so a client that lost the answer to a POST or a PATCH can send it
// again safely. GET, PUT and DELETE are idempotent by themselves (RFC 9110
// 9.2.2) and take none. The header and its name follow the IETF HTTP API
// working group's Idempotency-Key draft; the endpoint chooses the name.
//
// The library declares the key, writes it into the OpenAPI document and the
// Markdown spec, and checks it on every request: a required key that is not
// sent is refused with 400, as the draft says, and a key that does not
// validate against its declaration with 422, as any other parameter. The
// handler reads it from aepr.IdempotencyKey. Remembering which keys were used
// and answering a repeat as the first request was answered is the handler's
// work: it needs the service's own store and its own idea of "the same
// request". OPENAPI.md section 2.8 has the rules.

// openAPIIdempotencyKeyMethods are the methods that take an idempotency key:
// the ones RFC 9110 does not already make idempotent.
var openAPIIdempotencyKeyMethods = map[string]bool{"POST": true, "PATCH": true}

// openAPIIdempotencyKeyTypes are the types a key may be declared with. A key
// is opaque text; its format is said with the string bounds (Pattern,
// MinLength, MaxLength), so a UUID key is a string with a UUID pattern.
var openAPIIdempotencyKeyTypes = map[dxlibTypes.APIParameterType]bool{
	dxlibTypes.APIParameterTypeString:         true,
	dxlibTypes.APIParameterTypeNonEmptyString: true,
}

// SetEndPointIdempotencyKey declares the idempotency key of the endpoint
// registered on method and uri. key.NameId is the header name and
// key.IsMustExist says whether every request must send it. NewEndPoint
// returns a copy of what it registers, so the key is set here, on the
// registered endpoint, rather than on that copy.
func (a *DXAPI) SetEndPointIdempotencyKey(method, uri string, key DXAPIEndPointParameter) error {
	for i := range a.EndPoints {
		ep := &a.EndPoints[i]
		if ep.Method != method || ep.Uri != uri {
			continue
		}
		if err := checkIdempotencyKey(ep, &key); err != nil {
			return err
		}
		ep.IdempotencyKey = &key
		return nil
	}
	return errors.Errorf("IDEMPOTENCY_KEY_ENDPOINT_NOT_FOUND:%s:%s", method, uri)
}

// checkIdempotencyKey refuses a key declaration that the request check or a
// reader of the document could not make sense of.
func checkIdempotencyKey(ep *DXAPIEndPoint, key *DXAPIEndPointParameter) error {
	if key == nil {
		return nil
	}
	if ep.EndPointType == EndPointTypeWS {
		return errors.Errorf("IDEMPOTENCY_KEY_ON_A_WEBSOCKET_ENDPOINT:%s", ep.Uri)
	}
	if !openAPIIdempotencyKeyMethods[ep.Method] {
		return errors.Errorf("IDEMPOTENCY_KEY_ON_%s:%s:RFC_9110_MAKES_%s_IDEMPOTENT_ALREADY", ep.Method, ep.Uri, ep.Method)
	}
	if !isHTTPHeaderName(key.NameId) {
		return errors.Errorf("IDEMPOTENCY_KEY_BAD_HEADER_NAME:%q:%s", key.NameId, ep.Uri)
	}
	if !openAPIIdempotencyKeyTypes[key.Type] {
		return errors.Errorf("IDEMPOTENCY_KEY_TYPE_NOT_A_STRING:%q:%s:%s", string(key.Type), key.NameId, ep.Uri)
	}
	if key.IsNullable {
		return errors.Errorf("IDEMPOTENCY_KEY_NULLABLE:%s:%s:A_HEADER_IS_SENT_OR_NOT", key.NameId, ep.Uri)
	}
	if len(key.Children) > 0 {
		return errors.Errorf("IDEMPOTENCY_KEY_WITH_CHILDREN:%s:%s", key.NameId, ep.Uri)
	}
	if key.Default != nil || key.Const != nil {
		return errors.Errorf("IDEMPOTENCY_KEY_WITH_ONE_VALUE:%s:%s:EVERY_REQUEST_CARRIES_ITS_OWN_KEY", key.NameId, ep.Uri)
	}
	if err := key.checkBounds("string"); err != nil {
		return errors.Wrapf(err, "IDEMPOTENCY_KEY:%s", ep.Uri)
	}
	if err := key.checkAnnotations("string"); err != nil {
		return errors.Wrapf(err, "IDEMPOTENCY_KEY:%s", ep.Uri)
	}
	return nil
}

// isHTTPHeaderName says whether s is a field name: an RFC 9110 token, one or
// more tchar.
func isHTTPHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\'' || c == '*' ||
			c == '+' || c == '-' || c == '.' || c == '^' || c == '_' || c == '`' || c == '|' || c == '~':
		default:
			return false
		}
	}
	return true
}

// readIdempotencyKey checks the request's key against the endpoint's
// declaration and keeps it in aepr.IdempotencyKey. A header sent empty counts
// as not sent.
func (aepr *DXAPIEndPointRequest) readIdempotencyKey() error {
	aepr.IdempotencyKey = ""
	key := aepr.EndPoint.IdempotencyKey
	if key == nil {
		return nil
	}
	value := aepr.Request.Header.Get(key.NameId)
	if value == "" {
		if !key.IsMustExist {
			return nil
		}
		s := "IDEMPOTENCY_KEY_MISSING:" + key.NameId
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, s, s)
	}
	rpv := &DXAPIEndPointRequestParameterValue{Owner: aepr, Metadata: *key}
	if err := rpv.SetRawValue(value, key.NameId); err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusUnprocessableEntity, "", err.Error())
	}
	if err := rpv.Validate(); err != nil {
		aepr.WriteResponseAsError(http.StatusUnprocessableEntity, err)
		return err
	}
	aepr.IdempotencyKey = value
	return nil
}
