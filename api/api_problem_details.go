package api

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/donnyhardyanto/dxlib/utils"
)

// ContentTypeProblemJSON is the media type of an RFC 9457 problem document.
const ContentTypeProblemJSON = "application/problem+json"

// problemTypeCode is the shape of a reason that names a code: upper case
// letters, digits and underscores, starting with a letter.
var problemTypeCode = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// legacyRefusalKeys are the members of dxlib's own refusal body. A problem
// document says the same through type, title, status and detail, so these are
// not copied into it.
var legacyRefusalKeys = map[string]bool{
	"status":         true,
	"status_code":    true,
	"reason":         true,
	"reason_message": true,
}

// problemDetailsSetting answers whether the request's API answers a refusal
// with a problem document, and the base its problem types are named under.
func (aepr *DXAPIEndPointRequest) problemDetailsSetting() (enabled bool, typeBaseURI string) {
	if aepr.EndPoint == nil || aepr.EndPoint.Owner == nil {
		return false, ""
	}
	a := aepr.EndPoint.Owner
	return a.ProblemDetailsEnabled, a.ProblemTypeBaseURI
}

// problemInstance is the path the refused request was sent to, without its
// query string, which may carry values that do not belong in a response.
func (aepr *DXAPIEndPointRequest) problemInstance() string {
	if aepr.Request == nil || aepr.Request.URL == nil {
		return ""
	}
	return aepr.Request.URL.Path
}

// problemType names the problem type for a reason. A reason that is a code
// (UNIQUE_FIELD_VIOLATION) becomes the last segment of the type, after
// typeBaseURI. A reason that is not a code, such as the status text a refusal
// carries when it names no reason of its own, gives "about:blank", which
// RFC 9457 defines as "nothing beyond the status code".
func problemType(typeBaseURI string, reason any) string {
	code, ok := reason.(string)
	if !ok || !problemTypeCode.MatchString(code) {
		return "about:blank"
	}
	return typeBaseURI + code
}

// NewProblemDetails turns a refusal body in dxlib's own shape
// ({status, status_code, reason, reason_message, ...}) into an RFC 9457
// problem document: type from reason (see problemType), title from the status
// text, status as the number, detail from reason_message, and instance when it
// is not empty. Every other member, such as fields or error_log_ref, is kept
// as an extension member; a member named like one of the problem members is
// replaced by it.
func NewProblemDetails(statusCode int, body utils.JSON, typeBaseURI string, instance string) utils.JSON {
	problem := utils.JSON{}
	for k, v := range body {
		if !legacyRefusalKeys[k] {
			problem[k] = v
		}
	}
	problem["type"] = problemType(typeBaseURI, body["reason"])
	problem["title"] = http.StatusText(statusCode)
	problem["status"] = statusCode
	if detail, ok := body["reason_message"].(string); ok && strings.TrimSpace(detail) != "" {
		problem["detail"] = detail
	} else {
		delete(problem, "detail")
	}
	if instance != "" {
		problem["instance"] = instance
	} else {
		delete(problem, "instance")
	}
	return problem
}

// plainRefusal renders a refusal written outside WriteResponseAsJSON, such as
// the plain answer an encrypted endpoint gives when its keys are gone, in the
// shape the API is set to answer with, and the Content-Type it goes with.
func (aepr *DXAPIEndPointRequest) plainRefusal(statusCode int, body utils.JSON) (utils.JSON, string) {
	enabled, typeBaseURI := aepr.problemDetailsSetting()
	if !enabled {
		return body, "application/json"
	}
	return NewProblemDetails(statusCode, body, typeBaseURI, aepr.problemInstance()), ContentTypeProblemJSON
}
