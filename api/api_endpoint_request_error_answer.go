package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
)

// WriteResponseAsDomainError answers a domain error the way the route handler
// answers one returned by OnExecute: the code and the log details go to the
// log at Warn with the request dump, and the client gets the error's status
// with a body that holds only its reason code. A helper that writes its own
// response, such as DXRawTable.DoCreate, uses it so a client sees the same
// answer whichever path refused the request. Nothing is written when a
// response has already been sent; the log line is written either way.
func (aepr *DXAPIEndPointRequest) WriteResponseAsDomainError(domainErr DXAPIDomainError) {
	requestDump, err2 := aepr.RequestDumpAsString()
	if err2 != nil {
		requestDump = "REQUEST_DUMP_ERROR"
	}
	decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
	domainErrWrapped := errors.Errorf("DOMAIN_VALIDATION:%s:%s", domainErr.DomainErrorCode(), domainErr.DomainErrorLogDetails())
	aepr.Log.LogText(domainErrWrapped, log.DXLogLevelWarn, "", fmt.Sprintf("Raw Request:\n%s%s", requestDump, decryptedDump))
	if !aepr.ResponseHeaderSent {
		aepr.WriteResponseAsJSON(domainErr.DomainErrorHTTPStatusCode(), nil, domainErr.DomainErrorResponseBody())
	}
}

// WriteResponseAsInternalServerError answers an error that is the server's the
// way the route handler answers one returned by OnExecute: the full error and
// the request dump go to the log at Error, with the DB operation context
// (operation, table, masked data) when the error carries one, and the client
// gets a 500 whose body (ErrorResponseBody) names the reason
// INTERNAL_SERVER_ERROR, the operation (the request's method and endpoint),
// the cause (the error's text, credentials masked), what to do next and the
// error_log_ref that finds the log entry. A DXAPIError in err's chain is
// answered with its own status and body instead, and logged the same way
// (the whole chain, its stack trace and DB context). label heads the log line
// (EXECUTE_ERROR for the route handler) so the log says which path failed.
// Nothing is written when a response has already been sent; the log line is
// written either way.
func (aepr *DXAPIEndPointRequest) WriteResponseAsInternalServerError(label string, err error) {
	var apiErr *DXAPIError
	if errors.As(err, &apiErr) {
		aepr.logAndAnswerAPIError(label, err, apiErr)
		return
	}
	requestDump, err2 := aepr.RequestDumpAsString()
	if err2 != nil {
		requestDump = "REQUEST_DUMP_ERROR"
	}
	dbContextStr := dbContextLine(err)
	decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
	aepr.Log.Errorf(err, "%s:%+v%s\nRaw Request:\n%s%s", label, err, dbContextStr, requestDump, decryptedDump)
	aepr.writeErrorAnswer(http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", aepr.operationName("executing"), err,
		"the server failed while handling the request; send the error_log_ref to the service's operator, who can find the full error and request in the log", true)
}

// operationName names what the route handler was doing for the request:
// what, the method and the endpoint, as "executing POST /v1/things".
func (aepr *DXAPIEndPointRequest) operationName(what string) string {
	method := ""
	if aepr.Request != nil {
		method = aepr.Request.Method
	}
	uri := ""
	if aepr.EndPoint != nil {
		uri = aepr.EndPoint.Uri
	} else if aepr.Request != nil && aepr.Request.URL != nil {
		uri = aepr.Request.URL.Path
	}
	return strings.TrimSpace(strings.Join([]string{what, method, uri}, " "))
}
