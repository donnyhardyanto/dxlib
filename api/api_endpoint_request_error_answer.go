package api

import (
	"fmt"
	"net/http"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
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
// gets a 500 whose body names only INTERNAL_SERVER_ERROR and the
// error_log_ref that finds the log entry. label heads the log line
// (EXECUTE_ERROR for the route handler) so the log says which path failed.
// Nothing is written when a response has already been sent; the log line is
// written either way.
func (aepr *DXAPIEndPointRequest) WriteResponseAsInternalServerError(label string, err error) {
	requestDump, err2 := aepr.RequestDumpAsString()
	if err2 != nil {
		requestDump = "REQUEST_DUMP_ERROR"
	}
	dbContextStr := ""
	var dbCtx dbContextCarrier
	if errors.As(err, &dbCtx) {
		dbContextStr = fmt.Sprintf("\nDB_CONTEXT: %s table=%s data=%s", dbCtx.DBOperation(), dbCtx.DBTableName(), dbCtx.DBMaskedDataString())
	}
	decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
	aepr.Log.Errorf(err, "%s:%+v%s\nRaw Request:\n%s%s", label, err, dbContextStr, requestDump, decryptedDump)
	if aepr.ResponseHeaderSent {
		return
	}
	errorLogRef := fmt.Sprintf("%d:%s", aepr.Log.LastErrorLogId, aepr.Log.LastErrorLogUid)
	aepr.WriteResponseAsJSON(http.StatusInternalServerError, nil, utils.JSON{
		"status":         "Internal Server Error",
		"status_code":    http.StatusInternalServerError,
		"reason":         "INTERNAL_SERVER_ERROR",
		"reason_message": "INTERNAL_SERVER_ERROR",
		"error_log_ref":  errorLogRef,
	})
}
