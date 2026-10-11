package api

import (
	stderrors "errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXAPIError is an error that says how the request should be answered: the
// HTTP status, a reason code a client can branch on, the operation that
// failed, the underlying cause and what the caller can do next. A handler or
// middleware returns it (wrapped or not) and the route handler answers with
// its status and ErrorResponseBody, instead of the fixed 400 or 500 an
// untyped error gets.
type DXAPIError struct {
	StatusCode int
	Code       string
	Operation  string
	Cause      error
	Next       string
}

// NewAPIError returns a DXAPIError. code may be empty; the status text in
// upper snake case is used then (CONFLICT, SERVICE_UNAVAILABLE).
func NewAPIError(statusCode int, code string, operation string, cause error, next string) *DXAPIError {
	return &DXAPIError{StatusCode: statusCode, Code: code, Operation: operation, Cause: cause, Next: next}
}

func (e *DXAPIError) Error() string {
	return errorLine(e.Operation, causeText(e.Cause))
}

func (e *DXAPIError) Unwrap() error { return e.Cause }

// ResponseBody is the answer body for the error, see ErrorResponseBody. A DB
// error in the cause is named without its row values (responseCause).
func (e *DXAPIError) ResponseBody() utils.JSON {
	return ErrorResponseBody(e.StatusCode, e.Code, e.Operation, responseCause(e.Cause), e.Next)
}

func causeText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func errorLine(operation, cause string) string {
	switch {
	case operation == "":
		return cause
	case cause == "":
		return operation
	}
	return operation + ": " + cause
}

// statusCode returns the code for a status: its text in upper snake case.
func statusCode(statusCode int) string {
	text := http.StatusText(statusCode)
	if text == "" {
		return fmt.Sprintf("HTTP_%d", statusCode)
	}
	return strings.ToUpper(strings.NewReplacer(" ", "_", "-", "_", "'", "").Replace(text))
}

// ErrorResponseBody builds the body of an error answer:
//
//	error        the one line a caller shows, "<operation>: <cause>"
//	operation    what was being done when it failed
//	cause        the underlying error, with credentials masked (RedactErrorText)
//	next         what the caller can do next (left out when empty)
//	status_code  the HTTP status, as a number
//
// It also carries dxlib's older members, so existing clients keep working:
// status (the status text), reason (the code; the status text in upper snake
// case when code is empty) and reason_message (the same line as error).
func ErrorResponseBody(statusCode_ int, code string, operation string, cause string, next string) utils.JSON {
	if code == "" {
		code = statusCode(statusCode_)
	}
	cause = RedactErrorText(cause)
	line := errorLine(operation, cause)
	if line == "" {
		line = code
	}
	body := utils.JSON{
		"error":          line,
		"operation":      operation,
		"cause":          cause,
		"status":         http.StatusText(statusCode_),
		"status_code":    statusCode_,
		"reason":         code,
		"reason_message": line,
	}
	if next != "" {
		body["next"] = next
	}
	return body
}

// credentialInText finds a credential written into error text as key=value or
// key: value, the way a DSN, a URL query or a wrapped config error carries
// one. The value is masked, the key is kept so the text still says what was
// wrong with it.
var credentialInText = regexp.MustCompile(`(?i)\b([a-z0-9_.-]*(?:password|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key|private[_-]?key|credential|session[_-]?key|authorization)[a-z0-9_.-]*)(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s&,;)"']+)`)

// bearerInText finds an Authorization scheme followed by its credential.
var bearerInText = regexp.MustCompile(`(?i)\b(bearer|basic|token)\s+[A-Za-z0-9._~+/=-]{8,}`)

// userInfoInURL finds the password of a URL's user info (scheme://user:pass@host).
var userInfoInURL = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]+):[^\s@/]+@`)

// RedactErrorText masks credentials that error text can carry, so the text can
// go into a response or a log: values of key=value and key: value pairs whose
// key names a credential, Bearer and Basic credentials, and the password in a
// URL's user info. Everything else is left as it is.
func RedactErrorText(s string) string {
	if s == "" {
		return s
	}
	s = userInfoInURL.ReplaceAllString(s, "$1:***REDACTED***@")
	s = bearerInText.ReplaceAllString(s, "$1 ***REDACTED***")
	s = credentialInText.ReplaceAllString(s, "$1$2***REDACTED***")
	return s
}

// writeErrorAnswer writes the answer for an error that reached the route
// handler: a DXAPIError in err's chain answers with its own status and body;
// any other error with defaultStatus, defaultCode, operation and next, its
// text as the cause. With withLogRef, the error_log_ref of the Error log line
// just written is added so a report can be matched to the log. Nothing is
// written when a response has already been sent.
func (aepr *DXAPIEndPointRequest) writeErrorAnswer(defaultStatus int, defaultCode string, operation string, err error, next string, withLogRef bool) {
	if aepr.ResponseHeaderSent {
		return
	}
	var body utils.JSON
	status := defaultStatus
	var apiErr *DXAPIError
	if errors.As(err, &apiErr) {
		status = apiErr.StatusCode
		body = apiErr.ResponseBody()
	} else {
		body = ErrorResponseBody(status, defaultCode, operation, responseCause(err), next)
	}
	errorLogRef := fmt.Sprintf("%d:%s", aepr.Log.LastErrorLogId, aepr.Log.LastErrorLogUid)
	if withLogRef {
		body["error_log_ref"] = errorLogRef
	}
	if status >= http.StatusInternalServerError && aepr.omitServerErrorCause() {
		cause := "withheld by this API's settings; the full error is in the server log"
		if withLogRef {
			cause += " entry " + errorLogRef
		}
		operation, _ := body["operation"].(string)
		body["cause"] = cause
		body["error"] = errorLine(operation, cause)
		body["reason_message"] = body["error"]
	}
	aepr.ResponseStatusCode = status
	aepr.WriteResponseAsJSON(status, nil, body)
}

// WriteResponseAsAPIError answers the request with status and an error body
// (ErrorResponseBody) naming operation, the cause err and next, and logs the
// same line with the masked request dump (see logAndAnswerAPIError). A
// handler can use it to answer an error itself and return nil. Nothing is
// written when a response has already been sent; the log line is written
// either way.
func (aepr *DXAPIEndPointRequest) WriteResponseAsAPIError(statusCode int, operation string, err error, next string) {
	aepr.logAndAnswerAPIError("API_ERROR", NewAPIError(statusCode, "", operation, err, next))
}

// logAndAnswerAPIError logs apiErr with the masked request dump and answers
// with its status and body: at Error, with error_log_ref in the body, for a
// status of 500 or more; at Warn below it, since a refusal of the caller's
// request is not a fault of the server. label heads the log line.
func (aepr *DXAPIEndPointRequest) logAndAnswerAPIError(label string, apiErr *DXAPIError) {
	requestDump, err2 := aepr.RequestDumpAsString()
	if err2 != nil {
		requestDump = fmt.Sprintf("REQUEST_DUMP_ERROR: %v", err2)
	}
	decryptedDump := "\n\n" + aepr.DecryptedRequestDumpAsString()
	isServerFault := apiErr.StatusCode >= http.StatusInternalServerError
	if isServerFault {
		aepr.Log.Errorf(apiErr, "%s:HTTP %d:%+v\nRaw Request:\n%s%s", label, apiErr.StatusCode, apiErr, requestDump, decryptedDump)
	} else {
		aepr.Log.Warnf("%s:HTTP %d:%v\nRaw Request:\n%s%s", label, apiErr.StatusCode, apiErr, requestDump, decryptedDump)
	}
	aepr.writeErrorAnswer(apiErr.StatusCode, apiErr.Code, apiErr.Operation, apiErr, apiErr.Next, isServerFault)
}

// omitServerErrorCause answers whether the request's API keeps the cause out
// of a 5xx answer (DXAPI.OmitServerErrorCause).
func (aepr *DXAPIEndPointRequest) omitServerErrorCause() bool {
	return aepr.EndPoint != nil && aepr.EndPoint.Owner != nil && aepr.EndPoint.Owner.OmitServerErrorCause
}

// rowDataInText finds the row values a DB error writes into its text
// (data={...}, set={...}, where={...}).
var rowDataInText = regexp.MustCompile(`\b(data|set|where)=\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}`)

// responseCause is err's text for an answer. The row values a DB error
// carries stay in the log: a DB error is named by its operation, table and the
// driver's error, and any data=, set= or where= block left in the text is
// replaced by a note that it is in the log.
func responseCause(err error) string {
	text := causeText(err)
	var dbCtx dbContextCarrier
	if errors.As(err, &dbCtx) {
		if dbErr, ok := dbCtx.(error); ok {
			short := fmt.Sprintf("DB_%s_ERROR table=%s", dbCtx.DBOperation(), dbCtx.DBTableName())
			if inner := stderrors.Unwrap(dbErr); inner != nil {
				short += ": " + inner.Error()
			}
			text = strings.Replace(text, dbErr.Error(), short, 1)
		}
	}
	return rowDataInText.ReplaceAllString(text, "$1=(values in the server log)")
}
