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
// wrong with it. maskCredentialPair decides which matches are credentials.
var credentialInText = regexp.MustCompile(`(?i)\b([a-z0-9_.-]*(password|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key|private[_-]?key|credential|session[_-]?key|authorization)[a-z0-9_.-]*)(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s&,;)"']+)`)

// bearerInText finds an HTTP authorization scheme and the word after it, with
// the Authorization header name when it is there.
var bearerInText = regexp.MustCompile(`(?i)(\bauthorization\s*[:=]\s*)?\b(bearer|basic)\s+([A-Za-z0-9._~+/=-]+)`)

// errorCodeKey is an error code in upper snake case (TOKEN_EXPIRED,
// SET_ENCRYPTION_SESSION_KEY_ERROR), which heads error text as "CODE: message".
var errorCodeKey = regexp.MustCompile(`^[A-Z0-9_]+$`)

// passwordKey is a key whose value is a password, often a plain word.
var passwordKey = regexp.MustCompile(`(?i)(password|passwd|pwd)`)

// userInfoInURL finds the password of a URL's user info (scheme://user:pass@host).
var userInfoInURL = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]+):[^\s@/]+@`)

// looksLikeCredential answers whether a word is shaped like a generated
// credential rather than prose: it carries a digit or one of . _ ~ + / = - or
// is 20 characters or longer.
func looksLikeCredential(word string) bool {
	if len(word) >= 20 {
		return true
	}
	return strings.ContainsAny(word, "0123456789._~+/=-")
}

// maskCredentialPair masks the value of one credentialInText match. key=value
// is always a credential. key: value is also how error text names a code and
// its message ("TOKEN_EXPIRED: session expired") or explains itself in prose
// ("invalid token: signature mismatch"), so it is masked only when the key is
// not an upper snake case error code and the value is quoted, shaped like a
// credential (looksLikeCredential) or follows a password key.
func maskCredentialPair(match string) string {
	m := credentialInText.FindStringSubmatch(match)
	key, sep, value := m[1], m[3], m[4]
	if strings.Contains(sep, ":") {
		if errorCodeKey.MatchString(key) {
			return match
		}
		quoted := strings.HasPrefix(value, `"`) || strings.HasPrefix(value, "'")
		if !quoted && !passwordKey.MatchString(key) && !looksLikeCredential(value) {
			return match
		}
	}
	return key + sep + "***REDACTED***"
}

// maskAuthorization masks the credential of one bearerInText match: always
// after an Authorization header name, otherwise only a word shaped like a
// credential, so "Basic authentication failed" keeps its words.
func maskAuthorization(match string) string {
	m := bearerInText.FindStringSubmatch(match)
	header, scheme, value := m[1], m[2], m[3]
	if header == "" && !looksLikeCredential(value) {
		return match
	}
	return header + scheme + " ***REDACTED***"
}

// RedactErrorText masks credentials that error text can carry, so the text can
// go into a response or a log: values of key=value and key: value pairs whose
// key names a credential (see maskCredentialPair), Bearer and Basic
// credentials, and the password in a URL's user info. Everything else is left
// as it is.
func RedactErrorText(s string) string {
	if s == "" {
		return s
	}
	s = userInfoInURL.ReplaceAllString(s, "$1:***REDACTED***@")
	s = bearerInText.ReplaceAllStringFunc(s, maskAuthorization)
	s = credentialInText.ReplaceAllStringFunc(s, maskCredentialPair)
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
		cause := withheldCause
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

// withheldCause is the cause a 5xx answer carries when its API keeps the error
// text out of it (DXAPI.OmitServerErrorCause).
const withheldCause = "withheld by this API's settings; the full error is in the server log"

// writeRefusal writes the answer of the older helpers (WriteResponseAndNewErrorf,
// WriteResponseAndLogAsError, WriteResponseAsError): status, status_code, reason
// (code, or the cause when code is empty, as WriteResponseAsError always
// answered) and reason_message (the cause), plus error, operation, cause and
// next as the route handler's error answers carry them. The cause loses any row
// values a DB error wrote into it and has its credentials masked; with
// DXAPI.OmitServerErrorCause a 5xx answer carries none of its text. A 2xx status
// is answered as 500. Nothing is written when a response has already been sent.
func (aepr *DXAPIEndPointRequest) writeRefusal(statusCode_ int, code string, cause string) {
	if aepr.ResponseHeaderSent {
		return
	}
	if (200 <= statusCode_) && (statusCode_ < 300) {
		statusCode_ = http.StatusInternalServerError
	}
	cause = RedactErrorText(rowDataInText.ReplaceAllString(cause, "$1=(values in the server log)"))
	code = RedactErrorText(rowDataInText.ReplaceAllString(code, "$1=(values in the server log)"))
	isServerFault := statusCode_ >= http.StatusInternalServerError
	if isServerFault && aepr.omitServerErrorCause() {
		cause = withheldCause
		// A caller may pass the error text as the code; only a bare code stays.
		if !errorCodeKey.MatchString(code) {
			code = statusCode(statusCode_)
		}
	}
	if code == "" {
		code = cause
	}
	operation := aepr.operationName("handling")
	next := "correct the request as the cause says and send it again"
	if isServerFault {
		next = "the server failed while handling the request; the service's operator can find the full error and request in the log"
	}
	body := ErrorResponseBody(statusCode_, code, operation, cause, next)
	body["reason_message"] = cause
	aepr.WriteResponseAsJSON(statusCode_, nil, body)
}
