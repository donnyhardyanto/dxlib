package tables

import (
	"net/http"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/utils"
)

// Reason codes for a row the database refused for a reason that is the
// client's. They are what the client reads in the response body; the driver
// message, the table and the data stay in the server log.
const (
	InsertRefusedReasonDuplicateKey        = "ERROR_DB_DUPLICATE_KEY"
	InsertRefusedReasonConstraintViolation = "ERROR_DB_CONSTRAINT_VIOLATION"
)

// ErrInsertRefused is what DXTable.DoCreate and DXTableAuditOnly.DoCreate
// return when the database refused the row for a reason that is the client's:
// a duplicate key (409 Conflict) or a foreign-key, check or not-null
// violation (422 Unprocessable Entity). It is an api.DXAPIDomainError, so the
// route handler answers that status with a body holding only the reason
// code, and logs the full error (driver message, table, masked data) at Warn
// with the request dump, as it does for ErrUniqueFieldViolation. Anything
// else the insert fails with is returned as it is, and the route handler
// answers its sanitized 500 with an error_log_ref.
//
// The callers keep the error they had: it is still non-nil, and Unwrap gives
// the insert error with the driver error as its cause.
type ErrInsertRefused struct {
	StatusCode int
	Reason     string
	Err        error
}

func (e *ErrInsertRefused) Error() string {
	return e.Reason + ": " + e.Err.Error()
}

func (e *ErrInsertRefused) Unwrap() error {
	return e.Err
}

func (e *ErrInsertRefused) DomainErrorCode() string {
	return e.Reason
}

func (e *ErrInsertRefused) DomainErrorHTTPStatusCode() int {
	return e.StatusCode
}

func (e *ErrInsertRefused) DomainErrorResponseBody() utils.JSON {
	return utils.JSON{
		"reason":         e.Reason,
		"reason_message": e.Reason,
	}
}

func (e *ErrInsertRefused) DomainErrorLogDetails() string {
	return e.Err.Error()
}

var _ api.DXAPIDomainError = (*ErrInsertRefused)(nil)

// insertError is the error a DoCreate that writes no error response of its
// own hands back to the route handler. A duplicate key becomes a 409
// ErrInsertRefused, a constraint violation a 422 one, by the same predicates
// insertErrorStatusCode uses for DXRawTable.DoCreate; everything else is
// returned unchanged, so the route handler treats it as the server error it
// is.
func insertError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case databases.IsDuplicateKeyError(err):
		return &ErrInsertRefused{StatusCode: http.StatusConflict, Reason: InsertRefusedReasonDuplicateKey, Err: err}
	case databases.IsConstraintViolationError(err):
		return &ErrInsertRefused{StatusCode: http.StatusUnprocessableEntity, Reason: InsertRefusedReasonConstraintViolation, Err: err}
	default:
		return err
	}
}
