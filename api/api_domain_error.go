package api

import (
	"fmt"
	"net/http"

	"github.com/donnyhardyanto/dxlib/utils"
)

// DXAPIDomainError is an interface that domain errors implement.
// The API framework checks for this interface and writes structured HTTP responses.
// This keeps domain validation errors separate from unexpected server errors.
type DXAPIDomainError interface {
	error
	DomainErrorCode() string
	DomainErrorHTTPStatusCode() int
	DomainErrorResponseBody() utils.JSON
	DomainErrorLogDetails() string
}

// ErrUniqueFieldViolation is returned when a unique field constraint would be violated.
// TableName and Fields are logged server-side only (never sent in API response for security).
// Values is included in the API response so the frontend can show which values are duplicated.
type ErrUniqueFieldViolation struct {
	TableName string
	Fields    []string
	Values    utils.JSON
}

func (e *ErrUniqueFieldViolation) Error() string {
	return fmt.Sprintf("UNIQUE_FIELD_VIOLATION:TABLE=%s,FIELDS=%v,VALUES=%v", e.TableName, e.Fields, e.Values)
}

func (e *ErrUniqueFieldViolation) DomainErrorCode() string {
	return "UNIQUE_FIELD_VIOLATION"
}

func (e *ErrUniqueFieldViolation) DomainErrorHTTPStatusCode() int {
	return http.StatusConflict
}

func (e *ErrUniqueFieldViolation) DomainErrorResponseBody() utils.JSON {
	return utils.JSON{
		"reason":         "UNIQUE_FIELD_VIOLATION",
		"reason_message": "UNIQUE_FIELD_VIOLATION",
		"values":         e.Values,
	}
}

func (e *ErrUniqueFieldViolation) DomainErrorLogDetails() string {
	return fmt.Sprintf("TABLE=%s,FIELDS=%v,VALUES=%v", e.TableName, e.Fields, e.Values)
}
