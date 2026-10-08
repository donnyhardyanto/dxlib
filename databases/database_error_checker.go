package databases

import (
	"database/sql"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/sijms/go-ora/v2/network"
)

// StackTraceError is a custom error type that preserves stack traces
type StackTraceError struct {
	message    string
	stackTrace errors.StackTrace
	cause      error
}

// Error returns the error message
func (e *StackTraceError) Error() string {
	return e.message
}

// Cause returns the underlying cause of the error
func (e *StackTraceError) Cause() error {
	return e.cause
}

// StackTrace returns the preserved stack trace
func (e *StackTraceError) StackTrace() errors.StackTrace {
	return e.stackTrace
}

// Format implements fmt.Formatter to properly format the error
// This makes it work with fmt.Printf("%+v", err)
func (e *StackTraceError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = fmt.Fprintf(s, "%s\n", e.message)
			_, _ = fmt.Fprintf(s, "%v\n", e.cause)
			for _, pc := range e.stackTrace {
				_, _ = fmt.Fprintf(s, "%+v\n", errors.Frame(pc))
			}
			return
		}
		fallthrough
	case 's':
		_, _ = fmt.Fprintf(s, "%s", e.message)
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.message)
	default:
		_, _ = fmt.Fprintf(s, "%s", e.message)
	}
}

// Unwrap supports Go 1.13+ error unwrapping
func (e *StackTraceError) Unwrap() error {
	return e.cause
}

// GetDriverSpecificErrorMessage extracts the specific error message from different databases drivers
func GetDriverSpecificErrorMessage(err error) string {
	if err == nil {
		return ""
	}

	// Unwrap to get the original driver error
	cause := errors.Cause(err)

	// PostgreSQL
	if pgErr, ok := cause.(*pgconn.PgError); ok {
		return fmt.Sprintf("[PostgreSQL: %s] %s", pgErr.Code, pgErr.Message)
	}

	// MySQL/MariaDB
	if mariaDbErr, ok := cause.(*mysql.MySQLError); ok {
		return fmt.Sprintf("[MariaDB: %d] %s", mariaDbErr.Number, mariaDbErr.Message)
	}

	// SQL Server
	if mssqlErr, ok := cause.(mssql.Error); ok {
		return fmt.Sprintf("[SQL Server: %d] %s", mssqlErr.Number, mssqlErr.Message)
	}

	// For other errors, just return the error message
	return err.Error()
}

// ReplaceErrorWithDriverDetails replaces the error message with one that includes driver-specific details
func ReplaceErrorWithDriverDetails(originalErr error, standardMessage string) error {
	if originalErr == nil {
		return nil
	}

	driverMsg := GetDriverSpecificErrorMessage(originalErr)
	detailedMessage := fmt.Sprintf("%s: %s", standardMessage, driverMsg)

	return ReplaceErrorMessage(originalErr, detailedMessage)
}

// ReplaceErrorMessage replaces the message of an error while preserving its stack trace
func ReplaceErrorMessage(originalErr error, newMessage string) error {
	// Check if the original error has a stack trace
	type stackTracer interface {
		StackTrace() errors.StackTrace
	}

	st, ok := originalErr.(stackTracer)
	if !ok {
		// If the original error doesn't have a stack trace, just return a new error
		return errors.New(newMessage)
	}

	// Create a new error with the new message and the original stack trace
	return &StackTraceError{
		message:    newMessage,
		stackTrace: st.StackTrace(),
		cause:      errors.Cause(originalErr), // Preserve the original cause if it exists
	}
}

// CheckDatabaseError identifies common databases errors and returns standardized errors
// while preserving the original error information
func CheckDatabaseError(err error) error {
	if err == nil {
		return nil
	}

	// Check for connection errors
	if IsConnectionError(err) {
		return ReplaceErrorWithDriverDetails(err, "ERROR_DB_NOT_CONNECTED")
	}

	// Check for duplicate key errors
	if IsDuplicateKeyError(err) {
		return ReplaceErrorWithDriverDetails(err, "ERROR_DB_DUPLICATE_KEY")
	}

	// Return the wrapped original error for other cases
	return err
}

// IsDuplicateKeyError detects duplicate key violations across different databases systems
func IsDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	// Type-specific checks

	// PostgreSQL
	if pgErr, ok := errors.Cause(err).(*pgconn.PgError); ok {
		return pgErr.Code == "23505" // Unique violation
	}

	// MySQL/MariaDB
	if mariaDbErr, ok := errors.Cause(err).(*mysql.MySQLError); ok {
		return mariaDbErr.Number == 1062 // Duplicate entry
	}

	// SQL Server
	if mssqlErr, ok := errors.Cause(err).(mssql.Error); ok {
		return mssqlErr.Number == 2627 || mssqlErr.Number == 2601 // Unique constraint/index violation
	}

	// Oracle (go-ora)
	if oraErr, ok := errors.Cause(err).(*network.OracleError); ok {
		return oraErr.ErrCode == 1 // ORA-00001 unique constraint violated
	}

	// Error string pattern matching. A bare error number is never matched on
	// its own: "2601" sits inside PostgreSQL SQLSTATE 42601 (a syntax error) and
	// any id or amount may carry such digits, so each number is taken only in
	// the framing its driver prints.
	errMsg := err.Error()

	// PostgreSQL
	if strings.Contains(errMsg, "duplicate key") ||
		strings.Contains(errMsg, "SQLSTATE 23505") ||
		strings.Contains(errMsg, "violates unique constraint") {
		return true
	}

	// MySQL/MariaDB
	if strings.Contains(errMsg, "Duplicate entry") ||
		strings.Contains(errMsg, "Error 1062 ") {
		return true
	}

	// SQL Server
	if strings.Contains(errMsg, "Violation of UNIQUE KEY constraint") ||
		strings.Contains(errMsg, "Cannot insert duplicate key row") {
		return true
	}

	// Oracle
	if strings.Contains(errMsg, "ORA-00001") ||
		strings.Contains(errMsg, "unique constraint") {
		return true
	}

	// SQLite
	if strings.Contains(errMsg, "UNIQUE constraint failed") {
		return true
	}

	// Generic check
	if strings.Contains(strings.ToLower(errMsg), "duplicate") &&
		(strings.Contains(strings.ToLower(errMsg), "key") ||
			strings.Contains(strings.ToLower(errMsg), "unique")) {
		return true
	}

	return false
}

// IsConstraintViolationError detects a row the database refused for a reason
// other than a duplicate key: a foreign-key, check or not-null violation. Such
// an error says the request carried a value the schema cannot take, which is
// the caller's problem, where a duplicate key is a conflict and anything else
// (a lost connection, a syntax error) is the server's. Checked by driver type
// first, then by message, in the same way as IsDuplicateKeyError.
func IsConstraintViolationError(err error) bool {
	if err == nil {
		return false
	}
	if IsDuplicateKeyError(err) {
		return false
	}

	cause := errors.Cause(err)

	// PostgreSQL: SQLSTATE class 23 is "integrity constraint violation";
	// 23505 (unique) is taken above, so what is left is 23503 foreign key,
	// 23514 check, 23502 not null, 23000 generic, 23P01 exclusion.
	if pgErr, ok := cause.(*pgconn.PgError); ok {
		return strings.HasPrefix(pgErr.Code, "23")
	}

	// MySQL/MariaDB
	if mariaDbErr, ok := cause.(*mysql.MySQLError); ok {
		switch mariaDbErr.Number {
		case 1048, // column cannot be null
			1216, 1217, // foreign key constraint fails (older servers)
			1364,       // field has no default value
			1451, 1452, // foreign key constraint fails
			3819, // check constraint violated (MySQL 8.0.16+)
			4025: // CONSTRAINT failed (MariaDB)
			return true
		}
		return false
	}

	// SQL Server: 547 is both FOREIGN KEY and CHECK, 515 is NOT NULL.
	if mssqlErr, ok := cause.(mssql.Error); ok {
		return mssqlErr.Number == 547 || mssqlErr.Number == 515
	}

	// Oracle (go-ora)
	if oraErr, ok := cause.(*network.OracleError); ok {
		switch oraErr.ErrCode {
		case 1400, // cannot insert NULL
			2290, // check constraint violated
			2291, // integrity constraint violated - parent key not found
			2292: // integrity constraint violated - child record found
			return true
		}
		return false
	}

	// Error string pattern matching
	errMsg := err.Error()

	// PostgreSQL
	if strings.Contains(errMsg, "violates foreign key constraint") ||
		strings.Contains(errMsg, "violates check constraint") ||
		strings.Contains(errMsg, "violates not-null constraint") ||
		strings.Contains(errMsg, "SQLSTATE 23503") ||
		strings.Contains(errMsg, "SQLSTATE 23514") ||
		strings.Contains(errMsg, "SQLSTATE 23502") {
		return true
	}

	// MySQL/MariaDB. MariaDB words a CHECK violation (4025) as
	// "CONSTRAINT `c` failed for `s`.`t`"; MySQL (3819) as
	// "Check constraint 'c' is violated."
	if strings.Contains(errMsg, "a foreign key constraint fails") ||
		(strings.Contains(strings.ToLower(errMsg), "constraint") && strings.Contains(errMsg, "is violated")) ||
		(strings.Contains(errMsg, "CONSTRAINT `") && strings.Contains(errMsg, "` failed for ")) ||
		strings.Contains(errMsg, "cannot be null") ||
		strings.Contains(errMsg, "doesn't have a default value") {
		return true
	}

	// SQL Server
	if strings.Contains(errMsg, "conflicted with the FOREIGN KEY constraint") ||
		strings.Contains(errMsg, "conflicted with the REFERENCE constraint") ||
		strings.Contains(errMsg, "conflicted with the CHECK constraint") ||
		strings.Contains(errMsg, "Cannot insert the value NULL") {
		return true
	}

	// Oracle
	if strings.Contains(errMsg, "ORA-01400") ||
		strings.Contains(errMsg, "ORA-02290") ||
		strings.Contains(errMsg, "ORA-02291") ||
		strings.Contains(errMsg, "ORA-02292") {
		return true
	}

	// SQLite
	if strings.Contains(errMsg, "FOREIGN KEY constraint failed") ||
		strings.Contains(errMsg, "CHECK constraint failed") ||
		strings.Contains(errMsg, "NOT NULL constraint failed") {
		return true
	}

	return false
}

// IsConnectionError detects databases connection issues across different databases systems
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common network errors
	causeErr := errors.Cause(err)
	if causeErr == io.EOF ||
		errors.Is(causeErr, sql.ErrConnDone) ||
		errors.Is(causeErr, net.ErrClosed) ||
		errors.Is(causeErr, io.ErrUnexpectedEOF) {
		return true
	}

	// Unwrap the error to check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Type-specific checks

	// PostgreSQL
	if pgErr, ok := errors.Cause(err).(*pgconn.PgError); ok {
		// Class 08 - Connection Exception
		return strings.HasPrefix(pgErr.Code, "08")
	}

	// MySQL/MariaDB connection errors
	if mariaDbErr, ok := errors.Cause(err).(*mysql.MySQLError); ok {
		connectionErrors := map[uint16]bool{
			1040: true, 1042: true, 1043: true, 1047: true, 1053: true,
			1077: true, 1129: true, 1130: true, 2002: true, 2003: true,
			2005: true, 2006: true, 2013: true,
		}
		return connectionErrors[mariaDbErr.Number]
	}

	// SQL Server connection errors
	if mssqlErr, ok := errors.Cause(err).(mssql.Error); ok {
		connectionErrors := map[int32]bool{
			53: true, 10053: true, 10054: true, 10060: true,
			10061: true, 233: true, -2: true,
		}
		return connectionErrors[mssqlErr.Number]
	}

	// Error string pattern matching
	errMsg := strings.ToLower(err.Error())

	// General connection errors
	connectionPhrases := []string{
		"connection refused", "connection reset", "connection timed out",
		"connection closed", "connection lost", "broken pipe",
		"no connection", "cannot connect", "network error",
		"timeout", "timed out", "server has gone away",
		"lost connection", "socket", "server closed", "driver closed",
	}

	for _, phrase := range connectionPhrases {
		if strings.Contains(errMsg, phrase) {
			return true
		}
	}

	// Database-specific connection errors
	if strings.Contains(errMsg, "ora-03113") || strings.Contains(errMsg, "ora-03114") ||
		strings.Contains(errMsg, "ora-03135") || strings.Contains(errMsg, "ora-12541") ||
		strings.Contains(errMsg, "ora-12170") || strings.Contains(errMsg, "ora-12224") {
		return true
	}

	return false
}

// IsErrorType checks if an error is of a specific error type
func IsErrorType(err, target error) bool {
	if err == nil {
		return false
	}

	if target == nil {
		return false
	}

	return errors.Is(err, target) || strings.Contains(err.Error(), target.Error())
}
