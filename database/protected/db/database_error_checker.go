package db

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
	"io"
	"net"
	"strings"
)

// Define specific error constants
var (
	ERROR_DB_DUPLICATE_KEY = errors.New("ERROR_DB_DUPLICATE_KEY")
	ERROR_DB_NOT_CONNECTED = errors.New("ERROR_DB_NOT_CONNECTED")
)

// CheckDatabaseError identifies common database errors and returns standardized errors
// while preserving the original error information
func CheckDatabaseError(err error) error {
	if err == nil {
		return nil
	}

	// Check for connection errors
	if isConnectionError(err) {
		return ERROR_DB_NOT_CONNECTED
	}

	// Check for duplicate key errors
	if IsDuplicateKeyError(err) {
		return ERROR_DB_DUPLICATE_KEY
	}

	// Return the wrapped original error for other cases
	return errors.WithStack(err)
}

// IsDuplicateKeyError detects duplicate key violations across different database systems
func IsDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	// Type-specific checks

	// PostgreSQL
	if pqErr, ok := errors.Cause(err).(*pq.Error); ok {
		return pqErr.Code == "23505" // Unique violation
	}

	// MySQL/MariaDB
	if mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError); ok {
		return mysqlErr.Number == 1062 // Duplicate entry
	}

	// SQL Server
	if mssqlErr, ok := errors.Cause(err).(mssql.Error); ok {
		return mssqlErr.Number == 2627 || mssqlErr.Number == 2601 // Unique constraint/index violation
	}

	// Error string pattern matching
	errMsg := err.Error()

	// PostgreSQL
	if strings.Contains(errMsg, "duplicate key") ||
		strings.Contains(errMsg, "23505") ||
		strings.Contains(errMsg, "violates unique constraint") {
		return true
	}

	// MySQL/MariaDB
	if strings.Contains(errMsg, "Duplicate entry") ||
		strings.Contains(errMsg, "1062") {
		return true
	}

	// SQL Server
	if strings.Contains(errMsg, "Violation of UNIQUE KEY constraint") ||
		strings.Contains(errMsg, "2627") ||
		strings.Contains(errMsg, "2601") {
		return true
	}

	// Oracle
	if strings.Contains(errMsg, "ORA-00001") ||
		strings.Contains(errMsg, "unique constraint") {
		return true
	}

	// SQLite
	if strings.Contains(errMsg, "UNIQUE constraint failed") ||
		strings.Contains(errMsg, "1555") {
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

// isConnectionError detects database connection issues across different database systems
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common network errors
	causeErr := errors.Cause(err)
	if causeErr == io.EOF ||
		causeErr == sql.ErrConnDone ||
		causeErr == net.ErrClosed ||
		causeErr == io.ErrUnexpectedEOF {
		return true
	}

	// Unwrap the error to check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Type-specific checks

	// PostgreSQL
	if pqErr, ok := errors.Cause(err).(*pq.Error); ok {
		// Class 08 - Connection Exception
		return strings.HasPrefix(string(pqErr.Code), "08")
	}

	// MySQL/MariaDB connection errors
	if mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError); ok {
		connectionErrors := map[uint16]bool{
			1040: true, 1042: true, 1043: true, 1047: true, 1053: true,
			1077: true, 1129: true, 1130: true, 2002: true, 2003: true,
			2005: true, 2006: true, 2013: true,
		}
		return connectionErrors[mysqlErr.Number]
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
