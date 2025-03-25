package utils

import (
	"encoding/json"
	"fmt"
	"github.com/donnyhardyanto/dxlib/database2/utils/sql_expression"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	go_ora "github.com/sijms/go-ora/v2/network"
	"math/big"
	"strconv"
	"strings"
	"time"
)

type FieldsOrderBy map[string]string
type FieldTypeMapping map[string]string

// DbDriverFormatIdentifier formats an identifier (column/table name) according to database requirements
func DbDriverFormatIdentifier(driverName string, identifier string) string {
	switch driverName {
	case "oracle", "db2", "sqlserver", "mysql", "mariadb":
		// Use uppercase for all case-insensitive databases for consistency
		return strings.ToUpper(identifier)
	case "postgres":
		// PostgreSQL is case-sensitive but folds unquoted identifiers to lowercase
		return identifier // Keep as-is for PostgreSQL
	default:
		return identifier
	}
}

func DBDriverExcludeSQLExpressionFromWhereKeyValues(driverName string, kv utils.JSON) (r utils.JSON, err error) {
	r = utils.JSON{}
	for k, v := range kv {
		formattedKey := DbDriverFormatIdentifier(driverName, k)
		v, err := DbDriverConvertValueTypeToDBCompatible(driverName, v)
		if err != nil {
			return nil, err
		}
		r[formattedKey] = v
	}
	return r, nil
}

// validateOrderDirection ensures the order direction is valid
func validateOrderDirection(direction string) (string, error) {
	// Normalize the direction to uppercase for comparison
	upperDir := strings.ToUpper(strings.TrimSpace(direction))

	switch upperDir {
	case "ASC", "DESC":
		return upperDir, nil
	case "": // Default to ASC if empty
		return "ASC", nil
	default:
		return "", errors.Errorf("invalid sort direction: %s. Must be ASC or DESC", direction)
	}
}

func DbDriverFormatOrderByFieldName(driverName string, fieldName string, direction string) (string, error) {
	// Format the identifier according to database requirements
	fieldName = DbDriverFormatIdentifier(driverName, fieldName)

	// Validate direction
	validDirection, err := validateOrderDirection(direction)
	if err != nil {
		return "", err
	}

	switch driverName {
	case "oracle", "postgres", "mariadb", "db2":
		// These databases either support NULLS FIRST/LAST syntax directly
		// or just need standard ORDER BY syntax
		return fieldName + " " + validDirection, nil

	case "sqlserver", "mysql":
		// SQL Server and MySQL need special handling for NULLS FIRST/LAST
		// Since fieldName is now in uppercase (due to DbDriverFormatIdentifier),
		// we need to check for "NULLS FIRST" and "NULLS LAST" in uppercase

		if strings.Contains(fieldName, "NULLS FIRST") {
			cleanField := strings.TrimSpace(strings.Replace(fieldName, "NULLS FIRST", "", -1))

			if validDirection == "ASC" {
				// Default behavior: NULLS FIRST for ASC
				return cleanField + " " + validDirection, nil
			} else { // DESC with NULLS FIRST
				if driverName == "sqlserver" {
					return fmt.Sprintf("CASE WHEN %s IS NULL THEN 0 ELSE 1 END, %s %s", cleanField, cleanField, validDirection), nil
				} else { // mysql
					return fmt.Sprintf("%s IS NULL DESC, %s %s", cleanField, cleanField, validDirection), nil
				}
			}
		} else if strings.Contains(fieldName, "NULLS LAST") {
			cleanField := strings.TrimSpace(strings.Replace(fieldName, "NULLS LAST", "", -1))

			if validDirection == "DESC" {
				// Default behavior: NULLS LAST for DESC
				return cleanField + " " + validDirection, nil
			} else { // ASC with NULLS LAST
				if driverName == "sqlserver" {
					return fmt.Sprintf("CASE WHEN %s IS NULL THEN 1 ELSE 0 END, %s %s", cleanField, cleanField, validDirection), nil
				} else { // mysql
					return fmt.Sprintf("%s IS NULL, %s %s", cleanField, cleanField, validDirection), nil
				}
			}
		}

		// No NULLS specification, return as is
		return fieldName + " " + validDirection, nil

	default:
		return fieldName + " " + validDirection, nil
	}
}

func DbDriverConvertValueTypeToDBCompatible(driverName string, v any) (any, error) {
	switch v.(type) {
	case bool:
		// Convert booleans to integers for all databases
		// Oracle, SQL Server, MySQL, MariaDB all work with 0/1 for boolean values
		if v.(bool) {
			return 1, nil
		} else {
			return 0, nil
		}

	case sql_expression.SQLExpression:
		// Keep SQL expressions as is since they're handled specially
		return v, nil

	case []string:
		// Handle string arrays based on database type
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native array support
			return pq.Array(v.([]string)), nil
		case "oracle", "sqlserver", "mysql", "mariadb":
			// These databases don't have native array types
			// Convert to JSON string representation
			vx, err := utils.ArrayToJSON[string](v.([]string))
			if err != nil {
				return nil, err
			}
			return vx, nil
		default:
			// For unknown databases, use JSON representation as fallback
			vx, err := utils.ArrayToJSON[string](v.([]string))
			if err != nil {
				return nil, err
			}
			return vx, nil
		}

	case []int:
		// Handle int arrays based on database type
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native array support
			return pq.Array(v.([]int)), nil
		case "oracle", "sqlserver", "mysql", "mariadb":
			// These databases don't have native array types
			// Convert to JSON string representation
			vx, err := utils.ArrayToJSON[int](v.([]int))
			if err != nil {
				return nil, err
			}
			return vx, nil
		default:
			// For unknown databases, use JSON representation as fallback
			vx, err := utils.ArrayToJSON[int](v.([]int))
			if err != nil {
				return nil, err
			}
			return vx, nil
		}

	case []int64:
		// Handle int64 arrays based on database type
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native array support
			return pq.Array(v.([]int64)), nil
		case "oracle", "sqlserver", "mysql", "mariadb":
			// These databases don't have native array types
			// Convert to JSON string representation
			vx, err := utils.ArrayToJSON[int64](v.([]int64))
			if err != nil {
				return nil, err
			}
			return vx, nil
		default:
			// For unknown databases, use JSON representation as fallback
			vx, err := utils.ArrayToJSON[int64](v.([]int64))
			if err != nil {
				return nil, err
			}
			return vx, nil
		}

	case []float32:
		// Handle float32 arrays based on database type
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native array support
			return pq.Array(v.([]float32)), nil
		case "oracle", "sqlserver", "mysql", "mariadb":
			// These databases don't have native array types
			// Convert to JSON string representation
			vx, err := utils.ArrayToJSON[float32](v.([]float32))
			if err != nil {
				return nil, err
			}
			return vx, nil
		default:
			// For unknown databases, use JSON representation as fallback
			vx, err := utils.ArrayToJSON[float32](v.([]float32))
			if err != nil {
				return nil, err
			}
			return vx, nil
		}

	case []float64:
		// Handle float64 arrays based on database type
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native array support
			return pq.Array(v.([]float64)), nil
		case "oracle", "sqlserver", "mysql", "mariadb":
			// These databases don't have native array types
			// Convert to JSON string representation
			vx, err := utils.ArrayToJSON[float64](v.([]float64))
			if err != nil {
				return nil, err
			}
			return vx, nil
		default:
			// For unknown databases, use JSON representation as fallback
			vx, err := utils.ArrayToJSON[float64](v.([]float64))
			if err != nil {
				return nil, err
			}
			return vx, nil
		}

	case []bool:
		// Handle boolean arrays based on database type
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native array support
			return pq.Array(v.([]bool)), nil
		case "oracle", "sqlserver", "mysql", "mariadb":
			// These databases don't have native array types
			// Convert to JSON string representation
			vx, err := utils.ArrayToJSON[bool](v.([]bool))
			if err != nil {
				return nil, err
			}
			return vx, nil
		default:
			// For unknown databases, use JSON representation as fallback
			vx, err := utils.ArrayToJSON[bool](v.([]bool))
			if err != nil {
				return nil, err
			}
			return vx, nil
		}

	case time.Time:
		// Ensure consistent time formatting across databases
		switch driverName {
		case "oracle":
			// Oracle has specific datetime handling requirements
			// For Oracle, convert to a format it understands well
			return v.(time.Time).Format("2006-01-02 15:04:05"), nil
		default:
			// All the other major databases support standard time format
			return v, nil
		}

	case uuid.UUID:
		// UUID handling
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native UUID support
			return v, nil
		case "oracle", "sqlserver", "mysql", "mariadb":
			// For databases without native UUID support, convert to string
			return v.(uuid.UUID).String(), nil
		default:
			return v.(uuid.UUID).String(), nil
		}

	case json.RawMessage:
		// JSON handling
		switch driverName {
		case "postgres", "postgresql":
			// PostgreSQL has native JSON support
			return v, nil
		case "mysql", "mariadb":
			// MySQL 5.7+ and MariaDB 10.2+ have JSON support
			return v, nil
		case "sqlserver":
			// SQL Server 2016+ has JSON support but as NVARCHAR
			return string(v.(json.RawMessage)), nil
		case "oracle":
			// Oracle has limited JSON support, convert to string
			return string(v.(json.RawMessage)), nil
		default:
			return string(v.(json.RawMessage)), nil
		}

	case big.Int:
		// Big integers - convert to string for all databases to avoid overflow
		bigInt := v.(big.Int)
		return bigInt.String(), nil

		// Or if you're using pointer to big.Int:
	case *big.Int:
		// Big integers - convert to string for all databases to avoid overflow
		return v.(*big.Int).String(), nil

	case decimal.Decimal:
		// Decimal types (assuming github.com/shopspring/decimal)
		// Convert to string for all databases to preserve precision
		return v.(decimal.Decimal).String(), nil

	case nil:
		// Return nil as is for all databases
		return nil, nil

	default:
		// Pass through all other types unchanged
		return v, nil
	}
}

// DBDriverGenerateLimitOffsetClause generates the appropriate LIMIT/OFFSET clause for each database type

func DBDriverGenerateLimitOffsetClause(driverName string, limitAsInt64, offsetAsInt64 int64, hasLimit bool, currentOrderBy string, orderbyFieldNameDirections FieldsOrderBy) (string, string, error) {
	effectiveLimitOffsetClause := ""
	effectiveOrderBy := currentOrderBy

	switch driverName {
	case "sqlserver":
		// SQL Server (2012+) uses OFFSET-FETCH for pagination
		if orderbyFieldNameDirections == nil || len(orderbyFieldNameDirections) == 0 {
			// SQL Server requires ORDER BY for OFFSET-FETCH
			// Add a default ORDER BY if none exists
			effectiveOrderBy = " order by (select null)"
		}

		// Always include OFFSET clause
		effectiveLimitOffsetClause = " offset " + strconv.FormatInt(offsetAsInt64, 10) + " rows"

		if hasLimit && limitAsInt64 > 0 {
			effectiveLimitOffsetClause += " fetch next " + strconv.FormatInt(limitAsInt64, 10) + " rows only"
		}

	case "postgres":
		// PostgreSQL uses LIMIT and OFFSET clauses after ORDER BY
		if hasLimit && limitAsInt64 > 0 {
			effectiveLimitOffsetClause = " limit " + strconv.FormatInt(limitAsInt64, 10)
		}

		// Always include OFFSET clause
		effectiveLimitOffsetClause += " offset " + strconv.FormatInt(offsetAsInt64, 10)

	case "oracle":
		// Oracle 12c+ uses OFFSET n ROWS FETCH NEXT m ROWS ONLY
		// Always include OFFSET clause
		effectiveLimitOffsetClause = " offset " + strconv.FormatInt(offsetAsInt64, 10) + " rows"

		if hasLimit && limitAsInt64 > 0 {
			effectiveLimitOffsetClause += " fetch next " + strconv.FormatInt(limitAsInt64, 10) + " rows only"
		}

	case "mysql", "mariadb":
		// MySQL/MariaDB use LIMIT clause with optional OFFSET
		if hasLimit && limitAsInt64 > 0 {
			effectiveLimitOffsetClause = " limit " + strconv.FormatInt(limitAsInt64, 10)
		} else {
			// MySQL/MariaDB require a limit when using offset
			effectiveLimitOffsetClause = " limit 18446744073709551615" // Max value for MySQL
		}

		// Always include OFFSET clause
		effectiveLimitOffsetClause += " offset " + strconv.FormatInt(offsetAsInt64, 10)

	default:
		return "", "", errors.New("UNKNOWN_DATABASE_TYPE:" + driverName)
	}

	return effectiveLimitOffsetClause, effectiveOrderBy, nil
}

func DeformatIdentifier(identifier string, driverName string) string {
	// Remove the quotes from the identifier
	deformattedIdentifier := strings.Trim(identifier, ""
	")
	deformattedIdentifier = strings.ToLower(deformattedIdentifier)
	return deformattedIdentifier
}

func DeformatKeys(kv map[string]interface{}, driverName string, fieldTypeMapping FieldTypeMapping) (r map[string]interface{}, err error) {
	r = map[string]interface{}{}
	for k, v := range kv {
		newKey := DeformatIdentifier(k, driverName)
		if fieldTypeMapping != nil {
			fieldValueType, isExist := fieldTypeMapping[newKey]
			if isExist {
				switch fieldValueType {
				case "array-string":
					v, err = utils.GetArrayFromV(v)
					if err != nil {
						return nil, err
					}
				case "json":
					v, err = utils.GetJSONFromV(v)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		r[newKey] = v
	}
	return r, nil
}

// SQLPartWhereAndFieldNameValues generates WHERE clause conditions for different database types
//
// Parameters:
//   - whereKeyValues: JSON object mapping field names to values for filtering
//   - driverName: Database driver name for proper identifier formatting
//
// Returns:
//   - A properly formatted WHERE clause string (without the "WHERE" keyword)
//
// The function handles different value types:
//   - nil values are converted to "IS NULL" conditions
//   - SQLExpression values are converted to their string representation
//   - Regular values use parameterized queries with "=" operator
//
// All conditions are joined with "AND" operators
func SQLPartWhereAndFieldNameValues(whereKeyValues utils.JSON, driverName string) string {
	if len(whereKeyValues) == 0 {
		return ""
	}

	var conditions []string

	for k, v := range whereKeyValues {
		// Format the field name according to database requirements
		k = DbDriverFormatIdentifier(driverName, k)

		var condition string
		if v == nil {
			// Handle NULL values according to SQL standard (works in all databases)
			condition = k + " IS NULL"
		} else {
			switch v := v.(type) {
			case sql_expression.SQLExpression:
				// Handle custom SQL expressions
				condition = v.String()
			default:
				// Handle regular equality conditions
				condition = k + "=:" + k
			}
		}
		conditions = append(conditions, condition)
	}

	// Join all conditions with AND
	return strings.Join(conditions, " AND ")
}

// Define error codes as constants for readability
const (
	// PostgreSQL connection error class
	pgConnectionErrorClass = "08" // Class 08 - Connection Exception
)

var (
	// Oracle connection error codes
	oracleConnectionErrors = map[int]bool{
		3113:  true, // End-of-file on communication channel
		3114:  true, // Not connected to Oracle
		12170: true, // TNS connect timeout
		12541: true, // No listener
		12543: true, // TNS destination host unreachable
		12571: true, // TNS packet writer failure
	}

	// MariaDB/MySQL connection error codes
	mariadbConnectionErrors = map[uint16]bool{
		1042: true, // Can't get hostname
		1043: true, // Bad handshake
		1044: true, // Access denied
		1045: true, // Access denied
		1047: true, // Connection refused
		1129: true, // Host blocked
		1130: true, // Not allowed to connect
		2002: true, // Can't connect
		2003: true, // Can't connect
		2004: true, // Can't create TCP/IP socket
		2005: true, // Unknown host
		2006: true, // Server gone away
	}

	// Generic connection error patterns for SQL Server and others
	genericConnectionErrorPatterns = []string{
		"connection reset",
		"connection refused",
		"connection closed",
		"network error",
		"dial tcp",
		"broken pipe",
		"no connection",
		"connection timed out",
		"timeout expired",
		"net/http: request canceled",
		"read: connection reset by peer",
		"write: broken pipe",
	}
)

// IsConnectionError checks if the error is a database connection error
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check vendor-specific error types
	switch e := err.(type) {
	case *go_ora.OracleError: // Oracle
		return oracleConnectionErrors[e.ErrCode]

	case *pq.Error: // PostgreSQL
		return e.Code.Class() == pgConnectionErrorClass

	case *mysql.MySQLError: // MariaDB/MySQL
		return mariadbConnectionErrors[e.Number]
	}

	// Generic check for all database drivers based on error message
	msg := strings.ToLower(err.Error())
	for _, pattern := range genericConnectionErrorPatterns {
		if strings.Contains(msg, pattern) {
			return true
		}
	}

	return false
}
