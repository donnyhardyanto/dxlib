package utils

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// IsValidIdentifier checks if a string is a valid SQL identifier (alphanumeric + underscore, optionally with dot for table.field)
func IsValidIdentifier(s string) bool {
	if s == "" {
		return false
	}
	// Allow table.field format
	for i, c := range s {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
		} else {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
				return false
			}
		}
	}
	return true
}

// QuoteIdentifierByDbType quotes a SQL identifier based on database type to prevent SQL injection
func QuoteIdentifierByDbType(dbType base.DXDatabaseType, identifier string) string {
	switch dbType {
	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses [identifier] - escape ] as ]]
		return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
	case base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypeMariaDB, base.DXDatabaseTypeOracle:
		// PostgreSQL, MariaDB, Oracle use "identifier" - escape " as ""
		return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
	default:
		// PostgreSQL style as fallback
		return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
	}
}

// QuoteFieldWithPrefixByDbType quotes a field that may have table prefix (e.g., "t.field_name" -> "t"."field_name")
func QuoteFieldWithPrefixByDbType(dbType base.DXDatabaseType, field string) string {
	parts := strings.SplitN(field, ".", 2)
	if len(parts) == 2 {
		return QuoteIdentifierByDbType(dbType, parts[0]) + "." + QuoteIdentifierByDbType(dbType, parts[1])
	}
	return QuoteIdentifierByDbType(dbType, field)
}

// === SQL Utility Functions ===

// SQLBuildWhereInClauseStrings builds a WHERE IN clause for string values
func SQLBuildWhereInClauseStrings(fieldName string, values []string) string {
	l := len(values)
	if l == 0 {
		return ""
	}
	quotedValues := make([]string, l)
	for i, v := range values {
		quotedValues[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	}
	if l == 1 {
		return fieldName + " = " + quotedValues[0]
	}
	return "(" + fieldName + " IN (" + strings.Join(quotedValues, ",") + "))"
}

// SQLBuildWhereInClauseInt64 builds a WHERE IN clause for int64 values
func SQLBuildWhereInClauseInt64(fieldName string, values []int64) string {
	l := len(values)
	if l == 0 {
		return ""
	}
	valueStrings := make([]string, l)
	for i, v := range values {
		valueStrings[i] = fmt.Sprintf("%d", v)
	}
	if l == 1 {
		return fieldName + " = " + valueStrings[0]
	}
	return fieldName + " IN (" + strings.Join(valueStrings, ",") + ")"
}
