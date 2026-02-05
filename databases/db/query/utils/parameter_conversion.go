package utils

import (
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// ParameterizedSQLQueryNamedBasedToIndexBased converts a SQL query with named parameters (:param_name)
// to index-based positional parameters ($1, $2 for PostgreSQL, ? for MariaDB, etc.)
// and returns the converted query string along with an ordered slice of argument values.
//
// Example:
//
//	query: "SELECT * FROM users WHERE name = :name AND age > :age"
//	namedArgs: {"name": "John", "age": 25}
//
//	For PostgreSQL returns:
//	  query: "SELECT * FROM users WHERE name = $1 AND age > $2"
//	  args: ["John", 25]
//
//	For MariaDB returns:
//	  query: "SELECT * FROM users WHERE name = ? AND age > ?"
//	  args: ["John", 25]
func ParameterizedSQLQueryNamedBasedToIndexBased(dbType base.DXDatabaseType, query string, namedArgs utils.JSON) (string, []any, error) {
	if namedArgs == nil {
		namedArgs = utils.JSON{}
	}

	var result strings.Builder
	var args []any
	paramIndex := 0
	i := 0

	for i < len(query) {
		// Check for named parameter starting with :
		if query[i] == ':' {
			// Check if this is a valid parameter start (not ::, which is PostgreSQL cast operator)
			if i+1 < len(query) && query[i+1] == ':' {
				// This is :: (PostgreSQL cast), keep it as is
				result.WriteString("::")
				i += 2
				continue
			}

			// Extract parameter name
			paramStart := i + 1
			paramEnd := paramStart
			for paramEnd < len(query) {
				c := query[paramEnd]
				if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
					(c >= '0' && c <= '9') || c == '_' {
					paramEnd++
				} else {
					break
				}
			}

			if paramEnd > paramStart {
				paramName := query[paramStart:paramEnd]

				// Look up value in namedArgs
				value, exists := namedArgs[paramName]
				if !exists {
					return "", nil, errors.Errorf("NAMED_PARAMETER_NOT_FOUND:%s", paramName)
				}

				// Write the appropriate positional placeholder
				paramIndex++
				switch dbType {
				case base.DXDatabaseTypePostgreSQL:
					result.WriteString("$" + strconv.Itoa(paramIndex))
				case base.DXDatabaseTypeSQLServer:
					result.WriteString("@p" + strconv.Itoa(paramIndex))
				case base.DXDatabaseTypeMariaDB:
					result.WriteString("?")
				case base.DXDatabaseTypeOracle:
					result.WriteString(":" + strconv.Itoa(paramIndex))
				default:
					// Default to PostgreSQL style
					result.WriteString("$" + strconv.Itoa(paramIndex))
				}

				args = append(args, value)
				i = paramEnd
			} else {
				// Just a lone colon, keep it
				result.WriteByte(query[i])
				i++
			}
		} else if query[i] == '\'' {
			// Skip string literals to avoid matching :param inside strings
			result.WriteByte(query[i])
			i++
			for i < len(query) {
				result.WriteByte(query[i])
				if query[i] == '\'' {
					// Check for escaped quote ''
					if i+1 < len(query) && query[i+1] == '\'' {
						result.WriteByte(query[i+1])
						i += 2
					} else {
						i++
						break
					}
				} else {
					i++
				}
			}
		} else {
			result.WriteByte(query[i])
			i++
		}
	}

	return result.String(), args, nil
}
