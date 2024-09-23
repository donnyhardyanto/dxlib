package utils

import (
	"database/sql"
	"strings"
)

func FormatIdentifier(identifier string, driverName string) string {
	// Convert the identifier to lowercase as the base case
	formattedIdentifier := strings.ToLower(identifier)

	// Apply database-specific formatting
	switch driverName {
	case "oracle", "db2":
		formattedIdentifier = strings.ToUpper(formattedIdentifier)
		return formattedIdentifier
	}

	// Wrap the identifier in quotes to preserve case in the SQL statement
	return `"` + formattedIdentifier + `"`
}

func DeformatIdentifier(identifier string, driverName string) string {
	// Remove the quotes from the identifier
	deformattedIdentifier := strings.Trim(identifier, `"`)
	deformattedIdentifier = strings.ToLower(deformattedIdentifier)
	return deformattedIdentifier
}

func DeformatKeys(kv map[string]interface{}, driverName string) (r map[string]interface{}) {
	r = map[string]interface{}{}
	for k, v := range kv {
		r[DeformatIdentifier(k, driverName)] = v
	}
	return r
}

func PrepareArrayArgs(keyValues map[string]any, driverName string) (fieldNames string, fieldValues string, fieldArgs []any) {
	for k, v := range keyValues {
		if fieldNames != "" {
			fieldNames += ", "
			fieldValues += ", "
		}

		fieldName := FormatIdentifier(k, driverName)
		fieldNames += fieldName
		fieldValues += ":" + fieldName

		var s sql.NamedArg
		switch v.(type) {
		case bool:
			switch driverName {
			case "oracle", "sqlserver":
				if v.(bool) == true {
					keyValues[k] = 1
				} else {
					keyValues[k] = 0
				}

			default:
			}

		default:
		}
		s = sql.Named(fieldName, keyValues[k])
		fieldArgs = append(fieldArgs, s)
	}

	return fieldNames, fieldValues, fieldArgs
}
