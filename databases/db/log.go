package db

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// sensitiveFieldPatterns contains patterns that indicate sensitive data
var sensitiveFieldPatterns = []string{
	"password",
	"passwd",
	"secret",
	"token",
	"key",
	"credential",
	"pin",
	"auth",
	"private",
	"apikey",
	"api_key",
	"access_token",
	"refresh_token",
	"otp",
	"cvv",
	"ssn",
	"credit_card",
	"card_number",
}

// maskedValue is the string used to replace sensitive values
const maskedValue = "***MASKED***"

// isSensitiveField checks if a field name indicates sensitive data
func isSensitiveField(fieldName string) bool {
	lowerField := strings.ToLower(fieldName)
	for _, pattern := range sensitiveFieldPatterns {
		if strings.Contains(lowerField, pattern) {
			return true
		}
	}
	return false
}

// maskSensitiveArgs creates a copy of arguments with sensitive values masked
func maskSensitiveArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}

	masked := make([]any, len(args))
	copy(masked, args)
	return masked
}

// maskSensitiveJSON creates a copy of JSON with sensitive values masked
func maskSensitiveJSON(data utils.JSON) utils.JSON {
	if data == nil {
		return nil
	}

	masked := utils.JSON{}
	for k, v := range data {
		if isSensitiveField(k) {
			masked[k] = maskedValue
		} else {
			masked[k] = v
		}
	}
	return masked
}

// formatArgsForLog formats arguments for logging, truncating long values
func formatArgsForLog(args []any) string {
	if len(args) == 0 {
		return "[]"
	}

	parts := make([]string, len(args))
	for i, arg := range args {
		argStr := fmt.Sprintf("%v", arg)
		if len(argStr) > 100 {
			argStr = argStr[:100] + "...(truncated)"
		}
		parts[i] = argStr
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// formatJSONForLog formats JSON for logging, masking sensitive fields
func formatJSONForLog(data utils.JSON) string {
	if len(data) == 0 {
		return "{}"
	}

	masked := maskSensitiveJSON(data)
	parts := make([]string, 0, len(masked))
	for k, v := range masked {
		valStr := fmt.Sprintf("%v", v)
		if len(valStr) > 100 {
			valStr = valStr[:100] + "...(truncated)"
		}
		parts = append(parts, fmt.Sprintf("%s=%v", k, valStr))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// LogDBOperation logs a databases operation with SQL and arguments
func LogDBOperation(operation, sqlStatement string, args []any, err error) {
	maskedArgs := maskSensitiveArgs(args)
	argsStr := formatArgsForLog(maskedArgs)

	if err != nil {
		log.Log.Errorf(err, "DB_%s_ERROR sql=%s args=%s", operation, sqlStatement, argsStr)
	} else {
		log.Log.Debugf("DB_%s sql=%s args=%s", operation, sqlStatement, argsStr)
	}
}

// LogDBOperationWithJSON logs a databases operation with SQL and JSON arguments
func LogDBOperationWithJSON(operation, sqlStatement string, data utils.JSON, err error) {
	dataStr := formatJSONForLog(data)

	if err != nil {
		log.Log.Errorf(err, "DB_%s_ERROR sql=%s data=%s", operation, sqlStatement, dataStr)
	} else {
		log.Log.Debugf("DB_%s sql=%s data=%s", operation, sqlStatement, dataStr)
	}
}

// LogDBInsert logs an INSERT operation
func LogDBInsert(tableName string, data utils.JSON, err error) {
	dataStr := formatJSONForLog(data)
	if err != nil {
		log.Log.Errorf(err, "DB_INSERT_ERROR table=%s data=%s", tableName, dataStr)
	} else {
		log.Log.Debugf("DB_INSERT table=%s data=%s", tableName, dataStr)
	}
}

// LogDBUpdate logs an UPDATE operation
func LogDBUpdate(tableName string, setData, whereData utils.JSON, err error) {
	setStr := formatJSONForLog(setData)
	whereStr := formatJSONForLog(whereData)
	if err != nil {
		log.Log.Errorf(err, "DB_UPDATE_ERROR table=%s set=%s where=%s", tableName, setStr, whereStr)
	} else {
		log.Log.Debugf("DB_UPDATE table=%s set=%s where=%s", tableName, setStr, whereStr)
	}
}

// LogDBDelete logs a DELETE operation
func LogDBDelete(tableName string, whereData utils.JSON, err error) {
	whereStr := formatJSONForLog(whereData)
	if err != nil {
		log.Log.Errorf(err, "DB_DELETE_ERROR table=%s where=%s", tableName, whereStr)
	} else {
		log.Log.Debugf("DB_DELETE table=%s where=%s", tableName, whereStr)
	}
}
