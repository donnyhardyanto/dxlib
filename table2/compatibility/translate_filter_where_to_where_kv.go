package compatibility

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/pkg/errors"
)

func TranslateFilterWhereToWhereKV(filterWhereAsString string, filterKeyValues map[string]interface{}) (map[string]interface{}, error) {
	if filterWhereAsString == "" && len(filterKeyValues) == 0 {
		return nil, errors.New("both filterWhereAsString and filterKeyValues are empty")
	}

	result := make(map[string]interface{})

	// Add filterKeyValues directly (all are equality checks with direct values)
	for k, v := range filterKeyValues {
		result[k] = v
	}

	// Parse filterWhereAsString if provided
	if filterWhereAsString != "" {
		if err := parseWhereString(filterWhereAsString, filterKeyValues, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func parseWhereString(whereStr string, filterKeyValues map[string]interface{}, result map[string]interface{}) error {
	whereStr = strings.TrimSpace(whereStr)
	if whereStr == "" {
		return nil
	}

	// Split by AND (case insensitive)
	clauses := splitByAND(whereStr)

	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}

		// Try to parse as simple equality: column = :value
		if parsed, ok := parseSimpleEquality(clause); ok {
			placeholderName := strings.TrimPrefix(parsed.placeholder, ":")

			// Get direct value from filterKeyValues
			if value, exists := filterKeyValues[placeholderName]; exists {
				result[parsed.column] = value
			} else {
				return errors.Errorf("placeholder %s not found in filterKeyValues", parsed.placeholder)
			}
			continue
		}

		// Not a simple equality - create SQLExpression
		// Replace placeholders with actual values in the expression
		expression := replacePlaceholders(clause, filterKeyValues)

		// Extract column name (first word before operator)
		parts := strings.Fields(clause)
		if len(parts) == 0 {
			return errors.Errorf("invalid clause: %s", clause)
		}

		columnName := parts[0]
		result[columnName] = db.SQLExpression{Expression: expression}
	}

	return nil
}

func replacePlaceholders(expression string, filterKeyValues map[string]interface{}) string {
	result := expression

	// Replace all :placeholder with actual values
	for key, value := range filterKeyValues {
		placeholder := ":" + key
		if strings.Contains(result, placeholder) {
			// Format value based on type
			var valueStr string
			switch v := value.(type) {
			case string:
				valueStr = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // Escape single quotes
			case nil:
				valueStr = "NULL"
			default:
				valueStr = fmt.Sprintf("%v", v)
			}
			result = strings.ReplaceAll(result, placeholder, valueStr)
		}
	}

	return result
}

type parsedEquality struct {
	column      string
	placeholder string
}

func parseSimpleEquality(clause string) (parsedEquality, bool) {
	clause = strings.TrimSpace(clause)

	// Find the = sign
	eqIndex := strings.Index(clause, "=")
	if eqIndex == -1 {
		return parsedEquality{}, false
	}

	left := strings.TrimSpace(clause[:eqIndex])
	right := strings.TrimSpace(clause[eqIndex+1:])

	// Check if left is a simple column name (no spaces, no operators)
	if strings.ContainsAny(left, " \t\n<>!") {
		return parsedEquality{}, false
	}

	// Check if right is a placeholder (:xxx)
	if !strings.HasPrefix(right, ":") {
		return parsedEquality{}, false
	}

	// Extract placeholder name
	placeholder := strings.TrimPrefix(right, ":")
	placeholder = strings.TrimSpace(placeholder)

	// Make sure it's just the placeholder (no other operators)
	if strings.ContainsAny(placeholder, " \t\n<>=!()") {
		return parsedEquality{}, false
	}

	return parsedEquality{
		column:      left,
		placeholder: ":" + placeholder,
	}, true
}

func splitByAND(whereStr string) []string {
	upperStr := strings.ToUpper(whereStr)
	var result []string
	var current strings.Builder

	i := 0
	for i < len(whereStr) {
		// Look for " AND " pattern
		if i+5 <= len(upperStr) && upperStr[i:i+5] == " AND " {
			result = append(result, current.String())
			current.Reset()
			i += 5
			continue
		}

		current.WriteByte(whereStr[i])
		i++
	}

	// Add last clause
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}
