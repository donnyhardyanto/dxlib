// sqlchecker/checker.go
package sqlchecker

import (
	"fmt"
	"github.com/donnyhardyanto/dxlib/database/database_type"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata"
)

var AllowRisk = false

// Common SQL injection patterns
var (
	// Valid identifier pattern (letters, numbers, underscores, and dots for schema.table.column)
	identifierPattern = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")

	// Suspicious patterns that might indicate SQL injection
	suspiciousRegexQueryPatterns = []string{
		"--", `"\/\*`, `\*\/`, ";",
		`\bunion\b`, `\bdrop\b`,
		`\bexec\b`, `\bexecute\b`, `\btruncate\b`,
		`\bcreate\b`, `\balter\b`, `\bgrant\b`,
		`\brevoke\b`, `\bcommit\b`, `\brollback\b`,
		`\binto outfile\b`, `\binto dumpfile\b`,
		`\bload_file\b`, `\bsleep\b`, `\bbenchmark\b`,
		`\bwaitfor\b`, `\bdelay\b`, `\bsys_eval\b`,
		`\binformation_schema\b`, `\bsysobjects\b`,
		`\bxp_\w*\b`, `\bsp_\w*\b`, `\bdeclare\b`,
		`\b\d+\s*=\s*\d+\b`,
	}

	suspiciousValuePatterns = []string{
		";", "--", "*", "#",
	}

	// Maximum identifier lengths per dialect
	maxIdentifierLengths = map[database_type.DXDatabaseType]int{
		database_type.PostgreSQL: 63,
		database_type.MySQL:      64,
		database_type.SQLServer:  128,
		database_type.Oracle:     128,
	}

	// Valid operators for each dialect
	validOperators = map[database_type.DXDatabaseType]map[string]bool{
		database_type.PostgreSQL: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "ilike": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		database_type.MySQL: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		database_type.SQLServer: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		database_type.Oracle: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
	}
)

// CheckIdentifier validates table and column names
func CheckIdentifier(identifier string, dialect database_type.DXDatabaseType) error {
	if identifier == "" {
		return errors.Errorf("identifier cannot be empty")
	}

	// Handle qualified names (e.g., schema.table.column)
	parts := strings.Split(identifier, ".")
	for _, part := range parts {
		if part == "" {
			return errors.Errorf("empty part in identifier %q", identifier)
		}

		// Check pattern
		if !identifierPattern.MatchString(part) {
			return errors.Errorf("invalid identifier format: %s", part)
		}

		// Check length
		if maxLen := maxIdentifierLengths[dialect]; len(part) > maxLen {
			return errors.Errorf("identifier %q exceeds maximum length of %d for dialect %s", part, maxLen, dialect)
		}

		// Check for suspicious patterns
		if err := checkSuspiciousQueryPatterns(part, false); err != nil {
			return errors.Errorf("invalid identifier %q: %w", part, err)
		}
	}

	return nil
}

// CheckOperator validates SQL operators
func CheckOperator(operator string, dialect database_type.DXDatabaseType) error {
	op := strings.ToLower(strings.TrimSpace(operator))
	if ops, ok := validOperators[dialect]; ok {
		if !ops[op] {
			return errors.Errorf("operator %q not supported for dialect %s", operator, dialect)
		}
	}
	return nil
}

// CheckValue validates a value for SQL injection
func CheckValue(value any) error {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case *string:
		vv := *v
		return checkStringValue(vv)
	case string:
		return checkStringValue(v)
	case []any:
		for _, item := range v {
			if err := CheckValue(item); err != nil {
				return errors.Wrap(err, "error occured")
			}
		}
	case []string:
		for _, item := range v {
			if err := CheckValue(item); err != nil {
				return errors.Wrap(err, "error occured")
			}
		}
	case []uint8, []uint64, []int64, []int32, []int16, []int8, []int, []float64, []float32, []bool:
		return nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		// Numeric and boolean values are safe
		return nil
	case map[string]interface{}:
		// Handle JSONB data type
		for key, val := range v {
			if err := CheckIdentifier(key, database_type.PostgreSQL); err != nil {
				return errors.Wrap(err, "error occured")
			}
			if err := CheckValue(val); err != nil {
				return errors.Wrap(err, "error occured")
			}
		}
	case time.Time:
		return nil
	case decimal.Decimal:
		return nil
	default:
		return nil
		//return errors.Wrap(err, "error occured")ors.Errorf("unsupported value type: %T", value)
	}

	return nil

}

// CheckLikePattern validates LIKE patterns
func CheckLikePattern(query string) error {
	// Convert to lowercase for case-insensitive matching
	loweredQuery := strings.ToLower(query)

	// Find all LIKE or ILIKE clauses
	likePositions := []int{}
	likeKeywords := []string{"like", "ilike"}

	for _, keyword := range likeKeywords {
		currentPos := 0
		for {
			// Find next occurrence starting from currentPos
			foundPos := strings.Index(loweredQuery[currentPos:], keyword)
			if foundPos == -1 {
				break
			}
			// Add the absolute position
			absolutePos := currentPos + foundPos
			likePositions = append(likePositions, absolutePos)
			// Move past this occurrence
			currentPos = absolutePos + len(keyword)
		}
	}

	// For each LIKE/ILIKE found, extract and check its pattern
	for _, pos := range likePositions {
		// Find the next value after LIKE/ILIKE (usually enclosed in quotes)
		remainingQuery := query[pos:]
		quotePos := strings.Index(remainingQuery, "'")
		if quotePos == -1 {
			continue // No pattern found, skip
		}

		// Find the closing quote
		endQuotePos := strings.Index(remainingQuery[quotePos+1:], "'")
		if endQuotePos == -1 {
			continue // Unclosed quote, skip
		}

		// Extract the pattern between quotes
		pattern := remainingQuery[quotePos+1 : quotePos+1+endQuotePos]

		// Check the actual pattern
		if err := checkStringValue(pattern); err != nil {
			return errors.Wrap(err, "error occured")
		}

		// Check wildcard count
		if strings.Count(pattern, "%") > 5 {
			return errors.Errorf("too many wildcards in LIKE pattern")
		}
	}

	return nil
}

// CheckOrderBy validates ORDER BY expressions
func CheckOrderBy(expr string, dialect database_type.DXDatabaseType) error {
	if expr == "" {
		return errors.Errorf("empty order by expression")
	}

	for _, part := range strings.Split(expr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split into field and direction
		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			return errors.Errorf("empty order by part")
		}

		// Check field name
		if err := CheckIdentifier(tokens[0], dialect); err != nil {
			return errors.Errorf("invalid field in order by: %w", err)
		}

		// Check direction if specified
		if len(tokens) > 1 {
			dir := strings.ToUpper(tokens[1])
			if dir != "ASC" && dir != "DESC" {
				return errors.Errorf("invalid sort direction: %s", tokens[1])
			}
		}

		// Check for NULLS FIRST/LAST if present
		if len(tokens) > 2 {
			if tokens[2] != "NULLS" || len(tokens) < 4 || (tokens[3] != "FIRST" && tokens[3] != "LAST") {
				return errors.Errorf("invalid NULLS FIRST/LAST syntax")
			}
		}
	}

	return nil
}

// CheckBaseQuery validates the base query for suspicious patterns
func CheckBaseQuery(query string, dialect database_type.DXDatabaseType) error {
	if query == "" {
		return errors.Errorf("empty query")
	}

	loweredQuery := strings.ToLower(query)

	// Check for multiple statements
	if strings.Count(query, ";") > 0 {
		return errors.Errorf("multiple statements not allowed")
	}

	// Check for suspicious patterns
	if err := checkSuspiciousQueryPatterns(loweredQuery, false); err != nil {
		return errors.Errorf("query validation failed: %w", err)
	}

	return nil
}

// Internal helper functions

func checkStringValue(value string) error {
	/*lowered := strings.ToLower(value)

	// Check for suspicious patterns
	for _, pattern := range suspiciousValuePatterns {
		if strings.Contains(lowered, pattern) {
			return errors.Wrap(err, "error occured")ors.Errorf("suspicious pattern (%s) detected in value: %s", pattern, value)
		}
	}*/
	return nil
}

func checkSuspiciousQueryPatterns(value string, ignoreInComments bool) error {
	lowered := strings.ToLower(value)

	// First, check if the value is within a comment
	if ignoreInComments && (strings.Contains(lowered, "/*") || strings.Contains(lowered, "*/") || strings.Contains(lowered, "--")) {
		return nil
	}

	for _, pattern := range suspiciousRegexQueryPatterns {
		// Use a more specific logic to avoid false positives

		if regexp.MustCompile(pattern).MatchString(lowered) {
			return errors.Errorf("suspicious pattern detected: %s", pattern)
		}

	}
	return nil
}

func CheckAll(dbDriverName string, query string, arg any) (err error) {
	if AllowRisk {
		return nil
	}
	err = CheckBaseQuery(query, database_type.StringToDXDatabaseType(dbDriverName))
	if err != nil {
		return errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w=%s +%v", err, query, arg)
	}

	err = CheckValue(arg)
	if err != nil {
		return errors.Errorf("SQL_INJECTION_DETECTED:VALUE_VALIDATION_FAILED: %w=%s +%v", err, query, arg)
	}

	// Check LIKE patterns
	if strings.Contains(query, "LIKE") {
		err = CheckLikePattern(query)
		if err != nil {
			return errors.Errorf("SQL_INJECTION_DETECTED:LIKE_PATTERN_VALIDATION_FAILED: %w", err)
		}
	}

	// Check ORDER BY expressions
	if strings.Contains(query, "ORDER BY") {
		err = CheckOrderBy(query, database_type.StringToDXDatabaseType(dbDriverName))
		if err != nil {
			return errors.Errorf("SQL_INJECTION_DETECTED:ORDER_BY_VALIDATION_FAILED: %w", err)
		}
	}

	return nil
}

// ValidateAndSanitizeOrderBy validates and sanitizes the order by clause
func ValidateAndSanitizeOrderBy(orderBy string) (string, error) {
	if strings.TrimSpace(orderBy) == "" {
		return "id ASC", nil // Default order
	}

	// Allowed field names - add your fields here
	allowedFields := map[string]bool{
		"id":         true,
		"code":       true,
		"name":       true,
		"created_at": true,
		"updated_at": true,
		// Add other allowed fields here
	}

	// Split by comma and validate each part
	parts := strings.Split(orderBy, ",")
	var sanitizedParts []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split into field and direction
		components := strings.Fields(part)
		if len(components) == 0 || len(components) > 2 {
			return "", errors.Errorf("invalid order by format: %s", part)
		}

		// Validate field name (only allow alphanumeric and underscore)
		field := strings.ToLower(components[0])
		if !allowedFields[field] {
			return "", errors.Errorf("invalid field name: %s", field)
		}

		// Validate direction if provided
		direction := "ASC" // default direction
		if len(components) == 2 {
			dir := strings.ToUpper(components[1])
			if dir != "ASC" && dir != "DESC" {
				return "", errors.Errorf("invalid sort direction: %s", components[1])
			}
			direction = dir
		}

		sanitizedParts = append(sanitizedParts, fmt.Sprintf("%s %s", field, direction))
	}

	if len(sanitizedParts) == 0 {
		return "id ASC", nil
	}

	return strings.Join(sanitizedParts, ", "), nil
}

// Example usage in handler
func ValidateAndSanitizeOrderByExampleUsage() {
	// Valid examples
	examples := []string{
		"id ASC",
		"name DESC, created_at ASC",
		"code asc, id desc",
		"updated_at", // Will use default ASC
		"",           // Will use default "id ASC"
	}

	for _, example := range examples {
		result, err := ValidateAndSanitizeOrderBy(example)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fmt.Printf("Input: %s -> Sanitized: %s\n", example, result)
	}

	// Invalid examples that will be rejected
	invalidExamples := []string{
		"id ASC; DROP TABLE users",
		"name' OR '1'='1",
		"id) UNION SELECT",
		"unknown_field ASC",
		"id ASCENDING", // Invalid direction
		"id ASC DESC",  // Too many directions
		"id, , name",   // Empty part
	}

	for _, example := range invalidExamples {
		_, err := ValidateAndSanitizeOrderBy(example)
		fmt.Printf("Invalid input '%s': %v\n", example, err)
	}
}
