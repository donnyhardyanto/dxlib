package db

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/shopspring/decimal"
)

var AllowRisk = false

// Common SQL injection patterns

// Regular expressions for unquoted identifiers by database type
var (
	identifierPatterns = map[base.DXDatabaseType]*regexp.Regexp{
		base.DXDatabaseTypePostgreSQL: regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$"),
		base.DXDatabaseTypeMariaDB:    regexp.MustCompile("^[a-zA-Z0-9_$]+$"),
		base.DXDatabaseTypeSQLServer:  regexp.MustCompile("^[a-zA-Z@#_][a-zA-Z0-9@#_$]*$"),
		base.DXDatabaseTypeOracle:     regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_$#]*$"),
	}

	// QuoteCharacters defines the start and end quote characters for different databases
	QuoteCharacters = map[base.DXDatabaseType]struct {
		Start []rune
		End   []rune
	}{
		base.DXDatabaseTypePostgreSQL: {
			Start: []rune{'"'},
			End:   []rune{'"'},
		},
		base.DXDatabaseTypeMariaDB: {
			Start: []rune{'"'},
			End:   []rune{'"'},
		},
		base.DXDatabaseTypeSQLServer: {
			Start: []rune{'[', '"'},
			End:   []rune{']', '"'},
		},
		base.DXDatabaseTypeOracle: {
			Start: []rune{'"'},
			End:   []rune{'"'},
		},
	}

	// Common SQL keywords across most dialects
	commonKeywords = map[string]bool{
		"SELECT": true, "FROM": true, "WHERE": true, "INSERT": true,
		"UPDATE": true, "DELETE": true, "DROP": true, "CREATE": true,
		"TABLE": true, "INDEX": true, "ALTER": true, "ADD": true,
		"COLUMN": true, "ORDER": true, "BY": true, "GROUP": true,
		"HAVING": true, "JOIN": true, "INNER": true, "OUTER": true,
		"LEFT": true, "RIGHT": true, "FULL": true, "ON": true,
		"AS": true, "DISTINCT": true, "CASE": true, "WHEN": true,
		"THEN": true, "ELSE": true, "END": true, "AND": true,
		"OR": true, "NOT": true, "IN": true, "BETWEEN": true,
		"LIKE": true, "IS": true, "NULL": true, "TRUE": true,
		"FALSE": true, "DESC": true, "ASC": true, "LIMIT": true,
		"OFFSET": true, "WITH": true, "VALUES": true, "INTO": true,
		"PROCEDURE": true, "FUNCTION": true, "TRIGGER": true,
		"VIEW": true, "SEQUENCE": true, "GRANT": true, "REVOKE": true,
		"USER": true, "ROLE": true, "DATABASE": true, "SCHEMA": true,
	}

	// Suspicious patterns that might indicate SQL injection
	suspiciousRegexQueryPatterns = []string{
		`--`, `\/\*`, `\*\/`, `; `,
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

	// Maximum identifier lengths per dialect
	maxIdentifierLengths = map[base.DXDatabaseType]int{
		base.DXDatabaseTypePostgreSQL: 63,
		base.DXDatabaseTypeSQLServer:  128,
		base.DXDatabaseTypeOracle:     128,
		base.DXDatabaseTypeMariaDB:    64,
	}

	// Valid operators for each dialect
	validOperators = map[base.DXDatabaseType]map[string]bool{
		base.DXDatabaseTypePostgreSQL: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "ilike": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		base.DXDatabaseTypeMariaDB: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		base.DXDatabaseTypeSQLServer: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		base.DXDatabaseTypeOracle: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
	}
)

// there isReservedKeyword checks if an identifier is a reserved keyword in the specific dialect
func isReservedKeyword(dialect base.DXDatabaseType, word string) bool {
	// Convert to uppercase for case-insensitive comparison
	upperWord := strings.ToUpper(word)

	var c map[string]bool
	// Add dialect-specific keywords
	switch dialect {
	case base.DXDatabaseTypePostgreSQL:
		c = postgresKeywords
	case base.DXDatabaseTypeMariaDB:
		c = mysqlKeywords
	case base.DXDatabaseTypeSQLServer:
		c = sqlServerKeywords
	case base.DXDatabaseTypeOracle:
		c = oracleKeywords
	default:
		panic("unhandled default case")
	}

	if v, ok := c[upperWord]; ok {
		return v
	}

	return false
}

func isKeywordCanBeUseAsFieldNameDirectly(dialect base.DXDatabaseType, word string) bool {
	// Convert to uppercase for case-insensitive comparison
	upperWord := strings.ToUpper(word)

	// Add dialect-specific keywords
	var c map[string]bool
	switch dialect {
	case base.DXDatabaseTypePostgreSQL:
		c = postgresKeywordsCanBeUsedAsFieldNameDirectly
	case base.DXDatabaseTypeMariaDB:
		c = mariadbKeywordsCanBeUsedAsFieldNameDirectly
	case base.DXDatabaseTypeSQLServer:
		c = sqlServerKeywordsCanBeUsedAsFieldNameDirectly
	case base.DXDatabaseTypeOracle:
		c = oracleKeywordsCanBeUsedAsFieldNameDirectly
	default:
		panic("unhandled default case")
	}

	if v, ok := c[upperWord]; ok {
		return v
	}

	return false
}

// CheckIdentifier validates table and column names according to database-specific rules
func CheckIdentifier(dialect base.DXDatabaseType, identifier string) error {
	if identifier == "" {
		return errors.Errorf("identifier cannot be empty")
	}

	// Check for quoted identifiers
	isQuoted := false
	quoteType := -1

	if len(identifier) >= 2 {
		quoteChars := QuoteCharacters[dialect]
		for i, startChar := range quoteChars.Start {
			endChar := quoteChars.End[i]
			if rune(identifier[0]) == startChar && rune(identifier[len(identifier)-1]) == endChar {
				isQuoted = true
				quoteType = i
				break
			}
		}
	}

	if isQuoted {
		// Extract content without quotes
		content := identifier[1 : len(identifier)-1]

		// For quoted identifiers, mainly check length and basic sanity
		if content == "" {
			return errors.Errorf("empty quoted identifier")
		}

		// Check length
		if maxLen := maxIdentifierLengths[dialect]; len(content) > maxLen {
			return errors.Errorf("quoted identifier %q exceeds maximum length of %d for dialect %s",
				identifier, maxLen, dialect)
		}

		// Check for quote doubling (escaping) in the content
		quoteChar := QuoteCharacters[dialect].Start[quoteType]
		if strings.Count(content, string(quoteChar)) > 0 {
			// In SQL, quotes within quoted identifiers must be doubled
			// e.g., "column" "name" is valid and represents: column"name
			// Verify this pattern
			if !strings.Contains(content, string(quoteChar)+string(quoteChar)) {
				return errors.Errorf("invalid quote character in identifier without proper escaping")
			}
		}

		// Still check for suspicious patterns, as even quoted identifiers shouldn't contain SQL injection
		if err := checkSuspiciousQueryPatterns(content, false); err != nil {
			return errors.Errorf("potentially dangerous quoted identifier: %+v", err)
		}

		return nil
	}

	// Handle qualified names (e.g., schema.table.column) for unquoted identifiers
	parts := strings.Split(identifier, ".")
	for _, part := range parts {
		if part == "" {
			return errors.Errorf("empty part in identifier %q", identifier)
		}

		// Get the appropriate pattern for this dialect
		pattern, exists := identifierPatterns[dialect]
		if !exists {
			return errors.Errorf("unknown database dialect: %s", dialect)
		}

		// Check pattern for unquoted identifiers
		if !pattern.MatchString(part) {
			return errors.Errorf("invalid identifier format for %s: %s", dialect, part)
		}

		// Check if dentifier is a reserved keyword
		if isReservedKeyword(dialect, part) {
			if !isKeywordCanBeUseAsFieldNameDirectly(dialect, part) {
				return errors.Errorf("identifier %q is a reserved keyword in %s", part, dialect)
			}
		}

		// Check length
		if maxLen := maxIdentifierLengths[dialect]; len(part) > maxLen {
			return errors.Errorf("identifier %q exceeds maximum length of %d for dialect %s",
				part, maxLen, dialect)
		}
	}

	return nil
}

// CheckOperator validates SQL operators
func CheckOperator(dialect base.DXDatabaseType, operator string) error {
	op := strings.ToLower(strings.TrimSpace(operator))
	if ops, ok := validOperators[dialect]; ok {
		if !ops[op] {
			return errors.Errorf("operator %q not supported for dialect %s", operator, dialect)
		}
	}
	return nil
}

// CheckValue validates a value for SQL injection
func CheckValue(dialect base.DXDatabaseType, value any) error {
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
			if err := CheckValue(dialect, item); err != nil {
				return err
			}
		}
	case []string:
		for _, item := range v {
			if err := CheckValue(dialect, item); err != nil {
				return err
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
			if err := CheckIdentifier(dialect, key); err != nil {
				return err
			}
			if err := CheckValue(dialect, val); err != nil {
				return err
			}
		}
	case time.Time:
		return nil
	case decimal.Decimal:
		return nil
	default:
		return nil
		//return errors.Errorf("unsupported value type: %T", value)
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
			return err
		}

		// Check wildcard count
		if strings.Count(pattern, "%") > 5 {
			return errors.Errorf("too many wildcards in LIKE pattern")
		}
	}

	return nil
}

// CheckOrderBy validates ORDER BY expressions
func CheckOrderBy(dialect base.DXDatabaseType, expr string) error {
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
		if err := CheckIdentifier(dialect, tokens[0]); err != nil {
			return errors.Wrap(err, fmt.Sprintf("invalid field in order by: ", err.Error()))
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

func CheckOrderByDirection(dialect base.DXDatabaseType, direction string) error {
	if direction == "" {
		return errors.New("empty order by expression")
	}

	// Normalize: trim and uppercase, collapse whitespace
	normalizedDirection := strings.Join(strings.Fields(strings.ToUpper(direction)), " ")

	// Check basic directions (all databases support)
	switch normalizedDirection {
	case "ASC", "DESC":
		return nil

	case "ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST":
		// Only PostgreSQL and Oracle support NULLS syntax
		switch dialect {
		case base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypeOracle:
			return nil
		case base.DXDatabaseTypeMariaDB:
			return errors.Errorf("MariaDB/MySQL does not support '%s' syntax", normalizedDirection)
		case base.DXDatabaseTypeSQLServer:
			return errors.Errorf("SQL Server does not support '%s' syntax", normalizedDirection)
		default:
			return errors.Errorf("unsupported database type for '%s'", normalizedDirection)
		}
	default:
	}

	return errors.Errorf("invalid sort direction: %s", direction)
}

// CheckBaseQuery validates the base query for suspicious patterns
func CheckBaseQuery(dialect base.DXDatabaseType, query string) error {
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
		return errors.Errorf("query validation failed: %+v", err)
	}

	return nil
}

// Internal helper functions

func checkStringValue(value string) error {
	/*lowered := strings.ToLower(value)

	  // Check for suspicious patterns
	  for _, pattern := range suspiciousValuePatterns {
	  	if strings.Contains(lowered, pattern) {
	  		return errors.Errorf("suspicious pattern (%s) detected in value: %s", pattern, value)
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

func CheckAll(dialect base.DXDatabaseType, query string, arg any) (err error) {
	if AllowRisk {
		return nil
	}
	err = CheckBaseQuery(dialect, query)
	if err != nil {
		return errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v=%s +%v", err, query, arg)
	}

	err = CheckValue(dialect, arg)
	if err != nil {
		return errors.Errorf("SQL_INJECTION_DETECTED:VALUE_VALIDATION_FAILED: %+v", err)
	}

	// Check LIKE patterns
	if strings.Contains(query, "LIKE") {
		err = CheckLikePattern(query)
		if err != nil {
			return errors.Errorf("SQL_INJECTION_DETECTED:LIKE_PATTERN_VALIDATION_FAILED: %+v", err)
		}
	}

	// Check ORDER BY expressions
	if strings.Contains(query, "ORDER BY") {
		err = CheckOrderBy(dialect, query)
		if err != nil {
			return errors.Errorf("SQL_INJECTION_DETECTED:ORDER_BY_VALIDATION_FAILED: %+v", err)
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

		// Validate a direction if provided
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

// ValidateAndSanitizeOrderByExampleUsage Example usage in handler
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
