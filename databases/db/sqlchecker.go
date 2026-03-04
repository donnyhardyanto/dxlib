package db

import (
	"regexp"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
)

// Regular expressions for unquoted identifiers by databases type
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

	// BUG-BE-174 FIX: pre-compiled at init instead of per-call
	suspiciousQueryPatterns = func() []*regexp.Regexp {
		patterns := []string{
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
		compiled := make([]*regexp.Regexp, len(patterns))
		for i, p := range patterns {
			compiled[i] = regexp.MustCompile(p)
		}
		return compiled
	}()

	// Maximum identifier lengths per dialect
	maxIdentifierLengths = map[base.DXDatabaseType]int{
		base.DXDatabaseTypePostgreSQL: 63,
		base.DXDatabaseTypeSQLServer:  128,
		base.DXDatabaseTypeOracle:     128,
		base.DXDatabaseTypeMariaDB:    64,
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

// CheckIdentifier validates table and column names according to databases-specific rules
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
			return errors.Errorf("unknown databases dialect: %s", dialect)
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

func checkSuspiciousQueryPatterns(value string, ignoreInComments bool) error {
	lowered := strings.ToLower(value)

	// First, check if the value is within a comment
	if ignoreInComments && (strings.Contains(lowered, "/*") || strings.Contains(lowered, "*/") || strings.Contains(lowered, "--")) {
		return nil
	}

	for _, re := range suspiciousQueryPatterns {
		if re.MatchString(lowered) {
			return errors.Errorf("suspicious pattern detected: %s", re.String())
		}
	}
	return nil
}
