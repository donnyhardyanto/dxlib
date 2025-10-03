package compatibility

import (
	"strings"

	"github.com/pkg/errors"
)

func TranslateFilterOrderByToOrderByKV(filterOrderBy string) (map[string]string, error) {
	if filterOrderBy == "" {
		return nil, errors.New("empty order by expression")
	}

	result := make(map[string]string)

	// Split by comma for multiple columns
	columns := strings.Split(filterOrderBy, ",")

	for _, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}

		// Parse each column's order specification
		columnName, direction, err := parseOrderByColumn(column)
		if err != nil {
			return nil, err
		}

		result[columnName] = direction
	}

	if len(result) == 0 {
		return nil, errors.New("no valid order by columns found")
	}

	return result, nil
}

func parseOrderByColumn(columnSpec string) (string, string, error) {
	// Normalize whitespace
	columnSpec = strings.Join(strings.Fields(columnSpec), " ")
	columnSpec = strings.TrimSpace(columnSpec)

	if columnSpec == "" {
		return "", "", errors.New("empty column specification")
	}

	parts := strings.Fields(columnSpec)

	// Case 1: Just column name (default to ASC)
	if len(parts) == 1 {
		return parts[0], "ASC", nil
	}

	columnName := parts[0]
	remaining := strings.Join(parts[1:], " ")
	remainingUpper := strings.ToUpper(remaining)

	// Case 2: column ASC or column DESC
	if remainingUpper == "ASC" || remainingUpper == "DESC" {
		return columnName, remainingUpper, nil
	}

	// Case 3: column ASC NULLS FIRST/LAST or column DESC NULLS FIRST/LAST
	validDirections := []string{
		"ASC NULLS FIRST",
		"ASC NULLS LAST",
		"DESC NULLS FIRST",
		"DESC NULLS LAST",
	}

	for _, validDir := range validDirections {
		if remainingUpper == validDir {
			return columnName, validDir, nil
		}
	}

	return "", "", errors.Errorf("invalid order by specification: %s", columnSpec)
}
