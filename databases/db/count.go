package db

import (
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// isSubquery checks if a string is likely a SQL subquery rather than a table name
//
// Parameters:
//   - str: The string to analyze
//
// Returns:
//   - true if the string appears to be a SQL subquery
//   - false if the string appears to be a simple table name
//
// The function uses multiple heuristics to detect subqueries:
//   - Checks if the string is enclosed in parentheses
//   - Searches for SQL keywords like SELECT, FROM, JOIN, etc.
//   - Analyzes string patterns like spaces and special characters
//
// This is a heuristic function and may not be 100% accurate in all cases,
// but it covers most common scenarios.
func isSubquery(str string) bool {
	normalized := strings.ToLower(strings.TrimSpace(str))

	// Check for parentheses which often enclose subqueries
	if strings.HasPrefix(normalized, "(") && strings.HasSuffix(normalized, ")") {
		return true
	}

	// Check for SQL keywords that indicate it's a query
	selectKeywords := []string{
		"select ", "with ", "union ", "from ",
		"join ", "where ", "group by ", "order by ",
	}

	for _, keyword := range selectKeywords {
		if strings.Contains(normalized, keyword) {
			return true
		}
	}

	// Check if it has multiple spaces or parentheses anywhere
	return strings.Count(normalized, " ") > 2 || strings.Contains(normalized, "(")
}

// Count executes a count query against the databases and returns the result as an int64.
// This implementation leverages BaseSelect for all databases interactions.
//
// Parameters:
//   - db: The databases connection
//   - tableOrSubquery: The table name or subquery to count from
//   - countExpr: The count expression (e.g., "count(*)", "count(distinct user_id)")
//   - whereAndFieldNameValues: Conditions for filtering results
//   - joinSQLPart: Optional JOIN clause
//   - groupByFields: OutFields to group by
//   - havingClause: Optional HAVING clause for filtering grouped results
//   - withCTE: Optional Common Table Expression (WITH clause)
//
// Returns:
//   - count: The count result as an int64
//   - err: Any error that occurred during query execution
//
// When using Common Table Expressions (CTE):
//   - Define the CTE in the withCTE parameter (e.g., "recent_orders AS (SELECT * FROM orders WHERE date > '2023-01-01')")
//   - Reference the CTE name in the tableOrSubquery parameter (e.g., "recent_orders")
//
// Examples:
//
//	// Simple count
//	count, err := Count(db, "users", "", nil, nil, nil, "", "")
//	// Generates: SELECT COUNT(*) FROM users
//
//	// Count with condition
//	count, err := Count(db, "orders", "", utils.JSON{"status": "completed"}, nil, nil, "", "")
//	// Generates: SELECT COUNT(*) FROM orders WHERE status = 'completed'
//
//	// Count with CTE
//	cte := "active_users AS (SELECT * FROM users WHERE status = 'active')"
//	count, err := Count(db, "active_users", "", nil, nil, nil, "", cte)
//	// Generates: WITH active_users AS (SELECT * FROM users WHERE status = 'active') SELECT COUNT(*) FROM active_users
//
//	// Count with subquery
//	count, err := Count(db, "(SELECT * FROM orders WHERE date > '2023-01-01')", "", nil, nil, nil, "", "")
//	// Generates: SELECT COUNT(*) FROM (SELECT * FROM orders WHERE date > '2023-01-01') AS subquery__sq_[unique_id]
func Count(db *sqlx.DB, tableOrSubquery string, countExpression string, whereAndFieldNameValues utils.JSON,
	joinSQLPart any, groupByFields []string, havingClause string, withCTE string) (count int64, err error) {

	// Determine if this is a subquery
	isSubquery := isSubquery(tableOrSubquery)

	// When using a subquery, we shouldn't apply WHERE conditions to the outer query
	if isSubquery && whereAndFieldNameValues != nil && len(whereAndFieldNameValues) > 0 {
		return 0, errors.New("cannot apply WHERE conditions to outer level of a subquery; include them in the subquery instead")
	}

	// Prepare the count expression
	effectiveCountExpression := "count(*)"
	if countExpression != "" {
		effectiveCountExpression = countExpression
	}

	// OPTIONAL: Optimize for a GROUP BY case -- WARNING: IT WILL WORK ON MARIADB BUT NOT IN MYSQL
	if len(groupByFields) > 0 && countExpression == "" {
		// When counting groups, we just need to select something
		// SELECT 1 is more efficient than COUNT(*)
		effectiveCountExpression = "1"
	}

	// For subqueries, wrap them properly
	effectiveTable := tableOrSubquery
	if isSubquery {
		// Create a unique alias
		uniqueSuffix := "__sq_" + strconv.FormatInt(time.Now().UnixNano(), 36)

		// Handle databases-specific subquery syntax
		if db.DriverName() == "oracle" {
			effectiveTable = "(" + tableOrSubquery + ") subquery" + uniqueSuffix
		} else {
			effectiveTable = "(" + tableOrSubquery + ") as subquery" + uniqueSuffix
		}

		// Clear WHERE conditions for subqueries
		whereAndFieldNameValues = utils.JSON{}
	}

	// Execute the SELECT query with a COUNT expression
	rowsInfo, rows, err := BaseSelect(db, effectiveTable, nil, []string{effectiveCountExpression},
		whereAndFieldNameValues, joinSQLPart, groupByFields, nil, nil, nil,
		nil, nil, withCTE)

	if err != nil {
		return 0, err
	}

	if len(groupByFields) > 0 {
		return int64(len(rows)), nil
	}

	// Validate the result
	if len(rows) == 0 || len(rowsInfo.Columns) == 0 {
		return 0, errors.New("no results returned from count query")
	}

	// Extract the count value from the first column
	firstColumn := rowsInfo.Columns[0]
	countValue, ok := rows[0][firstColumn]
	if !ok {
		return 0, errors.Errorf("count column '%s' not found in result", firstColumn)
	}

	// Convert to int64
	return utils.ConvertToInt64(countValue)
}

// TxCount executes a count query within a transaction and returns the result as an int64.
// This implementation leverages BaseTxSelect for all databases interactions.
//
// Parameters:
//   - tx: The databases transaction
//   - tableOrSubquery: The table name or subquery to count from
//   - countExpr: The count expression (e.g., "count(*)", "count(distinct user_id)")
//   - whereAndFieldNameValues: Conditions for filtering results
//   - joinSQLPart: Optional JOIN clause
//   - groupByFields: OutFields to group by
//   - havingClause: Optional HAVING clause for filtering grouped results
//   - withCTE: Optional Common Table Expression (WITH clause)
//
// Returns:
//   - count: The count result as an int64
//   - err: Any error that occurred during query execution
//
// This function is a transaction-based version of the Count function.
// It provides the same functionality but within a transaction context,
// which ensures consistency across multiple databases operations.
func TxCount(tx *sqlx.Tx, tableOrSubquery string, countExpression string, whereAndFieldNameValues utils.JSON,
	joinSQLPart any, groupByFields []string, havingClause utils.JSON, withCTE string) (count int64, err error) {

	// Determine if this is a subquery
	isSubquery := isSubquery(tableOrSubquery)

	// When using a subquery, we shouldn't apply WHERE conditions to the outer query
	if isSubquery && whereAndFieldNameValues != nil && len(whereAndFieldNameValues) > 0 {
		return 0, errors.New("cannot apply WHERE conditions to outer level of a subquery; include them in the subquery instead")
	}

	// Prepare the count expression
	effectiveCountExpression := "count(*)"
	if countExpression != "" {
		effectiveCountExpression = countExpression
	}

	// OPTIONAL: Optimize for a GROUP BY case -- WARNING: IT WILL WORK ON MARIADB BUT NOT IN MYSQL
	if len(groupByFields) > 0 && countExpression == "" {
		// When counting groups, we just need to select something
		// SELECT 1 is more efficient than COUNT(*)
		effectiveCountExpression = "1"
	}

	// For subqueries, wrap them properly
	effectiveTable := tableOrSubquery
	if isSubquery {
		// Create a unique alias
		uniqueSuffix := "__sq_" + strconv.FormatInt(time.Now().UnixNano(), 36)

		// Handle databases-specific subquery syntax
		if tx.DriverName() == "oracle" {
			effectiveTable = "(" + tableOrSubquery + ") subquery" + uniqueSuffix
		} else {
			effectiveTable = "(" + tableOrSubquery + ") as subquery" + uniqueSuffix
		}

		// Clear WHERE conditions for subqueries
		whereAndFieldNameValues = utils.JSON{}
	}

	// Execute the SELECT query with a COUNT expression
	rowsInfo, rows, err := BaseTxSelect(tx, effectiveTable, nil, []string{effectiveCountExpression},
		whereAndFieldNameValues, joinSQLPart, groupByFields, havingClause, nil, nil, nil, nil,
		withCTE)

	if err != nil {
		return 0, err
	}

	if len(groupByFields) > 0 {
		return int64(len(rows)), nil
	}

	// Validate the result
	if len(rows) == 0 || len(rowsInfo.Columns) == 0 {
		return 0, errors.New("no results returned from count query")
	}

	// Extract the count value from the first column
	firstColumn := rowsInfo.Columns[0]
	countValue, ok := rows[0][firstColumn]
	if !ok {
		return 0, errors.Errorf("count column '%s' not found in result", firstColumn)
	}

	// Convert to int64
	return utils.ConvertToInt64(countValue)
}
