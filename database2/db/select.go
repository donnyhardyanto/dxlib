package db

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/database2/db/raw"
	"github.com/donnyhardyanto/dxlib/database2/db/sqlchecker"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// FieldsOrderBy is a map that defines ordering directions for fields
// The key is the field name, and the value is the direction ("ASC" or "DESC")

// SQLPartFieldNames formats field names for use in a SELECT clause
//
// Parameters:
//   - fieldNames: Array of fields to include in the SELECT clause
//   - driverName: Database driver name for proper identifier formatting
//
// Returns:
//   - Properly formatted field list for the SELECT statement
//
// If fieldNames is nil, returns "*" to select all fields
// Otherwise, joins the field names with commas after formatting each identifier
// according to database-specific rules
func SQLPartFieldNames(fieldNames []string, driverName string) (s string) {
	showFieldNames := ""
	if fieldNames == nil {
		return "*"
	}
	for _, v := range fieldNames {
		if showFieldNames != "" {
			showFieldNames = showFieldNames + ", "
		}
		showFieldNames = showFieldNames + utils2.DbDriverFormatIdentifier(driverName, v)
	}
	return showFieldNames
}

// SQLPartOrderByFieldNameDirections generates ORDER BY clause for different database types
//
// Parameters:
//   - orderByKeyValues: Map of field names to sort directions
//   - driverName: Database driver name for proper identifier formatting
//
// Returns:
//   - Properly formatted ORDER BY clause string (without the "ORDER BY" keyword)
//   - Error if formatting fails for any field
//
// The function formats each field and direction according to database-specific rules
// and joins them with commas
func SQLPartOrderByFieldNameDirections(orderByKeyValues map[string]string, driverName string) (string, error) {
	if len(orderByKeyValues) == 0 {
		return "", nil
	}

	var orderParts []string

	for fieldName, direction := range orderByKeyValues {
		formattedPart, err := utils2.DbDriverFormatOrderByFieldName(driverName, fieldName, direction)
		if err != nil {
			return "", errors.Errorf("error formatting ORDER BY for fieldName %s: %w", fieldName, err)
		}
		orderParts = append(orderParts, formattedPart)
	}

	return strings.Join(orderParts, ", "), nil
}

// SQLPartConstructSelect creates a SELECT query with support for all major SQL features
//
// Parameters:
//   - driverName: Database driver name
//   - tableName: The table name or subquery to select from
//   - fieldNames: Fields to select (use []string{"*"} for all fields)
//   - whereAndFieldNameValues: Conditions for filtering results
//   - joinSQLPart: Optional JOIN clause
//   - orderByFieldNameDirections: Optional ORDER BY specifications
//   - limit: Maximum number of rows to return
//   - offset: Number of rows to skip before returning results
//   - forUpdatePart: Whether to lock rows with FOR UPDATE
//   - groupByFields: Fields to group by
//   - havingClause: Optional HAVING clause for filtering grouped results
//   - withCTE: Optional Common Table Expression (WITH clause)
//
// Returns:
//   - The complete SQL query string
//   - Any error that occurred during query construction
//
// This function builds a complete SQL query by combining all the provided parts
// in the correct order according to SQL syntax rules. It handles various database-specific
// formatting requirements.
func SQLPartConstructSelect(driverName string, tableName string, fieldNames []string,
	whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections DXDatabaseTableFieldsOrderBy, limit any, offset any, forUpdatePart any,
	groupByFields []string, havingClause utils.JSON, withCTE string) (s string, err error) {

	// Common parts preparation
	f := SQLPartFieldNames(fieldNames, driverName)
	w := utils2.SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
	effectiveWhere := ""
	if w != "" {
		effectiveWhere = " where " + w
	}

	j := ""
	if joinSQLPart != nil {
		j = " " + joinSQLPart.(string)
	}

	o, err := SQLPartOrderByFieldNameDirections(orderByFieldNameDirections, driverName)
	if err != nil {
		return "", err
	}
	effectiveOrderBy := ""
	if o != "" {
		effectiveOrderBy = " order by " + o
	}

	// Handle WITH clause (Common Table Expressions) if provided
	effectiveWith := ""
	if withCTE != "" {
		effectiveWith = "with " + withCTE + " "
	}

	// Handle GROUP BY if provided
	effectiveGroupBy := ""
	if groupByFields != nil && len(groupByFields) > 0 {
		groupByColumns := make([]string, len(groupByFields))
		for i, field := range groupByFields {
			groupByColumns[i] = utils2.DbDriverFormatIdentifier(driverName, field)
		}
		effectiveGroupBy = " group by " + strings.Join(groupByColumns, ", ")
	}

	// Handle HAVING clause if provided
	havingClauseAsString := utils2.SQLPartWhereAndFieldNameValues(havingClause, driverName)

	effectiveHaving := ""
	if havingClauseAsString != "" && effectiveGroupBy != "" {
		effectiveHaving = " having " + havingClauseAsString
	}

	// Convert limit to int64 if provided
	var limitAsInt64 int64
	if limit != nil {
		switch v := limit.(type) {
		case int:
			limitAsInt64 = int64(v)
		case int16:
			limitAsInt64 = int64(v)
		case int32:
			limitAsInt64 = int64(v)
		case int64:
			limitAsInt64 = v
		default:
			return "", errors.New("SHOULD_NOT_HAPPEN:CANT_CONVERT_LIMIT_TO_INT64")
		}
	}

	// Convert offset to int64, defaulting to 0 if not provided
	var offsetAsInt64 int64 = 0 // Default to 0
	if offset != nil {
		switch v := offset.(type) {
		case int:
			offsetAsInt64 = int64(v)
		case int16:
			offsetAsInt64 = int64(v)
		case int32:
			offsetAsInt64 = int64(v)
		case int64:
			offsetAsInt64 = v
		default:
			return "", errors.New("SHOULD_NOT_HAPPEN:CANT_CONVERT_OFFSET_TO_INT64")
		}
	}

	// Handle FOR UPDATE clause
	u := ""
	if forUpdatePart == nil {
		forUpdatePart = false
	}
	if forUpdatePart == true {
		u = " for update"
	}

	// Generate database-specific limit and offset clauses
	effectiveLimitOffsetClause, additionalOrderBy, err := utils2.DBDriverGenerateLimitOffsetClause(driverName, limitAsInt64, offsetAsInt64, limit != nil, effectiveOrderBy, orderByFieldNameDirections)
	if err != nil {
		return "", err
	}

	// Use the additionalOrderBy if it was modified in generateLimitOffsetClause
	if additionalOrderBy != "" {
		effectiveOrderBy = additionalOrderBy
	}

	// Generate the final SQL
	s = effectiveWith + "select " + f + " from " + tableName + j + effectiveWhere +
		effectiveGroupBy + effectiveHaving + effectiveOrderBy + effectiveLimitOffsetClause + u

	return s, nil
}

// BaseSelect is the foundational function for executing SQL SELECT queries.
// It supports all major SQL features including GROUP BY, HAVING, and Common Table Expressions (CTE).
//
// Parameters:
//   - db: The database connection
//   - fieldTypeMapping: Type conversion mapping for fields
//   - tableName: The table name or subquery to select from
//   - fieldNames: Fields to select (use []string{"*"} for all fields)
//   - whereAndFieldNameValues: Conditions for filtering results (nil for no conditions)
//   - joinSQLPart: Optional JOIN clause (nil for no joins)
//   - orderByFieldNameDirections: Optional ORDER BY specifications (nil for no ordering)
//   - limit: Maximum number of rows to return (nil for no limit)
//   - offset: Number of rows to skip before returning results (nil for no offset)
//   - forUpdatePart: Whether to lock rows with FOR UPDATE (nil or false for no locking)
//   - groupByFields: Fields to group by (nil for no grouping)
//   - havingClause: Optional HAVING clause for filtering grouped results (empty string for none)
//   - withCTE: Optional Common Table Expression (WITH clause) (empty string for none)
//
// Returns:
//   - rowsInfo: Information about the returned columns
//   - r: The query results as a slice of JSON objects
//   - err: Any error that occurred during query execution
//
// Examples:
//
//	// Simple select
//	rows, err := BaseSelect(db, mapping, "users", []string{"id", "name"}, nil, nil, nil, nil, nil, nil, nil, "", "")
//	// Generates: SELECT id, name FROM users
//
//	// Select with GROUP BY and HAVING
//	rows, err := BaseSelect(db, mapping, "orders", []string{"customer_id", "COUNT(*) as order_count"},
//	  nil, nil, nil, nil, nil, nil, []string{"customer_id"}, "COUNT(*) > 5", "")
//	// Generates: SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING COUNT(*) > 5
//
//	// Select with CTE
//	cte := "recent_orders AS (SELECT * FROM orders WHERE order_date > '2023-01-01')"
//	rows, err := BaseSelect(db, mapping, "recent_orders", []string{"*"}, nil, nil, nil, nil, nil, nil, nil, "", cte)
//	// Generates: WITH recent_orders AS (SELECT * FROM orders WHERE order_date > '2023-01-01') SELECT * FROM recent_orders
func BaseSelect(db *sqlx.DB, tableName string, fieldTypeMapping DXDatabaseTableFieldTypeMapping,
	fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupByFields []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy, limit any, offset any, forUpdatePart any,
	withCTE string) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	if fieldNames == nil {
		fieldNames = []string{"*"}
	}
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	driverName := db.DriverName()

	s, err := SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues,
		joinSQLPart, orderByFieldNameDirections, limit, offset, forUpdatePart,
		groupByFields, havingClause, withCTE)
	if err != nil {
		return nil, nil, err
	}

	wKV, err := utils2.DBDriverExcludeSQLExpressionFromWhereKeyValues(driverName, whereAndFieldNameValues)
	if err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = raw.QueryRows(db, fieldTypeMapping, s, wKV)
	return rowsInfo, r, err
}

// BaseTxSelect is the transaction version of BaseSelect for executing SQL SELECT queries within a transaction.
// It supports all major SQL features including GROUP BY, HAVING, and Common Table Expressions (CTE).
//
// Parameters:
//   - tx: The database transaction
//   - fieldTypeMapping: Type conversion mapping for fields
//   - tableName: The table name or subquery to select from
//   - fieldNames: Fields to select (use []string{"*"} for all fields)
//   - whereAndFieldNameValues: Conditions for filtering results (nil for no conditions)
//   - joinSQLPart: Optional JOIN clause (nil for no joins)
//   - orderByFieldNameDirections: Optional ORDER BY specifications (nil for no ordering)
//   - limit: Maximum number of rows to return (nil for no limit)
//   - offset: Number of rows to skip before returning results (nil for no offset)
//   - forUpdatePart: Whether to lock rows with FOR UPDATE (nil or false for no locking)
//   - groupByFields: Fields to group by (nil for no grouping)
//   - havingClause: Optional HAVING clause for filtering grouped results (empty string for none)
//   - withCTE: Optional Common Table Expression (WITH clause) (empty string for none)
//
// Returns:
//   - rowsInfo: Information about the returned columns
//   - r: The query results as a slice of JSON objects
//   - err: Any error that occurred during query execution
//
// This function is similar to BaseSelect but operates within a transaction context,
// allowing for consistent reads and potential row locking when used with forUpdatePart=true
func BaseTxSelect(tx *sqlx.Tx, tableName string, fieldTypeMapping DXDatabaseTableFieldTypeMapping,
	fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, groupByFields []string, havingClause utils.JSON,
	orderByFieldNameDirections utils2.FieldsOrderBy, limit any, offset any, forUpdatePart any,
	withCTE string) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	if fieldNames == nil {
		fieldNames = []string{"*"}
	}
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	driverName := tx.DriverName()

	dbType := StringToDXDatabaseType(driverName)

	// Validate table name explicitly
	if err := sqlchecker.CheckIdentifier(dbType, tableName); err != nil {
		return nil, nil, errors.Wrap(err, "invalid table name")
	}

	// Validate field names (except for "*" which is handled specially)
	for _, fieldName := range fieldNames {
		if fieldName != "*" {
			// Skip validation for expressions (functions, etc.)
			if strings.Contains(fieldName, "(") || strings.Contains(fieldName, " ") {
				continue
			}

			if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
				return nil, nil, errors.Wrapf(err, "invalid field name: %s", fieldName)
			}
		}
	}

	for fieldName := range whereAndFieldNameValues {
		// Skip SQL expressions
		if _, ok := whereAndFieldNameValues[fieldName].(SQLExpression); ok {
			continue
		}

		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid WHERE field name: %s", fieldName)
		}
	}

	// Validate ORDER BY field names
	for fieldName := range orderByFieldNameDirections {
		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid ORDER BY field name: %s", fieldName)
		}
	}

	// Validate GROUP BY field names if present
	if groupByFields != nil {
		for _, fieldName := range groupByFields {
			if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
				return nil, nil, errors.Wrapf(err, "invalid GROUP BY field name: %s", fieldName)
			}
		}
	}

	s, err := SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues,
		joinSQLPart, orderByFieldNameDirections, limit, offset, forUpdatePart,
		groupByFields, havingClause, withCTE)
	if err != nil {
		return nil, nil, err
	}

	wKV, err := utils2.DBDriverExcludeSQLExpressionFromWhereKeyValues(driverName, whereAndFieldNameValues)
	if err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = raw.TxQueryRows(tx, fieldTypeMapping, s, wKV)
	return rowsInfo, r, err
}

// Select is a simplified version of BaseSelect that maintains compatibility with existing code.
// It doesn't include the additional GROUP BY, HAVING, and CTE features.
//
// Parameters:
//   - db: The database connection
//   - fieldTypeMapping: Type conversion mapping for fields
//   - tableName: The table name to select from
//   - fieldNames: Fields to select
//   - whereAndFieldNameValues: Conditions for filtering results
//   - joinSQLPart: Optional JOIN clause
//   - orderByFieldNameDirections: Optional ORDER BY specifications
//   - limit: Maximum number of rows to return
//   - offset: Number of rows to skip before returning results
//   - forUpdatePart: Whether to lock rows with FOR UPDATE
//
// Returns:
//   - rowsInfo: Information about the returned columns
//   - r: The query results as a slice of JSON objects
//   - err: Any error that occurred during query execution
//
// This function is a backward-compatible wrapper around BaseSelect.
// It passes nil or empty values for the GROUP BY, HAVING, and CTE parameters.
func Select(db *sqlx.DB, tableName string, fieldTypeMapping DXDatabaseTableFieldTypeMapping, fieldNames []string,
	whereAndFieldNameValues utils.JSON, joinSQLPart any, groupByFields []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy,
	limit any, offset any, forUpdatePart any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	return BaseSelect(db, tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues,
		joinSQLPart, groupByFields, havingClause, orderByFieldNameDirections, limit, offset, forUpdatePart, "")
}

// TxSelect is a transaction-based version of the Select function that maintains compatibility with existing code.
// It doesn't include the additional GROUP BY, HAVING, and CTE features.
//
// Parameters:
//   - tx: The database transaction
//   - fieldTypeMapping: Type conversion mapping for fields
//   - tableName: The table name to select from
//   - fieldNames: Fields to select
//   - whereAndFieldNameValues: Conditions for filtering results
//   - joinSQLPart: Optional JOIN clause
//   - orderByFieldNameDirections: Optional ORDER BY specifications
//   - limit: Maximum number of rows to return
//   - offset: Number of rows to skip before returning results
//   - forUpdatePart: Whether to lock rows with FOR UPDATE
//
// Returns:
//   - rowsInfo: Information about the returned columns
//   - r: The query results as a slice of JSON objects
//   - err: Any error that occurred during query execution
//
// This function is a transaction-based wrapper around BaseTxSelect.
// It passes nil or empty values for the GROUP BY, HAVING, and CTE parameters.
func TxSelect(tx *sqlx.Tx, tableName string, fieldTypeMapping DXDatabaseTableFieldTypeMapping, fieldNames []string,
	whereAndFieldNameValues utils.JSON, joinSQLPart any, groupByFields []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy,
	limit any, offset any, forUpdatePart any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	return BaseTxSelect(tx, tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues,
		joinSQLPart, groupByFields, havingClause, orderByFieldNameDirections, limit, offset, forUpdatePart, "")
}

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
		"join ", "where ", "group by", "order by",
	}

	for _, keyword := range selectKeywords {
		if strings.Contains(normalized, keyword) {
			return true
		}
	}

	// Check if it has multiple spaces or parentheses anywhere
	return strings.Count(normalized, " ") > 2 || strings.Contains(normalized, "(")
}

// Count executes a count query against the database and returns the result as an int64.
// This implementation leverages BaseSelect for all database interaction.
//
// Parameters:
//   - db: The database connection
//   - tableOrSubquery: The table name or subquery to count from
//   - countExpr: The count expression (e.g., "count(*)", "count(distinct user_id)")
//   - whereAndFieldNameValues: Conditions for filtering results
//   - joinSQLPart: Optional JOIN clause
//   - groupByFields: Fields to group by
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

	// OPTIONAL: Optimize for GROUP BY case -- WARNING: IT WILL WORK ON MARIADB BUT NOT IN MYSQL
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

		// Handle database-specific subquery syntax
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
		whereAndFieldNameValues, joinSQLPart, nil, nil, nil, nil,
		groupByFields, havingClause, withCTE)

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
// This implementation leverages BaseTxSelect for all database interactions.
//
// Parameters:
//   - tx: The database transaction
//   - tableOrSubquery: The table name or subquery to count from
//   - countExpr: The count expression (e.g., "count(*)", "count(distinct user_id)")
//   - whereAndFieldNameValues: Conditions for filtering results
//   - joinSQLPart: Optional JOIN clause
//   - groupByFields: Fields to group by
//   - havingClause: Optional HAVING clause for filtering grouped results
//   - withCTE: Optional Common Table Expression (WITH clause)
//
// Returns:
//   - count: The count result as an int64
//   - err: Any error that occurred during query execution
//
// This function is a transaction-based version of the Count function.
// It provides the same functionality but within a transaction context,
// which ensures consistency across multiple database operations.
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

	// OPTIONAL: Optimize for GROUP BY case -- WARNING: IT WILL WORK ON MARIADB BUT NOT IN MYSQL
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

		// Handle database-specific subquery syntax
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
		whereAndFieldNameValues, joinSQLPart, nil, nil, nil, nil,
		groupByFields, havingClause, withCTE)

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

func SelectPaging(db *sqlx.DB, pageIndex int64, rowsPerPage int64, tableName string, fieldTypeMapping DXDatabaseTableFieldTypeMapping, fieldNames []string,
	whereAndFieldNameValues utils.JSON, joinSQLPart any, groupByFields []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy) (totalRowCount int64, rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	dtx, err := db.Beginx()
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() {
		if err != nil {
			errTx := dtx.Rollback()
			if errTx != nil {
				fmt.Printf("Suppressed Error rolling back transaction:%s from %s", errTx.Error(), err.Error())
			}
			return
		}
		errTx := dtx.Commit()
		if errTx != nil {
			fmt.Printf("Suppressed Error commit transaction:%s from %s", errTx.Error(), err.Error())
			errTx2 := dtx.Rollback()
			if errTx2 != nil {
				fmt.Printf("Suppressed Error rolling back transaction:%s from %s", errTx2.Error(), errTx.Error())
			}
		}
		return
	}()
	rowCount, err := TxCount(dtx, tableName, "", whereAndFieldNameValues, joinSQLPart, groupByFields, havingClause, "")
	if err != nil {
		return 0, nil, nil, err
	}

	limit := rowsPerPage
	offset := pageIndex * limit

	rowsInfo, r, err = BaseTxSelect(dtx, tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues,
		joinSQLPart, groupByFields, havingClause, orderByFieldNameDirections, limit, offset, nil, "")
	if err != nil {
		return rowCount, nil, nil, err
	}
	return rowCount, rowsInfo, r, nil
}
