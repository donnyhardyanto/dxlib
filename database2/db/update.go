package db

import (
	"database/sql"
	"strings"

	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/database2/db/raw"
	"github.com/donnyhardyanto/dxlib/database2/db/sqlchecker"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// SQLPartUpdateSetFieldValues generates the SET clause for UPDATE statements
func SQLPartUpdateSetFieldValues(setFieldValues utils.JSON, driverName string) (s string) {
	if len(setFieldValues) == 0 {
		return ""
	}

	var setParts []string
	for k, v := range setFieldValues {
		formattedKey := k
		switch driverName {
		case "oracle":
			formattedKey = strings.ToUpper(k)
		}

		var setPart string
		if v == nil {
			setPart = formattedKey + "=NULL"
		} else {
			switch v := v.(type) {
			case SQLExpression:
				setPart = formattedKey + "=" + v.String()
			default:
				setPart = formattedKey + "=:" + k
			}
		}
		setParts = append(setParts, setPart)
	}

	return strings.Join(setParts, ", ")
}

// Update executes an SQL UPDATE statement with support for returning values
//
// Parameters:
//   - db: Database connection
//   - tableName: Name of the table to update
//   - setFieldValues: Map of field names to new values
//   - whereAndFieldNameValues: Conditions for filtering which rows to update
//   - returningFieldNames: List of field names to return after update
//
// Returns:
//   - rowsAffected: Number of rows affected by the update
//   - returningFieldValues: Map of returned field names to their values (if requested)
//   - err: Any error that occurred during the operation
//
// Supports different returning clause implementations for:
//   - PostgreSQL/MariaDB: RETURNING clause
//   - SQL Server: OUTPUT clause
//   - Oracle: RETURNING INTO clause
//   - MySQL: Separate SELECT query after UPDATE (with limitations)
func Update(db *sqlx.DB, tableName string, setFieldNameValues utils.JSON, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	// Basic input validation
	if db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	if tableName == "" {
		return nil, nil, errors.New("table name cannot be empty")
	}
	if len(setFieldNameValues) == 0 {
		return nil, nil, errors.New("no fields to update")
	}

	// Get the database driver name
	driverName := strings.ToLower(db.DriverName())
	dbType := database_type.StringToDXDatabaseType(driverName)

	// Validate table name
	if err := sqlchecker.CheckIdentifier(dbType, tableName); err != nil {
		return nil, nil, errors.Wrap(err, "invalid table name")
	}

	// Validate SET field names
	for fieldName := range setFieldNameValues {
		// Skip SQL expressions
		if _, ok := setFieldNameValues[fieldName].(SQLExpression); ok {
			continue
		}

		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid SET field name: %s", fieldName)
		}
	}

	// Validate WHERE field names
	for fieldName := range whereAndFieldNameValues {
		// Skip SQL expressions
		if _, ok := whereAndFieldNameValues[fieldName].(SQLExpression); ok {
			continue
		}

		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid WHERE field name: %s", fieldName)
		}
	}

	// Validate RETURNING field names
	for _, fieldName := range returningFieldNames {
		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid RETURNING field name: %s", fieldName)
		}
	}

	// Prepare SET and WHERE clauses
	setClause := SQLPartUpdateSetFieldValues(setFieldNameValues, driverName)
	whereClause := utils2.SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)

	var effectiveWhere string
	if whereClause != "" {
		effectiveWhere = " WHERE " + whereClause
	}

	// Initialize result values
	returningFieldValues = []utils.JSON{}

	// Combine SET and WHERE values
	combinedParams := utils.JSON{}
	for k, v := range setFieldNameValues {
		if _, ok := v.(SQLExpression); !ok {
			combinedParams[k] = v
		}
	}
	for k, v := range whereAndFieldNameValues {
		if _, ok := v.(SQLExpression); !ok {
			combinedParams[k] = v
		}
	}

	// Handle database-specific UPDATE with RETURNING
	switch driverName {
	case "postgres", "mariadb":
		// PostgreSQL and MariaDB support RETURNING clause
		baseSQL := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple update without returning
			result, err := raw.Exec(db, baseSQL, combinedParams)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing update")
			}
			return result, returningFieldValues, nil
		}

		// Update with RETURNING clause
		returningClause := strings.Join(returningFieldNames, ", ")
		sqlStatement := strings.Join([]string{
			baseSQL,
			"RETURNING",
			returningClause,
		}, " ")

		_, rows, err := raw.QueryRows(db, nil, sqlStatement, combinedParams)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing update with RETURNING clause")
		}

		return result, rows, nil

	case "sqlserver", "mssql":
		// SQL Server uses OUTPUT clause
		if len(returningFieldNames) == 0 {
			// Simple update without returning
			baseSQL := strings.Join([]string{
				"UPDATE",
				tableName,
				"SET",
				setClause,
				effectiveWhere,
			}, " ")
			result, err := raw.Exec(db, baseSQL, combinedParams)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing update")
			}
			return result, returningFieldValues, nil
		}

		// Build OUTPUT clause
		var outputFields []string
		for _, key := range returningFieldNames {
			outputFields = append(outputFields, "DELETED."+key)
		}

		outputClause := strings.Join(outputFields, ", ")
		sqlStatement := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			"OUTPUT",
			outputClause,
			effectiveWhere,
		}, " ")

		_, rows, err := raw.QueryRows(db, nil, sqlStatement, combinedParams)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing update with OUTPUT clause")
		}

		return nil, rows, nil

	case "oracle":
		// Oracle uses RETURNING INTO syntax
		baseSQL := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple update without returning
			result, err := raw.Exec(db, baseSQL, combinedParams)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing oracle update")
			}
			return result, returningFieldValues, nil
		}

		// For returning values in Oracle, use a specialized approach similar to the Insert function
		// This would require building RETURNING INTO parameters similar to the Insert function
		// For brevity, this implementation returns a not supported error
		return nil, nil, errors.New("Oracle RETURNING INTO for UPDATE not implemented in this version")

	case "mysql":
		return nil, nil, errors.New("MySQL support has been dropped, change to MariaDB")

		/*// MySQL doesn't support RETURNING directly
		baseSQL := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			effectiveWhere,
		}, " ")

		// Execute the update
		result, err := raw.Exec(db, baseSQL, combinedParams)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing mysql update")
		}

		// If no returning fields requested, return just the rows affected
		if len(returningFieldNames) == 0 {
			return result, returningFieldValues, nil
		}

		// For MySQL, we need to run a separate SELECT query to get the updated values
		// This will only work if we have a WHERE clause that can uniquely identify the updated rows
		if whereClause == "" {
			return result, nil, errors.New("cannot use RETURNING with MySQL unless WHERE clause uniquely identifies rows")
		}

		// Query the updated rows
		selectSQL := strings.Join([]string{
			"SELECT",
			strings.Join(returningFieldNames, ", "),
			"FROM",
			tableName,
			effectiveWhere,
		}, " ")

		_, rows, err := raw.QueryRows(db, nil, selectSQL, whereAndFieldNameValues)
		if err != nil {
			// Log the error but don't fail - we've already done the update
			return result, nil, errors.Wrap(err, "error fetching updated rows")
		}

		return result, rows, nil
		*/
	default:
		// Unsupported database type
		return nil, nil, errors.Errorf("unsupported database driver: %s", driverName)
	}
}

func TxUpdate(tx *sqlx.Tx, tableName string, setFieldValues utils.JSON, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	// Basic input validation
	if tx == nil {
		return nil, nil, errors.New("database transaction connection is nil")
	}
	if tableName == "" {
		return nil, nil, errors.New("table name cannot be empty")
	}
	if len(setFieldValues) == 0 {
		return nil, nil, errors.New("no fields to update")
	}

	// Get the database driver name
	driverName := strings.ToLower(tx.DriverName())
	dbType := database_type.StringToDXDatabaseType(driverName)

	// Validate table name
	if err := sqlchecker.CheckIdentifier(dbType, tableName); err != nil {
		return nil, nil, errors.Wrap(err, "invalid table name")
	}

	// Validate SET field names
	for fieldName := range setFieldValues {
		// Skip SQL expressions
		if _, ok := setFieldValues[fieldName].(SQLExpression); ok {
			continue
		}

		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid SET field name: %s", fieldName)
		}
	}

	// Validate WHERE field names
	for fieldName := range whereAndFieldNameValues {
		// Skip SQL expressions
		if _, ok := whereAndFieldNameValues[fieldName].(SQLExpression); ok {
			continue
		}

		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid WHERE field name: %s", fieldName)
		}
	}

	// Validate RETURNING field names
	for _, fieldName := range returningFieldNames {
		if err := sqlchecker.CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid RETURNING field name: %s", fieldName)
		}
	}

	// Prepare SET and WHERE clauses
	setClause := SQLPartUpdateSetFieldValues(setFieldValues, driverName)
	whereClause := utils2.SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)

	var effectiveWhere string
	if whereClause != "" {
		effectiveWhere = " WHERE " + whereClause
	}

	// Initialize result values
	returningFieldValues = []utils.JSON{}

	// Combine SET and WHERE values
	combinedParams := utils.JSON{}
	for k, v := range setFieldValues {
		if _, ok := v.(SQLExpression); !ok {
			combinedParams[k] = v
		}
	}
	for k, v := range whereAndFieldNameValues {
		if _, ok := v.(SQLExpression); !ok {
			combinedParams[k] = v
		}
	}

	// Handle database-specific UPDATE with RETURNING
	switch driverName {
	case "postgres", "mariadb":
		// PostgreSQL and MariaDB support RETURNING clause
		baseSQL := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple update without returning
			result, err := raw.TxExec(tx, baseSQL, combinedParams)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing update")
			}
			return result, returningFieldValues, nil
		}

		// Update with RETURNING clause
		returningClause := strings.Join(returningFieldNames, ", ")
		sqlStatement := strings.Join([]string{
			baseSQL,
			"RETURNING",
			returningClause,
		}, " ")

		_, rows, err := raw.TxQueryRows(tx, nil, sqlStatement, combinedParams)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing update with RETURNING clause")
		}

		return result, rows, nil

	case "sqlserver", "mssql":
		// SQL Server uses OUTPUT clause
		if len(returningFieldNames) == 0 {
			// Simple update without returning
			baseSQL := strings.Join([]string{
				"UPDATE",
				tableName,
				"SET",
				setClause,
				effectiveWhere,
			}, " ")
			result, err := raw.TxExec(tx, baseSQL, combinedParams)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing update")
			}
			return result, returningFieldValues, nil
		}

		// Build OUTPUT clause
		var outputFields []string
		for _, key := range returningFieldNames {
			outputFields = append(outputFields, "DELETED."+key)
		}

		outputClause := strings.Join(outputFields, ", ")
		sqlStatement := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			"OUTPUT",
			outputClause,
			effectiveWhere,
		}, " ")

		_, rows, err := raw.TxQueryRows(tx, nil, sqlStatement, combinedParams)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing update with OUTPUT clause")
		}

		return result, rows, nil

	case "oracle":
		// Oracle uses RETURNING INTO syntax
		baseSQL := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple update without returning
			result, err := raw.TxExec(tx, baseSQL, combinedParams)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing oracle update")
			}
			return result, returningFieldValues, nil
		}

		// For returning values in Oracle, use a specialized approach similar to the Insert function
		// This would require building RETURNING INTO parameters similar to the Insert function
		// For brevity, this implementation returns a not supported error
		return nil, nil, errors.New("Oracle RETURNING INTO for UPDATE not implemented in this version")

	case "mysql":
		return nil, nil, errors.New("MySQL support has been dropped, change to MariaDB")

		/*// MySQL doesn't support RETURNING directly
		baseSQL := strings.Join([]string{
			"UPDATE",
			tableName,
			"SET",
			setClause,
			effectiveWhere,
		}, " ")

		// Execute the update
		result, err := raw.TxExec(tx, baseSQL, combinedParams)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing mysql update")
		}

		// If no returning fields requested, return just the rows affected
		if len(returningFieldNames) == 0 {
			return result, returningFieldValues, nil
		}

		// For MySQL, we need to run a separate SELECT query to get the updated values
		// This will only work if we have a WHERE clause that can uniquely identify the updated rows
		if whereClause == "" {
			return result, nil, errors.New("cannot use RETURNING with MySQL unless WHERE clause uniquely identifies rows")
		}

		// Query the updated rows
		selectSQL := strings.Join([]string{
			"SELECT",
			strings.Join(returningFieldNames, ", "),
			"FROM",
			tableName,
			effectiveWhere,
		}, " ")

		_, rows, err := raw.TxQueryRows(tx, nil, selectSQL, whereAndFieldNameValues)
		if err != nil {
			// Log the error but don't fail - we've already done the update
			return result, nil, errors.Wrap(err, "error fetching updated rows")
		}

		return result, rows, nil*/

	default:
		// Unsupported database type
		return nil, nil, errors.Errorf("unsupported database driver: %s", driverName)
	}
}
