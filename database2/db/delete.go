package db

import (
	"database/sql"
	"strings"

	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// Delete executes an SQL DELETE statement with support for returning values
//
// Parameters:
//   - db: Database connection
//   - tableName: Name of the table to delete from
//   - whereAndFieldNameValues: Conditions for filtering which rows to delete
//   - returningFieldNames: List of field names to return after delete
//
// Returns:
//   - rowsAffected: Number of rows affected by the delete
//   - returningFieldValues: Map of returned field names to their values (if requested)
//   - err: Any error that occurred during the operation
//
// Supports different returning clause implementations for:
//   - PostgreSQL/MariaDB: RETURNING clause
//   - SQL Server: OUTPUT clause
//   - Oracle: RETURNING INTO clause
//   - MySQL: Not supported for DELETE (will return errors.Wrap(err, "error occured")or if requested)
func Delete(db *sqlx.DB, tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	// Basic input validation
	if db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	if tableName == "" {
		return nil, nil, errors.New("table name cannot be empty")
	}

	// Get the database driver name
	driverName := strings.ToLower(db.DriverName())
	dbType := StringToDXDatabaseType(driverName)

	// Validate table name
	if err := CheckIdentifier(dbType, tableName); err != nil {
		return nil, nil, errors.Wrap(err, "invalid table name")
	}

	// Validate WHERE field names
	for fieldName := range whereAndFieldNameValues {
		// Skip SQL expressions
		if _, ok := whereAndFieldNameValues[fieldName].(SQLExpression); ok {
			continue
		}

		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid WHERE field name: %s", fieldName)
		}
	}

	// Validate RETURNING field names
	for _, fieldName := range returningFieldNames {
		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid RETURNING field name: %s", fieldName)
		}
	}

	// Prepare WHERE clause
	whereClause := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)

	var effectiveWhere string
	if whereClause != "" {
		effectiveWhere = " WHERE " + whereClause
	} else {
		// For safety, require explicit WHERE clause for DELETE
		return nil, nil, errors.New("DELETE without WHERE clause is not allowed")
	}

	// Initialize result values
	returningFieldValues = []utils.JSON{}

	// Handle database-specific DELETE with RETURNING
	switch driverName {
	case "postgres", "mariadb":
		// PostgreSQL and MariaDB support RETURNING clause
		baseSQL := strings.Join([]string{
			"DELETE FROM",
			tableName,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple delete without returning
			result, err := Exec(db, baseSQL, whereAndFieldNameValues)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing delete")
			}
			return result, returningFieldValues, nil
		}

		// Delete with RETURNING clause
		returningClause := strings.Join(returningFieldNames, ", ")
		sqlStatement := strings.Join([]string{
			baseSQL,
			"RETURNING",
			returningClause,
		}, " ")

		_, rows, err := QueryRows(db, nil, sqlStatement, whereAndFieldNameValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing delete with RETURNING clause")
		}

		return result, rows, nil

	case "sqlserver", "mssql":
		// SQL Server uses OUTPUT clause
		if len(returningFieldNames) == 0 {
			// Simple delete without returning
			baseSQL := strings.Join([]string{
				"DELETE FROM",
				tableName,
				effectiveWhere,
			}, " ")
			result, err := Exec(db, baseSQL, whereAndFieldNameValues)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing delete")
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
			"DELETE FROM",
			tableName,
			"OUTPUT",
			outputClause,
			effectiveWhere,
		}, " ")

		_, rows, err := QueryRows(db, nil, sqlStatement, whereAndFieldNameValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing delete with OUTPUT clause")
		}

		return result, rows, nil

	case "oracle":
		// Oracle uses RETURNING INTO syntax
		baseSQL := strings.Join([]string{
			"DELETE FROM",
			tableName,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple delete without returning
			result, err := Exec(db, baseSQL, whereAndFieldNameValues)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing oracle delete")
			}
			return result, returningFieldValues, nil
		}

		// For returning values in Oracle, use a specialized approach
		// For brevity, this implementation returns a not supported error
		return nil, nil, errors.New("Oracle RETURNING INTO for DELETE not implemented in this version")

	case "mysql":
		return nil, nil, errors.New("MySQL support has been dropped, change to MariaDB")

		/*// MySQL doesn't support RETURNING directly for DELETE
		baseSQL := strings.Join([]string{
			"DELETE FROM",
			tableName,
			effectiveWhere,
		}, " ")

		// Execute the delete
		result, err := raw.Exec(db, baseSQL, whereAndFieldNameValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing mysql delete")
		}

		// If no returning fields requested, return just the rows affected
		if len(returningFieldNames) == 0 {
			return result, returningFieldValues, nil
		}

		// MySQL doesn't allow getting values from deleted rows
		return nil, nil, errors.New("RETURNING not supported for DELETE with mysql")*/

	default:
		// Unsupported database type
		return nil, nil, errors.Errorf("unsupported database driver: %s", driverName)
	}
}

// TxDelete executes an SQL DELETE statement within a transaction with support for returning values
func TxDelete(tx *sqlx.Tx, tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	// Basic input validation
	if tx == nil {
		return nil, nil, errors.New("database transaction connection is nil")
	}
	if tableName == "" {
		return nil, nil, errors.New("table name cannot be empty")
	}

	// Get the database driver name
	driverName := strings.ToLower(tx.DriverName())
	dbType := StringToDXDatabaseType(driverName)

	// Validate table name
	if err := CheckIdentifier(dbType, tableName); err != nil {
		return nil, nil, errors.Wrap(err, "invalid table name")
	}

	// Validate WHERE field names
	for fieldName := range whereAndFieldNameValues {
		// Skip SQL expressions
		if _, ok := whereAndFieldNameValues[fieldName].(SQLExpression); ok {
			continue
		}

		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid WHERE field name: %s", fieldName)
		}
	}

	// Validate RETURNING field names
	for _, fieldName := range returningFieldNames {
		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid RETURNING field name: %s", fieldName)
		}
	}

	// Prepare WHERE clause
	whereClause := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)

	var effectiveWhere string
	if whereClause != "" {
		effectiveWhere = " WHERE " + whereClause
	} else {
		// For safety, require explicit WHERE clause for DELETE
		return nil, nil, errors.New("DELETE without WHERE clause is not allowed")
	}

	// Initialize result values
	returningFieldValues = []utils.JSON{}

	// Handle database-specific DELETE with RETURNING
	switch driverName {
	case "postgres", "mariadb":
		// PostgreSQL and MariaDB support RETURNING clause
		baseSQL := strings.Join([]string{
			"DELETE FROM",
			tableName,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple delete without returning
			result, err = TxExec(tx, baseSQL, whereAndFieldNameValues)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing delete")
			}
			return result, returningFieldValues, nil
		}

		// Delete with RETURNING clause
		returningClause := strings.Join(returningFieldNames, ", ")
		sqlStatement := strings.Join([]string{
			baseSQL,
			"RETURNING",
			returningClause,
		}, " ")

		_, rows, err := TxQueryRows(tx, nil, sqlStatement, whereAndFieldNameValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing delete with RETURNING clause")
		}

		return result, rows, nil

	case "sqlserver", "mssql":
		// SQL Server uses OUTPUT clause
		if len(returningFieldNames) == 0 {
			// Simple delete without returning
			baseSQL := strings.Join([]string{
				"DELETE FROM",
				tableName,
				effectiveWhere,
			}, " ")
			result, err := TxExec(tx, baseSQL, whereAndFieldNameValues)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing delete")
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
			"DELETE FROM",
			tableName,
			"OUTPUT",
			outputClause,
			effectiveWhere,
		}, " ")

		_, rows, err := TxQueryRows(tx, nil, sqlStatement, whereAndFieldNameValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing delete with OUTPUT clause")
		}

		return result, rows, nil

	case "oracle":
		// Oracle uses RETURNING INTO syntax
		baseSQL := strings.Join([]string{
			"DELETE FROM",
			tableName,
			effectiveWhere,
		}, " ")

		if len(returningFieldNames) == 0 {
			// Simple delete without returning
			result, err = TxExec(tx, baseSQL, whereAndFieldNameValues)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error executing oracle delete")
			}
			return result, returningFieldValues, nil
		}

		// For returning values in Oracle, use a specialized approach
		// For brevity, this implementation returns a not supported error
		return nil, nil, errors.New("Oracle RETURNING INTO for DELETE not implemented in this version")

	case "mysql":
		return nil, nil, errors.New("MySQL support has been dropped, change to MariaDB")

		/*// MySQL doesn't support RETURNING directly for DELETE
		baseSQL := strings.Join([]string{
			"DELETE FROM",
			tableName,
			effectiveWhere,
		}, " ")

		// Execute the delete
		result, err := raw.TxExec(tx, baseSQL, whereAndFieldNameValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing mysql delete")
		}

		// If no returning fields requested, return just the rows affected
		if len(returningFieldNames) == 0 {
			return result, returningFieldValues, nil
		}

		// MySQL doesn't allow getting values from deleted rows
		return nil, nil, errors.New("RETURNING not supported for DELETE with MySQL")*/

	default:
		// Unsupported database type
		return nil, nil, errors.Errorf("unsupported database driver: %s", driverName)
	}
}
