package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// SQLPartInsertFieldNamesFieldValues generates the field names and values parts for an SQL INSERT statement
func SQLPartInsertFieldNamesFieldValues(insertKeyValues utils.JSON, driverName string) (fieldNames string, fieldValues string) {
	for k, v := range insertKeyValues {
		switch driverName {
		case "oracle":
			k = strings.ToUpper(k)
		default:
		}
		if fieldNames != "" {
			fieldNames = fieldNames + ","
		}
		fieldNames = fieldNames + k
		if fieldValues != "" {
			fieldValues = fieldValues + ","
		}
		switch v.(type) {
		case SQLExpression:
			fieldValues = fieldValues + v.(SQLExpression).String()
		default:
			fieldValues = fieldValues + ":" + k
		}
	}
	return fieldNames, fieldValues
}

// Insert performs a databases insert with support for returning values across different databases types
// Parameters:
//   - db: Database connection
//   - tableName: Target table name
//   - setFieldValues: Map of column names to values
//   - returningFieldNames: List of field names to return after insert
//
// Returns:
//   - returningFieldValues: Map of field names to their values after insert
//   - err: Error if any occurred
func Insert(ctx context.Context, db *sqlx.DB, tableName string, setFieldValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues utils.JSON, err error) {
	defer func() {
		if err != nil {
			err = NewDBOperationError("INSERT", tableName, setFieldValues, err)
		} else {
			log.Log.Debugf("DB_INSERT table=%s data=%s", tableName, formatJSONForLog(setFieldValues))
		}
	}()
	// Basic input validation
	if db == nil {
		return nil, nil, errors.New("databases connection is nil")
	}
	if tableName == "" {
		return nil, nil, errors.New("table name cannot be empty")
	}

	// Get the databases driver name
	driverName := strings.ToLower(db.DriverName())
	dbType := base.StringToDXDatabaseType(driverName)

	// Validate table name explicitly
	if err := CheckIdentifier(dbType, tableName); err != nil {
		return nil, nil, errors.Wrap(err, "invalid table name")
	}

	// Validate field names in setFieldValues and convert values to DB-compatible types
	convertedFieldValues := utils.JSON{}
	for fieldName, fieldValue := range setFieldValues {
		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid field name: %s", fieldName)
		}
		convertedValue, err := DbDriverConvertValueTypeToDBCompatible(driverName, fieldValue)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to convert field value: %s", fieldName)
		}
		convertedFieldValues[fieldName] = convertedValue
	}

	// Validate returning field names
	for _, fieldName := range returningFieldNames {
		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid returning field name: %s", fieldName)
		}
	}

	// Prepare field names and values for the INSERT statement
	fieldNames, fieldValues := SQLPartInsertFieldNamesFieldValues(convertedFieldValues, driverName)

	// Base INSERT statement
	baseSQL := strings.Join([]string{
		"INSERT INTO",
		tableName,
		fmt.Sprintf("(%s)", fieldNames),
		"VALUES",
		fmt.Sprintf("(%s)", fieldValues),
	}, " ")

	// Initialize return values
	returningFieldValues = utils.JSON{}

	// If no returning keys requested, simply execute the insert
	if len(returningFieldNames) == 0 {
		result, err := Exec(ctx, db, baseSQL, convertedFieldValues)
		return result, returningFieldValues, err
	}

	// Handle databases-specific RETURNING clauses
	switch driverName {
	case "postgres", "mysql":
		// PostgreSQL and MariaDB (driver="mysql") support RETURNING clause with the same syntax
		sqlStatement := fmt.Sprintf("%s RETURNING %s", baseSQL, strings.Join(returningFieldNames, ", "))
		_, rows, err := QueryRows(ctx, db, nil, sqlStatement, convertedFieldValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing insert with RETURNING clause")
		}

		if len(rows) > 0 {
			returningFieldValues = rows[0]
		}

	case "sqlserver", "mssql":
		// SQL Server supports OUTPUT clause
		// Build OUTPUT clause
		var outputFields []string
		for _, key := range returningFieldNames {
			outputFields = append(outputFields, fmt.Sprintf("INSERTED.%s", key))
		}

		// Insert with OUTPUT clause
		sqlStatement := strings.Join([]string{
			"INSERT INTO",
			tableName,
			fmt.Sprintf("(%s)", fieldNames),
			"OUTPUT",
			strings.Join(outputFields, ", "),
			"VALUES",
			fmt.Sprintf("(%s)", fieldValues),
		}, " ")

		_, rows, err := QueryRows(ctx, db, nil, sqlStatement, convertedFieldValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing insert with OUTPUT clause")
		}

		if len(rows) > 0 {
			returningFieldValues = rows[0]
		}

	case "oracle":
		// Oracle uses RETURNING INTO syntax
		// Prepare named arguments for Oracle
		namedArgs := make([]interface{}, 0, len(convertedFieldValues))
		for name, value := range convertedFieldValues {
			// Skip SQL expressions
			if _, ok := value.(SQLExpression); !ok {
				namedArgs = append(namedArgs, sql.Named(strings.ToUpper(name), value))
			}
		}

		// Build RETURNING INTO clause
		var returningFields []string
		var returningIntoFields []string

		for _, key := range returningFieldNames {
			returningFields = append(returningFields, key)
			returningIntoFields = append(returningIntoFields, fmt.Sprintf(":%s_out", key))

			// Add output parameters
			var outParam interface{}
			namedArgs = append(namedArgs, sql.Named(key+"_out", sql.Out{Dest: &outParam}))
		}

		sqlStatement := fmt.Sprintf("%s RETURNING %s INTO %s",
			baseSQL,
			strings.Join(returningFields, ", "),
			strings.Join(returningIntoFields, ", "))

		// Execute directly for Oracle with output parameters
		result, err = db.ExecContext(ctx, sqlStatement, namedArgs...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing oracle insert with RETURNING INTO")
		}

		// Extract output parameters
		for _, arg := range namedArgs {
			namedArg, ok := arg.(sql.NamedArg)
			if !ok {
				continue
			}

			if strings.HasSuffix(namedArg.Name, "_out") {
				outArg, ok := namedArg.Value.(sql.Out)
				if !ok {
					continue
				}

				originalKey := strings.TrimSuffix(namedArg.Name, "_out")
				if outArg.Dest != nil {
					dest := outArg.Dest.(*interface{})
					returningFieldValues[originalKey] = *dest
				}
			}
		}

	default:
		// Unsupported databases type
		return nil, nil, errors.Errorf("unsupported databases driver: %s", driverName)
	}

	return result, returningFieldValues, nil
}

func TxInsert(ctx context.Context, tx *sqlx.Tx, tableName string, setFieldValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues utils.JSON, err error) {
	defer func() {
		if err != nil {
			err = NewDBOperationError("INSERT", tableName, setFieldValues, err)
		} else {
			log.Log.Debugf("DB_INSERT table=%s data=%s", tableName, formatJSONForLog(setFieldValues))
		}
	}()

	// Basic input validation
	if tx == nil {
		return nil, nil, errors.New("databases transaction connection is nil")
	}
	if tableName == "" {
		return nil, nil, errors.New("table name cannot be empty")
	}

	// Get the databases driver name
	driverName := strings.ToLower(tx.DriverName())
	dbType := base.StringToDXDatabaseType(driverName)

	// Validate table name explicitly
	if err := CheckIdentifier(dbType, tableName); err != nil {
		return nil, nil, errors.Wrap(err, "invalid table name")
	}

	// Validate field names in setFieldValues and convert values to DB-compatible types
	convertedFieldValues := utils.JSON{}
	for fieldName, fieldValue := range setFieldValues {
		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid field name: %s", fieldName)
		}
		convertedValue, err := DbDriverConvertValueTypeToDBCompatible(driverName, fieldValue)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to convert field value: %s", fieldName)
		}
		convertedFieldValues[fieldName] = convertedValue
	}

	// Validate returning field names
	for _, fieldName := range returningFieldNames {
		if err := CheckIdentifier(dbType, fieldName); err != nil {
			return nil, nil, errors.Wrapf(err, "invalid returning field name: %s", fieldName)
		}
	}

	// Prepare field names and values for the INSERT statement
	fieldNames, fieldValues := SQLPartInsertFieldNamesFieldValues(convertedFieldValues, driverName)

	// Base INSERT statement
	baseSQL := strings.Join([]string{
		"INSERT INTO",
		tableName,
		fmt.Sprintf("(%s)", fieldNames),
		"VALUES",
		fmt.Sprintf("(%s)", fieldValues),
	}, " ")

	// Initialize return values
	returningFieldValues = utils.JSON{}

	// If no returning keys requested, simply execute the insert
	if len(returningFieldNames) == 0 {
		result, err := TxExec(ctx, tx, baseSQL, convertedFieldValues)
		return result, returningFieldValues, err
	}

	// Handle databases-specific RETURNING clauses
	switch driverName {
	case "postgres", "mysql":
		// PostgreSQL and MariaDB (driver="mysql") support RETURNING clause with the same syntax
		sqlStatement := fmt.Sprintf("%s RETURNING %s", baseSQL, strings.Join(returningFieldNames, ", "))
		_, rows, err := TxQueryRows(ctx, tx, nil, sqlStatement, convertedFieldValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing insert with RETURNING clause")
		}

		if len(rows) > 0 {
			returningFieldValues = rows[0]
		}

	case "sqlserver", "mssql":
		// SQL Server supports OUTPUT clause
		// Build OUTPUT clause
		var outputFields []string
		for _, key := range returningFieldNames {
			outputFields = append(outputFields, fmt.Sprintf("INSERTED.%s", key))
		}

		// Insert with OUTPUT clause
		sqlStatement := strings.Join([]string{
			"INSERT INTO",
			tableName,
			fmt.Sprintf("(%s)", fieldNames),
			"OUTPUT",
			strings.Join(outputFields, ", "),
			"VALUES",
			fmt.Sprintf("(%s)", fieldValues),
		}, " ")

		_, rows, err := TxQueryRows(ctx, tx, nil, sqlStatement, convertedFieldValues)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing insert with OUTPUT clause")
		}

		if len(rows) > 0 {
			returningFieldValues = rows[0]
		}

	case "oracle":
		// Oracle uses RETURNING INTO syntax
		// Prepare named arguments for Oracle
		namedArgs := make([]interface{}, 0, len(convertedFieldValues))
		for name, value := range convertedFieldValues {
			// Skip SQL expressions
			if _, ok := value.(SQLExpression); !ok {
				namedArgs = append(namedArgs, sql.Named(strings.ToUpper(name), value))
			}
		}

		// Build RETURNING INTO clause
		var returningFields []string
		var returningIntoFields []string

		for _, key := range returningFieldNames {
			returningFields = append(returningFields, key)
			returningIntoFields = append(returningIntoFields, fmt.Sprintf(":%s_out", key))

			// Add output parameters
			var outParam interface{}
			namedArgs = append(namedArgs, sql.Named(key+"_out", sql.Out{Dest: &outParam}))
		}

		sqlStatement := fmt.Sprintf("%s RETURNING %s INTO %s",
			baseSQL,
			strings.Join(returningFields, ", "),
			strings.Join(returningIntoFields, ", "))

		// Execute directly for Oracle with output parameters
		_, err = tx.ExecContext(ctx, sqlStatement, namedArgs...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing oracle insert with RETURNING INTO")
		}

		// Extract output parameters
		for _, arg := range namedArgs {
			namedArg, ok := arg.(sql.NamedArg)
			if !ok {
				continue
			}

			if strings.HasSuffix(namedArg.Name, "_out") {
				outArg, ok := namedArg.Value.(sql.Out)
				if !ok {
					continue
				}

				originalKey := strings.TrimSuffix(namedArg.Name, "_out")
				if outArg.Dest != nil {
					dest := outArg.Dest.(*interface{})
					returningFieldValues[originalKey] = *dest
				}
			}
		}

	default:
		// Unsupported databases type
		return nil, nil, errors.Errorf("unsupported databases driver: %s", driverName)
	}

	return result, returningFieldValues, nil
}
