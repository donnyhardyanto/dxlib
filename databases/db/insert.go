package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	go_ora "github.com/sijms/go-ora/v2"
)

// SQLPartInsertFieldNamesFieldValues generates the field names and values parts for an SQL INSERT statement
func SQLPartInsertFieldNamesFieldValues(insertKeyValues utils.JSON, driverName string) (fieldNames string, fieldValues string) {
	for k, v := range insertKeyValues {
		// Format the COLUMN identifier per the engine's rules (Oracle: quoted-
		// uppercase, reserved-word-safe — matches the DDL), but bind the named
		// ":placeholder" by the ORIGINAL key — the arg maps handed to sqlx.Named /
		// sql.Named are keyed by the original field name (same rule as the WHERE
		// and SET builders). On the other engines formattedColumn == k.
		formattedColumn := k
		if driverName == "oracle" {
			formattedColumn = DbDriverFormatIdentifier(driverName, k)
		}
		if fieldNames != "" {
			fieldNames = fieldNames + ","
		}
		fieldNames = fieldNames + formattedColumn
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
	driverName := base.NormalizeDriverName(strings.ToLower(db.DriverName()))
	dbType := base.StringToDXDatabaseType(driverName)

	// Validate table name explicitly
	// MariaDB virtual-schema: collapse schema.table to a single backtick id (no-op on other engines).
	tableName = QualifyTableNameForExec(dbType, tableName)
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
	case "postgres", "mariadb":
		// PostgreSQL and MariaDB support RETURNING clause with the same syntax
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
		// Build RETURNING INTO clause
		var returningFields []string
		var returningIntoFields []string
		outParams := make([]interface{}, 0, len(returningFieldNames))
		outDests := make([]*string, len(returningFieldNames))

		for i, key := range returningFieldNames {
			// RETURNING column quoted-uppercase (reserved-word-safe, e.g. "UID");
			// the :name_out bind keeps the original key (out-bind names are never
			// bare column names, so no reserved-word risk).
			returningFields = append(returningFields, DbDriverFormatIdentifier(driverName, key))
			returningIntoFields = append(returningIntoFields, fmt.Sprintf(":%s_out", key))

			// Out binds are SIZED STRINGS (go_ora.Out): an untyped sql.Out dest is
			// ORA-03146 (go-ora cannot size the TTC buffer), and the column's Go type
			// is unknown here — Oracle implicitly converts any RETURNING value to the
			// VARCHAR2 bind. Integer-looking values are coerced back below.
			outDests[i] = new(string)
			outParams = append(outParams, sql.Named(key+"_out", go_ora.Out{Dest: outDests[i], Size: 4000}))
		}

		sqlStatement := fmt.Sprintf("%s RETURNING %s INTO %s",
			baseSQL,
			strings.Join(returningFields, ", "),
			strings.Join(returningIntoFields, ", "))

		// Rewrite the VALUES binds to reserved-word-safe ":p_<name>" (ORA-01745)
		// with matching sql.Named args; :<key>_out binds are untouched (word
		// boundary). SQLExpression entries carry no bind, and OracleSafeBindNames
		// leaves an arg without a matching placeholder unrewritten — so exclude them.
		bindValues := utils.JSON{}
		for name, value := range convertedFieldValues {
			if _, ok := value.(SQLExpression); !ok {
				bindValues[name] = value
			}
		}
		modifiedSQL, namedArgs := OracleSafeBindNames(sqlStatement, bindValues)
		namedArgs = append(namedArgs, outParams...)

		// Execute directly for Oracle with output parameters
		result, err = db.ExecContext(ctx, modifiedSQL, namedArgs...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing oracle insert with RETURNING INTO")
		}

		// Extract output parameters. The out binds are strings (see above); a
		// CANONICAL integer (round-trips through ParseInt/FormatInt — so "0100"
		// or "+1" stay strings) comes back as int64 so numeric consumers
		// (utils/json.GetInt64) keep working. A NULL value arrives as "".
		for i, key := range returningFieldNames {
			s := *outDests[i]
			if n, convErr := strconv.ParseInt(s, 10, 64); convErr == nil && strconv.FormatInt(n, 10) == s {
				returningFieldValues[key] = n
			} else {
				returningFieldValues[key] = s
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
	driverName := base.NormalizeDriverName(strings.ToLower(tx.DriverName()))
	dbType := base.StringToDXDatabaseType(driverName)

	// Validate table name explicitly
	// MariaDB virtual-schema: collapse schema.table to a single backtick id (no-op on other engines).
	tableName = QualifyTableNameForExec(dbType, tableName)
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
	case "postgres", "mariadb":
		// PostgreSQL and MariaDB support RETURNING clause with the same syntax
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
		// Build RETURNING INTO clause
		var returningFields []string
		var returningIntoFields []string
		outParams := make([]interface{}, 0, len(returningFieldNames))
		outDests := make([]*string, len(returningFieldNames))

		for i, key := range returningFieldNames {
			// RETURNING column quoted-uppercase (reserved-word-safe, e.g. "UID");
			// the :name_out bind keeps the original key (out-bind names are never
			// bare column names, so no reserved-word risk).
			returningFields = append(returningFields, DbDriverFormatIdentifier(driverName, key))
			returningIntoFields = append(returningIntoFields, fmt.Sprintf(":%s_out", key))

			// Out binds are SIZED STRINGS (go_ora.Out): an untyped sql.Out dest is
			// ORA-03146 (go-ora cannot size the TTC buffer), and the column's Go type
			// is unknown here — Oracle implicitly converts any RETURNING value to the
			// VARCHAR2 bind. Integer-looking values are coerced back below.
			outDests[i] = new(string)
			outParams = append(outParams, sql.Named(key+"_out", go_ora.Out{Dest: outDests[i], Size: 4000}))
		}

		sqlStatement := fmt.Sprintf("%s RETURNING %s INTO %s",
			baseSQL,
			strings.Join(returningFields, ", "),
			strings.Join(returningIntoFields, ", "))

		// Rewrite the VALUES binds to reserved-word-safe ":p_<name>" (ORA-01745)
		// with matching sql.Named args; :<key>_out binds are untouched (word
		// boundary). SQLExpression entries carry no bind, and OracleSafeBindNames
		// leaves an arg without a matching placeholder unrewritten — so exclude them.
		bindValues := utils.JSON{}
		for name, value := range convertedFieldValues {
			if _, ok := value.(SQLExpression); !ok {
				bindValues[name] = value
			}
		}
		modifiedSQL, namedArgs := OracleSafeBindNames(sqlStatement, bindValues)
		namedArgs = append(namedArgs, outParams...)

		// Execute directly for Oracle with output parameters
		_, err = tx.ExecContext(ctx, modifiedSQL, namedArgs...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error executing oracle insert with RETURNING INTO")
		}

		// Extract output parameters. The out binds are strings (see above); a
		// CANONICAL integer (round-trips through ParseInt/FormatInt — so "0100"
		// or "+1" stay strings) comes back as int64 so numeric consumers
		// (utils/json.GetInt64) keep working. A NULL value arrives as "".
		for i, key := range returningFieldNames {
			s := *outDests[i]
			if n, convErr := strconv.ParseInt(s, 10, 64); convErr == nil && strconv.FormatInt(n, 10) == s {
				returningFieldValues[key] = n
			} else {
				returningFieldValues[key] = s
			}
		}

	default:
		// Unsupported databases type
		return nil, nil, errors.Errorf("unsupported databases driver: %s", driverName)
	}

	return result, returningFieldValues, nil
}
