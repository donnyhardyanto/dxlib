package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/donnyhardyanto/dxlib/database/database_type"
	databaseProtectedUtils "github.com/donnyhardyanto/dxlib/database/protected/utils"
	"github.com/donnyhardyanto/dxlib/database/sqlchecker"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	go_ora "github.com/sijms/go-ora/v2/network"
	"strconv"
	"strings"
)

type FieldsOrderBy map[string]string

var (
	oracleConnectionErrors = []int{
		3113,  // End-of-file on communication channel
		3114,  // Not connected to Oracle
		12170, // TNS connect timeout
		12541, // No listener
		12543, // TNS destination host unreachable
		12571, // TNS packet writer failure
	}

	mariadbConnectionErrors = []uint16{
		1042, // Can't get hostname
		1043, // Bad handshake
		1044, // Access denied
		1045, // Access denied
		1047, // Connection refused
		1129, // Host blocked
		1130, // Not allowed to connect
		2002, // Can't connect
		2003, // Can't connect
		2004, // Can't create TCP/IP socket
		2005, // Unknown host
		2006, // Server gone away
	}
)

// IsConnectionError checks if the error is a database connection error
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	switch e := err.(type) {
	case *go_ora.OracleError: // Oracle
		for _, code := range oracleConnectionErrors {
			if e.ErrCode == code {
				return true
			}
		}

	case *pq.Error: // PostgreSQL
		return e.Code.Class() == "08" // Class 08 - Connection Exception

	case *mysql.MySQLError: // MariaDB/MySQL
		for _, code := range mariadbConnectionErrors {
			if e.Number == code {
				return true
			}
		}

	default: // SQL Server and generic checks
		msg := err.Error()
		connectionErrors := []string{
			"connection reset",
			"connection refused",
			"connection closed",
			"network error",
			"dial tcp",
			"broken pipe",
		}

		for _, errText := range connectionErrors {
			if strings.Contains(strings.ToLower(msg), errText) {
				return true
			}
		}
	}

	return false
}

type RowsInfo struct {
	Columns     []string
	ColumnTypes []*sql.ColumnType
}

func MergeMapExcludeSQLExpression(m1 utils.JSON, m2 utils.JSON, driverName string) (r utils.JSON) {
	r = utils.JSON{}
	for k, v := range m1 {
		switch driverName {
		case "oracle":
			k = strings.ToUpper(k)
		}
		switch v.(type) {
		case bool:
			if !v.(bool) {
				r[k] = 0
			} else {
				r[k] = 1
			}
		case SQLExpression:
			break
		default:
			r[k] = v
		}
	}
	for k, v := range m2 {
		switch driverName {
		case "oracle":
			k = strings.ToUpper(k)
		}
		switch v.(type) {
		case bool:
			if !v.(bool) {
				r[k] = 0
			} else {
				r[k] = 1
			}
		case SQLExpression:
			break
		default:
			r[k] = v
		}
	}
	return r
}

func ArrayToJSON[T any](arr []T) (string, error) {
	jsonBytes, err := json.Marshal(arr)
	if err != nil {
		return "", fmt.Errorf("failed to marshal array: %w", err)
	}
	return string(jsonBytes), nil
}

// JSONToArray converts a JSON string back to an array
func JSONToArray[T any](jsonStr string) ([]T, error) {
	var arr []T
	err := json.Unmarshal([]byte(jsonStr), &arr)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %w", err)
	}
	return arr, nil
}

func ExcludeSQLExpression(kv utils.JSON, driverName string) (r utils.JSON, err error) {
	r = utils.JSON{}
	for k, v := range kv {
		switch driverName {
		case "oracle":
			k = strings.ToUpper(k)
		}
		switch v.(type) {
		case bool:
			if !v.(bool) {
				r[k] = 0
			} else {
				r[k] = 1
			}
		case SQLExpression:
			break
		case []string:
			switch driverName {
			case "postgresql":
				r[k] = pq.Array(v.([]string))
			default:
				vx, err := ArrayToJSON[string](v.([]string))
				if err != nil {
					return r, err
				}
				r[k] = vx
			}
			break
		default:
			r[k] = v
		}
	}
	return r, nil
}

type SQLExpression struct {
	Expression string
}

func (se SQLExpression) String() (s string) {
	for _, c := range se.Expression {
		if c == ':' {
			s = s + `::`
		} else {
			s = s + string(c)
		}
	}
	return s
}

func SQLPartFieldNames(fieldNames []string, driverName string) (s string) {
	showFieldNames := ``
	if fieldNames == nil {
		return `*`
	}
	for _, v := range fieldNames {
		if showFieldNames != `` {
			showFieldNames = showFieldNames + `, `
		}
		switch driverName {
		case "oracle":
			v = strings.ToUpper(v)
		}
		showFieldNames = showFieldNames + v
	}
	return showFieldNames
}

// formatIdentifierForDB formats an identifier (column/table name) according to database requirements
func formatIdentifierForDB(identifier string, driverName string) string {
	switch driverName {
	case "oracle":
		return strings.ToUpper(identifier)
	case "db2":
		return strings.ToUpper(identifier) // DB2 is case-insensitive but conventionally uppercase
	case "sqlserver":
		return identifier // SQL Server is case-insensitive
	case "mysql":
		return identifier // MySQL on Windows is case-insensitive, on Unix case-sensitive
	case "postgres":
		return identifier // PostgreSQL is case-sensitive
	default:
		return identifier
	}
}

// SQLPartWhereAndFieldNameValues generates WHERE clause conditions for different database types
func SQLPartWhereAndFieldNameValues(whereKeyValues utils.JSON, driverName string) string {
	if len(whereKeyValues) == 0 {
		return ""
	}

	var conditions []string

	for k, v := range whereKeyValues {
		// Format the field name according to database requirements
		k = formatIdentifierForDB(k, driverName)

		var condition string
		if v == nil {
			// Handle NULL values according to SQL standard (works in all databases)
			condition = k + " IS NULL"
		} else {
			switch v := v.(type) {
			case SQLExpression:
				// Handle custom SQL expressions
				switch driverName {
				case "oracle", "db2":
					// Convert the expression to uppercase for case-insensitive databases
					condition = strings.ToUpper(v.String())
				default:
					condition = v.String()
				}
			default:
				// Handle regular equality conditions
				switch driverName {
				case "postgres":
					// PostgreSQL supports case-sensitive parameter names
					condition = k + "=:" + k
				case "mysql":
					// MySQL uses ? for parameters by default, but we're using named parameters
					condition = k + "=:" + k
				case "sqlserver":
					// SQL Server supports both @ and : for parameters
					condition = k + "=:" + k
				case "oracle":
					// Oracle uses : for parameters and uppercase
					condition = k + "=:" + strings.ToUpper(k)
				case "db2":
					// DB2 uses ? for parameters by default, but we're using named parameters
					condition = k + "=:" + strings.ToUpper(k)
				default:
					condition = k + "=:" + k
				}
			}
		}
		conditions = append(conditions, condition)
	}

	// Join all conditions with AND
	return strings.Join(conditions, " AND ")
}

/*func SQLPartWhereAndFieldNameValues(whereKeyValues utils.JSON, driverName string) (s string) {
	andFieldNameValues := ``
	for k, v := range whereKeyValues {
		switch driverName {
		case "oracle":
			k = strings.ToUpper(k)
		}
		if andFieldNameValues != `` {
			andFieldNameValues = andFieldNameValues + ` and `
		}
		if v == nil {
			andFieldNameValues = andFieldNameValues + k + ` is null `
		} else {
			switch v.(type) {
			case SQLExpression:
				andFieldNameValues = andFieldNameValues + v.(SQLExpression).String()
			default:
				andFieldNameValues = andFieldNameValues + k + `=:` + k
			}
		}
	}
	return andFieldNameValues
}*/

// validateOrderDirection ensures the order direction is valid
func validateOrderDirection(direction string) (string, error) {
	// Normalize the direction to uppercase for comparison
	upperDir := strings.ToUpper(strings.TrimSpace(direction))

	switch upperDir {
	case "ASC", "DESC":
		return upperDir, nil
	case "": // Default to ASC if empty
		return "ASC", nil
	default:
		return "", fmt.Errorf("invalid sort direction: %s. Must be ASC or DESC", direction)
	}
}

// formatOrderByField formats a field name for ORDER BY according to database requirements
func formatOrderByField(field string, direction string, driverName string) (string, error) {
	validDirection, err := validateOrderDirection(direction)
	if err != nil {
		return "", err
	}

	switch driverName {
	case "oracle", "db2":
		// Oracle and DB2 conventionally use uppercase
		return strings.ToUpper(field) + " " + validDirection, nil

	case "sqlserver":
		// SQL Server supports NULLS LAST/FIRST but needs specific syntax
		if strings.Contains(strings.ToLower(field), "nulls") {
			return "", fmt.Errorf("SQL Server doesn't support NULLS FIRST/LAST in ORDER BY directly")
		}
		return field + " " + validDirection, nil

	case "postgres":
		// PostgreSQL supports NULLS LAST/FIRST
		// Check if the field already contains NULLS specification
		if strings.Contains(strings.ToLower(field), "nulls") {
			return field + " " + validDirection, nil
		}
		// Add default NULLS LAST for DESC, NULLS FIRST for ASC
		nullsPos := "FIRST"
		if validDirection == "DESC" {
			nullsPos = "LAST"
		}
		return fmt.Sprintf("%s %s NULLS %s", field, validDirection, nullsPos), nil

	case "mysql":
		// MySQL handles NULLs differently and doesn't support NULLS FIRST/LAST
		// NULL values are considered lower than non-NULL values
		return field + " " + validDirection, nil

	default:
		return field + " " + validDirection, nil
	}
}

// SQLPartOrderByFieldNameDirections generates ORDER BY clause for different database types
func SQLPartOrderByFieldNameDirections(orderbyKeyValues map[string]string, driverName string) (string, error) {
	if len(orderbyKeyValues) == 0 {
		return "", nil
	}

	var orderParts []string

	for field, direction := range orderbyKeyValues {
		formattedPart, err := formatOrderByField(field, direction, driverName)
		if err != nil {
			return "", fmt.Errorf("error formatting ORDER BY for field %s: %w", field, err)
		}
		orderParts = append(orderParts, formattedPart)
	}

	return strings.Join(orderParts, ", "), nil
}

/*
	func SQLPartOrderByFieldNameDirections(orderbyKeyValues map[string]string, driverName string) (s string) {
		orderbyFieldNameDirections := ``
		for k, v := range orderbyKeyValues {
			switch driverName {
			case "oracle":
				k = strings.ToUpper(k)
			}
			if orderbyFieldNameDirections != `` {
				orderbyFieldNameDirections = orderbyFieldNameDirections + `, `
			}
			orderbyFieldNameDirections = orderbyFieldNameDirections + k + ` ` + v
		}
		return orderbyFieldNameDirections
	}
*/
func SQLPartSetFieldNameValues(setKeyValues utils.JSON, driverName string) (newSetKeyValues utils.JSON, s string) {
	setFieldNameValues := ``
	newSetKeyValues = utils.JSON{}
	for k, v := range setKeyValues {
		if setFieldNameValues != `` {
			setFieldNameValues = setFieldNameValues + `,`
		}
		switch v.(type) {
		case SQLExpression:
			setFieldNameValues = setFieldNameValues + v.(SQLExpression).String()
			newSetKeyValues[k] = v
		default:
			switch driverName {
			case "oracle":
				k = strings.ToUpper(k)
			}
			setFieldNameValues = setFieldNameValues + k + `=:NEW_` + k
			newSetKeyValues[`NEW_`+k] = v
		}
	}
	return newSetKeyValues, setFieldNameValues
}

func SQLPartInsertFieldNamesFieldValues(insertKeyValues utils.JSON, driverName string) (fieldNames string, fieldValues string) {
	for k, v := range insertKeyValues {
		switch driverName {
		case "oracle":
			k = strings.ToUpper(k)
		}
		if fieldNames != `` {
			fieldNames = fieldNames + `,`
		}
		fieldNames = fieldNames + k
		if fieldValues != `` {
			fieldValues = fieldValues + `,`
		}
		switch v.(type) {
		case SQLExpression:
			fieldValues = fieldValues + v.(SQLExpression).String()
		default:
			fieldValues = fieldValues + `:` + k
		}
	}
	return fieldNames, fieldValues
}

func SQLPartConstructSelect(driverName string, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections FieldsOrderBy, limit any, forUpdatePart any) (s string, err error) {
	switch driverName {
	case "sqlserver":
		f := SQLPartFieldNames(fieldNames, driverName)
		w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
		effectiveWhere := ``
		if w != `` {
			effectiveWhere = ` where ` + w
		}
		j := ``
		if joinSQLPart != nil {
			j = ` ` + joinSQLPart.(string)
		}
		o, err := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, driverName)
		if err != nil {
			return ``, err
		}
		effectiveOrderBy := ``
		if o != `` {
			effectiveOrderBy = ` order by ` + o
		}
		effectiveLimitAsString := ``
		if limit != nil {
			var limitAsInt64 int64
			switch limit.(type) {
			case int:
				limitAsInt64 = int64(limit.(int))
			case int16:
				limitAsInt64 = int64(limit.(int16))
			case int32:
				limitAsInt64 = int64(limit.(int32))
			case int64:
				limitAsInt64 = limit.(int64)
			default:
				err := errors.New(`SHOULD_NOT_HAPPEN:CANT_CONVERT_LIMIT_TO_INT64`)
				return ``, err
			}
			if limitAsInt64 > 0 {
				effectiveLimitAsString = ` top ` + strconv.FormatInt(limitAsInt64, 10)
			}
		}
		u := ``
		if forUpdatePart == nil {
			forUpdatePart = false
		}
		if forUpdatePart == true {
			u = ` for update `
		}
		s = `select ` + effectiveLimitAsString + ` ` + f + ` from ` + tableName + j + effectiveWhere + effectiveOrderBy + u
		return s, nil
	case "postgres":
		f := SQLPartFieldNames(fieldNames, driverName)
		w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
		effectiveWhere := ``
		if w != `` {
			effectiveWhere = ` where ` + w
		}
		j := ``
		if joinSQLPart != nil {
			j = ` ` + joinSQLPart.(string)
		}
		o, err := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, driverName)
		if err != nil {
			return ``, err
		}

		effectiveOrderBy := ``
		if o != `` {
			effectiveOrderBy = ` order by ` + o
		}
		effectiveLimitAsString := ``
		if limit != nil {
			var limitAsInt64 int64
			switch limit.(type) {
			case int:
				limitAsInt64 = int64(limit.(int))
			case int16:
				limitAsInt64 = int64(limit.(int16))
			case int32:
				limitAsInt64 = int64(limit.(int32))
			case int64:
				limitAsInt64 = limit.(int64)
			default:
				err := errors.New(`SHOULD_NOT_HAPPEN:CANT_CONVERT_LIMIT_TO_INT64`)
				return ``, err
			}
			if limitAsInt64 > 0 {
				effectiveLimitAsString = ` limit ` + strconv.FormatInt(limitAsInt64, 10)
			}
		}
		u := ``
		if forUpdatePart == nil {
			forUpdatePart = false
		}
		if forUpdatePart == true {
			u = ` for update `
		}
		s = `select ` + f + ` from ` + tableName + j + effectiveWhere + effectiveOrderBy + effectiveLimitAsString + u
		return s, nil
	case "oracle":
		f := SQLPartFieldNames(fieldNames, driverName)
		w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
		effectiveWhere := ``
		if w != `` {
			effectiveWhere = ` where ` + w
		}
		j := ``
		if joinSQLPart != nil {
			j = ` ` + joinSQLPart.(string)
		}
		o, err := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, driverName)
		if err != nil {
			return ``, err
		}
		effectiveOrderBy := ``
		if o != `` {
			effectiveOrderBy = ` order by ` + o
		}
		effectiveLimitAsString := ``
		if limit != nil {
			var limitAsInt64 int64
			switch limit.(type) {
			case int:
				limitAsInt64 = int64(limit.(int))
			case int16:
				limitAsInt64 = int64(limit.(int16))
			case int32:
				limitAsInt64 = int64(limit.(int32))
			case int64:
				limitAsInt64 = limit.(int64)
			default:
				err := errors.New(`SHOULD_NOT_HAPPEN:CANT_CONVERT_LIMIT_TO_INT64`)
				return ``, err
			}
			if limitAsInt64 > 0 {
				effectiveLimitAsString = ` FETCH FIRST ` + strconv.FormatInt(limitAsInt64, 10) + ` ROWS ONLY`
			}
		}
		u := ``
		if forUpdatePart == nil {
			forUpdatePart = false
		}
		if forUpdatePart == true {
			u = ` for update `
		}
		s = `select ` + f + ` from ` + tableName + j + effectiveWhere + effectiveOrderBy + effectiveLimitAsString + u
		return s, nil
	default:
		err := errors.New(`UNKNOWN_DATABASE_TYPE:` + driverName)
		return ``, err
	}
}

func NamedQueryRow(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, query string, arg any) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	/*	var argAsArray []any
		switch arg.(type) {
		case map[string]any:
			_, _, argAsArray = PrepareArrayArgs(arg.(map[string]any), db.DriverName())
		}

		stmt, err := db.PrepareNamed(query)
		if err != nil {
			return nil, nil, err
		}
		defer stmt.Close()
		xr, err := stmt.Query(argAsArray)
		if err != nil {
			return nil, nil, err
		}
		rows := xr*/
	switch db.DriverName() {
	case "oracle":
		rowInfo, x, err := _oracleSelectRaw(db, fieldTypeMapping, query, arg)
		if err != nil {
			return nil, nil, err
		}
		if x == nil {
			return rowInfo, nil, err
		}
		if len(x) < 1 {
			return rowInfo, nil, err
		}
		return rowInfo, x[0], err
	}

	err = sqlchecker.CheckAll(db.DriverName(), query, arg)
	if err != nil {
		return nil, nil, fmt.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %w", err)
	}

	rows, err := db.NamedQuery(query, arg)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &RowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return rowsInfo, nil, err
	}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, err
		}
		rowJSON, err = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		return rowsInfo, rowJSON, nil
	}

	return rowsInfo, nil, nil
}

func ShouldNamedQueryRow(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, query string, args any) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = NamedQueryRow(db, fieldTypeMapping, query, args)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New(`ROW_MUST_EXIST:` + query)
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}

func OracleInsertReturning(db *sqlx.DB, tableName string, fieldNameForRowId string, keyValues map[string]interface{}) (int64, error) {
	tableName = strings.ToUpper(tableName)
	fieldNameForRowId = strings.ToUpper(fieldNameForRowId)
	returningClause := fmt.Sprintf("RETURNING %s INTO :new_id", fieldNameForRowId)

	fieldNames, fieldValues, fieldArgs := databaseProtectedUtils.PrepareArrayArgs(keyValues, db.DriverName())

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s", tableName, fieldNames, fieldValues, returningClause)

	stmt, err := db.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = stmt.Close()
	}()

	// Add the returning parameter
	newId := int64(99)
	fieldArgs = append(fieldArgs, sql.Named("new_id", sql.Out{Dest: &newId}))

	err = sqlchecker.CheckAll(db.DriverName(), query, fieldArgs)
	if err != nil {
		return 0, fmt.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %w", err)
	}

	// Execute the statement
	_, err = stmt.Exec(fieldArgs...)
	if err != nil {
		return 0, err
	}

	return newId, nil
}

func OracleDelete(db *sqlx.DB, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	tableName = strings.ToUpper(tableName)
	whereClause := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, db.DriverName())
	if whereClause != `` {
		whereClause = ` WHERE ` + whereClause
	}

	_, _, fieldArgs := databaseProtectedUtils.PrepareArrayArgs(whereAndFieldNameValues, db.DriverName())

	query := fmt.Sprintf("DELETE FROM %s %s", tableName, whereClause)

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = stmt.Close()
	}()

	err = sqlchecker.CheckAll(db.DriverName(), query, fieldArgs)
	if err != nil {
		return nil, fmt.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %w", err)
	}

	// Execute the statement
	r, err = stmt.Exec(fieldArgs...)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func OracleEdit(db *sqlx.DB, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result sql.Result, err error) {
	tableName = strings.ToUpper(tableName)
	setKeyValues, setFieldNameValues := SQLPartSetFieldNameValues(setKeyValues, db.DriverName())
	whereClause := SQLPartWhereAndFieldNameValues(whereKeyValues, db.DriverName())

	_, _, setFieldArgs := databaseProtectedUtils.PrepareArrayArgs(setKeyValues, db.DriverName())
	_, _, setWhereFieldArgs := databaseProtectedUtils.PrepareArrayArgs(whereKeyValues, db.DriverName())

	if whereClause != "" {
		whereClause = ` WHERE ` + whereClause
	}

	for _, v := range setWhereFieldArgs {
		setFieldArgs = append(setFieldArgs, v)
	}

	query := fmt.Sprintf("UPDATE "+tableName+" SET %s %s", setFieldNameValues, whereClause)

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = stmt.Close()
	}()

	err = sqlchecker.CheckAll(db.DriverName(), query, setFieldArgs)
	if err != nil {
		return nil, fmt.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %w", err)
	}

	// Execute the statement
	result, err = stmt.Exec(setFieldArgs...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func _oracleSelectRaw(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, query string, fieldArgs ...any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = stmt.Close()
	}()

	err = sqlchecker.CheckAll(db.DriverName(), query, fieldArgs)
	if err != nil {
		return nil, r, fmt.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %w", err)
	}

	// Execute the statement
	arows, err := stmt.Query(fieldArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = arows.Close()
	}()
	rows := sqlx.Rows{Rows: arows}

	rowsInfo = &RowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, err
	}
	rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return rowsInfo, r, err
	}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, err
		}
		rowJSON, err = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func OracleSelect(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections FieldsOrderBy) (rowsInfo *RowsInfo, r []utils.JSON, err error) {

	tableName = strings.ToUpper(tableName)
	tableName = strings.ToUpper(tableName)
	fieldNamesStr := SQLPartFieldNames(fieldNames, db.DriverName())

	whereClause := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, db.DriverName())
	if whereClause != `` {
		whereClause = ` WHERE ` + whereClause
	}

	orderByClause, err := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, db.DriverName())
	if err != nil {
		return nil, nil, err
	}

	if orderByClause != `` {
		orderByClause = ` order by ` + orderByClause
	}
	limitClause := ""

	_, _, fieldArgs := databaseProtectedUtils.PrepareArrayArgs(whereAndFieldNameValues, db.DriverName())

	query := fmt.Sprintf("SELECT %s from %s %s %s %s", fieldNamesStr, tableName, whereClause, orderByClause, limitClause)

	return _oracleSelectRaw(db, fieldTypeMapping, query, fieldArgs)
	/*stmt, err := db.Prepare(query)
	if err != nil {
		return nil, nil, err
	}
	defer stmt.Close()

	// Execute the statement
	arows, err := stmt.Query(fieldArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = arows.Close()
	}()
	rows := sqlx.Rows{Rows: arows}

	rowsInfo = &RowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, err
	}
	rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return rowsInfo, r, err
	}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, err
		}
		rowJSON = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName())
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil*/
}

func ShouldNamedQueryId(db *sqlx.DB, query string, arg any) (int64, error) {

	err := sqlchecker.CheckAll(db.DriverName(), query, arg)
	if err != nil {
		return 0, fmt.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	rows, err := db.NamedQuery(query, arg)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			fmt.Printf("Error closing rows: %v\n", closeErr)
		}
	}()

	var returningId int64
	if rows.Next() {
		err := rows.Scan(&returningId)
		if err != nil {
			return 0, err
		}
	} else {
		err := errors.New(`NO_ID_RETURNED:` + query)
		return 0, err
	}
	return returningId, nil
}

func NamedQueryRows(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, query string, arg any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	if arg == nil {
		arg = utils.JSON{}
	}

	err = sqlchecker.CheckAll(db.DriverName(), query, arg)
	if err != nil {
		return nil, r, fmt.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	rows, err := db.NamedQuery(query, arg)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &RowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, err
	}
	rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return rowsInfo, r, err
	}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, err
		}
		rowJSON, err = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func QueryRows(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, query string, arg []any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}

	err = sqlchecker.CheckAll(db.DriverName(), query, arg)
	if err != nil {
		return nil, r, fmt.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	rows, err := db.Queryx(query, arg...)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &RowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, err
	}
	rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return rowsInfo, r, err
	}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, err
		}
		rowJSON, err = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

// buildCountQuery generates count SQL based on database type
func buildCountQuery(dbType string, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart string) (string, error) {
	effectiveWherePart := ""
	if whereQueryPart != "" {
		effectiveWherePart = " where " + whereQueryPart
	}

	effectiveJoinPart := ""
	if joinQueryPart != "" {
		effectiveJoinPart = " " + joinQueryPart
	}

	var summaryCalcFields string

	switch dbType {
	case "sqlserver":
		summaryCalcFields = `cast(count(*) as bigint) as "s___total_rows"`
	case "postgres":
		summaryCalcFields = "cast(count(*) as bigint) as s___total_rows"
	case "oracle":
		summaryCalcFields = `count(*) as "s___total_rows"`
	case "mysql":
		summaryCalcFields = "cast(count(*) as signed) as s___total_rows"
	case "db2":
		summaryCalcFields = `cast(count(*) as bigint) as "s___total_rows"`
	default:
		return "", errors.New("UNSUPPORTED_DATABASE_SQL_COUNT")
	}

	if summaryCalcFieldsPart != "" {
		summaryCalcFields += "," + summaryCalcFieldsPart
	}

	return "select " + summaryCalcFields + " from " + fromQueryPart + effectiveWherePart + effectiveJoinPart, nil
}

// ShouldCountQuery executes the count query and returns the total rows and summary
func ShouldCountQuery(dbAppInstance *sqlx.DB, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart string,
	arg any) (totalRows int64, summaryRows utils.JSON, err error) {

	driverName := dbAppInstance.DriverName()
	countSQL, err := buildCountQuery(driverName, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart)
	if err != nil {
		return 0, nil, err
	}

	_, summaryRows, err = ShouldNamedQueryRow(dbAppInstance, nil, countSQL, arg)
	if err != nil {
		return 0, nil, err
	}

	// Handle different database types for total rows extraction
	if driverName == "oracle" {
		totalRowsAsAny, err := utils.ConvertToInterfaceInt64FromAny(summaryRows["s___total_rows"])
		if err != nil {
			return 0, summaryRows, err
		}

		totalRows, ok := totalRowsAsAny.(int64)
		if !ok {
			return 0, summaryRows, errors.New(fmt.Sprintf("CANT_CONVERT_TOTAL_ROWS_TO_INT64:%v", totalRowsAsAny))
		}
		return totalRows, summaryRows, nil
	}

	totalRows = summaryRows["s___total_rows"].(int64)
	return totalRows, summaryRows, nil
}

// QueryPaging updated to use the extracted count query function
func QueryPaging(dbAppInstance *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, summaryCalcFieldsPart string, rowsPerPage int64, pageIndex int64,
	returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg []any) (rowsInfo *RowsInfo, rows []utils.JSON, totalRows int64, totalPage int64, summaryRows utils.JSON, err error) {

	// Execute count query
	totalRows, summaryRows, err = ShouldCountQuery(dbAppInstance, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart, arg)
	if err != nil {
		return nil, nil, 0, 0, nil, err
	}

	if returnFieldsQueryPart == "" {
		returnFieldsQueryPart = "*"
	}

	effectiveWherePart := ""
	if whereQueryPart != "" {
		effectiveWherePart = " where " + whereQueryPart
	}

	effectiveJoinPart := ""
	if joinQueryPart != "" {
		effectiveJoinPart = " " + joinQueryPart
	}

	// Calculate total pages
	if rowsPerPage == 0 {
		totalPage = 1
	} else {
		totalPage = ((totalRows - 1) / rowsPerPage) + 1
	}

	driverName := dbAppInstance.DriverName()

	query := ""
	switch driverName {
	case "sqlserver":
		effectiveLimitPart := ""
		if rowsPerPage > 0 {
			effectiveLimitPart = " offset " + strconv.FormatInt(pageIndex*rowsPerPage, 10) +
				" ROWS FETCH NEXT " + strconv.FormatInt(rowsPerPage, 10) + " ROWS ONLY"
		}

		if orderByQueryPart == "" {
			orderByQueryPart = "1"
		}
		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + " order by " + orderByQueryPart + effectiveLimitPart

	case "postgres":
		effectiveLimitPart := ""
		if rowsPerPage > 0 {
			effectiveLimitPart = " limit " + strconv.FormatInt(rowsPerPage, 10) +
				" offset " + strconv.FormatInt(pageIndex*rowsPerPage, 10)
		}

		effectiveOrderByPart := ""
		if orderByQueryPart != "" {
			effectiveOrderByPart = " order by " + orderByQueryPart
		}

		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + effectiveOrderByPart + effectiveLimitPart

	case "oracle":
		effectiveLimitPart := ""
		if rowsPerPage > 0 {
			effectiveLimitPart = " offset " + strconv.FormatInt(pageIndex*rowsPerPage, 10) +
				" ROWS FETCH NEXT " + strconv.FormatInt(rowsPerPage, 10) + " ROWS ONLY"
		}

		effectiveOrderByPart := ""
		if orderByQueryPart != "" {
			effectiveOrderByPart = " order by " + orderByQueryPart
		}

		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + effectiveOrderByPart + effectiveLimitPart

	default:
		return rowsInfo, rows, 0, 0, summaryRows, errors.New("UNSUPPORTED_DATABASE_SQL_SELECT")
	}

	rowsInfo, rows, err = QueryRows(dbAppInstance, fieldTypeMapping, query, arg)
	if err != nil {
		return rowsInfo, rows, 0, 0, summaryRows, err
	}

	return rowsInfo, rows, totalRows, totalPage, summaryRows, err
}

// NamedQueryPaging updated to use the extracted count query function
func NamedQueryPaging(dbAppInstance *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, summaryCalcFieldsPart string, rowsPerPage int64, pageIndex int64,
	returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg any) (rowsInfo *RowsInfo, rows []utils.JSON, totalRows int64, totalPage int64, summaryRows utils.JSON, err error) {

	// Execute count query
	totalRows, summaryRows, err = ShouldCountQuery(dbAppInstance, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart, arg)
	if err != nil {
		return nil, nil, 0, 0, nil, err
	}

	if returnFieldsQueryPart == "" {
		returnFieldsQueryPart = "*"
	}

	effectiveWherePart := ""
	if whereQueryPart != "" {
		effectiveWherePart = " where " + whereQueryPart
	}

	effectiveJoinPart := ""
	if joinQueryPart != "" {
		effectiveJoinPart = " " + joinQueryPart
	}

	// Calculate total pages
	if rowsPerPage == 0 {
		totalPage = 1
	} else {
		totalPage = ((totalRows - 1) / rowsPerPage) + 1
	}

	driverName := dbAppInstance.DriverName()

	query := ""
	switch driverName {
	case "sqlserver":
		effectiveLimitPart := ""
		if rowsPerPage > 0 {
			effectiveLimitPart = " offset " + strconv.FormatInt(pageIndex*rowsPerPage, 10) +
				" ROWS FETCH NEXT " + strconv.FormatInt(rowsPerPage, 10) + " ROWS ONLY"
		}

		if orderByQueryPart == "" {
			orderByQueryPart = "1"
		}
		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + " order by " + orderByQueryPart + effectiveLimitPart

	case "postgres":
		effectiveLimitPart := ""
		if rowsPerPage > 0 {
			effectiveLimitPart = " limit " + strconv.FormatInt(rowsPerPage, 10) +
				" offset " + strconv.FormatInt(pageIndex*rowsPerPage, 10)
		}

		effectiveOrderByPart := ""
		if orderByQueryPart != "" {
			effectiveOrderByPart = " order by " + orderByQueryPart
		}

		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + effectiveOrderByPart + effectiveLimitPart

	case "oracle":
		effectiveLimitPart := ""
		if rowsPerPage > 0 {
			effectiveLimitPart = " offset " + strconv.FormatInt(pageIndex*rowsPerPage, 10) +
				" ROWS FETCH NEXT " + strconv.FormatInt(rowsPerPage, 10) + " ROWS ONLY"
		}

		effectiveOrderByPart := ""
		if orderByQueryPart != "" {
			effectiveOrderByPart = " order by " + orderByQueryPart
		}

		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + effectiveOrderByPart + effectiveLimitPart

	default:
		return rowsInfo, rows, 0, 0, summaryRows, errors.New("UNSUPPORTED_DATABASE_SQL_SELECT")
	}

	rowsInfo, rows, err = NamedQueryRows(dbAppInstance, fieldTypeMapping, query, arg)
	if err != nil {
		return rowsInfo, rows, 0, 0, summaryRows, err
	}

	return rowsInfo, rows, totalRows, totalPage, summaryRows, err
}

// NamedQueryPagingList updated to use the extracted count query function
func NamedQueryList(dbAppInstance *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping,
	returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg any) (rowsInfo *RowsInfo, rows []utils.JSON, err error) {

	if returnFieldsQueryPart == "" {
		returnFieldsQueryPart = "*"
	}

	effectiveWherePart := ""
	if whereQueryPart != "" {
		effectiveWherePart = " where " + whereQueryPart
	}

	effectiveJoinPart := ""
	if joinQueryPart != "" {
		effectiveJoinPart = " " + joinQueryPart
	}

	driverName := dbAppInstance.DriverName()

	query := ""
	switch driverName {
	case "sqlserver":
		effectiveLimitPart := ""

		if orderByQueryPart == "" {
			orderByQueryPart = "1"
		}
		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + " order by " + orderByQueryPart + effectiveLimitPart

	case "postgres":
		effectiveLimitPart := ""

		effectiveOrderByPart := ""
		if orderByQueryPart != "" {
			effectiveOrderByPart = " order by " + orderByQueryPart
		}

		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + effectiveOrderByPart + effectiveLimitPart

	case "oracle":
		effectiveLimitPart := ""

		effectiveOrderByPart := ""
		if orderByQueryPart != "" {
			effectiveOrderByPart = " order by " + orderByQueryPart
		}

		query = "select " + returnFieldsQueryPart + " from " + fromQueryPart +
			effectiveWherePart + effectiveJoinPart + effectiveOrderByPart + effectiveLimitPart

	default:
		return rowsInfo, rows, errors.New("UNSUPPORTED_DATABASE_SQL_SELECT")
	}

	rowsInfo, rows, err = NamedQueryRows(dbAppInstance, fieldTypeMapping, query, arg)
	if err != nil {
		return rowsInfo, rows, err
	}

	return rowsInfo, rows, err
}

/*
func NamedQueryPaging(dbAppInstance *sqlx.DB, summaryCalcFieldsPart string, rowsPerPage int64, pageIndex int64, returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,

		arg any) (rowsInfo *RowsInfo, rows []utils.JSON, totalRows int64, totalPage int64, summaryRows utils.JSON, err error) {
		if returnFieldsQueryPart == `` {
			returnFieldsQueryPart = `*`
		}
		effectiveWhereQueryPart := ``
		if whereQueryPart != `` {
			effectiveWhereQueryPart = ` where ` + whereQueryPart
		}

		effectiveJoinQueryPart := ``
		if joinQueryPart != `` {
			effectiveJoinQueryPart = ` ` + joinQueryPart
		}

		query := ``
		switch dbAppInstance.DriverName() {
		case "sqlserver":
			summaryCalcFields := `cast(count(*) as bigint) as "s___total_rows"`
			if summaryCalcFieldsPart != `` {
				summaryCalcFields = summaryCalcFields + `,` + summaryCalcFieldsPart
			}
			countSQL := `select ` + summaryCalcFields + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveJoinQueryPart
			_, summaryRows, err = ShouldNamedQueryRow(dbAppInstance, countSQL, arg)
			if err != nil {
				return nil, nil, 0, 0, nil, err
			}

			totalRows = summaryRows[`s___total_rows`].(int64)

			effectiveLimitQueryPart := ``
			if rowsPerPage == 0 {
				totalPage = 1
			} else {
				totalPage = ((totalRows - 1) / rowsPerPage) + 1
				effectiveLimitQueryPart = ` offset ` + strconv.FormatInt(pageIndex*rowsPerPage, 10) + ` ROWS FETCH NEXT ` + strconv.FormatInt(rowsPerPage, 10) + ` ROWS ONLY`
			}

			effectiveOrderByQueryPart := ``
			if orderByQueryPart == `` {
				orderByQueryPart = `1`
			}
			effectiveOrderByQueryPart = ` order by ` + orderByQueryPart

			query = `select ` + returnFieldsQueryPart + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveOrderByQueryPart + effectiveLimitQueryPart
		case "postgres":
			summaryCalcFields := `cast(count(*) as bigint) as s___total_rows`
			if summaryCalcFieldsPart != `` {
				summaryCalcFields = summaryCalcFields + `,` + summaryCalcFieldsPart
			}
			countSQL := `select ` + summaryCalcFields + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveJoinQueryPart
			_, summaryRows, err = ShouldNamedQueryRow(dbAppInstance, countSQL, arg)
			if err != nil {
				return nil, nil, 0, 0, nil, err
			}

			totalRows = summaryRows[`s___total_rows`].(int64)

			effectiveLimitQueryPart := ``
			if rowsPerPage == 0 {
				totalPage = 1
			} else {
				totalPage = ((totalRows - 1) / rowsPerPage) + 1
				effectiveLimitQueryPart = ` limit ` + strconv.FormatInt(rowsPerPage, 10) + ` offset ` + strconv.FormatInt(pageIndex*rowsPerPage, 10)
			}

			effectiveOrderByQueryPart := ``
			if orderByQueryPart != `` {
				effectiveOrderByQueryPart = ` order by ` + orderByQueryPart
			}

			query = `select ` + returnFieldsQueryPart + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveOrderByQueryPart + effectiveLimitQueryPart
		case "oracle":
			summaryCalcFields := `count(*) as "s___total_rows"`
			if summaryCalcFieldsPart != `` {
				summaryCalcFields = summaryCalcFields + `,` + summaryCalcFieldsPart
			}
			countSQL := `select ` + summaryCalcFields + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveJoinQueryPart
			_, summaryRows, err = ShouldNamedQueryRow(dbAppInstance, countSQL, arg)
			if err != nil {
				return nil, nil, 0, 0, nil, err
			}

			totalRowsAsAny, err := utils.ConvertToInterfaceInt64FromAny(summaryRows[`s___total_rows`])
			if err != nil {
				return nil, nil, 0, 0, nil, err
			}

			ok := true
			totalRows, ok = totalRowsAsAny.(int64)
			if !ok {
				return nil, nil, 0, 0, nil, errors.New(fmt.Sprintf(`CANT_CONVERT_TOTAL_ROWS_TO_INT64:%v`, totalRowsAsAny))
			}

			effectiveLimitQueryPart := ``
			if rowsPerPage == 0 {
				totalPage = 1
			} else {
				totalPage = ((totalRows - 1) / rowsPerPage) + 1
				effectiveLimitQueryPart = ` offset ` + strconv.FormatInt(pageIndex*rowsPerPage, 10) + ` ROWS FETCH NEXT ` + strconv.FormatInt(rowsPerPage, 10) + ` ROWS ONLY`
			}

			effectiveOrderByQueryPart := ``
			if orderByQueryPart != `` {
				effectiveOrderByQueryPart = ` order by ` + orderByQueryPart
			}

			query = `select ` + returnFieldsQueryPart + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveOrderByQueryPart + effectiveLimitQueryPart
		default:
			err = errors.New(`UNSUPPORTED_DATABASE_SQL_SELECT`)
			if err != nil {
				return rowsInfo, rows, 0, 0, summaryRows, err
			}
		}

		rowsInfo, rows, err = NamedQueryRows(dbAppInstance, query, arg)
		if err != nil {
			return rowsInfo, rows, 0, 0, summaryRows, err
		}
		return rowsInfo, rows, totalRows, totalPage, summaryRows, err
	}
*/

/*func QueryPaging(dbAppInstance *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, rowsPerPage int64, pageIndex int64, returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg any) (rowsInfo *RowsInfo, rows []utils.JSON, totalRows int64, totalPage int64, err error) {
	if returnFieldsQueryPart == `` {
		returnFieldsQueryPart = `*`
	}
	effectiveWhereQueryPart := ``
	if whereQueryPart != `` {
		effectiveWhereQueryPart = ` where ` + whereQueryPart
	}

	effectiveJoinQueryPart := ``
	if joinQueryPart != `` {
		effectiveJoinQueryPart = ` ` + joinQueryPart
	}

	countSQL := `SELECT COUNT(*) FROM ` + fromQueryPart + effectiveWhereQueryPart + effectiveJoinQueryPart
	totalRows, err = ShouldNamedQueryId(dbAppInstance, countSQL, arg)
	if err != nil {
		return nil, nil, 0, 0, err
	}

	effectiveLimitQueryPart := ``
	if rowsPerPage == 0 {
		totalPage = 1
	} else {
		totalPage = ((totalRows - 1) / rowsPerPage) + 1
		effectiveLimitQueryPart = ` limit ` + strconv.FormatInt(rowsPerPage, 10) + ` offset ` + strconv.FormatInt(pageIndex*rowsPerPage, 10)
	}

	effectiveOrderByQueryPart := ``
	if orderByQueryPart != `` {
		effectiveOrderByQueryPart = ` order by ` + orderByQueryPart
	}

	query := `select ` + returnFieldsQueryPart + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveOrderByQueryPart + effectiveLimitQueryPart
	rowsInfo, rows, err = QueryRows(dbAppInstance, fieldTypeMapping, query, arg)
	if err != nil {
		return rowsInfo, rows, 0, 0, err
	}
	return rowsInfo, rows, totalRows, totalPage, err
}*/

func ShouldSelectWhereId(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, tableName string, idValue int64) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = ShouldNamedQueryRow(db, fieldTypeMapping, `SELECT * FROM `+tableName+` where `+databaseProtectedUtils.FormatIdentifier(`id`, db.DriverName())+`=:id`, utils.JSON{
		`id`: idValue,
	})
	return rowsInfo, r, err
}

/*func SelectOne(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		rowsInfo, rx, err := OracleSelect(db, fieldTypeMapping, tableName,
			fieldNames, whereAndFieldNameValues,
			joinSQLPart,
			orderbyFieldNameDirections)
		if err != nil {
			return rowsInfo, nil, err
		}
		if rx == nil {
			return rowsInfo, nil, err
		}
		if len(rx) < 1 {
			return rowsInfo, nil, err
		}
		return rowsInfo, rx[0], err
	}
	s, err := SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, 1, nil)
	if err != nil {
		return nil, nil, err
	}
	wKV := ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	rowsInfo, r, err = NamedQueryRow(db, fieldTypeMapping, s, wKV)
	return rowsInfo, r, err
}
*/
/*func ShouldSelectOne(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = SelectOne(db, fieldTypeMapping, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New("ROW_MUST_EXIST:" + tableName)
		return rowsInfo, nil, err
	}
	return rowsInfo, r, nil
}*/

func Select(db *sqlx.DB, fieldTypeMapping databaseProtectedUtils.FieldTypeMapping, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections FieldsOrderBy,
	limit any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {

	if fieldNames == nil {
		fieldNames = []string{"*"}
	}
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		rowsInfo, r, err := OracleSelect(db, fieldTypeMapping, tableName,
			fieldNames, whereAndFieldNameValues,
			joinSQLPart,
			orderbyFieldNameDirections)
		return rowsInfo, r, err
	}
	s, err := SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, nil)
	if err != nil {
		return nil, nil, err
	}
	wKV, err := ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	if err != nil {
		return nil, nil, err
	}
	rowsInfo, r, err = NamedQueryRows(db, fieldTypeMapping, s, wKV)
	return rowsInfo, r, err
}

// Count performs a count query with optional field summaries for multiple database types
func Count(db *sqlx.DB, tableName string, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON,
	joinSQLPart any) (totalRows int64, summaryRows utils.JSON, err error) {

	driverName := db.DriverName()

	// Handle Oracle's uppercase requirement
	if driverName == "oracle" {
		tableName = strings.ToUpper(tableName)
		if summaryCalcFieldsPart != "" {
			// Split by comma and handle each field
			fields := strings.Split(summaryCalcFieldsPart, ",")
			for i, field := range fields {
				fields[i] = strings.ToUpper(strings.TrimSpace(field))
			}
			summaryCalcFieldsPart = strings.Join(fields, ",")
		}
	}

	// Prepare where clause
	whereClause := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)

	// Prepare join clause
	joinClause := ""
	if joinSQLPart != nil {
		joinClause = joinSQLPart.(string)
	}

	// Process arguments based on database type
	args, err := ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	if err != nil {
		return 0, nil, err
	}

	// Special handling for different databases
	switch driverName {
	case "sqlserver", "postgres", "oracle", "mysql", "db2":
		totalRows, summaryRows, err = ShouldCountQuery(
			db,
			summaryCalcFieldsPart,
			tableName,
			whereClause,
			joinClause,
			args,
		)
		if err != nil {
			return 0, nil, fmt.Errorf("count query failed for %s: %w", driverName, err)
		}

		// Special handling for Oracle's number types
		if driverName == "oracle" && summaryRows != nil {
			summaryRows = convertOracleTypes(summaryRows)
		}

	default:
		return 0, nil, fmt.Errorf("unsupported database type: %s", driverName)
	}

	return totalRows, summaryRows, nil
}

// convertOracleTypes handles Oracle's specific number types
func convertOracleTypes(rows utils.JSON) utils.JSON {
	result := make(utils.JSON)
	for k, v := range rows {
		switch v := v.(type) {
		case []uint8: // Handle Oracle's raw number type
			if newVal, err := utils.ConvertToInterfaceInt64FromAny(v); err == nil {
				result[k] = newVal
			} else if newVal, err := utils.ConvertToInterfaceFloat64FromAny(v); err == nil {
				result[k] = newVal
			} else {
				result[k] = v
			}
		default:
			result[k] = v
		}
	}
	return result
}

// CountOne performs a count query expecting exactly one row
func CountOne(db *sqlx.DB, tableName string, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON,
	joinSQLPart any) (totalRows int64, summaryRows utils.JSON, err error) {

	totalRows, summaryRows, err = Count(db, tableName, summaryCalcFieldsPart, whereAndFieldNameValues, joinSQLPart)
	if err != nil {
		return 0, nil, err
	}

	if totalRows != 1 {
		return totalRows, summaryRows, fmt.Errorf("expected exactly one row, got %d rows", totalRows)
	}

	return totalRows, summaryRows, nil
}

// ShouldCount performs a count query and ensures at least one row exists
func ShouldCount(db *sqlx.DB, tableName string, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON,
	joinSQLPart any) (totalRows int64, summaryRows utils.JSON, err error) {

	totalRows, summaryRows, err = Count(db, tableName, summaryCalcFieldsPart, whereAndFieldNameValues, joinSQLPart)
	if err != nil {
		return 0, nil, err
	}

	if totalRows == 0 {
		return 0, nil, fmt.Errorf("NO_ROWS_FOUND:%s", tableName)
	}

	return totalRows, summaryRows, nil
}

func Delete(db *sqlx.DB, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		r, err = OracleDelete(db, tableName, whereAndFieldNameValues)
		return r, err
	}
	w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
	s := `DELETE FROM ` + tableName + ` where ` + w
	wKV, err := ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	if err != nil {
		return nil, err
	}

	err = sqlchecker.CheckAll(db.DriverName(), s, wKV)
	if err != nil {
		return nil, fmt.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %w", err)
	}

	r, err = db.NamedExec(s, wKV)
	return r, err
}

func Update(db *sqlx.DB, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result sql.Result, err error) {
	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		result, err = OracleEdit(db, tableName, setKeyValues, whereKeyValues)
		return result, err
	}
	setKeyValues, u := SQLPartSetFieldNameValues(setKeyValues, driverName)
	w := SQLPartWhereAndFieldNameValues(whereKeyValues, driverName)
	joinedKeyValues := MergeMapExcludeSQLExpression(setKeyValues, whereKeyValues, driverName)
	s := `update ` + tableName + ` set ` + u + ` where ` + w

	err = sqlchecker.CheckAll(db.DriverName(), s, joinedKeyValues)
	if err != nil {
		return nil, fmt.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %w", err)
	}

	result, err = db.NamedExec(s, joinedKeyValues)
	return result, err
}

func Insert(db *sqlx.DB, tableName string, fieldNameForRowId string, keyValues utils.JSON) (id int64, err error) {
	s := ``
	driverName := db.DriverName()
	switch driverName {
	case "postgres":
		fn, fv := SQLPartInsertFieldNamesFieldValues(keyValues, driverName)
		s = `INSERT INTO ` + tableName + ` (` + fn + `) VALUES (` + fv + `) RETURNING ` + fieldNameForRowId
	case "sqlserver":
		fn, fv := SQLPartInsertFieldNamesFieldValues(keyValues, driverName)
		s = `INSERT INTO ` + tableName + ` (` + fn + `) OUTPUT INSERTED.` + fieldNameForRowId + ` VALUES (` + fv + `)`
	case "oracle":
		id, err = OracleInsertReturning(db, tableName, fieldNameForRowId, keyValues)
		if err != nil {
			return 0, err
		}
		return id, nil
	default:
		err = errors.New(`UNSUPPORTED_DATABASE_SQL_INSERT`)
		return 0, err
	}
	kv, err := ExcludeSQLExpression(keyValues, driverName)
	if err != nil {
		return 0, err
	}

	id, err = ShouldNamedQueryId(db, s, kv)
	return id, err
}

func XInsert(db *sqlx.DB, tableName string, fieldNameForRowId string, keyValues utils.JSON) (id int64, err error) {
	s := ``
	driverName := db.DriverName()
	switch driverName {
	case "postgres":
		fn, fv := SQLPartInsertFieldNamesFieldValues(keyValues, driverName)
		s = `INSERT INTO ` + tableName + ` (` + fn + `) VALUES (` + fv + `) RETURNING ` + fieldNameForRowId
	case "sqlserver":
		fn, fv := SQLPartInsertFieldNamesFieldValues(keyValues, driverName)
		s = `INSERT INTO ` + tableName + ` (` + fn + `) OUTPUT INSERTED.` + fieldNameForRowId + ` VALUES (` + fv + `)`
	case "oracle":
		fn, fv := SQLPartInsertFieldNamesFieldValues(keyValues, driverName)
		s = `INSERT INTO ` + tableName + ` (` + fn + `) VALUES (` + fv + `) RETURNING ` + fieldNameForRowId
	default:
		err = errors.New(`UNSUPPORTED_DATABASE_SQL_INSERT`)
		return 0, err
	}
	kv, err := ExcludeSQLExpression(keyValues, driverName)
	if err != nil {
		return 0, err
	}

	newQuery, newArgs, err := database_type.ConvertParamsWithMap(s, kv, database_type.StringToDXDatabaseType(driverName))
	if err != nil {
		return 0, err
	}

	newId := int64(0)
	switch driverName {
	case "oracle":
		newQuery = newQuery + ` INTO :` + fieldNameForRowId
		newArgs = append(newArgs, sql.Named(fieldNameForRowId, sql.Out{Dest: &newId}))
	}

	_, r, err := QueryRows(db, nil, newQuery, newArgs)
	if err != nil {
		return 0, err
	}

	switch driverName {
	case "oracle":
		return newId, nil
	}

	if r == nil {
		return 0, errors.New(`NO_ROWS_RETURNED_FROM_INSERT`)
	}
	if len(r) < 1 {
		return 0, errors.New(`NO_ROWS_RETURNED_FROM_INSERT`)
	}
	firstRow := r[0]
	id, ok := firstRow[fieldNameForRowId].(int64)
	if !ok {
		return 0, errors.New(`NO_ID_RETURNED_FROM_INSERT`)
	}
	return id, nil
}
