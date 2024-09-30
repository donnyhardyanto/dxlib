package db

import (
	"database/sql"
	"errors"
	"fmt"
	databaseProtectedUtils "github.com/donnyhardyanto/dxlib/database/protected/utils"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"strconv"
	"strings"
)

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

func ExcludeSQLExpression(kv utils.JSON, driverName string) (r utils.JSON) {
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
		default:
			r[k] = v
		}
	}
	return r
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

func SQLPartWhereAndFieldNameValues(whereKeyValues utils.JSON, driverName string) (s string) {
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
}

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
	orderbyFieldNameDirections map[string]string, limit any, forUpdatePart any) (s string, err error) {
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
		o := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, driverName)
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
		o := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, driverName)
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
		o := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, driverName)
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

func NamedQueryRow(db *sqlx.DB, query string, arg any) (rowsInfo *RowsInfo, r utils.JSON, err error) {
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
		rowInfo, x, err := _oracleSelectRaw(db, query, arg)
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
		rowJSON = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName())
		return rowsInfo, rowJSON, nil
	}

	return rowsInfo, nil, nil
}

func ShouldNamedQueryRow(db *sqlx.DB, query string, args any) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = NamedQueryRow(db, query, args)
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
	defer stmt.Close()

	// Add the returning parameter
	newId := int64(99)
	fieldArgs = append(fieldArgs, sql.Named("new_id", sql.Out{Dest: &newId}))

	// Execute the statement
	_, err = stmt.Exec(fieldArgs...)
	if err != nil {
		return 0, err
	}

	return newId, nil
}

func OracleDelete(ddb *sqlx.DB, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	tableName = strings.ToUpper(tableName)
	whereClause := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, ddb.DriverName())
	if whereClause != `` {
		whereClause = ` WHERE ` + whereClause
	}

	_, _, fieldArgs := databaseProtectedUtils.PrepareArrayArgs(whereAndFieldNameValues, ddb.DriverName())

	query := fmt.Sprintf("DELETE FROM %s %s", tableName, whereClause)

	stmt, err := ddb.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

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
	defer stmt.Close()

	// Execute the statement
	result, err = stmt.Exec(setFieldArgs...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func _oracleSelectRaw(db *sqlx.DB, query string, fieldArgs ...any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {
	stmt, err := db.Prepare(query)
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
	return rowsInfo, r, nil
}

func OracleSelect(db *sqlx.DB, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string) (rowsInfo *RowsInfo, r []utils.JSON, err error) {

	tableName = strings.ToUpper(tableName)
	tableName = strings.ToUpper(tableName)
	fieldNamesStr := SQLPartFieldNames(fieldNames, db.DriverName())

	whereClause := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, db.DriverName())
	if whereClause != `` {
		whereClause = ` WHERE ` + whereClause
	}

	orderByClause := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections, db.DriverName())
	if orderByClause != `` {
		orderByClause = ` order by ` + orderByClause
	}
	limitClause := ""

	_, _, fieldArgs := databaseProtectedUtils.PrepareArrayArgs(whereAndFieldNameValues, db.DriverName())

	query := fmt.Sprintf("SELECT %s from %s %s %s %s", fieldNamesStr, tableName, whereClause, orderByClause, limitClause)

	return _oracleSelectRaw(db, query, fieldArgs)
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
	rows, err := db.NamedQuery(query, arg)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rows.Close()
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

func NamedQueryRows(db *sqlx.DB, query string, arg any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	if arg == nil {
		arg = utils.JSON{}
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
		rowJSON = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName())
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func QueryRows(db *sqlx.DB, query string, arg any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	rows, err := db.Queryx(query, arg)
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
		rowJSON = databaseProtectedUtils.DeformatKeys(rowJSON, db.DriverName())
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

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

func QueryPaging(dbAppInstance *sqlx.DB, rowsPerPage int64, pageIndex int64, returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
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
	rowsInfo, rows, err = QueryRows(dbAppInstance, query, arg)
	if err != nil {
		return rowsInfo, rows, 0, 0, err
	}
	return rowsInfo, rows, totalRows, totalPage, err
}

func ShouldSelectWhereId(db *sqlx.DB, tableName string, idValue int64) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = ShouldNamedQueryRow(db, `SELECT * FROM `+tableName+` where `+databaseProtectedUtils.FormatIdentifier(`id`, db.DriverName())+`=:id`, utils.JSON{
		`id`: idValue,
	})
	return rowsInfo, r, err
}

func SelectOne(db *sqlx.DB, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		rowsInfo, rx, err := OracleSelect(db, tableName,
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
	rowsInfo, r, err = NamedQueryRow(db, s, wKV)
	return rowsInfo, r, err
}

func ShouldSelectOne(db *sqlx.DB, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string) (rowsInfo *RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = SelectOne(db, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New("ROW_MUST_EXIST:" + tableName)
		return rowsInfo, nil, err
	}
	return rowsInfo, r, nil
}

func Select(db *sqlx.DB, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections map[string]string,
	limit any) (rowsInfo *RowsInfo, r []utils.JSON, err error) {
	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		rowsInfo, r, err := OracleSelect(db, tableName,
			fieldNames, whereAndFieldNameValues,
			joinSQLPart,
			orderbyFieldNameDirections)
		return rowsInfo, r, err
	}
	s, err := SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, nil)
	if err != nil {
		return nil, nil, err
	}
	wKV := ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	rowsInfo, r, err = NamedQueryRows(db, s, wKV)
	return rowsInfo, r, err
}

func DeleteWhereKeyValues(db *sqlx.DB, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		r, err = OracleDelete(db, tableName, whereAndFieldNameValues)
		return r, err
	}
	w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
	s := `DELETE FROM ` + tableName + ` where ` + w
	wKV := ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	r, err = db.NamedExec(s, wKV)
	return r, err
}

func UpdateWhereKeyValues(db *sqlx.DB, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result sql.Result, err error) {
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
	kv := ExcludeSQLExpression(keyValues, driverName)
	id, err = ShouldNamedQueryId(db, s, kv)
	return id, err
}
