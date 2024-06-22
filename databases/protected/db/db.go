package db

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"strconv"

	"dxlib/v3/utils"
)

func MergeMapExcludeSQLExpression(m1 utils.JSON, m2 utils.JSON) (r utils.JSON) {
	r = utils.JSON{}
	for k, v := range m1 {
		switch v.(type) {
		case SQLExpression:
			break
		default:
			r[k] = v
		}
	}
	for k, v := range m2 {
		switch v.(type) {
		case SQLExpression:
			break
		default:
			r[k] = v
		}
	}
	return r
}

func ExcludeSQLExpression(kv utils.JSON) (r utils.JSON) {
	r = utils.JSON{}
	for k, v := range kv {
		switch v.(type) {
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

func SQLPartFieldNames(fieldNames []string) (s string) {
	showFieldNames := ``
	if fieldNames == nil {
		return `*`
	}
	for _, v := range fieldNames {
		if showFieldNames != `` {
			showFieldNames = showFieldNames + `, `
		}
		showFieldNames = showFieldNames + v
	}
	return showFieldNames
}

func SQLPartWhereAndFieldNameValues(whereKeyValues utils.JSON) (s string) {
	andFieldNameValues := ``
	for k, v := range whereKeyValues {
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

func SQLPartOrderByFieldNameDirections(orderbyKeyValues map[string]string) (s string) {
	orderbyFieldNameDirections := ``
	for k, v := range orderbyKeyValues {
		if orderbyFieldNameDirections != `` {
			orderbyFieldNameDirections = orderbyFieldNameDirections + `, `
		}
		orderbyFieldNameDirections = orderbyFieldNameDirections + k + ` ` + v
	}
	return orderbyFieldNameDirections
}

func SQLPartSetFieldNameValues(setKeyValues utils.JSON) (s string) {
	setFieldNameValues := ``
	for k, v := range setKeyValues {
		if setFieldNameValues != `` {
			setFieldNameValues = setFieldNameValues + `,`
		}
		switch v.(type) {
		case SQLExpression:
			setFieldNameValues = setFieldNameValues + v.(SQLExpression).String()
		default:
			setFieldNameValues = setFieldNameValues + k + `=:` + k
		}
	}
	return setFieldNameValues
}

func SQLPartInsertFieldNamesFieldValues(insertKeyValues utils.JSON) (fieldNames string, fieldValues string) {
	for k, v := range insertKeyValues {
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
		f := SQLPartFieldNames(fieldNames)
		w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues)
		effectiveWhere := ``
		if w != `` {
			effectiveWhere = ` where ` + w
		}
		j := ``
		if joinSQLPart != nil {
			j = ` ` + joinSQLPart.(string)
		}
		o := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections)
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
				err := errors.New(`LimitCannotConvertToInt64`)
				return ``, err
			}
			if limitAsInt64 > 0 {
				//effectiveLimitQueryPart = ` offset ` + strconv.FormatInt(pageIndex*rowsPerPage, 10) + ` ROWS FETCH NEXT ` + strconv.FormatInt(rowsPerPage, 10) + ` ROWS ONLY`
				effectiveLimitAsString = ` top ` + strconv.FormatInt(limitAsInt64, 10)
				//effectiveLimitAsString = ` limit ` + strconv.FormatInt(limitAsInt64, 10)
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
		f := SQLPartFieldNames(fieldNames)
		w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues)
		effectiveWhere := ``
		if w != `` {
			effectiveWhere = ` where ` + w
		}
		j := ``
		if joinSQLPart != nil {
			j = ` ` + joinSQLPart.(string)
		}
		o := SQLPartOrderByFieldNameDirections(orderbyFieldNameDirections)
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
				err := errors.New(`LimitCannotConvertToInt64`)
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
	default:
		err := errors.New(`UnknownDatabaseType`)
		return ``, err
	}
}

func NamedQueryRow(db *sqlx.DB, query string, arg any) (r utils.JSON, err error) {
	rows, err := db.NamedQuery(query, arg)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, err
		}
		return rowJSON, nil
	}
	return nil, nil
}

func NamedQueryRowMustExist(db *sqlx.DB, query string, args any) (r utils.JSON, err error) {
	r, err = NamedQueryRow(db, query, args)
	if err != nil {
		return nil, err
	}
	if r == nil {
		err = errors.New(`QueryRowMustExist`)
		return nil, err
	}
	return r, nil
}

func NamedQueryIdMustExist(dbAppInstance *sqlx.DB, query string, arg any) (int64, error) {
	rows, err := dbAppInstance.NamedQuery(query, arg)
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
		err := errors.New(`QueryReturnEmpty`)
		return 0, err
	}
	return returningId, nil
}

func NamedQueryRows(dbAppInstance *sqlx.DB, query string, arg any) (r []utils.JSON, err error) {
	r = []utils.JSON{}
	if arg == nil {
		arg = utils.JSON{}
	}

	rows, err := dbAppInstance.NamedQuery(query, arg)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, err
		}
		r = append(r, rowJSON)
	}
	return r, nil
}

func QueryRows(dbAppInstance *sqlx.DB, query string, arg any) (r []utils.JSON, err error) {
	r = []utils.JSON{}
	rows, err := dbAppInstance.Queryx(query, arg)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, err
		}
		r = append(r, rowJSON)
	}
	return r, nil
}

func NamedQueryPaging(dbAppInstance *sqlx.DB, summaryCalcFieldsPart string, rowsPerPage int64, pageIndex int64, returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg any) (rows []utils.JSON, totalRows int64, totalPage int64, summaryRows utils.JSON, err error) {
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

	summaryCalcFields := `count(*) as _total_rows`
	if summaryCalcFieldsPart != `` {
		summaryCalcFields = summaryCalcFields + `,` + summaryCalcFieldsPart
	}
	countSQL := `select ` + summaryCalcFields + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveJoinQueryPart
	summaryRows, err = NamedQueryRowMustExist(dbAppInstance, countSQL, arg)
	if err != nil {
		return nil, 0, 0, nil, err
	}

	totalRows = summaryRows[`_total_rows`].(int64)

	query := ``
	switch dbAppInstance.DriverName() {
	case "sqlserver":
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
		if orderByQueryPart != `` {
			effectiveOrderByQueryPart = ` order by ` + orderByQueryPart
		}

		query = `select ` + returnFieldsQueryPart + ` from ` + fromQueryPart + effectiveWhereQueryPart + effectiveOrderByQueryPart + effectiveLimitQueryPart
	case "postgres":
	default:
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
	}

	rows, err = NamedQueryRows(dbAppInstance, query, arg)
	if err != nil {
		return nil, 0, 0, summaryRows, err
	}
	return rows, totalRows, totalPage, summaryRows, err
}

func QueryPaging(dbAppInstance *sqlx.DB, rowsPerPage int64, pageIndex int64, returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg any) (rows []utils.JSON, totalRows int64, totalPage int64, err error) {
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
	totalRows, err = NamedQueryIdMustExist(dbAppInstance, countSQL, arg)
	if err != nil {
		return nil, 0, 0, err
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
	rows, err = QueryRows(dbAppInstance, query, arg)
	if err != nil {
		return nil, 0, 0, err
	}
	return rows, totalRows, totalPage, err
}

func SelectWhereIdMustExist(db *sqlx.DB, tableName string, idValue int64) (r utils.JSON, err error) {
	r, err = NamedQueryRowMustExist(db, `SELECT * FROM `+tableName+` where id=:id`, utils.JSON{
		`id`: idValue,
	})
	return r, err
}

func SelectOne(db *sqlx.DB, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string) (r utils.JSON, err error) {
	s, err := SQLPartConstructSelect(db.DriverName(), tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, 1, nil)
	if err != nil {
		return nil, err
	}
	wKV := ExcludeSQLExpression(whereAndFieldNameValues)
	r, err = NamedQueryRow(db, s, wKV)
	return r, err
}

func SelectOneMustExist(db *sqlx.DB, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string) (r utils.JSON, err error) {
	r, err = SelectOne(db, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
	if err != nil {
		return nil, err
	}
	if r == nil {
		err = errors.New("RowNotFoundIn:" + tableName)
		return nil, err
	}
	return r, err
}

func Select(db *sqlx.DB, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections map[string]string,
	limit any) (r []utils.JSON, err error) {
	s, err := SQLPartConstructSelect(db.DriverName(), tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, nil)
	if err != nil {
		return nil, err
	}
	wKV := ExcludeSQLExpression(whereAndFieldNameValues)
	r, err = NamedQueryRows(db, s, wKV)
	return r, err
}

func DeleteWhereKeyValues(db *sqlx.DB, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	w := SQLPartWhereAndFieldNameValues(whereAndFieldNameValues)
	s := `DELETE FROM ` + tableName + ` where ` + w
	wKV := ExcludeSQLExpression(whereAndFieldNameValues)
	r, err = db.NamedExec(s, wKV)
	return r, err
}

func UpdateWhereKeyValues(db *sqlx.DB, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result sql.Result, err error) {
	u := SQLPartSetFieldNameValues(setKeyValues)
	w := SQLPartWhereAndFieldNameValues(whereKeyValues)
	joinedKeyValues := MergeMapExcludeSQLExpression(setKeyValues, whereKeyValues)
	s := `update ` + tableName + ` set ` + u + ` where ` + w
	result, err = db.NamedExec(s, joinedKeyValues)
	return result, err
}

func Insert(db *sqlx.DB, tableName string, keyValues utils.JSON) (id int64, err error) {
	fn, fv := SQLPartInsertFieldNamesFieldValues(keyValues)
	s := ``
	switch db.DriverName() {
	case "postgres":
		s = `INSERT INTO ` + tableName + ` (` + fn + `) VALUES (` + fv + `) RETURNING id`
	case "sqlserver":
		s = `INSERT INTO ` + tableName + ` (` + fn + `) OUTPUT INSERTED.id VALUES (` + fv + `)`
	default:
		fmt.Println("Unknown database type. Using Postgresql Dialect")
		s = `INSERT INTO ` + tableName + ` (` + fn + `) values (` + fv + `) returning id`
	}
	kv := ExcludeSQLExpression(keyValues)
	id, err = NamedQueryIdMustExist(db, s, kv)
	return id, err
}
