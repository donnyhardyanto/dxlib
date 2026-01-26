package db

import (
	"fmt"
	"strconv"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// NamedQueryRows executes a named query and returns all matching rows
// It supports both named parameters (map/struct) and positional parameters (slice)
func NamedQueryRows(db *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	if arg == nil {
		arg = utils.JSON{}
	}

	dbt := base.StringToDXDatabaseType(db.DriverName())
	err = CheckAll(dbt, query, arg)
	if err != nil {
		return nil, nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v=%s +%v", err, query, arg)
	}

	// Check if arg is a slice (positional parameters) or map/struct (named parameters)
	var rows *sqlx.Rows
	switch v := arg.(type) {
	case []any:
		// Positional parameters - use Queryx
		if len(v) == 0 {
			rows, err = db.Queryx(query)
		} else {
			rows, err = db.Queryx(query, v...)
		}
	default:
		// Named parameters - use NamedQuery
		rows, err = db.NamedQuery(query, arg)
	}
	if err != nil {
		return nil, nil, errors.Wrapf(err, "NAMED_QUERY_ROWS_ERROR:QUERY=%s", query)
	}
	defer func() {
		_ = rows.Close()
	}()

	rowsInfo = &DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, errors.Wrapf(err, "NAMED_QUERY_ROWS_COLUMNS_ERROR:QUERY=%s", query)
	}

	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "NAMED_QUERY_ROWS_SCAN_ERROR:QUERY=%s", query)
		}
		rowJSON, err = DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "NAMED_QUERY_ROWS_DEFORMAT_ERROR:QUERY=%s", query)
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

// NamedQueryRow executes a named query and returns a single row
func NamedQueryRow(db *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg any) (rowsInfo *DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, rows, err := NamedQueryRows(db, fieldTypeMapping, query, arg)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// TxNamedQueryRows executes a named query within a transaction and returns all matching rows
// It supports both named parameters (map/struct) and positional parameters (slice)
func TxNamedQueryRows(tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	if arg == nil {
		arg = utils.JSON{}
	}

	dbt := base.StringToDXDatabaseType(tx.DriverName())
	err = CheckAll(dbt, query, arg)
	if err != nil {
		return nil, nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v=%s +%v", err, query, arg)
	}

	// Check if arg is a slice (positional parameters) or map/struct (named parameters)
	var rows *sqlx.Rows
	switch v := arg.(type) {
	case []any:
		// Positional parameters - use Queryx
		if len(v) == 0 {
			rows, err = tx.Queryx(query)
		} else {
			rows, err = tx.Queryx(query, v...)
		}
	default:
		// Named parameters - use NamedQuery
		rows, err = tx.NamedQuery(query, arg)
	}
	if err != nil {
		return nil, nil, errors.Wrapf(err, "TX_NAMED_QUERY_ROWS_ERROR:QUERY=%s", query)
	}
	defer func() {
		_ = rows.Close()
	}()

	rowsInfo = &DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, errors.Wrapf(err, "TX_NAMED_QUERY_ROWS_COLUMNS_ERROR:QUERY=%s", query)
	}

	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "TX_NAMED_QUERY_ROWS_SCAN_ERROR:QUERY=%s", query)
		}
		rowJSON, err = DeformatKeys(rowJSON, tx.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "TX_NAMED_QUERY_ROWS_DEFORMAT_ERROR:QUERY=%s", query)
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

// TxNamedQueryRow executes a named query within a transaction and returns a single row
func TxNamedQueryRow(tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg any) (rowsInfo *DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, rows, err := TxNamedQueryRows(tx, fieldTypeMapping, query, arg)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// NamedQueryList executes a named list query (for export) and returns all matching rows
func NamedQueryList(dbAppInstance *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, returnFieldsQueryPart string, fromQueryPart string,
	whereQueryPart string, joinQueryPart string, orderByQueryPart string, arg any) (rowsInfo *DXDatabaseTableRowsInfo, rows []utils.JSON, err error) {

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

	effectiveOrderByPart := ""
	if orderByQueryPart != "" {
		effectiveOrderByPart = " order by " + orderByQueryPart
	}

	query := "select " + returnFieldsQueryPart + " from " + fromQueryPart +
		effectiveWherePart + effectiveJoinPart + effectiveOrderByPart

	return NamedQueryRows(dbAppInstance, fieldTypeMapping, query, arg)
}

// ShouldNamedQueryRow executes a named query and returns a single row, erroring if no row found
func ShouldNamedQueryRow(db *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, args any) (rowsInfo *DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = NamedQueryRow(db, fieldTypeMapping, query, args)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New("ROW_MUST_EXIST:" + query)
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}

// TxShouldNamedQueryRow executes a named query within a transaction and returns a single row, erroring if no row found
func TxShouldNamedQueryRow(tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, args any) (rowsInfo *DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = TxNamedQueryRow(tx, fieldTypeMapping, query, args)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New("ROW_MUST_EXIST:" + query)
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}

// buildCountQuery builds a count query string for different database types
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

// ShouldNamedCountQuery executes a count query with named parameters and returns total rows
func ShouldNamedCountQuery(dbAppInstance *sqlx.DB, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart string,
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

// TxShouldNamedCountQuery executes a count query within a transaction and returns total rows
func TxShouldNamedCountQuery(tx *sqlx.Tx, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart string,
	arg any) (totalRows int64, summaryRows utils.JSON, err error) {

	driverName := tx.DriverName()
	countSQL, err := buildCountQuery(driverName, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart)
	if err != nil {
		return 0, nil, err
	}

	_, summaryRows, err = TxShouldNamedQueryRow(tx, nil, countSQL, arg)
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

// NamedQueryPaging executes a paging query with named parameters
func NamedQueryPaging(dbAppInstance *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, summaryCalcFieldsPart string, rowsPerPage int64, pageIndex int64,
	returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg any) (rowsInfo *DXDatabaseTableRowsInfo, rows []utils.JSON, totalRows int64, totalPage int64, summaryRows utils.JSON, err error) {

	// Execute count query
	totalRows, summaryRows, err = ShouldNamedCountQuery(dbAppInstance, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart, arg)
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

// TxNamedQueryPaging executes a paging query within a transaction
func TxNamedQueryPaging(tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, summaryCalcFieldsPart string, rowsPerPage int64, pageIndex int64,
	returnFieldsQueryPart string, fromQueryPart string, whereQueryPart string, joinQueryPart string, orderByQueryPart string,
	arg any) (rowsInfo *DXDatabaseTableRowsInfo, rows []utils.JSON, totalRows int64, totalPage int64, summaryRows utils.JSON, err error) {

	// Execute count query
	totalRows, summaryRows, err = TxShouldNamedCountQuery(tx, summaryCalcFieldsPart, fromQueryPart, whereQueryPart, joinQueryPart, arg)
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

	driverName := tx.DriverName()

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

	rowsInfo, rows, err = TxNamedQueryRows(tx, fieldTypeMapping, query, arg)
	if err != nil {
		return rowsInfo, rows, 0, 0, summaryRows, err
	}

	return rowsInfo, rows, totalRows, totalPage, summaryRows, err
}
