package databases

import (
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (d *DXDatabase) Select(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, showFieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy,
	limit any, offset any, forUpdatePart any) (rowsInfo *db.DXDatabaseTableRowsInfo, resultDataRows []utils.JSON, err error) {

	err = d.EnsureConnection()
	if err != nil {
		return nil, nil, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		rowsInfo, resultDataRows, err = db.Select(d.Connection, tableName, fieldTypeMapping, showFieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause,
			orderByFieldNameDirections, limit, offset, forUpdatePart)
		if err == nil {
			return rowsInfo, resultDataRows, nil
		}
		if !db.IsConnectionError(err) {
			log.Log.Errorf(err, "SELECT_ERROR:%s=%+v", tableName, err)
			return nil, nil, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:%+v", err)
		}
	}
	return nil, nil, err
}

func (d *DXDatabase) SelectOne(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, offset any, forUpdatePart any) (rowsInfo *db.DXDatabaseTableRowsInfo, resultDataRow utils.JSON, err error) {

	rowsInfo, rr, err := d.Select(tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause, orderByFieldNameDirections, 1, offset, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	if len(rr) == 0 {
		return nil, nil, nil
	}
	return rowsInfo, rr[0], nil
}

func (d *DXDatabase) ShouldSelectOne(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, offset any, forUpdatePart any) (
	rowsInfo *db.DXDatabaseTableRowsInfo, resultDataRow utils.JSON, err error) {

	rowsInfo, resultDataRow, err = d.SelectOne(tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause,
		orderByFieldNameDirections, offset, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	if resultDataRow == nil {
		return nil, nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", tableName)
	}
	return rowsInfo, resultDataRow, err
}

func (d *DXDatabase) Count(tableName string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (count int64, err error) {
	err = d.EnsureConnection()
	if err != nil {
		return 0, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		count, err = db.Count(d.Connection, tableName, "", whereAndFieldNameValues, joinSQLPart, nil, "", "")
		if err == nil {
			return count, nil
		}
		log.Log.Warnf("COUNT_ERROR:%s=%+v", tableName, err)
		if !IsConnectionError(err) {
			return 0, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:TRY_COUNT=%d,ERR=%+v", tryCount, err)
		}
	}
	return 0, err
}

func (d *DXDatabase) SelectPaging(pageIndex int64, rowsPerPage int64, tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy) (totalRowCount int64, rowsInfo *db.DXDatabaseTableRowsInfo, resultDataRows []utils.JSON, err error) {

	err = d.EnsureConnection()
	if err != nil {
		return 0, nil, nil, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		totalRowCount, rowsInfo, resultDataRows, err = db.SelectPaging(d.Connection, pageIndex, rowsPerPage, tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart,
			groupBy, havingClause, orderByFieldNameDirections)
		if err == nil {
			return 0, nil, nil, err
		}
		log.Log.Warnf("PAGING_ERROR:%s=%+v", tableName, err)
		if !IsConnectionError(err) {
			return 0, nil, nil, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:TRY_COUNT=%d,ERR=%+v", tryCount, err)
		}
	}
	return totalRowCount, rowsInfo, resultDataRows, err
}
