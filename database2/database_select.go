package database2

import (
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/database2/db"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
)

func (d *DXDatabase) Select(tableName string, fieldTypeMapping utils2.FieldTypeMapping, showFieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy,
	limit any, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, resultDataRows []utils.JSON, err error) {

	err = d.EnsureConnection()
	if err != nil {
		return nil, nil, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		rowsInfo, resultDataRows, err = db.Select(d.Connection, fieldTypeMapping, tableName, showFieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause,
			orderByFieldNameDirections, limit, offset, forUpdatePart)
		if err == nil {
			return rowsInfo, resultDataRows, nil
		}
		log.Log.Warnf("SELECT_ERROR:%s=%v", tableName, err.Error())
		if !utils2.IsConnectionError(err) {
			return nil, nil, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:%s", err.Error())
		}
	}
	return nil, nil, err
}

func (d *DXDatabase) SelectOne(tableName string, fieldTypeMapping utils2.FieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, resultDataRow utils.JSON, err error) {

	rowsInfo, rr, err := d.Select(tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause, orderByFieldNameDirections, 1, offset, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	if len(rr) == 0 {
		return nil, nil, nil
	}
	return rowsInfo, rr[0], nil
}

func (d *DXDatabase) ShouldSelectOne(tableName string, fieldTypeMapping utils2.FieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdatePart any) (
	rowsInfo *database_type.RowsInfo, resultDataRow utils.JSON, err error) {

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
		log.Log.Warnf("COUNT_ERROR:%s=%v", tableName, err.Error())
		if !IsConnectionError(err) {
			return 0, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:TRY_COUNT=%d,MSG=%s", tryCount, err.Error())
		}
	}
	return 0, err
}

func (d *DXDatabase) SelectPaging(pageIndex int64, rowsPerPage int64, tableName string, fieldTypeMapping utils2.FieldTypeMapping, showFieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections utils2.FieldsOrderBy,
	limit any, offset any, forUpdatePart any) (totalRowCount int64, rowsInfo *database_type.RowsInfo, resultDataRows []utils.JSON, err error) {

	err = d.EnsureConnection()
	if err != nil {
		return 0, nil, nil, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		totalRowCount, rowsInfo, resultDataRows, err = db.SelectPaging(d.Connection, pageIndex, rowsPerPage, fieldTypeMapping, tableName, showFieldNames, whereAndFieldNameValues, joinSQLPart,
			groupBy, havingClause, orderByFieldNameDirections, limit, offset)
		if err == nil {
			return 0, nil, nil, err
		}
		log.Log.Warnf("COUNT_ERROR:%s=%v", tableName, err.Error())
		if !IsConnectionError(err) {
			return 0, nil, nil, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:TRY_COUNT=%d,MSG=%s", tryCount, err.Error())
		}
	}
	return totalRowCount, rowsInfo, resultDataRows, err
}
