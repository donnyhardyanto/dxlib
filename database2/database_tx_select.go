package database2

import (
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/database2/db"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
)

func (dtx *DXDatabaseTx) Select(tableName string, fieldTypeMapping utils2.FieldTypeMapping, showFieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections utils2.FieldsOrderBy,
	limit any, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, resultData []utils.JSON, err error) {

	rowsInfo, resultData, err = db.TxSelect(dtx.Tx, fieldTypeMapping, tableName, showFieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, offset, forUpdatePart)
	if err != nil {
		return rowsInfo, resultData, err
	}

	return rowsInfo, resultData, nil
}

func (dtx *DXDatabaseTx) Count(tableName string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (count int64, err error) {

	count, err = db.TxCount(dtx.Tx, tableName, "COUNT(*)", whereAndFieldNameValues, joinSQLPart, nil, "", "")
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (dtx *DXDatabaseTx) SelectOne(tableName string, fieldTypeMapping utils2.FieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {

	rowsInfo, rr, err := dtx.Select(tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, 1, offset, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	if len(rr) == 0 {
		return nil, nil, nil
	}
	return rowsInfo, rr[0], nil
}

func (dtx *DXDatabaseTx) ShouldSelectOne(tableName string, fieldTypeMapping utils2.FieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdatePart any) (
	rowsInfo *database_type.RowsInfo, resultData utils.JSON, err error) {

	rowsInfo, resultData, err = dtx.SelectOne(tableName, fieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, offset, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	if resultData == nil {
		return nil, nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", tableName)
	}
	return rowsInfo, resultData, err
}
