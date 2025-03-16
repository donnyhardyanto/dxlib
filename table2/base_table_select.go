package table

import (
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (bt *DXBaseTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections utils2.FieldsOrderBy, limit any, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, r []utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = bt.Database.Select(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, offset, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (bt *DXBaseTable) ShouldSelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdate any) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	return bt.Database.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, nil, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdate any) (
	rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return bt.Database.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, offset, forUpdate)
}
