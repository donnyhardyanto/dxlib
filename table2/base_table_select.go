package table2

import (
	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (bt *DXBaseTable2) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, limit any, offset any, forUpdatePart any) (rowsInfo *db.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	// Ensure database2 is initialized
	if err := bt.DbEnsureInitializeConnection(); err != nil {
		return nil, nil, err
	}

	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return nil, nil, err
		}
	}

	rowsInfo, r, err = bt.Database.Select(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause, orderByFieldNameDirections, limit, offset, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (bt *DXBaseTable2) Count(log *log.DXLog, whereAndFieldNameValues utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON) (count int64, err error) {

	// Ensure database2 is initialized
	if err := bt.DbEnsureInitializeConnection(); err != nil {
		return 0, err
	}

	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return 0, err
		}
	}

	count, err = bt.Database.Count(bt.ListViewNameId, whereAndFieldNameValues, joinSQLPart)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (bt *DXBaseTable2) ShouldSelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, offset any, forUpdate any) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	// Ensure database2 is initialized
	if err := bt.DbEnsureInitializeConnection(); err != nil {
		return nil, nil, err
	}

	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return nil, nil, err
		}
	}

	return bt.Database.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable2) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, offset any, forUpdate any) (
	rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	// Ensure database2 is initialized
	if err := bt.DbEnsureInitializeConnection(); err != nil {
		return nil, nil, err
	}

	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return nil, nil, err
		}
	}

	return bt.Database.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable2) TxSelect(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, limit any, offset any, forUpdatePart any) (rowsInfo *db.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return nil, nil, err
		}
	}
	return tx.Select(bt.ListViewNameId, bt.FieldTypeMapping, nil, whereAndFieldNameValues, nil, groupBy, havingClause, orderByFieldNameDirections, limit, offset, forUpdatePart)
}

func (bt *DXBaseTable2) TxCount(tx *database2.DXDatabaseTx, whereAndFieldNameValues utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON) (count int64, err error) {

	// Ensure database2 is initialized
	if err := bt.DbEnsureInitializeConnection(); err != nil {
		return 0, err
	}

	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return 0, err
		}
	}

	count, err = tx.Count(bt.ListViewNameId, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (bt *DXBaseTable2) TxSelectOne(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, offset any,
	forUpdate any) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return nil, nil, err
		}
	}

	return tx.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, nil, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable2) TxShouldSelectOne(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	groupBy []string, havingClause utils.JSON, orderByFieldNameDirections db.DXDatabaseTableFieldsOrderBy, offset any, forUpdate any) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.DoOverrideSelectValues != nil {
		whereAndFieldNameValues, err = bt.DoOverrideSelectValues(whereAndFieldNameValues)
		if err != nil {
			return nil, nil, err
		}
	}
	return tx.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, groupBy, havingClause, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable2) IsFieldValueExistAsString(log *log.DXLog, fieldName string, fieldValue string) (bool, error) {
	_, r, err := bt.SelectOne(log, nil, utils.JSON{
		fieldName: fieldValue,
	}, nil, nil, nil, nil, nil, nil)
	if err != nil {
		return false, err
	}
	if r == nil {
		return false, nil
	}
	return true, nil
}
