package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXTable Auto Builder Methods — adds is_deleted=false filter for SELECT, then delegates to DXRawTable.

// TxSelectWithBuilderAuto adds is_deleted=false filter and delegates to DXRawTable.TxSelectWithBuilderAuto.
func (t *DXTable) TxSelectWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	tqb.Eq("is_deleted", false)
	return t.DXRawTable.TxSelectWithBuilderAuto(dtx, tqb)
}

// TxSelectOneWithBuilderAuto adds is_deleted=false filter and returns a single row with encryption support.
func (t *DXTable) TxSelectOneWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	tqb.Eq("is_deleted", false)
	return t.DXRawTable.TxSelectOneWithBuilderAuto(dtx, tqb)
}

// TxShouldSelectOneWithBuilderAuto adds is_deleted=false filter and returns a single row or RECORD_NOT_FOUND error.
func (t *DXTable) TxShouldSelectOneWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	tqb.Eq("is_deleted", false)
	rowsInfo, row, err := t.DXRawTable.TxSelectOneWithBuilderAuto(dtx, tqb)
	if err != nil {
		return nil, nil, err
	}
	if row == nil {
		return nil, nil, errors.New("RECORD_NOT_FOUND")
	}
	return rowsInfo, row, nil
}

// TxCountWithBuilderAuto adds is_deleted=false filter and returns total row count with encryption support.
func (t *DXTable) TxCountWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (int64, error) {
	tqb.Eq("is_deleted", false)
	return t.DXRawTable.TxCountWithBuilderAuto(dtx, tqb)
}

// TxInsertWithBuilderAuto delegates to DXRawTable.TxInsertWithBuilderAuto.
func (t *DXTable) TxInsertWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableInsertQueryBuilder) (sql.Result, utils.JSON, error) {
	return t.DXRawTable.TxInsertWithBuilderAuto(dtx, tqb)
}

// TxUpdateWithBuilderAuto delegates to DXRawTable.TxUpdateWithBuilderAuto.
func (t *DXTable) TxUpdateWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	return t.DXRawTable.TxUpdateWithBuilderAuto(dtx, tqb)
}

// TxUpdateByIdWithBuilderAuto delegates to DXRawTable.TxUpdateByIdWithBuilderAuto.
func (t *DXTable) TxUpdateByIdWithBuilderAuto(dtx *databases.DXDatabaseTx, id int64, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	return t.DXRawTable.TxUpdateByIdWithBuilderAuto(dtx, id, tqb)
}

// TxDeleteWithBuilderAuto delegates to DXRawTable.TxDeleteWithBuilderAuto.
func (t *DXTable) TxDeleteWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	return t.DXRawTable.TxDeleteWithBuilderAuto(dtx, tqb)
}

// TxDeleteByIdWithBuilderAuto delegates to DXRawTable.TxDeleteByIdWithBuilderAuto.
func (t *DXTable) TxDeleteByIdWithBuilderAuto(dtx *databases.DXDatabaseTx, id int64, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	return t.DXRawTable.TxDeleteByIdWithBuilderAuto(dtx, id, tqb)
}
