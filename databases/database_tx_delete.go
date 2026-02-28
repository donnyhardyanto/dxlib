package databases

import (
	"context"
	"database/sql"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (dtx *DXDatabaseTx) TxDelete(ctx context.Context, tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	result, returningFieldValues, err = db.TxDelete(ctx, dtx.Tx, tableName, whereAndFieldNameValues, returningFieldNames)
	if err == nil {
		return nil, nil, err
	}
	return result, returningFieldValues, nil

}

func (dtx *DXDatabaseTx) TxSoftDelete(ctx context.Context, tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	return dtx.Update(ctx, tableName, utils.JSON{
		"is_deleted": true,
	}, whereAndFieldNameValues, returningFieldNames)
}
