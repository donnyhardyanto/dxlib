package databases

import (
	"context"
	"database/sql"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (dtx *DXDatabaseTx) Update(ctx context.Context, tableName string, setFieldValues utils.JSON, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	result, returningFieldValues, err = db.TxUpdate(ctx, dtx.Tx, tableName, setFieldValues, whereAndFieldNameValues, returningFieldNames)
	if err == nil {
		return nil, nil, err
	}

	return result, returningFieldValues, nil

}
