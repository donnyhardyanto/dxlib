package database2

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (dtx *DXDatabaseTx) Update(tableName string, setFieldValues utils.JSON, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	result, returningFieldValues, err = db.TxUpdate(dtx.Tx, tableName, setFieldValues, whereAndFieldNameValues, returningFieldNames)
	if err == nil {
		return nil, nil, err
	}

	return result, returningFieldValues, nil

}
