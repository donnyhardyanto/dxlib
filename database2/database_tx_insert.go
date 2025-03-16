package database2

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (dtx *DXDatabaseTx) Insert(tableName string, setFieldValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues utils.JSON, err error) {

	result, returningFieldValues, err = db.TxInsert(dtx.Tx, tableName, setFieldValues, returningFieldNames)
	if err != nil {
		return nil, nil, err
	}

	return result, returningFieldValues, nil

}
