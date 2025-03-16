package database2

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (dtx *DXDatabaseTx) TxDelete(tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	result, returningFieldValues, err = db.TxDelete(dtx.Tx, tableName, whereAndFieldNameValues, returningFieldNames)
	if err == nil {
		return nil, nil, err
	}
	return result, returningFieldValues, nil

}

func (dtx *DXDatabaseTx) TxSoftDelete(tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	return dtx.Update(tableName, utils.JSON{
		`is_deleted`: true,
	}, whereAndFieldNameValues, returningFieldNames)
}
