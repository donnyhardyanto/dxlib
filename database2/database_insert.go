package database2

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (d *DXDatabase) Insert(tableName string, setFieldValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues utils.JSON, err error) {
	err = d.EnsureConnection()
	if err != nil {
		return nil, nil, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		result, returningFieldValues, err = db.Insert(d.Connection, tableName, setFieldValues, returningFieldNames)
		if err == nil {
			return result, returningFieldValues, nil
		}
		err = CheckDatabaseError(err)
		if err == nil {
			return nil, nil, err
		}
		if err.Error() != "ERROR_DB_NOT_CONNECTED" {
			return nil, nil, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:%s", err.Error())
		}
	}

	return nil, nil, err
}
