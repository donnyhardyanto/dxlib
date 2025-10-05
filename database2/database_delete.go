package database2

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (d *DXDatabase) Delete(tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	err = d.EnsureConnection()
	if err != nil {
		return nil, nil, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		result, returningFieldValues, err = db.Delete(d.Connection, tableName, whereAndFieldNameValues, returningFieldNames)
		if err == nil {
			return result, returningFieldValues, nil
		}
		log.Log.Warnf("DELETE_ERROR:%s=%v", tableName, err.Error())
		if !db.IsConnectionError(err) {
			return nil, nil, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:%s", err.Error())
		}
	}
	return nil, nil, err
}

func (d *DXDatabase) SoftDelete(tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	return d.Update(tableName, utils.JSON{
		"is_deleted": true,
	}, whereAndFieldNameValues, returningFieldNames)
}
