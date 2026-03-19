package databases

import (
	"context"
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (d *DXDatabase) Update(ctx context.Context, tableName string, setFieldValues utils.JSON, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (result sql.Result, returningFieldValues []utils.JSON, err error) {
	err = d.EnsureConnection()
	if err != nil {
		return nil, nil, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		result, returningFieldValues, err = db.Update(ctx, d.Connection, tableName, setFieldValues, whereAndFieldNameValues, returningFieldNames)
		if err == nil {
			return result, returningFieldValues, nil
		}
		log.Log.Warnf("UPDATE_ERROR:%s=%v", tableName, err.Error())
		if !db.IsConnectionError(err) {
			return nil, nil, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:%s", err.Error())
		}
	}
	return result, returningFieldValues, err
}
