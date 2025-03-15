package database2

import (
	oldDb "github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (d *DXDatabase) Count(tableName string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (count int64, err error) {
	err = d.EnsureConnection()
	if err != nil {
		return 0, err
	}

	for tryCount := 0; tryCount < 4; tryCount++ {
		count, err = db.Count(d.Connection, tableName, "COUNT(*)", whereAndFieldNameValues, joinSQLPart, nil, "", "")
		if err == nil {
			return count, nil
		}
		log.Log.Warnf("COUNT_ERROR:%s=%v", tableName, err.Error())
		if !oldDb.IsConnectionError(err) {
			return 0, err
		}
		err = d.CheckConnectionAndReconnect()
		if err != nil {
			log.Log.Warnf("RECONNECT_ERROR:TRY_COUNT=%d,MSG=%s", tryCount, err.Error())
		}
	}
	return 0, err
}
