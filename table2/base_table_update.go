package table2

import (
	"database/sql"

	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (bt *DXBaseTable2) Update(setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitializeConnection(); err != nil {
		return nil, err
	}

	if bt.DoOverrideUpdateValues != nil {
		setKeyValues, whereAndFieldNameValues, err = bt.DoOverrideUpdateValues(setKeyValues, whereAndFieldNameValues)
	}

	r, _, err = bt.Database.Update(bt.NameId, setKeyValues, whereAndFieldNameValues, nil)
	return r, err
}

func (bt *DXBaseTable2) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (r sql.Result, err error) {
	_, _, err = bt.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}

	return bt.Update(setKeyValues, utils.JSON{
		bt.FieldNameForRowId: FieldValueForId,
	})
}

func (bt *DXBaseTable2) UpdateOneByUid(l *log.DXLog, FieldValueForUid string, setKeyValues utils.JSON) (r sql.Result, err error) {
	_, _, err = bt.ShouldGetByUid(l, FieldValueForUid)
	if err != nil {
		return nil, err
	}

	return bt.Update(setKeyValues, utils.JSON{
		bt.FieldNameForRowUid: FieldValueForUid,
	})
}
func (bt *DXBaseTable2) TxUpdate(tx *database.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	r, _, err = tx.Update(bt.NameId, setKeyValues, whereAndFieldNameValues, nil)
	return r, err
}
