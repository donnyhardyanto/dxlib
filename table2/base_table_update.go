package table

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (bt *DXBaseTable) Update(setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, err
	}

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	result, _, err = bt.Database.Update(bt.NameId, setKeyValues, whereAndFieldNameValues, nil)
	return result, err
}

func (bt *DXBaseTable) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = bt.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}
	if bt.Database == nil {
		bt.Database = database.Manager.Databases[t.DatabaseNameId]
	}
	return bt.Database.Update(t.NameId, setKeyValues, utils.JSON{
		bt.FieldNameForRowId: FieldValueForId,
	})
}

func (bt *DXBaseTable) UpdateOneByUid(l *log.DXLog, FieldValueForUid string, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = bt.ShouldGetByUid(l, FieldValueForUid)
	if err != nil {
		return nil, err
	}
	if bt.Database == nil {
		bt.Database = database.Manager.Databases[t.DatabaseNameId]
	}
	return bt.Database.Update(t.NameId, setKeyValues, utils.JSON{
		bt.FieldNameForRowUid: FieldValueForUid,
	})
}
