package table

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"net/http"
)

func (bt *DXBaseTable) Update(setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, err
	}

	result, _, err = bt.Database.Update(bt.NameId, setKeyValues, whereAndFieldNameValues, nil)
	return result, err
}

func (bt *DXBaseTable) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = bt.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}

	return bt.Update(setKeyValues, utils.JSON{
		bt.FieldNameForRowId: FieldValueForId,
	})
}

func (bt *DXBaseTable) UpdateOneByUid(l *log.DXLog, FieldValueForUid string, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = bt.ShouldGetByUid(l, FieldValueForUid)
	if err != nil {
		return nil, err
	}

	return bt.Update(setKeyValues, utils.JSON{
		bt.FieldNameForRowUid: FieldValueForUid,
	})
}

func (bt *DXBaseTable) DoRequestEditByIdOrUid(aepr *api.DXAPIEndPointRequest, id int64, uid string, newKeyValues utils.JSON) (err error) {

	if bt.OnBeforeUpdate != nil {
		if err := bt.OnBeforeUpdate(aepr, newKeyValues); err != nil {
			return err
		}
	}

	p := utils.JSON{}
	if id != 0 {
		p[bt.FieldNameForRowId] = id
	}
	if uid != "" {
		p[bt.FieldNameForRowUid] = uid
	}

	_, err = bt.Update(newKeyValues, p)

	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (bt *DXBaseTable) RequestEdit(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(bt.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, newFieldValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	err = bt.DoRequestEditByIdOrUid(aepr, id, "", newFieldValues)
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable) RequestEditByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(bt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, newFieldValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	err = bt.DoRequestEditByIdOrUid(aepr, 0, uid, newFieldValues)
	if err != nil {
		return err
	}
	return nil
}
