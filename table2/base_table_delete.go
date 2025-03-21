package table

import (
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"net/http"
)

func (bt *DXBaseTable) DeleteById(log *log.DXLog, id int64) (err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return err
	}
	_, _, err = bt.ShouldGetById(log, id)
	if err != nil {
		return err
	}

	_, _, err = db.Delete(bt.Database.Connection, bt.NameId, utils.JSON{
		bt.FieldNameForRowId: id,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable) DoRequestDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {
	err = bt.DeleteById(&aepr.Log, id)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (bt *DXBaseTable) DeleteByUid(log *log.DXLog, uid string) (err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return err
	}
	_, _, err = bt.ShouldGetByUid(log, uid)
	if err != nil {
		return err
	}

	_, _, err = db.Delete(bt.Database.Connection, bt.NameId, utils.JSON{
		bt.FieldNameForRowUid: uid,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable) DoRequestDeleteByUid(aepr *api.DXAPIEndPointRequest, uid string) (err error) {
	err = bt.DeleteByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}
