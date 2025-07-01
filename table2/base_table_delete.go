package table2

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/utils"
	"net/http"
)

func (bt *DXBaseTable2) Delete(whereKeyValues utils.JSON) (err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return err
	}

	_, _, err = db.Delete(bt.Database.Connection, bt.NameId, whereKeyValues, nil)
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable2) DeleteById(id int64) (err error) {
	err = bt.Delete(utils.JSON{
		bt.FieldNameForRowId: id,
	})
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable2) DeleteByUid(uid string) (err error) {
	err = bt.Delete(utils.JSON{
		bt.FieldNameForRowUid: uid,
	})
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable2) DoRequestDeleteByIdOrUid(aepr *api.DXAPIEndPointRequest, id int64, uid string) (err error) {
	if id != 0 {
		err = bt.DeleteById(id)
	} else {
		err = bt.DeleteByUid(uid)
	}
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (bt *DXBaseTable2) RequestHardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(bt.FieldNameForRowId)
	if err != nil {
		return err
	}

	err = bt.DoRequestDeleteByIdOrUid(aepr, id, "")
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable2) RequestHardDeleteByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(bt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	err = bt.DoRequestDeleteByIdOrUid(aepr, 0, uid)
	if err != nil {
		return err
	}
	return nil
}

func (bt *DXBaseTable2) TxHardDelete(tx *database2.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	r, _, err = tx.TxDelete(bt.NameId, whereAndFieldNameValues, nil)
	return r, err
}
