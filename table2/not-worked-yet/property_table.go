package not_worked_yet

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/table2"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/pkg/errors"
)

/*
	type DXPropertyTable2 struct {
		DatabaseNameId             string
		Database                   *database2.DXDatabase
		NameId                     string
		ResultObjectName           string
		ListViewNameId             string
		FieldNameForRowId          string
		FieldNameForRowNameId      string
		FieldNameForRowUid         string
		DXDatabaseTableFieldTypeMapping           db.DXDatabaseTableFieldTypeMapping
		ResponseEnvelopeObjectName string
	}
*/
func GetAs[T any](l *log.DXLog, expectedType string, property map[string]any) (T, error) {
	var zero T

	actualType, ok := property["type"].(string)
	if !ok {
		return zero, l.ErrorAndCreateErrorf("INVALID_TYPE_FIELD_FORMAT: %T", property["type"])
	}
	if actualType != expectedType {
		return zero, l.ErrorAndCreateErrorf("TYPE_MISMATCH_ERROR: EXPECTED_%s_GOT_%s", expectedType, actualType)
	}

	rawValue, err := utils.GetJSONFromKV(property, "value")
	if err != nil {
		return zero, l.ErrorAndCreateErrorf("MISSING_VALUE_FIELD")
	}

	value, ok := rawValue["value"].(T)
	if !ok {
		return zero, l.ErrorAndCreateErrorf("PropertyGetAsInteger:CAN_NOT_GET_JSON_VALUE:%v", err)
	}

	return value, nil
}

func (pt *table2.DXPropertyTable2) GetAsString(l *log.DXLog, propertyId string) (vv string, err error) {

	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return "", err
	}

	vv, err = GetAs[string](l, "STRING", v)
	if err != nil {
		return "", err
	}

	return vv, nil
}

func (pt *table2.DXPropertyTable2) GetAsStringDefault(l *log.DXLog, propertyId string, defaultValue string) (vv string, err error) {
	_, v, err := pt.SelectOne(l, nil, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return "", err
	}
	if v == nil {
		err = pt.SetAsString(l, propertyId, defaultValue)
		if err != nil {
			return "", err
		}
		return defaultValue, nil
	}
	vv, err = GetAs[string](l, "STRING", v)
	if err != nil {
		return "", err
	}

	return vv, nil
}

func (pt *table2.DXPropertyTable2) TxSetAsString(dtx *database2.DXDatabaseTx, propertyId string, value string) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.TxInsert(dtx, utils.JSON{
		"nameid": propertyId,
		"type":   "STRING",
		"value":  v,
	})
	return err
}

func (pt *table2.DXPropertyTable2) SetAsString(log *log.DXLog, propertyId string, value string) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.Insert(log, utils.JSON{
		"nameid": propertyId,
		"type":   "STRING",
		"value":  string(v),
	})
	return err
}

func (pt *table2.DXPropertyTable2) GetAsInt(l *log.DXLog, propertyId string) (int, error) {
	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return 0, err
	}

	vv, err := GetAs[float64](l, "INT", v)
	if err != nil {
		return 0, err
	}

	return int(vv), nil
}

func (pt *table2.DXPropertyTable2) TxSetAsInt(dtx *database2.DXDatabaseTx, propertyId string, value int) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})
	_, err = pt.TxInsert(dtx, utils.JSON{
		"nameid": propertyId,
		"type":   "INT",
		"value":  v,
	})
	return err
}

func (pt *table2.DXPropertyTable2) SetAsInt(log *log.DXLog, propertyId string, value int) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})
	_, err = pt.Insert(log, utils.JSON{
		"nameid": propertyId,
		"type":   "INT",
		"value":  v,
	})
	return err
}

func (pt *table2.DXPropertyTable2) GetAsInt64(l *log.DXLog, propertyId string) (int64, error) {
	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return 0, err
	}

	vv, err := GetAs[float64](l, "INT64", v)
	if err != nil {
		return 0, err
	}

	return int64(vv), nil
}

func (pt *table2.DXPropertyTable2) TxSetAsInt64(dtx *database2.DXDatabaseTx, propertyId string, value int64) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.TxInsert(dtx, utils.JSON{
		"nameid": propertyId,
		"type":   "INT64",
		"value":  v,
	})
	return err
}

func (pt *table2.DXPropertyTable2) SetAsInt64(log *log.DXLog, propertyId string, value int64) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.Insert(log, utils.JSON{
		"nameid": propertyId,
		"type":   "INT64",
		"value":  v,
	})
	return err
}

func (pt *table2.DXPropertyTable2) TxSetAsJSON(dtx *database2.DXDatabaseTx, propertyId string, value map[string]any) (err error) {
	_, property, err := pt.TxSelectOne(dtx, nil, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return err
	}
	v, err := json.Marshal(utils.JSON{"value": value})

	if property == nil {
		_, err = pt.TxInsert(dtx, utils.JSON{
			"nameid": propertyId,
			"type":   "JSON",
			"value":  v,
		})
		if err != nil {
			return err
		}
	} else {
		_, err = pt.TxUpdate(dtx, utils.JSON{
			"value": v,
		}, utils.JSON{
			"nameid": propertyId,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (pt *table2.DXPropertyTable2) SetAsJSON(log *log.DXLog, propertyId string, value map[string]any) (err error) {
	_, property, err := pt.SelectOne(log, nil, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return err
	}
	v, err := json.Marshal(utils.JSON{"value": value})

	if property == nil {
		_, err = pt.Insert(log, utils.JSON{
			"nameid": propertyId,
			"type":   "JSON",
			"value":  v,
		})
		if err != nil {
			return err
		}
	} else {
		_, err = pt.Update(log, utils.JSON{
			"value": v,
		}, utils.JSON{
			"nameid": propertyId,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (pt *table2.DXPropertyTable2) GetAsJSON(l *log.DXLog, propertyId string) (map[string]any, error) {
	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return nil, err
	}

	vv, err := GetAs[map[string]any](l, "JSON", v)
	if err != nil {
		return nil, err
	}

	return vv, nil
}

func (pt *table2.DXPropertyTable2) DoInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	newKeyValues["is_deleted"] = false

	tt := time.Now().UTC()
	newKeyValues["created_at"] = tt
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = tt
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}

	newId, err = pt.Database.Insert(pt.NameId, pt.FieldNameForRowId, newKeyValues)
	if err != nil {
		return 0, err
	}

	p := utils.JSON{
		pt.FieldNameForRowId: newId,
	}

	if pt.FieldNameForRowUid != "" {
		_, n, err := pt.Database.SelectOne(pt.ListViewNameId, nil, nil, utils.JSON{
			"id": newId,
		}, nil, nil)
		if err != nil {
			return 0, err
		}
		uid, ok := n[pt.FieldNameForRowUid].(string)
		if !ok {
			return 0, errors.New("IMPOSSIBLE:UID")
		}
		p[pt.FieldNameForRowUid] = uid
	}

	data := utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, p)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return newId, nil
}

func (pt *table2.DXPropertyTable2) GetById(log *log.DXLog, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.SelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	}, map[string]string{pt.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.ShouldSelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	}, map[string]string{pt.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) ShouldGetByUid(log *log.DXLog, uid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.ShouldSelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowUid: uid,
		"is_deleted":          false,
	}, map[string]string{pt.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.ShouldSelectOne(log, nil, utils.JSON{
		"utag":       utag,
		"is_deleted": false,
	}, map[string]string{pt.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.SelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameid,
		"is_deleted":             false,
	}, map[string]string{pt.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.ShouldSelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameid,
		"is_deleted":             false,
	}, map[string]string{pt.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) TxShouldGetById(tx *database2.DXDatabaseTx, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(pt.ListViewNameId, nil, nil, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) TxGetByNameId(tx *database2.DXDatabaseTx, nameId string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(pt.ListViewNameId, nil, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameId,
		"is_deleted":             false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) TxShouldGetByNameId(tx *database2.DXDatabaseTx, nameId string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(pt.ListViewNameId, nil, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameId,
		"is_deleted":             false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) TxInsert(tx *database2.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
	//n := utils.NowAsString()
	tt := time.Now().UTC()
	newKeyValues["is_deleted"] = false
	//newKeyValues["created_at"] = n
	newKeyValues["created_at"] = tt

	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		newKeyValues["created_by_user_id"] = "0"
		newKeyValues["created_by_user_nameid"] = "SYSTEM"
		newKeyValues["last_modified_by_user_id"] = "0"
		newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
	}

	newId, err = tx.Insert(pt.NameId, newKeyValues)
	return newId, err
}

func (pt *table2.DXPropertyTable2) InRequestTxInsert(aepr *api.DXAPIEndPointRequest, tx *database2.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
	n := utils.NowAsString()
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = n
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = n
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	newId, err = tx.Insert(pt.NameId, newKeyValues)
	return newId, err
}

func (pt *table2.DXPropertyTable2) Insert(log *log.DXLog, newKeyValues utils.JSON) (newId int64, err error) {
	tt := time.Now().UTC()
	newKeyValues["created_at"] = tt
	newKeyValues["last_modified_at"] = tt
	newKeyValues["is_deleted"] = false
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		newKeyValues["created_by_user_id"] = "0"
		newKeyValues["created_by_user_nameid"] = "SYSTEM"
		newKeyValues["last_modified_by_user_id"] = "0"
		newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	newId, err = pt.Database.Insert(pt.NameId, pt.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (pt *table2.DXPropertyTable2) Update(l *log.DXLog, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	return pt.Database.Update(pt.NameId, setKeyValues, whereAndFieldNameValues)
}

func (pt *table2.DXPropertyTable2) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = pt.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	return pt.Database.Update(pt.NameId, setKeyValues, utils.JSON{
		pt.FieldNameForRowId: FieldValueForId,
	})
}

func (pt *table2.DXPropertyTable2) InRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	n := utils.NowAsString()
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = n
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = n
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	newId, err = pt.Database.Insert(pt.NameId, pt.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (pt *table2.DXPropertyTable2) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(pt.FieldNameForRowId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := pt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, utils.JSON{pt.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (pt *table2.DXPropertyTable2) RequestReadByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(pt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	rowsInfo, d, err := pt.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, utils.JSON{pt.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (pt *table2.DXPropertyTable2) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
	_, nameid, err := aepr.GetParameterValueAsString(pt.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := pt.ShouldGetByNameId(&aepr.Log, nameid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, utils.JSON{pt.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (pt *table2.DXPropertyTable2) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, d, err := pt.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, utils.JSON{pt.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (pt *table2.DXPropertyTable2) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, newKeyValues utils.JSON) (err error) {
	_, _, err = pt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	tt := time.Now().UTC()
	newKeyValues["last_modified_at"] = tt

	_, ok := newKeyValues["last_modified_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	for k, v := range newKeyValues {
		if v == nil {
			delete(newKeyValues, k)
		}
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	_, err = db.Update(pt.Database.Connection, pt.NameId, newKeyValues, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoEdit (%s) ", pt.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, utils.JSON{
		pt.FieldNameForRowId: id,
	},
	))
	return nil
}

func (pt *table2.DXPropertyTable2) DoEditByUid(aepr *api.DXAPIEndPointRequest, uid string, newKeyValues utils.JSON) (err error) {
	_, _, err = pt.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}
	tt := time.Now().UTC()
	newKeyValues["last_modified_at"] = tt

	_, ok := newKeyValues["last_modified_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	for k, v := range newKeyValues {
		if v == nil {
			delete(newKeyValues, k)
		}
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	_, err = db.Update(pt.Database.Connection, pt.NameId, newKeyValues, utils.JSON{
		pt.FieldNameForRowUid: uid,
		"is_deleted":          false,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoEdit (%s) ", pt.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, utils.JSON{
		pt.FieldNameForRowUid: uid,
	},
	))
	return nil
}

func (pt *table2.DXPropertyTable2) RequestEdit(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(pt.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, newFieldValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	err = pt.DoEdit(aepr, id, newFieldValues)
	return err
}

func (pt *table2.DXPropertyTable2) RequestEditByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(pt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, newFieldValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	err = pt.DoEditByUid(aepr, uid, newFieldValues)
	return err
}

func (pt *table2.DXPropertyTable2) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {
	_, _, err = pt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	_, err = db.Delete(pt.Database.Connection, pt.NameId, utils.JSON{
		pt.FieldNameForRowId: id,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoDelete (%s) ", pt.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (pt *table2.DXPropertyTable2) DoDeleteByUid(aepr *api.DXAPIEndPointRequest, uid string) (err error) {
	_, _, err = pt.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	_, err = db.Delete(pt.Database.Connection, pt.NameId, utils.JSON{
		pt.FieldNameForRowUid: uid,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoDelete (%s) ", pt.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (pt *table2.DXPropertyTable2) RequestSoftDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(pt.FieldNameForRowId)
	if err != nil {
		return err
	}

	newFieldValues := utils.JSON{
		"is_deleted": true,
	}

	err = pt.DoEdit(aepr, id, newFieldValues)
	if err != nil {
		aepr.Log.Errorf("Error at %s.RequestSoftDelete (%s) ", pt.NameId, err.Error())
		return err
	}
	return err
}

func (pt *table2.DXPropertyTable2) RequestSoftDeleteById(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(pt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	newFieldValues := utils.JSON{
		"is_deleted": true,
	}

	err = pt.DoEditByUid(aepr, uid, newFieldValues)
	if err != nil {
		aepr.Log.Errorf("Error at %s.RequestSoftDelete (%s) ", pt.NameId, err.Error())
		return err
	}
	return err
}

func (pt *table2.DXPropertyTable2) RequestHardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(pt.FieldNameForRowId)
	if err != nil {
		return err
	}

	err = pt.DoDelete(aepr, id)
	if err != nil {
		aepr.Log.Errorf("Error at %s.RequestHardDelete (%s) ", pt.NameId, err.Error())
		return err
	}
	return err
}

func (pt *table2.DXPropertyTable2) RequestHardDeleteByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(pt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	err = pt.DoDeleteByUid(aepr, uid)
	if err != nil {
		aepr.Log.Errorf("Error at %s.RequestHardDelete (%s) ", pt.NameId, err.Error())
		return err
	}
	return err
}

/*func (t *DXPropertyTable2) Count(log *log.DXLog, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (totalRows int64, summaryCalcRow utils.JSON, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}
		if pt.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	totalRows, summaryCalcRow, err = pt.Database.ShouldCount(pt.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues, joinSQLPart)
	return totalRows, summaryCalcRow, err
}*/

/*
	func (t *DXPropertyTable2) TxSelectCount(tx *database2.DXDatabaseTx, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON) (totalRows int64, summaryCalcRow utils.JSON, err error) {
		if whereAndFieldNameValues == nil {
			whereAndFieldNameValues = utils.JSON{
				"is_deleted": false,
			}
			if pt.Database.DatabaseType.String() == "sqlserver" {
				whereAndFieldNameValues["is_deleted"] = 0
			}
		}

		totalRows, summaryCalcRow, err = tx.ShouldCount(pt.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues)
		return totalRows, summaryCalcRow, err
	}
*/
func (pt *table2.DXPropertyTable2) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (rowsInfo *db.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}

		if pt.Database == nil {
			pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
		}
		if pt.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	rowsInfo, r, err = pt.Database.Select(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (pt *table2.DXPropertyTable2) ShouldSelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}

	return pt.Database.ShouldSelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections)
}

func (pt *table2.DXPropertyTable2) TxShouldSelectOne(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.ShouldSelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, nil)
}

func (pt *table2.DXPropertyTable2) TxShouldSelectOneForUpdate(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.ShouldSelectOne(pt.NameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (pt *table2.DXPropertyTable2) TxSelect(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy, limit any) (rowsInfo *db.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.Select(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, limit, false)
}

func (pt *table2.DXPropertyTable2) TxSelectOne(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.SelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, false)
}

func (pt *table2.DXPropertyTable2) TxSelectOneForUpdate(tx *database2.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.SelectOne(pt.NameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (pt *table2.DXPropertyTable2) TxUpdate(tx *database2.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.Update(pt.NameId, setKeyValues, whereAndFieldNameValues)
}

func (pt *table2.DXPropertyTable2) TxSoftDelete(tx *database2.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Update(pt.NameId, map[string]any{
		"is_deleted": true,
	}, whereAndFieldNameValues)
}

func (pt *table2.DXPropertyTable2) TxHardDelete(tx *database2.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Delete(pt.NameId, whereAndFieldNameValues)
}

func (pt *table2.DXPropertyTable2) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	if !pt.Database.Connected {
		err := pt.Database.Connect()
		if err != nil {
			return err
		}
	}
	rowsInfo, list, totalRows, totalPage, _, err := db.NamedQueryPaging(pt.Database.Connection, pt.FieldTypeMapping, "", rowPerPage, pageIndex, "*", pt.ListViewNameId,
		filterWhere, "", filterOrderBy, filterKeyValues)
	if err != nil {
		return err
	}

	for i := range list {

		if onResultList != nil {
			aListRow, err := onResultList(list[i])
			if err != nil {
				return err
			}
			list[i] = aListRow
		}

	}

	data := utilsJson.Encapsulate(pt.ResponseEnvelopeObjectName, utils.JSON{
		"list": utils.JSON{
			"rows":       list,
			"total_rows": totalRows,
			"total_page": totalPage,
			"rows_info":  rowsInfo,
		},
	})

	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return nil
}

func (pt *table2.DXPropertyTable2) RequestPagingList(aepr *api.DXAPIEndPointRequest) (err error) {
	isExistFilterWhere, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}
	if !isExistFilterWhere {
		filterWhere = ""
	}
	isExistFilterOrderBy, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}
	if !isExistFilterOrderBy {
		filterOrderBy = ""
	}

	isExistFilterKeyValues, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}
	if !isExistFilterKeyValues {
		filterKeyValues = nil
	}

	_, isDeletedIncluded, err := aepr.GetParameterValueAsBool("is_deleted", false)
	if err != nil {
		return err
	}

	if !isDeletedIncluded {
		if filterWhere != "" {
			filterWhere = fmt.Sprintf("(%s) and ", filterWhere)
		}

		if pt.Database == nil {
			pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
		}
		switch pt.Database.DatabaseType.String() {
		case "sqlserver":
			filterWhere = filterWhere + "(is_deleted=0)"
		case "postgres":
			filterWhere = filterWhere + "(is_deleted=false)"
		default:
			filterWhere = filterWhere + "(is_deleted=0)"
		}
	}

	return pt.DoRequestPagingList(aepr, filterWhere, filterOrderBy, filterKeyValues, nil)
}

func (pt *table2.DXPropertyTable2) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, orderbyFieldNameDirections db.DXDatabaseTableFieldsOrderBy) (
	rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	if pt.Database == nil {
		pt.Database = database2.Manager.Databases[pt.DatabaseNameId]
	}
	return pt.Database.SelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections)
}
