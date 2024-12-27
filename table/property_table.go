package table

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	databaseUtils "github.com/donnyhardyanto/dxlib/database/protected/utils"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"net/http"
	"time"
)

type DXPropertyTable struct {
	DatabaseNameId        string
	Database              *database.DXDatabase
	NameId                string
	ResultObjectName      string
	ListViewNameId        string
	FieldNameForRowId     string
	FieldNameForRowNameId string
	FieldTypeMapping      databaseUtils.FieldTypeMapping
}

func (pt *DXPropertyTable) GetAsString(l *log.DXLog, propertyId string) (string, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return "", err
	}

	aType, ok := v["type"].(string)
	if !ok {
		return "", l.ErrorAndCreateErrorf("PropertyGetAsString: type is not string: %v", v["type"])
	}

	value, err := utils.GetJSONFromKV(v, "value")
	if err != nil {
		return "", l.ErrorAndCreateErrorf("PropertyGetAsString:CAN_NOT_GET_JSON_VALUE:%v", err)
	}
	vv, ok := value["value"].(string)
	if !ok {
		return "", l.ErrorAndCreateErrorf("PropertyGetAsString: value is not a number: %v", value[aType])
	}

	return vv, nil
}

func (pt *DXPropertyTable) TxSetAsString(dtx *database.DXDatabaseTx, propertyId string, value string) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.TxInsert(dtx, utils.JSON{
		"nameid": propertyId,
		"type":   "STRING",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) SetAsString(log *log.DXLog, propertyId string, value string) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.Insert(log, utils.JSON{
		"nameid": propertyId,
		"type":   "STRING",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) GetAsInteger(l *log.DXLog, propertyId string) (int, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return 0, err
	}

	aType, ok := v["type"].(string)
	if !ok {
		return 0, l.ErrorAndCreateErrorf("PropertyGetAsInteger: type is not string: %v", v["type"])
	}

	value, err := utils.GetJSONFromKV(v, "value")
	if err != nil {
		return 0, l.ErrorAndCreateErrorf("PropertyGetAsInteger:CAN_NOT_GET_JSON_VALUE:%v", err)
	}
	vv, ok := value["value"].(float64)
	if !ok {
		return 0, l.ErrorAndCreateErrorf("PropertyGetAsInteger: value is not a number: %v", value[aType])
	}

	return int(vv), nil
}

func (pt *DXPropertyTable) TxSetAsInteger(dtx *database.DXDatabaseTx, propertyId string, value int) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})
	_, err = pt.TxInsert(dtx, utils.JSON{
		"nameid": propertyId,
		"type":   "INT",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) SetAsInteger(log *log.DXLog, propertyId string, value int) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})
	_, err = pt.Insert(log, utils.JSON{
		"nameid": propertyId,
		"type":   "INT",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) GetAsInt64(l *log.DXLog, propertyId string) (int64, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return 0, err
	}

	aType, ok := v["type"].(string)
	if !ok {
		return 0, l.ErrorAndCreateErrorf("PropertyGetAsInteger: type is not string: %v", v["type"])
	}

	value, err := utils.GetJSONFromKV(v, "value")
	if err != nil {
		return 0, l.ErrorAndCreateErrorf("PropertyGetAsInteger:CAN_NOT_GET_JSON_VALUE:%v", err)
	}
	vv, ok := value["value"].(float64)
	if !ok {
		return 0, l.ErrorAndCreateErrorf("PropertyGetAsInteger: value is not a number: %v", value[aType])
	}

	return int64(vv), nil
}

func (pt *DXPropertyTable) TxSetAsInt64(dtx *database.DXDatabaseTx, propertyId string, value int64) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.TxInsert(dtx, utils.JSON{
		"nameid": propertyId,
		"type":   "INT64",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) SetAsInt64(log *log.DXLog, propertyId string, value int64) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})

	_, err = pt.Insert(log, utils.JSON{
		"nameid": propertyId,
		"type":   "INT64",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) TxSetAsJSON(dtx *database.DXDatabaseTx, propertyId string, value map[string]any) (err error) {
	_, property, err := pt.TxSelectOne(dtx, utils.JSON{
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

func (pt *DXPropertyTable) SetAsJSON(log *log.DXLog, propertyId string, value map[string]any) (err error) {
	_, property, err := pt.SelectOne(log, utils.JSON{
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

func (pt *DXPropertyTable) GetAsJSON(l *log.DXLog, propertyId string) (map[string]any, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{
		"nameid": propertyId,
	}, nil)
	if err != nil {
		return nil, err
	}

	aType, ok := v["type"].(string)
	if !ok {
		return nil, l.ErrorAndCreateErrorf("PropertyGetAsJSON: type is not string: %v", v["type"])
	}

	value, err := utils.GetJSONFromKV(v, "value")
	if err != nil {
		return nil, l.ErrorAndCreateErrorf("PropertyGetAsJSON:CAN_NOT_GET_JSON_VALUE:%v", err)
	}
	vv, ok := value["value"].(map[string]any)
	if !ok {
		return nil, l.ErrorAndCreateErrorf("PropertyGetAsJSON: value is not a JSON: %v", value[aType])
	}

	return vv, nil
}

func (t *DXPropertyTable) DoInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
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

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	if err != nil {
		return 0, err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		t.FieldNameForRowId: newId,
	})
	return newId, nil
}

func (t *DXPropertyTable) GetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXPropertyTable) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXPropertyTable) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		"utag":       utag,
		"is_deleted": false,
	}, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXPropertyTable) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: nameid,
		"is_deleted":            false,
	}, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXPropertyTable) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: nameid,
		"is_deleted":            false,
	}, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXPropertyTable) TxShouldGetById(tx *database.DXDatabaseTx, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, []string{`*`}, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXPropertyTable) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(t.ListViewNameId, []string{`*`}, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXPropertyTable) TxShouldGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, []string{`*`}, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXPropertyTable) TxInsert(tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
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

	newId, err = tx.Insert(t.NameId, newKeyValues)
	return newId, err
}

func (t *DXPropertyTable) InRequestTxInsert(aepr *api.DXAPIEndPointRequest, tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
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

	newId, err = tx.Insert(t.NameId, newKeyValues)
	return newId, err
}

func (t *DXPropertyTable) Insert(log *log.DXLog, newKeyValues utils.JSON) (newId int64, err error) {
	//n := utils.NowAsString()
	/*	if t.Database.DatabaseType.String() == "sqlserver" {
		t, err := time.Parse(time.RFC3339, n)
		if err != nil {
			fmt.Println("Error:", err)
			return 0, err
		}
		// Format the time.Time value back into a string without the timezone offset
		n = t.Format("2006-01-02 15:04:05")
	}*/
	//n := utils.NowAsString()
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

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (t *DXPropertyTable) Update(l *log.DXLog, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return t.Database.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXPropertyTable) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = t.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}
	return t.Database.Update(t.NameId, setKeyValues, utils.JSON{
		t.FieldNameForRowId: FieldValueForId,
	})
}

func (t *DXPropertyTable) InRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
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

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (t *DXPropertyTable) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (t *DXPropertyTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
	_, nameid, err := aepr.GetParameterValueAsString(t.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.ShouldGetByNameId(&aepr.Log, nameid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (t *DXPropertyTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (t *DXPropertyTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, newKeyValues utils.JSON) (err error) {
	_, _, err = t.ShouldGetById(&aepr.Log, id)
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

	_, err = db.Update(t.Database.Connection, t.NameId, newKeyValues, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoEdit (%s) ", t.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		t.FieldNameForRowId: id,
	})
	return nil
}

func (t *DXPropertyTable) RequestEdit(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, newFieldValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	err = t.DoEdit(aepr, id, newFieldValues)
	return err
}

func (t *DXPropertyTable) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {
	_, _, err = t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	_, err = db.Delete(t.Database.Connection, t.NameId, utils.JSON{
		t.FieldNameForRowId: id,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoDelete (%s) ", t.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (t *DXPropertyTable) RequestSoftDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	newFieldValues := utils.JSON{
		"is_deleted": true,
	}

	err = t.DoEdit(aepr, id, newFieldValues)
	if err != nil {
		aepr.Log.Errorf("Error at %s.RequestSoftDelete (%s) ", t.NameId, err.Error())
		return err
	}
	return err
}

func (t *DXPropertyTable) RequestHardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	err = t.DoDelete(aepr, id)
	if err != nil {
		aepr.Log.Errorf("Error at %s.RequestHardDelete (%s) ", t.NameId, err.Error())
		return err
	}
	return err
}

func (t *DXPropertyTable) SelectAll(log *log.DXLog) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	return t.Select(log, nil, nil, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil)
}

func (t *DXPropertyTable) SelectCount(log *log.DXLog, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (totalRows int64, summaryCalcRow utils.JSON, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}
		if t.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	totalRows, summaryCalcRow, err = t.Database.ShouldSelectCount(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues, joinSQLPart)
	return totalRows, summaryCalcRow, err
}

/*
	func (t *DXPropertyTable) TxSelectCount(tx *database.DXDatabaseTx, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON) (totalRows int64, summaryCalcRow utils.JSON, err error) {
		if whereAndFieldNameValues == nil {
			whereAndFieldNameValues = utils.JSON{
				"is_deleted": false,
			}
			if t.Database.DatabaseType.String() == "sqlserver" {
				whereAndFieldNameValues["is_deleted"] = 0
			}
		}

		totalRows, summaryCalcRow, err = tx.ShouldSelectCount(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues)
		return totalRows, summaryCalcRow, err
	}
*/
func (t *DXPropertyTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}
		if t.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	rowsInfo, r, err = t.Database.Select(t.ListViewNameId, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, t.FieldTypeMapping)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (t *DXPropertyTable) ShouldSelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return t.Database.ShouldSelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, t.FieldTypeMapping)
}

func (t *DXPropertyTable) TxShouldSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.ShouldSelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, nil)
}

func (t *DXPropertyTable) TxShouldSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.ShouldSelectOne(t.NameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXPropertyTable) TxSelect(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.Select(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, limit, false)
}

func (t *DXPropertyTable) TxSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.SelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, false)
}

func (t *DXPropertyTable) TxSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.SelectOne(t.NameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXPropertyTable) TxUpdate(tx *database.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXPropertyTable) TxSoftDelete(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Update(t.NameId, map[string]any{
		`is_deleted`: true,
	}, whereAndFieldNameValues)
}

func (t *DXPropertyTable) TxHardDelete(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Delete(t.NameId, whereAndFieldNameValues)
}

func (t *DXPropertyTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	if !t.Database.Connected {
		err := t.Database.Connect()
		if err != nil {
			return err
		}
	}

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	rowsInfo, list, totalRows, totalPage, _, err := db.NamedQueryPaging(t.Database.Connection, t.FieldTypeMapping, "", rowPerPage, pageIndex, "*", t.ListViewNameId,
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

	data := utils.JSON{
		"list": utils.JSON{
			"rows":       list,
			"total_rows": totalRows,
			"total_page": totalPage,
			"rows_info":  rowsInfo,
		},
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return nil
}

func (t *DXPropertyTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) (err error) {
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

		switch t.Database.DatabaseType.String() {
		case "sqlserver":
			filterWhere = filterWhere + "(is_deleted=0)"
		case "postgres":
			filterWhere = filterWhere + "(is_deleted=false)"
		default:
			filterWhere = filterWhere + "(is_deleted=0)"
		}
	}

	return t.DoRequestPagingList(aepr, filterWhere, filterOrderBy, filterKeyValues, nil)
}

func (t *DXPropertyTable) SelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON, orderbyFieldNameDirections db.FieldsOrderBy) (
	rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return t.Database.SelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, t.FieldTypeMapping)
}
