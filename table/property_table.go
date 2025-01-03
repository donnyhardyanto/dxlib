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

func (pt *DXPropertyTable) GetAsString(l *log.DXLog, propertyId string) (vv string, err error) {
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

func (pt *DXPropertyTable) GetAsStringDefault(l *log.DXLog, propertyId string, defaultValue string) (vv string, err error) {
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

func (pt *DXPropertyTable) GetAsInt(l *log.DXLog, propertyId string) (int, error) {
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

	/*aType, ok := v["type"].(string)
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

	return int(vv), nil*/
}

func (pt *DXPropertyTable) TxSetAsInt(dtx *database.DXDatabaseTx, propertyId string, value int) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})
	_, err = pt.TxInsert(dtx, utils.JSON{
		"nameid": propertyId,
		"type":   "INT",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) SetAsInt(log *log.DXLog, propertyId string, value int) (err error) {
	v, err := json.Marshal(utils.JSON{"value": value})
	_, err = pt.Insert(log, utils.JSON{
		"nameid": propertyId,
		"type":   "INT",
		"value":  v,
	})
	return err
}

func (pt *DXPropertyTable) GetAsInt64(l *log.DXLog, propertyId string) (int64, error) {
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

	/*	aType, ok := v["type"].(string)
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

		return int64(vv), nil*/
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

func (pt *DXPropertyTable) SetAsJSON(log *log.DXLog, propertyId string, value map[string]any) (err error) {
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

func (pt *DXPropertyTable) GetAsJSON(l *log.DXLog, propertyId string) (map[string]any, error) {
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

	/*aType, ok := v["type"].(string)
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

	return vv, nil*/
}

func (pt *DXPropertyTable) DoInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
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

	newId, err = pt.Database.Insert(pt.NameId, pt.FieldNameForRowId, newKeyValues)
	if err != nil {
		return 0, err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		pt.FieldNameForRowId: newId,
	})
	return newId, nil
}

func (pt *DXPropertyTable) GetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.SelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	}, map[string]string{pt.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.ShouldSelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	}, map[string]string{pt.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.ShouldSelectOne(log, nil, utils.JSON{
		"utag":       utag,
		"is_deleted": false,
	}, map[string]string{pt.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.SelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameid,
		"is_deleted":             false,
	}, map[string]string{pt.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = pt.ShouldSelectOne(log, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameid,
		"is_deleted":             false,
	}, map[string]string{pt.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) TxShouldGetById(tx *database.DXDatabaseTx, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(pt.ListViewNameId, nil, nil, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(pt.ListViewNameId, nil, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameId,
		"is_deleted":             false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) TxShouldGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(pt.ListViewNameId, nil, nil, utils.JSON{
		pt.FieldNameForRowNameId: nameId,
		"is_deleted":             false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (pt *DXPropertyTable) TxInsert(tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
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

func (pt *DXPropertyTable) InRequestTxInsert(aepr *api.DXAPIEndPointRequest, tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
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

func (pt *DXPropertyTable) Insert(log *log.DXLog, newKeyValues utils.JSON) (newId int64, err error) {
	//n := utils.NowAsString()
	/*	if pt.Database.DatabaseType.String() == "sqlserver" {
		pt, err := time.Parse(time.RFC3339, n)
		if err != nil {
			fmt.Println("Error:", err)
			return 0, err
		}
		// Format the time.Time value back into a string without the timezone offset
		n = pt.Format("2006-01-02 15:04:05")
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

	newId, err = pt.Database.Insert(pt.NameId, pt.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (pt *DXPropertyTable) Update(l *log.DXLog, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return pt.Database.Update(pt.NameId, setKeyValues, whereAndFieldNameValues)
}

func (pt *DXPropertyTable) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = pt.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}
	return pt.Database.Update(pt.NameId, setKeyValues, utils.JSON{
		pt.FieldNameForRowId: FieldValueForId,
	})
}

func (pt *DXPropertyTable) InRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
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

	newId, err = pt.Database.Insert(pt.NameId, pt.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (pt *DXPropertyTable) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(pt.FieldNameForRowId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := pt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{pt.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (pt *DXPropertyTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
	_, nameid, err := aepr.GetParameterValueAsString(pt.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := pt.ShouldGetByNameId(&aepr.Log, nameid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{pt.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (pt *DXPropertyTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, d, err := pt.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{pt.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (pt *DXPropertyTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, newKeyValues utils.JSON) (err error) {
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

	_, err = db.Update(pt.Database.Connection, pt.NameId, newKeyValues, utils.JSON{
		pt.FieldNameForRowId: id,
		"is_deleted":         false,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoEdit (%s) ", pt.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		pt.FieldNameForRowId: id,
	})
	return nil
}

func (pt *DXPropertyTable) RequestEdit(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (pt *DXPropertyTable) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {
	_, _, err = pt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
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

func (pt *DXPropertyTable) RequestSoftDelete(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (pt *DXPropertyTable) RequestHardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (pt *DXPropertyTable) SelectAll(log *log.DXLog) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	return pt.Select(log, nil, nil, nil, map[string]string{pt.FieldNameForRowId: "asc"}, nil)
}

/*func (t *DXPropertyTable) SelectCount(log *log.DXLog, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (totalRows int64, summaryCalcRow utils.JSON, err error) {
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
}*/

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
func (pt *DXPropertyTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}
		if pt.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	rowsInfo, r, err = pt.Database.Select(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (pt *DXPropertyTable) ShouldSelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return pt.Database.ShouldSelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections)
}

func (pt *DXPropertyTable) TxShouldSelectOne(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.ShouldSelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, nil)
}

func (pt *DXPropertyTable) TxShouldSelectOneForUpdate(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.ShouldSelectOne(pt.NameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (pt *DXPropertyTable) TxSelect(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.Select(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, limit, false)
}

func (pt *DXPropertyTable) TxSelectOne(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.SelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, false)
}

func (pt *DXPropertyTable) TxSelectOneForUpdate(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.SelectOne(pt.NameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (pt *DXPropertyTable) TxUpdate(tx *database.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return tx.Update(pt.NameId, setKeyValues, whereAndFieldNameValues)
}

func (pt *DXPropertyTable) TxSoftDelete(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Update(pt.NameId, map[string]any{
		`is_deleted`: true,
	}, whereAndFieldNameValues)
}

func (pt *DXPropertyTable) TxHardDelete(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Delete(pt.NameId, whereAndFieldNameValues)
}

func (pt *DXPropertyTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
	if pt.Database == nil {
		pt.Database = database.Manager.Databases[pt.DatabaseNameId]
	}

	if !pt.Database.Connected {
		err := pt.Database.Connect()
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

func (pt *DXPropertyTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (pt *DXPropertyTable) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, orderbyFieldNameDirections db.FieldsOrderBy) (
	rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	_, ok := whereAndFieldNameValues["is_deleted"]
	if !ok {
		whereAndFieldNameValues["is_deleted"] = false
	}

	return pt.Database.SelectOne(pt.ListViewNameId, pt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, nil, orderbyFieldNameDirections)
}
