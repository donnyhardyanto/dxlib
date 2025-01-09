package table

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/database/protected/export"
	databaseUtils "github.com/donnyhardyanto/dxlib/database/protected/utils"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"
)

type DXTable struct {
	DatabaseNameId        string
	Database              *database.DXDatabase
	NameId                string
	ResultObjectName      string
	ListViewNameId        string
	FieldNameForRowId     string
	FieldNameForRowNameId string
	FieldTypeMapping      databaseUtils.FieldTypeMapping
}

func (t *DXTable) DoInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
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

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
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

func (t *DXTable) DoCreate(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	newId, err = t.DoInsert(aepr, newKeyValues)
	if err != nil {
		return 0, err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		t.FieldNameForRowId: newId,
	})

	return newId, nil
}

func (t *DXTable) GetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXTable) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXTable) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		"utag":       utag,
		"is_deleted": false,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXTable) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowNameId: nameid,
		"is_deleted":            false,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXTable) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: nameid,
		"is_deleted":            false,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXTable) TxShouldGetById(tx *database.DXDatabaseTx, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXTable) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXTable) TxShouldGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXTable) TxInsert(tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
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

func (t *DXTable) InRequestTxInsert(aepr *api.DXAPIEndPointRequest, tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
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

func (t *DXTable) Insert(log *log.DXLog, newKeyValues utils.JSON) (newId int64, err error) {
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

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (t *DXTable) Update(setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	return t.Database.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXTable) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = t.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	return t.Database.Update(t.NameId, setKeyValues, utils.JSON{
		t.FieldNameForRowId: FieldValueForId,
	})
}

func (t *DXTable) InRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
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

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (t *DXTable) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, newKeyValues utils.JSON) (err error) {
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

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
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

func (t *DXTable) RequestEdit(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXTable) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {
	_, _, err = t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
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

func (t *DXTable) SoftDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {
	_, _, err = t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	_, err = db.Update(t.Database.Connection, t.NameId, utils.JSON{
		"is_deleted": true,
	}, utils.JSON{
		t.FieldNameForRowId: id,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoDelete (%s) ", t.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (t *DXTable) RequestSoftDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	err = t.DoEdit(aepr, id, utils.JSON{
		"is_deleted": true,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.RequestSoftDelete (%s) ", t.NameId, err.Error())
		return err
	}
	return err
}

func (t *DXTable) RequestHardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXTable) SelectAll(log *log.DXLog) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	return t.Select(log, nil, nil, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil)
}

func (t *DXTable) Count(log *log.DXLog, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (totalRows int64, summaryCalcRow utils.JSON, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}
		if t.Database == nil {
			t.Database = database.Manager.Databases[t.DatabaseNameId]
		}
		if t.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	totalRows, summaryCalcRow, err = t.Database.CountOne(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues, joinSQLPart)
	return totalRows, summaryCalcRow, err
}

/*
	func (t *DXTable) TxSelectCount(tx *database.DXDatabaseTx, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON) (totalRows int64, summaryCalcRow utils.JSON, err error) {
		if whereAndFieldNameValues == nil {
			whereAndFieldNameValues = utils.JSON{
				"is_deleted": false,
			}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

			if t.Database.DatabaseType.String() == "sqlserver" {
				whereAndFieldNameValues["is_deleted"] = 0
			}
		}

		totalRows, summaryCalcRow, err = tx.ShouldCount(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues)
		return totalRows, summaryCalcRow, err
	}
*/
func (t *DXTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}

		if t.Database == nil {
			t.Database = database.Manager.Databases[t.DatabaseNameId]
		}

		if t.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	rowsInfo, r, err = t.Database.Select(t.ListViewNameId, t.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, limit)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (t *DXTable) ShouldSelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	return t.Database.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
}

func (t *DXTable) TxShouldSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, nil)
}

func (t *DXTable) TxShouldSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.ShouldSelectOne(t.NameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXTable) TxSelect(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.Select(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, limit, false)
}

func (t *DXTable) TxSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, false)
}

func (t *DXTable) TxSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.SelectOne(t.NameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXTable) TxUpdate(tx *database.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXTable) TxSoftDelete(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Update(t.NameId, map[string]any{
		`is_deleted`: true,
	}, whereAndFieldNameValues)
}

func (t *DXTable) TxHardDelete(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Delete(t.NameId, whereAndFieldNameValues)
}

func (t *DXTable) DoRequestList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	if !t.Database.Connected {
		err := t.Database.Connect()
		if err != nil {
			return err
		}
	}

	rowsInfo, list, err := db.NamedQueryList(t.Database.Connection, t.FieldTypeMapping, "*", t.ListViewNameId,
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
			"rows":      list,
			"rows_info": rowsInfo,
		},
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return nil
}

func (t *DXTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
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

func (t *DXTable) RequestListAll(aepr *api.DXAPIEndPointRequest) (err error) {
	return t.DoRequestList(aepr, "", "", nil, nil)
}

func (t *DXTable) RequestList(aepr *api.DXAPIEndPointRequest) (err error) {
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

		if t.Database == nil {
			t.Database = database.Manager.Databases[t.DatabaseNameId]
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

	return t.DoRequestList(aepr, filterWhere, filterOrderBy, filterKeyValues, nil)
}

func (t *DXTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) (err error) {
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

		if t.Database == nil {
			t.Database = database.Manager.Databases[t.DatabaseNameId]
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

func (t *DXTable) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections db.FieldsOrderBy) (
	rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	return t.Database.SelectOne(t.ListViewNameId, t.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
}

func (t *DXTable) IsFieldValueExistAsString(log *log.DXLog, fieldName string, fieldValue string) (bool, error) {
	_, r, err := t.SelectOne(log, nil, utils.JSON{
		fieldName: fieldValue,
	}, nil, nil)
	if err != nil {
		return false, err
	}
	if r == nil {
		return false, nil
	}
	return true, nil
}

func (t *DXTable) RequestCreate(aepr *api.DXAPIEndPointRequest) (err error) {
	p := map[string]interface{}{}
	for k, v := range aepr.ParameterValues {
		p[k] = v.Value
	}
	_, err = t.DoCreate(aepr, p)

	return err
}

func (t *DXTable) RequestListDownload(aepr *api.DXAPIEndPointRequest) (err error) {
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

	_, format, err := aepr.GetParameterValueAsString("format")
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, `FORMAT_PARAMETER_ERROR:%s`, err.Error())
	}

	format = strings.ToLower(format)

	isDeletedIncluded := false
	if !isDeletedIncluded {
		if filterWhere != "" {
			filterWhere = fmt.Sprintf("(%s) and ", filterWhere)
		}

		if t.Database == nil {
			t.Database = database.Manager.Databases[t.DatabaseNameId]
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

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	if !t.Database.Connected {
		err := t.Database.Connect()
		if err != nil {
			aepr.Log.Errorf("error At reconnect db At table %s list (%s) ", t.NameId, err.Error())
			return err
		}
	}

	rowsInfo, list, err := db.NamedQueryList(t.Database.Connection, t.FieldTypeMapping, "*", t.ListViewNameId,
		filterWhere, "", filterOrderBy, filterKeyValues)

	if err != nil {
		return err
	}

	// Set export options
	opts := export.ExportOptions{
		Format:     export.ExportFormat(format),
		SheetName:  "Sheet1",
		DateFormat: "2006-01-02 15:04:05",
	}

	// Get file as stream
	data, contentType, err := export.ExportToStream(rowsInfo, list, opts)
	if err != nil {
		return err
	}

	// Set response headers
	filename := fmt.Sprintf("export_%s_%s.%s", t.NameId, time.Now().Format("20060102_150405"), format)

	responseWriter := *aepr.GetResponseWriter()
	responseWriter.Header().Set("Content-Type", contentType)
	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	responseWriter.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	responseWriter.WriteHeader(http.StatusOK)
	aepr.ResponseStatusCode = http.StatusOK

	_, err = responseWriter.Write(data)
	if err != nil {
		return err
	}

	aepr.ResponseHeaderSent = true
	aepr.ResponseBodySent = true

	return nil
}
