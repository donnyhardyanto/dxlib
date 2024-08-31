package table

import (
	"database/sql"
	"dxlib/v3/api"
	"dxlib/v3/database"
	"dxlib/v3/database/protected/db"
	"dxlib/v3/log"
	"dxlib/v3/utils"
	"fmt"
	"net/http"
)

type DXTableManager struct {
	Tables                               map[string]*DXTable
	StandardOperationResponsePossibility map[string]map[string]*api.DXAPIEndPointResponsePossibility
}

type DXTable struct {
	DatabaseNameId        string
	Database              *database.DXDatabase
	NameId                string
	ResultObjectName      string
	ListViewNameId        string
	FieldNameForRowId     string
	FieldNameForRowNameId string
}

func (tm *DXTableManager) ConnectAll() (err error) {
	for _, t := range tm.Tables {
		d, ok := database.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return err
		}
		t.Database = d
	}
	return nil
}

func (tm *DXTableManager) NewTable(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForId string) *DXTable {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := DXTable{
		DatabaseNameId:        databaseNameId,
		NameId:                tableNameId,
		ResultObjectName:      resultObjectName,
		ListViewNameId:        tableListViewNameId,
		FieldNameForRowId:     tableFieldNameForId,
		FieldNameForRowNameId: tableFieldNameForRowNameId,
	}
	t.Database = database.Manager.Databases[databaseNameId]
	tm.Tables[tableNameId] = &t
	return &t
}

func (t *DXTable) DoCreate(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	n := utils.NowAsString()
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = n
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.ID != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.ID
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.Name
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = n
		if aepr.CurrentUser.ID != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.ID
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.Name
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	if err != nil {
		return 0, aepr.WriteResponseAndNewErrorf(http.StatusConflict, "ERROR_INSERTING_TABLE:%s=%s", t.NameId, err)
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		t.FieldNameForRowId: newId,
	})

	return newId, err
}

func (t *DXTable) MustGetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.MustSelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXTable) MustGetByNameId(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.MustSelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: id,
		"is_deleted":            false,
	}, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXTable) TxMustGetById(tx *database.DXDatabaseTx, code string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.MustSelectOne(t.ListViewNameId, []string{`*`}, utils.JSON{
		t.FieldNameForRowId: code,
		"is_deleted":        false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXTable) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(t.ListViewNameId, []string{`*`}, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXTable) TxMustGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.MustSelectOne(t.ListViewNameId, []string{`*`}, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXTable) TxInsert(tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
	n := utils.NowAsString()
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = n
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
		if aepr.CurrentUser.ID != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.ID
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.Name
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = n
		if aepr.CurrentUser.ID != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.ID
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.Name
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	newId, err = tx.Insert(t.NameId, newKeyValues)
	return newId, err
}

func (t *DXTable) Insert(log *log.DXLog, newKeyValues utils.JSON) (newId int64, err error) {
	n := utils.NowAsString()
	/*	if t.Database.DatabaseType.String() == "sqlserver" {
		t, err := time.Parse(time.RFC3339, n)
		if err != nil {
			fmt.Println("Error:", err)
			return 0, err
		}
		// Format the time.Time value back into a string without the timezone offset
		n = t.Format("2006-01-02 15:04:05")
	}*/
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = n
	newKeyValues["last_modified_at"] = n
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

func (t *DXTable) Update(setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return t.Database.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXTable) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
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
		if aepr.CurrentUser.ID != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.ID
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.Name
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = n
		if aepr.CurrentUser.ID != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.ID
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.Name
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (t *DXTable) Read(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.Database.SelectOne(t.ListViewNameId, nil, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (t *DXTable) ReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
	_, nameid, err := aepr.GetParameterValueAsString(t.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.Database.SelectOne(t.ListViewNameId, nil, utils.JSON{
		t.FieldNameForRowNameId: nameid,
		"is_deleted":            false,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"})
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo})

	return nil
}

func (t *DXTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, newKeyValues utils.JSON) (err error) {
	n := utils.NowAsString()
	newKeyValues["last_modified_at"] = n
	_, ok := newKeyValues["last_modified_by_user_id"]
	if !ok {
		if aepr.CurrentUser.ID != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.ID
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.Name
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	_, err = db.UpdateWhereKeyValues(t.Database.Connection, t.NameId, newKeyValues, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoEdit (%s) ", t.NameId, err)
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (t *DXTable) Edit(aepr *api.DXAPIEndPointRequest) (err error) {
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
	_, err = db.DeleteWhereKeyValues(t.Database.Connection, t.NameId, utils.JSON{
		t.FieldNameForRowId: id,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoDelete (%s) ", t.NameId, err)
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (t *DXTable) SoftDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	newFieldValues := utils.JSON{
		"is_deleted": true,
	}

	err = t.DoEdit(aepr, id, newFieldValues)
	if err != nil {
		aepr.Log.Errorf("Error at %s.SoftDelete (%s) ", t.NameId, err)
		return err
	}
	return err
}

func (t *DXTable) HardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	err = t.DoDelete(aepr, id)
	if err != nil {
		aepr.Log.Errorf("Error at %s.HardDelete (%s) ", t.NameId, err)
		return err
	}
	return err
}

func (t *DXTable) Select(log *log.DXLog, fieldNames *[]string, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections map[string]string, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if fieldNames == nil {
		fieldNames = &[]string{"*"}
	}

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{
			"is_deleted": false,
		}
		if t.Database.DatabaseType.String() == "sqlserver" {
			whereAndFieldNameValues["is_deleted"] = 0
		}
	}

	rowsInfo, r, err = t.Database.Select(t.ListViewNameId, *fieldNames,
		whereAndFieldNameValues, orderbyFieldNameDirections, limit)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (t *DXTable) MustSelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections map[string]string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return t.Database.MustSelectOne(t.ListViewNameId,
		whereAndFieldNameValues, orderbyFieldNameDirections)
}

func (t *DXTable) TxMustSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections map[string]string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.MustSelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, nil)
}

func (t *DXTable) TxSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections map[string]string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.SelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, false)
}

func (t *DXTable) TxSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections map[string]string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.SelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXTable) TxUpdate(tx *database.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result utils.JSON, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	return tx.UpdateOne(t.ListViewNameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXTable) List(aepr *api.DXAPIEndPointRequest) (err error) {
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

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
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
			filterWhere = filterWhere + "(is_deleted=false)"
		}
	}

	if !t.Database.Connected {
		err := t.Database.Connect()
		if err != nil {
			aepr.Log.Errorf("error at reconnect db at table %s list (%s) ", t.NameId, err)
			return err
		}
	}

	rowsInfo, list, totalRows, totalPage, _, err := db.NamedQueryPaging(t.Database.Connection, "", rowPerPage, pageIndex, "*", t.ListViewNameId,
		filterWhere, "", filterOrderBy, filterKeyValues)
	if err != nil {
		aepr.Log.Errorf("Error at paging table %s (%s) ", t.NameId, err)
		return err
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

func (t *DXTable) SelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON, orderbyFieldNameDirections map[string]string) (
	rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false

	if t.Database == nil {
		err = t.Database.Connect()
		if err != nil {
			return nil, nil, err
		}
	}

	return t.Database.SelectOne(t.ListViewNameId, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections)
}

func (t *DXTable) IsFieldValueExistAsString(log *log.DXLog, fieldName string, fieldValue string) (bool, error) {
	_, r, err := t.SelectOne(log, utils.JSON{
		fieldName: fieldValue,
	}, nil)
	if err != nil {
		return false, err
	}
	if r == nil {
		return false, nil
	}
	return true, nil
}

var Manager DXTableManager

func init() {
	Manager = DXTableManager{Tables: map[string]*DXTable{}, StandardOperationResponsePossibility: map[string]map[string]*api.DXAPIEndPointResponsePossibility{
		"create": {
			"success": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  200,
				Description: "Success - 200",
				DataTemplate: []*api.DXAPIEndPointParameter{
					{NameId: "id", Type: "int64", Description: "", IsMustExist: true},
				},
			},
			"invalid_request": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   400,
				Description:  "Invalid request - 400",
				DataTemplate: nil,
			},
			"invalid_credential": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   409,
				Description:  "Invalid credential - 409",
				DataTemplate: nil,
			},
			"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   422,
				Description:  "Unprocessable entity - 422",
				DataTemplate: nil,
			},
			"internal_error": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  500,
				Description: "Internal error - 500",
			}},
		"read": {
			"success": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   200,
				Description:  "Success - 200",
				DataTemplate: []*api.DXAPIEndPointParameter{},
			},
			"invalid_request": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   400,
				Description:  "Invalid request - 400",
				DataTemplate: nil,
			},
			"invalid_credential": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   409,
				Description:  "Invalid credential - 409",
				DataTemplate: nil,
			},
			"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   422,
				Description:  "Unprocessable entity - 422",
				DataTemplate: nil,
			},
			"internal_error": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  500,
				Description: "Internal error - 500",
			}},
		"edit": {
			"success": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   200,
				Description:  "Success - 200",
				DataTemplate: []*api.DXAPIEndPointParameter{},
			},
			"invalid_request": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   400,
				Description:  "Invalid request - 400",
				DataTemplate: nil,
			},
			"invalid_credential": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   409,
				Description:  "Invalid credential - 409",
				DataTemplate: nil,
			},
			"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   422,
				Description:  "Unprocessable entity - 422",
				DataTemplate: nil,
			},
			"internal_error": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  500,
				Description: "Internal error - 500",
			}},
		"delete": {
			"success": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   200,
				Description:  "Success - 200",
				DataTemplate: []*api.DXAPIEndPointParameter{},
			},
			"invalid_request": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   400,
				Description:  "Invalid request - 400",
				DataTemplate: nil,
			},
			"invalid_credential": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   409,
				Description:  "Invalid credential - 409",
				DataTemplate: nil,
			},
			"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   422,
				Description:  "Unprocessable entity - 422",
				DataTemplate: nil,
			},
			"internal_error": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  500,
				Description: "Internal error - 500",
			}},
		"list": {
			"success": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  200,
				Description: "Success - 200",
				DataTemplate: []*api.DXAPIEndPointParameter{
					{NameId: "list", Type: "json", Description: "", IsMustExist: true, Children: []api.DXAPIEndPointParameter{
						{NameId: "rows", Type: "array", Description: "", IsMustExist: true},
						{NameId: "total_rows", Type: "int64", Description: "", IsMustExist: true},
						{NameId: "total_page", Type: "int64", Description: "", IsMustExist: true},
					}},
				},
			},
			"invalid_request": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   400,
				Description:  "Invalid request - 400",
				DataTemplate: nil,
			},
			"invalid_credential": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   409,
				Description:  "Invalid credential - 409",
				DataTemplate: nil,
			},
			"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
				StatusCode:   422,
				Description:  "Unprocessable entity - 422",
				DataTemplate: nil,
			},
			"internal_error": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  500,
				Description: "Internal error - 500",
			}},
	}}
}
