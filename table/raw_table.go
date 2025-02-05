package table

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/database/protected/export"
	utils2 "github.com/donnyhardyanto/dxlib/database/protected/utils"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"
)

type OnResultList func(listRow utils.JSON) (utils.JSON, error)

type DXRawTable struct {
	DatabaseNameId        string
	Database              *database.DXDatabase
	NameId                string
	ResultObjectName      string
	ListViewNameId        string
	FieldNameForRowId     string
	FieldNameForRowNameId string
	FieldTypeMapping      utils2.FieldTypeMapping
}

func (t *DXRawTable) RequestDoCreate(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	if err != nil {
		return 0, aepr.WriteResponseAndNewErrorf(http.StatusConflict, "ERROR_INSERTING_TABLE:"+t.NameId+"="+err.Error())
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		t.FieldNameForRowId: newId,
	})

	return newId, err
}

func (t *DXRawTable) GetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXRawTable) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXRawTable) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		"utag": utag,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXRawTable) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXRawTable) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXRawTable) TxShouldGetById(tx *database.DXDatabaseTx, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable) TxShouldGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable) TxInsert(tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
	newId, err = tx.Insert(t.NameId, newKeyValues)
	return newId, err
}

func (t *DXRawTable) InRequestTxInsert(aepr *api.DXAPIEndPointRequest, tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
	newId, err = tx.Insert(t.NameId, newKeyValues)
	return newId, err
}

func (t *DXRawTable) Insert(log *log.DXLog, newKeyValues utils.JSON) (newId int64, err error) {
	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (t *DXRawTable) Update(setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	return t.Database.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXRawTable) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
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

func (t *DXRawTable) InRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}
	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}

func (t *DXRawTable) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXRawTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXRawTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXRawTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, newKeyValues utils.JSON) (err error) {
	_, _, err = t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
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

func (t *DXRawTable) RequestEdit(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXRawTable) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {
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

func (t *DXRawTable) RequestHardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (t *DXRawTable) SelectAll(log *log.DXLog) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	return t.Select(log, nil, nil, nil, nil, nil)
}

/*func (t *DXRawTable) Count(log *log.DXLog, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (totalRows int64, summaryCalcRow utils.JSON, err error) {
	totalRows, summaryCalcRow, err = t.Database.ShouldCount(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues, joinSQLPart)
	return totalRows, summaryCalcRow, err
}
*/
/*
func (t *DXRawTable) TxSelectCount(tx *database.DXDatabaseTx, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON) (totalRows int64, summaryCalcRow utils.JSON, err error) {

		totalRows, summaryCalcRow, err = tx.ShouldCount(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues)
		return totalRows, summaryCalcRow, err
	}
*/
func (t *DXRawTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	rowsInfo, r, err = t.Database.Select(t.ListViewNameId, t.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (t *DXRawTable) ShouldSelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	return t.Database.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
}

func (t *DXRawTable) TxShouldSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, nil)
}
func (t *DXRawTable) TxShouldSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.ShouldSelectOne(t.NameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXRawTable) TxSelect(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	return tx.Select(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, limit, false)
}

func (t *DXRawTable) TxSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, false)
}

func (t *DXRawTable) TxSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.SelectOne(t.NameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXRawTable) TxUpdate(tx *database.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
}

func (t *DXRawTable) TxHardDelete(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	return tx.Delete(t.NameId, whereAndFieldNameValues)
}

func (t *DXRawTable) DoRequestList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
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

func (t *DXRawTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
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

func (t *DXRawTable) RequestListAll(aepr *api.DXAPIEndPointRequest) (err error) {
	return t.DoRequestList(aepr, "", "", nil, nil)
}

func (t *DXRawTable) RequestList(aepr *api.DXAPIEndPointRequest) (err error) {
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

	return t.DoRequestList(aepr, filterWhere, filterOrderBy, filterKeyValues, nil)
}

func (t *DXRawTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) (err error) {
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

	return t.DoRequestPagingList(aepr, filterWhere, filterOrderBy, filterKeyValues, nil)
}

func (t *DXRawTable) SelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections db.FieldsOrderBy) (
	rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	if !t.Database.Connected {
		err := t.Database.Connect()
		if err != nil {
			return nil, nil, err
		}
	}

	return t.Database.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
}

func (t *DXRawTable) IsFieldValueExistAsString(log *log.DXLog, fieldName string, fieldValue string) (bool, error) {
	_, r, err := t.SelectOne(log, utils.JSON{
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

func (t *DXRawTable) RequestCreate(aepr *api.DXAPIEndPointRequest) (err error) {
	p := map[string]interface{}{}
	for k, v := range aepr.ParameterValues {
		p[k] = v.Value
	}
	_, err = t.RequestDoCreate(aepr, p)

	return err
}

func (t *DXRawTable) RequestListDownload(aepr *api.DXAPIEndPointRequest) (err error) {
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
