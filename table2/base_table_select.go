package table

import (
	"fmt"
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/database/protected/export"
	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/database2/db/raw"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/database2/sqlchecker"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/pkg/errors"
	"net/http"
	"strings"
	"time"
)

func (bt *DXBaseTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections utils2.FieldsOrderBy, limit any, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, r []utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = bt.Database.Select(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, limit, offset, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (bt *DXBaseTable) Count(log *log.DXLog, whereAndFieldNameValues utils.JSON, joinSQLPart any) (count int64, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return 0, err
	}

	count, err = bt.Database.Count(bt.ListViewNameId, whereAndFieldNameValues, joinSQLPart)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (bt *DXBaseTable) ShouldSelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdate any) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	return bt.Database.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdate any) (
	rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	return bt.Database.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(bt.FieldNameForRowId)
	if err != nil {
		return err
	}
	rowsInfo, d, err := bt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil,
		utilsJson.Encapsulate(bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (bt *DXBaseTable) RequestReadByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(bt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	rowsInfo, d, err := bt.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil,
		utilsJson.Encapsulate(bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (bt *DXBaseTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
	_, nameid, err := aepr.GetParameterValueAsString(bt.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := bt.ShouldGetByNameId(&aepr.Log, nameid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(
		bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (bt *DXBaseTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, d, err := bt.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(
		bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (bt *DXBaseTable) TxSelect(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections utils2.FieldsOrderBy, limit any, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, r []utils.JSON, err error) {

	return tx.Select(bt.ListViewNameId, bt.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderByFieldNameDirections, limit, offset, forUpdatePart)
}

func (bt *DXBaseTable) TxCount(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON, joinSQLPart any) (count int64, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return 0, err
	}

	count, err = tx.Count(bt.ListViewNameId, whereAndFieldNameValues, joinSQLPart)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (bt *DXBaseTable) TxSelectOne(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderByFieldNameDirections utils2.FieldsOrderBy, offset any,
	forUpdate any) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {

	return tx.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, nil, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable) TxShouldSelectOne(tx *database.DXDatabaseTx, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdate any) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	return tx.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable) DoRequestList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return err
	}

	sqlStatement := strings.Join([]string{"SELECT * FROM", bt.ListViewNameId}, " ")

	if filterWhere != "" {
		err = sqlchecker.CheckBaseQuery(filterWhere, bt.DatabaseType)
		sqlStatement = sqlStatement + " WHERE " + filterWhere
	}
	if filterOrderBy != "" {
		err = sqlchecker.CheckOrderBy(filterOrderBy, bt.DatabaseType)
		if err != nil {
			return err
		}
		sqlStatement = sqlStatement + " ORDER BY " + filterOrderBy
	}
	err = sqlchecker.CheckAll(bt.DatabaseType, sqlStatement, nil)
	if err != nil {
		return err
	}

	rowsInfo, list, err := raw.QueryRows(bt.Database.Connection, bt.FieldTypeMapping, sqlStatement, filterKeyValues)

	if err != nil {
		return err
	}

	for i := range list {

		if bt.OnResultProcessEachListRow != nil {
			aListRow, err := bt.OnResultProcessEachListRow(aepr, bt, list[i])
			if err != nil {
				return err
			}
			list[i] = aListRow
		}

	}

	responseObject := utils.JSON{}
	if bt.OnResponseObjectConstructor != nil {
		responseObject, err = bt.OnResponseObjectConstructor(aepr, bt, responseObject)
	} else {
		responseObject = utilsJson.Encapsulate(
			"data", utils.JSON{
				"list": utils.JSON{
					"rows":      list,
					"rows_info": rowsInfo,
				},
			})
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseObject)

	return nil
}

func (bt *DXBaseTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) (err error) {
	sqlStatement := strings.Join([]string{"SELECT * FROM", bt.ListViewNameId}, " ")
	sqlCountStatement := strings.Join([]string{"SELECT count(*) as count_result FROM", bt.ListViewNameId}, " ")

	if filterWhere != "" {
		err = sqlchecker.CheckBaseQuery(filterWhere, bt.DatabaseType)
		sqlStatement = sqlStatement + " WHERE " + filterWhere
		sqlCountStatement = sqlCountStatement + " WHERE " + filterWhere
	}
	if filterOrderBy != "" {
		err = sqlchecker.CheckOrderBy(filterOrderBy, bt.DatabaseType)
		if err != nil {
			return err
		}
		sqlStatement = sqlStatement + " ORDER BY " + filterOrderBy
	}

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return errors.Wrap(err, "error occured")
	}

	err = sqlchecker.CheckAll(bt.DatabaseType, sqlStatement, nil)
	if err != nil {
		return err
	}

	dtx, err := bt.Database.TransactionBegin(database.LevelRepeatableRead)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			dtx.Rollback()
		} else {
			dtx.Commit()
		}
	}()

	count, err := raw.TxQueryRows(dtx, bt.FieldTypeMapping, sqlCountStatement, filterKeyValues)
	if err != nil {
		return err
	}

	_, countList, err := raw.QueryRows(bt.Database.Connection, bt.FieldTypeMapping, sqlCountStatement, filterKeyValues)
	if err != nil {
		return err
	}

	totalRows := int(countList[0].(utils.JSON)["count_result"].(float64))
	totalPage := int((totalRows + rowPerPage - 1) / rowPerPage)

	rowsInfo, list, err := raw.QueryRows(bt.Database.Connection, bt.FieldTypeMapping, sqlStatement, filterKeyValues)
	if err != nil {
		return errors.Wrap(err, "error occured")
	}

	/*	rowsInfo, list, totalRows, totalPage, _, err := db.NamedQueryPaging(bt.Database.Connection, bt.FieldTypeMapping, "", rowPerPage, pageIndex, "*", bt.ListViewNameId,
			filterWhere, "", filterOrderBy, filterKeyValues)
		if err != nil {
			return errors.Wrap(err, "error occured")
		}
	*/
	for i := range list {

		if onResultList != nil {
			aListRow, err := onResultList(list[i])
			if err != nil {
				return errors.Wrap(err, "error occured")
			}
			list[i] = aListRow
		}

	}

	data := utils.JSON{
		"data": utils.JSON{
			"list": utils.JSON{
				"rows":       list,
				"total_rows": totalRows,
				"total_page": totalPage,
				"rows_info":  rowsInfo,
			},
		},
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return nil
}

func (bt *DXBaseTable) RequestListAll(aepr *api.DXAPIEndPointRequest) (err error) {
	return bt.DoRequestList(aepr, "", "", nil, nil)
}

func (bt *DXBaseTable) RequestList(aepr *api.DXAPIEndPointRequest) (err error) {
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

	return bt.DoRequestList(aepr, filterWhere, filterOrderBy, filterKeyValues, nil)
}

func (bt *DXBaseTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) (err error) {
	isExistFilterWhere, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return errors.Wrap(err, "error occured")
	}
	if !isExistFilterWhere {
		filterWhere = ""
	}
	isExistFilterOrderBy, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return errors.Wrap(err, "error occured")
	}
	if !isExistFilterOrderBy {
		filterOrderBy = ""
	}

	isExistFilterKeyValues, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return errors.Wrap(err, "error occured")
	}
	if !isExistFilterKeyValues {
		filterKeyValues = nil
	}

	return bt.DoRequestPagingList(aepr, filterWhere, filterOrderBy, filterKeyValues, nil)
}

func (bt *DXBaseTable) IsFieldValueExistAsString(log *log.DXLog, fieldName string, fieldValue string) (bool, error) {
	_, r, err := bt.SelectOne(log, nil, utils.JSON{
		fieldName: fieldValue,
	}, nil, nil, nil, nil)
	if err != nil {
		return false, err
	}
	if r == nil {
		return false, nil
	}
	return true, nil
}

func (bt *DXBaseTable) RequestListDownload(aepr *api.DXAPIEndPointRequest) (err error) {
	isExistFilterWhere, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return errors.Wrap(err, "error occured")
	}
	if !isExistFilterWhere {
		filterWhere = ""
	}
	isExistFilterOrderBy, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return errors.Wrap(err, "error occured")
	}
	if !isExistFilterOrderBy {
		filterOrderBy = ""
	}

	isExistFilterKeyValues, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return errors.Wrap(err, "error occured")
	}
	if !isExistFilterKeyValues {
		filterKeyValues = nil
	}

	_, format, err := aepr.GetParameterValueAsString("format")
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "FORMAT_PARAMETER_ERROR:%s", err.Error())
	}

	format = strings.ToLower(format)

	isDeletedIncluded := false
	if !isDeletedIncluded {
		if filterWhere != "" {
			filterWhere = fmt.Sprintf("(%s) and ", filterWhere)
		}

		if bt.Database == nil {
			bt.Database = database.Manager.Databases[bt.DatabaseNameId]
		}

		switch bt.Database.DatabaseType.String() {
		case "sqlserver":
			filterWhere = filterWhere + "(is_deleted=0)"
		case "postgres":
			filterWhere = filterWhere + "(is_deleted=false)"
		default:
			filterWhere = filterWhere + "(is_deleted=0)"
		}
	}

	if bt.Database == nil {
		bt.Database = database.Manager.Databases[bt.DatabaseNameId]
	}

	if !bt.Database.Connected {
		err = bt.Database.Connect()
		if err != nil {
			aepr.Log.Errorf("error At reconnect db At table %s list (%+v) ", bt.NameId, err)
			return errors.Wrap(err, "error occured")
		}
	}

	rowsInfo, list, err := db.NamedQueryList(bt.Database.Connection, bt.FieldTypeMapping, "*", bt.ListViewNameId,
		filterWhere, "", filterOrderBy, filterKeyValues)

	if err != nil {
		return errors.Wrap(err, "error occured")
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
		return errors.Wrap(err, "error occured")
	}

	// Set response headers
	filename := fmt.Sprintf("export_%s_%s.%s", bt.NameId, time.Now().Format("20060102_150405"), format)

	responseWriter := *aepr.GetResponseWriter()
	responseWriter.Header().Set("Content-Type", contentType)
	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	responseWriter.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	responseWriter.WriteHeader(http.StatusOK)
	aepr.ResponseStatusCode = http.StatusOK

	_, err = responseWriter.Write(data)
	if err != nil {
		return errors.Wrap(err, "error occured")
	}

	aepr.ResponseHeaderSent = true
	aepr.ResponseBodySent = true

	return nil
}
