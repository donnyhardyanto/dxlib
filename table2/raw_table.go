package table2

import (
	"fmt"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/database2/export"
	"github.com/donnyhardyanto/dxlib/table2/compatibility"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/pkg/errors"
)

func (rt *DXRawTable2) DoRequestEdit(aepr *api.DXAPIEndPointRequest, id int64, newKeyValues utils.JSON) (err error) {
	_, _, err = rt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	_, err = rt.Update(newKeyValues, utils.JSON{
		rt.FieldNameForRowId: id,
	})
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(rt.ResponseEnvelopeObjectName, utils.JSON{
		rt.FieldNameForRowId: id,
	},
	))
	return nil
}

func (bt *DXRawTable2) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) RequestReadByUid(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) DoRequestDeleteByIdOrUid(aepr *api.DXAPIEndPointRequest, id int64, uid string) (err error) {
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

func (bt *DXRawTable2) RequestHardDelete(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) RequestHardDeleteByUid(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) DoRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (err error) {

	// Execute OnBeforeInsert callback if provided
	if bt.OnBeforeInsert != nil {
		if err := bt.OnBeforeInsert(aepr, newKeyValues); err != nil {
			return err
		}
	}

	newId, newUid, err := bt.Insert(newKeyValues)
	if err != nil {
		return err
	}

	data := utilsJson.Encapsulate(bt.ResponseEnvelopeObjectName, utils.JSON{
		bt.FieldNameForRowId:  newId,
		bt.FieldNameForRowUid: newUid,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return nil
}

// Note: New name is RequestInsert, the RequestCreate is depreciated

func (bt *DXRawTable2) RequestInsert(aepr *api.DXAPIEndPointRequest) (err error) {
	p := map[string]interface{}{}
	for k, v := range aepr.ParameterValues {
		p[k] = v.Value
	}
	err = bt.DoRequestInsert(aepr, p)
	if err != nil {
		return err
	}
	return nil
}

// Note: New name is RequestInsert, the RequestCreate is depreciated

func (bt *DXRawTable2) RequestCreate(aepr *api.DXAPIEndPointRequest) (err error) {
	return bt.RequestInsert(aepr)
}

func (bt *DXRawTable2) DoRequestEditByIdOrUid(aepr *api.DXAPIEndPointRequest, id int64, uid string, newKeyValues utils.JSON) (err error) {

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

func (bt *DXRawTable2) RequestEdit(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) RequestEditByUid(aepr *api.DXAPIEndPointRequest) (err error) {
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

func (bt *DXRawTable2) DoRequestList(aepr *api.DXAPIEndPointRequest, filterWhere utils.JSON, filterOrderBy map[string]string, onResultList DXBaseTable2OnResultProcessEachListRow) (err error) {

	if bt.DoOverrideSelectValues != nil {
		filterWhere, err = bt.DoOverrideSelectValues(filterWhere)
		if err != nil {
			return err
		}
	}

	if filterWhere != nil {
		for k, v := range filterWhere {
			if vAsExpression, ok := v.(db.SQLExpression); ok {
				err = db.CheckBaseQuery(bt.DatabaseType, vAsExpression.String())
				if err != nil {
					return err
				}
			} else {
				err = db.CheckIdentifier(bt.DatabaseType, k)
				if err != nil {
					return err
				}
				err = db.CheckValue(bt.DatabaseType, v)
				if err != nil {
					return err
				}
			}
		}
	}

	if filterOrderBy != nil {
		for k, v := range filterOrderBy {
			err = db.CheckIdentifier(bt.DatabaseType, k)
			if err != nil {
				return err
			}
			err = db.CheckOrderByDirection(bt.DatabaseType, v)
			if err != nil {
				return err
			}
		}
	}

	rowsInfo, list, err := bt.Database.Select(bt.ListViewNameId, bt.FieldTypeMapping, nil,
		filterWhere, nil, nil, nil, filterOrderBy, nil, nil, nil)
	if err != nil {
		return errors.Wrap(err, "error occured")
	}

	if onResultList != nil {
		bt.OnResultProcessEachListRow = onResultList
	}
	for i := range list {
		if bt.OnResultProcessEachListRow != nil {
			aListRow, err := bt.OnResultProcessEachListRow(aepr, list[i])
			if err != nil {
				return err
			}
			list[i] = aListRow
		}

	}

	responseObject := utils.JSON{}
	if bt.OnResponseObjectConstructor != nil {
		responseObject, err = bt.OnResponseObjectConstructor(aepr, responseObject)
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

func (bt *DXRawTable2) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere utils.JSON, filterOrderBy map[string]string, pageIndex int64, rowsPerPage int64, onResultList DXBaseTable2OnResultProcessEachListRow) (err error) {
	if filterWhere != nil {
		for k, v := range filterWhere {
			if vAsExpression, ok := v.(db.SQLExpression); ok {
				err = db.CheckBaseQuery(bt.DatabaseType, vAsExpression.String())
				if err != nil {
					return err
				}
			} else {
				err = db.CheckIdentifier(bt.DatabaseType, k)
				if err != nil {
					return err
				}
				err = db.CheckValue(bt.DatabaseType, v)
				if err != nil {
					return err
				}
			}
		}
	}

	if filterOrderBy != nil {
		for k, v := range filterOrderBy {
			err = db.CheckIdentifier(bt.DatabaseType, k)
			if err != nil {
				return err
			}
			err = db.CheckOrderByDirection(bt.DatabaseType, v)
			if err != nil {
				return err
			}
		}
	}

	totalRowsCount, rowsInfo, list, err := bt.Database.SelectPaging(pageIndex, rowsPerPage, bt.ListViewNameId, bt.FieldTypeMapping, nil,
		filterWhere, nil, nil, nil, filterOrderBy)
	if err != nil {
		return err
	}

	totalPage := int((totalRowsCount + rowsPerPage - 1) / rowsPerPage)

	if onResultList != nil {
		bt.OnResultProcessEachListRow = onResultList
	}
	for i := range list {
		if bt.OnResultProcessEachListRow != nil {
			aListRow, err := bt.OnResultProcessEachListRow(aepr, list[i])
			if err != nil {
				return err
			}
			list[i] = aListRow
		}

	}

	responseObject := utils.JSON{}
	if bt.OnResponseObjectConstructor != nil {
		responseObject, err = bt.OnResponseObjectConstructor(aepr, responseObject)
	} else {
		responseObject = utilsJson.Encapsulate(
			"data", utils.JSON{
				"list": utils.JSON{
					"rows":       list,
					"rows_info":  rowsInfo,
					"total_rows": totalRowsCount,
					"total_page": totalPage,
				},
			})
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseObject)

	return nil
}

func (bt *DXRawTable2) RequestListAll(aepr *api.DXAPIEndPointRequest) (err error) {
	return bt.DoRequestList(aepr, nil, nil, nil)
}

func (bt *DXRawTable2) RequestList(aepr *api.DXAPIEndPointRequest) (err error) {
	_, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}

	ifExistWhereKV, whereKV, err := aepr.GetParameterValueAsJSON("where")
	if err != nil {
		return err
	}

	_, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}

	isExistOrderByKV, orderByKVAsJSON, err := aepr.GetParameterValueAsJSON("order_by")
	if err != nil {
		return err
	}
	var orderByKV map[string]string

	if isExistOrderByKV {
		for k, v := range orderByKVAsJSON {
			orderByKV[k] = v.(string)
		}
	}

	_, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}

	if ifExistWhereKV {
		whereKV, err = compatibility.TranslateFilterWhereToWhereKV(filterWhere, filterKeyValues)
		if err != nil {
			return err
		}
	}

	if isExistOrderByKV {
		orderByKV, err = compatibility.TranslateFilterOrderByToOrderByKV(filterOrderBy)
		if err != nil {
			return err
		}
	}

	return bt.DoRequestList(aepr, whereKV, orderByKV, nil)
}

func (bt *DXRawTable2) RequestPagingList(aepr *api.DXAPIEndPointRequest) (err error) {
	_, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}

	ifExistWhereKV, whereKV, err := aepr.GetParameterValueAsJSON("where")
	if err != nil {
		return err
	}

	_, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}

	isExistOrderByKV, orderByKVAsJSON, err := aepr.GetParameterValueAsJSON("order_by")
	if err != nil {
		return err
	}
	var orderByKV map[string]string

	if isExistOrderByKV {
		for k, v := range orderByKVAsJSON {
			orderByKV[k] = v.(string)
		}
	}

	_, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}

	if ifExistWhereKV {
		whereKV, err = compatibility.TranslateFilterWhereToWhereKV(filterWhere, filterKeyValues)
		if err != nil {
			return err
		}
	}

	if isExistOrderByKV {
		orderByKV, err = compatibility.TranslateFilterOrderByToOrderByKV(filterOrderBy)
		if err != nil {
			return err
		}
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	_, rowsPerPage, err := aepr.GetParameterValueAsInt64("rows_per_page")
	if err != nil {
		return err
	}

	return bt.DoRequestPagingList(aepr, whereKV, orderByKV, pageIndex, rowsPerPage, nil)
}

func (bt *DXRawTable2) RequestListDownload(aepr *api.DXAPIEndPointRequest) (err error) {
	_, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}

	isWhereKVExist, whereKV, err := aepr.GetParameterValueAsJSON("where")
	if err != nil {
		return err
	}

	_, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}

	isOrderByKVExist, orderByKVAsJSON, err := aepr.GetParameterValueAsJSON("order_by")
	if err != nil {
		return err
	}
	var orderByKV map[string]string

	if isOrderByKVExist {
		for k, v := range orderByKVAsJSON {
			orderByKV[k] = v.(string)
		}
	}

	_, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}

	if !isWhereKVExist {
		whereKV, err = compatibility.TranslateFilterWhereToWhereKV(filterWhere, filterKeyValues)
		if err != nil {
			return err
		}
	}

	if !isOrderByKVExist {
		orderByKV, err = compatibility.TranslateFilterOrderByToOrderByKV(filterOrderBy)
		if err != nil {
			return err
		}
	}

	_, format, err := aepr.GetParameterValueAsString("format")
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "FORMAT_PARAMETER_ERROR:%s", err.Error())
	}

	format = strings.ToLower(format)

	_, isDeleted, err := aepr.GetParameterValueAsBool("is_deleted", true)
	if err != nil {
		return err
	}
	if !isDeleted {
		whereKV["is_deleted"] = false
	}

	rowsInfo, list, err := db.Select(bt.Database.Connection, bt.ListViewNameId, bt.FieldTypeMapping, nil,
		whereKV, "", nil, nil, orderByKV, nil, nil, nil)

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
