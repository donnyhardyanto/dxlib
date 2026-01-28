package tables

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// DXTable - Table wrapper with soft-delete and audit fields

// DXTable extends DXRawTable with soft-delete and audit fields
type DXTable struct {
	DXRawTable
}

// Audit ModelDBField Helpers

// SetInsertAuditFields sets created_at, created_by_user_id, etc. for insert
func (t *DXTable) SetInsertAuditFields(aepr *api.DXAPIEndPointRequest, data utils.JSON) {
	now := time.Now().UTC()

	data["is_deleted"] = false
	data["created_at"] = now
	data["last_modified_at"] = now

	if aepr != nil && aepr.CurrentUser.Id != "" {
		data["created_by_user_id"] = aepr.CurrentUser.Id
		data["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		data["last_modified_by_user_id"] = aepr.CurrentUser.Id
		data["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
	} else {
		data["created_by_user_id"] = "0"
		data["created_by_user_nameid"] = "SYSTEM"
		data["last_modified_by_user_id"] = "0"
		data["last_modified_by_user_nameid"] = "SYSTEM"
	}
}

// SetUpdateAuditFields sets last_modified_at, last_modified_by_user_id, etc. for update
func (t *DXTable) SetUpdateAuditFields(aepr *api.DXAPIEndPointRequest, data utils.JSON) {
	now := time.Now().UTC()

	data["last_modified_at"] = now

	if aepr != nil && aepr.CurrentUser.Id != "" {
		data["last_modified_by_user_id"] = aepr.CurrentUser.Id
		data["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
	} else {
		data["last_modified_by_user_id"] = "0"
		data["last_modified_by_user_nameid"] = "SYSTEM"
	}
}

// Soft Delete Operations

// SoftDelete marks rows as deleted
func (t *DXTable) SoftDelete(l *log.DXLog, where utils.JSON) (sql.Result, error) {
	data := utils.JSON{
		"is_deleted": true,
	}
	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.DXRawTable.Update(l, data, where, nil)
	return result, err
}

// TxSoftDelete marks rows as deleted within a transaction
func (t *DXTable) TxSoftDelete(dtx *database.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	data := utils.JSON{
		"is_deleted": true,
	}
	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.DXRawTable.TxUpdate(dtx, data, where, nil)
	return result, err
}

// SoftDeleteById marks a row as deleted by ID
func (t *DXTable) SoftDeleteById(l *log.DXLog, id int64) (sql.Result, error) {
	return t.SoftDelete(l, utils.JSON{t.FieldNameForRowId: id})
}

// TxSoftDeleteById marks a row as deleted by ID within a transaction
func (t *DXTable) TxSoftDeleteById(dtx *database.DXDatabaseTx, id int64) (sql.Result, error) {
	return t.TxSoftDelete(dtx, utils.JSON{t.FieldNameForRowId: id})
}

// DoSoftDelete is an API helper for soft delete
func (t *DXTable) DoSoftDelete(aepr *api.DXAPIEndPointRequest, id int64) error {
	_, row, err := t.ShouldGetByIdNotDeleted(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	data := utils.JSON{
		"is_deleted": true,
	}
	t.SetUpdateAuditFields(aepr, data)

	_, err = t.DXRawTable.UpdateById(&aepr.Log, id, data)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// TxHardDelete deletes rows within a transaction (bypasses soft-delete)
func (t *DXTable) TxHardDelete(dtx *database.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable.TxDelete(dtx, where, nil)
	return result, err
}

// Paging Operations (with is_deleted = false filter)

// NewQueryBuilder creates a QueryBuilder with NotDeleted filter pre-applied
func (t *DXTable) NewQueryBuilder() *QueryBuilder {
	qb := NewQueryBuilder(t.GetDbType())
	qb.NotDeleted()
	return qb
}

// NewQueryBuilderRaw creates a QueryBuilder without NotDeleted filter (for special cases)
func (t *DXTable) NewQueryBuilderRaw() *QueryBuilder {
	return NewQueryBuilder(t.GetDbType())
}

// addNotDeletedToWhere adds is_deleted filter to existing WHERE clause
func (t *DXTable) addNotDeletedToWhere(whereClause string) string {
	var notDeletedClause string
	switch t.GetDbType() {
	case base.DXDatabaseTypeSQLServer:
		notDeletedClause = "is_deleted = 0"
	default:
		notDeletedClause = "is_deleted = false"
	}

	if whereClause == "" {
		return notDeletedClause
	}
	return fmt.Sprintf("(%s) AND %s", whereClause, notDeletedClause)
}

// Paging executes a paging query with automatic is_deleted filter
func (t *DXTable) Paging(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	return t.DXRawTable.Paging(l, rowPerPage, pageIndex, t.addNotDeletedToWhere(whereClause), orderBy, args)
}

// PagingWithBuilder executes a paging query using a QueryBuilder (assumes NotDeleted already added)
func (t *DXTable) PagingWithBuilder(l *log.DXLog, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.DXRawTable.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// PagingIncludeDeleted executes a paging query including deleted records
func (t *DXTable) PagingIncludeDeleted(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	return t.DXRawTable.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPaging is an API helper with automatic is_deleted filter
func (t *DXTable) DoPaging(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	return t.Paging(&aepr.Log, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPagingWithBuilder is an API helper using QueryBuilder
func (t *DXTable) DoPagingWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	return t.PagingWithBuilder(&aepr.Log, rowPerPage, pageIndex, qb, orderBy)
}

// DoPagingResponse executes paging and writes standard JSON response
func (t *DXTable) DoPagingResponse(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) error {
	result, err := t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// DoPagingResponseWithBuilder executes paging with QueryBuilder and writes response
func (t *DXTable) DoPagingResponseWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) error {
	result, err := t.DoPagingWithBuilder(aepr, rowPerPage, pageIndex, qb, orderBy)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// Upsert Operations (with audit fields)

// Upsert inserts or updates with audit fields
func (t *DXTable) Upsert(l *log.DXLog, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, 0, err
	}

	_, existing, err := t.DXRawTable.SelectOne(l, nil, where, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	if existing == nil {
		t.SetInsertAuditFields(nil, data)
		insertData := utilsJson.DeepMerge2(data, where)
		_, returningValues, err := t.Database.Insert(t.TableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.Database.Update(t.TableName(), data, where, nil)
	return result, 0, err
}

// TxUpsert inserts or updates within a transaction with audit fields
func (t *DXTable) TxUpsert(dtx *database.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	_, existing, err := t.DXRawTable.TxSelectOne(dtx, nil, where, nil, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	if existing == nil {
		t.SetInsertAuditFields(nil, data)
		insertData := utilsJson.DeepMerge2(data, where)
		_, returningValues, err := dtx.Insert(t.TableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	t.SetUpdateAuditFields(nil, data)
	result, _, err := dtx.Update(t.TableName(), data, where, nil)
	return result, 0, err
}

// API Request Helpers

// RequestPagingList handles list/paging API requests (with is_deleted filter)
func (t *DXTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) error {
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

	result, err := t.Paging(&aepr.Log, rowPerPage, pageIndex, filterWhere, filterOrderBy, filterKeyValues)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// DoRequestPagingList handles paging with optional result processing.
// If the request contains an "is_deleted" parameter set to true, deleted records are included.
// Otherwise, only non-deleted records are returned (is_deleted=false filter applied).
func (t *DXTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) error {
	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	_, isDeletedIncluded, _ := aepr.GetParameterValueAsBool("is_deleted", false)

	var result *PagingResult
	if isDeletedIncluded {
		result, err = t.PagingIncludeDeleted(&aepr.Log, rowPerPage, pageIndex, filterWhere, filterOrderBy, filterKeyValues)
	} else {
		result, err = t.Paging(&aepr.Log, rowPerPage, pageIndex, filterWhere, filterOrderBy, filterKeyValues)
	}
	if err != nil {
		return err
	}

	if onResultList != nil {
		result.Rows, err = onResultList(aepr, result.Rows)
		if err != nil {
			return err
		}
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// RequestSoftDelete handles soft delete by ID API requests
func (t *DXTable) RequestSoftDelete(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	return t.DoSoftDelete(aepr, id)
}

// RequestHardDelete handles hard delete by ID API requests (bypasses soft-delete)
func (t *DXTable) RequestHardDelete(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, row, err := t.ShouldGetByIdNotDeleted(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	_, err = t.DXRawTable.DeleteById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// RequestPagingListAll handles paging list all API requests (no filter, all records)
func (t *DXTable) RequestPagingListAll(aepr *api.DXAPIEndPointRequest) error {
	return t.DoRequestPagingList(aepr, "", "", nil, nil)
}

// RequestList handles list API requests (with filters from parameters)
func (t *DXTable) RequestList(aepr *api.DXAPIEndPointRequest) error {
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

// RequestListAll handles list all API requests (no paging, all records)
func (t *DXTable) RequestListAll(aepr *api.DXAPIEndPointRequest) error {
	return t.RequestList(aepr)
}

// RequestSoftDeleteByUid handles soft delete by UID API requests
func (t *DXTable) RequestSoftDeleteByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.ShouldGetByUidNotDeleted(&aepr.Log, uid)
	if err != nil {
		return err
	}

	id, ok := row[t.FieldNameForRowId].(int64)
	if !ok {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "CANNOT_GET_ID_FROM_ROW")
	}

	return t.DoSoftDelete(aepr, id)
}

// RequestHardDeleteByUid handles hard delete by UID API requests
func (t *DXTable) RequestHardDeleteByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.ShouldGetByUidNotDeleted(&aepr.Log, uid)
	if err != nil {
		return err
	}

	id, ok := row[t.FieldNameForRowId].(int64)
	if !ok {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "CANNOT_GET_ID_FROM_ROW")
	}

	_, err = t.DXRawTable.DeleteById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}
