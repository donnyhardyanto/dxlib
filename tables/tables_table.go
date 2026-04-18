package tables

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
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
func (t *DXTable) SoftDelete(ctx context.Context, l *log.DXLog, where utils.JSON) (sql.Result, error) {
	data := utils.JSON{
		"is_deleted": true,
	}
	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.DXRawTable.Update(ctx, l, data, where, nil)
	return result, err
}

// TxSoftDelete marks rows as deleted within a transaction
func (t *DXTable) TxSoftDelete(dtx *databases.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	data := utils.JSON{
		"is_deleted": true,
	}
	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.DXRawTable.TxUpdate(dtx, data, where, nil)
	return result, err
}

// SoftDeleteById marks a row as deleted by ID
func (t *DXTable) SoftDeleteById(ctx context.Context, l *log.DXLog, id int64) (sql.Result, error) {
	return t.SoftDelete(ctx, l, utils.JSON{t.FieldNameForRowId: id})
}

// TxSoftDeleteById marks a row as deleted by ID within a transaction
func (t *DXTable) TxSoftDeleteById(dtx *databases.DXDatabaseTx, id int64) (sql.Result, error) {
	return t.TxSoftDelete(dtx, utils.JSON{t.FieldNameForRowId: id})
}

// DoSoftDelete is an API helper for soft delete
func (t *DXTable) DoSoftDelete(aepr *api.DXAPIEndPointRequest, id int64) error {
	_, row, err := t.GetByIdNotDeletedAuto(aepr.Context, &aepr.Log, id)
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

	_, err = t.DXRawTable.UpdateById(aepr.Context, &aepr.Log, id, data)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// TxHardDelete deletes rows within a transaction (bypasses soft-delete)
func (t *DXTable) TxHardDelete(dtx *databases.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable.TxDelete(dtx, where, nil)
	return result, err
}

// Upsert Operations (with audit fields)

// Upsert atomically inserts or updates a row identified by where, setting audit fields per path.
// On INSERT path: created_* + last_modified_* are set. On UPDATE path: only last_modified_* changes.
// whereKeys columns MUST have a UNIQUE/PK constraint; see db.Upsert godoc for full contract.
// Returns: (result, id, isInsert, error).
func (t *DXTable) Upsert(ctx context.Context, l *log.DXLog, data utils.JSON, where utils.JSON) (sql.Result, int64, bool, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, 0, false, err
	}

	insertData := utilsJson.DeepMerge2(data, where)
	t.SetInsertAuditFields(nil, insertData)

	updateData := utils.JSON{}
	for k, v := range data {
		updateData[k] = v
	}
	t.SetUpdateAuditFields(nil, updateData)

	return db.Upsert(ctx, t.Database.Connection, t.GetFullTableName(), insertData, updateData, where, t.FieldNameForRowId)
}

// TxUpsert is the transactional variant of Upsert. Same audit semantics.
func (t *DXTable) TxUpsert(dtx *databases.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, int64, bool, error) {
	insertData := utilsJson.DeepMerge2(data, where)
	t.SetInsertAuditFields(nil, insertData)

	updateData := utils.JSON{}
	for k, v := range data {
		updateData[k] = v
	}
	t.SetUpdateAuditFields(nil, updateData)

	return db.TxUpsert(dtx.Ctx, dtx.Tx, t.GetFullTableName(), insertData, updateData, where, t.FieldNameForRowId)
}

// API Request Helpers

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

	_, row, err := t.GetByIdNotDeletedAuto(aepr.Context, &aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	_, err = t.DXRawTable.DeleteById(aepr.Context, &aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// RequestSearchPagingList overrides DXRawTable to add NotDeleted filter by default
func (t *DXTable) RequestSearchPagingList(aepr *api.DXAPIEndPointRequest) error {
	qb := t.DXRawTable.NewTableSelectQueryBuilder()

	// DXTable always has is_deleted — apply filter unless explicitly including deleted.
	// This is a safety net: even if is_deleted is missing from FilterableFieldNames,
	// DXTable search_paging will still exclude soft-deleted rows by default.
	isIncludeDeletedExist, isIncludeDeleted, err := aepr.GetParameterValueAsBool("is_include_deleted")
	if err != nil {
		return err
	}
	if !isIncludeDeletedExist || !isIncludeDeleted {
		qb.Eq("is_deleted", false)
	}

	return t.DoRequestSearchPagingList(aepr, qb, nil)
}

// RequestSearchPagingDownload overrides DXRawTable to add NotDeleted filter by default
func (t *DXTable) RequestSearchPagingDownload(aepr *api.DXAPIEndPointRequest) error {
	_, searchText, err := aepr.GetParameterValueAsString("search_text")
	if err != nil {
		return err
	}

	isFilterKeyValuesExist, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}

	_, orderByArray, err := aepr.GetParameterValueAsArrayOfAny("order_by")
	if err != nil {
		return err
	}

	if err := t.EnsureDatabase(); err != nil {
		return err
	}

	qb := t.DXRawTable.NewTableSelectQueryBuilder()
	if searchText != "" {
		qb.SearchLike(searchText, t.SearchTextFieldNames...)
	}
	if isFilterKeyValuesExist && filterKeyValues != nil {
		err := t.processFilterKeyValues(qb, filterKeyValues)
		if err != nil {
			return err
		}
	}

	// DXTable always has is_deleted — apply filter unless explicitly including deleted
	isIncludeDeletedExist, isIncludeDeleted, err := aepr.GetParameterValueAsBool("is_include_deleted")
	if err != nil {
		return err
	}
	if !isIncludeDeletedExist || !isIncludeDeleted {
		qb.Eq("is_deleted", false)
	}

	// Parse order_by into OrderBy calls with validation
	qb.ParseOrderByFromArray(orderByArray)

	return t.DoRequestSearchPagingDownload(aepr, qb)
}

// RequestSoftDeleteByUid handles soft delete by UID API requests
func (t *DXTable) RequestSoftDeleteByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.GetByUidNotDeletedAuto(aepr.Context, &aepr.Log, uid)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%s", uid)
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

	_, row, err := t.GetByUidNotDeletedAuto(aepr.Context, &aepr.Log, uid)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%s", uid)
	}

	id, ok := row[t.FieldNameForRowId].(int64)
	if !ok {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "CANNOT_GET_ID_FROM_ROW")
	}

	_, err = t.DXRawTable.DeleteById(aepr.Context, &aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}
