package tables

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
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
func (t *DXTable) TxSoftDelete(dtx *databases.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
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
func (t *DXTable) TxSoftDeleteById(dtx *databases.DXDatabaseTx, id int64) (sql.Result, error) {
	return t.TxSoftDelete(dtx, utils.JSON{t.FieldNameForRowId: id})
}

// DoSoftDelete is an API helper for soft delete
func (t *DXTable) DoSoftDelete(aepr *api.DXAPIEndPointRequest, id int64) error {
	_, row, err := t.ShouldGetByIdNotDeletedAuto(&aepr.Log, id)
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
func (t *DXTable) TxHardDelete(dtx *databases.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable.TxDelete(dtx, where, nil)
	return result, err
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
		_, returningValues, err := t.Database.Insert(t.GetFullTableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.Database.Update(t.GetFullTableName(), data, where, nil)
	return result, 0, err
}

// TxUpsert inserts or updates within a transaction with audit fields
func (t *DXTable) TxUpsert(dtx *databases.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	_, existing, err := t.DXRawTable.TxSelectOne(dtx, nil, where, nil, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	if existing == nil {
		t.SetInsertAuditFields(nil, data)
		insertData := utilsJson.DeepMerge2(data, where)
		_, returningValues, err := dtx.Insert(t.GetFullTableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	t.SetUpdateAuditFields(nil, data)
	result, _, err := dtx.Update(t.GetFullTableName(), data, where, nil)
	return result, 0, err
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

	_, row, err := t.ShouldGetByIdNotDeletedAuto(&aepr.Log, id)
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

// RequestSearchPagingList overrides DXRawTable to add NotDeleted filter by default
func (t *DXTable) RequestSearchPagingList(aepr *api.DXAPIEndPointRequest) error {
	qb := t.DXRawTable.NewTableSelectQueryBuilder()
	qb.NotDeleted()
	return t.DoRequestSearchPagingList(aepr, qb, nil)
}

// RequestSoftDeleteByUid handles soft delete by UID API requests
func (t *DXTable) RequestSoftDeleteByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.ShouldGetByUidNotDeletedAuto(&aepr.Log, uid)
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

	_, row, err := t.ShouldGetByUidNotDeletedAuto(&aepr.Log, uid)
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
