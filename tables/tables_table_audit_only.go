package tables

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// DXTableAuditOnly - Table wrapper with audit fields ONLY (NO soft-delete)
// Use this for junction/association tables that need audit tracking but do NOT have is_deleted column

// DXTableAuditOnly extends DXRawTable with audit fields (created_at, created_by_*, last_modified_*)
// Does NOT add is_deleted filtering
type DXTableAuditOnly struct {
	DXRawTable
}

// Audit Field Helpers

// SetInsertAuditFields sets created_at, created_by_user_id, etc. for insert
// NOTE: Does NOT set is_deleted (that's only in DXTable)
func (t *DXTableAuditOnly) SetInsertAuditFields(aepr *api.DXAPIEndPointRequest, data utils.JSON) {
	now := time.Now().UTC()

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
func (t *DXTableAuditOnly) SetUpdateAuditFields(aepr *api.DXAPIEndPointRequest, data utils.JSON) {
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

// Insert Operations (with audit fields)

// Insert inserts with audit fields
func (t *DXTableAuditOnly) Insert(l *log.DXLog, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.Insert(l, data, returningFieldNames)
}

// TxInsert inserts within a transaction with audit fields
func (t *DXTableAuditOnly) TxInsert(dtx *databases.DXDatabaseTx, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsert(dtx, data, returningFieldNames)
}

// DoInsert is an API helper with audit fields
func (t *DXTableAuditOnly) DoInsert(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	return t.DXRawTable.DoInsert(aepr, data)
}

// DoCreate inserts a row with audit fields and writes API response
func (t *DXTableAuditOnly) DoCreate(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	_, returningValues, err := t.DXRawTable.Insert(&aepr.Log, data, []string{t.FieldNameForRowId, t.FieldNameForRowUid})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	newUid := ""
	if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
		newUid = uid
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowUid: newUid,
	}))
	return newId, nil
}

// DoCreateReturnId inserts a row with audit fields and writes API response with id
func (t *DXTableAuditOnly) DoCreateReturnId(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	_, returningValues, err := t.DXRawTable.Insert(&aepr.Log, data, []string{t.FieldNameForRowId, t.FieldNameForRowUid})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowId: newId,
	}))
	return newId, nil
}

// RequestCreate handles create API requests (reads parameters and inserts)
func (t *DXTableAuditOnly) RequestCreate(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreate(aepr, data)
	return err
}

// RequestCreateReturnId handles create API requests (reads parameters and inserts, returns id)
func (t *DXTableAuditOnly) RequestCreateReturnId(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreateReturnId(aepr, data)
	return err
}

// DoCreateReturnUid inserts a row with audit fields and writes API response with uid (not id)
func (t *DXTableAuditOnly) DoCreateReturnUid(aepr *api.DXAPIEndPointRequest, data utils.JSON) (string, error) {
	t.SetInsertAuditFields(aepr, data)
	_, returningValues, err := t.DXRawTable.Insert(&aepr.Log, data, []string{t.FieldNameForRowUid})
	if err != nil {
		return "", err
	}
	newUid := ""
	if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
		newUid = uid
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowUid: newUid,
	}))
	return newUid, nil
}

// RequestCreateReturnUid handles create API requests and returns uid in response
func (t *DXTableAuditOnly) RequestCreateReturnUid(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreateReturnUid(aepr, data)
	return err
}

// Update Operations (with audit fields)

// Update updates with audit fields
func (t *DXTableAuditOnly) Update(l *log.DXLog, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.Update(l, data, where, returningFieldNames)
}

// TxUpdate updates within a transaction with audit fields
func (t *DXTableAuditOnly) TxUpdate(dtx *databases.DXDatabaseTx, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdate(dtx, data, where, returningFieldNames)
}

// UpdateById updates with audit fields
func (t *DXTableAuditOnly) UpdateById(l *log.DXLog, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateById(l, id, data)
}

// TxUpdateById updates within a transaction with audit fields
func (t *DXTableAuditOnly) TxUpdateById(dtx *databases.DXDatabaseTx, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateById(dtx, id, data)
}

// DoUpdate is an API helper with audit fields
func (t *DXTableAuditOnly) DoUpdate(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	_, row, err := t.DXRawTable.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	for k, v := range data {
		if v == nil {
			delete(data, k)
		}
	}

	t.SetUpdateAuditFields(aepr, data)

	_, err = t.DXRawTable.UpdateById(&aepr.Log, id, data)
	if err != nil {
		return err
	}

	// Re-fetch and return updated row
	_, updatedRow, err := t.DXRawTable.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: updatedRow,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// DoEdit is an alias for DoUpdate (backward compatibility)
func (t *DXTableAuditOnly) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	return t.DoUpdate(aepr, id, data)
}

// RequestEdit handles edit by ID API requests (with audit fields)
func (t *DXTableAuditOnly) RequestEdit(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, newKeyValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	// Remove nil values
	for k, v := range newKeyValues {
		if v == nil {
			delete(newKeyValues, k)
		}
	}

	return t.DoUpdate(aepr, id, newKeyValues)
}

// RequestEditByUid handles edit by UID API requests
func (t *DXTableAuditOnly) RequestEditByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.DXRawTable.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	id, ok := row[t.FieldNameForRowId].(int64)
	if !ok {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "CANNOT_GET_ID_FROM_ROW")
	}

	_, data, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	return t.DoEdit(aepr, id, data)
}

// Upsert Operations (with audit fields)

// Upsert inserts or updates with audit fields
func (t *DXTableAuditOnly) Upsert(l *log.DXLog, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
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
func (t *DXTableAuditOnly) TxUpsert(dtx *databases.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
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

// Delete Operations (hard delete only - no soft delete)

// TxHardDelete deletes rows within a transaction (hard delete)
func (t *DXTableAuditOnly) TxHardDelete(dtx *databases.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable.TxDelete(dtx, where, nil)
	return result, err
}

// RequestHardDelete handles hard delete by ID API requests
func (t *DXTableAuditOnly) RequestHardDelete(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, row, err := t.DXRawTable.ShouldGetById(&aepr.Log, id)
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

// RequestHardDeleteByUid handles hard delete by UID API requests
func (t *DXTableAuditOnly) RequestHardDeleteByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.DXRawTable.ShouldGetByUid(&aepr.Log, uid)
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

// RequestListAll handles list all API requests (no paging, all records)
func (t *DXTableAuditOnly) RequestListAll(aepr *api.DXAPIEndPointRequest) error {
	return t.RequestSearchPagingList(aepr)
}

// Select helpers - pass through to DXRawTable (NO is_deleted filter)

// Select returns rows (NO is_deleted filter)
func (t *DXTableAuditOnly) Select(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.Select(l, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOne returns a single row (NO is_deleted filter)
func (t *DXTableAuditOnly) SelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectOne(l, fieldNames, where, joinSQLPart, orderBy)
}

// ShouldSelectOne returns a single row or error if not found (NO is_deleted filter)
func (t *DXTableAuditOnly) ShouldSelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldSelectOne(l, fieldNames, where, joinSQLPart, orderBy)
}

// GetById returns a row by ID (NO is_deleted filter)
func (t *DXTableAuditOnly) GetById(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetById(l, id, fieldNames...)
}

// ShouldGetById returns a row by ID or error if not found (NO is_deleted filter)
func (t *DXTableAuditOnly) ShouldGetById(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetById(l, id, fieldNames...)
}

// GetByUid returns a row by UID (NO is_deleted filter)
func (t *DXTableAuditOnly) GetByUid(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.DXRawTable.GetByUid(l, uid, fieldNames...)
}

// ShouldGetByUid returns a row by UID or error if not found (NO is_deleted filter)
func (t *DXTableAuditOnly) ShouldGetByUid(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.DXRawTable.ShouldGetByUid(l, uid, fieldNames...)
}

// GetByNameId returns a row by NameId (NO is_deleted filter)
func (t *DXTableAuditOnly) GetByNameId(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.DXRawTable.GetByNameId(l, nameId, fieldNames...)
}

// ShouldGetByNameId returns a row by NameId or error if not found (NO is_deleted filter)
func (t *DXTableAuditOnly) ShouldGetByNameId(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.DXRawTable.ShouldGetByNameId(l, nameId, fieldNames...)
}

// Transaction Select Operations (NO is_deleted filter)

// TxSelect returns rows within a transaction (NO is_deleted filter)
func (t *DXTableAuditOnly) TxSelect(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxSelect(dtx, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOne returns a single row within a transaction (NO is_deleted filter)
func (t *DXTableAuditOnly) TxSelectOne(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectOne(dtx, fieldNames, where, joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOne returns a single row or error if not found within a transaction (NO is_deleted filter)
func (t *DXTableAuditOnly) TxShouldSelectOne(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOne(dtx, fieldNames, where, joinSQLPart, orderBy, forUpdatePart)
}

// TxGetById returns a row by ID within a transaction (NO is_deleted filter)
func (t *DXTableAuditOnly) TxGetById(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetById returns a row by ID or error if not found within a transaction (NO is_deleted filter)
func (t *DXTableAuditOnly) TxShouldGetById(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// Count Operations (NO is_deleted filter)

// Count returns total row count (NO is_deleted filter)
func (t *DXTableAuditOnly) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	return t.DXRawTable.Count(l, where, joinSQLPart)
}

// SelectAll returns all rows from the table (NO is_deleted filter)
func (t *DXTableAuditOnly) SelectAll(l *log.DXLog) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, nil, nil, nil, nil, nil, nil)
}

// API Select Helpers

// RequestRead handles read by ID API requests (NO is_deleted filter)
func (t *DXTableAuditOnly) RequestRead(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, row, err := t.GetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByUid handles read by UID API requests (NO is_deleted filter)
func (t *DXTableAuditOnly) RequestReadByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.GetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%s", uid)
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByNameId handles read by NameId API requests (NO is_deleted filter)
func (t *DXTableAuditOnly) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) error {
	_, nameId, err := aepr.GetParameterValueAsString(t.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	_, row, err := t.GetByNameId(&aepr.Log, nameId)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%s", nameId)
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}
