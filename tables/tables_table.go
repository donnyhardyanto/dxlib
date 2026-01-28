package tables

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/errors"
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

// Insert Operations (with audit fields)

// Insert inserts with audit fields
func (t *DXTable) Insert(l *log.DXLog, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.Insert(l, data, returningFieldNames)
}

// TxInsert inserts within a transaction with audit fields
func (t *DXTable) TxInsert(dtx *database.DXDatabaseTx, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsert(dtx, data, returningFieldNames)
}

// DoInsert is an API helper with audit fields
func (t *DXTable) DoInsert(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	return t.DXRawTable.DoInsert(aepr, data)
}

// Update Operations (with audit fields)

// Update updates with audit fields
func (t *DXTable) Update(l *log.DXLog, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.Update(l, data, where, returningFieldNames)
}

// TxUpdate updates within a transaction with audit fields
func (t *DXTable) TxUpdate(dtx *database.DXDatabaseTx, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdate(dtx, data, where, returningFieldNames)
}

// UpdateById updates with audit fields
func (t *DXTable) UpdateById(l *log.DXLog, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateById(l, id, data)
}

// TxUpdateById updates within a transaction with audit fields
func (t *DXTable) TxUpdateById(dtx *database.DXDatabaseTx, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateById(dtx, id, data)
}

// DoUpdate is an API helper with audit fields
func (t *DXTable) DoUpdate(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	_, row, err := t.ShouldGetByIdNotDeleted(&aepr.Log, id)
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
	_, updatedRow, err := t.ShouldGetByIdNotDeleted(&aepr.Log, id)
	if err != nil {
		return err
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: updatedRow,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
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

// Select Operations (with is_deleted = false filter)

// addNotDeletedFilter adds is_deleted=false to where condition
func (t *DXTable) addNotDeletedFilter(where utils.JSON) utils.JSON {
	if where == nil {
		where = utils.JSON{}
	}
	where["is_deleted"] = false
	return where
}

// Select returns non-deleted rows
func (t *DXTable) Select(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.Select(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOne returns a single non-deleted row
func (t *DXTable) SelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectOne(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// ShouldSelectOne returns a single non-deleted row or error if not found
func (t *DXTable) ShouldSelectOne(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldSelectOne(l, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// GetByIdNotDeleted returns a non-deleted row by ID
func (t *DXTable) GetByIdNotDeleted(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdNotDeleted returns a non-deleted row by ID or error if not found
func (t *DXTable) ShouldGetByIdNotDeleted(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUidNotDeleted returns a non-deleted row by UID
func (t *DXTable) GetByUidNotDeleted(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidNotDeleted returns a non-deleted row by UID or error if not found
func (t *DXTable) ShouldGetByUidNotDeleted(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByNameIdNotDeleted returns a non-deleted row by NameId
func (t *DXTable) GetByNameIdNotDeleted(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdNotDeleted returns a non-deleted row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdNotDeleted(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// Transaction Select Operations (with is_deleted = false filter)

// TxSelect returns non-deleted rows within a transaction
func (t *DXTable) TxSelect(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxSelect(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOne returns a single non-deleted row within a transaction
func (t *DXTable) TxSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOne returns a single non-deleted row or error if not found within a transaction
func (t *DXTable) TxShouldSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxGetByIdNotDeleted returns a non-deleted row by ID within a transaction
func (t *DXTable) TxGetByIdNotDeleted(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdNotDeleted returns a non-deleted row by ID or error if not found within a transaction
func (t *DXTable) TxShouldGetByIdNotDeleted(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// Count Operations (with is_deleted = false filter)

// Count returns total non-deleted row count
func (t *DXTable) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	return t.DXRawTable.Count(l, t.addNotDeletedFilter(where), joinSQLPart)
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

// DXTable - Additional API Helper Methods (with soft-delete)

// SelectAll returns all non-deleted rows from the table
func (t *DXTable) SelectAll(l *log.DXLog) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, nil, nil, nil, nil, nil, nil)
}

// TxShouldGetByNameIdNotDeleted returns a non-deleted row by NameId within a transaction or error if not found
func (t *DXTable) TxShouldGetByNameIdNotDeleted(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameId is an alias for TxShouldGetByNameIdNotDeleted
func (t *DXTable) TxShouldGetByNameId(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldGetByNameIdNotDeleted(dtx, nameId)
}

// TxHardDelete deletes rows within a transaction (bypasses soft-delete)
func (t *DXTable) TxHardDelete(dtx *database.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable.TxDelete(dtx, where, nil)
	return result, err
}

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

// RequestRead handles read by ID API requests (with is_deleted filter)
func (t *DXTable) RequestRead(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, row, err := t.GetByIdNotDeleted(&aepr.Log, id)
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

// RequestReadByUid handles read by UID API requests (with is_deleted filter)
func (t *DXTable) RequestReadByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.GetByUidNotDeleted(&aepr.Log, uid)
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

// RequestEdit handles edit by ID API requests (with is_deleted filter and audit fields)
func (t *DXTable) RequestEdit(aepr *api.DXAPIEndPointRequest) error {
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

// RequestReadByNameId handles read by NameId API requests (with is_deleted filter)
func (t *DXTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) error {
	_, nameId, err := aepr.GetParameterValueAsString(t.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	_, row, err := t.GetByNameIdNotDeleted(&aepr.Log, nameId)
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

// DoEdit is an alias for DoUpdate (backward compatibility)
func (t *DXTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	return t.DoUpdate(aepr, id, data)
}

// DoRequestPagingList handles paging with optional result processing (with is_deleted filter)
func (t *DXTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) error {
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

// DoCreate inserts a row with audit fields and writes API response
func (t *DXTable) DoCreate(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	newId, err := t.DXRawTable.InsertReturningId(&aepr.Log, data)
	if err != nil {
		return 0, err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowId: newId,
	}))
	return newId, nil
}

// RequestCreate handles create API requests (reads parameters and inserts)
func (t *DXTable) RequestCreate(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreate(aepr, data)
	return err
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

// RequestEditByUid handles edit by UID API requests
func (t *DXTable) RequestEditByUid(aepr *api.DXAPIEndPointRequest) error {
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

	_, data, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	return t.DoEdit(aepr, id, data)
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
