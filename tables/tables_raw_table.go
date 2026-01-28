package tables

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/database/export"
	"github.com/donnyhardyanto/dxlib/database/models"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// DXRawTable - Basic table wrapper without soft-delete

// DXRawTable wraps database3 with connection management and basic CRUD
type DXRawTable struct {
	DatabaseNameId             string
	Database                   *database.DXDatabase
	DBTable                    *models.ModelDBTable
	TableNameDirect            string // Used when DBTable is nil
	FieldNameForRowId          string
	FieldNameForRowUid         string
	FieldNameForRowUtag        string
	FieldNameForRowNameId      string
	ResultObjectName           string
	ListViewNameId             string // View name for list/search queries
	ResponseEnvelopeObjectName string
	FieldTypeMapping           db.DXDatabaseTableFieldTypeMapping
	FieldMaxLengths            map[string]int // Maximum lengths for fields (for truncation)

	// Encryption definitions for automatic encryption/decryption
	EncryptionKeyDefs    []*database.EncryptionKeyDef   // session keys only (for views that already handle decryption)
	EncryptionColumnDefs []database.EncryptionColumnDef // for INSERT/UPDATE/SELECT encryption/decryption
}

// EnsureDatabase ensures database connection is initialized
func (t *DXRawTable) EnsureDatabase() error {
	if t.Database == nil {
		t.Database = database.Manager.GetOrCreate(t.DatabaseNameId)
		if t.Database == nil {
			return errors.Errorf("database not found: %s", t.DatabaseNameId)
		}
	}
	return t.Database.EnsureConnection()
}

// GetDbType returns the database type
func (t *DXRawTable) GetDbType() base.DXDatabaseType {
	if t.Database == nil {
		return base.DXDatabaseTypePostgreSQL
	}
	return t.Database.DatabaseType
}

// TableName returns the full table name from DBTable or TableNameDirect
func (t *DXRawTable) TableName() string {
	if t.DBTable != nil {
		return t.DBTable.FullTableName()
	}
	return t.TableNameDirect
}

// Insert Operations

// Insert inserts a new row and returns result
func (t *DXRawTable) Insert(l *log.DXLog, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Insert(t.TableName(), data, returningFieldNames)
}

// TxInsert inserts within a transaction
func (t *DXRawTable) TxInsert(dtx *database.DXDatabaseTx, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	return dtx.Insert(t.TableName(), data, returningFieldNames)
}

// InsertReturningId is a simplified insert that returns just the new ID (backward compatible)
func (t *DXRawTable) InsertReturningId(l *log.DXLog, data utils.JSON) (int64, error) {
	_, returningValues, err := t.Insert(l, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// TxInsertReturningId is a simplified TxInsert that returns just the new ID (backward compatible)
func (t *DXRawTable) TxInsertReturningId(dtx *database.DXDatabaseTx, data utils.JSON) (int64, error) {
	_, returningValues, err := t.TxInsert(dtx, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// DoInsert is an API helper that inserts and writes response
func (t *DXRawTable) DoInsert(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}

	returningFields := []string{t.FieldNameForRowId}
	if t.FieldNameForRowUid != "" {
		returningFields = append(returningFields, t.FieldNameForRowUid)
	}

	_, returningValues, err := t.Database.Insert(t.TableName(), data, returningFields)
	if err != nil {
		return 0, err
	}

	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)

	response := utils.JSON{
		t.FieldNameForRowId: newId,
	}

	if t.FieldNameForRowUid != "" {
		if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
			response[t.FieldNameForRowUid] = uid
		}
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, response)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)

	return newId, nil
}

// Update Operations

// Update updates rows matching where condition
func (t *DXRawTable) Update(l *log.DXLog, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Update(t.TableName(), data, where, returningFieldNames)
}

// TxUpdate updates within a transaction
func (t *DXRawTable) TxUpdate(dtx *database.DXDatabaseTx, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return dtx.Update(t.TableName(), data, where, returningFieldNames)
}

// UpdateSimple is a simplified update that just takes data and where (backward compatible)
func (t *DXRawTable) UpdateSimple(data utils.JSON, where utils.JSON) (sql.Result, error) {
	result, _, err := t.Update(nil, data, where, nil)
	return result, err
}

// TxUpdateSimple is a simplified transaction update (backward compatible)
func (t *DXRawTable) TxUpdateSimple(dtx *database.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, error) {
	result, _, err := t.TxUpdate(dtx, data, where, nil)
	return result, err
}

// UpdateById updates a single row by ID
func (t *DXRawTable) UpdateById(l *log.DXLog, id int64, data utils.JSON) (sql.Result, error) {
	result, _, err := t.Update(l, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// TxUpdateById updates a single row by ID within a transaction
func (t *DXRawTable) TxUpdateById(dtx *database.DXDatabaseTx, id int64, data utils.JSON) (sql.Result, error) {
	result, _, err := t.TxUpdate(dtx, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DoUpdate is an API helper that updates and writes response
func (t *DXRawTable) DoUpdate(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	_, row, err := t.ShouldGetById(&aepr.Log, id)
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

	_, err = t.UpdateById(&aepr.Log, id, data)
	if err != nil {
		return err
	}

	// Re-fetch and return updated row
	_, updatedRow, err := t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: updatedRow,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// Delete Operations (Hard Delete)

// Delete performs hard delete of rows matching where condition
func (t *DXRawTable) Delete(l *log.DXLog, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Delete(t.TableName(), where, returningFieldNames)
}

// TxDelete deletes within a transaction
func (t *DXRawTable) TxDelete(dtx *database.DXDatabaseTx, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return dtx.TxDelete(t.TableName(), where, returningFieldNames)
}

// DeleteById deletes a single row by ID
func (t *DXRawTable) DeleteById(l *log.DXLog, id int64) (sql.Result, error) {
	result, _, err := t.Delete(l, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// TxDeleteById deletes a single row by ID within a transaction
func (t *DXRawTable) TxDeleteById(dtx *database.DXDatabaseTx, id int64) (sql.Result, error) {
	result, _, err := t.TxDelete(dtx, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DoDelete is an API helper that deletes and writes response
func (t *DXRawTable) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) error {
	_, row, err := t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	_, err = t.DeleteById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// Select Operations

// Select returns multiple rows matching where condition
func (t *DXRawTable) Select(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Select(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// SelectOne returns a single row matching where condition
func (t *DXRawTable) SelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.SelectOne(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// ShouldSelectOne returns a single row or error if not found
func (t *DXRawTable) ShouldSelectOne(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.ShouldSelectOne(t.GetListViewName(), t.FieldTypeMapping, nil, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// GetById returns a row by ID
func (t *DXRawTable) GetById(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetById returns a row by ID or error if not found
func (t *DXRawTable) ShouldGetById(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUid returns a row by UID
func (t *DXRawTable) GetByUid(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUid returns a row by UID or error if not found
func (t *DXRawTable) ShouldGetByUid(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByUtag returns a row by Utag
func (t *DXRawTable) GetByUtag(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtag returns a row by Utag or error if not found
func (t *DXRawTable) ShouldGetByUtag(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// GetByNameId returns a row by NameId
func (t *DXRawTable) GetByNameId(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameId returns a row by NameId or error if not found
func (t *DXRawTable) ShouldGetByNameId(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// Transaction Select Operations

// TxSelect returns multiple rows within a transaction
func (t *DXRawTable) TxSelect(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return dtx.Select(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// TxSelectOne returns a single row within a transaction
func (t *DXRawTable) TxSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	// Use table name instead of view when FOR UPDATE is requested (views with outer joins don't support FOR UPDATE)
	tableName := t.GetListViewName()
	if forUpdatePart != nil && forUpdatePart != false && forUpdatePart != "" {
		tableName = t.TableName()
	}
	return dtx.SelectOne(tableName, t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxShouldSelectOne returns a single row or error if not found within a transaction
func (t *DXRawTable) TxShouldSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	// Use table name instead of view when FOR UPDATE is requested (views with outer joins don't support FOR UPDATE)
	tableName := t.GetListViewName()
	if forUpdatePart != nil && forUpdatePart != false && forUpdatePart != "" {
		tableName = t.TableName()
	}
	return dtx.ShouldSelectOne(tableName, t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxGetById returns a row by ID within a transaction
func (t *DXRawTable) TxGetById(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetById returns a row by ID or error if not found within a transaction
func (t *DXRawTable) TxShouldGetById(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByUid returns a row by UID within a transaction
func (t *DXRawTable) TxGetByUid(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUid returns a row by UID or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByUid(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUtag returns a row by Utag within a transaction
func (t *DXRawTable) TxGetByUtag(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxShouldGetByUtag returns a row by Utag or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByUtag(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxGetByNameId returns a row by NameId within a transaction
func (t *DXRawTable) TxGetByNameId(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// Count Operations

// Count returns total row count
func (t *DXRawTable) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}
	return t.Database.Count(t.GetListViewName(), where, joinSQLPart)
}

// Upsert Operations

// Upsert inserts or updates a row based on where condition
func (t *DXRawTable) Upsert(l *log.DXLog, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, 0, err
	}

	_, existing, err := t.SelectOne(l, nil, where, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	if existing == nil {
		insertData := utilsJson.DeepMerge2(data, where)
		_, returningValues, err := t.Database.Insert(t.TableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	result, _, err := t.Database.Update(t.TableName(), data, where, nil)
	return result, 0, err
}

// TxUpsert inserts or updates within a transaction
func (t *DXRawTable) TxUpsert(dtx *database.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	_, existing, err := t.TxSelectOne(dtx, nil, where, nil, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	if existing == nil {
		insertData := utilsJson.DeepMerge2(data, where)
		_, returningValues, err := dtx.Insert(t.TableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	result, _, err := dtx.Update(t.TableName(), data, where, nil)
	return result, 0, err
}

// Paging Operations

// GetListViewName returns the view name for list queries (falls back to table name)
func (t *DXRawTable) GetListViewName() string {
	if t.ListViewNameId != "" {
		return t.ListViewNameId
	}
	return t.TableName()
}

// NewQueryBuilder creates a QueryBuilder with the table's database type
func (t *DXRawTable) NewQueryBuilder() *QueryBuilder {
	return NewQueryBuilder(t.GetDbType())
}

// Paging executes a paging query with WHERE clause and ORDER BY
func (t *DXRawTable) Paging(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	rowsInfo, list, totalRows, totalPages, _, err := db.NamedQueryPaging(
		t.Database.Connection,
		t.FieldTypeMapping,
		"",
		rowPerPage,
		pageIndex,
		"*",
		t.GetListViewName(),
		whereClause,
		"",
		orderBy,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &PagingResult{
		RowsInfo:   rowsInfo,
		Rows:       list,
		TotalRows:  totalRows,
		TotalPages: totalPages,
	}, nil
}

// PagingWithBuilder executes a paging query using a QueryBuilder
func (t *DXRawTable) PagingWithBuilder(l *log.DXLog, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPaging is an API helper that handles paging
func (t *DXRawTable) DoPaging(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	result, err := t.Paging(&aepr.Log, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		aepr.Log.Errorf(err, "Error at paging table %s (%s)", t.TableName(), err.Error())
		return nil, err
	}
	return result, nil
}

// DoPagingWithBuilder is an API helper using QueryBuilder
func (t *DXRawTable) DoPagingWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPagingResponse executes paging and writes standard JSON response
func (t *DXRawTable) DoPagingResponse(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) error {
	result, err := t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// DoPagingResponseWithBuilder executes paging with QueryBuilder and writes response
func (t *DXRawTable) DoPagingResponseWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) error {
	whereClause, args := qb.Build()
	return t.DoPagingResponse(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DXRawTable - Additional API Helper Methods

// SelectAll returns all rows from the table
func (t *DXRawTable) SelectAll(l *log.DXLog) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, nil, nil, nil, nil, nil, nil)
}

// TxShouldGetByNameId returns a row by NameId within a transaction or error if not found
func (t *DXRawTable) TxShouldGetByNameId(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxHardDelete deletes rows within a transaction (hard delete)
func (t *DXRawTable) TxHardDelete(dtx *database.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.TxDelete(dtx, where, nil)
	return result, err
}

// RequestPagingList handles list/paging API requests
func (t *DXRawTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) error {
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

// RequestRead handles read by ID API requests
func (t *DXRawTable) RequestRead(aepr *api.DXAPIEndPointRequest) error {
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

// RequestReadByUid handles read by UID API requests
func (t *DXRawTable) RequestReadByUid(aepr *api.DXAPIEndPointRequest) error {
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

// RequestReadByUtag handles read by Utag API requests
func (t *DXRawTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) error {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, row, err := t.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
		"rows_info":        rowsInfo,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestEdit handles edit by ID API requests
func (t *DXRawTable) RequestEdit(aepr *api.DXAPIEndPointRequest) error {
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

// RequestHardDelete handles hard delete by ID API requests
func (t *DXRawTable) RequestHardDelete(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	return t.DoDelete(aepr, id)
}

// DoCreate inserts a row and writes API response (suppresses errors)
func (t *DXRawTable) DoCreate(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	returningFields := []string{t.FieldNameForRowId}
	if t.FieldNameForRowUid != "" {
		returningFields = append(returningFields, t.FieldNameForRowUid)
	}

	_, returningValues, err := t.Insert(&aepr.Log, data, returningFields)
	if err != nil {
		aepr.WriteResponseAsError(http.StatusConflict, err)
		return 0, nil
	}

	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)

	response := utils.JSON{
		t.FieldNameForRowId: newId,
	}

	if t.FieldNameForRowUid != "" {
		if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
			response[t.FieldNameForRowUid] = uid
		}
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, response))
	return newId, nil
}

// RequestReadByNameId handles read by NameId API requests
func (t *DXRawTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) error {
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

// RequestListDownload handles list download API requests (export to xlsx/csv/xls)
func (t *DXRawTable) RequestListDownload(aepr *api.DXAPIEndPointRequest) error {
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
		rowPerPage = 0 // No limit if not specified
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		pageIndex = 0
	}

	_, format, err := aepr.GetParameterValueAsString("format")
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "FORMAT_PARAMETER_ERROR:%s", err.Error())
	}
	format = strings.ToLower(format)

	// Validate format
	switch format {
	case "xls", "xlsx", "csv":
	default:
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "UNSUPPORTED_EXPORT_FORMAT:%s", format)
	}

	if err := t.EnsureDatabase(); err != nil {
		return err
	}

	rowsInfo, list, _, _, _, err := db.NamedQueryPaging(
		t.Database.Connection,
		db.DXDatabaseTableFieldTypeMapping(t.FieldTypeMapping),
		"",
		rowPerPage,
		pageIndex,
		"*",
		t.GetListViewName(),
		filterWhere,
		"",
		filterOrderBy,
		filterKeyValues,
	)
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

	// Override contentType based on format
	switch format {
	case "xls":
		contentType = "application/vnd.ms-excel"
	case "xlsx":
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "csv":
		contentType = "application/octet-stream"
	}

	// Set response headers
	filename := fmt.Sprintf("export_%s_%s.%s", t.TableName(), time.Now().Format("20060102_150405"), format)

	rw := *aepr.GetResponseWriter()
	rw.Header().Set("Content-Type", contentType)
	rw.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	rw.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	rw.Header().Set("X-Content-Type-Options", "nosniff")

	rw.WriteHeader(http.StatusOK)
	aepr.ResponseStatusCode = http.StatusOK

	if _, err = rw.Write(data); err != nil {
		return err
	}

	aepr.ResponseHeaderSent = true
	aepr.ResponseBodySent = true

	return nil
}

// RequestCreate handles create API requests (alias for backward compatibility)
func (t *DXRawTable) RequestCreate(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreate(aepr, data)
	return err
}

// DoEdit is an alias for DoUpdate (backward compatibility)
func (t *DXRawTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	return t.DoUpdate(aepr, id, data)
}

// OnResultList is a callback type for paging result processing
type OnResultList func(aepr *api.DXAPIEndPointRequest, list []utils.JSON) ([]utils.JSON, error)

// DoRequestPagingList handles paging with optional result processing
func (t *DXRawTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) error {
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
