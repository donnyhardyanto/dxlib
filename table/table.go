package table

import (
	"database/sql"
	"encoding/json"
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
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// ============================================================================
// QueryBuilder - Fluent API for building WHERE clauses
// ============================================================================

// QueryBuilder builds SQL WHERE clauses with fluent API
type QueryBuilder struct {
	conditions []string
	args       utils.JSON
	dbType     base.DXDatabaseType
}

// NewQueryBuilder creates a new QueryBuilder
func NewQueryBuilder(dbType base.DXDatabaseType) *QueryBuilder {
	return &QueryBuilder{
		conditions: []string{},
		args:       utils.JSON{},
		dbType:     dbType,
	}
}

// And adds a raw condition with AND
func (qb *QueryBuilder) And(condition string) *QueryBuilder {
	if condition != "" {
		qb.conditions = append(qb.conditions, condition)
	}
	return qb
}

// Eq adds field = value condition
func (qb *QueryBuilder) Eq(field string, value any) *QueryBuilder {
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s = :%s", field, field))
	qb.args[field] = value
	return qb
}

// Ne adds field != value condition
func (qb *QueryBuilder) Ne(field string, value any) *QueryBuilder {
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s != :%s", field, field))
	qb.args[field] = value
	return qb
}

// Like adds field LIKE value condition (case-sensitive)
func (qb *QueryBuilder) Like(field string, value string) *QueryBuilder {
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s LIKE :%s", field, field))
	qb.args[field] = "%" + value + "%"
	return qb
}

// ILike adds field ILIKE value condition (case-insensitive, PostgreSQL)
func (qb *QueryBuilder) ILike(field string, value string) *QueryBuilder {
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s ILIKE :%s", field, field))
	qb.args[field] = "%" + value + "%"
	return qb
}

// SearchLike adds OR condition for multiple fields with ILIKE
func (qb *QueryBuilder) SearchLike(value string, fields ...string) *QueryBuilder {
	if value == "" || len(fields) == 0 {
		return qb
	}
	var parts []string
	for i, field := range fields {
		argName := fmt.Sprintf("search_%d", i)
		parts = append(parts, fmt.Sprintf("%s ILIKE :%s", field, argName))
		qb.args[argName] = "%" + value + "%"
	}
	qb.conditions = append(qb.conditions, "("+strings.Join(parts, " OR ")+")")
	return qb
}

// In adds field IN (values) condition
func (qb *QueryBuilder) In(field string, values any) *QueryBuilder {
	qb.conditions = append(qb.conditions, qb.buildInClause(field, values))
	return qb
}

// InInt64 adds field IN (values) for int64 slice
func (qb *QueryBuilder) InInt64(field string, values []int64) *QueryBuilder {
	if len(values) == 0 {
		return qb
	}
	var strVals []string
	for _, v := range values {
		strVals = append(strVals, fmt.Sprintf("%d", v))
	}
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s IN (%s)", field, strings.Join(strVals, ", ")))
	return qb
}

// InStrings adds field IN (values) for string slice
func (qb *QueryBuilder) InStrings(field string, values []string) *QueryBuilder {
	if len(values) == 0 {
		return qb
	}
	var quotedVals []string
	for _, v := range values {
		quotedVals = append(quotedVals, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
	}
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s IN (%s)", field, strings.Join(quotedVals, ", ")))
	return qb
}

// NotDeleted adds is_deleted = false condition (database-aware)
func (qb *QueryBuilder) NotDeleted() *QueryBuilder {
	switch qb.dbType {
	case base.DXDatabaseTypeSQLServer:
		qb.conditions = append(qb.conditions, "is_deleted = 0")
	default:
		qb.conditions = append(qb.conditions, "is_deleted = false")
	}
	return qb
}

// OrAnyLocationCode adds OR condition for multiple location code fields
func (qb *QueryBuilder) OrAnyLocationCode(locationCode string, fields ...string) *QueryBuilder {
	if locationCode == "" || len(fields) == 0 {
		return qb
	}
	var parts []string
	for _, field := range fields {
		parts = append(parts, fmt.Sprintf("%s = '%s'", field, strings.ReplaceAll(locationCode, "'", "''")))
	}
	qb.conditions = append(qb.conditions, "("+strings.Join(parts, " OR ")+")")
	return qb
}

// Build returns the WHERE clause string and args
func (qb *QueryBuilder) Build() (string, utils.JSON) {
	if len(qb.conditions) == 0 {
		return "", qb.args
	}
	return strings.Join(qb.conditions, " AND "), qb.args
}

// BuildWithPrefix returns WHERE clause with prefix for combining with existing conditions
func (qb *QueryBuilder) BuildWithPrefix(existingWhere string) (string, utils.JSON) {
	where, args := qb.Build()
	if existingWhere != "" && where != "" {
		return fmt.Sprintf("(%s) AND %s", existingWhere, where), args
	}
	if existingWhere != "" {
		return existingWhere, args
	}
	return where, args
}

func (qb *QueryBuilder) buildInClause(field string, values any) string {
	switch v := values.(type) {
	case []int64:
		if len(v) == 0 {
			return "1=0" // Always false
		}
		var strVals []string
		for _, val := range v {
			strVals = append(strVals, fmt.Sprintf("%d", val))
		}
		return fmt.Sprintf("%s IN (%s)", field, strings.Join(strVals, ", "))
	case []string:
		if len(v) == 0 {
			return "1=0"
		}
		var quotedVals []string
		for _, val := range v {
			quotedVals = append(quotedVals, fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")))
		}
		return fmt.Sprintf("%s IN (%s)", field, strings.Join(quotedVals, ", "))
	default:
		return fmt.Sprintf("%s IN (%v)", field, values)
	}
}

// ============================================================================
// SQL Utility Functions
// ============================================================================

// SQLBuildWhereInClauseStrings builds a WHERE IN clause for string values
func SQLBuildWhereInClauseStrings(fieldName string, values []string) string {
	l := len(values)
	if l == 0 {
		return ""
	}
	quotedValues := make([]string, l)
	for i, v := range values {
		quotedValues[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	}
	if l == 1 {
		return fieldName + " = " + quotedValues[0]
	}
	return "(" + fieldName + " IN (" + strings.Join(quotedValues, ",") + "))"
}

// SQLBuildWhereInClauseInt64 builds a WHERE IN clause for int64 values
func SQLBuildWhereInClauseInt64(fieldName string, values []int64) string {
	l := len(values)
	if l == 0 {
		return ""
	}
	valueStrings := make([]string, l)
	for i, v := range values {
		valueStrings[i] = fmt.Sprintf("%d", v)
	}
	if l == 1 {
		return fieldName + " = " + valueStrings[0]
	}
	return fieldName + " IN (" + strings.Join(valueStrings, ",") + ")"
}

// ============================================================================
// PagingResult - Standardized paging response
// ============================================================================

// PagingResult contains paging query results
type PagingResult struct {
	RowsInfo   *db.DXDatabaseTableRowsInfo
	Rows       []utils.JSON
	TotalRows  int64
	TotalPages int64
}

// ToResponseJSON converts PagingResult to standard JSON response format
func (pr *PagingResult) ToResponseJSON() utils.JSON {
	return utils.JSON{
		"data": utils.JSON{
			"list": utils.JSON{
				"rows":       pr.Rows,
				"total_rows": pr.TotalRows,
				"total_page": pr.TotalPages,
				"rows_info":  pr.RowsInfo,
			},
		},
	}
}

// ============================================================================
// Standalone Paging Functions - using database2.DXDatabase
// ============================================================================

// NamedQueryPaging executes a paging query using database.DXDatabase
func NamedQueryPaging(
	dxDb3 *database.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	whereClause, orderBy string,
	args utils.JSON,
) (*PagingResult, error) {
	if dxDb3 == nil {
		return nil, errors.New("database3 connection is nil")
	}

	if err := dxDb3.EnsureConnection(); err != nil {
		return nil, err
	}

	rowsInfo, list, totalRows, totalPages, _, err := db.NamedQueryPaging(
		dxDb3.Connection,
		fieldTypeMapping,
		"",
		rowPerPage,
		pageIndex,
		"*",
		tableName,
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

// NamedQueryPagingWithBuilder executes a paging query using QueryBuilder and database3
func NamedQueryPagingWithBuilder(
	dxDb3 *database.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	qb *QueryBuilder,
	orderBy string,
) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return NamedQueryPaging(dxDb3, fieldTypeMapping, tableName, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoNamedQueryPagingResponse executes paging and writes standard JSON response
func DoNamedQueryPagingResponse(
	aepr *api.DXAPIEndPointRequest,
	dxDb3 *database.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	whereClause, orderBy string,
	args utils.JSON,
) error {
	result, err := NamedQueryPaging(dxDb3, fieldTypeMapping, tableName, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		aepr.Log.Errorf(err, "Error at paging table %s (%s)", tableName, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// DoNamedQueryPagingResponseWithBuilder executes paging with QueryBuilder and writes response
func DoNamedQueryPagingResponseWithBuilder(
	aepr *api.DXAPIEndPointRequest,
	dxDb3 *database.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	qb *QueryBuilder,
	orderBy string,
) error {
	whereClause, args := qb.Build()
	return DoNamedQueryPagingResponse(aepr, dxDb3, fieldTypeMapping, tableName, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// ============================================================================
// DXRawTable - Basic table wrapper without soft-delete
// ============================================================================

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
	EncryptedColumnDefs []database.EncryptedColumnDef // for INSERT/UPDATE
	DecryptedColumnDefs []database.DecryptedColumnDef // for SELECT
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

// ============================================================================
// Insert Operations
// ============================================================================

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

// ============================================================================
// Update Operations
// ============================================================================

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

// ============================================================================
// Delete Operations (Hard Delete)
// ============================================================================

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

// ============================================================================
// Select Operations
// ============================================================================

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

// ============================================================================
// Transaction Select Operations
// ============================================================================

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

// ============================================================================
// Count Operations
// ============================================================================

// Count returns total row count
func (t *DXRawTable) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}
	return t.Database.Count(t.GetListViewName(), where, joinSQLPart)
}

// ============================================================================
// Upsert Operations
// ============================================================================

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

// ============================================================================
// Paging Operations
// ============================================================================

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

// ============================================================================
// DXTable - Table wrapper with soft-delete and audit fields
// ============================================================================

// DXTable extends DXRawTable with soft-delete and audit fields
type DXTable struct {
	DXRawTable
}

// ============================================================================
// Audit ModelDBField Helpers
// ============================================================================

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

// ============================================================================
// Insert Operations (with audit fields)
// ============================================================================

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

// ============================================================================
// Update Operations (with audit fields)
// ============================================================================

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

// ============================================================================
// Soft Delete Operations
// ============================================================================

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

// ============================================================================
// Select Operations (with is_deleted = false filter)
// ============================================================================

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

// ============================================================================
// Transaction Select Operations (with is_deleted = false filter)
// ============================================================================

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

// ============================================================================
// Count Operations (with is_deleted = false filter)
// ============================================================================

// Count returns total non-deleted row count
func (t *DXTable) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	return t.DXRawTable.Count(l, t.addNotDeletedFilter(where), joinSQLPart)
}

// ============================================================================
// Paging Operations (with is_deleted = false filter)
// ============================================================================

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

// ============================================================================
// Upsert Operations (with audit fields)
// ============================================================================

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

// ============================================================================
// DXRawTable - Additional API Helper Methods
// ============================================================================

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
	isExistFilterWhere, customFilterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}
	if isExistFilterWhere && customFilterWhere != "" {
		if filterWhere != "" {
			filterWhere = fmt.Sprintf("(%s) AND (%s)", filterWhere, customFilterWhere)
		} else {
			filterWhere = customFilterWhere
		}
	}

	isExistFilterOrderBy, customFilterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}
	if isExistFilterOrderBy && customFilterOrderBy != "" {
		filterOrderBy = customFilterOrderBy
	}

	isExistFilterKeyValues, customFilterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}
	if isExistFilterKeyValues && customFilterKeyValues != nil {
		if filterKeyValues == nil {
			filterKeyValues = customFilterKeyValues
		} else {
			for k, v := range customFilterKeyValues {
				filterKeyValues[k] = v
			}
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

// ============================================================================
// DXTable - Additional API Helper Methods (with soft-delete)
// ============================================================================

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
	isExistFilterWhere, customFilterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}
	if isExistFilterWhere && customFilterWhere != "" {
		if filterWhere != "" {
			filterWhere = fmt.Sprintf("(%s) AND (%s)", filterWhere, customFilterWhere)
		} else {
			filterWhere = customFilterWhere
		}
	}

	isExistFilterOrderBy, customFilterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}
	if isExistFilterOrderBy && customFilterOrderBy != "" {
		filterOrderBy = customFilterOrderBy
	}

	isExistFilterKeyValues, customFilterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}
	if isExistFilterKeyValues && customFilterKeyValues != nil {
		if filterKeyValues == nil {
			filterKeyValues = customFilterKeyValues
		} else {
			for k, v := range customFilterKeyValues {
				filterKeyValues[k] = v
			}
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

// ============================================================================
// Table3 Manager - Registry for tables
// ============================================================================

// DXTable3Manager manages a collection of DXTable instances
type DXTable3Manager struct {
	Tables                               map[string]*DXTable
	RawTables                            map[string]*DXRawTable
	StandardOperationResponsePossibility map[string]map[string]*api.DXAPIEndPointResponsePossibility
}

// ConnectAll connects all registered tables to their databases
func (tm *DXTable3Manager) ConnectAll() error {
	for _, t := range tm.Tables {
		err := t.EnsureDatabase()
		if err != nil {
			return err
		}
	}
	for _, t := range tm.RawTables {
		err := t.EnsureDatabase()
		if err != nil {
			return err
		}
	}
	return nil
}

// Manager is the global table3 manager instance
var Manager = DXTable3Manager{
	Tables:    make(map[string]*DXTable),
	RawTables: make(map[string]*DXRawTable),
	StandardOperationResponsePossibility: map[string]map[string]*api.DXAPIEndPointResponsePossibility{
		"create": {
			"success": &api.DXAPIEndPointResponsePossibility{
				StatusCode:  200,
				Description: "Success - 200",
				DataTemplate: []*api.DXAPIEndPointParameter{
					{NameId: "id", Type: dxlibTypes.APIParameterTypeInt64, Description: "", IsMustExist: true},
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
					{NameId: "list", Type: dxlibTypes.APIParameterTypeJSON, Description: "", IsMustExist: true, Children: []api.DXAPIEndPointParameter{
						{NameId: "rows", Type: dxlibTypes.APIParameterTypeArray, Description: "", IsMustExist: true},
						{NameId: "total_rows", Type: dxlibTypes.APIParameterTypeInt64, Description: "", IsMustExist: true},
						{NameId: "total_page", Type: dxlibTypes.APIParameterTypeInt64, Description: "", IsMustExist: true},
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
	},
}

// RegisterTable registers a DXTable with the manager
func (m *DXTable3Manager) RegisterTable(name string, table *DXTable) {
	m.Tables[name] = table
}

// RegisterRawTable registers a DXRawTable with the manager
func (m *DXTable3Manager) RegisterRawTable(name string, table *DXRawTable) {
	m.RawTables[name] = table
}

// GetTable returns a registered DXTable by name
func (m *DXTable3Manager) GetTable(name string) *DXTable {
	return m.Tables[name]
}

// GetRawTable returns a registered DXRawTable by name
func (m *DXTable3Manager) GetRawTable(name string) *DXRawTable {
	return m.RawTables[name]
}

// NewDXRawTable3 creates a new DXRawTable wrapping a models.ModelDBTable
func NewDXRawTable3(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:    databaseNameId,
		DBTable:           dbTable,
		FieldNameForRowId: fieldNameForRowId,
	}
}

// NewDXRawTable3WithView creates a new DXRawTable with a custom list view
func NewDXRawTable3WithView(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId, listViewNameId string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:    databaseNameId,
		DBTable:           dbTable,
		FieldNameForRowId: fieldNameForRowId,
		ListViewNameId:    listViewNameId,
	}
}

// NewDXTable3 creates a new DXTable wrapping a models.ModelDBTable
func NewDXTable3(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:    databaseNameId,
			DBTable:           dbTable,
			FieldNameForRowId: fieldNameForRowId,
		},
	}
}

// NewDXTable3WithView creates a new DXTable with a custom list view
func NewDXTable3WithView(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId, listViewNameId string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:    databaseNameId,
			DBTable:           dbTable,
			FieldNameForRowId: fieldNameForRowId,
			ListViewNameId:    listViewNameId,
		},
	}
}

// ============================================================================
// Simple Factory Functions - without models.ModelDBTable (for gradual migration)
// ============================================================================

// NewDXRawTable3Simple creates a DXRawTable with direct table name (no models.ModelDBTable needed)
func NewDXRawTable3Simple(databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:             databaseNameId,
		TableNameDirect:            tableName,
		ResultObjectName:           resultObjectName,
		ListViewNameId:             listViewNameId,
		FieldNameForRowId:          fieldNameForRowId,
		FieldNameForRowUid:         fieldNameForRowUid,
		FieldNameForRowNameId:      fieldNameForRowNameId,
		ResponseEnvelopeObjectName: responseEnvelopeObjectName,
	}
}

// NewDXTable3Simple creates a DXTable with direct table name (no models.ModelDBTable needed)
func NewDXTable3Simple(databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:             databaseNameId,
			TableNameDirect:            tableName,
			ResultObjectName:           resultObjectName,
			ListViewNameId:             listViewNameId,
			FieldNameForRowId:          fieldNameForRowId,
			FieldNameForRowUid:         fieldNameForRowUid,
			FieldNameForRowNameId:      fieldNameForRowNameId,
			ResponseEnvelopeObjectName: responseEnvelopeObjectName,
		},
	}
}

// NewDXTable3WithEncryption creates a DXTable with encryption/decryption definitions
func NewDXTable3WithEncryption(
	databaseNameId, tableName, resultObjectName, listViewNameId,
	fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string,
	encryptedColumnDefs []database.EncryptedColumnDef,
	decryptedColumnDefs []database.DecryptedColumnDef,
) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:             databaseNameId,
			TableNameDirect:            tableName,
			ResultObjectName:           resultObjectName,
			ListViewNameId:             listViewNameId,
			FieldNameForRowId:          fieldNameForRowId,
			FieldNameForRowUid:         fieldNameForRowUid,
			FieldNameForRowNameId:      fieldNameForRowNameId,
			ResponseEnvelopeObjectName: responseEnvelopeObjectName,
			EncryptedColumnDefs:        encryptedColumnDefs,
			DecryptedColumnDefs:        decryptedColumnDefs,
		},
	}
}

// ============================================================================
// DXPropertyTable - Property Table for key-value storage with typed values
// ============================================================================

// DXPropertyTable is a table specialized for storing typed property values
type DXPropertyTable struct {
	DXTable
}

// NewDXPropertyTable3Simple creates a DXPropertyTable with direct table name
func NewDXPropertyTable3Simple(databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string) *DXPropertyTable {
	return &DXPropertyTable{
		DXTable: DXTable{
			DXRawTable: DXRawTable{
				DatabaseNameId:             databaseNameId,
				TableNameDirect:            tableName,
				ResultObjectName:           resultObjectName,
				ListViewNameId:             listViewNameId,
				FieldNameForRowId:          fieldNameForRowId,
				FieldNameForRowUid:         fieldNameForRowUid,
				FieldNameForRowNameId:      fieldNameForRowNameId,
				ResponseEnvelopeObjectName: responseEnvelopeObjectName,
			},
		},
	}
}

// propertyGetAs is a helper to extract typed values from property rows
func propertyGetAs[T any](l *log.DXLog, expectedType string, property map[string]any) (T, error) {
	var zero T

	actualType, ok := property["type"].(string)
	if !ok {
		return zero, l.ErrorAndCreateErrorf("INVALID_TYPE_FIELD_FORMAT: %T", property["type"])
	}
	if actualType != expectedType {
		return zero, l.ErrorAndCreateErrorf("TYPE_MISMATCH_ERROR: EXPECTED_%s_GOT_%s", expectedType, actualType)
	}

	rawValue, err := utils.GetJSONFromKV(property, "value")
	if err != nil {
		return zero, l.ErrorAndCreateErrorf("MISSING_VALUE_FIELD")
	}

	value, ok := rawValue["value"].(T)
	if !ok {
		return zero, l.ErrorAndCreateErrorf("PropertyGetAs:CAN_NOT_GET_VALUE:%v", err)
	}

	return value, nil
}

// GetAsString gets a string property value
func (pt *DXPropertyTable) GetAsString(l *log.DXLog, propertyId string) (string, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return "", err
	}
	return propertyGetAs[string](l, "STRING", v)
}

// SetAsString sets a string property value
func (pt *DXPropertyTable) SetAsString(l *log.DXLog, propertyId string, value string) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "STRING",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// GetAsInt gets an int property value
func (pt *DXPropertyTable) GetAsInt(l *log.DXLog, propertyId string) (int, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return 0, err
	}
	vv, err := propertyGetAs[float64](l, "INT", v)
	if err != nil {
		return 0, err
	}
	return int(vv), nil
}

// GetAsIntOrDefault gets an int property value, returns default if not found
func (pt *DXPropertyTable) GetAsIntOrDefault(l *log.DXLog, propertyId string, defaultValue int) (int, error) {
	_, v, err := pt.SelectOne(l, nil, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return 0, err
	}
	if v == nil {
		err = pt.SetAsInt(l, propertyId, defaultValue)
		if err != nil {
			return 0, err
		}
		return defaultValue, nil
	}
	vv, err := propertyGetAs[float64](l, "INT", v)
	if err != nil {
		return 0, err
	}
	return int(vv), nil
}

// SetAsInt sets an int property value
func (pt *DXPropertyTable) SetAsInt(l *log.DXLog, propertyId string, value int) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "INT",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// TxSetAsInt sets an int property value within a transaction
func (pt *DXPropertyTable) TxSetAsInt(dtx *database.DXDatabaseTx, propertyId string, value int) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.TxUpsert(dtx, utils.JSON{
		"type":  "INT",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// GetAsInt64 gets an int64 property value
func (pt *DXPropertyTable) GetAsInt64(l *log.DXLog, propertyId string) (int64, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return 0, err
	}
	vv, err := propertyGetAs[float64](l, "INT64", v)
	if err != nil {
		return 0, err
	}
	return int64(vv), nil
}

// SetAsInt64 sets an int64 property value
func (pt *DXPropertyTable) SetAsInt64(l *log.DXLog, propertyId string, value int64) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "INT64",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// GetAsJSON gets a JSON property value
func (pt *DXPropertyTable) GetAsJSON(l *log.DXLog, propertyId string) (map[string]any, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return nil, err
	}
	return propertyGetAs[map[string]any](l, "JSON", v)
}

// SetAsJSON sets a JSON property value
func (pt *DXPropertyTable) SetAsJSON(l *log.DXLog, propertyId string, value map[string]any) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return errors.Wrap(err, "SetAsJSON.Marshal")
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "JSON",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// TxSetAsJSON sets a JSON property value within a transaction
func (pt *DXPropertyTable) TxSetAsJSON(dtx *database.DXDatabaseTx, propertyId string, value map[string]any) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return errors.Wrap(err, "TxSetAsJSON.Marshal")
	}
	_, _, err = pt.TxUpsert(dtx, utils.JSON{
		"type":  "JSON",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}
