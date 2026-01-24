package table3

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/database2/export"
	"github.com/donnyhardyanto/dxlib/database3"
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
// Standalone Paging Functions - using database3.DXDatabase3
// ============================================================================

// NamedQueryPaging executes a paging query using database3.DXDatabase3
func NamedQueryPaging(
	dxDb3 *database3.DXDatabase3,
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
		dxDb3.Database.Connection,
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
	dxDb3 *database3.DXDatabase3,
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
	dxDb3 *database3.DXDatabase3,
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
	dxDb3 *database3.DXDatabase3,
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
// DXRawTable3 - Basic table wrapper without soft-delete
// ============================================================================

// DXRawTable3 wraps database3 with connection management and basic CRUD
type DXRawTable3 struct {
	DatabaseNameId             string
	Database                   *database3.DXDatabase3
	DBTable                    *database3.DBTable
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
}

// EnsureDatabase ensures database connection is initialized
func (t *DXRawTable3) EnsureDatabase() error {
	if t.Database == nil {
		t.Database = database3.Manager3.GetOrCreate(t.DatabaseNameId)
		if t.Database == nil {
			return errors.Errorf("database not found: %s", t.DatabaseNameId)
		}
	}
	return t.Database.EnsureConnection()
}

// GetDbType returns the database type
func (t *DXRawTable3) GetDbType() base.DXDatabaseType {
	if t.Database == nil || t.Database.Database == nil {
		return base.DXDatabaseTypePostgreSQL
	}
	return t.Database.Database.DatabaseType
}

// TableName returns the full table name from DBTable or TableNameDirect
func (t *DXRawTable3) TableName() string {
	if t.DBTable != nil {
		return t.DBTable.FullTableName()
	}
	return t.TableNameDirect
}

// ============================================================================
// Insert Operations
// ============================================================================

// Insert inserts a new row and returns result
func (t *DXRawTable3) Insert(l *log.DXLog, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Insert(t.TableName(), data, returningFieldNames)
}

// TxInsert inserts within a transaction
func (t *DXRawTable3) TxInsert(dtx *database3.DXDatabaseTx3, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	return dtx.Insert(t.TableName(), data, returningFieldNames)
}

// InsertReturningId is a simplified insert that returns just the new ID (backward compatible)
func (t *DXRawTable3) InsertReturningId(l *log.DXLog, data utils.JSON) (int64, error) {
	_, returningValues, err := t.Insert(l, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// TxInsertReturningId is a simplified TxInsert that returns just the new ID (backward compatible)
func (t *DXRawTable3) TxInsertReturningId(dtx *database3.DXDatabaseTx3, data utils.JSON) (int64, error) {
	_, returningValues, err := t.TxInsert(dtx, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// DoInsert is an API helper that inserts and writes response
func (t *DXRawTable3) DoInsert(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
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
func (t *DXRawTable3) Update(l *log.DXLog, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Update(t.TableName(), data, where, returningFieldNames)
}

// TxUpdate updates within a transaction
func (t *DXRawTable3) TxUpdate(dtx *database3.DXDatabaseTx3, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return dtx.Update(t.TableName(), data, where, returningFieldNames)
}

// UpdateSimple is a simplified update that just takes data and where (backward compatible)
func (t *DXRawTable3) UpdateSimple(data utils.JSON, where utils.JSON) (sql.Result, error) {
	result, _, err := t.Update(nil, data, where, nil)
	return result, err
}

// TxUpdateSimple is a simplified transaction update (backward compatible)
func (t *DXRawTable3) TxUpdateSimple(dtx *database3.DXDatabaseTx3, data utils.JSON, where utils.JSON) (sql.Result, error) {
	result, _, err := t.TxUpdate(dtx, data, where, nil)
	return result, err
}

// UpdateById updates a single row by ID
func (t *DXRawTable3) UpdateById(l *log.DXLog, id int64, data utils.JSON) (sql.Result, error) {
	result, _, err := t.Update(l, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// TxUpdateById updates a single row by ID within a transaction
func (t *DXRawTable3) TxUpdateById(dtx *database3.DXDatabaseTx3, id int64, data utils.JSON) (sql.Result, error) {
	result, _, err := t.TxUpdate(dtx, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DoUpdate is an API helper that updates and writes response
func (t *DXRawTable3) DoUpdate(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
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

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// ============================================================================
// Delete Operations (Hard Delete)
// ============================================================================

// Delete performs hard delete of rows matching where condition
func (t *DXRawTable3) Delete(l *log.DXLog, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Delete(t.TableName(), where, returningFieldNames)
}

// TxDelete deletes within a transaction
func (t *DXRawTable3) TxDelete(dtx *database3.DXDatabaseTx3, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return dtx.Delete(t.TableName(), where, returningFieldNames)
}

// DeleteById deletes a single row by ID
func (t *DXRawTable3) DeleteById(l *log.DXLog, id int64) (sql.Result, error) {
	result, _, err := t.Delete(l, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// TxDeleteById deletes a single row by ID within a transaction
func (t *DXRawTable3) TxDeleteById(dtx *database3.DXDatabaseTx3, id int64) (sql.Result, error) {
	result, _, err := t.TxDelete(dtx, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DoDelete is an API helper that deletes and writes response
func (t *DXRawTable3) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) error {
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
func (t *DXRawTable3) Select(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Select(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// SelectOne returns a single row matching where condition
func (t *DXRawTable3) SelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.SelectOne(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// ShouldSelectOne returns a single row or error if not found
func (t *DXRawTable3) ShouldSelectOne(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.ShouldSelectOne(t.TableName(), t.FieldTypeMapping, nil, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// GetById returns a row by ID
func (t *DXRawTable3) GetById(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetById returns a row by ID or error if not found
func (t *DXRawTable3) ShouldGetById(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUid returns a row by UID
func (t *DXRawTable3) GetByUid(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUid returns a row by UID or error if not found
func (t *DXRawTable3) ShouldGetByUid(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByUtag returns a row by Utag
func (t *DXRawTable3) GetByUtag(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtag returns a row by Utag or error if not found
func (t *DXRawTable3) ShouldGetByUtag(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// GetByNameId returns a row by NameId
func (t *DXRawTable3) GetByNameId(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameId returns a row by NameId or error if not found
func (t *DXRawTable3) ShouldGetByNameId(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ============================================================================
// Transaction Select Operations
// ============================================================================

// TxSelect returns multiple rows within a transaction
func (t *DXRawTable3) TxSelect(dtx *database3.DXDatabaseTx3, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return dtx.Select(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// TxSelectOne returns a single row within a transaction
func (t *DXRawTable3) TxSelectOne(dtx *database3.DXDatabaseTx3, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return dtx.SelectOne(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxShouldSelectOne returns a single row or error if not found within a transaction
func (t *DXRawTable3) TxShouldSelectOne(dtx *database3.DXDatabaseTx3, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return dtx.ShouldSelectOne(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxGetById returns a row by ID within a transaction
func (t *DXRawTable3) TxGetById(dtx *database3.DXDatabaseTx3, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetById returns a row by ID or error if not found within a transaction
func (t *DXRawTable3) TxShouldGetById(dtx *database3.DXDatabaseTx3, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// ============================================================================
// Count Operations
// ============================================================================

// Count returns total row count
func (t *DXRawTable3) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}
	return t.Database.Count(t.TableName(), where, joinSQLPart)
}

// ============================================================================
// Upsert Operations
// ============================================================================

// Upsert inserts or updates a row based on where condition
func (t *DXRawTable3) Upsert(l *log.DXLog, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
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
func (t *DXRawTable3) TxUpsert(dtx *database3.DXDatabaseTx3, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
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
func (t *DXRawTable3) GetListViewName() string {
	if t.ListViewNameId != "" {
		return t.ListViewNameId
	}
	return t.TableName()
}

// NewQueryBuilder creates a QueryBuilder with the table's database type
func (t *DXRawTable3) NewQueryBuilder() *QueryBuilder {
	return NewQueryBuilder(t.GetDbType())
}

// Paging executes a paging query with WHERE clause and ORDER BY
func (t *DXRawTable3) Paging(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	rowsInfo, list, totalRows, totalPages, _, err := db.NamedQueryPaging(
		t.Database.Database.Connection,
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
func (t *DXRawTable3) PagingWithBuilder(l *log.DXLog, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPaging is an API helper that handles paging
func (t *DXRawTable3) DoPaging(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	result, err := t.Paging(&aepr.Log, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		aepr.Log.Errorf(err, "Error at paging table %s (%s)", t.TableName(), err.Error())
		return nil, err
	}
	return result, nil
}

// DoPagingWithBuilder is an API helper using QueryBuilder
func (t *DXRawTable3) DoPagingWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPagingResponse executes paging and writes standard JSON response
func (t *DXRawTable3) DoPagingResponse(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) error {
	result, err := t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// DoPagingResponseWithBuilder executes paging with QueryBuilder and writes response
func (t *DXRawTable3) DoPagingResponseWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) error {
	whereClause, args := qb.Build()
	return t.DoPagingResponse(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// ============================================================================
// DXTable3 - Table wrapper with soft-delete and audit fields
// ============================================================================

// DXTable3 extends DXRawTable3 with soft-delete and audit fields
type DXTable3 struct {
	DXRawTable3
}

// ============================================================================
// Audit Field Helpers
// ============================================================================

// SetInsertAuditFields sets created_at, created_by_user_id, etc. for insert
func (t *DXTable3) SetInsertAuditFields(aepr *api.DXAPIEndPointRequest, data utils.JSON) {
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
func (t *DXTable3) SetUpdateAuditFields(aepr *api.DXAPIEndPointRequest, data utils.JSON) {
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
func (t *DXTable3) Insert(l *log.DXLog, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable3.Insert(l, data, returningFieldNames)
}

// TxInsert inserts within a transaction with audit fields
func (t *DXTable3) TxInsert(dtx *database3.DXDatabaseTx3, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable3.TxInsert(dtx, data, returningFieldNames)
}

// DoInsert is an API helper with audit fields
func (t *DXTable3) DoInsert(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	return t.DXRawTable3.DoInsert(aepr, data)
}

// ============================================================================
// Update Operations (with audit fields)
// ============================================================================

// Update updates with audit fields
func (t *DXTable3) Update(l *log.DXLog, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable3.Update(l, data, where, returningFieldNames)
}

// TxUpdate updates within a transaction with audit fields
func (t *DXTable3) TxUpdate(dtx *database3.DXDatabaseTx3, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable3.TxUpdate(dtx, data, where, returningFieldNames)
}

// UpdateById updates with audit fields
func (t *DXTable3) UpdateById(l *log.DXLog, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable3.UpdateById(l, id, data)
}

// TxUpdateById updates within a transaction with audit fields
func (t *DXTable3) TxUpdateById(dtx *database3.DXDatabaseTx3, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable3.TxUpdateById(dtx, id, data)
}

// DoUpdate is an API helper with audit fields
func (t *DXTable3) DoUpdate(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
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

	_, err = t.DXRawTable3.UpdateById(&aepr.Log, id, data)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// ============================================================================
// Soft Delete Operations
// ============================================================================

// SoftDelete marks rows as deleted
func (t *DXTable3) SoftDelete(l *log.DXLog, where utils.JSON) (sql.Result, error) {
	data := utils.JSON{
		"is_deleted": true,
	}
	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.DXRawTable3.Update(l, data, where, nil)
	return result, err
}

// TxSoftDelete marks rows as deleted within a transaction
func (t *DXTable3) TxSoftDelete(dtx *database3.DXDatabaseTx3, where utils.JSON) (sql.Result, error) {
	data := utils.JSON{
		"is_deleted": true,
	}
	t.SetUpdateAuditFields(nil, data)
	result, _, err := t.DXRawTable3.TxUpdate(dtx, data, where, nil)
	return result, err
}

// SoftDeleteById marks a row as deleted by ID
func (t *DXTable3) SoftDeleteById(l *log.DXLog, id int64) (sql.Result, error) {
	return t.SoftDelete(l, utils.JSON{t.FieldNameForRowId: id})
}

// TxSoftDeleteById marks a row as deleted by ID within a transaction
func (t *DXTable3) TxSoftDeleteById(dtx *database3.DXDatabaseTx3, id int64) (sql.Result, error) {
	return t.TxSoftDelete(dtx, utils.JSON{t.FieldNameForRowId: id})
}

// DoSoftDelete is an API helper for soft delete
func (t *DXTable3) DoSoftDelete(aepr *api.DXAPIEndPointRequest, id int64) error {
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

	_, err = t.DXRawTable3.UpdateById(&aepr.Log, id, data)
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
func (t *DXTable3) addNotDeletedFilter(where utils.JSON) utils.JSON {
	if where == nil {
		where = utils.JSON{}
	}
	where["is_deleted"] = false
	return where
}

// Select returns non-deleted rows
func (t *DXTable3) Select(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable3.Select(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOne returns a single non-deleted row
func (t *DXTable3) SelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable3.SelectOne(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// ShouldSelectOne returns a single non-deleted row or error if not found
func (t *DXTable3) ShouldSelectOne(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable3.ShouldSelectOne(l, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// GetByIdNotDeleted returns a non-deleted row by ID
func (t *DXTable3) GetByIdNotDeleted(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdNotDeleted returns a non-deleted row by ID or error if not found
func (t *DXTable3) ShouldGetByIdNotDeleted(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUidNotDeleted returns a non-deleted row by UID
func (t *DXTable3) GetByUidNotDeleted(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidNotDeleted returns a non-deleted row by UID or error if not found
func (t *DXTable3) ShouldGetByUidNotDeleted(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByNameIdNotDeleted returns a non-deleted row by NameId
func (t *DXTable3) GetByNameIdNotDeleted(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdNotDeleted returns a non-deleted row by NameId or error if not found
func (t *DXTable3) ShouldGetByNameIdNotDeleted(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ============================================================================
// Transaction Select Operations (with is_deleted = false filter)
// ============================================================================

// TxSelect returns non-deleted rows within a transaction
func (t *DXTable3) TxSelect(dtx *database3.DXDatabaseTx3, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable3.TxSelect(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOne returns a single non-deleted row within a transaction
func (t *DXTable3) TxSelectOne(dtx *database3.DXDatabaseTx3, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable3.TxSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOne returns a single non-deleted row or error if not found within a transaction
func (t *DXTable3) TxShouldSelectOne(dtx *database3.DXDatabaseTx3, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable3.TxShouldSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxGetByIdNotDeleted returns a non-deleted row by ID within a transaction
func (t *DXTable3) TxGetByIdNotDeleted(dtx *database3.DXDatabaseTx3, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdNotDeleted returns a non-deleted row by ID or error if not found within a transaction
func (t *DXTable3) TxShouldGetByIdNotDeleted(dtx *database3.DXDatabaseTx3, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// ============================================================================
// Count Operations (with is_deleted = false filter)
// ============================================================================

// Count returns total non-deleted row count
func (t *DXTable3) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	return t.DXRawTable3.Count(l, t.addNotDeletedFilter(where), joinSQLPart)
}

// ============================================================================
// Paging Operations (with is_deleted = false filter)
// ============================================================================

// NewQueryBuilder creates a QueryBuilder with NotDeleted filter pre-applied
func (t *DXTable3) NewQueryBuilder() *QueryBuilder {
	qb := NewQueryBuilder(t.GetDbType())
	qb.NotDeleted()
	return qb
}

// NewQueryBuilderRaw creates a QueryBuilder without NotDeleted filter (for special cases)
func (t *DXTable3) NewQueryBuilderRaw() *QueryBuilder {
	return NewQueryBuilder(t.GetDbType())
}

// addNotDeletedToWhere adds is_deleted filter to existing WHERE clause
func (t *DXTable3) addNotDeletedToWhere(whereClause string) string {
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
func (t *DXTable3) Paging(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	return t.DXRawTable3.Paging(l, rowPerPage, pageIndex, t.addNotDeletedToWhere(whereClause), orderBy, args)
}

// PagingWithBuilder executes a paging query using a QueryBuilder (assumes NotDeleted already added)
func (t *DXTable3) PagingWithBuilder(l *log.DXLog, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.DXRawTable3.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// PagingIncludeDeleted executes a paging query including deleted records
func (t *DXTable3) PagingIncludeDeleted(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	return t.DXRawTable3.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPaging is an API helper with automatic is_deleted filter
func (t *DXTable3) DoPaging(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	return t.Paging(&aepr.Log, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPagingWithBuilder is an API helper using QueryBuilder
func (t *DXTable3) DoPagingWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	return t.PagingWithBuilder(&aepr.Log, rowPerPage, pageIndex, qb, orderBy)
}

// DoPagingResponse executes paging and writes standard JSON response
func (t *DXTable3) DoPagingResponse(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) error {
	result, err := t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// DoPagingResponseWithBuilder executes paging with QueryBuilder and writes response
func (t *DXTable3) DoPagingResponseWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) error {
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
func (t *DXTable3) Upsert(l *log.DXLog, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, 0, err
	}

	_, existing, err := t.DXRawTable3.SelectOne(l, nil, where, nil, nil)
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
func (t *DXTable3) TxUpsert(dtx *database3.DXDatabaseTx3, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	_, existing, err := t.DXRawTable3.TxSelectOne(dtx, nil, where, nil, nil, nil)
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
// DXRawTable3 - Additional API Helper Methods
// ============================================================================

// SelectAll returns all rows from the table
func (t *DXRawTable3) SelectAll(l *log.DXLog) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, nil, nil, nil, nil, nil, nil)
}

// TxShouldGetByNameId returns a row by NameId within a transaction or error if not found
func (t *DXRawTable3) TxShouldGetByNameId(dtx *database3.DXDatabaseTx3, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxHardDelete deletes rows within a transaction (hard delete)
func (t *DXRawTable3) TxHardDelete(dtx *database3.DXDatabaseTx3, where utils.JSON) (sql.Result, error) {
	result, _, err := t.TxDelete(dtx, where, nil)
	return result, err
}

// RequestPagingList handles list/paging API requests
func (t *DXRawTable3) RequestPagingList(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXRawTable3) RequestRead(aepr *api.DXAPIEndPointRequest) error {
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

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, row)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByUid handles read by UID API requests
func (t *DXRawTable3) RequestReadByUid(aepr *api.DXAPIEndPointRequest) error {
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

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, row)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByUtag handles read by Utag API requests
func (t *DXRawTable3) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXRawTable3) RequestEdit(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXRawTable3) RequestHardDelete(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	return t.DoDelete(aepr, id)
}

// DoCreate inserts a row and writes API response (suppresses errors)
func (t *DXRawTable3) DoCreate(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	newId, err := t.DoInsert(aepr, data)
	if err != nil {
		aepr.WriteResponseAsError(http.StatusConflict, err)
		return 0, nil
	}
	return newId, nil
}

// RequestReadByNameId handles read by NameId API requests
func (t *DXRawTable3) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) error {
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

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, row)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestListDownload handles list download API requests (export to xlsx/csv/xls)
func (t *DXRawTable3) RequestListDownload(aepr *api.DXAPIEndPointRequest) error {
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
		t.Database.Database.Connection,
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
func (t *DXRawTable3) RequestCreate(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	newId, err := t.DoCreate(aepr, data)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{"id": newId})
	return nil
}

// DoEdit is an alias for DoUpdate (backward compatibility)
func (t *DXRawTable3) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	return t.DoUpdate(aepr, id, data)
}

// OnResultList is a callback type for paging result processing
type OnResultList func(aepr *api.DXAPIEndPointRequest, list []utils.JSON) ([]utils.JSON, error)

// DoRequestPagingList handles paging with optional result processing
func (t *DXRawTable3) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) error {
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
// DXTable3 - Additional API Helper Methods (with soft-delete)
// ============================================================================

// SelectAll returns all non-deleted rows from the table
func (t *DXTable3) SelectAll(l *log.DXLog) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, nil, nil, nil, nil, nil, nil)
}

// TxShouldGetByNameIdNotDeleted returns a non-deleted row by NameId within a transaction or error if not found
func (t *DXTable3) TxShouldGetByNameIdNotDeleted(dtx *database3.DXDatabaseTx3, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameId is an alias for TxShouldGetByNameIdNotDeleted
func (t *DXTable3) TxShouldGetByNameId(dtx *database3.DXDatabaseTx3, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldGetByNameIdNotDeleted(dtx, nameId)
}

// TxHardDelete deletes rows within a transaction (bypasses soft-delete)
func (t *DXTable3) TxHardDelete(dtx *database3.DXDatabaseTx3, where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable3.TxDelete(dtx, where, nil)
	return result, err
}

// RequestPagingList handles list/paging API requests (with is_deleted filter)
func (t *DXTable3) RequestPagingList(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXTable3) RequestRead(aepr *api.DXAPIEndPointRequest) error {
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

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, row)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByUid handles read by UID API requests (with is_deleted filter)
func (t *DXTable3) RequestReadByUid(aepr *api.DXAPIEndPointRequest) error {
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

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, row)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestEdit handles edit by ID API requests (with is_deleted filter and audit fields)
func (t *DXTable3) RequestEdit(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXTable3) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) error {
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

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, row)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// DoEdit is an alias for DoUpdate (backward compatibility)
func (t *DXTable3) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	return t.DoUpdate(aepr, id, data)
}

// DoRequestPagingList handles paging with optional result processing (with is_deleted filter)
func (t *DXTable3) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) error {
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
func (t *DXTable3) RequestSoftDelete(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	return t.DoSoftDelete(aepr, id)
}

// RequestHardDelete handles hard delete by ID API requests (bypasses soft-delete)
func (t *DXTable3) RequestHardDelete(aepr *api.DXAPIEndPointRequest) error {
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

	_, err = t.DXRawTable3.DeleteById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// DoCreate inserts a row with audit fields and writes API response (suppresses errors)
func (t *DXTable3) DoCreate(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	newId, err := t.DXRawTable3.DoInsert(aepr, data)
	if err != nil {
		aepr.WriteResponseAsError(http.StatusConflict, err)
		return 0, nil
	}
	return newId, nil
}

// RequestCreate handles create API requests (reads parameters and inserts)
func (t *DXTable3) RequestCreate(aepr *api.DXAPIEndPointRequest) error {
	_, data, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	newId, err := t.DoCreate(aepr, data)
	if err != nil {
		return err
	}

	_, row, err := t.ShouldGetByIdNotDeleted(&aepr.Log, newId)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
		"data": utils.JSON{
			t.ResponseEnvelopeObjectName: row,
		},
	})
	return nil
}

// RequestPagingListAll handles paging list all API requests (no filter, all records)
func (t *DXTable3) RequestPagingListAll(aepr *api.DXAPIEndPointRequest) error {
	return t.DoRequestPagingList(aepr, "", "", nil, nil)
}

// RequestList handles list API requests (with filters from parameters)
func (t *DXTable3) RequestList(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXTable3) RequestListAll(aepr *api.DXAPIEndPointRequest) error {
	return t.RequestList(aepr)
}

// RequestEditByUid handles edit by UID API requests
func (t *DXTable3) RequestEditByUid(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXTable3) RequestSoftDeleteByUid(aepr *api.DXAPIEndPointRequest) error {
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
func (t *DXTable3) RequestHardDeleteByUid(aepr *api.DXAPIEndPointRequest) error {
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

	_, err = t.DXRawTable3.DeleteById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// ============================================================================
// Table3 Manager - Registry for tables
// ============================================================================

// DXTable3Manager manages a collection of DXTable3 instances
type DXTable3Manager struct {
	Tables                               map[string]*DXTable3
	RawTables                            map[string]*DXRawTable3
	StandardOperationResponsePossibility map[string]map[string]*api.DXAPIEndPointResponsePossibility
}

// Manager is the global table3 manager instance
var Manager = DXTable3Manager{
	Tables:    make(map[string]*DXTable3),
	RawTables: make(map[string]*DXRawTable3),
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

// RegisterTable registers a DXTable3 with the manager
func (m *DXTable3Manager) RegisterTable(name string, table *DXTable3) {
	m.Tables[name] = table
}

// RegisterRawTable registers a DXRawTable3 with the manager
func (m *DXTable3Manager) RegisterRawTable(name string, table *DXRawTable3) {
	m.RawTables[name] = table
}

// GetTable returns a registered DXTable3 by name
func (m *DXTable3Manager) GetTable(name string) *DXTable3 {
	return m.Tables[name]
}

// GetRawTable returns a registered DXRawTable3 by name
func (m *DXTable3Manager) GetRawTable(name string) *DXRawTable3 {
	return m.RawTables[name]
}

// NewDXRawTable3 creates a new DXRawTable3 wrapping a database3.DBTable
func NewDXRawTable3(databaseNameId string, dbTable *database3.DBTable, fieldNameForRowId string) *DXRawTable3 {
	return &DXRawTable3{
		DatabaseNameId:    databaseNameId,
		DBTable:           dbTable,
		FieldNameForRowId: fieldNameForRowId,
	}
}

// NewDXRawTable3WithView creates a new DXRawTable3 with a custom list view
func NewDXRawTable3WithView(databaseNameId string, dbTable *database3.DBTable, fieldNameForRowId, listViewNameId string) *DXRawTable3 {
	return &DXRawTable3{
		DatabaseNameId:    databaseNameId,
		DBTable:           dbTable,
		FieldNameForRowId: fieldNameForRowId,
		ListViewNameId:    listViewNameId,
	}
}

// NewDXTable3 creates a new DXTable3 wrapping a database3.DBTable
func NewDXTable3(databaseNameId string, dbTable *database3.DBTable, fieldNameForRowId string) *DXTable3 {
	return &DXTable3{
		DXRawTable3: DXRawTable3{
			DatabaseNameId:    databaseNameId,
			DBTable:           dbTable,
			FieldNameForRowId: fieldNameForRowId,
		},
	}
}

// NewDXTable3WithView creates a new DXTable3 with a custom list view
func NewDXTable3WithView(databaseNameId string, dbTable *database3.DBTable, fieldNameForRowId, listViewNameId string) *DXTable3 {
	return &DXTable3{
		DXRawTable3: DXRawTable3{
			DatabaseNameId:    databaseNameId,
			DBTable:           dbTable,
			FieldNameForRowId: fieldNameForRowId,
			ListViewNameId:    listViewNameId,
		},
	}
}

// ============================================================================
// Simple Factory Functions - without database3.DBTable (for gradual migration)
// ============================================================================

// NewDXRawTable3Simple creates a DXRawTable3 with direct table name (no database3.DBTable needed)
func NewDXRawTable3Simple(databaseNameId, tableName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId string) *DXRawTable3 {
	return &DXRawTable3{
		DatabaseNameId:        databaseNameId,
		TableNameDirect:       tableName,
		ListViewNameId:        listViewNameId,
		FieldNameForRowId:     fieldNameForRowId,
		FieldNameForRowUid:    fieldNameForRowUid,
		FieldNameForRowNameId: fieldNameForRowNameId,
	}
}

// NewDXTable3Simple creates a DXTable3 with direct table name (no database3.DBTable needed)
func NewDXTable3Simple(databaseNameId, tableName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId string) *DXTable3 {
	return &DXTable3{
		DXRawTable3: DXRawTable3{
			DatabaseNameId:        databaseNameId,
			TableNameDirect:       tableName,
			ListViewNameId:        listViewNameId,
			FieldNameForRowId:     fieldNameForRowId,
			FieldNameForRowUid:    fieldNameForRowUid,
			FieldNameForRowNameId: fieldNameForRowNameId,
		},
	}
}

// ============================================================================
// DXPropertyTable3 - Property Table for key-value storage with typed values
// ============================================================================

// DXPropertyTable3 is a table specialized for storing typed property values
type DXPropertyTable3 struct {
	DXTable3
}

// NewDXPropertyTable3Simple creates a DXPropertyTable3 with direct table name
func NewDXPropertyTable3Simple(databaseNameId, tableName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId string) *DXPropertyTable3 {
	return &DXPropertyTable3{
		DXTable3: DXTable3{
			DXRawTable3: DXRawTable3{
				DatabaseNameId:        databaseNameId,
				TableNameDirect:       tableName,
				ListViewNameId:        listViewNameId,
				FieldNameForRowId:     fieldNameForRowId,
				FieldNameForRowUid:    fieldNameForRowUid,
				FieldNameForRowNameId: fieldNameForRowNameId,
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
func (pt *DXPropertyTable3) GetAsString(l *log.DXLog, propertyId string) (string, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return "", err
	}
	return propertyGetAs[string](l, "STRING", v)
}

// SetAsString sets a string property value
func (pt *DXPropertyTable3) SetAsString(l *log.DXLog, propertyId string, value string) error {
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
func (pt *DXPropertyTable3) GetAsInt(l *log.DXLog, propertyId string) (int, error) {
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
func (pt *DXPropertyTable3) GetAsIntOrDefault(l *log.DXLog, propertyId string, defaultValue int) (int, error) {
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
func (pt *DXPropertyTable3) SetAsInt(l *log.DXLog, propertyId string, value int) error {
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
func (pt *DXPropertyTable3) TxSetAsInt(dtx *database3.DXDatabaseTx3, propertyId string, value int) error {
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
func (pt *DXPropertyTable3) GetAsInt64(l *log.DXLog, propertyId string) (int64, error) {
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
func (pt *DXPropertyTable3) SetAsInt64(l *log.DXLog, propertyId string, value int64) error {
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
func (pt *DXPropertyTable3) GetAsJSON(l *log.DXLog, propertyId string) (map[string]any, error) {
	_, v, err := pt.ShouldSelectOne(l, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return nil, err
	}
	return propertyGetAs[map[string]any](l, "JSON", v)
}

// SetAsJSON sets a JSON property value
func (pt *DXPropertyTable3) SetAsJSON(l *log.DXLog, propertyId string, value map[string]any) error {
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
func (pt *DXPropertyTable3) TxSetAsJSON(dtx *database3.DXDatabaseTx3, propertyId string, value map[string]any) error {
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
