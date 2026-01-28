package tables

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/database/models"
	"github.com/donnyhardyanto/dxlib/errors"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
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
// Table3 Manager - Registry for tables
// ============================================================================

// DXTableManager manages a collection of DXTable instances
type DXTableManager struct {
	Tables                               map[string]*DXTable
	RawTables                            map[string]*DXRawTable
	StandardOperationResponsePossibility map[string]map[string]*api.DXAPIEndPointResponsePossibility
}

// ConnectAll connects all registered tables to their databases
func (tm *DXTableManager) ConnectAll() error {
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
var Manager = DXTableManager{
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
func (m *DXTableManager) RegisterTable(name string, table *DXTable) {
	m.Tables[name] = table
}

// RegisterRawTable registers a DXRawTable with the manager
func (m *DXTableManager) RegisterRawTable(name string, table *DXRawTable) {
	m.RawTables[name] = table
}

// GetTable returns a registered DXTable by name
func (m *DXTableManager) GetTable(name string) *DXTable {
	return m.Tables[name]
}

// GetRawTable returns a registered DXRawTable by name
func (m *DXTableManager) GetRawTable(name string) *DXRawTable {
	return m.RawTables[name]
}

// ============================================================================
// Factory Functions
// ============================================================================

// NewDXRawTable creates a new DXRawTable wrapping a models.ModelDBTable
func NewDXRawTable(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:    databaseNameId,
		DBTable:           dbTable,
		FieldNameForRowId: fieldNameForRowId,
	}
}

// NewDXRawTableWithView creates a new DXRawTable with a custom list view
func NewDXRawTableWithView(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId, listViewNameId string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:    databaseNameId,
		DBTable:           dbTable,
		FieldNameForRowId: fieldNameForRowId,
		ListViewNameId:    listViewNameId,
	}
}

// NewDXTable creates a new DXTable wrapping a models.ModelDBTable
func NewDXTable(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:    databaseNameId,
			DBTable:           dbTable,
			FieldNameForRowId: fieldNameForRowId,
		},
	}
}

// NewDXTableWithView creates a new DXTable with a custom list view
func NewDXTableWithView(databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId, listViewNameId string) *DXTable {
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

// NewDXRawTableSimple creates a DXRawTable with direct table name (no models.ModelDBTable needed)
func NewDXRawTableSimple(databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string, encryptionKeyDefs []*database.EncryptionKeyDef) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:             databaseNameId,
		TableNameDirect:            tableName,
		ResultObjectName:           resultObjectName,
		ListViewNameId:             listViewNameId,
		FieldNameForRowId:          fieldNameForRowId,
		FieldNameForRowUid:         fieldNameForRowUid,
		FieldNameForRowNameId:      fieldNameForRowNameId,
		ResponseEnvelopeObjectName: responseEnvelopeObjectName,
		EncryptionKeyDefs:          encryptionKeyDefs,
	}
}

// NewDXTableSimple creates a DXTable with direct table name (no models.ModelDBTable needed)
func NewDXTableSimple(databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string, encryptionKeyDefs []*database.EncryptionKeyDef) *DXTable {
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
			EncryptionKeyDefs:          encryptionKeyDefs,
		},
	}
}

// NewDXTableWithEncryption creates a DXTable with encryption/decryption definitions
func NewDXTableWithEncryption(
	databaseNameId, tableName, resultObjectName, listViewNameId,
	fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string,
	encryptionKeyDefs []*database.EncryptionKeyDef,
	encryptionColumnDefs []database.EncryptionColumnDef,
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
			EncryptionKeyDefs:          encryptionKeyDefs,
			EncryptionColumnDefs:       encryptionColumnDefs,
		},
	}
}
