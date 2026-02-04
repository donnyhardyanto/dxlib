package tables

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// QueryBuilder - Fluent API for building WHERE clauses

// QueryBuilder builds SQL WHERE clauses with fluent API
type QueryBuilder struct {
	Conditions     []string
	Args           utils.JSON
	DbType         base.DXDatabaseType
	TableInterface DXRawTableInterface
	Error          error
}

// NewQueryBuilder creates a new QueryBuilder
func NewQueryBuilder(dbType base.DXDatabaseType, tableInterface DXRawTableInterface) *QueryBuilder {
	return &QueryBuilder{
		Conditions:     []string{},
		Args:           utils.JSON{},
		DbType:         dbType,
		TableInterface: tableInterface,
	}
}

// And adds a raw condition with AND
func (qb *QueryBuilder) And(condition string) *QueryBuilder {
	if condition != "" {
		qb.Conditions = append(qb.Conditions, condition)
	}
	return qb
}

// quoteIdentifier quotes a SQL identifier based on database type to prevent SQL injection
func (qb *QueryBuilder) quoteIdentifier(identifier string) string {
	switch qb.DbType {
	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses [identifier] - escape ] as ]]
		return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
	case base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypeMariaDB, base.DXDatabaseTypeOracle:
		// PostgreSQL, MariaDB, Oracle use "identifier" - escape " as ""
		return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
	default:
		// PostgreSQL style as fallback
		return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
	}
}

func (qb *QueryBuilder) IsFieldExist(fieldName string) bool {
	if qb.TableInterface == nil {
		return false
	}
	searchFieldNames := qb.TableInterface.GetSearchTextFieldNames()
	if !slices.Contains(searchFieldNames, fieldName) {
		return false
	}
	return true
}

func (qb *QueryBuilder) CheckFieldExist(fieldName string) *QueryBuilder {
	if qb.TableInterface == nil {
		qb.Error = errors.New(fmt.Sprintf("SHOULD_NOT_HAPPEN:TABLE_NOT_SET:%s", fieldName))
		return qb
	}
	searchFieldNames := qb.TableInterface.GetSearchTextFieldNames()
	if !slices.Contains(searchFieldNames, fieldName) {
		qb.Error = errors.New(fmt.Sprintf("SHOULD_NOT_HAPPEN:INVALID_FIELD_NAME_IN_TABLE:%s:", qb.TableInterface.GetFullTableName(), fieldName))
		return qb
	}
	return qb
}

// Eq adds field = value condition
func (qb *QueryBuilder) Eq(fieldName string, value any) *QueryBuilder {
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}
	qb.Conditions = append(qb.Conditions, fmt.Sprintf("%s = :%s", qb.quoteIdentifier(fieldName), fieldName))
	qb.Args[fieldName] = value
	return qb
}

// EqOrIn adds field = value for single values, or field IN (values) for arrays
// This is useful for filter_key_values where values can be either single or array
// Supports: []any, []string, []int64, []float64 (converted to []int64)
func (qb *QueryBuilder) EqOrIn(fieldName string, value any) *QueryBuilder {
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}

	switch v := value.(type) {
	case []any:
		return qb.inFromAnySlice(fieldName, v)
	case []string:
		if len(v) == 0 {
			return qb
		}
		return qb.InStrings(fieldName, v)
	case []int64:
		if len(v) == 0 {
			return qb
		}
		return qb.InInt64(fieldName, v)
	case []float64:
		if len(v) == 0 {
			return qb
		}
		var int64Vals []int64
		for _, f := range v {
			int64Vals = append(int64Vals, int64(f))
		}
		return qb.InInt64(fieldName, int64Vals)
	default:
		// Single value - use Eq
		qb.Conditions = append(qb.Conditions, fmt.Sprintf("%s = :%s", qb.quoteIdentifier(fieldName), fieldName))
		qb.Args[fieldName] = value
		return qb
	}
}

// inFromAnySlice handles []any from JSON parsing and converts to appropriate IN clause
func (qb *QueryBuilder) inFromAnySlice(fieldName string, values []any) *QueryBuilder {
	if len(values) == 0 {
		return qb
	}

	// Detect type from first element
	first := values[0]
	switch first.(type) {
	case string:
		var strVals []string
		for _, v := range values {
			if s, ok := v.(string); ok {
				strVals = append(strVals, s)
			}
		}
		return qb.InStrings(fieldName, strVals)
	case float64:
		// JSON numbers are parsed as float64
		var int64Vals []int64
		for _, v := range values {
			if f, ok := v.(float64); ok {
				int64Vals = append(int64Vals, int64(f))
			}
		}
		return qb.InInt64(fieldName, int64Vals)
	case int64:
		var int64Vals []int64
		for _, v := range values {
			if i, ok := v.(int64); ok {
				int64Vals = append(int64Vals, i)
			}
		}
		return qb.InInt64(fieldName, int64Vals)
	default:
		// Fallback: treat as strings
		var strVals []string
		for _, v := range values {
			strVals = append(strVals, fmt.Sprintf("%v", v))
		}
		return qb.InStrings(fieldName, strVals)
	}
}

// Ne adds field != value condition
func (qb *QueryBuilder) Ne(fieldName string, value any) *QueryBuilder {
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}
	qb.Conditions = append(qb.Conditions, fmt.Sprintf("%s != :%s", qb.quoteIdentifier(fieldName), fieldName))
	qb.Args[fieldName] = value
	return qb
}

// Like adds field LIKE value condition (case-sensitive)
func (qb *QueryBuilder) Like(fieldName string, value string) *QueryBuilder {
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}
	qb.Conditions = append(qb.Conditions, fmt.Sprintf("%s LIKE :%s", qb.quoteIdentifier(fieldName), fieldName))
	qb.Args[fieldName] = "%" + value + "%"
	return qb
}

// ILike adds field ILIKE value condition (case-insensitive, PostgreSQL)
func (qb *QueryBuilder) ILike(fieldName string, value string) *QueryBuilder {
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}
	qb.Conditions = append(qb.Conditions, fmt.Sprintf("%s ILIKE :%s", qb.quoteIdentifier(fieldName), fieldName))
	qb.Args[fieldName] = "%" + value + "%"
	return qb
}

// SearchLike adds OR condition for multiple fields with ILIKE
func (qb *QueryBuilder) SearchLike(value string, fieldNames ...string) *QueryBuilder {
	if value == "" || len(fieldNames) == 0 {
		return qb
	}
	var parts []string
	for i, fieldName := range fieldNames {
		qb.CheckFieldExist(fieldName)
		if qb.Error != nil {
			return qb
		}
		argName := fmt.Sprintf("search_%d", i)
		parts = append(parts, fmt.Sprintf("%s ILIKE :%s", qb.quoteIdentifier(fieldName), argName))
		qb.Args[argName] = "%" + value + "%"
	}
	qb.Conditions = append(qb.Conditions, "("+strings.Join(parts, " OR ")+")")
	return qb
}

// In adds field IN (values) condition
func (qb *QueryBuilder) In(fieldName string, values any) *QueryBuilder {
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}
	qb.Conditions = append(qb.Conditions, qb.buildInClause(fieldName, values))
	return qb
}

// InInt64 adds field IN (values) for int64 slice
func (qb *QueryBuilder) InInt64(fieldName string, values []int64) *QueryBuilder {
	if len(values) == 0 {
		return qb
	}
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}
	var strVals []string
	for _, v := range values {
		strVals = append(strVals, fmt.Sprintf("%d", v))
	}
	qb.Conditions = append(qb.Conditions, fmt.Sprintf("%s IN (%s)", qb.quoteIdentifier(fieldName), strings.Join(strVals, ", ")))
	return qb
}

// InStrings adds field IN (values) for string slice
func (qb *QueryBuilder) InStrings(fieldName string, values []string) *QueryBuilder {
	if len(values) == 0 {
		return qb
	}
	qb.CheckFieldExist(fieldName)
	if qb.Error != nil {
		return qb
	}
	var quotedVals []string
	for _, v := range values {
		quotedVals = append(quotedVals, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
	}
	qb.Conditions = append(qb.Conditions, fmt.Sprintf("%s IN (%s)", qb.quoteIdentifier(fieldName), strings.Join(quotedVals, ", ")))
	return qb
}

// NotDeleted adds is_deleted = false condition (databases-aware)
func (qb *QueryBuilder) NotDeleted() *QueryBuilder {
	switch qb.DbType {
	case base.DXDatabaseTypeSQLServer:
		qb.Conditions = append(qb.Conditions, "is_deleted = 0")
	default:
		qb.Conditions = append(qb.Conditions, "is_deleted = false")
	}
	return qb
}

// OrAnyLocationCode adds OR condition for multiple location code fields
func (qb *QueryBuilder) OrAnyLocationCode(locationCode string, fieldNames ...string) *QueryBuilder {
	if locationCode == "" || len(fieldNames) == 0 {
		return qb
	}
	var parts []string
	for _, fieldName := range fieldNames {
		qb.CheckFieldExist(fieldName)
		if qb.Error != nil {
			return qb
		}
		parts = append(parts, fmt.Sprintf("%s = '%s'", qb.quoteIdentifier(fieldName), strings.ReplaceAll(locationCode, "'", "''")))
	}
	qb.Conditions = append(qb.Conditions, "("+strings.Join(parts, " OR ")+")")
	return qb
}

// BuildOrderByString validates and builds ORDER BY clause from API input array
// Each entry should have: field_name (required), direction (required: asc/desc), null_order (optional: first/last)
func (qb *QueryBuilder) BuildOrderByString(orderByArray []any) (string, error) {
	if qb.TableInterface == nil {
		return "", errors.New("SHOULD_NOT_HAPPEN:TABLE_NOT_SET_FOR_ORDER_BY")
	}

	if len(orderByArray) == 0 {
		return "", nil
	}

	allowedFields := qb.TableInterface.GetOrderByFieldNames()
	if len(allowedFields) == 0 {
		return "", errors.New("SHOULD_NOT_HAPPEN:ORDER_BY_FIELD_NAMES_NOT_CONFIGURED")
	}

	var parts []string
	for _, item := range orderByArray {
		entry, ok := item.(utils.JSON)
		if !ok {
			continue
		}

		fieldName, _ := entry["field_name"].(string)
		direction, _ := entry["direction"].(string)
		nullOrder, _ := entry["null_order"].(string)

		// Validate field_name against allowed list
		if fieldName == "" {
			continue
		}
		if !slices.Contains(allowedFields, fieldName) {
			return "", errors.Errorf("INVALID_ORDER_BY_FIELD_NAME:%s", fieldName)
		}

		// Validate direction (must be asc or desc, case-insensitive)
		if direction == "" {
			continue
		}
		direction = strings.ToLower(direction)
		if direction != string(databases.DXOrderByDirectionAsc) && direction != string(databases.DXOrderByDirectionDesc) {
			return "", errors.Errorf("INVALID_ORDER_BY_DIRECTION:%s", direction)
		}

		// Build the order by part with quoted identifier
		part := qb.quoteIdentifier(fieldName) + " " + strings.ToUpper(direction)

		// Validate null_order if provided (must be first or last, case-insensitive)
		if nullOrder != "" {
			nullOrder = strings.ToLower(nullOrder)
			if nullOrder != string(databases.DXOrderByNullPlacementFirst) && nullOrder != string(databases.DXOrderByNullPlacementLast) {
				return "", errors.Errorf("INVALID_ORDER_BY_NULL_PLACEMENT:%s", nullOrder)
			}
			part += " NULLS " + strings.ToUpper(nullOrder)
		}

		parts = append(parts, part)
	}

	return strings.Join(parts, ", "), nil
}

// Build returns the WHERE clause string and Args
func (qb *QueryBuilder) Build() (string, utils.JSON, error) {
	if qb.Error != nil {
		return "", nil, qb.Error
	}
	if len(qb.Conditions) == 0 {
		return "", qb.Args, nil
	}
	return strings.Join(qb.Conditions, " AND "), qb.Args, nil
}

// BuildWithPrefix returns WHERE clause with prefix for combining with existing Conditions
func (qb *QueryBuilder) BuildWithPrefix(existingWhere string) (string, utils.JSON, error) {
	where, args, err := qb.Build()
	if err != nil {
		return "", nil, err
	}
	if existingWhere != "" && where != "" {
		return fmt.Sprintf("(%s) AND %s", existingWhere, where), args, nil
	}
	if existingWhere != "" {
		return existingWhere, args, nil
	}
	return where, args, nil
}

func (qb *QueryBuilder) buildInClause(fieldName string, values any) string {
	switch v := values.(type) {
	case []int64:
		if len(v) == 0 {
			return "1=0" // Always false
		}
		var strVals []string
		for _, val := range v {
			strVals = append(strVals, fmt.Sprintf("%d", val))
		}
		return fmt.Sprintf("%s IN (%s)", qb.quoteIdentifier(fieldName), strings.Join(strVals, ", "))
	case []string:
		if len(v) == 0 {
			return "1=0"
		}
		var quotedVals []string
		for _, val := range v {
			quotedVals = append(quotedVals, fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")))
		}
		return fmt.Sprintf("%s IN (%s)", qb.quoteIdentifier(fieldName), strings.Join(quotedVals, ", "))
	default:
		// Unknown type - return always false condition to prevent SQL injection
		return "1=0"
	}
}

// SQL Utility Functions

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

// PagingResult - Standardized paging response

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

// Standalone Paging Functions - using database2.DXDatabase

// NamedQueryPaging executes a paging query using databases.DXDatabase
func NamedQueryPaging(
	dxDb3 *databases.DXDatabase,
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
	dxDb3 *databases.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	qb *QueryBuilder,
	orderBy string,
) (*PagingResult, error) {
	whereClause, args, err := qb.Build()
	if err != nil {
		return nil, err
	}
	return NamedQueryPaging(dxDb3, fieldTypeMapping, tableName, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoNamedQueryPagingResponse executes paging and writes standard JSON response
func DoNamedQueryPagingResponse(
	aepr *api.DXAPIEndPointRequest,
	dxDb3 *databases.DXDatabase,
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
	dxDb3 *databases.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	qb *QueryBuilder,
	orderBy string,
) error {
	whereClause, args, err := qb.Build()
	if err != nil {
		return err
	}
	return DoNamedQueryPagingResponse(aepr, dxDb3, fieldTypeMapping, tableName, rowPerPage, pageIndex, whereClause, orderBy, args)
}
