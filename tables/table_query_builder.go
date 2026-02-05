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
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	utils2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TableQueryBuilder - Fluent API for building SQL clauses with table-level field validation
// Embeds db.SelectQueryBuilder and adds DXRawTableInterface for field name validation

// TableQueryBuilder wraps db.SelectQueryBuilder with table-specific validation
type TableQueryBuilder struct {
	*builder.SelectQueryBuilder                     // Embed base QueryBuilder
	TableInterface              DXRawTableInterface // For field validation
}

// NewTableQueryBuilder creates a new TableQueryBuilder with table interface for validation
func NewTableQueryBuilder(dbType base.DXDatabaseType, tableInterface DXRawTableInterface) *TableQueryBuilder {
	return &TableQueryBuilder{
		SelectQueryBuilder: builder.NewSelectQueryBuilder(dbType),
		TableInterface:     tableInterface,
	}
}

// NewTableQueryBuilderWithSource creates a new TableQueryBuilder with explicit source name (table or view)
func NewTableQueryBuilderWithSource(dbType base.DXDatabaseType, sourceName string, tableInterface DXRawTableInterface) *TableQueryBuilder {
	return &TableQueryBuilder{
		SelectQueryBuilder: builder.NewSelectQueryBuilderWithSource(dbType, sourceName),
		TableInterface:     tableInterface,
	}
}

// === Field Validation Methods ===

// IsFieldExist checks if a field exists in the table's search field names
func (tqb *TableQueryBuilder) IsFieldExist(fieldName string) bool {
	if tqb.TableInterface == nil {
		return false
	}
	searchFieldNames := tqb.TableInterface.GetSearchTextFieldNames()
	return slices.Contains(searchFieldNames, fieldName)
}

// CheckFieldExist validates field exists and sets error if not
func (tqb *TableQueryBuilder) CheckFieldExist(fieldName string) *TableQueryBuilder {
	if tqb.TableInterface == nil {
		tqb.Error = errors.New(fmt.Sprintf("SHOULD_NOT_HAPPEN:TABLE_NOT_SET:%s", fieldName))
		return tqb
	}
	searchFieldNames := tqb.TableInterface.GetSearchTextFieldNames()
	if !slices.Contains(searchFieldNames, fieldName) {
		tqb.Error = errors.New(fmt.Sprintf("SHOULD_NOT_HAPPEN:INVALID_FIELD_NAME_IN_TABLE:%s:%s", tqb.TableInterface.GetFullTableName(), fieldName))
		return tqb
	}
	return tqb
}

// === WHERE Clause Methods with Field Validation ===

// Eq adds field = value condition with field validation
func (tqb *TableQueryBuilder) Eq(fieldName string, value any) *TableQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s = :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = value
	return tqb
}

// EqOrIn adds field = value for single values, or field IN (values) for arrays
// Supports: []any, []string, []int64, []float64 (converted to []int64)
func (tqb *TableQueryBuilder) EqOrIn(fieldName string, value any) *TableQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}

	switch v := value.(type) {
	case []any:
		return tqb.inFromAnySlice(fieldName, v)
	case []string:
		if len(v) == 0 {
			return tqb
		}
		return tqb.InStrings(fieldName, v)
	case []int64:
		if len(v) == 0 {
			return tqb
		}
		return tqb.InInt64(fieldName, v)
	case []float64:
		if len(v) == 0 {
			return tqb
		}
		var int64Vals []int64
		for _, f := range v {
			int64Vals = append(int64Vals, int64(f))
		}
		return tqb.InInt64(fieldName, int64Vals)
	default:
		// Single value - use Eq
		tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s = :%s", tqb.QuoteIdentifier(fieldName), fieldName))
		tqb.Args[fieldName] = value
		return tqb
	}
}

// inFromAnySlice handles []any from JSON parsing
func (tqb *TableQueryBuilder) inFromAnySlice(fieldName string, values []any) *TableQueryBuilder {
	if len(values) == 0 {
		return tqb
	}

	first := values[0]
	switch first.(type) {
	case string:
		var strVals []string
		for _, v := range values {
			if s, ok := v.(string); ok {
				strVals = append(strVals, s)
			}
		}
		return tqb.InStrings(fieldName, strVals)
	case float64:
		var int64Vals []int64
		for _, v := range values {
			if f, ok := v.(float64); ok {
				int64Vals = append(int64Vals, int64(f))
			}
		}
		return tqb.InInt64(fieldName, int64Vals)
	case int64:
		var int64Vals []int64
		for _, v := range values {
			if i, ok := v.(int64); ok {
				int64Vals = append(int64Vals, i)
			}
		}
		return tqb.InInt64(fieldName, int64Vals)
	default:
		var strVals []string
		for _, v := range values {
			strVals = append(strVals, fmt.Sprintf("%v", v))
		}
		return tqb.InStrings(fieldName, strVals)
	}
}

// Ne adds field != value condition with field validation
func (tqb *TableQueryBuilder) Ne(fieldName string, value any) *TableQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s != :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = value
	return tqb
}

// Like adds field LIKE value condition (case-sensitive) with field validation
func (tqb *TableQueryBuilder) Like(fieldName string, value string) *TableQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s LIKE :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = "%" + value + "%"
	return tqb
}

// ILike adds field ILIKE value condition (case-insensitive, PostgreSQL) with field validation
func (tqb *TableQueryBuilder) ILike(fieldName string, value string) *TableQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s ILIKE :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = "%" + value + "%"
	return tqb
}

// SearchLike adds OR condition for multiple fields with ILIKE
func (tqb *TableQueryBuilder) SearchLike(value string, fieldNames ...string) *TableQueryBuilder {
	if value == "" || len(fieldNames) == 0 {
		return tqb
	}
	var parts []string
	for i, fieldName := range fieldNames {
		tqb.CheckFieldExist(fieldName)
		if tqb.Error != nil {
			return tqb
		}
		argName := fmt.Sprintf("search_%d", i)
		parts = append(parts, fmt.Sprintf("%s ILIKE :%s", tqb.QuoteIdentifier(fieldName), argName))
		tqb.Args[argName] = "%" + value + "%"
	}
	tqb.Conditions = append(tqb.Conditions, "("+strings.Join(parts, " OR ")+")")
	return tqb
}

// In adds field IN (values) condition with field validation
func (tqb *TableQueryBuilder) In(fieldName string, values any) *TableQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, tqb.BuildInClause(fieldName, values))
	return tqb
}

// InInt64 adds field IN (values) for int64 slice with field validation
func (tqb *TableQueryBuilder) InInt64(fieldName string, values []int64) *TableQueryBuilder {
	if len(values) == 0 {
		return tqb
	}
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	var strVals []string
	for _, v := range values {
		strVals = append(strVals, fmt.Sprintf("%d", v))
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s IN (%s)", tqb.QuoteIdentifier(fieldName), strings.Join(strVals, ", ")))
	return tqb
}

// InStrings adds field IN (values) for string slice with field validation
func (tqb *TableQueryBuilder) InStrings(fieldName string, values []string) *TableQueryBuilder {
	if len(values) == 0 {
		return tqb
	}
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	var quotedVals []string
	for _, v := range values {
		quotedVals = append(quotedVals, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s IN (%s)", tqb.QuoteIdentifier(fieldName), strings.Join(quotedVals, ", ")))
	return tqb
}

// NotDeleted adds is_deleted = false condition (database-aware)
func (tqb *TableQueryBuilder) NotDeleted() *TableQueryBuilder {
	switch tqb.DbType {
	case base.DXDatabaseTypeSQLServer:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = 0")
	default:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = false")
	}
	return tqb
}

// OrAnyLocationCode adds OR condition for multiple location code fields
func (tqb *TableQueryBuilder) OrAnyLocationCode(locationCode string, fieldNames ...string) *TableQueryBuilder {
	if locationCode == "" || len(fieldNames) == 0 {
		return tqb
	}
	var parts []string
	for _, fieldName := range fieldNames {
		tqb.CheckFieldExist(fieldName)
		if tqb.Error != nil {
			return tqb
		}
		parts = append(parts, fmt.Sprintf("%s = '%s'", tqb.QuoteIdentifier(fieldName), strings.ReplaceAll(locationCode, "'", "''")))
	}
	tqb.Conditions = append(tqb.Conditions, "("+strings.Join(parts, " OR ")+")")
	return tqb
}

// === ORDER BY Methods with Field Validation ===

// OrderBy adds an ORDER BY clause with field validation
// direction: use databases.DXOrderByDirectionAsc or databases.DXOrderByDirectionDesc
// nullPlacement: use databases.DXOrderByNullPlacementFirst or databases.DXOrderByNullPlacementLast (or empty)
func (tqb *TableQueryBuilder) OrderBy(fieldName string, direction databases.DXOrderByDirection, nullPlacement databases.DXOrderByNullPlacement) *TableQueryBuilder {
	if tqb.Error != nil {
		return tqb
	}

	// Validate field name format
	if !utils2.IsValidIdentifier(fieldName) {
		tqb.Error = errors.Errorf("INVALID_ORDER_BY_FIELD:%s", fieldName)
		return tqb
	}

	// Validate against allowed OrderByFieldNames if TableInterface is set
	if tqb.TableInterface != nil {
		allowedFields := tqb.TableInterface.GetOrderByFieldNames()
		if len(allowedFields) > 0 && !slices.Contains(allowedFields, fieldName) {
			tqb.Error = errors.Errorf("FIELD_NOT_IN_ORDER_BY_WHITELIST:%s", fieldName)
			return tqb
		}
	}

	// Validate direction
	if direction != databases.DXOrderByDirectionAsc && direction != databases.DXOrderByDirectionDesc {
		tqb.Error = errors.Errorf("INVALID_ORDER_BY_DIRECTION:%s", direction)
		return tqb
	}

	// Validate null placement if provided
	if nullPlacement != "" && nullPlacement != databases.DXOrderByNullPlacementFirst && nullPlacement != databases.DXOrderByNullPlacementLast {
		tqb.Error = errors.Errorf("INVALID_ORDER_BY_NULL_PLACEMENT:%s", nullPlacement)
		return tqb
	}

	tqb.OrderByDefs = append(tqb.OrderByDefs, builder.OrderByDef{
		FieldName:     fieldName,
		Direction:     string(direction),
		NullPlacement: string(nullPlacement),
	})
	return tqb
}

// OrderByAsc adds an ascending ORDER BY clause
func (tqb *TableQueryBuilder) OrderByAsc(fieldName string) *TableQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionAsc, "")
}

// OrderByDesc adds a descending ORDER BY clause
func (tqb *TableQueryBuilder) OrderByDesc(fieldName string) *TableQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionDesc, "")
}

// OrderByAscNullsFirst adds an ascending ORDER BY with NULLS FIRST
func (tqb *TableQueryBuilder) OrderByAscNullsFirst(fieldName string) *TableQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionAsc, databases.DXOrderByNullPlacementFirst)
}

// OrderByAscNullsLast adds an ascending ORDER BY with NULLS LAST
func (tqb *TableQueryBuilder) OrderByAscNullsLast(fieldName string) *TableQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionAsc, databases.DXOrderByNullPlacementLast)
}

// OrderByDescNullsFirst adds a descending ORDER BY with NULLS FIRST
func (tqb *TableQueryBuilder) OrderByDescNullsFirst(fieldName string) *TableQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionDesc, databases.DXOrderByNullPlacementFirst)
}

// OrderByDescNullsLast adds a descending ORDER BY with NULLS LAST
func (tqb *TableQueryBuilder) OrderByDescNullsLast(fieldName string) *TableQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionDesc, databases.DXOrderByNullPlacementLast)
}

// BuildOrderByString validates and builds ORDER BY clause from API input array
// Each entry should have: field_name (required), direction (required: asc/desc), null_order (optional: first/last)
func (tqb *TableQueryBuilder) BuildOrderByString(orderByArray []any) (string, error) {
	if tqb.TableInterface == nil {
		return "", errors.New("SHOULD_NOT_HAPPEN:TABLE_NOT_SET_FOR_ORDER_BY")
	}

	if len(orderByArray) == 0 {
		return "", nil
	}

	allowedFields := tqb.TableInterface.GetOrderByFieldNames()
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

		// Validate direction
		if direction == "" {
			continue
		}
		direction = strings.ToLower(direction)
		if direction != string(databases.DXOrderByDirectionAsc) && direction != string(databases.DXOrderByDirectionDesc) {
			return "", errors.Errorf("INVALID_ORDER_BY_DIRECTION:%s", direction)
		}

		// Build the order by part with quoted identifier
		part := tqb.QuoteIdentifier(fieldName) + " " + strings.ToUpper(direction)

		// Validate null_order if provided
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

// === Paging Support ===

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

// === Standalone Paging Functions ===

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

// NamedQueryPagingWithBuilder executes a paging query using TableQueryBuilder
func NamedQueryPagingWithBuilder(
	dxDb3 *databases.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	tqb *TableQueryBuilder,
	orderBy string,
) (*PagingResult, error) {
	whereClause, args, err := tqb.Build()
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

// DoNamedQueryPagingResponseWithBuilder executes paging with TableQueryBuilder and writes response
func DoNamedQueryPagingResponseWithBuilder(
	aepr *api.DXAPIEndPointRequest,
	dxDb3 *databases.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	tqb *TableQueryBuilder,
	orderBy string,
) error {
	whereClause, args, err := tqb.Build()
	if err != nil {
		return err
	}
	return DoNamedQueryPagingResponse(aepr, dxDb3, fieldTypeMapping, tableName, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// === Backward Compatibility Aliases ===

// QueryBuilder is an alias for TableQueryBuilder for backward compatibility
// Deprecated: Use TableQueryBuilder directly
type QueryBuilder = TableQueryBuilder

// NewQueryBuilder creates a new TableQueryBuilder (backward compatibility)
// Deprecated: Use NewTableQueryBuilder directly
func NewQueryBuilder(dbType base.DXDatabaseType, tableInterface DXRawTableInterface) *TableQueryBuilder {
	return NewTableQueryBuilder(dbType, tableInterface)
}

// Re-export types from db package for backward compatibility
type (
	JoinType       = builder.JoinType
	JoinDef        = builder.JoinDef
	CTEDef         = builder.CTEDef
	OrderByDef     = builder.OrderByDef
	HavingOperator = builder.HavingOperator
	HavingDef      = builder.HavingConditionDef
)

// Re-export constants from db package for backward compatibility
const (
	JoinTypeInner = builder.JoinTypeInner
	JoinTypeLeft  = builder.JoinTypeLeft
	JoinTypeRight = builder.JoinTypeRight
	JoinTypeFull  = builder.JoinTypeFull

	HavingOpEq  = builder.HavingOpEq
	HavingOpNe  = builder.HavingOpNe
	HavingOpGt  = builder.HavingOpGt
	HavingOpLt  = builder.HavingOpLt
	HavingOpGte = builder.HavingOpGte
	HavingOpLte = builder.HavingOpLte
)

// Re-export utility functions from db package for backward compatibility
var (
	SQLBuildWhereInClauseStrings = utils2.SQLBuildWhereInClauseStrings
	SQLBuildWhereInClauseInt64   = utils2.SQLBuildWhereInClauseInt64
)
