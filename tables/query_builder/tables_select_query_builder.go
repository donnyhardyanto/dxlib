package query_builder

import (
	"fmt"
	"slices"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	utils2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TableInterface defines the interface for table-level field validation.
// This avoids import cycle with the tables package.
type TableInterface interface {
	GetSearchTextFieldNames() []string
	GetOrderByFieldNames() []string
	GetFullTableName() string
}

// TableSelectQueryBuilder wraps builder.SelectQueryBuilder with table-specific field validation.
// Drop-in replacement for the old TableQueryBuilder in the tables package.
type TableSelectQueryBuilder struct {
	*builder.SelectQueryBuilder
	TableInterface TableInterface
}

// NewTableSelectQueryBuilder creates a new TableSelectQueryBuilder with table interface for validation
func NewTableSelectQueryBuilder(dbType base.DXDatabaseType, tableInterface TableInterface) *TableSelectQueryBuilder {
	return &TableSelectQueryBuilder{
		SelectQueryBuilder: builder.NewSelectQueryBuilder(dbType),
		TableInterface:     tableInterface,
	}
}

// NewTableSelectQueryBuilderWithSource creates a new TableSelectQueryBuilder with explicit source name
func NewTableSelectQueryBuilderWithSource(dbType base.DXDatabaseType, sourceName string, tableInterface TableInterface) *TableSelectQueryBuilder {
	return &TableSelectQueryBuilder{
		SelectQueryBuilder: builder.NewSelectQueryBuilderWithSource(dbType, sourceName),
		TableInterface:     tableInterface,
	}
}

// === Field Validation Methods ===

// IsFieldExist checks if a field exists in the table's search field names
func (tqb *TableSelectQueryBuilder) IsFieldExist(fieldName string) bool {
	if tqb.TableInterface == nil {
		return false
	}
	searchFieldNames := tqb.TableInterface.GetSearchTextFieldNames()
	return slices.Contains(searchFieldNames, fieldName)
}

// CheckFieldExist validates field exists and sets error if not
func (tqb *TableSelectQueryBuilder) CheckFieldExist(fieldName string) *TableSelectQueryBuilder {
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
func (tqb *TableSelectQueryBuilder) Eq(fieldName string, value any) *TableSelectQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s = :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = value
	return tqb
}

// EqOrIn adds field = value for single values, or field IN (values) for arrays
func (tqb *TableSelectQueryBuilder) EqOrIn(fieldName string, value any) *TableSelectQueryBuilder {
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
		tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s = :%s", tqb.QuoteIdentifier(fieldName), fieldName))
		tqb.Args[fieldName] = value
		return tqb
	}
}

// inFromAnySlice handles []any from JSON parsing
func (tqb *TableSelectQueryBuilder) inFromAnySlice(fieldName string, values []any) *TableSelectQueryBuilder {
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
func (tqb *TableSelectQueryBuilder) Ne(fieldName string, value any) *TableSelectQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s != :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = value
	return tqb
}

// Like adds field LIKE value condition (case-sensitive) with field validation
func (tqb *TableSelectQueryBuilder) Like(fieldName string, value string) *TableSelectQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s LIKE :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = "%" + value + "%"
	return tqb
}

// ILike adds field ILIKE value condition (case-insensitive, PostgreSQL) with field validation
func (tqb *TableSelectQueryBuilder) ILike(fieldName string, value string) *TableSelectQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, fmt.Sprintf("%s ILIKE :%s", tqb.QuoteIdentifier(fieldName), fieldName))
	tqb.Args[fieldName] = "%" + value + "%"
	return tqb
}

// SearchLike adds OR condition for multiple fields with ILIKE
func (tqb *TableSelectQueryBuilder) SearchLike(value string, fieldNames ...string) *TableSelectQueryBuilder {
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
func (tqb *TableSelectQueryBuilder) In(fieldName string, values any) *TableSelectQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.Conditions = append(tqb.Conditions, tqb.BuildInClause(fieldName, values))
	return tqb
}

// InInt64 adds field IN (values) for int64 slice with field validation
func (tqb *TableSelectQueryBuilder) InInt64(fieldName string, values []int64) *TableSelectQueryBuilder {
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
func (tqb *TableSelectQueryBuilder) InStrings(fieldName string, values []string) *TableSelectQueryBuilder {
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
func (tqb *TableSelectQueryBuilder) NotDeleted() *TableSelectQueryBuilder {
	switch tqb.DbType {
	case base.DXDatabaseTypeSQLServer:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = 0")
	default:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = false")
	}
	return tqb
}

// OrAnyLocationCode adds OR condition for multiple location code fields
func (tqb *TableSelectQueryBuilder) OrAnyLocationCode(locationCode string, fieldNames ...string) *TableSelectQueryBuilder {
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
func (tqb *TableSelectQueryBuilder) OrderBy(fieldName string, direction databases.DXOrderByDirection, nullPlacement databases.DXOrderByNullPlacement) *TableSelectQueryBuilder {
	if tqb.Error != nil {
		return tqb
	}

	if !utils2.IsValidIdentifier(fieldName) {
		tqb.Error = errors.Errorf("INVALID_ORDER_BY_FIELD:%s", fieldName)
		return tqb
	}

	if tqb.TableInterface != nil {
		allowedFields := tqb.TableInterface.GetOrderByFieldNames()
		if len(allowedFields) > 0 && !slices.Contains(allowedFields, fieldName) {
			tqb.Error = errors.Errorf("FIELD_NOT_IN_ORDER_BY_WHITELIST:%s", fieldName)
			return tqb
		}
	}

	if direction != databases.DXOrderByDirectionAsc && direction != databases.DXOrderByDirectionDesc {
		tqb.Error = errors.Errorf("INVALID_ORDER_BY_DIRECTION:%s", direction)
		return tqb
	}

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
func (tqb *TableSelectQueryBuilder) OrderByAsc(fieldName string) *TableSelectQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionAsc, "")
}

// OrderByDesc adds a descending ORDER BY clause
func (tqb *TableSelectQueryBuilder) OrderByDesc(fieldName string) *TableSelectQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionDesc, "")
}

// OrderByAscNullsFirst adds an ascending ORDER BY with NULLS FIRST
func (tqb *TableSelectQueryBuilder) OrderByAscNullsFirst(fieldName string) *TableSelectQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionAsc, databases.DXOrderByNullPlacementFirst)
}

// OrderByAscNullsLast adds an ascending ORDER BY with NULLS LAST
func (tqb *TableSelectQueryBuilder) OrderByAscNullsLast(fieldName string) *TableSelectQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionAsc, databases.DXOrderByNullPlacementLast)
}

// OrderByDescNullsFirst adds a descending ORDER BY with NULLS FIRST
func (tqb *TableSelectQueryBuilder) OrderByDescNullsFirst(fieldName string) *TableSelectQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionDesc, databases.DXOrderByNullPlacementFirst)
}

// OrderByDescNullsLast adds a descending ORDER BY with NULLS LAST
func (tqb *TableSelectQueryBuilder) OrderByDescNullsLast(fieldName string) *TableSelectQueryBuilder {
	return tqb.OrderBy(fieldName, databases.DXOrderByDirectionDesc, databases.DXOrderByNullPlacementLast)
}

// BuildOrderByString validates and builds ORDER BY clause from API input array
func (tqb *TableSelectQueryBuilder) BuildOrderByString(orderByArray []any) (string, error) {
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

		if fieldName == "" {
			continue
		}
		if !slices.Contains(allowedFields, fieldName) {
			return "", errors.Errorf("INVALID_ORDER_BY_FIELD_NAME:%s", fieldName)
		}

		if direction == "" {
			continue
		}
		direction = strings.ToLower(direction)
		if direction != string(databases.DXOrderByDirectionAsc) && direction != string(databases.DXOrderByDirectionDesc) {
			return "", errors.Errorf("INVALID_ORDER_BY_DIRECTION:%s", direction)
		}

		part := tqb.QuoteIdentifier(fieldName) + " " + strings.ToUpper(direction)

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
