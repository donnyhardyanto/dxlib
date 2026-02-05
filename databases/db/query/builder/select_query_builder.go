package builder

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	utils2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// SelectQueryBuilder - Fluent API for building SELECT SQL clauses (Database Level)
// This is the base builder that handles generic SQL construction.
// For table-specific validation, use tables.TableSelectQueryBuilder which embeds this.

// === JOIN Types ===

// JoinType defines the type of SQL JOIN
type JoinType string

const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
	JoinTypeRight JoinType = "RIGHT"
	JoinTypeFull  JoinType = "FULL"
)

// JoinDef defines a safe JOIN clause
type JoinDef struct {
	Type    JoinType // INNER, LEFT, RIGHT, FULL
	Table   string   // table/view name (will be quoted)
	Alias   string   // optional alias
	OnLeft  string   // left field of ON condition (can include table prefix)
	OnRight string   // right field of ON condition (can include table prefix)
}

// === CTE Types ===

// CTEDef defines a safe Common Table Expression
type CTEDef struct {
	Name      string   // CTE name (will be validated/quoted)
	Columns   []string // optional column list
	SelectSQL string   // the SELECT query (should be built safely)
}

// === ORDER BY Types ===
// Note: Uses databases.DXOrderByDirection and databases.DXOrderByNullPlacement

// OrderByDef defines a safe ORDER BY clause
type OrderByDef struct {
	FieldName     string // field name (will be validated and quoted)
	Direction     string // "asc" or "desc" (use databases.DXOrderByDirection* constants)
	NullPlacement string // "first" or "last" (use databases.DXOrderByNullPlacement* constants), empty for default
}

// === HAVING Types ===

// HavingOperator defines allowed operators for HAVING clause
type HavingOperator string

const (
	HavingOpEq  HavingOperator = "="
	HavingOpNe  HavingOperator = "<>"
	HavingOpGt  HavingOperator = ">"
	HavingOpLt  HavingOperator = "<"
	HavingOpGte HavingOperator = ">="
	HavingOpLte HavingOperator = "<="
)

// HavingConditionDef defines a safe HAVING condition
type HavingConditionDef struct {
	Expression string         // aggregate expression (e.g., "COUNT(*)", "SUM(amount)")
	Operator   HavingOperator // =, >, <, >=, <=, <>
	Value      any            // parameterized value
	ParamName  string         // parameter name for binding
}

// === SelectQueryBuilder ===

// SelectQueryBuilder builds SQL clauses with fluent API
type SelectQueryBuilder struct {
	// Source name for FROM clause (table or view name)
	SourceName string

	// WHERE clause building
	Conditions []string
	Args       utils.JSON
	DbType     base.DXDatabaseType
	Error      error

	// Extended SQL clause building
	Joins            []JoinDef
	CTEs             []CTEDef
	GroupByFields    []string
	HavingConditions []HavingConditionDef
	OrderByDefs      []OrderByDef
	RawOrderBys      []string     // Raw ORDER BY expressions (e.g., PostGIS distance)
	OutFields        []string     // OutFields for SELECT or RETURNING clause
	LimitValue       int64    // LIMIT clause value (0 = no limit)
	OffsetValue      int64    // OFFSET clause value (0 = no offset)
	havingArgCount   int      // internal counter for unique HAVING param names
}

// NewSelectQueryBuilder creates a new SelectQueryBuilder
func NewSelectQueryBuilder(dbType base.DXDatabaseType) *SelectQueryBuilder {
	return &SelectQueryBuilder{
		Conditions: []string{},
		Args:       utils.JSON{},
		DbType:     dbType,
	}
}

// NewSelectQueryBuilderWithSource creates a new SelectQueryBuilder with source name (table or view)
func NewSelectQueryBuilderWithSource(dbType base.DXDatabaseType, sourceName string) *SelectQueryBuilder {
	return &SelectQueryBuilder{
		SourceName: sourceName,
		Conditions: []string{},
		Args:       utils.JSON{},
		DbType:     dbType,
	}
}

// And adds a raw condition with AND
func (qb *SelectQueryBuilder) And(condition string) *SelectQueryBuilder {
	if condition != "" {
		qb.Conditions = append(qb.Conditions, condition)
	}
	return qb
}

// QuoteIdentifier quotes a SQL identifier based on database type to prevent SQL injection
func (qb *SelectQueryBuilder) QuoteIdentifier(identifier string) string {
	return utils2.QuoteIdentifierByDbType(qb.DbType, identifier)
}

// QuoteFieldWithPrefix quotes a field that may have table prefix (e.g., "t.field_name" -> "t"."field_name")
func (qb *SelectQueryBuilder) QuoteFieldWithPrefix(field string) string {
	return utils2.QuoteFieldWithPrefixByDbType(qb.DbType, field)
}

// === JOIN Methods ===

// Join adds an INNER JOIN clause
func (qb *SelectQueryBuilder) Join(table, onLeft, onRight string) *SelectQueryBuilder {
	return qb.addJoin(JoinTypeInner, table, "", onLeft, onRight)
}

// JoinWithAlias adds an INNER JOIN with alias
func (qb *SelectQueryBuilder) JoinWithAlias(table, alias, onLeft, onRight string) *SelectQueryBuilder {
	return qb.addJoin(JoinTypeInner, table, alias, onLeft, onRight)
}

// LeftJoin adds a LEFT JOIN clause
func (qb *SelectQueryBuilder) LeftJoin(table, alias, onLeft, onRight string) *SelectQueryBuilder {
	return qb.addJoin(JoinTypeLeft, table, alias, onLeft, onRight)
}

// RightJoin adds a RIGHT JOIN clause
func (qb *SelectQueryBuilder) RightJoin(table, alias, onLeft, onRight string) *SelectQueryBuilder {
	return qb.addJoin(JoinTypeRight, table, alias, onLeft, onRight)
}

// FullJoin adds a FULL OUTER JOIN clause
func (qb *SelectQueryBuilder) FullJoin(table, alias, onLeft, onRight string) *SelectQueryBuilder {
	return qb.addJoin(JoinTypeFull, table, alias, onLeft, onRight)
}

// addJoin is the internal method to add any type of JOIN
func (qb *SelectQueryBuilder) addJoin(joinType JoinType, table, alias, onLeft, onRight string) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}

	// Validate table name
	if !utils2.IsValidIdentifier(table) {
		qb.Error = errors.Errorf("INVALID_JOIN_TABLE_NAME:%s", table)
		return qb
	}

	// Validate alias if provided
	if alias != "" && !utils2.IsValidIdentifier(alias) {
		qb.Error = errors.Errorf("INVALID_JOIN_ALIAS:%s", alias)
		return qb
	}

	// Validate ON fields
	if !utils2.IsValidIdentifier(onLeft) {
		qb.Error = errors.Errorf("INVALID_JOIN_ON_LEFT_FIELD:%s", onLeft)
		return qb
	}
	if !utils2.IsValidIdentifier(onRight) {
		qb.Error = errors.Errorf("INVALID_JOIN_ON_RIGHT_FIELD:%s", onRight)
		return qb
	}

	qb.Joins = append(qb.Joins, JoinDef{
		Type:    joinType,
		Table:   table,
		Alias:   alias,
		OnLeft:  onLeft,
		OnRight: onRight,
	})
	return qb
}

// === CTE Methods ===

// WithCTE adds a Common Table Expression
// Note: selectSQL should be built using safe methods, not raw user input
func (qb *SelectQueryBuilder) WithCTE(name string, columns []string, selectSQL string) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}

	// Validate CTE name
	if !utils2.IsValidIdentifier(name) {
		qb.Error = errors.Errorf("INVALID_CTE_NAME:%s", name)
		return qb
	}

	// Validate column names if provided
	for _, col := range columns {
		if !utils2.IsValidIdentifier(col) {
			qb.Error = errors.Errorf("INVALID_CTE_COLUMN_NAME:%s", col)
			return qb
		}
	}

	// Basic validation of selectSQL - must not be empty
	if strings.TrimSpace(selectSQL) == "" {
		qb.Error = errors.New("CTE_SELECT_SQL_CANNOT_BE_EMPTY")
		return qb
	}

	qb.CTEs = append(qb.CTEs, CTEDef{
		Name:      name,
		Columns:   columns,
		SelectSQL: selectSQL,
	})
	return qb
}

// === GROUP BY Methods ===

// GroupBy adds GROUP BY fields
func (qb *SelectQueryBuilder) GroupBy(fields ...string) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}

	for _, field := range fields {
		if !utils2.IsValidIdentifier(field) {
			qb.Error = errors.Errorf("INVALID_GROUP_BY_FIELD:%s", field)
			return qb
		}
		qb.GroupByFields = append(qb.GroupByFields, field)
	}
	return qb
}

// === HAVING Methods ===

// Having adds a HAVING condition with parameterized value
// expression: aggregate expression like "COUNT(*)", "SUM(amount)", "MAX(price)"
// operator: comparison operator (use HavingOp* constants)
// value: the value to compare against (will be parameterized)
func (qb *SelectQueryBuilder) Having(expression string, operator HavingOperator, value any) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}

	// Validate operator
	switch operator {
	case HavingOpEq, HavingOpNe, HavingOpGt, HavingOpLt, HavingOpGte, HavingOpLte:
		// valid
	default:
		qb.Error = errors.Errorf("INVALID_HAVING_OPERATOR:%s", operator)
		return qb
	}

	// Basic validation of expression - should not be empty
	if strings.TrimSpace(expression) == "" {
		qb.Error = errors.New("HAVING_EXPRESSION_CANNOT_BE_EMPTY")
		return qb
	}

	// Generate unique parameter name
	paramName := fmt.Sprintf("having_%d", qb.havingArgCount)
	qb.havingArgCount++

	qb.HavingConditions = append(qb.HavingConditions, HavingConditionDef{
		Expression: expression,
		Operator:   operator,
		Value:      value,
		ParamName:  paramName,
	})
	return qb
}

// === FIELDS Methods ===

// Select specifies fields for SELECT or RETURNING clause
func (qb *SelectQueryBuilder) Select(fields ...string) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.OutFields = append(qb.OutFields, fields...)
	return qb
}

// Limit sets the LIMIT clause value
func (qb *SelectQueryBuilder) Limit(limit int64) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.LimitValue = limit
	return qb
}

// Offset sets the OFFSET clause value
func (qb *SelectQueryBuilder) Offset(offset int64) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.OffsetValue = offset
	return qb
}

// === ORDER BY Methods (without table validation) ===

// AddOrderBy adds an ORDER BY clause with field name, direction, and optional null placement
// direction: "asc" or "desc"
// nullPlacement: "first" or "last" (empty for default)
// Note: This method does NOT validate against allowed fields - use TableSelectQueryBuilder for that
func (qb *SelectQueryBuilder) AddOrderBy(fieldName string, direction string, nullPlacement string) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}

	// Validate field name format
	if !utils2.IsValidIdentifier(fieldName) {
		qb.Error = errors.Errorf("INVALID_ORDER_BY_FIELD:%s", fieldName)
		return qb
	}

	// Validate direction
	dir := strings.ToLower(direction)
	if dir != "asc" && dir != "desc" {
		qb.Error = errors.Errorf("INVALID_ORDER_BY_DIRECTION:%s", direction)
		return qb
	}

	// Validate null placement if provided
	if nullPlacement != "" {
		np := strings.ToLower(nullPlacement)
		if np != "first" && np != "last" {
			qb.Error = errors.Errorf("INVALID_ORDER_BY_NULL_PLACEMENT:%s", nullPlacement)
			return qb
		}
	}

	qb.OrderByDefs = append(qb.OrderByDefs, OrderByDef{
		FieldName:     fieldName,
		Direction:     strings.ToLower(direction),
		NullPlacement: strings.ToLower(nullPlacement),
	})
	return qb
}

// AddOrderByRaw adds a raw ORDER BY expression (e.g., PostGIS distance expressions).
// No validation is performed on the expression — caller is responsible for safety.
func (qb *SelectQueryBuilder) AddOrderByRaw(rawExpr string) *SelectQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	if rawExpr != "" {
		qb.RawOrderBys = append(qb.RawOrderBys, rawExpr)
	}
	return qb
}

// === Build Methods for Extended Clauses ===

// BuildJoinClause returns the JOIN clause string
func (qb *SelectQueryBuilder) BuildJoinClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.Joins) == 0 {
		return "", nil
	}

	var parts []string
	for _, j := range qb.Joins {
		var joinSQL string

		// Build: TYPE JOIN "table" [AS "alias"] ON "left" = "right"
		quotedTable := qb.QuoteIdentifier(j.Table)
		if j.Alias != "" {
			quotedTable += " AS " + qb.QuoteIdentifier(j.Alias)
		}

		quotedLeft := qb.QuoteFieldWithPrefix(j.OnLeft)
		quotedRight := qb.QuoteFieldWithPrefix(j.OnRight)

		joinSQL = fmt.Sprintf("%s JOIN %s ON %s = %s", j.Type, quotedTable, quotedLeft, quotedRight)
		parts = append(parts, joinSQL)
	}

	return strings.Join(parts, " "), nil
}

// BuildCTEClause returns the WITH clause string
func (qb *SelectQueryBuilder) BuildCTEClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.CTEs) == 0 {
		return "", nil
	}

	var parts []string
	for _, cte := range qb.CTEs {
		var cteSQL string

		quotedName := qb.QuoteIdentifier(cte.Name)

		if len(cte.Columns) > 0 {
			var quotedCols []string
			for _, col := range cte.Columns {
				quotedCols = append(quotedCols, qb.QuoteIdentifier(col))
			}
			cteSQL = fmt.Sprintf("%s (%s) AS (%s)", quotedName, strings.Join(quotedCols, ", "), cte.SelectSQL)
		} else {
			cteSQL = fmt.Sprintf("%s AS (%s)", quotedName, cte.SelectSQL)
		}

		parts = append(parts, cteSQL)
	}

	return "WITH " + strings.Join(parts, ", ") + " ", nil
}

// BuildGroupByClause returns the GROUP BY clause string
func (qb *SelectQueryBuilder) BuildGroupByClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.GroupByFields) == 0 {
		return "", nil
	}

	var quotedFields []string
	for _, field := range qb.GroupByFields {
		quotedFields = append(quotedFields, qb.QuoteFieldWithPrefix(field))
	}

	return "GROUP BY " + strings.Join(quotedFields, ", "), nil
}

// BuildHavingClause returns the HAVING clause and additional args
func (qb *SelectQueryBuilder) BuildHavingClause() (string, utils.JSON, error) {
	if qb.Error != nil {
		return "", nil, qb.Error
	}

	if len(qb.HavingConditions) == 0 {
		return "", nil, nil
	}

	args := utils.JSON{}
	var parts []string

	for _, h := range qb.HavingConditions {
		// Build: expression operator :paramName
		part := fmt.Sprintf("%s %s :%s", h.Expression, h.Operator, h.ParamName)
		parts = append(parts, part)
		args[h.ParamName] = h.Value
	}

	return "HAVING " + strings.Join(parts, " AND "), args, nil
}

// BuildReturningClause returns the RETURNING clause string for PostgreSQL/MariaDB
// For SQL Server OUTPUT clause, use BuildOutputClause instead
func (qb *SelectQueryBuilder) BuildReturningClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.OutFields) == 0 {
		return "", nil
	}

	var quotedFields []string
	for _, field := range qb.OutFields {
		quotedFields = append(quotedFields, qb.QuoteIdentifier(field))
	}

	return "RETURNING " + strings.Join(quotedFields, ", "), nil
}

// BuildOutputClause returns the OUTPUT clause string for SQL Server
// prefix should be "INSERTED" for INSERT, "DELETED" for DELETE, or "DELETED"/"INSERTED" for UPDATE
func (qb *SelectQueryBuilder) BuildOutputClause(prefix string) (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.OutFields) == 0 {
		return "", nil
	}

	// Validate prefix
	upperPrefix := strings.ToUpper(prefix)
	if upperPrefix != "INSERTED" && upperPrefix != "DELETED" {
		return "", errors.Errorf("INVALID_OUTPUT_PREFIX:%s", prefix)
	}

	var quotedFields []string
	for _, field := range qb.OutFields {
		quotedFields = append(quotedFields, upperPrefix+"."+qb.QuoteIdentifier(field))
	}

	return "OUTPUT " + strings.Join(quotedFields, ", "), nil
}

// BuildOrderByClause returns the ORDER BY clause string from OrderByDefs and RawOrderBys
func (qb *SelectQueryBuilder) BuildOrderByClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.OrderByDefs) == 0 && len(qb.RawOrderBys) == 0 {
		return "", nil
	}

	var parts []string
	for _, o := range qb.OrderByDefs {
		// Build: "field" ASC [NULLS FIRST|LAST]
		part := qb.QuoteIdentifier(o.FieldName) + " " + strings.ToUpper(o.Direction)

		if o.NullPlacement != "" {
			part += " NULLS " + strings.ToUpper(o.NullPlacement)
		}

		parts = append(parts, part)
	}

	// Append raw ORDER BY expressions
	parts = append(parts, qb.RawOrderBys...)

	return strings.Join(parts, ", "), nil
}

// Build returns the WHERE clause string and Args
func (qb *SelectQueryBuilder) Build() (string, utils.JSON, error) {
	if qb.Error != nil {
		return "", nil, qb.Error
	}
	if len(qb.Conditions) == 0 {
		return "", qb.Args, nil
	}
	return strings.Join(qb.Conditions, " AND "), qb.Args, nil
}

// BuildWithPrefix returns WHERE clause with prefix for combining with existing Conditions
func (qb *SelectQueryBuilder) BuildWithPrefix(existingWhere string) (string, utils.JSON, error) {
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

// BuildInClause builds an IN clause for the given field and values
func (qb *SelectQueryBuilder) BuildInClause(fieldName string, values any) string {
	switch v := values.(type) {
	case []int64:
		if len(v) == 0 {
			return "1=0" // Always false
		}
		var strVals []string
		for _, val := range v {
			strVals = append(strVals, fmt.Sprintf("%d", val))
		}
		return fmt.Sprintf("%s IN (%s)", qb.QuoteIdentifier(fieldName), strings.Join(strVals, ", "))
	case []string:
		if len(v) == 0 {
			return "1=0"
		}
		var quotedVals []string
		for _, val := range v {
			quotedVals = append(quotedVals, fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")))
		}
		return fmt.Sprintf("%s IN (%s)", qb.QuoteIdentifier(fieldName), strings.Join(quotedVals, ", "))
	default:
		// Unknown type - return always false condition to prevent SQL injection
		return "1=0"
	}
}
